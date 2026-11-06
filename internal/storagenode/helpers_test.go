package storagenode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	t0        = int64(1_792_800_000_000)
	testShard = 1
	testPod   = "storage-1-1"
	// testServerID is testPod's Raft ServerID (podName/incarnation).
	testServerID = testPod + "/00000000-0000-4000-8000-000000000011"
	testToken    = "op-secret"
)

// operatorAuth is the header /mgmt/bootstrap requires.
var operatorAuth = map[string]string{"Authorization": "Bearer " + testToken}

func loadRing(t *testing.T) *ring.Ring {
	t.Helper()
	rg, err := ring.Load("../../contracts/ring.yaml")
	if err != nil {
		t.Fatalf("load ring: %v", err)
	}
	return rg
}

// keyOn returns prefix+id for the first id "<base>-<n>" whose key the ring
// places on shard, and the id.
func keyOn(t *testing.T, rg *ring.Ring, shard int, prefix, base string) (key, id string) {
	t.Helper()
	for n := range 10_000 {
		id = fmt.Sprintf("%s-%d", base, n)
		owner, err := rg.Owner(prefix + id)
		if err != nil {
			t.Fatal(err)
		}
		if owner == shard {
			return prefix + id, id
		}
	}
	t.Fatalf("no %s key on shard %d", prefix, shard)
	return "", ""
}

// fakeNode records commands and serves reads from a real in-memory FSM.
type fakeNode struct {
	mu       sync.Mutex
	applied  []any
	applyRes store.Result
	applyErr error
	modes    []raftstore.ReadMode
	readErr  error
	// readDelay makes every Read take this long, or until its ctx ends
	// (then it fails with ctx's error, as a fenced read does).
	readDelay time.Duration
	f         *fsm.FSM
	index     uint64
	status    raftstore.Status
	statuses  int // Status calls
	term      uint64
	bootErr   error
	booted    []raft.Server
	snapInfo  raftstore.SnapshotInfo
	snapErr   error
	snaps     int // Snapshot calls

	storage    raftstore.StorageInfo // StorageInfo's answer (metrics)
	storageErr error

	stepDownErr error
	stepDowns   int // StepDown calls

	reading      bool          // a Read is running
	waits        []uint64      // WaitApplyAfter indexes
	waitedInRead bool          // WaitApplyAfter ran inside a Read
	onWait       func()        // if set, WaitApplyAfter runs it and returns (a commit)
	waitDone     chan struct{} // closed when a WaitApplyAfter returns
}

var fakeSeq atomic.Int64

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	f, err := fsm.Open(context.Background(), fmt.Sprintf("file:storagenode-fake-%d?mode=memory", fakeSeq.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return &fakeNode{f: f, applyRes: store.Result{Outcome: store.OutcomeOK, OperationID: "op", Key: "k", Version: 3, Shard: testShard, AppliedIndex: 42}}
}

// seed applies a command to the backing FSM directly.
func (n *fakeNode) seed(t *testing.T, cmd any) store.Result {
	t.Helper()
	entry, err := fsm.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	n.index++
	res, err := n.f.Apply(n.index, entry)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (n *fakeNode) Apply(ctx context.Context, cmd any) (store.Result, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applied = append(n.applied, cmd)
	return n.applyRes, n.applyErr
}

func (n *fakeNode) Read(ctx context.Context, mode raftstore.ReadMode, fn func(context.Context, raftstore.Reader) error) (raftstore.ReadInfo, error) {
	n.mu.Lock()
	n.modes = append(n.modes, mode)
	readErr, delay := n.readErr, n.readDelay
	n.mu.Unlock()
	if readErr != nil {
		return raftstore.ReadInfo{}, readErr
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return raftstore.ReadInfo{}, ctx.Err()
		}
	}
	info := raftstore.ReadInfo{Consistency: "strong", ServedBy: testPod, AppliedIndex: n.f.AppliedIndex()}
	switch mode {
	case raftstore.ReadStale:
		info.Consistency, info.StalePossible = "stale", true
	case raftstore.ReadLinearizable:
		info.Consistency = "linearizable"
	}
	n.setReading(true)
	defer n.setReading(false)
	return info, fn(ctx, n.f)
}

func (n *fakeNode) setReading(v bool) {
	n.mu.Lock()
	n.reading = v
	n.mu.Unlock()
}

func (n *fakeNode) WaitApplyAfter(ctx context.Context, index uint64) {
	n.mu.Lock()
	n.waits = append(n.waits, index)
	n.waitedInRead = n.waitedInRead || n.reading
	onWait, done := n.onWait, n.waitDone
	n.mu.Unlock()
	if done != nil {
		defer close(done)
	}
	if onWait != nil {
		onWait()
		return
	}
	<-ctx.Done()
}

func (n *fakeNode) Status() raftstore.Status {
	n.mu.Lock()
	n.statuses++
	n.mu.Unlock()
	return n.status
}

func (n *fakeNode) Term() uint64 { return n.term }

func (n *fakeNode) Bootstrap(ctx context.Context, servers []raft.Server) error {
	n.booted = servers
	return n.bootErr
}

func (n *fakeNode) Snapshot(ctx context.Context) (raftstore.SnapshotInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snaps++
	return n.snapInfo, n.snapErr
}

func (n *fakeNode) StepDown(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stepDowns++
	return n.stepDownErr
}

func (n *fakeNode) appliedCmds() []any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]any(nil), n.applied...)
}

func newTestServer(t *testing.T, n node) *server {
	t.Helper()
	return &server{
		node:          n,
		ring:          loadRing(t),
		shard:         testShard,
		podName:       testPod,
		serverID:      testServerID,
		timeout:       2 * time.Second,
		leaderHTTP:    func(a raft.ServerAddress) string { return "http-of-" + string(a) },
		operatorToken: testToken,
	}
}

type call struct {
	method, path string
	header       map[string]string
	body         any // marshalled unless it is a string
}

func do(t *testing.T, h http.Handler, c call) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	switch b := c.body.(type) {
	case nil:
	case string:
		body = bytes.NewBufferString(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(raw)
	}
	req := httptest.NewRequestWithContext(context.Background(), c.method, c.path, body)
	for k, v := range c.header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeJSON[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T from %d %q: %v", v, rec.Code, rec.Body.String(), err)
	}
	return v
}

func expectError(t *testing.T, rec *httptest.ResponseRecorder, status int, code internalapi.ErrorCode) internalapi.Error {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d (%s), want %d %s", rec.Code, rec.Body.String(), status, code)
	}
	e := decodeJSON[internalapi.Error](t, rec)
	if e.Code != code {
		t.Fatalf("code = %s (%s), want %s", e.Code, e.Message, code)
	}
	return e
}

func meta(op string) store.Meta { return store.Meta{OperationID: op, At: t0} }
