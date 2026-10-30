package coordclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store/storetest"
)

const (
	clusterWait   = 15 * time.Second
	operatorToken = "op-secret"
)

var fastProfile = raftstore.Profile{
	Name: "test", SnapshotThreshold: 1 << 30, TrailingLogs: 64, SnapshotInterval: time.Hour,
	HeartbeatTimeout: 200 * time.Millisecond, ElectionTimeout: 200 * time.Millisecond, LeaderLeaseTimeout: 100 * time.Millisecond,
}

// hit is one request a storage replica served.
type hit struct {
	pod, method, path string
	status            int
	body              []byte
}

// replica is one in-process storage node with its heartbeat loop.
type replica struct {
	pod        string
	shard      int
	node       *raftstore.Node
	data, mgmt *httptest.Server
	stopBeats  context.CancelFunc
	beatsDone  chan error
}

func (r *replica) dataHost() string { return strings.TrimPrefix(r.data.URL, "http://") }

// cluster is 3 shards × 3 replicas behind one coordinator.
type cluster struct {
	t      *testing.T
	rg     *ring.Ring
	shards [][]*replica
	coord  *httptest.Server
	client *coordclient.Client

	mu   sync.Mutex
	hits []hit
}

// newCluster starts every replica, bootstraps each shard through its
// management endpoint (the coordinator never bootstraps), starts the
// coordinator and the heartbeat loops, and waits until every shard has a
// leader the coordinator knows.
func newCluster(t *testing.T, mod func(*coordinator.Config)) *cluster {
	t.Helper()
	rg, err := ring.Load("../../contracts/ring.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, rg: rg}

	cfg := coordinator.Config{Ring: rg, OperatorToken: operatorToken}
	if mod != nil {
		mod(&cfg)
	}
	co, err := coordinator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.coord = httptest.NewServer(co.Handler())
	t.Cleanup(c.coord.Close)

	var hostsMu sync.Mutex
	hosts := map[raft.ServerAddress]string{}
	for s := range rg.Shards() {
		var row []*replica
		for i := range 3 {
			pod := fmt.Sprintf("storage-%d-%d", s, i)
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), clusterWait)
			n, err := raftstore.Open(ctx, raftstore.Config{
				PodName: pod, GroupID: fmt.Sprintf("shard-%d", s), Shard: s,
				DataDir: filepath.Join(root, "pvc"), FSMDir: filepath.Join(root, "emptydir"),
				BindAddr: "127.0.0.1:0", AdvertiseAddr: "localhost:0", Profile: fastProfile,
				Logger: hclog.New(&hclog.LoggerOptions{Name: pod, Level: hclog.Error}),
			})
			cancel()
			if err != nil {
				t.Fatalf("open %s: %v", pod, err)
			}
			srv := storagenode.New(n, storagenode.Config{Ring: rg, Shard: s, PodName: pod, Timeout: 3 * time.Second, OperatorToken: operatorToken,
				LeaderHTTP: func(a raft.ServerAddress) string { hostsMu.Lock(); defer hostsMu.Unlock(); return hosts[a] }})
			r := &replica{pod: pod, shard: s, node: n, data: httptest.NewServer(c.record(pod, srv.DataRoutes())), mgmt: httptest.NewServer(srv.MgmtRoutes())}
			hostsMu.Lock()
			hosts[n.Addr()] = r.dataHost()
			hostsMu.Unlock()
			bctx, bcancel := context.WithCancel(context.Background())
			r.stopBeats, r.beatsDone = bcancel, make(chan error, 1)
			go func() {
				r.beatsDone <- srv.RunHeartbeats(bctx, storagenode.HeartbeatConfig{CoordinatorURL: c.coord.URL, Address: r.dataHost(), Interval: 100 * time.Millisecond})
			}()
			row = append(row, r)
		}
		c.shards = append(c.shards, row)
	}
	t.Cleanup(func() {
		for _, row := range c.shards {
			for _, r := range row {
				c.stop(r)
			}
		}
	})

	for s, row := range c.shards {
		var servers []map[string]string
		for _, r := range row {
			servers = append(servers, map[string]string{"id": string(r.node.Identity().ServerID()), "address": string(r.node.Addr())})
		}
		rec := c.http(http.MethodPost, row[0].mgmt.URL+"/mgmt/bootstrap", map[string]string{"Authorization": "Bearer " + operatorToken}, map[string]any{"servers": servers})
		if rec.Code != http.StatusOK {
			t.Fatalf("bootstrap shard %d: %d %s", s, rec.Code, rec.Body.String())
		}
	}
	for s := range c.shards {
		c.waitCoordinatorLeader(s, "")
	}
	c.waitAllAdmitted()
	ctx, cancel := context.WithTimeout(context.Background(), clusterWait)
	defer cancel()
	if c.client, err = coordclient.New(ctx, c.coord.URL); err != nil {
		t.Fatalf("coordclient: %v", err)
	}
	return c
}

// record wraps a replica's data routes to log every request it serves.
func (c *cluster) record(pod string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(sw, r)
		c.mu.Lock()
		c.hits = append(c.hits, hit{pod: pod, method: r.Method, path: r.URL.Path, status: sw.status, body: body})
		c.mu.Unlock()
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

// hitsFor returns the requests whose body carries operationID.
func (c *cluster) hitsFor(operationID string) []hit {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []hit
	for _, h := range c.hits {
		if bytes.Contains(h.body, []byte(`"operationId":"`+operationID+`"`)) {
			out = append(out, h)
		}
	}
	return out
}

func (c *cluster) stop(r *replica) {
	if r.node == nil {
		return
	}
	r.stopBeats()
	if err := <-r.beatsDone; err != nil {
		c.t.Errorf("%s heartbeats: %v", r.pod, err)
	}
	r.data.Close()
	r.mgmt.Close()
	if err := r.node.Close(); err != nil {
		c.t.Errorf("close %s: %v", r.pod, err)
	}
	r.node = nil
}

// raftLeader waits until one live replica of shard is the Raft leader.
func (c *cluster) raftLeader(shard int) *replica {
	c.t.Helper()
	for deadline := time.Now().Add(clusterWait); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		for _, r := range c.shards[shard] {
			if r.node != nil && r.node.Status().State == raft.Leader.String() {
				return r
			}
		}
	}
	c.t.Fatalf("shard %d: no Raft leader", shard)
	return nil
}

// waitCoordinatorLeader waits until /admin/locate for a key of shard names a
// leader other than notPod.
func (c *cluster) waitCoordinatorLeader(shard int, notPod string) string {
	c.t.Helper()
	key := c.keyOn(shard, "kv:locate")
	for deadline := time.Now().Add(clusterWait); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		rec := c.http(http.MethodGet, c.coord.URL+"/admin/locate/"+key, map[string]string{"Authorization": "Bearer " + operatorToken}, nil)
		if rec.Code == http.StatusOK {
			var p internalapi.Placement
			if json.Unmarshal(rec.Body.Bytes(), &p) == nil && p.LeaderId != nil && !strings.HasPrefix(*p.LeaderId, notPod+"/") {
				return *p.LeaderId
			}
		}
	}
	c.t.Fatalf("coordinator never learned a leader for shard %d", shard)
	return ""
}

// waitAllAdmitted waits until the coordinator admits every replica (M7):
// admission follows the committed configuration, which a newly elected
// leader reports about one heartbeat after it is first named leader.
func (c *cluster) waitAllAdmitted() {
	c.t.Helper()
	want := 0
	for _, row := range c.shards {
		want += len(row)
	}
	n := 0
	for deadline := time.Now().Add(clusterWait); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		rec := c.http(http.MethodGet, c.coord.URL+"/admin/nodes", map[string]string{"Authorization": "Bearer " + operatorToken}, nil)
		var nodes internalapi.AdminNodes
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &nodes) != nil {
			continue
		}
		n = 0
		for _, node := range nodes.Nodes {
			if node.Admitted {
				n++
			}
		}
		if n == want {
			return
		}
	}
	c.t.Fatalf("the coordinator admitted %d of %d replicas", n, want)
}

func (c *cluster) keyOn(shard int, base string) string {
	c.t.Helper()
	for n := range 10_000 {
		key := fmt.Sprintf("%s-%d", base, n)
		if owner, _ := c.rg.Owner(key); owner == shard {
			return key
		}
	}
	c.t.Fatalf("no key on shard %d", shard)
	return ""
}

func (c *cluster) http(method, url string, header map[string]string, body any) *httptest.ResponseRecorder {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterWait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		c.t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	rec := httptest.NewRecorder()
	rec.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(rec.Body, resp.Body)
	return rec
}

func clusterCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), clusterWait)
	t.Cleanup(cancel)
	return ctx
}

func m(op string) store.Meta { return store.Meta{OperationID: op, At: storetest.T0} }

// Verify (S1.4): a write is routed by the ring. Each shard's write reaches
// only that shard's replicas and is read back through the coordinator.
func TestClusterWriteRoutedByRing(t *testing.T) {
	c := newCluster(t, nil)
	ctx := clusterCtx(t)
	for s := range 3 {
		key := c.keyOn(s, "kv:routed")
		op := fmt.Sprintf("op-routed-%d", s)
		res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m(op), Key: key, Value: json.RawMessage(fmt.Sprint(s))})
		if err != nil || res.Outcome != store.OutcomeOK || res.Shard != s {
			t.Fatalf("create %s: %+v, %v; want OK on shard %d", key, res, err, s)
		}
		for _, h := range c.hitsFor(op) {
			if !strings.HasPrefix(h.pod, fmt.Sprintf("storage-%d-", s)) {
				t.Fatalf("write for shard %d reached %s", s, h.pod)
			}
		}
		rec, err := c.client.ReadRecord(ctx, key)
		if err != nil || rec.Version != 1 || string(rec.Value) != fmt.Sprint(s) {
			t.Fatalf("read %s: %+v, %v", key, rec, err)
		}
	}
}

// Verify (S1.4): with heartbeats paused the coordinator keeps a stale
// leader hint; after the leader steps down, a claim is sent to the old
// leader, gets NOT_LEADER, and is retried at the new leader with the same
// operationId and the same bytes. It commits once.
func TestClusterStaleLeaderHintRetriesSameOperation(t *testing.T) {
	// Paused heartbeats must not turn the stale leader DOWN during the test.
	c := newCluster(t, func(cfg *coordinator.Config) { cfg.SuspectAfter, cfg.DownAfter = time.Minute, 2*time.Minute })
	ctx := clusterCtx(t)
	const shard = 1
	rkey := store.ResourceKey(strings.TrimPrefix(c.keyOn(shard, "res:stale"), "res:"))
	id := strings.TrimPrefix(rkey, "res:")
	r := &store.Resource{ID: id, Type: store.TypeWater, Name: "tanker", Geohash: "ttnfv", AvailableFrom: storetest.T0 - 1, AvailableUntil: storetest.T0 + 3_600_000, Status: store.ResourceAvailable, Reusable: true}
	if res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m("op-create-stale"), Key: rkey, Resource: r}); err != nil || res.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v, %v", res, err)
	}

	old := c.raftLeader(shard)
	for _, row := range c.shards {
		for _, rep := range row {
			rep.stopBeats()
			if err := <-rep.beatsDone; err != nil {
				t.Fatalf("%s heartbeats: %v", rep.pod, err)
			}
			rep.beatsDone <- nil // for cleanup
		}
	}
	if err := old.node.StepDown(ctx); err != nil {
		t.Fatalf("step down %s: %v", old.pod, err)
	}
	cur := c.raftLeader(shard)
	if cur == old {
		t.Fatalf("%s still leads", old.pod)
	}

	claim := store.TryClaim{Meta: m("op-claim-stale"), ResourceKey: rkey, RequestID: "q-1",
		Attempt: store.Attempt{AttemptID: "a-1", ResourceID: id, Fingerprint: "fp"}, Type: store.TypeWater}
	res, err := c.client.TryClaim(ctx, claim)
	if err != nil || res.Outcome != store.ClaimSuccess || res.Replayed {
		t.Fatalf("claim through a stale hint: %+v, %v; want a first-time SUCCESS", res, err)
	}

	hits := c.hitsFor("op-claim-stale")
	if len(hits) < 2 || hits[0].pod != old.pod || hits[0].status != http.StatusServiceUnavailable {
		t.Fatalf("attempts = %+v; want the stale leader %s first, answering 503 NOT_LEADER", summarize(hits), old.pod)
	}
	last := hits[len(hits)-1]
	if last.pod != cur.pod || last.status != http.StatusOK {
		t.Fatalf("last attempt = %s %d, want the new leader %s with 200", last.pod, last.status, cur.pod)
	}
	for _, h := range hits {
		if !bytes.Equal(h.body, hits[0].body) {
			t.Fatalf("attempt at %s sent %q, the first sent %q", h.pod, h.body, hits[0].body)
		}
	}
	if got, err := c.client.Operation(ctx, rkey, "op-claim-stale"); err != nil || got.Outcome != store.ClaimSuccess || got.AppliedIndex != res.AppliedIndex {
		t.Fatalf("operation = %+v, %v; want the one committed SUCCESS", got, err)
	}
}

func summarize(hits []hit) []string {
	var out []string
	for _, h := range hits {
		out = append(out, fmt.Sprintf("%s:%d", h.pod, h.status))
	}
	return out
}

// Verify (S1.4): with shard 2 fully down, a scan is partial with
// failedShards [2] and still returns shards 0 and 1; shards 0 and 1 keep
// working; the coordinator marks shard 2's nodes DOWN, and then a mutation
// to shard 2 is 503 (never proposed, so not an unknown outcome).
func TestClusterShardDown(t *testing.T) {
	c := newCluster(t, func(cfg *coordinator.Config) {
		cfg.SuspectAfter, cfg.DownAfter = 300*time.Millisecond, 600*time.Millisecond
		cfg.Budget = 2 * time.Second
	})
	ctx := clusterCtx(t)
	var want []string
	for s := range 3 {
		key := store.ResourceKey(strings.TrimPrefix(c.keyOn(s, "res:down"), "res:"))
		id := strings.TrimPrefix(key, "res:")
		r := &store.Resource{ID: id, Type: store.TypeWater, Name: id, Geohash: "ttdown", AvailableFrom: 0, AvailableUntil: 1, Status: store.ResourceAvailable}
		if res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m("op-" + id), Key: key, Resource: r}); err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("create %s: %+v, %v", key, res, err)
		}
		if s != 2 {
			want = append(want, key)
		}
	}
	for _, r := range c.shards[2] {
		c.stop(r)
	}

	q := store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{"ttdown"}, Limit: 100}
	got, err := c.client.Scan(ctx, q)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var keys []string
	for _, r := range got.Records {
		keys = append(keys, r.Key)
	}
	slices.Sort(keys)
	slices.Sort(want)
	if !got.Partial || !slices.Equal(got.FailedShards, []int{2}) || !slices.Equal(keys, want) || len(got.Positions) != 2 {
		t.Fatalf("scan with shard 2 down = partial=%v failed=%v keys=%v positions=%v; want partial [2] %v",
			got.Partial, got.FailedShards, keys, got.Positions, want)
	}

	for s := range 2 {
		key := c.keyOn(s, "kv:alive")
		if res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m(fmt.Sprintf("op-alive-%d", s)), Key: key, Value: json.RawMessage(`1`)}); err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("shard %d write while shard 2 is down: %+v, %v", s, res, err)
		}
		if _, err := c.client.ReadRecord(ctx, key); err != nil {
			t.Fatalf("shard %d read while shard 2 is down: %v", s, err)
		}
	}

	for deadline := time.Now().Add(clusterWait); ; time.Sleep(50 * time.Millisecond) {
		rec := c.http(http.MethodGet, c.coord.URL+"/admin/nodes", map[string]string{"Authorization": "Bearer " + operatorToken}, nil)
		var nodes internalapi.AdminNodes
		_ = json.Unmarshal(rec.Body.Bytes(), &nodes)
		states := map[string]internalapi.NodeState{}
		for _, n := range nodes.Nodes {
			states[n.NodeId] = n.State
		}
		if states["storage-2-0"] == internalapi.DOWN && states["storage-2-1"] == internalapi.DOWN && states["storage-2-2"] == internalapi.DOWN &&
			states["storage-0-0"] == internalapi.UP && states["storage-1-0"] == internalapi.UP {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node states = %v; want shard 2 DOWN, the rest UP", states)
		}
	}

	// The mutation is checked once shard 2 is DOWN, when it is refused
	// without dialling: before that its freed ports can be bound by another
	// process (issue #46), whose answer is unverified and so an unknown
	// outcome (TestClusterImpostorOnAFreedPort).
	down := c.keyOn(2, "kv:down")
	rec := c.http(http.MethodPut, c.coord.URL+"/internal/data/"+down, map[string]string{"If-None-Match": "*"},
		map[string]any{"operationId": "op-down-raw", "at": storetest.T0, "ringVersion": 1, "value": 1})
	var e internalapi.Error
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if rec.Code != http.StatusServiceUnavailable || e.Code != internalapi.NOLEADER || e.OutcomeUnknown || !e.Retryable {
		t.Fatalf("mutation to the down shard = %d %s, want 503 NO_LEADER (never proposed)", rec.Code, rec.Body.String())
	}
	_, err = c.client.CreateEntity(ctx, store.CreateEntity{Meta: m("op-down"), Key: down, Value: json.RawMessage(`1`)})
	var ce *coordclient.Error
	if !errors.As(err, &ce) || ce.Status != http.StatusServiceUnavailable || errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("client mutation to the down shard: err = %v, want a 503 that is not an unknown outcome", err)
	}

	// Once shard 2 is DOWN by heartbeat age, it fails at once: no 2 s wait.
	start := time.Now()
	got, err = c.client.Scan(ctx, q)
	if d := time.Since(start); err != nil || !got.Partial || !slices.Equal(got.FailedShards, []int{2}) || d > 500*time.Millisecond {
		t.Fatalf("scan with shard 2 DOWN = partial=%v failed=%v, %v after %s; want partial [2] within 500ms", got.Partial, got.FailedShards, err, d)
	}
}

// Verify (S1.4): coordclient passes the store contract memstore passes, on
// the real cluster.
func TestClusterStoreContract(t *testing.T) {
	c := newCluster(t, nil)
	storetest.Run(t, func(*testing.T) store.Store { return c.client })
}

// Issue #46: a stopped replica's freed port is bound by another process
// before the coordinator marks the replica DOWN. Whatever that process
// answers, the coordinator must not relay it as shard 2's: an answer as
// another shard was never proposed here (503 NO_LEADER); an answer as
// another shard-2 server, or with no identity, may have been (503
// OUTCOME_UNKNOWN). Never 200.
func TestClusterImpostorOnAFreedPort(t *testing.T) {
	// The stopped replicas stay UP for the whole test, so every call dials
	// the impostors rather than failing fast.
	c := newCluster(t, func(cfg *coordinator.Config) {
		cfg.SuspectAfter, cfg.DownAfter = time.Minute, 2*time.Minute
		cfg.Budget = time.Second
	})
	var claim atomic.Pointer[[2]string] // serverID, shard; nil: no headers
	var impostors []*http.Server
	for _, r := range c.shards[2] {
		addr := r.dataHost()
		c.stop(r)
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
		if err != nil {
			t.Fatalf("rebind %s: %v", addr, err)
		}
		srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if id := claim.Load(); id != nil {
				w.Header().Set(internalapi.HeaderServerID, id[0])
				w.Header().Set(internalapi.HeaderShard, id[1])
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(internalapi.MutationResponse{RingVersion: 1,
				Result: store.Result{Outcome: store.OutcomeOK, OperationID: "op", Key: "k", Version: 1, Shard: 2, AppliedIndex: 7}})
		})}
		go func() { _ = srv.Serve(ln) }()
		impostors = append(impostors, srv)
	}
	t.Cleanup(func() {
		for _, srv := range impostors {
			_ = srv.Close()
		}
	})

	down := c.keyOn(2, "kv:impostor")
	for i, tc := range []struct {
		name  string
		claim *[2]string
		code  internalapi.ErrorCode
	}{
		{"another shard", &[2]string{c.shards[0][0].pod + "/00000000-0000-4000-8000-000000000000", "0"}, internalapi.NOLEADER},
		{"same pod, another incarnation", &[2]string{"storage-2-0/00000000-0000-4000-8000-000000000020", "2"}, internalapi.OUTCOMEUNKNOWN},
		{"no identity", nil, internalapi.OUTCOMEUNKNOWN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim.Store(tc.claim)
			rec := c.http(http.MethodPut, c.coord.URL+"/internal/data/"+down, map[string]string{"If-None-Match": "*"},
				map[string]any{"operationId": fmt.Sprintf("op-impostor-%d", i), "at": storetest.T0, "ringVersion": 1, "value": 1})
			var e internalapi.Error
			_ = json.Unmarshal(rec.Body.Bytes(), &e)
			if rec.Code != http.StatusServiceUnavailable || e.Code != tc.code || e.OutcomeUnknown != (tc.code == internalapi.OUTCOMEUNKNOWN) {
				t.Fatalf("write to shard 2 answered by an impostor (%s) = %d %s, want 503 %s", tc.name, rec.Code, rec.Body.String(), tc.code)
			}
		})
	}
}
