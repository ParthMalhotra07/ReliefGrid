package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// The cluster's ring (contracts/ring.yaml), as the coordinator serves it.
var testRing = internalapi.Ring{RingVersion: 1, Hash: internalapi.Xxhash64, Shards: 3, VnodesPerShard: 128,
	Checksum: "sha256:670e1eca690b168c83fd910b2bdf1e2fbf81f4cd8de91994d8d5ac2989de6360"}

func TestProbeFlag(t *testing.T) {
	var p probeFlags
	if err := p.Set("1=http://storage-shard-1-0.storage-shard-1.reliefgrid.svc.cluster.local:8080"); err != nil {
		t.Fatal(err)
	}
	if len(p) != 1 || p[0].shard != 1 || p[0].name != "storage-shard-1-0" || p[0].base != "http://storage-shard-1-0.storage-shard-1.reliefgrid.svc.cluster.local:8080" {
		t.Fatalf("parsed %+v", p)
	}
	for _, bad := range []string{"http://x:8080", "a=http://x:8080", "-1=http://x:8080", "0=ftp://x", "0=http://x:8080/internal", "0=http://x:8080?a=1", "0="} {
		if err := p.Set(bad); err == nil {
			t.Errorf("--probe %q accepted", bad)
		}
	}
}

// owner is the ring the coordinator served, checked against its checksum.
func TestOwnerChecksTheRing(t *testing.T) {
	shardOf, err := owner(testRing)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		s, err := shardOf("kv-" + k)
		if err != nil || s < 0 || s > 2 {
			t.Fatalf("shard of kv-%s = %d, %v", k, s, err)
		}
		seen[s] = true
	}
	if len(seen) < 2 {
		t.Fatalf("10 keys on %d shard(s)", len(seen))
	}
	bad := testRing
	bad.Checksum = "sha256:" + strings.Repeat("0", 64)
	if _, err := owner(bad); err == nil {
		t.Fatal("a ring with the wrong checksum was accepted")
	}
}

// fakeNode serves GET /internal/data/{key} from s, as one storage node, and
// records the consistency each read asked for; refuse makes it answer
// 503 NOT_LEADER (an isolated leader's fence).
func fakeNode(t *testing.T, s store.Store, served string, refuse *atomic.Bool) (*httptest.Server, func() []string) {
	var mu sync.Mutex
	var modes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.URL.Path, "/internal/data/")
		if !ok || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		mu.Lock()
		modes = append(modes, r.URL.Query().Get("consistency"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if refuse != nil && refuse.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(internalapi.Error{Code: internalapi.NOTLEADER, Message: "not the leader", Retryable: true})
			return
		}
		rec, err := s.ReadRecord(r.Context(), key)
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(internalapi.Error{Code: internalapi.NOTFOUND, Message: "no such key"})
			return
		}
		_ = json.NewEncoder(w).Encode(internalapi.RecordResponse{Record: rec, RingVersion: 1,
			Read: internalapi.ReadInfo{Consistency: internalapi.ReadInfoConsistency(served), ServedBy: "storage-shard-0-0"}})
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), modes...) }
}

// directRead: a 200 is the record and the mode the node served; a missing
// key is store.ErrNotFound; a refusal is a failed read naming status and code.
func TestDirectRead(t *testing.T) {
	ctx := context.Background()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "op", At: invchecktest.T0}, Key: "kv-a", Value: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	var refuse atomic.Bool
	srv, modes := fakeNode(t, s, "linearizable", &refuse)
	read := directRead(srv.Client(), srv.URL, "linearizable")
	rec, served, err := read(ctx, "kv-a")
	if err != nil || rec.Version != 1 || served != "linearizable" {
		t.Fatalf("read = %+v, %q, %v", rec, served, err)
	}
	if _, _, err := read(ctx, "kv-missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing key: %v, want store.ErrNotFound", err)
	}
	refuse.Store(true)
	if _, _, err := read(ctx, "kv-a"); err == nil || !strings.Contains(err.Error(), "HTTP 503 NOT_LEADER") {
		t.Fatalf("refused read: %v, want an error naming HTTP 503 NOT_LEADER", err)
	}
	for _, m := range modes() {
		if m != "linearizable" {
			t.Fatalf("asked consistency=%q, want linearizable", m)
		}
	}
}

// load with a probe at a node that refuses every read during a fault, then
// verify --window: the probe's reads are failed (not stale), and the
// availability report counts them for the probe's shard, with none served
// while the fault fully held.
func TestProbeLoadThenVerifyAvailability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	shardOf, err := owner(testRing)
	if err != nil {
		t.Fatal(err)
	}
	var refuse atomic.Bool
	refuse.Store(true)
	srv, _ := fakeNode(t, s, "linearizable", &refuse)
	var targets probeFlags
	if err := targets.Set("0=" + srv.URL); err != nil {
		t.Fatal(err)
	}
	ps := probes(targets, shardOf, "linearizable", srv.Client())
	path := filepath.Join(t.TempDir(), "ledger.json")
	from := time.Now().UnixMilli()
	d := ledger.Driver{Store: s, Prefix: "pv", Workers: 3, Keys: 4, Ops: 30, Retries: 1, At: invchecktest.T0,
		ReadInterval: time.Millisecond, Probes: ps, ReadConsistency: "linearizable"}
	var out bytes.Buffer
	if code := load(ctx, d, path, &out); code != exitClean {
		t.Fatalf("load exit %d: %s", code, out.String())
	}
	to := time.Now().UnixMilli()
	out.Reset()
	w := window{from: from, to: to, set: true, graceMs: 0, shardOf: shardOf}
	if code := verify(ctx, s, path, true, w, &out); code != exitClean {
		t.Fatalf("verify exit %d: %s", code, out.String())
	}
	var js struct {
		Verdict struct {
			Reads ledger.ReadStats `json:"reads"`
		} `json:"verdict"`
		Availability *ledger.WindowStats `json:"availability"`
	}
	if err := json.Unmarshal(out.Bytes(), &js); err != nil || js.Availability == nil {
		t.Fatalf("verify --json = %s (%v), want availability", out.String(), err)
	}
	if js.Verdict.Reads.Failed == 0 || js.Verdict.Reads.Failed != js.Verdict.Reads.Total {
		t.Fatalf("reads %+v: every probe read was refused, so all should be failed", js.Verdict.Reads)
	}
	name := targets[0].name
	var s0 *ledger.ShardWindow
	for i := range js.Availability.Shards {
		if js.Availability.Shards[i].Shard == 0 {
			s0 = &js.Availability.Shards[i]
		}
	}
	if s0 == nil || s0.ProbeReadsDuring[name] == 0 || s0.ProbeServedInsideFault[name] != 0 || s0.AcksDuring == 0 {
		t.Fatalf("shard 0 availability %+v: want probe %s reads counted, none served, and acks during", s0, name)
	}
}

// invcheck ring prints the ring the coordinator serves.
func TestRingCommand(t *testing.T) {
	f := newFakeCoordinator(t)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"ring", "--coordinator", f.srv.URL, "--timeout", "5s"}, &stdout, &stderr); code != exitClean {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	var got internalapi.Ring
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil || got != testRing {
		t.Fatalf("ring = %s (%v), want %+v", stdout.String(), err, testRing)
	}
}
