package storagenode

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	gometrics "github.com/hashicorp/go-metrics"
	dto "github.com/prometheus/client_model/go"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// The metrics contract is docs/operations/storage-metrics.md (#113). These
// tests pin its names, types, labels and the outbox semantics: an exact
// count, and a failed observation that is absent rather than zero.

// metricsNow is the scrape clock: 90 s after t0, the seeded rows' proposer time.
var metricsNow = time.UnixMilli(t0 + 90_000)

func (n *fakeNode) StorageInfo() (raftstore.StorageInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.storage, n.storageErr
}

func newMetricsServer(t *testing.T, n node) *server {
	t.Helper()
	s := newTestServer(t, n)
	s.metrics = newNodeMetrics(testShard, testPod, func() time.Time { return metricsNow })
	s.metrics.attach(n)
	return s
}

// family gathers the registry and returns the named family, or nil.
func family(t *testing.T, s *server, name string) *dto.MetricFamily {
	t.Helper()
	fams, err := s.metrics.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// sample returns the metric in family name whose labels include want.
func sample(t *testing.T, s *server, name string, want map[string]string) *dto.Metric {
	t.Helper()
	f := family(t, s, name)
	if f == nil {
		return nil
	}
	for _, m := range f.GetMetric() {
		got := map[string]string{}
		for _, l := range m.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
			}
		}
		if match {
			return m
		}
	}
	return nil
}

func gauge(t *testing.T, s *server, name string) float64 {
	t.Helper()
	m := sample(t, s, name, map[string]string{"shard": "1", "node": testPod})
	if m == nil || m.GetGauge() == nil {
		t.Fatalf("%s: no gauge with shard=1 node=%s", name, testPod)
	}
	return m.GetGauge().GetValue()
}

func seedOutbox(t *testing.T, n *fakeNode, ids ...string) {
	t.Helper()
	rg := loadRing(t)
	for _, id := range ids {
		key, rid := keyOn(t, rg, testShard, store.ResourceKeyPrefix, id)
		ev := store.OutboxEvent{EventID: "ev-" + id, EventType: "resource.registered", SchemaVersion: 1, Exchange: "relief.events",
			RoutingKey: "resource.registered.water", Payload: []byte(`{"id":"x"}`)}
		n.seed(t, store.CreateEntity{Meta: meta("op-" + id), Key: key, Outbox: []store.OutboxEvent{ev},
			Resource: &store.Resource{ID: rid, Type: store.TypeWater, Name: "T", Geohash: "ttnfv8k", AvailableFrom: t0,
				AvailableUntil: t0 + 86_400_000, Status: store.ResourceAvailable, Reusable: true}})
	}
}

func TestMetricsEndpoint(t *testing.T) {
	n := newFakeNode(t)
	seedOutbox(t, n, "a", "b", "c")
	n.seed(t, store.MarkOutboxPublished{Meta: meta("op-mark"), Shard: testShard, EventIDs: []string{"ev-a"}})
	n.status = raftstore.Status{State: "Leader", Term: 3, CommitIndex: 10, LastLogIndex: 11, RaftAppliedIndex: 10, AppliedIndex: 9, LeaderCommitIndex: 12}
	n.storage = raftstore.StorageInfo{LogBytes: 4096, SnapshotBytes: 2048, LastSnapshotIndex: 5}
	s := newMetricsServer(t, n)
	h := s.metricsRoutes()

	rec := do(t, h, call{method: http.MethodGet, path: "/metrics"})
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("GET /metrics: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	for _, want := range []string{
		`storage_raft_leader{node="storage-1-1",shard="1"} 1`,
		`relief_outbox_pending{node="storage-1-1",shard="1"} 2`,
		"# TYPE storage_command_latency_seconds histogram",
		"# TYPE storage_raft_commit_seconds histogram",
		"process_resident_memory_bytes",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	for _, c := range []struct {
		name string
		want float64
	}{
		{"storage_raft_leader", 1},
		{"storage_raft_term", 3},
		{"storage_raft_commit_index", 10},
		{"storage_raft_applied_index", 9},
		{"storage_raft_last_log_index", 11},
		{"storage_raft_last_snapshot_index", 5},
		{"storage_replication_lag_entries", 2}, // leader commit 12 - Raft applied 10 (not the FSM's 9)
		{"storage_raft_log_bytes", 4096},
		{"storage_snapshot_bytes", 2048},
		{"relief_outbox_pending", 2}, // exact: three written, one published
		{"relief_outbox_oldest_age_seconds", 90},
		{"relief_outbox_observation_success", 1},
		{"relief_outbox_observation_timestamp_seconds", float64(metricsNow.Unix())},
		{"relief_outbox_observed_applied_index", 4}, // the fake FSM's index after four seeds
	} {
		if got := gauge(t, s, c.name); got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}

	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if rec := do(t, h, call{method: m, path: "/metrics"}); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /metrics: %d, want 405", m, rec.Code)
		}
	}
	for _, p := range []string{"/mgmt/status", "/mgmt/bootstrap", "/healthz", "/"} {
		if rec := do(t, h, call{method: http.MethodGet, path: p}); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s on the metrics port: %d, want 404 (only /metrics is served)", p, rec.Code)
		}
	}
}

// An apply that commits after Node.Read took its applied index but before
// the stats query must not be exported under the older index (#132): one
// scrape's relief_outbox_observed_applied_index is the index COUNT/MIN saw.
func TestMetricsOutboxIndexPairedWithCount(t *testing.T) {
	n := newFakeNode(t)
	seedOutbox(t, n, "a", "b", "c")
	n.inRead = func() {
		n.inRead = nil
		seedOutbox(t, n, "d") // index 4 commits inside the read, after index 3 was taken
	}
	s := newMetricsServer(t, n)
	fams, err := s.metrics.reg.Gather() // one scrape: both values from one Collect
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			if m.GetGauge() != nil {
				got[f.GetName()] = m.GetGauge().GetValue()
			}
		}
	}
	if got["relief_outbox_pending"] != 4 || got["relief_outbox_observed_applied_index"] != 4 {
		t.Fatalf("pending %v at observed index %v; want 4 at 4 (count and index from one snapshot)",
			got["relief_outbox_pending"], got["relief_outbox_observed_applied_index"])
	}
}

func TestMetricsOutboxEmptyAndLag(t *testing.T) {
	n := newFakeNode(t)
	n.status = raftstore.Status{State: "Follower", CommitIndex: 7, RaftAppliedIndex: 9, AppliedIndex: 9, LeaderCommitIndex: 7}
	s := newMetricsServer(t, n)
	if got := gauge(t, s, "relief_outbox_pending"); got != 0 {
		t.Errorf("pending = %v, want 0", got)
	}
	if got := gauge(t, s, "relief_outbox_oldest_age_seconds"); got != 0 {
		t.Errorf("oldest age with nothing pending = %v, want 0", got)
	}
	if got := gauge(t, s, "storage_raft_leader"); got != 0 {
		t.Errorf("leader = %v on a follower", got)
	}
	if got := gauge(t, s, "storage_replication_lag_entries"); got != 0 {
		t.Errorf("lag = %v with applied ahead of the learned commit, want 0 (never negative)", got)
	}
}

// The lag gauge is in Raft log entries: barrier-only progress (Raft applied
// at the leader's commit, the FSM still at the last command) is no lag; a
// replica whose Raft log stalled is behind by every entry it lacks.
func TestMetricsLagIsRaftLevel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status raftstore.Status
		want   float64
	}{
		// 20 barriers after the last command: the FSM stays at 100.
		{"barrier-only progress", raftstore.Status{State: "Follower", CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100, LeaderCommitIndex: 120}, 0},
		{"barrier-only progress on the leader", raftstore.Status{State: "Leader", CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100, LeaderCommitIndex: 120}, 0},
		{"stalled follower", raftstore.Status{State: "Follower", CommitIndex: 80, LastLogIndex: 80, RaftAppliedIndex: 80, AppliedIndex: 80, LeaderCommitIndex: 120}, 40},
		{"stalled behind barriers too", raftstore.Status{State: "Follower", CommitIndex: 110, LastLogIndex: 110, RaftAppliedIndex: 110, AppliedIndex: 100, LeaderCommitIndex: 120}, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			n.status = tc.status
			s := newMetricsServer(t, n)
			if got := gauge(t, s, "storage_replication_lag_entries"); got != tc.want {
				t.Errorf("lag = %v, want %v (status %+v)", got, tc.want, tc.status)
			}
		})
	}
}

func TestMetricsOutboxFailureIsAbsent(t *testing.T) {
	n := newFakeNode(t)
	seedOutbox(t, n, "a")
	n.readErr = errors.New("fsm closed")
	n.storageErr = errors.New("stat failed")
	s := newMetricsServer(t, n)
	for _, name := range []string{"relief_outbox_pending", "relief_outbox_oldest_age_seconds", "relief_outbox_observed_applied_index",
		"storage_raft_log_bytes", "storage_snapshot_bytes", "storage_raft_last_snapshot_index"} {
		if family(t, s, name) != nil {
			t.Errorf("%s is exported after a failed observation; it must be absent, not a fresh value", name)
		}
	}
	if got := gauge(t, s, "relief_outbox_observation_success"); got != 0 {
		t.Errorf("observation_success = %v after a failure, want 0", got)
	}
	if got := gauge(t, s, "relief_outbox_observation_timestamp_seconds"); got != float64(metricsNow.Unix()) {
		t.Errorf("observation timestamp = %v", got)
	}
	n.readErr = nil
	if got := gauge(t, s, "relief_outbox_pending"); got != 1 {
		t.Errorf("pending = %v once the read works again, want 1", got)
	}
}

func TestMetricsCommandAndReadLatency(t *testing.T) {
	rg := loadRing(t)
	key, _ := keyOn(t, rg, testShard, "", "kv:m")
	n := newFakeNode(t)
	n.status.CaughtUp = true // a stale read needs it
	s := newMetricsServer(t, n)
	h := s.dataRoutes()

	put := call{method: http.MethodPut, path: "/internal/data/" + key, header: map[string]string{"If-None-Match": "*"}, body: map[string]any{
		"value": "v", "ringVersion": rg.Version(), "operationId": "op-put", "at": t0}}
	if rec := do(t, h, put); rec.Code >= 300 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body)
	}
	n.applyErr = raftstore.ErrNotLeader
	put.body.(map[string]any)["operationId"] = "op-put-2"
	do(t, h, put)
	do(t, h, call{method: http.MethodGet, path: "/internal/data/" + key + "?consistency=stale"})
	do(t, h, call{method: http.MethodGet, path: "/internal/data/" + key + "?consistency=linearizable"})

	count := func(name string, labels map[string]string) uint64 {
		m := sample(t, s, name, labels)
		if m == nil || m.GetHistogram() == nil {
			t.Fatalf("%s %v: no histogram", name, labels)
		}
		return m.GetHistogram().GetSampleCount()
	}
	if got := count("storage_command_latency_seconds", map[string]string{"command": "CompareAndSet", "outcome": "ok"}) +
		count("storage_command_latency_seconds", map[string]string{"command": "CreateEntity", "outcome": "ok"}); got != 1 {
		t.Errorf("ok mutations observed: %d, want 1", got)
	}
	if got := count("storage_command_latency_seconds", map[string]string{"command": "CompareAndSet", "outcome": "error"}) +
		count("storage_command_latency_seconds", map[string]string{"command": "CreateEntity", "outcome": "error"}); got != 1 {
		t.Errorf("failed mutations observed: %d, want 1", got)
	}
	for _, c := range []string{"stale", "linearizable"} {
		if got := count("storage_read_latency_seconds", map[string]string{"consistency": c}); got != 1 {
			t.Errorf("%s reads observed: %d, want 1", c, got)
		}
	}
}

// Every command class and read consistency is present (zero) from the first
// scrape, and no label carries a value outside the contract's fixed sets.
func TestMetricsLabelsBounded(t *testing.T) {
	s := newMetricsServer(t, newFakeNode(t))
	allowed := map[string][]string{
		"shard":       {"1"},
		"node":        {testPod},
		"command":     {"CreateEntity", "CompareAndSet", "TransitionRequest", "RetryWorkflow", "AgeRequest", "TransitionResource", "TryClaim", "CloseAttempt", "MarkOutboxPublished"},
		"outcome":     {"ok", "error"},
		"consistency": {"strong", "linearizable", "stale"},
		"to":          {"leader", "candidate", "follower"},
	}
	fams, err := s.metrics.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if !strings.HasPrefix(f.GetName(), "storage_") && !strings.HasPrefix(f.GetName(), "relief_") {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				vals, ok := allowed[l.GetName()]
				if !ok || !slices.Contains(vals, l.GetValue()) {
					t.Errorf("%s: label %s=%q is not in the contract", f.GetName(), l.GetName(), l.GetValue())
				}
			}
		}
	}
	if f := family(t, s, "storage_command_latency_seconds"); f == nil || len(f.GetMetric()) != 18 {
		t.Errorf("storage_command_latency_seconds: want all 9 commands x 2 outcomes pre-declared, got %v", f)
	}
	if f := family(t, s, "storage_read_latency_seconds"); f == nil || len(f.GetMetric()) != 3 {
		t.Errorf("storage_read_latency_seconds: want 3 consistencies pre-declared")
	}
}

// The raft sink maps hashicorp/raft's go-metrics keys (milliseconds) to the
// contract's histograms (seconds) and counters, and drops everything else,
// including the legacy per-peer keys.
func TestMetricsRaftSink(t *testing.T) {
	s := newMetricsServer(t, newFakeNode(t))
	sink := s.metrics.raftSink()
	var _ gometrics.MetricSink = sink
	sink.AddSample([]string{"raft", "commitTime"}, 2.5)
	sink.AddSample([]string{"raft", "commitTime"}, 40)
	sink.AddSample([]string{"raft", "snapshot", "create"}, 1500)
	sink.AddSampleWithLabels([]string{"raft", "replication", "installSnapshot"}, 800, []gometrics.Label{{Name: "peer_id", Value: "storage-1-2"}})
	sink.AddSample([]string{"raft", "replication", "installSnapshot", "storage-1-2"}, 800) // legacy key: dropped
	sink.IncrCounter([]string{"raft", "state", "leader"}, 1)
	sink.IncrCounter([]string{"raft", "state", "candidate"}, 1)
	sink.IncrCounter([]string{"raft", "state", "candidate"}, 1)
	sink.IncrCounter([]string{"raft", "transition", "heartbeat_timeout"}, 1)
	sink.AddSample([]string{"raft", "rpc", "appendEntries"}, 1) // not in the contract: dropped
	sink.SetGauge([]string{"raft", "peers"}, 3)

	hist := func(name string) *dto.Histogram {
		m := sample(t, s, name, map[string]string{"shard": "1", "node": testPod})
		if m == nil || m.GetHistogram() == nil {
			t.Fatalf("%s: no histogram", name)
		}
		return m.GetHistogram()
	}
	if h := hist("storage_raft_commit_seconds"); h.GetSampleCount() != 2 || h.GetSampleSum() < 0.0424 || h.GetSampleSum() > 0.0426 {
		t.Errorf("commit: count %d sum %v, want 2 and 0.0425 s", h.GetSampleCount(), h.GetSampleSum())
	}
	if h := hist("storage_raft_snapshot_create_seconds"); h.GetSampleCount() != 1 {
		t.Errorf("snapshot create count %d", h.GetSampleCount())
	}
	if h := hist("storage_raft_install_snapshot_seconds"); h.GetSampleCount() != 1 {
		t.Errorf("install snapshot count %d, want 1 (the legacy per-peer key is dropped)", h.GetSampleCount())
	}
	counter := func(name string, labels map[string]string) float64 {
		m := sample(t, s, name, labels)
		if m == nil || m.GetCounter() == nil {
			t.Fatalf("%s %v: no counter", name, labels)
		}
		return m.GetCounter().GetValue()
	}
	if got := counter("storage_raft_transitions_total", map[string]string{"to": "candidate"}); got != 2 {
		t.Errorf("transitions to candidate = %v", got)
	}
	if got := counter("storage_raft_transitions_total", map[string]string{"to": "leader"}); got != 1 {
		t.Errorf("transitions to leader = %v", got)
	}
	if got := counter("storage_raft_heartbeat_timeouts_total", nil); got != 1 {
		t.Errorf("heartbeat timeouts = %v", got)
	}
	fams, _ := s.metrics.reg.Gather()
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), "raft_") || strings.Contains(f.GetName(), "appendEntries") || strings.Contains(f.GetName(), "peers") {
			t.Errorf("unmapped raft key exported as %s", f.GetName())
		}
	}
}

// A scrape is an observation: it never proposes a command.
func TestMetricsScrapeProposesNothing(t *testing.T) {
	n := newFakeNode(t)
	s := newMetricsServer(t, n)
	for range 3 {
		do(t, s.metricsRoutes(), call{method: http.MethodGet, path: "/metrics"})
	}
	if cmds := n.appliedCmds(); len(cmds) != 0 {
		t.Fatalf("a scrape proposed %d commands", len(cmds))
	}
	n.mu.Lock()
	modes := slices.Clone(n.modes)
	n.mu.Unlock()
	for _, m := range modes {
		if m != raftstore.ReadStale {
			t.Fatalf("a scrape read with mode %v; only a local stale read is allowed", m)
		}
	}
}
