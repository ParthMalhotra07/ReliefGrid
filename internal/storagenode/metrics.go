package storagenode

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gometrics "github.com/hashicorp/go-metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// The metrics contract is docs/operations/storage-metrics.md (#113): names,
// types, labels and units below are that contract. Every family carries
// shard and node; the other labels have fixed value sets. A scrape is an
// observation only: one local stale read and a stat of the PVC, never a
// proposal and never an input to an FSM decision.

var (
	// latencyBuckets (seconds) for commands, reads and Raft's own timings.
	latencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	// snapshotBuckets add room for a large image.
	snapshotBuckets = append(append([]float64(nil), latencyBuckets...), 30, 60)

	commandOps = []fsm.Op{fsm.OpCreateEntity, fsm.OpCompareAndSet, fsm.OpTransitionRequest, fsm.OpRetryWorkflow, fsm.OpAgeRequest,
		fsm.OpTransitionResource, fsm.OpTryClaim, fsm.OpCloseAttempt, fsm.OpMarkOutboxPublished}
	readConsistencies = map[raftstore.ReadMode]string{
		raftstore.ReadStrong: "strong", raftstore.ReadLinearizable: "linearizable", raftstore.ReadStale: "stale"}
	raftTransitions = []string{"leader", "candidate", "follower"}

	// raftHistograms maps hashicorp/raft's go-metrics keys (milliseconds)
	// to the contract's histograms (seconds). Any other key is dropped,
	// including the legacy keys that carry a peer ID in the name.
	raftHistograms = []struct {
		key, name, help string
		buckets         []float64
	}{
		{"raft.commitTime", "storage_raft_commit_seconds", "Leader only: log append to commit.", latencyBuckets},
		{"raft.fsm.apply", "storage_raft_fsm_apply_seconds", "FSM apply of one entry.", latencyBuckets},
		{"raft.snapshot.create", "storage_raft_snapshot_create_seconds", "Snapshot taken; _count is the number of snapshots.", snapshotBuckets},
		{"raft.snapshot.persist", "storage_raft_snapshot_persist_seconds", "Snapshot written to the PVC.", snapshotBuckets},
		{"raft.fsm.restore", "storage_raft_snapshot_restore_seconds", "Snapshot restored into the FSM (replay at start or an install).", snapshotBuckets},
		{"raft.replication.installSnapshot", "storage_raft_install_snapshot_seconds", "Leader only: snapshot sent to a lagging follower.", snapshotBuckets},
		{"raft.compactLogs", "storage_raft_compact_logs_seconds", "Log compaction after a snapshot.", latencyBuckets},
	}
)

// outboxStatsTimeout bounds the scrape's outbox read: past it the
// observation fails (absent gauges, success 0), never a stale or zero value.
const outboxStatsTimeout = time.Second

// storageInfoer is the PVC view *raftstore.Node has; test fakes may lack it.
type storageInfoer interface {
	StorageInfo() (raftstore.StorageInfo, error)
}

// nodeMetrics is one replica's registry.
type nodeMetrics struct {
	reg         *prometheus.Registry
	now         func() time.Time
	constLabels prometheus.Labels
	command     *prometheus.HistogramVec
	read        *prometheus.HistogramVec
	raftHist    map[string]prometheus.Histogram
	transitions *prometheus.CounterVec
	hbTimeouts  prometheus.Counter
	src         atomic.Pointer[node]
}

func newNodeMetrics(shard int, pod string, now func() time.Time) *nodeMetrics {
	m := &nodeMetrics{
		reg:         prometheus.NewRegistry(),
		now:         now,
		constLabels: prometheus.Labels{"shard": strconv.Itoa(shard), "node": pod},
		raftHist:    map[string]prometheus.Histogram{},
	}
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg := prometheus.WrapRegistererWith(m.constLabels, m.reg)

	m.command = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "storage_command_latency_seconds",
		Help:    "One mutation on the replica that received it: propose, majority commit and FSM apply. error: no result (not leader, timeout, unknown outcome).",
		Buckets: latencyBuckets}, []string{"command", "outcome"})
	m.read = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "storage_read_latency_seconds",
		Help: "One read served by this replica, including its fence.", Buckets: latencyBuckets}, []string{"consistency"})
	m.transitions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "storage_raft_transitions_total",
		Help: "Raft state changes of this replica; to=leader counts elections won."}, []string{"to"})
	m.hbTimeouts = prometheus.NewCounter(prometheus.CounterOpts{Name: "storage_raft_heartbeat_timeouts_total",
		Help: "Follower lost the leader's heartbeat."})
	reg.MustRegister(m.command, m.read, m.transitions, m.hbTimeouts, (*nodeCollector)(m))
	// Pre-declare every fixed label value, so each series exists (at zero)
	// from the first scrape and rate() has a starting point.
	for _, op := range commandOps {
		for _, outcome := range []string{"ok", "error"} {
			m.command.WithLabelValues(string(op), outcome)
		}
	}
	for _, c := range readConsistencies {
		m.read.WithLabelValues(c)
	}
	for _, to := range raftTransitions {
		m.transitions.WithLabelValues(to)
	}
	for _, h := range raftHistograms {
		hist := prometheus.NewHistogram(prometheus.HistogramOpts{Name: h.name, Help: h.help, Buckets: h.buckets})
		reg.MustRegister(hist)
		m.raftHist[h.key] = hist
	}
	return m
}

// attach points the scrape-time gauges at n.
func (m *nodeMetrics) attach(n node) { m.src.Store(&n) }

func (m *nodeMetrics) observeCommand(cmd any, err error, d time.Duration) {
	if m == nil {
		return
	}
	op, opErr := fsm.OpOf(cmd)
	if opErr != nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.command.WithLabelValues(string(op), outcome).Observe(d.Seconds())
}

func (m *nodeMetrics) observeRead(mode raftstore.ReadMode, d time.Duration) {
	if m == nil {
		return
	}
	if c, ok := readConsistencies[mode]; ok {
		m.read.WithLabelValues(c).Observe(d.Seconds())
	}
}

func (m *nodeMetrics) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	return mux
}

// nodeCollector produces the scrape-time gauges: Raft positions from
// Status, PVC sizes and the outbox backlog.
type nodeCollector nodeMetrics

var (
	descLeader         = newDesc("storage_raft_leader", "1 while this replica is leader.")
	descTerm           = newDesc("storage_raft_term", "Current term.")
	descCommit         = newDesc("storage_raft_commit_index", "Commit index as this replica knows it.")
	descApplied        = newDesc("storage_raft_applied_index", "FSM's completed index.")
	descLastLog        = newDesc("storage_raft_last_log_index", "Last index in the local log.")
	descLag            = newDesc("storage_replication_lag_entries", "leaderCommitIndex - appliedIndex on this replica (never negative).")
	descLastSnap       = newDesc("storage_raft_last_snapshot_index", "Raft index of the newest local snapshot; 0 if none.")
	descLogBytes       = newDesc("storage_raft_log_bytes", "Size of raft.db (log and stable store) on the PVC.")
	descSnapBytes      = newDesc("storage_snapshot_bytes", "Size of the snapshot directory on the PVC.")
	descOutboxPending  = newDesc("relief_outbox_pending", "Exact count of unpublished outbox rows in this replica's FSM; absent if the observation failed.")
	descOutboxAge      = newDesc("relief_outbox_oldest_age_seconds", "Scrape time minus the oldest unpublished row's proposer time; 0 when none pending; absent if the observation failed.")
	descOutboxOK       = newDesc("relief_outbox_observation_success", "1 if the outbox observation succeeded at this scrape.")
	descOutboxAt       = newDesc("relief_outbox_observation_timestamp_seconds", "When the outbox observation was taken (Unix seconds).")
	descOutboxObserved = newDesc("relief_outbox_observed_applied_index", "FSM applied index of the SQLite snapshot the outbox count and oldest age were read from.")
)

// newDesc declares a gauge with no variable labels: shard and node come
// from the wrapping registerer.
func newDesc(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descLeader, descTerm, descCommit, descApplied, descLastLog, descLag, descLastSnap,
		descLogBytes, descSnapBytes, descOutboxPending, descOutboxAge, descOutboxOK, descOutboxAt, descOutboxObserved} {
		ch <- d
	}
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	p := c.src.Load()
	if p == nil {
		return
	}
	n := *p
	g := func(d *prometheus.Desc, v float64) { ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v) }

	st := n.Status()
	leader := 0.0
	if st.State == "Leader" {
		leader = 1
	}
	g(descLeader, leader)
	g(descTerm, float64(st.Term))
	g(descCommit, float64(st.CommitIndex))
	g(descApplied, float64(st.AppliedIndex))
	g(descLastLog, float64(st.LastLogIndex))
	lag := 0.0
	if st.LeaderCommitIndex > st.AppliedIndex {
		lag = float64(st.LeaderCommitIndex - st.AppliedIndex)
	}
	g(descLag, lag)

	if si, ok := n.(storageInfoer); ok {
		if info, err := si.StorageInfo(); err == nil {
			g(descLastSnap, float64(info.LastSnapshotIndex))
			g(descLogBytes, float64(info.LogBytes))
			g(descSnapBytes, float64(info.SnapshotBytes))
		}
	}

	now := c.now()
	ctx, cancel := context.WithTimeout(context.Background(), outboxStatsTimeout)
	defer cancel()
	var stats fsm.OutboxStats
	// ReadInfo.AppliedIndex is taken before the query runs; the stats carry
	// the index of their own snapshot (#132).
	_, err := n.Read(ctx, raftstore.ReadStale, func(ctx context.Context, rd raftstore.Reader) (err error) {
		stats, err = rd.OutboxStats(ctx)
		return err
	})
	g(descOutboxAt, float64(now.Unix()))
	if err != nil {
		g(descOutboxOK, 0)
		return
	}
	g(descOutboxOK, 1)
	g(descOutboxPending, float64(stats.Pending))
	age := 0.0
	if stats.Pending > 0 {
		age = max(0, float64(now.UnixMilli()-stats.OldestCreatedAt)/1000)
	}
	g(descOutboxAge, age)
	g(descOutboxObserved, float64(stats.AppliedIndex))
}

// raftSink is a go-metrics sink that feeds hashicorp/raft's measurements
// into the contract's families. Only the keys in raftHistograms and the
// state-transition counters are kept.
type raftSink nodeMetrics

func (m *nodeMetrics) raftSink() *raftSink { return (*raftSink)(m) }

func (s *raftSink) AddSample(key []string, val float32) {
	if h, ok := s.raftHist[strings.Join(key, ".")]; ok {
		h.Observe(float64(val) / 1000) // go-metrics timings are milliseconds
	}
}

func (s *raftSink) AddSampleWithLabels(key []string, val float32, _ []gometrics.Label) {
	s.AddSample(key, val)
}

func (s *raftSink) IncrCounter(key []string, val float32) {
	switch k := strings.Join(key, "."); k {
	case "raft.state.leader", "raft.state.candidate", "raft.state.follower":
		s.transitions.WithLabelValues(strings.TrimPrefix(k, "raft.state.")).Add(float64(val))
	case "raft.transition.heartbeat_timeout":
		s.hbTimeouts.Add(float64(val))
	}
}

func (s *raftSink) IncrCounterWithLabels(key []string, val float32, _ []gometrics.Label) {
	s.IncrCounter(key, val)
}

func (s *raftSink) SetGauge([]string, float32)                              {}
func (s *raftSink) SetGaugeWithLabels([]string, float32, []gometrics.Label) {}
func (s *raftSink) EmitKey([]string, float32)                               {}

// Metrics is one storage node's metrics registry (docs/operations/storage-metrics.md).
type Metrics struct{ m *nodeMetrics }

// NewMetrics makes the registry for one replica.
func NewMetrics(shard int, podName string) *Metrics {
	return &Metrics{m: newNodeMetrics(shard, podName, time.Now)}
}

// InstallRaftSink makes m the process's go-metrics sink, so hashicorp/raft's
// timings and transitions reach it. Call it once, before opening the Raft
// node, so start-up restores are counted.
func (m *Metrics) InstallRaftSink() error {
	cfg := gometrics.DefaultConfig("")
	cfg.EnableHostname, cfg.EnableRuntimeMetrics, cfg.EnableServiceLabel = false, false, false
	_, err := gometrics.NewGlobal(cfg, m.m.raftSink())
	return err
}

// Handler serves GET /metrics from m. It serves before the node opens (the
// Raft histograms and process metrics; positions and outbox once attached).
func (m *Metrics) Handler() http.Handler { return m.m.handler() }
