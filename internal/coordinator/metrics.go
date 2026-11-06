package coordinator

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// The coordinator's metrics (docs/operations/storage-metrics.md, #113).
// Labels are the ring's shards, registered pod names and fixed outcome and
// state sets. Metrics observe routing; nothing reads them back.

// Forward outcomes: how one routed call ended.
const (
	outcomeAnswered  = "answered"  // a node answered: a result, a rejection or a validation error
	outcomeAllDown   = "all_down"  // every member DOWN by heartbeat age: failed fast
	outcomeExhausted = "exhausted" // the budget ran out
	outcomeMisrouted = "misrouted" // every member answered as another server
)

var (
	forwardOutcomes = []string{outcomeAnswered, outcomeAllDown, outcomeExhausted, outcomeMisrouted}
	nodeStates      = []internalapi.NodeState{internalapi.UP, internalapi.SUSPECT, internalapi.DOWN}
	descNodeState   = prometheus.NewDesc("coordinator_node_state",
		"1 for each registered member's current heartbeat state, 0 for the other two.", []string{"shard", "node", "state"}, nil)
)

type coordMetrics struct {
	reg      *prometheus.Registry
	forward  *prometheus.CounterVec
	hints    *prometheus.CounterVec
	attempts *prometheus.HistogramVec
}

func newCoordMetrics(c *Coordinator) *coordMetrics {
	m := &coordMetrics{
		reg: prometheus.NewRegistry(),
		forward: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "coordinator_forward_total",
			Help: "Routed calls by how they ended: answered, all_down (failed fast), exhausted (budget) or misrouted."}, []string{"shard", "outcome"}),
		hints: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "coordinator_leader_hints_total",
			Help: "NOT_LEADER hints followed (leader discovery)."}, []string{"shard"}),
		attempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "coordinator_forward_attempts",
			Help: "Nodes dialled per routed call.", Buckets: []float64{1, 2, 3, 4, 6, 8, 12, 16}}, []string{"shard"}),
	}
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.forward, m.hints, m.attempts, (*stateCollector)(c))
	for s := range c.ring.Shards() {
		shard := strconv.Itoa(s)
		for _, o := range forwardOutcomes {
			m.forward.WithLabelValues(shard, o)
		}
		m.hints.WithLabelValues(shard)
		m.attempts.WithLabelValues(shard)
	}
	return m
}

func (m *coordMetrics) observeForward(shard int, outcome string, attempts int) {
	s := strconv.Itoa(shard)
	m.forward.WithLabelValues(s, outcome).Inc()
	m.attempts.WithLabelValues(s).Observe(float64(attempts))
}

func (m *coordMetrics) observeHint(shard int) { m.hints.WithLabelValues(strconv.Itoa(shard)).Inc() }

// stateCollector reports every registered member's heartbeat state at
// scrape time; a member that leaves the registry loses its series.
type stateCollector Coordinator

func (sc *stateCollector) Describe(ch chan<- *prometheus.Desc) { ch <- descNodeState }

func (sc *stateCollector) Collect(ch chan<- prometheus.Metric) {
	c := (*Coordinator)(sc)
	now := c.cfg.Now()
	type row struct {
		shard, node string
		state       internalapi.NodeState
	}
	c.mu.Lock()
	rows := make([]row, 0, len(c.nodes))
	for _, m := range c.nodes {
		rows = append(rows, row{strconv.Itoa(m.reg.ShardId), string(m.reg.NodeId), c.state(m, now)})
	}
	c.mu.Unlock()
	for _, r := range rows {
		for _, st := range nodeStates {
			v := 0.0
			if st == r.state {
				v = 1
			}
			ch <- prometheus.MustNewConstMetric(descNodeState, prometheus.GaugeValue, v, r.shard, r.node, string(st))
		}
	}
}

// MetricsHandler serves GET /metrics only: read-only, no operator token
// (docs/operations/storage-metrics.md). Serve it on its own port.
func (c *Coordinator) MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(c.metrics.reg, promhttp.HandlerOpts{Timeout: 5 * time.Second}))
	return mux
}
