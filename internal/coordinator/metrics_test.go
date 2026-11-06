package coordinator_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// The coordinator's half of docs/operations/storage-metrics.md (#113):
// member heartbeat state and the outcome of every routed call.

func scrape(t *testing.T, h *harness) map[string]*dto.MetricFamily {
	t.Helper()
	rec := httptest.NewRecorder()
	h.c.MetricsHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: %d", rec.Code)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(rec.Body.String()))
	if err != nil {
		t.Fatalf("parse /metrics: %v", err)
	}
	return fams
}

func value(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	f := fams[name]
	if f == nil {
		return 0, false
	}
	for _, m := range f.GetMetric() {
		got := map[string]string{}
		for _, l := range m.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}
		match := true
		for k, v := range labels {
			match = match && got[k] == v
		}
		if !match {
			continue
		}
		switch {
		case m.GetGauge() != nil:
			return m.GetGauge().GetValue(), true
		case m.GetCounter() != nil:
			return m.GetCounter().GetValue(), true
		case m.GetHistogram() != nil:
			return float64(m.GetHistogram().GetSampleCount()), true
		}
	}
	return 0, false
}

func must(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := value(t, fams, name, labels)
	if !ok || got != want {
		t.Errorf("%s%v = %v (present %v), want %v", name, labels, got, ok, want)
	}
}

func TestMetricsNodeState(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	node := map[string]string{"shard": "1", "node": "storage-1-2"}
	with := func(state string) map[string]string {
		m := map[string]string{"state": state}
		for k, v := range node {
			m[k] = v
		}
		return m
	}
	fams := scrape(t, h)
	must(t, fams, "coordinator_node_state", with("UP"), 1)
	must(t, fams, "coordinator_node_state", with("SUSPECT"), 0)
	must(t, fams, "coordinator_node_state", with("DOWN"), 0)
	if n := len(fams["coordinator_node_state"].GetMetric()); n != 9*3 {
		t.Errorf("coordinator_node_state has %d series, want 27 (9 members x 3 states)", n)
	}

	h.clock.advance(4 * time.Second) // past SuspectAfter (3 s)
	fams = scrape(t, h)
	must(t, fams, "coordinator_node_state", with("UP"), 0)
	must(t, fams, "coordinator_node_state", with("SUSPECT"), 1)

	h.clock.advance(2 * time.Second) // past DownAfter (5 s)
	fams = scrape(t, h)
	must(t, fams, "coordinator_node_state", with("SUSPECT"), 0)
	must(t, fams, "coordinator_node_state", with("DOWN"), 1)
}

func TestMetricsForwardOutcomes(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 300 * time.Millisecond })
	h.ready()
	shard := map[string]string{"shard": "2"}
	outcome := func(o string) map[string]string { return map[string]string{"shard": "2", "outcome": o} }

	fams := scrape(t, h)
	for _, o := range []string{"answered", "all_down", "exhausted", "misrouted"} {
		for s := range 3 {
			must(t, fams, "coordinator_forward_total", map[string]string{"shard": string(rune('0' + s)), "outcome": o}, 0)
		}
	}

	// Answered at once by the believed leader: one attempt.
	if rec := h.do(routedCalls(t, h, 2)[0].call); rec.Code != http.StatusOK {
		t.Fatalf("routed call: %d %s", rec.Code, rec.Body)
	}
	fams = scrape(t, h)
	must(t, fams, "coordinator_forward_total", outcome("answered"), 1)
	must(t, fams, "coordinator_forward_attempts", shard, 1)
	must(t, fams, "coordinator_leader_hints_total", shard, 0)

	// A stale leader's NOT_LEADER hint is followed: answered, one hint.
	h.nodes[2][0].setHandler(notLeader(h.nodes[2][1], 2))
	if rec := h.do(routedCalls(t, h, 2)[0].call); rec.Code != http.StatusOK {
		t.Fatalf("routed call through a hint: %d %s", rec.Code, rec.Body)
	}
	fams = scrape(t, h)
	must(t, fams, "coordinator_forward_total", outcome("answered"), 2)
	must(t, fams, "coordinator_leader_hints_total", shard, 1)
	must(t, fams, "coordinator_forward_attempts", shard, 2) // two routed calls observed

	// Every member answers NO_LEADER until the budget runs out.
	for _, n := range h.nodes[2] {
		n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
	}
	h.do(routedCalls(t, h, 2)[0].call)
	fams = scrape(t, h)
	must(t, fams, "coordinator_forward_total", outcome("exhausted"), 1)

	// Every member DOWN by heartbeat age: fails fast.
	h.clock.advance(6 * time.Second)
	h.do(routedCalls(t, h, 2)[0].call)
	fams = scrape(t, h)
	must(t, fams, "coordinator_forward_total", outcome("all_down"), 1)
}

func TestMetricsCoordinatorLabelsBounded(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	h.do(routedCalls(t, h, 0)[0].call)
	allowed := map[string][]string{
		"shard":   {"0", "1", "2"},
		"outcome": {"answered", "all_down", "exhausted", "misrouted"},
		"state":   {"UP", "SUSPECT", "DOWN"},
		"le":      nil, // histogram buckets
	}
	for name, f := range scrape(t, h) {
		if !strings.HasPrefix(name, "coordinator_") {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				vals, ok := allowed[l.GetName()]
				if l.GetName() == "node" {
					ok, vals = strings.HasPrefix(l.GetValue(), "storage-"), nil
				}
				if !ok || (vals != nil && !slices.Contains(vals, l.GetValue())) {
					t.Errorf("%s: label %s=%q is not in the contract", name, l.GetName(), l.GetValue())
				}
			}
		}
	}
	rec := httptest.NewRecorder()
	h.c.MetricsHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics: %d, want 405", rec.Code)
	}
}
