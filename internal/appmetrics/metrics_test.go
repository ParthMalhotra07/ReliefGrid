package appmetrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	dto "github.com/prometheus/client_model/go"
)

func realStore(t *testing.T) *memstore.Store {
	t.Helper()
	s, err := memstore.New(t.Context(), memstore.WithRouter(func(string) int { return 0 }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func process(t *testing.T, s store.Store, at time.Time) *Process {
	t.Helper()
	p, err := New(s, "matcher", func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func metric(t *testing.T, p *Process, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := p.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.Metric {
			match := true
			for k, v := range labels {
				found := false
				for _, l := range m.Label {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				match = match && found
			}
			if match {
				return m
			}
		}
	}
	t.Fatalf("missing metric %s %v", name, labels)
	return nil
}
func seed(t *testing.T, s store.Store, at int64, n int) {
	t.Helper()
	for i := range n {
		id := fmt.Sprintf("metrics-%03d", i)
		r, err := s.CreateEntity(t.Context(), store.CreateEntity{Meta: store.Meta{OperationID: "create-" + id, At: at}, Key: store.RequestKey(id), Request: &store.Request{ID: id, Type: store.TypeWater, Geohash: "s000000", PeopleAffected: 1, Urgency: 2}})
		if err != nil || r.Outcome != store.OutcomeOK {
			t.Fatalf("seed %s: %+v %v", id, r, err)
		}
	}
}
func TestPartialAndFailedLatePageNeverReplaceCompleteMetrics(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint("late=", late), func(t *testing.T) {
			s := realStore(t)
			now := time.Date(2026, 11, 6, 1, 0, 0, 0, time.UTC)
			seed(t, s, now.Add(-30*time.Second).UnixMilli(), 101)
			var fail atomic.Bool
			var calls atomic.Int32
			faulty := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
				if c.Method != "Scan" || !fail.Load() {
					return memstore.Fault{}
				}
				if late {
					if calls.Add(1) == 2 {
						return memstore.Fault{DropBefore: true}
					}
					return memstore.Fault{}
				}
				return memstore.Fault{FailShards: []int{1}}
			})
			p := process(t, faulty, now)
			if err := p.Sample(t.Context()); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{"state": "PENDING", "urgency": "HIGH"}
			if got := metric(t, p, "relief_workflow_pending", labels).GetGauge().GetValue(); got != 101 {
				t.Fatalf("paged count=%g", got)
			}
			if got := metric(t, p, "relief_workflow_age_seconds", labels).GetGauge().GetValue(); got != 30 {
				t.Fatalf("workflow age=%g", got)
			}
			before := metric(t, p, "relief_metrics_last_success_timestamp_seconds", nil).GetGauge().GetValue()
			fail.Store(true)
			if err := p.Sample(t.Context()); err == nil {
				t.Fatal("incomplete observation accepted")
			}
			if metric(t, p, "relief_metrics_observation_success", nil).GetGauge().GetValue() != 0 {
				t.Fatal("incomplete walk claimed success")
			}
			if metric(t, p, "relief_workflow_pending", labels).GetGauge().GetValue() != 101 {
				t.Fatal("partial walk replaced last complete count")
			}
			if metric(t, p, "relief_metrics_last_success_timestamp_seconds", nil).GetGauge().GetValue() != before {
				t.Fatal("failed walk refreshed success timestamp")
			}
		})
	}
}
func TestOnlyNonReplayedCommittedMatchContributesBusinessLatency(t *testing.T) {
	p := process(t, realStore(t), time.UnixMilli(31000))
	before := store.Request{CreatedAt: 1000, Urgency: 2}
	cmd := store.TransitionRequest{Meta: store.Meta{At: 31000}, To: store.RequestMatched}
	for _, r := range []store.Result{{Outcome: store.OutcomeVersionConflict}, {Outcome: store.OutcomeOK, Replayed: true}} {
		p.Transition(before, cmd, r)
	}
	label := map[string]string{"urgency": "HIGH"}
	if metric(t, p, "relief_match_latency_seconds", label).GetHistogram().GetSampleCount() != 0 {
		t.Fatal("replay/rejection counted as new match")
	}
	p.Transition(before, cmd, store.Result{Outcome: store.OutcomeOK})
	h := metric(t, p, "relief_match_latency_seconds", label).GetHistogram()
	if h.GetSampleCount() != 1 || h.GetSampleSum() != 30 {
		t.Fatalf("created-to-matched histogram=%v", h)
	}
	p.observe("cas", store.Result{Outcome: store.OutcomeVersionConflict}, store.ErrUnknownOutcome)
	if metric(t, p, "relief_cas_conflicts_total", map[string]string{"command": "cas"}).GetCounter().GetValue() != 0 {
		t.Fatal("unknown became rejection")
	}
}
func TestMetricsLabelsCannotContainRequestIDsOrErrorStrings(t *testing.T) {
	p := process(t, realStore(t), time.Now())
	for i := range 1000 {
		p.Attempt(fmt.Sprintf("request-%d", i), fmt.Errorf("private payload %d", i))
		p.Transition(store.Request{CreatedAt: 1, Urgency: 1000 + i}, store.TransitionRequest{Meta: store.Meta{At: 2000}, To: store.RequestMatched}, store.Result{Outcome: store.OutcomeOK})
	}
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), "GET", "/metrics", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), "private payload") || strings.Contains(w.Body.String(), "request-") {
		t.Fatalf("unbounded or sensitive label: HTTP%d", w.Code)
	}
	families, err := p.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		for _, m := range f.Metric {
			for _, l := range m.Label {
				switch l.GetName() {
				case "service", "outcome", "urgency", "command", "state", "shard":
				default:
					t.Fatalf("unexpected label %s", l.GetName())
				}
			}
		}
	}
}
func TestPrivateMetricsListenerStopsOnParentCancellation(t *testing.T) {
	p := process(t, realStore(t), time.Now())
	ctx, cancel := context.WithCancel(t.Context())
	stop, err := p.Start(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	address := p.Address()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	get := func() bool {
		req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+address+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := client.Do(req)
		if err != nil {
			return false
		}
		_ = r.Body.Close()
		return r.StatusCode == 200
	}
	if !get() {
		t.Fatal("private listener never started")
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for get() {
		if time.Now().After(deadline) {
			t.Fatal("metrics listener outlived parent cancellation; would consume API pod drain budget")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMatchLatencyIncludesCommittedResponseDelay(t *testing.T) {
	p := process(t, realStore(t), time.UnixMilli(33000))
	p.Transition(store.Request{CreatedAt: 1000, Urgency: 2}, store.TransitionRequest{Meta: store.Meta{At: 31000}, To: store.RequestMatched}, store.Result{Outcome: store.OutcomeOK})
	got := metric(t, p, "relief_match_latency_seconds", map[string]string{"urgency": "HIGH"}).GetHistogram().GetSampleSum()
	if got != 32 {
		t.Fatalf("creation-to-committed-response latency=%g, want32 including two-second commit/response delay", got)
	}
}

// modeObserver records the read mode of every scan and outbox page (Sample
// reads the shards' outbox pages concurrently).
type modeObserver struct {
	store.Store
	mu            sync.Mutex
	scans, outbox []store.Consistency
}

func (o *modeObserver) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	o.mu.Lock()
	o.scans = append(o.scans, q.Consistency)
	o.mu.Unlock()
	return o.Store.Scan(ctx, q)
}

func (o *modeObserver) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	o.mu.Lock()
	o.outbox = append(o.outbox, q.Consistency)
	o.mu.Unlock()
	return o.Store.PendingOutbox(ctx, q)
}

// S4.1: the metrics walk and its outbox pages are read through the read
// fence (linearizable): no log entry per page on every shard each sample.
func TestSampleReadsAreLinearizable(t *testing.T) {
	s := realStore(t)
	now := time.Date(2026, 11, 6, 1, 0, 0, 0, time.UTC)
	seed(t, s, now.Add(-30*time.Second).UnixMilli(), 3)
	o := &modeObserver{Store: s}
	if err := process(t, o, now).Sample(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(o.scans) == 0 || len(o.outbox) == 0 {
		t.Fatalf("observed %d scans and %d outbox pages, want both", len(o.scans), len(o.outbox))
	}
	for _, m := range append(o.scans, o.outbox...) {
		if m != store.ConsistencyLinearizable {
			t.Errorf("sample read %q, want linearizable (scans %v, outbox %v)", m, o.scans, o.outbox)
		}
	}
}
