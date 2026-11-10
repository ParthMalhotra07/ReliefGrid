// Package appmetrics exports bounded, observational application metrics.
// It never participates in workflow decisions or durable handoffs.
package appmetrics

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var states = []string{"PENDING", "WAITING", "MATCHING", "RETRYING", "CANCELLING", "FULFILLING"}
var urgencies = []string{"NORMAL", "HIGH", "CRITICAL", "unknown"}
var outcomes = []string{"matched", "waiting", "cancelled", "fulfilled", "conflict", "failed", "unknown", "deferred", "ignored", "retry_scheduled"}
var commands = []string{"create", "cas", "request_transition", "retry", "resource_transition", "claim", "close", "outbox_mark"}

type Process struct {
	address                                                atomic.Pointer[string]
	source                                                 store.Store
	now                                                    func() time.Time
	registry                                               *prometheus.Registry
	outcomes                                               *prometheus.CounterVec
	latency                                                *prometheus.HistogramVec
	conflicts                                              *prometheus.CounterVec
	calls                                                  *prometheus.CounterVec
	ages, counts                                           *prometheus.GaugeVec
	valid, lastSuccess                                     prometheus.Gauge
	shardRead, batch, batchAge, batchValid, batchTruncated *prometheus.GaugeVec
}

func New(source store.Store, service string, now func() time.Time) (*Process, error) {
	if source == nil || source.Shards() != 3 || (service != "api" && service != "matcher") {
		return nil, fmt.Errorf("metrics: three-shard store and fixed api/matcher service required")
	}
	if now == nil {
		now = time.Now
	}
	p := &Process{source: source, now: now, registry: prometheus.NewPedanticRegistry()}
	labels := prometheus.Labels{"service": service}
	p.outcomes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relief_match_outcomes_total", Help: "Workflow invocation outcomes observed by this process; not a durable unique-request ledger.", ConstLabels: labels}, []string{"outcome"})
	p.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relief_match_latency_seconds", Help: "Creation-to-observed-committed-MATCHED duration for non-replayed successful responses; not handler latency.", ConstLabels: labels, Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}}, []string{"urgency"})
	p.conflicts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relief_cas_conflicts_total", Help: "Non-replayed committed VERSION_CONFLICT responses observed by command class.", ConstLabels: labels}, []string{"command"})
	p.calls = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relief_store_calls_total", Help: "Application mutation call responses, including retries; unknown is never a committed rejection.", ConstLabels: labels}, []string{"command", "outcome"})
	p.ages = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_workflow_age_seconds", Help: "Oldest unfinished request age since creation by current state and urgency, from last complete walk. Check observation validity and timestamp.", ConstLabels: labels}, []string{"state", "urgency"})
	p.counts = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_workflow_pending", Help: "Unfinished request count from the last complete authoritative walk; not a live atomic global snapshot.", ConstLabels: labels}, []string{"state", "urgency"})
	p.valid = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relief_metrics_observation_success", Help: "1 when the last bounded authoritative workflow walk completed; 0 means unknown or stale, never empty.", ConstLabels: labels})
	p.lastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relief_metrics_last_success_timestamp_seconds", Help: "Wall-clock time of the last complete workflow walk; zero means never observed.", ConstLabels: labels})
	p.shardRead = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_shard_read_available", Help: "Whether the latest workflow walk confirmed each shard; not Kubernetes readiness or business completion.", ConstLabels: labels}, []string{"shard"})
	p.batch = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_outbox_batch_pending", Help: "Size of first unpublished batch, capped at 100. Lower bound only; NEVER an exact outbox total.", ConstLabels: labels}, []string{"shard"})
	p.batchAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_outbox_batch_oldest_age_seconds", Help: "Oldest age in the observed first outbox batch only, not necessarily the full backlog.", ConstLabels: labels}, []string{"shard"})
	p.batchValid = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_outbox_batch_observation_success", Help: "1 when the latest first-batch observation succeeded; failed reads retain previous values.", ConstLabels: labels}, []string{"shard"})
	p.batchTruncated = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relief_outbox_batch_may_have_more", Help: "1 when the batch is full and more rows may exist; exact totals require the storage metrics handoff.", ConstLabels: labels}, []string{"shard"})
	p.registry.MustRegister(p.outcomes, p.latency, p.conflicts, p.calls, p.ages, p.counts, p.valid, p.lastSuccess, p.shardRead, p.batch, p.batchAge, p.batchValid, p.batchTruncated)
	for _, v := range outcomes {
		p.outcomes.WithLabelValues(v)
	}
	for _, v := range urgencies {
		p.latency.WithLabelValues(v)
		for _, s := range states {
			p.ages.WithLabelValues(s, v)
			p.counts.WithLabelValues(s, v)
		}
	}
	for _, v := range commands {
		p.conflicts.WithLabelValues(v)
	}
	for shard := range 3 {
		v := strconv.Itoa(shard)
		p.shardRead.WithLabelValues(v)
		p.batch.WithLabelValues(v)
		p.batchAge.WithLabelValues(v)
		p.batchValid.WithLabelValues(v)
		p.batchTruncated.WithLabelValues(v)
	}
	return p, nil
}
func (p *Process) Handler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{Timeout: 3 * time.Second, MaxRequestsInFlight: 4})
}
func urgency(v int) string {
	switch v {
	case 1:
		return "NORMAL"
	case 2:
		return "HIGH"
	case 3:
		return "CRITICAL"
	default:
		return "unknown"
	}
}
func (p *Process) Attempt(outcome string, err error) {
	name := "ignored"
	if err != nil {
		switch {
		case errors.Is(err, store.ErrUnknownOutcome), errors.Is(err, workflow.ErrOwnershipUnknown):
			name = "unknown"
		case errors.Is(err, workflow.ErrConcurrentChange):
			name = "conflict"
		default:
			name = "failed"
		}
	} else {
		switch outcome {
		case "MATCHED":
			name = "matched"
		case "NO_CANDIDATE":
			name = "waiting"
		case "CANCELLED":
			name = "cancelled"
		case "FULFILLED":
			name = "fulfilled"
		case "DEFERRED":
			name = "deferred"
		case "RETRY_SCHEDULED":
			name = "retry_scheduled"
		}
	}
	p.outcomes.WithLabelValues(name).Inc()
}
func (p *Process) Transition(before store.Request, cmd store.TransitionRequest, result store.Result) {
	if result.Outcome != store.OutcomeOK || result.Replayed || cmd.To != store.RequestMatched || before.CreatedAt <= 0 {
		return
	}
	observedAt := p.now().UnixMilli()
	if observedAt < before.CreatedAt {
		return
	}
	p.latency.WithLabelValues(urgency(before.Urgency)).Observe(float64(observedAt-before.CreatedAt) / 1000)
}
func (p *Process) observe(command string, r store.Result, err error) {
	// Command labels are supplied only by the fixed wrappers below.
	outcome := "unknown"
	if err == nil {
		switch r.Outcome {
		case store.OutcomeOK, store.ClaimSuccess:
			outcome = "ok"
		case store.OutcomeVersionConflict:
			outcome = "version_conflict"
		case store.ClaimConflictClosed, store.ClaimRejectedClosed, store.ClaimClosed:
			outcome = "closed"
		default:
			outcome = "rejected"
		}
	}
	p.calls.WithLabelValues(command, outcome).Inc()
	if err == nil && r.Outcome == store.OutcomeVersionConflict && !r.Replayed {
		p.conflicts.WithLabelValues(command).Inc()
	}
}

// Address reports the actual bound private listener address, including a chosen port zero.
func (p *Process) Address() string {
	if v := p.address.Load(); v != nil {
		return *v
	}
	return ""
}
