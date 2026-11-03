package raftstore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// newTracedCluster is newBootstrappedCluster with node i reporting spans to
// tps[i] (nil: no TracerProvider, tracing off).
func newTracedCluster(t testing.TB, tps []trace.TracerProvider) *testCluster {
	t.Helper()
	c := newTestCluster(t, 3, testProfile)
	for i := range c.cfgs {
		c.cfgs[i].TracerProvider = tps[i]
		c.restart(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := c.nodes[0].Bootstrap(ctx, c.servers()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c.waitLeader()
	return c
}

func spanRecorders(t testing.TB, n int) ([]*tracetest.SpanRecorder, []trace.TracerProvider) {
	t.Helper()
	srs := make([]*tracetest.SpanRecorder, n)
	tps := make([]trace.TracerProvider, n)
	for i := range n {
		srs[i] = tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(srs[i]))
		t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		tps[i] = tp
	}
	return srs, tps
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func raftApplySpans(sr *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "raft.apply" {
			out = append(out, s)
		}
	}
	return out
}

func outboxEvent(id, typ string) store.OutboxEvent {
	return store.OutboxEvent{EventID: id, EventType: typ, SchemaVersion: 1, Exchange: "relief.events",
		RoutingKey: typ, Payload: json.RawMessage(`{}`)}
}

// The leader times each proposal as raft.apply, a child of the caller's
// span, from proposal to the FSM's response; the outbox rows it carries are
// named on the span. Followers apply the same entry with no span at all:
// nothing trace-related runs inside FSM apply (invariant 2).
func TestApplyRecordsRaftApplyOnTheProposerOnly(t *testing.T) {
	srs, tps := spanRecorders(t, 3)
	c := newTracedCluster(t, tps)
	li, l := c.waitLeader()

	ctx, parent := tps[li].Tracer("test").Start(testCtx(t), "caller")
	cmd := store.CreateEntity{Meta: store.Meta{OperationID: "op-trace", At: t0}, Key: "kv:trace", Value: json.RawMessage(`1`),
		Outbox: []store.OutboxEvent{outboxEvent("ev-1", "kv.created"), outboxEvent("ev-2", "kv.audited")}}
	res, err := l.Apply(ctx, cmd)
	parent.End()
	if err != nil || res.Outcome != store.OutcomeOK {
		t.Fatalf("Apply: %+v, %v", res, err)
	}

	spans := raftApplySpans(srs[li])
	if len(spans) != 1 {
		t.Fatalf("leader recorded %d raft.apply spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("raft.apply parent = %s, want the caller %s", s.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if s.SpanKind() != trace.SpanKindInternal {
		t.Errorf("kind = %v, want internal", s.SpanKind())
	}
	for key, want := range map[attribute.Key]attribute.Value{
		"reliefgrid.shard":   attribute.IntValue(1),
		"raft.op":            attribute.StringValue("CreateEntity"),
		"raft.index":         attribute.Int64Value(int64(res.AppliedIndex)),
		"raft.outcome":       attribute.StringValue("OK"),
		"outbox.event_ids":   attribute.StringSliceValue([]string{"ev-1", "ev-2"}),
		"outbox.event_types": attribute.StringSliceValue([]string{"kv.created", "kv.audited"}),
	} {
		got, ok := spanAttr(s, key)
		if !ok || got.String() != want.String() {
			t.Errorf("%s = %q (present %v), want %q", key, got.String(), ok, want.String())
		}
	}

	for i := range c.nodes {
		if i == li {
			continue
		}
		if err := c.nodes[i].WaitApplied(testCtx(t), res.AppliedIndex); err != nil {
			t.Fatalf("follower %d WaitApplied(%d): %v", i, res.AppliedIndex, err)
		}
		if got := srs[i].Ended(); len(got) != 0 {
			t.Errorf("follower %d recorded %d spans applying the entry, want 0", i, len(got))
		}
	}
}

// A committed rejection is an outcome, not a span error; a proposal on a
// follower is never appended and says so.
func TestRaftApplyOutcomes(t *testing.T) {
	srs, tps := spanRecorders(t, 3)
	c := newTracedCluster(t, tps)
	li, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))

	if res := mustApply(t, l, cas("op-2", "kv:a", 9, 2)); res.Outcome != store.OutcomeVersionConflict {
		t.Fatalf("stale CAS: %+v", res)
	}
	spans := raftApplySpans(srs[li])
	if len(spans) != 2 {
		t.Fatalf("leader recorded %d raft.apply spans, want 2", len(spans))
	}
	if v, _ := spanAttr(spans[1], "raft.outcome"); v.AsString() != "VERSION_CONFLICT" {
		t.Errorf("rejected CAS raft.outcome = %q, want VERSION_CONFLICT", v.AsString())
	}
	if v, _ := spanAttr(spans[1], "raft.op"); v.AsString() != "CompareAndSet" {
		t.Errorf("raft.op = %q, want CompareAndSet", v.AsString())
	}

	fi, f := c.follower(li)
	if _, err := f.Apply(testCtx(t), cas("op-3", "kv:b", 0, 1)); err == nil {
		t.Fatal("Apply on a follower succeeded")
	}
	spans = raftApplySpans(srs[fi])
	if len(spans) != 1 {
		t.Fatalf("follower recorded %d raft.apply spans for its refused proposal, want 1", len(spans))
	}
	if v, _ := spanAttr(spans[0], "raft.outcome"); v.AsString() != "NOT_LEADER" {
		t.Errorf("follower raft.outcome = %q, want NOT_LEADER", v.AsString())
	}
	if _, ok := spanAttr(spans[0], "raft.index"); ok {
		t.Error("a proposal never appended has a raft.index")
	}
}

// A proposal whose wait ends without an answer may still commit: its span
// says unknown, never a rejection (invariant 5).
func TestRaftApplyUnknownOutcome(t *testing.T) {
	srs, tps := spanRecorders(t, 3)
	c := newTracedCluster(t, tps)
	li, l := c.waitLeader()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Apply(ctx, cas("op-u", "kv:u", 0, 1)); err == nil {
		t.Fatal("Apply under a cancelled context succeeded")
	}
	spans := raftApplySpans(srs[li])
	if len(spans) != 1 {
		t.Fatalf("leader recorded %d raft.apply spans, want 1", len(spans))
	}
	if v, _ := spanAttr(spans[0], "raft.outcome"); v.AsString() != "UNKNOWN" {
		t.Errorf("raft.outcome = %q, want UNKNOWN", v.AsString())
	}
}

// BenchmarkApply measures the cost of the raft.apply span on a 3-node
// loopback cluster: no SDK (the default no-op provider), the SDK sampling
// everything into a discarding exporter, and the SDK at 10%. S4.4 evidence.
//
//	go test ./internal/raftstore -run '^$' -bench 'Apply$' -benchtime 2000x -count 10
func BenchmarkApply(b *testing.B) {
	sdk := func(ratio float64) trace.TracerProvider {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
			sdktrace.WithBatcher(tracetest.NewNoopExporter()))
		b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		return tp
	}
	for _, cfg := range []struct {
		name string
		tp   func() trace.TracerProvider
	}{
		{"nosdk", func() trace.TracerProvider { return nil }},
		{"sdk100", func() trace.TracerProvider { return sdk(1) }},
		{"sdk10", func() trace.TracerProvider { return sdk(0.1) }},
	} {
		b.Run(cfg.name, func(b *testing.B) {
			tp := cfg.tp()
			c := newTracedCluster(b, slices.Repeat([]trace.TracerProvider{tp}, 3))
			_, l := c.waitLeader()
			i := 0
			for b.Loop() {
				i++
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := l.Apply(ctx, cas(fmt.Sprintf("op-%d", i), fmt.Sprintf("kv:%d", i), 0, i))
				cancel()
				if err != nil || res.Outcome != store.OutcomeOK {
					b.Fatalf("Apply %d: %+v, %v", i, res, err)
				}
			}
		})
	}
}
