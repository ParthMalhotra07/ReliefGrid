//go:build schedules

package workflow

import (
	"context"
	"os"
	"reflect"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

type capturedHandoff struct {
	outboxHandoff
	replay func(context.Context) (store.Result, error)
}

type observingStore struct {
	store.Store
	x *execution
}

func handoffType(c store.TransitionRequest) (string, string) {
	switch c.To {
	case store.RequestMatching:
		return "match.proposed", "match.proposed"
	case store.RequestMatched:
		return "match.completed", "match.completed"
	case store.RequestWaiting:
		route := "delay.5s"
		if c.NextAttemptAt-c.At >= 120000 {
			route = "delay.120s"
		} else if c.NextAttemptAt-c.At >= 30000 {
			route = "delay.30s"
		}
		return "request.retry", route
	default:
		return "request.state_changed", "request.status." + strings.ToLower(string(c.To))
	}
}

func (s observingStore) TransitionRequest(ctx context.Context, c store.TransitionRequest) (store.Result, error) {
	result, err := s.Store.TransitionRequest(ctx, c)
	if err == nil && result.Outcome == store.OutcomeOK {
		eventType, route := handoffType(c)
		h := capturedHandoff{outboxHandoff{c.Meta, c.Key, c.ExpectedVersion + 1, eventType, route, c.Outbox}, func(ctx context.Context) (store.Result, error) { return s.Store.TransitionRequest(ctx, c) }}
		s.x.observedMu.Lock()
		s.x.observed = append(s.x.observed, h)
		s.x.observedMu.Unlock()
	}
	return result, err
}

func (s observingStore) RetryWorkflow(ctx context.Context, c store.RetryWorkflow) (store.Result, error) {
	result, err := s.Store.RetryWorkflow(ctx, c)
	if err == nil && result.Outcome == store.OutcomeOK {
		_, route := handoffType(store.TransitionRequest{Meta: c.Meta, To: store.RequestWaiting, NextAttemptAt: c.NextAttemptAt})
		h := capturedHandoff{outboxHandoff{c.Meta, c.Key, c.ExpectedVersion + 1, "request.retry", route, c.Outbox}, func(ctx context.Context) (store.Result, error) { return s.Store.RetryWorkflow(ctx, c) }}
		s.x.observedMu.Lock()
		s.x.observed = append(s.x.observed, h)
		s.x.observedMu.Unlock()
	}
	return result, err
}

// Matchers use the real FSM/coordinator through this recording decorator. Fault
// seam type assertions still see x.s, not a replacement state machine.
func (x *execution) confirm() {
	x.t.Helper()
	if _, err := x.matcher(x.s).Match(x.ctx, x.entities["R"]); err != nil {
		x.t.Fatal(err)
	}
}

func (x *execution) assertClosedA(at int64) store.Closure {
	x.t.Helper()
	a := x.attempts["attemptA"]
	closure, err := x.s.ReadClosure(x.ctx, store.ResourceKey(a.ResourceID), a.AttemptID)
	if err != nil {
		x.t.Fatalf("SCH04 strong closure read: %v", err)
	}
	if err := validateClosedAttempt(closure, store.ResourceKey(a.ResourceID), x.entities["R"], a, at); err != nil {
		x.t.Fatal(err)
	}
	return closure
}

func assertScheduleOutbox(x *execution, f ScheduleFixture) {
	x.t.Helper()
	x.observedMu.Lock()
	captured := append([]capturedHandoff(nil), x.observed...)
	x.observedMu.Unlock()
	expected := map[string][]string{
		"01": {"match.completed"}, "02": {"match.completed"}, "03": {"match.completed"},
		"04": {"request.state_changed", "request.retry", "match.completed"},
		"05": {"request.state_changed"}, "06": {"request.state_changed"}, "07": {"request.retry"},
		"08": {"request.retry", "match.completed"}, "09": {"request.state_changed"},
		"10": {"request.state_changed"}, "11": {"request.state_changed"}, "12": {"match.completed"},
		"14": {"match.completed"}, "15": {"request.retry", "match.completed"},
		"16": {"match.proposed", "match.completed"}, "17": {"match.proposed", "match.completed"},
	}
	counts := map[string]int{}
	for _, h := range captured {
		counts[h.eventType]++
	}
	for _, kind := range expected[scheduleCode(f)] {
		if counts[kind] == 0 {
			x.t.Fatalf("%s never observed production %s handoff", f.Tag, kind)
		}
	}
	if len(captured) == 0 {
		return
	} // SCH13 restores an already-matched initial state.
	pending, err := x.s.PendingOutbox(x.ctx, store.OutboxQuery{Shard: x.requestShard, Limit: 100, Consistency: store.ConsistencyStrong})
	if err != nil {
		x.t.Fatal(err)
	}
	before := x.read("R")
	for _, h := range captured {
		result, err := x.s.Operation(x.ctx, h.key, h.meta.OperationID)
		if err != nil {
			x.t.Fatal(err)
		}
		if err := validateHandoff(h.outboxHandoff, pending, result); err != nil {
			x.t.Fatal(err)
		}
		replay, err := h.replay(x.ctx)
		if err != nil || !replay.Replayed || replay.OperationID != h.meta.OperationID || replay.Outcome != store.OutcomeOK || replay.Version != h.version {
			x.t.Fatalf("identical handoff replay changed: %+v %v", replay, err)
		}
	}
	after := x.read("R")
	again, err := x.s.PendingOutbox(x.ctx, store.OutboxQuery{Shard: x.requestShard, Limit: 100, Consistency: store.ConsistencyStrong})
	if err != nil {
		x.t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(pending, again) {
		x.t.Fatal("replayed handoff changed current state or duplicated outbox")
	}
	x.t.Logf("%s durable handoffs=%d; exact operation replay preserved state/outbox", f.Tag, len(captured))
	if os.Getenv("SCHEDULE_BACKEND") == "cluster" {
		drainObservedOutbox(x, pending)
	}
}

// The public pending endpoint is bounded to 100 rows without a cursor. Drain
// each cluster fixture through real routed confirms after its assertions, so a
// 50-seed run never hides later events behind earlier fixtures' pending rows.
// No test-owned mark occurs before the actual broker confirms the handoff.
func drainObservedOutbox(x *execution, pending []store.OutboxEvent) {
	x.t.Helper()
	for _, event := range pending {
		if event.EntityKey != store.RequestKey(x.entities["R"]) {
			x.t.Fatal("foreign pending outbox work: use a clean schedule deployment")
		}
	}
	connection, address := brokerForExecution(x)
	channel, err := connection.Channel()
	if err != nil {
		x.t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	queue, err := channel.QueueDeclare("schedule.outbox.audit", true, false, false, false, map[string]any{"x-queue-type": "quorum"})
	if err != nil {
		x.t.Fatal(err)
	}
	if err := channel.QueueBind(queue.Name, "#", "relief.events", false, nil); err != nil {
		x.t.Fatal(err)
	}
	publisher, err := mq.NewPublisher(x.ctx, address)
	if err != nil {
		x.t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()
	relay, err := outbox.New(outbox.Config{Store: x.s})
	if err != nil {
		x.t.Fatal(err)
	}
	report, err := relay.Step(x.ctx, x.requestShard, publisher)
	if err != nil || report.Read != len(pending) || report.Published != len(pending) || report.Marked != len(pending) {
		x.t.Fatalf("confirmed schedule drain: %+v: %v", report, err)
	}
	remaining, err := x.s.PendingOutbox(x.ctx, store.OutboxQuery{Shard: x.requestShard, Limit: 100, Consistency: store.ConsistencyStrong})
	if err != nil || len(remaining) != 0 {
		x.t.Fatalf("confirmed drain left work: %d %v", len(remaining), err)
	}
}
