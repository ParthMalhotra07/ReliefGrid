package reconcile_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/reconcile"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"go.opentelemetry.io/otel/trace"
	"testing"
	"time"
)

type ageCapture struct {
	store.Store
	commands []store.AgeRequest
	drops    int
}

func (s *ageCapture) AgeRequest(ctx context.Context, c store.AgeRequest) (store.Result, error) {
	s.commands = append(s.commands, c)
	if s.drops > 0 {
		s.drops--
		return store.Result{}, store.ErrUnknownOutcome
	}
	return s.Store.AgeRequest(ctx, c)
}
func agingEngine(t *testing.T, s store.Store, clock *time.Time) *reconcile.Engine {
	t.Helper()
	cfg := reconcile.Config{Store: s, Now: func() time.Time { return *clock }, PageSize: 2, BatchSize: 1}
	cfg.AgingInterval = time.Minute
	e, err := reconcile.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func agedWaiting(t *testing.T, ctx context.Context, s store.Store, created time.Time) string {
	t.Helper()
	key := store.RequestKey(id("aging-request"))
	q := store.Request{ID: id("aging-request"), Type: store.TypeWater, Geohash: "s000000", PeopleAffected: 1, Urgency: 1}
	result, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: id("aging-create"), At: created.UnixMilli()}, Key: key, Request: &q})
	must(t, result, err)
	result, err = s.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: id("aging-wait"), At: created.UnixMilli() + 1}, Key: key, ExpectedVersion: 1, ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: now.Add(time.Hour).UnixMilli()})
	must(t, result, err)
	return key
}
func TestAgingPrecedesFutureBackoffAndPreservesWorkPriority(t *testing.T) {
	ctx, raw, _ := fixture(t)
	key := agedWaiting(t, ctx, raw, now.Add(-2*time.Minute))
	clock := now
	observed := &ageCapture{Store: raw}
	e := agingEngine(t, observed, &clock)
	for i := 0; i < 3; i++ {
		if _, err := e.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rec := read(t, ctx, raw, key)
	if len(observed.commands) == 0 || rec.Request.Urgency != 1 || rec.Request.EffectiveUrgency != 3 {
		t.Fatalf("unaged or caller urgency overwritten: %+v", rec.Request)
	}
	if rec.Request.NextAttemptAt >= now.Add(time.Hour).UnixMilli() {
		t.Fatal("aging never woke future backoff")
	}
	delayed, immediate := false, false
	for shard := 0; shard < 3; shard++ {
		rows, err := raw.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.EventType != "request.retry" {
				continue
			}
			var body struct {
				Payload struct {
					Urgency int `json:"urgency"`
				}
			}
			if err := json.Unmarshal(row.Payload, &body); err != nil {
				t.Fatal(err)
			}
			if body.Payload.Urgency < 2 {
				t.Fatal("subsequent work event downgraded effective urgency")
			}
			immediate = immediate || row.Exchange == store.AgingWakeExchange
			delayed = delayed || row.Exchange == "relief.retry"
		}
	}
	if !delayed || !immediate {
		t.Fatal("did not observe both aging wake and later production retry")
	}
}
func TestAgingUnknownRetryKeepsCommandAcrossClockBoundary(t *testing.T) {
	ctx, raw, _ := fixture(t)
	agedWaiting(t, ctx, raw, now.Add(-time.Minute))
	clock := now
	ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: [16]byte{1}, SpanID: [8]byte{1}, TraceFlags: trace.FlagsSampled}))
	observed := &ageCapture{Store: raw, drops: 2}
	e := agingEngine(t, observed, &clock)
	if _, err := e.Step(ctx); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("unknown=%v", err)
	}
	clock = clock.Add(2 * time.Hour)
	ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: [16]byte{2}, SpanID: [8]byte{2}, TraceFlags: trace.FlagsSampled}))
	if _, err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if len(observed.commands) < 3 {
		t.Fatal("missing retry observation")
	}
	first, _ := json.Marshal(observed.commands[0])
	for _, cmd := range observed.commands[1:3] {
		data, _ := json.Marshal(cmd)
		if string(data) != string(first) {
			t.Fatal("clock changed unresolved operation bytes")
		}
	}
}
func TestAgingExactThreshold(t *testing.T) {
	ctx, raw, _ := fixture(t)
	key := agedWaiting(t, ctx, raw, now.Add(-time.Minute).Add(time.Millisecond))
	clock := now
	observed := &ageCapture{Store: raw}
	e := agingEngine(t, observed, &clock)
	if _, err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if len(observed.commands) != 0 {
		t.Fatal("aged before threshold")
	}
	clock = clock.Add(time.Millisecond)
	if _, err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if read(t, ctx, raw, key).Request.EffectiveUrgency != 2 {
		t.Fatal("exact threshold was not aged")
	}
}

func TestAgingDisabledAndPartialScanDoNotPropose(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "partial"}[partial], func(t *testing.T) {
			ctx, raw, _ := fixture(t)
			agedWaiting(t, ctx, raw, now.Add(-time.Hour))
			clock := now
			var source store.Store = raw
			if partial {
				source = memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
					if c.Method == "Scan" {
						return memstore.Fault{FailShards: []int{1}}
					}
					return memstore.Fault{}
				})
			}
			observed := &ageCapture{Store: source}
			cfg := reconcile.Config{Store: observed, Now: func() time.Time { return clock }}
			if partial {
				cfg.AgingInterval = time.Minute
			}
			e, err := reconcile.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.Step(ctx)
			if partial && !errors.Is(err, reconcile.ErrIncompleteScan) {
				t.Fatalf("partial=%v", err)
			}
			if !partial && err != nil {
				t.Fatal(err)
			}
			if len(observed.commands) != 0 {
				t.Fatal("disabled/incomplete scan issued aging command")
			}
		})
	}
}

type ageBarrier struct {
	store.Store
	entered chan store.AgeRequest
	release chan struct{}
}

func (s ageBarrier) AgeRequest(ctx context.Context, c store.AgeRequest) (store.Result, error) {
	select {
	case s.entered <- c:
	case <-ctx.Done():
		return store.Result{}, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return store.Result{}, ctx.Err()
	}
	return s.Store.AgeRequest(ctx, c)
}
func TestAgingCancellationWinsAfterProposal(t *testing.T) {
	ctx, raw, _ := fixture(t)
	key := agedWaiting(t, ctx, raw, now.Add(-time.Minute))
	barrier := ageBarrier{Store: raw, entered: make(chan store.AgeRequest, 1), release: make(chan struct{})}
	m, err := workflow.New(workflow.Config{Store: barrier, ScanLimit: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		record store.Record
		err    error
	}
	done := make(chan answer, 1)
	go func() { record, err := m.Age(ctx, id("aging-request"), time.Minute); done <- answer{record, err} }()
	var cmd store.AgeRequest
	select {
	case cmd = <-barrier.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err := raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: id("cancel-during-age"), At: now.UnixMilli()}, Key: key, ExpectedVersion: cmd.ExpectedVersion, ExpectedStatus: store.RequestWaiting, To: store.RequestCancelled})
	must(t, result, err)
	close(barrier.release)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.record.Request == nil || got.record.Request.Status != store.RequestCancelled || got.record.Version != 3 {
			t.Fatalf("Age must return authoritative CANCELLED/v3 after VERSION_CONFLICT, got %+v", got.record)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	current := read(t, ctx, raw, key)
	if current.Request.Status != store.RequestCancelled || current.Request.EffectiveUrgency != 0 {
		t.Fatal("aging resurrected or reprioritized cancellation")
	}
	for shard := 0; shard < raw.Shards(); shard++ {
		rows, err := raw.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil || len(rows) != 0 {
			t.Fatal("rejected aging emitted wake", err)
		}
	}
}

type historicalAgeResult struct {
	store.Store
	t      *testing.T
	replay store.Result
}

func (s *historicalAgeResult) AgeRequest(ctx context.Context, c store.AgeRequest) (store.Result, error) {
	first, err := s.Store.AgeRequest(ctx, c)
	must(s.t, first, err)
	cancelled, err := s.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: id("cancel-after-historical-age"), At: c.At + 1}, Key: c.Key, ExpectedVersion: first.Version, ExpectedStatus: store.RequestWaiting, To: store.RequestCancelled})
	must(s.t, cancelled, err)
	// A lost successful reply can be retried after cancellation has committed.
	s.replay, err = s.Store.AgeRequest(ctx, c)
	return s.replay, err
}
func TestAgingHistoricalOKReturnsCurrentCancelledVersion(t *testing.T) {
	ctx, raw, _ := fixture(t)
	key := agedWaiting(t, ctx, raw, now.Add(-time.Minute))
	wrapped := &historicalAgeResult{Store: raw, t: t}
	m, err := workflow.New(workflow.Config{Store: wrapped, ScanLimit: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.Age(ctx, id("aging-request"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.replay.Outcome != store.OutcomeOK || !wrapped.replay.Replayed || wrapped.replay.Version != 3 {
		t.Fatalf("historical OK/v3 not exercised: %+v", wrapped.replay)
	}
	if got.Key != key || got.Request == nil || got.Request.Status != store.RequestCancelled || got.Version != 4 {
		t.Fatalf("Age must return authoritative CANCELLED/v4 after historical OK/v3, got %+v", got)
	}
	current := read(t, ctx, raw, key)
	if current.Version != got.Version || current.Request.Status != got.Request.Status {
		t.Fatal("returned record differs from current committed state")
	}
}
