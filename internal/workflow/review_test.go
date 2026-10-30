package workflow

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestMatchLaterPageFailureLeavesPendingWithoutHandoff(t *testing.T) {
	ctx, raw := testStore(t, memstore.WithRouter(func(string) int { return 1 }))
	createRequest(t, ctx, raw, request("R"))
	first := resource("a-future", .01)
	first.AvailableFrom = testNow.Add(time.Minute).UnixMilli()
	createResource(t, ctx, raw, first)
	createResource(t, ctx, raw, resource("z-eligible", .02))
	pages := 0
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			pages++
			return memstore.Fault{DropBefore: pages >= 2}
		}
		return memstore.Fault{}
	})
	cfg := testConfig(s, 1)
	cfg.ScanLimit = 1
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Match(ctx, "R")
	if pages < 2 || !errors.Is(err, ErrIncompleteScan) || result.Outcome == "NO_CANDIDATE" {
		t.Fatalf("later-page failure = %+v, %v, pages=%d", result, err, pages)
	}
	r := read(t, ctx, raw, store.RequestKey("R"))
	if r.Version != 1 || r.Request.Status != store.RequestPending || r.Request.CurrentAttempt != nil {
		t.Fatalf("incomplete walk changed request: %+v", r)
	}
	for shard := range raw.Shards() {
		events, err := raw.PendingOutbox(ctx, shard, 100)
		if err != nil || len(events) != 0 {
			t.Fatalf("incomplete walk persisted handoff: %v, %v", events, err)
		}
	}
}

func TestMatchClaimCommandSurvivesRetryWorkflowUpdatedAtChange(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	drop := true
	s := &traceStore{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: drop && c.Method == "TryClaim"}
	})}
	cfg := testConfig(s, 1)
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("fault = %v; want unknown", err)
	}
	if len(s.claims) == 0 {
		t.Fatal("no claim to resume")
	}
	original := s.claims[0]
	r := read(t, ctx, raw, store.RequestKey("R"))
	result, err := raw.RetryWorkflow(ctx, store.RetryWorkflow{
		Meta: store.Meta{OperationID: "schedule-recovery", At: testNow.Add(time.Second).UnixMilli()},
		Key:  r.Key, ExpectedVersion: r.Version, NextAttemptAt: testNow.Add(2 * time.Second).UnixMilli(),
		Outbox: []store.OutboxEvent{{EventID: "recovery-event", EventType: "request.retry", SchemaVersion: 1}},
	})
	mustOutcome(t, result, err, store.OutcomeOK)
	updated := read(t, ctx, raw, r.Key)
	if updated.UpdatedAt == r.UpdatedAt {
		t.Fatal("fixture did not change UpdatedAt")
	}
	drop = false
	cfg.Now = func() time.Time { return testNow.Add(3 * time.Second) }
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, raw, "R")
	for _, command := range s.claims {
		if !reflect.DeepEqual(command, original) {
			t.Errorf("RetryWorkflow changed unresolved claim: %+v; want %+v", command, original)
		}
	}
}

func TestMatchConcurrentIntentCASLoserNeverClaims(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	arrived, release := make(chan struct{}, 2), make(chan struct{})
	var transitions, claims atomic.Int32
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "TryClaim" {
			claims.Add(1)
		}
		if c.Method == "TransitionRequest" && transitions.Add(1) <= 2 {
			return memstore.Fault{Before: func(ctx context.Context) error {
				arrived <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
		}
		return memstore.Fault{}
	})
	cfg := testConfig(s, 1)
	cfg.NewID = uuid.NewString
	var workers sync.WaitGroup
	errorsCh := make(chan error, 2)
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		workers.Wait()
	}()
	for range 2 {
		m, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := m.Match(ctx, "R")
			errorsCh <- err
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("both workers did not reach their intent CAS", ctx.Err())
		}
	}
	releaseOnce.Do(func() { close(release) })
	workers.Wait()
	for range 2 {
		if err := <-errorsCh; err != nil && !errors.Is(err, ErrConcurrentChange) {
			t.Fatal(err)
		}
	}
	matched(t, ctx, raw, "R")
	if got := claims.Load(); got != 1 {
		t.Errorf("claim calls = %d; CAS loser must not issue a resource claim", got)
	}
}
