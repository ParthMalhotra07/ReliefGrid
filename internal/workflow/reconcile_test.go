package workflow

import (
	"context"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"reflect"
	"testing"
	"time"
)

func TestRecoverOldMatchingClosesUnknownClaimBeforeClearingIntent(t *testing.T) {
	ctx, s := testStore(t)
	trace := &traceStore{Store: s}
	createResource(t, ctx, s, resource("r1", 0))
	createRequest(t, ctx, s, request("old"))
	trace.afterClaim = func(_ context.Context, _ store.TryClaim, _ store.Result, _ error) (store.Result, error) {
		return store.Result{}, store.ErrUnknownOutcome
	}
	clock := testNow
	cfg := testConfig(trace, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Match(ctx, "old"); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("unknown claim=%v", err)
	}
	old := read(t, ctx, s, store.RequestKey("old"))
	a := *old.Request.CurrentAttempt
	clock = testNow.Add(70 * time.Second)
	if _, err = m.ScheduleRetry(ctx, "old", store.ErrUnknownOutcome); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Second)
	if err = m.Recover(ctx, "old", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	got := read(t, ctx, s, old.Key)
	if got.Request.Status != store.RequestWaiting || got.Request.CurrentAttempt != nil {
		t.Fatalf("old attempt not fenced/closed: %+v", got.Request)
	}
	if _, err := s.ReadClosure(ctx, store.ResourceKey("r1"), a.AttemptID); err != nil {
		t.Fatal("intent cleared without durable closure", err)
	}
	late, err := s.TryClaim(ctx, store.TryClaim{Meta: store.Meta{OperationID: "late-after-recovery", At: testNow.UnixMilli()}, ResourceKey: store.ResourceKey("r1"), RequestID: "old", Attempt: a, Type: store.TypeWater})
	mustOutcome(t, late, err, store.ClaimClosed)
	if r := read(t, ctx, s, store.ResourceKey("r1")); r.Resource.ActiveClaim != nil {
		t.Fatal("old claim survived recovery")
	}
}

func TestRecoverCannotAbandonANewerIntentAfterConcurrentRetarget(t *testing.T) {
	ctx, s := testStore(t)
	trace := &traceStore{Store: s}
	createResource(t, ctx, s, resource("r1", 0))
	createResource(t, ctx, s, resource("r2", .01))
	createRequest(t, ctx, s, request("race"))
	clock := testNow
	cfg := testConfig(trace, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	trace.afterClaim = func(_ context.Context, _ store.TryClaim, _ store.Result, _ error) (store.Result, error) {
		return store.Result{}, store.ErrUnknownOutcome
	}
	if _, err := m.Match(ctx, "race"); err == nil {
		t.Fatal("missing fault")
	}
	old := read(t, ctx, s, store.RequestKey("race"))
	a := *old.Request.CurrentAttempt
	clock = clock.Add(time.Minute)
	replacement := store.Attempt{AttemptID: "newer", ResourceID: "r2", Fingerprint: "newer-immutable-inputs"}
	trace.beforeTransition = func(ctx context.Context, cmd store.TransitionRequest) {
		if cmd.To != store.RequestRetrying {
			return
		}
		trace.beforeTransition = nil
		result, err := s.CloseAttempt(ctx, store.CloseAttempt{Meta: store.Meta{OperationID: "other-close", At: clock.UnixMilli()}, ResourceKey: store.ResourceKey(a.ResourceID), RequestID: "race", Attempt: a, Disposition: store.ResourceAvailable, Reason: "retarget"})
		mustOutcome(t, result, err, store.OutcomeOK)
		result, err = s.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "other-intent", At: clock.UnixMilli()}, Key: old.Key, ExpectedVersion: old.Version, ExpectedStatus: store.RequestMatching, ExpectedAttemptID: a.AttemptID, To: store.RequestMatching, Attempt: &replacement})
		mustOutcome(t, result, err, store.OutcomeOK)
	}
	if err := m.Recover(ctx, "race", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	got := read(t, ctx, s, old.Key)
	if got.Version != old.Version+1 || got.Request.NextAttemptAt != 0 || got.Request.Status != store.RequestMatching || got.Request.CurrentAttempt == nil || *got.Request.CurrentAttempt != replacement {
		t.Fatalf("newer intent was abandoned: %+v", got.Request)
	}
	if _, err := s.ReadClosure(ctx, store.ResourceKey("r2"), replacement.AttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("newer attempt closed: %v", err)
	}
}

func TestResourceWakeShortensBackoffWithOneDurableHandoff(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("waiting"))
	clock := testNow
	cfg := testConfig(s, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "waiting"); err != nil {
		t.Fatal(err)
	}
	before := read(t, ctx, s, store.RequestKey("waiting"))
	clock = clock.Add(time.Second)
	if err := m.Wake(ctx, "waiting", "registration", clock.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	after := read(t, ctx, s, before.Key)
	if after.Request.Status != store.RequestWaiting || after.Request.CurrentAttempt != nil || after.Version != before.Version+1 || after.Request.NextAttemptAt != clock.UnixMilli()+1 || after.Request.NextAttemptAt >= before.Request.NextAttemptAt {
		t.Fatalf("resource wake did not shorten durable schedule: before=%+v after=%+v", before.Request, after.Request)
	}
	if after.Request.LastError != "RESOURCE_AVAILABLE" {
		t.Fatalf("wake reason=%s", after.Request.LastError)
	}
}

type lostWake struct {
	store.Store
	lose     bool
	commands []store.RetryWorkflow
}

func (s *lostWake) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	s.commands = append(s.commands, cmd)
	r, err := s.Store.RetryWorkflow(ctx, cmd)
	if err == nil && s.lose {
		return store.Result{}, store.ErrUnknownOutcome
	}
	return r, err
}
func TestResourceWakeUnknownResultRetriesIdenticalCommand(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("lost-wake"))
	wrapped := &lostWake{Store: s}
	clock := testNow
	cfg := testConfig(wrapped, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "lost-wake"); err != nil {
		t.Fatal(err)
	}
	wrapped.lose = true
	clock = clock.Add(time.Second)
	if err := m.Wake(ctx, "lost-wake", "registration", clock.UnixMilli()); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("wake unknown=%v", err)
	}
	if len(wrapped.commands) != 2 || !reflect.DeepEqual(wrapped.commands[0], wrapped.commands[1]) {
		t.Fatalf("uncertain wake changed its command: %+v", wrapped.commands)
	}
	if rec := read(t, ctx, s, store.RequestKey("lost-wake")); rec.Request.Status != store.RequestWaiting || rec.Request.CurrentAttempt != nil {
		t.Fatal("unknown wake damaged state")
	}
}

func TestResourceWakeFutureHintCannotPostponeAuthoritativeWork(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("future-hint"))
	clock := testNow
	cfg := testConfig(s, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "future-hint"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Second)
	if err := m.Wake(ctx, "future-hint", "future-event", clock.Add(365*24*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	after := read(t, ctx, s, store.RequestKey("future-hint"))
	if after.Request.NextAttemptAt > clock.UnixMilli()+1 {
		t.Fatalf("untrusted event time postponed work: next=%d now=%d", after.Request.NextAttemptAt, clock.UnixMilli())
	}
}

func TestConcurrentHelperDoesNotInvalidateWinningIntentWithRetryMetadata(t *testing.T) {
	ctx, s := testStore(t)
	createResource(t, ctx, s, resource("winner-resource", 0))
	createRequest(t, ctx, s, request("winner-request"))
	trace := &traceStore{Store: s}
	winner, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	loser, err := New(testConfig(trace, 2))
	if err != nil {
		t.Fatal(err)
	}
	var winning store.Record
	trace.beforeTransition = func(ctx context.Context, cmd store.TransitionRequest) {
		if cmd.To != store.RequestMatching {
			return
		}
		trace.beforeTransition = nil
		rec := read(t, ctx, s, cmd.Key)
		var done *Result
		var err error
		winning, done, err = winner.propose(ctx, rec)
		if err != nil || done != nil {
			t.Fatalf("winning intent failed: %+v %v", done, err)
		}
	}
	if err := loser.Handle(ctx, "winner-request"); err != nil {
		t.Fatal(err)
	}
	current := read(t, ctx, s, store.RequestKey("winner-request"))
	if winning.Request == nil || current.Version != winning.Version || current.Request.NextAttemptAt != 0 {
		t.Fatalf("losing helper invalidated winner with retry metadata: winning v%d current %+v", winning.Version, current)
	}
	_, done, err := winner.resolve(ctx, winning)
	if err != nil || done == nil || done.Outcome != "MATCHED" {
		t.Fatalf("winning confirmation stalled: %+v %v", done, err)
	}
}

func TestConcurrentMatchingHelperPreservesCommittedCleanupHandoff(t *testing.T) {
	ctx, s := testStore(t)
	createResource(t, ctx, s, resource("cancel-race-resource", 0))
	createRequest(t, ctx, s, request("cancel-race"))
	trace := &traceStore{Store: s}
	m, err := New(testConfig(trace, 0))
	if err != nil {
		t.Fatal(err)
	}
	var committed store.Record
	trace.afterClaim = func(ctx context.Context, _ store.TryClaim, r store.Result, err error) (store.Result, error) {
		trace.afterClaim = nil
		if err != nil {
			return r, err
		}
		rec := read(t, ctx, s, store.RequestKey("cancel-race"))
		cmd := transition(rec, store.RequestCancelling, testNow.UnixMilli())
		cmd.OperationID = "concurrent-cancellation"
		cmd.Disposition = store.ResourceAvailable
		cmd.Outbox, err = cleanupEvent(rec, cmd)
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.TransitionRequest(ctx, cmd)
		mustOutcome(t, result, err, store.OutcomeOK)
		committed = read(t, ctx, s, rec.Key)
		return r, nil
	}
	if err := m.Handle(ctx, "cancel-race"); err != nil {
		t.Fatal(err)
	}
	current := read(t, ctx, s, committed.Key)
	if current.Version != committed.Version || current.Request.NextAttemptAt != 0 {
		t.Fatalf("obsolete matcher postponed cleanup: committed v%d current %+v", committed.Version, current)
	}
	if _, err := m.Cleanup(ctx, "cancel-race"); err != nil {
		t.Fatal(err)
	}
	if rec := read(t, ctx, s, committed.Key); rec.Request.Status != store.RequestCancelled {
		t.Fatal("committed cleanup did not converge")
	}
}

func TestConcurrentHelperPreservesImmediateDurableWakeSchedule(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("wake-race"))
	clock := testNow
	cfg := testConfig(s, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "wake-race"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(6 * time.Second)
	createResource(t, ctx, s, resource("wake-race-resource", 0))
	trace := &traceStore{Store: s}
	cfg.Store = trace
	loser, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled store.Record
	trace.beforeTransition = func(ctx context.Context, cmd store.TransitionRequest) {
		if cmd.To != store.RequestMatching {
			return
		}
		trace.beforeTransition = nil
		rec := read(t, ctx, s, cmd.Key)
		wake := store.RetryWorkflow{Meta: store.Meta{OperationID: "concurrent-durable-wake", At: clock.UnixMilli()}, Key: rec.Key, ExpectedVersion: rec.Version, NextAttemptAt: clock.UnixMilli() + 1, LastError: "RESOURCE_AVAILABLE"}
		wake.Outbox, err = events(rec, store.TransitionRequest{Meta: wake.Meta, NextAttemptAt: wake.NextAttemptAt}, "request.retry", nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := s.RetryWorkflow(ctx, wake)
		mustOutcome(t, res, err, store.OutcomeOK)
		scheduled = read(t, ctx, s, rec.Key)
		clock = clock.Add(2 * time.Millisecond)
	}
	if err := loser.Handle(ctx, "wake-race"); err != nil {
		t.Fatal(err)
	}
	current := read(t, ctx, s, scheduled.Key)
	if current.Version != scheduled.Version || current.Request.NextAttemptAt != scheduled.Request.NextAttemptAt {
		t.Fatal("losing helper postponed committed immediate wake")
	}
	if err := m.Handle(ctx, "wake-race"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, s, "wake-race")
}
