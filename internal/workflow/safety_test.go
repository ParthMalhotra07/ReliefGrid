package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestMatchUnknownClaimRetainsIntentAndRetriesIdenticalCommandAfterRestart(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	drop := true
	s := &traceStore{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: drop && c.Method == "TryClaim"}
	})}
	clock := testNow
	cfg := testConfig(s, 1)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("dropped claim error = %v; want ErrUnknownOutcome", err)
	}
	r := read(t, ctx, raw, store.RequestKey("R"))
	if r.Request.Status != store.RequestMatching || r.Request.CurrentAttempt == nil {
		t.Fatalf("unknown claim cleared intent: %+v", r.Request)
	}
	intent := *r.Request.CurrentAttempt
	if len(s.claims) == 0 {
		t.Fatal("claim fault was not exercised")
	}
	original := s.claims[0]
	if original.OperationID == "" || original.At == 0 {
		t.Fatalf("claim lacks immutable command metadata: %+v", original)
	}
	// A new matcher instance has no in-process operation cache. The stored intent
	// must still supply the exact command even though the proposer's clock moves.
	drop = false
	clock = testNow.Add(time.Second)
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, raw, "R")
	if len(s.claims) < 2 {
		t.Fatal("unapplied claim was never retried")
	}
	for _, cmd := range s.claims {
		if !reflect.DeepEqual(cmd, original) {
			t.Errorf("unknown-outcome retry changed command:\noriginal=%+v\nretry=%+v", original, cmd)
		}
	}
	if got := read(t, ctx, raw, store.RequestKey("R")).Request.CurrentAttempt; got == nil || *got != intent {
		t.Fatalf("resumption changed immutable intent: %+v; want %+v", got, intent)
	}
}

func TestMatchDroppedIntentCASResponseUsesCommittedIntent(t *testing.T) {
	f := schedule(t, "SCH02")
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	assertStates(t, ctx, raw, f.InitialState, "")
	dropped := false
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "TransitionRequest" && !dropped {
			dropped = true
			return memstore.Fault{DropResponse: true}
		}
		return memstore.Fault{}
	})
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Match(ctx, "R")
	if err != nil && !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatal(err)
	}
	if err != nil {
		intermediate := read(t, ctx, raw, store.RequestKey("R"))
		if intermediate.Request.Status != store.RequestMatching || intermediate.Request.CurrentAttempt == nil {
			t.Fatalf("lost CAS response discarded committed intent: %+v", intermediate.Request)
		}
		if _, err := m.Match(ctx, "R"); err != nil {
			t.Fatal(err)
		}
	}
	if !dropped {
		t.Fatal("intent CAS response was not dropped")
	}
	matched(t, ctx, raw, "R")
	a := read(t, ctx, raw, store.RequestKey("R")).Request.CurrentAttempt
	assertStates(t, ctx, raw, f.FinalState, a.AttemptID)
}

func TestMatchRetargetRequiresClosureForExactOldAttempt(t *testing.T) {
	for _, rejection := range []string{"held", "offline", "closed"} {
		t.Run(rejection, func(t *testing.T) {
			ctx, raw := testStore(t)
			createRequest(t, ctx, raw, request("R"))
			createResource(t, ctx, raw, resource("a-stale", .01))
			createResource(t, ctx, raw, resource("z-live", .02))
			a := store.Attempt{AttemptID: "old-attempt", ResourceID: "a-stale", Fingerprint: "old-fingerprint"}
			result, err := raw.TransitionRequest(ctx, store.TransitionRequest{
				Meta: store.Meta{OperationID: "record-old-intent", At: testNow.UnixMilli()},
				Key:  store.RequestKey("R"), ExpectedVersion: 1, ExpectedStatus: store.RequestPending,
				To: store.RequestMatching, Attempt: &a,
			})
			mustOutcome(t, result, err, store.OutcomeOK)
			switch rejection {
			case "held":
				result, err = raw.TryClaim(ctx, store.TryClaim{
					Meta:        store.Meta{OperationID: "other-claim", At: testNow.UnixMilli()},
					ResourceKey: store.ResourceKey(a.ResourceID), RequestID: "other-request",
					Attempt: store.Attempt{AttemptID: "other-attempt", ResourceID: a.ResourceID, Fingerprint: "other-fingerprint"},
					Type:    store.TypeWater,
				})
				mustOutcome(t, result, err, store.ClaimSuccess)
			case "offline":
				result, err = raw.TransitionResource(ctx, store.TransitionResource{
					Meta: store.Meta{OperationID: "take-offline", At: testNow.UnixMilli()},
					Key:  store.ResourceKey(a.ResourceID), ExpectedVersion: 1, Status: store.ResourceOffline,
				})
				mustOutcome(t, result, err, store.OutcomeOK)
			case "closed":
				result, err = raw.CloseAttempt(ctx, store.CloseAttempt{
					Meta:        store.Meta{OperationID: "already-closed", At: testNow.UnixMilli()},
					ResourceKey: store.ResourceKey(a.ResourceID), RequestID: "R", Attempt: a,
					Disposition: store.ResourceOffline, Reason: "eligibility revoked",
				})
				mustOutcome(t, result, err, store.OutcomeOK)
				// A fence with no claim leaves the resource version/status alone.
				// Revoke eligibility separately so a fresh attempt chooses z-live.
				result, err = raw.TransitionResource(ctx, store.TransitionResource{
					Meta: store.Meta{OperationID: "revoke-closed-resource", At: testNow.UnixMilli()},
					Key:  store.ResourceKey(a.ResourceID), ExpectedVersion: 1, Status: store.ResourceOffline,
				})
				mustOutcome(t, result, err, store.OutcomeOK)
			}
			s := &traceStore{Store: raw}
			retargeted := false
			s.beforeTransition = func(ctx context.Context, cmd store.TransitionRequest) {
				if cmd.To != store.RequestMatching || cmd.Attempt == nil || cmd.Attempt.AttemptID == a.AttemptID {
					return
				}
				closure, err := raw.ReadClosure(ctx, store.ResourceKey(a.ResourceID), a.AttemptID)
				if err != nil || closure.RequestID != "R" || closure.Fingerprint != a.Fingerprint {
					t.Fatalf("retarget without exact old closure: %+v, %v", closure, err)
				}
				retargeted = true
			}
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Match(ctx, "R"); err != nil {
				t.Fatal(err)
			}
			if got := matched(t, ctx, raw, "R"); got != "z-live" || !retargeted {
				t.Fatalf("retarget = %s, observed=%v", got, retargeted)
			}
			if rejection == "held" {
				old := read(t, ctx, raw, store.ResourceKey(a.ResourceID)).Resource.ActiveClaim
				if old == nil || old.RequestID != "other-request" {
					t.Fatalf("retarget released somebody else's claim: %+v", old)
				}
			}
		})
	}
}

func TestMatchHeldObservationWithUnknownClaimCannotRetarget(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("a-stale", .01))
	createResource(t, ctx, raw, resource("z-live", .02))
	a := store.Attempt{AttemptID: "old-attempt", ResourceID: "a-stale", Fingerprint: "old-fingerprint"}
	result, err := raw.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "intent", At: testNow.UnixMilli()},
		Key:  store.RequestKey("R"), ExpectedVersion: 1, ExpectedStatus: store.RequestPending,
		To: store.RequestMatching, Attempt: &a,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
	result, err = raw.TryClaim(ctx, store.TryClaim{
		Meta: store.Meta{OperationID: "other-claim", At: testNow.UnixMilli()}, ResourceKey: store.ResourceKey(a.ResourceID),
		RequestID: "other", Attempt: store.Attempt{AttemptID: "other", ResourceID: a.ResourceID, Fingerprint: "other"}, Type: store.TypeWater,
	})
	mustOutcome(t, result, err, store.ClaimSuccess)
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: c.Method == "TryClaim" || c.Method == "CloseAttempt"}
	})
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("unresolved held attempt = %v; want unknown", err)
	}
	r := read(t, ctx, raw, store.RequestKey("R"))
	if r.Request.CurrentAttempt == nil || *r.Request.CurrentAttempt != a ||
		(r.Request.Status != store.RequestMatching && r.Request.Status != store.RequestRetrying) {
		t.Fatalf("held observation discarded intent without closure: %+v", r.Request)
	}
	if _, err := raw.ReadClosure(ctx, store.ResourceKey(a.ResourceID), a.AttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("drop-before unexpectedly closed attempt: %v", err)
	}
	if live := read(t, ctx, raw, store.ResourceKey("z-live")); live.Resource.ActiveClaim != nil {
		t.Fatal("new candidate claimed before old attempt closed")
	}
}

func TestMatchDelayedClaimAfterClosureIsClosed(t *testing.T) {
	f := schedule(t, "SCH07")
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	s := &traceStore{Store: raw}
	s.beforeClaim = func(ctx context.Context, cmd store.TryClaim) {
		assertStates(t, ctx, raw, f.InitialState, cmd.Attempt.AttemptID)
		result, err := raw.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "fence-confirmation", At: testNow.UnixMilli()},
			Key:  store.RequestKey("R"), ExpectedVersion: 2, ExpectedStatus: store.RequestMatching,
			ExpectedAttemptID: cmd.Attempt.AttemptID, To: store.RequestRetrying,
		})
		mustOutcome(t, result, err, store.OutcomeOK)
		result, err = raw.CloseAttempt(ctx, store.CloseAttempt{
			Meta:        store.Meta{OperationID: "close-before-arrival", At: testNow.UnixMilli()},
			ResourceKey: cmd.ResourceKey, RequestID: cmd.RequestID, Attempt: cmd.Attempt,
			Disposition: store.ResourceAvailable, Reason: "recovery",
		})
		mustOutcome(t, result, err, store.OutcomeOK)
	}
	s.afterClaim = func(ctx context.Context, cmd store.TryClaim, result store.Result, err error) (store.Result, error) {
		mustOutcome(t, result, err, store.ClaimClosed)
		result2, err2 := raw.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "durable-wait", At: testNow.UnixMilli()},
			Key:  store.RequestKey("R"), ExpectedVersion: 3, ExpectedStatus: store.RequestRetrying,
			ExpectedAttemptID: cmd.Attempt.AttemptID, To: store.RequestWaiting,
			NextAttemptAt: testNow.Add(time.Minute).UnixMilli(),
			Outbox:        []store.OutboxEvent{{EventID: "retry-event", EventType: "request.retry", SchemaVersion: 1}},
		})
		mustOutcome(t, result2, err2, store.OutcomeOK)
		return result, err
	}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	if len(s.claims) != 1 {
		t.Fatalf("old worker issued %d claims; want one delayed closed claim", len(s.claims))
	}
	assertStates(t, ctx, raw, f.FinalState, "")
}

func TestMatchHistoricalSuccessAfterCancellationCannotConfirm(t *testing.T) {
	f := schedule(t, "SCH09")
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	s := &traceStore{Store: raw}
	s.afterClaim = func(ctx context.Context, cmd store.TryClaim, result store.Result, err error) (store.Result, error) {
		mustOutcome(t, result, err, store.ClaimSuccess)
		assertStates(t, ctx, raw, f.InitialState, cmd.Attempt.AttemptID)
		result, err = raw.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "cancel-start", At: testNow.UnixMilli()},
			Key:  store.RequestKey("R"), ExpectedVersion: 2, ExpectedStatus: store.RequestMatching,
			ExpectedAttemptID: cmd.Attempt.AttemptID, To: store.RequestCancelling, Disposition: store.ResourceAvailable,
		})
		mustOutcome(t, result, err, store.OutcomeOK)
		result, err = raw.CloseAttempt(ctx, store.CloseAttempt{
			Meta:        store.Meta{OperationID: "cancel-close", At: testNow.UnixMilli()},
			ResourceKey: cmd.ResourceKey, RequestID: cmd.RequestID, Attempt: cmd.Attempt,
			Disposition: store.ResourceAvailable, Reason: "cancelled",
		})
		mustOutcome(t, result, err, store.OutcomeOK)
		result, err = raw.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "cancel-finish", At: testNow.UnixMilli()},
			Key:  store.RequestKey("R"), ExpectedVersion: 3, ExpectedStatus: store.RequestCancelling,
			ExpectedAttemptID: cmd.Attempt.AttemptID, To: store.RequestCancelled,
		})
		mustOutcome(t, result, err, store.OutcomeOK)
		// Deliver an actual historical result from the FSM, never a fabricated SUCCESS.
		replay, replayErr := raw.TryClaim(ctx, cmd)
		mustOutcome(t, replay, replayErr, store.ClaimSuccess)
		if !replay.Replayed {
			t.Fatal("fault did not deliver historical SUCCESS")
		}
		return replay, replayErr
	}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	assertStates(t, ctx, raw, f.FinalState, "")
	if len(s.claims) != 1 {
		t.Errorf("historical SUCCESS caused new claim calls: %d", len(s.claims))
	}
	for _, cmd := range s.transitions {
		if cmd.To == store.RequestMatched {
			t.Error("historical SUCCESS was used to attempt confirmation after cancellation")
		}
	}
}
