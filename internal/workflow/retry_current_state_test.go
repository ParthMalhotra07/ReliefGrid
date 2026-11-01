package workflow

import (
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestRetrySCH08CurrentRetryingRejectsConfirmAndReachesFixtureFinalState(t *testing.T) {
	fixture := schedule(t, "SCH08")
	ctx, raw := testStore(t)
	initial, oldClaim := retryIntentFixture(t, ctx, raw, true)
	const nextResource = "660e8400-e29b-41d4-a716-446655440001"
	const nextAttempt = "770e8400-e29b-41d4-a716-446655440001"
	createResource(t, ctx, raw, resource(nextResource, .02))
	result, err := raw.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "sch08-enter-retrying", At: testNow.UnixMilli()},
		Key:  initial.Key, ExpectedVersion: initial.Version, ExpectedStatus: store.RequestMatching,
		ExpectedAttemptID: cleanupAttempt, To: store.RequestRetrying,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
	retrying := read(t, ctx, raw, initial.Key)
	// This command has the current version, state and attempt. Only the state
	// machine's transition rule can reject it; stale-v2 CAS coverage cannot.
	result, err = raw.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "sch08-current-retrying-confirm", At: testNow.UnixMilli()},
		Key:  retrying.Key, ExpectedVersion: retrying.Version, ExpectedStatus: store.RequestRetrying,
		ExpectedAttemptID: cleanupAttempt, To: store.RequestMatched,
	})
	mustOutcome(t, result, err, store.OutcomeDomainConflict)
	if after := read(t, ctx, raw, initial.Key); !reflect.DeepEqual(after, retrying) {
		t.Fatalf("rejected confirmation changed current RETRYING intent: before=%+v after=%+v", retrying, after)
	}
	m, err := New(testConfig(raw, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Abandon(ctx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	waiting := read(t, ctx, raw, initial.Key)
	if waiting.Request.Status != store.RequestWaiting || waiting.Version != 4 || waiting.Request.CurrentAttempt != nil {
		t.Fatalf("SCH08 closure did not reach WAITING v4: %+v", waiting)
	}
	closure, err := raw.ReadClosure(ctx, oldClaim.ResourceKey, cleanupAttempt)
	if err != nil || closure.RequestID != cleanupRequest {
		t.Fatalf("exact old-attempt closure: %+v %v", closure, err)
	}
	attempt := store.Attempt{AttemptID: nextAttempt, ResourceID: nextResource, Fingerprint: "sch08-next-fingerprint"}
	result, err = raw.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "sch08-next-intent", At: testNow.UnixMilli()},
		Key:  waiting.Key, ExpectedVersion: waiting.Version, ExpectedStatus: store.RequestWaiting,
		To: store.RequestMatching, Attempt: &attempt,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
	result, err = raw.TryClaim(ctx, store.TryClaim{
		Meta:        store.Meta{OperationID: "sch08-next-claim", At: testNow.UnixMilli()},
		ResourceKey: store.ResourceKey(nextResource), RequestID: cleanupRequest, Attempt: attempt, Type: store.TypeWater,
	})
	mustOutcome(t, result, err, store.ClaimSuccess)
	result, err = raw.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "sch08-next-confirm", At: testNow.UnixMilli()},
		Key:  waiting.Key, ExpectedVersion: 5, ExpectedStatus: store.RequestMatching,
		ExpectedAttemptID: nextAttempt, To: store.RequestMatched,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
	oldClaim.OperationID = "sch08-late-closed-claim"
	result, err = raw.TryClaim(ctx, oldClaim)
	mustOutcome(t, result, err, store.ClaimClosed)
	records := map[string]store.Record{
		"R":    read(t, ctx, raw, initial.Key),
		"Res":  read(t, ctx, raw, oldClaim.ResourceKey),
		"Res2": read(t, ctx, raw, store.ResourceKey(nextResource)),
	}
	for _, want := range fixture.FinalState {
		got := records[want.Entity]
		if got.Version != uint64(want.Version) {
			t.Errorf("%s version=%d want fixture %d", want.Entity, got.Version, want.Version)
		}
		switch want.Entity {
		case "R":
			if got.Request.Status != want.ReqStatus || got.Request.CurrentAttempt == nil || got.Request.CurrentAttempt.AttemptID != nextAttempt || got.Request.AssignedResourceID != nextResource {
				t.Errorf("SCH08 request final state: %+v; fixture=%+v", got, want)
			}
		case "Res":
			if got.Resource.Status != want.ResStatus || got.Resource.ActiveClaim != nil {
				t.Errorf("SCH08 old resource final state: %+v; fixture=%+v", got, want)
			}
		case "Res2":
			if got.Resource.Status != want.ResStatus || got.Resource.ActiveClaim == nil || got.Resource.ActiveClaim.RequestID != cleanupRequest || got.Resource.ActiveClaim.AttemptID != nextAttempt {
				t.Errorf("SCH08 new resource final state: %+v; fixture=%+v", got, want)
			}
		}
	}
}
