package workflow

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestRetryTransitionsUsePersistedTimeAcrossRestart(t *testing.T) {
	for _, status := range []store.RequestStatus{store.RequestMatching, store.RequestRetrying} {
		t.Run(string(status), func(t *testing.T) {
			ctx, raw := testStore(t)
			createResource(t, ctx, raw, resource("r1", 0))
			createRequest(t, ctx, raw, request("persisted-retry"))
			a := store.Attempt{AttemptID: "persisted-attempt", ResourceID: "r1", Fingerprint: "frozen"}
			r, err := raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "persisted-intent", At: testNow.UnixMilli()}, Key: store.RequestKey("persisted-retry"), ExpectedVersion: 1, ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a})
			mustOutcome(t, r, err, store.OutcomeOK)
			if status == store.RequestRetrying {
				r, err = raw.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "persisted-fence", At: testNow.Add(time.Second).UnixMilli()}, Key: store.RequestKey("persisted-retry"), ExpectedVersion: 2, ExpectedStatus: store.RequestMatching, ExpectedAttemptID: a.AttemptID, To: store.RequestRetrying})
				mustOutcome(t, r, err, store.OutcomeOK)
			}
			before := read(t, ctx, raw, store.RequestKey("persisted-retry"))
			drop := true
			trace := &cleanupTrace{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				return memstore.Fault{DropBefore: drop && c.Method == "TransitionRequest"}
			})}
			cfg := testConfig(trace, 0)
			cfg.Now = func() time.Time { return testNow.Add(time.Hour) }
			m, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Abandon(ctx, "persisted-retry"); !errors.Is(err, store.ErrUnknownOutcome) {
				t.Fatal("missing pre-apply unknown:", err)
			}
			cfg.Now = func() time.Time { return testNow.Add(2 * time.Hour) }
			restarted, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			drop = false
			if _, err = restarted.Abandon(ctx, "persisted-retry"); err != nil {
				t.Fatal(err)
			}
			target := store.RequestRetrying
			if status == store.RequestRetrying {
				target = store.RequestWaiting
			}
			var first *store.TransitionRequest
			count := 0
			for _, cmd := range trace.finals {
				if cmd.To != target {
					continue
				}
				count++
				if cmd.At != before.UpdatedAt {
					t.Fatalf("%s transition At=%d want persisted=%d", status, cmd.At, before.UpdatedAt)
				}
				if first == nil {
					copy := cmd
					first = &copy
				} else if !reflect.DeepEqual(*first, cmd) {
					t.Fatalf("restart changed operation/command: first=%+v later=%+v", *first, cmd)
				}
			}
			if count < 2 {
				t.Fatal("did not observe transition before and after restart")
			}
			after := read(t, ctx, raw, before.Key)
			if after.Request.Status != store.RequestWaiting || after.Request.CurrentAttempt != nil {
				t.Fatalf("handoff not complete: %+v", after.Request)
			}
		})
	}
}
