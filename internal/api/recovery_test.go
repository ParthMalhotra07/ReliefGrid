package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestAPIDurableCreationIntentResumesBothCrashWindows(t *testing.T) {
	for _, crash := range []string{"intent-response-lost", "before-domain-create"} {
		t.Run(crash, func(t *testing.T) {
			ctx, s := realStore(t)
			var once atomic.Bool
			faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
				if crash == "intent-response-lost" && c.Method == "CompareAndSet" && strings.HasPrefix(c.Key, "api-create:") && once.CompareAndSwap(false, true) {
					return memstore.Fault{DropResponse: true}
				}
				if crash == "before-domain-create" && c.Method == "CreateEntity" && once.CompareAndSwap(false, true) {
					return memstore.Fault{DropBefore: true}
				}
				return memstore.Fault{}
			})
			first := call(ctx, handler(t, faulted, now, "dispatcher-a"), "POST", "/api/v1/requests", requestBody, fixtureKey(crash))
			expectError(t, first, 503, "OUTCOME_UNKNOWN", true, true)
			rows, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 1000})
			if err != nil || len(rows.Records) != 0 || first.Header().Get("Location") != "" {
				t.Fatalf("intent alone must not report/create a successful entity: %+v, %v", rows, err)
			}
			restarted := handler(t, faulted, now.Add(time.Hour), "dispatcher-a")
			created := object(t, call(ctx, restarted, "POST", "/api/v1/requests", requestBody, fixtureKey(crash)), 201)
			if created["createdAt"] != now.Format(time.RFC3339) {
				t.Fatalf("resumption changed frozen metadata: %#v", created)
			}
			rows, err = s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 1000})
			if err != nil || len(rows.Records) != 1 {
				t.Fatalf("resumed creation = %+v, %v", rows, err)
			}
		})
	}
}

func TestAPIConcurrentDifferentPayloadsCommitOnlyOne(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	bodies := []string{requestBody, strings.Replace(requestBody, "Synthetic water request", "Different synthetic need", 1)}
	responses := make([]*httptest.ResponseRecorder, len(bodies))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			responses[i] = call(ctx, h, "POST", "/api/v1/requests", bodies[i], fixtureKey("different-payload-race"))
		}()
	}
	close(start)
	wg.Wait()
	winners := 0
	for _, response := range responses {
		if response.Code == 201 {
			object(t, response, 201)
			winners++
		} else {
			expectError(t, response, 409, "CONFLICT_IDEMPOTENCY_KEY", false, false)
		}
	}
	if winners != 1 {
		t.Fatalf("%d creators won; want exactly one", winners)
	}
}

func TestAPIKeyIsScopedByOperationAndOptional(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	r := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("shared-operation-key")), 201)
	resource := object(t, call(ctx, h, "POST", "/api/v1/resources", resourceBody, fixtureKey("shared-operation-key")), 201)
	if r["requestId"] == resource["resourceId"] {
		t.Fatal("request and resource operations shared the same deterministic identity")
	}
	first := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	second := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	if first["requestId"] == second["requestId"] {
		t.Fatal("unkeyed creates unexpectedly reused an entity")
	}
}

func TestAPIPublicCASAndOperationReuseErrors(t *testing.T) {
	for _, code := range []string{"VERSION_CONFLICT", "OPERATION_ID_REUSED"} {
		t.Run(code, func(t *testing.T) {
			ctx, s := realStore(t)
			base := handler(t, s, now, "dispatcher-a")
			created := object(t, call(ctx, base, "POST", "/api/v1/requests", requestBody, ""), 201)
			var once atomic.Bool
			faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
				if c.Method != "TransitionRequest" || !once.CompareAndSwap(false, true) {
					return memstore.Fault{}
				}
				return memstore.Fault{Before: func(ctx context.Context) error {
					if code == "OPERATION_ID_REUSED" {
						// A rejected command is also durably deduplicated. Reuse
						// the real operation ID with a different command on the
						// same shard, then let the API's real command reach Apply.
						result, err := s.CompareAndSet(ctx, store.CompareAndSet{
							Meta: store.Meta{OperationID: c.OperationID, At: now.UnixMilli()}, Key: c.Key, Value: json.RawMessage(`1`),
						})
						if err == nil && result.Outcome != store.OutcomeDomainConflict {
							t.Errorf("fault setup result = %+v", result)
						}
						return err
					}
					result, err := s.TransitionRequest(ctx, store.TransitionRequest{
						Meta: store.Meta{OperationID: fixtureKey("concurrent-cancel"), At: now.UnixMilli()}, Key: c.Key,
						ExpectedVersion: 1, ExpectedStatus: store.RequestPending, To: store.RequestCancelled,
					})
					if err == nil && result.Outcome != store.OutcomeOK {
						t.Errorf("fault setup result = %+v", result)
					}
					return err
				}}
			})
			h := handler(t, faulted, now, "dispatcher-a")
			expectError(t, call(ctx, h, "PUT", "/api/v1/requests/"+id(t, created, "requestId")+"/status", `{"status":"CANCELLED"}`, ""), 409, code, false, false)
		})
	}
}

func TestAPIReadFailuresKeepKnownAndUnknownSeparate(t *testing.T) {
	for _, code := range []string{"NO_LEADER", "NOT_LEADER", "RING_VERSION_MISMATCH", "STORAGE_UNAVAILABLE"} {
		t.Run(code, func(t *testing.T) {
			ctx, s := realStore(t)
			faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
				if c.Method == "Scan" {
					return memstore.Fault{Before: func(context.Context) error {
						return &coordclient.Error{Status: 503, Code: internalapi.ErrorCode(code), Retryable: true}
					}}
				}
				return memstore.Fault{}
			})
			expectError(t, call(ctx, handler(t, faulted, now, "dispatcher-a"), "GET", "/api/v1/requests", "", ""), 503, code, true, false)
		})
	}
}

func TestAPIEveryRemoteCallHasACappedDeadline(t *testing.T) {
	ctx, s := realStore(t)
	var observed atomic.Int32
	faulted := memstore.WithFaults(s, func(memstore.Call) memstore.Fault {
		return memstore.Fault{Before: func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 100*time.Millisecond {
				t.Error("store call lacks its 100ms cap")
			}
			observed.Add(1)
			return nil
		}}
	})
	h, err := api.New(api.Config{Store: faulted, Now: func() time.Time { return now }, CallerNamespace: "dispatcher-a", Budget: time.Second, CallTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	object(t, call(ctx, h, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("bounded-calls")), 201)
	if observed.Load() < 3 {
		t.Fatal("did not exercise read, intent CAS and entity creation")
	}
}
