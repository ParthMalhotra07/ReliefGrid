package api_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestAPIMatchingNoCandidateAndDeferredWork(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	created := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	rid := id(t, created, "requestId")
	result := object(t, call(ctx, h, "POST", "/api/v1/match/"+rid, "", ""), 200)
	if result["outcome"] != "NO_CANDIDATE" {
		t.Fatalf("empty complete scan = %#v", result)
	}
	waiting, err := s.ReadRecord(ctx, store.RequestKey(rid))
	if err != nil || waiting.Request.Status != store.RequestWaiting || waiting.Request.NextAttemptAt <= now.UnixMilli() || waiting.Request.CurrentAttempt != nil {
		t.Fatalf("no-candidate result lacks a durable retry handoff: %+v, %v", waiting, err)
	}
	deferred := call(ctx, h, "POST", "/api/v1/match/"+rid, "", "")
	if object(t, deferred, 202)["outcome"] != "IN_PROGRESS" || deferred.Header().Get("Location") != "/api/v1/requests/"+rid {
		t.Fatalf("deferred response = %s", deferred.Body)
	}
	after, err := s.ReadRecord(ctx, waiting.Key)
	if err != nil || after.Version != waiting.Version {
		t.Fatalf("obsolete retry advanced the request: %+v, %v", after, err)
	}
	faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	other := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	expectError(t, call(ctx, handler(t, faulted, now, "dispatcher-a"), "POST", "/api/v1/match/"+id(t, other, "requestId"), "", ""), 503, "STORAGE_UNAVAILABLE", true, false)
}

func TestAPIStatusChangesRetainClaimsUntilClosure(t *testing.T) {
	for _, tc := range []struct {
		name, target, intermediate string
		reusable, expired          bool
		disposition                store.ResourceStatus
	}{
		{"cancel", "CANCELLED", "CANCELLING", true, false, store.ResourceAvailable},
		{"fulfill reusable", "FULFILLED", "FULFILLING", true, false, store.ResourceAvailable},
		{"fulfill consumable", "FULFILLED", "FULFILLING", false, false, store.ResourceConsumed},
		{"fulfill expired reusable", "FULFILLED", "FULFILLING", true, true, store.ResourceOffline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, s := realStore(t)
			h := handler(t, s, now, "dispatcher-a")
			created := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
			rid := id(t, created, "requestId")
			body := resourceBody
			if !tc.reusable {
				body = strings.Replace(body, `"reusable":true`, `"reusable":false`, 1)
			}
			res := object(t, call(ctx, h, "POST", "/api/v1/resources", body, ""), 201)
			resID := id(t, res, "resourceId")
			object(t, call(ctx, h, "POST", "/api/v1/match/"+rid, "", ""), 200)
			at := now
			if tc.expired {
				at = now.Add(2 * time.Hour)
			}
			h = handler(t, s, at, "dispatcher-a")
			statusBody := `{"status":"` + tc.target + `"}`
			progress := object(t, call(ctx, h, "PUT", "/api/v1/requests/"+rid+"/status", statusBody, ""), 200)
			if progress["status"] != tc.intermediate {
				t.Fatalf("premature terminal response: %#v", progress)
			}
			rec, err := s.ReadRecord(ctx, store.RequestKey(rid))
			if err != nil || rec.Request.CurrentAttempt == nil || rec.Request.Disposition != tc.disposition {
				t.Fatalf("cleanup intent/disposition lost: %+v, %v", rec, err)
			}
			object(t, call(ctx, h, "PUT", "/api/v1/requests/"+rid+"/status", statusBody, ""), 200)
			repeated, err := s.ReadRecord(ctx, rec.Key)
			if err != nil || repeated.Version != rec.Version {
				t.Fatalf("repeated status call changed cleanup: %+v, %v", repeated, err)
			}
			resource, err := s.ReadRecord(ctx, store.ResourceKey(resID))
			if err != nil || resource.Resource.ActiveClaim == nil || resource.Resource.ActiveClaim.RequestID != rid {
				t.Fatalf("API cleared claim before closure: %+v, %v", resource, err)
			}
			// Complete the protocol with the real FSM, as A2.1 will do. The
			// public projection must not advertise historical ownership.
			attempt := rec.Request.CurrentAttempt
			closed, err := s.CloseAttempt(ctx, store.CloseAttempt{
				Meta:        store.Meta{OperationID: fixtureKey(tc.name + "-close"), At: at.UnixMilli()},
				ResourceKey: resource.Key, RequestID: rid, Attempt: *attempt,
				Disposition: tc.disposition, Reason: "public projection fixture",
			})
			if err != nil || closed.Outcome != store.OutcomeOK {
				t.Fatalf("closure fixture failed: %+v, %v", closed, err)
			}
			terminal, err := s.TransitionRequest(ctx, store.TransitionRequest{
				Meta: store.Meta{OperationID: fixtureKey(tc.name + "-terminal"), At: at.UnixMilli()}, Key: rec.Key,
				ExpectedVersion: rec.Version, ExpectedStatus: rec.Request.Status, ExpectedAttemptID: attempt.AttemptID,
				To: store.RequestStatus(tc.target),
			})
			if err != nil || terminal.Outcome != store.OutcomeOK {
				t.Fatalf("terminal fixture failed: %+v, %v", terminal, err)
			}
			view := object(t, call(ctx, h, "GET", "/api/v1/requests/"+rid, "", ""), 200)
			if view["assignedResourceId"] != nil {
				t.Fatalf("terminal projection presents historical assignment as current: %#v", view)
			}
			expectError(t, call(ctx, h, "POST", "/api/v1/match/"+rid, "", ""), 409, "DOMAIN_CONFLICT", false, false)
		})
	}
}

func TestAPIOutboxEnvelopesAndRouting(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	r := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("outbox-request")), 201)
	object(t, call(ctx, h, "PUT", "/api/v1/requests/"+id(t, r, "requestId")+"/status", `{"status":"CANCELLED"}`, ""), 200)
	res := object(t, call(ctx, h, "POST", "/api/v1/resources", resourceBody, fixtureKey("outbox-resource")), 201)
	object(t, call(ctx, h, "PUT", "/api/v1/resources/"+id(t, res, "resourceId")+"/status", `{"status":"OFFLINE"}`, ""), 200)
	all := make([]store.OutboxEvent, 0, 4)
	for shard := 0; shard < s.Shards(); shard++ {
		rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, rows...)
	}
	if len(all) != 4 {
		t.Fatalf("got %d events; want one per committed domain mutation", len(all))
	}
	for _, row := range all {
		var envelope map[string]any
		if err := json.Unmarshal(row.Payload, &envelope); err != nil {
			t.Fatal(err)
		}
		id(t, envelope, "eventId")
		if envelope["eventType"] != row.EventType || envelope["entityVersion"] != float64(row.EntityVersion) || row.Published {
			t.Fatalf("outbox row/envelope mismatch: %+v / %#v", row, envelope)
		}
		if row.EventType == "request.created" {
			if row.Exchange != "relief.requests" || row.RoutingKey != "request.created.water" {
				t.Fatalf("bad matching trigger route: %+v", row)
			}
		} else if row.Exchange != "relief.events" {
			t.Fatalf("bad projection exchange: %+v", row)
		}
	}
	if path := os.Getenv("A11_EVENTS_FILE"); path != "" {
		encoded, err := json.Marshal(all)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
