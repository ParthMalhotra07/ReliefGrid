package api_test

import (
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"net/http"
	"testing"
)

func TestPSResourceStatusReturns200AfterCommittedUnclaimedUpdate(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	resource := object(t, call(ctx, h, "POST", "/api/v1/resources", resourceBody, fixtureKey("ps-status")), 201)
	rid := id(t, resource, "resourceId")
	response := object(t, call(ctx, h, "PUT", "/api/v1/resources/"+rid+"/status", `{"status":"OFFLINE"}`, ""), http.StatusOK)
	stored, err := s.ReadRecord(ctx, store.ResourceKey(rid))
	if err != nil || stored.Resource == nil || stored.Resource.Status != store.ResourceOffline || stored.Resource.ActiveClaim != nil {
		t.Fatalf("uncommitted or claimed status update: %+v, %v", stored, err)
	}
	if response["status"] != "OFFLINE" || response["resourceId"] != rid || response["activeClaim"] != nil {
		t.Fatalf("incorrect status response: %#v", response)
	}
}

func TestPSMatch202MeansDeferredWorkflowNotRejection(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	request := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("ps-deferred")), 201)
	rid := id(t, request, "requestId")
	first := object(t, call(ctx, h, "POST", "/api/v1/match/"+rid, "", ""), 200)
	if first["outcome"] != "NO_CANDIDATE" {
		t.Fatalf("missing deferred fixture: %#v", first)
	}
	w := call(ctx, h, "POST", "/api/v1/match/"+rid, "", "")
	response := object(t, w, http.StatusAccepted)
	current := response["request"].(map[string]any)
	if response["outcome"] != "IN_PROGRESS" || current["requestId"] != rid || current["status"] != "WAITING" || current["nextAttemptAt"] == nil || w.Header().Get("Location") != "/api/v1/requests/"+rid {
		t.Fatalf("202 lost workflow/poll location: %#v, %v", response, w.Header())
	}
	stored, err := s.ReadRecord(ctx, store.RequestKey(rid))
	if err != nil || stored.Request == nil || stored.Request.Status != store.RequestWaiting || stored.Request.NextAttemptAt <= now.UnixMilli() {
		t.Fatalf("202 is not a committed deferred workflow: %+v %v", stored, err)
	}
}
