package api_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func TestAPICleanupCompletesDurableStatusIntent(t *testing.T) {
	for _, target := range []string{"CANCELLED", "FULFILLED"} {
		t.Run(target, func(t *testing.T) {
			ctx, raw := realStore(t)
			h := handler(t, raw, now, "dispatcher-a")
			created := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("cleanup-request")), 201)
			rid := id(t, created, "requestId")
			body := strings.Replace(resourceBody, `"reusable":true`, `"reusable":false`, 1)
			res := object(t, call(ctx, h, "POST", "/api/v1/resources", body, fixtureKey("cleanup-resource")), 201)
			resID := id(t, res, "resourceId")
			object(t, call(ctx, h, "POST", "/api/v1/match/"+rid, "", ""), 200)
			statusBody := `{"status":"` + target + `"}`
			intermediate := object(t, call(ctx, h, "PUT", "/api/v1/requests/"+rid+"/status", statusBody, ""), 200)
			if intermediate["status"] == target {
				t.Fatal("API returned terminal before closure")
			}
			expectError(t, call(ctx, h, "PUT", "/api/v1/resources/"+resID+"/status", `{"status":"OFFLINE"}`, ""), 409, "DOMAIN_CONFLICT", false, false)
			m, err := workflow.New(workflow.Config{Store: raw, ScanLimit: 100, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Match(ctx, rid); err != nil {
				t.Fatal(err)
			}
			view := object(t, call(ctx, h, "GET", "/api/v1/requests/"+rid, "", ""), 200)
			if view["status"] != target || view["assignedResourceId"] != nil {
				t.Fatalf("bad terminal projection: %#v", view)
			}
			before, err := raw.ReadRecord(ctx, store.RequestKey(rid))
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if got := object(t, call(ctx, h, "PUT", "/api/v1/requests/"+rid+"/status", statusBody, ""), 200); got["status"] != target {
					t.Fatalf("repeat status = %#v", got)
				}
			}
			after, err := raw.ReadRecord(ctx, before.Key)
			if err != nil || after.Version != before.Version {
				t.Fatalf("repeat changed terminal record: %+v %v", after, err)
			}
			r, err := raw.ReadRecord(ctx, store.ResourceKey(resID))
			want := store.ResourceAvailable
			if target == "FULFILLED" {
				want = store.ResourceConsumed
			}
			if err != nil || r.Resource.ActiveClaim != nil || r.Resource.Status != want {
				t.Fatalf("bad closed resource: %+v %v", r, err)
			}
			other := "CANCELLED"
			if target == "CANCELLED" {
				other = "FULFILLED"
			}
			expectError(t, call(ctx, h, "PUT", "/api/v1/requests/"+rid+"/status", `{"status":"`+other+`"}`, ""), 409, "DOMAIN_CONFLICT", false, false)
		})
	}
}
