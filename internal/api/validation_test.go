package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
)

func TestAPIIdempotencyKeyMatchesFrozenUUIDContract(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	for _, endpoint := range []string{"requests", "resources"} {
		body := requestBody
		if endpoint == "resources" {
			body = resourceBody
		}
		for _, key := range []string{"stable-key", strings.Repeat("x", 36), "550e8400e29b41d4a716446655440000", "urn:uuid:" + fixtureKey("urn")} {
			expectError(t, call(ctx, h, "POST", "/api/v1/"+endpoint, body, key), 400, "INVALID", false, false)
		}
		for _, keys := range [][]string{{""}, {fixtureKey("one"), fixtureKey("two")}} {
			r := httptest.NewRequestWithContext(ctx, "POST", "/api/v1/"+endpoint, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			r.Header["Idempotency-Key"] = keys
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expectError(t, w, 400, "INVALID", false, false)
		}
		object(t, call(ctx, h, "POST", "/api/v1/"+endpoint, body, fixtureKey(endpoint)), 201)
	}
}

func TestAPIStrictWireValidation(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	for _, tc := range []struct{ name, method, path, body string }{
		{"nullable people prohibited", "POST", "/api/v1/requests", strings.Replace(requestBody, `"peopleAffected":2`, `"peopleAffected":null`, 1)},
		{"nullable urgency prohibited", "POST", "/api/v1/requests", strings.Replace(requestBody, `"urgency":"HIGH"`, `"urgency":null`, 1)},
		{"duplicate fields", "POST", "/api/v1/requests", strings.TrimSuffix(requestBody, "}") + `,"urgency":"NORMAL"}`},
		{"trailing JSON", "POST", "/api/v1/requests", requestBody + `{}`},
		{"case-sensitive fields", "POST", "/api/v1/requests", strings.Replace(requestBody, "requestType", "RequestType", 1)},
		{"nested unknown field", "POST", "/api/v1/requests", strings.Replace(requestBody, `"lat":0`, `"alt":1,"lat":0`, 1)},
		{"oversized details", "POST", "/api/v1/requests", strings.Replace(requestBody, "Synthetic water request", strings.Repeat("x", 4097), 1)},
		{"oversized name", "POST", "/api/v1/resources", strings.Replace(resourceBody, "Synthetic water unit", strings.Repeat("x", 256), 1)},
		{"longitude out of range", "POST", "/api/v1/resources", strings.Replace(resourceBody, `"lng":0.01`, `"lng":181`, 1)},
		{"window has no timezone", "POST", "/api/v1/resources", strings.Replace(resourceBody, "T09:00:00Z", "T09:00:00", 1)},
		{"unknown query", "GET", "/api/v1/requests?cursor=x", ""},
		{"duplicate limit", "GET", "/api/v1/requests?limit=1&limit=2", ""},
		{"bad request status", "GET", "/api/v1/requests?status=UNMATCHED", ""},
		{"nondecimal near", "GET", "/api/v1/resources?near=1e1,0", ""},
		{"infinite radius", "GET", "/api/v1/resources?near=0,0&radiusKm=Inf", ""},
		{"NaN radius", "GET", "/api/v1/resources?near=0,0&radiusKm=NaN", ""},
		{"malformed ID", "GET", "/api/v1/requests/not-a-uuid", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectError(t, call(ctx, h, tc.method, tc.path, tc.body, ""), 400, "INVALID", false, false)
		})
	}
	for _, resourceType := range []string{"WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"} {
		object(t, call(ctx, h, "POST", "/api/v1/resources", strings.Replace(resourceBody, "WATER", resourceType, 1), ""), 201)
	}
	// Defaults and explicit defaults describe the same canonical client payload.
	minimal := `{"requestType":"WATER","location":{"lat":-90,"lng":180},"details":""}`
	first := object(t, call(ctx, h, "POST", "/api/v1/requests", minimal, fixtureKey("defaults")), 201)
	explicit := strings.TrimSuffix(minimal, "}") + `,"peopleAffected":1,"urgency":"NORMAL"}`
	replay := object(t, call(ctx, h, "POST", "/api/v1/requests", explicit, fixtureKey("defaults")), 201)
	if first["requestId"] != replay["requestId"] || replay["peopleAffected"] != float64(1) || replay["urgency"] != "NORMAL" {
		t.Fatalf("normalized defaults differ: %#v / %#v", first, replay)
	}
}

func TestAPIProtectedSSEErrors(t *testing.T) {
	ctx, s := realStore(t)
	h, err := api.New(api.Config{Store: s, CallerNamespace: "dispatcher-a", StreamToken: "local-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, call(ctx, h, "GET", "/api/v1/events", "", ""), 401, "UNAUTHORIZED", false, false)
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/events", nil)
	r.Header.Set("Authorization", "Bearer incorrect-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	expectError(t, w, 403, "FORBIDDEN", false, false)
}
