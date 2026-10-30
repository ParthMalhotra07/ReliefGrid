package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

var now = time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)

const requestBody = `{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic water request","peopleAffected":2,"urgency":"HIGH"}`
const resourceBody = `{"resourceType":"WATER","name":"Synthetic water unit","location":{"lat":0,"lng":0.01},"windowStart":"2026-10-30T09:00:00Z","windowEnd":"2026-10-30T11:00:00Z","reusable":true}`

func fixtureKey(label string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("reliefgrid/A1.1/test/"+label)).String()
}

func realStore(t *testing.T) (context.Context, *memstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, s
}

// New/Config are the proposed A1.1 application surface, not a storage fake.
func handler(t *testing.T, s store.Store, at time.Time, caller string) http.Handler {
	t.Helper()
	h, err := api.New(api.Config{
		Store: s, Now: func() time.Time { return at }, CallerNamespace: caller,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func call(ctx context.Context, h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func object(t *testing.T, w *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	if w.Code != status {
		t.Fatalf("HTTP %d: %s; want %d", w.Code, w.Body.String(), status)
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("Content-Type = %q", w.Header().Get("Content-Type"))
	}
	var v map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("invalid JSON %q: %v", w.Body.String(), err)
	}
	assertResponseSchema(t, v)
	return v
}

var schemaOnce sync.Once
var publicSpec *openapi3.T
var schemaErr error

func assertResponseSchema(t *testing.T, value map[string]any) {
	t.Helper()
	schemaOnce.Do(func() {
		publicSpec, schemaErr = openapi3.NewLoader().LoadFromFile("../../contracts/public.yaml")
	})
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	name := "ErrorEnvelope"
	switch {
	case value["outcome"] != nil:
		name = "MatchResponse"
	case value["requestId"] != nil:
		name = "RequestResponse"
	case value["resourceId"] != nil:
		name = "ResourceResponse"
	case value["items"] != nil:
		name = "RequestListResponse"
		if items, ok := value["items"].([]any); ok && len(items) > 0 {
			if first, ok := items[0].(map[string]any); ok && first["resourceId"] != nil {
				name = "ResourceListResponse"
			}
		}
	}
	if err := publicSpec.Components.Schemas[name].Value.VisitJSON(value, openapi3.VisitAsResponse()); err != nil {
		t.Fatalf("response violates frozen %s schema: %v", name, err)
	}
}

func id(t *testing.T, v map[string]any, field string) string {
	t.Helper()
	s, ok := v[field].(string)
	if !ok {
		t.Fatalf("%s = %#v; want UUID string", field, v[field])
	}
	if _, err := uuid.Parse(s); err != nil {
		t.Fatalf("%s = %q: %v", field, s, err)
	}
	return s
}

func expectError(t *testing.T, w *httptest.ResponseRecorder, status int, code string, retryable, unknown bool) {
	t.Helper()
	v := object(t, w, status)
	if v["code"] != code || v["retryable"] != retryable || v["outcomeUnknown"] != unknown {
		t.Fatalf("error = %#v; want %s, retryable=%t, unknown=%t", v, code, retryable, unknown)
	}
	if message, ok := v["message"].(string); !ok || message == "" {
		t.Fatalf("missing error message: %#v", v)
	}
	id(t, v, "correlationId")
	if retryable && w.Header().Get("Retry-After") == "" {
		t.Fatal("retryable error has no Retry-After")
	}
}

func TestAPIRequiredRoutesAndReciprocalMatch(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	created := call(ctx, h, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("request-1"))
	r := object(t, created, http.StatusCreated)
	rid := id(t, r, "requestId")
	if r["status"] != "PENDING" || created.Header().Get("Location") != "/api/v1/requests/"+rid {
		t.Fatalf("creation state/location: %#v, %q", r, created.Header().Get("Location"))
	}
	for _, field := range []string{"assignedResourceId", "nextAttemptAt", "retryCount", "lastError"} {
		if _, present := r[field]; !present {
			t.Fatalf("required field %q omitted: %#v", field, r)
		}
	}
	if r["assignedResourceId"] != nil || r["nextAttemptAt"] != nil {
		t.Fatalf("unassigned/new request fields must be null: %#v", r)
	}
	if version, ok := r["version"].(string); !ok || version == "" {
		t.Fatalf("opaque version missing: %#v", r["version"])
	}
	got := object(t, call(ctx, h, http.MethodGet, "/api/v1/requests/"+rid, "", ""), http.StatusOK)
	if !reflect.DeepEqual(r, got) {
		t.Fatalf("GET differs from committed create: %#v / %#v", r, got)
	}
	for _, path := range []string{"/api/v1/requests?type=WATER&status=PENDING&limit=1", "/api/v1/resources?type=WATER&near=0,0&radiusKm=50&limit=1"} {
		list := object(t, call(ctx, h, http.MethodGet, path, "", ""), http.StatusOK)
		if list["partial"] != false || list["consistency"] != "strong" {
			t.Fatalf("authoritative scan metadata = %#v", list)
		}
		for _, field := range []string{"items", "positions", "failedShards"} {
			if _, ok := list[field].([]any); !ok {
				t.Fatalf("list %s must be an array: %#v", field, list)
			}
		}
	}
	resource := object(t, call(ctx, h, http.MethodPost, "/api/v1/resources", resourceBody, fixtureKey("resource-1")), http.StatusCreated)
	resID := id(t, resource, "resourceId")
	if claim, present := resource["activeClaim"]; !present || claim != nil {
		t.Fatalf("activeClaim must be present and null: %#v", resource)
	}
	stored, err := s.ReadRecord(ctx, store.ResourceKey(resID))
	if err != nil {
		t.Fatal(err)
	}
	wantHash, err := geo.Encode(store.Location{Lat: 0, Lng: .01}, 7)
	if err != nil || stored.Resource == nil || stored.Resource.Geohash != wantHash {
		t.Fatalf("registration must index geography: %+v, %v; want %s", stored.Resource, err, wantHash)
	}
	match := object(t, call(ctx, h, http.MethodPost, "/api/v1/match/"+rid, "", ""), http.StatusOK)
	matched, ok := match["request"].(map[string]any)
	if !ok || match["outcome"] != "MATCHED" || matched["assignedResourceId"] != resID {
		t.Fatalf("matching did not use registered resource: %#v", match)
	}
	repeat := object(t, call(ctx, h, http.MethodPost, "/api/v1/match/"+rid, "", ""), http.StatusOK)
	if repeat["outcome"] != "MATCHED" {
		t.Fatalf("repeat match = %#v", repeat)
	}
	stored, err = s.ReadRecord(ctx, store.ResourceKey(resID))
	if err != nil || stored.Resource == nil || stored.Resource.ActiveClaim == nil || stored.Resource.ActiveClaim.RequestID != rid {
		t.Fatalf("missing reciprocal claim: %+v, %v", stored.Resource, err)
	}
	object(t, call(ctx, h, http.MethodPut, "/api/v1/requests/"+rid+"/status", `{"status":"IN_PROGRESS"}`, ""), http.StatusOK)
	expectError(t, call(ctx, h, http.MethodPut, "/api/v1/resources/"+resID+"/status", `{"status":"OFFLINE"}`, ""), 409, "DOMAIN_CONFLICT", false, false)
}

func TestAPICreateReplaySurvivesMutationAndHandlerRestart(t *testing.T) {
	for _, endpoint := range []string{"requests", "resources"} {
		t.Run(endpoint, func(t *testing.T) {
			ctx, s := realStore(t)
			h := handler(t, s, now, "dispatcher-a")
			body, field := requestBody, "requestId"
			if endpoint == "resources" {
				body, field = resourceBody, "resourceId"
			}
			first := call(ctx, h, http.MethodPost, "/api/v1/"+endpoint, body, fixtureKey("stable-key"))
			original := object(t, first, 201)
			entityID := id(t, original, field)
			update := `{"status":"CANCELLED"}`
			if endpoint == "resources" {
				update = `{"status":"OFFLINE","windowStart":"2026-10-30T08:00:00Z","windowEnd":"2026-10-30T12:00:00Z"}`
			}
			object(t, call(ctx, h, http.MethodPut, "/api/v1/"+endpoint+"/"+entityID+"/status", update, ""), 200)
			var payload map[string]any
			if err := json.Unmarshal([]byte(body), &payload); err != nil {
				t.Fatal(err)
			}
			canonical, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			restarted := handler(t, s, now.Add(2*time.Hour), "dispatcher-a")
			replay := call(ctx, restarted, http.MethodPost, "/api/v1/"+endpoint, " \n"+string(canonical)+"\n", fixtureKey("stable-key"))
			if !reflect.DeepEqual(original, object(t, replay, 201)) || first.Header().Get("Location") != replay.Header().Get("Location") {
				t.Fatalf("replay must preserve original body/location after mutation and restart: first=%s replay=%s", first.Body, replay.Body)
			}
			if endpoint == "requests" {
				payload["details"] = "A different synthetic request"
			} else {
				payload["reusable"] = false
			}
			changed, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			expectError(t, call(ctx, restarted, http.MethodPost, "/api/v1/"+endpoint, string(changed), fixtureKey("stable-key")), 409, "CONFLICT_IDEMPOTENCY_KEY", false, false)
		})
	}
}

func TestAPIConcurrentKeyedCreatesReturnOneOriginalResult(t *testing.T) {
	ctx, s := realStore(t)
	handlers := []http.Handler{handler(t, s, now, "dispatcher-a"), handler(t, s, now.Add(time.Minute), "dispatcher-a")}
	start := make(chan struct{})
	responses := make([]*httptest.ResponseRecorder, 8)
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			responses[i] = call(ctx, handlers[i%2], http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("same-key"))
		}()
	}
	close(start)
	wg.Wait()
	original := object(t, responses[0], 201)
	for _, response := range responses[1:] {
		if !reflect.DeepEqual(original, object(t, response, 201)) || response.Header().Get("Location") != responses[0].Header().Get("Location") {
			t.Fatalf("racing proposals returned different original responses: %s / %s", responses[0].Body, response.Body)
		}
	}
	rows, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 1000})
	if err != nil || rows.Partial || len(rows.Records) != 1 {
		t.Fatalf("duplicate entities after keyed race: %+v, %v", rows, err)
	}
	other := handler(t, s, now, "dispatcher-b")
	different := object(t, call(ctx, other, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("same-key")), 201)
	if different["requestId"] == original["requestId"] {
		t.Fatal("caller namespaces shared a deterministic ID")
	}
}

func TestAPILostCreateResponseIsUnknownAndKeyedRetryRecovers(t *testing.T) {
	ctx, s := realStore(t)
	var dropped, blocked atomic.Bool
	faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "CreateEntity" && strings.HasPrefix(c.Key, store.RequestKeyPrefix) && dropped.CompareAndSwap(false, true) {
			blocked.Store(true)
			return memstore.Fault{DropResponse: true}
		}
		if blocked.Load() {
			return memstore.Fault{DropBefore: true}
		}
		return memstore.Fault{}
	})
	h := handler(t, faulted, now, "dispatcher-a")
	expectError(t, call(ctx, h, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("lost-response")), 503, "OUTCOME_UNKNOWN", true, true)
	if !dropped.Load() {
		t.Fatal("fault did not reach a committed request creation")
	}
	blocked.Store(false)
	restarted := handler(t, faulted, now.Add(time.Hour), "dispatcher-a")
	replayed := object(t, call(ctx, restarted, http.MethodPost, "/api/v1/requests", requestBody, fixtureKey("lost-response")), 201)
	if replayed["createdAt"] != now.Format(time.RFC3339) {
		t.Fatalf("lost-response retry regenerated server metadata: %#v", replayed)
	}
	rows, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 1000})
	if err != nil || rows.Partial || len(rows.Records) != 1 {
		t.Fatalf("unknown outcome produced duplicate/lost entity: %+v, %v", rows, err)
	}
}

func TestAPIValidationAndKnownErrors(t *testing.T) {
	ctx, s := realStore(t)
	h := handler(t, s, now, "dispatcher-a")
	for _, tc := range []struct{ name, method, path, body string }{
		{"bad JSON", "POST", "/api/v1/requests", `{`},
		{"missing details", "POST", "/api/v1/requests", `{"requestType":"WATER","location":{"lat":0,"lng":0}}`},
		{"invalid type", "POST", "/api/v1/requests", strings.Replace(requestBody, "WATER", "FUEL", 1)},
		{"invalid latitude", "POST", "/api/v1/requests", strings.Replace(requestBody, `"lat":0`, `"lat":91`, 1)},
		{"missing longitude", "POST", "/api/v1/requests", strings.Replace(requestBody, `,"lng":0`, "", 1)},
		{"zero people", "POST", "/api/v1/requests", strings.Replace(requestBody, `"peopleAffected":2`, `"peopleAffected":0`, 1)},
		{"invalid urgency", "POST", "/api/v1/requests", strings.Replace(requestBody, "HIGH", "URGENT", 1)},
		{"client assignment", "POST", "/api/v1/requests", strings.TrimSuffix(requestBody, "}") + `,"assignedResourceId":"550e8400-e29b-41d4-a716-446655440000"}`},
		{"missing reusable", "POST", "/api/v1/resources", strings.Replace(resourceBody, `,"reusable":true`, "", 1)},
		{"empty name", "POST", "/api/v1/resources", strings.Replace(resourceBody, "Synthetic water unit", "", 1)},
		{"reversed window", "POST", "/api/v1/resources", strings.Replace(resourceBody, "T11:00:00Z", "T08:00:00Z", 1)},
		{"non-UTC window", "POST", "/api/v1/resources", strings.Replace(resourceBody, "T09:00:00Z", "T09:00:00+01:00", 1)},
		{"zero limit", "GET", "/api/v1/requests?limit=0", ""},
		{"excessive radius", "GET", "/api/v1/resources?near=0,0&radiusKm=51", ""},
		{"bad near", "GET", "/api/v1/resources?near=91,0", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectError(t, call(ctx, h, tc.method, tc.path, tc.body, ""), 400, "INVALID", false, false)
		})
	}
	missing := "550e8400-e29b-41d4-a716-446655440000"
	expectError(t, call(ctx, h, "GET", "/api/v1/requests/"+missing, "", ""), 404, "NOT_FOUND", false, false)
	r := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	expectError(t, call(ctx, h, "PUT", "/api/v1/requests/"+id(t, r, "requestId")+"/status", `{"status":"IN_PROGRESS"}`, ""), 409, "DOMAIN_CONFLICT", false, false)
	for _, typ := range []string{"WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"} {
		object(t, call(ctx, h, "POST", "/api/v1/requests", strings.Replace(requestBody, "WATER", typ, 1), ""), 201)
	}
}

func TestAPIPartialAuthoritativeListsReturnUnavailable(t *testing.T) {
	ctx, s := realStore(t)
	faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	h := handler(t, faulted, now, "dispatcher-a")
	for _, path := range []string{"/api/v1/requests", "/api/v1/resources"} {
		w := call(ctx, h, "GET", path, "", "")
		v := object(t, w, 503)
		if v["retryable"] != true || v["outcomeUnknown"] != false || w.Header().Get("Retry-After") == "" {
			t.Fatalf("partial scan misreported as mutation uncertainty/empty success: %#v", v)
		}
		id(t, v, "correlationId")
	}
}

func TestAPISSEHeartbeatAndCancellation(t *testing.T) {
	_, s := realStore(t)
	base := handler(t, s, now, "dispatcher-a")
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		base.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("SSE status/content type: %d, %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, ":") {
		t.Fatalf("expected heartbeat comment; got %q, %v", line, err)
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not stop after request cancellation")
	}
}
