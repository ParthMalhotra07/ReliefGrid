package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func conditional(ctx context.Context, h http.Handler, path, body, tag string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, "PUT", path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("If-Match", tag)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAPIConditionalUpdatesUseCommittedVersion(t *testing.T) {
	ctx, raw := realStore(t)
	h := handler(t, raw, now, "dispatcher-a")
	first := call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("etag-create"))
	created := object(t, first, 201)
	rid := id(t, created, "requestId")
	path := "/api/v1/requests/" + rid
	if first.Header().Get("ETag") != `"v1"` {
		t.Fatalf("creation ETag=%q", first.Header().Get("ETag"))
	}
	get := call(ctx, h, "GET", path, "", "")
	if get.Header().Get("ETag") != first.Header().Get("ETag") {
		t.Fatal("GET did not expose entity ETag")
	}
	updated := conditional(ctx, h, path+"/status", `{"status":"CANCELLED"}`, first.Header().Get("ETag"))
	object(t, updated, 200)
	if updated.Header().Get("ETag") != `"v2"` {
		t.Fatal("update did not expose committed version")
	}
	stale := conditional(ctx, h, path+"/status", `{"status":"CANCELLED"}`, `"v1"`)
	expectError(t, stale, 412, "PRECONDITION_FAILED", false, false)
	if stale.Header().Get("ETag") != `"v2"` {
		t.Fatal("412 missing current ETag")
	}
	replay := call(ctx, h, "POST", "/api/v1/requests", requestBody, fixtureKey("etag-create"))
	object(t, replay, 201)
	if replay.Header().Get("ETag") != `"v1"` {
		t.Fatal("keyed replay must retain original response version")
	}
}

func TestAPIConditionalCASRaceAndUnknownOutcomeStayDistinct(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "race", true: "unknown"}[unknown], func(t *testing.T) {
			ctx, raw := realStore(t)
			h := handler(t, raw, now, "dispatcher-a")
			created := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
			rid := id(t, created, "requestId")
			var once atomic.Bool
			wrapped := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				if c.Method != "TransitionRequest" || !once.CompareAndSwap(false, true) {
					return memstore.Fault{}
				}
				if unknown {
					return memstore.Fault{DropResponse: true}
				}
				return memstore.Fault{Before: func(ctx context.Context) error {
					result, err := raw.RetryWorkflow(ctx, store.RetryWorkflow{Meta: store.Meta{OperationID: fixtureKey("etag-race"), At: now.UnixMilli()}, Key: c.Key, ExpectedVersion: 1, NextAttemptAt: now.UnixMilli() + 1000})
					if err == nil && result.Outcome != store.OutcomeOK {
						t.Fatalf("race setup=%+v", result)
					}
					return err
				}}
			})
			reply := conditional(ctx, handler(t, wrapped, now, "dispatcher-a"), "/api/v1/requests/"+rid+"/status", `{"status":"CANCELLED"}`, `"v1"`)
			if unknown {
				expectError(t, reply, 503, "OUTCOME_UNKNOWN", true, true)
			} else {
				expectError(t, reply, 412, "PRECONDITION_FAILED", false, false)
				if reply.Header().Get("ETag") != `"v2"` {
					t.Fatal("committed conflict missing current ETag")
				}
			}
			rec, err := raw.ReadRecord(ctx, store.RequestKey(rid))
			if err != nil {
				t.Fatal(err)
			}
			if (!unknown && rec.Request.Status != store.RequestPending) || (unknown && rec.Request.Status != store.RequestCancelled) {
				t.Fatalf("wrong committed state:%+v", rec)
			}
		})
	}
}

func TestAPIResourceConditionalTagSyntax(t *testing.T) {
	ctx, raw := realStore(t)
	h := handler(t, raw, now, "dispatcher-a")
	first := call(ctx, h, "POST", "/api/v1/resources", resourceBody, "")
	rid := id(t, object(t, first, 201), "resourceId")
	path := "/api/v1/resources/" + rid + "/status"
	for _, tag := range []string{`W/"v1"`, `"old"`} {
		expectError(t, conditional(ctx, h, path, `{"status":"OFFLINE"}`, tag), 412, "PRECONDITION_FAILED", false, false)
	}
	for _, tag := range []string{"", `v1`, `"v1",`, `*, "v1"`} {
		expectError(t, conditional(ctx, h, path, `{"status":"OFFLINE"}`, tag), 400, "INVALID", false, false)
	}
	object(t, conditional(ctx, h, path, `{"status":"OFFLINE"}`, `"old", "v1"`), 200)
	object(t, conditional(ctx, h, path, `{"status":"AVAILABLE"}`, `*`), 200)
}

func TestAPICursorWalkNeverDropsMergedShardRows(t *testing.T) {
	ctx, raw := realStore(t)
	h := handler(t, raw, now, "dispatcher-a")
	wanted := map[string]bool{}
	for i := 0; i < 29; i++ {
		row := object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
		wanted[id(t, row, "requestId")] = true
	}
	path := "/api/v1/requests?type=WATER&limit=2"
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 32; page++ {
		next := path
		if cursor != "" {
			next += "&cursor=" + url.QueryEscape(cursor)
		}
		body := object(t, call(ctx, h, "GET", next, "", ""), 200)
		for _, item := range body["items"].([]any) {
			rid := id(t, item.(map[string]any), "requestId")
			if seen[rid] || !wanted[rid] {
				t.Fatal("duplicate/foreign cursor item")
			}
			seen[rid] = true
		}
		token, _ := body["nextCursor"].(string)
		if token == "" {
			break
		}
		if token == cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = token
	}
	if len(seen) != len(wanted) {
		t.Fatalf("cursor walk visited%d/%d", len(seen), len(wanted))
	}
	expectError(t, call(ctx, h, "GET", path+"&cursor=invalid", "", ""), 400, "INVALID", false, false)
	first := object(t, call(ctx, h, "GET", path, "", ""), 200)
	token, _ := first["nextCursor"].(string)
	expectError(t, call(ctx, h, "GET", "/api/v1/requests?type=FOOD&limit=2&cursor="+url.QueryEscape(token), "", ""), 400, "INVALID", false, false)
	faulted := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	expectError(t, call(ctx, handler(t, faulted, now, "dispatcher-a"), "GET", path+"&cursor="+url.QueryEscape(token), "", ""), 503, "STORAGE_UNAVAILABLE", true, false)
}

func TestAPISpatialCursorWalkFiltersEveryPage(t *testing.T) {
	ctx, raw := realStore(t)
	h := handler(t, raw, now, "dispatcher-a")
	wanted := map[string]bool{}
	for i := 0; i < 17; i++ {
		body := strings.Replace(resourceBody, "0.01", "0.005", 1)
		if i%3 == 0 {
			body = strings.Replace(resourceBody, "0.01", "0.03", 1)
		}
		row := object(t, call(ctx, h, "POST", "/api/v1/resources", body, ""), 201)
		if i%3 != 0 {
			wanted[id(t, row, "resourceId")] = true
		}
	}
	path := "/api/v1/resources?type=WATER&near=0,0&radiusKm=1&limit=2"
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < 32; page++ {
		next := path
		if cursor != "" {
			next += "&cursor=" + url.QueryEscape(cursor)
		}
		body := object(t, call(ctx, h, "GET", next, "", ""), 200)
		for _, item := range body["items"].([]any) {
			rid := id(t, item.(map[string]any), "resourceId")
			if !wanted[rid] || seen[rid] {
				t.Fatal("spatial cursor returned a duplicate or out-of-radius item")
			}
			seen[rid] = true
		}
		nextCursor, _ := body["nextCursor"].(string)
		if nextCursor == "" {
			break
		}
		if nextCursor == cursor {
			t.Fatal("nonadvancing spatial cursor")
		}
		cursor = nextCursor
	}
	if len(seen) != len(wanted) {
		t.Fatalf("spatial walk=%d/%d", len(seen), len(wanted))
	}
	first := object(t, call(ctx, h, "GET", path, "", ""), 200)
	token := first["nextCursor"].(string)
	expectError(t, call(ctx, h, "GET", strings.Replace(path, "radiusKm=1", "radiusKm=2", 1)+"&cursor="+url.QueryEscape(token), "", ""), 400, "INVALID", false, false)
}

type changingRingStore struct {
	store.Store
	other   store.Store
	changed bool
}

func (s *changingRingStore) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	if s.changed {
		return s.other.Scan(ctx, q)
	}
	return s.Store.Scan(ctx, q)
}
func TestAPICursorRingAndFailedShardAfterContinuation(t *testing.T) {
	ctx, raw := realStore(t)
	newer, err := memstore.New(ctx, memstore.WithShards(3), memstore.WithRingVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := newer.Close(); err != nil {
			t.Error(err)
		}
	})
	switched := &changingRingStore{Store: raw, other: newer}
	h := handler(t, switched, now, "dispatcher-a")
	for i := 0; i < 15; i++ {
		object(t, call(ctx, h, "POST", "/api/v1/requests", requestBody, ""), 201)
	}
	for _, limit := range []string{"1", "2"} {
		t.Run("limit"+limit, func(t *testing.T) {
			defer func() { switched.changed = false }()
			path := "/api/v1/requests?limit=" + limit
			first := object(t, call(ctx, h, "GET", path, "", ""), 200)
			token := first["nextCursor"].(string)
			data, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				t.Fatal(err)
			}
			var public struct {
				Scan string `json:"s"`
			}
			if err := json.Unmarshal(data, &public); err != nil {
				t.Fatal(err)
			}
			if public.Scan == "" {
				t.Error("first-page continuation lost ring binding")
			}
			switched.changed = true
			expectError(t, call(ctx, h, "GET", path+"&cursor="+url.QueryEscape(token), "", ""), 503, "RING_VERSION_MISMATCH", true, false)
			switched.changed = false
			// Walk until the underlying storage cursor advances beyond the first page.
			advanced := false
			for i := 0; i < 20; i++ {
				data, _ := base64.RawURLEncoding.DecodeString(token)
				_ = json.Unmarshal(data, &public)
				if public.Scan != "" {
					inner, err := store.DecodeScanCursor(public.Scan)
					if err != nil {
						t.Fatal(err)
					}
					for _, after := range inner.After {
						if after != "" {
							advanced = true
						}
					}
				}
				if advanced {
					break
				}
				body := object(t, call(ctx, h, "GET", path+"&cursor="+url.QueryEscape(token), "", ""), 200)
				token, _ = body["nextCursor"].(string)
				if token == "" {
					break
				}
			}
			if !advanced {
				t.Fatal("did not exercise a nonempty storage continuation")
			}
			switched.changed = true
			expectError(t, call(ctx, h, "GET", path+"&cursor="+url.QueryEscape(token), "", ""), 503, "RING_VERSION_MISMATCH", true, false)
			switched.changed = false
			broken := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				if c.Method == "Scan" {
					return memstore.Fault{FailShards: []int{1}}
				}
				return memstore.Fault{}
			})
			expectError(t, call(ctx, handler(t, broken, now, "dispatcher-a"), "GET", path+"&cursor="+url.QueryEscape(token), "", ""), 503, "STORAGE_UNAVAILABLE", true, false)
		})
	}
}
