package coordinator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

func TestAuthRefusalAfterUnknownRemainsUnknown(t *testing.T) {
	for _, unknownFirst := range []bool{false, true} {
		h := newHarness(t, func(c *coordinator.Config) {})
		h.ready()
		key, _ := keyOn(t, h.rg, 0, "kv:", "auth")
		for i, n := range h.nodes[0] {
			if unknownFirst && i == 0 {
				n.setHandler(failWith(503, internalapi.OUTCOMEUNKNOWN, true))
			} else {
				n.setHandler(failWith(401, internalapi.UNAUTHORIZED, false))
			}
		}
		rec := h.do(call{method: http.MethodPut, path: "/internal/data/" + key, body: map[string]any{"ringVersion": h.rg.Version(), "operationId": "same-operation", "at": t0, "value": 1}})
		e := decodeJSON[internalapi.Error](t, rec)
		if unknownFirst {
			if rec.Code != 503 || e.Code != internalapi.OUTCOMEUNKNOWN || !e.OutcomeUnknown {
				t.Fatalf("erased unknown: %d %+v", rec.Code, e)
			}
		} else if rec.Code != 401 || e.Code != internalapi.UNAUTHORIZED || e.OutcomeUnknown {
			t.Fatalf("first refusal: %d %+v", rec.Code, e)
		}
	}
}

func TestCoordinatorSeparatesAppAndNodeCredentials(t *testing.T) {
	app, node := auth.ReceiverConfig{Token: "app"}, auth.ReceiverConfig{Token: "node"}
	c, err := coordinator.New(coordinator.Config{Ring: loadRing(t), AppAuth: &app, NodeAuth: &node})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, token string
		refused     bool
	}{
		{"/internal/ring", "", true}, {"/internal/ring", "node", true}, {"/internal/ring", "app", false},
		{"/internal/nodes/register", "app", true}, {"/internal/nodes/register", "node", false},
		{"/admin/ring", "", false},
	} {
		method := "GET"
		if tc.path == "/internal/nodes/register" {
			method = "POST"
		}
		req := httptest.NewRequest(method, tc.path, nil)
		if tc.token != "" {
			req.Header.Set(auth.Header, tc.token)
		}
		rec := httptest.NewRecorder()
		c.Handler().ServeHTTP(rec, req)
		if (rec.Code == 401) != tc.refused {
			t.Fatalf("%s token=%q: %d %s", tc.path, tc.token, rec.Code, rec.Body.String())
		}
	}
}
