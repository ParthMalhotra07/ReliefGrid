//go:build integration

package scripts_test

import (
	"strings"
	"testing"
)

func TestPSTable2AliasesAreNarrowAndUseCanonicalAPI(t *testing.T) {
	docs := appTracingRender(t)
	middleware := appDocument(t, docs, "Middleware", "reliefgrid-ps-prefix")
	appEqual(t, appField(t, middleware, "spec", "addPrefix", "prefix"), "/api/v1", "same API handlers")
	ingress := appDocument(t, docs, "IngressRoute", "reliefgrid-ps-aliases")
	routes := appField(t, ingress, "spec", "routes").([]any)
	if len(routes) != 7 {
		t.Fatalf("want precisely seven PS operation routes, got%d", len(routes))
	}
	for _, raw := range routes {
		route := raw.(map[string]any)
		rule := appField(t, route, "match").(string)
		if !strings.Contains(rule, "Method(") || strings.Contains(rule, "PathPrefix") || strings.Contains(rule, "events") {
			t.Fatalf("broad alias shadows UI/SSE: %s", rule)
		}
		if strings.Contains(rule, "PathRegexp") && !strings.Contains(rule, "[0-9a-fA-F]") {
			t.Fatalf("ID route must preserve named UI routes: %s", rule)
		}
		services := appField(t, route, "services").([]any)
		appEqual(t, appField(t, services[0].(map[string]any), "name"), "reliefgrid-api", "alias backend")
		middlewares := appField(t, route, "middlewares").([]any)
		appEqual(t, appField(t, middlewares[0].(map[string]any), "name"), "reliefgrid-ps-prefix", "only add canonical prefix")
	}
}
