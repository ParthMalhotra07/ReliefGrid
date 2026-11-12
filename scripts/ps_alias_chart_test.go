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
		t.Fatalf("want precisely seven PS operation routes, got %d", len(routes))
	}

	const uuid = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
	operations := []struct{ name, rule string }{
		{"submit request", "Method(`POST`) && Path(`/requests`)"},
		{"request details", "Method(`GET`) && PathRegexp(`^/requests/" + uuid + "$`)"},
		{"request status", "Method(`PUT`) && PathRegexp(`^/requests/" + uuid + "/status$`)"},
		{"register resource", "Method(`POST`) && Path(`/resources`)"},
		{"list resources", "Method(`GET`) && Path(`/resources`)"},
		{"resource status", "Method(`PUT`) && PathRegexp(`^/resources/" + uuid + "/status$`)"},
		{"match request", "Method(`POST`) && PathRegexp(`^/match/" + uuid + "$`)"},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			count := 0
			for _, raw := range routes {
				if appField(t, raw.(map[string]any), "match") == operation.rule {
					count++
				}
			}
			if count != 1 {
				t.Errorf("missing PS operation route or duplicate: %s: want exactly one %q, got %d", operation.name, operation.rule, count)
			}
		})
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
		if len(services) != 1 {
			t.Fatalf("want one API backend, got %d", len(services))
		}
		appEqual(t, appField(t, services[0].(map[string]any), "name"), "reliefgrid-api", "alias backend")
		middlewares := appField(t, route, "middlewares").([]any)
		if len(middlewares) != 1 {
			t.Fatalf("want one prefix middleware, got %d", len(middlewares))
		}
		appEqual(t, appField(t, middlewares[0].(map[string]any), "name"), "reliefgrid-ps-prefix", "only add canonical prefix")
	}
}
