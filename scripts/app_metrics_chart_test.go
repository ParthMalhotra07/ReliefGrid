//go:build integration

package scripts_test

import (
	"path/filepath"
	"testing"
)

func TestAppMetricsArePrivateAndOnlyApplicationWorkersExposeThem(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"))
	for _, name := range []string{"api", "matcher", "ui"} {
		d := appDocument(t, docs, "Deployment", "reliefgrid-"+name)
		containers := appField(t, d, "spec", "template", "spec", "containers").([]any)
		c := containers[0].(map[string]any)
		foundPort, foundEnv := false, false
		for _, raw := range c["ports"].([]any) {
			p := raw.(map[string]any)
			if p["name"] == "metrics" {
				foundPort = true
				appEqual(t, p["containerPort"], 9090, name+" metrics port")
			}
		}
		for _, raw := range c["env"].([]any) {
			e := raw.(map[string]any)
			if e["name"] == "METRICS_ADDR" {
				foundEnv = true
				appEqual(t, e["value"], ":9090", name+" metrics listen")
			}
		}
		want := name != "ui"
		if foundPort != want || foundEnv != want {
			t.Fatalf("%s metrics port=%v env=%v, want %v", name, foundPort, foundEnv, want)
		}
	}
	checkAppRoutes(t, docs) // The public ingress retains only API-v1 and UI routes.
}
