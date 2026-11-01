//go:build integration

package scripts_test

import "testing"

func TestAppDemoProfileHasThirtySecondEvictionTolerations(t *testing.T) {
	docs := renderApp(t, "-f", "../deploy/helm/reliefgrid/values-app-kind.yaml")
	for _, name := range []string{"reliefgrid-api", "reliefgrid-matcher", "reliefgrid-ui"} {
		d := appDocument(t, docs, "Deployment", name)
		pod := appField(t, d, "spec", "template", "spec").(map[string]any)
		tolerations, _ := pod["tolerations"].([]any)
		for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
			found := false
			for _, item := range tolerations {
				tol := item.(map[string]any)
				if tol["key"] == key && tol["operator"] == "Exists" && tol["effect"] == "NoExecute" && tol["tolerationSeconds"] == 30 {
					found = true
				}
			}
			if !found {
				t.Errorf("%s lacks a 30-second NoExecute toleration for %s", name, key)
			}
		}
	}
}
