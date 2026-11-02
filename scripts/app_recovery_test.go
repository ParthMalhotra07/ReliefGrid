//go:build integration

package scripts_test

import "testing"

func TestAppAPIRecoveryBudget(t *testing.T) {
	docs := renderApp(t, "--set", "app.enabled=true", "--set", "storage.enabled=false")
	var found bool
	for _, d := range docs {
		if d["kind"] != "PodDisruptionBudget" {
			continue
		}
		meta := d["metadata"].(map[string]any)
		labels, _ := meta["labels"].(map[string]any)
		if labels["app.kubernetes.io/component"] != "api" {
			continue
		}
		found = true
		spec := d["spec"].(map[string]any)
		if spec["minAvailable"] != 1 {
			t.Fatalf("API PDB minAvailable=%v, want1", spec["minAvailable"])
		}
		selector := spec["selector"].(map[string]any)["matchLabels"].(map[string]any)
		if selector["app.kubernetes.io/component"] != "api" || selector["app.kubernetes.io/instance"] != "reliefgrid" {
			t.Fatalf("PDB does not select only this release API: %v", selector)
		}
	}
	if !found {
		t.Fatal("API minAvailable=1 PodDisruptionBudget missing")
	}
}
