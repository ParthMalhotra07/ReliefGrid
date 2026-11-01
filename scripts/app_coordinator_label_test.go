//go:build integration

package scripts_test

import "testing"

// NetworkPolicy #40 selects pod labels, not Deployment labels. The relay is
// hosted by matcher. Future reconciliation workers need the same pod label.
func TestAppCoordinatorClientsCarryNetworkPolicyLabel(t *testing.T) {
	const label = "reliefgrid.io/coordinator-client"
	for _, storage := range []string{"true", "false"} {
		t.Run("storage="+storage, func(t *testing.T) {
			docs := renderApp(t, "-f", "../deploy/helm/reliefgrid/values-app-kind.yaml", "--set", "storage.enabled="+storage)
			for _, name := range []string{"reliefgrid-api", "reliefgrid-matcher", "reliefgrid-ui"} {
				d := appDocument(t, docs, "Deployment", name)
				labels := appField(t, d, "spec", "template", "metadata", "labels").(map[string]any)
				if name == "reliefgrid-ui" {
					if _, ok := labels[label]; ok {
						t.Error("UI must not acquire coordinator access")
					}
					continue
				}
				appEqual(t, labels[label], "true", name+" coordinator policy admission")
				selector := appField(t, d, "spec", "selector", "matchLabels").(map[string]any)
				if _, ok := selector[label]; ok {
					t.Error("policy admission must not change immutable Deployment selectors")
				}
			}
		})
	}
}
