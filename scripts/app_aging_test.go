//go:build integration

package scripts_test

import "testing"

func TestAppAgingIsExplicitMatcherPolicy(t *testing.T) {
	for _, interval := range []string{"0s", "1m"} {
		docs := renderApp(t, "--set", "app.enabled=true", "--set-string", "app.agingInterval="+interval)
		for _, component := range []string{"api", "matcher", "ui"} {
			d := appDocument(t, docs, "Deployment", "reliefgrid-"+component)
			containers := appField(t, d, "spec", "template", "spec", "containers").([]any)
			env := appEnvironment(t, containers[0].(map[string]any))
			if component == "matcher" {
				appEqual(t, appField(t, env["AGING_INTERVAL"], "value"), interval, "matcher aging policy")
			} else if _, ok := env["AGING_INTERVAL"]; ok {
				t.Fatal("aging enabled outside matcher")
			}
		}
	}
}
