//go:build integration

package scripts_test

import (
	"path/filepath"
	"testing"
)

func TestAppInternalAuthMountsExcludeUI(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"))
	for _, name := range []string{"api", "matcher", "ui"} {
		d := appDocument(t, docs, "Deployment", "reliefgrid-"+name)
		spec := appField(t, d, "spec", "template", "spec").(map[string]any)
		found := false
		if volumes, ok := spec["volumes"].([]any); ok {
			for _, raw := range volumes {
				v := raw.(map[string]any)
				if v["name"] != "internal-auth" {
					continue
				}
				found = true
				secret := v["secret"].(map[string]any)
				appEqual(t, secret["secretName"], "reliefgrid-internal-auth", name+" secret")
				items := secret["items"].([]any)
				if len(items) != 1 {
					t.Fatal("app got receiver keys")
				}
				appEqual(t, items[0].(map[string]any)["key"], "sender-token", name+" sender key")
			}
		}
		if found != (name != "ui") {
			t.Fatalf("%s auth mount=%v", name, found)
		}
	}
}
