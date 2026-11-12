//go:build integration

package scripts_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppInternalAuthMountsExcludeUI(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"), "--set", "internalAuth.enabled=true")
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

func TestAppInternalAuthDisabled(t *testing.T) {
	for _, extra := range [][]string{nil, {"--set", "internalAuth.enabled=false"}} {
		flags := append([]string{"-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml")}, extra...)
		docs := renderApp(t, flags...)
		for _, name := range []string{"api", "matcher", "ui"} {
			d := appDocument(t, docs, "Deployment", "reliefgrid-"+name)
			encoded, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range []string{"internal-auth", "INTERNAL_AUTH_TOKEN_FILE"} {
				if strings.Contains(string(encoded), s) {
					t.Fatalf("disabled %s contains %s", name, s)
				}
			}
		}
	}
}
