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
		t.Run(name, func(t *testing.T) {
			d := appDocument(t, docs, "Deployment", "reliefgrid-"+name)
			spec := appField(t, d, "spec", "template", "spec").(map[string]any)
			if name == "ui" {
				encoded, err := json.Marshal(d)
				if err != nil {
					t.Fatal(err)
				}
				for _, forbidden := range []string{"INTERNAL_AUTH_TOKEN_FILE", "reliefgrid-internal-auth", "reliefgrid-node-auth", "/var/run/reliefgrid/internal-auth"} {
					if strings.Contains(string(encoded), forbidden) {
						t.Fatalf("UI received %s", forbidden)
					}
				}
				return
			}
			appEqual(t, appField(t, spec, "securityContext", "fsGroup"), 65532, name+" credential file group")
			volumes := 0
			for _, raw := range spec["volumes"].([]any) {
				volume := raw.(map[string]any)
				if volume["name"] != "internal-auth" {
					continue
				}
				volumes++
				secret := volume["secret"].(map[string]any)
				appEqual(t, secret["secretName"], "reliefgrid-internal-auth", name+" secret")
				appEqual(t, secret["defaultMode"], 0o440, name+" secret mode")
				items := secret["items"].([]any)
				if len(items) != 1 {
					t.Fatal("app got receiver keys or omitted sender key")
				}
				item := items[0].(map[string]any)
				appEqual(t, item["key"], "sender-token", name+" projected Secret key")
				appEqual(t, item["path"], "sender-token", name+" projected filename")
			}
			appEqual(t, volumes, 1, name+" credential volume count")
			for _, raw := range spec["containers"].([]any) {
				container := raw.(map[string]any)
				environment := appEnvironment(t, container)
				entry, ok := environment["INTERNAL_AUTH_TOKEN_FILE"]
				if !ok {
					t.Fatal("sender-file environment missing")
				}
				appEqual(t, entry["value"], "/var/run/reliefgrid/internal-auth/sender-token", name+" sender-file path")
				if entry["valueFrom"] != nil {
					t.Fatal("sender path replaced by inline Secret content")
				}
				mounts := 0
				for _, raw := range container["volumeMounts"].([]any) {
					mount := raw.(map[string]any)
					if mount["name"] != "internal-auth" {
						continue
					}
					mounts++
					appEqual(t, mount["mountPath"], "/var/run/reliefgrid/internal-auth", name+" credential mount directory")
					appEqual(t, mount["readOnly"], true, name+" read-only credential mount")
				}
				appEqual(t, mounts, 1, name+" credential mount count")
			}
		})
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
