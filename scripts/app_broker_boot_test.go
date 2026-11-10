//go:build integration

package scripts_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// RabbitMQ must own definition import in its boot sequence, without racing
// the entrypoint in a postStart CLI. A blank broker importing definitions does not
// create its default user, so the import must include the configured credentials.
func TestAppBrokerNativeBootImport(t *testing.T) {
	for _, credentials := range []struct{ user, password string }{
		{"reliefgrid", "reliefgrid"}, {"reliefgrid-ui-monitor", "collision-fixture"}, {"boot-test", `custom:p@ss/word"`},
	} {
		t.Run(credentials.user, func(t *testing.T) {
			docs := renderApp(t, "--set-string", "rabbitmq.username="+credentials.user,
				"--set-string", "rabbitmq.password="+credentials.password)
			sts := appDocument(t, docs, "StatefulSet", "reliefgrid-rabbitmq")
			pod := appField(t, sts, "spec", "template", "spec").(map[string]any)
			c := appField(t, pod, "containers").([]any)[0].(map[string]any)
			if _, ok := c["lifecycle"]; ok {
				t.Fatal("boot import must not run in a concurrent postStart CLI")
			}
			cm := appDocument(t, docs, "ConfigMap", "reliefgrid-rabbitmq-definitions")
			config := appField(t, cm, "data", "rabbitmq.conf").(string)
			for _, want := range []string{"definitions.import_backend = local_filesystem", "definitions.local.path = /etc/reliefgrid/definitions.json", "definitions.skip_if_unchanged = true"} {
				if !strings.Contains(config, want) {
					t.Fatalf("missing boot configuration %q", want)
				}
			}
			secret := appDocument(t, docs, "Secret", "reliefgrid-rabbitmq")
			raw, err := base64.StdEncoding.DecodeString(appField(t, secret, "data", "definitions.json").(string))
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			decode := func(key string) string {
				t.Helper()
				value, err := base64.StdEncoding.DecodeString(appField(t, secret, "data", key).(string))
				if err != nil {
					t.Fatal(err)
				}
				return string(value)
			}
			monitorUser, monitorPassword := decode("ui-monitor-username"), decode("ui-monitor-password")
			if monitorUser == credentials.user || monitorPassword == credentials.password || len(monitorPassword) < 32 {
				t.Fatal("monitor identity must be independent")
			}
			wantUsers := []any{map[string]any{"name": credentials.user, "password": credentials.password, "tags": "administrator"}, map[string]any{"name": monitorUser, "password": monitorPassword, "tags": "management"}}
			wantPermissions := []any{map[string]any{"user": credentials.user, "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}, map[string]any{"user": monitorUser, "vhost": "/", "configure": "^$", "write": "^$", "read": "^$"}}
			if !reflect.DeepEqual(got["users"], wantUsers) || !reflect.DeepEqual(got["permissions"], wantPermissions) {
				t.Fatal("boot import must provision configured user and permissions")
			}
			delete(got, "users")
			delete(got, "permissions")
			canonical, err := os.ReadFile(filepath.Join("..", "deploy", "helm", "reliefgrid", "files", "rabbitmq-definitions.json"))
			if err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			if err := json.Unmarshal(canonical, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("native import changed the canonical topology")
			}
			// Compare the whole public document: substring matching a default
			// password such as "reliefgrid" also matches non-secret path names.
			var public map[string]any
			if err := json.Unmarshal([]byte(appField(t, cm, "data", "definitions.json").(string)), &public); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(public, want) {
				t.Fatal("ConfigMap must contain only canonical non-secret topology")
			}
			volumes := appField(t, pod, "volumes").([]any)
			mountedSecret, mountedConfig := false, false
			for _, v := range volumes {
				volume := v.(map[string]any)
				if volume["name"] == "definitions" {
					appEqual(t, appField(t, volume, "secret", "secretName"), "reliefgrid-rabbitmq", "secret definitions")
					mountedSecret = true
				}
			}
			for _, m := range c["volumeMounts"].([]any) {
				mount := m.(map[string]any)
				if mount["mountPath"] == "/etc/rabbitmq/conf.d/20-reliefgrid.conf" {
					appEqual(t, mount["subPath"], "rabbitmq.conf", "config snippet")
					mountedConfig = true
				}
			}
			if !mountedSecret || !mountedConfig {
				t.Fatal("boot definitions and native config must be mounted without replacing Docker defaults")
			}
		})
	}
}
