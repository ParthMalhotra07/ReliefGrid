//go:build integration

package scripts_test

import (
	"encoding/base64"
	"net/url"
	"testing"
)

func TestAppUIBrokerMonitorIdentity(t *testing.T) {
	docs := renderApp(t, "--set", "app.enabled=true")
	secret := appDocument(t, docs, "Secret", "reliefgrid-rabbitmq")
	data := appField(t, secret, "data").(map[string]any)
	decode := func(key string) string {
		t.Helper()
		value, ok := data[key].(string)
		if !ok {
			t.Fatalf("missing independent UI credential: %s", key)
		}
		raw, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	user, password := decode("ui-monitor-username"), decode("ui-monitor-password")
	if user == decode("username") || password == decode("password") || len(password) < 32 {
		t.Fatal("UI must not reuse application credentials")
	}
	address, err := url.Parse(decode("ui-management-url"))
	if err != nil {
		t.Fatal("invalid UI management origin")
	}
	actual, _ := address.User.Password()
	if address.User.Username() != user || actual != password {
		t.Fatal("UI origin does not use its own identity")
	}
	pod := appDocument(t, docs, "Deployment", "reliefgrid-ui")
	containers := appField(t, pod, "spec", "template", "spec", "containers").([]any)
	found := false
	for _, item := range containers[0].(map[string]any)["env"].([]any) {
		env := item.(map[string]any)
		if env["name"] == "AMQP_URL" {
			t.Fatal("UI must not receive the application AMQP credential")
		}
		if value, ok := env["valueFrom"].(map[string]any); ok {
			if secret, ok := value["secretKeyRef"].(map[string]any); ok && secret["name"] == "reliefgrid-rabbitmq" && secret["key"] != "ui-management-url" {
				t.Fatal("UI received a privileged broker Secret key")
			}
		}
		if env["name"] == "UI_BROKER_MANAGEMENT_URL" {
			found = true
			appEqual(t, appField(t, env, "valueFrom", "secretKeyRef", "key"), "ui-management-url", "UI read-only identity")
		}
	}
	if !found {
		t.Fatal("UI management credential missing")
	}
	sts := appDocument(t, docs, "StatefulSet", "reliefgrid-rabbitmq")
	appEqual(t, appField(t, sts, "spec", "template", "metadata", "annotations", "reliefgrid.io/broker-accounts"), "ui-monitor-v2", "migration rollout")
}
