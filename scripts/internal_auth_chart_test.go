package scripts

import (
	"context"
	"go.yaml.in/yaml/v3"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestInternalAuthChartSecretsAndMounts(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not on PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, helm, "template", "storage", "../deploy/helm/storage", "--set", "internalAuth.enabled=true", "--set", "internalAuth.token=app-current", "--set", "nodeAuth.token=node-new", "--set", "nodeAuth.previousToken=node-old", "--set", "nodeAuth.senderToken=node-old").Output()
	if err != nil {
		t.Fatalf("helm: %v: %s", err, out)
	}
	secrets := map[string]map[string]any{}
	pods := 0
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		var v map[string]any
		if err := yaml.Unmarshal([]byte(doc), &v); err != nil {
			t.Fatalf("decode: %v\n%s", err, doc)
		}
		meta, _ := v["metadata"].(map[string]any)
		if v["kind"] == "Secret" {
			name, _ := meta["name"].(string)
			data, _ := v["data"].(map[string]any)
			secrets[name] = data
		}
		if v["kind"] == "Deployment" || v["kind"] == "StatefulSet" {
			pods++
			for _, fragment := range []string{"name: node-auth", "secretName: reliefgrid-node-auth", "mountPath: /var/run/reliefgrid/node-auth", "name: NODE_AUTH_SENDER_TOKEN_FILE"} {
				if !strings.Contains(doc, fragment) {
					t.Errorf("%v missing %s", meta["name"], fragment)
				}
			}
			if v["kind"] == "Deployment" && !strings.Contains(doc, "secretName: reliefgrid-internal-auth") {
				t.Error("coordinator missing app receiver secret")
			}
			if v["kind"] == "StatefulSet" && strings.Contains(doc, "reliefgrid-internal-auth") {
				t.Error("node received app secret")
			}
		}
	}
	if pods != 4 {
		t.Fatalf("pods=%d", pods)
	}
	node := secrets["reliefgrid-node-auth"]
	if node["token"] != "bm9kZS1uZXc=" || node["sender-token"] != "bm9kZS1vbGQ=" || node["previous-token"] != "bm9kZS1vbGQ=" {
		t.Fatalf("rotation staging incorrect: %v", node)
	}
	app := secrets["reliefgrid-internal-auth"]
	if app["token"] != "YXBwLWN1cnJlbnQ=" || app["sender-token"] != app["token"] {
		t.Fatal("app key not independent")
	}
}

func TestInternalAuthDisabledChart(t *testing.T) {
	for _, flags := range [][]string{nil, {"--set", "internalAuth.enabled=false", "--set", "nodeAuth.previousToken=old"}} {
		args := append([]string{"template", "storage", "../deploy/helm/storage"}, flags...)
		out, err := exec.CommandContext(t.Context(), "helm", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("helm: %v: %s", err, out)
		}
		for _, fragment := range []string{"reliefgrid-internal-auth", "reliefgrid-node-auth", "AUTH_TOKEN_FILE", "AUTH_ENABLED", "name: node-auth", "name: internal-auth"} {
			if strings.Contains(string(out), fragment) {
				t.Errorf("disabled chart contains %s", fragment)
			}
		}
	}
}

func TestInternalAuthRotationRequiresSender(t *testing.T) {
	for _, kind := range []string{"internal", "node"} {
		for _, sender := range []string{"", "old", "new"} {
			args := []string{"template", "storage", "../deploy/helm/storage", "--set", "internalAuth.enabled=true", "--set", kind + "Auth.token=new", "--set", kind + "Auth.previousToken=old"}
			if sender != "" {
				args = append(args, "--set", kind+"Auth.senderToken="+sender)
			}
			out, err := exec.CommandContext(t.Context(), "helm", args...).CombinedOutput()
			if sender == "" {
				if err == nil || !strings.Contains(string(out), kind+"Auth.senderToken must be explicit") {
					t.Fatalf("missing sender accepted: %v %s", err, out)
				}
			} else if err != nil {
				t.Fatalf("explicit sender refused: %v %s", err, out)
			}
		}
	}
}
