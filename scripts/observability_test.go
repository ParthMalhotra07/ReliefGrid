//go:build integration

package scripts_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestObservabilityChartIsPrivateAndBounded(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "helm", "template", "observability", filepath.Join("..", "deploy", "helm", "observability"), "-n", "reliefgrid")
	body, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	var docs []map[string]any
	var promConfig string
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if len(doc) == 0 {
			continue
		}
		docs = append(docs, doc)
		switch doc["kind"] {
		case "Ingress", "ClusterRole", "ClusterRoleBinding":
			t.Fatalf("monitoring acquired public routing or cluster-wide permissions: %v", doc["kind"])
		case "Service":
			if spec := doc["spec"].(map[string]any); spec["type"] != "ClusterIP" {
				t.Fatal("public monitoring service")
			}
		case "Role":
			rules := doc["rules"].([]any)
			if len(rules) != 1 {
				t.Fatal("excess discovery permissions")
			}
			r := rules[0].(map[string]any)
			resources := r["resources"].([]any)
			if len(resources) != 1 || resources[0] != "pods" {
				t.Fatalf("excess resources: %v", resources)
			}
			for _, v := range r["verbs"].([]any) {
				if v != "get" && v != "list" && v != "watch" {
					t.Fatal("write RBAC")
				}
			}
		case "ConfigMap":
			data := doc["data"].(map[string]any)
			if v, ok := data["prometheus.yml"]; ok {
				promConfig = v.(string)
			}
		case "Deployment":
			pod := appField(t, doc, "spec", "template", "spec").(map[string]any)
			if appField(t, pod, "nodeSelector", "role") != "infra" {
				t.Fatal("monitoring must stay on infra worker")
			}
			for _, raw := range pod["containers"].([]any) {
				container := raw.(map[string]any)
				if !strings.Contains(container["image"].(string), "@sha256:") {
					t.Fatal("unpinned monitoring image")
				}
				if !appField(t, container, "securityContext", "readOnlyRootFilesystem").(bool) {
					t.Fatal("writable image filesystem")
				}
			}
		}
	}
	if len(docs) < 9 || promConfig == "" {
		t.Fatal("incomplete monitoring chart")
	}
	var config map[string]any
	if err := yaml.Unmarshal([]byte(promConfig), &config); err != nil {
		t.Fatal(err)
	}
	jobs := config["scrape_configs"].([]any)
	if len(jobs) != 2 {
		t.Fatal("missing app/broker scrape job")
	}
	broker := jobs[1].(map[string]any)
	if broker["metrics_path"] != "/metrics/detailed" {
		t.Fatal("unbounded per-object broker scrape")
	}
	if !strings.Contains(promConfig, "matching") || strings.Contains(promConfig, "labelmap") {
		t.Fatal("queue whitelist or bounded labels missing")
	}
	if path := os.Getenv("A42_PROM_CONFIG"); path != "" {
		if err := os.WriteFile(path, []byte(promConfig), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := filepath.Glob(filepath.Join("..", "deploy", "helm", "observability", "dashboards", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Fatalf("dashboards=%d, want3", len(paths))
	}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var dashboard struct {
			UID    string
			Panels []struct {
				Type    string
				Targets []struct{ Expr string }
			}
		}
		if err := json.Unmarshal(b, &dashboard); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(dashboard.UID, "reliefgrid-") || len(dashboard.Panels) < 6 {
			t.Fatal("incomplete dashboard")
		}
		for _, p := range dashboard.Panels {
			if p.Type == "text" {
				continue
			}
			if len(p.Targets) == 0 || p.Targets[0].Expr == "" {
				t.Fatal("missing real query")
			}
			for _, q := range p.Targets {
				if strings.Contains(q.Expr, "vector(0)") {
					t.Fatal("missing telemetry cannot be fabricated as zero")
				}
			}
		}
	}
}
