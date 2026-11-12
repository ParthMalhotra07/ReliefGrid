package scripts

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// The storage chart's half of docs/operations/storage-metrics.md (#113):
// storagenode and coordinator pods expose a container port named metrics on
// 9090 with prometheus.io/scrape, and their NetworkPolicies open 9090, and
// only 9090, to pods labelled reliefgrid.io/metrics-scraper=true. The mgmt
// port (8081) stays operator-only and the data port keeps its rules.

func renderStorage(t *testing.T, flags ...string) []map[string]any {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("REQUIRE_HELM") != "" {
			t.Fatalf("REQUIRE_HELM is set but helm is not on PATH: %v", err)
		}
		t.Skip("helm not on PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	args := append([]string{"template", "storage", "../deploy/helm/storage", "--namespace", "reliefgrid"}, flags...)
	out, err := exec.CommandContext(ctx, helm, args...).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("helm template: %v\n%s", err, ee.Stderr)
		}
		t.Fatalf("helm template: %v", err)
	}
	var docs []map[string]any
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		var m map[string]any
		if err := yaml.Unmarshal([]byte(doc), &m); err != nil {
			t.Fatal(err)
		}
		if m != nil {
			docs = append(docs, m)
		}
	}
	return docs
}

func TestStorageChartMetricsPort(t *testing.T) {
	workloads := 0
	for _, d := range renderStorage(t) {
		kind := d["kind"]
		if kind != "StatefulSet" && kind != "Deployment" {
			continue
		}
		name := dig(d, "metadata", "name").(string)
		if comp := dig(d, "metadata", "labels", "app.kubernetes.io/component"); comp != "storagenode" && comp != "coordinator" {
			continue
		}
		workloads++
		if dig(d, "spec", "template", "metadata", "annotations", "prometheus.io/scrape") != "true" {
			t.Errorf("%s: pod lacks prometheus.io/scrape: \"true\"", name)
		}
		c := dig(d, "spec", "template", "spec", "containers", 0)
		found := false
		for _, p := range dig(c, "ports").([]any) {
			if dig(p, "name") == "metrics" {
				found = dig(p, "containerPort") == 9090
			}
		}
		if !found {
			t.Errorf("%s: no container port metrics=9090", name)
		}
		env := map[string]any{}
		for _, e := range dig(c, "env").([]any) {
			env[dig(e, "name").(string)] = dig(e, "value")
		}
		if env["METRICS_ADDR"] != ":9090" {
			t.Errorf("%s: METRICS_ADDR = %v, want :9090", name, env["METRICS_ADDR"])
		}
	}
	if workloads != 4 {
		t.Fatalf("checked %d storage workloads, want 4 (three shards and the coordinator)", workloads)
	}
}

func TestStorageChartMetricsPolicy(t *testing.T) {
	policies := 0
	for _, d := range renderStorage(t) {
		if d["kind"] != "NetworkPolicy" {
			continue
		}
		name := dig(d, "metadata", "name").(string)
		if strings.HasSuffix(name, "-raft") {
			for _, rule := range dig(d, "spec", "ingress").([]any) {
				for _, p := range dig(rule, "ports").([]any) {
					if dig(p, "port") != 7000 {
						t.Errorf("%s: the Raft policy opens port %v", name, dig(p, "port"))
					}
				}
			}
			continue
		}
		policies++
		// port -> the label every peer of that rule must carry ("" = any pod)
		peers := map[int][]string{}
		for _, rule := range dig(d, "spec", "ingress").([]any) {
			var labels []string
			for _, from := range dig(rule, "from").([]any) {
				ml, _ := dig(from, "podSelector", "matchLabels").(map[string]any)
				var keys []string
				for k := range ml {
					if strings.HasPrefix(k, "reliefgrid.io/") {
						keys = append(keys, k)
					}
				}
				slices.Sort(keys)
				labels = append(labels, strings.Join(keys, ","))
			}
			for _, p := range dig(rule, "ports").([]any) {
				port := dig(p, "port").(int)
				peers[port] = append(peers[port], labels...)
			}
		}
		if got := peers[9090]; !slices.Equal(got, []string{"reliefgrid.io/metrics-scraper"}) {
			t.Errorf("%s: 9090 is open to %q, want only reliefgrid.io/metrics-scraper pods", name, got)
		}
		if slices.Contains(peers[8081], "reliefgrid.io/metrics-scraper") || slices.Contains(peers[8080], "reliefgrid.io/metrics-scraper") {
			t.Errorf("%s: the scraper gets a port other than 9090: %v", name, peers)
		}
		if name != "storage-coordinator" && !slices.Equal(peers[8081], []string{"reliefgrid.io/storage-operator"}) {
			t.Errorf("%s: mgmt 8081 is open to %q, want only operator pods", name, peers[8081])
		}
	}
	if policies != 4 {
		t.Fatalf("checked %d policies, want 4 (three shards and the coordinator)", policies)
	}
}
