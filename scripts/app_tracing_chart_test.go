//go:build integration

package scripts_test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// S4.4 part (b) deployment: the app chart's tracing switch. Off by default,
// the API and matcher get no OTEL_* variables (their binaries then install
// only the W3C propagator); deploy/helm/reliefgrid/values-tracing.yaml turns
// it on, and make tracing-up / tracing-down switch the app release too.
// Rendering only: no cluster is touched.

const appTracingOverlay = "../deploy/helm/reliefgrid/values-tracing.yaml"

// appTracingRender renders the kind app profile without the storage subchart
// (storage is its own release, as make app-deploy installs it).
func appTracingRender(t *testing.T, flags ...string) []map[string]any {
	t.Helper()
	return renderApp(t, append([]string{"-f", "../deploy/helm/reliefgrid/values-app-kind.yaml", "--set", "storage.enabled=false"}, flags...)...)
}

// appTracingWorkload is the Deployment whose pods have component.
func appTracingWorkload(t *testing.T, docs []map[string]any, component string) map[string]any {
	t.Helper()
	for _, d := range docs {
		if d["kind"] != "Deployment" {
			continue
		}
		if labels := appField(t, d, "spec", "template", "metadata", "labels").(map[string]any); labels["app.kubernetes.io/component"] == component {
			return d
		}
	}
	t.Fatalf("no %s Deployment rendered", component)
	return nil
}

// appTracingEnv is the workload's first container's env as name -> value.
func appTracingEnv(t *testing.T, w map[string]any) map[string]string {
	t.Helper()
	c := appField(t, w, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)
	out := map[string]string{}
	for name, e := range appEnvironment(t, c) {
		out[name] = fmt.Sprint(e["value"])
	}
	return out
}

// appTracingEgressAllowsCollector reports whether a NetworkPolicy has an
// egress rule to the Collector's pods on TCP 4318.
func appTracingEgressAllowsCollector(np map[string]any) bool {
	rules, _ := np["spec"].(map[string]any)["egress"].([]any)
	for _, r := range rules {
		rule, _ := r.(map[string]any)
		to, ports := false, false
		for _, peer := range asList(rule["to"]) {
			sel, _ := peer.(map[string]any)["podSelector"].(map[string]any)
			labels, _ := sel["matchLabels"].(map[string]any)
			to = to || labels["app.kubernetes.io/name"] == "otel-collector"
		}
		for _, p := range asList(rule["ports"]) {
			pm, _ := p.(map[string]any)
			ports = ports || fmt.Sprint(pm["port"]) == "4318" && (pm["protocol"] == nil || pm["protocol"] == "TCP")
		}
		if to && ports {
			return true
		}
	}
	return false
}

func asList(v any) []any { l, _ := v.([]any); return l }

// appTracingEgressPolicies are the NetworkPolicies restricting egress of pods
// with labels.
func appTracingEgressPolicies(docs []map[string]any, labels map[string]any) []map[string]any {
	var out []map[string]any
	for _, d := range docs {
		if d["kind"] != "NetworkPolicy" {
			continue
		}
		spec, _ := d["spec"].(map[string]any)
		egress := false
		for _, pt := range asList(spec["policyTypes"]) {
			egress = egress || pt == "Egress"
		}
		sel, _ := spec["podSelector"].(map[string]any)
		ml, _ := sel["matchLabels"].(map[string]any)
		match := true
		for k, v := range ml {
			match = match && labels[k] == v
		}
		if egress && match {
			out = append(out, d)
		}
	}
	return out
}

func TestAppChartTracingIsOffByDefault(t *testing.T) {
	docs := appTracingRender(t)
	for _, comp := range []string{"api", "matcher", "ui"} {
		for k := range appTracingEnv(t, appTracingWorkload(t, docs, comp)) {
			if strings.HasPrefix(k, "OTEL_") {
				t.Errorf("%s sets %s with tracing off", comp, k)
			}
		}
	}
	for _, d := range docs {
		if d["kind"] == "NetworkPolicy" && appTracingEgressAllowsCollector(d) {
			t.Errorf("NetworkPolicy %v allows the Collector with tracing off", appField(t, d, "metadata", "name"))
		}
	}
}

// With values-tracing.yaml the API and matcher export to the Collector with
// the storage chart's sampler settings, named api and matcher; the UI stays
// untraced; and any NetworkPolicy restricting their egress allows the
// Collector on 4318 (the chart has none today, so this guards a future one).
func TestAppChartTracingOverlay(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		ratio string
	}{
		{"demo", []string{"-f", appTracingOverlay}, "1.0"},
		{"load", []string{"-f", appTracingOverlay, "--set-string", "app.tracing.samplerArg=0.1"}, "0.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs := appTracingRender(t, tc.args...)
			for _, comp := range []string{"api", "matcher"} {
				w := appTracingWorkload(t, docs, comp)
				e := appTracingEnv(t, w)
				if ep := e["OTEL_EXPORTER_OTLP_ENDPOINT"]; !strings.Contains(ep, "otel-collector") || !strings.HasSuffix(ep, ":4318") {
					t.Errorf("%s OTEL_EXPORTER_OTLP_ENDPOINT = %q, want the Collector on 4318", comp, ep)
				}
				if e["OTEL_TRACES_SAMPLER"] != "parentbased_traceidratio" || e["OTEL_TRACES_SAMPLER_ARG"] != tc.ratio {
					t.Errorf("%s sampler %q arg %q, want parentbased_traceidratio %s", comp, e["OTEL_TRACES_SAMPLER"], e["OTEL_TRACES_SAMPLER_ARG"], tc.ratio)
				}
				if e["OTEL_SERVICE_NAME"] != comp {
					t.Errorf("%s OTEL_SERVICE_NAME = %q, want %q", comp, e["OTEL_SERVICE_NAME"], comp)
				}
				labels := appField(t, w, "spec", "template", "metadata", "labels").(map[string]any)
				for _, np := range appTracingEgressPolicies(docs, labels) {
					if !appTracingEgressAllowsCollector(np) {
						t.Errorf("NetworkPolicy %v restricts %s egress without allowing the Collector on 4318", appField(t, np, "metadata", "name"), comp)
					}
				}
			}
			for k := range appTracingEnv(t, appTracingWorkload(t, docs, "ui")) {
				if strings.HasPrefix(k, "OTEL_") {
					t.Errorf("ui sets %s; the UI does not export spans", k)
				}
			}
		})
	}
}

// make tracing-up turns tracing on in the app release as well as storage;
// tracing-down turns it off there before uninstalling the Collector.
func TestAppMakeTracingTargetsCoverTheApp(t *testing.T) {
	for target, want := range map[string][]string{
		"tracing-up":   {"deploy/helm/reliefgrid", "reliefgrid/values-tracing.yaml"},
		"tracing-down": {"deploy/helm/reliefgrid", "app.tracing.enabled=false"},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		out, err := exec.CommandContext(ctx, "make", "-n", "-C", "..", target).CombinedOutput()
		cancel()
		if err != nil {
			t.Errorf("make -n %s: %v\n%s", target, err, out)
			continue
		}
		for _, w := range want {
			if !strings.Contains(string(out), w) {
				t.Errorf("make -n %s does not mention %q:\n%s", target, w, out)
			}
		}
	}
}
