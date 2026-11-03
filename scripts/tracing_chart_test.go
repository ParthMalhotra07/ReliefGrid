package scripts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// S4.4 deployment: the storage chart's tracing switch (off by default, on
// with values-tracing.yaml), the tracing chart (OTel Collector core and
// Jaeger v2, both pinned by digest, on the infra worker, no route), and
// make tracing-up / tracing-down. Rendering only: no cluster is touched.

type obj = map[string]any

// helmRender renders chart with args and returns its objects.
func helmRender(t *testing.T, chart string, args ...string) []obj {
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
	cmd := append([]string{"template", "rel", chart, "--namespace", "reliefgrid"}, args...)
	out, err := exec.CommandContext(ctx, helm, cmd...).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("helm %s: %v\n%s", strings.Join(cmd, " "), err, ee.Stderr)
		}
		t.Fatalf("helm %s: %v", strings.Join(cmd, " "), err)
	}
	var objs []obj
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var o obj
		if err := dec.Decode(&o); err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decode %s: %v", chart, err)
		}
		if o != nil {
			objs = append(objs, o)
		}
	}
	return objs
}

// dig walks m by keys; ints index lists.
func dig(m any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			mm, ok := m.(obj)
			if !ok {
				return nil
			}
			m = mm[k]
		case int:
			l, ok := m.([]any)
			if !ok || k >= len(l) {
				return nil
			}
			m = l[k]
		}
	}
	return m
}

func list(v any) []any { l, _ := v.([]any); return l }

func kindOf(o obj) string { s, _ := o["kind"].(string); return s }
func nameOf(o obj) string { s, _ := dig(o, "metadata", "name").(string); return s }

// workloads are the Deployments and StatefulSets whose pod labels have
// component (coordinator or storagenode).
func workloads(objs []obj, component string) []obj {
	var out []obj
	for _, o := range objs {
		if k := kindOf(o); k != "Deployment" && k != "StatefulSet" {
			continue
		}
		if dig(o, "spec", "template", "metadata", "labels", "app.kubernetes.io/component") == component {
			out = append(out, o)
		}
	}
	return out
}

// env is the first container's environment as name -> value.
func env(o obj) map[string]string {
	out := map[string]string{}
	for _, e := range list(dig(o, "spec", "template", "spec", "containers", 0, "env")) {
		name, _ := dig(e, "name").(string)
		val, _ := dig(e, "value").(string)
		out[name] = val
	}
	return out
}

// selects reports whether a NetworkPolicy's podSelector.matchLabels is a
// subset of labels.
func selects(np obj, labels obj) bool {
	ml, _ := dig(np, "spec", "podSelector", "matchLabels").(obj)
	for k, v := range ml {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// allowsCollector reports whether a NetworkPolicy has an egress rule to the
// Collector's pods on TCP 4318.
func allowsCollector(np obj) bool {
	for _, rule := range list(dig(np, "spec", "egress")) {
		toCollector := false
		for _, to := range list(dig(rule, "to")) {
			if dig(to, "podSelector", "matchLabels", "app.kubernetes.io/name") == "otel-collector" {
				toCollector = true
			}
		}
		for _, p := range list(dig(rule, "ports")) {
			port := fmt.Sprint(dig(p, "port"))
			proto, _ := dig(p, "protocol").(string)
			if toCollector && port == "4318" && (proto == "" || proto == "TCP") {
				return true
			}
		}
	}
	return false
}

func hasEgress(np obj) bool {
	return slices.Contains(list(dig(np, "spec", "policyTypes")), any("Egress"))
}

const storageChart = "../deploy/helm/storage"

func TestStorageChartTracingIsOffByDefault(t *testing.T) {
	objs := helmRender(t, storageChart)
	for _, comp := range []string{"coordinator", "storagenode"} {
		ws := workloads(objs, comp)
		if len(ws) == 0 {
			t.Fatalf("no %s workload rendered", comp)
		}
		for _, w := range ws {
			for k := range env(w) {
				if strings.HasPrefix(k, "OTEL_EXPORTER_OTLP") {
					t.Errorf("%s %s sets %s with tracing off", kindOf(w), nameOf(w), k)
				}
			}
		}
	}
	for _, o := range objs {
		if kindOf(o) == "NetworkPolicy" && allowsCollector(o) {
			t.Errorf("NetworkPolicy %s allows the Collector with tracing off", nameOf(o))
		}
	}
}

// values-tracing.yaml turns tracing on: every coordinator and storage pod
// exports to the Collector, sampling parentbased_traceidratio at 1.0 (the
// demos; tracing.samplerArg=0.1 under load), and every NetworkPolicy that
// restricts their egress lets them reach the Collector on 4318.
func TestStorageChartTracingOverlay(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		ratio string
	}{
		{"demo", []string{"-f", storageChart + "/values-tracing.yaml"}, "1.0"},
		{"load", []string{"-f", storageChart + "/values-tracing.yaml", "--set-string", "tracing.samplerArg=0.1"}, "0.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := helmRender(t, storageChart, tc.args...)
			for _, comp := range []string{"coordinator", "storagenode"} {
				ws := workloads(objs, comp)
				if len(ws) == 0 {
					t.Fatalf("no %s workload rendered", comp)
				}
				for _, w := range ws {
					e := env(w)
					if ep := e["OTEL_EXPORTER_OTLP_ENDPOINT"]; !strings.Contains(ep, "otel-collector") || !strings.HasSuffix(ep, ":4318") {
						t.Errorf("%s OTEL_EXPORTER_OTLP_ENDPOINT = %q, want the Collector on 4318", nameOf(w), ep)
					}
					if e["OTEL_TRACES_SAMPLER"] != "parentbased_traceidratio" || e["OTEL_TRACES_SAMPLER_ARG"] != tc.ratio {
						t.Errorf("%s sampler %q arg %q, want parentbased_traceidratio %s", nameOf(w), e["OTEL_TRACES_SAMPLER"], e["OTEL_TRACES_SAMPLER_ARG"], tc.ratio)
					}
					if e["OTEL_SERVICE_NAME"] != comp {
						t.Errorf("%s OTEL_SERVICE_NAME = %q, want %q", nameOf(w), e["OTEL_SERVICE_NAME"], comp)
					}
				}
			}
			egressPolicies := map[string]int{}
			for _, np := range objs {
				if kindOf(np) != "NetworkPolicy" || !hasEgress(np) {
					continue
				}
				for _, comp := range []string{"coordinator", "storagenode"} {
					for _, w := range workloads(objs, comp) {
						labels, _ := dig(w, "spec", "template", "metadata", "labels").(obj)
						if !selects(np, labels) {
							continue
						}
						egressPolicies[comp]++
						if !allowsCollector(np) {
							t.Errorf("NetworkPolicy %s restricts %s egress without allowing the Collector on 4318", nameOf(np), nameOf(w))
						}
					}
				}
			}
			if egressPolicies["coordinator"] == 0 {
				t.Error("no egress NetworkPolicy selects the coordinator; this test no longer checks it")
			}
		})
	}
}

var digestRef = regexp.MustCompile(`^([^@:]+):[^@]+@sha256:[0-9a-f]{64}$`)

func TestTracingChart(t *testing.T) {
	objs := helmRender(t, "../deploy/helm/tracing")
	images := map[string]obj{} // repository -> workload
	for _, o := range objs {
		switch kindOf(o) {
		case "Ingress", "IngressRoute", "IngressRouteTCP", "HTTPRoute":
			t.Errorf("%s %s: Jaeger and the Collector have no route (keep them private)", kindOf(o), nameOf(o))
		case "Service":
			if typ, _ := dig(o, "spec", "type").(string); typ != "" && typ != "ClusterIP" {
				t.Errorf("Service %s is %s, want ClusterIP", nameOf(o), typ)
			}
		case "Deployment", "StatefulSet":
			img, _ := dig(o, "spec", "template", "spec", "containers", 0, "image").(string)
			m := digestRef.FindStringSubmatch(img)
			if m == nil {
				t.Errorf("%s image %q is not pinned as repo:tag@sha256:digest", nameOf(o), img)
				continue
			}
			images[m[1]] = o
			if dig(o, "spec", "template", "spec", "nodeSelector", "role") != "infra" {
				t.Errorf("%s is not pinned to the infra worker", nameOf(o))
			}
			tolerated := false
			for _, tol := range list(dig(o, "spec", "template", "spec", "tolerations")) {
				if dig(tol, "key") == "role" && dig(tol, "value") == "infra" && dig(tol, "effect") == "NoSchedule" {
					tolerated = true
				}
			}
			if !tolerated {
				t.Errorf("%s does not tolerate role=infra:NoSchedule", nameOf(o))
			}
		}
	}
	collector, jaeger := images["otel/opentelemetry-collector"], images["jaegertracing/jaeger"]
	if collector == nil {
		t.Errorf("no otel/opentelemetry-collector (core distro) workload; images: %v", keys(images))
	} else if dig(collector, "spec", "template", "metadata", "labels", "app.kubernetes.io/name") != "otel-collector" {
		t.Error("Collector pods are not labelled app.kubernetes.io/name=otel-collector (the NetworkPolicies select it)")
	}
	if jaeger == nil {
		t.Errorf("no jaegertracing/jaeger (v2) workload; images: %v", keys(images))
	}

	svcPorts := map[string][]string{}
	for _, o := range objs {
		if kindOf(o) == "Service" {
			for _, p := range list(dig(o, "spec", "ports")) {
				svcPorts[nameOf(o)] = append(svcPorts[nameOf(o)], fmt.Sprint(dig(p, "port")))
			}
		}
	}
	if !slices.Contains(svcPorts["otel-collector"], "4318") {
		t.Errorf("Service otel-collector ports %v, want 4318", svcPorts["otel-collector"])
	}

	col, jcfg := configDoc(t, objs, "otel-collector"), configDoc(t, objs, "jaeger")
	if dig(col, "receivers", "otlp", "protocols", "http") == nil {
		t.Error("Collector has no OTLP/HTTP receiver")
	}
	traces := dig(col, "service", "pipelines", "traces")
	if got := fmt.Sprint(dig(traces, "processors")); got != "[memory_limiter batch]" {
		t.Errorf("traces pipeline processors = %s, want [memory_limiter batch] (memory_limiter first)", got)
	}
	if dig(col, "processors", "memory_limiter", "limit_mib") == nil && dig(col, "processors", "memory_limiter", "limit_percentage") == nil {
		t.Error("memory_limiter has no limit")
	}
	exporters := list(dig(traces, "exporters"))
	if len(exporters) != 1 {
		t.Fatalf("traces pipeline exporters = %v, want one (to Jaeger)", exporters)
	}
	ep, _ := dig(col, "exporters", exporters[0], "endpoint").(string)
	if !strings.Contains(ep, "jaeger") || !strings.HasSuffix(ep, ":4318") {
		t.Errorf("exporter %v endpoint %q, want Jaeger's OTLP/HTTP on 4318", exporters[0], ep)
	}
	if !findMaxTraces(jcfg, 20000) {
		t.Error("Jaeger has no in-memory store with max_traces: 20000")
	}
}

// configDoc parses the first YAML data value of the ConfigMap whose name
// contains name.
func configDoc(t *testing.T, objs []obj, name string) obj {
	t.Helper()
	for _, o := range objs {
		if kindOf(o) != "ConfigMap" || !strings.Contains(nameOf(o), name) {
			continue
		}
		data, _ := o["data"].(obj)
		for _, v := range data {
			var doc obj
			if err := yaml.Unmarshal([]byte(fmt.Sprint(v)), &doc); err != nil {
				t.Fatalf("ConfigMap %s: %v", nameOf(o), err)
			}
			return doc
		}
	}
	t.Fatalf("no ConfigMap named like %q", name)
	return nil
}

// findMaxTraces finds a memory: {max_traces: n} anywhere in doc.
func findMaxTraces(doc any, n int) bool {
	switch v := doc.(type) {
	case obj:
		if mt := dig(v, "memory", "max_traces"); mt != nil && fmt.Sprint(mt) == fmt.Sprint(n) {
			return true
		}
		for _, c := range v {
			if findMaxTraces(c, n) {
				return true
			}
		}
	case []any:
		for _, c := range v {
			if findMaxTraces(c, n) {
				return true
			}
		}
	}
	return false
}

func keys(m map[string]obj) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// make tracing-up installs the tracing chart and turns tracing on in the
// storage release with values-tracing.yaml; tracing-down reverses both.
// Dry run (make -n): nothing is executed.
func TestMakeTracingTargets(t *testing.T) {
	for target, want := range map[string][]string{
		"tracing-up":   {"deploy/helm/tracing", "values-tracing.yaml"},
		"tracing-down": {"uninstall"},
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
