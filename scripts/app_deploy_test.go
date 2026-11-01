//go:build integration

package scripts_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

var appDependencies sync.Once

func TestAppHelmFullDeployment(t *testing.T) {
	profile := filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml")
	if _, err := os.Stat(profile); err != nil {
		t.Fatalf("A1.5 app profile is missing: %v", err)
	}
	docs := renderApp(t, "-f", profile)
	for name, replicas := range map[string]int{"reliefgrid-api": 2, "reliefgrid-matcher": 2, "reliefgrid-ui": 1, "storage-coordinator": 1} {
		d := appDocument(t, docs, "Deployment", name)
		appEqual(t, appField(t, d, "spec", "replicas"), replicas, name+" replicas")
		if name == "storage-coordinator" {
			continue
		}
		pod := appField(t, d, "spec", "template", "spec").(map[string]any)
		appEqual(t, appField(t, pod, "nodeSelector", "role"), "data", name+" placement")
		appEqual(t, appField(t, pod, "securityContext", "runAsNonRoot"), true, name+" non-root")
		appEqual(t, appField(t, pod, "securityContext", "runAsUser"), 65532, name+" uid")
		containers, ok := pod["containers"].([]any)
		if !ok || len(containers) != 1 {
			t.Fatalf("%s must have one app container", name)
		}
		c := containers[0].(map[string]any)
		appEqual(t, appField(t, c, "securityContext", "readOnlyRootFilesystem"), true, name+" read-only root")
		appEqual(t, appField(t, c, "securityContext", "allowPrivilegeEscalation"), false, name+" privileges")
		appEqual(t, appField(t, c, "readinessProbe", "httpGet", "path"), "/readyz", name+" readiness")
		appEqual(t, appField(t, c, "livenessProbe", "httpGet", "path"), "/healthz", name+" liveness")
		_ = appField(t, c, "resources", "requests", "memory")
		_ = appField(t, c, "resources", "limits", "memory")
		if name != "reliefgrid-ui" {
			env := appEnvironment(t, c)
			appEqual(t, appField(t, env["COORDINATOR_URL"], "value"), "http://storage-coordinator:8080", name+" coordinator")
			if name == "reliefgrid-matcher" {
				_ = appField(t, env["AMQP_URL"], "valueFrom", "secretKeyRef", "name")
			}
			if name == "reliefgrid-api" {
				_ = appField(t, env["CALLER_NAMESPACE"], "value")
			}
		}
		_ = appDocument(t, docs, "Service", name)
	}
	for _, name := range []string{"storage-shard-0", "storage-shard-1", "storage-shard-2"} {
		d := appDocument(t, docs, "StatefulSet", name)
		appEqual(t, appField(t, d, "spec", "replicas"), 3, name+" replicas")
	}
	broker := appDocument(t, docs, "StatefulSet", "reliefgrid-rabbitmq")
	appEqual(t, appField(t, broker, "spec", "template", "spec", "nodeSelector", "role"), "infra", "broker placement")
	containers := appField(t, broker, "spec", "template", "spec", "containers").([]any)
	appEqual(t, appField(t, containers[0].(map[string]any), "image"), "rabbitmq:4.3.7-management@sha256:57bcedccc5c75e55ac33e5f3eaeb28529317b5a02fe5e2722a025f2674c8063e", "broker pin")
	traefik := appDocument(t, docs, "Service", "reliefgrid-traefik")
	appEqual(t, appField(t, traefik, "spec", "type"), "NodePort", "kind ingress exposure")
	ports := appField(t, traefik, "spec", "ports").([]any)
	wantPorts := map[string]int{"web": 30080, "websecure": 30443}
	for _, p := range ports {
		port := p.(map[string]any)
		name, _ := port["name"].(string)
		if want, ok := wantPorts[name]; ok {
			appEqual(t, port["nodePort"], want, name+" NodePort")
			delete(wantPorts, name)
		}
	}
	if len(wantPorts) != 0 {
		t.Fatalf("missing ingress NodePorts: %v", wantPorts)
	}
	checkAppRoutes(t, docs)
	class := appDocument(t, docs, "IngressClass", "traefik")
	appEqual(t, appField(t, class, "spec", "controller"), "traefik.io/ingress-controller", "app ingress controller")
}

func TestAppBrokerOnlyProfileRemainsIndependent(t *testing.T) {
	docs := renderApp(t, "--set", "traefik.enabled=false")
	_ = appDocument(t, docs, "StatefulSet", "reliefgrid-rabbitmq")
	for _, d := range docs {
		name, _ := appField(t, d, "metadata", "name").(string)
		if strings.HasPrefix(name, "storage-") || name == "reliefgrid-api" || name == "reliefgrid-matcher" || name == "reliefgrid-ui" {
			t.Fatalf("broker-only profile unexpectedly deploys %s", name)
		}
	}
}

func renderApp(t *testing.T, flags ...string) []map[string]any {
	t.Helper()
	chart := filepath.Join("..", "deploy", "helm", "reliefgrid")
	appDependencies.Do(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, "bash", "app-chart-deps.sh").CombinedOutput()
		if err != nil {
			t.Fatalf("app chart dependencies: %v\n%s", err, out)
		}
	})
	args := append([]string{"template", "reliefgrid", chart, "--namespace", "reliefgrid"}, flags...)
	decoder := yaml.NewDecoder(bytes.NewReader(appHelm(t, args...)))
	var docs []map[string]any
	for {
		var d map[string]any
		err := decoder.Decode(&d)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

func appHelm(t *testing.T, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "helm", args...)
	var diagnostics bytes.Buffer
	cmd.Stderr = &diagnostics
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("helm %s: %v: %s", args[0], err, diagnostics.Bytes())
	}
	return output
}

func appDocument(t *testing.T, docs []map[string]any, kind, name string) map[string]any {
	t.Helper()
	for _, d := range docs {
		if d["kind"] == kind && appField(t, d, "metadata", "name") == name {
			return d
		}
	}
	t.Fatalf("missing %s %s", kind, name)
	return nil
}

func appField(t *testing.T, doc map[string]any, keys ...string) any {
	t.Helper()
	var value any = doc
	for _, key := range keys {
		m, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("missing object while reading %v", keys)
		}
		value, ok = m[key]
		if !ok {
			t.Fatalf("missing field %v", keys)
		}
	}
	return value
}

func appEqual(t *testing.T, got, want any, description string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v want %v", description, got, want)
	}
}

func appEnvironment(t *testing.T, container map[string]any) map[string]map[string]any {
	t.Helper()
	env := map[string]map[string]any{}
	for _, item := range appField(t, container, "env").([]any) {
		e := item.(map[string]any)
		env[e["name"].(string)] = e
	}
	return env
}

func checkAppRoutes(t *testing.T, docs []map[string]any) {
	t.Helper()
	found := map[string]bool{}
	for _, d := range docs {
		if d["kind"] != "Ingress" {
			continue
		}
		appEqual(t, appField(t, d, "spec", "ingressClassName"), "traefik", "ingress class")
		for _, rule := range appField(t, d, "spec", "rules").([]any) {
			for _, p := range appField(t, rule.(map[string]any), "http", "paths").([]any) {
				path := p.(map[string]any)
				name := appField(t, path, "backend", "service", "name")
				switch path["path"] {
				case "/api/v1":
					appEqual(t, name, "reliefgrid-api", "API route")
					found["api"] = true
				case "/":
					appEqual(t, name, "reliefgrid-ui", "UI route")
					found["ui"] = true
				default:
					t.Errorf("unexpected public route %v -> %v", path["path"], name)
				}
				appEqual(t, path["pathType"], "Prefix", "route prefix")
			}
		}
	}
	if !found["api"] || !found["ui"] {
		t.Fatalf("missing public routes: %v", found)
	}
}

// A runner starts with no Helm repositories or cached indexes.
func TestAppChartDependenciesFromEmptyHelmHome(t *testing.T) {
	for _, key := range []string{"HELM_CONFIG_HOME", "HELM_CACHE_HOME", "HELM_DATA_HOME"} {
		t.Setenv(key, t.TempDir())
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "bash", "app-chart-deps.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("fresh Helm dependencies: %v\n%s", err, out)
	}
}

func TestAppStandaloneStorageProfileDoesNotRenderStorage(t *testing.T) {
	docs := renderApp(t, "-f", "../deploy/helm/reliefgrid/values-app-kind.yaml", "--set", "storage.enabled=false")
	for _, doc := range docs {
		name, _ := appField(t, doc, "metadata", "name").(string)
		if strings.HasPrefix(name, "storage-") {
			t.Fatalf("app-only release tried to own standalone storage: %s", name)
		}
	}
	for _, name := range []string{"reliefgrid-api", "reliefgrid-matcher"} {
		deployment := appDocument(t, docs, "Deployment", name)
		containers := appField(t, deployment, "spec", "template", "spec", "containers").([]any)
		env := appEnvironment(t, containers[0].(map[string]any))
		appEqual(t, appField(t, env["COORDINATOR_URL"], "value"), "http://storage-coordinator:8080", name+" standalone coordinator")
	}
}
