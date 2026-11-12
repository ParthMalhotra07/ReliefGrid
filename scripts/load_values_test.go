package scripts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

var appChartDeps sync.Once

// renderAppChart is helm template of deploy/helm/reliefgrid with its
// dependencies prepared (scripts/app-chart-deps.sh).
func renderAppChart(t *testing.T, flags ...string) []map[string]any {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("REQUIRE_HELM") != "" {
			t.Fatalf("REQUIRE_HELM is set but helm is not on PATH: %v", err)
		}
		t.Skip("helm not on PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var depErr error
	var depOut []byte
	appChartDeps.Do(func() { depOut, depErr = exec.CommandContext(ctx, "bash", "app-chart-deps.sh").CombinedOutput() })
	if depErr != nil {
		t.Fatalf("app chart dependencies: %v\n%s", depErr, depOut)
	}
	args := append([]string{"template", "reliefgrid", "../deploy/helm/reliefgrid", "--namespace", "reliefgrid"}, flags...)
	out, err := exec.CommandContext(ctx, helm, args...).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("helm template: %v\n%s", err, ee.Stderr)
		}
		t.Fatal(err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	var docs []map[string]any
	for {
		var d map[string]any
		if err := dec.Decode(&d); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if len(d) != 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

// S5.2 load runs use values-load.yaml overlays on both charts (never the
// defaults): every container's requests equal its limits (Guaranteed QoS),
// storage runs the load Raft profile, and the CPU limits fit the kind
// nodes' cpuset (0-5) with room for the control plane.

type container struct{ workload, name, reqCPU, limCPU, reqMem, limMem string }

func containersOf(t *testing.T, docs []map[string]any) []container {
	t.Helper()
	var out []container
	for _, d := range docs {
		if d["kind"] != "Deployment" && d["kind"] != "StatefulSet" {
			continue
		}
		w := fmt.Sprint(dig(d, "metadata", "name"))
		spec := dig(d, "spec", "template", "spec")
		for _, key := range []string{"initContainers", "containers"} {
			for _, c := range list(dig(spec, key)) {
				s := func(p ...any) string {
					v := dig(c, p...)
					if v == nil {
						return ""
					}
					return fmt.Sprint(v)
				}
				out = append(out, container{w, s("name"), s("resources", "requests", "cpu"), s("resources", "limits", "cpu"),
					s("resources", "requests", "memory"), s("resources", "limits", "memory")})
			}
		}
	}
	return out
}

func guaranteed(t *testing.T, cs []container) (cpuMilli int64) {
	t.Helper()
	if len(cs) == 0 {
		t.Fatal("no containers rendered")
	}
	for _, c := range cs {
		if c.limCPU == "" || c.limMem == "" || c.reqCPU != c.limCPU || c.reqMem != c.limMem {
			t.Errorf("%s/%s: requests %s/%s, limits %s/%s; want equal and set (Guaranteed)", c.workload, c.name, c.reqCPU, c.reqMem, c.limCPU, c.limMem)
			continue
		}
		m, err := milliCPU(c.limCPU)
		if err != nil {
			t.Fatalf("%s/%s: %v", c.workload, c.name, err)
		}
		replicas := int64(1)
		if strings.HasPrefix(c.workload, "storage-shard-") {
			replicas = 3
		} else if c.workload == "reliefgrid-api" || c.workload == "reliefgrid-matcher" {
			replicas = 2
		}
		cpuMilli += m * replicas
	}
	return cpuMilli
}

func TestLoadValuesStorage(t *testing.T) {
	cpu := guaranteed(t, containersOf(t, renderStorage(t, "-f", "../deploy/helm/storage/values-load.yaml")))
	for _, d := range renderStorage(t, "-f", "../deploy/helm/storage/values-load.yaml") {
		if d["kind"] == "StatefulSet" {
			for _, e := range list(dig(d, "spec", "template", "spec", "containers", 0, "env")) {
				if dig(e, "name") == "PROFILE" && dig(e, "value") != "load" {
					t.Errorf("%v: PROFILE %v, want load", dig(d, "metadata", "name"), dig(e, "value"))
				}
			}
		}
	}
	app := guaranteed(t, containersOf(t, renderAppChart(t, "-f", "../deploy/helm/reliefgrid/values-app-kind.yaml",
		"--set", "storage.enabled=false", "-f", "../deploy/helm/reliefgrid/values-load.yaml")))
	if total := cpu + app; total > 5500 {
		t.Errorf("load overlays claim %dm CPU; the kind nodes get 6 cores (0-5) and the control plane needs some", total)
	} else {
		t.Logf("load overlays claim %dm CPU (storage %dm, app %dm)", total, cpu, app)
	}
}

// The overlays change nothing by default: the demo profile and the default
// (Burstable) resources stay.
func TestLoadValuesNotDefault(t *testing.T) {
	for _, c := range containersOf(t, renderStorage(t)) {
		if c.workload == "storage-shard-0" && c.reqCPU == c.limCPU {
			t.Errorf("default storage resources are requests = limits (%s); the load overlay leaked into values.yaml", c.reqCPU)
		}
	}
	for _, d := range renderStorage(t) {
		if d["kind"] == "StatefulSet" {
			for _, e := range list(dig(d, "spec", "template", "spec", "containers", 0, "env")) {
				if dig(e, "name") == "PROFILE" && dig(e, "value") != "demo" {
					t.Errorf("default PROFILE %v, want demo (M4)", dig(e, "value"))
				}
			}
		}
	}
}

// milliCPU parses a CPU quantity: "250m" or "1" / "0.5" cores.
func milliCPU(q string) (int64, error) {
	if v, ok := strings.CutSuffix(q, "m"); ok {
		return strconv.ParseInt(v, 10, 64)
	}
	f, err := strconv.ParseFloat(q, 64)
	return int64(f * 1000), err
}
