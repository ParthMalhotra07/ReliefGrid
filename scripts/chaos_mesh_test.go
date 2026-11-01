package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Chaos Mesh (S4.3) is pinned in tools.mk: the chart by sha256 and both images
// by their linux/amd64 manifest digests. chaos/mesh/install.sh is the only
// thing that installs it; these tests run it with fake docker, kubectl and
// helm on PATH and never touch a cluster or a registry.

func toolsPins(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile("../tools.mk")
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]string{}
	for _, m := range regexp.MustCompile(`(?m)^(CHAOS_[A-Z0-9_]+)\s*:=\s*(\S+)`).FindAllStringSubmatch(string(b), -1) {
		pins[m[1]] = m[2]
	}
	return pins
}

func TestChaosMeshPinned(t *testing.T) {
	pins := toolsPins(t)
	checks := map[string]*regexp.Regexp{
		"CHAOS_MESH_VERSION":        regexp.MustCompile(`^2\.8\.7$`),
		"CHAOS_MESH_CHART_SHA256":   regexp.MustCompile(`^[0-9a-f]{64}$`),
		"CHAOS_MESH_IMAGE_DIGEST":   regexp.MustCompile(`^sha256:[0-9a-f]{64}$`),
		"CHAOS_DAEMON_IMAGE_DIGEST": regexp.MustCompile(`^sha256:[0-9a-f]{64}$`),
	}
	for name, re := range checks {
		if !re.MatchString(pins[name]) {
			t.Errorf("tools.mk %s = %q, want %s", name, pins[name], re)
		}
	}

	var v struct {
		ClusterScoped     bool `yaml:"clusterScoped"`
		ControllerManager struct {
			ReplicaCount    int               `yaml:"replicaCount"`
			ImagePullPolicy string            `yaml:"imagePullPolicy"`
			NodeSelector    map[string]string `yaml:"nodeSelector"`
			Image           map[string]any    `yaml:"image"`
		} `yaml:"controllerManager"`
		ChaosDaemon struct {
			Runtime         string            `yaml:"runtime"`
			SocketPath      string            `yaml:"socketPath"`
			ImagePullPolicy string            `yaml:"imagePullPolicy"`
			NodeSelector    map[string]string `yaml:"nodeSelector"`
			Image           map[string]any    `yaml:"image"`
		} `yaml:"chaosDaemon"`
		Dashboard struct{ Create bool } `yaml:"dashboard"`
		DNSServer struct{ Create bool } `yaml:"dnsServer"`
	}
	b, err := os.ReadFile("../chaos/mesh/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	cm, cd := v.ControllerManager, v.ChaosDaemon
	switch {
	case v.ClusterScoped:
		t.Error("values: clusterScoped must be false (inject only into the target namespace)")
	case cm.ReplicaCount != 1 || cm.NodeSelector["role"] != "infra":
		t.Errorf("values: controller %d replicas on %v, want 1 on role=infra", cm.ReplicaCount, cm.NodeSelector)
	case cd.NodeSelector["role"] != "data":
		t.Errorf("values: daemon on %v, want role=data only", cd.NodeSelector)
	case cd.Runtime != "containerd" || cd.SocketPath != "/run/containerd/containerd.sock":
		t.Errorf("values: daemon runtime %s at %s, want kind's containerd", cd.Runtime, cd.SocketPath)
	case cm.ImagePullPolicy != "Never" || cd.ImagePullPolicy != "Never":
		t.Error("values: imagePullPolicy must be Never (images are imported and tagged by digest)")
	case cm.Image != nil || cd.Image != nil:
		t.Error("values: no image tags here; install.sh sets them from tools.mk, the one place the digests live")
	case v.Dashboard.Create || v.DNSServer.Create:
		t.Error("values: dashboard and dnsServer must be off")
	}
}

// meshFakeTools puts docker, kubectl, helm and kind on PATH that record any call
// in calls and fail.
func meshFakeTools(t *testing.T) (env []string, calls string) {
	t.Helper()
	dir := t.TempDir()
	calls = filepath.Join(dir, "calls")
	for _, tool := range []string{"docker", "kubectl", "helm", "kind"} {
		script := "#!/usr/bin/env bash\necho \"" + tool + " $*\" >>" + calls + "\nexit 1\n"
		if err := os.WriteFile(filepath.Join(dir, tool), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Only the pins a test passes: none inherited (make verify-S4.3 exports them).
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "CHAOS_") {
			env = append(env, e)
		}
	}
	return append(env, "PATH="+dir+":"+os.Getenv("PATH"), "CHAOS_CACHE="+filepath.Join(dir, "cache")), calls
}

func runInstall(t *testing.T, env []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{"../chaos/mesh/install.sh"}, args...)...)
	cmd.Env = env
	out, _ := cmd.CombinedOutput()
	return string(out), cmd.ProcessState.ExitCode()
}

func withPins(env []string, pins map[string]string) []string {
	for k, v := range pins {
		env = append(env, k+"="+v)
	}
	return env
}

// install refuses before touching anything when a pin is missing or malformed.
func TestChaosMeshInstallRefusesBadPins(t *testing.T) {
	good := toolsPins(t)
	for _, tc := range []struct {
		name string
		edit func(map[string]string)
		has  string
	}{
		{"missing chart sha", func(p map[string]string) { delete(p, "CHAOS_MESH_CHART_SHA256") }, "CHAOS_MESH_CHART_SHA256 is not set"},
		{"short chart sha", func(p map[string]string) { p["CHAOS_MESH_CHART_SHA256"] = "c963e0f4" }, "not a sha256"},
		{"tag instead of digest", func(p map[string]string) { p["CHAOS_DAEMON_IMAGE_DIGEST"] = "v2.8.7" }, "sha256:<64 hex>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := map[string]string{}
			for k, v := range good {
				p[k] = v
			}
			tc.edit(p)
			env, calls := meshFakeTools(t)
			out, code := runInstall(t, withPins(env, p), "install")
			if code != 1 || !strings.Contains(out, tc.has) {
				t.Fatalf("exit %d, output %q; want exit 1 mentioning %q", code, out, tc.has)
			}
			if b, err := os.ReadFile(calls); err == nil {
				t.Fatalf("called tools before refusing:\n%s", b)
			}
		})
	}
}

// A cached chart whose sha256 differs from the pin is refused before any
// image is imported or anything is installed.
func TestChaosMeshInstallRefusesTamperedChart(t *testing.T) {
	pins := toolsPins(t)
	env, calls := meshFakeTools(t)
	var cache string
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, "CHAOS_CACHE="); ok {
			cache = v
		}
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "chaos-mesh-"+pins["CHAOS_MESH_VERSION"]+".tgz"), []byte("not the chart"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, code := runInstall(t, withPins(env, pins), "install")
	if code != 1 || !strings.Contains(out, "pinned "+pins["CHAOS_MESH_CHART_SHA256"]) {
		t.Fatalf("exit %d, output %q; want exit 1 naming the pinned sha256", code, out)
	}
	if b, err := os.ReadFile(calls); err == nil {
		t.Fatalf("called tools after a tampered chart:\n%s", b)
	}
}

func TestChaosMeshInstallUsage(t *testing.T) {
	env, _ := meshFakeTools(t)
	if out, code := runInstall(t, env, "bogus"); code != 2 || !strings.Contains(out, "usage") {
		t.Fatalf("exit %d, output %q; want usage, exit 2", code, out)
	}
}
