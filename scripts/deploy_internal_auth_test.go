package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// INTERNAL_AUTH for make demo-reset and scripts/app-deploy.sh (#29). testdata/internal-auth/
// deploy-harness.sh runs both scripts against fake kubectl, helm, make, kind, docker and
// curl and logs every command; deploy-calls.off.txt is that log for the scripts before this
// switch (generated from their previous version).

func runDeploy(t *testing.T, env ...string) (calls, resetOut, appOut string) {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "bash", "testdata/internal-auth/deploy-harness.sh", root, work)
	base := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "INTERNAL_AUTH=") && !strings.HasPrefix(kv, "APP_WITH_STORAGE=") && !strings.HasPrefix(kv, "FAKE_STORAGE_VALUES=") {
			base = append(base, kv)
		}
	}
	cmd.Env = append(base, env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("harness: %v\n%s", err, out)
	}
	read := func(f string) string { b, _ := os.ReadFile(filepath.Join(work, f)); return string(b) }
	return canonical(t, read("calls")), read("reset.out"), read("app.out")
}

// canonical orders a call log deterministically. reset.sh starts the coordinator
// port-forward in the background and polls through it at once, so the fake kubectl's
// port-forward line and the fake curl's first line are logged in either order. The
// port-forward line (exactly one) is moved to the end, after a marker; every other line keeps
// its order.
func canonical(t *testing.T, calls string) string {
	t.Helper()
	var rest, background []string
	for _, l := range strings.SplitAfter(calls, "\n") {
		if strings.Contains(l, " port-forward svc/storage-coordinator ") {
			background = append(background, l)
		} else if l != "" {
			rest = append(rest, l)
		}
	}
	if len(background) > 1 {
		t.Fatalf("want at most one coordinator port-forward, got %d:\n%s", len(background), calls)
	}
	return strings.Join(rest, "") + "-- background --\n" + strings.Join(background, "")
}

func golden(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/internal-auth/deploy-calls.off.txt")
	if err != nil {
		t.Fatal(err)
	}
	return canonical(t, string(b))
}

const (
	storageOn  = `{"internalAuth":{"enabled":true}}`
	storageOff = `{"internalAuth":{"enabled":false}}`
	getValues  = `helm --kube-context kind-reliefgrid get values storage -n reliefgrid -a -o json` + "\n"
	// app-deploy's first command: the storage check runs before it.
	appFirst = `kubectl --request-timeout=10s --context kind-reliefgrid get nodes -o json` + "\n"
)

// Unset: every command reset and app-deploy run is byte-identical to before the switch.
func TestDeployInternalAuthUnsetIsUnchanged(t *testing.T) {
	calls, _, _ := runDeploy(t)
	if calls != golden(t) {
		t.Fatalf("commands differ from before the switch:\n got:\n%s\nwant:\n%s", calls, golden(t))
	}
}

// false: the same commands, plus app-deploy's check that the storage release agrees.
func TestDeployInternalAuthFalse(t *testing.T) {
	calls, _, _ := runDeploy(t, "INTERNAL_AUTH=false", "FAKE_STORAGE_VALUES="+storageOff)
	want := strings.ReplaceAll(golden(t), `\[INTERNAL_AUTH=unset\]`, `\[INTERNAL_AUTH=false\]`)
	want = strings.Replace(want, appFirst, getValues+appFirst, 1)
	if calls != want {
		t.Fatalf("got:\n%s\nwant:\n%s", calls, want)
	}
}

// true: each release gets its values file, and nothing else changes: no token and no
// --set internalAuth on any command line (the storage chart generates the tokens).
func TestDeployInternalAuthTrue(t *testing.T) {
	calls, resetOut, appOut := runDeploy(t, "INTERNAL_AUTH=true", "FAKE_STORAGE_VALUES="+storageOn)
	want := strings.ReplaceAll(golden(t), `\[INTERNAL_AUTH=unset\]`, `\[INTERNAL_AUTH=true\]`)
	want = strings.Replace(want, "install storage deploy/helm/storage -n reliefgrid --create-namespace --wait",
		"install storage deploy/helm/storage -n reliefgrid --create-namespace -f deploy/helm/storage/values-internal-auth.yaml --wait", 1)
	want = strings.Replace(want, "--set storage.enabled=false --set-string",
		"--set storage.enabled=false -f deploy/helm/reliefgrid/values-internal-auth.yaml --set-string", 1)
	want = strings.Replace(want, appFirst, getValues+appFirst, 1)
	if calls != want {
		t.Fatalf("got:\n%s\nwant:\n%s\nreset:\n%s\napp:\n%s", calls, want, resetOut, appOut)
	}
	if strings.Contains(calls, "internalAuth") || strings.Contains(strings.ToLower(calls), "token=") {
		t.Errorf("an auth setting or token on a command line:\n%s", calls)
	}
}

// Refusals, each before anything is deployed by the script that refuses.
func TestDeployInternalAuthRefuses(t *testing.T) {
	for _, c := range []struct {
		name  string
		env   []string
		calls string // the whole call log
		msg   string
	}{
		{"not a boolean", []string{"INTERNAL_AUTH=yes"},
			"reset exit 2\napp-deploy exit 2\n-- background --\n", "INTERNAL_AUTH must be true or false, not 'yes'"},
		{"storage release has auth off", []string{"INTERNAL_AUTH=true", "FAKE_STORAGE_VALUES=" + storageOff},
			"", "INTERNAL_AUTH=true but the storage release has internalAuth.enabled=false"},
		{"storage release value is a string", []string{"INTERNAL_AUTH=true", `FAKE_STORAGE_VALUES={"internalAuth":{"enabled":"true"}}`},
			"", "the storage release has internalAuth.enabled=invalid"},
		{"storage release unreadable", []string{"INTERNAL_AUTH=true"},
			"", "INTERNAL_AUTH: cannot read the storage release values"},
		{"bundled storage", []string{"INTERNAL_AUTH=true", "APP_WITH_STORAGE=true"},
			"", "INTERNAL_AUTH=true is for the standalone storage release"},
	} {
		t.Run(c.name, func(t *testing.T) {
			calls, resetOut, appOut := runDeploy(t, c.env...)
			if c.calls != "" && calls != c.calls {
				t.Errorf("calls:\n%s\nwant:\n%s", calls, c.calls)
			}
			if !strings.Contains(resetOut+appOut, c.msg) {
				t.Errorf("want %q in:\nreset:\n%s\napp:\n%s", c.msg, resetOut, appOut)
			}
			if !strings.Contains(calls, "app-deploy exit 2\n") || strings.Contains(calls, "upgrade --install") {
				t.Errorf("app-deploy did not refuse before deploying:\n%s", calls)
			}
		})
	}
}

// The values files only turn the switch on: no token is ever committed.
func TestInternalAuthValuesFiles(t *testing.T) {
	for _, c := range []string{"storage", "reliefgrid"} {
		b, err := os.ReadFile(filepath.Join("..", "deploy", "helm", c, "values-internal-auth.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var body []string
		for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "#") {
				body = append(body, l)
			}
		}
		if strings.Join(body, "\n") != "internalAuth:\n  enabled: true" {
			t.Errorf("%s values file:\n%s", c, b)
		}
	}
}
