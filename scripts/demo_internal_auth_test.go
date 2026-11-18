package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The shared demo helpers (scripts/demos/kube.sh, demo-1.sh's replica reads) send the
// internal sender tokens when internalAuth.enabled (#29). testdata/internal-auth/
// demo-harness.sh drives every direct internal call they make against a fake kubectl and
// curl; demo-calls.off.txt is what it logged on main's files.

// demoHarness runs the harness on this tree with the given release values and Secrets.
func demoHarness(t *testing.T, values string, secrets map[string]string) (out, calls, headers string, dir string) {
	t.Helper()
	dir, env := authFakes(t, values, secrets)
	// The operator token the demos read with -o jsonpath={.data.token}: the bare base64.
	if err := os.WriteFile(filepath.Join(dir, "secrets", "storage-operator-token"), []byte("b3AtdG9rZW4="), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("testdata/internal-auth/fake-curl")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "curl"), b, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "curl.log")
	cmd := exec.CommandContext(t.Context(), "bash", "testdata/internal-auth/demo-harness.sh", root)
	cmd.Env = append(env, "CURL_LOG="+log)
	o, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("harness: %v\n%s", err, o)
	}
	c, _ := os.ReadFile(log)
	h, _ := os.ReadFile(log + ".headers")
	return string(o), string(c), string(h), dir
}

// Off: every demo internal call is byte-for-byte main's, and no Secret is read.
func TestDemoInternalCallsUnchangedWithAuthOff(t *testing.T) {
	wantCalls, err := os.ReadFile("testdata/internal-auth/demo-calls.off.txt")
	if err != nil {
		t.Fatal(err)
	}
	wantOut, err := os.ReadFile("testdata/internal-auth/demo-harness.off.txt")
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range []string{authOff, `{"image":{"tag":"dev"}}`} {
		out, calls, headers, dir := demoHarness(t, values, bothTokens)
		if calls != string(wantCalls) {
			t.Errorf("%s: the demo calls differ from main's:\n got:\n%s\nwant:\n%s", values, calls, wantCalls)
		}
		if out != string(wantOut) {
			t.Errorf("%s: harness output:\n got %q\nwant %q", values, out, wantOut)
		}
		if headers != "" {
			t.Errorf("%s: header files sent with auth off:\n%s", values, headers)
		}
		argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
		if strings.Contains(string(argv), "reliefgrid-internal-auth") || strings.Contains(string(argv), "reliefgrid-node-auth") {
			t.Errorf("%s: a token Secret was read with auth off:\n%s", values, argv)
		}
	}
}

// On: coordinator calls carry the app token, direct storage calls the node token, each
// through a header file; the files are gone when the demo ends.
func TestDemoInternalCallsSendTokensWithAuthOn(t *testing.T) {
	out, calls, headers, dir := demoHarness(t, authOn, bothTokens)
	if !strings.Contains(out, "coord_forward ok") || !strings.Contains(out, "after cleanup: INTERNAL_AUTH_TOKEN_FILE=unset") {
		t.Fatalf("harness:\n%s", out)
	}
	wantCalls, _ := os.ReadFile("testdata/internal-auth/demo-calls.off.txt")
	off := strings.Split(strings.TrimSpace(string(wantCalls)), "\n")
	got := strings.Split(strings.TrimSpace(calls), "\n")
	if len(got) != len(off) {
		t.Fatalf("%d calls, want %d:\n%s", len(got), len(off), calls)
	}
	hdr := regexp.MustCompile(` -H @\S+/(app|node)-header`)
	for i, line := range got {
		m := hdr.FindStringSubmatch(line)
		want := "node"
		if strings.Contains(line, "127.0.0.1:7001/") { // the coordinator's forward
			want = "app"
		}
		if m == nil || m[1] != want {
			t.Errorf("call %d should send the %s token:\n%s", i, want, line)
			continue
		}
		// Apart from the header argument, the call is main's.
		if stripped := hdr.ReplaceAllString(line, ""); stripped != off[i] {
			t.Errorf("call %d differs beyond the token header:\n got %s\nwant %s", i, stripped, off[i])
		}
	}
	appLines := strings.Count(headers, "app-header: X-ReliefGrid-Internal-Token: "+appToken+"\n")
	nodeLines := strings.Count(headers, "node-header: X-ReliefGrid-Internal-Token: "+nodeToken+"\n")
	if appLines != 3 || nodeLines != 4 {
		t.Errorf("headers sent: %d app, %d node (want 3, 4):\n%s", appLines, nodeLines, headers)
	}
	noTokenIn(t, "the curl command lines", []byte(calls))
	noTokenIn(t, "the demo output", []byte(out))
	argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
	noTokenIn(t, "a kubectl or helm command line", argv)
	if left := tempLeft(t, dir); len(left) > 0 {
		t.Errorf("token files left after the demo: %v", left)
	}
}

// A missing Secret, or a release whose auth state cannot be read, stops the demo at
// coord_forward with a reason naming it; no internal call is made.
func TestDemoAuthStopsWithAClearReason(t *testing.T) {
	for _, c := range []struct {
		name, values string
		secrets      map[string]string
		want         string
	}{
		{"node Secret missing", authOn, map[string]string{"reliefgrid-internal-auth": appToken},
			"coord_forward failed: internal auth: cannot read reliefgrid-node-auth/sender-token"},
		{"app Secret missing", authOn, map[string]string{"reliefgrid-node-auth": nodeToken},
			"coord_forward failed: internal auth: cannot read reliefgrid-internal-auth/sender-token"},
		{"release unreadable", "", bothTokens,
			"coord_forward failed: internal auth: cannot read the values of release storage"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, calls, _, dir := demoHarness(t, c.values, c.secrets)
			if !strings.Contains(out, c.want) {
				t.Errorf("want %q in:\n%s", c.want, out)
			}
			if calls != "" {
				t.Errorf("internal calls made anyway:\n%s", calls)
			}
			if left := tempLeft(t, dir); len(left) > 0 {
				t.Errorf("files left: %v", left)
			}
		})
	}
}

// preflight and demo 7 report the auth reason, not "cannot reach the coordinator".
func TestDemoPreflightReportsTheAuthReason(t *testing.T) {
	kube, err := os.ReadFile("demos/kube.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(kube), `coord_forward || why "${AUTH_WHY:-cannot reach the coordinator (Service storage-coordinator)}"`) {
		t.Errorf("kube.sh preflight does not report AUTH_WHY")
	}
	d7, err := os.ReadFile("demos/demo-7.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(d7), `fail "preflight: ${AUTH_WHY:-cannot reach the coordinator}"`) {
		t.Errorf("demo-7.sh does not report AUTH_WHY")
	}
}

// Every direct internal call in the demo shell scripts goes through the auth arrays.
// app_demo.py (#172) and the M8 runner are not shell demo helpers and are not covered here.
func TestDemoShellInternalCallsCarryTheAuthArguments(t *testing.T) {
	files, err := filepath.Glob("demos/*.sh")
	if err != nil {
		t.Fatal(err)
	}
	internal := regexp.MustCompile(`curl [^\n]*/internal/`)
	n := 0
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range internal.FindAll(src, -1) {
			n++
			s := string(l)
			app := strings.Contains(s, "$(coord)/internal/")
			arr := `"${NODE_AUTH_CURL[@]}"`
			if app {
				arr = `"${INTERNAL_AUTH_CURL[@]}"`
			}
			if !strings.Contains(s, arr) {
				t.Errorf("%s: an internal call without %s: %s", f, arr, s)
			}
		}
	}
	if n != 4 {
		t.Errorf("found %d internal curl calls in the demo scripts, want 4 (cput, cget, stale_get, demo 1's replica reads)", n)
	}
}

// Demo 6 loads the helper twice (chaos/scripts/lib.sh and kube.sh): a second load must not
// forget token files already written.
func TestInternalAuthHelperSourcedTwice(t *testing.T) {
	dir, env := authFakes(t, authOn, bothTokens)
	out, err := runBash(t, env, `set -euo pipefail
. ../scripts/internal-auth-env.sh
internal_auth_setup kind-x ns
d="$INTERNAL_AUTH_DIR"
. ../scripts/internal-auth-env.sh
echo "dir kept: $([ "$INTERNAL_AUTH_DIR" = "$d" ] && echo yes) curl=${#INTERNAL_AUTH_CURL[@]}"
internal_auth_cleanup
[ ! -e "$d" ] && echo removed`)
	if err != nil || !strings.Contains(out, "dir kept: yes curl=2") || !strings.Contains(out, "removed") {
		t.Fatalf("%v\n%s", err, out)
	}
	if left := tempLeft(t, dir); len(left) > 0 {
		t.Errorf("files left: %v", left)
	}
}
