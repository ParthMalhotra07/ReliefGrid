package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The kind part of scripts/verify-S4.5.sh scales the app to 0 to quiesce the shards. These
// tests run it against fake kind, kubectl and helm and check that its EXIT trap scales every
// app Deployment back to its replicas in the Helm release, whether the run fails, is
// interrupted or is terminated after the quiesce. reliefgrid-api starts at 0, as an earlier
// broken run would have left it: the restore must use Helm's 2, not the 0 it saw.

const s45FakeKubectl = `#!/usr/bin/env bash
# Fake kubectl: logs each call (without --context and -n) to $FAKE/calls.
args=()
while [ $# -gt 0 ]; do
	case "$1" in
	--context | -n) shift 2 ;;
	*) args+=("$1"); shift ;;
	esac
done
set -- "${args[@]}"
echo "$*" >>"$FAKE/calls"
case "$*" in
"get statefulset "* | "get pvc "* | "delete "* | "wait "* | "scale "*) exit 0 ;;
"exec "*"/storagectl status") echo '{"appliedIndex":10}' ;;
"get secret reliefgrid-rabbitmq -o jsonpath={.data.username}") printf rg-user | base64 ;;
"get secret reliefgrid-rabbitmq -o jsonpath={.data.password}") printf 'p@ss:w/rd' | base64 ;;
"get secret reliefgrid-rabbitmq") exit 0 ;;
"get secret reliefgrid-internal-auth -o json") echo "{\"data\":{\"sender-token\":\"$(printf s45-app-sender-token | base64)\"}}" ;;
"get secret reliefgrid-node-auth -o json") echo "{\"data\":{\"sender-token\":\"$(printf s45-node-sender-token | base64)\"}}" ;;
"port-forward "*)
	# A real listener on the local port, so the script's readiness wait succeeds.
	exec python3 -c 'import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1].split(":")[0]))); s.listen(); time.sleep(600)' "$3"
	;;
"logs job/s45-check-"*)
	# The check after the quiesce: one handoff pending on shard 1 until the drain ran.
	if [ -e "$FAKE/drained" ]; then echo '{"ok":true,"verdict":"clean","report":{"outbox":[{"shard":0,"pending":0},{"shard":1,"pending":0},{"shard":2,"pending":0}],"quiescent":true}}'
	else echo '{"ok":true,"verdict":"clean","report":{"outbox":[{"shard":0,"pending":0},{"shard":1,"pending":1,"oldestAt":1},{"shard":2,"pending":0}],"quiescent":true}}'; fi
	;;
"apply -f -")
	name="$(jq -r .metadata.name)"
	case "$name" in s45-check-*) touch "$FAKE/check-started" ;; esac
	;;
"get job s45-check-"*) [ "$MODE" = hang ] || echo 1 ;; # a hung check Job never finishes
"get job "*) echo 1 ;;
"get pods -l job-name="*) echo 0 ;;
"logs job/s45-verify-"*) echo '{"ledger":{"acked":1,"unknown":0},"verdict":{"lost":[]}}' ;;
"logs job/s45-check-"*) echo '{"report":{"outbox":[]}}' ;;
"get deployments -o json")
	cat <<'EOF'
{"items": [
 {"metadata": {"name": "reliefgrid-api", "annotations": {"meta.helm.sh/release-name": "reliefgrid"}}, "spec": {"replicas": 0}},
 {"metadata": {"name": "reliefgrid-matcher", "annotations": {"meta.helm.sh/release-name": "reliefgrid"}}, "spec": {"replicas": 2}},
 {"metadata": {"name": "reliefgrid-traefik", "annotations": {"meta.helm.sh/release-name": "reliefgrid"}}, "spec": {"replicas": 1}},
 {"metadata": {"name": "reliefgrid-ui", "annotations": {"meta.helm.sh/release-name": "reliefgrid"}}, "spec": {"replicas": 1}},
 {"metadata": {"name": "storage-coordinator", "annotations": {"meta.helm.sh/release-name": "storage"}}, "spec": {"replicas": 1}}
]}
EOF
	;;
"get deployment.apps/"*" -o json") echo '{"spec":{"selector":{"matchLabels":{"app":"x"}}}}' ;;
esac
exit 0
`

// s45FakeDrain stands in for scripts/demos/drain-outbox (DRAIN_BIN): it records that it ran
// (in $FAKE/calls, so its order against the scale-downs and the check Job shows) and, unless
// DRAIN_NOOP is set, publishes the pending handoff ($FAKE/drained).
const s45FakeDrain = `#!/usr/bin/env bash
echo "drain-outbox COORDINATOR_URL=${COORDINATOR_URL%%:*}" >>"$FAKE/calls"
case "$AMQP_URL" in "amqp://rg-user:p%40ss%3Aw%2Frd@127.0.0.1:"*/) ;; *) echo "bad AMQP_URL" >&2; exit 9 ;; esac
[ -n "${DRAIN_NOOP:-}" ] || touch "$FAKE/drained"
# With internal auth on, the app sender token arrives as a file (never in the environment).
if [ -n "${INTERNAL_AUTH_TOKEN_FILE:-}" ]; then echo "$INTERNAL_AUTH_TOKEN_FILE" >"$FAKE/drain-token-path"; cat "$INTERNAL_AUTH_TOKEN_FILE" >"$FAKE/drain-token"; fi
echo "dial: refused (p@ss:w/rd)" >&2
echo '{"Read":1,"Published":1,"Marked":1,"Retries":0}'
exit "${DRAIN_EXIT:-0}"
`

const s45FakeHelm = `#!/usr/bin/env bash
case "$*" in
*"get values storage -a -o json") echo "{\"internalAuth\":{\"enabled\":$([ "${FAKE_AUTH:-off}" = on ] && echo true || echo false)}}" ;; # internal auth (#29): off unless FAKE_AUTH=on
*"get manifest reliefgrid")
	cat <<'EOF'
---
# Source: reliefgrid/templates/app.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reliefgrid-api
  labels:
    name: not-this-one
spec:
  replicas: 2
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: "reliefgrid-matcher"
spec:
  replicas: 2
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: reliefgrid-rabbitmq
spec:
  replicas: 3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reliefgrid-traefik
spec:
  template: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reliefgrid-ui
spec:
  replicas: 1
EOF
	;;
*) exit 1 ;;
esac
`

func TestVerifyS45KindRestoresAppReplicas(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string         // hang: the check Job after the quiesce never finishes
		sig  syscall.Signal // sent once the quiesce is done (0: none)
		exit int
	}{
		{"checks fail after the quiesce", "fail", 0, 1},
		{"SIGTERM after the quiesce", "hang", syscall.SIGTERM, 143},
		{"SIGINT after the quiesce", "hang", syscall.SIGINT, 130},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, fake, bin := s45Fixture(t)

			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts", "verify-S4.5.sh"), "--kind")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fake, "MODE="+tc.mode, "DRAIN_BIN="+filepath.Join(bin, "drain-outbox"))
			var out strings.Builder
			cmd.Stdout, cmd.Stderr = &out, &out
			cmd.WaitDelay = 10 * time.Second
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			// stop kills the run and fails the test; out is read only after Wait returns.
			stop := func(why string) {
				_ = cmd.Process.Kill()
				<-done
				t.Fatalf("%s:\n%s", why, out.String())
			}
			bound := time.After(60 * time.Second)
			if tc.sig != 0 {
				for reached := false; !reached; {
					select {
					case <-done:
						t.Fatalf("the run ended before the check after the quiesce:\n%s", out.String())
					case <-bound:
						stop("the run never reached the check after the quiesce")
					case <-time.After(50 * time.Millisecond):
						_, err := os.Stat(filepath.Join(fake, "check-started"))
						reached = err == nil
					}
				}
				// To the script's process alone, not its group: the trap must still run
				// promptly while a Job helper is polling.
				if err := cmd.Process.Signal(tc.sig); err != nil {
					t.Fatal(err)
				}
				bound = time.After(15 * time.Second)
			}
			select {
			case <-done:
			case <-bound:
				stop("the run did not exit in time (a trap held off by a subshell?)")
			}
			if got := cmd.ProcessState.ExitCode(); got != tc.exit {
				t.Errorf("exit %d, want %d\n%s", got, tc.exit, out.String())
			}

			b, err := os.ReadFile(filepath.Join(fake, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			calls := strings.Split(strings.TrimSpace(string(b)), "\n")
			lastDown := -1
			for i, c := range calls {
				if strings.HasPrefix(c, "scale ") && strings.HasSuffix(c, " --replicas 0") {
					lastDown = i
				}
			}
			if lastDown < 0 {
				t.Fatalf("the run never quiesced:\n%s", b)
			}
			var restored []string
			for _, c := range calls[lastDown+1:] {
				if strings.HasPrefix(c, "scale ") {
					restored = append(restored, strings.TrimPrefix(c, "scale "))
				}
			}
			want := []string{
				"deployment.apps/reliefgrid-api --replicas 2",
				"deployment.apps/reliefgrid-matcher --replicas 2",
				"deployment.apps/reliefgrid-traefik --replicas 1",
				"deployment.apps/reliefgrid-ui --replicas 1",
			}
			if strings.Join(restored, "\n") != strings.Join(want, "\n") {
				t.Errorf("restored after the quiesce:\n%s\nwant:\n%s\noutput:\n%s", strings.Join(restored, "\n"), strings.Join(want, "\n"), out.String())
			}
			if strings.Contains(string(b), "storage-coordinator --replicas") {
				t.Errorf("the coordinator was scaled:\n%s", b)
			}
			if tc.sig == 0 { // a run that ends on its own writes evidence; git diff --check rejects a blank last line
				ev, _ := filepath.Glob(filepath.Join(root, "docs", "evidence", "S4.5", "kind-*.txt"))
				if len(ev) != 1 {
					t.Fatalf("evidence files: %v", ev)
				}
				e, err := os.ReadFile(ev[0])
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasSuffix(string(e), "\n\n") || !strings.HasSuffix(string(e), "\n") {
					t.Errorf("the evidence must end in exactly one newline:\n%q", e[max(0, len(e)-80):])
				}
			}
		})
	}
}

// s45Files is every repository file verify-S4.5.sh --kind needs in its tree: the script, the
// chaos Job helpers it sources and the shared internal-auth helper those source (#29). Both
// kind tests build their tree from this one list, so they cannot drift apart.
var s45Files = []string{"scripts/verify-S4.5.sh", "chaos/scripts/lib.sh", "chaos/scripts/faults.sh", "scripts/internal-auth-env.sh"}

// s45Fixture builds a tree with s45Files and a fake bin (kubectl, helm, kind, the drainer);
// it returns the tree root, the fake's state directory and the bin directory.
func s45Fixture(t *testing.T) (root, fake, bin string) {
	t.Helper()
	root, fake = t.TempDir(), t.TempDir()
	bin = filepath.Join(fake, "bin")
	s45Write(t, filepath.Join(bin, "kubectl"), s45FakeKubectl, 0o755)
	s45Write(t, filepath.Join(bin, "helm"), s45FakeHelm, 0o755)
	s45Write(t, filepath.Join(bin, "kind"), "#!/usr/bin/env bash\necho reliefgrid\n", 0o755)
	s45Write(t, filepath.Join(bin, "drain-outbox"), s45FakeDrain, 0o755)
	for _, f := range s45Files {
		b, err := os.ReadFile(filepath.Join("..", f))
		if err != nil {
			t.Fatal(err)
		}
		s45Write(t, filepath.Join(root, f), string(b), 0o755)
	}
	return root, fake, bin
}

func s45Write(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

// The strict check after the quiesce (#29 rehearsal, 13 Nov: shard 1 had one handoff pending
// because the relay was stopped before publishing it): the outbox is drained with the
// relay-only drain-outbox once the app is at 0, and only then checked, with pending 0 required
// on every shard. A pending event at scale-down is drained, not failed.
func TestVerifyS45KindDrainsBeforeTheStrictCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want []string // lines that must be in the output
		bad  []string // lines that must not
	}{
		{"pending at scale-down is drained", nil,
			[]string{"PASS  drained the outbox with the app at 0 (read 1, published 1, marked 1, retries 0)",
				"PASS  invcheck check (strict): claim invariants clean, no write during the walk, outbox pending 0 on every shard"},
			[]string{"FAIL  drain", "FAIL  invcheck check"}},
		{"drain fails", []string{"DRAIN_EXIT=1", "DRAIN_NOOP=1"},
			[]string{"FAIL  drain the outbox with the app at 0: exit 1",
				"FAIL  invcheck check (strict): outbox pending [0,1,0]"},
			[]string{"PASS  drained"}},
		{"drain leaves it pending", []string{"DRAIN_NOOP=1"},
			[]string{"PASS  drained the outbox with the app at 0",
				"FAIL  invcheck check (strict): outbox pending [0,1,0]"},
			[]string{"PASS  invcheck check"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, fake, bin := s45Fixture(t)
			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts", "verify-S4.5.sh"), "--kind")
			cmd.Env = append(append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fake, "MODE=fail",
				"DRAIN_BIN="+filepath.Join(bin, "drain-outbox")), tc.env...)
			cmd.WaitDelay = 10 * time.Second
			b, _ := cmd.CombinedOutput()
			out := string(b)
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("want %q in:\n%s", w, out)
				}
			}
			for _, w := range tc.bad {
				if strings.Contains(out, w) {
					t.Errorf("unexpected %q in:\n%s", w, out)
				}
			}
			if strings.Contains(out, "p@ss:w/rd") || strings.Contains(out, "p%40ss") {
				t.Errorf("the broker password reached the output:\n%s", out)
			}
			ev, _ := filepath.Glob(filepath.Join(root, "docs", "evidence", "S4.5", "kind-*.txt"))
			for _, e := range ev {
				if b, _ := os.ReadFile(e); strings.Contains(string(b), "p@ss:w/rd") || strings.Contains(string(b), "p%40ss") {
					t.Errorf("the broker password reached the evidence")
				}
			}
			// Order: the last scale-down, then the drain, then the check Job.
			callsB, _ := os.ReadFile(filepath.Join(fake, "calls"))
			calls := strings.Split(strings.TrimSpace(string(callsB)), "\n")
			lastDown, drain, check := -1, -1, -1
			for i, c := range calls {
				switch {
				case strings.HasPrefix(c, "scale ") && strings.HasSuffix(c, " --replicas 0"):
					lastDown = i
				case strings.HasPrefix(c, "drain-outbox"):
					drain = i
				case strings.HasPrefix(c, "logs job/s45-check-") && check < 0:
					check = i
				}
			}
			if lastDown < 0 || drain < lastDown || check < drain {
				t.Errorf("order: last scale-down at %d, drain at %d, check read at %d:\n%s", lastDown, drain, check, callsB)
			}
		})
	}
}

// With internal auth on (#29), the drain sends the app sender token: the drainer gets it as a
// file from the storage release's Secret, the file is gone after the drain, and the token is
// in no output or evidence. Off (every other case), the drainer gets no token file.
func TestVerifyS45KindDrainSendsTheTokenWithAuthOn(t *testing.T) {
	for _, auth := range []string{"on", "off"} {
		t.Run("auth "+auth, func(t *testing.T) {
			root, fake, bin := s45Fixture(t)
			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts", "verify-S4.5.sh"), "--kind")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fake, "MODE=fail",
				"DRAIN_BIN="+filepath.Join(bin, "drain-outbox"), "FAKE_AUTH="+auth)
			cmd.WaitDelay = 10 * time.Second
			b, _ := cmd.CombinedOutput()
			out := string(b)
			for _, w := range []string{"PASS  drained the outbox with the app at 0",
				"PASS  invcheck check (strict): claim invariants clean, no write during the walk, outbox pending 0 on every shard"} {
				if !strings.Contains(out, w) {
					t.Errorf("want %q in:\n%s", w, out)
				}
			}
			token, _ := os.ReadFile(filepath.Join(fake, "drain-token"))
			path, _ := os.ReadFile(filepath.Join(fake, "drain-token-path"))
			if auth == "off" {
				if len(token) != 0 || len(path) != 0 {
					t.Fatalf("auth off: the drainer got a token file %q", path)
				}
				return
			}
			if string(token) != "s45-app-sender-token" {
				t.Fatalf("auth on: the drainer sent %q, want the app sender token", token)
			}
			if p := strings.TrimSpace(string(path)); p == "" {
				t.Fatal("auth on: no token file")
			} else if _, err := os.Stat(p); err == nil {
				t.Errorf("auth on: token file %s left after the drain", p)
			}
			ev, _ := filepath.Glob(filepath.Join(root, "docs", "evidence", "S4.5", "kind-*.txt"))
			for _, e := range append(ev, "") {
				var body []byte
				if e == "" {
					body = b
				} else if body, _ = os.ReadFile(e); body == nil {
					continue
				}
				if strings.Contains(string(body), "s45-app-sender-token") || strings.Contains(string(body), "s45-node-sender-token") {
					t.Errorf("a sender token reached %q", e)
				}
			}
		})
	}
}
