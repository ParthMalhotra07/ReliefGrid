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

const s45FakeHelm = `#!/usr/bin/env bash
case "$*" in
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
			root, fake := t.TempDir(), t.TempDir()
			bin := filepath.Join(fake, "bin")
			s45Write(t, filepath.Join(bin, "kubectl"), s45FakeKubectl, 0o755)
			s45Write(t, filepath.Join(bin, "helm"), s45FakeHelm, 0o755)
			s45Write(t, filepath.Join(bin, "kind"), "#!/usr/bin/env bash\necho reliefgrid\n", 0o755)
			for _, f := range []string{"scripts/verify-S4.5.sh", "chaos/scripts/lib.sh", "chaos/scripts/faults.sh"} {
				b, err := os.ReadFile(filepath.Join("..", f))
				if err != nil {
					t.Fatal(err)
				}
				s45Write(t, filepath.Join(root, f), string(b), 0o755)
			}

			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "scripts", "verify-S4.5.sh"), "--kind")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fake, "MODE="+tc.mode)
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

func s45Write(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}
