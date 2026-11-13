package scripts

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The S-side callers of the internal HTTP routes (#29) get their sender tokens through
// scripts/internal-auth-env.sh. These tests run it, and each caller's piece, against a
// fake helm and kubectl: with internalAuth.enabled=false nothing may change; with it on,
// tokens go only into 0600 files that are removed afterwards.

const (
	appToken  = "app-sender-token-0123456789abcdefghijklmnopqrstuvwxyzABCDEF"
	nodeToken = "node-sender-token-ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210zyxw"
)

// authFakes writes fake helm and kubectl into a new directory and returns it with the
// environment that points the fakes at their answers. values is the storage release's
// computed values (helm get values -a -o json); "" makes helm fail. secrets maps a
// Secret name to its sender-token bytes.
func authFakes(t *testing.T, values string, secrets map[string]string) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	sec := filepath.Join(dir, "secrets")
	for _, d := range []string{bin, sec} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, tok := range secrets {
		body, _ := json.Marshal(map[string]any{"data": map[string]string{"sender-token": base64.StdEncoding.EncodeToString([]byte(tok))}})
		if err := os.WriteFile(filepath.Join(sec, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	valuesFile := filepath.Join(dir, "values.json")
	if err := os.WriteFile(valuesFile, []byte(values), 0o644); err != nil {
		t.Fatal(err)
	}
	fakes := map[string]string{
		"helm": `#!/bin/sh
echo "helm $*" >> "$ARGV"
[ -s "$VALUES" ] || exit 1
cat "$VALUES"
`,
		"kubectl": `#!/bin/sh
echo "kubectl $*" >> "$ARGV"
for a in "$@"; do last2="$last1"; last1="$a"; done
# kubectl --context C -n N get secret NAME -o json
name=""; prev=""
for a in "$@"; do [ "$prev" = secret ] && name="$a"; prev="$a"; done
[ -n "$name" ] && [ -f "$SECRETS/$name" ] || exit 1
cat "$SECRETS/$name"
`,
	}
	for name, body := range fakes {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tmp := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "VALUES="+valuesFile, "SECRETS="+sec,
		"ARGV="+filepath.Join(dir, "argv"), "TMPDIR="+tmp)
	return dir, env
}

const (
	authOn  = `{"internalAuth":{"enabled":true},"image":{"tag":"dev"}}`
	authOff = `{"internalAuth":{"enabled":false},"image":{"tag":"dev"}}`
)

var bothTokens = map[string]string{"reliefgrid-internal-auth": appToken, "reliefgrid-node-auth": nodeToken}

func runBash(t *testing.T, env []string, script string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func tempLeft(t *testing.T, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "tmp", "*"))
	return m
}

func noTokenIn(t *testing.T, where string, b []byte) {
	t.Helper()
	for _, tok := range []string{appToken, nodeToken} {
		if strings.Contains(string(b), tok) {
			t.Errorf("a sender token appears in %s", where)
		}
	}
}

func TestInternalAuthHelperOff(t *testing.T) {
	for name, values := range map[string]string{
		"enabled false":     authOff,
		"no internalAuth":   `{"image":{"tag":"dev"}}`,
		"internalAuth null": `{"internalAuth":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir, env := authFakes(t, values, bothTokens)
			job := `{"spec":{"template":{"spec":{"containers":[{"name":"c"}]}}}}`
			out, err := runBash(t, append(env, "INTERNAL_AUTH_TOKEN_FILE=/operator/own"), `set -euo pipefail
. ../scripts/internal-auth-env.sh
internal_auth_setup kind-x ns
echo "on=$INTERNAL_AUTH_ON curl=${#INTERNAL_AUTH_CURL[@]} node=${#NODE_AUTH_CURL[@]} dir=[$INTERNAL_AUTH_DIR]"
echo "app=${INTERNAL_AUTH_TOKEN_FILE-unset} nodefile=${NODE_AUTH_SENDER_TOKEN_FILE-unset}"
printf '%s' '`+job+`' | internal_auth_patch_job --node > "$TMPDIR/../patched"
echo "overrides=[$(internal_auth_run_overrides c)]"
internal_auth_cleanup
echo "after app=${INTERNAL_AUTH_TOKEN_FILE-unset}"
`)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			for _, want := range []string{"on=false curl=0 node=0 dir=[]", "app=/operator/own nodefile=unset", "overrides=[]", "after app=/operator/own"} {
				if !strings.Contains(out, want) {
					t.Errorf("want %q in:\n%s", want, out)
				}
			}
			if got, _ := os.ReadFile(filepath.Join(dir, "patched")); string(got) != job {
				t.Errorf("off: the Job changed:\n%s", got)
			}
			if argv, _ := os.ReadFile(filepath.Join(dir, "argv")); strings.Contains(string(argv), "kubectl") {
				t.Errorf("off: a Secret was read:\n%s", argv)
			}
			if left := tempLeft(t, dir); len(left) > 0 {
				t.Errorf("off: files written: %v", left)
			}
		})
	}
}

func TestInternalAuthHelperOn(t *testing.T) {
	dir, env := authFakes(t, authOn, bothTokens)
	out, err := runBash(t, env, `set -euo pipefail
. ../scripts/internal-auth-env.sh
INTERNAL_AUTH_RELEASE=storage-x internal_auth_setup kind-x ns
echo "on=$INTERNAL_AUTH_ON"
echo "dirmode=$(stat -c %a "$INTERNAL_AUTH_DIR")"
for f in "$INTERNAL_AUTH_DIR"/*; do echo "file $(basename "$f") $(stat -c %a "$f")"; done
echo "appfile=$INTERNAL_AUTH_TOKEN_FILE"
echo "nodefile=$NODE_AUTH_SENDER_TOKEN_FILE"
echo "exported=$(bash -c 'echo "$INTERNAL_AUTH_TOKEN_FILE|$NODE_AUTH_SENDER_TOKEN_FILE"')"
echo "curl=${INTERNAL_AUTH_CURL[*]}"
echo "nodecurl=${NODE_AUTH_CURL[*]}"
cp "$INTERNAL_AUTH_TOKEN_FILE" "$TMPDIR/../app.copy"
cp "$NODE_AUTH_SENDER_TOKEN_FILE" "$TMPDIR/../node.copy"
cp "${INTERNAL_AUTH_CURL[1]#@}" "$TMPDIR/../app.header"
d="$INTERNAL_AUTH_DIR"
internal_auth_cleanup
[ ! -e "$d" ] && echo "removed"
echo "after=${INTERNAL_AUTH_TOKEN_FILE-unset}|${NODE_AUTH_SENDER_TOKEN_FILE-unset}|${#INTERNAL_AUTH_CURL[@]}"
internal_auth_cleanup
echo "second cleanup ok"
`)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, want := range []string{"on=true", "dirmode=700", "file app-sender-token 600", "file node-sender-token 600",
		"file app-header 600", "file node-header 600", "removed", "after=unset|unset|0", "second cleanup ok"} {
		if !strings.Contains(out, want) {
			t.Errorf("want %q in:\n%s", want, out)
		}
	}
	if !regexp.MustCompile(`exported=/\S+/app-sender-token\|/\S+/node-sender-token`).MatchString(out) {
		t.Errorf("token files not exported to children:\n%s", out)
	}
	if !regexp.MustCompile(`curl=-H @/\S+/app-header\n`).MatchString(out) || !regexp.MustCompile(`nodecurl=-H @/\S+/node-header\n`).MatchString(out) {
		t.Errorf("curl arguments:\n%s", out)
	}
	for file, want := range map[string]string{"app.copy": appToken, "node.copy": nodeToken,
		"app.header": "X-ReliefGrid-Internal-Token: " + appToken + "\n"} {
		if got, _ := os.ReadFile(filepath.Join(dir, file)); string(got) != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
	if !strings.Contains(string(argv), "helm --kube-context kind-x -n ns get values storage-x -a -o json") {
		t.Errorf("release values not read as expected:\n%s", argv)
	}
	noTokenIn(t, "a command line", argv)
	noTokenIn(t, "the output", []byte(out))
	if left := tempLeft(t, dir); len(left) > 0 {
		t.Errorf("token files left after cleanup: %v", left)
	}
}

// An unreadable release or a malformed credential stops the caller: never "off", never a
// partial set of files.
func TestInternalAuthHelperRefuses(t *testing.T) {
	for _, c := range []struct {
		name    string
		values  string
		secrets map[string]string
		want    string
	}{
		{"helm fails", "", bothTokens, "refusing to guess"},
		{"values not JSON", "not json", bothTokens, "not JSON"},
		{"enabled not a boolean", `{"internalAuth":{"enabled":"yes"}}`, bothTokens, "not a boolean"},
		{"node Secret missing", authOn, map[string]string{"reliefgrid-internal-auth": appToken}, "cannot read reliefgrid-node-auth/sender-token"},
		{"empty token", authOn, map[string]string{"reliefgrid-internal-auth": "", "reliefgrid-node-auth": nodeToken}, "reliefgrid-internal-auth/sender-token is not a valid credential"},
		{"trailing newline", authOn, map[string]string{"reliefgrid-internal-auth": appToken + "\n", "reliefgrid-node-auth": nodeToken}, "not a valid credential"},
		{"comma", authOn, map[string]string{"reliefgrid-internal-auth": "a,b", "reliefgrid-node-auth": nodeToken}, "not a valid credential"},
		{"space", authOn, map[string]string{"reliefgrid-internal-auth": appToken, "reliefgrid-node-auth": "a b"}, "not a valid credential"},
		{"too long", authOn, map[string]string{"reliefgrid-internal-auth": strings.Repeat("x", 4097), "reliefgrid-node-auth": nodeToken}, "not a valid credential"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, env := authFakes(t, c.values, c.secrets)
			out, err := runBash(t, env, `. ../scripts/internal-auth-env.sh
if internal_auth_setup kind-x ns; then echo "setup passed"; else echo "setup refused"; fi
echo "app=${INTERNAL_AUTH_TOKEN_FILE-unset} curl=${#INTERNAL_AUTH_CURL[@]}"
`)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if !strings.Contains(out, "setup refused") || !strings.Contains(out, c.want) || !strings.Contains(out, "app=unset curl=0") {
				t.Errorf("want a refusal naming %q:\n%s", c.want, out)
			}
			if left := tempLeft(t, dir); len(left) > 0 {
				t.Errorf("files left after a refusal: %v", left)
			}
		})
	}
	// The 4096-byte limit itself is accepted.
	dir, env := authFakes(t, authOn, map[string]string{"reliefgrid-internal-auth": strings.Repeat("x", 4096), "reliefgrid-node-auth": nodeToken})
	if out, err := runBash(t, env, `set -e; . ../scripts/internal-auth-env.sh; internal_auth_setup kind-x ns; internal_auth_cleanup`); err != nil {
		t.Errorf("4096 bytes refused: %v\n%s", err, out)
	} else if left := tempLeft(t, dir); len(left) > 0 {
		t.Errorf("files left: %v", left)
	}
}

// patchJob runs internal_auth_patch_job on job with auth on.
func patchJob(t *testing.T, job string, args ...string) (map[string]any, string, error) {
	t.Helper()
	_, env := authFakes(t, authOn, bothTokens)
	out, err := runBash(t, append(env, "JOB="+job), `set -euo pipefail
. ../scripts/internal-auth-env.sh
internal_auth_detect kind-x ns
printf '%s' "$JOB" | internal_auth_patch_job `+strings.Join(args, " "))
	if err != nil {
		return nil, out, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return m, out, nil
}

func TestInternalAuthPatchJob(t *testing.T) {
	job := `{"spec":{"template":{"spec":{"securityContext":{"runAsUser":65532},"volumes":[{"name":"ledger"}],` +
		`"containers":[{"name":"invcheck","volumeMounts":[{"name":"ledger","mountPath":"/ledger"}]}]}}}}`
	for _, node := range []bool{false, true} {
		var args []string
		if node {
			args = []string{"--node"}
		}
		m, out, err := patchJob(t, job, args...)
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		spec := m["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		c := spec["containers"].([]any)[0].(map[string]any)
		wantVols := `[{"name":"ledger"},{"name":"internal-auth","secret":{"defaultMode":288,"items":[{"key":"sender-token","path":"sender-token"}],"secretName":"reliefgrid-internal-auth"}}`
		wantEnv := `[{"name":"INTERNAL_AUTH_TOKEN_FILE","value":"/var/run/reliefgrid/internal-auth/sender-token"}`
		wantMounts := `[{"mountPath":"/ledger","name":"ledger"},{"mountPath":"/var/run/reliefgrid/internal-auth","name":"internal-auth","readOnly":true}`
		if node {
			wantVols += `,{"name":"node-auth","secret":{"defaultMode":288,"items":[{"key":"sender-token","path":"sender-token"}],"secretName":"reliefgrid-node-auth"}}`
			wantEnv += `,{"name":"NODE_AUTH_SENDER_TOKEN_FILE","value":"/var/run/reliefgrid/node-auth/sender-token"}`
			wantMounts += `,{"mountPath":"/var/run/reliefgrid/node-auth","name":"node-auth","readOnly":true}`
		}
		for name, pair := range map[string][2]any{"volumes": {spec["volumes"], wantVols + "]"},
			"env": {c["env"], wantEnv + "]"}, "volumeMounts": {c["volumeMounts"], wantMounts + "]"}} {
			got, _ := json.Marshal(pair[0])
			if string(got) != pair[1] {
				t.Errorf("node=%v %s:\n got %s\nwant %s", node, name, got, pair[1])
			}
		}
		if sc := spec["securityContext"].(map[string]any); sc["fsGroup"] != float64(65532) || sc["runAsUser"] != float64(65532) {
			t.Errorf("securityContext %v", sc)
		}
		noTokenIn(t, "the Job", []byte(out))
	}
	for name, bad := range map[string]string{
		"two containers":    `{"spec":{"template":{"spec":{"containers":[{"name":"a"},{"name":"b"}]}}}}`,
		"conflicting group": `{"spec":{"template":{"spec":{"securityContext":{"fsGroup":1000},"containers":[{"name":"a"}]}}}}`,
	} {
		if _, out, err := patchJob(t, bad); err == nil {
			t.Errorf("%s: patched anyway:\n%s", name, out)
		}
	}
}

func TestInternalAuthRunOverrides(t *testing.T) {
	_, env := authFakes(t, authOn, bothTokens)
	out, err := runBash(t, env, `set -euo pipefail
. ../scripts/internal-auth-env.sh
internal_auth_detect kind-x ns
mapfile -t a < <(internal_auth_run_overrides probe)
echo "${#a[@]}|${a[0]}|${a[1]}"
printf '%s' "${a[2]}"`)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	head, body, _ := strings.Cut(out, "\n")
	if head != "3|--override-type=strategic|--overrides" {
		t.Fatalf("arguments %q", head)
	}
	want := `{"apiVersion":"v1","spec":{"securityContext":{"fsGroup":65532},"volumes":[{"name":"internal-auth","secret":{"secretName":"reliefgrid-internal-auth","defaultMode":288,"items":[{"key":"sender-token","path":"sender-token"}]}}],"containers":[{"name":"probe","env":[{"name":"INTERNAL_AUTH_TOKEN_FILE","value":"/var/run/reliefgrid/internal-auth/sender-token"}],"volumeMounts":[{"name":"internal-auth","mountPath":"/var/run/reliefgrid/internal-auth","readOnly":true}]}]}}`
	if body != want {
		t.Errorf("overrides:\n got %s\nwant %s", body, want)
	}
}

// chaos/scripts/lib.sh start_job: off, the Job is byte-for-byte what main applied
// (testdata from main's start_job); on, it also mounts the app token, and the node token
// only for --probe reads.
func TestChaosStartJobInternalAuth(t *testing.T) {
	golden, err := os.ReadFile("testdata/internal-auth/chaos-start-job.off.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, values string
		probe        bool
	}{{"off", authOff, true}, {"on", authOn, false}, {"on with probe", authOn, true}} {
		t.Run(c.name, func(t *testing.T) {
			dir, env := authFakes(t, c.values, bothTokens)
			args := `load --coordinator http://c:8080 --ledger /ledger/x.json`
			if c.probe {
				args += ` --probe "1=http://p:8080"`
			}
			out, err := runBash(t, append(env, "OUT="+filepath.Join(dir, "job.json")), `set -u
EXPERIMENT=x DESCRIPTION=x
. ../chaos/scripts/lib.sh
kn() { if [ "$1" = apply ]; then jq -S -c . > "$OUT"; fi; }
TOOLS_IMAGE=reliefgrid/tools:test run_id=golden
start_job chaos-load-golden `+args+` && echo started`)
			if err != nil || !strings.Contains(out, "started") {
				t.Fatalf("%v\n%s", err, out)
			}
			got, _ := os.ReadFile(filepath.Join(dir, "job.json"))
			if c.values == authOff {
				if string(got) != string(golden) {
					t.Fatalf("off: the Job differs from main's:\n got %s\nwant %s", got, golden)
				}
				return
			}
			s := string(got)
			if !strings.Contains(s, `"INTERNAL_AUTH_TOKEN_FILE"`) || !strings.Contains(s, `"secretName":"reliefgrid-internal-auth"`) {
				t.Errorf("on: no app token mount:\n%s", s)
			}
			if strings.Contains(s, "NODE_AUTH_SENDER_TOKEN_FILE") != c.probe {
				t.Errorf("on: node token mounted=%v, want %v:\n%s", !c.probe, c.probe, s)
			}
			noTokenIn(t, "the Job", got)
		})
	}
	// A release whose auth state cannot be read never starts a Job.
	dir, env := authFakes(t, "", bothTokens)
	out, _ := runBash(t, append(env, "OUT="+filepath.Join(dir, "job.json")), `set -u
EXPERIMENT=x DESCRIPTION=x
. ../chaos/scripts/lib.sh
kn() { if [ "$1" = apply ]; then cat > "$OUT"; fi; }
start_job chaos-x load && echo started || echo refused`)
	if !strings.Contains(out, "refused") {
		t.Errorf("unknown auth state: %s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
		t.Errorf("a Job was applied with the auth state unknown")
	}
}

// loadtest/run.sh invcheck_job: off, main's Job; on, plus the app token mount.
func TestLoadRunInvcheckJobInternalAuth(t *testing.T) {
	golden, err := os.ReadFile("testdata/internal-auth/run-invcheck-job.off.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range []string{authOff, authOn} {
		_, env := authFakes(t, values, bothTokens)
		out, err := runFuncs(t, []string{"invcheck_job"}, `. ../scripts/internal-auth-env.sh
internal_auth_detect kind-x ns
TOOLS_IMAGE=reliefgrid/tools:test
invcheck_job golden-1 | jq -S -c .`, env...)
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		if values == authOff {
			if out != string(golden) {
				t.Errorf("off: the Job differs from main's:\n got %s\nwant %s", out, golden)
			}
			continue
		}
		if !strings.Contains(out, `"INTERNAL_AUTH_TOKEN_FILE"`) || strings.Contains(out, "NODE_AUTH") || !strings.Contains(out, `"fsGroup":65532`) {
			t.Errorf("on: %s", out)
		}
	}
}

// loadtest/run.sh drain_handoffs with auth on: the drainer gets the app token as a file
// it can read, the file is gone afterwards, and the token is in no log or command line.
// (Off: TestLoadRunDrainHandoffs, unchanged.)
func TestLoadRunDrainHandoffsInternalAuth(t *testing.T) {
	dir, env := authFakes(t, authOn, bothTokens)
	bin := filepath.Join(dir, "drain-outbox")
	if err := os.WriteFile(bin, []byte(`#!/bin/sh
echo "file=$INTERNAL_AUTH_TOKEN_FILE" > "$SEEN"
cat "$INTERNAL_AUTH_TOKEN_FILE" > "$SEEN.token"
echo '{"Read":1,"Published":1,"Marked":1,"Retries":0}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "run", "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	out, err := runFuncs(t, []string{"drain_handoffs"}, `. ../scripts/internal-auth-env.sh
kn() {
	case "$*" in
	"get secret r-rabbitmq -o jsonpath={.data.username}") printf u | base64 ;;
	"get secret r-rabbitmq -o jsonpath={.data.password}") printf p | base64 ;;
	*) return 1 ;;
	esac
}
internal_auth_detect kind-x ns
RELEASE=r DIR="$RUN" DRAIN_BIN="$BIN"
drain_handoffs http://127.0.0.1:5801 127.0.0.1:5999 && echo "drain ok" || echo "drain failed"
echo "after=${INTERNAL_AUTH_TOKEN_FILE-unset}"
`, append(env, "RUN="+filepath.Join(dir, "run"), "BIN="+bin, "SEEN="+filepath.Join(dir, "seen"))...)
	if err != nil || !strings.Contains(out, "drain ok") || !strings.Contains(out, "after=unset") {
		t.Fatalf("%v\n%s", err, out)
	}
	seen, _ := os.ReadFile(filepath.Join(dir, "seen"))
	file := strings.TrimSpace(strings.TrimPrefix(string(seen), "file="))
	if file == "" || !strings.HasSuffix(file, "/app-sender-token") {
		t.Fatalf("drainer saw %q", seen)
	}
	if tok, _ := os.ReadFile(filepath.Join(dir, "seen.token")); string(tok) != appToken {
		t.Errorf("drainer read %q", tok)
	}
	if _, err := os.Stat(file); err == nil {
		t.Errorf("token file %s left after the drain", file)
	}
	for _, f := range []string{"argv", "run/raw/handoff-drain.log", "run/raw/handoff-drain.json"} {
		b, _ := os.ReadFile(filepath.Join(dir, f))
		noTokenIn(t, f, b)
	}
	noTokenIn(t, "the output", []byte(out))
}

// scripts/verify-storage-k8s.sh cnp_run: off, `kubectl run` gets exactly main's arguments;
// on, a tools-image probe also gets the token overrides, and other images never do.
func TestVerifyStorageCnpRunInternalAuth(t *testing.T) {
	src, err := os.ReadFile("verify-storage-k8s.sh")
	if err != nil {
		t.Fatal(err)
	}
	fn := regexp.MustCompile(`(?ms)^cnp_run\(\) \{.*?^\}`).Find(src)
	if fn == nil {
		t.Fatal("no cnp_run")
	}
	for _, values := range []string{authOff, authOn} {
		dir, env := authFakes(t, values, bothTokens)
		script := `. ../scripts/internal-auth-env.sh
internal_auth_detect kind-x ns
tmp="$WORK"; TOOLS_IMAGE=reliefgrid/tools:dev IMAGE=reliefgrid/storagenode:dev
kn() { [ "$1" = run ] && printf '%s\n' "$@" > "$WORK/run-$2"; [ "$1" = get ] && echo Succeeded; return 0; }
sleep() { :; }
` + string(fn) + `
cnp_run probe-tools "$TOOLS_IMAGE" reliefgrid.io/coordinator-client=true /invcheck load --coordinator http://c
cnp_run probe-dial "$IMAGE" "" /storagectl dial --addr c:8080
`
		out, err := runBash(t, append(env, "WORK="+dir), script)
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		tools, _ := os.ReadFile(filepath.Join(dir, "run-probe-tools"))
		dial, _ := os.ReadFile(filepath.Join(dir, "run-probe-dial"))
		wantTools := "run\nprobe-tools\n--image\nreliefgrid/tools:dev\n--image-pull-policy\nNever\n--restart\nNever\n--labels\nreliefgrid.io/coordinator-client=true\n"
		wantDial := "run\nprobe-dial\n--image\nreliefgrid/storagenode:dev\n--image-pull-policy\nNever\n--restart\nNever\n--command\n--\n/storagectl\ndial\n--addr\nc:8080\n"
		if string(dial) != wantDial {
			t.Errorf("%s: storagectl probe arguments:\n%s", values, dial)
		}
		tail := "--command\n--\n/invcheck\nload\n--coordinator\nhttp://c\n"
		if values == authOff {
			if string(tools) != wantTools+tail {
				t.Errorf("off: tools probe arguments differ from main's:\n%s", tools)
			}
			continue
		}
		if !strings.HasPrefix(string(tools), wantTools+"--override-type=strategic\n--overrides\n{") || !strings.HasSuffix(string(tools), tail) ||
			!strings.Contains(string(tools), `"name":"probe-tools"`) {
			t.Errorf("on: tools probe arguments:\n%s", tools)
		}
	}
}

// Every /internal call these scripts make to a storage pod or the coordinator carries the
// auth argument array (empty when auth is off), so none is missed when it is turned on.
func TestInternalCallsCarryTheAuthArguments(t *testing.T) {
	for file, pattern := range map[string]*regexp.Regexp{
		"verify-storage-k8s.sh": regexp.MustCompile(`curl [^\n]*127\.0\.0\.1:\$(c?port|pf_port)/internal/`),
		"verify-S4.6b.sh":       regexp.MustCompile(`curl [^\n]*/internal/`),
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := pattern.FindAll(src, -1)
		if len(lines) == 0 {
			t.Fatalf("%s: no internal curl found", file)
		}
		withToken, without := 0, 0
		for _, l := range lines {
			s := string(l)
			switch {
			case strings.Contains(s, "$cport/") && strings.Contains(s, `"${INTERNAL_AUTH_CURL[@]}"`),
				strings.Contains(s, "$port/") && strings.Contains(s, `"${NODE_AUTH_CURL[@]}"`),
				strings.Contains(s, "$pf_port/") && strings.Contains(s, `"${NODE_AUTH_CURL[@]}"`),
				file == "verify-S4.6b.sh" && strings.Contains(s, `"${INTERNAL_AUTH_CURL[@]}"`):
				withToken++
			case strings.Contains(s, "noauth"):
				without++ // the deliberate requests without a token
			default:
				t.Errorf("%s: an internal call without the auth arguments: %s", file, s)
			}
		}
		t.Logf("%s: %d calls with a token, %d without by design", file, withToken, without)
	}
	// The seams verify sets up the tokens only once its EXIT trap can remove them.
	src, _ := os.ReadFile("verify-S4.6b.sh")
	trap, setup := strings.Index(string(src), "trap restore EXIT"), strings.Index(string(src), "internal_auth_setup")
	if trap < 0 || setup < 0 || setup < trap {
		t.Errorf("verify-S4.6b.sh: internal_auth_setup (at %d) must follow trap restore EXIT (at %d)", setup, trap)
	}
	if !strings.Contains(string(src), "internal_auth_cleanup") {
		t.Errorf("verify-S4.6b.sh: restore does not remove the token files")
	}
}

func TestVerifyStorageRejectsBadAuthSetting(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "bash", "verify-storage-k8s.sh")
	cmd.Env = append(os.Environ(), "STORAGE_INTERNAL_AUTH=yes")
	out, err := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); err == nil || code != 2 || !strings.Contains(string(out), "STORAGE_INTERNAL_AUTH must be true or false") {
		t.Fatalf("exit %d: %s", code, out)
	}
}
