package scripts_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A go-test timeout exits the child before its t.Cleanup callbacks execute.
// The outer shell must restore the cluster independently of those callbacks.
func TestSchedulesFailureRestoresShardAndAdmission(t *testing.T) {
	for _, fixture := range []struct {
		name        string
		goExit      string
		failRestore string
		selection   string
		failImage   string
	}{{"test-command-fails", "2", "0", "14", "0"}, {"shard-restore-fails", "0", "1", "14", "0"}, {"seams-test-command-fails", "2", "0", "12", "0"}, {"seams-image-restore-fails", "0", "0", "12", "1"}} {
		t.Run(fixture.name, func(t *testing.T) {
			testScheduleRollback(t, fixture.goExit, fixture.failRestore, fixture.selection, fixture.failImage)
		})
	}
}

func TestSchedulesNonRelaySubsetChecksPendingWorkBeforeStarting(t *testing.T) {
	testScheduleRollback(t, "foreign", "0", "4", "0")
}

func TestSchedulesInternalAuthOnAndUnreadable(t *testing.T) {
	for _, mode := range []string{"on", "unreadable"} {
		t.Run(mode, func(t *testing.T) { testScheduleRollback(t, "2", "0", "4", "0", mode) })
	}
}

func testScheduleRollback(t *testing.T, goExit, failRestore, selection, failImage string, authMode ...string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	scripts := filepath.Join(root, "scripts")
	for _, dir := range []string{bin, scripts} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	source, err := os.ReadFile("test-schedules.sh")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(scripts, "test-schedules.sh")
	if err := os.WriteFile(path, source, 0755); err != nil {
		t.Fatal(err)
	}
	forward, err := os.ReadFile("schedules-forward.sh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scripts, "schedules-forward.sh"), forward, 0755); err != nil {
		t.Fatal(err)
	}

	mode := "off"
	if len(authMode) > 0 {
		mode = authMode[0]
	}
	// This fake is the boundary to #174's independently tested Secret-staging
	// helper. The assertions below exercise this runner's actual curl/go/EXIT
	// orchestration rather than parsing source text for a header substring.
	helper := `INTERNAL_AUTH_CURL=()
internal_auth_setup() {
 echo auth-setup >> "$SCHEDULE_TEST_LOG"
 if [ "$SCHEDULE_AUTH_CASE" = unreadable ];then return 1;fi
 if [ "$SCHEDULE_AUTH_CASE" = on ];then
  umask 077
  _schedule_test_auth_dir="$(mktemp -d "${SCHEDULE_TEST_LOG}.auth.XXXXXX")"
  printf schedule-fixture-credential > "$_schedule_test_auth_dir/token"
  printf 'X-ReliefGrid-Internal-Token: schedule-fixture-credential\n' > "$_schedule_test_auth_dir/header"
  export INTERNAL_AUTH_TOKEN_FILE="$_schedule_test_auth_dir/token"
  INTERNAL_AUTH_CURL=(-H "@$_schedule_test_auth_dir/header")
 fi
}
internal_auth_cleanup() {
 echo auth-cleanup >> "$SCHEDULE_TEST_LOG"
 if [ -n "${_schedule_test_auth_dir:-}" ];then rm -rf -- "$_schedule_test_auth_dir";fi
 unset INTERNAL_AUTH_TOKEN_FILE
}
`
	if err := os.WriteFile(filepath.Join(scripts, "internal-auth-env.sh"), []byte(helper), 0600); err != nil {
		t.Fatal(err)
	}

	stubs := map[string]string{
		"kubectl": `#!/usr/bin/env bash
printf '%s\n' "$*" >> "$SCHEDULE_TEST_LOG"
case "$*" in
 *"get deployment reliefgrid-matcher"*) printf '2';;
 *"get statefulset storage-shard-0"*) printf 'reliefgrid/storagenode:dev';;
 *"get statefulset storage-shard-1"*) printf '3';;
 *"port-forward --address"*) exec sleep 30;;
 *"scale statefulset/storage-shard-1"*) if [ "$SCHEDULE_FAIL_RESTORE" = 1 ];then exit 7;fi;;
esac
`,
		"helm": `#!/usr/bin/env bash
printf '%s\n' "$*" >> "$SCHEDULE_TEST_LOG"
case "$*" in *"image.tag=dev"*) if [ "$SCHEDULE_FAIL_IMAGE" = 1 ];then exit 8;fi;;esac
`,
		"make": "#!/usr/bin/env bash\nexit 0\n",
		"curl": `#!/usr/bin/env bash
if [ "$SCHEDULE_AUTH_CASE" = on ];then
 header="";prev=""
 for arg in "$@";do [ "$prev" = -H ] && header="$arg";prev="$arg";done
 [ "$header" = "@${INTERNAL_AUTH_TOKEN_FILE%/*}/header" ] || exit 71
 grep -qx 'X-ReliefGrid-Internal-Token: schedule-fixture-credential' "${header#@}" || exit 72
 echo auth-curl-ok >> "$SCHEDULE_TEST_LOG"
fi
printf '{"events":[]}'
`,
		"jq":      "#!/usr/bin/env bash\ncat >/dev/null\nif [ \"$SCHEDULE_GO_EXIT\" = foreign ]; then exit 1; fi\nexit 0\n",
		"python3": "#!/usr/bin/env bash\nprintf '12345'\n",
		"go": `#!/usr/bin/env bash
echo go-started >> "$SCHEDULE_TEST_LOG"
if [ "$SCHEDULE_AUTH_CASE" = on ];then
 [ "$(cat "$INTERNAL_AUTH_TOKEN_FILE")" = schedule-fixture-credential ] || exit 73
 echo auth-go-ok >> "$SCHEDULE_TEST_LOG"
fi
if [ "$SCHEDULE_GO_EXIT" = foreign ];then exit 0;fi
exit "$SCHEDULE_GO_EXIT"
`,
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0755); err != nil {
			t.Fatal(err)
		}
	}
	log := filepath.Join(root, "cluster.log")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", path)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "SCHEDULE_BACKEND=cluster", "SCHEDULE_RESET=0", "SCHEDULE_RUN_ID=abort-check", "SEEDS=1", "MQ_AMQP_URL=amqp://isolated-test/", "MQ_MANAGEMENT_URL=http://isolated-test/", "SCHEDULE_TEST_LOG="+log, "SCHEDULE_GO_EXIT="+goExit, "SCHEDULE_FAIL_RESTORE="+failRestore, "SCHEDULE_IDS="+selection, "SCHEDULE_FAIL_IMAGE="+failImage, "SCHEDULE_AUTH_CASE="+mode)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("failed test command was reported as success")
	}
	if ctx.Err() != nil {
		t.Fatalf("cleanup hung: %s", output)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(calls), "auth-cleanup") {
		t.Fatal("runner did not call shared credential cleanup")
	}
	if left, _ := filepath.Glob(log + ".auth.*"); len(left) != 0 {
		t.Fatal("runner left staged credentials after failure")
	}
	if mode == "unreadable" {
		if strings.Contains(string(calls), "go-started") || strings.Contains(string(calls), "scale ") || strings.Contains(string(calls), "image.tag=") {
			t.Fatal("runner changed cluster or ran tests after unreadable auth state")
		}
		return
	}
	if mode == "on" && (strings.Count(string(calls), "auth-curl-ok") != 4 || !strings.Contains(string(calls), "auth-go-ok")) {
		t.Fatalf("runner did not authenticate every preflight and child: %s", calls)
	}

	if goExit == "foreign" && strings.Contains(string(calls), "go-started") {
		t.Fatal("non-relay subset began without a clean pending-outbox preflight")
	}
	expected := []string{"scale statefulset/storage-shard-1 --replicas=3", "rollout status statefulset/storage-shard-1 --timeout=90s", "scale deployment reliefgrid-matcher --replicas=2"}
	if selection == "12" {
		expected = append(expected, "image.tag=dev", "rollout status statefulset/storage-shard-0 --timeout=600s", "reliefgrid.io/chaos-isolated-")
	}
	for _, want := range expected { // independent restoration must run even after another cleanup step fails.
		if !strings.Contains(string(calls), want) {
			t.Errorf("missing rollback %q in %s", want, calls)
		}
	}
}
