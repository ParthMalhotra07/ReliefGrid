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

func testScheduleRollback(t *testing.T, goExit, failRestore, selection, failImage string) {
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
		"make":    "#!/usr/bin/env bash\nexit 0\n",
		"curl":    "#!/usr/bin/env bash\nprintf '{\"events\":[]}'\n",
		"jq":      "#!/usr/bin/env bash\nif [ \"$SCHEDULE_GO_EXIT\" = foreign ]; then exit 1; fi\nexit 0\n",
		"python3": "#!/usr/bin/env bash\nprintf '12345'\n",
		"go":      "#!/usr/bin/env bash\necho go-started >> \"$SCHEDULE_TEST_LOG\"\nif [ \"$SCHEDULE_GO_EXIT\" = foreign ]; then exit 0; fi\nexit \"$SCHEDULE_GO_EXIT\"\n",
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
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "SCHEDULE_BACKEND=cluster", "SCHEDULE_RESET=0", "SCHEDULE_RUN_ID=abort-check", "SEEDS=1", "MQ_AMQP_URL=amqp://isolated-test/", "MQ_MANAGEMENT_URL=http://isolated-test/", "SCHEDULE_TEST_LOG="+log, "SCHEDULE_GO_EXIT="+goExit, "SCHEDULE_FAIL_RESTORE="+failRestore, "SCHEDULE_IDS="+selection, "SCHEDULE_FAIL_IMAGE="+failImage)
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
