package scripts

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Use the actual shared helper, not a source-substring check. An unreadable
// existing release must be refused before the default destructive reset.
func TestSchedulesAuthPreflightBeforeReset(t *testing.T) {
	for _, tc := range []struct {
		name, values, reset, after      string
		wantReset, wantChild, wantToken bool
	}{
		{"unreadable-default-reset", "", "", "", false, false, false},
		{"unreadable-both-default", "", "", "", false, false, false},
		{"unreadable-explicit-reset", "", "1", "", false, false, false},
		{"enabled-default-reset-refused", authOn, "", "", false, false, false},
		{"enabled-no-reset", authOn, "0", "", false, true, true},
		{"disabled-reset-allowed", authOff, "1", authOff, true, true, false},
		{"post-reset-read-failure", authOff, "1", "", true, false, false},
		{"post-reset-auth-reloaded", authOff, "1", authOn, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, env := authFakes(t, tc.values, bothTokens)
			fixture := filepath.Join(dir, "fixture")
			scripts := filepath.Join(fixture, "scripts")
			if err := os.MkdirAll(scripts, 0755); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"test-schedules.sh", "schedules-forward.sh", "internal-auth-env.sh"} {
				data, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(scripts, name), data, 0755); err != nil {
					t.Fatal(err)
				}
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Rename(filepath.Join(bin, "kubectl"), filepath.Join(bin, "kubectl-secret")); err != nil {
				t.Fatal(err)
			}
			stubs := map[string]string{
				"kubectl": `#!/usr/bin/env bash
echo "kubectl $*" >> "$ARGV"
case "$*" in
 *"get secret"*) exec "$(dirname "$0")/kubectl-secret" "$@";;
 *"get deployment reliefgrid-matcher"*) printf 2;;
 *"get statefulset storage-shard-1"*) printf 3;;
 *"port-forward --address"*) exec sleep 30;;
esac
`,
				"make": `#!/usr/bin/env bash
echo "make $*" >> "$ARGV"
if [ "$*" = demo-reset ];then printf '%s' "$RESET_AUTH_VALUES" > "$VALUES";fi
`,
				"go": `#!/usr/bin/env bash
echo go-started >> "$ARGV"
if [ "$EXPECT_SENDER" = true ];then
 [ -n "${INTERNAL_AUTH_TOKEN_FILE:-}" ] && [ "$(cat "$INTERNAL_AUTH_TOKEN_FILE")" = "$EXPECTED_APP_TOKEN" ] || exit 71
 echo sender-file-ok >> "$ARGV"
fi
exit 23
`,
				"curl": `#!/usr/bin/env bash
if [ "$EXPECT_SENDER" = true ];then
 header="";previous=""
 for arg in "$@";do [ "$previous" = -H ] && header="$arg";previous="$arg";done
 [ -n "$header" ] && [ "$(cat "${header#@}")" = "X-ReliefGrid-Internal-Token: $EXPECTED_APP_TOKEN" ] || exit 72
 echo sender-header-ok >> "$ARGV"
fi
printf '{"events":[]}'
`,
			}
			for name, data := range stubs {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(data), 0755); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", filepath.Join(scripts, "test-schedules.sh"))
			sender := "false"
			if tc.wantToken {
				sender = "true"
			}
			backend := "cluster"
			if tc.name == "unreadable-both-default" {
				backend = "both"
			}
			command.Env = append(env, "SCHEDULE_BACKEND="+backend, "SCHEDULE_RESET="+tc.reset,
				"SCHEDULE_RUN_ID=auth-preflight", "SCHEDULE_IDS=4", "SEEDS=1",
				"MQ_AMQP_URL=amqp://isolated/", "MQ_MANAGEMENT_URL=http://isolated/",
				"RESET_AUTH_VALUES="+tc.after, "EXPECT_SENDER="+sender, "EXPECTED_APP_TOKEN="+appToken)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatal("fixture child failure or preflight refusal was reported as success")
			}
			if ctx.Err() != nil {
				t.Fatalf("runner hung: %s", output)
			}
			data, err := os.ReadFile(filepath.Join(dir, "argv"))
			if err != nil {
				t.Fatal(err)
			}
			calls := string(data)
			noTokenIn(t, "command lines", data)
			noTokenIn(t, "runner output", output)
			if got := strings.Contains(calls, "make demo-reset"); got != tc.wantReset {
				t.Fatalf("reset=%v want=%v: %s\n%s", got, tc.wantReset, calls, output)
			}
			if got := strings.Contains(calls, "go-started"); got != tc.wantChild {
				t.Fatalf("child=%v want=%v: %s\n%s", got, tc.wantChild, calls, output)
			}
			if !tc.wantChild && (strings.Contains(calls, "scale ") || strings.Contains(calls, "storage-image") || strings.Contains(calls, "image.tag=")) {
				t.Fatalf("cluster changed after refused preflight: %s", calls)
			}
			if tc.wantReset && strings.Index(calls, "helm ") > strings.Index(calls, "make demo-reset") {
				t.Fatalf("reset preceded auth-state read: %s", calls)
			}
			if tc.wantReset && strings.Count(calls, "get values storage") < 2 {
				t.Fatalf("auth state not reloaded after reset: %s", calls)
			}
			if tc.wantToken && tc.wantChild && (!strings.Contains(calls, "sender-file-ok") || strings.Count(calls, "sender-header-ok") != 4) {
				t.Fatalf("post-setup credential not passed to all consumers: %s", calls)
			}
			if left := tempLeft(t, dir); len(left) != 0 {
				t.Fatalf("credential files left behind: %v", left)
			}
		})
	}
}
