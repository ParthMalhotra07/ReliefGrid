//go:build schedules

package workflow

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Drive kill itself with a fake kubelet that reports one injected restart,
// then optionally one extra crash before readiness. The child lets the parent
// inspect a real testing.T failure without swallowing the helper's Fatal.
func TestKillRejectsExtraRestartBeforeReady(t *testing.T) {
	if os.Getenv("RG_KILL_RESTART_CHILD") == "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		x := &execution{t: t, ctx: ctx}
		c := &kindCluster{t: t, x: x, ctx: "kind-test", ns: "test", fwd: map[string]*forward{}}
		c.kill("storage-shard-0-0")
		return
	}
	for _, extra := range []string{"0", "1"} {
		t.Run("extra="+extra, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			if err := os.Mkdir(bin, 0755); err != nil {
				t.Fatal(err)
			}
			kube := `#!/usr/bin/env bash
set -eu
case "$*" in
 *restartCount*)
  if [ -f "$RG_KILL_STATE/ready" ];then echo "$((6 + RG_KILL_EXTRA))"
  elif [ -f "$RG_KILL_STATE/read" ];then echo 6
  else touch "$RG_KILL_STATE/read";echo 5;fi;;
 *metadata.uid*) echo original-uid;;
 *spec.nodeName*) echo kind-worker;;
 *"wait --for=condition=Ready"*) touch "$RG_KILL_STATE/ready";;
 *"delete pod"*) touch "$RG_KILL_STATE/deleted";;
 *"--ignore-not-found -o json"*)
  test -f "$RG_KILL_STATE/deleted"
  echo '{"metadata":{"uid":"new-uid"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"storagenode","ready":true,"restartCount":0}]}}';;
 *) echo "unexpected kubectl: $*" >&2;exit 9;;
esac
`
			docker := `#!/usr/bin/env bash
set -eu
case "$*" in
 *"crictl ps"*) echo container-id;;
 *"crictl inspect"*) echo 123;;
 *"kill -9"*) exit 0;;
 *) echo "unexpected docker: $*" >&2;exit 9;;
esac
`
			for name, body := range map[string]string{"kubectl": kube, "docker": docker} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0755); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestKillRejectsExtraRestartBeforeReady$", "-test.v")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "RG_KILL_RESTART_CHILD=1", "RG_KILL_STATE="+root, "RG_KILL_EXTRA="+extra)
			out, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("kill helper hung: %s", out)
			}
			if extra == "0" {
				if err != nil {
					t.Fatalf("single injected restart must pass: %v\n%s", err, out)
				}
			} else if err == nil || !strings.Contains(string(out), "restart count") {
				t.Fatalf("extra crash must fail specifically on restart count: error=%v\n%s", err, out)
			}
		})
	}
}
