package scripts

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// Demo 7's catch-up assertion (caught_up_to in scripts/demos/kube.sh) runs
// against /mgmt/status JSON; these cases need no cluster.
func TestDemo7CaughtUpTo(t *testing.T) {
	kube, err := filepath.Abs("demos/kube.sh")
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, status, target string
		want                 bool
	}{
		// The 2026-11-01 health check, with the app installed: strong-read
		// barriers moved the Raft commit index past the last command, so the
		// FSM's appliedIndex (3592) can never reach the raw target (3970),
		// although the replica has applied every command and says caughtUp.
		{"health check: barriers past the last command", `{"state":"Follower","appliedIndex":3592,"commitIndex":4032,"leaderCommitIndex":4032,"restoredIndex":3592,"caughtUp":true}`, "3970", true},
		{"replayed past the target", `{"appliedIndex":4100,"commitIndex":4100,"leaderCommitIndex":4100,"caughtUp":true}`, "3970", true},
		// Strict: a replica that is really behind still fails.
		{"behind: not caughtUp", `{"appliedIndex":3500,"commitIndex":3500,"leaderCommitIndex":4032,"caughtUp":false}`, "3970", false},
		{"behind: applied past the target but not caughtUp", `{"appliedIndex":3980,"commitIndex":4032,"leaderCommitIndex":4032,"caughtUp":false}`, "3970", false},
		{"caughtUp to an older leader commit", `{"appliedIndex":3592,"commitIndex":3900,"leaderCommitIndex":3900,"caughtUp":true}`, "3970", false},
		{"no leader commit learned yet", `{"appliedIndex":3592,"commitIndex":0,"leaderCommitIndex":0,"caughtUp":true}`, "3970", false},
		{"no status", ``, "3970", false},
		{"no target", `{"appliedIndex":4100,"leaderCommitIndex":4100,"caughtUp":true}`, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "bash", "-c", `. "$1"; caught_up_to "$2" "$3"`, "test", kube, c.status, c.target)
			cmd.Env = append(cmd.Environ(), "REPO_ROOT="+root, "CLUSTER=reliefgrid")
			out, err := cmd.CombinedOutput()
			if got := err == nil; got != c.want {
				t.Errorf("caught_up_to(%s, %s) = %v, want %v (%s)", c.status, c.target, got, c.want, out)
			}
		})
	}
}
