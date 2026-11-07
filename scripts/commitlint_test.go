// Package scripts holds tests for the repository's shell scripts.
package scripts

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommitLint runs scripts/commit-lint.sh on a throwaway repository with
// one commit per case. Only real attribution lines fail: a Co-authored-by
// trailer, or a line that starts with "Generated with" (optionally after an
// emoji). The phrase inside prose passes.
func TestCommitLint(t *testing.T) {
	script, err := filepath.Abs("commit-lint.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, msg string
		fail      bool
	}{
		{"regenerated in prose", "feat(contracts): additive fields [S1.4]\n\nAdditive internal.yaml changes, regenerated with make gen.", false},
		{"phrase mid-line", "docs: note\n\nThe table was generated with care.", false},
		{"co-authored-by in prose", "docs: explain\n\nWe do not use co-authored-by: trailers here.", false},
		{"plain", "fix(ring): bound tokens [S0.3]", false},
		{"Generated with a tool name", "feat: x\n\nGenerated with SomeTool", true},
		{"emoji Generated with", "feat: x\n\n🤖 Generated with [SomeTool](https://example.com/sometool)", true},
		{"lowercase, indented", "feat: x\n\n   generated with some tool", true},
		{"Generated with as subject", "Generated with a tool", true},
		{"Co-authored-by trailer", "feat: x\n\nCo-authored-by: Someone <someone@example.com>", true},
		{"lowercase trailer", "feat: x\n\nco-authored-by: Someone <someone@example.com>", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "git", args...)
				cmd.Dir = dir
				cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
					"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			git("init", "-q")
			git("commit", "-q", "--allow-empty", "-m", "base")
			base := git("rev-parse", "HEAD")
			git("commit", "-q", "--allow-empty", "--cleanup=verbatim", "-m", tc.msg)
			head := git("rev-parse", "HEAD")

			cmd := exec.CommandContext(t.Context(), script, base, head)
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			if failed := err != nil; failed != tc.fail {
				t.Fatalf("commit-lint failed=%v (%v), want failed=%v for %q\n%s", failed, err, tc.fail, tc.msg, out)
			}
			if tc.fail {
				var ee *exec.ExitError
				if !errors.As(err, &ee) || ee.ExitCode() != 1 {
					t.Fatalf("exit = %v, want 1 (an attribution line, not a usage error)\n%s", err, out)
				}
			}
		})
	}
}
