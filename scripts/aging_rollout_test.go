package scripts

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed aging_rollout.py
var agingRolloutSource []byte

//go:embed aging_rollout_test.py
var agingRolloutTests []byte

func TestAgingRolloutRegressions(t *testing.T) {
	directory := t.TempDir()
	for name, data := range map[string][]byte{"aging_rollout.py": agingRolloutSource, "aging_rollout_test.py": agingRolloutTests} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.CommandContext(t.Context(), "python3", filepath.Join(directory, "aging_rollout_test.py"), "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("aging rollout: %v\n%s", err, output)
	}
}
