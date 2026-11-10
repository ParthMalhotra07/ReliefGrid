package scripts

import (
	"os/exec"
	"testing"
)

func TestAppEvidenceIsolation(t *testing.T) {
	output, err := exec.CommandContext(t.Context(), "python3", "app_evidence_test.py", "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("evidence isolation: %v\n%s", err, output)
	}
}
