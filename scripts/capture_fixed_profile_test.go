package scripts

import (
	"os/exec"
	"testing"
)

func TestFixedProfileCaptureRegressions(t *testing.T) {
	output, err := exec.CommandContext(t.Context(), "python3", "capture_fixed_profile_test.py", "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("fixed-profile evidence: %v\n%s", err, output)
	}
}
