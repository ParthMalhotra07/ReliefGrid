package scripts

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Embed both inputs so Python-only edits change the Go test action/cache key.
// The subprocess runs precisely those compiled inputs in an isolated directory.
//
//go:embed capture_fixed_profile.py
var fixedProfileSource []byte

//go:embed capture_fixed_profile_test.py
var fixedProfileTests []byte

func TestFixedProfileCaptureRegressions(t *testing.T) {
	directory := t.TempDir()
	for name, content := range map[string][]byte{"capture_fixed_profile.py": fixedProfileSource, "capture_fixed_profile_test.py": fixedProfileTests} {
		if err := os.WriteFile(filepath.Join(directory, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.CommandContext(t.Context(), "python3", filepath.Join(directory, "capture_fixed_profile_test.py"), "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("fixed-profile evidence: %v\n%s", err, output)
	}
}
