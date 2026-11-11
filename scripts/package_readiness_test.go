package scripts

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed package_readiness.py
var packageReadinessSource []byte

//go:embed package_readiness_test.py
var packageReadinessTests []byte

func TestPackageReadinessRegressions(t *testing.T) {
	directory := t.TempDir()
	for name, data := range map[string][]byte{"package_readiness.py": packageReadinessSource, "package_readiness_test.py": packageReadinessTests} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.CommandContext(t.Context(), "python3", filepath.Join(directory, "package_readiness_test.py"), "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("package readiness: %v\n%s", err, output)
	}
}
