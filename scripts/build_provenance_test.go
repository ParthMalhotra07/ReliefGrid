package scripts

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed build_provenance.py
var buildProvenanceSource []byte

//go:embed build_provenance_test.py
var buildProvenanceTests []byte

func TestBuildProvenanceRegressions(t *testing.T) {
	directory := t.TempDir()
	for name, data := range map[string][]byte{"build_provenance.py": buildProvenanceSource, "build_provenance_test.py": buildProvenanceTests} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.CommandContext(t.Context(), "python3", filepath.Join(directory, "build_provenance_test.py"), "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("build provenance: %v\n%s", err, output)
	}
}
