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

func TestAutoscalingRefusesAnAppOwnedStorageRelease(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "mutation")
	stub := `#!/usr/bin/env bash
case "$*" in
  *' get values '*) printf '%s\n' '{"storage":{"enabled":true}}' ;;
  *) touch "$MUTATION_MARKER"; exit 99 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "helm"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "autoscaling-profile.sh", "true")
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "MUTATION_MARKER="+marker)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "requires the standalone storage release") {
		t.Fatalf("unsafe release was not refused before mutation: %v\n%s", err, out)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Helm was allowed to mutate an app-owned storage release: %v", err)
	}
}
