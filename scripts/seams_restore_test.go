package scripts_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A passing fault test is not a successful verification if its cleanup leaves
// the cluster running the instrumented image. Exercise the actual shell trap.
func TestVerifySeamsFailsIfDefaultImageRestoreFails(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0755); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("verify-S4.6b.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "verify.sh")
	if err := os.WriteFile(script, body, 0755); err != nil {
		t.Fatal(err)
	}
	stubs := map[string]string{
		"kind":    "#!/usr/bin/env bash\nprintf 'reliefgrid\\n'\n",
		"kubectl": "#!/usr/bin/env bash\nexit 0\n",
		"make":    "#!/usr/bin/env bash\nexit 0\n",
		"helm":    "#!/usr/bin/env bash\ncase \"$*\" in *image.tag=dev*) exit 9;;esac\nexit 0\n",
		"python3": "#!/usr/bin/env bash\nprintf '12345'\n",
		"curl":    "#!/usr/bin/env bash\nexit 0\n",
		"go":      "#!/usr/bin/env bash\nprintf 'PASS\\n'\nexit 0\n",
	}
	for name, content := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "SEAMS_CONTEXT=kind-reliefgrid", "DEFAULT_TAG=dev")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("cleanup hung: %s", out)
	}
	if err == nil {
		t.Fatalf("restore failure returned success: %s", out)
	}
	if !strings.Contains(string(out), "RESTORE FAILED") {
		t.Fatalf("did not exercise restore failure: %s", out)
	}
}
