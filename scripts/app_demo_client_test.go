package scripts

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestAppDemoClientRetryAndReadAssertions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "demos/app_demo_test.py")
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("app demo transport/strong-read assertions: %v\n%s", err, out)
	}
}
