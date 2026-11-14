package scripts

import (
	"os"
	"os/exec"
	"testing"
)

func TestOperationalInternalAuthCallers(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "python3", "-I", "operational-auth-test.py")
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1", "INTERNAL_AUTH_TOKEN_FILE=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("operational auth: %v\n%s", err, out)
	}
}
