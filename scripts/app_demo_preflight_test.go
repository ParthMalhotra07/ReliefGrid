package scripts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppDemoLiveVerifyRefusesUnroutedTopologyBeforeReset(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"scripts", "internal/mq", "bin"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	source, err := os.ReadFile("verify-A3.1.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "scripts/verify-A3.1.sh")
	if err := os.WriteFile(script, source, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "internal/mq/definitions.json"), []byte(`{"bindings":[]}`), 0644); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(root, "calls")
	makeScript := "#!/usr/bin/env bash\necho make >> \"$VERIFY_CALLS\"\necho reliefgrid\n"
	if err := os.WriteFile(filepath.Join(root, "bin/make"), []byte(makeScript), 0755); err != nil {
		t.Fatal(err)
	}
	out, code := runScript(t, "", []string{"PATH=" + filepath.Join(root, "bin") + ":" + os.Getenv("PATH"), "VERIFY_CALLS=" + calls}, script, "--live")
	if code == 0 || !strings.Contains(out, "resource.state_changed") {
		t.Fatalf("missing topology did not stop Verify: exit=%d\n%s", code, out)
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("Verify called make/reset before checking topology: %v", err)
	}
}
