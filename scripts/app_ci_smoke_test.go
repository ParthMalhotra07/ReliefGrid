package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppCIEntryPointRunsAfterStorage(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "ci-smoke").CombinedOutput()
	if err != nil {
		t.Fatalf("make dry run: %v\n%s", err, out)
	}
	storage := strings.Index(string(out), "bash scripts/ci-smoke.sh")
	app := strings.Index(string(out), "bash scripts/app-ci-smoke.sh")
	if storage < 0 || app <= storage {
		t.Fatalf("CI must verify prebuilt storage before the app:\n%s", out)
	}
}

func TestAppCISmokePropagatesFailuresAndRetainsEvidence(t *testing.T) {
	for _, failure := range []string{"", "app-deploy", "e2e-smoke"} {
		t.Run("failure="+failure, func(t *testing.T) {
			bin, evidence := t.TempDir(), t.TempDir()
			calls := filepath.Join(t.TempDir(), "calls")
			fake := `#!/usr/bin/env bash
echo "$* namespace=$NAMESPACE evidence=$A15_EVIDENCE_DIR" >> "$APP_TEST_CALLS"
echo "observed $*"
if [ "$*" = "$APP_TEST_FAILURE" ]; then exit 17; fi
`
			if err := os.WriteFile(filepath.Join(bin, "make"), []byte(fake), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/usr/bin/env bash\necho diagnostics\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "bash", "app-ci-smoke.sh")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "APP_TEST_CALLS="+calls,
				"APP_TEST_FAILURE="+failure, "EVIDENCE_DIR="+evidence, "SMOKE_RUN_ID=ci-test-1",
				"NAMESPACE=storage-original", "APP_CI_NAMESPACE=reliefgrid-app-smoke")
			out, err := cmd.CombinedOutput()
			if (failure == "") != (err == nil) {
				t.Fatalf("failure=%q err=%v\n%s", failure, err, out)
			}
			data, readErr := os.ReadFile(calls)
			if readErr != nil {
				t.Fatal(readErr)
			}
			text := string(data)
			if !strings.Contains(text, "app-deploy namespace=reliefgrid-app-smoke") || strings.Contains(text, "storage-original") {
				t.Fatalf("app must use its own namespace: %s", text)
			}
			if failure == "app-deploy" && strings.Contains(text, "e2e-smoke") {
				t.Fatalf("smoke ran after failed deployment: %s", text)
			}
			if failure != "app-deploy" && !strings.Contains(text, "e2e-smoke namespace=reliefgrid-app-smoke") {
				t.Fatalf("missing end-to-end verification: %s", text)
			}
			root := filepath.Join(evidence, "app-ci-test-1")
			for _, name := range []string{"app-deploy.log", "diagnostics/pods.txt"} {
				if _, err := os.Stat(filepath.Join(root, name)); err != nil {
					t.Errorf("missing success/failure evidence %s: %v", name, err)
				}
			}
		})
	}
}
