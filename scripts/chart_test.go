package scripts

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// The storage chart renders with no duplicate mapping key (#66). Helm and the
// PyYAML parse in verify-storage-k8s.sh keep the last of two equal keys
// silently; a strict parser (go.yaml.in/yaml/v3, kubectl's strict decoding)
// rejects the whole document. Locally it needs helm (make doctor checks it);
// CI's smoke job, which installs helm, runs it with REQUIRE_HELM=1.
func TestStorageChartNoDuplicateKeys(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("REQUIRE_HELM") != "" {
			t.Fatalf("REQUIRE_HELM is set but helm is not on PATH: %v", err)
		}
		t.Skip("helm not on PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, helm, "template", "storage", "../deploy/helm/storage", "--namespace", "reliefgrid").Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("helm template: %v\n%s", err, ee.Stderr)
		}
		t.Fatalf("helm template: %v", err)
	}
	docs := 0
	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		docs++
		source, _, _ := strings.Cut(strings.TrimPrefix(strings.TrimSpace(doc), "---\n"), "\n")
		var v any
		if err := yaml.Unmarshal([]byte(doc), &v); err != nil {
			t.Errorf("%s: %v", source, err)
		}
	}
	if docs == 0 {
		t.Fatal("helm template rendered no documents")
	}
}
