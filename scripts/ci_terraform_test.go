package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// S5.1: Terraform is pinned in tools.mk with the sha256 of its linux/amd64 zip,
// like kind, kubectl and helm. ci-tools.sh installs a named subset of the tools
// (the default stays kind, kubectl and helm), and a terraform job runs fmt and
// validate (offline: no backend, no credentials) when a PR touches infra/gcp.

func makePin(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-s", "print-"+name).Output()
	if err != nil || strings.TrimSpace(string(out)) == "" {
		t.Fatalf("make print-%s: %v %q", name, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestTerraformPinned(t *testing.T) {
	v := makePin(t, "TERRAFORM_VERSION")
	if v != "1.17.1" {
		t.Errorf("TERRAFORM_VERSION = %q, want 1.17.1", v)
	}
	if sum := makePin(t, "TERRAFORM_SHA256"); !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(sum) {
		t.Errorf("TERRAFORM_SHA256 %q is not 64 hex digits", sum)
	}
	b, err := os.ReadFile(filepath.Join(tfDir, "versions.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"`+">= "+v) {
		t.Errorf("versions.tf required_version does not start at the pinned %s", v)
	}
}

func TestCIToolsTerraform(t *testing.T) {
	script, err := filepath.Abs("ci-tools.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script, t.TempDir(), "terraform")
	cmd.Env = append(os.Environ(), "CI_TOOLS_DRY_RUN=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ci-tools.sh <bin> terraform: %v\n%s", err, out)
	}
	v := makePin(t, "TERRAFORM_VERSION")
	want := "terraform https://releases.hashicorp.com/terraform/" + v + "/terraform_" + v + "_linux_amd64.zip " + makePin(t, "TERRAFORM_SHA256")
	if got := strings.TrimSpace(string(out)); got != want {
		t.Errorf("dry run:\n%s\nwant:\n%s", got, want)
	}
	cmd = exec.CommandContext(t.Context(), "bash", script, t.TempDir(), "nosuchtool")
	cmd.Env = append(os.Environ(), "CI_TOOLS_DRY_RUN=1")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Errorf("an unknown tool was accepted: %s", out)
	}
}

func TestCIChangesInfra(t *testing.T) {
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{"infra/gcp/cluster.tf"}, "infra=true"},
		{[]string{"infra/gcp/.terraform.lock.hcl"}, "infra=true"},
		{[]string{"tools.mk"}, "infra=true"}, // the terraform pin
		{[]string{".github/workflows/ci.yml"}, "infra=true"},
		{[]string{"scripts/gke_test.go"}, "infra=true"}, // the checks it runs
		{[]string{"scripts/ci-tools.sh"}, "infra=true"},
		{[]string{"internal/ring/ring.go"}, "infra=false"},
		{[]string{"docs/a.md"}, "infra=false"},
		{[]string{"infranotes.md"}, "infra=false"},
	} {
		t.Run(strings.Join(tc.files, ","), func(t *testing.T) {
			if lines := ciChanges(t, tc.files); !slices.Contains(lines, tc.want) {
				t.Errorf("got %q, want a line %q", lines, tc.want)
			}
		})
	}
}

func TestCITerraformJob(t *testing.T) {
	w := loadCI(t)
	j := mustJob(t, w, "terraform")
	if !slices.Contains(j.needs(), "changes") || !strings.Contains(j.If, "needs.changes.outputs.infra == 'true'") {
		t.Errorf("terraform needs %v if %q; it runs when changes says infra=true", j.needs(), j.If)
	}
	if j.Timeout == 0 || j.Timeout > 10 {
		t.Errorf("terraform timeout-minutes = %d, want 1..10", j.Timeout)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.Contains(s.Run, "scripts/ci-tools.sh") && strings.Contains(s.Run, "terraform")
	}) {
		t.Error("terraform does not install the pinned terraform with scripts/ci-tools.sh")
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool {
		return strings.Contains(s.Run, "TestGCPTerraform") && s.Env["REQUIRE_TERRAFORM"] != ""
	}) {
		t.Error("terraform does not run the TestGCPTerraform tests with REQUIRE_TERRAFORM set")
	}
	if strings.Contains(j.text(), "apply") || strings.Contains(j.text(), "google-github-actions/auth") {
		t.Error("the terraform job must stay offline: no apply, no cloud credentials")
	}
	if c := mustJob(t, w, "changes"); c.Outputs["infra"] == "" {
		t.Error("changes has no infra output")
	}
	// smoke has helm: it runs the GKE chart render tests too, not only the duplicate-key check.
	if !slices.ContainsFunc(mustJob(t, w, "smoke").Steps, func(s step) bool {
		return strings.Contains(s.Run, "TestStorageValues") && s.Env["REQUIRE_HELM"] != ""
	}) {
		t.Error("smoke does not run the TestStorageValues render tests with REQUIRE_HELM set")
	}
}
