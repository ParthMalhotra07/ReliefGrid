package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// verify-A0.1 validates contracts/public.yaml with the Python
// openapi-spec-validator. It is pinned in tools.mk; make doctor checks it and
// verify-A0.1 names the pinned version when it is missing.

// fakePython puts a python3 first on PATH that prints version for any
// importlib.metadata query, or exits 1 (module missing) when version is "".
func fakePython(t *testing.T, version string) string {
	t.Helper()
	dir := t.TempDir()
	body := "#!/bin/sh\nexit 1\n"
	if version != "" {
		body = "#!/bin/sh\necho " + version + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "python3"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir + string(os.PathListSeparator) + os.Getenv("PATH")
}

func specValidatorPin(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../tools.mk")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^OPENAPI_SPEC_VALIDATOR_VERSION\s*:=\s*(\S+)`).FindSubmatch(b)
	if m == nil {
		t.Fatal("tools.mk pins no OPENAPI_SPEC_VALIDATOR_VERSION")
	}
	return string(m[1])
}

func TestDoctorChecksSpecValidator(t *testing.T) {
	pin := specValidatorPin(t)
	for _, c := range []struct{ installed, want string }{
		{pin, `ok\s+openapi-spec-validator\s+` + regexp.QuoteMeta(pin)},
		{"0.7.2", `MISMATCH\s+openapi-spec-validator\s+want ` + regexp.QuoteMeta(pin) + `, got 0\.7\.2`},
		{"", `MISSING\s+openapi-spec-validator\s+want ` + regexp.QuoteMeta(pin) + `\n\s+pip3 install --user --break-system-packages openapi-spec-validator==` + regexp.QuoteMeta(pin)},
	} {
		cmd := exec.CommandContext(t.Context(), "make", "-s", "-C", "..", "doctor")
		cmd.Env = append(os.Environ(), "PATH="+fakePython(t, c.installed))
		out, _ := cmd.CombinedOutput()
		if !regexp.MustCompile(c.want).Match(out) {
			t.Errorf("installed %q: no line matching %q in:\n%s", c.installed, c.want, out)
		}
	}
}

func TestVerifyA01NamesPinnedSpecValidator(t *testing.T) {
	pin := specValidatorPin(t)
	cmd := exec.CommandContext(t.Context(), "bash", "scripts/verify-A0.1.sh")
	cmd.Dir = ".."
	cmd.Env = append(os.Environ(), "PATH="+fakePython(t, ""))
	out, _ := cmd.CombinedOutput()
	if want := "pip3 install --break-system-packages openapi-spec-validator==" + pin; !strings.Contains(string(out), want) {
		t.Fatalf("missing validator: want %q in:\n%s", want, out)
	}
}
