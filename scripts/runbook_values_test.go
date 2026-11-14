package scripts

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"go.yaml.in/yaml/v3"
)

const runbook = "../docs/operations/internal-auth.md"

// runbookValuesFunc is the rotation's values() helper, exactly as the runbook prints it.
func runbookValuesFunc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(runbook)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile("(?ms)^values\\(\\) \\{\n.*?^\\}\n").Find(b)
	if m == nil {
		t.Fatal("no values() helper in the runbook")
	}
	return string(m)
}

// Every byte a receiver accepts as a credential (printable ASCII, no comma) must reach the
// chart unchanged, including quotes, backslashes and escape-like sequences (#179 review).
var roundTripTokens = map[string]string{
	"app":        `q"u'o\t\\e\"\n\u0041{}:#&*!|>%@` + "`",
	"old-app":    `back\\slash"and"quote\`,
	"node":       "0123456789abcdef0123456789abcdef0123456789abcdef", // the generated-hex control
	"old-node":   `"leading-and-trailing-quotes"`,
	"yaml-chars": `-[x]:{y}?!tag&a*a|>%@`,
}

type rotationValues map[string]struct {
	Token         string `yaml:"token"`
	PreviousToken string `yaml:"previousToken"`
	SenderToken   string `yaml:"senderToken"`
}

func writeRotationValues(t *testing.T, args ...string) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	for name, tok := range roundTripTokens {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(tok), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := "set -euo pipefail\numask 077\nTMP=\"$1\"; shift\n" + runbookValuesFunc(t) + "values \"$@\"\n"
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{"-c", script, "runbook", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("values: %v\n%s", err, out)
	}
	file := filepath.Join(dir, "rotation.yaml")
	st, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("rotation.yaml mode %o, want 600", st.Mode().Perm())
	}
	b, _ := os.ReadFile(file)
	return file, b
}

// Phase 2 (both receiver tokens, the new sender) and phase 3 (previous cleared): every token
// parses back as YAML exactly as it was written.
func TestRunbookValuesRoundTripEveryCredential(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want map[string][3]string // class -> token, previous, sender
	}{
		{"phase 2", []string{"app", "old-app", "app", "node", "old-node", "node"}, map[string][3]string{
			"internalAuth": {roundTripTokens["app"], roundTripTokens["old-app"], roundTripTokens["app"]},
			"nodeAuth":     {roundTripTokens["node"], roundTripTokens["old-node"], roundTripTokens["node"]}}},
		{"phase 3", []string{"yaml-chars", "-", "yaml-chars", "old-app", "-", "old-app"}, map[string][3]string{
			"internalAuth": {roundTripTokens["yaml-chars"], "", roundTripTokens["yaml-chars"]},
			"nodeAuth":     {roundTripTokens["old-app"], "", roundTripTokens["old-app"]}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, b := writeRotationValues(t, c.args...)
			var got rotationValues
			if err := yaml.Unmarshal(b, &got); err != nil {
				t.Fatalf("the values file is not valid YAML: %v\n%s", err, b)
			}
			for class, w := range c.want {
				g := got[class]
				if g.Token != w[0] || g.PreviousToken != w[1] || g.SenderToken != w[2] {
					t.Errorf("%s: got %q/%q/%q, want %q/%q/%q", class, g.Token, g.PreviousToken, g.SenderToken, w[0], w[1], w[2])
				}
			}
		})
	}
}

// End to end where helm is installed: the storage chart rendered with internal auth on and
// the phase-2 values file holds every token byte for byte in its Secrets.
func TestRunbookValuesRenderThroughTheChart(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not installed; TestRunbookValuesRoundTripEveryCredential covers the YAML")
	}
	file, _ := writeRotationValues(t, "app", "old-app", "app", "node", "old-node", "node")
	out, err := exec.CommandContext(t.Context(), helm, "template", "storage", "../deploy/helm/storage",
		"-f", "../deploy/helm/storage/values-internal-auth.yaml", "-f", file).Output()
	if err != nil {
		t.Fatalf("helm template: %v", err)
	}
	want := map[string]map[string]string{
		"reliefgrid-internal-auth": {"token": roundTripTokens["app"], "previous-token": roundTripTokens["old-app"], "sender-token": roundTripTokens["app"]},
		"reliefgrid-node-auth":     {"token": roundTripTokens["node"], "previous-token": roundTripTokens["old-node"], "sender-token": roundTripTokens["node"]},
	}
	found := 0
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc struct {
			Kind     string                `yaml:"kind"`
			Metadata struct{ Name string } `yaml:"metadata"`
			Data     map[string]string     `yaml:"data"`
		}
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		keys, ok := want[doc.Metadata.Name]
		if doc.Kind != "Secret" || !ok {
			continue
		}
		found++
		for k, v := range keys {
			got, err := base64.StdEncoding.DecodeString(doc.Data[k])
			if err != nil || string(got) != v {
				t.Errorf("%s %s: got %q (%v), want %q", doc.Metadata.Name, k, got, err, v)
			}
		}
	}
	if found != 2 {
		t.Fatalf("rendered %d auth Secrets, want 2", found)
	}
}
