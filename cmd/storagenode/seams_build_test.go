//go:build seams

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// S4.6b: the test seams are compiled out of the default storagenode binary
// (the image the demo and production run) and present only with -tags
// seams. Checking both builds keeps the absence check from passing
// vacuously when the names change. The file itself is seams-tagged: the two
// builds cost over a minute on a cold CI cache, so the check runs in make
// check-seams (the seams job, when the seam packages change), not in check.
func TestSeamsCompiledOutOfDefaultBinary(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go toolchain not on PATH: %v", err)
	}
	markers := [][]byte{[]byte("/mgmt/test/seams"), []byte("raftstore.(*Node).ArmSeam")}
	for _, tc := range []struct {
		tags string
		want bool
	}{{"", false}, {"seams", true}} {
		bin := filepath.Join(t.TempDir(), "storagenode")
		cmd := exec.CommandContext(t.Context(), gobin, "build", "-trimpath", "-tags", tc.tags, "-o", bin, ".")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build -tags %q: %v\n%s", tc.tags, err, out)
		}
		raw, err := os.ReadFile(bin)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range markers {
			if got := bytes.Contains(raw, m); got != tc.want {
				t.Errorf("binary built with tags %q contains %q = %v, want %v", tc.tags, m, got, tc.want)
			}
		}
	}
}
