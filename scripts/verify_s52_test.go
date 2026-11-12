package scripts

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// verify-S5.2.sh names the saturation point it passed: the Python snippet it
// runs on a step report must print it, not fail and leave the line empty.
func TestVerifyS52PrintsTheSaturationPoint(t *testing.T) {
	b, err := os.ReadFile("verify-S5.2.sh")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`pass "saturation point in \$d: \$\(python3 -I -c '([^']+)' "\$d/summary.json"\)"`).FindSubmatch(b)
	if m == nil {
		t.Fatal("verify-S5.2.sh: saturation point line not found")
	}
	run := func(dir string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "python3", "-I", "-c", string(m[1]), dir+"/summary.json").CombinedOutput()
		if err != nil {
			t.Fatalf("snippet failed: %v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	if got, want := run(loadFixture), "30 req/s; broken at 40 by writeP99,readP99,listP99,errorRate,droppedIterations"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Saturation above the tested range: no point in the report, a lower bound.
	r := loadReport(t)
	unbroken(t, r)
	delete(r, "saturation")
	if got, want := run(writeReport(t, r)), "> 40 req/s; no step broken in the tested range"; got != want {
		t.Fatalf("above range: got %q, want %q", got, want)
	}
}
