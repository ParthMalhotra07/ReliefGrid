package scripts

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const k6Lib = "../loadtest/k6/lib.js"

// sharedArrayCallbacks returns the source spans of every `new SharedArray(`
// call in src, from the call to its closing parenthesis.
func sharedArrayCallbacks(src string) [][2]int {
	var spans [][2]int
	for _, m := range regexp.MustCompile(`new SharedArray\(`).FindAllStringIndex(src, -1) {
		depth := 0
		for i := m[1] - 1; i < len(src); i++ {
			switch src[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 {
				spans = append(spans, [2]int{m[0], i})
				break
			}
		}
	}
	return spans
}

// k6 runs a module's init code once per VU. A dataset read or parsed there is
// held by every VU: the exploratory 10->160 run (14.9 MB dataset, 116 VUs)
// grew k6 to 22.4 GB and dropped iterations. The file must be read and parsed
// only inside a SharedArray callback, which k6 runs once and shares.
func TestK6DatasetIsParsedOnlyInsideSharedArray(t *testing.T) {
	b, err := os.ReadFile(k6Lib)
	if err != nil {
		t.Fatal(err)
	}
	src := regexp.MustCompile(`(?m)//.*$`).ReplaceAllString(string(b), "")
	spans := sharedArrayCallbacks(src)
	if len(spans) == 0 {
		t.Fatal("lib.js: no SharedArray")
	}
	for _, call := range []string{"open(", "JSON.parse("} {
		idx := regexp.MustCompile(`\b`+regexp.QuoteMeta(call)).FindAllStringIndex(src, -1)
		if len(idx) == 0 {
			t.Fatalf("lib.js: no %s", call)
		}
		for _, at := range idx {
			inside := false
			for _, s := range spans {
				inside = inside || (at[0] > s[0] && at[0] < s[1])
			}
			if !inside {
				line := strings.Count(src[:at[0]], "\n") + 1
				t.Errorf("lib.js:%d: %s outside a SharedArray callback runs in every VU", line, call)
			}
		}
	}
}

// With k6 present (K6=<path to the pinned k6 binary>), measure it: k6's peak
// RSS for 20 VUs against 2 on a 15 MB dataset. Each extra VU must cost far
// less than the dataset: before the fix each held its own parsed copy
// (346 MB per VU measured on a 21 MB dataset).
func TestK6DatasetMemoryPerVUStaysFlat(t *testing.T) {
	k6 := os.Getenv("K6")
	if k6 == "" {
		t.Skip("K6 not set: the loader check above still guards lib.js")
	}
	if _, err := os.Stat("/usr/bin/time"); err != nil {
		t.Skip("no /usr/bin/time to read k6's peak RSS")
	}
	dir := t.TempDir()
	ops := make([]map[string]any, 0, 120000)
	for i := range 120000 {
		ops = append(ops, map[string]any{"kind": "create_request", "seq": i,
			"body": map[string]any{"category": "water", "quantity": i % 97, "note": strings.Repeat("x", 40)}})
	}
	ds, err := json.Marshal(map[string]any{"seed": 52, "run": "mem-test", "counts": map[string]any{}, "mix": map[string]any{}, "ops": ops})
	if err != nil {
		t.Fatal(err)
	}
	dataset := filepath.Join(dir, "dataset.json")
	if err := os.WriteFile(dataset, ds, 0o644); err != nil {
		t.Fatal(err)
	}
	lib, err := filepath.Abs(k6Lib)
	if err != nil {
		t.Fatal(err)
	}
	// Init only: every VU imports lib.js and runs one empty iteration.
	script := filepath.Join(dir, "init.js")
	if err := os.WriteFile(script, []byte(`import { meta } from '`+lib+`';
export const options = { scenarios: { init: { executor: 'per-vu-iterations', vus: Number(__ENV.VUS), iterations: 1 } } };
export default function () { if (meta.seed !== 52) throw new Error('meta not loaded'); }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	peak := func(vus int) int {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "/usr/bin/time", "-f", "PEAK_KB %M", k6, "run", "--quiet",
			"-e", "DATASET="+dataset, "-e", "VUS="+strconv.Itoa(vus), script)
		cmd.Env = append(os.Environ(), "K6_NO_USAGE_REPORT=true")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("k6 with %d VUs: %v\n%s", vus, err, out)
		}
		m := regexp.MustCompile(`PEAK_KB (\d+)`).FindSubmatch(out)
		if m == nil {
			t.Fatalf("no peak RSS:\n%s", out)
		}
		kb, _ := strconv.Atoi(string(m[1]))
		return kb
	}
	low, high := peak(2), peak(20)
	perVU := float64(high-low) / 18
	datasetKB := float64(len(ds)) / 1024
	t.Logf("dataset %.1f MB; peak RSS 2 VUs %.1f MB, 20 VUs %.1f MB; %.2f MB per extra VU",
		datasetKB/1024, float64(low)/1024, float64(high)/1024, perVU/1024)
	// Before the fix each VU held its own parsed copy, several times the file.
	if limit := datasetKB / 4; perVU > limit {
		t.Fatalf("%s per extra VU, over a quarter of the dataset (%s): VUs hold their own copy",
			fmt.Sprintf("%.1f MB", perVU/1024), fmt.Sprintf("%.1f MB", limit/1024))
	}
}
