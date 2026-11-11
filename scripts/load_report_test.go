package scripts

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// S5.2's Verify reads a load report (docs/evidence/S5.2/<runId>/) through
// scripts/load-report-check.py. These tests run the checker on the fixture in
// testdata/load-report/step and on copies broken one field at a time: every
// figure the plan asks for (throughput, p50/p95/p99, error rate, saturation
// point, hardware and versions) must be present and self-consistent.

const loadFixture = "testdata/load-report/step"

func loadReport(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(loadFixture, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// writeReport writes m as summary.json next to a copy of the fixture's other
// files and a fresh SHA256SUMS, and returns the directory.
func writeReport(t *testing.T, m map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	k6, err := os.ReadFile(filepath.Join(loadFixture, "k6-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "k6-summary.json"), k6, 0o644); err != nil {
		t.Fatal(err)
	}
	writeSums(t, dir)
	return dir
}

func writeSums(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if e.Name() != "SHA256SUMS" && !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	var sb strings.Builder
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%x  %s\n", sha256.Sum256(b), n)
	}
	if err := os.WriteFile(filepath.Join(dir, "SHA256SUMS"), []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runLoadCheck(t *testing.T, dir string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "python3", append([]string{"-I", "load-report-check.py", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), string(out)
	}
	t.Fatalf("run checker: %v", err)
	return -1, ""
}

// at walks a dotted path ("results.classes.write") to the parent map and
// returns it with the last key.
func at(t *testing.T, m map[string]any, path string) (map[string]any, string) {
	t.Helper()
	parts := strings.Split(path, ".")
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]any)
		if !ok {
			t.Fatalf("fixture has no map at %s", p)
		}
		cur = next
	}
	return cur, parts[len(parts)-1]
}

func TestLoadReportCheckFixturePasses(t *testing.T) {
	for _, args := range [][]string{nil, {"--require-saturation"}} {
		code, out := runLoadCheck(t, writeReport(t, loadReport(t)), args...)
		if code != 0 {
			t.Fatalf("args %v: good fixture rejected (exit %d):\n%s", args, code, out)
		}
	}
}

func TestLoadReportCheckCommittedFixtureChecksums(t *testing.T) {
	// The committed fixture itself is a valid report directory.
	if code, out := runLoadCheck(t, loadFixture, "--require-saturation"); code != 0 {
		t.Fatalf("committed fixture rejected (exit %d):\n%s", code, out)
	}
}

func TestLoadReportCheckRejects(t *testing.T) {
	del := func(path string) func(*testing.T, map[string]any) {
		return func(t *testing.T, m map[string]any) {
			p, k := at(t, m, path)
			delete(p, k)
		}
	}
	set := func(path string, v any) func(*testing.T, map[string]any) {
		return func(t *testing.T, m map[string]any) {
			p, k := at(t, m, path)
			p[k] = v
		}
	}
	// depth sets (nil: deletes) one queue's depth at the pause; queue names
	// have dots, so not through at.
	depth := func(queue string, v any) func(*testing.T, map[string]any) {
		return func(t *testing.T, m map[string]any) {
			p, k := at(t, m, "audit.headroom.depthsAtPause")
			d := p[k].(map[string]any)
			if v == nil {
				delete(d, queue)
			} else {
				d[queue] = v
			}
		}
	}
	step := func(i int, key string, v any) func(*testing.T, map[string]any) {
		return func(t *testing.T, m map[string]any) {
			m["steps"].([]any)[i].(map[string]any)[key] = v
		}
	}
	for _, c := range []struct {
		name   string
		mutate func(*testing.T, map[string]any)
		want   string // a fragment of the FAIL line
	}{
		{"wrong goal", set("goal", "S5.1"), "goal"},
		{"dirty tree", set("git.dirty", true), "git.dirty"},
		{"short sha", set("git.sha", "0123abc"), "git.sha"},
		{"no CPU model", del("host.cpuModel"), "host.cpuModel"},
		{"no RAM", del("host.memTotalGiB"), "host.memTotalGiB"},
		{"k6 not pinned to cores", del("host.k6Cpus"), "host.k6Cpus"},
		{"kind cpuset not recorded", del("host.kindCpus"), "host.kindCpus"},
		{"k6 shares the kind cores", set("host.k6Cpus", "5-7"), "overlap"},
		{"power profile not restored", del("host.powerProfileRestored"), "host.powerProfileRestored"},
		{"no k6 version", del("versions.k6"), "versions.k6"},
		{"no kind node image", del("versions.kindNodeImage"), "versions.kindNodeImage"},
		{"image by tag", set("versions.images.api", "reliefgrid/api:dev"), "versions.images.api"},
		{"no seed", del("dataset.seed"), "dataset.seed"},
		{"no duration", del("load.durationSeconds"), "load.durationSeconds"},
		{"mix does not sum to 1", set("load.mix.read", 0.5), "load.mix"},
		{"threshold changed", set("thresholds.readP99Ms", 500), "thresholds"},
		{"no percentile definition", del("percentiles"), "percentiles"},
		{"no throughput", del("results.throughputRps"), "results.throughputRps"},
		{"no write p99", del("results.classes.write.p99Ms"), "results.classes.write.p99Ms"},
		{"no list class", del("results.classes.list"), "results.classes.list"},
		{"list threshold changed", set("thresholds.listP99Ms", 1000), "thresholds"},
		{"list threshold missing", del("thresholds.listP99Ms"), "thresholds"},
		{"saturation ignores the list threshold", func(t *testing.T, m map[string]any) {
			// Step 40 breaks only the list threshold: saturation is still 30.
			for k, v := range map[string]any{"writeP99Ms": 900, "readP99Ms": 240, "listP99Ms": 700, "errorRate": 0.005, "droppedIterations": 0} {
				step(3, k, v)(t, m)
			}
			set("saturation.brokenBy", []any{"writeP99"})(t, m)
		}, "step 40 breaks ['listP99']"},
		{"step without list p99", func(t *testing.T, m map[string]any) {
			delete(m["steps"].([]any)[1].(map[string]any), "listP99Ms")
		}, "steps[1].listP99Ms"},
		{"percentiles out of order", set("results.classes.read.p95Ms", 300), "results.classes.read"},
		{"error rate disagrees with counts", set("results.errors.rate", 0.0001), "results.errors.rate"},
		{"error counts by status disagree", set("results.errors.byStatus", map[string]any{"503": 1}), "results.errors.byStatus"},
		{"no error codes", del("results.errors.byCode"), "results.errors.byCode"},
		{"match not labelled", set("match.label", "all requests"), "match.label"},
		{"no waiting count", del("match.waiting"), "match.waiting"},
		{"no raft commit latency", del("raft"), "raft"},
		{"no pod resources", del("pods"), "pods"},
		{"invcheck failed", set("invariants.invcheck", "FAIL"), "invariants.invcheck"},
		{"saturation not the last passing step", set("saturation.rps", 20), "the steps give 30 req/s"},
		{"saturation reason wrong", set("saturation.brokenBy", []any{"writeP99"}), "saturation.brokenBy"},
		{"no step breaks a threshold", func(t *testing.T, m map[string]any) {
			for k, v := range map[string]any{"writeP99Ms": 900, "readP99Ms": 240, "listP99Ms": 480, "errorRate": 0.005, "droppedIterations": 0} {
				step(3, k, v)(t, m)
			}
		}, "no step breaks a threshold"},
		{"first step already saturated", step(0, "errorRate", 0.5), "already breaks"},
		{"inconclusive cannot hide safety failure", func(t *testing.T, m map[string]any) {
			step(0, "errorRate", 0.5)(t, m)
			set("invariants.invcheck", "FAIL")(t, m)
		}, "invariants.invcheck"},
		{"steps not increasing", step(2, "targetRps", 15), "strictly increase"},
		{"raw file without checksum", set("raw", []any{map[string]any{"path": "raw/k6-points.jsonl.gz", "committed": false}}), "raw"},
		{"committed raw file missing", set("raw", []any{map[string]any{"path": "raw/missing.json", "committed": true}}), "raw"},
		// invcheck can only be quiescent with retry admission stopped, and the
		// run must hand the cluster back as it found it.
		{"no audit", del("audit"), "audit"},
		{"audit with admission running", set("audit.mode", "with-admission"), "audit.mode"},
		{"matcher not restored", set("audit.matcherReplicasAfter", 0), "audit.matcherReplicasAfter"},
		{"no matcher replicas", set("audit.matcherReplicas", 0), "audit.matcherReplicas"},
		{"drain not recorded", del("audit.drain.marked"), "audit.drain.marked"},
		{"drain retries not recorded", del("audit.drain.retries"), "audit.drain.retries"},
		{"no headroom check", del("audit.headroom"), "audit.headroom"},
		{"headroom bound expired", set("audit.headroom.ok", false), "audit.headroom.ok"},
		{"headroom past the bound", set("audit.headroom.waitedSeconds", 601), "audit.headroom.waitedSeconds"},
		{"headroom not a percentage", set("audit.headroom.pct", 120), "audit.headroom.pct"},
		{"headroom without limits", set("audit.headroom.limits", map[string]any{}), "audit.headroom.limits"},
		{"queue over the threshold at the pause", depth("matching.wakeups", map[string]any{"ready": 7985, "unacked": 16}), "audit.headroom.depthsAtPause.matching.wakeups"},
		{"limited queue not sampled at the pause", depth("retry.5s", nil), "audit.headroom.depthsAtPause.retry.5s"},
		// The retry load and broker backpressure k6 cannot see.
		{"no background", del("background"), "background"},
		{"background run unsampled", set("background.run.samples", 0), "background.run.samples"},
		{"background run only gaps", func(t *testing.T, m map[string]any) {
			run, _ := at(t, m, "background.run.gaps")
			run["gaps"] = run["samples"]
		}, "background.run"},
		{"unconfirmed publishes not a count", set("background.run.publishesNotConfirmed", "1310"), "background.run.publishesNotConfirmed"},
		{"no queue depths", set("background.afterRun.queuePeak", map[string]any{}), "background.afterRun.queuePeak"},
		{"waiting not a count", set("background.run.waitingLast", "7"), "background.run.waitingLast"},
		{"no pause sample", set("background.atPause", nil), "background.atPause"},
		{"step without background", func(t *testing.T, m map[string]any) {
			delete(m["steps"].([]any)[1].(map[string]any), "background")
		}, "steps[1].background"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := loadReport(t)
			c.mutate(t, m)
			code, out := runLoadCheck(t, writeReport(t, m), "--require-saturation")
			wantCode, label := 1, "FAIL"
			if c.name == "no step breaks a threshold" || c.name == "first step already saturated" {
				wantCode, label = 3, "INCONCLUSIVE"
			}
			if code != wantCode || !strings.Contains(out, label) || !strings.Contains(out, c.want) {
				t.Fatalf("want exit 1 with a FAIL naming %q, got exit %d:\n%s", c.want, code, out)
			}
		})
	}
}

func TestLoadReportCheckFiles(t *testing.T) {
	t.Run("tampered file", func(t *testing.T) {
		dir := writeReport(t, loadReport(t))
		if err := os.WriteFile(filepath.Join(dir, "k6-summary.json"), []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if code, out := runLoadCheck(t, dir); code != 1 || !strings.Contains(out, "SHA256SUMS") {
			t.Fatalf("want a SHA256SUMS failure, got exit %d:\n%s", code, out)
		}
	})
	t.Run("file not in SHA256SUMS", func(t *testing.T) {
		dir := writeReport(t, loadReport(t))
		if err := os.WriteFile(filepath.Join(dir, "extra.txt"), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if code, out := runLoadCheck(t, dir); code != 1 || !strings.Contains(out, "extra.txt") {
			t.Fatalf("want a failure naming extra.txt, got exit %d:\n%s", code, out)
		}
	})
	t.Run("no SHA256SUMS", func(t *testing.T) {
		dir := writeReport(t, loadReport(t))
		if err := os.Remove(filepath.Join(dir, "SHA256SUMS")); err != nil {
			t.Fatal(err)
		}
		if code, out := runLoadCheck(t, dir); code != 1 || !strings.Contains(out, "SHA256SUMS") {
			t.Fatalf("want a SHA256SUMS failure, got exit %d:\n%s", code, out)
		}
	})
	t.Run("smoke report needs no saturation", func(t *testing.T) {
		m := loadReport(t)
		m["scenario"] = "smoke"
		delete(m, "steps")
		delete(m, "saturation")
		if code, out := runLoadCheck(t, writeReport(t, m)); code != 0 {
			t.Fatalf("smoke report rejected (exit %d):\n%s", code, out)
		}
		if code, out := runLoadCheck(t, writeReport(t, m), "--require-saturation"); code != 1 || !strings.Contains(out, "saturation") {
			t.Fatalf("--require-saturation accepted a smoke report (exit %d):\n%s", code, out)
		}
	})
}

func TestLoadSmokeRejectsInvalidMetrics(t *testing.T) {
	for _, path := range []string{"results.classes.write.count", "results.classes.read.p99Ms", "results.errors.total", "results.requests", "results.droppedIterations"} {
		for _, value := range []any{nil, "0", false, -1.0} {
			t.Run(fmt.Sprint(path, value), func(t *testing.T) {
				m := loadReport(t)
				m["scenario"] = "smoke"
				p, k := at(t, m, path)
				p[k] = value
				code, out := runLoadCheck(t, writeReport(t, m))
				if code != 1 || strings.Contains(out, "Traceback") {
					t.Fatalf("exit %d: %s", code, out)
				}
			})
		}
	}
	for _, path := range []string{"results.classes.write.count", "results.requests", "results.classes.read.p99Ms", "results.droppedIterations"} {
		t.Run(path, func(t *testing.T) {
			m := loadReport(t)
			m["scenario"] = "smoke"
			p, k := at(t, m, path)
			if strings.HasSuffix(path, "count") || path == "results.requests" {
				p[k] = 0
			} else {
				p[k] = 10000
			}
			code, out := runLoadCheck(t, writeReport(t, m))
			if code != 1 || strings.Contains(out, "Traceback") {
				t.Fatalf("exit %d: %s", code, out)
			}
		})
	}
}
