package report_test

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/report"
)

var t0 = time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)

type req struct {
	at     time.Duration
	class  string
	op     string
	status int
	ms     float64
}

// runDir writes a complete synthetic run: steps of 10 s at 10, 20 and 30
// req/s; the third breaks the read threshold.
func runDir(t *testing.T, reqs []req, created []string, statuses []report.Status, dropAt time.Duration) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "S5.2-step-test")
	for _, d := range []string{"raw/metrics-start", "raw/metrics-end"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	// k6 points
	f, err := os.Create(filepath.Join(dir, "raw", "k6-points.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
	point := func(metric string, at time.Duration, v float64, tags map[string]string) {
		_ = enc.Encode(map[string]any{"type": "Point", "metric": metric, "data": map[string]any{"time": t0.Add(at), "value": v, "tags": tags}})
	}
	_ = enc.Encode(map[string]any{"type": "Metric", "metric": "http_req_duration", "data": map[string]any{"name": "http_req_duration"}})
	for _, r := range reqs {
		tags := map[string]string{"class": r.class, "op": r.op, "status": fmt.Sprint(r.status)}
		point("http_req_duration", r.at, r.ms, tags)
		failed := 0.0
		if r.status < 200 || r.status > 299 {
			failed = 1
			point("api_errors", r.at, 1, map[string]string{"status": fmt.Sprint(r.status), "code": "STORAGE_UNAVAILABLE", "op": r.op})
		}
		point("http_req_failed", r.at, failed, tags)
	}
	for i, id := range created {
		point("created_request", time.Duration(i)*time.Second, 1, map[string]string{"request_id": id})
	}
	if dropAt >= 0 {
		point("dropped_iterations", dropAt, 3, nil)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	if err := report.WriteStatuses(filepath.Join(dir, "raw", "statuses.jsonl"), statuses); err != nil {
		t.Fatal(err)
	}
	var cg strings.Builder
	for i, ms := range []int64{0, 5000, 10000} {
		for _, pod := range []string{"storage-shard-0-0", "reliefgrid-api-x"} {
			// 0.5 then 1.5 CPU-seconds per 5 s: 100 then 300 millicores
			usage := []int64{0, 500_000, 2_000_000}[i]
			fmt.Fprintf(&cg, `{"tMs":%d,"pod":%q,"usageUsec":%d,"nrThrottled":%d,"memBytes":%d}`+"\n", t0.UnixMilli()+ms, pod, usage, i*2, (100+i*10)<<20)
		}
	}
	write(t, filepath.Join(dir, "raw", "cgroup.jsonl"), cg.String())
	for s := range 3 {
		pod := fmt.Sprintf("storage-shard-%d-0", s)
		write(t, filepath.Join(dir, "raw", "metrics-start", pod+".prom"), prom(s, pod, [3]int{0, 0, 0}, 1, 0, 1000))
		// 90 commits under 5 ms, 9 under 25 ms, 1 under 100 ms; one snapshot
		write(t, filepath.Join(dir, "raw", "metrics-end", pod+".prom"), prom(s, pod, [3]int{90, 99, 100}, 2, 1, 4096))
	}
	write(t, filepath.Join(dir, "raw", "metrics-end", "coordinator.prom"), "# TYPE coordinator_forward_total counter\ncoordinator_forward_total{outcome=\"answered\",shard=\"0\"} 5\n")

	facts := map[string]any{
		"goal": "S5.2", "runId": "S5.2-step-test", "scenario": "step",
		"startedAt": t0, "endedAt": t0.Add(30 * time.Second), "warmupSeconds": 0,
		"git": map[string]any{"sha": strings.Repeat("ab", 20), "dirty": false},
		"host": map[string]any{"cpuModel": "Test CPU", "logicalCpus": 8, "memTotalGiB": 30.6, "kernel": "7.0.0", "powerProfile": "performance",
			"k6Cpus": "6-7", "swapUsedMiB": 0, "loadAvgStart": 0.5, "cpuMHzStart": 2000, "cpuMHzEnd": 1900},
		"versions": map[string]any{"k6": "v2.3.0", "kind": "v0.33.0", "kindNodeImage": "kindest/node:v1.37.0@sha256:x", "kubectl": "v1.37.1",
			"helm": "v3.22.1", "go": "1.27.2", "images": map[string]any{"api": "sha256:" + strings.Repeat("1", 64)}},
		"config": map[string]any{"raftProfile": "load", "readFence": "fence", "tracing": "off", "autoscaling": "off"},
		"load":   map[string]any{"executor": "ramping-arrival-rate", "durationSeconds": 30, "maxVUs": 50},
		"steps": []map[string]any{
			{"targetRps": 10, "from": t0, "to": t0.Add(10 * time.Second)},
			{"targetRps": 20, "from": t0.Add(10 * time.Second), "to": t0.Add(20 * time.Second)},
			{"targetRps": 30, "from": t0.Add(20 * time.Second), "to": t0.Add(30 * time.Second)},
		},
		"invcheck": map[string]any{"exitCode": 0},
	}
	writeJSON(t, filepath.Join(dir, "facts.json"), facts)
	writeJSON(t, filepath.Join(dir, "dataset-info.json"), map[string]any{"seed": 52, "ops": 1000, "requests": 150, "resources": 100,
		"sha256": strings.Repeat("c", 64), "payloadSha256": strings.Repeat("d", 64),
		"mix": map[string]float64{"create_request": 0.15, "register_resource": 0.10, "match": 0.10, "read_request": 0.55, "list_requests": 0.05, "list_resources": 0.05}})
	return dir
}

func prom(shard int, pod string, commitCum [3]int, term int, snaps int, logBytes int) string {
	l := fmt.Sprintf(`node=%q,shard="%d"`, pod, shard)
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE storage_raft_term gauge\nstorage_raft_term{%s} %d\n", l, term)
	fmt.Fprintf(&b, "# TYPE storage_raft_log_bytes gauge\nstorage_raft_log_bytes{%s} %d\n", l, logBytes)
	fmt.Fprintf(&b, "# TYPE storage_snapshot_bytes gauge\nstorage_snapshot_bytes{%s} %d\n", l, logBytes/2)
	b.WriteString("# TYPE storage_raft_commit_seconds histogram\n")
	for i, ub := range []string{"0.005", "0.025", "0.1"} {
		fmt.Fprintf(&b, "storage_raft_commit_seconds_bucket{%s,le=%q} %d\n", l, ub, commitCum[i])
	}
	fmt.Fprintf(&b, "storage_raft_commit_seconds_bucket{%s,le=\"+Inf\"} %d\nstorage_raft_commit_seconds_sum{%s} 1\nstorage_raft_commit_seconds_count{%s} %d\n", l, commitCum[2], l, l, commitCum[2])
	b.WriteString("# TYPE storage_raft_snapshot_create_seconds histogram\n")
	fmt.Fprintf(&b, "storage_raft_snapshot_create_seconds_bucket{%s,le=\"+Inf\"} %d\nstorage_raft_snapshot_create_seconds_sum{%s} 0\nstorage_raft_snapshot_create_seconds_count{%s} %d\n", l, snaps, l, l, snaps)
	return b.String()
}

func write(t *testing.T, p, s string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeJSON(t *testing.T, p string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	write(t, p, string(b))
}

// steady makes n requests per class per second over [from, to) seconds at
// the given latencies.
func steady(from, to int, perSec map[string]float64) []req {
	var out []req
	op := map[string]string{"write": "create_request", "read": "read_request", "list": "list_requests"}
	for s := from; s < to; s++ {
		for class, ms := range perSec {
			for i := range 10 {
				out = append(out, req{at: time.Duration(s)*time.Second + time.Duration(i)*100*time.Millisecond, class: class, op: op[class], status: 201, ms: ms})
			}
		}
	}
	return out
}

func checker(t *testing.T, dir string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "python3", append([]string{"-I", "../../scripts/load-report-check.py", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok { //nolint:errorlint // exec returns it unwrapped
			return ee.ExitCode(), string(out)
		}
		t.Fatal(err)
	}
	return 0, string(out)
}

func TestBuildPassesTheChecker(t *testing.T) {
	reqs := append(append(steady(0, 10, map[string]float64{"write": 50, "read": 10, "list": 40}),
		steady(10, 20, map[string]float64{"write": 80, "read": 20, "list": 60})...),
		steady(20, 30, map[string]float64{"write": 400, "read": 300, "list": 200})...) // reads break 250 ms
	reqs = append(reqs, req{at: 15 * time.Second, class: "write", op: "create_request", status: 503, ms: 3000})
	ids := []string{"r1", "r2", "r3"}
	statuses := []report.Status{
		{ID: "r1", HTTPStatus: 200, Status: "MATCHED", CreatedAt: "2026-11-10T09:00:00Z", UpdatedAt: "2026-11-10T09:00:00.300Z"},
		{ID: "r2", HTTPStatus: 200, Status: "MATCHED", CreatedAt: "2026-11-10T09:00:01Z", UpdatedAt: "2026-11-10T09:00:01.500Z"},
		{ID: "r3", HTTPStatus: 200, Status: "WAITING", CreatedAt: "2026-11-10T09:00:02Z", UpdatedAt: "2026-11-10T09:00:02Z"},
	}
	dir := runDir(t, reqs, ids, statuses, 25*time.Second)
	sum, err := report.Build(dir, report.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if code, out := checker(t, dir, "--require-saturation"); code != 0 {
		t.Fatalf("checker rejected Build's report (exit %d):\n%s", code, out)
	}
	b, _ := json.Marshal(sum)
	var s struct {
		Results struct {
			Requests int
			Classes  map[string]report.ClassStats
			Errors   struct {
				Total    int
				ByStatus map[string]int
				ByCode   map[string]int
			}
			DroppedIterations int
		}
		Match      report.Match
		Saturation struct {
			Rps, BrokenAt float64
			BrokenBy      []string
		}
		Raft       struct{ Shards []report.ShardRaft }
		Pods       []report.PodFigures
		Invariants map[string]any
	}
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	if s.Results.Requests != 901 || s.Results.Errors.Total != 1 || s.Results.Errors.ByStatus["503"] != 1 || s.Results.Errors.ByCode["STORAGE_UNAVAILABLE"] != 1 {
		t.Errorf("results %+v", s.Results)
	}
	if s.Results.DroppedIterations != 3 {
		t.Errorf("dropped %d, want 3", s.Results.DroppedIterations)
	}
	if c := s.Results.Classes["list"]; c.Count != 300 || c.P99Ms != 200 {
		t.Errorf("list class %+v: listings are their own class", c)
	}
	// Step 20 holds one 3 s 503 among 101 writes: p99 interpolates to 80 ms
	// and 1/301 failed (0.33%), so it holds; step 30 breaks reads and drops.
	if s.Saturation.Rps != 20 || s.Saturation.BrokenAt != 30 || strings.Join(s.Saturation.BrokenBy, ",") != "readP99,droppedIterations" {
		t.Errorf("saturation %+v, want 20 req/s broken at 30 by readP99,droppedIterations", s.Saturation)
	}
	if s.Match.Label != "matched only" || s.Match.Matched != 2 || s.Match.Waiting != 1 || s.Match.P50Ms != 400 {
		t.Errorf("match %+v, want 2 matched (300 and 500 ms, p50 400), 1 waiting", s.Match)
	}
	if len(s.Raft.Shards) != 3 || s.Raft.Shards[0].Commits != 100 || s.Raft.Shards[0].Snapshots != 1 || s.Raft.Shards[0].LogBytesMax != 4096 {
		t.Errorf("raft %+v", s.Raft.Shards)
	}
	// 100 commits: rank 50 in the first bucket (0..5 ms): 50/90*5 = 2.78 ms;
	// rank 99 in the third (25..100 ms): 25 + 75*(99-99)/1 = 25 ms.
	if r := s.Raft.Shards[1]; math.Abs(r.CommitP50Ms-2.78) > 0.01 || r.CommitP99Ms != 25 {
		t.Errorf("commit quantiles %+v, want p50 2.78 ms and p99 25 ms", r)
	}
	if len(s.Pods) != 2 || s.Pods[1].CPUMillicoresMax != 300 || s.Pods[1].CPUMillicoresAvg != 200 || s.Pods[1].MemMiBMax != 120 || s.Pods[1].CPUThrottledPeriods != 4 {
		t.Errorf("pods %+v", s.Pods)
	}
	if s.Invariants["ledger"] != "PASS" || s.Invariants["invcheck"] != "PASS" {
		t.Errorf("invariants %v", s.Invariants)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "requests.csv.gz")); err != nil {
		t.Errorf("no compact requests file: %v", err)
	}
}

func TestBuildSaturationFromSteps(t *testing.T) {
	reqs := append(append(steady(0, 10, map[string]float64{"write": 50, "read": 10, "list": 40}),
		steady(10, 20, map[string]float64{"write": 80, "read": 20, "list": 60})...),
		steady(20, 30, map[string]float64{"write": 90, "read": 30, "list": 700})...) // only listings break
	dir := runDir(t, reqs, nil, nil, -1)
	sum, err := report.Build(dir, report.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sat, _ := sum["saturation"].(map[string]any)
	if sat["rps"] != 20.0 || sat["brokenAt"] != 30.0 || fmt.Sprint(sat["brokenBy"]) != "[listP99]" {
		t.Fatalf("saturation %v, want 20 req/s broken at 30 by listP99 alone", sat)
	}
}

func TestBuildLostWriteFailsTheLedger(t *testing.T) {
	reqs := steady(0, 30, map[string]float64{"write": 50, "read": 10, "list": 40})
	reqs = append(reqs, steady(20, 30, map[string]float64{"read": 900})...)
	statuses := []report.Status{
		{ID: "r1", HTTPStatus: 200, Status: "MATCHED", CreatedAt: "2026-11-10T09:00:00Z", UpdatedAt: "2026-11-10T09:00:00.3Z"},
		{ID: "r2", HTTPStatus: 404},
	}
	dir := runDir(t, reqs, []string{"r1", "r2"}, statuses, -1)
	sum, err := report.Build(dir, report.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if inv := sum["invariants"].(map[string]any); inv["ledger"] != "FAIL" || inv["unreadable"] != 1 {
		t.Fatalf("invariants %v: an acknowledged create that reads 404 is a lost write", inv)
	}
	if code, out := checker(t, dir); code != 1 || !strings.Contains(out, "invariants.ledger") {
		t.Fatalf("checker accepted a lost write (exit %d):\n%s", code, out)
	}
}

func TestBuildSpillsLargeRawFiles(t *testing.T) {
	dir := runDir(t, steady(0, 30, map[string]float64{"write": 50, "read": 10, "list": 40}), nil, nil, 25*time.Second)
	spill := t.TempDir()
	sum, err := report.Build(dir, report.Options{MaxCommitBytes: 2048, SpillDir: spill})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(sum["raw"])
	var raw []report.RawFile
	_ = json.Unmarshal(b, &raw)
	spilled := 0
	for _, r := range raw {
		if r.Path == "raw/k6-points.jsonl.gz" {
			if r.Committed || r.SHA256 == "" || r.Bytes <= 2048 {
				t.Errorf("k6 points %+v: want spilled with size and sha256", r)
			}
			if _, err := os.Stat(filepath.Join(spill, filepath.Base(dir), r.Path)); err != nil {
				t.Errorf("spilled file not in the spill directory: %v", err)
			}
			spilled++
		}
	}
	if spilled != 1 {
		t.Fatalf("raw list %+v", raw)
	}
	if code, out := checker(t, dir); code != 0 {
		t.Fatalf("checker rejected a report with a spilled file (exit %d):\n%s", code, out)
	}
}

func TestQuantile(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for q, want := range map[float64]float64{0: 1, 0.5: 5.5, 0.95: 9.55, 0.99: 9.91, 1: 10} {
		if got := report.Quantile(v, q); got != want {
			t.Errorf("Quantile(%v) = %v, want %v", q, got, want)
		}
	}
	if report.Quantile(nil, 0.5) != 0 {
		t.Error("no values: want 0")
	}
}

// Cgroup samples stamped in another unit (nanoseconds from a date that
// ignores %3N) would make every CPU rate ~0: Build refuses them.
func TestBuildRejectsSamplesOutsideTheRun(t *testing.T) {
	dir := runDir(t, steady(0, 30, map[string]float64{"write": 50, "read": 10, "list": 40}), nil, nil, 25*time.Second)
	write(t, filepath.Join(dir, "raw", "cgroup.jsonl"),
		fmt.Sprintf(`{"tMs":%d,"pod":"p","usageUsec":1,"nrThrottled":0,"memBytes":1}`+"\n", t0.UnixNano()))
	if _, err := report.Build(dir, report.Options{}); err == nil || !strings.Contains(err.Error(), "cgroup") {
		t.Fatalf("Build accepted a sample stamped in nanoseconds: %v", err)
	}
}
