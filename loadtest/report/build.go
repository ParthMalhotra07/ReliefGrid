package report

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/dataset"
)

// Thresholds are the S5.2 limits (decided 2026-11-06): writes, reads by ID
// and listings each have their own p99.
var Thresholds = map[string]float64{"writeP99Ms": 1000, "readP99Ms": 250, "listP99Ms": 500, "errorRate": 0.01}

// PercentileDefinition is stated in every report.
const PercentileDefinition = "k6 http_req_duration (request sent to last response byte, measured at the client) of the requests " +
	"that started in the measured window (after warm-up), per request class (write: creates, registrations and matches; " +
	"read: one request by ID; list: listings, which scan every shard); p50/p95/p99 by linear interpolation between the " +
	"closest ranks of the raw samples. Matched latency is server-side (updatedAt - createdAt) for MATCHED requests only."

// Facts are what loadtest/run.sh records about a run (facts.json).
type Facts struct {
	Goal          string         `json:"goal"`
	RunID         string         `json:"runId"`
	Scenario      string         `json:"scenario"`
	StartedAt     time.Time      `json:"startedAt"`
	EndedAt       time.Time      `json:"endedAt"`
	WarmupSeconds float64        `json:"warmupSeconds"`
	Git           map[string]any `json:"git"`
	Host          map[string]any `json:"host"`
	Versions      map[string]any `json:"versions"`
	Config        map[string]any `json:"config"`
	Load          struct {
		Executor        string  `json:"executor"`
		DurationSeconds float64 `json:"durationSeconds"`
		MaxVUs          int     `json:"maxVUs"`
	} `json:"load"`
	Steps []struct {
		TargetRps float64   `json:"targetRps"`
		From      time.Time `json:"from"`
		To        time.Time `json:"to"`
	} `json:"steps,omitempty"`
	Invcheck struct {
		ExitCode int `json:"exitCode"`
	} `json:"invcheck"`
	// Audit: how admission was stopped for invcheck (loadtest/run.sh).
	Audit map[string]any `json:"audit"`
}

// DatasetInfo is what loadtest/cmd/gendata printed (dataset-info.json).
type DatasetInfo struct {
	Seed          int64              `json:"seed"`
	Ops           int                `json:"ops"`
	Requests      int                `json:"requests"`
	Resources     int                `json:"resources"`
	SHA256        string             `json:"sha256"`
	PayloadSHA256 string             `json:"payloadSha256"`
	Mix           map[string]float64 `json:"mix"`
}

// Step is one step's figures.
type Step struct {
	TargetRps         float64 `json:"targetRps"`
	AchievedRps       float64 `json:"achievedRps"`
	WriteP99Ms        float64 `json:"writeP99Ms"`
	ReadP99Ms         float64 `json:"readP99Ms"`
	ListP99Ms         float64 `json:"listP99Ms"`
	ErrorRate         float64 `json:"errorRate"`
	DroppedIterations int     `json:"droppedIterations"`
	// PodsAtLimit: pods at their CPU limit during the step's hold, busiest
	// first (which component saturates).
	PodsAtLimit []PodAtLimit `json:"podsAtLimit"`
	// Background: retry load and broker backpressure during the hold.
	Background Background `json:"background"`
}

// RawFile is one raw data file and whether it is committed beside the report.
type RawFile struct {
	Path      string `json:"path"`
	Committed bool   `json:"committed"`
	Bytes     int64  `json:"bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	Where     string `json:"where,omitempty"` // for an uncommitted file: where it was kept
}

// Options control Build.
type Options struct {
	// MaxCommitBytes: a raw file larger than this is moved to SpillDir and
	// recorded by size and sha256 only (default 5 MiB).
	MaxCommitBytes int64
	SpillDir       string
}

// Build writes dir/summary.json and dir/SHA256SUMS from the run's files:
// facts.json, dataset-info.json, raw/k6-points.jsonl.gz, raw/statuses.jsonl,
// raw/cgroup.jsonl, raw/background.jsonl and raw/metrics-start/, raw/metrics-end/.
func Build(dir string, opt Options) (map[string]any, error) {
	if opt.MaxCommitBytes <= 0 {
		opt.MaxCommitBytes = 5 << 20
	}
	var facts Facts
	if err := readJSON(filepath.Join(dir, "facts.json"), &facts); err != nil {
		return nil, err
	}
	var ds DatasetInfo
	if err := readJSON(filepath.Join(dir, "dataset-info.json"), &ds); err != nil {
		return nil, err
	}
	pts, err := ReadPoints(filepath.Join(dir, "raw", "k6-points.jsonl.gz"))
	if err != nil {
		return nil, err
	}
	statuses, err := ReadStatuses(filepath.Join(dir, "raw", "statuses.jsonl"))
	if err != nil {
		return nil, err
	}
	samples, err := ReadCgroup(filepath.Join(dir, "raw", "cgroup.jsonl"))
	if err != nil {
		return nil, err
	}
	// A sample outside the run (give or take an hour) is in another unit:
	// every rate computed from it would be wrong.
	for _, s := range samples {
		if at := time.UnixMilli(s.TMs); at.Before(facts.StartedAt.Add(-time.Hour)) || at.After(facts.EndedAt.Add(time.Hour)) {
			return nil, fmt.Errorf("report: cgroup sample of %s at tMs %d is not within the run (%v..%v): wrong unit?", s.Pod, s.TMs, facts.StartedAt, facts.EndedAt)
		}
	}
	background, err := ReadBackground(filepath.Join(dir, "raw", "background.jsonl"))
	if err != nil {
		return nil, err
	}
	for _, s := range background {
		if at := time.UnixMilli(s.TMs); at.Before(facts.StartedAt.Add(-time.Hour)) || at.After(facts.EndedAt.Add(time.Hour)) {
			return nil, fmt.Errorf("report: background sample at tMs %d is not within the run (%v..%v): wrong unit?", s.TMs, facts.StartedAt, facts.EndedAt)
		}
	}
	start, err := ReadScrapes(filepath.Join(dir, "raw", "metrics-start"))
	if err != nil {
		return nil, err
	}
	end, err := ReadScrapes(filepath.Join(dir, "raw", "metrics-end"))
	if err != nil {
		return nil, err
	}
	raft, err := RaftFigures(start, end)
	if err != nil {
		return nil, err
	}
	if err := writeRequestsCSV(filepath.Join(dir, "raw", "requests.csv.gz"), pts, facts.StartedAt); err != nil {
		return nil, err
	}

	w := Window{From: facts.StartedAt.Add(time.Duration(facts.WarmupSeconds * float64(time.Second))), To: facts.EndedAt}
	if w.Seconds() <= 0 {
		return nil, fmt.Errorf("report: empty measured window %v..%v", w.From, w.To)
	}
	st := pts.In(w)
	byStatus, byCode := map[string]int{}, map[string]int{}
	for _, r := range pts.Reqs {
		if w.has(r.T) && r.Failed {
			byStatus[r.Status]++
		}
	}
	for _, e := range pts.Errors {
		if w.has(e.T) {
			byCode[e.Code]++
		}
	}
	match, lost := Summarise(statuses)
	ledger := "PASS"
	if len(lost) > 0 || len(statuses) != len(pts.Created) {
		ledger = "FAIL"
	}
	invcheck := "PASS"
	if facts.Invcheck.ExitCode != 0 {
		invcheck = "FAIL"
	}

	mix := map[string]float64{}
	for k, v := range ds.Mix {
		mix[dataset.ClassOf(dataset.OpKind(k))] += v
	}
	errRate := 0.0
	if st.Requests > 0 {
		errRate = round6(float64(st.Failed) / float64(st.Requests))
	}
	summary := map[string]any{
		"goal": "S5.2", "runId": facts.RunID, "scenario": facts.Scenario, "startedAt": facts.StartedAt.UTC().Format(time.RFC3339),
		"git": facts.Git, "host": facts.Host, "versions": facts.Versions, "config": facts.Config,
		"dataset": map[string]any{"seed": ds.Seed, "ops": ds.Ops, "requests": ds.Requests, "resources": ds.Resources,
			"sha256": ds.SHA256, "payloadSha256": ds.PayloadSHA256},
		"load": map[string]any{"executor": facts.Load.Executor, "durationSeconds": facts.Load.DurationSeconds,
			"warmupSeconds": facts.WarmupSeconds, "maxVUs": facts.Load.MaxVUs, "mix": mix},
		"thresholds":  Thresholds,
		"percentiles": PercentileDefinition,
		"results": map[string]any{
			"requests": st.Requests, "throughputRps": round2(float64(st.Requests) / w.Seconds()), "classes": st.Classes,
			"errors":            map[string]any{"total": st.Failed, "rate": errRate, "byStatus": byStatus, "byCode": byCode},
			"droppedIterations": st.Dropped, "skippedOps": pts.Skipped,
		},
		"match":      match,
		"raft":       map[string]any{"shards": raft},
		"pods":       PodUse(samples),
		"invariants": map[string]any{"ledger": ledger, "invcheck": invcheck, "acknowledgedCreates": len(pts.Created), "unreadable": len(lost)},
		"background": backgroundFigures(background),
		"audit":      facts.Audit,
	}
	if len(facts.Steps) > 0 {
		steps, sat := stepFigures(pts, facts, samples, background, podLimits(facts))
		summary["steps"] = steps
		if sat != nil {
			summary["saturation"] = sat
		}
	}

	raw, err := placeRaw(dir, facts.RunID, opt)
	if err != nil {
		return nil, err
	}
	summary["raw"] = raw
	b, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), append(b, '\n'), 0o644); err != nil { //nolint:gosec // evidence is world-readable
		return nil, err
	}
	return summary, writeSums(dir)
}

// podLimits reads config.podCpuLimitsMillicores (pod -> CPU limit).
func podLimits(f Facts) map[string]float64 {
	out := map[string]float64{}
	if m, ok := f.Config["podCpuLimitsMillicores"].(map[string]any); ok {
		for k, v := range m {
			if n, ok := v.(float64); ok {
				out[k] = n
			}
		}
	}
	return out
}

func stepFigures(pts Points, f Facts, samples []CgroupSample, background []BackgroundSample, limits map[string]float64) ([]Step, map[string]any) {
	var steps []Step
	for _, s := range f.Steps {
		w := Window{From: s.From, To: s.To}
		st := pts.In(w)
		rate := 0.0
		if st.Requests > 0 {
			rate = round6(float64(st.Failed) / float64(st.Requests))
		}
		steps = append(steps, Step{TargetRps: s.TargetRps, AchievedRps: round2(float64(st.Requests) / w.Seconds()),
			WriteP99Ms: st.Classes["write"].P99Ms, ReadP99Ms: st.Classes["read"].P99Ms, ListP99Ms: st.Classes["list"].P99Ms,
			ErrorRate: rate, DroppedIterations: st.Dropped, PodsAtLimit: PodsAtLimit(samples, w, limits),
			Background: stepBackground(background, w)})
	}
	var firstAtLimit map[string]any
	for _, s := range steps {
		if len(s.PodsAtLimit) > 0 {
			var names []string
			for _, p := range s.PodsAtLimit {
				names = append(names, p.Name)
			}
			firstAtLimit = map[string]any{"targetRps": s.TargetRps, "pods": names}
			break
		}
	}
	for i, s := range steps {
		if by := brokenBy(s); len(by) > 0 {
			if i == 0 {
				return steps, nil // already over at the first step: no saturation point
			}
			sat := map[string]any{"rps": steps[i-1].TargetRps, "brokenAt": s.TargetRps, "brokenBy": by}
			if firstAtLimit != nil {
				sat["firstAtLimit"] = firstAtLimit
			}
			return steps, sat
		}
	}
	return steps, nil // never broken: raise the top step
}

func brokenBy(s Step) []string {
	var out []string
	if s.WriteP99Ms > Thresholds["writeP99Ms"] {
		out = append(out, "writeP99")
	}
	if s.ReadP99Ms > Thresholds["readP99Ms"] {
		out = append(out, "readP99")
	}
	if s.ListP99Ms > Thresholds["listP99Ms"] {
		out = append(out, "listP99")
	}
	if s.ErrorRate > Thresholds["errorRate"] {
		out = append(out, "errorRate")
	}
	if s.DroppedIterations > 0 {
		out = append(out, "droppedIterations")
	}
	return out
}

// writeRequestsCSV is the compact per-request record that is committed even
// when k6's full output is too large: ms since start, class, op, status, ms.
func writeRequestsCSV(path string, p Points, t0 time.Time) error {
	f, err := os.Create(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	_, _ = io.WriteString(gz, "t_ms,class,op,status,duration_ms\n")
	for _, r := range p.Reqs {
		_, _ = fmt.Fprintf(gz, "%d,%s,%s,%s,%s\n", r.T.Sub(t0).Milliseconds(), r.Class, r.Op, r.Status, strconv.FormatFloat(r.DurMs, 'f', 3, 64))
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// placeRaw lists the raw files (raw/**, dataset.json, k6-summary.json),
// moving each one over MaxCommitBytes to SpillDir/<runID>/. The run ID, not
// the directory's name, keys the spill: supervised runs all live in a
// directory named "run". A file already there is never replaced.
func placeRaw(dir, runID string, opt Options) ([]RawFile, error) {
	var paths []string
	for _, p := range []string{"dataset.json", "k6-summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, p)); err == nil {
			paths = append(paths, p)
		}
	}
	err := filepath.WalkDir(filepath.Join(dir, "raw"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var out []RawFile
	for _, rel := range paths {
		p := filepath.Join(dir, rel)
		fi, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if fi.Size() <= opt.MaxCommitBytes {
			out = append(out, RawFile{Path: rel, Committed: true})
			continue
		}
		if opt.SpillDir == "" {
			return nil, fmt.Errorf("report: %s is %d bytes, over %d, and no spill directory is set", rel, fi.Size(), opt.MaxCommitBytes)
		}
		sum, err := fileSHA(p)
		if err != nil {
			return nil, err
		}
		if runID == "" || runID != filepath.Base(runID) || runID == "." || runID == ".." {
			return nil, fmt.Errorf("report: run ID %q cannot name a spill directory", runID)
		}
		dst := filepath.Join(opt.SpillDir, runID, rel)
		if _, err := os.Lstat(dst); err == nil {
			return nil, fmt.Errorf("report: %s already exists; not replacing another run's file", dst)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return nil, err
		}
		if err := os.Rename(p, dst); err != nil {
			return nil, err
		}
		out = append(out, RawFile{Path: rel, Committed: false, Bytes: fi.Size(), SHA256: sum, Where: "kept off-repo by the run's author"})
	}
	return out, nil
}

func writeSums(dir string) error {
	var lines []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if rel == "SHA256SUMS" {
			return nil
		}
		sum, err := fileSHA(p)
		if err != nil {
			return err
		}
		lines = append(lines, sum+"  "+rel)
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(lines, func(a, b string) int { return strings.Compare(a[66:], b[66:]) })
	return os.WriteFile(filepath.Join(dir, "SHA256SUMS"), []byte(strings.Join(lines, "\n")+"\n"), 0o644) //nolint:gosec // evidence
}

func fileSHA(p string) (string, error) {
	f, err := os.Open(p) //nolint:gosec // the run directory's own file
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("report: %s: %w", path, err)
	}
	return nil
}

func round6(v float64) float64 { return float64(int64(v*1e6+0.5)) / 1e6 }
