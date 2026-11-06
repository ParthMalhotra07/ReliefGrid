package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// --- storage /metrics (docs/operations/storage-metrics.md) -------------------------------

// Scrape is one pod's /metrics at one instant.
type Scrape map[string]*dto.MetricFamily

// ReadScrapes reads dir/<pod>.prom for every pod.
func ReadScrapes(dir string) (map[string]Scrape, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.prom"))
	if err != nil {
		return nil, err
	}
	out := map[string]Scrape{}
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // the run directory's own file
		if err != nil {
			return nil, err
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		fams, err := parser.TextToMetricFamilies(strings.NewReader(string(b)))
		if err != nil {
			return nil, fmt.Errorf("report: %s: %w", f, err)
		}
		out[strings.TrimSuffix(filepath.Base(f), ".prom")] = fams
	}
	return out, nil
}

func label(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// shardOf is the shard label of the pod's storage_raft_term, or "".
func (s Scrape) shardOf() string {
	if f := s["storage_raft_term"]; f != nil && len(f.GetMetric()) > 0 {
		return label(f.GetMetric()[0], "shard")
	}
	return ""
}

func (s Scrape) histogram(name string) *dto.Histogram {
	if f := s[name]; f != nil && len(f.GetMetric()) > 0 {
		return f.GetMetric()[0].GetHistogram()
	}
	return nil
}

func (s Scrape) gauge(name string) float64 {
	if f := s[name]; f != nil && len(f.GetMetric()) > 0 {
		return f.GetMetric()[0].GetGauge().GetValue()
	}
	return 0
}

// bucketDelta is a histogram's cumulative buckets at end minus start.
type bucketDelta struct {
	upper []float64
	cum   []float64
	count float64
}

func (d *bucketDelta) add(start, end *dto.Histogram) {
	if end == nil {
		return
	}
	if d.upper == nil {
		for _, b := range end.GetBucket() {
			d.upper = append(d.upper, b.GetUpperBound())
		}
		d.cum = make([]float64, len(d.upper))
	}
	for i, b := range end.GetBucket() {
		v := float64(b.GetCumulativeCount())
		if start != nil && i < len(start.GetBucket()) {
			v -= float64(start.GetBucket()[i].GetCumulativeCount())
		}
		d.cum[i] += v
	}
	n := float64(end.GetSampleCount())
	if start != nil {
		n -= float64(start.GetSampleCount())
	}
	d.count += n
}

// quantile as Prometheus's histogram_quantile: linear within the bucket.
func (d *bucketDelta) quantile(q float64) float64 {
	if d.count <= 0 || len(d.upper) == 0 {
		return 0
	}
	rank := q * d.count
	lower, prev := 0.0, 0.0
	for i, ub := range d.upper {
		if d.cum[i] >= rank {
			if d.cum[i] == prev {
				return ub
			}
			return lower + (ub-lower)*(rank-prev)/(d.cum[i]-prev)
		}
		lower, prev = ub, d.cum[i]
	}
	return d.upper[len(d.upper)-1] // in +Inf: the highest finite bound
}

// ShardRaft is one shard's Raft figures over the run.
type ShardRaft struct {
	Shard       int     `json:"shard"`
	CommitP50Ms float64 `json:"commitP50Ms"`
	CommitP99Ms float64 `json:"commitP99Ms"`
	Commits     int     `json:"commits"`
	Snapshots   int     `json:"snapshots"` // snapshots taken by the shard's replicas
	LogBytesMax float64 `json:"logBytesMax"`
	SnapBytes   float64 `json:"snapshotBytesMax"`
}

// RaftFigures compares the scrapes taken before and after the run. Commit
// latency is observed only on the leader, so summing the shard's replicas'
// buckets counts each commit once.
func RaftFigures(start, end map[string]Scrape) ([]ShardRaft, error) {
	type acc struct {
		commit, snap bucketDelta
		log, snapB   float64
	}
	byShard := map[string]*acc{}
	for pod, e := range end {
		sh := e.shardOf()
		if sh == "" {
			continue // the coordinator
		}
		a := byShard[sh]
		if a == nil {
			a = &acc{}
			byShard[sh] = a
		}
		s := start[pod]
		a.commit.add(s.histogram("storage_raft_commit_seconds"), e.histogram("storage_raft_commit_seconds"))
		a.snap.add(s.histogram("storage_raft_snapshot_create_seconds"), e.histogram("storage_raft_snapshot_create_seconds"))
		a.log = math.Max(a.log, e.gauge("storage_raft_log_bytes"))
		a.snapB = math.Max(a.snapB, e.gauge("storage_snapshot_bytes"))
	}
	if len(byShard) == 0 {
		return nil, fmt.Errorf("report: no storage node scrapes")
	}
	var out []ShardRaft
	for sh, a := range byShard {
		n, err := strconv.Atoi(sh)
		if err != nil {
			return nil, err
		}
		out = append(out, ShardRaft{Shard: n, CommitP50Ms: round2(1000 * a.commit.quantile(0.5)), CommitP99Ms: round2(1000 * a.commit.quantile(0.99)),
			Commits: int(a.commit.count), Snapshots: int(a.snap.count), LogBytesMax: a.log, SnapBytes: a.snapB})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Shard < out[j].Shard })
	return out, nil
}

// --- cgroup samples (loadtest/run.sh) -------------------------------------------------------

// CgroupSample is one pod's cgroup counters at one instant, from the kind
// node: cpu.stat usage_usec and nr_throttled, memory.current.
type CgroupSample struct {
	TMs         int64  `json:"tMs"`
	Pod         string `json:"pod"`
	UsageUsec   int64  `json:"usageUsec"`
	NrThrottled int64  `json:"nrThrottled"`
	MemBytes    int64  `json:"memBytes"`
}

// PodFigures is one pod's peak use over the run.
type PodFigures struct {
	Name                string  `json:"name"`
	CPUMillicoresMax    float64 `json:"cpuMillicoresMax"`
	CPUMillicoresAvg    float64 `json:"cpuMillicoresAvg"`
	MemMiBMax           float64 `json:"memMiBMax"`
	CPUThrottledPeriods int64   `json:"cpuThrottledPeriods"`
}

// ReadCgroup reads the samples file (JSON lines).
func ReadCgroup(path string) ([]CgroupSample, error) {
	f, err := os.Open(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []CgroupSample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var s CgroupSample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, fmt.Errorf("report: %s: %w", path, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

// PodUse turns samples into per-pod figures: peak CPU is the highest rate
// between consecutive samples (a sampling-interval average, not an
// instantaneous peak), memory the highest memory.current seen.
func PodUse(samples []CgroupSample) []PodFigures {
	byPod := map[string][]CgroupSample{}
	for _, s := range samples {
		byPod[s.Pod] = append(byPod[s.Pod], s)
	}
	var out []PodFigures
	for pod, ss := range byPod {
		slices.SortFunc(ss, func(a, b CgroupSample) int { return int(a.TMs - b.TMs) })
		f := PodFigures{Name: pod}
		for i, s := range ss {
			f.MemMiBMax = math.Max(f.MemMiBMax, float64(s.MemBytes)/(1<<20))
			if i == 0 {
				continue
			}
			if dt := ss[i].TMs - ss[i-1].TMs; dt > 0 {
				f.CPUMillicoresMax = math.Max(f.CPUMillicoresMax, float64(ss[i].UsageUsec-ss[i-1].UsageUsec)/float64(dt))
			}
		}
		if n := len(ss); n > 1 && ss[n-1].TMs > ss[0].TMs {
			f.CPUMillicoresAvg = float64(ss[n-1].UsageUsec-ss[0].UsageUsec) / float64(ss[n-1].TMs-ss[0].TMs)
			f.CPUThrottledPeriods = ss[n-1].NrThrottled - ss[0].NrThrottled
		}
		f.CPUMillicoresMax, f.CPUMillicoresAvg, f.MemMiBMax = round2(f.CPUMillicoresMax), round2(f.CPUMillicoresAvg), round2(f.MemMiBMax)
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
