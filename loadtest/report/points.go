// Package report assembles the S5.2 load report
// (docs/evidence/S5.2/<runId>/summary.json) from one run's raw data: k6's
// JSON output, the storage nodes' /metrics before and after, cgroup samples
// of every pod, the final status of every request the run created, and the
// facts loadtest/run.sh records (host, versions, configuration, timing,
// invcheck). scripts/load-report-check.py checks what it writes.
package report

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"time"
)

// Req is one HTTP request from k6's output.
type Req struct {
	T      time.Time
	Class  string // write, read or list (loadtest/k6/lib.js)
	Op     string
	Status string
	DurMs  float64
	Failed bool
}

// APIError is one api_errors point: a non-2xx answer by status and JSON code.
type APIError struct {
	T            time.Time
	Status, Code string
}

// Points is what the report uses from k6's raw output.
type Points struct {
	Reqs    []Req
	Errors  []APIError
	Created []string    // request IDs from created_request, in order
	Dropped []time.Time // one per dropped iteration
	Skipped int
}

type point struct {
	Type   string `json:"type"`
	Metric string `json:"metric"`
	Data   struct {
		Time  time.Time         `json:"time"`
		Value float64           `json:"value"`
		Tags  map[string]string `json:"tags"`
	} `json:"data"`
}

// ReadPoints reads k6's --out json file (gzip if it ends in .gz).
func ReadPoints(path string) (Points, error) {
	f, err := os.Open(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return Points{}, err
	}
	defer func() { _ = f.Close() }()
	var r io.Reader = f
	if len(path) > 3 && path[len(path)-3:] == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return Points{}, err
		}
		defer func() { _ = gz.Close() }()
		r = gz
	}
	var p Points
	// http_req_failed and http_req_duration arrive as separate points with
	// the same time and tags; pair them in order per (time, name, op).
	failed := map[string][]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var pt point
		if err := json.Unmarshal(sc.Bytes(), &pt); err != nil {
			return Points{}, fmt.Errorf("report: %s: %w", path, err)
		}
		if pt.Type != "Point" {
			continue
		}
		tags := pt.Data.Tags
		switch pt.Metric {
		case "http_req_duration":
			p.Reqs = append(p.Reqs, Req{T: pt.Data.Time, Class: tags["class"], Op: tags["op"], Status: tags["status"], DurMs: pt.Data.Value})
		case "http_req_failed":
			k := reqKey(pt)
			failed[k] = append(failed[k], pt.Data.Value != 0)
		case "api_errors":
			p.Errors = append(p.Errors, APIError{T: pt.Data.Time, Status: tags["status"], Code: tags["code"]})
		case "created_request":
			p.Created = append(p.Created, tags["request_id"])
		case "dropped_iterations":
			for range int(pt.Data.Value) {
				p.Dropped = append(p.Dropped, pt.Data.Time)
			}
		case "skipped_ops":
			p.Skipped += int(pt.Data.Value)
		}
	}
	if err := sc.Err(); err != nil {
		return Points{}, err
	}
	for i := range p.Reqs {
		k := p.Reqs[i].T.Format(time.RFC3339Nano) + "|" + p.Reqs[i].Op + "|" + p.Reqs[i].Status
		if l := failed[k]; len(l) > 0 {
			p.Reqs[i].Failed, failed[k] = l[0], l[1:]
		} else {
			// No http_req_failed point: judge by the status, as lib.js does.
			p.Reqs[i].Failed = p.Reqs[i].Status < "200" || p.Reqs[i].Status > "299"
		}
	}
	return p, nil
}

func reqKey(pt point) string {
	return pt.Data.Time.Format(time.RFC3339Nano) + "|" + pt.Data.Tags["op"] + "|" + pt.Data.Tags["status"]
}

// Window is a measured interval [From, To).
type Window struct{ From, To time.Time }

func (w Window) has(t time.Time) bool { return !t.Before(w.From) && t.Before(w.To) }

// Seconds is the window's length.
func (w Window) Seconds() float64 { return w.To.Sub(w.From).Seconds() }

// ClassStats are one class's latency figures in a window.
type ClassStats struct {
	Count int     `json:"count"`
	P50Ms float64 `json:"p50Ms"`
	P95Ms float64 `json:"p95Ms"`
	P99Ms float64 `json:"p99Ms"`
}

// Stats is a window's figures.
type Stats struct {
	Requests int
	Failed   int
	Dropped  int
	Classes  map[string]ClassStats
}

// Classes are the report's request classes, each with its own threshold.
var Classes = []string{"write", "read", "list"}

// In computes the figures of the requests that started in w.
func (p Points) In(w Window) Stats {
	durs := map[string][]float64{}
	s := Stats{Classes: map[string]ClassStats{}}
	for _, r := range p.Reqs {
		if !w.has(r.T) {
			continue
		}
		s.Requests++
		if r.Failed {
			s.Failed++
		}
		durs[r.Class] = append(durs[r.Class], r.DurMs)
	}
	for _, c := range Classes {
		d := durs[c]
		slices.Sort(d)
		s.Classes[c] = ClassStats{Count: len(d), P50Ms: Quantile(d, 0.50), P95Ms: Quantile(d, 0.95), P99Ms: Quantile(d, 0.99)}
	}
	for _, t := range p.Dropped {
		if w.has(t) {
			s.Dropped++
		}
	}
	return s
}

// Quantile of sorted values by linear interpolation between the closest
// ranks (k6's trend method); 0 for no values. Rounded to 0.01 ms.
func Quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	pos := q * float64(len(sorted)-1)
	lo := int(pos)
	v := sorted[lo]
	if lo+1 < len(sorted) {
		v += (pos - float64(lo)) * (sorted[lo+1] - sorted[lo])
	}
	return round2(v)
}

func round2(v float64) float64 { return float64(int64(v*100+0.5)) / 100 }
