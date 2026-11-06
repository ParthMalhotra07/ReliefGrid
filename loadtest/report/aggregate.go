package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Spread is one figure across repeated runs.
type Spread struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	N      int     `json:"n"`
}

// aggregated are the figures reported as median plus min/max across runs.
var aggregated = []string{
	"results.throughputRps", "results.errors.rate",
	"results.classes.write.p50Ms", "results.classes.write.p95Ms", "results.classes.write.p99Ms",
	"results.classes.read.p50Ms", "results.classes.read.p95Ms", "results.classes.read.p99Ms",
	"results.classes.list.p50Ms", "results.classes.list.p95Ms", "results.classes.list.p99Ms",
	"match.matched", "match.waiting", "match.p50Ms", "match.p99Ms",
	"saturation.rps",
}

// Aggregate reads each dir's summary.json. Every run must be the same
// scenario with the same payloads (dataset.payloadSha256); a figure a run
// lacks (a smoke has no saturation) is left out of that figure's N.
func Aggregate(dirs []string) (map[string]any, error) {
	if len(dirs) == 0 {
		return nil, fmt.Errorf("report: no runs to aggregate")
	}
	values := map[string][]float64{}
	var runs []string
	var scenario, payload string
	for i, d := range dirs {
		var s map[string]any
		if err := readJSON(filepath.Join(d, "summary.json"), &s); err != nil {
			return nil, err
		}
		sc, _ := s["scenario"].(string)
		pl, _ := lookup(s, "dataset.payloadSha256").(string)
		if i == 0 {
			scenario, payload = sc, pl
		} else if sc != scenario || pl != payload {
			return nil, fmt.Errorf("report: %s is %s with payloads %.12s, the first run is %s with %.12s: not comparable", d, sc, pl, scenario, payload)
		}
		id, _ := s["runId"].(string)
		runs = append(runs, id)
		for _, f := range aggregated {
			if v, ok := lookup(s, f).(float64); ok {
				values[f] = append(values[f], v)
			}
		}
	}
	figs := map[string]Spread{}
	for f, v := range values {
		slices.Sort(v)
		med := v[len(v)/2]
		if len(v)%2 == 0 {
			med = (v[len(v)/2-1] + v[len(v)/2]) / 2
		}
		figs[f] = Spread{Median: round2(med), Min: v[0], Max: v[len(v)-1], N: len(v)}
	}
	return map[string]any{"scenario": scenario, "payloadSha256": payload, "runs": runs, "figures": figs}, nil
}

// WriteAggregate writes Aggregate's result as JSON to path.
func WriteAggregate(path string, agg map[string]any) error {
	b, err := json.MarshalIndent(agg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644) //nolint:gosec // evidence is world-readable
}

func lookup(m map[string]any, path string) any {
	var cur any = m
	for _, k := range strings.Split(path, ".") {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}
