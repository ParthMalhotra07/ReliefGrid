package report_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/report"
)

// Repeated runs are reported as median plus min/max (decided 2026-11-06):
// matched and waiting vary run to run because matching is asynchronous.
func TestAggregate(t *testing.T) {
	var dirs []string
	for i, v := range []struct {
		matched, waiting int
		rps, p99         float64
		sat              float64
	}{{8, 4, 30, 200, 30}, {6, 6, 28, 240, 30}, {7, 5, 31, 180, 20}} {
		d := filepath.Join(t.TempDir(), "run", string(rune('a'+i)))
		_ = os.MkdirAll(d, 0o750)
		writeJSON(t, filepath.Join(d, "summary.json"), map[string]any{
			"runId": "r" + string(rune('a'+i)), "scenario": "step", "dataset": map[string]any{"payloadSha256": "same"},
			"results": map[string]any{"throughputRps": v.rps, "errors": map[string]any{"rate": 0.001},
				"classes": map[string]any{"write": map[string]any{"p50Ms": 10, "p95Ms": 50, "p99Ms": v.p99},
					"read": map[string]any{"p50Ms": 1, "p95Ms": 2, "p99Ms": 3}, "list": map[string]any{"p50Ms": 4, "p95Ms": 5, "p99Ms": 6}}},
			"match":      map[string]any{"matched": v.matched, "waiting": v.waiting, "p50Ms": 100, "p99Ms": 900},
			"saturation": map[string]any{"rps": v.sat},
		})
		dirs = append(dirs, d)
	}
	agg, err := report.Aggregate(dirs)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(agg)
	var got struct {
		Runs    []string
		Figures map[string]report.Spread
	}
	_ = json.Unmarshal(b, &got)
	for name, want := range map[string]report.Spread{
		"match.matched":               {Median: 7, Min: 6, Max: 8, N: 3},
		"match.waiting":               {Median: 5, Min: 4, Max: 6, N: 3},
		"results.throughputRps":       {Median: 30, Min: 28, Max: 31, N: 3},
		"results.classes.write.p99Ms": {Median: 200, Min: 180, Max: 240, N: 3},
		"saturation.rps":              {Median: 30, Min: 20, Max: 30, N: 3},
	} {
		if got.Figures[name] != want {
			t.Errorf("%s = %+v, want %+v", name, got.Figures[name], want)
		}
	}
	if len(got.Runs) != 3 {
		t.Errorf("runs %v", got.Runs)
	}
	// Runs of different scenarios or datasets are not comparable.
	writeJSON(t, filepath.Join(dirs[2], "summary.json"), map[string]any{"runId": "x", "scenario": "baseline", "dataset": map[string]any{"payloadSha256": "same"}})
	if _, err := report.Aggregate(dirs); err == nil {
		t.Error("Aggregate mixed a baseline run into step runs")
	}
}
