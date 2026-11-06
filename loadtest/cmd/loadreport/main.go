// Command loadreport builds the S5.2 load report for one run directory
// (loadtest/run.sh calls it).
//
//	loadreport statuses -dir D -api URL   read back every request the run created
//	                                     (raw/statuses.jsonl); prints matched/waiting
//	loadreport build -dir D [-spill DIR] [-max-commit-bytes N]
//	                                     write D/summary.json and D/SHA256SUMS
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/report"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	dir := fs.String("dir", "", "run directory (required)")
	api := fs.String("api", "http://127.0.0.1:8080", "public API base URL")
	parallel := fs.Int("parallel", 4, "statuses: concurrent reads")
	spill := fs.String("spill", "", "build: where raw files too large to commit are moved")
	maxBytes := fs.Int64("max-commit-bytes", 5<<20, "build: the largest raw file committed beside the report")
	_ = fs.Parse(os.Args[2:])
	if *dir == "" {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "statuses":
		err = statuses(ctx, *dir, *api, *parallel)
	case "build":
		var sum map[string]any
		sum, err = report.Build(*dir, report.Options{MaxCommitBytes: *maxBytes, SpillDir: *spill})
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(map[string]any{"results": sum["results"], "match": sum["match"],
				"saturation": sum["saturation"], "invariants": sum["invariants"]})
		}
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadreport:", err)
		os.Exit(1)
	}
}

func statuses(ctx context.Context, dir, api string, parallel int) error {
	pts, err := report.ReadPoints(filepath.Join(dir, "raw", "k6-points.jsonl.gz"))
	if err != nil {
		return err
	}
	ss := report.CollectStatuses(ctx, api, pts.Created, parallel)
	if err := report.WriteStatuses(filepath.Join(dir, "raw", "statuses.jsonl"), ss); err != nil {
		return err
	}
	m, lost := report.Summarise(ss)
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"created": len(pts.Created), "matched": m.Matched, "waiting": m.Waiting,
		"other": m.Other, "unreadable": len(lost)})
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: loadreport statuses|build -dir <run dir> [flags]")
	os.Exit(2)
}
