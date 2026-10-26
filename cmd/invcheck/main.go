// Command invcheck checks the storage invariants over a full read of the
// cluster and records or verifies an acknowledged-write ledger. Every
// command reads and writes through coordclient.
//
//	invcheck check --coordinator URL [--json] [--max-backlog N] [--page-limit N]
//	                       walk every request and resource, check the claim
//	                       invariants and report the outbox backlog. Exit 0:
//	                       complete, clean and quiescent; 1: violations; 2:
//	                       the read was incomplete, so no verdict; 3: a write
//	                       committed during the walk (not quiescent), whatever
//	                       was found.
//	invcheck load --coordinator URL --ledger FILE [--prefix P] [--workers N]
//	                       [--keys N] [--ops N] [--retries N]
//	                       drive writes and save the acknowledged-write ledger
//	                       to FILE. Exit 2 if interrupted (the ledger so far
//	                       is still saved).
//	invcheck verify --coordinator URL --ledger FILE [--json]
//	                       after writers stop: resolve unknown outcomes by
//	                       operation lookup, then check every acknowledged
//	                       write. Exit 0: none lost; 1: some lost; 2: a read
//	                       or lookup failed, so no verdict.
//
// --timeout (default 5m) bounds the whole command.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

const (
	exitClean          = 0
	exitViolations     = 1
	exitIncomplete     = 2
	exitNotQuiescent   = 3
	exitUsage          = 64
	defaultTimeout     = 5 * time.Minute
	defaultCoordinator = "http://localhost:8080"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func usage(stderr io.Writer) int {
	_, _ = fmt.Fprintln(stderr, "usage: invcheck check|load|verify --coordinator URL [flags]")
	return exitUsage
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		return usage(stderr)
	}
	fs := flag.NewFlagSet("invcheck "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	coord := fs.String("coordinator", defaultCoordinator, "coordinator base URL")
	timeout := fs.Duration("timeout", defaultTimeout, "deadline for the whole command")
	asJSON := fs.Bool("json", false, "print the result as JSON")
	path := fs.String("ledger", "", "ledger file (load, verify)")

	var opt invcheck.Options
	fs.IntVar(&opt.PageLimit, "page-limit", 0, "scan page size per shard (check; default 500)")
	fs.Func("max-backlog", "most unpublished events allowed per shard (check; unset: report only)", func(v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		opt.MaxBacklog = &n
		return nil
	})
	d := ledger.Driver{Prefix: fmt.Sprintf("load-%d", time.Now().UnixMilli())}
	fs.StringVar(&d.Prefix, "prefix", d.Prefix, "key and operation ID prefix (load)")
	fs.IntVar(&d.Workers, "workers", 4, "concurrent writers (load)")
	fs.IntVar(&d.Keys, "keys", 4, "update keys per writer (load)")
	fs.IntVar(&d.Ops, "ops", 200, "writes per writer (load)")
	fs.IntVar(&d.Retries, "retries", 3, "same-operation retries of an unknown outcome (load)")
	if err := fs.Parse(args[1:]); err != nil {
		return exitUsage
	}
	if args[0] != "check" && *path == "" {
		_, _ = fmt.Fprintln(stderr, "invcheck: --ledger is required")
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client, err := coordclient.New(ctx, *coord)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "invcheck: coordinator %s: %v\n", *coord, err)
		return exitIncomplete
	}
	switch args[0] {
	case "check":
		return check(ctx, client, opt, *asJSON, stdout)
	case "load":
		d.Store, d.At = client, time.Now().UnixMilli()
		return load(ctx, d, *path, stdout)
	case "verify":
		return verify(ctx, client, *path, *asJSON, stdout)
	}
	return usage(stderr)
}

// check runs one invariant check on s, writes the report to out and returns
// the exit code.
func check(ctx context.Context, s store.Store, opt invcheck.Options, asJSON bool, out io.Writer) int {
	rep, err := invcheck.Run(ctx, s, opt)
	code := exitClean
	switch {
	case err != nil:
		code = exitIncomplete
	case !rep.Quiescent:
		code = exitNotQuiescent
	case !rep.OK():
		code = exitViolations
	}
	if asJSON {
		res := struct {
			OK         bool             `json:"ok"`
			Verdict    string           `json:"verdict,omitempty"`
			Incomplete string           `json:"incomplete,omitempty"`
			Report     *invcheck.Report `json:"report,omitempty"`
		}{OK: code == exitClean}
		if err != nil {
			res.Incomplete = err.Error()
		} else {
			res.Verdict, res.Report = rep.Verdict(), &rep
		}
		writeJSON(out, res)
		return code
	}
	if err != nil {
		_, _ = fmt.Fprintf(out, "incomplete read, no verdict (%v)\n", err)
		return code
	}
	_, _ = fmt.Fprintf(out, "verdict: %s\n", rep.Verdict())
	_, _ = fmt.Fprintf(out, "read %d requests and %d resources in %d pages (%d partial, resumed)\n",
		rep.Requests, rep.Resources, rep.Pages, rep.PartialPages)
	for shard := range s.Shards() {
		_, _ = fmt.Fprintf(out, "shard %d: applied %d -> %d\n", shard, rep.AppliedBefore[shard], rep.AppliedAfter[shard])
	}
	for _, b := range rep.Outbox {
		atLeast := ""
		if b.AtLeast {
			atLeast = "at least "
		}
		_, _ = fmt.Fprintf(out, "outbox shard %d: %s%d pending, oldest at %d\n", b.Shard, atLeast, b.Pending, b.OldestAt)
	}
	for _, v := range rep.Violations {
		_, _ = fmt.Fprintf(out, "violation %s %s: %s\n", v.Rule, v.Key, v.Detail)
	}
	return code
}

// load runs d and saves its ledger to path, even when interrupted.
func load(ctx context.Context, d ledger.Driver, path string, out io.Writer) int {
	l, runErr := d.Run(ctx)
	if err := saveLedger(l, path); err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: %v\n", err)
		return exitIncomplete
	}
	_, _ = fmt.Fprintf(out, "ledger %s: %d acknowledged, %d unknown, %d rejected\n", path, len(l.Acked), len(l.Unknown), len(l.Rejected))
	if runErr != nil {
		_, _ = fmt.Fprintf(out, "interrupted: %v\n", runErr)
		return exitIncomplete
	}
	return exitClean
}

// saveLedger writes l to path through a temporary file, so a crash never
// leaves half a ledger.
func saveLedger(l *ledger.Ledger, path string) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("save ledger: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if err := l.Save(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("save ledger: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("save ledger: %w", err)
	}
	return nil
}

// verify resolves the ledger's unknown writes and checks every acknowledged one.
func verify(ctx context.Context, s store.Store, path string, asJSON bool, out io.Writer) int {
	f, err := os.Open(path)
	if err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: %v\n", err)
		return exitIncomplete
	}
	l, err := ledger.Load(f)
	_ = f.Close()
	if err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: %v\n", err)
		return exitIncomplete
	}
	res, err := l.Resolve(ctx, s)
	var v ledger.Verdict
	if err == nil {
		v, err = l.Verify(ctx, s)
	}
	code := exitClean
	switch {
	case err != nil:
		code = exitIncomplete
	case !v.OK():
		code = exitViolations
	}
	if asJSON {
		r := struct {
			OK         bool              `json:"ok"`
			Incomplete string            `json:"incomplete,omitempty"`
			Resolution ledger.Resolution `json:"resolution"`
			Verdict    *ledger.Verdict   `json:"verdict,omitempty"`
		}{OK: code == exitClean, Resolution: res}
		if err != nil {
			r.Incomplete = err.Error()
		} else {
			r.Verdict = &v
		}
		writeJSON(out, r)
		return code
	}
	_, _ = fmt.Fprintf(out, "resolved %d unknown writes: %d committed, %d never applied, %d still unknown\n",
		res.Resolved+res.NotApplied+res.StillUnknown, res.Resolved, res.NotApplied, res.StillUnknown)
	if err != nil {
		_, _ = fmt.Fprintf(out, "incomplete read, no verdict (%v)\n", err)
		return code
	}
	_, _ = fmt.Fprintf(out, "checked %d acknowledged writes over %d keys: %d lost\n", v.Checked, v.Keys, len(v.Lost))
	for _, loss := range v.Lost {
		_, _ = fmt.Fprintf(out, "lost %s %s: %s\n", loss.Entry.OperationID, loss.Entry.Key, loss.Reason)
	}
	return code
}

func writeJSON(out io.Writer, v any) {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: %v\n", err)
	}
}
