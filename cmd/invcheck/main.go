// Command invcheck checks the storage invariants over a full read of the
// cluster and records or verifies an acknowledged-write ledger. Every
// command reads and writes through coordclient.
//
//	invcheck check --coordinator URL [--json] [--max-backlog N] [--page-limit N]
//	               [--consistency strong|linearizable]
//	                       walk every request and resource, check the claim
//	                       invariants and report the outbox backlog. Exit 0:
//	                       complete, clean and quiescent; 1: violations; 2:
//	                       the read was incomplete, so no verdict; 3: a write
//	                       committed during the walk (not quiescent), whatever
//	                       was found.
//	invcheck load --coordinator URL --ledger FILE [--prefix P] [--workers N]
//	                       [--keys N] [--ops N] [--duration D] [--interval D]
//	                       [--retries N] [--readers N] [--read-interval D]
//	                       [--consistency strong|linearizable|stale]
//	                       drive writes and save the acknowledged-write ledger
//	                       to FILE: --ops writes per worker, or with --ops 0
//	                       until --duration has passed. --readers read the
//	                       update keys in --consistency mode while the writers
//	                       run, and their reads go into the ledger (stale is
//	                       the negative control: verify must catch it). Exit
//	                       2 if interrupted (the ledger so far is still saved).
//	invcheck verify --coordinator URL --ledger FILE [--json] [--window FROM:TO]
//	                [--consistency strong|linearizable]
//	                       after writers stop: resolve unknown outcomes by
//	                       operation lookup, then check every acknowledged
//	                       write, and every reader's read against the writes
//	                       acknowledged before it began. Exit 0: none lost
//	                       and no stale read; 1: some lost or stale; 2: a read
//	                       or lookup failed, so no verdict. --json also gives
//	                       the ledger as written (counts, failed attempts and
//	                       the writes acknowledged between FROM and TO, unix ms).
//	invcheck inject --coordinator URL --ledger FILE --confirm
//	                       chaos build only (go build -tags chaos, the tools
//	                       image): overwrite one acknowledged write so that a
//	                       harness can show it catches a loss.
//
// --timeout (default 5m) bounds the whole command. --consistency (default
// strong) is how every read is made, and an answer served weaker than that
// is refused (#72). check and verify refuse stale: a lagging replica could
// report a violation or a loss that is not there.
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
	"strings"
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

// command is a subcommand beyond check, load and verify that this build
// includes: inject, in the chaos build only (inject.go).
type command struct {
	flags func(fs *flag.FlagSet)      // registers its own flags
	valid func(stderr io.Writer) bool // after parsing, before connecting
	run   func(ctx context.Context, s store.Store, path string, out io.Writer) int
}

var commands = map[string]*command{}

func usage(stderr io.Writer) int {
	names := "check|load|verify"
	for name := range commands {
		names += "|" + name
	}
	_, _ = fmt.Fprintf(stderr, "usage: invcheck %s --coordinator URL [flags]\n", names)
	return exitUsage
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		return usage(stderr)
	}
	extra := commands[args[0]]
	switch args[0] {
	case "check", "load", "verify":
	default:
		if extra == nil {
			_, _ = fmt.Fprintf(stderr, "invcheck: unknown command %q\n", args[0])
			return usage(stderr)
		}
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
	fs.IntVar(&d.Ops, "ops", 200, "writes per writer; 0: until --duration (load)")
	fs.DurationVar(&d.Duration, "duration", 0, "stop starting writes after this long (load)")
	fs.DurationVar(&d.Interval, "interval", 0, "pause between one writer's writes (load)")
	var win window
	fs.Func("window", "FROM:TO unix ms: count the writes acknowledged in it (verify --json)", win.parse)
	if extra != nil && extra.flags != nil {
		extra.flags(fs)
	}
	fs.IntVar(&d.Retries, "retries", 3, "same-operation retries of an unknown outcome (load)")
	mode := fs.String("consistency", "strong", "read mode: strong or linearizable; stale for load's readers only")
	fs.IntVar(&d.Readers, "readers", 0, "concurrent readers of the update keys while the writers run (load)")
	fs.DurationVar(&d.ReadInterval, "read-interval", 20*time.Millisecond, "pause between one reader's reads (load)")
	if err := fs.Parse(args[1:]); err != nil {
		return exitUsage
	}
	switch store.Consistency(*mode) {
	case store.ConsistencyStrong, store.ConsistencyLinearizable:
	case store.ConsistencyStale:
		if args[0] != "load" {
			_, _ = fmt.Fprintf(stderr, "invcheck: --consistency stale is refused for %s: a lagging replica could report a violation or a loss that is not there\n", args[0])
			return exitUsage
		}
	default:
		_, _ = fmt.Fprintf(stderr, "invcheck: --consistency %q: want strong or linearizable (or stale, for load)\n", *mode)
		return exitUsage
	}
	if d.Readers < 0 {
		_, _ = fmt.Fprintln(stderr, "invcheck: --readers must be 0 or more")
		return exitUsage
	}
	if args[0] != "check" && *path == "" {
		_, _ = fmt.Fprintln(stderr, "invcheck: --ledger is required")
		return exitUsage
	}
	if args[0] == "load" && d.Ops <= 0 && d.Duration <= 0 {
		_, _ = fmt.Fprintln(stderr, "invcheck: load needs --ops or --duration")
		return exitUsage
	}
	if extra != nil && extra.valid != nil && !extra.valid(stderr) {
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client, err := coordclient.New(ctx, *coord, coordclient.WithReadConsistency(store.Consistency(*mode)))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "invcheck: coordinator %s: %v\n", *coord, err)
		return exitIncomplete
	}
	switch args[0] {
	case "check":
		return check(ctx, client, opt, *asJSON, stdout)
	case "load":
		d.Store, d.At = client, time.Now().UnixMilli()
		if d.Readers > 0 {
			d.ReadConsistency = *mode
			d.Read = func(ctx context.Context, key string) (store.Record, string, error) {
				rec, info, err := client.ReadRecordInfo(ctx, key)
				return rec, string(info.Consistency), err
			}
		}
		return load(ctx, d, *path, stdout)
	case "verify":
		return verify(ctx, client, *path, *asJSON, win, stdout)
	}
	return extra.run(ctx, client, *path, stdout)
}

// window is verify's --window FROM:TO, in unix ms.
type window struct {
	from, to int64
	set      bool
}

func (w *window) parse(v string) error {
	a, b, ok := strings.Cut(v, ":")
	from, err1 := strconv.ParseInt(a, 10, 64)
	to, err2 := strconv.ParseInt(b, 10, 64)
	if !ok || err1 != nil || err2 != nil || from > to {
		return fmt.Errorf("want FROM:TO in unix ms, FROM <= TO")
	}
	w.from, w.to, w.set = from, to, true
	return nil
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
	_, _ = fmt.Fprintf(out, "ledger %s: %d acknowledged, %d unknown, %d rejected, %d reads\n", path, len(l.Acked), len(l.Unknown), len(l.Rejected), len(l.Reads))
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

// verify resolves the ledger's unknown writes and checks every acknowledged
// one; with JSON it also reports the ledger as written, counting the writes
// acknowledged inside w.
func verify(ctx context.Context, s store.Store, path string, asJSON bool, w window, out io.Writer) int {
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
	from, to := w.from, w.to
	if !w.set {
		from, to = 1, 0 // an empty window
	}
	stats := l.Stats(from, to)
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
			Ledger     ledger.Stats      `json:"ledger"`
			Resolution ledger.Resolution `json:"resolution"`
			Verdict    *ledger.Verdict   `json:"verdict,omitempty"`
		}{OK: code == exitClean, Ledger: stats, Resolution: res}
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
	r := v.Reads
	_, _ = fmt.Fprintf(out, "checked %d of %d reads (%d before any acknowledgement, %d failed; served %v): %d stale\n",
		r.Checked, r.Total, r.Unchecked, r.Failed, r.Served, len(v.StaleReads))
	for i, sr := range v.StaleReads {
		if i == maxStaleListed {
			_, _ = fmt.Fprintf(out, "... and %d more stale reads\n", len(v.StaleReads)-i)
			break
		}
		_, _ = fmt.Fprintf(out, "stale read %s: %s\n", sr.Read.Key, sr.Reason)
	}
	return code
}

// maxStaleListed bounds the stale reads verify prints; --json has them all.
const maxStaleListed = 50

func writeJSON(out io.Writer, v any) {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: %v\n", err)
	}
}
