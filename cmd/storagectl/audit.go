package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit"
)

// exitCodeError makes main exit with code instead of 1.
type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }
func (e *exitCodeError) Unwrap() error { return e.err }

// runMerkle is storagectl merkle: GET /mgmt/merkle on one pod with its
// OPERATOR_TOKEN, printing the digest. --at asks for exactly that command
// index (the node answers 409 otherwise); without it the digest is at
// whatever index the read sees, for looking at one pod, not comparing.
// --table with --bucket prints one bucket's leaves.
func runMerkle(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("merkle", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:8081", "management endpoint host:port")
	timeout := fs.Duration("timeout", 60*time.Second, "deadline for the call")
	at := fs.String("at", "", "exact command index (default: any)")
	table := fs.String("table", "", "table of the bucket to list (with --bucket)")
	bucket := fs.Int("bucket", -1, "bucket to list, 0-255 (with --table)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q := url.Values{}
	if *at != "" {
		n, err := strconv.ParseUint(*at, 10, 64)
		if err != nil || n == math.MaxUint64 {
			return fmt.Errorf("merkle: --at %q: want a command index", *at)
		}
		q.Set("at", *at)
	}
	if (*table == "") != (*bucket < 0) {
		return errors.New("merkle: --table and --bucket go together")
	}
	if *table != "" {
		q.Set("table", *table)
		q.Set("bucket", strconv.Itoa(*bucket))
	}
	path := "/mgmt/merkle"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	return operatorRequest(ctx, "merkle", http.MethodGet, *addr, path, *timeout, getenv, out)
}

type addrList []string

func (a *addrList) String() string     { return strings.Join(*a, ",") }
func (a *addrList) Set(v string) error { *a = append(*a, v); return nil }

// runAudit is storagectl audit: compare one shard's replicas' Merkle
// digests at one command index they all hold (internal/storagenode/audit)
// and print the report as JSON. Exit 0 equal, 1 divergent (the report names
// table, key and pod), 2 incomplete (no verdict), 3 not quiescent (the
// replicas would not hold one index; quiesce writers and relay). It only
// reads: a divergent replica is rebuilt through Raft, never patched.
func runAudit(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	var addrs addrList
	fs.Var(&addrs, "addr", "management endpoint host:port of one replica (repeat for each)")
	attempts := fs.Int("attempts", 5, "target indexes to try before giving up as not quiescent")
	settle := fs.Duration("settle", 30*time.Second, "how long every replica may take to reach a target")
	poll := fs.Duration("poll", 200*time.Millisecond, "status poll interval while waiting")
	timeout := fs.Duration("timeout", 5*time.Minute, "deadline for the whole audit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := getenv("OPERATOR_TOKEN")
	if token == "" {
		return errors.New("audit: OPERATOR_TOKEN is required")
	}
	if len(addrs) < 2 {
		return errors.New("audit: give at least two --addr, one per replica")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	rep := audit.Run(ctx, audit.Config{Addrs: addrs, Token: token, Attempts: *attempts, Settle: *settle, Poll: *poll})
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, string(b)); err != nil {
		return err
	}
	if code := rep.Verdict.ExitCode(); code != 0 {
		return &exitCodeError{code: code, err: fmt.Errorf("audit: %s: %s", rep.Verdict, rep.Error)}
	}
	return nil
}
