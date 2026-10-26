package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

type corrupt struct{ store.Store }

// Scan drops the claim of the MATCHED fixture request's resource.
func (c corrupt) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	res, err := c.Store.Scan(ctx, q)
	for i, r := range res.Records {
		if r.Key == "res:cli-r-matched" {
			x := *r.Resource
			x.Status, x.ActiveClaim = store.ResourceAvailable, nil
			res.Records[i].Resource = &x
		}
	}
	return res, err
}

// The exit code tells a script what it may conclude: 0 clean, 1 violations,
// 2 no verdict. An incomplete read never prints a clean report.
func TestCheckExitCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := invchecktest.Seed(ctx, s, "cli", 5); err != nil {
		t.Fatal(err)
	}
	down := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{2}}
		}
		return memstore.Fault{}
	})
	var writes atomic.Int64
	busy := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{Before: func(ctx context.Context) error {
				op := fmt.Sprintf("busy-%d", writes.Add(1))
				_, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: op, At: invchecktest.T0}, Key: "kv:" + op, Value: []byte(`1`)})
				return err
			}}
		}
		return memstore.Fault{}
	})
	opt := invcheck.Options{MaxPartialPages: 2, PartialBackoff: time.Millisecond}
	zero := 0
	gated := opt
	gated.MaxBacklog = &zero
	for _, tc := range []struct {
		name string
		s    store.Store
		opt  invcheck.Options
		code int
		has  string
	}{
		{"clean", s, opt, exitClean, "verdict: clean\n"},
		{"violation", corrupt{s}, opt, exitViolations, string(invcheck.MissingReciprocal)},
		{"backlog over --max-backlog", s, gated, exitViolations, string(invcheck.OutboxBacklog)},
		{"shard down", down, opt, exitIncomplete, "incomplete"},
		{"busy", busy, opt, exitNotQuiescent, "verdict: clean (not quiescent)"},
		{"busy with a violation", corrupt{busy}, opt, exitNotQuiescent, "verdict: violations (not quiescent)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var text, js bytes.Buffer
			if code := check(ctx, tc.s, tc.opt, false, &text); code != tc.code || !strings.Contains(text.String(), tc.has) {
				t.Fatalf("exit %d, output %q; want exit %d mentioning %q", code, text.String(), tc.code, tc.has)
			}
			if tc.code == exitIncomplete && strings.Contains(text.String(), "verdict:") {
				t.Fatalf("incomplete read printed %q", text.String())
			}
			if code := check(ctx, tc.s, tc.opt, true, &js); code != tc.code {
				t.Fatalf("--json exit %d, want %d", code, tc.code)
			}
			var out struct {
				OK         bool   `json:"ok"`
				Incomplete string `json:"incomplete"`
			}
			if err := json.Unmarshal(js.Bytes(), &out); err != nil || out.OK != (tc.code == exitClean) || (out.Incomplete != "") != (tc.code == exitIncomplete) {
				t.Fatalf("--json = %s (%v)", js.String(), err)
			}
		})
	}
}

// load saves a ledger that verify then passes; after an acknowledged write
// is overwritten, verify names it and exits 1. A missing ledger is no verdict.
func TestLoadThenVerify(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	path := filepath.Join(t.TempDir(), "ledger.json")

	var out bytes.Buffer
	d := ledger.Driver{Store: s, Prefix: "cli", Workers: 2, Keys: 2, Ops: 10, Retries: 1, At: invchecktest.T0}
	if code := load(ctx, d, path, &out); code != exitClean || !strings.Contains(out.String(), "24 acknowledged, 0 unknown") {
		t.Fatalf("load exit %d: %s", code, out.String())
	}
	out.Reset()
	if code := verify(ctx, s, path, false, &out); code != exitClean || !strings.Contains(out.String(), "checked 24 acknowledged writes over 14 keys: 0 lost") {
		t.Fatalf("verify exit %d: %s", code, out.String())
	}

	rec, err := s.ReadRecord(ctx, "cli-w0-k0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: "overwrite", At: invchecktest.T0}, Key: rec.Key,
		ExpectedVersion: rec.Version, Value: []byte(`{"stale":true}`)}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := verify(ctx, s, path, false, &out); code != exitViolations || !strings.Contains(out.String(), "lost cli-w0-op") || !strings.Contains(out.String(), "cli-w0-k0") {
		t.Fatalf("verify after overwrite exit %d: %s", code, out.String())
	}
	out.Reset()
	if code := verify(ctx, s, path, true, &out); code != exitViolations || !strings.Contains(out.String(), `"ok": false`) {
		t.Fatalf("verify --json exit %d: %s", code, out.String())
	}
	if code := verify(ctx, s, filepath.Join(t.TempDir(), "absent.json"), false, &out); code != exitIncomplete {
		t.Fatalf("verify of a missing ledger exit %d", code)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

// Bad flags are a usage error before anything is read.
func TestUsage(t *testing.T) {
	var stderr bytes.Buffer
	for _, args := range [][]string{
		nil,
		{"check", "--max-backlog", "many"},
		{"load"},
		{"verify", "--coordinator", "http://127.0.0.1:1"},
	} {
		if code := run(context.Background(), args, &bytes.Buffer{}, &stderr); code != exitUsage {
			t.Fatalf("run %q = %d, want usage error", args, code)
		}
	}
}
