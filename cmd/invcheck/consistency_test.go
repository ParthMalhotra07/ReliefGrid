package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// #72: --consistency is strong (default) or linearizable for check and
// verify. stale is refused: a lagging replica could make check report a
// violation or verify a loss that is not there. load takes stale too, for
// its readers only: that is the negative control the read check must catch.
func TestConsistencyFlag(t *testing.T) {
	ledgerPath := filepath.Join(t.TempDir(), "l.json")
	for _, tc := range []struct {
		args []string
		has  string
	}{
		{[]string{"check", "--consistency", "stale"}, "stale"},
		{[]string{"verify", "--ledger", ledgerPath, "--consistency", "stale"}, "stale"},
		{[]string{"check", "--consistency", "eventual"}, "eventual"},
		{[]string{"load", "--ledger", ledgerPath, "--ops", "1", "--consistency", "eventual"}, "eventual"},
		{[]string{"load", "--ledger", ledgerPath, "--ops", "1", "--readers", "-1"}, "invcheck: --readers must be 0 or more\n"},
	} {
		var stdout, stderr bytes.Buffer
		// No coordinator listens here: a usage error must come before any connection.
		args := append(tc.args, "--coordinator", "http://127.0.0.1:1", "--timeout", "2s")
		if code := run(context.Background(), args, &stdout, &stderr); code != exitUsage || !strings.Contains(stderr.String(), tc.has) {
			t.Errorf("%v: exit %d, stderr %q; want exit %d mentioning %q", tc.args, code, stderr.String(), exitUsage, tc.has)
		}
	}
}

// fakeCoordinator serves an empty three-shard cluster and records the
// consistency every read asked for, echoing it back as the served mode.
type fakeCoordinator struct {
	mu    sync.Mutex
	modes []string // path=consistency of every non-ring request
	srv   *httptest.Server
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	f := &fakeCoordinator{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/ring" {
			_ = json.NewEncoder(w).Encode(internalapi.Ring{RingVersion: 1, Hash: internalapi.Xxhash64, Shards: 3, VnodesPerShard: 128,
				Checksum: "sha256:670e1eca690b168c83fd910b2bdf1e2fbf81f4cd8de91994d8d5ac2989de6360"})
			return
		}
		mode := r.URL.Query().Get("consistency")
		f.mu.Lock()
		f.modes = append(f.modes, r.URL.Path+"="+mode)
		f.mu.Unlock()
		info := internalapi.ReadInfo{Consistency: internalapi.ReadInfoConsistency(mode), ServedBy: "storage-0-0"}
		switch r.URL.Path {
		case "/internal/scan":
			_ = json.NewEncoder(w).Encode(internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistency(mode),
				Records: []store.Record{}, Positions: []store.ShardPosition{{Shard: 0}, {Shard: 1}, {Shard: 2}}})
		case "/internal/outbox":
			_ = json.NewEncoder(w).Encode(internalapi.OutboxPage{Events: []store.OutboxEvent{}, Read: info, RingVersion: 1})
		default:
			w.WriteHeader(http.StatusTeapot)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCoordinator) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.modes...)
}

// check --consistency linearizable makes every scan and outbox read through
// the fence; without the flag every read is strong, as before.
func TestCheckReadsInTheChosenMode(t *testing.T) {
	for _, tc := range []struct {
		flag []string
		want string
	}{
		{nil, "strong"},
		{[]string{"--consistency", "strong"}, "strong"},
		{[]string{"--consistency", "linearizable"}, "linearizable"},
	} {
		f := newFakeCoordinator(t)
		var stdout, stderr bytes.Buffer
		args := append([]string{"check", "--coordinator", f.srv.URL, "--timeout", "10s"}, tc.flag...)
		if code := run(context.Background(), args, &stdout, &stderr); code != exitClean {
			t.Fatalf("%v: exit %d\n%s%s", tc.flag, code, stdout.String(), stderr.String())
		}
		seen := f.seen()
		if len(seen) == 0 {
			t.Fatalf("%v: no reads", tc.flag)
		}
		for _, s := range seen {
			if !strings.HasSuffix(s, "="+tc.want) {
				t.Errorf("%v: read %s, want consistency=%s", tc.flag, s, tc.want)
			}
		}
	}
}

// behind is a replica one write behind, for the CLI's negative control.
type behind struct {
	store.Store
	mu   sync.Mutex
	prev map[string]store.Record
}

func (s *behind) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	before, err := s.ReadRecord(ctx, cmd.Key)
	res, cerr := s.Store.CompareAndSet(ctx, cmd)
	if err == nil && cerr == nil && res.Outcome == store.OutcomeOK {
		s.mu.Lock()
		s.prev[cmd.Key] = before
		s.mu.Unlock()
	}
	return res, cerr
}

func (s *behind) read(ctx context.Context, key string) (store.Record, string, error) {
	s.mu.Lock()
	rec, ok := s.prev[key]
	s.mu.Unlock()
	if ok {
		return rec, "stale", nil
	}
	rec, err := s.ReadRecord(ctx, key)
	return rec, "stale", err
}

// load with readers saves their reads; verify checks them with the writes.
// Clean readers pass; readers on a replica one write behind fail verify
// (exit 1) with the stale reads named, though no write was lost.
func TestVerifyChecksTheReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name  string
		stale bool
		code  int
	}{{"clean readers", false, exitClean}, {"readers one write behind", true, exitViolations}} {
		t.Run(tc.name, func(t *testing.T) {
			mem, err := memstore.New(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = mem.Close() }()
			lag := &behind{Store: mem, prev: map[string]store.Record{}}
			read := func(ctx context.Context, key string) (store.Record, string, error) {
				rec, err := mem.ReadRecord(ctx, key)
				return rec, "linearizable", err
			}
			mode := "linearizable"
			if tc.stale {
				read, mode = lag.read, "stale"
			}
			path := filepath.Join(t.TempDir(), "ledger.json")
			d := ledger.Driver{Store: lag, Prefix: "cr", Workers: 2, Keys: 2, Ops: 40, Retries: 1, At: invchecktest.T0,
				Readers: 2, ReadInterval: time.Millisecond, Read: read, ReadConsistency: mode}
			var out bytes.Buffer
			if code := load(ctx, d, path, &out); code != exitClean || !strings.Contains(out.String(), "reads") {
				t.Fatalf("load exit %d: %s", code, out.String())
			}
			out.Reset()
			if code := verify(ctx, mem, path, false, window{}, &out); code != tc.code || !strings.Contains(out.String(), "0 lost") {
				t.Fatalf("verify exit %d, want %d: %s", code, tc.code, out.String())
			}
			if tc.stale != strings.Contains(out.String(), "stale read ") {
				t.Fatalf("verify output %q: stale reads named = %v, want %v", out.String(), !tc.stale, tc.stale)
			}
			out.Reset()
			verify(ctx, mem, path, true, window{}, &out)
			var js struct {
				Verdict struct {
					Reads      ledger.ReadStats   `json:"reads"`
					StaleReads []ledger.StaleRead `json:"staleReads"`
				} `json:"verdict"`
			}
			if err := json.Unmarshal(out.Bytes(), &js); err != nil || js.Verdict.Reads.Total == 0 || (len(js.Verdict.StaleReads) > 0) != tc.stale {
				t.Fatalf("--json verdict %s (%v)", out.String(), err)
			}
		})
	}
}
