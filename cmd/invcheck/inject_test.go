//go:build chaos

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// inject refuses without --confirm, before it connects to anything.
func TestInjectNeedsConfirm(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"inject", "--ledger", "x.json", "--coordinator", "http://127.0.0.1:1"}, &bytes.Buffer{}, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), "--confirm") {
		t.Fatalf("inject without --confirm: exit %d, %q", code, stderr.String())
	}
}

// inject commits a real write over the key of the ledger's last
// acknowledged update, with a payload no acknowledged write produced, and
// names it; verify then reports that key lost.
func TestInjectMakesVerifyFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	path := filepath.Join(t.TempDir(), "ledger.json")
	d := ledger.Driver{Store: s, Prefix: "inj", Workers: 2, Keys: 2, Ops: 10, Retries: 1, At: invchecktest.T0}
	if code := load(ctx, d, path, &bytes.Buffer{}); code != exitClean {
		t.Fatalf("load exit %d", code)
	}

	var out bytes.Buffer
	if code := inject(ctx, s, path, &out); code != exitClean {
		t.Fatalf("inject exit %d: %s", code, out.String())
	}
	var inj injected
	if err := json.Unmarshal(out.Bytes(), &inj); err != nil {
		t.Fatalf("inject output: %v: %s", err, out.String())
	}
	if inj.Kind != "lost-write" || inj.Key != "inj-w1-k0" || !strings.HasPrefix(inj.OperationID, "inject-") || inj.Version == 0 {
		t.Fatalf("injected %+v, want the highest-seq acknowledged update (inj-w1-k0)", inj)
	}
	rec, err := s.ReadRecord(ctx, inj.Key)
	if err != nil || rec.Version != inj.Version {
		t.Fatalf("%s after inject: %+v, %v", inj.Key, rec, err)
	}

	out.Reset()
	if code := verify(ctx, s, path, false, window{}, &out); code != exitViolations || !strings.Contains(out.String(), inj.Key) {
		t.Fatalf("verify after inject: exit %d: %s", code, out.String())
	}
}
