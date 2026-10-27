//go:build chaos

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// inject exists only in the chaos build (the tools image), so that the
// failure harness can show it catches a lost write: it really commits,
// through the coordinator, a write that overwrites an acknowledged one.

var injectConfirmed bool

func init() {
	commands["inject"] = &command{
		flags: func(fs *flag.FlagSet) {
			fs.BoolVar(&injectConfirmed, "confirm", false, "really overwrite an acknowledged write (inject)")
		},
		valid: func(stderr io.Writer) bool {
			if !injectConfirmed {
				_, _ = fmt.Fprintln(stderr, "invcheck: inject overwrites acknowledged data; pass --confirm")
			}
			return injectConfirmed
		},
		run: inject,
	}
}

// injected is what inject did, as JSON on stdout.
type injected struct {
	Kind        string `json:"kind"` // "lost-write"
	Key         string `json:"key"`
	OperationID string `json:"operationId"`
	Version     uint64 `json:"version"` // the key's version after the overwrite
}

// inject overwrites the key of the ledger's acknowledged update with the
// highest Seq (ties: the greatest key) at its current version, with a
// payload no acknowledged write produced. Exit 0 once that write commits;
// 2 if it did not, so nothing was injected.
func inject(ctx context.Context, s store.Store, path string, out io.Writer) int {
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
	var target *ledger.Entry
	for i, e := range l.Acked {
		if e.Op == ledger.OpUpdate && (target == nil || e.Seq > target.Seq || e.Seq == target.Seq && e.Key > target.Key) {
			target = &l.Acked[i]
		}
	}
	if target == nil {
		_, _ = fmt.Fprintln(out, "invcheck: inject: the ledger has no acknowledged update")
		return exitIncomplete
	}
	rec, err := s.ReadRecord(ctx, target.Key)
	if err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: inject: read %s: %v\n", target.Key, err)
		return exitIncomplete
	}
	value, err := json.Marshal(map[string]string{"injected": "lost-write", "overwrites": target.OperationID})
	if err != nil {
		_, _ = fmt.Fprintf(out, "invcheck: inject: %v\n", err)
		return exitIncomplete
	}
	cmd := store.CompareAndSet{
		Meta:            store.Meta{OperationID: fmt.Sprintf("inject-%s-v%d", target.Key, rec.Version), At: time.Now().UnixMilli()},
		Key:             target.Key,
		ExpectedVersion: rec.Version,
		Value:           value,
	}
	res, err := s.CompareAndSet(ctx, cmd)
	if err != nil || res.Outcome != store.OutcomeOK {
		_, _ = fmt.Fprintf(out, "invcheck: inject: overwrite %s: %s %v\n", target.Key, res.Outcome, err)
		return exitIncomplete
	}
	writeJSON(out, injected{Kind: "lost-write", Key: target.Key, OperationID: cmd.OperationID, Version: res.Version})
	return exitClean
}
