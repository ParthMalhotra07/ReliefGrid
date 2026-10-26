package fsm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// openDumpFSM is a file database (WAL, a separate read pool), as on a pod,
// with n generic records kv:0..kv:n-1 applied at indexes 1..n.
func openDumpFSM(t *testing.T, n int) *fsm.FSM {
	t.Helper()
	f, err := fsm.Open(context.Background(), "file:"+filepath.Join(t.TempDir(), "fsm-0.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	for i := range n {
		applyKV(t, f, i)
	}
	return f
}

// applyKV creates kv:<i> at the next index: one records row, one
// operation_results row and the meta applied_index, in one transaction.
func applyKV(t *testing.T, f *fsm.FSM, i int) {
	t.Helper()
	entry := mustEncode(t, store.CompareAndSet{Meta: meta(fmt.Sprintf("kv-%d", i)), Key: fmt.Sprintf("kv:%d", i), Value: json.RawMessage(`1`)})
	res, err := f.Apply(f.AppliedIndex()+1, entry)
	if err != nil {
		t.Errorf("Apply kv:%d: %v", i, err)
		return
	}
	if res.Outcome != store.OutcomeOK {
		t.Errorf("Apply kv:%d: outcome %s", i, res.Outcome)
	}
}

// dumpConsistent reports why a dump is not one committed state: every
// command wrote a records row, an operation_results row and the applied
// index together, so the three must agree.
func dumpConsistent(d map[string][]string) string {
	var applied string
	for _, line := range d["meta"] {
		if strings.HasPrefix(line, "k=applied_index | v=") {
			applied = strings.TrimPrefix(line, "k=applied_index | v=")
		}
	}
	if applied == "" {
		return "no applied_index in meta"
	}
	if got, want := fmt.Sprint(len(d["operation_results"])), applied; got != want {
		return fmt.Sprintf("%s operation results, applied_index %s", got, want)
	}
	if got, want := fmt.Sprint(len(d["records"])), applied; got != want {
		return fmt.Sprintf("%s records, applied_index %s", got, want)
	}
	return ""
}

// Dump reads every table in one read transaction: a command committed
// while it runs is either wholly in the dump or wholly absent. Here a write
// lands after records was read and before the other tables are.
func TestDumpIsOneCommittedState(t *testing.T) {
	f := openDumpFSM(t, 3)
	var once sync.Once
	fsm.SetAfterDumpTable(t, func(table string) {
		if table == "records" {
			once.Do(func() { applyKV(t, f, 3) }) // a concurrent Apply, on the write connection
		}
	})
	d, err := f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if why := dumpConsistent(d); why != "" {
		t.Fatalf("dump mixes two states: %s", why)
	}
	if len(d["records"]) != 3 {
		t.Fatalf("%d records, want the 3 committed before the dump began", len(d["records"]))
	}
	after, err := f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if why := dumpConsistent(after); why != "" || len(after["records"]) != 4 {
		t.Fatalf("next dump: %d records (%s), want 4", len(after["records"]), why)
	}
}

// Audits run under load: every dump taken while a writer keeps applying is
// one committed state.
func TestDumpUnderConcurrentWrites(t *testing.T) {
	const writes = 2000
	f := openDumpFSM(t, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= writes; i++ {
			applyKV(t, f, i)
		}
	}()
	dumps, moved := 0, 0 // dumps taken; dumps at a different applied index than the one before
	last := ""
	for running := true; running; dumps++ {
		select {
		case <-done:
			running = false
		default:
		}
		d, err := f.Dump(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if why := dumpConsistent(d); why != "" {
			t.Fatalf("dump %d under load mixes two states: %s", dumps, why)
		}
		if idx := strings.Join(d["meta"], ","); idx != last {
			moved++
			last = idx
		}
	}
	if moved < 3 {
		t.Fatalf("only %d of %d dumps saw the writer move: the test did not overlap them", moved, dumps)
	}
}
