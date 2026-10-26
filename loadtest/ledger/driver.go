package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Driver is a load generator that records every write in a Ledger.
//
// Each worker owns Keys update keys, created first, and then makes Ops
// writes alternating between a new create-only key and the next update of
// one of its keys. An unknown outcome is retried with the same operation ID
// and payload up to Retries times (AGENTS.md 5); if it stays unknown the
// write goes to Ledger.Unknown and that key gets no further updates, since
// its version is no longer known.
type Driver struct {
	Store   store.Store
	Prefix  string // key and operation ID prefix; unique per run
	Workers int    // default 4
	Keys    int    // update keys per worker, default 4
	Ops     int    // writes per worker after the update keys are created
	Retries int    // same-operation retries of an unknown outcome, default 3
	At      int64  // Meta.At of every command, unix ms
}

// Run drives the load until every worker is done or ctx ends, and returns
// the ledger so far.
func (d Driver) Run(ctx context.Context) (*Ledger, error) {
	if d.Workers == 0 {
		d.Workers = 4
	}
	if d.Keys == 0 {
		d.Keys = 4
	}
	if d.Retries == 0 {
		d.Retries = 3
	}
	l := &Ledger{}
	var wg sync.WaitGroup
	for w := range d.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.worker(ctx, l, w)
		}()
	}
	wg.Wait()
	return l, ctx.Err()
}

type worker struct {
	d   Driver
	l   *Ledger
	w   int
	ops int
}

func (d Driver) worker(ctx context.Context, l *Ledger, w int) {
	wk := &worker{d: d, l: l, w: w}
	version := make([]uint64, d.Keys) // 0: the key is frozen
	seq := make([]int, d.Keys)
	for k := range d.Keys {
		key := fmt.Sprintf("%s-w%d-k%d", d.Prefix, w, k)
		if res, ok := wk.create(ctx, key, fmt.Sprintf(`{"w":%d,"k":%d,"seq":0}`, w, k)); ok {
			version[k] = res.Version
		}
	}
	for i := range d.Ops {
		if ctx.Err() != nil {
			return
		}
		if i%2 == 0 {
			wk.create(ctx, fmt.Sprintf("%s-w%d-c%d", d.Prefix, w, i/2), fmt.Sprintf(`{"w":%d,"c":%d}`, w, i/2))
			continue
		}
		k := (i / 2) % d.Keys
		if version[k] == 0 {
			continue // frozen: its last write is unknown or was refused
		}
		seq[k]++
		key := fmt.Sprintf("%s-w%d-k%d", d.Prefix, w, k)
		value := json.RawMessage(fmt.Sprintf(`{"w":%d,"k":%d,"seq":%d}`, w, k, seq[k]))
		cmd := store.CompareAndSet{Meta: wk.meta(), Key: key, ExpectedVersion: version[k], Value: value}
		res, ok := wk.write(ctx, Entry{OperationID: cmd.OperationID, Key: key, Op: OpUpdate, Seq: seq[k], PayloadHash: PayloadHash(value)},
			func(ctx context.Context) (store.Result, error) { return d.Store.CompareAndSet(ctx, cmd) })
		version[k] = 0
		if ok {
			version[k] = res.Version
		}
	}
}

func (wk *worker) meta() store.Meta {
	wk.ops++
	return store.Meta{OperationID: fmt.Sprintf("%s-w%d-op%d", wk.d.Prefix, wk.w, wk.ops), At: wk.d.At}
}

func (wk *worker) create(ctx context.Context, key, value string) (store.Result, bool) {
	cmd := store.CreateEntity{Meta: wk.meta(), Key: key, Value: json.RawMessage(value)}
	return wk.write(ctx, Entry{OperationID: cmd.OperationID, Key: key, Op: OpCreate, PayloadHash: PayloadHash(cmd.Value)},
		func(ctx context.Context) (store.Result, error) { return wk.d.Store.CreateEntity(ctx, cmd) })
}

// write makes one call, retrying the same command while its outcome is
// unknown, records it, and reports whether it committed OK.
func (wk *worker) write(ctx context.Context, e Entry, call func(context.Context) (store.Result, error)) (store.Result, bool) {
	var err error
	for attempt := 0; ; attempt++ {
		var res store.Result
		res, err = call(ctx)
		if err == nil {
			wk.l.Record(e, res, nil)
			return res, res.Outcome == store.OutcomeOK
		}
		if attempt == wk.d.Retries || ctx.Err() != nil {
			break
		}
		t := time.NewTimer(time.Duration(attempt+1) * 20 * time.Millisecond)
		select {
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
	wk.l.Record(e, store.Result{}, err)
	return store.Result{}, false
}
