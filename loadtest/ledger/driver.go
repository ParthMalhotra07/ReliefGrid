package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Driver is a load generator that records every write in a Ledger.
//
// Each worker owns Keys update keys, created first, and then makes Ops
// writes (or, with Ops 0, writes until Duration has passed) alternating
// between a new create-only key and the next update of one of its keys,
// pausing Interval between writes. An unknown outcome is retried with the
// same operation ID and payload up to Retries times (AGENTS.md 5); if it
// stays unknown the write goes to Ledger.Unknown and that key gets no
// further updates, since its version is no longer known. Every failed
// attempt goes to Ledger.Errors.
//
// Readers, if set, read the writers' update keys in turn with Read until
// the writers stop, each pausing ReadInterval between reads; every read goes
// to Ledger.Reads for Ledger.CheckReads. Each Probe is one more reader that
// reads only the keys it Owns with its own Read (straight from one node,
// say an isolated leader), recorded with its Name as Read.Target. Readers,
// probes and writers share one clock: nanoseconds since Run began, from
// Now's monotonic reading, so an acknowledgement and a read's start are
// ordered without clock skew. Ledger.StartedAtMs is that instant on the wall
// clock, for lining the run up with a harness's fault times.
type Driver struct {
	Store    store.Store
	Prefix   string        // key and operation ID prefix; unique per run
	Workers  int           // default 4
	Keys     int           // update keys per worker, default 4
	Ops      int           // writes per worker after the update keys are created; 0: until Duration
	Duration time.Duration // stop starting writes after this long; 0: no limit (Ops must be set)
	Interval time.Duration // pause between one worker's writes
	Retries  int           // same-operation retries of an unknown outcome, default 3
	At       int64         // Meta.At of every command, unix ms
	// Now stamps entries and attempt errors; default time.Now. It never
	// reaches a command: Meta.At is At.
	Now func() time.Time

	Readers      int           // concurrent readers; 0: none
	ReadInterval time.Duration // pause between one reader's reads
	Read         ReadFunc      // required when Readers > 0
	// ReadConsistency is the mode Read asks for, recorded in the ledger.
	ReadConsistency string
	Probes          []Probe // direct readers, beside Readers

	deadline time.Time // set by Run from Duration
	base     time.Time // Run's start: the zero of every *Ns instant
}

// ReadFunc reads one key and says how the read was served.
type ReadFunc func(ctx context.Context, key string) (store.Record, string, error)

// Probe is a reader of its own: Read is called for each update key Owns
// accepts (all, if Owns is nil), recorded with Target Name.
type Probe struct {
	Name string
	Owns func(key string) bool
	Read ReadFunc
}

// Run drives the load until every worker is done or ctx ends, and returns
// the ledger so far.
func (d Driver) Run(ctx context.Context) (*Ledger, error) {
	if d.Ops <= 0 && d.Duration <= 0 {
		return nil, fmt.Errorf("ledger: driver needs Ops or Duration")
	}
	if d.Readers < 0 || (d.Readers > 0 && d.Read == nil) {
		return nil, fmt.Errorf("ledger: %d readers need Readers >= 0 and a Read function", d.Readers)
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	d.base = d.Now()
	if d.Workers == 0 {
		d.Workers = 4
	}
	if d.Keys == 0 {
		d.Keys = 4
	}
	probeKeys := make([][]string, len(d.Probes))
	for i, p := range d.Probes {
		if p.Name == "" || p.Read == nil {
			return nil, fmt.Errorf("ledger: probe %d needs a Name and a Read function", i)
		}
		for _, key := range d.updateKeys() {
			if p.Owns == nil || p.Owns(key) {
				probeKeys[i] = append(probeKeys[i], key)
			}
		}
		if len(probeKeys[i]) == 0 {
			return nil, fmt.Errorf("ledger: probe %s owns none of the %d update keys", p.Name, d.Workers*d.Keys)
		}
	}
	if d.Duration > 0 {
		d.deadline = d.Now().Add(d.Duration)
	}
	if d.Retries == 0 {
		d.Retries = 3
	}
	l := &Ledger{StartedAtMs: d.base.UnixMilli()}
	if d.Readers > 0 || len(d.Probes) > 0 {
		l.ReadConsistency = d.ReadConsistency
	}
	var wg, readers sync.WaitGroup
	for w := range d.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.worker(ctx, l, w)
		}()
	}
	stop := make(chan struct{})
	keys := d.updateKeys()
	for r := range d.Readers {
		readers.Add(1)
		go func() {
			defer readers.Done()
			d.readLoop(ctx, l, keys, r, d.Readers, "", d.Read, stop)
		}()
	}
	for i, p := range d.Probes {
		readers.Add(1)
		go func() {
			defer readers.Done()
			d.readLoop(ctx, l, probeKeys[i], 0, 1, p.Name, p.Read, stop)
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	return l, ctx.Err()
}

// since is now on the run's monotonic clock, in nanoseconds.
func (d Driver) since() int64 { return int64(d.Now().Sub(d.base)) }

// updateKeys are every worker's update keys, in worker then key order.
func (d Driver) updateKeys() []string {
	keys := make([]string, 0, d.Workers*d.Keys)
	for w := range d.Workers {
		for k := range d.Keys {
			keys = append(keys, fmt.Sprintf("%s-w%d-k%d", d.Prefix, w, k))
		}
	}
	return keys
}

// readLoop reads keys with read, starting at index first and stepping by
// step, recording each read with target, until stop closes or ctx ends.
func (d Driver) readLoop(ctx context.Context, l *Ledger, keys []string, first, step int, target string, read ReadFunc, stop <-chan struct{}) {
	for i := first; ; i += step {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		key := keys[i%len(keys)]
		rd := Read{Key: key, Target: target, StartNs: d.since()}
		rec, served, err := read(ctx, key)
		rd.EndNs, rd.Served = d.since(), served
		switch {
		case errors.Is(err, store.ErrNotFound):
			rd.NotFound = true
		case err != nil:
			rd.Error = err.Error()
		case rec.Deleted:
			rd.NotFound, rd.Version = true, rec.Version
		default:
			rd.Version, rd.PayloadHash = rec.Version, PayloadHash(rec.Value)
		}
		l.RecordRead(rd)
		if d.ReadInterval > 0 {
			t := time.NewTimer(d.ReadInterval)
			select {
			case <-t.C:
			case <-stop:
			case <-ctx.Done():
			}
			t.Stop()
		}
	}
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
	for i := 0; d.Ops <= 0 || i < d.Ops; i++ {
		if ctx.Err() != nil || d.done() {
			return
		}
		if i > 0 && !d.pause(ctx) {
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

// done reports whether Duration has passed.
func (d Driver) done() bool { return !d.deadline.IsZero() && !d.Now().Before(d.deadline) }

// pause waits Interval; false if ctx ended first.
func (d Driver) pause(ctx context.Context) bool {
	if d.Interval <= 0 {
		return true
	}
	t := time.NewTimer(d.Interval)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
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
	e.StartNs = wk.d.since()
	for attempt := 0; ; attempt++ {
		var res store.Result
		res, err = call(ctx)
		if err == nil {
			e.At, e.AckNs = wk.d.Now().UnixMilli(), wk.d.since()
			wk.l.Record(e, res, nil)
			return res, res.Outcome == store.OutcomeOK
		}
		wk.l.RecordError(AttemptError{At: wk.d.Now().UnixMilli(), OperationID: e.OperationID, Key: e.Key, Attempt: attempt, Error: err.Error()})
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
	e.At = wk.d.Now().UnixMilli()
	wk.l.Record(e, store.Result{}, err)
	return store.Result{}, false
}
