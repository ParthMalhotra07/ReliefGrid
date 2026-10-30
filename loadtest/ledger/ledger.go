// Package ledger records acknowledged writes so that a later read can prove
// none was lost (v3.1 §26, "Acknowledged-write ledger").
//
// The Driver writes create-only immutable keys and per-key update sequences
// through store.Store. Every committed result goes into the ledger with its
// operation ID, key and payload hash; an unknown outcome is kept apart and
// resolved later by operation lookup, never counted as acknowledged or as
// rejected. Verify then reads every key: create-only keys must hold exactly
// their payload, update keys the payload and version of their last
// acknowledged write, and every acknowledged operation's stored result must
// still be the one that was acknowledged. A higher version alone proves
// nothing: a wrong value at a higher version is a loss.
package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Op is the kind of write an entry records.
type Op string

const (
	OpCreate Op = "create" // CreateEntity of a KV key
	OpUpdate Op = "update" // CompareAndSet of a KV key the writer owns
)

// Entry is one write.
type Entry struct {
	OperationID string `json:"operationId"`
	Key         string `json:"key"`
	Op          Op     `json:"op"`
	// Seq orders a key's writes: 0 for its create, then 1, 2, ... for its
	// updates. One writer owns a key, so the order is the commit order.
	Seq         int    `json:"seq"`
	PayloadHash string `json:"payloadHash"`
	// Result is the committed result: as acknowledged, or as found by
	// Resolve. It is nil for an entry whose outcome is still unknown.
	Result *store.Result `json:"result,omitempty"`
	// Error is the last error of an unknown outcome.
	Error string `json:"error,omitempty"`
	// At is when the outcome was known (unix ms, the driver's clock): the
	// acknowledgement, the rejection, or the last failed attempt.
	At int64 `json:"at"`
}

// AttemptError is one failed attempt of a write, kept even when a retry
// later commits it: temporary errors are recorded, not hidden behind the
// final result (v3.1 §26).
type AttemptError struct {
	At          int64  `json:"at"` // unix ms
	OperationID string `json:"operationId"`
	Key         string `json:"key"`
	Attempt     int    `json:"attempt"` // 0 for the first call
	Error       string `json:"error"`
}

// Ledger holds every write of a run, split by what is known of it.
type Ledger struct {
	mu sync.Mutex
	// Acked are writes committed with outcome OK: acknowledged, or unknown
	// and later found by Resolve.
	Acked []Entry `json:"acked"`
	// Unknown are writes whose outcome is not known.
	Unknown []Entry `json:"unknown"`
	// Rejected are writes committed with an outcome other than OK.
	Rejected []Entry `json:"rejected"`
	// NotApplied are unknown writes Resolve found never committed.
	NotApplied []Entry `json:"notApplied"`
	// Errors are every failed attempt, in the order they happened.
	Errors []AttemptError `json:"errors"`
}

// PayloadHash is the hex SHA-256 of v in compact JSON form, so that it
// matches whatever whitespace the store returns.
func PayloadHash(v json.RawMessage) string {
	var buf bytes.Buffer
	b := []byte(v)
	if json.Compact(&buf, v) == nil {
		b = buf.Bytes()
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Record files e with the outcome of its call: OK goes to Acked, another
// committed outcome to Rejected, an error to Unknown.
func (l *Ledger) Record(e Entry, res store.Result, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		e.Result, e.Error = nil, err.Error()
		l.Unknown = append(l.Unknown, e)
		return
	}
	e.Result, e.Error = &res, ""
	if res.Outcome == store.OutcomeOK {
		l.Acked = append(l.Acked, e)
	} else {
		l.Rejected = append(l.Rejected, e)
	}
}

// RecordError keeps one failed attempt.
func (l *Ledger) RecordError(e AttemptError) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Errors = append(l.Errors, e)
}

// MaxStatErrors bounds the attempt errors Stats lists; ErrorCount has them all.
const MaxStatErrors = 200

// Stats is the ledger as written, before Resolve: what a run's report shows.
type Stats struct {
	Acked    int `json:"acked"`
	Unknown  int `json:"unknown"`
	Rejected int `json:"rejected"`
	// AckedInWindow counts acknowledged writes with At in [from, to].
	AckedInWindow int            `json:"ackedInWindow"`
	ErrorCount    int            `json:"errorCount"`
	Errors        []AttemptError `json:"errors"` // the first MaxStatErrors
}

// Stats counts l, and the acknowledgements between from and to (unix ms,
// inclusive).
func (l *Ledger) Stats(from, to int64) Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := Stats{Acked: len(l.Acked), Unknown: len(l.Unknown), Rejected: len(l.Rejected), ErrorCount: len(l.Errors),
		Errors: slices.Clone(l.Errors[:min(len(l.Errors), MaxStatErrors)])}
	if st.Errors == nil {
		st.Errors = []AttemptError{}
	}
	for _, e := range l.Acked {
		if e.At >= from && e.At <= to {
			st.AckedInWindow++
		}
	}
	return st
}

// Save writes l as JSON.
func (l *Ledger) Save(w io.Writer) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	if err := enc.Encode(l); err != nil {
		return fmt.Errorf("ledger: save: %w", err)
	}
	return nil
}

// Load reads a ledger written by Save.
func Load(r io.Reader) (*Ledger, error) {
	l := &Ledger{}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(l); err != nil {
		return nil, fmt.Errorf("ledger: load: %w", err)
	}
	return l, nil
}

// Resolution counts what Resolve learned.
type Resolution struct {
	Resolved     int `json:"resolved"`     // found committed; moved to Acked or Rejected
	NotApplied   int `json:"notApplied"`   // not found: never committed
	StillUnknown int `json:"stillUnknown"` // the lookup itself failed
}

// Resolve looks up every unknown write by operation ID. Run it only after
// writers have stopped and retries have drained: a "not found" means the
// write never committed only once nothing can still commit it. A failed
// lookup leaves the write unknown and is returned joined with the others.
func (l *Ledger) Resolve(ctx context.Context, s store.Store) (Resolution, error) {
	l.mu.Lock()
	unknown := l.Unknown
	l.Unknown = nil
	l.mu.Unlock()

	var res Resolution
	var errs []error
	for _, e := range unknown {
		got, err := s.Operation(ctx, e.Key, e.OperationID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			res.NotApplied++
			l.mu.Lock()
			l.NotApplied = append(l.NotApplied, e)
			l.mu.Unlock()
		case err != nil:
			res.StillUnknown++
			errs = append(errs, fmt.Errorf("ledger: look up %s: %w", e.OperationID, err))
			l.mu.Lock()
			l.Unknown = append(l.Unknown, e)
			l.mu.Unlock()
		default:
			res.Resolved++
			l.Record(e, got, nil)
		}
	}
	return res, errors.Join(errs...)
}

// Loss is an acknowledged write the store no longer shows.
type Loss struct {
	Entry  Entry  `json:"entry"`
	Reason string `json:"reason"`
}

// Verdict is the result of Verify.
type Verdict struct {
	Checked int    `json:"checked"` // acknowledged writes checked
	Keys    int    `json:"keys"`    // keys read
	Lost    []Loss `json:"lost"`
	// Unknown is the number of writes still unknown; a key whose latest
	// write is unknown may hold that write or the one before it.
	Unknown int `json:"unknown"`
}

// OK reports whether no acknowledged write was lost.
func (v Verdict) OK() bool { return len(v.Lost) == 0 }

// Verify reads every key and operation in the ledger. Each acknowledged
// operation's stored result must match what was acknowledged, and each
// key must hold the payload and version of its last acknowledged write (or
// of a later write whose outcome is still unknown). A read that fails,
// other than not found, is an error: no verdict.
func (l *Ledger) Verify(ctx context.Context, s store.Store) (Verdict, error) {
	l.mu.Lock()
	acked := slices.Clone(l.Acked)
	unknown := slices.Clone(l.Unknown)
	l.mu.Unlock()

	v := Verdict{Unknown: len(unknown)}
	reasons := map[string][]string{} // operation ID -> why it is lost
	lose := func(e Entry, format string, args ...any) {
		reasons[e.OperationID] = append(reasons[e.OperationID], fmt.Sprintf(format, args...))
	}

	last := map[string]Entry{} // key -> its acknowledged write with the highest Seq
	var keys []string
	for _, e := range acked {
		v.Checked++
		got, err := s.Operation(ctx, e.Key, e.OperationID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			lose(e, "no stored result for the operation")
		case err != nil:
			return Verdict{}, fmt.Errorf("ledger: operation %s: %w", e.OperationID, err)
		case got.Outcome != e.Result.Outcome || got.Version != e.Result.Version:
			lose(e, "stored result %s v%d, acknowledged %s v%d", got.Outcome, got.Version, e.Result.Outcome, e.Result.Version)
		}
		cur, seen := last[e.Key]
		if !seen {
			keys = append(keys, e.Key)
		}
		if !seen || e.Seq > cur.Seq {
			last[e.Key] = e
		}
	}
	later := map[string][]Entry{} // key -> unknown writes after its last acknowledged one
	for _, e := range unknown {
		if cur, ok := last[e.Key]; ok && e.Seq > cur.Seq {
			later[e.Key] = append(later[e.Key], e)
		}
	}

	for _, key := range keys {
		v.Keys++
		want := last[key]
		rec, err := s.ReadRecord(ctx, key)
		switch {
		case errors.Is(err, store.ErrNotFound):
			lose(want, "key %s is missing", key)
			continue
		case err != nil:
			return Verdict{}, fmt.Errorf("ledger: read %s: %w", key, err)
		case rec.Deleted:
			lose(want, "key %s is deleted at v%d", key, rec.Version)
			continue
		}
		hash := PayloadHash(rec.Value)
		if hash == want.PayloadHash && rec.Version == want.Result.Version {
			continue
		}
		if slices.ContainsFunc(later[key], func(e Entry) bool { return e.PayloadHash == hash }) {
			continue // a later write whose outcome is unknown committed
		}
		lose(want, "key %s holds v%d with payload %.12s, last acknowledged write is seq %d at v%d with payload %.12s",
			key, rec.Version, hash, want.Seq, want.Result.Version, want.PayloadHash)
	}

	for _, e := range acked {
		if r, ok := reasons[e.OperationID]; ok {
			v.Lost = append(v.Lost, Loss{Entry: e, Reason: strings.Join(r, "; ")})
			delete(reasons, e.OperationID)
		}
	}
	return v, nil
}
