// Package memstore is the in-memory store.Store (M8): the real internal/fsm
// rules on in-memory SQLite, one FSM per shard, commands applied serially and
// no Raft. Wrap it with WithFaults to inject lost responses, delays and
// conflicting writes.
package memstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"sync/atomic"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// Store implements store.Store with one internal/fsm FSM per shard.
type Store struct {
	shards        []*shard
	route         func(key string) int
	ringVersion   uint64
	checkpointDir string // "" unless WithCheckpointDir
	workDir       string // live and staged databases after a Restart
	id            int64
}

// shard is one FSM and its log. Commands are appended (given the next
// index) and then applied strictly in index order, so with no seam armed
// each command applies as soon as it is appended, as before.
type shard struct {
	mu   sync.RWMutex // f, down and the log; Apply and swaps hold it for writing
	turn *sync.Cond   // on &mu: signalled whenever applied or gen moves
	f    *fsm.FSM

	index   uint64 // last log index handed out
	applied uint64 // last index applied
	gen     uint64 // bumped by Crash: waiters from an older life give up
	down    bool

	// With WithCheckpointDir: the entries after the last complete
	// checkpoint, which Restart replays.
	log []logEntry

	// Armed seams (seams.go).
	barrier      *testseam.Barrier
	ckptPause    *testseam.Hold
	restorePause *testseam.Hold
	paused       []*testseam.Hold // holds Crash must abort
	ckptSeq      uint64
}

type logEntry struct {
	index uint64
	entry []byte
}

var _ store.Store = (*Store)(nil)

type Option func(*config)

type config struct {
	shards        int
	route         func(key string) int
	ringVersion   uint64
	checkpointDir string
}

// WithShards sets the shard count (default 3).
func WithShards(n int) Option { return func(c *config) { c.shards = n } }

// WithRouter replaces the default key-to-shard function (an FNV-1a hash until
// internal/ring lands).
func WithRouter(route func(key string) int) Option { return func(c *config) { c.route = route } }

// WithRingVersion sets the ring version scan cursors are bound to (default 1).
func WithRingVersion(v uint64) Option { return func(c *config) { c.ringVersion = v } }

var storeSeq atomic.Int64

func New(ctx context.Context, opts ...Option) (*Store, error) {
	cfg := config{shards: 3, ringVersion: 1}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.shards < 1 {
		return nil, fmt.Errorf("memstore: %d shards", cfg.shards)
	}
	if cfg.route == nil {
		n := uint32(cfg.shards)
		cfg.route = func(key string) int {
			h := fnv.New32a()
			_, _ = h.Write([]byte(key))
			return int(h.Sum32() % n)
		}
	}
	s := &Store{route: cfg.route, ringVersion: cfg.ringVersion, checkpointDir: cfg.checkpointDir, id: storeSeq.Add(1)}
	if s.checkpointDir != "" {
		if err := os.MkdirAll(s.checkpointDir, 0o750); err != nil {
			return nil, fmt.Errorf("memstore: checkpoint dir: %w", err)
		}
		work, err := os.MkdirTemp("", "memstore-work-")
		if err != nil {
			return nil, fmt.Errorf("memstore: work dir: %w", err)
		}
		s.workDir = work
	}
	for i := range cfg.shards {
		f, err := fsm.Open(ctx, fmt.Sprintf("file:memstore-%d-shard%d?mode=memory", s.id, i))
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("memstore: shard %d: %w", i, err)
		}
		sh := &shard{f: f}
		sh.turn = sync.NewCond(&sh.mu)
		s.shards = append(s.shards, sh)
	}
	return s, nil
}

func (s *Store) Close() error {
	var errs []error
	for _, sh := range s.shards {
		sh.mu.Lock()
		if sh.f != nil {
			errs = append(errs, sh.f.Close())
			sh.f = nil
		}
		sh.down = true
		sh.gen++
		sh.turn.Broadcast()
		sh.mu.Unlock()
	}
	if s.workDir != "" {
		errs = append(errs, os.RemoveAll(s.workDir))
	}
	return errors.Join(errs...)
}

func (s *Store) Shards() int { return len(s.shards) }

func (s *Store) shardFor(key string) (int, error) {
	i := s.route(key)
	if i < 0 || i >= len(s.shards) {
		return 0, fmt.Errorf("memstore: router sent %q to shard %d of %d", key, i, len(s.shards))
	}
	return i, nil
}

// apply encodes body, appends it to shard i's log and applies it once
// every earlier entry has applied. An infrastructure failure, or a crash
// of the shard between append and apply, is an unknown outcome, as a real
// replica crashing mid-apply would be: the entry stays in the log.
func (s *Store) apply(ctx context.Context, i int, body any) (store.Result, error) {
	if err := ctx.Err(); err != nil {
		return store.Result{}, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, err)
	}
	entry, err := fsm.Encode(body)
	if err != nil {
		return store.Result{}, err
	}
	sh := s.shards[i]

	// Append.
	sh.mu.Lock()
	if sh.down {
		sh.mu.Unlock()
		return store.Result{}, fmt.Errorf("memstore: shard %d: %w", i, ErrShardDown)
	}
	sh.index++
	index, gen, barrier := sh.index, sh.gen, sh.barrier
	if s.checkpointDir != "" {
		sh.log = append(sh.log, logEntry{index: index, entry: entry})
	}
	if barrier != nil {
		// Under mu, so the barrier knows the index before it can apply.
		if _, opID, err := fsm.Describe(entry); err == nil {
			barrier.Appended(index, opID)
		}
	}

	// Wait for this entry's turn.
	for sh.applied != index-1 && sh.gen == gen {
		sh.turn.Wait()
	}
	if sh.gen != gen {
		sh.mu.Unlock()
		return store.Result{}, fmt.Errorf("%w: shard %d crashed before index %d applied", store.ErrUnknownOutcome, i, index)
	}
	if barrier != nil {
		sh.mu.Unlock()
		barrier.BeforeApply(index) // may hold; later entries wait their turn
		sh.mu.Lock()
		if sh.gen != gen {
			sh.mu.Unlock()
			return store.Result{}, fmt.Errorf("%w: shard %d crashed before index %d applied", store.ErrUnknownOutcome, i, index)
		}
	}
	defer sh.mu.Unlock()
	res, err := sh.f.Apply(index, entry)
	if err != nil {
		return store.Result{}, fmt.Errorf("%w: shard %d: %w", store.ErrUnknownOutcome, i, err)
	}
	sh.applied = index
	sh.turn.Broadcast()
	res.Shard = i
	return res, nil
}

// read runs fn on shard i's FSM unless the shard is down.
func (s *Store) read(i int, fn func(f *fsm.FSM) error) error {
	sh := s.shards[i]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if sh.down {
		return fmt.Errorf("memstore: shard %d: %w", i, ErrShardDown)
	}
	return fn(sh.f)
}

func (s *Store) applyKey(ctx context.Context, key string, body any) (store.Result, error) {
	i, err := s.shardFor(key)
	if err != nil {
		return store.Result{}, err
	}
	return s.apply(ctx, i, body)
}

func (s *Store) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) AgeRequest(ctx context.Context, cmd store.AgeRequest) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	return s.applyKey(ctx, cmd.Key, cmd)
}

func (s *Store) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	return s.applyKey(ctx, cmd.ResourceKey, cmd)
}

func (s *Store) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	return s.applyKey(ctx, cmd.ResourceKey, cmd)
}

func (s *Store) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	if cmd.Shard < 0 || cmd.Shard >= len(s.shards) {
		return store.Result{Outcome: store.OutcomeInvalid, OperationID: cmd.OperationID,
			Reason: fmt.Sprintf("no shard %d", cmd.Shard)}, nil
	}
	return s.apply(ctx, cmd.Shard, cmd)
}

func (s *Store) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	i, err := s.shardFor(key)
	if err != nil {
		return store.Record{}, err
	}
	var rec store.Record
	err = s.read(i, func(f *fsm.FSM) (err error) { rec, err = f.ReadRecord(ctx, key); return err })
	return rec, err
}

func (s *Store) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	i, err := s.shardFor(resourceKey)
	if err != nil {
		return store.Closure{}, err
	}
	var c store.Closure
	err = s.read(i, func(f *fsm.FSM) (err error) { c, err = f.ReadClosure(ctx, resourceKey, attemptID); return err })
	return c, err
}

func (s *Store) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	return s.scan(ctx, q, nil)
}

// scan reads every shard q.Cursor lists (every shard on a first page)
// except those in failed, which are reported as failed so the result is
// partial rather than silently smaller, and kept in NextCursor.
func (s *Store) scan(ctx context.Context, q store.ScanQuery, failed []int) (store.ScanResult, error) {
	if q.Limit < 1 || q.Limit > store.MaxScanLimit {
		return store.ScanResult{}, fmt.Errorf("memstore: scan limit %d: must be 1 to %d records per shard", q.Limit, store.MaxScanLimit)
	}
	var after map[int]string // nil on a first page
	if q.Cursor != "" {
		cur, err := s.scanCursor(q)
		if err != nil {
			return store.ScanResult{}, err
		}
		after = cur.After
	}
	down := make(map[int]bool, len(failed))
	for _, i := range failed {
		down[i] = true
	}
	var out store.ScanResult
	next := map[int]string{}
	for i := range s.shards {
		if _, listed := after[i]; after != nil && !listed {
			continue // exhausted
		}
		var (
			recs    []store.Record
			applied uint64
		)
		err := errShardDown
		if !down[i] {
			// A crashed shard is a failed shard: the page is partial.
			err = s.read(i, func(f *fsm.FSM) (err error) {
				recs, err = f.Scan(ctx, q, after[i])
				applied = f.AppliedIndex()
				return err
			})
		}
		if err != nil {
			out.Partial = true
			out.FailedShards = append(out.FailedShards, i)
			next[i] = after[i]
			continue
		}
		out.Records = append(out.Records, recs...)
		hasMore := len(recs) == q.Limit
		if hasMore {
			next[i] = recs[len(recs)-1].Key
		}
		out.Positions = append(out.Positions, store.ShardPosition{Shard: i, AppliedIndex: applied, HasMore: hasMore})
	}
	if err := ctx.Err(); err != nil {
		return store.ScanResult{}, err
	}
	out.NextCursor = store.EncodeScanCursor(store.ScanCursor{RingVersion: s.ringVersion, Query: store.ScanQueryHash(q), After: next})
	return out, nil
}

var errShardDown = errors.New("memstore: shard failed by fault hook")

// scanCursor decodes q.Cursor and checks it belongs to q on this ring.
func (s *Store) scanCursor(q store.ScanQuery) (store.ScanCursor, error) {
	cur, err := store.DecodeScanCursor(q.Cursor)
	if err != nil {
		return store.ScanCursor{}, fmt.Errorf("memstore: %w", err)
	}
	if cur.RingVersion != s.ringVersion {
		return store.ScanCursor{}, fmt.Errorf("memstore: cursor from ring version %d, store has %d: %w", cur.RingVersion, s.ringVersion, store.ErrRingVersionMismatch)
	}
	if cur.Query != store.ScanQueryHash(q) {
		return store.ScanCursor{}, errors.New("memstore: cursor belongs to a scan with other filters or another limit")
	}
	for i := range cur.After {
		if i >= len(s.shards) {
			return store.ScanCursor{}, fmt.Errorf("memstore: cursor names shard %d of %d", i, len(s.shards))
		}
	}
	return cur, nil
}

// PendingOutbox reads strong whatever q.Consistency says and returns at once:
// q.Wait is a hint to a networked store, and nothing here commits while a
// caller waits.
func (s *Store) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	if q.Shard < 0 || q.Shard >= len(s.shards) {
		return nil, fmt.Errorf("memstore: no shard %d", q.Shard)
	}
	var out []store.OutboxEvent
	err := s.read(q.Shard, func(f *fsm.FSM) (err error) { out, err = f.PendingOutbox(ctx, q.Limit); return err })
	return out, err
}

func (s *Store) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	i, err := s.shardFor(key)
	if err != nil {
		return store.Result{}, err
	}
	var res store.Result
	if err := s.read(i, func(f *fsm.FSM) (err error) { res, err = f.Operation(ctx, operationID); return err }); err != nil {
		return store.Result{}, err
	}
	res.Shard = i
	return res, nil
}
