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
	"sync"
	"sync/atomic"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Store implements store.Store with one internal/fsm FSM per shard.
type Store struct {
	mu          sync.Mutex // serialises Apply across all shards
	shards      []*shard
	route       func(key string) int
	ringVersion uint64
}

type shard struct {
	f     *fsm.FSM
	index uint64 // last log index handed to Apply; guarded by Store.mu
}

var _ store.Store = (*Store)(nil)

type Option func(*config)

type config struct {
	shards      int
	route       func(key string) int
	ringVersion uint64
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
	s := &Store{route: cfg.route, ringVersion: cfg.ringVersion}
	id := storeSeq.Add(1)
	for i := range cfg.shards {
		f, err := fsm.Open(ctx, fmt.Sprintf("file:memstore-%d-shard%d?mode=memory", id, i))
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("memstore: shard %d: %w", i, err)
		}
		s.shards = append(s.shards, &shard{f: f})
	}
	return s, nil
}

func (s *Store) Close() error {
	var errs []error
	for _, sh := range s.shards {
		errs = append(errs, sh.f.Close())
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

// apply encodes body and applies it as the next entry of shard i. An
// infrastructure failure is reported as an unknown outcome, as a real
// replica crashing mid-apply would be.
func (s *Store) apply(ctx context.Context, i int, body any) (store.Result, error) {
	if err := ctx.Err(); err != nil {
		return store.Result{}, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, err)
	}
	entry, err := fsm.Encode(body)
	if err != nil {
		return store.Result{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh := s.shards[i]
	sh.index++
	res, err := sh.f.Apply(sh.index, entry)
	if err != nil {
		return store.Result{}, fmt.Errorf("%w: shard %d: %w", store.ErrUnknownOutcome, i, err)
	}
	res.Shard = i
	return res, nil
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
	return s.shards[i].f.ReadRecord(ctx, key)
}

func (s *Store) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	i, err := s.shardFor(resourceKey)
	if err != nil {
		return store.Closure{}, err
	}
	return s.shards[i].f.ReadClosure(ctx, resourceKey, attemptID)
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
	for i, sh := range s.shards {
		if _, listed := after[i]; after != nil && !listed {
			continue // exhausted
		}
		var recs []store.Record
		err := errShardDown
		if !down[i] {
			recs, err = sh.f.Scan(ctx, q, after[i])
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
		out.Positions = append(out.Positions, store.ShardPosition{Shard: i, AppliedIndex: sh.f.AppliedIndex(), HasMore: hasMore})
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
	return s.shards[q.Shard].f.PendingOutbox(ctx, q.Limit)
}

func (s *Store) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	i, err := s.shardFor(key)
	if err != nil {
		return store.Result{}, err
	}
	res, err := s.shards[i].f.Operation(ctx, operationID)
	if err != nil {
		return store.Result{}, err
	}
	res.Shard = i
	return res, nil
}
