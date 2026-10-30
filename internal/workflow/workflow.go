// Package workflow implements intent-first matching over store.Store. Neither
// candidate hints nor historical operation results are grants of ownership.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

var (
	ErrIncompleteScan   = errors.New("workflow: incomplete candidate scan")
	ErrConcurrentChange = errors.New("workflow: request or candidates changed; reread before retry")
	ErrNeedsRecovery    = errors.New("workflow: cleanup requires recovery")
	ErrOwnershipUnknown = errors.New("workflow: current ownership not established")
	ErrWorkBudget       = errors.New("workflow: matching work budget exhausted")
)

// Scanner may serve stale candidate hints. Store must serve authoritative reads
// and scans. Without a separate scanner, the authoritative store supplies both.
type Scanner interface {
	Scan(context.Context, store.ScanQuery) (store.ScanResult, error)
}

// Config keeps time, IDs and selection reproducible in workflow tests. NewID
// must return globally unique UUIDs in production; calls are serialized. Call
// and work timeouts bound even a caller that supplies no deadline.
type Config struct {
	Store            store.Store
	CandidateScanner Scanner
	Now              func() time.Time
	NewID            func() string
	Seed             uint64
	ScanLimit        int
	CallTimeout      time.Duration
	WorkTimeout      time.Duration
}

type Matcher struct {
	cfg  Config
	idMu sync.Mutex
}

// Result describes committed progress, not permission to acknowledge an error.
// NO_CANDIDATE includes a durable retry handoff. DEFERRED is an existing future
// schedule. Cleanup states and unresolved operations return an error instead.
type Result struct {
	Outcome       string
	ResourceID    string
	NextAttemptAt int64
}

func New(cfg Config) (*Matcher, error) {
	if cfg.Store == nil || cfg.Store.Shards() < 1 {
		return nil, fmt.Errorf("workflow: authoritative store required")
	}
	if cfg.ScanLimit < 1 || cfg.ScanLimit > store.MaxScanLimit {
		return nil, fmt.Errorf("workflow: scan limit must be 1..%d", store.MaxScanLimit)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = uuid.NewString
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = 2 * time.Second
	}
	if cfg.WorkTimeout == 0 {
		cfg.WorkTimeout = 10 * time.Second
	}
	if cfg.CallTimeout < 0 || cfg.WorkTimeout < 0 {
		return nil, fmt.Errorf("workflow: timeouts must be positive")
	}
	return &Matcher{cfg: cfg}, nil
}

func remote[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(callCtx)
}

func (m *Matcher) read(ctx context.Context, key string) (store.Record, error) {
	r, err := remote(ctx, m.cfg.CallTimeout, func(ctx context.Context) (store.Record, error) {
		return m.cfg.Store.ReadRecord(ctx, key)
	})
	if err != nil {
		return r, fmt.Errorf("workflow read %s: %w", key, err)
	}
	if r.Deleted {
		return r, fmt.Errorf("workflow read %s: %w", key, store.ErrNotFound)
	}
	return r, nil
}

// mutate retries the exact closure-captured command once after an error. It
// never converts a transport error into a committed rejection.
func (m *Matcher) mutate(ctx context.Context, fn func(context.Context) (store.Result, error)) (store.Result, error) {
	r, err := remote(ctx, m.cfg.CallTimeout, fn)
	if err != nil && ctx.Err() == nil {
		r, err = remote(ctx, m.cfg.CallTimeout, fn)
	}
	if err != nil {
		return r, fmt.Errorf("workflow mutation: %w: %w", store.ErrUnknownOutcome, err)
	}
	return r, nil
}

func (m *Matcher) id() string {
	m.idMu.Lock()
	defer m.idMu.Unlock()
	return m.cfg.NewID()
}

// Match performs bounded work for one request. Reconciliation and RabbitMQ ack
// decisions belong to their later goals; callers must not ack an error here.
func (m *Matcher) Match(ctx context.Context, requestID string) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(requestID))
	if err != nil {
		return Result{}, err
	}
	if rec.Request == nil {
		return Result{}, fmt.Errorf("workflow: %s is not a request", rec.Key)
	}
	if rec.Request.Status != store.RequestMatching && rec.Request.Status != store.RequestPending && rec.Request.Status != store.RequestWaiting {
		return m.obsolete(rec)
	}
	if rec.Request.Status == store.RequestWaiting && rec.Request.NextAttemptAt > m.cfg.Now().UnixMilli() {
		return m.obsolete(rec)
	}
	if rec.Request.Status != store.RequestMatching {
		var done *Result
		rec, done, err = m.propose(ctx, rec)
		if err != nil {
			return Result{}, err
		}
		if done != nil {
			return *done, nil
		}
	}
	for attempts := 0; attempts < 8; attempts++ {
		closed, done, err := m.resolve(ctx, rec)
		if err != nil {
			return Result{}, err
		}
		if done != nil {
			return *done, nil
		}
		if !closed {
			return Result{}, ErrOwnershipUnknown
		}
		// Only resolve can authorize this MATCHING(a) -> MATCHING(b)/WAITING.
		rec, done, err = m.propose(ctx, rec)
		if err != nil {
			return Result{}, err
		}
		if done != nil {
			return *done, nil
		}
	}
	return Result{}, ErrWorkBudget
}

func (m *Matcher) obsolete(rec store.Record) (Result, error) {
	if rec.Request == nil {
		return Result{}, ErrConcurrentChange
	}
	switch rec.Request.Status {
	case store.RequestMatched, store.RequestInProgress:
		return Result{Outcome: "MATCHED", ResourceID: rec.Request.AssignedResourceID}, nil
	case store.RequestCancelled, store.RequestFulfilled:
		return Result{Outcome: "IGNORED"}, nil
	case store.RequestWaiting:
		if rec.Request.NextAttemptAt > m.cfg.Now().UnixMilli() {
			return Result{Outcome: "DEFERRED", NextAttemptAt: rec.Request.NextAttemptAt}, nil
		}
	case store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling:
		return Result{}, ErrNeedsRecovery
	}
	return Result{}, ErrConcurrentChange
}

func sameIntent(a, b store.Record) bool {
	return b.Request != nil && b.Request.Status == store.RequestMatching && b.Version == a.Version &&
		a.Request.CurrentAttempt != nil && b.Request.CurrentAttempt != nil && *a.Request.CurrentAttempt == *b.Request.CurrentAttempt
}

func (m *Matcher) closure(ctx context.Context, rec store.Record) (bool, error) {
	a := rec.Request.CurrentAttempt
	cl, err := remote(ctx, m.cfg.CallTimeout, func(ctx context.Context) (store.Closure, error) {
		return m.cfg.Store.ReadClosure(ctx, store.ResourceKey(a.ResourceID), a.AttemptID)
	})
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("workflow closure: %w", err)
	}
	if cl.RequestID != rec.Request.ID || cl.AttemptID != a.AttemptID || cl.ResourceKey != store.ResourceKey(a.ResourceID) || cl.Fingerprint != a.Fingerprint {
		return false, fmt.Errorf("workflow: closure is bound to different immutable inputs")
	}
	return true, nil
}

func (m *Matcher) resolve(ctx context.Context, rec store.Record) (bool, *Result, error) {
	a := rec.Request.CurrentAttempt
	if a == nil || a.AttemptID == "" || a.ResourceID == "" || a.Fingerprint == "" {
		return false, nil, fmt.Errorf("workflow: MATCHING requires a complete immutable intent")
	}
	closed, err := m.closure(ctx, rec)
	if err != nil {
		return false, nil, err
	}
	if closed {
		return true, nil, nil
	}
	at, err := claimTime(rec)
	if err != nil {
		return false, nil, err
	}
	cmd := store.TryClaim{
		Meta:        store.Meta{OperationID: operationID("claim", rec.Key, a.AttemptID), At: at},
		ResourceKey: store.ResourceKey(a.ResourceID), RequestID: rec.Request.ID, Attempt: *a, Type: rec.Request.Type,
	}
	result, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) {
		return m.cfg.Store.TryClaim(ctx, cmd)
	})
	if err != nil {
		return false, nil, err
	}
	current, err := m.read(ctx, rec.Key)
	if err != nil {
		return false, nil, err
	}
	if !sameIntent(rec, current) {
		out, err := m.obsolete(current)
		return false, &out, err
	}
	switch result.Outcome {
	case store.ClaimConflictClosed, store.ClaimRejectedClosed, store.ClaimClosed:
		return true, nil, nil
	case store.ClaimSuccess:
		// Replayed SUCCESS is history. Establish current reciprocal ownership
		// before attempting the request-shard confirmation CAS.
		resource, err := m.read(ctx, cmd.ResourceKey)
		if err != nil {
			return false, nil, err
		}
		if resource.Resource == nil || resource.Resource.Status != store.ResourceAssigned || resource.Resource.ActiveClaim == nil ||
			*resource.Resource.ActiveClaim != (store.Claim{RequestID: cmd.RequestID, AttemptID: a.AttemptID, Fingerprint: a.Fingerprint}) {
			closed, err := m.closure(ctx, rec)
			if err != nil {
				return false, nil, err
			}
			if !closed {
				return false, nil, ErrOwnershipUnknown
			}
			return true, nil, nil
		}
		out, err := m.confirm(ctx, rec, at)
		return false, &out, err
	default:
		return false, nil, fmt.Errorf("workflow claim rejected with %s: %s", result.Outcome, result.Reason)
	}
}

func (m *Matcher) applyTransition(ctx context.Context, rec store.Record, cmd store.TransitionRequest) (store.Record, error) {
	r, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) {
		return m.cfg.Store.TransitionRequest(ctx, cmd)
	})
	if err != nil {
		return store.Record{}, err
	}
	if r.Outcome != store.OutcomeOK && r.Outcome != store.OutcomeVersionConflict {
		return store.Record{}, fmt.Errorf("workflow transition rejected with %s: %s", r.Outcome, r.Reason)
	}
	current, err := m.read(ctx, rec.Key)
	if err != nil {
		return current, err
	}
	if r.Outcome == store.OutcomeVersionConflict {
		return current, ErrConcurrentChange
	}
	return current, nil
}

func transition(rec store.Record, to store.RequestStatus, at int64) store.TransitionRequest {
	cmd := store.TransitionRequest{
		Meta: store.Meta{At: at}, Key: rec.Key, ExpectedVersion: rec.Version,
		ExpectedStatus: rec.Request.Status, To: to,
	}
	if rec.Request.CurrentAttempt != nil {
		cmd.ExpectedAttemptID = rec.Request.CurrentAttempt.AttemptID
	}
	return cmd
}

func (m *Matcher) propose(ctx context.Context, rec store.Record) (store.Record, *Result, error) {
	at := m.cfg.Now().UnixMilli()
	chosen, err := m.choose(ctx, rec, at)
	if err != nil {
		return rec, nil, err
	}
	if chosen == nil {
		cmd := transition(rec, store.RequestWaiting, at)
		cmd.NextAttemptAt = at + 5000
		cmd.LastError = "NO_CANDIDATE"
		cmd.OperationID = transitionID(cmd)
		cmd.Outbox, err = events(rec, cmd, "request.retry", nil)
		if err != nil {
			return rec, nil, err
		}
		current, err := m.applyTransition(ctx, rec, cmd)
		if err != nil {
			return current, nil, err
		}
		if current.Request == nil || current.Request.Status != store.RequestWaiting || current.Version != rec.Version+1 {
			out, err := m.obsolete(current)
			return current, &out, err
		}
		return current, &Result{Outcome: "NO_CANDIDATE", NextAttemptAt: cmd.NextAttemptAt}, nil
	}
	a, err := m.attempt(rec, *chosen, at)
	if err != nil {
		return rec, nil, err
	}
	cmd := transition(rec, store.RequestMatching, at)
	cmd.Attempt = &a
	cmd.OperationID = transitionID(cmd)
	cmd.Outbox, err = events(rec, cmd, "match.proposed", &a)
	if err != nil {
		return rec, nil, err
	}
	current, err := m.applyTransition(ctx, rec, cmd)
	if err != nil {
		return current, nil, err
	}
	if current.Request == nil || current.Request.Status != store.RequestMatching || current.Request.CurrentAttempt == nil ||
		*current.Request.CurrentAttempt != a || current.Version != rec.Version+1 {
		out, err := m.obsolete(current)
		return current, &out, err
	}
	return current, nil, nil
}

func (m *Matcher) confirm(ctx context.Context, rec store.Record, at int64) (Result, error) {
	cmd := transition(rec, store.RequestMatched, at)
	cmd.OperationID = transitionID(cmd)
	var err error
	cmd.Outbox, err = events(rec, cmd, "match.completed", rec.Request.CurrentAttempt)
	if err != nil {
		return Result{}, err
	}
	current, err := m.applyTransition(ctx, rec, cmd)
	if err != nil {
		return Result{}, err
	}
	return m.obsolete(current)
}
