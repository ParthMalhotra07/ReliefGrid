// Package outbox relays committed per-shard events with at-least-once delivery.
// A historical mark result proves only that the immutable batch was recorded.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/google/uuid"
)

type Publisher interface {
	Publish(context.Context, store.OutboxEvent) error
	Close() error
}

type Config struct {
	Store            store.Store
	PublisherFactory func(context.Context) (Publisher, error)
	BatchSize        int
}

type Relay struct {
	config Config
	shards []shardState
}

type shardState struct {
	permit chan struct{}
	mark   *store.MarkOutboxPublished
}

type Report struct{ Read, Published, Marked int }

var ErrMarkRejected = errors.New("outbox: committed mark rejected")

func New(config Config) (*Relay, error) {
	if config.Store == nil || config.Store.Shards() < 1 {
		return nil, fmt.Errorf("outbox requires a sharded store")
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.BatchSize < 1 || config.BatchSize > 100 {
		return nil, fmt.Errorf("outbox batch size must be 1..100")
	}
	r := &Relay{config: config, shards: make([]shardState, config.Store.Shards())}
	for i := range r.shards {
		r.shards[i].permit = make(chan struct{}, 1)
	}
	return r, nil
}

// Step handles one shard independently. After an uncertain mark, it retries the
// exact frozen command before reading or publishing again. After process death,
// unpublished rows can be sent again; consumers must reread authoritative state.
func (r *Relay) Step(ctx context.Context, shard int, p Publisher) (Report, error) {
	var report Report
	if shard < 0 || shard >= len(r.shards) || p == nil {
		return report, fmt.Errorf("invalid outbox shard or publisher")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s := &r.shards[shard]
	select {
	case s.permit <- struct{}{}:
	case <-ctx.Done():
		return report, ctx.Err()
	}
	defer func() { <-s.permit }()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if s.mark != nil {
		count, err := r.mark(ctx, s)
		report.Marked = count
		return report, err
	}
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	rows, err := r.config.Store.PendingOutbox(readCtx, store.OutboxQuery{Shard: shard, Limit: r.config.BatchSize, Wait: time.Second, Consistency: store.ConsistencyStrong})
	readCancel()
	if err != nil {
		return report, fmt.Errorf("pending outbox shard %d: %w", shard, err)
	}
	if len(rows) > r.config.BatchSize {
		return report, fmt.Errorf("pending outbox exceeded requested batch size")
	}
	report.Read = len(rows)
	var confirmed []store.OutboxEvent
	var publishErr error
	for _, event := range rows {
		publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
		err := p.Publish(publishCtx, event)
		publishCancel()
		if err != nil {
			publishErr = errors.Join(publishErr, fmt.Errorf("event %s: %w", event.EventID, err))
			if errors.Is(err, mq.ErrUnroutable) || errors.Is(err, mq.ErrNack) || errors.Is(err, mq.ErrInvalidEvent) {
				continue
			}
			break
		}
		confirmed = append(confirmed, event)
	}
	report.Published = len(confirmed)
	if len(confirmed) != 0 {
		cmd := markCommand(shard, confirmed)
		s.mark = &cmd
		count, err := r.mark(ctx, s)
		report.Marked = count
		return report, errors.Join(publishErr, err)
	}
	return report, publishErr
}

// markCommand depends only on the owning shard and immutable confirmed rows.
// Replicas/restarts canonicalize ordering and derive At from the committed rows,
// so replaying an identical batch has an identical FSM command hash.
func markCommand(shard int, rows []store.OutboxEvent) store.MarkOutboxPublished {
	ids := make([]string, len(rows))
	at := rows[0].CreatedAt
	for i, row := range rows {
		ids[i] = row.EventID
		at = max(at, row.CreatedAt)
	}
	slices.Sort(ids)
	identity := fmt.Sprintf("reliefgrid/outbox-mark/v1:%d:%q:%d", shard, ids, at)
	return store.MarkOutboxPublished{Meta: store.Meta{OperationID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity)).String(), At: at}, Shard: shard, EventIDs: ids}
}

func (r *Relay) mark(ctx context.Context, s *shardState) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := *s.mark
	cmd.EventIDs = slices.Clone(cmd.EventIDs)
	result, err := r.config.Store.MarkOutboxPublished(ctx, cmd)
	if err != nil {
		return 0, fmt.Errorf("mark outbox shard %d: %w", cmd.Shard, err)
	}
	if result.Outcome != store.OutcomeOK {
		return 0, fmt.Errorf("%w: shard=%d operation=%s outcome=%s", ErrMarkRejected, cmd.Shard, cmd.OperationID, result.Outcome)
	}
	s.mark = nil
	return len(cmd.EventIDs), nil
}

// Run owns one independent publisher/worker per shard. Empty polls and failures
// back off with cancellable timers; an unavailable shard cannot block the others.
func (r *Relay) Run(ctx context.Context) error {
	if r.config.PublisherFactory == nil {
		return fmt.Errorf("outbox publisher factory is required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsCh := make(chan error, len(r.shards))
	var wg sync.WaitGroup
	for shard := range r.shards {
		wg.Go(func() {
			if err := r.runShard(ctx, shard); err != nil && !errors.Is(err, context.Canceled) {
				errorsCh <- err
				cancel()
			}
		})
	}
	wg.Wait()
	close(errorsCh)
	var failures []error
	for err := range errorsCh {
		failures = append(failures, err)
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return ctx.Err()
}

func (r *Relay) runShard(ctx context.Context, shard int) error {
	var p Publisher
	defer func() {
		if p != nil {
			_ = p.Close()
		}
	}()
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		var err error
		if p == nil {
			// A failed constructor can return a typed nil in its interface. Only
			// retain a ready publisher; closing the failed value can panic.
			var candidate Publisher
			candidate, err = r.config.PublisherFactory(ctx)
			if err == nil {
				p = candidate
			}
		}
		var report Report
		if err == nil {
			report, err = r.Step(ctx, shard, p)
		}
		if errors.Is(err, ErrMarkRejected) {
			return err
		}
		if err != nil {
			if p != nil {
				_ = p.Close()
				p = nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.WarnContext(ctx, "outbox retry", "shard", shard, "error", err, "backoff", backoff)
		} else {
			backoff = 100 * time.Millisecond
		}
		if err == nil && report.Read > 0 {
			continue
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err != nil {
			backoff = min(2*backoff, 5*time.Second)
		}
	}
	return ctx.Err()
}
