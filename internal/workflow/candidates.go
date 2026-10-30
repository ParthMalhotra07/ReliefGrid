package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const radiusKM = 50

type candidate struct {
	resource store.Resource
	distance float64
}

func eligible(r store.Record, request *store.Request, at int64) (candidate, bool) {
	x := r.Resource
	if r.Deleted || r.Kind != store.KindResource || x == nil || r.Key != store.ResourceKey(x.ID) ||
		x.Type != request.Type || x.Status != store.ResourceAvailable || x.ActiveClaim != nil ||
		at < x.AvailableFrom || at >= x.AvailableUntil {
		return candidate{}, false
	}
	if _, err := geo.Encode(x.Location, 7); err != nil {
		return candidate{}, false
	}
	distance := geo.HaversineKM(request.Location, x.Location)
	return candidate{resource: *x, distance: distance}, distance <= radiusKM
}

func rank(best []candidate, next candidate) []candidate {
	for _, old := range best {
		if old.resource.ID == next.resource.ID {
			return best
		}
	}
	best = append(best, next)
	slices.SortFunc(best, func(a, b candidate) int {
		if a.distance < b.distance {
			return -1
		}
		if a.distance > b.distance {
			return 1
		}
		if a.resource.ID < b.resource.ID {
			return -1
		}
		if a.resource.ID > b.resource.ID {
			return 1
		}
		return 0
	})
	return best[:min(3, len(best))]
}

// walk retains only the nearest three, but reads every page before selecting.
// Missing positions, partial pages, stalled cursors and budget exhaustion are
// incomplete work, never evidence of an empty eligible set.
func (m *Matcher) walk(ctx context.Context, scanner Scanner, rec store.Record, at int64) ([]candidate, error) {
	prefixes, err := geo.Cover(rec.Request.Location, radiusKM)
	if err != nil {
		return nil, err
	}
	q := store.ScanQuery{
		Kind: store.KindResource, Type: rec.Request.Type, Statuses: []string{string(store.ResourceAvailable)},
		GeohashPrefixes: prefixes, Limit: m.cfg.ScanLimit,
	}
	allShards := func() map[int]bool {
		shards := make(map[int]bool, m.cfg.Store.Shards())
		for shard := range m.cfg.Store.Shards() {
			shards[shard] = true
		}
		return shards
	}
	expected := allShards()
	seen := make(map[string]bool)
	var best []candidate
	restarted := false
	for page := 0; page < 1024; page++ {
		result, err := remote(ctx, m.cfg.CallTimeout, func(ctx context.Context) (store.ScanResult, error) {
			return scanner.Scan(ctx, q)
		})
		if errors.Is(err, store.ErrRingVersionMismatch) && !restarted {
			// Discard every old-ring candidate and position before starting over.
			q.Cursor, best, expected, seen, restarted = "", nil, allShards(), make(map[string]bool), true
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("workflow candidate scan: %w: %w", ErrIncompleteScan, err)
		}
		if result.Partial || len(result.FailedShards) != 0 {
			return nil, fmt.Errorf("%w: failed shards %v", ErrIncompleteScan, result.FailedShards)
		}
		answered, more := make(map[int]bool), make(map[int]bool)
		for _, position := range result.Positions {
			if !expected[position.Shard] || answered[position.Shard] {
				return nil, fmt.Errorf("%w: unexpected or duplicate shard position", ErrIncompleteScan)
			}
			answered[position.Shard] = true
			if position.HasMore {
				more[position.Shard] = true
			}
		}
		if len(answered) != len(expected) || (len(more) == 0) != (result.NextCursor == "") {
			return nil, fmt.Errorf("%w: missing position or inconsistent continuation", ErrIncompleteScan)
		}
		for _, row := range result.Records {
			if next, ok := eligible(row, rec.Request, at); ok {
				best = rank(best, next)
			}
		}
		if result.NextCursor == "" {
			return best, nil
		}
		if seen[result.NextCursor] {
			return nil, fmt.Errorf("%w: cursor did not advance", ErrIncompleteScan)
		}
		seen[result.NextCursor] = true
		q.Cursor, expected = result.NextCursor, more
	}
	return nil, fmt.Errorf("%w: page budget exhausted", ErrIncompleteScan)
}

// choose ranks the complete hint walk, revalidates the selected resource using
// an authoritative read, and falls back to a complete authoritative walk before
// declaring a negative result. A stable hash spreads different requests/seeds.
func (m *Matcher) choose(ctx context.Context, rec store.Record, at int64) (*store.Resource, error) {
	var hintErr error
	if m.cfg.CandidateScanner != nil {
		var hints []candidate
		hints, hintErr = m.walk(ctx, m.cfg.CandidateScanner, rec, at)
		if hintErr == nil && len(hints) > 0 {
			chosen, err := m.currentCandidate(ctx, rec, hints, at)
			if err != nil || chosen != nil {
				return chosen, err
			}
		}
	}
	best, err := m.walk(ctx, m.cfg.Store, rec, at)
	if err != nil {
		return nil, errors.Join(hintErr, err)
	}
	if len(best) == 0 {
		return nil, nil
	}
	chosen, err := m.currentCandidate(ctx, rec, best, at)
	if err == nil && chosen == nil {
		// All of the observed top three changed. Other candidates may exist;
		// this is not a complete negative result.
		err = ErrConcurrentChange
	}
	return chosen, err
}

func (m *Matcher) currentCandidate(ctx context.Context, rec store.Record, best []candidate, at int64) (*store.Resource, error) {
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], m.cfg.Seed)
	digest := sha256.Sum256(append(seed[:], []byte(fmt.Sprintf("%s/%d", rec.Key, rec.Version))...))
	start := int(binary.BigEndian.Uint64(digest[:8]) % uint64(len(best)))
	for offset := range best {
		x := best[(start+offset)%len(best)]
		current, err := m.read(ctx, store.ResourceKey(x.resource.ID))
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if candidate, ok := eligible(current, rec.Request, at); ok {
			return &candidate.resource, nil
		}
	}
	return nil, nil
}
