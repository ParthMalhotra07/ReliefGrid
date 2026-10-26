// Package invchecktest seeds a store with a workflow that passes every
// invariant: requests in every lifecycle state, reached through the real
// commands, so the same fixture serves memstore and the cluster.
package invchecktest

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// T0 is the fixture clock, unix ms.
const T0 = int64(1_792_000_000_000)

const hour = int64(3_600_000)

// Seeded describes what Seed wrote.
type Seeded struct {
	Requests  int
	Resources int
	// Events is the number of outbox events written; Seed publishes none.
	Events int
	// ByStatus counts requests per status.
	ByStatus map[store.RequestStatus]int
}

type seeder struct {
	ctx context.Context
	s   store.Store
	p   string
	ops int
	out Seeded
}

// Seed writes, under prefix p, one request in each of PENDING, WAITING,
// MATCHING (before and after its claim), MATCHED, IN_PROGRESS, RETRYING
// (claim held and claim closed), WAITING after a retry, CANCELLING,
// CANCELLED, FULFILLING and FULFILLED, a request that lost a contested claim
// and retargeted, and extra PENDING requests and AVAILABLE resources. Each
// created request carries one outbox event.
func Seed(ctx context.Context, s store.Store, p string, extra int) (Seeded, error) {
	sd := &seeder{ctx: ctx, s: s, p: p, out: Seeded{ByStatus: map[store.RequestStatus]int{}}}
	if err := sd.run(extra); err != nil {
		return Seeded{}, err
	}
	return sd.out, nil
}

type req struct {
	id      string
	version uint64
	status  store.RequestStatus
	attempt *store.Attempt
}

func (sd *seeder) meta() store.Meta {
	sd.ops++
	return store.Meta{OperationID: fmt.Sprintf("%s-op-%d", sd.p, sd.ops), At: T0}
}

func (sd *seeder) id(s string) string { return sd.p + "-" + s }

func want(what string, res store.Result, err error, outcome store.Outcome) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if res.Outcome != outcome {
		return fmt.Errorf("%s: %s (%s), want %s", what, res.Outcome, res.Reason, outcome)
	}
	return nil
}

func (sd *seeder) resource(name string, reusable bool) (string, error) {
	id := sd.id(name)
	res, err := sd.s.CreateEntity(sd.ctx, store.CreateEntity{Meta: sd.meta(), Key: store.ResourceKey(id), Resource: &store.Resource{
		ID: id, Type: store.TypeWater, Name: "unit " + name, Location: store.Location{Lat: 28.61, Lng: 77.21}, Geohash: "ttnfv",
		AvailableFrom: T0 - hour, AvailableUntil: T0 + 24*hour, Status: store.ResourceAvailable, Reusable: reusable,
	}})
	if err := want("create resource "+id, res, err, store.OutcomeOK); err != nil {
		return "", err
	}
	sd.out.Resources++
	return id, nil
}

func (sd *seeder) request(name string) (*req, error) {
	id := sd.id(name)
	cmd := store.CreateEntity{Meta: sd.meta(), Key: store.RequestKey(id), Request: &store.Request{
		ID: id, Type: store.TypeWater, Location: store.Location{Lat: 28.62, Lng: 77.22}, Geohash: "ttnfv",
		PeopleAffected: 4, Urgency: 2, Status: store.RequestPending,
	}, Outbox: []store.OutboxEvent{{EventID: id + "-created", EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.requests", RoutingKey: "request.created.water", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}}}
	res, err := sd.s.CreateEntity(sd.ctx, cmd)
	if err := want("create request "+id, res, err, store.OutcomeOK); err != nil {
		return nil, err
	}
	sd.out.Requests++
	sd.out.Events += len(cmd.Outbox)
	return &req{id: id, version: res.Version, status: store.RequestPending}, nil
}

// move commits r -> to; attempt is required for MATCHING, disposition for
// CANCELLING and FULFILLING.
func (sd *seeder) move(r *req, to store.RequestStatus, attempt *store.Attempt, disposition store.ResourceStatus) error {
	cmd := store.TransitionRequest{Meta: sd.meta(), Key: store.RequestKey(r.id), ExpectedVersion: r.version,
		ExpectedStatus: r.status, To: to, Attempt: attempt, Disposition: disposition}
	if r.attempt != nil {
		cmd.ExpectedAttemptID = r.attempt.AttemptID
	}
	res, err := sd.s.TransitionRequest(sd.ctx, cmd)
	if err := want(fmt.Sprintf("%s %s -> %s", r.id, r.status, to), res, err, store.OutcomeOK); err != nil {
		return err
	}
	r.version, r.status = res.Version, to
	switch to {
	case store.RequestMatching:
		a := *attempt
		r.attempt = &a
	case store.RequestWaiting:
		r.attempt = nil
	}
	return nil
}

func (sd *seeder) match(r *req, attemptName, resourceID string) error {
	a := store.Attempt{AttemptID: sd.id(attemptName), ResourceID: resourceID, Fingerprint: "fp-" + attemptName}
	return sd.move(r, store.RequestMatching, &a, "")
}

func (sd *seeder) claim(r *req, outcome store.Outcome) error {
	res, err := sd.s.TryClaim(sd.ctx, store.TryClaim{Meta: sd.meta(), ResourceKey: store.ResourceKey(r.attempt.ResourceID),
		RequestID: r.id, Attempt: *r.attempt, Type: store.TypeWater})
	return want("claim for "+r.id, res, err, outcome)
}

func (sd *seeder) close(r *req, disposition store.ResourceStatus) error {
	res, err := sd.s.CloseAttempt(sd.ctx, store.CloseAttempt{Meta: sd.meta(), ResourceKey: store.ResourceKey(r.attempt.ResourceID),
		RequestID: r.id, Attempt: *r.attempt, Disposition: disposition, Reason: "seed"})
	return want("close for "+r.id, res, err, store.OutcomeOK)
}

// steps runs fns in order, stopping at the first error.
func steps(fns ...func() error) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (sd *seeder) run(extra int) error {
	type plan struct {
		name     string
		reusable bool
		walk     func(r *req, res string) error
	}
	plans := []plan{
		{"pending", true, func(*req, string) error { return nil }},
		{"waiting", true, func(r *req, _ string) error { return sd.move(r, store.RequestWaiting, nil, "") }},
		{"matching-unclaimed", true, func(r *req, res string) error { return sd.match(r, "a-matching-unclaimed", res) }},
		{"matching-claimed", true, func(r *req, res string) error {
			return steps(func() error { return sd.match(r, "a-matching-claimed", res) }, func() error { return sd.claim(r, store.ClaimSuccess) })
		}},
		{"matched", true, func(r *req, res string) error { return sd.matched(r, "a-matched", res) }},
		{"in-progress", true, func(r *req, res string) error {
			return steps(func() error { return sd.matched(r, "a-in-progress", res) },
				func() error { return sd.move(r, store.RequestInProgress, nil, "") })
		}},
		{"retrying-held", true, func(r *req, res string) error {
			return steps(func() error { return sd.match(r, "a-retrying-held", res) }, func() error { return sd.claim(r, store.ClaimSuccess) },
				func() error { return sd.move(r, store.RequestRetrying, nil, "") })
		}},
		{"retrying-closed", true, func(r *req, res string) error {
			return steps(func() error { return sd.match(r, "a-retrying-closed", res) }, func() error { return sd.claim(r, store.ClaimSuccess) },
				func() error { return sd.move(r, store.RequestRetrying, nil, "") }, func() error { return sd.close(r, store.ResourceAvailable) })
		}},
		{"waiting-after-retry", true, func(r *req, res string) error {
			return steps(func() error { return sd.match(r, "a-waiting-after-retry", res) }, func() error { return sd.claim(r, store.ClaimSuccess) },
				func() error { return sd.move(r, store.RequestRetrying, nil, "") }, func() error { return sd.close(r, store.ResourceAvailable) },
				func() error { return sd.move(r, store.RequestWaiting, nil, "") })
		}},
		{"cancelling", true, func(r *req, res string) error {
			return steps(func() error { return sd.matched(r, "a-cancelling", res) },
				func() error { return sd.move(r, store.RequestCancelling, nil, store.ResourceAvailable) })
		}},
		{"cancelled", true, func(r *req, res string) error {
			return steps(func() error { return sd.matched(r, "a-cancelled", res) },
				func() error { return sd.move(r, store.RequestCancelling, nil, store.ResourceAvailable) },
				func() error { return sd.close(r, store.ResourceAvailable) }, func() error { return sd.move(r, store.RequestCancelled, nil, "") })
		}},
		{"cancelled-pending", true, func(r *req, _ string) error { return sd.move(r, store.RequestCancelled, nil, "") }},
		{"fulfilling", false, func(r *req, res string) error {
			return steps(func() error { return sd.matched(r, "a-fulfilling", res) }, func() error { return sd.move(r, store.RequestInProgress, nil, "") },
				func() error { return sd.move(r, store.RequestFulfilling, nil, store.ResourceConsumed) })
		}},
		{"fulfilled", false, func(r *req, res string) error {
			return steps(func() error { return sd.matched(r, "a-fulfilled", res) }, func() error { return sd.move(r, store.RequestInProgress, nil, "") },
				func() error { return sd.move(r, store.RequestFulfilling, nil, store.ResourceConsumed) },
				func() error { return sd.close(r, store.ResourceConsumed) }, func() error { return sd.move(r, store.RequestFulfilled, nil, "") })
		}},
	}
	for _, pl := range plans {
		res, err := sd.resource("r-"+pl.name, pl.reusable)
		if err != nil {
			return err
		}
		r, err := sd.request("q-" + pl.name)
		if err != nil {
			return err
		}
		if err := pl.walk(r, res); err != nil {
			return err
		}
		sd.out.ByStatus[r.status]++
	}
	if err := sd.contested(); err != nil {
		return err
	}
	for i := range extra {
		if _, err := sd.resource(fmt.Sprintf("r-extra-%04d", i), true); err != nil {
			return err
		}
		if _, err := sd.request(fmt.Sprintf("q-extra-%04d", i)); err != nil {
			return err
		}
		sd.out.ByStatus[store.RequestPending]++
	}
	return nil
}

func (sd *seeder) matched(r *req, attemptName, res string) error {
	return steps(func() error { return sd.match(r, attemptName, res) }, func() error { return sd.claim(r, store.ClaimSuccess) },
		func() error { return sd.move(r, store.RequestMatched, nil, "") })
}

// contested: two requests intend one resource; the loser's claim is closed
// by the conflict, so it retargets to another resource and is MATCHED there.
func (sd *seeder) contested() error {
	shared, err := sd.resource("r-contested", true)
	if err != nil {
		return err
	}
	other, err := sd.resource("r-contested-other", true)
	if err != nil {
		return err
	}
	winner, err := sd.request("q-contested-winner")
	if err != nil {
		return err
	}
	loser, err := sd.request("q-contested-loser")
	if err != nil {
		return err
	}
	if err := steps(
		func() error { return sd.match(winner, "a-contested-winner", shared) },
		func() error { return sd.match(loser, "a-contested-loser", shared) },
		func() error { return sd.claim(winner, store.ClaimSuccess) },
		func() error { return sd.claim(loser, store.ClaimConflictClosed) },
		func() error { return sd.move(winner, store.RequestMatched, nil, "") },
		func() error { return sd.match(loser, "a-contested-retarget", other) },
		func() error { return sd.claim(loser, store.ClaimSuccess) },
		func() error { return sd.move(loser, store.RequestMatched, nil, "") },
	); err != nil {
		return err
	}
	sd.out.ByStatus[winner.status]++
	sd.out.ByStatus[loser.status]++
	return nil
}
