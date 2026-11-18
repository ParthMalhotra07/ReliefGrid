//go:build schedules

package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	engine "github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// This harness proposes real Store commands and invokes the production cleanup
// and matching helpers. It does not implement an alternative state machine.
type execution struct {
	observedMu       sync.Mutex
	observed         []capturedHandoff
	brokerConnection *amqp.Connection
	brokerAddress    string
	t                *testing.T
	ctx              context.Context
	s                store.Store
	prefix           string
	at               time.Time
	sequence         atomic.Uint64
	entities         map[string]string
	attempts         map[string]store.Attempt
	lastClaim        store.TryClaim
	requestShard     int
	seed             uint64
}

func (x *execution) id(name string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(x.prefix+"/"+name)).String()
}
func (x *execution) meta() store.Meta {
	n := x.sequence.Add(1)
	return store.Meta{OperationID: x.id(fmt.Sprint("op-", n)), At: x.at.UnixMilli() + int64(n)}
}
func (x *execution) must(r store.Result, err error, want store.Outcome) {
	x.t.Helper()
	if err != nil || r.Outcome != want {
		x.t.Fatalf("command outcome=%s want=%s error=%v", r.Outcome, want, err)
	}
}
func (x *execution) read(name string) store.Record {
	x.t.Helper()
	key := store.ResourceKey(x.entities[name])
	if name == "R" {
		key = store.RequestKey(x.entities[name])
	}
	r, err := x.s.ReadRecord(x.ctx, key)
	if err != nil {
		x.t.Fatal(err)
	}
	return r
}
func (x *execution) transition(rec store.Record, to store.RequestStatus, a *store.Attempt) store.TransitionRequest {
	c := store.TransitionRequest{Meta: x.meta(), Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: rec.Request.Status, To: to, Attempt: a}
	minimumAt := rec.UpdatedAt
	if to == store.RequestMatching {
		minimumAt = max(minimumAt, rec.Request.NextAttemptAt)
	}
	if c.At < minimumAt {
		x.sequence.Add(uint64(minimumAt - c.At))
		c.At = minimumAt
	}
	if rec.Request.CurrentAttempt != nil {
		c.ExpectedAttemptID = rec.Request.CurrentAttempt.AttemptID
	}
	if to == store.RequestCancelling || to == store.RequestFulfilling {
		c.Disposition = store.ResourceAvailable
	}
	return c
}
func (x *execution) move(to store.RequestStatus, a *store.Attempt) store.TransitionRequest {
	x.t.Helper()
	c := x.transition(x.read("R"), to, a)
	if to == store.RequestRetrying && strings.Contains(x.prefix, "/SCH15_") {
		c.Outbox = []store.OutboxEvent{retryHint(x, c.Meta, c.ExpectedVersion+1)}
	}
	r, err := x.s.TransitionRequest(x.ctx, c)
	x.must(r, err, store.OutcomeOK)
	return c
}
func (x *execution) claim(a store.Attempt) store.TryClaim {
	x.t.Helper()
	c := store.TryClaim{Meta: x.meta(), ResourceKey: store.ResourceKey(a.ResourceID), RequestID: x.entities["R"], Attempt: a, Type: store.TypeWater}
	r, err := x.s.TryClaim(x.ctx, c)
	x.must(r, err, store.ClaimSuccess)
	x.lastClaim = c
	return c
}
func (x *execution) matcher(s store.Store) *engine.Matcher {
	x.t.Helper()
	m, err := engine.New(engine.Config{Store: observingStore{Store: s, x: x}, Now: func() time.Time { return x.at.Add(time.Duration(x.sequence.Load()+100) * time.Millisecond) }, Seed: x.seed, ScanLimit: 2})
	if err != nil {
		x.t.Fatal(err)
	}
	return m
}
func (x *execution) closeRetry() {
	x.t.Helper()
	if _, err := x.matcher(x.s).Abandon(x.ctx, x.entities["R"]); err != nil {
		x.t.Fatal(err)
	}
}
func (x *execution) cleanup() {
	x.t.Helper()
	if _, err := x.matcher(x.s).Cleanup(x.ctx, x.entities["R"]); err != nil {
		x.t.Fatal(err)
	}
}
func (x *execution) late(a store.Attempt) {
	x.t.Helper()
	r, err := x.s.TryClaim(x.ctx, store.TryClaim{Meta: x.meta(), ResourceKey: store.ResourceKey(a.ResourceID), RequestID: x.entities["R"], Attempt: a, Type: store.TypeWater})
	x.must(r, err, store.ClaimClosed)
}
func (x *execution) match(a store.Attempt) {
	x.move(store.RequestMatching, &a)
	x.claim(a)
	x.confirm()
}

func newExecution(t *testing.T, f ScheduleFixture, seed int, run string) *execution {
	t.Helper()
	budget := 30 * time.Second
	// Owner's kind runs measured 44.23s/53.02s for transfer/stage; keep
	// ordinary workflow limits while allowing bounded physical recovery.
	if os.Getenv("SCHEDULE_BACKEND") == "cluster" && (scheduleCode(f) == "12" || strings.HasPrefix(scheduleCode(f), "13")) {
		budget = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	t.Cleanup(cancel)
	var s store.Store
	if os.Getenv("SCHEDULE_BACKEND") == "cluster" {
		c, err := scheduleCoordinatorClient(ctx, os.Getenv("COORDINATOR_URL"))
		if err != nil {
			t.Fatal(err)
		}
		s = c
	} else {
		opts := []memstore.Option{memstore.WithShards(3), memstore.WithCheckpointDir(t.TempDir())}
		if strings.HasPrefix(f.Tag, "SCH14_") {
			opts = append(opts, memstore.WithRouter(func(string) int { return 0 }))
		}
		c, err := memstore.New(ctx, opts...)
		if err != nil {
			t.Fatal(err)
		}
		s = c
		t.Cleanup(func() {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	x := &execution{t: t, ctx: ctx, s: s, prefix: fmt.Sprintf("%s/%d/%s", run, seed, f.Tag), at: time.Now().UTC(), entities: map[string]string{}, attempts: map[string]store.Attempt{}}
	x.seed = uint64(seed)
	fixtureIndex := -1
	for i, candidate := range Schedules {
		if candidate.Tag == f.Tag {
			fixtureIndex = i
			break
		}
	}
	if fixtureIndex < 0 {
		t.Fatal("fixture missing from canonical schedule list")
	}
	index := (seed-1)*len(Schedules) + fixtureIndex
	location := store.Location{Lat: -60 + float64(index/180)*2, Lng: -179 + float64(index%180)*2}
	hash, err := geo.Encode(location, 7)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"R", "Res", "Res2"} {
		x.entities[n] = x.id(n)
	}
	if strings.HasPrefix(f.Tag, "SCH14_") && os.Getenv("SCHEDULE_BACKEND") == "cluster" {
		placeHealthyShard(x)
	}
	for _, n := range []string{"attemptA", "attemptB"} {
		res := "Res"
		if n == "attemptB" && strings.HasPrefix(f.Tag, "SCH08") {
			res = "Res2"
		}
		x.attempts[n] = store.Attempt{AttemptID: x.id(n), ResourceID: x.entities[res], Fingerprint: x.id("fingerprint-" + n)}
	}
	for _, want := range f.InitialState {
		if want.ResStatus == "" {
			continue
		}
		resource := store.Resource{ID: x.entities[want.Entity], Type: store.TypeWater, Name: "Synthetic seeded schedule", Location: location, Geohash: hash, Reusable: true, Status: store.ResourceAvailable, AvailableFrom: x.at.Add(-time.Hour).UnixMilli(), AvailableUntil: x.at.Add(time.Hour).UnixMilli(), LocationRevision: 1}
		r, err := s.CreateEntity(ctx, store.CreateEntity{Meta: x.meta(), Key: store.ResourceKey(resource.ID), Resource: &resource})
		x.must(r, err, store.OutcomeOK)
	}
	req := store.Request{ID: x.entities["R"], Type: store.TypeWater, Location: location, Geohash: hash, PeopleAffected: 1, Urgency: 1}
	r, err := s.CreateEntity(ctx, store.CreateEntity{Meta: x.meta(), Key: store.RequestKey(req.ID), Request: &req})
	x.must(r, err, store.OutcomeOK)
	x.requestShard = r.Shard
	for _, want := range f.InitialState {
		if want.Entity != "R" {
			continue
		}
		if want.ReqStatus == store.RequestMatching || want.ReqStatus == store.RequestMatched || want.ReqStatus == store.RequestRetrying {
			a := x.attempts["attemptA"]
			x.move(store.RequestMatching, &a)
			for _, res := range f.InitialState {
				if res.ResStatus == store.ResourceAssigned {
					x.claim(a)
				}
			}
			if want.ReqStatus == store.RequestMatched {
				x.move(store.RequestMatched, nil)
			}
			if want.ReqStatus == store.RequestRetrying {
				x.move(store.RequestRetrying, nil)
			}
		}
	}
	return x
}

type terminalCapture struct {
	store.Store
	final *store.TransitionRequest
}

func (s *terminalCapture) TransitionRequest(ctx context.Context, c store.TransitionRequest) (store.Result, error) {
	if c.To == store.RequestFulfilled {
		s.final = &c
	}
	return s.Store.TransitionRequest(ctx, c)
}

type partialCandidates struct{ store.Store }

func (s partialCandidates) Scan(context.Context, store.ScanQuery) (store.ScanResult, error) {
	return store.ScanResult{Partial: true, FailedShards: []int{1}}, nil
}

func executeWorkflowSchedule(x *execution, number int) {
	a, b := x.attempts["attemptA"], x.attempts["attemptB"]
	switch number {
	case 1:
		rec := x.read("R")
		first, second := x.transition(rec, store.RequestMatching, &a), x.transition(rec, store.RequestMatching, &b)
		entered := make(chan struct{}, 2)
		releaseA, releaseB := make(chan struct{}), make(chan struct{})
		results := make(chan store.Result, 2)
		errs := make(chan error, 2)
		propose := func(c store.TransitionRequest, release <-chan struct{}) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-x.ctx.Done():
				results <- store.Result{}
				errs <- x.ctx.Err()
				return
			}
			r, err := x.s.TransitionRequest(x.ctx, c)
			results <- r
			errs <- err
		}
		go propose(first, releaseA)
		go propose(second, releaseB)
		for range 2 {
			select {
			case <-entered:
			case <-x.ctx.Done():
				x.t.Fatal("CAS barrier not reached")
			}
		}
		close(releaseA)
		x.must(<-results, <-errs, store.OutcomeOK)
		close(releaseB)
		r, err := <-results, <-errs
		if err != nil || r.Outcome == store.OutcomeOK {
			x.t.Fatal("second queued CAS won", r, err)
		}
		x.claim(a)
		if _, err := x.matcher(x.s).Match(x.ctx, x.entities["R"]); err != nil {
			x.t.Fatal(err)
		}
	case 2:
		c := x.transition(x.read("R"), store.RequestMatching, &a)
		faulty := memstore.WithFaults(x.s, func(memstore.Call) memstore.Fault { return memstore.Fault{DropResponse: true} })
		if _, err := faulty.TransitionRequest(x.ctx, c); !errors.Is(err, store.ErrUnknownOutcome) {
			x.t.Fatal("lost response did not report unknown outcome:", err)
		}
		r, err := x.s.TransitionRequest(x.ctx, c)
		x.must(r, err, store.OutcomeOK)
		if x.read("R").Version != 2 {
			x.t.Fatal("same operation applied twice")
		}
		x.claim(a)
		if _, err := x.matcher(x.s).Match(x.ctx, x.entities["R"]); err != nil {
			x.t.Fatal(err)
		}
	case 3:
		c := x.transition(x.read("R"), store.RequestMatching, &a)
		faulty := memstore.WithFaults(x.s, func(memstore.Call) memstore.Fault { return memstore.Fault{DropBefore: true} })
		if _, err := faulty.TransitionRequest(x.ctx, c); !errors.Is(err, store.ErrUnknownOutcome) {
			x.t.Fatal("pre-proposal crash did not report unknown outcome:", err)
		}
		if x.read("R").Version != 1 {
			x.t.Fatal("crash before proposal changed state")
		}
		x.match(b)
	case 4:
		x.move(store.RequestMatching, &a)
		closedAt := x.read("R").UpdatedAt
		x.closeRetry()
		closure := x.assertClosedA(closedAt)
		x.late(a)
		x.match(b)
		if after := x.assertClosedA(closedAt); after != closure {
			x.t.Fatal("retarget changed permanent attemptA closure")
		}
		x.late(a)
	case 5:
		x.move(store.RequestMatching, &a)
		x.move(store.RequestCancelling, nil)
		x.cleanup()
		x.late(a)
	case 6:
		x.move(store.RequestCancelling, nil)
		x.cleanup()
		x.late(a)
	case 7:
		x.closeRetry()
		x.late(a)
	case 8:
		stale := x.transition(x.read("R"), store.RequestMatched, nil)
		x.move(store.RequestRetrying, nil)
		r, err := x.s.TransitionRequest(x.ctx, stale)
		if err != nil || r.Outcome == store.OutcomeOK {
			x.t.Fatal("stale confirmer escaped fence", r, err)
		}
		current := x.transition(x.read("R"), store.RequestMatched, nil)
		r, err = x.s.TransitionRequest(x.ctx, current)
		if err != nil || r.Outcome == store.OutcomeOK {
			x.t.Fatal("current RETRYING confirmation escaped fence", r, err)
		}
		x.closeRetry()
		x.match(b)
		x.late(a)
	case 9:
		original := x.lastClaim
		x.move(store.RequestCancelling, nil)
		x.cleanup()
		before := x.read("Res")
		r, err := x.s.TryClaim(x.ctx, original)
		x.must(r, err, store.ClaimSuccess)
		after := x.read("Res")
		if before.Version != after.Version || after.Resource.ActiveClaim != nil {
			x.t.Fatal("historical success reclaimed resource")
		}
	case 10:
		x.move(store.RequestFulfilling, nil)
		capture := &terminalCapture{Store: x.s}
		if _, err := x.matcher(capture).Cleanup(x.ctx, x.entities["R"]); err != nil {
			x.t.Fatal(err)
		}
		if capture.final == nil {
			x.t.Fatal("terminal command was not captured")
		}
		before := x.read("R")
		r, err := x.s.TransitionRequest(x.ctx, *capture.final)
		x.must(r, err, store.OutcomeOK)
		if x.read("R").Version != before.Version {
			x.t.Fatal("duplicate terminal event mutated state")
		}
		pending, err := x.s.PendingOutbox(x.ctx, store.OutboxQuery{Shard: x.requestShard, Limit: 100, Consistency: store.ConsistencyStrong})
		if err != nil {
			x.t.Fatal(err)
		}
		count := 0
		for _, event := range pending {
			if event.EntityKey == before.Key && event.EventType == "request.state_changed" && event.RoutingKey == "request.status.fulfilled" {
				count++
			}
		}
		if count != 1 {
			x.t.Fatalf("duplicate terminal command produced %d fulfilled events, want1", count)
		}
	case 11:
		x.move(store.RequestCancelling, nil)
		x.cleanup()
		h, err := api.New(api.Config{Store: x.s, CallerNamespace: "schedule"})
		if err != nil {
			x.t.Fatal(err)
		}
		for _, call := range []struct {
			status string
			code   int
		}{{"CANCELLED", 200}, {"FULFILLED", 409}} {
			w := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(x.ctx, "PUT", "/api/v1/requests/"+x.entities["R"]+"/status", strings.NewReader(`{"status":"`+call.status+`"}`))
			req.Header.Set("Content-Type", "application/json")
			h.ServeHTTP(w, req)
			if w.Code != call.code {
				x.t.Fatalf("repeat %s HTTP%d want%d: %s", call.status, w.Code, call.code, w.Body.String())
			}
		}
	case 17:
		if _, err := x.matcher(partialCandidates{Store: x.s}).Match(x.ctx, x.entities["R"]); err == nil {
			x.t.Fatal("partial scan accepted")
		}
		if rec := x.read("R"); rec.Version != 1 || rec.Request.Status != store.RequestPending {
			x.t.Fatal("partial result committed no-candidate")
		}
		if _, err := x.matcher(x.s).Match(x.ctx, x.entities["R"]); err != nil {
			x.t.Fatal(err)
		}
		// The production matcher chooses its own deterministic attempt identity.
		// Bind the fixture's symbolic attemptA to that actually committed intent.
		actual := x.read("R")
		if actual.Request.CurrentAttempt == nil {
			x.t.Fatal("matcher returned without a durable intent")
		}
		x.attempts["attemptA"] = *actual.Request.CurrentAttempt
	case 14:
		executeCoordinatorRestart(x)
	case 15:
		executeBrokerOutage(x)
	case 16:
		executePoison(x)
	default:
		x.t.Fatalf("SCH%02d needs its infrastructure fault driver; full acceptance unavailable (snapshot seams: issue #88)", number)
	}
}

func assertFixtureFinal(x *execution, f ScheduleFixture) {
	x.t.Helper()
	for _, want := range f.FinalState {
		got := x.read(want.Entity)
		if got.Version != uint64(want.Version) {
			x.t.Errorf("%s version%d want%d", want.Entity, got.Version, want.Version)
		}
		if want.ReqStatus != "" {
			if got.Request.Status != want.ReqStatus {
				x.t.Errorf("request status%s want%s", got.Request.Status, want.ReqStatus)
			}
			if want.ActiveClaim != "" {
				if got.Request.CurrentAttempt == nil || got.Request.CurrentAttempt.AttemptID != x.attempts[want.ActiveClaim].AttemptID {
					x.t.Error("wrong final request attempt")
				}
			} else if got.Request.Status == store.RequestWaiting && got.Request.CurrentAttempt != nil {
				x.t.Error("waiting retains intent")
			}
		}
		if want.ResStatus != "" {
			if got.Resource.Status != want.ResStatus {
				x.t.Errorf("resource status%s want%s", got.Resource.Status, want.ResStatus)
			}
			if want.ActiveClaim == "" {
				if got.Resource.ActiveClaim != nil {
					x.t.Error("resource retains claim")
				}
			} else {
				a := x.attempts[want.ActiveClaim]
				if c := got.Resource.ActiveClaim; c == nil || c.RequestID != x.entities["R"] || c.AttemptID != a.AttemptID || c.Fingerprint != a.Fingerprint {
					x.t.Error("nonreciprocal final claim")
				}
			}
		}
	}
}

func scheduleSeeds(t *testing.T) int {
	t.Helper()
	seeds := 1
	if raw := os.Getenv("SEEDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 50 {
			t.Fatal("SEEDS must be 1..50 (isolated geographic fixture grid)")
		}
		seeds = n
	}
	return seeds
}

// This proves the proposer-side stale-version race only. It is deliberately
// separate from SCH01, which requires both commands in the FSM log before apply.
func TestCASProposalBarrierProbe(t *testing.T) {
	run := os.Getenv("SCHEDULE_RUN_ID")
	if run == "" {
		run = uuid.NewString()
	}
	t.Logf("proposer probe only: run=%s backend=%s", run, os.Getenv("SCHEDULE_BACKEND"))
	for seed := 1; seed <= scheduleSeeds(t); seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			x := newExecution(t, Schedules[0], seed, run)
			executeWorkflowSchedule(x, 1)
			assertFixtureFinal(x, Schedules[0])
		})
	}
}

func TestSeededWorkflowSchedules(t *testing.T) {
	seeds := scheduleSeeds(t)
	run := os.Getenv("SCHEDULE_RUN_ID")
	if run == "" {
		run = uuid.NewString()
	}
	t.Logf("run=%s backend=%s seeds=%d", run, os.Getenv("SCHEDULE_BACKEND"), seeds)
	selected := os.Getenv("SCHEDULE_IDS")
	if backend := os.Getenv("SCHEDULE_BACKEND"); backend != "" && backend != "m8" && backend != "cluster" {
		t.Fatal("SCHEDULE_BACKEND must be m8 or cluster")
	}
	if selected != "" {
		seen := map[string]bool{}
		for _, item := range strings.Split(selected, ",") {
			id := item
			valid := id == "13"
			for _, fixture := range Schedules {
				if strings.TrimLeft(scheduleCode(fixture), "0") == strings.TrimLeft(id, "0") {
					valid = true
				}
			}
			if !valid || seen[id] {
				t.Fatal("invalid or duplicate SCHEDULE_IDS entry:", item)
			}
			seen[id] = true
		}
		t.Log("SUBSET execution; not complete A2.3 acceptance")
	}
	for seed := 1; seed <= seeds; seed++ {
		for _, f := range Schedules {
			code := scheduleCode(f)
			chosen := selected == ""
			for _, item := range strings.Split(selected, ",") {
				if strings.TrimLeft(item, "0") == strings.TrimLeft(code, "0") || item == "13" && strings.HasPrefix(code, "13") {
					chosen = true
				}
			}
			if !chosen {
				continue
			}
			if code == "13a" && os.Getenv("SCHEDULE_BACKEND") != "cluster" {
				if selected == "13a" {
					t.Fatal("SCH13a needs the cluster backend; M8 has no transfer")
				}
				t.Logf("seed-%d SCH13a NOT APPLICABLE on M8: transfer is covered on cluster", seed)
				continue
			}
			t.Run(fmt.Sprintf("seed-%d/%s", seed, f.Tag), func(t *testing.T) {
				x := newExecution(t, f, seed, run)
				switch code {
				case "01":
					executeLogBarrier(x)
				case "12", "13a", "13b":
					executeSnapshotBoundary(x, code)
				default:
					n, err := strconv.Atoi(code)
					if err != nil {
						t.Fatal(err)
					}
					executeWorkflowSchedule(x, n)
				}
				assertFixtureFinal(x, f)
				assertScheduleOutbox(x, f)
			})
		}
	}
}
