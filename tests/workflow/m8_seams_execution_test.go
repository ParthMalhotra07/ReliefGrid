//go:build schedules

package workflow

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

type scheduleAnswer struct {
	result store.Result
	err    error
}

func executeLogBarrier(x *execution) {
	x.t.Helper()
	a, b := x.attempts["attemptA"], x.attempts["attemptB"]
	first := x.transition(x.read("R"), store.RequestMatching, &a)
	second := x.transition(x.read("R"), store.RequestMatching, &b)
	propose := func(c store.TransitionRequest) <-chan scheduleAnswer {
		done := make(chan scheduleAnswer, 1)
		go func() { r, err := x.s.TransitionRequest(x.ctx, c); done <- scheduleAnswer{r, err} }()
		return done
	}
	var p testseam.Point
	var one, two <-chan scheduleAnswer
	if s, ok := x.s.(*memstore.Store); ok {
		barrier, err := s.ArmLogBarrier(x.requestShard, 2500*time.Millisecond, first.OperationID, second.OperationID)
		if err != nil {
			x.t.Fatal(err)
		}
		x.t.Cleanup(func() { barrier.Hold().Release() })
		one = propose(first)
		if _, err = barrier.WaitAppended(x.ctx, first.OperationID); err != nil {
			x.t.Fatal(err)
		}
		two = propose(second)
		p, err = barrier.Hold().WaitReached(x.ctx)
		if err != nil {
			x.t.Fatal(err)
		}
		if state := barrier.Hold().Status().State; state != testseam.Reached {
			x.t.Fatalf("barrier no longer held: %s", state)
		}
		barrier.Hold().Release()
	} else {
		p, one, two = clusterLogBarrier(x, first, second, propose)
	}
	if p.Indexes[first.OperationID] == 0 || p.Indexes[second.OperationID] <= p.Indexes[first.OperationID] || p.CommitIndex < p.Indexes[second.OperationID] || p.AppliedIndex >= p.Indexes[first.OperationID] {
		x.t.Fatalf("invalid log-before-apply proof: %+v", p)
	}
	for _, entry := range []struct {
		ch   <-chan scheduleAnswer
		want store.Outcome
	}{{one, store.OutcomeOK}, {two, store.OutcomeVersionConflict}} {
		select {
		case got := <-entry.ch:
			x.must(got.result, got.err, entry.want)
		case <-x.ctx.Done():
			x.t.Fatal("proposal did not finish:", x.ctx.Err())
		}
	}
	x.claim(a)
	x.confirm()
	x.t.Logf("log barrier: first=%d second=%d commit=%d applied=%d", p.Indexes[first.OperationID], p.Indexes[second.OperationID], p.CommitIndex, p.AppliedIndex)
}

func seamPoint(x *execution, h *testseam.Hold) testseam.Point {
	x.t.Helper()
	p, err := h.WaitReached(x.ctx)
	if err != nil {
		x.t.Fatal(err)
	}
	if h.Status().State != testseam.Reached || p.Bytes <= 0 || p.Bytes >= p.Size {
		x.t.Fatalf("no active partial-image boundary: %+v %+v", h.Status(), p)
	}
	return p
}

func m8Dumps(x *execution, s *memstore.Store) []map[string][]string {
	x.t.Helper()
	var out []map[string][]string
	for shard := range 3 {
		d, err := s.Dump(x.ctx, shard)
		if err != nil {
			x.t.Fatal(err)
		}
		out = append(out, d)
	}
	return out
}

func executeSnapshotBoundary(x *execution, code string) {
	x.t.Helper()
	s, ok := x.s.(*memstore.Store)
	if !ok {
		clusterSnapshotBoundary(x, code)
		return
	}
	if code == "13a" {
		x.t.Fatal("SCH13a is a Raft transfer; it has no M8 equivalent")
	}
	prior, err := s.Checkpoint(x.ctx, x.requestShard)
	if err != nil {
		x.t.Fatal(err)
	}
	// A committed tail after the complete checkpoint proves restart replays logs.
	var tail store.Result
	for n := range 1000 {
		tail, err = s.CompareAndSet(x.ctx, store.CompareAndSet{Meta: x.meta(), Key: "kv:" + x.id(fmt.Sprintf("snapshot-tail-%d", n)), Value: json.RawMessage(`1`)})
		x.must(tail, err, store.OutcomeOK)
		if tail.Shard == x.requestShard {
			break
		}
	}
	if tail.Shard != x.requestShard || tail.AppliedIndex <= prior.AppliedIndex {
		x.t.Fatal("no committed tail on snapshot shard")
	}
	before := m8Dumps(x, s)
	var h *testseam.Hold
	done := make(chan error, 1)
	switch code {
	case "12":
		h, err = s.ArmCheckpointPause(x.requestShard, 10*time.Second)
		if err != nil {
			x.t.Fatal(err)
		}
		go func() { _, e := s.Checkpoint(x.ctx, x.requestShard); done <- e }()
	case "13b":
		h, err = s.ArmRestorePause(x.requestShard, 10*time.Second)
		if err != nil {
			x.t.Fatal(err)
		}
		if err = s.Crash(x.requestShard); err != nil {
			x.t.Fatal(err)
		}
		go func() { _, e := s.Restart(x.ctx, x.requestShard); done <- e }()
	default:
		x.t.Fatal("unknown snapshot schedule:", code)
	}
	x.t.Cleanup(h.Release)
	p := seamPoint(x, h)
	if code == "12" && p.ImageIndex != tail.AppliedIndex || code == "13b" && p.ImageIndex != prior.AppliedIndex {
		x.t.Fatalf("wrong interrupted image index: %+v", p)
	}
	if err = s.Crash(x.requestShard); err != nil {
		x.t.Fatal(err)
	}
	select {
	case e := <-done:
		if e == nil {
			x.t.Fatal("interrupted operation reported success")
		}
	case <-x.ctx.Done():
		x.t.Fatal(x.ctx.Err())
	}
	info, err := s.Restart(x.ctx, x.requestShard)
	if err != nil {
		x.t.Fatal(err)
	}
	if info.RestoredIndex != prior.AppliedIndex || info.AppliedIndex != tail.AppliedIndex {
		x.t.Fatalf("restart %+v; want checkpoint=%d replay=%d", info, prior.AppliedIndex, tail.AppliedIndex)
	}
	if !reflect.DeepEqual(before, m8Dumps(x, s)) {
		x.t.Fatal("committed FSM rows changed across crash/restart")
	}
	x.t.Logf("SCH%s partial bytes=%d/%d restored=%d replayed=%d", code, p.Bytes, p.Size, info.RestoredIndex, info.AppliedIndex)
	if code == "12" {
		x.confirm()
	}
}

// scheduleCode is stable when one source schedule has multiple physical cases.
func scheduleCode(f ScheduleFixture) string {
	return strings.SplitN(strings.TrimPrefix(f.Tag, "SCH"), "_", 2)[0]
}
