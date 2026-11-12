package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
)

// fakeRelay answers Step from a script per shard; past the script it is idle.
type fakeRelay struct {
	steps map[int][]step
	calls int
}

type step struct {
	report outbox.Report
	err    error
}

func (f *fakeRelay) Step(_ context.Context, shard int, _ outbox.Publisher) (outbox.Report, error) {
	f.calls++
	s := f.steps[shard]
	if len(s) == 0 {
		return outbox.Report{}, nil
	}
	f.steps[shard] = s[1:]
	return s[0].report, s[0].err
}

func nack(id string) error {
	return errors.Join(fmt.Errorf("event %s: %w: event=%s", id, mq.ErrNack, id))
}

func noSleep(time.Duration) {}

// A refused publish (a full queue under reject-publish) is retried until the
// relay publishes it; the retries are counted.
func TestDrainRetriesRefusedPublishes(t *testing.T) {
	relay := &fakeRelay{steps: map[int][]step{
		0: {
			{outbox.Report{Read: 2, Published: 1}, nack("a")},
			{outbox.Report{Read: 1}, nack("a")},
			{outbox.Report{Read: 1, Published: 1}, nil},
			{outbox.Report{Marked: 2}, nil},
		},
		1: {{outbox.Report{Read: 1, Published: 1}, nil}, {outbox.Report{Marked: 1}, nil}},
	}}
	got, err := drainLoop(t.Context(), relay, 2, nil, 5*time.Second, noSleep)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	want := result{Report: outbox.Report{Read: 5, Published: 3, Marked: 3}, Retries: 2}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// Refusals that outlast the retry window fail the drain: an audit must not
// run over handoffs that were never published.
func TestDrainGivesUpAfterTheRetryWindow(t *testing.T) {
	refusing := make([]step, 1000)
	for i := range refusing {
		refusing[i] = step{outbox.Report{Read: 1}, nack("a")}
	}
	relay := &fakeRelay{steps: map[int][]step{0: refusing}}
	now := time.Now()
	clock := func(d time.Duration) { now = now.Add(d) }
	_, err := drainLoopAt(t.Context(), relay, 1, nil, 2*time.Second, clock, func() time.Time { return now })
	if !errors.Is(err, mq.ErrNack) {
		t.Fatalf("want the refusal after the retry window, got %v", err)
	}
	if relay.calls < 3 || relay.calls > 10 {
		t.Fatalf("want a few retries within 2 s at the retry pause, got %d steps", relay.calls)
	}
}

// Any other failure is not retried.
func TestDrainDoesNotRetryOtherErrors(t *testing.T) {
	boom := errors.New("pending outbox shard 0: unavailable")
	relay := &fakeRelay{steps: map[int][]step{0: {{outbox.Report{}, boom}, {outbox.Report{Read: 1, Published: 1}, nil}}}}
	_, err := drainLoop(t.Context(), relay, 1, nil, 5*time.Second, noSleep)
	if !errors.Is(err, boom) || relay.calls != 1 {
		t.Fatalf("want the first error unretried, got %v after %d steps", err, relay.calls)
	}
}
