package workflow

import (
	"context"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"reflect"
	"testing"
	"time"
)

func TestSecondWakeAfterUnknownReusesCommand(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("wake-redrive"))
	drop := false
	wrapped := &lostWake{Store: memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		return memstore.Fault{DropBefore: drop && c.Method == "RetryWorkflow"}
	})}
	clock := testNow
	cfg := testConfig(wrapped, 0)
	cfg.Now = func() time.Time { return clock }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Match(ctx, "wake-redrive"); err != nil {
		t.Fatal(err)
	}
	drop = true
	clock = clock.Add(time.Second)
	source := clock.UnixMilli()
	if err = m.Wake(ctx, "wake-redrive", "same-event", source); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatal(err)
	}
	clock = clock.Add(time.Second)
	drop = false
	if err = m.Wake(ctx, "wake-redrive", "same-event", source); err != nil {
		t.Fatal(err)
	}
	if len(wrapped.commands) < 3 {
		t.Fatal("second Wake was not driven")
	}
	for _, c := range wrapped.commands {
		if !reflect.DeepEqual(c, wrapped.commands[0]) {
			t.Fatalf("redrive changed command: %+v", wrapped.commands)
		}
	}
}

func TestRetryCloseRestartUsesSameCommand(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[applied], func(t *testing.T) {
			ctx, raw := testStore(t)
			createResource(t, ctx, raw, resource("r1", 0))
			createRequest(t, ctx, raw, request("retry-restart"))
			trace := &traceStore{Store: raw}
			trace.afterClaim = func(context.Context, store.TryClaim, store.Result, error) (store.Result, error) {
				return store.Result{}, store.ErrUnknownOutcome
			}
			m, err := New(testConfig(trace, 0))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Match(ctx, "retry-restart"); !errors.Is(err, store.ErrUnknownOutcome) {
				t.Fatal(err)
			}
			drop := true
			s := &cleanupTrace{Store: memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
				return memstore.Fault{DropBefore: drop && !applied && c.Method == "CloseAttempt", DropResponse: drop && applied && c.Method == "CloseAttempt"}
			})}
			cfg := testConfig(s, 0)
			m, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Abandon(ctx, "retry-restart"); !errors.Is(err, store.ErrUnknownOutcome) {
				t.Fatal(err)
			}
			drop = false
			cfg.Now = func() time.Time { return testNow.Add(time.Hour) }
			restarted, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = restarted.Abandon(ctx, "retry-restart"); err != nil {
				t.Fatal(err)
			}
			for _, cmd := range s.closes {
				if !reflect.DeepEqual(cmd, s.closes[0]) {
					t.Fatal("restart changed retry-close command")
				}
			}
			rec := read(t, ctx, raw, store.RequestKey("retry-restart"))
			if rec.Request.Status != store.RequestWaiting || rec.Request.CurrentAttempt != nil {
				t.Fatalf("retry did not close: %+v", rec.Request)
			}
		})
	}
}
