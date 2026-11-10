package appmetrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Start owns a private listener and sampler. Parent cancellation starts their
// shutdown immediately, independently of the public API's readiness/drain grace.
func (p *Process) Start(parent context.Context, address string) (func() error, error) {
	listener, err := (&net.ListenConfig{}).Listen(parent, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("metrics listen: %w", err)
	}
	actualAddress := listener.Addr().String()
	p.address.Store(&actualAddress)
	ctx, cancel := context.WithCancel(parent)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", p.Handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 4 * time.Second, IdleTimeout: 15 * time.Second}
	served, sampled := make(chan error, 1), make(chan struct{})
	go func() { served <- server.Serve(listener) }()
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			_ = p.Sample(ctx) // Failure is exported as invalid/stale; never domain readiness.
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	stopped, requestStop := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var stopErr error
	go func() {
		defer close(stopped)
		alreadyServed := false
		select {
		case err = <-served:
			alreadyServed = true
		case <-ctx.Done():
		case <-requestStop:
		}
		cancel()
		shutdown, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		stopErr = server.Shutdown(shutdown)
		if stopErr != nil {
			stopErr = errors.Join(stopErr, server.Close())
		}
		_ = listener.Close()
		if !alreadyServed {
			err = <-served
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			stopErr = errors.Join(stopErr, err)
		}
		<-sampled
	}()
	return func() error { once.Do(func() { close(requestStop) }); <-stopped; return stopErr }, nil
}
