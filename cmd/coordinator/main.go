// Command coordinator serves contracts/internal.yaml: storage-node
// registration and heartbeats, routing to shard leaders, scatter-gather scans
// and the /admin views. It loads the fixed ring and refuses to start if its
// checksum does not match.
//
//	HTTP_ADDR           (:8080)
//	RING_PATH           (/etc/reliefgrid/ring.yaml)
//	OPERATOR_TOKEN      bearer token for /admin; empty disables /admin (403)
//	SUSPECT_AFTER_MS    (3000)
//	DOWN_AFTER_MS       (5000)
//	BUDGET_MS           (4000)  one routed request, retries included
//	ATTEMPT_TIMEOUT_MS  (3500, capped at BUDGET_MS)  one forward to a storage node
//
// BUDGET_MS must be below the 5 s HTTP budget and ATTEMPT_TIMEOUT_MS must
// not exceed it (plan §14).
//
// GET /healthz answers 200 while the process serves (not in the contract).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	err := withTracing(ctx, "coordinator", func() error {
		cfg, err := loadConfig(os.Getenv)
		if err != nil {
			return err
		}
		return run(ctx, cfg, nil)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "coordinator:", err)
		os.Exit(1)
	}
}

// config is read from the environment the Deployment sets.
type config struct {
	HTTPAddr    string
	RingPath    string
	Coordinator coordinator.Config // without Ring; run loads it from RingPath
}

func loadConfig(getenv func(string) string) (config, error) {
	get := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	ms := func(k string, def int) (time.Duration, error) {
		n, err := strconv.Atoi(get(k, strconv.Itoa(def)))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("%s %q must be a positive integer (milliseconds)", k, getenv(k))
		}
		return time.Duration(n) * time.Millisecond, nil
	}
	cfg := config{HTTPAddr: get("HTTP_ADDR", ":8080"), RingPath: get("RING_PATH", "/etc/reliefgrid/ring.yaml")}
	if _, _, err := net.SplitHostPort(cfg.HTTPAddr); err != nil {
		return config{}, fmt.Errorf("HTTP_ADDR %q: %w", cfg.HTTPAddr, err)
	}
	c := &cfg.Coordinator
	c.OperatorToken = getenv("OPERATOR_TOKEN")
	var err error
	for _, d := range []struct {
		key string
		def int
		to  *time.Duration
	}{
		{"SUSPECT_AFTER_MS", 3000, &c.SuspectAfter},
		{"DOWN_AFTER_MS", 5000, &c.DownAfter},
		{"BUDGET_MS", 4000, &c.Budget},
	} {
		if *d.to, err = ms(d.key, d.def); err != nil {
			return config{}, err
		}
	}
	defAttempt := min(coordinator.DefaultAttemptTimeout, c.Budget)
	if c.AttemptTimeout, err = ms("ATTEMPT_TIMEOUT_MS", int(defAttempt.Milliseconds())); err != nil {
		return config{}, err
	}
	if err := coordinator.ValidateBudgets(c.Budget, c.AttemptTimeout); err != nil {
		return config{}, err
	}
	if c.SuspectAfter >= c.DownAfter {
		return config{}, fmt.Errorf("SUSPECT_AFTER_MS (%s) must be below DOWN_AFTER_MS (%s)", c.SuspectAfter, c.DownAfter)
	}
	return cfg, nil
}

// run loads the ring, refusing a checksum mismatch, and serves until ctx
// ends. If ready is not nil it receives the listen address once serving.
func run(ctx context.Context, cfg config, ready chan<- string) error {
	rg, err := ring.Load(cfg.RingPath)
	if err != nil {
		return fmt.Errorf("refusing to start: %w", err)
	}
	cc := cfg.Coordinator
	cc.Ring = rg
	c, err := coordinator.New(cc)
	if err != nil {
		return err
	}
	if cc.OperatorToken == "" {
		slog.Warn("OPERATOR_TOKEN is not set; /admin is disabled")
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/", c.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	slog.Info("serving", "addr", ln.Addr().String(), "ringVersion", rg.Version(), "checksum", rg.Checksum())
	if ready != nil {
		ready <- ln.Addr().String()
	}

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

// withTracing runs fn with tracing set up from the standard OTEL_*
// variables (S4.4): with no OTLP endpoint only the W3C propagator, so
// traceparent still flows. Spans are flushed when fn returns.
func withTracing(ctx context.Context, service string, fn func() error) error {
	enabled := obs.EnabledFromEnv()
	shutdown, err := obs.Init(ctx, obs.Config{ServiceName: service, Enabled: enabled})
	if err != nil {
		return err
	}
	slog.Info("tracing", "enabled", enabled)
	err = fn()
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := shutdown(flushCtx); serr != nil {
		slog.Warn("flushing traces", "err", serr)
	}
	return err
}
