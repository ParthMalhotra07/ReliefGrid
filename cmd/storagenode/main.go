// Command storagenode runs one replica of one shard: a raftstore.Node behind
// two HTTP listeners.
//
//	HTTP_ADDR (:8080)  data routes of contracts/internal.yaml for this shard
//	MGMT_ADDR (:8081)  management: status, bootstrap, liveness, readiness
//
// The management listener is separate so that M7 admission (S4.2) can keep a
// fresh disk on management only, with the data and Raft listeners closed.
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

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	cfg, err := loadConfig(os.Getenv)
	if err == nil {
		err = run(ctx, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "storagenode:", err)
		os.Exit(1)
	}
}

// config is read from the environment the StatefulSet sets.
type config struct {
	Raft           raftstore.Config
	HTTPAddr       string        // HTTP_ADDR, default :8080
	MgmtAddr       string        // MGMT_ADDR, default :8081
	HTTPPort       string        // port peers' data endpoints listen on, from HTTP_ADDR; used in leader hints
	RingPath       string        // RING_PATH, default /etc/reliefgrid/ring.yaml
	RequestTimeout time.Duration // REQUEST_TIMEOUT_MS, default 3000; the deadline on every Apply and read
	OperatorToken  string        // OPERATOR_TOKEN (a Secret); empty disables /mgmt/bootstrap
}

// loadConfig reads POD_NAME, SHARD, GROUP_ID (default shard-<SHARD>),
// DATA_DIR, FSM_DIR, RAFT_BIND (default :7000), RAFT_ADVERTISE (required,
// stable DNS host:port), PROFILE (demo or load, default demo) and the fields
// above.
func loadConfig(getenv func(string) string) (config, error) {
	get := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	var missing []string
	for _, k := range []string{"POD_NAME", "SHARD", "DATA_DIR", "FSM_DIR", "RAFT_ADVERTISE"} {
		if getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return config{}, fmt.Errorf("missing environment: %v", missing)
	}
	shard, err := strconv.Atoi(getenv("SHARD"))
	if err != nil || shard < 0 {
		return config{}, fmt.Errorf("SHARD %q must be a non-negative integer", getenv("SHARD"))
	}
	profile, err := raftstore.ProfileByName(get("PROFILE", "demo"))
	if err != nil {
		return config{}, err
	}
	timeoutMS, err := strconv.Atoi(get("REQUEST_TIMEOUT_MS", "3000"))
	if err != nil || timeoutMS <= 0 {
		return config{}, fmt.Errorf("REQUEST_TIMEOUT_MS %q must be a positive integer", getenv("REQUEST_TIMEOUT_MS"))
	}
	cfg := config{
		Raft: raftstore.Config{
			PodName: getenv("POD_NAME"), GroupID: get("GROUP_ID", fmt.Sprintf("shard-%d", shard)), Shard: shard,
			DataDir: getenv("DATA_DIR"), FSMDir: getenv("FSM_DIR"),
			BindAddr: get("RAFT_BIND", ":7000"), AdvertiseAddr: getenv("RAFT_ADVERTISE"), Profile: profile,
		},
		HTTPAddr:       get("HTTP_ADDR", ":8080"),
		MgmtAddr:       get("MGMT_ADDR", ":8081"),
		RingPath:       get("RING_PATH", "/etc/reliefgrid/ring.yaml"),
		RequestTimeout: time.Duration(timeoutMS) * time.Millisecond,
		OperatorToken:  getenv("OPERATOR_TOKEN"),
	}
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return config{}, fmt.Errorf("HTTP_ADDR %q: %w", cfg.HTTPAddr, err)
	}
	cfg.HTTPPort = port
	if _, _, err := net.SplitHostPort(cfg.MgmtAddr); err != nil {
		return config{}, fmt.Errorf("MGMT_ADDR %q: %w", cfg.MgmtAddr, err)
	}
	return cfg, nil
}

// run opens the node and serves both listeners until ctx ends.
func run(ctx context.Context, cfg config) error {
	rg, err := ring.Load(cfg.RingPath)
	if err != nil {
		return err
	}
	if cfg.Raft.Shard >= rg.Shards() {
		return fmt.Errorf("SHARD %d is outside the ring's %d shards", cfg.Raft.Shard, rg.Shards())
	}
	openCtx, cancel := context.WithTimeout(ctx, time.Minute)
	n, err := raftstore.Open(openCtx, cfg.Raft)
	cancel()
	if err != nil {
		return err
	}
	if cfg.OperatorToken == "" {
		slog.Warn("OPERATOR_TOKEN is not set; /mgmt/bootstrap is disabled")
	}
	s := &server{
		node: n, ring: rg, shard: cfg.Raft.Shard, podName: cfg.Raft.PodName, timeout: cfg.RequestTimeout,
		leaderHTTP:    func(a raft.ServerAddress) string { return httpAddrFor(a, cfg.HTTPPort) },
		operatorToken: cfg.OperatorToken,
	}
	servers := []*http.Server{
		{Addr: cfg.HTTPAddr, Handler: s.dataRoutes(), ReadHeaderTimeout: 5 * time.Second},
		{Addr: cfg.MgmtAddr, Handler: s.mgmtRoutes(), ReadHeaderTimeout: 5 * time.Second},
	}
	errc := make(chan error, len(servers))
	for _, srv := range servers {
		go func() { errc <- srv.ListenAndServe() }()
	}
	slog.Info("serving", "pod", cfg.Raft.PodName, "shard", cfg.Raft.Shard, "raft", n.Addr(),
		"data", cfg.HTTPAddr, "mgmt", cfg.MgmtAddr, "profile", cfg.Raft.Profile.Name, "ringVersion", rg.Version())

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, n.Close())
}
