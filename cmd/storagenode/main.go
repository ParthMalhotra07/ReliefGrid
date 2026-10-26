// Command storagenode runs one replica of one shard: a raftstore.Node behind
// two HTTP listeners.
//
//	HTTP_ADDR (:8080)  data routes of contracts/internal.yaml for this shard
//	MGMT_ADDR (:8081)  management: status, bootstrap, liveness, readiness
//
// With COORDINATOR_URL set it registers with the coordinator and heartbeats
// every HEARTBEAT_INTERVAL_MS (1000), advertising HTTP_ADVERTISE (default:
// RAFT_ADVERTISE's host on the HTTP_ADDR port). Without it the node serves
// but the coordinator cannot route to it.
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
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode"
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
	Heartbeat      storagenode.HeartbeatConfig
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
	hb := &cfg.Heartbeat
	hb.CoordinatorURL = getenv("COORDINATOR_URL")
	if u, err := url.Parse(hb.CoordinatorURL); hb.CoordinatorURL != "" && (err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Host == "") {
		return config{}, fmt.Errorf("COORDINATOR_URL %q must be an http(s) URL", hb.CoordinatorURL)
	}
	hb.Address = get("HTTP_ADVERTISE", storagenode.HTTPAddrFor(raft.ServerAddress(cfg.Raft.AdvertiseAddr), port))
	if _, _, err := net.SplitHostPort(hb.Address); err != nil {
		return config{}, fmt.Errorf("HTTP_ADVERTISE %q: %w", hb.Address, err)
	}
	intervalMS, err := strconv.Atoi(get("HEARTBEAT_INTERVAL_MS", "1000"))
	if err != nil || intervalMS <= 0 {
		return config{}, fmt.Errorf("HEARTBEAT_INTERVAL_MS %q must be a positive integer", getenv("HEARTBEAT_INTERVAL_MS"))
	}
	hb.Interval = time.Duration(intervalMS) * time.Millisecond
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
	s := storagenode.New(n, storagenode.Config{
		Ring: rg, Shard: cfg.Raft.Shard, PodName: cfg.Raft.PodName, Timeout: cfg.RequestTimeout,
		LeaderHTTP:    func(a raft.ServerAddress) string { return storagenode.HTTPAddrFor(a, cfg.HTTPPort) },
		OperatorToken: cfg.OperatorToken,
	})
	servers := []*http.Server{
		{Addr: cfg.HTTPAddr, Handler: s.DataRoutes(), ReadHeaderTimeout: 5 * time.Second},
		{Addr: cfg.MgmtAddr, Handler: s.MgmtRoutes(), ReadHeaderTimeout: 5 * time.Second},
	}
	errc := make(chan error, len(servers))
	for _, srv := range servers {
		go func() { errc <- srv.ListenAndServe() }()
	}
	hbCtx, stopHeartbeats := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	if cfg.Heartbeat.CoordinatorURL == "" {
		slog.Warn("COORDINATOR_URL is not set; not registering with the coordinator")
		close(hbDone)
	} else {
		go func() {
			defer close(hbDone)
			_ = s.RunHeartbeats(hbCtx, cfg.Heartbeat)
		}()
	}
	slog.Info("serving", "pod", cfg.Raft.PodName, "shard", cfg.Raft.Shard, "raft", n.Addr(),
		"data", cfg.HTTPAddr, "mgmt", cfg.MgmtAddr, "profile", cfg.Raft.Profile.Name, "ringVersion", rg.Version())

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	stopHeartbeats()
	<-hbDone // before the node closes under it
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
