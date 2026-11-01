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
// M7 admission: before the Raft transport opens, the replica reads its
// shard's manifest (MANIFEST_NAME in POD_NAMESPACE) and waits until the disk
// is admitted (internal/admission). Until then only the management listener
// serves (state Unadmitted); the Raft and data listeners are not open.
// ADMISSION=off skips the gate for tests and local runs only; the chart
// never sets it.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/admission"
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

	// M7 admission. ManifestName (MANIFEST_NAME) and Namespace
	// (POD_NAMESPACE) name the shard's manifest; run reads it in-cluster
	// unless Admission is already set (tests). AdmissionOff (ADMISSION=off)
	// skips the gate entirely.
	ManifestName  string
	Namespace     string
	Admission     admission.Source
	AdmissionPoll time.Duration
	AdmissionOff  bool
}

// loadConfig reads POD_NAME, SHARD, GROUP_ID (default shard-<SHARD>),
// DATA_DIR, FSM_DIR, RAFT_BIND (default :7000), RAFT_ADVERTISE (required,
// stable DNS host:port), PROFILE (demo or load, default demo),
// STRONG_READ_FENCE (fence or barrier, default fence: how
// ?consistency=linearizable is served) and the fields above.
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
	fence := raftstore.ReadFence(get("STRONG_READ_FENCE", string(raftstore.ReadFenceIndex)))
	if fence != raftstore.ReadFenceIndex && fence != raftstore.ReadFenceBarrier {
		return config{}, fmt.Errorf("STRONG_READ_FENCE %q: want %s or %s", fence, raftstore.ReadFenceIndex, raftstore.ReadFenceBarrier)
	}
	timeoutMS, err := strconv.Atoi(get("REQUEST_TIMEOUT_MS", "3000"))
	if err != nil || timeoutMS <= 0 {
		return config{}, fmt.Errorf("REQUEST_TIMEOUT_MS %q must be a positive integer", getenv("REQUEST_TIMEOUT_MS"))
	}
	cfg := config{
		Raft: raftstore.Config{
			PodName: getenv("POD_NAME"), GroupID: get("GROUP_ID", fmt.Sprintf("shard-%d", shard)), Shard: shard,
			DataDir: getenv("DATA_DIR"), FSMDir: getenv("FSM_DIR"),
			BindAddr: get("RAFT_BIND", ":7000"), AdvertiseAddr: getenv("RAFT_ADVERTISE"), Profile: profile, ReadFence: fence,
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

	switch getenv("ADMISSION") {
	case "off":
		cfg.AdmissionOff = true
	case "":
		cfg.ManifestName, cfg.Namespace = getenv("MANIFEST_NAME"), getenv("POD_NAMESPACE")
		if cfg.ManifestName == "" || cfg.Namespace == "" {
			return config{}, fmt.Errorf("MANIFEST_NAME and POD_NAMESPACE are required for M7 admission (ADMISSION=off skips it, for tests and local runs only)")
		}
		cfg.AdmissionPoll = 2 * time.Second
	default:
		return config{}, fmt.Errorf("ADMISSION %q: the only value is off", getenv("ADMISSION"))
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
	if cfg.AdmissionOff {
		slog.Warn("!!! ADMISSION=off: M7 admission is DISABLED; an empty or wiped disk opens Raft at once. Tests and local runs only, never a cluster. !!!")
	} else if cfg.Admission == nil {
		if cfg.Admission, err = admission.InCluster(cfg.Namespace, cfg.ManifestName); err != nil {
			return err
		}
	}
	if cfg.AdmissionPoll <= 0 {
		cfg.AdmissionPoll = 2 * time.Second
	}

	// Management serves from the start: gated while the disk waits for
	// admission, then the full endpoint.
	var mgmt atomic.Pointer[http.Handler]
	starting := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "starting", http.StatusServiceUnavailable)
	}))
	mgmt.Store(&starting)
	mgmtSrv := &http.Server{Addr: cfg.MgmtAddr, ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*mgmt.Load()).ServeHTTP(w, r) })}
	errc := make(chan error, 2)
	go func() { errc <- mgmtSrv.ListenAndServe() }()

	gateCtx, stopGate := context.WithCancel(ctx)
	defer stopGate()
	if cfg.Admission != nil {
		cfg.Raft.Admit = func(ctx context.Context, id raftstore.Identity, hasState bool) error {
			g := storagenode.NewGated(id, hasState, storagenode.GatedConfig{RingVersion: rg.Version(), Address: cfg.Raft.AdvertiseAddr, OperatorToken: cfg.OperatorToken})
			h := g.MgmtRoutes()
			mgmt.Store(&h)
			if cfg.Heartbeat.CoordinatorURL != "" {
				go func() { _ = g.RunHeartbeats(gateCtx, cfg.Heartbeat) }()
			}
			last := ""
			err := admission.Wait(ctx, cfg.Admission, id, hasState, cfg.AdmissionPoll, func(why string) {
				g.SetReason(why)
				if why != last {
					slog.Warn("waiting for admission (M7)", "serverId", id.ServerID(), "reason", why)
					last = why
				}
			})
			stopGate()
			if err == nil {
				slog.Info("admitted", "serverId", id.ServerID(), "hasState", hasState)
			}
			return err
		}
	}
	n, err := raftstore.Open(ctx, cfg.Raft)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgmtSrv.Shutdown(shutdownCtx)
		if ctx.Err() != nil {
			return nil // stopped while waiting for admission
		}
		return err
	}
	if cfg.OperatorToken == "" {
		slog.Warn("OPERATOR_TOKEN is not set; operator endpoints (/mgmt/bootstrap, /mgmt/snapshot, /mgmt/dump) are disabled")
	}
	s := storagenode.New(n, storagenode.Config{
		Ring: rg, Shard: cfg.Raft.Shard, PodName: cfg.Raft.PodName, Timeout: cfg.RequestTimeout,
		LeaderHTTP:    func(a raft.ServerAddress) string { return storagenode.HTTPAddrFor(a, cfg.HTTPPort) },
		OperatorToken: cfg.OperatorToken,
	})
	full := s.MgmtRoutes()
	mgmt.Store(&full)
	dataSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: s.DataRoutes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { errc <- dataSrv.ListenAndServe() }()
	servers := []*http.Server{dataSrv, mgmtSrv}
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
