// Command raft-spike is the S0.3 throwaway experiment: one HashiCorp Raft group of
// three StatefulSet pods with a SQLite CAS state machine. See FINDINGS.md.
//
//	raft-spike serve          run a node (StatefulSet container)
//	raft-spike bootstrap-job  bootstrap a fresh group once (Job)
//	raft-spike ctl ...        talk to the local node (kubectl exec); see ctl.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raft-spike serve | bootstrap-job | ctl <command>")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(ctx, loadConfig())
	case "bootstrap-job":
		err = bootstrapJob(ctx, loadConfig())
	case "ctl":
		stop()
		os.Exit(ctl(loadConfig(), os.Args[2:]))
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type config struct {
	pod, namespace, service string
	replicas                int
	raftPort, httpPort      int
	dataDir, fsmPath        string
	// Zero keeps the library default.
	snapshotThreshold, trailingLogs uint64
	snapshotInterval                time.Duration
}

func loadConfig() config {
	return config{
		pod:               os.Getenv("POD_NAME"),
		namespace:         env("POD_NAMESPACE", "default"),
		service:           env("SERVICE", "storage-spike"),
		replicas:          int(envUint("REPLICAS", 3)),
		raftPort:          int(envUint("RAFT_PORT", 7000)),
		httpPort:          int(envUint("HTTP_PORT", 8080)),
		dataDir:           env("DATA_DIR", "/data"),
		fsmPath:           env("FSM_PATH", "/fsm/fsm.db"),
		snapshotThreshold: envUint("SNAPSHOT_THRESHOLD", 0),
		trailingLogs:      envUint("TRAILING_LOGS", 0),
		snapshotInterval:  time.Duration(envUint("SNAPSHOT_INTERVAL_SECONDS", 0)) * time.Second,
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envUint(key string, def uint64) uint64 {
	n, err := strconv.ParseUint(os.Getenv(key), 10, 64)
	if err != nil {
		return def
	}
	return n
}

// host is a pod's stable DNS name through the headless Service.
func (c config) host(pod string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod, c.service, c.namespace)
}

func (c config) raftAddr(pod string) string { return fmt.Sprintf("%s:%d", c.host(pod), c.raftPort) }
func (c config) httpURL(pod string) string {
	return fmt.Sprintf("http://%s:%d", c.host(pod), c.httpPort)
}

// peers is the explicit three-voter configuration: StatefulSet ordinals at their DNS names.
func (c config) peers() []raft.Server {
	servers := make([]raft.Server, c.replicas)
	for i := range servers {
		pod := fmt.Sprintf("%s-%d", c.service, i)
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(pod), Address: raft.ServerAddress(c.raftAddr(pod))}
	}
	return servers
}

func serve(ctx context.Context, c config) error {
	if c.pod == "" {
		return errors.New("POD_NAME is not set")
	}
	f, err := openFSM(c.fsmPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := os.MkdirAll(c.dataDir, 0o750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(c.dataDir, "raft.db")})
	if err != nil {
		return fmt.Errorf("open bolt store: %w", err)
	}
	defer func() { _ = store.Close() }()
	snaps, err := raft.NewFileSnapshotStore(c.dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}
	trans, err := newDNSTransport(fmt.Sprintf(":%d", c.raftPort), c.raftAddr(c.pod), os.Stderr)
	if err != nil {
		return err
	}

	rc := raft.DefaultConfig() // default heartbeat/election/lease timeouts on purpose
	rc.LocalID = raft.ServerID(c.pod)
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Info, Output: os.Stderr})
	if c.snapshotThreshold > 0 {
		rc.SnapshotThreshold = c.snapshotThreshold
	}
	if c.trailingLogs > 0 {
		rc.TrailingLogs = c.trailingLogs
	}
	if c.snapshotInterval > 0 {
		rc.SnapshotInterval = c.snapshotInterval
	}
	// NewRaft never bootstraps: a fresh disk waits with an empty configuration
	// until the bootstrap Job asks one pod to bootstrap (bootstrap.go).
	r, err := raft.NewRaft(rc, f, store, store, snaps, trans)
	if err != nil {
		return fmt.Errorf("start raft: %w", err)
	}

	n := &node{cfg: c, raft: r, fsm: f}
	srv := &http.Server{Addr: fmt.Sprintf(":%d", c.httpPort), Handler: n.routes(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("serving", "pod", c.pod, "advertise", c.raftAddr(c.pod), "http", srv.Addr)

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	// No LeadershipTransfer on SIGTERM: the spike measures failure detection, as
	// after a crash, not a graceful handover.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if serr := r.Shutdown().Error(); serr != nil {
		slog.Error("raft shutdown", "err", serr)
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
