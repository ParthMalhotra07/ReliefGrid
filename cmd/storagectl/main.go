// Command storagectl is the storage operator tool.
//
//	storagectl bootstrap   one-time group bootstrap, run by the chart's Job
//	                       (v3.1 §10). Environment: POD_NAMESPACE, MANIFESTS
//	                       (JSON list, in shard order, of {name, groupId,
//	                       shard, ringVersion, peers}: each manifest is created
//	                       as PENDING if absent, and an existing one is used
//	                       as it is), OPERATOR_TOKEN, MGMT_PORT (8081),
//	                       HOLD_SECONDS (0), BOOTSTRAP_TIMEOUT_SECONDS (600).
//	                       Exit 2: refused.
//	storagectl status [--addr host:port] [--timeout d]
//	                       GET /mgmt/status (default localhost:8081) as JSON.
//	storagectl dial --addr host:port [--timeout d]
//	                       exit 0 if a TCP connection opens (NetworkPolicy checks).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const exitRefused = 2

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "bootstrap":
		err = runBootstrap(ctx, os.Getenv)
	case "status":
		err = runStatus(ctx, os.Args[2:])
	case "dial":
		err = runDial(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "storagectl:", err)
		if errors.Is(err, errRefused) {
			os.Exit(exitRefused)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: storagectl bootstrap | storagectl status [--addr host:port] [--timeout d] | storagectl dial --addr host:port [--timeout d]")
	os.Exit(64)
}

func runBootstrap(ctx context.Context, getenv func(string) string) error {
	ns, token := getenv("POD_NAMESPACE"), getenv("OPERATOR_TOKEN")
	if ns == "" || token == "" {
		return errors.New("POD_NAMESPACE and OPERATOR_TOKEN are required")
	}
	specs, err := parseSpecs(getenv("MANIFESTS"))
	if err != nil {
		return err
	}
	envInt := func(k string, def int) (int, error) {
		v := getenv(k)
		if v == "" {
			return def, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("%s %q must be a non-negative integer", k, v)
		}
		return n, nil
	}
	hold, err := envInt("HOLD_SECONDS", 0)
	if err != nil {
		return err
	}
	timeout, err := envInt("BOOTSTRAP_TIMEOUT_SECONDS", 600)
	if err != nil {
		return err
	}
	port := getenv("MGMT_PORT")
	if port == "" {
		port = "8081"
	}
	store, err := inClusterStore(ns)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	b := &bootstrapper{
		store: store, peers: newHTTPPeers(token, 5*time.Second), mgmtPort: port,
		poll: time.Second, call: 5 * time.Second, hold: time.Duration(hold) * time.Second,
		log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	return b.run(ctx, specs)
}

// spec is one MANIFESTS entry: what the chart knows about a shard's group.
type spec struct {
	Name        string `json:"name"`
	GroupID     string `json:"groupId"`
	Shard       int    `json:"shard"`
	RingVersion uint64 `json:"ringVersion"`
	Peers       []peer `json:"peers"`
}

// parseSpecs reads MANIFESTS into PENDING manifests, validated as the
// manifest itself would be.
func parseSpecs(v string) ([]manifest, error) {
	var specs []spec
	if err := json.Unmarshal([]byte(v), &specs); err != nil {
		return nil, fmt.Errorf("MANIFESTS: %w", err)
	}
	if len(specs) == 0 {
		return nil, errors.New("MANIFESTS lists no manifests")
	}
	seen := map[string]bool{}
	ms := make([]manifest, 0, len(specs))
	for _, s := range specs {
		if s.Name == "" || seen[s.Name] {
			return nil, fmt.Errorf("MANIFESTS: name %q is empty or repeated", s.Name)
		}
		seen[s.Name] = true
		m := manifest{Name: s.Name, GroupID: s.GroupID, Shard: s.Shard, RingVersion: s.RingVersion, Peers: s.Peers, Phase: phasePending}
		if _, err := parseManifest(m.Name, "", m.data()); err != nil {
			return nil, fmt.Errorf("MANIFESTS: %w", err)
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func runDial(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dial", flag.ContinueOnError)
	addr := fs.String("addr", "", "host:port to connect to")
	timeout := fs.Duration("timeout", 3*time.Second, "deadline for the connection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" {
		return errors.New("dial: --addr is required")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	return dial(ctx, *addr)
}

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:8081", "management endpoint host:port")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for the call")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	b, err := getStatus(ctx, &http.Client{}, *addr)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}
