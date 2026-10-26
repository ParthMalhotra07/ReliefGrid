// Command storagectl is the storage operator tool.
//
//	storagectl bootstrap   one-time group bootstrap, run by the chart's Job
//	                       (v3.1 §10). Environment: POD_NAMESPACE, MANIFESTS
//	                       (comma-separated manifest ConfigMaps, in order),
//	                       OPERATOR_TOKEN, MGMT_PORT (8081), HOLD_SECONDS (0),
//	                       BOOTSTRAP_TIMEOUT_SECONDS (600). Exit 2: refused.
//	storagectl status [--addr host:port] [--timeout d]
//	                       GET /mgmt/status (default localhost:8081) as JSON.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	fmt.Fprintln(os.Stderr, "usage: storagectl bootstrap | storagectl status [--addr host:port] [--timeout d]")
	os.Exit(64)
}

func runBootstrap(ctx context.Context, getenv func(string) string) error {
	ns, token := getenv("POD_NAMESPACE"), getenv("OPERATOR_TOKEN")
	var names []string
	for n := range strings.SplitSeq(getenv("MANIFESTS"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, n)
		}
	}
	if ns == "" || token == "" || len(names) == 0 {
		return errors.New("POD_NAMESPACE, OPERATOR_TOKEN and MANIFESTS are required")
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
	return b.run(ctx, names)
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
