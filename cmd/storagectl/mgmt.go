package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

// runSnapshot is storagectl snapshot: POST /mgmt/snapshot with the pod's
// OPERATOR_TOKEN, printing the node's JSON answer.
func runSnapshot(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	return operatorCall(ctx, "snapshot", http.MethodPost, "/mgmt/snapshot", 60*time.Second, args, getenv, out)
}

// runDump is storagectl dump: GET /mgmt/dump with the pod's OPERATOR_TOKEN,
// printing the node's JSON digest of its local FSM.
func runDump(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	return operatorCall(ctx, "dump", http.MethodGet, "/mgmt/dump", 30*time.Second, args, getenv, out)
}

// operatorCall makes one operator-token call to a management endpoint
// (default localhost:8081) and copies a 200 body to out; any other status
// is an error carrying the code and body.
func operatorCall(ctx context.Context, name, method, path string, defTimeout time.Duration, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	addr := fs.String("addr", "localhost:8081", "management endpoint host:port")
	timeout := fs.Duration("timeout", defTimeout, "deadline for the call")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := getenv("OPERATOR_TOKEN")
	if token == "" {
		return errors.New(name + ": OPERATOR_TOKEN is required")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+*addr+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("storagectl: %s %s: %w", name, *addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("storagectl: %s %s: %w", name, *addr, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("storagectl: %s %s: HTTP %d: %s", name, *addr, resp.StatusCode, bytes.TrimSpace(b))
	}
	_, err = fmt.Fprintln(out, string(bytes.TrimSpace(b)))
	return err
}
