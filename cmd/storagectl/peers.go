package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// errHasState: the node refused a bootstrap because it already has Raft
// state (HTTP 409 from /mgmt/bootstrap).
var errHasState = errors.New("storagectl: node already has raft state")

// peerClient calls storage nodes' management endpoints.
type peerClient interface {
	Status(ctx context.Context, mgmtAddr string) (nodeStatus, error)
	Bootstrap(ctx context.Context, mgmtAddr string, voters []peerVoter) error
}

type peerVoter struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// httpPeers is the real client. Every call carries ctx and a per-call timeout.
type httpPeers struct {
	client *http.Client
	token  string
}

func newHTTPPeers(token string, timeout time.Duration) *httpPeers {
	return &httpPeers{client: &http.Client{Timeout: timeout}, token: token}
}

// mgmtAddr is the management endpoint of a peer: its Raft host, mgmt port.
func mgmtAddr(raftAddr, mgmtPort string) (string, error) {
	host, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("storagectl: peer address %q: %w", raftAddr, err)
	}
	return net.JoinHostPort(host, mgmtPort), nil
}

func (h *httpPeers) Status(ctx context.Context, addr string) (nodeStatus, error) {
	b, err := getStatus(ctx, h.client, addr)
	if err != nil {
		return nodeStatus{}, err
	}
	var s nodeStatus
	if err := json.Unmarshal(b, &s); err != nil {
		return nodeStatus{}, fmt.Errorf("storagectl: decode status from %s: %w", addr, err)
	}
	return s, nil
}

// getStatus returns the raw GET /mgmt/status body.
func getStatus(ctx context.Context, client *http.Client, addr string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/mgmt/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storagectl: status %s: %w", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("storagectl: status %s: %w", addr, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("storagectl: status %s: HTTP %d: %s", addr, resp.StatusCode, bytes.TrimSpace(b))
	}
	return b, nil
}

func (h *httpPeers) Bootstrap(ctx context.Context, addr string, voters []peerVoter) error {
	body, err := json.Marshal(map[string]any{"servers": voters})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/mgmt/bootstrap", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("storagectl: bootstrap %s: %w", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return fmt.Errorf("%w: %s: %s", errHasState, addr, bytes.TrimSpace(b))
	default:
		return fmt.Errorf("storagectl: bootstrap %s: HTTP %d: %s", addr, resp.StatusCode, bytes.TrimSpace(b))
	}
}
