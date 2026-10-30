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

// errRejected: the node answered the bootstrap with 400, 401 or 403. That is
// a definite answer (nothing was applied), not a lost response, so the Job
// fails at once instead of re-reading until its deadline.
var errRejected = errors.New("storagectl: bootstrap request rejected")

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
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %s: HTTP %d: %s", errRejected, addr, resp.StatusCode, bytes.TrimSpace(b))
	default:
		return fmt.Errorf("storagectl: bootstrap %s: HTTP %d: %s", addr, resp.StatusCode, bytes.TrimSpace(b))
	}
}

// Membership sends one change to POST /mgmt/membership on the leader.
func (h *httpPeers) Membership(ctx context.Context, addr string, c membershipChange) (uint64, error) {
	body, err := json.Marshal(c)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/mgmt/membership", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("storagectl: membership %s %s at %s: %w", c.Op, c.ID, addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == http.StatusOK:
		var out struct {
			Index uint64 `json:"index"`
		}
		if err := json.Unmarshal(b, &out); err != nil {
			return 0, fmt.Errorf("storagectl: membership answer from %s: %w", addr, err)
		}
		return out.Index, nil
	case resp.StatusCode == http.StatusConflict && bytes.Contains(b, []byte("NOT_LEADER")):
		return 0, fmt.Errorf("%w: %s: %s", errNotLeader, addr, bytes.TrimSpace(b))
	case resp.StatusCode == http.StatusConflict && bytes.Contains(b, []byte("CONFIG_CHANGED")):
		return 0, fmt.Errorf("%w: %s: %s", errConfigChanged, addr, bytes.TrimSpace(b))
	}
	return 0, fmt.Errorf("storagectl: membership %s %s at %s: HTTP %d: %s", c.Op, c.ID, addr, resp.StatusCode, bytes.TrimSpace(b))
}

// dial opens and closes a TCP connection to addr (storagectl dial): a
// reachability check from inside a distroless pod, which has no shell.
func dial(ctx context.Context, addr string) error {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("storagectl: dial %s: %w", addr, err)
	}
	return c.Close()
}
