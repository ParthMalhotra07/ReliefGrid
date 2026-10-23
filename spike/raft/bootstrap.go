package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// Bootstrap is a one-time manual step. Two guards stop a second cluster:
//   - the Job refuses if any peer already has Raft state (term or log);
//   - the pod refuses if it has state itself: BootstrapCluster returns
//     ErrCantBootstrap when HasExistingState is true.
//
// The real system adds the manifest ConfigMap and admission gate (M7).

type bootstrapRequest struct {
	Servers []serverJSON `json:"servers"`
}

func (n *node) handleBootstrap(w http.ResponseWriter, req *http.Request) {
	var body bootstrapRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil || len(body.Servers) == 0 {
		writeError(w, http.StatusBadRequest, "body must list servers")
		return
	}
	var cfg raft.Configuration
	for _, s := range body.Servers {
		cfg.Servers = append(cfg.Servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(s.ID), Address: raft.ServerAddress(s.Address)})
	}
	if err := n.raft.BootstrapCluster(cfg).Error(); err != nil {
		if errors.Is(err, raft.ErrCantBootstrap) {
			writeError(w, http.StatusConflict, "refused: "+n.cfg.pod+" already has Raft state; bootstrap only works on a new group")
			return
		}
		writeError(w, http.StatusInternalServerError, "bootstrap: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"bootstrapped": n.cfg.pod})
}

func bootstrapBody(c config) bootstrapRequest {
	var body bootstrapRequest
	for _, s := range c.peers() {
		body.Servers = append(body.Servers, serverJSON{ID: string(s.ID), Address: string(s.Address), Suffrage: s.Suffrage.String()})
	}
	return body
}

func postBootstrap(ctx context.Context, url string, body bootstrapRequest) (int, string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/bootstrap", bytes.NewReader(b))
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if msg := out["error"]; msg != "" {
		return resp.StatusCode, msg, nil
	}
	return resp.StatusCode, out["bootstrapped"], nil
}

// bootstrapJob waits for every peer's HTTP endpoint (by DNS name, not readiness),
// refuses if any has Raft state, bootstraps ordinal 0 with the explicit
// configuration and waits until a leader has committed it.
func bootstrapJob(ctx context.Context, c config) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	peers := c.peers()
	for _, p := range peers {
		s, err := waitStatus(ctx, c.httpURL(string(p.ID)))
		if err != nil {
			return fmt.Errorf("peer %s: %w", p.ID, err)
		}
		if s.Term > 0 || s.LastIndex > 0 {
			return fmt.Errorf("refused: %s already has Raft state (term %d, last index %d); not bootstrapping a second cluster", p.ID, s.Term, s.LastIndex)
		}
		fmt.Printf("peer %s reachable, no Raft state\n", p.ID)
	}
	target := string(peers[0].ID)
	code, msg, err := postBootstrap(ctx, c.httpURL(target), bootstrapBody(c))
	if err != nil {
		return fmt.Errorf("bootstrap %s: %w", target, err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("bootstrap %s: HTTP %d: %s", target, code, msg)
	}
	fmt.Printf("bootstrapped %s with %d voters\n", target, len(peers))
	for {
		for _, p := range peers {
			s, err := getStatus(ctx, c.httpURL(string(p.ID)))
			if err == nil && s.State == raft.Leader.String() && s.CommitIndex >= 1 {
				fmt.Printf("leader %s at term %d, commit index %d\n", s.ID, s.Term, s.CommitIndex)
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("no leader committed the configuration: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func waitStatus(ctx context.Context, url string) (statusJSON, error) {
	for {
		s, err := getStatus(ctx, url)
		if err == nil {
			return s, nil
		}
		select {
		case <-ctx.Done():
			return statusJSON{}, fmt.Errorf("not reachable: %w (last error: %w)", ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

func getStatus(ctx context.Context, url string) (statusJSON, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/status", nil)
	if err != nil {
		return statusJSON{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return statusJSON{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return statusJSON{}, fmt.Errorf("status: HTTP %d", resp.StatusCode)
	}
	var s statusJSON
	return s, json.NewDecoder(resp.Body).Decode(&s)
}
