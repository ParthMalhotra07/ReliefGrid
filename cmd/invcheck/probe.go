package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// probeReadTimeout bounds one direct read: an isolated leader answers by its
// lease or the deadline, whichever is first (S4.1), so a hung read is a
// failed read, not a stalled probe.
const probeReadTimeout = 3 * time.Second

// probeTarget is one --probe SHARD=URL: a storage node read directly,
// bypassing the coordinator, for the keys its shard owns.
type probeTarget struct {
	shard int
	base  string // http://host:port, no path
	name  string // the host's first label: storage-shard-0-0
}

// probeFlags is load's repeatable --probe.
type probeFlags []probeTarget

func (p *probeFlags) String() string {
	var parts []string
	for _, t := range *p {
		parts = append(parts, fmt.Sprintf("%d=%s", t.shard, t.base))
	}
	return strings.Join(parts, ",")
}

func (p *probeFlags) Set(v string) error {
	s, raw, ok := strings.Cut(v, "=")
	shard, err := strconv.Atoi(s)
	if !ok || err != nil || shard < 0 {
		return fmt.Errorf("want SHARD=URL, e.g. 0=http://storage-shard-0-0.storage-shard-0.reliefgrid.svc.cluster.local:8080")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.Trim(u.Path, "/") != "" || u.RawQuery != "" {
		return fmt.Errorf("probe URL %q: want http(s)://host:port with no path or query", raw)
	}
	name, _, _ := strings.Cut(u.Hostname(), ".")
	*p = append(*p, probeTarget{shard: shard, base: u.Scheme + "://" + u.Host, name: name})
	return nil
}

// owner maps keys to shards with the ring the coordinator served, checked
// against its published checksum.
func owner(r internalapi.Ring) (func(key string) (int, error), error) {
	rg, err := ring.New(ring.Config{RingVersion: r.RingVersion, Hash: string(r.Hash), Shards: r.Shards,
		VNodesPerShard: r.VnodesPerShard, Checksum: r.Checksum})
	if err != nil {
		return nil, fmt.Errorf("invcheck: ring: %w", err)
	}
	return rg.Owner, nil
}

// probes builds the Driver's probes: each reads the update keys its shard
// owns straight from its node, in mode.
func probes(targets probeFlags, shardOf func(string) (int, error), mode string, hc *http.Client) []ledger.Probe {
	out := make([]ledger.Probe, 0, len(targets))
	for _, t := range targets {
		out = append(out, ledger.Probe{
			Name: t.name,
			Owns: func(key string) bool { s, err := shardOf(key); return err == nil && s == t.shard },
			Read: directRead(hc, t.base, mode),
		})
	}
	return out
}

// directRead is GET <base>/internal/data/<key>?consistency=<mode> on one
// storage node. A 200 is the record and the mode the node says it served;
// 404 NOT_FOUND is store.ErrNotFound; anything else (NOT_LEADER, a refused
// fence, a timeout) is a failed read, kept with its status and code.
func directRead(hc *http.Client, base, mode string) ledger.ReadFunc {
	return func(ctx context.Context, key string) (store.Record, string, error) {
		ctx, cancel := context.WithTimeout(ctx, probeReadTimeout)
		defer cancel()
		u := base + "/internal/data/" + url.PathEscape(key) + "?consistency=" + url.QueryEscape(mode)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return store.Record{}, "", err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return store.Record{}, "", fmt.Errorf("probe %s: %w", base, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return store.Record{}, "", fmt.Errorf("probe %s: read body: %w", base, err)
		}
		if resp.StatusCode == http.StatusOK {
			var rr internalapi.RecordResponse
			if err := json.Unmarshal(body, &rr); err != nil {
				return store.Record{}, "", fmt.Errorf("probe %s: decode: %w", base, err)
			}
			return rr.Record, string(rr.Read.Consistency), nil
		}
		var e internalapi.Error
		_ = json.Unmarshal(body, &e)
		if resp.StatusCode == http.StatusNotFound && e.Code == internalapi.NOTFOUND {
			return store.Record{}, "", store.ErrNotFound
		}
		return store.Record{}, "", errors.New(strings.TrimSpace(fmt.Sprintf("probe %s: HTTP %d %s %s", base, resp.StatusCode, e.Code, e.Message)))
	}
}
