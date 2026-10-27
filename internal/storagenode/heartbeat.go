package storagenode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// Versions this replica reports at registration (contracts/internal.yaml).
const (
	protocolVersion = 1
	schemaVersion   = 1
)

// errNotRegistered: the coordinator does not know this replica (it
// restarted, or the disk incarnation changed); register again.
var errNotRegistered = errors.New("storagenode: not registered with the coordinator")

// RunHeartbeats registers this replica with the coordinator, retrying until
// it is accepted, then sends a heartbeat every Interval until ctx ends. A
// heartbeat carries Raft status (term, leader, applied and commit index) and
// the local record counts. The coordinator's answer is a routing hint only:
// nothing in it changes Raft state, and registration never bootstraps (M7).
// A heartbeat the coordinator does not recognise (404) sends this replica
// back to registration. It returns nil when ctx ends.
func (s *Server) RunHeartbeats(ctx context.Context, cfg HeartbeatConfig) error {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: time.Second}
	}
	base := strings.TrimRight(cfg.CoordinatorURL, "/")
	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()
	registered := false
	for {
		var err error
		if !registered {
			if err = s.s.register(ctx, cfg.Client, base, cfg.Address); err == nil {
				registered = true
				slog.Info("registered with the coordinator", "coordinator", base, "address", cfg.Address)
			}
		}
		if registered {
			if err = s.s.heartbeat(ctx, cfg.Client, base); errors.Is(err, errNotRegistered) {
				registered = false
			}
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("coordinator call failed", "registered", registered, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (s *server) register(ctx context.Context, c *http.Client, base, address string) error {
	id := s.node.Status().Identity
	reg := internalapi.NodeRegistration{NodeId: id.PodName, GroupId: id.GroupID, ShardId: id.Shard, Incarnation: id.Incarnation,
		Address: address, ProtocolVersion: protocolVersion, SchemaVersion: schemaVersion}
	var resp internalapi.RegisterResponse
	if err := post(ctx, c, base+"/internal/nodes/register", reg, &resp); err != nil {
		return err
	}
	if !resp.Accepted {
		reason := ""
		if resp.Reason != nil {
			reason = *resp.Reason
		}
		return fmt.Errorf("storagenode: registration refused: %s", reason)
	}
	return nil
}

func (s *server) heartbeat(ctx context.Context, c *http.Client, base string) error {
	st := s.node.Status()
	hb := internalapi.Heartbeat{NodeId: st.PodName, GroupId: st.GroupID, ShardId: st.Shard, Incarnation: st.Incarnation,
		Health: internalapi.HealthOk, Term: st.Term, AppliedIndex: st.AppliedIndex, CommitIndex: st.CommitIndex}
	if st.LeaderID != "" {
		id := string(st.LeaderID)
		hb.LeaderId = &id
	}
	// The coordinator admits registrations from the committed configuration
	// (M7, #21); a replica without Raft state has none to report, and a
	// coordinator from before #21 is not sent it (see below).
	if c := toConfigurationJSON(st.Configuration); c != nil && !s.noConfig.Load() {
		hb.Configuration = &internalapi.RaftConfiguration{Index: c.Index, Committed: c.Committed, Servers: []internalapi.RaftServer{}}
		for _, sv := range c.Servers {
			hb.Configuration.Servers = append(hb.Configuration.Servers,
				internalapi.RaftServer{Id: sv.ID, Address: sv.Address, Suffrage: internalapi.RaftServerSuffrage(sv.Suffrage)})
		}
	}
	// The counts are a local read: a heartbeat never goes through the log.
	var counts internalapi.RecordCounts
	rctx, cancel := context.WithTimeout(ctx, s.timeout)
	_, err := s.node.Read(rctx, raftstore.ReadStale, func(ctx context.Context, rd raftstore.Reader) error {
		c, err := rd.Counts(ctx)
		counts = internalapi.RecordCounts{Request: c.Request, Resource: c.Resource, Kv: c.KV, Tombstones: c.Tombstones}
		return err
	})
	cancel()
	if err == nil {
		hb.RecordCounts = &counts
	}
	if !st.CaughtUp || err != nil {
		hb.Health = internalapi.HealthDegraded
	}
	// The answer's leader hints are for clients: they feed only the address
	// in this replica's not-leader hints. It learns its leader from Raft alone.
	var resp internalapi.HeartbeatResponse
	err = post(ctx, c, base+"/internal/nodes/heartbeat", hb, &resp)
	if err != nil && hb.Configuration != nil && rejectsConfiguration(err) {
		// A coordinator from before #21 decodes heartbeats strictly and
		// answers 400 to the unknown field. It admits every node anyway, so
		// heartbeat without it rather than look DOWN: upgrading the storage
		// nodes before the coordinator must not be an outage.
		slog.Warn("the coordinator does not know heartbeat.configuration (older than #21); sending heartbeats without it")
		s.noConfig.Store(true)
		hb.Configuration = nil
		err = post(ctx, c, base+"/internal/nodes/heartbeat", hb, &resp)
	}
	if err != nil {
		return err
	}
	s.learnLeaders(resp)
	return nil
}

// rejectsConfiguration: a 400 naming heartbeat.configuration as an unknown field.
func rejectsConfiguration(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, " 400 ") && strings.Contains(msg, "unknown field") && strings.Contains(msg, "configuration")
}

// post sends v as JSON and decodes a 200 answer into out. A 404 is
// errNotRegistered.
func post(ctx context.Context, c *http.Client, url string, v, out any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return json.Unmarshal(raw, out)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", errNotRegistered, bytes.TrimSpace(raw))
	default:
		return fmt.Errorf("storagenode: %s: %d %s", url, resp.StatusCode, bytes.TrimSpace(raw))
	}
}
