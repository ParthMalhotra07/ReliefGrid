package main

import (
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

func TestLoadConfig(t *testing.T) {
	base := map[string]string{
		"POD_NAME": "storage-1-2", "SHARD": "1", "DATA_DIR": "/data", "FSM_DIR": "/fsm",
		"RAFT_ADVERTISE": "storage-1-2.storage-1.reliefgrid.svc.cluster.local:7000", "OPERATOR_TOKEN": "s3cret",
		"ADMISSION": "off", // TestLoadConfigAdmission covers the gate
	}
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	cfg, err := loadConfig(env(base))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	r := cfg.Raft
	if r.PodName != "storage-1-2" || r.Shard != 1 || r.GroupID != "shard-1" || r.DataDir != "/data" || r.FSMDir != "/fsm" ||
		r.BindAddr != ":7000" || r.AdvertiseAddr != base["RAFT_ADVERTISE"] || r.Profile != raftstore.ProfileDemo {
		t.Fatalf("raft config = %+v", r)
	}
	if cfg.HTTPAddr != ":8080" || cfg.MgmtAddr != ":8081" || cfg.HTTPPort != "8080" ||
		cfg.RingPath != "/etc/reliefgrid/ring.yaml" || cfg.RequestTimeout != 3*time.Second || cfg.OperatorToken != "s3cret" {
		t.Fatalf("config = %+v", cfg)
	}

	over := map[string]string{"PROFILE": "load", "HTTP_ADDR": ":9090", "GROUP_ID": "g1", "REQUEST_TIMEOUT_MS": "1500"}
	m := map[string]string{}
	for k, v := range base {
		m[k] = v
	}
	for k, v := range over {
		m[k] = v
	}
	cfg, err = loadConfig(env(m))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Raft.Profile != raftstore.ProfileLoad || cfg.HTTPPort != "9090" || cfg.Raft.GroupID != "g1" || cfg.RequestTimeout != 1500*time.Millisecond {
		t.Fatalf("overrides not applied: %+v", cfg)
	}

	for _, missing := range []string{"POD_NAME", "SHARD", "DATA_DIR", "FSM_DIR", "RAFT_ADVERTISE"} {
		m := map[string]string{}
		for k, v := range base {
			if k != missing {
				m[k] = v
			}
		}
		if _, err := loadConfig(env(m)); err == nil {
			t.Errorf("missing %s accepted", missing)
		}
	}
	for k, v := range map[string]string{"SHARD": "-1", "PROFILE": "fast", "REQUEST_TIMEOUT_MS": "soon", "HTTP_ADDR": "8080",
		"HEARTBEAT_INTERVAL_MS": "0", "HTTP_ADVERTISE": "no-port", "COORDINATOR_URL": "coordinator:8080"} {
		m := map[string]string{}
		for bk, bv := range base {
			m[bk] = bv
		}
		m[k] = v
		if _, err := loadConfig(env(m)); err == nil {
			t.Errorf("%s=%q accepted", k, v)
		}
	}
}

// Heartbeats go to COORDINATOR_URL; the address the coordinator routes to
// is HTTP_ADVERTISE, by default RAFT_ADVERTISE's host on the HTTP port.
func TestLoadConfigHeartbeats(t *testing.T) {
	base := map[string]string{
		"POD_NAME": "storage-1-2", "SHARD": "1", "DATA_DIR": "/data", "FSM_DIR": "/fsm",
		"RAFT_ADVERTISE": "storage-1-2.storage-1.reliefgrid.svc.cluster.local:7000", "ADMISSION": "off",
	}
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	cfg, err := loadConfig(env(base))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Heartbeat.CoordinatorURL != "" || cfg.Heartbeat.Address != "storage-1-2.storage-1.reliefgrid.svc.cluster.local:8080" || cfg.Heartbeat.Interval != time.Second {
		t.Fatalf("heartbeat defaults = %+v", cfg.Heartbeat)
	}
	base["COORDINATOR_URL"] = "http://coordinator.reliefgrid.svc.cluster.local:8080"
	base["HTTP_ADVERTISE"] = "10.0.0.7:8080"
	base["HEARTBEAT_INTERVAL_MS"] = "250"
	if cfg, err = loadConfig(env(base)); err != nil {
		t.Fatal(err)
	}
	if cfg.Heartbeat.CoordinatorURL != base["COORDINATOR_URL"] || cfg.Heartbeat.Address != "10.0.0.7:8080" || cfg.Heartbeat.Interval != 250*time.Millisecond {
		t.Fatalf("heartbeat overrides = %+v", cfg.Heartbeat)
	}
}
