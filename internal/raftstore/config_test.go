package raftstore

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestProfileByName(t *testing.T) {
	demo, err := ProfileByName("demo")
	if err != nil {
		t.Fatalf("demo: %v", err)
	}
	if demo.SnapshotThreshold != 1024 || demo.TrailingLogs != 512 || demo.SnapshotInterval != 30*time.Second {
		t.Fatalf("demo = %+v, want M4 settings 1024 / 512 / 30s", demo)
	}
	load, err := ProfileByName("load")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if load.SnapshotThreshold != 8192 || load.TrailingLogs != 10240 || load.SnapshotInterval != 120*time.Second {
		t.Fatalf("load = %+v, want library defaults 8192 / 10240 / 120s", load)
	}
	if _, err := ProfileByName("fast"); err == nil {
		t.Fatal("unknown profile accepted")
	}
}

func TestRaftConfigFromProfile(t *testing.T) {
	def := raft.DefaultConfig()

	c := Config{PodName: "storage-1-0", Profile: ProfileDemo}
	rc := c.raftConfig("storage-1-0/inc")
	if rc == nil {
		t.Fatal("raftConfig returned nil")
	}
	if rc.LocalID != "storage-1-0/inc" {
		t.Errorf("LocalID = %q", rc.LocalID)
	}
	if rc.SnapshotThreshold != 1024 || rc.TrailingLogs != 512 || rc.SnapshotInterval != 30*time.Second {
		t.Errorf("snapshot settings = %d / %d / %s, want the demo profile", rc.SnapshotThreshold, rc.TrailingLogs, rc.SnapshotInterval)
	}
	// Zero timeouts keep the library defaults (the spike measured those).
	if rc.HeartbeatTimeout != def.HeartbeatTimeout || rc.ElectionTimeout != def.ElectionTimeout || rc.LeaderLeaseTimeout != def.LeaderLeaseTimeout {
		t.Errorf("timeouts = %s / %s / %s, want library defaults", rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout)
	}
	if err := raft.ValidateConfig(rc); err != nil {
		t.Errorf("ValidateConfig: %v", err)
	}

	// The kind/WSL2 tuning the plan allows: 2 s heartbeat and election, 1 s lease.
	c.Profile.HeartbeatTimeout, c.Profile.ElectionTimeout, c.Profile.LeaderLeaseTimeout = 2*time.Second, 2*time.Second, time.Second
	rc = c.raftConfig("storage-1-0/inc")
	if rc.HeartbeatTimeout != 2*time.Second || rc.ElectionTimeout != 2*time.Second || rc.LeaderLeaseTimeout != time.Second {
		t.Errorf("timeouts = %s / %s / %s, want the profile's", rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout)
	}
}
