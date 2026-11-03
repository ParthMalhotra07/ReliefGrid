package audit_test

import (
	"context"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit/audittest"
)

func run(t *testing.T, c *audittest.Cluster) audit.Report {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := audit.Run(ctx, audit.Config{Addrs: c.Addrs(), Token: audittest.Token, Settle: 300 * time.Millisecond, Poll: 10 * time.Millisecond})
	c.ExpectExactQueries(t)
	return r
}

func TestAuditEqualReplicas(t *testing.T) {
	c := audittest.NewCluster(t, 300, "pod-0", "pod-1", "pod-2")
	r := run(t, c)
	if r.Verdict != audit.Equal || r.Verdict.ExitCode() != 0 || r.AppliedIndex != 9 {
		t.Fatalf("report = %+v", r)
	}
	if len(r.Pods) != 3 || r.Pods[0].Pod != "pod-0" || r.Pods[0].Root != r.Pods[2].Root || r.Pods[1].RaftAppliedIndex != 11 {
		t.Fatalf("pods = %+v", r.Pods)
	}
	if r.Tables != nil || r.Rows != nil {
		t.Fatalf("equal replicas reported differences: %+v %+v", r.Tables, r.Rows)
	}
	if len(c.Queries()) != 3 {
		t.Fatalf("queries %v, want one digest per pod", c.Queries())
	}
}

// One row changed on one replica is reported as that key on that pod, and
// only that; bucket answers are checked against the digests.
func TestAuditPinpointsDivergence(t *testing.T) {
	c := audittest.NewCluster(t, 300, "pod-0", "pod-1", "pod-2")
	c.Pods[2].Rows[42][2] = `{"n":"corrupt"}`
	r := run(t, c)
	if r.Verdict != audit.Divergent || r.Verdict.ExitCode() != 1 {
		t.Fatalf("verdict %s (%s), want divergent", r.Verdict, r.Error)
	}
	want := []merkle.RowDiff{{Table: "records", PK: "key:042", Kind: merkle.DiffContent, Pods: []string{"pod-2"}, Majority: []string{"pod-0", "pod-1"}}}
	if !reflect.DeepEqual(r.Rows, want) {
		t.Fatalf("rows = %+v, want %+v", r.Rows, want)
	}
	if len(r.Tables) != 1 || r.Tables[0].Table != "records" || len(r.Tables[0].Buckets) != 1 {
		t.Fatalf("tables = %+v", r.Tables)
	}
}

// A replica whose status claims AnyIndex as its applied index is never
// asked for a digest at it: no verdict.
func TestAuditNeverAsksForAnyIndex(t *testing.T) {
	c := audittest.NewCluster(t, 10, "pod-0", "pod-1", "pod-2")
	c.Pods[1].AppliedIndex = fsm.AnyIndex
	r := run(t, c)
	if r.Verdict != audit.Incomplete || r.Verdict.ExitCode() != 2 {
		t.Fatalf("verdict %s, want incomplete", r.Verdict)
	}
	if q := c.Queries(); len(q) != 0 {
		t.Fatalf("asked for digests %v", q)
	}
}

// A write landing between status and digest makes a replica answer 409 with
// its new index; the audit retargets and compares at the new index.
func TestAuditRetargetsWhenIndexMoves(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	moved := false
	c.OnMerkle = func(c *audittest.Cluster, _ *audittest.Pod) {
		if !moved {
			moved = true
			c.SetApplied(10)
		}
	}
	r := run(t, c)
	if r.Verdict != audit.Equal || r.AppliedIndex != 10 {
		t.Fatalf("report %s at %d (%s), want equal at 10", r.Verdict, r.AppliedIndex, r.Error)
	}
}

// Replicas that keep moving are never compared at different indexes and
// never reported divergent: exit 3, not quiescent.
func TestAuditNotQuiescent(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	c.Pods[2].Rows[3][2] = `{"n":"other"}` // would be divergent if ever compared
	c.OnMerkle = func(c *audittest.Cluster, p *audittest.Pod) { p.AppliedIndex++ }
	r := run(t, c)
	if r.Verdict != audit.NotQuiescent || r.Verdict.ExitCode() != 3 || r.Rows != nil {
		t.Fatalf("report %s (%s) rows %+v, want not quiescent", r.Verdict, r.Error, r.Rows)
	}
}

// A replica stuck behind the others is not compared: not quiescent.
func TestAuditStuckReplica(t *testing.T) {
	c := audittest.NewCluster(t, 50, "pod-0", "pod-1", "pod-2")
	c.Pods[2].AppliedIndex = 8
	r := run(t, c)
	if r.Verdict != audit.NotQuiescent || !strings.Contains(r.Error, "pod-2") || len(c.Queries()) != 0 {
		t.Fatalf("report %s (%s), queries %v", r.Verdict, r.Error, c.Queries())
	}
}

func TestAuditIncomplete(t *testing.T) {
	for name, change := range map[string]func(c *audittest.Cluster){
		"pod down":           func(c *audittest.Cluster) { c.Pods[1].Down = true },
		"other shard":        func(c *audittest.Cluster) { c.Pods[1].Shard = 2 },
		"other group":        func(c *audittest.Cluster) { c.Pods[1].GroupID = "shard-2" },
		"other ring version": func(c *audittest.Cluster) { c.Pods[1].RingVersion = 2 },
		"other schema":       func(c *audittest.Cluster) { c.Pods[1].SchemaVersion = "2" },
	} {
		t.Run(name, func(t *testing.T) {
			c := audittest.NewCluster(t, 20, "pod-0", "pod-1", "pod-2")
			change(c)
			if r := run(t, c); r.Verdict != audit.Incomplete || r.Verdict.ExitCode() != 2 || r.Error == "" {
				t.Fatalf("report %s (%q), want incomplete with a reason", r.Verdict, r.Error)
			}
		})
	}
	ctx := context.Background()
	for _, cfg := range []audit.Config{
		{Addrs: []string{"a:1"}, Token: "t"},
		{Addrs: []string{"a:1", "b:1"}},
	} {
		if r := audit.Run(ctx, cfg); r.Verdict != audit.Incomplete {
			t.Fatalf("Run(%+v) = %s", cfg, r.Verdict)
		}
	}
}

// Equal command indexes with different Raft indexes (term no-ops, barriers)
// are equal, with a note, not a difference.
func TestAuditNotesRaftIndexDifference(t *testing.T) {
	c := audittest.NewCluster(t, 20, "pod-0", "pod-1", "pod-2")
	c.Pods[0].RaftAppliedIndex = 13
	r := run(t, c)
	if r.Verdict != audit.Equal || len(r.Notes) == 0 || !strings.Contains(strings.Join(r.Notes, " "), "raft") {
		t.Fatalf("report %s, notes %v", r.Verdict, r.Notes)
	}
}

func TestAuditTokenIsSent(t *testing.T) {
	c := audittest.NewCluster(t, 5, "pod-0", "pod-1")
	r := audit.Run(context.Background(), audit.Config{Addrs: c.Addrs(), Token: "wrong", Settle: time.Second, Poll: 10 * time.Millisecond})
	if r.Verdict != audit.Incomplete || !strings.Contains(r.Error, strconv.Itoa(http.StatusUnauthorized)) {
		t.Fatalf("wrong token: %s %q", r.Verdict, r.Error)
	}
}
