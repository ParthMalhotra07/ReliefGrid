package storagenode

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit"
)

// Verify (S4.5): three real storage nodes behind their real management
// endpoints. After quiesced writes the audit finds equal roots at one
// command index; a row changed on one follower's FSM file, outside Raft, is
// reported as exactly that key on exactly that pod.
func TestAuditOverRealReplicas(t *testing.T) {
	reps := startReplicas(t)
	var servers []bootstrapServer
	for _, r := range reps {
		servers = append(servers, bootstrapServer{ID: string(r.node.Identity().ServerID()), Address: string(r.node.Addr())})
	}
	if rec := httpDo(t, http.MethodPost, reps[0].mgmt.URL+"/mgmt/bootstrap", operatorAuth, bootstrapRequest{Servers: servers}); rec.Code != http.StatusOK {
		t.Fatalf("bootstrap: %d %s", rec.Code, rec.Body.String())
	}
	leader := waitLeader(t, reps)
	rg := loadRing(t)
	var keys []string
	for i := range 12 {
		key, _ := keyOn(t, rg, testShard, "", fmt.Sprintf("kv:audit-%d", i))
		rec := httpDo(t, http.MethodPut, leader.data.URL+"/internal/data/"+key, map[string]string{"If-None-Match": "*"},
			map[string]any{"operationId": "op-audit-" + key, "at": t0, "ringVersion": 1, "value": i})
		if rec.Code != http.StatusOK {
			t.Fatalf("write %s: %d %s", key, rec.Code, rec.Body.String())
		}
		keys = append(keys, key)
	}

	var addrs []string
	for _, r := range reps {
		addrs = append(addrs, strings.TrimPrefix(r.mgmt.URL, "http://"))
	}
	runAudit := func() audit.Report {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return audit.Run(ctx, audit.Config{Addrs: addrs, Token: testToken, Settle: 10 * time.Second, Poll: 20 * time.Millisecond})
	}
	r := runAudit()
	if r.Verdict != audit.Equal || len(r.Pods) != 3 || r.AppliedIndex == 0 {
		t.Fatalf("clean audit: %+v", r)
	}
	for _, p := range r.Pods {
		if p.Root != r.Pods[0].Root || p.AppliedIndex != r.AppliedIndex {
			t.Fatalf("pods = %+v", r.Pods)
		}
	}

	var follower *replica
	var majority []string
	for _, rp := range reps {
		if rp != leader && follower == nil {
			follower = rp
		} else {
			majority = append(majority, rp.pod)
		}
	}
	victim := keys[5]
	db, err := sql.Open("sqlite", "file:"+filepath.Join(follower.fsmDir, "fsm-0.db"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.ExecContext(context.Background(), `UPDATE records SET body = replace(body, '"value":5', '"value":6') WHERE key = ?`, victim)
	if err != nil {
		t.Fatal(err)
	}
	if k, _ := res.RowsAffected(); k != 1 {
		t.Fatalf("corrupt: %d rows changed", k)
	}
	_ = db.Close()

	r = runAudit()
	want := []merkle.RowDiff{{Table: "records", PK: victim, Kind: merkle.DiffContent, Pods: []string{follower.pod}, Majority: majority}}
	if r.Verdict != audit.Divergent || !reflect.DeepEqual(r.Rows, want) {
		t.Fatalf("audit after corrupting %s on %s: %s %q rows %+v, want %+v", victim, follower.pod, r.Verdict, r.Error, r.Rows, want)
	}
}
