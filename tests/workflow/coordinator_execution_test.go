//go:build schedules

package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func scheduleRing(x *execution) internalapi.Ring {
	x.t.Helper()
	// A coordinator restart ends its old loopback port-forward. The harness
	// reconnects it; observe that transport within the same three-second budget.
	ctx, cancel := context.WithTimeout(x.ctx, 3*time.Second)
	defer cancel()
	var response *http.Response
	var err error
	for {
		var req *http.Request
		req, err = http.NewRequestWithContext(ctx, "GET", strings.TrimRight(os.Getenv("COORDINATOR_URL"), "/")+"/internal/ring", nil)
		if err != nil {
			x.t.Fatal(err)
		}
		response, err = (&http.Client{Timeout: time.Second}).Do(req)
		if err == nil {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			x.t.Fatal("coordinator forward unavailable:", err)
		case <-timer.C:
		}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		x.t.Fatalf("ring HTTP%d", response.StatusCode)
	}
	var r internalapi.Ring
	if err := json.NewDecoder(response.Body).Decode(&r); err != nil {
		x.t.Fatal(err)
	}
	return r
}
func placeHealthyShard(x *execution) {
	r := scheduleRing(x)
	rg, err := ring.New(ring.Config{RingVersion: r.RingVersion, Hash: string(r.Hash), Shards: r.Shards, VNodesPerShard: r.VnodesPerShard, Checksum: r.Checksum})
	if err != nil {
		x.t.Fatal(err)
	}
	for _, name := range []string{"R", "Res", "Res2"} {
		found := false
		for n := range 1000 {
			id := x.id(fmt.Sprintf("%s-healthy-%d", name, n))
			key := store.ResourceKey(id)
			if name == "R" {
				key = store.RequestKey(id)
			}
			shard, err := rg.Owner(key)
			if err != nil {
				x.t.Fatal(err)
			}
			if shard == 0 {
				x.entities[name] = id
				found = true
				break
			}
		}
		if !found {
			x.t.Fatal("could not route fixture onto healthy shard0")
		}
	}
}
func scheduleKube(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "kubectl", append([]string{"--context", "kind-reliefgrid", "--request-timeout=10s", "-n", "reliefgrid"}, args...)...)
	out, err := command.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("kubectl %v: %w: %s", args, err, out)
	}
	return string(out), nil
}

func readyCoordinatorUIDs(x *execution) map[string]bool {
	raw, err := scheduleKube(x.ctx, "get", "pods", "-l", "app.kubernetes.io/component=coordinator", "-o", "json")
	if err != nil {
		x.t.Fatal(err)
	}
	var list struct {
		Items []struct {
			Metadata struct {
				UID               string
				DeletionTimestamp *string
			}
			Status struct {
				Conditions []struct{ Type, Status string }
			}
		}
	}
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		x.t.Fatal(err)
	}
	uids := map[string]bool{}
	for _, p := range list.Items {
		if p.Metadata.DeletionTimestamp != nil {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				uids[p.Metadata.UID] = true
			}
		}
	}
	return uids
}
func executeCoordinatorRestart(x *execution) {
	if os.Getenv("SCHEDULE_BACKEND") != "cluster" {
		partial := memstore.WithFaults(x.s, func(c memstore.Call) memstore.Fault {
			if c.Method == "Scan" {
				return memstore.Fault{FailShards: []int{1}}
			}
			return memstore.Fault{}
		})
		page, err := partial.Scan(x.ctx, store.ScanQuery{Kind: store.KindResource, Limit: 1, Consistency: store.ConsistencyStrong})
		if err != nil {
			x.t.Fatal(err)
		}
		if !page.Partial || len(page.FailedShards) == 0 {
			x.t.Fatal("unavailable shard became empty success")
		}
		// A new helper has no volatile state; the committed M8 data stays intact.
		_ = x.matcher(x.s)
		x.match(x.attempts["attemptA"])
		return
	}
	before := scheduleRing(x)
	replicas, err := scheduleKube(x.ctx, "get", "statefulset/storage-shard-1", "-o", "jsonpath={.spec.replicas}")
	if err != nil {
		x.t.Fatal(err)
	}
	x.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
		if _, err := scheduleKube(ctx, "scale", "statefulset/storage-shard-1", "--replicas="+strings.TrimSpace(replicas)); err != nil {
			x.t.Error(err)
			return
		}
		if _, err := scheduleKube(ctx, "rollout", "status", "statefulset/storage-shard-1", "--timeout=90s"); err != nil {
			x.t.Error(err)
		}
	})
	if _, err := scheduleKube(x.ctx, "scale", "statefulset/storage-shard-1", "--replicas=0"); err != nil {
		x.t.Fatal(err)
	}
	if _, err := scheduleKube(x.ctx, "wait", "--for=delete", "pod/storage-shard-1-0", "pod/storage-shard-1-1", "pod/storage-shard-1-2", "--timeout=25s"); err != nil {
		x.t.Fatal(err)
	}
	old := readyCoordinatorUIDs(x)
	if len(old) != 1 {
		x.t.Fatal("coordinator must have exactly one Ready pod before restart")
	}
	if _, err := scheduleKube(x.ctx, "rollout", "restart", "deployment/storage-coordinator"); err != nil {
		x.t.Fatal(err)
	}
	if _, err := scheduleKube(x.ctx, "rollout", "status", "deployment/storage-coordinator", "--timeout=20s"); err != nil {
		x.t.Fatal(err)
	}
	current := readyCoordinatorUIDs(x)
	fresh := false
	for uid := range current {
		if !old[uid] {
			fresh = true
		}
	}
	if !fresh {
		x.t.Fatal("no new Ready coordinator UID after restart")
	}
	after := scheduleRing(x)
	if before.RingVersion != after.RingVersion || before.Checksum != after.Checksum {
		x.t.Fatal("restart changed ring identity")
	}
	page, err := x.s.Scan(x.ctx, store.ScanQuery{Kind: store.KindResource, Limit: 1, Consistency: store.ConsistencyStrong})
	if err != nil {
		x.t.Fatal(err)
	}
	missing := false
	for _, shard := range page.FailedShards {
		if shard == 1 {
			missing = true
		}
	}
	if !page.Partial || !missing {
		x.t.Fatal("unavailable shard not explicit after coordinator restart")
	}
	x.match(x.attempts["attemptA"])
}
