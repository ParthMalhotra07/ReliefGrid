//go:build schedules

package workflow

// The transport helpers follow the verified #94 seam procedures.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

const (
	isolatedLabel = "reliefgrid.io/chaos-isolated"
	// demoTrailingLogs is raftstore.ProfileDemo's TrailingLogs: a follower
	// must fall more than this far behind to need a snapshot.
	demoTrailingLogs = 512
	fillerMargin     = 100
	// barrierTimeout stays under the storage node's 3 s REQUEST_TIMEOUT_MS,
	// so a slow runner sees an expired barrier, not an unknown outcome.
	barrierTimeout = 2500 * time.Millisecond
	streamTimeout  = 90 * time.Second
)

type kindCluster struct {
	t      *testing.T
	ctx    string
	ns     string
	shard  int
	run    string
	token  string
	client store.Store
	x      *execution
	seq    int

	mu  sync.Mutex
	fwd map[string]*forward
}

type forward struct {
	cmd  *exec.Cmd
	port string
}

func newKindCluster(x *execution) *kindCluster {
	t := x.t
	t.Helper()
	c := &kindCluster{t: t, x: x, ctx: "kind-reliefgrid", ns: "reliefgrid", shard: x.requestShard, run: x.id("seams"), client: x.s, fwd: map[string]*forward{}}
	t.Cleanup(c.closeForwards)
	tok, err := c.kubectl("get", "secret", "storage-operator-token", "-o", "jsonpath={.data.token}")
	if err != nil {
		t.Fatalf("operator token: %v", err)
	}
	dec, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		t.Fatal(err)
	}
	c.token = string(dec)
	for _, p := range c.pods() {
		if !c.status(p).Seams {
			t.Fatalf("%s needs storagenode:seams", p)
		}
	}
	return c
}

func (c *kindCluster) kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(c.x.ctx, 20*time.Second)
	defer cancel()
	full := append([]string{"--context", c.ctx, "--request-timeout=10s", "-n", c.ns}, args...)
	out, err := exec.CommandContext(ctx, "kubectl", full...).CombinedOutput() //nolint:gosec // args are ours
	if err != nil {
		return "", fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *kindCluster) pods() []string {
	return []string{
		fmt.Sprintf("storage-shard-%d-0", c.shard),
		fmt.Sprintf("storage-shard-%d-1", c.shard),
		fmt.Sprintf("storage-shard-%d-2", c.shard),
	}
}

var forwarding = regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+)`)

// port returns a local port forwarded to pod's mgmt port (8081), opening
// one if needed. Port-forwards reach the pod through the kubelet, so the
// Raft isolation label does not cut them.
func (c *kindCluster) port(pod string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f := c.fwd[pod]; f != nil {
		return f.port, nil
	}
	cmd := exec.CommandContext(c.x.ctx, "kubectl", "--context", c.ctx, "-n", c.ns, "port-forward", "pod/"+pod, ":8081") //nolint:gosec // args are ours
	out, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	ports := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if m := forwarding.FindStringSubmatch(sc.Text()); m != nil {
				ports <- m[1]
			}
		}
	}()
	select {
	case p := <-ports:
		c.fwd[pod] = &forward{cmd: cmd, port: p}
		return p, nil
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return "", fmt.Errorf("port-forward to %s did not start", pod)
	}
}

func (c *kindCluster) dropForward(pod string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f := c.fwd[pod]; f != nil {
		_ = f.cmd.Process.Kill()
		_ = f.cmd.Wait()
		delete(c.fwd, pod)
	}
}

func (c *kindCluster) closeForwards() {
	c.mu.Lock()
	pods := make([]string, 0, len(c.fwd))
	for p := range c.fwd {
		pods = append(pods, p)
	}
	c.mu.Unlock()
	for _, p := range pods {
		c.dropForward(p)
	}
}

// mgmt calls pod's mgmt endpoint, reopening the port-forward once if it
// broke (a restarted container ends it).
func (c *kindCluster) mgmt(pod, method, path string, body, out any, timeout time.Duration) (int, error) {
	var last error
	attempts := 1
	if method == http.MethodGet {
		attempts = 2
	}
	for attempt := 0; attempt < attempts; attempt++ {
		port, err := c.port(pod)
		if err != nil {
			last = err
			continue
		}
		var rd io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				return 0, err
			}
			rd = bytes.NewReader(b)
		}
		ctx, cancel := context.WithTimeout(c.x.ctx, timeout)
		req, err := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:"+port+path, rd)
		if err != nil {
			cancel()
			return 0, err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			last = err
			c.dropForward(pod)
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		if err != nil {
			return resp.StatusCode, err
		}
		if out != nil && resp.StatusCode < 300 {
			if err := json.Unmarshal(raw, out); err != nil {
				return resp.StatusCode, fmt.Errorf("%s %s: decode %q: %w", method, path, raw, err)
			}
		}
		if resp.StatusCode >= 300 {
			return resp.StatusCode, fmt.Errorf("%s %s on %s: %d %s", method, path, pod, resp.StatusCode, bytes.TrimSpace(raw))
		}
		return resp.StatusCode, nil
	}
	return 0, last
}

type podStatus struct {
	State         string `json:"state"`
	LeaderID      string `json:"leaderId"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	AppliedIndex  uint64 `json:"appliedIndex"`
	RestoredIndex uint64 `json:"restoredIndex"`
	Seams         bool   `json:"seams"`
}

func (c *kindCluster) status(pod string) podStatus {
	c.t.Helper()
	var st podStatus
	if _, err := c.mgmt(pod, http.MethodGet, "/mgmt/status", nil, &st, 5*time.Second); err != nil {
		c.t.Fatalf("status of %s: %v", pod, err)
	}
	return st
}

// roles waits until the shard has a leader that every reachable replica
// names, and returns it and the followers.
func (c *kindCluster) roles() (string, []string) {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var leader string
		var followers []string
		ok := true
		for _, p := range c.pods() {
			st := c.status(p)
			switch {
			case st.State == "Leader":
				leader = p
			case st.State == "Follower" && st.LeaderID != "":
				followers = append(followers, p)
			default:
				ok = false
			}
		}
		if ok && leader != "" && len(followers) == 2 {
			return leader, followers
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatal("shard has no settled leader")
	return "", nil
}

type snapshotInfo struct {
	RaftIndex    uint64 `json:"raftIndex"`
	AppliedIndex uint64 `json:"appliedIndex"`
}

func (c *kindCluster) snapshot(pod string) snapshotInfo {
	c.t.Helper()
	var s snapshotInfo
	if _, err := c.mgmt(pod, http.MethodPost, "/mgmt/snapshot", nil, &s, 60*time.Second); err != nil {
		c.t.Fatalf("snapshot on %s: %v", pod, err)
	}
	return s
}

type dumpDigest struct {
	AppliedIndex uint64 `json:"appliedIndex"`
	SHA256       string `json:"sha256"`
}

func (c *kindCluster) dump(pod string) dumpDigest {
	c.t.Helper()
	var d dumpDigest
	if _, err := c.mgmt(pod, http.MethodGet, "/mgmt/dump", nil, &d, 30*time.Second); err != nil {
		c.t.Fatalf("dump of %s: %v", pod, err)
	}
	return d
}

type seamView struct {
	ID       string            `json:"id"`
	State    testseam.State    `json:"state"`
	Point    *testseam.Point   `json:"point"`
	Appended map[string]uint64 `json:"appended"`
}

func (c *kindCluster) arm(pod string, kind testseam.Kind, ops []string, timeout time.Duration) string {
	c.t.Helper()
	var v seamView
	body := map[string]any{"kind": kind, "timeoutMs": timeout.Milliseconds()}
	if len(ops) > 0 {
		body["operationIds"] = ops
	}
	if _, err := c.mgmt(pod, http.MethodPost, "/mgmt/test/seams", body, &v, 5*time.Second); err != nil {
		c.t.Fatalf("arm %s on %s: %v", kind, pod, err)
	}
	return v.ID
}

func (c *kindCluster) seam(pod, id string) seamView {
	c.t.Helper()
	var v seamView
	if _, err := c.mgmt(pod, http.MethodGet, "/mgmt/test/seams/"+id, nil, &v, 5*time.Second); err != nil {
		c.t.Fatalf("seam %s on %s: %v", id, pod, err)
	}
	return v
}

// waitSeam polls until cond holds; failing if the seam ends first or the
// wait runs out (#88: a boundary never reached is a failure).
func (c *kindCluster) waitSeam(pod, id string, within time.Duration, cond func(seamView) bool) seamView {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for {
		v := c.seam(pod, id)
		if v.State == testseam.Expired || v.State == testseam.Aborted || v.State == testseam.Released {
			c.t.Fatalf("seam %s on %s is %s before the expected point: %+v", id, pod, v.State, v)
		}
		if cond(v) {
			return v
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("seam %s on %s never reached (state %s)", id, pod, v.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *kindCluster) release(pod, id string) {
	c.t.Helper()
	if _, err := c.mgmt(pod, http.MethodPost, "/mgmt/test/seams/"+id+"/release", nil, nil, 5*time.Second); err != nil {
		c.t.Fatalf("release %s on %s: %v", id, pod, err)
	}
}

func (c *kindCluster) restartCount(pod string) int {
	c.t.Helper()
	out, err := c.kubectl("get", "pod", pod, "-o", `jsonpath={.status.containerStatuses[?(@.name=="storagenode")].restartCount}`)
	if err != nil {
		c.t.Fatal(err)
	}
	n, _ := strconv.Atoi(out)
	return n
}

// kill SIGKILLs pod's storagenode container through crictl on its kind
// node, so the kubelet restarts it in the same pod: the PVC and the
// emptyDir FSM directory both survive, exercising the wipe on start.
// Fallback (reported): kubectl delete --force, which replaces the pod and
// so its emptyDir.
func (c *kindCluster) kill(pod string) string {
	c.t.Helper()
	uid, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.metadata.uid}")
	if err != nil || uid == "" {
		c.t.Fatalf("read fault pod UID: %q %v", uid, err)
	}
	before := c.restartCount(pod)
	node, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
	if err == nil {
		err = crictlKill(c.x.ctx, node, pod, c.ns)
	}
	method := "crictl"
	if err != nil {
		c.t.Fatalf("required SIGKILL failed: %v", err)
	}
	// After all case assertions (or a recorded failure), give the next case
	// a fresh pod lifetime. SIGKILL recovery itself must use this same UID.
	c.t.Cleanup(func() { c.recycleFaultPod(pod, uid) })
	c.dropForward(pod)
	if method == "crictl" {
		deadline := time.Now().Add(180 * time.Second)
		for c.restartCount(pod) <= before {
			if time.Now().After(deadline) {
				c.t.Fatalf("%s was not restarted after the kill", pod)
			}
			time.Sleep(200 * time.Millisecond)
		}
	} else {
		time.Sleep(2 * time.Second) // the replacement pod
	}
	if _, err := c.kubectl("wait", "--for=condition=Ready", "pod/"+pod, "--timeout=180s"); err != nil {
		c.t.Fatal(err)
	}
	after, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.metadata.uid}")
	if err != nil || after != uid {
		c.t.Fatalf("SIGKILL recovery replaced pod %s: before=%s after=%s error=%v", pod, uid, after, err)
	}
	c.t.Logf("killed %s (%s)", pod, method)
	return method
}

// recycleFaultPod is fixture teardown, after the fault's recovery and row
// assertions. Kubernetes otherwise accumulates exponential restart backoff
// between independent seeds (observed 2m40s at seed5). A normal pod deletion
// resets that lifetime while retaining its StatefulSet PVC. It is never a
// replacement for SIGKILL or a successful same-pod recovery assertion.
func (c *kindCluster) recycleFaultPod(pod, oldUID string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := func(args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "kubectl", append([]string{"--context", c.ctx, "-n", c.ns, "--request-timeout=10s"}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("fixture recycle kubectl %v: %w: %s", args, err, bytes.TrimSpace(out))
		}
		return strings.TrimSpace(string(out)), nil
	}
	uid, err := command("get", "pod", pod, "-o", "jsonpath={.metadata.uid}")
	if err != nil || uid != oldUID {
		c.t.Errorf("fault pod changed before teardown: %s uid=%s want=%s error=%v", pod, uid, oldUID, err)
		return
	}
	c.dropForward(pod)
	if _, err := command("delete", "pod", pod, "--wait=false"); err != nil {
		c.t.Error(err)
		return
	}
	for {
		raw, err := command("get", "pod", pod, "--ignore-not-found", "-o", "json")
		if err != nil {
			c.t.Error(err)
			return
		}
		if raw != "" {
			var p struct {
				Metadata struct {
					UID string `json:"uid"`
				} `json:"metadata"`
				Status struct {
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
					Containers []struct {
						Name     string `json:"name"`
						Ready    bool   `json:"ready"`
						Restarts int    `json:"restartCount"`
					} `json:"containerStatuses"`
				} `json:"status"`
			}
			if err := json.Unmarshal([]byte(raw), &p); err != nil {
				c.t.Errorf("decode recycled pod: %v", err)
				return
			}
			if p.Metadata.UID != "" && p.Metadata.UID != oldUID {
				ready := false
				for _, cond := range p.Status.Conditions {
					if cond.Type == "Ready" && cond.Status == "True" {
						ready = true
					}
				}
				for _, container := range p.Status.Containers {
					if container.Name != "storagenode" {
						continue
					}
					if container.Restarts != 0 {
						c.t.Errorf("new fixture pod %s restarted without injection: %d", pod, container.Restarts)
						return
					}
					if ready && container.Ready {
						c.t.Logf("post-assertion fixture recycle: %s newUID=%s restartCount=0 (PVC retained)", pod, p.Metadata.UID)
						return
					}
				}
			}
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.t.Errorf("recycle %s did not become Ready: %v", pod, ctx.Err())
			return
		case <-timer.C:
		}
	}
}

func crictlKill(ctx context.Context, node, pod, ns string) error {
	out, err := exec.CommandContext(ctx, "docker", "exec", node, "crictl", "ps", "-q", "--name", "storagenode", //nolint:gosec // args are ours
		"--label", "io.kubernetes.pod.name="+pod, "--label", "io.kubernetes.pod.namespace="+ns).Output()
	if err != nil {
		return fmt.Errorf("crictl ps: %w", err)
	}
	cid := strings.TrimSpace(string(out))
	if cid == "" || strings.Contains(cid, "\n") {
		return fmt.Errorf("crictl ps: container ids %q", cid)
	}
	out, err = exec.CommandContext(ctx, "docker", "exec", node, "crictl", "inspect", "-o", "go-template", "--template", "{{.info.pid}}", cid).Output() //nolint:gosec // args are ours
	if err != nil {
		return fmt.Errorf("crictl inspect: %w", err)
	}
	pid := strings.TrimSpace(string(out))
	if _, err := strconv.Atoi(pid); err != nil {
		return fmt.Errorf("crictl inspect: pid %q", pid)
	}
	if out, err := exec.CommandContext(ctx, "docker", "exec", node, "kill", "-9", pid).CombinedOutput(); err != nil { //nolint:gosec // args are ours
		return fmt.Errorf("kill -9 %s: %w: %s", pid, err, out)
	}
	return nil
}

// logsContain reports whether pod's current storagenode container logged s.
func (c *kindCluster) logsContain(pod, s string) bool {
	c.t.Helper()
	out, err := c.kubectl("logs", pod, "-c", "storagenode")
	if err != nil {
		c.t.Fatal(err)
	}
	return strings.Contains(out, s)
}

// isolate cuts pod's Raft port both ways with the S2.3 label, clearing
// conntrack if the NetworkPolicy does not take (as chaos/scripts/lib.sh).
func (c *kindCluster) isolate(pod string) string {
	c.t.Helper()
	if _, err := c.kubectl("label", "pod", pod, isolatedLabel+"=true", "--overwrite"); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := scheduleKube(ctx, "label", "pod", pod, isolatedLabel+"-"); err != nil {
			c.t.Errorf("remove isolation: %v", err)
		}
	})
	if c.waitCut(pod, 8*time.Second) {
		return "networkpolicy"
	}
	ip, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.status.podIP}")
	if err != nil {
		c.t.Fatal(err)
	}
	nodes, err := exec.CommandContext(c.x.ctx, "kind", "get", "nodes", "--name", strings.TrimPrefix(c.ctx, "kind-")).Output() //nolint:gosec // args are ours
	if err != nil {
		c.t.Fatal(err)
	}
	for _, n := range strings.Fields(string(nodes)) {
		_ = exec.CommandContext(c.x.ctx, "docker", "exec", n, "conntrack", "-D", "-s", ip).Run() //nolint:gosec // args are ours
		_ = exec.CommandContext(c.x.ctx, "docker", "exec", n, "conntrack", "-D", "-d", ip).Run() //nolint:gosec // args are ours
	}
	if !c.waitCut(pod, 16*time.Second) {
		c.t.Fatalf("isolation: %s kept Raft contact (networkpolicy+conntrack)", pod)
	}
	return "networkpolicy+conntrack"
}

// waitCut: the follower has lost its leader.
func (c *kindCluster) waitCut(pod string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if st := c.status(pod); st.LeaderID == "" || st.State != "Follower" {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func (c *kindCluster) unisolate(pod string) {
	c.t.Helper()
	if _, err := c.kubectl("label", "pod", pod, isolatedLabel+"-"); err != nil {
		c.t.Fatal(err)
	}
}

func (c *kindCluster) opID(what string) string {
	c.seq++
	return fmt.Sprintf("s46-%s-%s-%d", c.run, what, c.seq)
}

// write creates a fresh key and returns its result if it landed on this
// shard (ok false otherwise).
func (c *kindCluster) write(what string) (store.Result, bool) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.x.ctx, 10*time.Second)
	defer cancel()
	id := c.opID(what)
	res, err := c.client.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: id, At: time.Now().UnixMilli()},
		Key: "kv:" + id, Value: json.RawMessage(`1`)})
	if err != nil {
		c.t.Fatalf("write %s: %v", id, err)
	}
	if res.Outcome != store.OutcomeOK {
		c.t.Fatalf("write %s: %+v", id, res)
	}
	return res, res.Shard == c.shard
}

// writeOnShard writes until n keys landed on this shard; it returns the
// last such result and how long that took.
func (c *kindCluster) writeOnShard(what string, n int) (store.Result, time.Duration) {
	c.t.Helper()
	start := time.Now()
	var last store.Result
	for got := 0; got < n; {
		if res, ok := c.write(what); ok {
			last, got = res, got+1
		}
	}
	return last, time.Since(start)
}

// sameRows waits until a and b have applied a final write and compares
// their row digests at the same index.
func (c *kindCluster) sameRows(a, b string) {
	c.t.Helper()
	final, _ := c.writeOnShard("final", 1)
	deadline := time.Now().Add(60 * time.Second)
	for c.status(a).AppliedIndex < final.AppliedIndex || c.status(b).AppliedIndex < final.AppliedIndex {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s and %s did not reach index %d", a, b, final.AppliedIndex)
		}
		time.Sleep(200 * time.Millisecond)
	}
	da, db := c.dump(a), c.dump(b)
	if da.AppliedIndex < final.AppliedIndex || da.AppliedIndex != db.AppliedIndex || len(da.SHA256) != 64 || da.SHA256 != db.SHA256 {
		c.t.Fatalf("rows differ: %s %+v, %s %+v", a, da, b, db)
	}
}

func clusterLogBarrier(x *execution, first, second store.TransitionRequest, propose func(store.TransitionRequest) <-chan scheduleAnswer) (testseam.Point, <-chan scheduleAnswer, <-chan scheduleAnswer) {
	c := newKindCluster(x)
	leader, _ := c.roles()
	id := c.arm(leader, testseam.LogBarrier, []string{first.OperationID, second.OperationID}, barrierTimeout)
	x.t.Cleanup(func() {
		_, _ = c.mgmt(leader, http.MethodPost, "/mgmt/test/seams/"+id+"/release", nil, nil, 3*time.Second)
	})
	one := propose(first)
	c.waitSeam(leader, id, barrierTimeout, func(v seamView) bool { return v.Appended[first.OperationID] != 0 })
	two := propose(second)
	v := c.waitSeam(leader, id, barrierTimeout, func(v seamView) bool { return v.State == testseam.Reached })
	if v.Point == nil {
		x.t.Fatal("reached barrier omitted its point")
	}
	c.release(leader, id)
	return *v.Point, one, two
}

func clusterSnapshotBoundary(x *execution, code string) {
	c := newKindCluster(x)
	leader, followers := c.roles()
	if code == "12" {
		prior := c.snapshot(leader)
		last, _ := c.writeOnShard("snapshot-tail", 8)
		id := c.arm(leader, testseam.SnapshotPersist, nil, streamTimeout)
		done := make(chan error, 1)
		go func() {
			_, err := c.mgmt(leader, http.MethodPost, "/mgmt/snapshot", nil, nil, streamTimeout)
			done <- err
		}()
		v := c.waitSeam(leader, id, 60*time.Second, func(v seamView) bool { return v.State == testseam.Reached })
		if v.Point == nil || v.Point.ImageIndex < last.AppliedIndex || v.Point.Bytes <= 0 || v.Point.Bytes >= v.Point.Size {
			x.t.Fatalf("not a partial new snapshot: %+v", v)
		}
		c.kill(leader)
		select {
		case err := <-done:
			if err == nil {
				x.t.Fatal("interrupted snapshot succeeded")
			}
		case <-x.ctx.Done():
			x.t.Fatal(x.ctx.Err())
		}
		st := c.status(leader)
		if st.RestoredIndex != prior.AppliedIndex || !c.logsContain(leader, "removed incomplete snapshot left by a crash") {
			x.t.Fatalf("incomplete snapshot not discarded: %+v, prior %+v", st, prior)
		}
		newLeader, fs := c.roles()
		for _, f := range fs {
			c.sameRows(newLeader, f)
		}
		if next := c.snapshot(leader); next.AppliedIndex <= prior.AppliedIndex {
			x.t.Fatal("new complete snapshot did not advance")
		}
		x.move(store.RequestMatched, nil)
		return
	}
	follower := followers[0]
	c.isolate(follower)
	behind := c.status(follower).LastLogIndex
	_, took := c.writeOnShard("filler", demoTrailingLogs+fillerMargin)
	snap := c.snapshot(leader)
	if snap.RaftIndex <= behind+demoTrailingLogs {
		x.t.Fatalf("snapshot %d has not compacted beyond follower %d", snap.RaftIndex, behind)
	}
	kind := testseam.RestoreStage
	if code == "13a" {
		kind = testseam.RestoreTransfer
	} else if code != "13b" {
		x.t.Fatal("unknown snapshot schedule:", code)
	}
	id := c.arm(follower, kind, nil, streamTimeout)
	c.unisolate(follower)
	v := c.waitSeam(follower, id, 60*time.Second, func(v seamView) bool { return v.State == testseam.Reached })
	if v.Point == nil || v.Point.Bytes <= 0 || v.Point.Bytes >= v.Point.Size {
		x.t.Fatalf("no partial transfer/stage: %+v", v)
	}
	c.kill(follower)
	st := c.status(follower)
	if code == "13a" && !c.logsContain(follower, "removed incomplete snapshot left by a crash") {
		x.t.Fatal("partial transfer cleanup not observed")
	}
	if code == "13b" && st.RestoredIndex != snap.AppliedIndex {
		x.t.Fatalf("restored index%d want own complete snapshot%d", st.RestoredIndex, snap.AppliedIndex)
	}
	newLeader, fs := c.roles()
	for _, f := range fs {
		c.sameRows(newLeader, f)
	}
	if got := c.status(follower).RestoredIndex; got != snap.AppliedIndex {
		x.t.Fatalf("restored %d want%d", got, snap.AppliedIndex)
	}
	x.t.Logf("SCH%s filler=%s partial=%d/%d restored=%d", code, took, v.Point.Bytes, v.Point.Size, snap.AppliedIndex)
}
