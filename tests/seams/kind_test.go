//go:build kind

// Package seams_test drives the S4.6b test seams (#88) on a kind deployment
// running the storagenode:seams image: one case each of SCH01, SCH12,
// SCH13a and SCH13b, with real SIGKILLs. Run it through
// scripts/verify-S4.6b.sh, which builds and installs the seams image, opens
// the coordinator port-forward and restores the default image afterwards.
//
// Environment: SEAMS_CONTEXT (kind-reliefgrid), SEAMS_NAMESPACE
// (reliefgrid), SEAMS_SHARD (1), COORDINATOR_URL (required), SEAMS_RUN_ID.
package seams_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type kindCluster struct {
	t      *testing.T
	ctx    string
	ns     string
	shard  int
	run    string
	token  string
	client *coordclient.Client
	seq    int

	mu  sync.Mutex
	fwd map[string]*forward
}

type forward struct {
	cmd  *exec.Cmd
	port string
}

func newKindCluster(t *testing.T) *kindCluster {
	t.Helper()
	shard, err := strconv.Atoi(env("SEAMS_SHARD", "1"))
	if err != nil {
		t.Fatalf("SEAMS_SHARD: %v", err)
	}
	url := os.Getenv("COORDINATOR_URL")
	if url == "" {
		t.Fatal("COORDINATOR_URL is required (scripts/verify-S4.6b.sh sets it)")
	}
	c := &kindCluster{t: t, ctx: env("SEAMS_CONTEXT", "kind-reliefgrid"), ns: env("SEAMS_NAMESPACE", "reliefgrid"),
		shard: shard, run: env("SEAMS_RUN_ID", strconv.FormatInt(time.Now().UnixMilli(), 36)), fwd: map[string]*forward{}}
	t.Cleanup(c.closeForwards)
	tok, err := c.kubectl("get", "secret", "storage-operator-token", "-o", "jsonpath={.data.token}")
	if err != nil {
		t.Fatalf("operator token: %v", err)
	}
	dec, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		t.Fatalf("operator token: %v", err)
	}
	c.token = string(dec)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// internalAuth.enabled (#29): scripts/verify-S4.6b.sh exports the app sender
	// token's file; unset (auth off) the client sends no token, as before.
	var opts []coordclient.Option
	if f := os.Getenv("INTERNAL_AUTH_TOKEN_FILE"); f != "" {
		opts = append(opts, coordclient.WithInternalAuthFile(f))
	}
	if c.client, err = coordclient.New(ctx, url, opts...); err != nil {
		t.Fatalf("coordclient: %v", err)
	}
	for _, p := range c.pods() {
		if st := c.status(p); !st.Seams {
			t.Fatalf("%s is not a seams build (status %+v): install storagenode:seams first", p, st)
		}
	}
	return c
}

func (c *kindCluster) kubectl(args ...string) (string, error) {
	full := append([]string{"--context", c.ctx, "-n", c.ns}, args...)
	out, err := exec.CommandContext(context.Background(), "kubectl", full...).CombinedOutput() //nolint:gosec // args are ours
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
	cmd := exec.CommandContext(context.Background(), "kubectl", "--context", c.ctx, "-n", c.ns, "port-forward", "pod/"+pod, ":8081") //nolint:gosec // args are ours
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
	for attempt := 0; attempt < 2; attempt++ {
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
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		if cond(v) {
			return v
		}
		if v.State == testseam.Expired || v.State == testseam.Aborted || v.State == testseam.Released {
			c.t.Fatalf("seam %s on %s is %s before the expected point: %+v", id, pod, v.State, v)
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
	before := c.restartCount(pod)
	node, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
	if err == nil {
		err = crictlKill(node, pod, c.ns)
	}
	method := "crictl"
	if err != nil {
		c.t.Logf("crictl kill of %s failed (%v); falling back to kubectl delete --force", pod, err)
		if _, err := c.kubectl("delete", "pod", pod, "--force", "--grace-period=0"); err != nil {
			c.t.Fatal(err)
		}
		method = "kubectl-delete-force"
	}
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
	c.t.Logf("killed %s (%s)", pod, method)
	return method
}

func crictlKill(node, pod, ns string) error {
	out, err := exec.CommandContext(context.Background(), "docker", "exec", node, "crictl", "ps", "-q", "--name", "storagenode", //nolint:gosec // args are ours
		"--label", "io.kubernetes.pod.name="+pod, "--label", "io.kubernetes.pod.namespace="+ns).Output()
	if err != nil {
		return fmt.Errorf("crictl ps: %w", err)
	}
	cid := strings.TrimSpace(string(out))
	if cid == "" || strings.Contains(cid, "\n") {
		return fmt.Errorf("crictl ps: container ids %q", cid)
	}
	out, err = exec.CommandContext(context.Background(), "docker", "exec", node, "crictl", "inspect", "-o", "go-template", "--template", "{{.info.pid}}", cid).Output() //nolint:gosec // args are ours
	if err != nil {
		return fmt.Errorf("crictl inspect: %w", err)
	}
	pid := strings.TrimSpace(string(out))
	if _, err := strconv.Atoi(pid); err != nil {
		return fmt.Errorf("crictl inspect: pid %q", pid)
	}
	if out, err := exec.CommandContext(context.Background(), "docker", "exec", node, "kill", "-9", pid).CombinedOutput(); err != nil { //nolint:gosec // args are ours
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
	c.t.Cleanup(func() { _, _ = c.kubectl("label", "pod", pod, isolatedLabel+"-") })
	if c.waitCut(pod, 8*time.Second) {
		return "networkpolicy"
	}
	ip, err := c.kubectl("get", "pod", pod, "-o", "jsonpath={.status.podIP}")
	if err != nil {
		c.t.Fatal(err)
	}
	nodes, err := exec.CommandContext(context.Background(), "kind", "get", "nodes", "--name", strings.TrimPrefix(c.ctx, "kind-")).Output() //nolint:gosec // args are ours
	if err != nil {
		c.t.Fatal(err)
	}
	for _, n := range strings.Fields(string(nodes)) {
		_ = exec.CommandContext(context.Background(), "docker", "exec", n, "conntrack", "-D", "-s", ip).Run() //nolint:gosec // args are ours
		_ = exec.CommandContext(context.Background(), "docker", "exec", n, "conntrack", "-D", "-d", ip).Run() //nolint:gosec // args are ours
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

// keyOnShard creates a key on this shard and returns it.
func (c *kindCluster) keyOnShard() string {
	c.t.Helper()
	for {
		res, ok := c.write("key")
		if ok {
			return "kv:" + res.OperationID
		}
	}
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
	if da.AppliedIndex != db.AppliedIndex || da.SHA256 != db.SHA256 {
		c.t.Fatalf("rows differ: %s %+v, %s %+v", a, da, b, db)
	}
}

// SCH01 on kind: on the leader, A then B are in the log and committed
// before either applies; on release A wins and B is a VERSION_CONFLICT.
func TestKindSCH01LogBarrier(t *testing.T) {
	c := newKindCluster(t)
	leader, _ := c.roles()
	key := c.keyOnShard()
	opA, opB := c.opID("a"), c.opID("b")
	id := c.arm(leader, testseam.LogBarrier, []string{opA, opB}, barrierTimeout)

	cas := func(op string, v int) <-chan error {
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			res, err := c.client.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: op, At: time.Now().UnixMilli()},
				Key: key, ExpectedVersion: 1, Value: json.RawMessage(strconv.Itoa(v))})
			switch {
			case err != nil:
				done <- err
			case op == opA && (res.Outcome != store.OutcomeOK || res.Version != 2):
				done <- fmt.Errorf("A: %+v, want OK at version 2", res)
			case op == opB && res.Outcome != store.OutcomeVersionConflict:
				done <- fmt.Errorf("B: %+v, want VERSION_CONFLICT", res)
			default:
				done <- nil
			}
		}()
		return done
	}
	a := cas(opA, 2)
	c.waitSeam(leader, id, barrierTimeout, func(v seamView) bool { return v.Appended[opA] != 0 })
	b := cas(opB, 3)
	v := c.waitSeam(leader, id, barrierTimeout, func(v seamView) bool { return v.State == testseam.Reached })
	c.release(leader, id)
	p := v.Point
	if p == nil || p.Indexes[opA] == 0 || p.Indexes[opB] <= p.Indexes[opA] || p.CommitIndex < p.Indexes[opB] || p.AppliedIndex >= p.Indexes[opA] {
		t.Fatalf("barrier point %+v: want A then B appended and committed, neither applied", p)
	}
	if err := <-a; err != nil {
		t.Fatal(err)
	}
	if err := <-b; err != nil {
		t.Fatal(err)
	}
	t.Logf("SCH01: A at %d, B at %d, commit %d, applied %d when reached", p.Indexes[opA], p.Indexes[opB], p.CommitIndex, p.AppliedIndex)
}

// SCH12 on kind: the leader is SIGKILLed halfway through writing a
// snapshot, after one complete snapshot. On restart it removes the
// incomplete snapshot, restores the complete one and replays the log; its
// rows equal the new leader's.
func TestKindSCH12CrashMidSnapshotCreation(t *testing.T) {
	c := newKindCluster(t)
	leader, _ := c.roles()
	c.writeOnShard("a", 8)
	prior := c.snapshot(leader)
	last, _ := c.writeOnShard("b", 8)
	id := c.arm(leader, testseam.SnapshotPersist, nil, streamTimeout)
	go func() { _, _ = c.mgmt(leader, http.MethodPost, "/mgmt/snapshot", nil, nil, streamTimeout) }() // dies with the leader
	v := c.waitSeam(leader, id, 60*time.Second, func(v seamView) bool { return v.State == testseam.Reached })
	if v.Point.ImageIndex < last.AppliedIndex || v.Point.Bytes <= 0 || v.Point.Bytes >= v.Point.Size {
		t.Fatalf("paused at %+v: want a partial image at or after %d", v.Point, last.AppliedIndex)
	}
	method := c.kill(leader)
	st := c.status(leader)
	if st.RestoredIndex != prior.AppliedIndex {
		t.Fatalf("restarted %s restored %d, want the prior complete snapshot's %d", leader, st.RestoredIndex, prior.AppliedIndex)
	}
	if method == "crictl" && !c.logsContain(leader, "removed incomplete snapshot left by a crash") {
		t.Fatal("the restarted node did not report removing the incomplete snapshot")
	}
	newLeader, _ := c.roles()
	if newLeader != leader {
		c.sameRows(newLeader, leader)
	} else {
		_, fs := c.roles()
		c.sameRows(leader, fs[0])
	}
	if next := c.snapshot(leader); next.AppliedIndex <= prior.AppliedIndex {
		t.Fatalf("next snapshot %+v, want one after %+v", next, prior)
	}
	t.Logf("SCH12: paused at %+v; restored %d; kill %s", v.Point, st.RestoredIndex, method)
}

// behindCompactedLog isolates a follower, writes past the trailing logs on
// the shard and snapshots the leader. It returns the follower and the
// snapshot, and logs the filler time (the 60 s/seed budget for SCH13).
func (c *kindCluster) behindCompactedLog() (leader, follower string, snap snapshotInfo) {
	c.t.Helper()
	leader, followers := c.roles()
	follower = followers[0]
	method := c.isolate(follower)
	behind := c.status(follower).LastLogIndex
	_, took := c.writeOnShard("filler", demoTrailingLogs+fillerMargin)
	c.t.Logf("filler: %d writes on shard %d in %v (isolation %s)", demoTrailingLogs+fillerMargin, c.shard, took, method)
	snap = c.snapshot(leader)
	if snap.RaftIndex <= behind+demoTrailingLogs {
		c.t.Fatalf("leader snapshot at %d does not compact past the follower's %d", snap.RaftIndex, behind)
	}
	return leader, follower, snap
}

func testKindSCH13(t *testing.T, kind testseam.Kind) {
	c := newKindCluster(t)
	leader, follower, snap := c.behindCompactedLog()
	id := c.arm(follower, kind, nil, streamTimeout)
	c.unisolate(follower)
	v := c.waitSeam(follower, id, 60*time.Second, func(v seamView) bool { return v.State == testseam.Reached })
	if v.Point.Bytes <= 0 || v.Point.Bytes >= v.Point.Size {
		t.Fatalf("paused at %+v: want a partial %s", v.Point, kind)
	}
	method := c.kill(follower)
	st := c.status(follower)
	switch kind {
	case testseam.RestoreTransfer:
		// The partial transfer was never a snapshot: the leader sends it again.
		if method == "crictl" && !c.logsContain(follower, "removed incomplete snapshot left by a crash") {
			t.Fatal("the restarted follower did not report removing the partial transfer")
		}
	case testseam.RestoreStage:
		// The snapshot was complete on the PVC: restored at Open.
		if st.RestoredIndex != snap.AppliedIndex {
			t.Fatalf("restarted follower restored %d at open, want its own copy at %d", st.RestoredIndex, snap.AppliedIndex)
		}
	}
	c.sameRows(leader, follower)
	if got := c.status(follower).RestoredIndex; got != snap.AppliedIndex {
		t.Fatalf("follower restored %d, want the leader's snapshot at %d", got, snap.AppliedIndex)
	}
	t.Logf("%s: paused at %+v; kill %s", kind, v.Point, method)
}

// SCH13a on kind: a follower is SIGKILLed halfway through receiving
// InstallSnapshot; on restart the leader sends it again.
func TestKindSCH13aCrashMidInstallTransfer(t *testing.T) { testKindSCH13(t, testseam.RestoreTransfer) }

// SCH13b on kind: a follower is SIGKILLed halfway through staging a
// snapshot it fully received; on restart it restores its own copy.
func TestKindSCH13bCrashMidRestoreStage(t *testing.T) { testKindSCH13(t, testseam.RestoreStage) }
