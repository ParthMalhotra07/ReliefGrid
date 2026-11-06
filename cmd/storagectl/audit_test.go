package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit"
	"github.com/ParthMalhotra07/ReliefGrid/internal/storagenode/audit/audittest"
)

func tokenEnv(token string) func(string) string {
	return func(k string) string {
		if k == "OPERATOR_TOKEN" {
			return token
		}
		return ""
	}
}

// storagectl merkle asks one pod for its digest: at only if given, and a
// bucket only with its table.
func TestMerkleCommand(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_, _ = w.Write([]byte(`{"podName":"storage-shard-0-0"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	for _, tc := range []struct {
		args  []string
		query string
	}{
		{nil, ""},
		{[]string{"--at", "7"}, "at=7"},
		{[]string{"--at", "7", "--table", "records", "--bucket", "3"}, "at=7&bucket=3&table=records"},
	} {
		var out bytes.Buffer
		if err := runMerkle(t.Context(), append([]string{"--addr", addr}, tc.args...), tokenEnv("tok"), &out); err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if got.URL.Path != "/mgmt/merkle" || got.URL.RawQuery != tc.query || got.Header.Get("Authorization") != "Bearer tok" {
			t.Fatalf("%v: sent %s?%s", tc.args, got.URL.Path, got.URL.RawQuery)
		}
		if !strings.Contains(out.String(), "storage-shard-0-0") {
			t.Fatalf("printed %q", out.String())
		}
	}
	for _, args := range [][]string{
		{"--table", "records"},
		{"--bucket", "1"},
		{"--at", "x"},
		{"--at", strconv.FormatUint(fsm.AnyIndex, 10)},
	} {
		if err := runMerkle(t.Context(), append([]string{"--addr", addr}, args...), tokenEnv("tok"), &bytes.Buffer{}); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

func auditArgs(c *audittest.Cluster) []string {
	args := []string{"--settle", "300ms", "--poll", "10ms"}
	for _, a := range c.Addrs() {
		args = append(args, "--addr", a)
	}
	return args
}

func runAuditCmd(t *testing.T, c *audittest.Cluster) (audit.Report, error) {
	t.Helper()
	var out bytes.Buffer
	err := runAudit(t.Context(), auditArgs(c), tokenEnv(audittest.Token), &out)
	c.ExpectExactQueries(t)
	var rep audit.Report
	if jerr := json.Unmarshal(out.Bytes(), &rep); jerr != nil {
		t.Fatalf("output %q: %v", out.String(), jerr)
	}
	return rep, err
}

func exitCodeOf(err error) int {
	var ec *exitCodeError
	if errors.As(err, &ec) {
		return ec.code
	}
	if err != nil {
		return 1
	}
	return 0
}

// storagectl audit prints the report as JSON and exits 0 equal, 1
// divergent, 2 incomplete, 3 not quiescent. It never compares at
// fsm.AnyIndex: every digest it asks for names one exact command index.
func TestAuditCommandVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(c *audittest.Cluster)
		verdict audit.Verdict
		code    int
	}{
		{"equal", func(*audittest.Cluster) {}, audit.Equal, 0},
		{"divergent", func(c *audittest.Cluster) { c.Pods[1].Rows[4][2] = `{"n":"x"}` }, audit.Divergent, 1},
		{"a pod claims AnyIndex", func(c *audittest.Cluster) { c.Pods[2].AppliedIndex = fsm.AnyIndex }, audit.Incomplete, 2},
		{"a pod is down", func(c *audittest.Cluster) { c.Pods[0].Down = true }, audit.Incomplete, 2},
		{"a pod is behind", func(c *audittest.Cluster) { c.Pods[2].AppliedIndex = 3 }, audit.NotQuiescent, 3},
		{"pods keep moving", func(c *audittest.Cluster) {
			c.OnMerkle = func(_ *audittest.Cluster, p *audittest.Pod) { p.AppliedIndex++ }
		}, audit.NotQuiescent, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := audittest.NewCluster(t, 40, "pod-0", "pod-1", "pod-2")
			tc.setup(c)
			rep, err := runAuditCmd(t, c)
			if rep.Verdict != tc.verdict || exitCodeOf(err) != tc.code {
				t.Fatalf("verdict %s exit %d (%v), want %s exit %d", rep.Verdict, exitCodeOf(err), err, tc.verdict, tc.code)
			}
			if tc.verdict == audit.Divergent && (len(rep.Rows) != 1 || rep.Rows[0].PK != "key:004" || rep.Rows[0].Pods[0] != "pod-1") {
				t.Fatalf("rows = %+v", rep.Rows)
			}
		})
	}
}

func TestAuditCommandUsage(t *testing.T) {
	c := audittest.NewCluster(t, 5, "pod-0", "pod-1")
	if err := runAudit(t.Context(), auditArgs(c), tokenEnv(""), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "OPERATOR_TOKEN") {
		t.Fatalf("no token: %v", err)
	}
	if err := runAudit(t.Context(), []string{"--addr", c.Addrs()[0]}, tokenEnv("t"), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "two") {
		t.Fatalf("one addr: %v", err)
	}
	if len(c.Queries()) != 0 {
		t.Fatalf("usage errors still queried: %v", c.Queries())
	}
}
