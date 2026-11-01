package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/chaos/judge"
)

// invcheck judge reads one run's observations and exits by the verdict:
// 0 passed, 1 failed, 2 no verdict; bad input is no verdict, not a pass.
func TestJudgeCommand(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	passing := `{"experiment":"kill-leader","shard":1,"target":"storage-shard-1-0","bounds":{"electionMaxSeconds":10,"recoveryMaxSeconds":60},
		"verify":{"ok":true,"ledger":{"acked":5,"unknown":1,"rejected":0,"reads":9,"ackedInWindow":2,"errorCount":1,"errors":[]},
			"resolution":{"resolved":1,"notApplied":0,"stillUnknown":0},
			"verdict":{"checked":5,"keys":2,"lost":[],"unknown":0,"reads":{"total":9,"checked":7,"unchecked":2,"failed":0,"served":{}},"staleReads":[]},
			"availability":{"fromMs":1,"toMs":2,"graceMs":0,"shards":[{"shard":1,"acksDuring":2,"firstAckAfterFaultMs":5,"firstAckAfterHealMs":5}]}},
		"verifyExit":0,"check":{"ok":true,"verdict":"clean","report":{"quiescent":true,"violations":[]}},"checkExit":0,
		"harness":{"settleOk":true}}`
	failing := strings.Replace(passing, `"stillUnknown":0`, `"stillUnknown":1`, 1)
	for _, tc := range []struct {
		name, body string
		code       int
		pass       bool
	}{
		{"passing", passing, exitClean, true},
		{"failing", failing, exitViolations, false},
		{"unknown field", `{"experiment":"kill-leader","surprise":1}`, exitIncomplete, false},
	} {
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), []string{"judge", "--input", write(tc.name+".json", tc.body)}, &stdout, &stderr)
		if code != tc.code {
			t.Errorf("%s: exit %d, want %d (%s%s)", tc.name, code, tc.code, stdout.String(), stderr.String())
			continue
		}
		if tc.code == exitIncomplete {
			continue
		}
		var r judge.Result
		if err := json.Unmarshal(stdout.Bytes(), &r); err != nil || r.Pass != tc.pass || r.Summary.UnknownRaw != 1 {
			t.Errorf("%s: result %s (%v)", tc.name, stdout.String(), err)
		}
	}
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"judge"}, &bytes.Buffer{}, &stderr); code != exitUsage {
		t.Errorf("judge without --input: exit %d, want %d", code, exitUsage)
	}
}
