package fsm_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Trace-only retry dedupe (apply.go, traceOnlyRetry) reads each event's
// committed traceparent from the outbox table. It is complete only while
// committed outbox rows are permanent and their event column is never
// rewritten. These checks fail if FSM code deletes outbox rows or updates
// anything but the published flag; a future retention or compaction change
// must keep or migrate what dedupe needs, then update this test.

var (
	sqlOutboxDelete  = regexp.MustCompile(`(?is)\bDELETE\s+FROM\s+outbox\b`)
	sqlOutboxReplace = regexp.MustCompile(`(?is)\b(REPLACE|INSERT\s+OR\s+REPLACE|INSERT\s+OR\s+IGNORE|UPSERT)\s+INTO\s+outbox\b|\bON\s+CONFLICT\b.*\bDO\s+UPDATE\b`)
	sqlOutboxDDL     = regexp.MustCompile(`(?is)\b(DROP|ALTER)\s+TABLE\s+outbox\b`)
	sqlOutboxUpdate  = regexp.MustCompile(`(?is)\bUPDATE\s+outbox\s+SET\s+(.*?)(\bWHERE\b|$)`)
	sqlSetColumn     = regexp.MustCompile(`(?is)^\s*([a-z_]+)\s*=`)
)

// outboxViolations reports SQL in src that could remove a committed outbox
// row or change anything in it other than the published flag.
func outboxViolations(src string) []string {
	var out []string
	if sqlOutboxDelete.MatchString(src) {
		out = append(out, "deletes outbox rows")
	}
	if sqlOutboxReplace.MatchString(src) && strings.Contains(strings.ToLower(src), "outbox") {
		out = append(out, "replaces or upserts outbox rows")
	}
	if sqlOutboxDDL.MatchString(src) {
		out = append(out, "drops or alters the outbox table")
	}
	for _, m := range sqlOutboxUpdate.FindAllStringSubmatch(src, -1) {
		for _, assign := range strings.Split(m[1], ",") {
			col := sqlSetColumn.FindStringSubmatch(assign)
			if col == nil || strings.ToLower(col[1]) != "published" {
				out = append(out, "updates outbox columns other than published: "+strings.TrimSpace(m[1]))
				break
			}
		}
	}
	return out
}

func TestOutboxViolationsDetector(t *testing.T) {
	for _, bad := range []string{
		`DELETE FROM outbox WHERE published = 1`,
		"delete\n  from outbox where seq < ?",
		`UPDATE outbox SET event = ? WHERE event_id = ?`,
		`UPDATE outbox SET published = 1, event = ? WHERE event_id = ?`,
		`UPDATE outbox SET event=json_set(event, '$.payload', ?)`,
		`INSERT OR REPLACE INTO outbox (event_id, event) VALUES (?, ?)`,
		`REPLACE INTO outbox (event_id) VALUES (?)`,
		`INSERT INTO outbox (event_id) VALUES (?) ON CONFLICT (event_id) DO UPDATE SET event = excluded.event`,
		`DROP TABLE outbox`,
		`ALTER TABLE outbox DROP COLUMN event`,
	} {
		if len(outboxViolations(bad)) == 0 {
			t.Errorf("detector missed %q", bad)
		}
	}
	for _, ok := range []string{
		`UPDATE outbox SET published = 1 WHERE published = 0 AND event_id IN (`,
		`INSERT INTO outbox (event_id, entity_key, entity_version, event, published, created_at) VALUES (?, ?, ?, ?, 0, ?)`,
		`SELECT event FROM outbox WHERE event_id = ?`,
		`DELETE FROM outbox_archive`,
	} {
		if v := outboxViolations(ok); len(v) != 0 {
			t.Errorf("detector flagged %q: %v", ok, v)
		}
	}
}

// Verify (S1.x): no FSM code removes committed outbox rows or rewrites them.
func TestFSMNeverDeletesOrRewritesOutboxRows(t *testing.T) {
	var files []string
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no FSM sources found")
	}
	fset := token.NewFileSet()
	literals := 0
	for _, path := range files {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			literals++
			for _, v := range outboxViolations(s) {
				t.Errorf("%s: %s (dedupe needs permanent outbox rows; see apply.go traceOnlyRetry)", fset.Position(lit.Pos()), v)
			}
			return true
		})
	}
	if literals == 0 {
		t.Fatal("scanned no string literals")
	}
}

// Publishing flips only the published column: the committed event, its
// traceparent included, is unchanged.
func TestMarkPublishedKeepsCommittedEvent(t *testing.T) {
	h := newHarness(t)
	expectOutcome(t, "create", h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
		Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceA)}}), store.OutcomeOK)
	before, err := h.f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	expectOutcome(t, "mark", h.apply(store.MarkOutboxPublished{Meta: meta("mark"), EventIDs: []string{"ev-c-q1"}}), store.OutcomeOK)
	after, err := h.f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(before["outbox"]) != 1 || len(after["outbox"]) != 1 {
		t.Fatalf("outbox rows before/after = %d/%d, want 1/1", len(before["outbox"]), len(after["outbox"]))
	}
	b, a := before["outbox"][0], after["outbox"][0]
	if !strings.Contains(a, traceA) || !strings.Contains(b, "published=0") ||
		strings.Replace(b, "published=0", "published=1", 1) != a {
		t.Fatalf("published row changed beyond the flag:\nbefore %s\nafter  %s", b, a)
	}
	expectOutcome(t, "trace-only retry after publish", h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
		Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceB)}}), store.OutcomeOK)
}
