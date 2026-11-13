package internalauth

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
)

// captureLogs sends the default logger to a JSON buffer for the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// Every refused request is logged once, naming the credential class, the method and route,
// the peer and why; the presented value and the configured tokens never appear (#29: a
// rotation phase must be able to see a refused sender).
func TestReceiverLogsEachRefusalWithoutTheToken(t *testing.T) {
	const current, previous = "current-secret-token-0123456789", "previous-secret-token-9876543210"
	cfg := auth.ReceiverConfig{Token: current, PreviousToken: previous}
	presented := "presented-wrong-token-ABCDEFGHIJ"
	for _, tc := range []struct {
		name    string
		cfg     auth.ReceiverConfig
		headers []string // values of the auth header, in order
		reason  string   // "" = accepted, nothing logged
	}{
		{"current", cfg, []string{current}, ""},
		{"previous", cfg, []string{previous}, ""},
		{"missing", cfg, nil, "missing"},
		{"wrong", cfg, []string{presented}, "unknown credential"},
		{"repeated", cfg, []string{current, current}, "repeated header"},
		{"malformed", cfg, []string{presented + " with space"}, "malformed"},
		{"comma", cfg, []string{presented + "," + current}, "malformed"},
		{"receiver misconfigured", auth.ReceiverConfig{}, []string{current}, "receiver misconfigured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			called := false
			h := ReceiverFor("node", tc.cfg, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(204) }))
			r := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/internal/data/cfg:k?consistency=strong", nil)
			r.RemoteAddr = "10.244.1.7:51234"
			for _, v := range tc.headers {
				r.Header.Add(auth.Header, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			logged := buf.String()
			for _, secret := range []string{current, previous, presented} {
				if strings.Contains(logged, secret) {
					t.Fatalf("a token reached the log: %s", logged)
				}
			}
			if tc.reason == "" {
				if !called || w.Code != 204 || logged != "" {
					t.Fatalf("accepted request: called=%v code=%d log=%q", called, w.Code, logged)
				}
				return
			}
			if called || w.Code != http.StatusUnauthorized {
				t.Fatalf("refused request reached the handler (called=%v, code %d)", called, w.Code)
			}
			lines := strings.Split(strings.TrimSpace(logged), "\n")
			if len(lines) != 1 {
				t.Fatalf("want one log line per refusal, got %d:\n%s", len(lines), logged)
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
				t.Fatal(err)
			}
			want := map[string]any{"msg": "internal auth refused", "level": "WARN", "credential": "node",
				"method": "PUT", "route": "/internal/data/cfg:k", "peer": "10.244.1.7:51234", "reason": tc.reason}
			for k, v := range want {
				if rec[k] != v {
					t.Errorf("%s = %v, want %v (%s)", k, rec[k], v, lines[0])
				}
			}
		})
	}
}

// Receiver without a class still logs, with credential "internal".
func TestReceiverDefaultClassLogs(t *testing.T) {
	buf := captureLogs(t)
	h := Receiver(auth.ReceiverConfig{Token: "current-secret-token"}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/internal/ring", nil))
	if !strings.Contains(buf.String(), `"credential":"internal"`) || !strings.Contains(buf.String(), `"reason":"missing"`) {
		t.Fatalf("log: %s", buf.String())
	}
}
