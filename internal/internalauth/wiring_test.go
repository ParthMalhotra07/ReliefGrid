package internalauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

func TestReceiverRefusesBeforeHandler(t *testing.T) {
	for _, token := range []string{"", "wrong", "current", "previous"} {
		called := false
		h := Receiver(auth.ReceiverConfig{Token: "current", PreviousToken: "previous"}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(204) }))
		req := httptest.NewRequest("POST", "/internal/data/x", nil)
		if token != "" {
			req.Header.Set(auth.Header, token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		allowed := token == "current" || token == "previous"
		if called != allowed {
			t.Fatalf("token %q: handler called=%v", token, called)
		}
		if !allowed {
			var e internalapi.Error
			if json.Unmarshal(rec.Body.Bytes(), &e) != nil || rec.Code != 401 || e.Code != internalapi.UNAUTHORIZED || e.Retryable || e.OutcomeUnknown || e.Result != nil {
				t.Fatalf("refusal: %d %s", rec.Code, rec.Body.String())
			}
		}
	}
}

func TestLoadMountedTokens(t *testing.T) {
	dir := t.TempDir()
	current := filepath.Join(dir, "token")
	previous := filepath.Join(dir, "previous-token")
	files := Files{Token: current, PreviousToken: previous}
	if _, err := files.Load(); err == nil {
		t.Fatal("missing current accepted")
	}
	for _, token := range []string{"", "\n", "with space", "valid-token"} {
		if err := os.WriteFile(current, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := files.Load()
		if (err == nil) != (token == "valid-token") {
			t.Fatalf("load %q: %v", token, err)
		}
		if err == nil && cfg.Token != token {
			t.Fatal("token changed")
		}
	}
	if err := os.WriteFile(previous, []byte("old-token"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := files.Load()
	if err != nil || cfg.PreviousToken != "old-token" {
		t.Fatalf("overlap: %v", err)
	}
	if err := os.WriteFile(previous, []byte("bad\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Load(); err == nil {
		t.Fatal("invalid previous accepted")
	}
	if _, err := (Files{}).Load(); err == nil {
		t.Fatal("empty path accepted")
	}
}

func TestSenderKeyRotatesIndependently(t *testing.T) {
	dir := t.TempDir()
	sender := filepath.Join(dir, "sender-token")
	receiver := filepath.Join(dir, "token")
	if err := os.WriteFile(sender, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiver, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadSender(sender)
	if err != nil || s.Token != "old" {
		t.Fatalf("sender: %v", err)
	}
	rc, err := (Files{Token: receiver}).Load()
	if err != nil || rc.Token != "new" {
		t.Fatalf("receiver: %v", err)
	}
	if err := os.WriteFile(sender, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSender(sender); err == nil {
		t.Fatal("empty sender accepted")
	}
}

func TestReceiverRejectsDuplicatesAndEmptyConfiguration(t *testing.T) {
	for _, cfg := range []auth.ReceiverConfig{{Token: "current"}, {}} {
		called := false
		h := Receiver(cfg, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		req := httptest.NewRequest("GET", "/internal/ring", nil)
		req.Header.Add(auth.Header, "current")
		req.Header.Add(auth.Header, "current")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if called || rec.Code != 401 {
			t.Fatal("duplicate or unconfigured receiver reached handler")
		}
	}
}
