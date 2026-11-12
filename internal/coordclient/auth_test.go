package coordclient_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestInternalAuthBeforeInitialRing(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get(auth.Header) != "app-token" {
			t.Error("missing app credential")
		}
		writeJSON(w, 200, ringV1)
	}))
	defer srv.Close()
	for _, opts := range [][]coordclient.Option{
		{coordclient.WithInternalAuth(auth.SenderConfig{}), coordclient.WithHTTPClient(srv.Client())},
		{coordclient.WithHTTPClient(srv.Client()), coordclient.WithInternalAuth(auth.SenderConfig{})},
	} {
		if _, err := coordclient.New(t.Context(), srv.URL, opts...); err == nil {
			t.Fatal("empty config accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid config sent %d calls", calls.Load())
	}
	if _, err := coordclient.New(t.Context(), srv.URL, coordclient.WithInternalAuth(auth.SenderConfig{Token: "app-token"}), coordclient.WithHTTPClient(srv.Client())); err != nil {
		t.Fatal(err)
	}

}

func TestAuthRefusalKeepsEarlierUnknownOperation(t *testing.T) {
	f := newFake(t)
	f.answer = func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			writeJSON(w, 503, internalapi.Error{Code: internalapi.OUTCOMEUNKNOWN, OutcomeUnknown: true, Retryable: true})
			return
		}
		writeJSON(w, 401, internalapi.Error{Code: internalapi.UNAUTHORIZED, Message: "refused before handler"})
	}
	c, err := coordclient.New(t.Context(), f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	cmd := store.CreateEntity{Meta: store.Meta{OperationID: "same-operation", At: t0}, Key: "kv:auth", Value: json.RawMessage(`1`)}
	for range 2 {
		if _, err := c.CreateEntity(t.Context(), cmd); !errors.Is(err, store.ErrUnknownOutcome) {
			t.Fatalf("lost unknown: %v", err)
		}
	}
	f.mu.Lock()
	recorded := append([]seen(nil), f.reqs...)
	f.mu.Unlock()
	for _, seen := range recorded {
		var got map[string]any
		if json.Unmarshal(seen.body, &got) != nil || got["operationId"] != "same-operation" {
			t.Fatal("operation changed")
		}
	}
	fresh, err := coordclient.New(t.Context(), f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.CreateEntity(t.Context(), cmd); err == nil || errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("first refusal should be definite: %v", err)
	}
}

func TestSeparateClientsShareOnlyRawTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "app-token"
		if r.URL.Path == "/node" {
			want = "node-token"
		}
		if r.URL.Path == "/raw" {
			want = ""
		}
		if r.Header.Get(auth.Header) != want {
			t.Errorf("%s credential mismatch", r.URL.Path)
		}
		writeJSON(w, 200, ringV1)
	}))
	defer srv.Close()
	raw := srv.Client()
	raw.Timeout = time.Second
	if _, err := coordclient.New(t.Context(), srv.URL, coordclient.WithHTTPClient(raw), coordclient.WithInternalAuth(auth.SenderConfig{Token: "app-token"})); err != nil {
		t.Fatal(err)
	}
	base := *raw
	base.Timeout = time.Second
	node, err := auth.NewClient(&base, auth.SenderConfig{Token: "node-token"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		client *http.Client
		path   string
	}{{node, "/node"}, {raw, "/raw"}} {
		req, err := http.NewRequestWithContext(t.Context(), "GET", srv.URL+item.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := item.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

func TestMalformedAuthRefusalCannotProveRejection(t *testing.T) {
	f := newFake(t)
	f.answer = func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte("proxy unauthorized"))
	}
	c, err := coordclient.New(t.Context(), f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	cmd := store.CreateEntity{Meta: store.Meta{OperationID: "auth-malformed", At: t0}, Key: "kv:auth", Value: json.RawMessage(`1`)}
	if _, err := c.CreateEntity(t.Context(), cmd); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("unstructured 401 treated as rejection: %v", err)
	}
	f.mu.Lock()
	f.answer = nil
	f.mu.Unlock()
	if _, err := c.CreateEntity(t.Context(), cmd); err != nil {
		t.Fatalf("committed retry: %v", err)
	}
	f.mu.Lock()
	f.answer = func(w http.ResponseWriter, _ *http.Request, _ int) {
		writeJSON(w, 401, internalapi.Error{Code: internalapi.UNAUTHORIZED})
	}
	f.mu.Unlock()
	if _, err := c.CreateEntity(t.Context(), cmd); errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("committed answer did not resolve uncertainty: %v", err)
	}
}

func TestDeleteAuthRefusalKeepsUnknown(t *testing.T) {
	f := newFake(t)
	f.answer = func(w http.ResponseWriter, r *http.Request, n int) {
		if r.URL.Query().Get("operationId") != "delete-auth" {
			t.Error("missing query operationId")
		}
		if n == 1 {
			writeJSON(w, 503, internalapi.Error{Code: internalapi.OUTCOMEUNKNOWN, OutcomeUnknown: true})
			return
		}
		writeJSON(w, 403, internalapi.Error{Code: internalapi.UNAUTHORIZED})
	}
	c, err := coordclient.New(t.Context(), f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	cmd := store.CompareAndSet{Delete: true, Meta: store.Meta{OperationID: "delete-auth", At: t0}, Key: "kv:auth"}
	for range 2 {
		if _, err := c.CompareAndSet(t.Context(), cmd); !errors.Is(err, store.ErrUnknownOutcome) {
			t.Fatalf("lost DELETE uncertainty: %v", err)
		}
	}
}

func TestStructured401WithOutcomeCannotProveRejection(t *testing.T) {
	for _, body := range []internalapi.Error{
		{Code: internalapi.UNAUTHORIZED, Result: &store.Result{Outcome: store.OutcomeOK}},
		{Code: internalapi.UNAUTHORIZED, OutcomeUnknown: true},
	} {
		f := newFake(t)
		f.answer = func(w http.ResponseWriter, _ *http.Request, _ int) { writeJSON(w, 401, body) }
		c, err := coordclient.New(t.Context(), f.srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CreateEntity(t.Context(), store.CreateEntity{Meta: store.Meta{OperationID: "auth-result", At: t0}, Key: "kv:auth", Value: json.RawMessage(`1`)})
		if !errors.Is(err, store.ErrUnknownOutcome) {
			t.Fatalf("401 with outcome treated as rejection: %v", err)
		}
	}
}

func TestInternalAuthFileBeforeInitialRing(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get(auth.Header) != "file-token" {
			t.Error("missing file token")
		}
		writeJSON(w, 200, ringV1)
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "sender-token")
	for _, token := range []string{"", "bad\n", "file-token"} {
		if err := os.WriteFile(path, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := coordclient.New(t.Context(), srv.URL, coordclient.WithInternalAuthFile(path))
		if (err == nil) != (token == "file-token") {
			t.Fatalf("file validation: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("invalid token sent: %d calls", calls.Load())
	}
	if _, err := coordclient.New(t.Context(), srv.URL, coordclient.WithInternalAuthFile(path+"-missing")); err == nil {
		t.Fatal("missing file accepted")
	}
}
