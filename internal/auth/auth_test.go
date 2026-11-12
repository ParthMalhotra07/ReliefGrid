package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const current = "fixture-current-credential"
const previous = "fixture-previous-credential"

func TestConfigValidationAndRedaction(t *testing.T) {
	for _, token := range []string{"", "has space", " leading", "trailing ", "a,b", "a\nb", "a\x00b", "nonascii-é", strings.Repeat("x", 4097)} {
		if err := (SenderConfig{Token: token}).Validate(); err == nil {
			t.Fatalf("accepted invalid sender token of length %d", len(token))
		}
		if err := (ReceiverConfig{Token: token}).Validate(); err == nil {
			t.Fatalf("accepted invalid receiver token of length %d", len(token))
		}
	}
	if err := (ReceiverConfig{Token: current, PreviousToken: "bad previous"}).Validate(); err == nil {
		t.Fatal("accepted malformed previous token")
	}
	for _, config := range []any{SenderConfig{Token: current}, ReceiverConfig{Token: current, PreviousToken: previous}} {
		var log strings.Builder
		slog.New(slog.NewJSONHandler(&log, nil)).Info("config", "auth", config)
		encoded, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		formatted := fmt.Sprintf("%v %+v %#v %s", config, config, config, config) + string(encoded) + log.String()
		if strings.Contains(formatted, current) || strings.Contains(formatted, previous) {
			t.Fatalf("credential leaked through config formatting: %s", formatted)
		}
	}
}

func TestReceiverOverlapStrictHeadersAndFailClosed(t *testing.T) {
	config := ReceiverConfig{Token: current, PreviousToken: previous}
	for _, token := range []string{current, previous} {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://storage/internal/data", nil)
		r.Header.Set(Header, token)
		if !config.Authorize(r) {
			t.Fatal("rotation credential rejected")
		}
		if (ReceiverConfig{Token: "different-class-token"}).Authorize(r) {
			t.Fatal("credential from another class accepted")
		}
	}
	for name, header := range map[string]http.Header{
		"missing": {}, "empty": {Header: {""}}, "wrong": {Header: {"wrong"}},
		"duplicate": {Header: {current, current}}, "mixed": {Header: {current, "wrong"}},
		"comma joined": {Header: {current + "," + previous}}, "space": {Header: {" " + current}},
		"case duplicate": {Header: {current}, strings.ToLower(Header): {current}},
		"bearer only":    {"Authorization": {"Bearer " + current}},
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://storage/internal/data", nil)
			r.Header = header
			if config.Authorize(r) {
				t.Fatal("malformed or absent credential accepted")
			}
		})
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://storage/internal/data", nil)
	r.Header.Set(Header, previous)
	if (ReceiverConfig{Token: current}).Authorize(r) {
		t.Fatal("retired token accepted")
	}
	for _, invalid := range []ReceiverConfig{{}, {PreviousToken: previous}, {Token: current, PreviousToken: "bad value"}} {
		if invalid.Authorize(r) {
			t.Fatal("unvalidated config failed open")
		}
	}
	if config.Authorize(nil) {
		t.Fatal("nil request accepted")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closingTransport struct {
	transportFunc
	closed bool
}

func (t *closingTransport) CloseIdleConnections() { t.closed = true }

func TestClientRequiresBoundedCallsAndPreservesIdleCleanup(t *testing.T) {
	var calls atomic.Int32
	transport := &closingTransport{transportFunc: func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	}}
	client, err := NewClient(&http.Client{Transport: transport}, SenderConfig{Token: current})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://coordinator/internal/ring", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(r)
	if response != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if err == nil || calls.Load() != 0 {
		t.Fatal("unbounded request reached the transport")
	}
	client.CloseIdleConnections()
	if !transport.closed {
		t.Fatal("idle connection cleanup was swallowed by wrapper")
	}
}

func TestClientReplacesCredentialWithoutMutatingSharedInputs(t *testing.T) {
	var calls atomic.Int32
	base := &http.Client{Timeout: time.Second, Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if got := r.Header.Values(Header); len(got) != 1 || got[0] != current || len(r.Header[strings.ToLower(Header)]) != 0 {
			t.Errorf("forwarded caller credential instead of configured credential")
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Correlation-Id") != "fixture-correlation" {
			t.Error("forwarded unrelated credentials or dropped a noncredential header")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	})}
	client, err := NewClient(base, SenderConfig{Token: current})
	if err != nil {
		t.Fatal(err)
	}
	if client == base || client.Timeout != base.Timeout || base.CheckRedirect != nil {
		t.Fatal("shared client mutated or timeout lost")
	}
	if _, err := NewClient(client, SenderConfig{Token: previous}); err == nil {
		t.Fatal("nested transports could override the intended credential class")
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://coordinator/internal/ring", nil)
	r.RequestURI = ""
	r.Header = http.Header{Header: {previous}, strings.ToLower(Header): {previous}}
	r.Header.Set("Authorization", "Bearer fixture-operator-token")
	r.Header.Set("Proxy-Authorization", "Basic fixture-proxy-token")
	r.Header.Set("X-Correlation-Id", "fixture-correlation")
	originalHeaders := r.Header.Clone()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			response, doErr := client.Do(r)
			if doErr != nil {
				t.Error(doErr)
				return
			}
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 12 || !reflect.DeepEqual(r.Header, originalHeaders) {
		t.Fatal("caller request changed or calls missing")
	}
	if _, err := NewClient(base, SenderConfig{}); err == nil || calls.Load() != 12 {
		t.Fatal("invalid sender created a client or made a remote call")
	}
}

func TestDefaultClientAndErrorRedaction(t *testing.T) {
	client, err := NewClient(nil, SenderConfig{Token: current})
	if err != nil || client == http.DefaultClient || client.Transport == nil {
		t.Fatal("default client was not safely copied")
	}
	for _, value := range []any{client.Transport, &SenderConfig{Token: current}, &ReceiverConfig{Token: current, PreviousToken: previous}} {
		if formatted := fmt.Sprintf("%v %+v %#v", value, value, value); strings.Contains(formatted, current) || strings.Contains(formatted, previous) {
			t.Fatal("credential leaked through nested formatting")
		}
	}
	bad := "confidential invalid token"
	if err := (SenderConfig{Token: bad}).Validate(); err == nil || strings.Contains(err.Error(), bad) {
		t.Fatal("validation accepted invalid value or leaked it")
	}
}

func TestClientNeverFollowsRedirectsIncludingInjectedClient(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		leaked.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(Header) != current {
					t.Error("source credential missing")
				}
				http.Redirect(w, r, target.URL, status)
			}))
			defer source.Close()
			var originalCalled atomic.Bool
			base := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { originalCalled.Store(true); return nil }}
			client, err := NewClient(base, SenderConfig{Token: current})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(r)
			if response != nil {
				if closeErr := response.Body.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if err == nil || originalCalled.Load() || leaked.Load() != 0 {
				t.Fatal("redirect policy bypassed or target received credential")
			}
		})
	}
}

func TestReceiverEmptyHeaderWithoutPrevious(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), "GET", "/internal/ring", nil)
	r.Header.Set(Header, "")
	if (ReceiverConfig{Token: current}).Authorize(r) {
		t.Fatal("empty header matched absent previous credential")
	}
}
