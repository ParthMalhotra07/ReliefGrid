// Package auth supplies internal-call credentials. Callers must explicitly wire
// protected routes; importing this package does not enable authentication.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

// Header is shared by application and node calls, with different credentials.
// Operator Bearer credentials remain a separate interface.
const Header = "X-ReliefGrid-Internal-Token"

// SenderConfig holds the current credential only. Do not serialize individual
// fields or log raw HTTP headers; config formatting deliberately redacts them.
type SenderConfig struct {
	Token string `json:"-" yaml:"-"`
}

// ReceiverConfig permits a receiver-only overlap during credential rotation.
type ReceiverConfig struct {
	Token         string `json:"-" yaml:"-"`
	PreviousToken string `json:"-" yaml:"-"`
}

func validToken(token string) bool {
	if len(token) == 0 || len(token) > 4096 {
		return false
	}
	for i := range len(token) {
		// A comma could represent multiple coalesced header values. Whitespace,
		// non-ASCII and controls are forbidden rather than silently trimmed.
		if token[i] < '!' || token[i] > '~' || token[i] == ',' {
			return false
		}
	}
	return true
}

// Validate checks wire syntax, not credential entropy. Provision random secrets.
func (c SenderConfig) Validate() error {
	if !validToken(c.Token) {
		return errors.New("internal auth: invalid current credential")
	}
	return nil
}

func (c ReceiverConfig) Validate() error {
	if err := (SenderConfig{Token: c.Token}).Validate(); err != nil {
		return err
	}
	if c.PreviousToken != "" && !validToken(c.PreviousToken) {
		return errors.New("internal auth: invalid previous credential")
	}
	return nil
}

func (SenderConfig) String() string         { return "SenderConfig{[REDACTED]}" }
func (c SenderConfig) GoString() string     { return c.String() }
func (SenderConfig) LogValue() slog.Value   { return slog.StringValue("[REDACTED]") }
func (ReceiverConfig) String() string       { return "ReceiverConfig{[REDACTED]}" }
func (c ReceiverConfig) GoString() string   { return c.String() }
func (ReceiverConfig) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

// Authorize fails closed even if a constructor bypasses startup validation.
// Both accepted credentials use fixed-size constant-time digest comparisons.
// This does not imply constant-time HTTP parsing or configuration validation.
func (c ReceiverConfig) Authorize(r *http.Request) bool {
	if c.Validate() != nil || r == nil {
		return false
	}
	var candidate string
	count := 0
	for key, values := range r.Header {
		if strings.EqualFold(key, Header) {
			count += len(values)
			if len(values) == 1 {
				candidate = values[0]
			}
		}
	}
	if count != 1 || !validToken(candidate) {
		return false
	}
	got := sha256.Sum256([]byte(candidate))
	current := sha256.Sum256([]byte(c.Token))
	previous := sha256.Sum256([]byte(c.PreviousToken))
	a := subtle.ConstantTimeCompare(got[:], current[:])
	b := subtle.ConstantTimeCompare(got[:], previous[:])
	return a|b == 1
}

type senderTransport struct {
	token string
	base  http.RoundTripper
}

func (senderTransport) String() string     { return "internal auth transport [REDACTED]" }
func (t senderTransport) GoString() string { return t.String() }

func (t senderTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if _, bounded := r.Context().Deadline(); !bounded {
		return nil, errors.New("internal auth: request requires a deadline or client timeout")
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	forward := r.Clone(r.Context())
	if forward.Header == nil {
		forward.Header = make(http.Header)
	}
	for key := range forward.Header {
		if strings.EqualFold(key, Header) || strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "Proxy-Authorization") {
			delete(forward.Header, key)
		}
	}
	forward.Header.Set(Header, t.token)
	return t.base.RoundTrip(forward)
}

func (t senderTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// NewClient validates before any I/O, copies the supplied HTTP client and
// replaces credentials on a cloned request. The caller owns the destination and
// must supply a request context with a deadline. Injected transports must be
// trusted: they necessarily see the credential. Redirects are always errors.
func NewClient(base *http.Client, config SenderConfig) (*http.Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if _, wrapped := transport.(senderTransport); wrapped {
		return nil, errors.New("internal auth: client already has a credential transport")
	}
	client.Transport = senderTransport{token: config.Token, base: transport}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("internal auth: redirects are forbidden")
	}
	return &client, nil
}
