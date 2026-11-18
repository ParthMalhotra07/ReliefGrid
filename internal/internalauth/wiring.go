// Package internalauth wires the shared credentials to the internal contract.
package internalauth

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// Files names mounted Secret keys. PreviousToken may be absent during steady
// state; a missing, empty or malformed current token always prevents startup.
// Bytes are not trimmed: accidentally mounted newlines are invalid credentials.
type Files struct{ Token, PreviousToken string }

func (f Files) Load() (auth.ReceiverConfig, error) {
	current, err := os.ReadFile(f.Token)
	if err != nil {
		return auth.ReceiverConfig{}, fmt.Errorf("internal auth: read current token: %w", err)
	}
	cfg := auth.ReceiverConfig{Token: string(current)}
	if f.PreviousToken != "" {
		previous, err := os.ReadFile(f.PreviousToken)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return auth.ReceiverConfig{}, fmt.Errorf("internal auth: read previous token: %w", err)
		}
		cfg.PreviousToken = string(previous)
	}
	if err := cfg.Validate(); err != nil {
		return auth.ReceiverConfig{}, err
	}
	return cfg, nil
}

// Receiver refuses before calling next. The response is the shared contract's
// UNAUTHORIZED shape: this attempt never entered a handler or proposed a write.
// Install only on internal routes; probes, metrics and operator bearer routes
// are separate handlers and retain their own access policy. Each refusal is
// logged (see ReceiverFor) under the credential class "internal".
func Receiver(cfg auth.ReceiverConfig, next http.Handler) http.Handler {
	return ReceiverFor("internal", cfg, next)
}

// ReceiverFor is Receiver for a named credential class ("app" or "node"). Every
// refused request is logged once at WARN as "internal auth refused" with the
// class, method, route (path only), peer and reason, so a rotation phase can see
// a sender still using a credential the receiver no longer accepts. The presented
// value and the configured tokens are never logged: the reason comes from the
// header's presence, count and syntax alone.
func ReceiverFor(class string, cfg auth.ReceiverConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !cfg.Authorize(r) {
			slog.Warn("internal auth refused", "credential", class, "method", r.Method,
				"route", r.URL.Path, "peer", r.RemoteAddr, "reason", refusalReason(cfg, r))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(internalapi.Error{Code: internalapi.UNAUTHORIZED, Message: "internal authentication required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// LoadSender reads a separately staged sender key, allowing receiver-first
// rotation even when one process both sends and receives internal calls.
func LoadSender(path string) (auth.SenderConfig, error) {
	token, err := os.ReadFile(path)
	if err != nil {
		return auth.SenderConfig{}, fmt.Errorf("internal auth: read sender token: %w", err)
	}
	cfg := auth.SenderConfig{Token: string(token)}
	if err := cfg.Validate(); err != nil {
		return auth.SenderConfig{}, err
	}
	return cfg, nil
}

// refusalReason says why Authorize refused r, from the receiver configuration and
// the header's presence, count and syntax only; it never reads the value into a
// message. Syntax follows the receiver's rule: 1-4096 printable ASCII bytes, no
// comma.
func refusalReason(cfg auth.ReceiverConfig, r *http.Request) string {
	if cfg.Validate() != nil {
		return "receiver misconfigured"
	}
	var values []string
	for key, v := range r.Header {
		if strings.EqualFold(key, auth.Header) {
			values = append(values, v...)
		}
	}
	switch {
	case len(values) == 0:
		return "missing"
	case len(values) > 1:
		return "repeated header"
	}
	v := values[0]
	if len(v) == 0 || len(v) > 4096 {
		return "malformed"
	}
	for i := range len(v) {
		if v[i] < '!' || v[i] > '~' || v[i] == ',' {
			return "malformed"
		}
	}
	return "unknown credential"
}
