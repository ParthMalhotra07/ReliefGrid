// Package internalauth wires the shared credentials to the internal contract.
package internalauth

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

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
// are separate handlers and retain their own access policy.
func Receiver(cfg auth.ReceiverConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !cfg.Authorize(r) {
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
