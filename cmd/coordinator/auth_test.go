package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalauth"
)

func mountedAuth(t *testing.T) internalauth.Files {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	return internalauth.Files{Token: path}
}

func TestRunRefusesMissingAuthBeforeListening(t *testing.T) {
	for _, token := range []string{"", "invalid token", "valid-token"} {
		files := mountedAuth(t)
		if err := os.WriteFile(files.Token, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		cfg := config{InternalAuthEnabled: true, RingPath: "../../contracts/ring.yaml", NodeAuthFiles: files, NodeSenderTokenFile: "/missing-s29-sender-token", AppAuthFiles: files}
		err := run(t.Context(), cfg, nil)
		if err == nil || !strings.Contains(err.Error(), "internal auth:") {
			t.Fatalf("credential %q: %v", token, err)
		}
	}
}

func TestEachCredentialFailsBeforeAnyListener(t *testing.T) {
	for _, kind := range []string{"node", "previous", "sender", "app"} {
		t.Run(kind, func(t *testing.T) {
			node := mountedAuth(t)
			sender := mountedAuth(t)
			cfg := config{InternalAuthEnabled: true, RingPath: "../../contracts/ring.yaml", NodeAuthFiles: node, NodeSenderTokenFile: sender.Token}
			cfg.AppAuthFiles = mountedAuth(t)

			switch kind {
			case "node":
				cfg.NodeAuthFiles.Token = "/missing-node-token"
			case "previous":
				cfg.NodeAuthFiles.PreviousToken = t.TempDir() // EISDIR, even as root
			case "sender":
				cfg.NodeSenderTokenFile = "/missing-sender-token"
			case "app":
				cfg.AppAuthFiles.Token = "/missing-app-token"

			}
			var listens atomic.Int32
			cfg.Listen = func(context.Context, string, string) (net.Listener, error) {
				listens.Add(1)
				return nil, errors.New("unexpected listen")
			}
			err := run(t.Context(), cfg, nil)
			if err == nil || !strings.Contains(err.Error(), "internal auth:") {
				t.Fatalf("%s load bypassed: %v", kind, err)
			}
			expectedPath := cfg.NodeAuthFiles.Token
			switch kind {
			case "previous":
				expectedPath = cfg.NodeAuthFiles.PreviousToken
			case "sender":
				expectedPath = cfg.NodeSenderTokenFile
			case "app":
				expectedPath = cfg.AppAuthFiles.Token

			}
			if !strings.Contains(err.Error(), expectedPath) {
				t.Fatalf("did not report the failed credential read %s: %v", expectedPath, err)
			}
			if listens.Load() != 0 {
				t.Fatal("opened a listener before validating credentials")
			}
		})
	}
}

func TestInternalAuthEnabledConfig(t *testing.T) {
	env := map[string]string{}

	get := func(k string) string { return env[k] }
	cfg, err := loadConfig(get)
	if err != nil || cfg.InternalAuthEnabled {
		t.Fatalf("default: %+v %v", cfg, err)
	}
	env["INTERNAL_AUTH_ENABLED"] = "true"
	cfg, err = loadConfig(get)
	if err != nil || !cfg.InternalAuthEnabled {
		t.Fatalf("enabled: %+v %v", cfg, err)
	}
	env["INTERNAL_AUTH_ENABLED"] = "typo"
	if _, err := loadConfig(get); err == nil {
		t.Fatal("malformed enable flag accepted")
	}
}

func TestDisabledAuthServesAnonymousRing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config{HTTPAddr: "127.0.0.1:0", RingPath: "../../contracts/ring.yaml", AppAuthFiles: internalauth.Files{Token: "/missing"}, NodeAuthFiles: internalauth.Files{Token: "/missing"}}, ready)
	}()
	select {
	case addr := <-ready:
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/internal/ring", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("anonymous ring status %d", resp.StatusCode)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatalf("disabled auth loaded credentials: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("did not start")
	}
}
