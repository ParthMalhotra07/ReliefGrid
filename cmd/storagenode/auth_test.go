package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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
		cfg := config{InternalAuthEnabled: true, RingPath: "../../contracts/ring.yaml", NodeAuthFiles: files, NodeSenderTokenFile: "/missing-s29-sender-token"}
		err := run(t.Context(), cfg)
		if err == nil || !strings.Contains(err.Error(), "internal auth:") {
			t.Fatalf("credential %q: %v", token, err)
		}
	}
}

func TestEachCredentialFailsBeforeAnyListener(t *testing.T) {
	for _, kind := range []string{"node", "previous", "sender"} {
		t.Run(kind, func(t *testing.T) {
			node := mountedAuth(t)
			sender := mountedAuth(t)
			raftAddr, dataAddr, mgmtAddr, listen := reserveAddrs(t)
			cfg := testConfig(t, raftAddr, dataAddr, mgmtAddr, listen)
			cfg.InternalAuthEnabled = true
			cfg.NodeAuthFiles = node
			cfg.NodeSenderTokenFile = sender.Token
			cfg.AdmissionOff = true

			switch kind {
			case "node":
				cfg.NodeAuthFiles.Token = "/missing-node-token"
			case "previous":
				cfg.NodeAuthFiles.PreviousToken = t.TempDir() // EISDIR, even as root
			case "sender":
				cfg.NodeSenderTokenFile = "/missing-sender-token"

			}
			var listens atomic.Int32
			cfg.Listen = func(context.Context, string, string) (net.Listener, error) {
				listens.Add(1)
				return nil, errors.New("unexpected listen")
			}
			err := run(t.Context(), cfg)
			if err == nil || !strings.Contains(err.Error(), "internal auth:") {
				t.Fatalf("%s load bypassed: %v", kind, err)
			}
			expectedPath := cfg.NodeAuthFiles.Token
			switch kind {
			case "previous":
				expectedPath = cfg.NodeAuthFiles.PreviousToken
			case "sender":
				expectedPath = cfg.NodeSenderTokenFile

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
	env := map[string]string{"DATA_DIR": "/data", "FSM_DIR": "/fsm", "POD_NAME": "storage-0-0", "SHARD": "0", "RAFT_ADVERTISE": "storage-0-0.storage-0.ns.svc.cluster.local:7000", "ADMISSION": "off"}

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

func TestServingAuthModes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			raftAddr, dataAddr, mgmtAddr, listen := reserveAddrs(t)
			cfg := testConfig(t, raftAddr, dataAddr, mgmtAddr, listen)
			cfg.AdmissionOff = true
			cfg.InternalAuthEnabled = enabled
			if !enabled {
				cfg.NodeAuthFiles.Token = "/missing-disabled-node"
				cfg.NodeSenderTokenFile = "/missing-disabled-sender"
			}
			startRun(t, cfg)
			client := &http.Client{Timeout: time.Second}
			deadline := time.Now().Add(5 * time.Second)
			for {
				req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+dataAddr+"/internal/not-a-route", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					if time.Now().After(deadline) {
						t.Fatal(err)
					}
					time.Sleep(10 * time.Millisecond)
					continue
				}
				_ = resp.Body.Close()
				want := 404
				if enabled {
					want = 401
				}
				if resp.StatusCode != want {
					t.Fatalf("enabled=%v anonymous=%d, want %d", enabled, resp.StatusCode, want)
				}
				if enabled {
					req.Header.Set("X-ReliefGrid-Internal-Token", "test-token")
					resp, err = client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_ = resp.Body.Close()
					if resp.StatusCode != 404 {
						t.Fatalf("valid credential status %d", resp.StatusCode)
					}
				}
				break
			}
		})
	}
}
