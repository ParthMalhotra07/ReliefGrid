package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		cfg := config{RingPath: "../../contracts/ring.yaml", NodeAuthFiles: files, NodeSenderTokenFile: "/missing-s29-sender-token", AppAuthFiles: files}
		err := run(t.Context(), cfg, nil)
		if err == nil || !strings.Contains(err.Error(), "internal auth:") {
			t.Fatalf("credential %q: %v", token, err)
		}
	}
}
