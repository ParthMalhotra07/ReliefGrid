package scripts_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestScheduleForwardReconnectsAndJoinsChild(t *testing.T) {
	root := t.TempDir()
	stub := `#!/usr/bin/env bash
if [ ! -e "$FORWARD_STARTED" ];then touch "$FORWARD_STARTED";exit 1;fi
echo $$ > "$FORWARD_CHILD"
exec sleep 30
`
	if err := os.WriteFile(filepath.Join(root, "kubectl"), []byte(stub), 0755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", "schedules-forward.sh", "12345")
	childPath := filepath.Join(root, "child")
	command.Env = append(os.Environ(), "PATH="+root+":"+os.Getenv("PATH"), "FORWARD_STARTED="+filepath.Join(root, "started"), "FORWARD_CHILD="+childPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill() }()
	var child int
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for child == 0 {
		if raw, err := os.ReadFile(childPath); err == nil {
			child, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		if child != 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("forward did not reconnect after first child exited")
		case <-ticker.C:
		}
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	if ctx.Err() != nil {
		t.Fatal("forward supervisor did not join")
	}
	if err := syscall.Kill(child, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("forward child %d remains after shutdown: %v", child, err)
	}
}
