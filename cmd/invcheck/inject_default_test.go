//go:build !chaos

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// Only the chaos build (the tools image) has inject: an ordinary invcheck
// can never overwrite data.
func TestInjectAbsentWithoutChaosTag(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"inject", "--confirm", "--ledger", "x.json", "--coordinator", "http://127.0.0.1:1"}, &bytes.Buffer{}, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), `unknown command "inject"`) {
		t.Fatalf("inject in the default build: exit %d, %q", code, stderr.String())
	}
}
