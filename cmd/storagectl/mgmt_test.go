package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// storagectl snapshot and storagectl dump call the local management
// endpoint with the pod's OPERATOR_TOKEN and print its JSON answer.
func TestSnapshotAndDumpCommands(t *testing.T) {
	for _, tc := range []struct {
		name         string
		run          func(*testing.T, []string, func(string) string, *bytes.Buffer) error
		method, path string
	}{
		{"snapshot", func(t *testing.T, args []string, env func(string) string, out *bytes.Buffer) error {
			return runSnapshot(t.Context(), args, env, out)
		}, http.MethodPost, "/mgmt/snapshot"},
		{"dump", func(t *testing.T, args []string, env func(string) string, out *bytes.Buffer) error {
			return runDump(t.Context(), args, env, out)
		}, http.MethodGet, "/mgmt/dump"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *http.Request
			code, body := http.StatusOK, `{"podName":"storage-shard-0-0","appliedIndex":1100}`
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r
				w.WriteHeader(code)
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			addr := strings.TrimPrefix(srv.URL, "http://")
			env := func(k string) string {
				if k == "OPERATOR_TOKEN" {
					return "tok"
				}
				return ""
			}

			var out bytes.Buffer
			if err := tc.run(t, []string{"--addr", addr}, env, &out); err != nil {
				t.Fatal(err)
			}
			if got.Method != tc.method || got.URL.Path != tc.path || got.Header.Get("Authorization") != "Bearer tok" {
				t.Fatalf("sent %s %s with %q", got.Method, got.URL.Path, got.Header.Get("Authorization"))
			}
			if strings.TrimSpace(out.String()) != body {
				t.Fatalf("printed %q, want the body", out.String())
			}

			code, body = http.StatusUnauthorized, `{"code":"UNAUTHORIZED"}`
			out.Reset()
			if err := tc.run(t, []string{"--addr", addr}, env, &out); err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("HTTP 401: %v", err)
			}
			if err := tc.run(t, []string{"--addr", addr}, func(string) string { return "" }, &out); err == nil || !strings.Contains(err.Error(), "OPERATOR_TOKEN") {
				t.Fatalf("no token: %v", err)
			}
		})
	}
}
