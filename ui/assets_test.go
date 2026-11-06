package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestBundledDispatchAssetsMatchTheirSources(t *testing.T) {
	data, err := assets.ReadFile("static/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Assets  []string          `json:"assets"`
		Sources map[string]string `json:"sources"`
		Outputs map[string]string `json:"outputs"`
	}
	if err = json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Sources) < 6 || len(m.Assets) < 3 {
		t.Fatal("incomplete dispatch build manifest")
	}
	for path, want := range m.Sources {
		if strings.HasPrefix(path, "/") || strings.Contains(path, "..") {
			t.Fatal("invalid source path")
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(body)
		if hex.EncodeToString(digest[:]) != want {
			t.Fatalf("%s changed: rebuild with npm ci && npm run build in ui", path)
		}
	}
	files, err := assets.ReadDir("static")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Outputs) != len(files)-1 {
		t.Fatal("incomplete runtime asset manifest")
	}
	for _, entry := range files {
		if entry.Name() == "manifest.json" {
			continue
		}
		body, err := assets.ReadFile("static/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(body)
		if hex.EncodeToString(digest[:]) != m.Outputs[entry.Name()] {
			t.Fatalf("bundled %s differs from its build manifest", entry.Name())
		}
	}
	for _, path := range append(m.Assets, "/sw.js") {
		w := httptest.NewRecorder()
		handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if w.Code != 200 || w.Header().Get("Content-Type") == "" || len(w.Body.Bytes()) == 0 {
			t.Fatalf("asset %s: %d", path, w.Code)
		}
	}
	for _, path := range []string{"/assets/manifest.json", "/assets/", "/internal/ring", "/admin/nodes"} {
		w := httptest.NewRecorder()
		handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if w.Code != 404 {
			t.Fatalf("%s exposed: %d", path, w.Code)
		}
	}
}
