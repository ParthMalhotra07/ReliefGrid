package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// fakeAPIServer serves one namespace's ConfigMaps with resourceVersion
// conflict detection, as the real API server does on PUT.
type fakeAPIServer struct {
	mu  sync.Mutex
	cms map[string]map[string]any
}

func (f *fakeAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer sa-token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodPost { // create, on the collection
		var in map[string]any
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &in); err != nil || filepath.Base(r.URL.Path) != "configmaps" {
			http.Error(w, "bad create", http.StatusBadRequest)
			return
		}
		name := in["metadata"].(map[string]any)["name"].(string)
		if _, ok := f.cms[name]; ok {
			http.Error(w, `{"reason":"AlreadyExists"}`, http.StatusConflict)
			return
		}
		in["metadata"].(map[string]any)["resourceVersion"] = "1"
		f.cms[name] = in
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(in)
		return
	}
	name := filepath.Base(r.URL.Path)
	cm, ok := f.cms[name]
	if !ok {
		http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
		return
	}
	meta := cm["metadata"].(map[string]any)
	switch r.Method {
	case http.MethodGet:
	case http.MethodPut:
		var in map[string]any
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if in["metadata"].(map[string]any)["resourceVersion"] != meta["resourceVersion"] {
			http.Error(w, `{"reason":"Conflict"}`, http.StatusConflict)
			return
		}
		rv, _ := strconv.Atoi(meta["resourceVersion"].(string))
		in["metadata"].(map[string]any)["resourceVersion"] = strconv.Itoa(rv + 1)
		f.cms[name], cm = in, in
	}
	_ = json.NewEncoder(w).Encode(cm)
}

func newTestKubeStore(t *testing.T, api *fakeAPIServer) *kubeStore {
	t.Helper()
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &kubeStore{base: srv.URL + "/api/v1/namespaces/reliefgrid/configmaps/", tokenFile: tokenFile, client: srv.Client()}
}

func TestKubeStoreCompareAndSwap(t *testing.T) {
	m := shardManifest(1)
	data := map[string]any{"admitted": `{"storage-shard-1-2":"inc-9"}`} // a key the struct doesn't model (M7)
	for k, v := range m.data() {
		data[k] = v
	}
	api := &fakeAPIServer{cms: map[string]map[string]any{m.Name: {
		"apiVersion": "v1", "kind": "ConfigMap", "data": data,
		"metadata": map[string]any{"name": m.Name, "resourceVersion": "10",
			"annotations": map[string]any{"reliefgrid.io/note": "kept"}},
	}}}
	k := newTestKubeStore(t, api)

	got, err := k.Get(t.Context(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.ResourceVersion != "10" || got.Phase != phasePending || got.GroupID != "shard-1" {
		t.Fatalf("get: %+v", got)
	}
	stale := got
	got.Phase, got.PeerIDs = phaseBootstrapping, []string{"a/1", "b/2", "c/3"}
	rv, err := k.Update(t.Context(), got)
	if err != nil || rv != "11" {
		t.Fatalf("update: %q %v", rv, err)
	}
	stale.Phase = phaseBootstrapped
	if _, err := k.Update(t.Context(), stale); !errors.Is(err, errConflict) {
		t.Fatalf("stale update: %v, want errConflict", err)
	}
	after, err := k.Get(t.Context(), m.Name)
	if err != nil || after.Phase != phaseBootstrapping || len(after.PeerIDs) != 3 {
		t.Fatalf("after: %+v %v", after, err)
	}
	if ann := api.cms[m.Name]["metadata"].(map[string]any)["annotations"]; ann == nil {
		t.Fatal("update dropped the annotations")
	}
	if got := api.cms[m.Name]["data"].(map[string]any)["admitted"]; got != `{"storage-shard-1-2":"inc-9"}` {
		t.Fatalf("update dropped a data key it doesn't model: admitted = %v", got)
	}
}

// storagectl creates the manifest itself, without Helm's ownership metadata,
// and never overwrites one that exists.
func TestKubeStoreCreateIfAbsent(t *testing.T) {
	api := &fakeAPIServer{cms: map[string]map[string]any{}}
	k := newTestKubeStore(t, api)
	m := shardManifest(2)
	if err := k.Create(t.Context(), m); err != nil {
		t.Fatalf("create: %v", err)
	}
	meta := api.cms[m.Name]["metadata"].(map[string]any)
	labels, _ := meta["labels"].(map[string]any)
	if labels["app.kubernetes.io/component"] != "storage-manifest" || labels["app.kubernetes.io/name"] != "storage" ||
		labels["reliefgrid.io/shard"] != "2" || labels["app.kubernetes.io/managed-by"] != "storagectl" {
		t.Fatalf("labels %v", labels)
	}
	if ann, _ := meta["annotations"].(map[string]any); ann["meta.helm.sh/release-name"] != nil {
		t.Fatal("manifest carries Helm ownership")
	}
	got, err := k.Get(t.Context(), m.Name)
	if err != nil || got.Phase != phasePending || got.GroupID != "shard-2" || len(got.Peers) != 3 {
		t.Fatalf("get after create: %+v %v", got, err)
	}

	got.Phase, got.PeerIDs = phaseBootstrapping, []string{"a/1", "b/2", "c/3"}
	if _, err := k.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	if err := k.Create(t.Context(), shardManifest(2)); !errors.Is(err, errExists) {
		t.Fatalf("second create: %v, want errExists", err)
	}
	if after, _ := k.Get(t.Context(), m.Name); after.Phase != phaseBootstrapping {
		t.Fatalf("create overwrote the manifest: %+v", after)
	}
}
