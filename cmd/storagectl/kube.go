package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// errConflict: the ConfigMap changed since it was read (HTTP 409); re-read
// and decide again.
var errConflict = errors.New("storagectl: manifest changed concurrently")

// errExists: the manifest ConfigMap is already there (HTTP 409
// AlreadyExists on create); read it and use it.
var errExists = errors.New("storagectl: manifest already exists")

// manifestStore creates, reads and compare-and-swaps manifest ConfigMaps.
type manifestStore interface {
	Get(ctx context.Context, name string) (manifest, error)
	// Create writes m.data() as a new ConfigMap; errExists if one exists,
	// which it never changes.
	Create(ctx context.Context, m manifest) error
	// Update writes m.data() if the ConfigMap is still at m.ResourceVersion
	// and returns the new resourceVersion; errConflict otherwise.
	Update(ctx context.Context, m manifest) (string, error)
}

// kubeStore talks to the API server's REST interface directly with the
// pod's service account: two verbs on one resource do not need client-go.
type kubeStore struct {
	base      string // https://host:port/api/v1/namespaces/<ns>/configmaps/
	tokenFile string
	client    *http.Client
}

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// inClusterStore uses KUBERNETES_SERVICE_HOST/PORT and the mounted service
// account token and CA.
func inClusterStore(namespace string) (*kubeStore, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("storagectl: not in a cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("storagectl: read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("storagectl: service account CA has no certificates")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &kubeStore{
		base:      "https://" + net.JoinHostPort(host, port) + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/configmaps/",
		tokenFile: serviceAccountDir + "/token",
		client:    &http.Client{Transport: tr, Timeout: 10 * time.Second},
	}, nil
}

// configMap is the subset of a v1.ConfigMap the Job reads; Update sends the
// whole object back so labels and annotations are preserved.
type configMap struct {
	raw     map[string]any
	version string
	data    map[string]string
}

func (k *kubeStore) do(ctx context.Context, method, name string, body []byte) (*http.Response, error) {
	// The token is re-read on every call: kubelet rotates projected tokens.
	tok, err := os.ReadFile(k.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("storagectl: read service account token: %w", err)
	}
	target := k.base + url.PathEscape(name)
	if name == "" { // the collection, for create
		target = strings.TrimSuffix(k.base, "/")
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(bytes.TrimSpace(tok)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return k.client.Do(req)
}

func (k *kubeStore) getRaw(ctx context.Context, name string) (configMap, error) {
	resp, err := k.do(ctx, http.MethodGet, name, nil)
	if err != nil {
		return configMap{}, fmt.Errorf("storagectl: get configmap %s: %w", name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return configMap{}, fmt.Errorf("storagectl: get configmap %s: %w", name, err)
	}
	if resp.StatusCode != http.StatusOK {
		return configMap{}, fmt.Errorf("storagectl: get configmap %s: HTTP %d: %s", name, resp.StatusCode, bytes.TrimSpace(b))
	}
	var obj struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Data map[string]string `json:"data"`
	}
	var cm configMap
	if err := json.Unmarshal(b, &obj); err != nil {
		return configMap{}, fmt.Errorf("storagectl: decode configmap %s: %w", name, err)
	}
	if err := json.Unmarshal(b, &cm.raw); err != nil {
		return configMap{}, fmt.Errorf("storagectl: decode configmap %s: %w", name, err)
	}
	cm.version, cm.data = obj.Metadata.ResourceVersion, obj.Data
	return cm, nil
}

func (k *kubeStore) Get(ctx context.Context, name string) (manifest, error) {
	cm, err := k.getRaw(ctx, name)
	if err != nil {
		return manifest{}, err
	}
	return parseManifest(name, cm.version, cm.data)
}

// manifestLabels select manifests for make storage-reset. There are no Helm
// ownership labels or annotations: the manifest is storagectl's, so helm
// upgrade, rollback and uninstall never write or delete it.
func manifestLabels(m manifest) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "storage",
		"app.kubernetes.io/component":  "storage-manifest",
		"app.kubernetes.io/managed-by": "storagectl",
		"reliefgrid.io/shard":          strconv.Itoa(m.Shard),
	}
}

func (k *kubeStore) Create(ctx context.Context, m manifest) error {
	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": m.Name, "labels": manifestLabels(m)},
		"data":       m.data(),
	})
	if err != nil {
		return err
	}
	resp, err := k.do(ctx, http.MethodPost, "", body)
	if err != nil {
		return fmt.Errorf("storagectl: create configmap %s: %w", m.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", errExists, m.Name)
	default:
		return fmt.Errorf("storagectl: create configmap %s: HTTP %d: %s", m.Name, resp.StatusCode, bytes.TrimSpace(b))
	}
}

func (k *kubeStore) Update(ctx context.Context, m manifest) (string, error) {
	cm, err := k.getRaw(ctx, m.Name)
	if err != nil {
		return "", err
	}
	// Send the object back at the version m was read at; the API server
	// rejects it with 409 if anything wrote in between.
	meta, _ := cm.raw["metadata"].(map[string]any)
	if meta == nil {
		return "", fmt.Errorf("storagectl: configmap %s has no metadata", m.Name)
	}
	meta["resourceVersion"] = m.ResourceVersion
	// Merge: keys the struct doesn't model (M7's admissions) are kept.
	data, _ := cm.raw["data"].(map[string]any)
	if data == nil {
		data = map[string]any{}
	}
	for key, v := range m.data() {
		data[key] = v
	}
	if len(m.PeerIDs) == 0 {
		delete(data, "peerIds")
	}
	cm.raw["data"] = data
	body, err := json.Marshal(cm.raw)
	if err != nil {
		return "", err
	}
	resp, err := k.do(ctx, http.MethodPut, m.Name, body)
	if err != nil {
		return "", fmt.Errorf("storagectl: update configmap %s: %w", m.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusConflict:
		return "", fmt.Errorf("%w: %s", errConflict, m.Name)
	default:
		return "", fmt.Errorf("storagectl: update configmap %s: HTTP %d: %s", m.Name, resp.StatusCode, bytes.TrimSpace(b))
	}
	var obj struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return "", fmt.Errorf("storagectl: decode updated configmap %s: %w", m.Name, err)
	}
	return obj.Metadata.ResourceVersion, nil
}
