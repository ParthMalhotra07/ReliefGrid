package admission

import (
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
	"strings"
	"time"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// configMapSource reads one ConfigMap with the pod's service account. The
// storage pods' Role allows get on the shards' manifests by name, nothing
// else.
type configMapSource struct {
	url       string
	tokenFile string
	client    *http.Client
}

// InCluster reads ConfigMap name in namespace through the API server
// (KUBERNETES_SERVICE_HOST/PORT, the mounted service account token and CA).
func InCluster(namespace, name string) (Source, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("admission: not in a cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("admission: read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("admission: service account CA has no certificates")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return newConfigMapSource("https://"+net.JoinHostPort(host, port)+"/api/v1/namespaces/"+url.PathEscape(namespace)+"/configmaps/"+url.PathEscape(name),
		serviceAccountDir+"/token", &http.Client{Transport: tr, Timeout: 5 * time.Second}), nil
}

func newConfigMapSource(u, tokenFile string, client *http.Client) *configMapSource {
	return &configMapSource{url: u, tokenFile: tokenFile, client: client}
}

func (s *configMapSource) Get(ctx context.Context) (map[string]string, error) {
	token, err := os.ReadFile(s.tokenFile) // re-read: projected tokens rotate
	if err != nil {
		return nil, fmt.Errorf("admission: read service account token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, fmt.Errorf("admission: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("admission: get manifest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("admission: read manifest: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusForbidden:
		return nil, fmt.Errorf("%w: %.200s", ErrForbidden, body)
	case http.StatusNotFound:
		return nil, fmt.Errorf("%w: %.200s", ErrNotFound, body)
	default:
		return nil, fmt.Errorf("admission: get manifest: HTTP %d: %.200s", resp.StatusCode, body)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		return nil, fmt.Errorf("admission: decode manifest: %w", err)
	}
	return cm.Data, nil
}
