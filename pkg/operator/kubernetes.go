// Package operator contains the narrow Kubernetes control-plane client used by
// the Rhiza recovery operator.
package operator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

const (
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	maxAPIResponse    = 16 << 20
)

// Kubernetes is the minimal in-cluster Kubernetes JSON REST client used by
// the operator. Callers construct rooted API paths and retain resourceVersion
// in values passed to Put.
type Kubernetes struct {
	BaseURL   string
	Namespace string
	TokenFile string
	Client    *http.Client
}

// APIError identifies a non-success Kubernetes response without copying the
// response body, which can contain sensitive resource data.
type APIError struct {
	StatusCode int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("kubernetes API returned HTTP %d", e.StatusCode)
}

// NewInCluster creates a TLS-verifying client from the mounted service account.
func NewInCluster(namespace string) (*Kubernetes, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST is required")
	}
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if namespace == "" {
		data, err := os.ReadFile(path.Join(serviceAccountDir, "namespace"))
		if err != nil {
			return nil, fmt.Errorf("read service account namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(data))
		if namespace == "" {
			return nil, fmt.Errorf("service account namespace is empty")
		}
	}
	caData, err := os.ReadFile(path.Join(serviceAccountDir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("service account CA contains no certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}
	return &Kubernetes{
		BaseURL:   "https://" + net.JoinHostPort(host, port),
		Namespace: namespace,
		TokenFile: path.Join(serviceAccountDir, "token"),
		Client:    &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}, nil
}

func (k *Kubernetes) Get(ctx context.Context, apiPath string, out any) error {
	return k.request(ctx, http.MethodGet, apiPath, nil, out)
}

func (k *Kubernetes) Put(ctx context.Context, apiPath string, value any, out any) error {
	return k.request(ctx, http.MethodPut, apiPath, value, out)
}

func (k *Kubernetes) Post(ctx context.Context, apiPath string, value any, out any) error {
	return k.request(ctx, http.MethodPost, apiPath, value, out)
}

func (k *Kubernetes) request(ctx context.Context, method, apiPath string, value any, out any) error {
	target, err := k.targetURL(apiPath)
	if err != nil {
		return err
	}
	token, err := os.ReadFile(k.TokenFile)
	if err != nil {
		return fmt.Errorf("read service account token: %w", err)
	}
	if token = bytes.TrimSpace(token); len(token) == 0 {
		return fmt.Errorf("service account token is empty")
	}
	var body io.Reader
	if value != nil {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode Kubernetes request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Accept", "application/json")
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := k.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return &APIError{StatusCode: response.StatusCode}
	}
	if out == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	data, err := readBoundedJSON(response.Body)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode Kubernetes response: %w", err)
	}
	return nil
}

func (k *Kubernetes) targetURL(apiPath string) (*url.URL, error) {
	base, err := url.Parse(k.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return nil, fmt.Errorf("invalid Kubernetes API base URL")
	}
	reference, err := url.ParseRequestURI(apiPath)
	if err != nil || !strings.HasPrefix(apiPath, "/") || strings.HasPrefix(apiPath, "//") || reference.IsAbs() || reference.Host != "" {
		return nil, fmt.Errorf("Kubernetes API path must be rooted and local")
	}
	decoded, err := url.PathUnescape(reference.EscapedPath())
	if err != nil || strings.Contains(decoded, "\\") {
		return nil, fmt.Errorf("invalid Kubernetes API path")
	}
	for _, segment := range strings.Split(decoded, "/") {
		if segment == ".." {
			return nil, fmt.Errorf("Kubernetes API path must not traverse")
		}
	}
	return base.ResolveReference(reference), nil
}

func readBoundedJSON(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxAPIResponse+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxAPIResponse {
		return nil, fmt.Errorf("Kubernetes response exceeds %d bytes", maxAPIResponse)
	}
	return data, nil
}
