package operator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testKubernetes(t *testing.T, handler http.Handler) (*Kubernetes, string, func()) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("first-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &Kubernetes{BaseURL: server.URL, Namespace: "test", TokenFile: tokenFile, Client: server.Client()}, tokenFile, server.Close
}

func TestKubernetesReturnsStatusErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client, _, closeServer := testKubernetes(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer closeServer()
			err := client.Get(context.Background(), "/api/v1/namespaces/test/pods", nil)
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
				t.Fatalf("error=%v, want HTTP %d APIError", err, status)
			}
		})
	}
}

func TestKubernetesRereadsRotatingToken(t *testing.T) {
	var mu sync.Mutex
	var tokens []string
	client, tokenFile, closeServer := testKubernetes(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		tokens = append(tokens, request.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer closeServer()
	if err := client.Get(context.Background(), "/api/v1/namespaces/test/pods", &map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("second-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), "/api/v1/namespaces/test/pods", &map[string]any{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) != 2 || tokens[0] != "Bearer first-token" || tokens[1] != "Bearer second-token" {
		t.Fatalf("authorization headers=%v", tokens)
	}
}

func TestKubernetesObjectRoundTripPreservesResourceVersion(t *testing.T) {
	client, _, closeServer := testKubernetes(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"metadata":{"name":"recovery","resourceVersion":"17"}}`)
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if string(data) != `{"metadata":{"name":"recovery","resourceVersion":"17"},"status":{"phase":"Blocked"}}` {
				t.Fatalf("PUT payload=%s", data)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer closeServer()
	var resource map[string]any
	if err := client.Get(context.Background(), "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery", &resource); err != nil {
		t.Fatal(err)
	}
	resource["status"] = map[string]any{"phase": "Blocked"}
	var updated map[string]any
	if err := client.Put(context.Background(), "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery/status", resource, &updated); err != nil {
		t.Fatal(err)
	}
	metadata, ok := updated["metadata"].(map[string]any)
	if !ok || metadata["resourceVersion"] != "17" {
		t.Fatalf("resourceVersion was not preserved: %#v", updated)
	}
}

func TestKubernetesHonorsContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client, _, closeServer := testKubernetes(t, http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(started)
		<-request.Context().Done()
		// Do not race cancellation with an implicit successful HTTP response.
		<-release
	}))
	defer closeServer()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Get(ctx, "/api/v1/namespaces/test/pods", nil) }()
	select {
	case <-started:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
}

func TestKubernetesRejectsNonLocalPaths(t *testing.T) {
	client := &Kubernetes{BaseURL: "https://kubernetes.example"}
	for _, apiPath := range []string{"https://other.example/api", "//other.example/api", "/api/../secrets", "/api/%2e%2e/secrets"} {
		if _, err := client.targetURL(apiPath); err == nil {
			t.Fatalf("accepted unsafe API path %q", apiPath)
		}
	}
}
