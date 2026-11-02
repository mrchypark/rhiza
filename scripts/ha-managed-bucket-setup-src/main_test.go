package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEndpointBoundary(t *testing.T) {
	bucket := "rhiza-managed-123-1-012345abcdef"
	for _, endpoint := range []string{"http://127.0.0.1:1234", "http://[::1]:1234"} {
		if err := validate(endpoint, bucket); err != nil {
			t.Fatal(err)
		}
	}
	for _, endpoint := range []string{"https://127.0.0.1:1234", "http://localhost:1234", "http://example.com:1234", "http://127.0.0.1:1234/path", "http://user@127.0.0.1:1234", "http://127.0.0.1:1234?x=1", "http://127.0.0.1"} {
		if validate(endpoint, bucket) == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
	if validate("http://127.0.0.1:1234", "production") == nil {
		t.Fatal("accepted arbitrary bucket")
	}
	if validate("http://127.0.0.1:1234", "rhiza-managed-"+strings.Repeat("1", 40)+"-1-012345abcdef") == nil {
		t.Fatal("accepted overlong bucket")
	}
}

func TestEmulatorProviderSDKRequests(t *testing.T) {
	const bucket = "rhiza-managed-123-1-012345abcdef"
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/no-ambient-credentials")
	t.Setenv("STORAGE_EMULATOR_HOST", "http://invalid.example:1")
	t.Setenv("HTTP_PROXY", "http://invalid.example:1")
	for _, provider := range []string{"s3", "gcs", "azure", "legacy-s3"} {
		t.Run(provider, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch provider {
				case "gcs":
					var body struct{ Name string }
					if r.Method != http.MethodPost || r.URL.Path != "/storage/v1/b" || r.URL.Query().Get("project") != "rhiza-local-test" || r.Header.Get("Authorization") != "" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Name != bucket {
						t.Error("GCS request differs from the explicit anonymous fixture contract")
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"name":"` + bucket + `"}`))
				case "azure":
					if r.Method != http.MethodPut || r.URL.Path != "/rhizatest/"+bucket || r.URL.Query().Get("restype") != "container" || !strings.HasPrefix(r.Header.Get("Authorization"), "SharedKey rhizatest:") {
						t.Error("Azure request differs from the explicit dummy-account contract")
					}
					w.WriteHeader(http.StatusCreated)
				default:
					if r.Method != http.MethodPut || r.URL.Path != "/"+bucket || !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=rhiza-local-test/") {
						t.Error("S3 request differs from the explicit dummy-account contract")
					}
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer server.Close()
			args := []string{provider, server.URL, bucket}
			if provider == "legacy-s3" {
				args = args[1:]
			}
			if err := run(args); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 {
				t.Fatalf("success request count = %d, want 1", requests.Load())
			}
			if os.Getenv("STORAGE_EMULATOR_HOST") != "http://invalid.example:1" {
				t.Fatal("helper changed the caller's emulator environment")
			}
		})
	}
}

func TestEmulatorProviderRejectsBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	for _, args := range [][]string{
		{"az", server.URL, "rhiza-managed-123-1-012345abcdef"},
		{"gcs", server.URL + "/path", "rhiza-managed-123-1-012345abcdef"},
		{"azure", server.URL, "foreign-container"},
	} {
		if run(args) == nil {
			t.Fatal("accepted an invalid provider, endpoint or bucket")
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid request reached an SDK endpoint")
	}
}

func TestEmulatorProviderRejectsRedirects(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	for _, provider := range []string{"s3", "gcs", "azure"} {
		if run([]string{provider, server.URL, "rhiza-managed-123-1-012345abcdef"}) == nil {
			t.Fatalf("%s accepted a redirect", provider)
		}
	}
	if redirected.Load() != 0 {
		t.Fatal("SDK followed redirect")
	}
}

func TestEmulatorServiceAccountPrivateOwnership(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ha-managed-private.owned")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "gcs-emulator.json")
	if err := run([]string{"emulator-service-account", path}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("credential is not a private regular file")
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var account map[string]string
	if json.Unmarshal(before, &account) != nil || account["type"] != "service_account" || account["project_id"] != "rhiza-local-test" || account["token_uri"] != "http://floci:4588/token" || account["client_email"] != "rhiza@rhiza-local-test.iam.gserviceaccount.com" {
		t.Fatal("credential identity or token route differs")
	}
	block, _ := pem.Decode([]byte(account["private_key"]))
	if block == nil {
		t.Fatal("credential key is not PEM")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil || key.N.BitLen() != 2048 {
		t.Fatal("credential key is not RSA-2048")
	}
	if writeEmulatorServiceAccount(path) == nil {
		t.Fatal("overwrote an existing credential")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("existing credential changed")
	}
	if writeEmulatorServiceAccount(filepath.Join(dir, "other.json")) == nil {
		t.Fatal("accepted an arbitrary destination")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if writeEmulatorServiceAccount(path) == nil {
		t.Fatal("accepted a non-private parent")
	}
}

func TestEmulatorServiceAccountRejectsSymlinks(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ha-managed-private.owned")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "ha-managed-private.link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if writeEmulatorServiceAccount(filepath.Join(link, "gcs-emulator.json")) == nil {
		t.Fatal("accepted a symlink directory")
	}
	target := filepath.Join(root, "untouched")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "gcs-emulator.json")); err != nil {
		t.Fatal(err)
	}
	if writeEmulatorServiceAccount(filepath.Join(dir, "gcs-emulator.json")) == nil {
		t.Fatal("accepted a symlink file")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "unchanged" {
		t.Fatal("symlink target changed")
	}
}
