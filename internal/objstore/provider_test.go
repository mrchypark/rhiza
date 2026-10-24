package objstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCloudProvidersAreWired(t *testing.T) {
	for _, test := range []struct {
		provider Provider
		want     string
	}{
		{ProviderGCS, "bucket"},
		{ProviderAzure, "storage account"},
	} {
		t.Run(string(test.provider), func(t *testing.T) {
			_, err := NewBucket(Config{Provider: test.provider})
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "not yet implemented") {
				t.Fatalf("provider error=%v", err)
			}
		})
	}
}

func TestLoadConfigReadsRHIZAObjectStoreCredentials(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_PROVIDER", "s3")
	t.Setenv("RHIZA_OBJSTORE_ENDPOINT", "http://object-store.test:9000")
	t.Setenv("RHIZA_OBJSTORE_BUCKET", "rhiza-test")
	t.Setenv("RHIZA_OBJSTORE_PREFIX", "rhiza-test-prefix")
	t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", "test-access-key")
	t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", "test-secret-key")
	t.Setenv("RHIZA_OBJSTORE_SESSION_TOKEN", "test-session-token")

	config := LoadConfig()
	if config.AccessKey != "test-access-key" {
		t.Errorf("access key = %q, want configured RHIZA object-store key", config.AccessKey)
	}
	if config.SecretKey != "test-secret-key" {
		t.Errorf("secret key = %q, want configured RHIZA object-store secret", config.SecretKey)
	}
	if config.SessionToken != "test-session-token" {
		t.Errorf("session token = %q, want configured RHIZA object-store token", config.SessionToken)
	}
}

func TestLoadConfigCredentialsReachS3Requests(t *testing.T) {
	tests := []struct {
		name        string
		rhizaKey    string
		rhizaSecret string
		rhizaToken  string
		unsetRHIZA  bool
		wantKey     string
		wantToken   string
	}{
		{name: "explicit RHIZA credentials take precedence", rhizaKey: "rhiza-test-key", rhizaSecret: "rhiza-test-secret", rhizaToken: "rhiza-test-token", wantKey: "rhiza-test-key", wantToken: "rhiza-test-token"},
		{name: "static RHIZA key pair without session token", rhizaKey: "rhiza-static-key", rhizaSecret: "rhiza-static-secret", wantKey: "rhiza-static-key"},
		{name: "empty RHIZA credentials use AWS SDK environment", wantKey: "aws-test-key", wantToken: "aws-test-token"},
		{name: "unset RHIZA credentials use AWS SDK environment", unsetRHIZA: true, wantKey: "aws-test-key", wantToken: "aws-test-token"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var authorization, sessionToken string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorization = r.Header.Get("Authorization")
				sessionToken = r.Header.Get("X-Amz-Security-Token")
				w.Header().Set("ETag", `"test-etag"`)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			t.Setenv("RHIZA_OBJSTORE_PROVIDER", "s3")
			t.Setenv("RHIZA_OBJSTORE_ENDPOINT", strings.TrimPrefix(server.URL, "http://"))
			t.Setenv("RHIZA_OBJSTORE_BUCKET", "rhiza-test")
			t.Setenv("RHIZA_OBJSTORE_PREFIX", "rhiza-test-prefix")
			t.Setenv("RHIZA_OBJSTORE_INSECURE", "true")
			if test.unsetRHIZA {
				for _, name := range []string{"RHIZA_OBJSTORE_ACCESS_KEY", "RHIZA_OBJSTORE_SECRET_KEY", "RHIZA_OBJSTORE_SESSION_TOKEN"} {
					previous, wasSet := os.LookupEnv(name)
					t.Cleanup(func() {
						if wasSet {
							_ = os.Setenv(name, previous)
						} else {
							_ = os.Unsetenv(name)
						}
					})
					if err := os.Unsetenv(name); err != nil {
						t.Fatalf("unset %s: %v", name, err)
					}
				}
			} else {
				t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", test.rhizaKey)
				t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", test.rhizaSecret)
				t.Setenv("RHIZA_OBJSTORE_SESSION_TOKEN", test.rhizaToken)
			}
			t.Setenv("AWS_ACCESS_KEY_ID", "aws-test-key")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-test-secret")
			t.Setenv("AWS_SESSION_TOKEN", "aws-test-token")
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			t.Setenv("AWS_REGION", "us-east-1")

			bucket, err := NewBucket(LoadConfig())
			if err != nil {
				t.Fatalf("create S3 bucket: %v", err)
			}
			defer bucket.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := bucket.Upload(ctx, "credential-probe", strings.NewReader("probe")); err != nil {
				t.Fatalf("upload signed request: %v", err)
			}
			if !strings.Contains(authorization, fmt.Sprintf("Credential=%s/", test.wantKey)) {
				t.Errorf("Authorization credential does not use configured access key: %q", authorization)
			}
			if sessionToken != test.wantToken {
				t.Errorf("signed session token = %q, want %q", sessionToken, test.wantToken)
			}
		})
	}
}

func TestLoadConfigPartialS3CredentialsAreRejectedByProvider(t *testing.T) {
	for _, test := range []struct {
		name      string
		accessKey string
		secretKey string
	}{
		{name: "access key only", accessKey: "test-access-key"},
		{name: "secret key only", secretKey: "test-secret-key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("RHIZA_OBJSTORE_PROVIDER", "s3")
			t.Setenv("RHIZA_OBJSTORE_ENDPOINT", "object-store.test:9000")
			t.Setenv("RHIZA_OBJSTORE_BUCKET", "rhiza-test")
			t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", test.accessKey)
			t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", test.secretKey)
			t.Setenv("RHIZA_OBJSTORE_SESSION_TOKEN", "")
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")
			t.Setenv("AWS_SESSION_TOKEN", "")
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

			if _, err := NewBucket(LoadConfig()); err == nil {
				t.Fatal("provider accepted an incomplete S3 key pair")
			}
		})
	}
}

func TestLoadConfigGCSServiceAccountRemainsUnchanged(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_PROVIDER", "gcs")
	t.Setenv("RHIZA_OBJSTORE_SERVICE_ACCOUNT", `{"type":"service_account","project_id":"test-project"}`)
	t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", "")
	t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", "")
	t.Setenv("RHIZA_OBJSTORE_SESSION_TOKEN", "")

	config := LoadConfig()
	if config.Provider != ProviderGCS || config.ServiceAccount != `{"type":"service_account","project_id":"test-project"}` {
		t.Fatalf("GCS config changed: provider=%q service_account=%q", config.Provider, config.ServiceAccount)
	}
	if config.AccessKey != "" || config.SecretKey != "" || config.SessionToken != "" {
		t.Fatal("S3 credentials unexpectedly populated in GCS config")
	}
}

func TestCloudProvidersRejectIgnoredTransportOptions(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
	}{
		{"GCS endpoint", Config{Provider: ProviderGCS, Bucket: "bucket", Endpoint: "emulator:4443"}},
		{"GCS insecure", Config{Provider: ProviderGCS, Bucket: "bucket", Insecure: true}},
		{"Azure insecure", Config{Provider: ProviderAzure, Bucket: "container", AzureStorageAccount: "account", Insecure: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateConfig(test.config); err == nil || !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("validation error=%v", err)
			}
		})
	}
}
