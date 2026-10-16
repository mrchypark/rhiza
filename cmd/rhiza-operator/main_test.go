package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestDeletionDefaultUsesActualPodNamespaceNotWatchOverride(t *testing.T) {
	file := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(file, []byte("service-account-ns\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ env, want string }{{"service-account-ns", "service-account-ns"}, {"", "service-account-ns"}} {
		t.Run(test.want, func(t *testing.T) {
			t.Setenv("POD_NAMESPACE", test.env)
			k := &operator.Kubernetes{Namespace: "watch-override", TokenFile: "/not-a-token"}
			if err := configureDeletion(k, "", file); err != nil {
				t.Fatal(err)
			}
			if err := k.DeletePod(t.Context(), "watch-override", "learner", "uid"); err == nil || !strings.Contains(err.Error(), "namespace is not allowed") {
				t.Fatalf("watch override broadened deletion: %v", err)
			}
			if err := k.DeletePod(t.Context(), test.want, "learner", "uid"); err == nil || strings.Contains(err.Error(), "namespace is not allowed") {
				t.Fatalf("actual Pod namespace was not allowed: %v", err)
			}
		})
	}
	t.Setenv("POD_NAMESPACE", "watch-override")
	if err := configureDeletion(&operator.Kubernetes{Namespace: "watch-override"}, "", file); err == nil {
		t.Fatal("POD_NAMESPACE mismatch with mounted namespace accepted")
	}
}

func TestDeletionScopeFailsClosedWithoutOwnNamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	k := &operator.Kubernetes{Namespace: "watch-override"}
	if err := configureDeletion(k, "", filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("missing actual namespace accepted")
	}
	for _, raw := range []string{"target-a,target-b", "*"} {
		if err := configureDeletion(k, raw, "/not-needed"); err != nil {
			t.Fatalf("explicit scope required own namespace: %v", err)
		}
	}
	t.Setenv("POD_NAMESPACE", "*")
	if err := configureDeletion(k, "", "/not-needed"); err == nil {
		t.Fatal("Pod namespace wildcard expanded default authority")
	}
}

func TestOperatorBucketConfigUsesObjectStoreEnvironment(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_PROVIDER", "s3")
	t.Setenv("RHIZA_OBJSTORE_ENDPOINT", "http://minio.test:9000")
	t.Setenv("RHIZA_OBJSTORE_BUCKET", "rhiza-recovery")
	t.Setenv("RHIZA_OBJSTORE_PREFIX", "operator-prefix")
	t.Setenv("RHIZA_OBJSTORE_REGION", "test-region")
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "true")
	t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", "test-access")
	t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", "test-secret")
	t.Setenv("RHIZA_OBJSTORE_SESSION_TOKEN", "test-session")
	t.Setenv("RHIZA_OBJSTORE_MAX_RETRIES", "7")
	config, err := operatorBucketConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.Provider != objectstore.ProviderS3 || config.Endpoint != "http://minio.test:9000" || config.Bucket != "rhiza-recovery" || config.Prefix != "operator-prefix" || config.Region != "test-region" || !config.Insecure || config.MaxRetries != 7 {
		t.Fatal("operator object-store endpoint settings were not preserved")
	}
	if config.AccessKey != "test-access" || config.SecretKey != "test-secret" || config.SessionToken != "test-session" {
		t.Fatal("operator object-store credentials were not preserved")
	}
}

func TestOperatorBucketConfigRejectsInvalidInsecureFlag(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "not-a-bool")
	if _, err := operatorBucketConfig(); err == nil || !strings.Contains(err.Error(), "RHIZA_OBJSTORE_INSECURE") {
		t.Fatalf("error=%v, want invalid insecure flag", err)
	}
}

func TestFencerBackendPreservesLegacyHTTPAndDefaultsToKubernetes(t *testing.T) {
	for _, test := range []struct {
		name, backend, url, token, ca, want string
		bad                                 bool
	}{
		{name: "new default", want: "kubernetes"},
		{name: "legacy HTTP", url: "https://fencer", token: "token-file", want: "http"},
		{name: "partial URL", url: "https://fencer", bad: true},
		{name: "partial token", token: "token-file", bad: true},
		{name: "partial CA", ca: "ca-file", bad: true},
		{name: "explicit HTTP", backend: "http", url: "https://fencer", token: "token-file", want: "http"},
		{name: "incomplete explicit HTTP", backend: "http", bad: true},
		{name: "explicit Kubernetes wins", backend: "kubernetes", url: "https://fencer", token: "token-file", want: "kubernetes"},
		{name: "unsupported", backend: "unknown", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := fencerBackend(test.backend, test.url, test.token, test.ca)
			if (err != nil) != test.bad || got != test.want {
				t.Fatalf("backend=%q err=%v", got, err)
			}
		})
	}
}
