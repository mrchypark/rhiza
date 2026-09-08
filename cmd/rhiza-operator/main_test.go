package main

import (
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
)

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
