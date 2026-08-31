package objstore

import (
	"strings"
	"testing"
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
