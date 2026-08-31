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
		{ProviderAzure, "storage_account_name"},
	} {
		t.Run(string(test.provider), func(t *testing.T) {
			_, err := NewBucket(Config{Provider: test.provider})
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "not yet implemented") {
				t.Fatalf("provider error=%v", err)
			}
		})
	}
}
