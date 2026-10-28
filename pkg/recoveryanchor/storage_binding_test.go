package recoveryanchor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestStorageBindingConfigExpectedBindingPreservesFiveFieldRecipe(t *testing.T) {
	cfg := StorageBindingConfig{Policy: StorageBindingDerived, Provider: "s3", Endpoint: "https://objects.example", Bucket: "archive", Prefix: "root"}
	for _, cluster := range []string{"source", "target-a", "target-b"} {
		got, err := cfg.ExpectedBinding(cluster)
		if err != nil {
			t.Fatalf("ExpectedBinding(%q): %v", cluster, err)
		}
		data, err := json.Marshal([5]string{"s3", "https://objects.example", "archive", "root", cluster})
		if err != nil {
			t.Fatal(err)
		}
		want := sha256.Sum256(data)
		if got != (Binding{ClusterID: cluster, StorageID: hex.EncodeToString(want[:])}) {
			t.Fatalf("ExpectedBinding(%q)=%+v, want existing five-field recipe", cluster, got)
		}
	}
	source, _ := cfg.ExpectedBinding("source")
	targetA, _ := cfg.ExpectedBinding("target-a")
	targetB, _ := cfg.ExpectedBinding("target-b")
	if source.StorageID == targetA.StorageID || targetA.StorageID == targetB.StorageID {
		t.Fatal("derived storage binding did not distinguish cluster IDs")
	}
	providerS3, err := (StorageBindingConfig{Policy: StorageBindingDerived, Provider: "s3", Bucket: "archive", Prefix: "root"}).ExpectedBinding("target-a")
	if err != nil {
		t.Fatal(err)
	}
	providerGCS, err := (StorageBindingConfig{Policy: StorageBindingDerived, Provider: "gcs", Bucket: "archive", Prefix: "root"}).ExpectedBinding("target-a")
	if err != nil || providerS3.StorageID == providerGCS.StorageID {
		t.Fatalf("derived storage binding ignored provider: s3=%+v gcs=%+v err=%v", providerS3, providerGCS, err)
	}
	for name, changed := range map[string]StorageBindingConfig{
		"endpoint": {Policy: StorageBindingDerived, Provider: cfg.Provider, Endpoint: "https://other.example", Bucket: cfg.Bucket, Prefix: cfg.Prefix},
		"bucket":   {Policy: StorageBindingDerived, Provider: cfg.Provider, Endpoint: cfg.Endpoint, Bucket: "other-archive", Prefix: cfg.Prefix},
		"prefix":   {Policy: StorageBindingDerived, Provider: cfg.Provider, Endpoint: cfg.Endpoint, Bucket: cfg.Bucket, Prefix: "other-root"},
	} {
		t.Run("changed-"+name, func(t *testing.T) {
			got, err := changed.ExpectedBinding("target-a")
			if err != nil {
				t.Fatal(err)
			}
			if got.StorageID == targetA.StorageID {
				t.Fatalf("derived storage ID ignored changed %s", name)
			}
		})
	}
}

func TestStorageBindingConfigRequiresExplicitUnambiguousPolicy(t *testing.T) {
	validDerived := StorageBindingConfig{Policy: StorageBindingDerived, Provider: "gcs", Bucket: "archive", Prefix: "root"}
	validOpaque := StorageBindingConfig{Policy: StorageBindingOpaque, OpaqueStorageID: "external-namespace"}
	if got, err := validOpaque.ExpectedBinding("target"); err != nil || got != (Binding{ClusterID: "target", StorageID: "external-namespace"}) {
		t.Fatalf("opaque binding=%+v err=%v", got, err)
	}
	for name, cfg := range map[string]StorageBindingConfig{
		"missing policy":        {Provider: "s3", Bucket: "archive"},
		"missing derived store": {Policy: StorageBindingDerived, Provider: "s3"},
		"local derived store":   {Policy: StorageBindingDerived, Provider: "filesystem", Bucket: "archive"},
		"ambiguous derived":     {Policy: StorageBindingDerived, Provider: "s3", Bucket: "archive", OpaqueStorageID: "external"},
		"azure five-field gap":  {Policy: StorageBindingDerived, Provider: "azure", Bucket: "container"},
		"missing opaque ID":     {Policy: StorageBindingOpaque},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cfg.ExpectedBinding("target"); err == nil {
				t.Fatal("invalid binding policy/configuration was accepted")
			}
		})
	}
	if got, err := validDerived.ExpectedBinding(""); err != nil || got.StorageID == "" {
		t.Fatalf("five-field recipe compatibility for empty cluster: binding=%+v err=%v", got, err)
	}
}
