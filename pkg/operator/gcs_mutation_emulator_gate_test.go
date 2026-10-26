//go:build localhooks

package operator

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/api/option"
)

func TestGCSMutationEmulatorRequiresDedicatedFlociOptIn(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "http://floci-gcs:4588")
	t.Setenv("RHIZA_LOCALHOOKS_ALLOW_FLOCI_GCS", "")
	t.Setenv("RHIZA_LOGICAL_AUTHORITY_FLOCI", "1")
	t.Setenv("CI", "true")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "floci-local")
	if !rejectGCSMutationEmulator() {
		t.Fatal("Floci endpoint was accepted without explicit localhooks opt-in")
	}
	t.Setenv("RHIZA_LOCALHOOKS_ALLOW_FLOCI_GCS", "1")
	if rejectGCSMutationEmulator() {
		t.Fatal("pinned Floci endpoint was rejected after explicit localhooks opt-in")
	}
	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := newGCSMutationProviderForProfile(client, AttestationStoreScope{Provider: "gcs", Bucket: "isolated", Prefix: "root/source"}, fencing.MutationExecutionProfile); err != nil {
		t.Fatalf("explicitly qualified Floci provider rejected: %v", err)
	}
	for _, endpoint := range []string{"http://127.0.0.1:4588", "https://storage.googleapis.com", "http://other:4588"} {
		t.Setenv("STORAGE_EMULATOR_HOST", endpoint)
		if !rejectGCSMutationEmulator() {
			t.Fatalf("unapproved endpoint %q was accepted", endpoint)
		}
	}
	t.Setenv("STORAGE_EMULATOR_HOST", "http://floci-gcs:4588")
	t.Setenv("CI", "")
	if !rejectGCSMutationEmulator() {
		t.Fatal("Floci endpoint was accepted outside CI")
	}
}
