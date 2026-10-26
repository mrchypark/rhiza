package operator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/thanos-io/objstore"
)

// validateAuthorityCASBucket checks the atomic upload conditions required by
// canonical state. Exact-version deletion is a separate capability.
func validateAuthorityCASBucket(bucket objstore.Bucket) error {
	if bucket == nil {
		return fmt.Errorf("fencing: canonical authority object store is required")
	}
	supported := bucket.SupportedObjectUploadOptions()
	if err := objstore.ValidateUploadOptions(supported, objstore.WithIfNotExists()); err != nil {
		return fmt.Errorf("fencing: canonical authority store requires conditional create: %w", err)
	}
	if err := objstore.ValidateUploadOptions(supported, objstore.WithIfMatch(&objstore.ObjectVersion{Type: objstore.ETag, Value: "capability-check"})); err != nil {
		return fmt.Errorf("fencing: canonical authority store requires conditional compare-and-swap: %w", err)
	}
	return nil
}

func validAuthorityObjectVersion(version *objstore.ObjectVersion) bool {
	return version != nil && version.Value != "" &&
		(version.Type == objstore.Generation || version.Type == objstore.ETag)
}

func newAuthorityRevision() (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("fencing: create canonical state revision: %w", err)
	}
	return hex.EncodeToString(nonce[:]), nil
}

func validAuthorityRevision(revision string) bool {
	if len(revision) != 64 {
		return false
	}
	_, err := hex.DecodeString(revision)
	return err == nil
}
