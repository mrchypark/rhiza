package recoveryanchor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// StorageBindingPolicy selects how an expected binding is established.
// Derived is the bundled service's version-1 five-field location recipe.
// Opaque is an independently configured external mapping.
type StorageBindingPolicy string

const (
	StorageBindingDerived StorageBindingPolicy = "derived"
	StorageBindingOpaque  StorageBindingPolicy = "opaque"
)

// StorageBindingConfig contains trusted configuration only. Credentials are
// deliberately excluded from both policies and from the derived hash.
type StorageBindingConfig struct {
	Policy          StorageBindingPolicy
	Provider        string
	Endpoint        string
	Bucket          string
	Prefix          string
	OpaqueStorageID string
}

// ExpectedBinding independently computes the expected binding for clusterID.
// The receipt must never be used as input to this calculation.
func (c StorageBindingConfig) ExpectedBinding(clusterID string) (Binding, error) {
	if err := c.Validate(); err != nil {
		return Binding{}, err
	}
	switch c.Policy {
	case StorageBindingDerived:
		identity := [5]string{c.Provider, c.Endpoint, c.Bucket, c.Prefix, clusterID}
		data, err := json.Marshal(identity)
		if err != nil {
			return Binding{}, fmt.Errorf("recovery anchor: encode derived storage binding: %w", err)
		}
		sum := sha256.Sum256(data)
		return Binding{ClusterID: clusterID, StorageID: fmt.Sprintf("%x", sum)}, nil
	case StorageBindingOpaque:
		return Binding{ClusterID: clusterID, StorageID: c.OpaqueStorageID}, nil
	default:
		return Binding{}, fmt.Errorf("recovery anchor: storage binding policy must be explicitly derived or opaque")
	}
}

// Validate rejects absent or ambiguous trusted binding configuration before
// any request is processed.
func (c StorageBindingConfig) Validate() error {
	switch c.Policy {
	case StorageBindingDerived:
		if c.OpaqueStorageID != "" {
			return fmt.Errorf("recovery anchor: derived and opaque storage binding are ambiguous")
		}
		switch c.Provider {
		case "s3":
			if c.Bucket == "" {
				return fmt.Errorf("recovery anchor: derived S3 binding requires a bucket")
			}
		case "gcs":
			if c.Bucket == "" || c.Endpoint != "" {
				return fmt.Errorf("recovery anchor: derived GCS binding requires a bucket and does not support endpoint overrides")
			}
		case "azure":
			return fmt.Errorf("recovery anchor: Azure storage identity is not fully covered by the five-field derived recipe; configure an opaque binding")
		default:
			return fmt.Errorf("recovery anchor: derived binding requires a supported shared provider")
		}
	case StorageBindingOpaque:
		if c.OpaqueStorageID == "" {
			return fmt.Errorf("recovery anchor: opaque binding requires an independently configured storage ID")
		}
	default:
		return fmt.Errorf("recovery anchor: storage binding policy must be explicitly derived or opaque")
	}
	return nil
}
