package main

import (
	"path"

	objstorecfg "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// hashStoreIdentity computes the deterministic StorageID from the trusted
// storage config. This is sha256(JSON([provider, endpoint, bucket, prefix,
// cluster])) — the same recipe the Ternal guard uses. Credentials are never
// included in the identity hash.
func hashStoreIdentity(cfg objstorecfg.Config, cluster string) string {
	binding, err := (recoveryanchor.StorageBindingConfig{
		Policy: recoveryanchor.StorageBindingDerived, Provider: string(cfg.Provider), Endpoint: cfg.Endpoint,
		Bucket: cfg.Bucket, Prefix: cfg.Prefix,
	}).ExpectedBinding(cluster)
	if err != nil {
		return ""
	}
	return binding.StorageID
}

func anchorStorageBindingConfig(cfg objstorecfg.Config, policy, opaqueStorageID string) (recoveryanchor.StorageBindingConfig, error) {
	binding := recoveryanchor.StorageBindingConfig{
		Policy: recoveryanchor.StorageBindingPolicy(policy), Provider: string(cfg.Provider), Endpoint: cfg.Endpoint,
		Bucket: cfg.Bucket, Prefix: cfg.Prefix, OpaqueStorageID: opaqueStorageID,
	}
	if err := binding.Validate(); err != nil {
		return recoveryanchor.StorageBindingConfig{}, err
	}
	return binding, nil
}

// storagePrefix returns the object store prefix for a given cluster.
func storagePrefix(cfg objstorecfg.Config, cluster string) string {
	return path.Join(cfg.Prefix, cluster)
}
