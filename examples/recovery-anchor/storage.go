package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"

	objstorecfg "github.com/mrchypark/rhiza/internal/objstore"
)

// hashStoreIdentity computes the deterministic StorageID from the trusted
// storage config. This is sha256(JSON([provider, endpoint, bucket, prefix,
// cluster])) — the same recipe the Ternal guard uses. Credentials are never
// included in the identity hash.
func hashStoreIdentity(cfg objstorecfg.Config, cluster string) string {
	identity := [5]string{
		string(cfg.Provider),
		cfg.Endpoint,
		cfg.Bucket,
		cfg.Prefix,
		cluster,
	}
	data, _ := json.Marshal(identity)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

// storagePrefix returns the object store prefix for a given cluster.
func storagePrefix(cfg objstorecfg.Config, cluster string) string {
	return path.Join(cfg.Prefix, cluster)
}
