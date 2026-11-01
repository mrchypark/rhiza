package types

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

// ManagedStorageBinding preserves the prepared-startup physical source digest.
// Pass the actual scoped ObjStorePrefix and generation ClusterID. This performs
// no SDK calls/provisioning and never includes credentials in the digest.
func ManagedStorageBinding(c ExecutionConfig) (string, error) {
	provider := c.ObjStoreProvider
	if provider == "" {
		provider = "s3"
	}
	endpoint, account, fsDir := c.ObjStoreEndpoint, c.ObjStoreAzureStorageAccount, ""
	if provider == "filesystem" {
		fsDir = c.ObjStoreDir
		if fsDir == "" {
			fsDir = "./objstore"
		}
		var err error
		fsDir, err = filepath.Abs(fsDir)
		if err != nil {
			return "", err
		}
	}
	if provider == "azure" && c.ObjStoreAzureConnectionString != "" {
		parts := map[string]string{}
		items := strings.Split(c.ObjStoreAzureConnectionString, ";")
		for i, item := range items {
			if item == "" && i == len(items)-1 {
				continue
			}
			key, value, ok := strings.Cut(item, "=")
			if !ok {
				return "", fmt.Errorf("invalid Azure connection descriptor")
			}
			key = strings.ToLower(strings.TrimSpace(key))
			if _, exists := parts[key]; exists {
				return "", fmt.Errorf("duplicate Azure connection field")
			}
			parts[key] = value
		}
		if parts["accountname"] == "" || parts["accountname"] != account || parts["blobendpoint"] == "" {
			return "", fmt.Errorf("Azure connection requires matching account and explicit blob endpoint")
		}
		endpoint = parts["blobendpoint"]
	}
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return "", fmt.Errorf("invalid storage endpoint")
		}
	}
	source, _ := json.Marshal(struct {
		Provider, Endpoint, Bucket, Prefix, Account, FilesystemDir, Cluster, Region string
		Insecure                                                                    bool
	}{provider, endpoint, c.ObjStoreBucket, path.Clean(c.ObjStorePrefix), account, fsDir, string(c.ClusterID), c.ObjStoreRegion, c.ObjStoreInsecure})
	return fmt.Sprintf("%x", sha256.Sum256(source)), nil
}
