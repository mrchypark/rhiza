package objstore

import (
	"fmt"
	"net/http"
	"os"

	kitlog "github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/s3"
)

// Provider is the object storage provider type.
type Provider string

const (
	ProviderS3         Provider = "s3"
	ProviderGCS        Provider = "gcs"
	ProviderAzure      Provider = "azure"
	ProviderFilesystem Provider = "filesystem"
)

// Config holds object storage configuration.
type Config struct {
	Provider Provider `json:"provider"`

	// Filesystem configuration
	FilesystemDir string `json:"filesystem_dir,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	Region        string `json:"region,omitempty"`
	Insecure      bool   `json:"insecure,omitempty"`
	MaxRetries    int    `json:"max_retries,omitempty"`
	AccessKey     string `json:"access_key,omitempty"`
	SecretKey     string `json:"secret_key,omitempty"`
	SessionToken  string `json:"session_token,omitempty"`

	// Prefix for all objects
	Prefix string `json:"prefix,omitempty"`
}

// NewBucket creates a metered bucket. S3 HTTP request counts include SDK
// retries and multipart calls, which is the billable request boundary.
func NewBucket(cfg Config) (*MeteredBucket, error) {
	metrics := &bucketMetrics{}
	var bucket objstore.Bucket
	var err error
	switch cfg.Provider {
	case ProviderFilesystem:
		bucket, err = newFilesystemBucket(cfg)
	case ProviderS3:
		if cfg.Bucket == "" {
			return nil, fmt.Errorf("S3 bucket is required")
		}
		bucket, err = s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
			Bucket: cfg.Bucket, Endpoint: cfg.Endpoint, Region: cfg.Region, Insecure: cfg.Insecure,
			AWSSDKAuth: cfg.AccessKey == "", AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
			SessionToken: cfg.SessionToken, MaxRetries: cfg.MaxRetries,
		}, "rhiza", func(next http.RoundTripper) http.RoundTripper { return metrics.transport(next) })
	case ProviderGCS, ProviderAzure:
		return nil, fmt.Errorf("provider %s not yet implemented", cfg.Provider)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", cfg.Provider)
	}
	if err != nil {
		return nil, err
	}
	return newMeteredBucket(bucket, metrics), nil
}

func newFilesystemBucket(cfg Config) (objstore.Bucket, error) {
	if cfg.FilesystemDir == "" {
		cfg.FilesystemDir = "./objstore"
	}

	return filesystem.NewBucket(cfg.FilesystemDir)
}

// LoadConfig loads object storage configuration from environment.
func LoadConfig() Config {
	provider := Provider(os.Getenv("RHIZA_OBJSTORE_PROVIDER"))
	if provider == "" {
		provider = ProviderFilesystem
	}

	cfg := Config{
		Provider: provider,
		Prefix:   os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		Endpoint: os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		Bucket:   os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		Region:   os.Getenv("AWS_REGION"),
	}

	switch provider {
	case ProviderFilesystem:
		cfg.FilesystemDir = os.Getenv("RHIZA_FILESYSTEM_DIR")
		if cfg.FilesystemDir == "" {
			cfg.FilesystemDir = "./objstore"
		}
	case ProviderS3:
		cfg.Insecure = os.Getenv("RHIZA_OBJSTORE_INSECURE") == "true"
	}

	return cfg
}
