package objstore

import (
	"fmt"
	"os"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
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

	// Prefix for all objects
	Prefix string `json:"prefix,omitempty"`
}

// NewBucket creates a new object storage bucket based on configuration.
func NewBucket(cfg Config) (objstore.Bucket, error) {
	switch cfg.Provider {
	case ProviderFilesystem:
		return newFilesystemBucket(cfg)
	case ProviderS3, ProviderGCS, ProviderAzure:
		return nil, fmt.Errorf("provider %s not yet implemented - use filesystem for now", cfg.Provider)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", cfg.Provider)
	}
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
	}

	switch provider {
	case ProviderFilesystem:
		cfg.FilesystemDir = os.Getenv("RHIZA_FILESYSTEM_DIR")
		if cfg.FilesystemDir == "" {
			cfg.FilesystemDir = "./objstore"
		}
	}

	return cfg
}
