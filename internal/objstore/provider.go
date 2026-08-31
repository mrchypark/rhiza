package objstore

import (
	"context"
	"fmt"
	"net/http"
	"os"

	kitlog "github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/azure"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/gcs"
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
	FilesystemDir          string `json:"filesystem_dir,omitempty"`
	Endpoint               string `json:"endpoint,omitempty"`
	Bucket                 string `json:"bucket,omitempty"`
	Region                 string `json:"region,omitempty"`
	Insecure               bool   `json:"insecure,omitempty"`
	MaxRetries             int    `json:"max_retries,omitempty"`
	AccessKey              string `json:"access_key,omitempty"`
	SecretKey              string `json:"secret_key,omitempty"`
	SessionToken           string `json:"session_token,omitempty"`
	ServiceAccount         string `json:"service_account,omitempty"`
	AzureTenantID          string `json:"azure_tenant_id,omitempty"`
	AzureClientID          string `json:"azure_client_id,omitempty"`
	AzureClientSecret      string `json:"azure_client_secret,omitempty"`
	AzureStorageAccount    string `json:"azure_storage_account,omitempty"`
	AzureStorageAccountKey string `json:"azure_storage_account_key,omitempty"`
	AzureConnectionString  string `json:"azure_connection_string,omitempty"`
	AzureUserAssignedID    string `json:"azure_user_assigned_id,omitempty"`

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
	case ProviderGCS:
		providerConfig := gcs.DefaultConfig
		providerConfig.Bucket, providerConfig.ServiceAccount, providerConfig.MaxRetries = cfg.Bucket, cfg.ServiceAccount, cfg.MaxRetries
		bucket, err = gcs.NewBucketWithConfig(context.Background(), kitlog.NewNopLogger(), providerConfig,
			"rhiza", func(next http.RoundTripper) http.RoundTripper { return metrics.transport(next) })
	case ProviderAzure:
		providerConfig := azure.DefaultConfig
		providerConfig.AzTenantID, providerConfig.ClientID, providerConfig.ClientSecret = cfg.AzureTenantID, cfg.AzureClientID, cfg.AzureClientSecret
		providerConfig.StorageAccountName, providerConfig.StorageAccountKey = cfg.AzureStorageAccount, cfg.AzureStorageAccountKey
		providerConfig.StorageConnectionString, providerConfig.ContainerName = cfg.AzureConnectionString, cfg.Bucket
		providerConfig.UserAssignedID, providerConfig.MaxRetries = cfg.AzureUserAssignedID, cfg.MaxRetries
		if cfg.Endpoint != "" {
			providerConfig.Endpoint = cfg.Endpoint
		}
		bucket, err = azure.NewBucketWithConfig(kitlog.NewNopLogger(), providerConfig,
			"rhiza", func(next http.RoundTripper) http.RoundTripper { return metrics.transport(next) })
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
		Provider:       provider,
		Prefix:         os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		Endpoint:       os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		Bucket:         os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		Region:         os.Getenv("AWS_REGION"),
		ServiceAccount: os.Getenv("RHIZA_OBJSTORE_SERVICE_ACCOUNT"),
		AzureTenantID:  os.Getenv("RHIZA_OBJSTORE_AZURE_TENANT_ID"), AzureClientID: os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_ID"),
		AzureClientSecret: os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_SECRET"), AzureStorageAccount: os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"),
		AzureStorageAccountKey: os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT_KEY"), AzureConnectionString: os.Getenv("RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"),
		AzureUserAssignedID: os.Getenv("RHIZA_OBJSTORE_AZURE_USER_ASSIGNED_ID"),
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
