package rhiza

import (
	"context"
	"fmt"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/node"
)

// PrepareHAInitRestore verifies the existing configured HA source and stages
// its native checkpoint for the later Node.Open adoption path. It is intended
// for the restore-init command; it never opens the application Node or WAL.
func PrepareHAInitRestore(ctx context.Context, config Config) error {
	internalConfig, err := executionConfig(config)
	if err != nil {
		return err
	}
	provider := internalobjstore.Provider(config.ObjStoreProvider)
	if provider == "" {
		return fmt.Errorf("HA init restore requires RHIZA_OBJSTORE_PROVIDER")
	}
	bucket, err := internalobjstore.NewBucketForSupervisedStartup(ctx, internalobjstore.Config{
		Provider: provider, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
		Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Region: config.ObjStoreRegion,
		Insecure: config.ObjStoreInsecure, MaxRetries: config.ObjStoreRetries,
		AccessKey: config.ObjStoreAccessKey, SecretKey: config.ObjStoreSecretKey, SessionToken: config.ObjStoreSessionToken,
		ServiceAccount: config.ObjStoreServiceAccount, AzureTenantID: config.ObjStoreAzureTenantID,
		AzureClientID: config.ObjStoreAzureClientID, AzureClientSecret: config.ObjStoreAzureClientSecret,
		AzureStorageAccount: config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
		AzureConnectionString: config.ObjStoreAzureConnectionString, AzureUserAssignedID: config.ObjStoreAzureUserAssignedID,
	})
	if err != nil {
		return fmt.Errorf("open HA restore source: %w", err)
	}
	defer bucket.Close()
	if err := node.PrepareHAInitRestore(ctx, internalConfig, bucket); err != nil {
		return err
	}
	return nil
}
