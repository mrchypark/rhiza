package node

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

func verifyInstalledPolicy(ctx context.Context, config *types.ExecutionConfig, bucket objstore.Bucket) (*recovery.VerifiedInstalledPolicy, error) {
	expectation, err := installedPolicyExpectation(config)
	if err != nil {
		return nil, err
	}
	// Node's object-store factory returns the raw provider bucket. Node.open
	// applies ObjStorePrefix when constructing manager object paths, so policy
	// lookup must apply the configured prefix exactly once here.
	return recovery.VerifyInstalledPolicy(ctx, bucket, config.ObjStorePrefix, expectation)
}

func installedPolicyExpectation(config *types.ExecutionConfig) (recovery.InstalledPolicyExpectation, error) {
	if config == nil || config.ExpectedInstalledPolicyHash == ([32]byte{}) {
		return recovery.InstalledPolicyExpectation{}, fmt.Errorf("independently configured installed-policy digest is required")
	}
	if config.Local {
		return recovery.InstalledPolicyExpectation{}, fmt.Errorf("installed-policy profile is not supported in Local mode")
	}
	if config.ClusterID == "" {
		return recovery.InstalledPolicyExpectation{}, fmt.Errorf("installed-policy configuration requires an explicit cluster ID")
	}
	if len(config.Members) == 0 {
		return recovery.InstalledPolicyExpectation{}, fmt.Errorf("installed-policy configuration requires initial membership")
	}
	provider := config.ObjStoreProvider
	if provider == "" {
		provider = "s3"
	}
	durability := config.ObjStoreDurability
	if durability == "" {
		// Match Node.open's ordinary non-Local default. Local is rejected above.
		durability = types.ObjectStoreDurabilityAsync
	}
	storageBinding, err := (recoveryanchor.StorageBindingConfig{
		Policy:   recoveryanchor.StorageBindingDerived,
		Provider: provider,
		Endpoint: config.ObjStoreEndpoint,
		Bucket:   config.ObjStoreBucket,
		Prefix:   config.ObjStorePrefix,
	}).ExpectedBinding(string(config.ClusterID))
	if err != nil {
		return recovery.InstalledPolicyExpectation{}, fmt.Errorf("derive trusted installed-policy storage binding: %w", err)
	}
	return recovery.InstalledPolicyExpectation{
		ExpectedDigest:   config.ExpectedInstalledPolicyHash,
		ClusterID:        string(config.ClusterID),
		StorageBindingID: storageBinding.StorageID,
		InitialCluster:   quepaxa.Cluster{ConfigID: 1, Members: append([]quepaxa.Member(nil), config.Members...)},
		Durability:       durability,
	}, nil
}
