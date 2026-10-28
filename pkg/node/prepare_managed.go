package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// PrepareManagedInit executes the authorized offline preparation for one
// assignment. It does not report namespace join or authority completion.
func PrepareManagedInit(ctx context.Context, config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) (fencing.InitLiveCleanup, string, error) {
	return prepareManagedInit(ctx, config, assignment, fencing.InitAdmission{})
}

// PrepareManagedInitWithAdmission additionally requires the exact authority
// response that reserved this init process before a DR assignment can bypass
// the public self-member restore guard.
func PrepareManagedInitWithAdmission(ctx context.Context, config *types.ExecutionConfig, admission fencing.InitAdmission) (fencing.InitLiveCleanup, string, error) {
	assignment := admission.Assignment()
	return prepareManagedInit(ctx, config, assignment, admission)
}

func prepareManagedInit(ctx context.Context, config *types.ExecutionConfig, assignment fencing.BootstrapAssignment, admission fencing.InitAdmission) (fencing.InitLiveCleanup, string, error) {
	if err := assignment.Validate(); err != nil {
		return fencing.InitLiveCleanup{}, "", err
	}
	if config == nil || config.PreparedPodUID != assignment.PodUID || string(config.ClusterID) != assignment.SourceClusterID || string(config.NodeID) != assignment.NodeID {
		return fencing.InitLiveCleanup{}, "", fmt.Errorf("managed preparation configuration differs from its assignment")
	}
	switch assignment.Purpose {
	case fencing.BootstrapInitial:
		execution, err := prepareInitialGenesis(ctx, config, assignment)
		cleanup := fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis}
		cleanup.CleanupSucceeded = err == nil && execution.CleanupErr == nil &&
			(execution.MarkerNoop || execution.MaterializerDone && execution.FilesSynced)
		marker, markerErr := preparedGenesisMarkerFor(config, assignment)
		if markerErr != nil {
			err = fmt.Errorf("prepare genesis digest: %w", markerErr)
			cleanup.CleanupSucceeded = false
		}
		return cleanup, marker.AssignmentHash, err
	case fencing.BootstrapLearner:
		if err := validateManagedLearnerAssignment(config, assignment); err != nil {
			return fencing.InitLiveCleanup{}, "", err
		}
		execution, err := prepareRestoreWithExecution(ctx, config, (*objstore.MeteredBucket)(nil))
		cleanup := fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: execution.BucketDrainSucceeded}
		if execution.MarkerNoop {
			cleanup.Mode = fencing.InitModeMarkerNoop
			cleanup.BucketDrainSucceeded = false
		}
		cleanup.CleanupSucceeded = err == nil && execution.CleanupErr == nil
		return cleanup, "", err
	case fencing.BootstrapDRSuccessor:
		if err := admission.ValidateFor(assignment); err != nil {
			return fencing.InitLiveCleanup{}, "", err
		}
		if _, err := validateManagedDRSuccessorAssignment(config, assignment); err != nil {
			return fencing.InitLiveCleanup{}, "", err
		}
		execution, err := prepareRestoreWithDRAssignment(ctx, config, (*objstore.MeteredBucket)(nil), assignment)
		cleanup := fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: execution.BucketDrainSucceeded}
		if execution.MarkerNoop {
			cleanup.Mode = fencing.InitModeMarkerNoop
			cleanup.BucketDrainSucceeded = false
		}
		cleanup.CleanupSucceeded = err == nil && execution.CleanupErr == nil
		return cleanup, "", err
	default:
		return fencing.InitLiveCleanup{}, "", fmt.Errorf("unsupported managed preparation purpose")
	}
}

func validateManagedDRSuccessorAssignment(config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) (recovery.EvidenceOptions, error) {
	fail := func() (recovery.EvidenceOptions, error) {
		return recovery.EvidenceOptions{}, fmt.Errorf("managed DR assignment differs from its approved target profile")
	}
	if config == nil || assignment.Validate() != nil || assignment.Purpose != fencing.BootstrapDRSuccessor ||
		config.PreparedPodUID != assignment.PodUID || string(config.ClusterID) != assignment.SourceClusterID ||
		string(config.NodeID) != assignment.NodeID || config.Local || config.Learner != nil || !config.EnableReconfiguration || len(config.Members) < 2 {
		return fail()
	}
	request := assignment.DRApproval.Request
	durability := string(config.ObjStoreDurability)
	if durability == "" {
		durability = string(types.ObjectStoreDurabilityAsync)
	}
	wantMembership := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, durability)
	if request.TargetClusterID != string(config.ClusterID) || request.StatefulSetUID != assignment.StatefulSetUID ||
		request.TargetPrefix != path.Join(config.ObjStorePrefix, string(config.ClusterID)) || request.TargetMembership != wantMembership ||
		assignment.MembershipHash != fencing.BootstrapMembersHash(config.Members) || assignment.Durability != durability ||
		assignment.StoreIdentityHash != managedStoreIdentityHash(config) {
		return fail()
	}
	localCount := 0
	observerID := quepaxa.NodeID("prepare-" + string(config.NodeID))
	seen := make(map[quepaxa.NodeID]struct{}, len(config.Members))
	for _, member := range config.Members {
		if member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) {
			return fail()
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fail()
		}
		seen[member.ID] = struct{}{}
		if member.ID == config.NodeID {
			localCount++
		}
		if member.ID == observerID {
			return fail()
		}
	}
	if localCount != 1 {
		return fail()
	}
	if assignment.AuthorizationHash != fencing.BootstrapDRAuthorizationHash(assignment) {
		return recovery.EvidenceOptions{}, fmt.Errorf("managed DR assignment authorization hash is invalid")
	}
	anchorHash, err := hex.DecodeString(request.TargetAnchorHash)
	if err != nil || len(anchorHash) != 32 || hex.EncodeToString(anchorHash) != request.TargetAnchorHash {
		return fail()
	}
	var expectedAnchorHash [32]byte
	copy(expectedAnchorHash[:], anchorHash)
	return recovery.EvidenceOptions{
		ExpectedAnchorHash: expectedAnchorHash, ExpectedMembership: request.TargetMembership,
		ExpectedForkResult: request.Fork, ExpectedSourcePrefix: request.SourcePrefix,
		ExpectedOperationID: request.OperationID,
	}, nil
}

func validateManagedLearnerAssignment(config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) error {
	if config == nil || config.PreparedPodUID != assignment.PodUID || string(config.ClusterID) != assignment.SourceClusterID || string(config.NodeID) != assignment.NodeID ||
		config.Learner == nil || config.Learner.ID != config.NodeID ||
		config.Learner.PublicKey == (quepaxa.PublicKey{}) || assignment.PublicMember.ID != assignment.NodeID ||
		assignment.PublicMember.URL == "" || assignment.PublicMember.PeerURL == "" || assignment.PublicMember.WALIdentity != "" {
		return fmt.Errorf("managed learner preparation requires its exact assigned public identity")
	}
	key, err := hex.DecodeString(assignment.PublicMember.PublicKey)
	if err != nil || len(key) != len(config.Learner.PublicKey) {
		return fmt.Errorf("managed learner assignment public key is invalid")
	}
	var publicKey quepaxa.PublicKey
	copy(publicKey[:], key)
	wantMember := quepaxa.Member{
		ID: quepaxa.NodeID(assignment.PublicMember.ID), URL: assignment.PublicMember.URL,
		PeerURL: assignment.PublicMember.PeerURL, LogURL: assignment.PublicMember.LogURL, PublicKey: publicKey,
	}
	if *config.Learner != wantMember || assignment.MembershipHash != fencing.BootstrapMembersHash(config.Members) {
		return fmt.Errorf("managed learner assignment differs from its immutable source profile")
	}
	durability := string(config.ObjStoreDurability)
	if durability == "" {
		durability = string(types.ObjectStoreDurabilityAsync)
	}
	if assignment.Durability != durability || assignment.StoreIdentityHash != managedStoreIdentityHash(config) {
		return fmt.Errorf("managed learner assignment differs from its object-store profile")
	}
	return nil
}

func managedStoreIdentityHash(config *types.ExecutionConfig) string {
	provider := config.ObjStoreProvider
	if provider == "" {
		provider = "filesystem"
	}
	connectionDigest := ""
	if raw := config.ObjStoreAzureConnectionString; raw != "" {
		encoded, _ := json.Marshal(raw)
		digest := sha256.Sum256(encoded)
		connectionDigest = hex.EncodeToString(digest[:])
	}
	return fencing.BootstrapStoreIdentityHash(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER":              provider,
		"RHIZA_OBJSTORE_ENDPOINT":              config.ObjStoreEndpoint,
		"RHIZA_OBJSTORE_BUCKET":                config.ObjStoreBucket,
		"RHIZA_OBJSTORE_PREFIX":                config.ObjStorePrefix,
		"RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT": config.ObjStoreAzureStorageAccount,
		"azure_connection_string_digest":       connectionDigest,
	})
}
