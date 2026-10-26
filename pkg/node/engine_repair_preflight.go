package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type engineRepairCandidateDescriptor struct {
	Bootstrap   fencing.Identity `json:"bootstrap"`
	OldIdentity voterIdentity    `json:"oldIdentity"`
	Candidate   fencing.Identity `json:"candidate"`
	Member      quepaxa.Member   `json:"member"`
}

// EngineRepairPreflight makes a repeatable, unauthorised proposal only for a
// registered voter whose WAL manifest is genuinely absent.
func EngineRepairPreflight(config *types.ExecutionConfig) fencing.EngineRepairPreflight {
	return func(ctx context.Context, physical fencing.Identity) (fencing.StartupPreflightResult, error) {
		if config == nil || !config.EnableReconfiguration || !config.EngineOwnedRemovals || len(config.Members) < 2 || physical.PodName == "" {
			return fencing.StartupPreflightResult{}, nil
		}
		local, err := loadVoterIdentity(config)
		if local.identity != nil && local.identity.Node != string(config.NodeID) {
			if err != nil {
				return fencing.StartupPreflightResult{}, err
			}
			descriptor, ok, descriptorErr := loadEngineRepairCandidateRestart(config, physical, local)
			if descriptorErr != nil {
				return fencing.StartupPreflightResult{}, descriptorErr
			}
			if !ok {
				return fencing.StartupPreflightResult{}, fmt.Errorf("%w: local voter identity differs from configured source member", ErrVoterStateLost)
			}
			remote, exists, remoteErr := readRemoteVoterIdentity(ctx, config)
			if remoteErr != nil {
				return fencing.StartupPreflightResult{}, remoteErr
			}
			if !exists || remote != descriptor.OldIdentity {
				return fencing.StartupPreflightResult{}, fmt.Errorf("%w: candidate descriptor does not match the immutable source voter registration", ErrVoterStateLost)
			}
			// The authority resolves this authenticated physical Pod against the
			// persisted assignment and creates a fresh RunID continuation. The
			// original descriptor RunID is history, not a restart restriction.
			return fencing.StartupPreflightResult{}, nil
		}
		if errors.Is(err, ErrVoterWALLost) {
			proposal, needed, err := buildLostWALProposal(config, physical, local)
			return fencing.StartupPreflightResult{PendingEngineRepair: proposalPointer(proposal, needed)}, err
		}
		if err != nil {
			return fencing.StartupPreflightResult{}, err
		}
		if local.identity != nil {
			want := *local.identity
			want.Nonce, want.Registered = "", false
			if local.identity.Cluster != string(config.ClusterID) || local.identity.Node != string(config.NodeID) || want != voterIdentityFor(config) {
				return fencing.StartupPreflightResult{}, fmt.Errorf("%w: local voter identity differs from cluster or store configuration", ErrVoterStateLost)
			}
			return fencing.StartupPreflightResult{}, nil
		}
		state, err := readRemoteVoterState(ctx, config)
		if err != nil {
			return fencing.StartupPreflightResult{}, err
		}
		if state.membershipPresent && state.identity == nil {
			if local.hadWAL {
				return fencing.StartupPreflightResult{}, fmt.Errorf("%w: local WAL exists without its immutable voter identity", ErrVoterStateLost)
			}
			pristine, err := initialVoterLocalStateIsPristine(config, local)
			if err != nil {
				return fencing.StartupPreflightResult{}, err
			}
			if !pristine || physical.SourceClusterID != string(config.ClusterID) || physical.NodeID != string(config.NodeID) || physical.PodName == "" || physical.PodUID == "" || physical.RunID == "" {
				return fencing.StartupPreflightResult{}, fmt.Errorf("%w: first-registration local state is not pristine", ErrVoterStateLost)
			}
			return fencing.StartupPreflightResult{InitialRegistration: true}, nil
		}
		if state.identity != nil {
			remote := *state.identity
			assigned, restartErr := loadUnboundEngineRepairCandidateRestart(ctx, config, physical, local, remote)
			if restartErr != nil {
				return fencing.StartupPreflightResult{}, restartErr
			}
			if assigned {
				// The saved descriptor plus absent candidate identity/WAL is the
				// pre-WAL state of an existing assignment, not a new old-voter loss.
				// Let the authority continue that exact candidate with this RunID.
				return fencing.StartupPreflightResult{}, nil
			}
			proposal, needed, err := buildLostWALProposal(config, physical, localVoterState{identity: &remote, hadWAL: local.hadWAL})
			return fencing.StartupPreflightResult{PendingEngineRepair: proposalPointer(proposal, needed)}, err
		}
		return fencing.StartupPreflightResult{}, nil
	}
}

func proposalPointer(proposal fencing.PendingEngineRepairProposal, needed bool) *fencing.PendingEngineRepairProposal {
	if !needed {
		return nil
	}
	return &proposal
}

func initialVoterLocalStateIsPristine(config *types.ExecutionConfig, local localVoterState) (bool, error) {
	if config == nil || local.identity != nil || local.hadWAL {
		return false, nil
	}
	entries, err := os.ReadDir(config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		// Only an empty, real qlog directory is harmless first-start preparation.
		if entry.Name() != "qlog" || !entry.IsDir() {
			return false, nil
		}
		children, err := os.ReadDir(filepath.Join(config.DataDir, entry.Name()))
		if err != nil {
			return false, err
		}
		if len(children) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func loadUnboundEngineRepairCandidateRestart(ctx context.Context, config *types.ExecutionConfig, physical fencing.Identity, local localVoterState, source voterIdentity) (bool, error) {
	if config == nil || local.identity != nil || local.hadWAL {
		return false, nil
	}
	path := filepath.Join(config.DataDir, "qlog", "engine-repair-candidate.json")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return false, fmt.Errorf("fencing: local recovery candidate descriptor is unsafe")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var descriptor engineRepairCandidateDescriptor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&descriptor); err != nil {
		return false, fmt.Errorf("%w: local recovery candidate descriptor is invalid", ErrVoterStateLost)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return false, fmt.Errorf("%w: local recovery candidate descriptor is invalid", ErrVoterStateLost)
	}
	oldMember, found := findSlotMember(config.Members, config.NodeID)
	candidate := descriptor.Candidate
	if !found || physical.SourceClusterID != string(config.ClusterID) || physical.NodeID != string(config.NodeID) ||
		physical.PodName == "" || physical.PodUID == "" || physical.RunID == "" ||
		descriptor.OldIdentity != source || descriptor.Bootstrap.SourceClusterID != string(config.ClusterID) ||
		descriptor.Bootstrap.NodeID != string(config.NodeID) || descriptor.Bootstrap.PodName != physical.PodName ||
		descriptor.Bootstrap.PodUID != physical.PodUID || descriptor.Bootstrap.RunID == "" ||
		candidate.SourceClusterID != string(config.ClusterID) || candidate.NodeID == "" || candidate.NodeID == string(config.NodeID) ||
		candidate.PodName != physical.PodName || candidate.PodUID != physical.PodUID || candidate.RunID != descriptor.Bootstrap.RunID || candidate.AssignmentID != "" ||
		candidate.WALIdentity == "" || descriptor.Member.ID != quepaxa.NodeID(candidate.NodeID) ||
		descriptor.Member.WALIdentity != candidate.WALIdentity ||
		descriptor.Member.PublicKey != quepaxa.PublicKey(network.PeerPublicKey(config.ClusterID, descriptor.Member.ID, config.PeerToken)) {
		return false, fmt.Errorf("%w: pre-WAL candidate descriptor differs from its original slot", ErrVoterStateLost)
	}
	wantMember := oldMember
	wantMember.ID = quepaxa.NodeID(candidate.NodeID)
	wantMember.WALIdentity = candidate.WALIdentity
	wantMember.PublicKey = quepaxa.PublicKey(network.PeerPublicKey(config.ClusterID, wantMember.ID, config.PeerToken))
	if descriptor.Member != wantMember {
		return false, fmt.Errorf("%w: pre-WAL candidate member differs from its original slot", ErrVoterStateLost)
	}
	if physical.RunID == "" {
		return false, fmt.Errorf("fencing: current supervisor RunID is required")
	}
	if _, err := hex.DecodeString(candidate.WALIdentity); err != nil || len(candidate.WALIdentity) != 64 {
		return false, fmt.Errorf("%w: pre-WAL candidate assignment has invalid WAL identity", ErrVoterStateLost)
	}
	// A remote candidate registration with no matching local identity is an
	// identity-loss case, not a pre-WAL retry. Never reissue its assignment.
	provider := objmetrics.Provider(config.ObjStoreProvider)
	if provider == "" {
		provider = objmetrics.ProviderS3
	}
	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
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
		return false, err
	}
	registered, readErr := readVoterRegistration(ctx, bucket, voterIdentityRegistrationKey(config, candidate.NodeID))
	closeErr := bucket.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	if registered != nil {
		return false, fmt.Errorf("%w: assigned candidate identity is remote but absent locally", ErrVoterStateLost)
	}
	return true, nil
}

func loadEngineRepairCandidateRestart(config *types.ExecutionConfig, physical fencing.Identity, local localVoterState) (engineRepairCandidateDescriptor, bool, error) {
	if config == nil || local.identity == nil || !local.identity.Registered || !local.hadWAL {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: assigned candidate identity or WAL is absent", ErrVoterStateLost)
	}
	if physical.SourceClusterID != string(config.ClusterID) || physical.NodeID != string(config.NodeID) ||
		physical.PodName == "" || physical.PodUID == "" || physical.RunID == "" {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("fencing: current supervisor identity is incomplete")
	}
	descriptorPath := filepath.Join(config.DataDir, "qlog", "engine-repair-candidate.json")
	info, err := os.Lstat(descriptorPath)
	if err != nil {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: assigned candidate descriptor is unavailable: %v", ErrVoterStateLost, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("fencing: local recovery candidate descriptor is unsafe")
	}
	data, err := os.ReadFile(descriptorPath)
	if err != nil {
		return engineRepairCandidateDescriptor{}, false, err
	}
	var descriptor engineRepairCandidateDescriptor
	if err := json.Unmarshal(data, &descriptor); err != nil {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: local recovery candidate descriptor is invalid", ErrVoterStateLost)
	}
	oldMember, found := findSlotMember(config.Members, config.NodeID)
	if !found || physical.SourceClusterID != string(config.ClusterID) || physical.NodeID != string(config.NodeID) ||
		descriptor.OldIdentity.Node != string(config.NodeID) || descriptor.OldIdentity.Cluster != string(config.ClusterID) ||
		!descriptor.OldIdentity.Registered || descriptor.Candidate.SourceClusterID != string(config.ClusterID) ||
		descriptor.Candidate.NodeID == string(config.NodeID) || descriptor.Candidate.NodeID == "" ||
		descriptor.Candidate.PodName != physical.PodName || descriptor.Candidate.PodUID != physical.PodUID ||
		descriptor.Candidate.RunID == "" || descriptor.Candidate.WALIdentity == "" || descriptor.Candidate.AssignmentID != "" ||
		descriptor.Bootstrap.SourceClusterID != string(config.ClusterID) || descriptor.Bootstrap.SourceClusterID != physical.SourceClusterID || descriptor.Bootstrap.NodeID != physical.NodeID ||
		descriptor.Bootstrap.PodName != physical.PodName || descriptor.Bootstrap.PodUID != physical.PodUID || descriptor.Bootstrap.RunID == "" ||
		descriptor.Candidate.RunID != descriptor.Bootstrap.RunID ||
		descriptor.Member.ID != quepaxa.NodeID(descriptor.Candidate.NodeID) || descriptor.Member.WALIdentity != descriptor.Candidate.WALIdentity ||
		descriptor.Member.URL != oldMember.URL || descriptor.Member.LogURL != oldMember.LogURL || descriptor.Member.PeerURL != oldMember.PeerURL ||
		descriptor.Member.PublicKey != quepaxa.PublicKey(network.PeerPublicKey(config.ClusterID, descriptor.Member.ID, config.PeerToken)) {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: assigned candidate descriptor differs from its original slot", ErrVoterStateLost)
	}
	assignedConfig := *config
	assignedConfig.NodeID = types.NodeID(descriptor.Candidate.NodeID)
	member := descriptor.Member
	assignedConfig.Learner = &member
	want := voterIdentityFor(&assignedConfig)
	if local.identity.Cluster != string(config.ClusterID) || local.identity.Node != descriptor.Candidate.NodeID ||
		local.identity.Nonce != descriptor.Candidate.WALIdentity || !sameVoterIdentityConfiguration(*local.identity, want) {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: current candidate identity differs from its saved assignment", ErrVoterStateLost)
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: assigned candidate WAL cannot be verified: %v", ErrVoterStateLost, err)
	}
	bound, hasBinding := wal.Identity()
	closeErr := wal.Close()
	wantNonce, decodeErr := hex.DecodeString(descriptor.Candidate.WALIdentity)
	if closeErr != nil || decodeErr != nil || len(wantNonce) != 32 || !hasBinding || !bytes.Equal(bound, wantNonce) {
		return engineRepairCandidateDescriptor{}, false, fmt.Errorf("%w: assigned candidate WAL identity differs", ErrVoterStateLost)
	}
	return descriptor, true, nil
}

func buildLostWALProposal(config *types.ExecutionConfig, physical fencing.Identity, local localVoterState) (fencing.PendingEngineRepairProposal, bool, error) {
	if local.identity == nil || !local.identity.Registered || local.hadWAL || local.identity.Cluster != string(config.ClusterID) || local.identity.Node != string(config.NodeID) {
		return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("%w: lost-WAL preflight does not match the registered voter", ErrVoterStateLost)
	}
	registered := *local.identity
	registered.Nonce, registered.Registered = "", false
	if registered != voterIdentityFor(config) {
		return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("%w: local identity differs from current cluster or store configuration", ErrVoterStateLost)
	}
	ordinal, err := statefulOrdinal(physical.PodName)
	if err != nil {
		return fencing.PendingEngineRepairProposal{}, false, err
	}
	var descriptor engineRepairCandidateDescriptor
	descriptorPath := filepath.Join(config.DataDir, "qlog", "engine-repair-candidate.json")
	data, err := os.ReadFile(descriptorPath)
	if err == nil {
		info, statErr := os.Lstat(descriptorPath)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("fencing: local recovery candidate descriptor is unsafe")
		}
		if err := json.Unmarshal(data, &descriptor); err != nil || descriptor.Bootstrap != physical || descriptor.OldIdentity != *local.identity {
			return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("%w: local recovery candidate descriptor differs", ErrVoterStateLost)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fencing.PendingEngineRepairProposal{}, false, err
	} else {
		member, ok := findSlotMember(config.Members, config.NodeID)
		if !ok {
			return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("%w: configured source member is missing", ErrVoterStateLost)
		}
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			return fencing.PendingEngineRepairProposal{}, false, err
		}
		newID := "recovered-" + hex.EncodeToString(random[:8])
		walIdentity := hex.EncodeToString(random[:])
		member.ID = quepaxa.NodeID(newID)
		member.WALIdentity = walIdentity
		member.PublicKey = quepaxa.PublicKey(network.PeerPublicKey(config.ClusterID, member.ID, config.PeerToken))
		descriptor = engineRepairCandidateDescriptor{Bootstrap: physical, OldIdentity: *local.identity,
			Candidate: fencing.Identity{SourceClusterID: string(config.ClusterID), NodeID: newID, PodName: physical.PodName,
				PodUID: physical.PodUID, RunID: physical.RunID, WALIdentity: walIdentity}, Member: member}
		if err := os.MkdirAll(filepath.Dir(descriptorPath), 0700); err != nil {
			return fencing.PendingEngineRepairProposal{}, false, err
		}
		encoded, _ := json.Marshal(descriptor)
		file, err := os.OpenFile(descriptorPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fencing.PendingEngineRepairProposal{}, false, err
		}
		_, writeErr := file.Write(encoded)
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return fencing.PendingEngineRepairProposal{}, false, err
		}
	}
	if descriptor.Candidate.SourceClusterID != string(config.ClusterID) || descriptor.Candidate.NodeID == string(config.NodeID) ||
		descriptor.Candidate.PodName != physical.PodName || descriptor.Candidate.PodUID != physical.PodUID || descriptor.Candidate.RunID != physical.RunID ||
		descriptor.Candidate.WALIdentity == "" || descriptor.Member.ID != quepaxa.NodeID(descriptor.Candidate.NodeID) ||
		descriptor.Member.WALIdentity != descriptor.Candidate.WALIdentity || descriptor.Member.PublicKey != quepaxa.PublicKey(network.PeerPublicKey(config.ClusterID, descriptor.Member.ID, config.PeerToken)) {
		return fencing.PendingEngineRepairProposal{}, false, fmt.Errorf("%w: candidate descriptor public identity is inconsistent", ErrVoterStateLost)
	}
	identityBytes, err := json.Marshal(*local.identity)
	if err != nil {
		return fencing.PendingEngineRepairProposal{}, false, err
	}
	digest := sha256.Sum256(identityBytes)
	return fencing.PendingEngineRepairProposal{Version: fencing.PendingEngineRepairRequestVersion,
		SourceClusterID: string(config.ClusterID), BootstrapIdentity: physical,
		StableOrdinal: ordinal, OldMemberID: string(config.NodeID), OldWALIdentity: local.identity.Nonce,
		Outcome: fencing.RecoveryBootstrapLostWAL, LocalIdentityHash: hex.EncodeToString(digest[:]),
		CandidateIdentity: descriptor.Candidate, CandidateMember: descriptor.Member}, true, nil
}

type remoteVoterState struct {
	membershipPresent bool
	identity          *voterIdentity
}

func readRemoteVoterState(ctx context.Context, config *types.ExecutionConfig) (remoteVoterState, error) {
	provider := objmetrics.Provider(config.ObjStoreProvider)
	if provider == "" {
		provider = objmetrics.ProviderS3
	}
	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
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
		return remoteVoterState{}, err
	}
	defer bucket.Close()
	genesisKey := voterMembershipRegistrationKey(config)
	genesis, err := readVoterRegistration(ctx, bucket, genesisKey)
	if err != nil {
		return remoteVoterState{}, err
	}
	if genesis == nil {
		return remoteVoterState{}, nil
	}
	var membership recovery.MembershipRecord
	decoder := json.NewDecoder(bytes.NewReader(genesis))
	decoder.DisallowUnknownFields()
	wantMembership := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	if decoder.Decode(&membership) != nil || decoder.Decode(new(any)) != io.EOF || membership.Version != recovery.MembershipRecordVersion ||
		membership.Cluster != string(config.ClusterID) || membership.Membership != wantMembership.Membership || membership.Durability != wantMembership.Durability {
		return remoteVoterState{}, fmt.Errorf("%w: immutable remote membership differs from configuration", ErrVoterStateLost)
	}
	canonical, err := json.Marshal(membership)
	if err != nil || !bytes.Equal(canonical, genesis) {
		return remoteVoterState{}, fmt.Errorf("%w: immutable remote membership is not canonical", ErrVoterStateLost)
	}
	state := remoteVoterState{membershipPresent: true}
	key := voterIdentityRegistrationKey(config, string(config.NodeID))
	data, err := readVoterRegistration(ctx, bucket, key)
	if err != nil {
		return remoteVoterState{}, err
	}
	if data == nil {
		return state, nil
	}
	var identity voterIdentity
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&identity) != nil || decoder.Decode(new(any)) != io.EOF || identity.Version != voterIdentityVersion ||
		identity.Cluster != string(config.ClusterID) || identity.Node != string(config.NodeID) || !identity.Registered {
		return remoteVoterState{}, fmt.Errorf("%w: immutable remote voter identity is invalid", ErrVoterStateLost)
	}
	if _, err := hex.DecodeString(identity.Nonce); err != nil || len(identity.Nonce) != 64 {
		return remoteVoterState{}, fmt.Errorf("%w: immutable remote WAL identity is invalid", ErrVoterStateLost)
	}
	want := voterIdentityFor(config)
	want.Nonce, want.Registered = identity.Nonce, true
	if want != identity {
		return remoteVoterState{}, fmt.Errorf("%w: immutable remote voter identity differs from configuration", ErrVoterStateLost)
	}
	state.identity = &identity
	return state, nil
}

func readRemoteVoterIdentity(ctx context.Context, config *types.ExecutionConfig) (voterIdentity, bool, error) {
	state, err := readRemoteVoterState(ctx, config)
	if err != nil {
		return voterIdentity{}, false, err
	}
	if !state.membershipPresent {
		return voterIdentity{}, false, nil
	}
	if state.identity == nil {
		return voterIdentity{}, false, fmt.Errorf("%w: remote membership exists without this voter's immutable registration", ErrVoterStateLost)
	}
	return *state.identity, true, nil
}

func statefulOrdinal(podName string) (int, error) {
	separator := strings.LastIndexByte(podName, '-')
	if separator <= 0 {
		return 0, fmt.Errorf("fencing: Pod name has no StatefulSet ordinal")
	}
	ordinal, err := strconv.Atoi(podName[separator+1:])
	if err != nil || ordinal < 0 {
		return 0, fmt.Errorf("fencing: invalid StatefulSet ordinal")
	}
	return ordinal, nil
}

func findSlotMember(members []types.NodeConfig, id types.NodeID) (quepaxa.Member, bool) {
	for _, member := range members {
		if member.ID == id {
			return quepaxa.Member(member), true
		}
	}
	return quepaxa.Member{}, false
}
