package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// ArchiveEmptyRecoveryRequest names the persisted recovery resource and binds
// the exact policy snapshot. All source evidence and archive scopes are read
// and validated by the authority, never supplied as caller assertions.
type ArchiveEmptyRecoveryRequest struct {
	RecoveryName           string                            `json:"recoveryName"`
	StatefulSetUID         string                            `json:"statefulSetUID"`
	TargetClusterID        string                            `json:"targetClusterID"`
	OperationID            string                            `json:"operationID"`
	SourceFenceOperationID string                            `json:"sourceFenceOperationID"`
	TargetMembershipHash   string                            `json:"targetMembershipHash"`
	TargetDurability       string                            `json:"targetDurability"`
	AllowDataLoss          bool                              `json:"allowDataLoss"`
	PolicyDigest           string                            `json:"policyDigest"`
	RecoveryMode           string                            `json:"recoveryMode,omitempty"`
	BucketEnvironment      *recovery.FrozenBucketEnvironment `json:"bucketEnvironment,omitempty"`
}

type ArchiveEmptyRecoveryResponse struct {
	State         string                                    `json:"state"`
	Authorization fencing.ArchiveEmptyRecoveryAuthorization `json:"authorization"`
}

func (r ArchiveEmptyRecoveryRequest) validate() error {
	if r.RecoveryName == "" || r.StatefulSetUID == "" || r.TargetClusterID == "" || r.OperationID == "" ||
		r.SourceFenceOperationID == "" || !validEvidenceDigest(r.TargetMembershipHash) || !validEvidenceDigest(r.PolicyDigest) ||
		(r.TargetDurability != "async" && r.TargetDurability != "before-ack") || r.OperationID == r.SourceFenceOperationID ||
		len(r.RecoveryName) > 253 || len(r.StatefulSetUID) > 128 || len(r.TargetClusterID) > 128 || len(r.OperationID) > 256 || len(r.SourceFenceOperationID) > 256 {
		return fmt.Errorf("fencing: empty recovery request identity or policy is invalid")
	}
	if (r.RecoveryMode == RecoveryModeFrozenSource) != (r.BucketEnvironment != nil) || (r.RecoveryMode != "" && r.RecoveryMode != RecoveryModeFrozenSource) {
		return fmt.Errorf("fencing: empty recovery mode and bucket environment differ")
	}
	return nil
}

func (e *FencingExecutor) RegisterArchiveEmptyRecovery(ctx context.Context, request ArchiveEmptyRecoveryRequest) (ArchiveEmptyRecoveryResponse, error) {
	if err := request.validate(); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	return e.registerArchiveEmptyRecovery(ctx, request)
}

func (e *FencingExecutor) registerArchiveEmptyRecovery(ctx context.Context, request ArchiveEmptyRecoveryRequest) (ArchiveEmptyRecoveryResponse, error) {
	if e == nil || ctx == nil || e.Kube == nil || e.AuthorityState == nil || e.ArchiveBucket == nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: canonical empty recovery authority is unavailable")
	}
	if err := request.validate(); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	var stored Resource
	if err := e.Kube.Get(ctx, e.controller().resourcePath(request.RecoveryName), &stored); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	uid := str(stored.Metadata["uid"])
	if uid == "" || uid+":"+stored.Spec.RecoveryID != request.OperationID || stored.Spec.RecoveryID != request.SourceFenceOperationID ||
		request.StatefulSetUID != str(stored.Status.StatefulSetUID) ||
		request.TargetDurability != stored.Spec.Durability || request.AllowDataLoss != stored.Spec.AllowDataLoss || request.PolicyDigest != hashJSON(stored.Spec) ||
		stored.Status.TargetBaseKind != "empty" || stored.Status.RecoveredTip != 0 || stored.Status.ManifestHash != "" || stored.Status.RecoveryMode != request.RecoveryMode {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery request differs from persisted recovery resource")
	}

	sts, err := e.archiveForkStatefulSet(ctx, request.StatefulSetUID)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	if len(containers) == 0 {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: source workload container is unavailable")
	}
	env, err := e.controller().environment(ctx, asObject(containers[0]))
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	sourceID := strings.TrimSpace(env["RHIZA_CLUSTER_ID"])
	if request.RecoveryMode == RecoveryModeFrozenSource {
		sourceID = strings.TrimSpace(stored.Spec.SourceClusterID)
	}
	identity := StoreIdentity(env)
	basePrefix := strings.Trim(identity["RHIZA_OBJSTORE_PREFIX"], "/")
	provider := strings.ToLower(strings.TrimSpace(identity["RHIZA_OBJSTORE_PROVIDER"]))
	bucket := strings.TrimSpace(identity["RHIZA_OBJSTORE_BUCKET"])
	sourcePrefix, targetPrefix := path.Join(basePrefix, sourceID), path.Join(basePrefix, request.TargetClusterID)
	if basePrefix == "." || provider != "gcs" || bucket == "" || targetPrefix == "." {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery requires the configured independent source archive")
	}
	targetScope := fencing.ArchiveScope{Provider: provider, Bucket: bucket, Prefix: targetPrefix, ClusterID: request.TargetClusterID}
	if err := targetScope.Validate(); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	claimScope := fencing.ArchiveScope{Provider: provider, Bucket: bucket,
		Prefix: path.Join(basePrefix, stored.Spec.SourceClusterID), ClusterID: stored.Spec.SourceClusterID}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		frozenIdentity, identityErr := e.controller().frozenSourceIdentity(&stored, sourceState{prefix: sourcePrefix, env: env})
		expectedTarget, targetErr := recovery.FrozenTargetID(frozenIdentity)
		if identityErr != nil || targetErr != nil || request.TargetClusterID != expectedTarget || stored.Status.Target != expectedTarget ||
			stored.Status.FrozenBucketEnvironment == nil || hashJSON(stored.Status.FrozenBucketEnvironment) != hashJSON(request.BucketEnvironment) {
			return ArchiveEmptyRecoveryResponse{}, errors.Join(identityErr, targetErr, fmt.Errorf("fencing: frozen empty identity or bucket environment differs from persisted mode"))
		}
		if err := e.verifyFrozenBucketEnvironment(ctx, bucket, sourcePrefix, *request.BucketEnvironment); err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
	}
	// Once the target has committed its immutable genesis, the workload may
	// already advertise the successor cluster ID and membership. Rejoin through
	// the persisted authorization/ref/receipt, not by reinterpreting the live
	// target configuration as the retired source.
	if response, handled, err := e.readExistingEmptyRecovery(ctx, request, stored, basePrefix, targetScope); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	} else if handled {
		mode := request.RecoveryMode
		reservationDigest := ""
		if mode == RecoveryModeFrozenSource {
			reserved, err := e.ArchiveBucket.Exists(ctx, path.Join(sourcePrefix, "recovery/successor.json"))
			if err != nil || reserved {
				return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: frozen-source retry cannot join a legacy successor reservation"))
			}
		} else {
			mode = sourceSuccessorModeLegacy
			reservationDigest, err = readLegacySuccessorReservation(ctx, e.ArchiveBucket, sourcePrefix, archiveForkReservation{
				Version: 1, OperationID: request.OperationID, Target: request.TargetClusterID, Source: sourceID,
				SourceMembership: stored.Status.SourceMembership, StatefulSetUID: request.StatefulSetUID,
				SpecHash: stored.Status.SpecHash, TargetBaseKind: "empty",
			})
			if err != nil {
				return ArchiveEmptyRecoveryResponse{}, err
			}
		}
		if err := e.verifySourceSuccessorClaim(ctx, claimScope, request.StatefulSetUID, uid, request.OperationID,
			request.SourceFenceOperationID, request.TargetClusterID, "empty", mode, reservationDigest); err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
		return response, nil
	}
	if sourceID == "" || sourceID != stored.Spec.SourceClusterID || sourceID == request.TargetClusterID || sourcePrefix == targetPrefix {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery source and target identities differ")
	}
	sourceDurability := env["RHIZA_OBJSTORE_DURABILITY"]
	if sourceDurability == "" {
		sourceDurability = "async"
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if sourceDurability != "async" || !request.AllowDataLoss {
			return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: guarded empty-source recovery requires explicit async allowDataLoss")
		}
	} else if request.RecoveryMode != "" || request.TargetClusterID != "rhiza-r-"+shortID(request.OperationID) {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: unsupported empty recovery mode or target")
	}

	sourceState, _, err := e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, sourceID)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	if err := e.validateMutationJournalProfile(sourceState); err != nil || !sourceState.Ledger.RequireMutationJournal || sourceState.Ledger.ClosedBy != request.SourceFenceOperationID ||
		sourceState.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || sourceState.Ledger.ArchiveGenesisRef == nil ||
		sourceState.Ledger.ArchiveGenesisRef.Mutation == nil {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	fence, sourceHeadVersion, sourceHead, err := e.verifyEmptyRecoverySource(ctx, request, sts, sourceID, sourcePrefix, sourceState, env)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	// Verify the registered source membership independently from the requested
	// target membership; the policy digest binds the persisted CR spec exactly.
	sourceVoters, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	if sourceDurability != "async" && sourceDurability != "before-ack" || (sourceDurability == "async" && !request.AllowDataLoss) {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: source durability policy does not authorize empty recovery")
	}
	sourceMembership, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(sourcePrefix, "voters/membership.json"), 64<<10)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	var sourceRecord recovery.MembershipRecord
	if err := decodeCanonicalArchiveJSON(sourceMembership, &sourceRecord); err != nil || sourceRecord != recovery.NewMembershipRecord(sourceID, sourceVoters, sourceDurability) {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: canonical source membership differs from enrolled workload"))
	}
	claimMode := request.RecoveryMode
	if claimMode == "" {
		claimMode = sourceSuccessorModeLegacy
	}
	if request.RecoveryMode == RecoveryModeFrozenSource && strings.TrimSpace(env["RHIZA_CLUSTER_ID"]) != stored.Spec.SourceClusterID {
		// A target-side retry is safe only if the original source-generation
		// winner already exists and is byte-for-byte the same immutable claim.
		if strings.TrimSpace(env["RHIZA_CLUSTER_ID"]) != request.TargetClusterID {
			return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: frozen empty recovery runtime is neither its source nor target identity")
		}
		expected, err := e.sourceSuccessorClaim(ctx, claimScope, request.StatefulSetUID, uid, request.OperationID,
			request.SourceFenceOperationID, request.TargetClusterID, "empty", RecoveryModeFrozenSource, "")
		if err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
		winner, err := e.AuthorityState.readSourceSuccessorClaim(ctx, claimScope, request.StatefulSetUID)
		if err != nil || winner != expected {
			return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: target-side retry lacks the original frozen source successor claim"))
		}
	}
	reservationDigest := ""
	if request.RecoveryMode == RecoveryModeFrozenSource {
		reserved, err := e.ArchiveBucket.Exists(ctx, path.Join(sourcePrefix, "recovery/successor.json"))
		if err != nil || reserved {
			return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: frozen-source registration cannot join a legacy successor reservation"))
		}
	} else {
		reservationDigest, err = readLegacySuccessorReservation(ctx, e.ArchiveBucket, sourcePrefix, archiveForkReservation{
			Version: 1, OperationID: request.OperationID, Target: request.TargetClusterID, Source: sourceID,
			SourceMembership: sourceFingerprint(sourceID, sourceDurability, sourceVoters, sourcePrefix),
			StatefulSetUID:   request.StatefulSetUID, SpecHash: immutableSpecHash(stored.Spec), TargetBaseKind: "empty",
		})
		if err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
	}
	if err := e.claimSourceSuccessor(ctx, claimScope, request.StatefulSetUID, uid, request.OperationID, request.SourceFenceOperationID,
		request.TargetClusterID, "empty", claimMode, reservationDigest); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	targetMembership, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(targetPrefix, "voters/membership.json"), 64<<10)
	if err != nil || authorityStateDigest(targetMembership) != request.TargetMembershipHash {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: target membership differs from persisted recovery policy"))
	}
	var targetRecord recovery.MembershipRecord
	if err := decodeCanonicalArchiveJSON(targetMembership, &targetRecord); err != nil || targetRecord.Cluster != request.TargetClusterID || targetRecord.Durability != request.TargetDurability {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fmt.Errorf("fencing: target membership identity differs"))
	}
	authorization, err := e.makeEmptyRecoveryAuthorization(request, sourceID, sourcePrefix, targetScope, sourceState, sourceRecord, sourceDurability, fence, sourceHeadVersion, sourceHead)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	if response, found, err := e.readCommittedEmptyRecovery(ctx, request.StatefulSetUID, request.TargetClusterID, targetScope, authorization); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	} else if found {
		if request.RecoveryMode == RecoveryModeFrozenSource {
			if err := e.verifyFrozenBucketEnvironment(ctx, bucket, sourcePrefix, *request.BucketEnvironment); err != nil {
				return ArchiveEmptyRecoveryResponse{}, err
			}
		}
		return response, nil
	}
	if err := recovery.RequireEmptyArchiveScope(ctx, e.ArchiveBucket, targetPrefix); err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: target archive scope is not pristine: %w", err)
	}
	if err := e.persistEmptyRecoveryAuthorization(ctx, request.StatefulSetUID, request.TargetClusterID, targetScope, authorization); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	response, err := e.emptyRecoveryResponse(ctx, request.StatefulSetUID, request.TargetClusterID, targetScope, authorization)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	return response, nil
}

func (e *FencingExecutor) readExistingEmptyRecovery(ctx context.Context, request ArchiveEmptyRecoveryRequest, stored Resource, basePrefix string, targetScope fencing.ArchiveScope) (ArchiveEmptyRecoveryResponse, bool, error) {
	claim, _, claimErr := e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	root, _, rootErr := e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, request.TargetClusterID)
	if errors.Is(claimErr, errAuthorityStateNotFound) && errors.Is(rootErr, errAuthorityStateNotFound) {
		return ArchiveEmptyRecoveryResponse{}, false, nil
	}
	if claimErr != nil || rootErr != nil {
		return ArchiveEmptyRecoveryResponse{}, true, errors.Join(claimErr, rootErr, fencing.ErrPending)
	}
	claimAuth, rootAuth := claim.EmptyRecoveryAuthorization, root.Ledger.EmptyRecoveryAuthorization
	if claimAuth == nil && rootAuth == nil {
		if claim.GenesisRef != nil || root.Ledger.ArchiveGenesisRef != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisEmptyRecoveryPending {
			return ArchiveEmptyRecoveryResponse{}, true, fmt.Errorf("fencing: target scope has unrelated or incomplete genesis state")
		}
		return ArchiveEmptyRecoveryResponse{}, false, nil
	}
	if claimAuth == nil || rootAuth == nil || hashJSON(claimAuth) != hashJSON(rootAuth) {
		return ArchiveEmptyRecoveryResponse{}, true, fencing.ErrPending
	}
	authorization := *claimAuth
	sourceScope := fencing.ArchiveScope{Provider: targetScope.Provider, Bucket: targetScope.Bucket,
		Prefix: path.Join(basePrefix, stored.Spec.SourceClusterID), ClusterID: stored.Spec.SourceClusterID}
	sourceScopeHash, sourceErr := sourceScope.Hash()
	targetScopeHash, targetErr := targetScope.Hash()
	if err := authorization.Validate(); err != nil || sourceErr != nil || targetErr != nil ||
		authorization.StatefulSetUID != request.StatefulSetUID || authorization.TargetClusterID != request.TargetClusterID ||
		authorization.OperationID != request.OperationID || authorization.SourceFenceOperationID != request.SourceFenceOperationID ||
		authorization.SourceScopeHash != sourceScopeHash || authorization.TargetScopeHash != targetScopeHash ||
		authorization.TargetMembershipHash != request.TargetMembershipHash || authorization.TargetDurability != request.TargetDurability ||
		authorization.AllowDataLoss != request.AllowDataLoss || authorization.PolicyDigest != hashJSON(stored.Spec) ||
		authorization.RecoveryMode != request.RecoveryMode || authorization.SourcePrefix != sourceScope.Prefix || authorization.BucketMetageneration != frozenBucketMetageneration(request.BucketEnvironment) || authorization.LifecyclePolicyDigest != frozenBucketLifecycleDigest(request.BucketEnvironment) ||
		stored.Spec.RecoveryID != request.SourceFenceOperationID || stored.Status.StatefulSetUID != request.StatefulSetUID {
		return ArchiveEmptyRecoveryResponse{}, true, errors.Join(err, sourceErr, targetErr, fmt.Errorf("fencing: persisted empty recovery authorization differs from request"))
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if stored.Status.FrozenBucketEnvironment == nil || hashJSON(stored.Status.FrozenBucketEnvironment) != hashJSON(request.BucketEnvironment) {
			return ArchiveEmptyRecoveryResponse{}, true, fmt.Errorf("fencing: persisted frozen environment differs from the recovery request")
		}
		if err := e.verifyFrozenBucketEnvironment(ctx, targetScope.Bucket, sourceScope.Prefix, *request.BucketEnvironment); err != nil {
			return ArchiveEmptyRecoveryResponse{}, true, err
		}
	}
	if response, found, err := e.readCommittedEmptyRecovery(ctx, request.StatefulSetUID, request.TargetClusterID, targetScope, authorization); err != nil {
		return ArchiveEmptyRecoveryResponse{}, true, err
	} else if found {
		return response, true, nil
	}
	response, err := e.emptyRecoveryResponse(ctx, request.StatefulSetUID, request.TargetClusterID, targetScope, authorization)
	return response, true, err
}

func (e *FencingExecutor) handleArchiveEmptyRecovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/archive-genesis/empty-recovery/register" || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}
	var request ArchiveEmptyRecoveryRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || r.Header.Get("Idempotency-Key") != request.OperationID {
		http.Error(w, "invalid empty recovery registration", http.StatusBadRequest)
		return
	}
	response, err := e.registerArchiveEmptyRecovery(r.Context(), request)
	if err != nil {
		if errors.Is(err, fencing.ErrPending) || transientFenceAuthority(err) {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		http.Error(w, "empty recovery registration rejected", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (e *FencingExecutor) verifyEmptyRecoverySource(ctx context.Context, request ArchiveEmptyRecoveryRequest, sts object, sourceID, sourcePrefix string, state fenceLedger, env map[string]string) (fenceResource, objstore.ObjectVersion, []byte, error) {
	if !state.Ledger.RequireMutationJournal || state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || state.Ledger.ArchiveGenesisRef == nil || state.Ledger.ArchiveGenesisRef.Mutation == nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fencing.ErrPending
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	forkRequest := ArchiveForkGenesisRequest{StatefulSetUID: request.StatefulSetUID, TargetClusterID: request.TargetClusterID, OperationID: request.OperationID, SourceFenceOperationID: request.SourceFenceOperationID}
	fence, err := e.readCanonicalSourceFence(ctx, forkRequest, sourceID, bindingUID, state)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	if fence.Spec.Scope != ScopeGeneration || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: source lacks terminal independent generation fence")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	if err := validateIndependentV2(fence.Spec, fence.Status.Proof.IndependentV2); err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	// Bind the terminal proof to the exact canonical closed root snapshot. The
	// root's immutable closed operation/hash and archive genesis are checked
	// again below; Kubernetes metadata is outside this root digest.
	if err := verifyEmptyRecoveryClosedRootBinding(e, state, fence.Status.Proof.IndependentV2.LedgerHash); err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	requestHash, err := requestHash(&fence.Spec)
	if err != nil || state.Ledger.ClosedBy != request.SourceFenceOperationID || state.Ledger.ClosedHash != requestHash {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: source root and persisted fence request differ")
	}
	storeScope, err := e.storageScope(ctx, sts, sourceID)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	if fence.Status.Proof.IndependentV2.StorageScope != storeScope {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: source proof names a different ObjectStorage scope")
	}
	if err := verifyIndependentProofCoverage(state, fence.Spec, fence.Status.Proof.IndependentV2); err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	currentUIDs, err := e.currentInventory(ctx, request.StatefulSetUID, nil)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	sort.Strings(currentUIDs)
	proofUIDs := append([]string(nil), fence.Status.Proof.IndependentV2.CurrentPodUIDs...)
	sort.Strings(proofUIDs)
	if !sameStrings(currentUIDs, proofUIDs) {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: source Pod inventory changed since the terminal proof")
	}
	claimScope := fencing.ArchiveScope{Provider: strings.ToLower(env["RHIZA_OBJSTORE_PROVIDER"]), Bucket: env["RHIZA_OBJSTORE_BUCKET"], Prefix: sourcePrefix, ClusterID: sourceID}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, claimScope)
	if err != nil || claim.GenesisRef == nil || sameArchiveGenesisRef(claim.GenesisRef, state.Ledger.ArchiveGenesisRef) == false {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fmt.Errorf("fencing: source empty genesis claim differs from its root"))
	}
	m := state.Ledger.ArchiveGenesisRef.Mutation
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil || authorityStateDigest(emptyHead) != m.Intent.PayloadDigest {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fmt.Errorf("fencing: source genesis ref is not the canonical unsealed empty HEAD"))
	}
	runtime, run, err := e.archiveGenesisProducerRun(ctx, state, m.ProducerUID)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	store, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, store, run.MutationProfile)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	if err := e.ExecuteSourceMutation(ctx, runtime, m.ProducerUID, m.Sequence, m.Intent, store, provider, receipts); err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fencing.ErrPending)
	}
	lookup, err := e.lookupMutation(ctx, request.StatefulSetUID, run.Identity, fencing.MutationLookupRequest{ProducerUID: m.ProducerUID, Sequence: m.Sequence, Intent: m.Intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != m.Intent || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationCommitted {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fencing.ErrPending)
	}
	manager := recovery.NewManager(e.ArchiveBucket, sourcePrefix, recovery.ArchiveConfigID)
	defer manager.Close()
	if err := manager.Load(ctx); err != nil || manager.Tip() != 0 {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fencing.ErrPending)
	}
	version, ok := manager.HeadVersion()
	if !ok {
		return fenceResource{}, objstore.ObjectVersion{}, nil, fencing.ErrPending
	}
	headKey := path.Join(sourcePrefix, "archive/head.bin")
	data, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, headKey, 64<<10)
	if err != nil {
		return fenceResource{}, objstore.ObjectVersion{}, nil, err
	}
	attrs, err := e.ArchiveBucket.Attributes(ctx, headKey)
	if err != nil || attrs.Version == nil || *attrs.Version != version {
		return fenceResource{}, objstore.ObjectVersion{}, nil, errors.Join(err, fencing.ErrPending)
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if lookup.Entry.GuardedLogicalResolution == nil || lookup.Entry.Resolution.AllCopiesTerminal || lookup.Entry.Preparation == nil ||
			!fencing.IsHeldCreateEligible(m.Intent, *lookup.Entry.Preparation) || m.Intent.Key != "archive/head.bin" ||
			m.Intent.Condition != (fencing.MutationCondition{Mode: fencing.MutationCreateOnly}) ||
			m.Intent.PayloadDigest != authorityStateDigest(emptyHead) {
			return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: source empty HEAD lacks the exact guarded held-create resolution")
		}
		if manager.Sealed() || !bytes.Equal(data, emptyHead) {
			return fenceResource{}, objstore.ObjectVersion{}, nil, fmt.Errorf("fencing: guarded empty source HEAD is not canonical and unsealed")
		}
		if err := e.verifyGuardedLogicalResolution(ctx, runtime, run, m.ProducerUID, m.Sequence, *lookup.Entry, provider); err != nil {
			return fenceResource{}, objstore.ObjectVersion{}, nil, err
		}
		if err := verifyFrozenEmptyArchiveNamespace(ctx, e.ArchiveBucket, sourcePrefix); err != nil {
			return fenceResource{}, objstore.ObjectVersion{}, nil, err
		}
	} else {
		if !lookup.Entry.Resolution.AllCopiesTerminal || !manager.Sealed() {
			return fenceResource{}, objstore.ObjectVersion{}, nil, fencing.ErrPending
		}
		if err := e.verifyEmptyRecoverySeal(ctx, sourcePrefix, request.SourceFenceOperationID); err != nil {
			return fenceResource{}, objstore.ObjectVersion{}, nil, err
		}
	}
	return fence, version, data, nil
}

func verifyFrozenEmptyArchiveNamespace(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	headKey := path.Join(prefix, "archive/head.bin")
	if err := bucket.Iter(ctx, prefix, func(key string) error {
		if strings.HasPrefix(key, path.Join(prefix, "archive")+"/") && key != headKey {
			return fmt.Errorf("fencing: guarded empty source contains additional archive history")
		}
		if strings.HasPrefix(key, path.Join(prefix, "checkpoint")+"/") {
			return fmt.Errorf("fencing: guarded empty source contains checkpoint history")
		}
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		return fmt.Errorf("fencing: cannot enumerate guarded empty source history: %w", err)
	}
	return nil
}

// verifyEmptyRecoveryClosedRootBinding keeps two checks distinct: the V2
// evidence must describe this exact root snapshot, while the authorization
// digest names only the immutable closed-root identity. Object metadata and
// unrelated mutable ledger bookkeeping are not treated as root identity.
func verifyEmptyRecoveryClosedRootBinding(e *FencingExecutor, state fenceLedger, proofLedgerHash string) error {
	if e == nil || state.StatefulSetUID == "" || state.Ledger.SourceClusterID == "" ||
		state.Ledger.ClosedBy == "" || state.Ledger.ClosedHash == "" || !state.Ledger.RequireMutationJournal ||
		state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || state.Ledger.ArchiveGenesisRef == nil ||
		proofLedgerHash == "" || proofLedgerHash != e.ledgerHash(state) {
		return fmt.Errorf("fencing: source independent proof is not bound to the canonical closed root")
	}
	return nil
}

func (e *FencingExecutor) verifyEmptyRecoverySeal(ctx context.Context, prefix, operationID string) error {
	data, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(prefix, "archive/seal.json"), 64<<10)
	if err != nil {
		return errors.Join(err, fencing.ErrPending)
	}
	var seal struct {
		Version     int    `json:"version"`
		OperationID string `json:"operation_id"`
	}
	if err := decodeCanonicalArchiveJSON(data, &seal); err != nil || seal.Version != 1 || seal.OperationID != operationID {
		return errors.Join(err, fmt.Errorf("fencing: source empty archive seal differs from recovery operation"))
	}
	return nil
}

func (e *FencingExecutor) makeEmptyRecoveryAuthorization(request ArchiveEmptyRecoveryRequest, sourceID, sourcePrefix string, targetScope fencing.ArchiveScope, sourceState fenceLedger, sourceMembership recovery.MembershipRecord, sourceDurability string, fence fenceResource, version objstore.ObjectVersion, sealedHead []byte) (fencing.ArchiveEmptyRecoveryAuthorization, error) {
	sourceScope := fencing.ArchiveScope{Provider: targetScope.Provider, Bucket: targetScope.Bucket, Prefix: sourcePrefix, ClusterID: sourceID}
	sourceHash, err := sourceScope.Hash()
	if err != nil {
		return fencing.ArchiveEmptyRecoveryAuthorization{}, err
	}
	targetHash, err := targetScope.Hash()
	if err != nil {
		return fencing.ArchiveEmptyRecoveryAuthorization{}, err
	}
	targetHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		return fencing.ArchiveEmptyRecoveryAuthorization{}, err
	}
	auth := fencing.ArchiveEmptyRecoveryAuthorization{
		Version: fencing.ArchiveGenesisVersion, StatefulSetUID: request.StatefulSetUID, TargetClusterID: request.TargetClusterID,
		OperationID: request.OperationID, SourceFenceOperationID: request.SourceFenceOperationID, ConfigID: recovery.ArchiveConfigID,
		SourceTip: 0, SourceScopeHash: sourceHash, TargetScopeHash: targetHash,
		SourceGenesisRefDigest: authorityStateDigest(jsonBytes(sourceState.Ledger.ArchiveGenesisRef)), SourceHeadDigest: authorityStateDigest(sealedHead),
		SourceHeadVersion: version, TargetEmptyHeadDigest: authorityStateDigest(targetHead),
		SourceRootDigest: emptyRecoveryClosedRootDigest(e.Kube.Namespace, sourceState), SourceFenceDigest: stableSourceFenceEvidenceDigest(fence),
		SourceMembershipHash: authorityStateDigest(jsonBytes(sourceMembership)), SourceDurability: sourceDurability,
		TargetMembershipHash: request.TargetMembershipHash, TargetDurability: request.TargetDurability,
		AllowDataLoss: request.AllowDataLoss, PolicyDigest: request.PolicyDigest,
		RecoveryMode:          request.RecoveryMode,
		SourcePrefix:          sourcePrefix,
		BucketName:            targetScope.Bucket,
		BucketMetageneration:  frozenBucketMetageneration(request.BucketEnvironment),
		LifecyclePolicyDigest: frozenBucketLifecycleDigest(request.BucketEnvironment),
	}
	if err := auth.Validate(); err != nil {
		return fencing.ArchiveEmptyRecoveryAuthorization{}, err
	}
	return auth, nil
}

func emptyRecoveryClosedRootDigest(namespace string, state fenceLedger) string {
	return hashJSON([]any{"rhiza-empty-recovery-closed-root-v1", namespace, state.StatefulSetUID,
		state.Ledger.SourceClusterID, state.Ledger.ClosedBy, state.Ledger.ClosedHash,
		state.Ledger.RequireMutationJournal, state.Ledger.ArchiveGenesisState,
		state.Ledger.ArchiveScopeHash, state.Ledger.ArchiveGenesisRef})
}

func stableSourceFenceEvidenceDigest(fence fenceResource) string {
	return hashJSON([]any{"rhiza-empty-recovery-source-fence-v1", str(fence.Metadata["uid"]), fence.Spec, fence.Status.Proof})
}

func (e *FencingExecutor) persistEmptyRecoveryAuthorization(ctx context.Context, uid, targetID string, scope fencing.ArchiveScope, authorization fencing.ArchiveEmptyRecoveryAuthorization) error {
	if err := e.prepareEmptyRecoveryRoot(ctx, uid, targetID, scope); err != nil {
		return err
	}
	registration := hashJSON([]any{"rhiza-empty-recovery-registration-v1", authorization})
	claim, err := e.AuthorityState.ClaimArchiveScope(ctx, scope, authorization.ConfigID, uid, targetID, registration)
	if err != nil {
		return err
	}
	if claim.GenesisRef != nil && claim.GenesisRef.Mutation != nil && claim.GenesisRef.Mutation.Intent.PayloadDigest != authorization.TargetEmptyHeadDigest {
		return fmt.Errorf("fencing: target genesis mutation is not the authorized empty HEAD")
	}
	if err := e.AuthorityState.SetArchiveScopeEmptyRecoveryAuthorization(ctx, scope, authorization); err != nil {
		return err
	}
	return e.AuthorityState.SetRootEmptyRecoveryAuthorization(ctx, uid, targetID, authorization)
}

func (e *FencingExecutor) prepareEmptyRecoveryRoot(ctx context.Context, uid, targetID string, scope fencing.ArchiveScope) error {
	hash, err := scope.Hash()
	if err != nil {
		return err
	}
	if err := e.initializeRoot(ctx, uid, targetID); err != nil {
		return err
	}
	for range 8 {
		state, version, err := e.AuthorityState.ReadRoot(ctx, uid, targetID)
		if err != nil {
			return err
		}
		if !state.Ledger.RequireMutationJournal {
			return fmt.Errorf("fencing: empty recovery target root is not independent")
		}
		switch state.Ledger.ArchiveGenesisState {
		case fencing.ArchiveGenesisRegistrationPending:
			state.Ledger.ArchiveGenesisState = fencing.ArchiveGenesisEmptyRecoveryPending
			state.Ledger.ArchiveScopeHash = hash
		case fencing.ArchiveGenesisEmptyRecoveryPending:
			if state.Ledger.ArchiveScopeHash != hash || state.Ledger.EmptyRecoveryAuthorization != nil || state.Ledger.ArchiveGenesisRef != nil {
				return fmt.Errorf("fencing: empty recovery target root reservation differs")
			}
			return nil
		case fencing.ArchiveGenesisAwaitingRef, fencing.ArchiveGenesisCommitted:
			if state.Ledger.ArchiveScopeHash != hash || state.Ledger.EmptyRecoveryAuthorization == nil {
				return fmt.Errorf("fencing: target scope has no matching empty recovery authorization")
			}
			return nil
		default:
			return fmt.Errorf("fencing: empty recovery target root genesis state is unavailable")
		}
		if err := e.AuthorityState.WriteRoot(ctx, uid, targetID, state, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			confirmed, _, readErr := e.AuthorityState.ReadRoot(ctx, uid, targetID)
			if readErr == nil && confirmed.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisEmptyRecoveryPending && confirmed.Ledger.ArchiveScopeHash == hash {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fencing.ErrUnavailable
}

func (e *FencingExecutor) emptyRecoveryResponse(ctx context.Context, uid, targetID string, scope fencing.ArchiveScope, authorization fencing.ArchiveEmptyRecoveryAuthorization) (ArchiveEmptyRecoveryResponse, error) {
	if authorization.RecoveryMode == RecoveryModeFrozenSource {
		if err := e.verifyFrozenBucketEnvironment(ctx, authorization.BucketName, authorization.SourcePrefix, recovery.FrozenBucketEnvironment{Metageneration: authorization.BucketMetageneration, LifecycleDigest: authorization.LifecyclePolicyDigest}); err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.EmptyRecoveryAuthorization == nil || hashJSON(claim.EmptyRecoveryAuthorization) != hashJSON(&authorization) {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, uid, targetID)
	if err != nil || root.Ledger.EmptyRecoveryAuthorization == nil || hashJSON(root.Ledger.EmptyRecoveryAuthorization) != hashJSON(&authorization) {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	response := ArchiveEmptyRecoveryResponse{State: fencing.EmptyRecoveryAuthorized, Authorization: authorization}
	if root.Ledger.ArchiveGenesisRef == nil {
		return response, nil
	}
	if root.Ledger.ArchiveGenesisRef.Mutation == nil || root.Ledger.ArchiveGenesisRef.Mutation.Intent.PayloadDigest != authorization.TargetEmptyHeadDigest {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: target genesis ref differs from empty authorization")
	}
	runtime, run, err := e.archiveGenesisProducerRun(ctx, root, root.Ledger.ArchiveGenesisRef.Mutation.ProducerUID)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	m := root.Ledger.ArchiveGenesisRef.Mutation
	store, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, store, run.MutationProfile)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	if err := e.ExecuteSourceMutation(ctx, runtime, m.ProducerUID, m.Sequence, m.Intent, store, provider, receipts); err != nil {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	lookup, err := e.lookupMutation(ctx, uid, run.Identity, fencing.MutationLookupRequest{ProducerUID: m.ProducerUID, Sequence: m.Sequence, Intent: m.Intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != m.Intent || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationCommitted || !lookup.Entry.Resolution.AllCopiesTerminal {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	if err := verifyCommittedEmptyRecoveryTargetHead(ctx, e.ArchiveBucket, scope.Prefix); err != nil {
		return ArchiveEmptyRecoveryResponse{}, errors.Join(err, fencing.ErrPending)
	}
	if authorization.RecoveryMode == RecoveryModeFrozenSource {
		if err := e.verifyFrozenBucketEnvironment(ctx, authorization.BucketName, authorization.SourcePrefix, recovery.FrozenBucketEnvironment{Metageneration: authorization.BucketMetageneration, LifecycleDigest: authorization.LifecyclePolicyDigest}); err != nil {
			return ArchiveEmptyRecoveryResponse{}, err
		}
	}
	response.State = fencing.EmptyRecoveryCommitted
	return response, nil
}

// verifyCommittedEmptyRecoveryTargetHead validates the live target archive,
// which may have advanced beyond its immutable empty-genesis receipt.
func verifyCommittedEmptyRecoveryTargetHead(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	manager := recovery.NewManager(bucket, prefix, recovery.ArchiveConfigID)
	defer manager.Close()
	if err := manager.Load(ctx); err != nil {
		return err
	}
	if manager.Sealed() {
		return fmt.Errorf("fencing: target archive was sealed after empty recovery genesis")
	}
	if _, ok := manager.HeadVersion(); !ok {
		return fmt.Errorf("fencing: committed empty recovery target HEAD is missing")
	}
	return nil
}

// readCommittedEmptyRecovery handles retries after the target has published
// its genesis (and possibly advanced its live HEAD). It never treats an
// unreferenced HEAD as proof; only the exact immutable ref and terminal receipt
// can bypass the pre-initialization pristine-scope check.
func (e *FencingExecutor) readCommittedEmptyRecovery(ctx context.Context, uid, targetID string, scope fencing.ArchiveScope, authorization fencing.ArchiveEmptyRecoveryAuthorization) (ArchiveEmptyRecoveryResponse, bool, error) {
	claim, _, claimErr := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	root, _, rootErr := e.AuthorityState.ReadRoot(ctx, uid, targetID)
	if errors.Is(claimErr, errAuthorityStateNotFound) && errors.Is(rootErr, errAuthorityStateNotFound) {
		return ArchiveEmptyRecoveryResponse{}, false, nil
	}
	if claimErr != nil || rootErr != nil {
		return ArchiveEmptyRecoveryResponse{}, false, errors.Join(claimErr, rootErr)
	}
	claimAuth, rootAuth := claim.EmptyRecoveryAuthorization, root.Ledger.EmptyRecoveryAuthorization
	if claimAuth != nil && rootAuth == nil && root.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisEmptyRecoveryPending {
		if hashJSON(claimAuth) != hashJSON(&authorization) {
			return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: immutable empty recovery authorization differs")
		}
		if err := e.AuthorityState.SetRootEmptyRecoveryAuthorization(ctx, uid, targetID, authorization); err != nil {
			return ArchiveEmptyRecoveryResponse{}, false, errors.Join(err, fencing.ErrPending)
		}
		root, _, rootErr = e.AuthorityState.ReadRoot(ctx, uid, targetID)
		if rootErr != nil {
			return ArchiveEmptyRecoveryResponse{}, false, rootErr
		}
		rootAuth = root.Ledger.EmptyRecoveryAuthorization
	}
	if (claimAuth == nil) != (rootAuth == nil) || (claimAuth != nil && hashJSON(claimAuth) != hashJSON(rootAuth)) {
		return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: empty recovery authorization differs between claim and root")
	}
	if claimAuth == nil {
		if claim.GenesisRef != nil || root.Ledger.ArchiveGenesisRef != nil {
			return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: target archive reference has no empty recovery authorization")
		}
		return ArchiveEmptyRecoveryResponse{}, false, nil
	}
	if hashJSON(claimAuth) != hashJSON(&authorization) {
		return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: immutable empty recovery authorization differs")
	}
	if claim.GenesisRef == nil {
		if root.Ledger.ArchiveGenesisRef != nil {
			return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: target root reference is missing from its scope claim")
		}
		return ArchiveEmptyRecoveryResponse{}, false, nil
	}
	if claim.GenesisRef.Mutation == nil || claim.GenesisRef.Mutation.Intent.PayloadDigest != authorization.TargetEmptyHeadDigest {
		return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: target genesis reference differs from its empty recovery authorization")
	}
	if root.Ledger.ArchiveGenesisRef == nil {
		if err := e.setRootArchiveGenesisRef(ctx, uid, targetID, *claim.GenesisRef); err != nil {
			return ArchiveEmptyRecoveryResponse{}, false, errors.Join(err, fencing.ErrPending)
		}
	} else if !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, claim.GenesisRef) {
		return ArchiveEmptyRecoveryResponse{}, false, fmt.Errorf("fencing: target root and claim genesis references differ")
	}
	response, err := e.emptyRecoveryResponse(ctx, uid, targetID, scope, authorization)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, false, err
	}
	if response.State != fencing.EmptyRecoveryCommitted {
		return ArchiveEmptyRecoveryResponse{}, false, fencing.ErrPending
	}
	return response, true, nil
}
