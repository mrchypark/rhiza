package operator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// ArchiveForkGenesisRequest is accepted only from the configured controller
// service account. OperationID is the recovery/anchor operation; the source
// fence operation is a distinct immutable RhizaFence identity.
type ArchiveForkGenesisRequest struct {
	RecoveryName           string                            `json:"recoveryName,omitempty"`
	StatefulSetUID         string                            `json:"statefulSetUID"`
	TargetClusterID        string                            `json:"targetClusterID"`
	OperationID            string                            `json:"operationID"`
	SourceFenceOperationID string                            `json:"sourceFenceOperationID"`
	RecoveryMode           string                            `json:"recoveryMode,omitempty"`
	BucketEnvironment      *recovery.FrozenBucketEnvironment `json:"bucketEnvironment,omitempty"`
}

// ArchiveForkGenesisResponse makes the persisted source profile explicit to
// HTTPS clients. A cooperative result never means a genesis reference exists.
type ArchiveForkGenesisResponse struct {
	Profile           string `json:"profile"`
	GenesisRegistered bool   `json:"genesisRegistered"`
}

// RegisterArchiveForkGenesis validates persisted recovery and fence evidence,
// then creates or joins the target root, scope claim, and immutable fork ref.
func (e *FencingExecutor) RegisterArchiveForkGenesis(ctx context.Context, request ArchiveForkGenesisRequest) error {
	_, err := e.registerArchiveForkGenesis(ctx, request)
	return err
}

func (e *FencingExecutor) registerArchiveForkGenesis(ctx context.Context, request ArchiveForkGenesisRequest) (ArchiveForkGenesisResponse, error) {
	if e == nil || ctx == nil || e.Kube == nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork registration is unavailable")
	}
	if request.StatefulSetUID == "" || request.TargetClusterID == "" || request.OperationID == "" || request.SourceFenceOperationID == "" ||
		len(request.StatefulSetUID) > 128 || len(request.TargetClusterID) > 128 || len(request.OperationID) > 256 || len(request.SourceFenceOperationID) > 256 ||
		(request.RecoveryMode != "" && request.RecoveryMode != RecoveryModeFrozenSource) {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork registration identity is incomplete")
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if request.RecoveryName == "" || request.BucketEnvironment == nil {
			return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: frozen-source registration lacks persisted recovery identity")
		}
		var stored Resource
		if err := e.Kube.Get(ctx, e.controller().resourcePath(request.RecoveryName), &stored); err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
		uid := str(stored.Metadata["uid"])
		if uid == "" || uid+":"+stored.Spec.RecoveryID != request.OperationID || stored.Spec.RecoveryID != request.SourceFenceOperationID ||
			stored.Status.StatefulSetUID != request.StatefulSetUID || stored.Status.RecoveryMode != RecoveryModeFrozenSource ||
			stored.Status.TargetBaseKind != "fork" || stored.Status.Stage != "FrozenForked" || stored.Status.Target != request.TargetClusterID || stored.Status.FrozenBucketEnvironment == nil ||
			hashJSON(stored.Status.FrozenBucketEnvironment) != hashJSON(request.BucketEnvironment) {
			return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: frozen-source registration differs from its persisted recovery mode")
		}
	} else if request.RecoveryName != "" || request.BucketEnvironment != nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: legacy fork cannot carry frozen-source recovery bindings")
	}
	sts, err := e.archiveForkStatefulSet(ctx, request.StatefulSetUID)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	env, err := e.controller().environment(ctx, asObject(containers[0]))
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	sourceID := strings.TrimSpace(env["RHIZA_CLUSTER_ID"])
	if sourceID == "" || sourceID == request.TargetClusterID {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork source and target identities differ")
	}
	state, err := e.readArchiveGenesisProfile(ctx, request.StatefulSetUID, sourceID)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if state.StatefulSetUID != request.StatefulSetUID || state.Ledger.SourceClusterID != sourceID {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: persisted source root identity differs")
	}
	// The stored ledger profile is authoritative. Executor startup flags may
	// require the independent profile, but cannot convert a cooperative root.
	if err := (&FencingExecutor{}).validateMutationJournalProfile(state); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if !state.Ledger.RequireMutationJournal {
		if request.RecoveryMode != "" {
			return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: frozen-source recovery requires the independently journaled profile")
		}
		if err := e.verifyCooperativeArchiveForkSource(ctx, request, sourceID, state); err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
		return ArchiveForkGenesisResponse{Profile: "cooperative", GenesisRegistered: false}, nil
	}
	if e.AuthorityState == nil || e.ArchiveBucket == nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: required archive fork registration needs canonical ObjectStorage")
	}
	// Once a valid required root has been read, pass that persisted profile to
	// existing registration helpers; this is not a fallback or profile upgrade.
	registration := *e
	registration.RequireMutationJournal = true
	e = &registration
	identity := StoreIdentity(env)
	basePrefix := strings.Trim(identity["RHIZA_OBJSTORE_PREFIX"], "/")
	sourcePrefix := path.Join(basePrefix, sourceID)
	targetPrefix := path.Join(basePrefix, request.TargetClusterID)
	if basePrefix == "." || sourcePrefix == targetPrefix || sourcePrefix == "" || targetPrefix == "" {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork prefixes are invalid")
	}
	var frozenBinding *recovery.FrozenSourceBinding
	if state.Ledger.ClosedBy != request.SourceFenceOperationID {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: source root is not closed by the requested fence")
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	fence, err := e.readCanonicalSourceFence(ctx, request, sourceID, bindingUID, state)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if fence.Spec.OperationID != request.SourceFenceOperationID || fence.Spec.StatefulSetUID != request.StatefulSetUID || fence.Spec.SourceClusterID != sourceID {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: canonical source fence identity differs")
	}
	if fence.Spec.Scope != ScopeGeneration || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: source generation fence lacks independent terminal evidence")
	}
	hash, err := requestHash(&fence.Spec)
	if err != nil || state.Ledger.ClosedHash != hash {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: canonical source fence hash differs")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if err := validateIndependentV2(fence.Spec, fence.Status.Proof.IndependentV2); err != nil || fence.Status.Proof.IndependentV2.LedgerHash != e.ledgerHash(state) {
		return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: source independent proof is not bound to the canonical closed root"))
	}
	storageScope, err := e.storageScope(ctx, sts, sourceID)
	if err != nil || fence.Status.Proof.IndependentV2.StorageScope != storageScope {
		return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: source independent proof names a different ObjectStorage scope"))
	}
	if err := verifyIndependentProofCoverage(state, fence.Spec, fence.Status.Proof.IndependentV2); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	currentUIDs, err := e.currentInventory(ctx, request.StatefulSetUID, nil)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	sort.Strings(currentUIDs)
	proofUIDs := append([]string(nil), fence.Status.Proof.IndependentV2.CurrentPodUIDs...)
	sort.Strings(proofUIDs)
	if !sameStrings(currentUIDs, proofUIDs) {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: source Pod inventory changed since the canonical fence proof")
	}

	scopeProvider := strings.ToLower(identity["RHIZA_OBJSTORE_PROVIDER"])
	scopeBucket := strings.TrimSpace(identity["RHIZA_OBJSTORE_BUCKET"])
	if scopeProvider != "gcs" || scopeBucket == "" {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork registration requires the configured GCS source provider")
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if request.BucketEnvironment == nil {
			return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: frozen-source bucket environment is missing")
		}
		if err := e.verifyFrozenBucketEnvironment(ctx, scopeBucket, sourcePrefix, *request.BucketEnvironment); err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
		sourceScope := fencing.ArchiveScope{Provider: scopeProvider, Bucket: scopeBucket, Prefix: sourcePrefix, ClusterID: sourceID}
		scopeHash, err := sourceScope.Hash()
		if err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
		sourceIdentity := recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: request.StatefulSetUID}
		expected, err := recovery.FrozenTargetID(sourceIdentity)
		if err != nil || request.TargetClusterID != expected {
			return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: frozen-source target differs from stable source identity"))
		}
		frozenBinding = &recovery.FrozenSourceBinding{Identity: sourceIdentity, FenceProofDigest: hashJSON(fence.Status.Proof), ClosedRootDigest: e.ledgerHash(state)}
	} else if request.BucketEnvironment != nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: legacy fork cannot carry a frozen-source bucket environment")
	}
	recoveryUID, _, ok := strings.Cut(request.OperationID, ":")
	if !ok || recoveryUID == "" {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: archive fork operation has no recovery resource identity")
	}
	claimMode := request.RecoveryMode
	if claimMode == "" {
		claimMode = sourceSuccessorModeLegacy
	}
	sourceScope := fencing.ArchiveScope{Provider: scopeProvider, Bucket: scopeBucket, Prefix: sourcePrefix, ClusterID: sourceID}
	reservationDigest := ""
	sourceMembers, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: source membership is unavailable: %w", err)
	}
	sourceDurability := env["RHIZA_OBJSTORE_DURABILITY"]
	if sourceDurability == "" {
		sourceDurability = "async"
	}
	if sourceDurability != "async" && sourceDurability != "before-ack" {
		return ArchiveForkGenesisResponse{}, fmt.Errorf("fencing: source durability mode is invalid")
	}
	sourceMembership, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(sourcePrefix, "voters/membership.json"), 64<<10)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	var sourceRecord recovery.MembershipRecord
	if err := decodeCanonicalArchiveJSON(sourceMembership, &sourceRecord); err != nil || sourceRecord != recovery.NewMembershipRecord(sourceID, sourceMembers, sourceDurability) {
		return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: source membership differs from enrolled workload"))
	}
	if request.RecoveryMode == "" {
		reservationData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(sourcePrefix, "recovery/successor.json"), 64<<10)
		if err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
		var reservation archiveForkReservation
		if err := decodeLegacySuccessorReservation(reservationData, &reservation); err != nil || reservation.Version != 1 || reservation.OperationID != request.OperationID || reservation.Target != request.TargetClusterID || reservation.Source != sourceID || reservation.StatefulSetUID != request.StatefulSetUID || reservation.SourceMembership != sourceFingerprint(sourceID, sourceDurability, sourceMembers, sourcePrefix) || !validEvidenceDigest(reservation.SpecHash) || (reservation.TargetBaseKind != "" && reservation.TargetBaseKind != "fork") {
			return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: archive fork request differs from the immutable successor reservation"))
		}
		reservationDigest = authorityStateDigest(reservationData)
	} else {
		reserved, err := e.ArchiveBucket.Exists(ctx, path.Join(sourcePrefix, "recovery/successor.json"))
		if err != nil || reserved {
			return ArchiveForkGenesisResponse{}, errors.Join(err, fmt.Errorf("fencing: frozen-source registration cannot join a legacy successor reservation"))
		}
	}
	if err := e.claimSourceSuccessor(ctx, sourceScope, request.StatefulSetUID, recoveryUID, request.OperationID, request.SourceFenceOperationID,
		request.TargetClusterID, "fork", claimMode, reservationDigest); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	scope := fencing.ArchiveScope{Provider: scopeProvider, Bucket: scopeBucket, Prefix: targetPrefix, ClusterID: request.TargetClusterID}
	if err := scope.Validate(); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	ref, err := e.verifyArchiveForkArtifacts(ctx, request, sourceID, sourcePrefix, targetPrefix, scope, sourceMembers, sourceDurability, frozenBinding)
	if err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if err := e.verifyFrozenBucketEnvironment(ctx, scopeBucket, sourcePrefix, *request.BucketEnvironment); err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
	}
	if err := e.initializeRoot(ctx, request.StatefulSetUID, request.TargetClusterID); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	runtime := fencing.RuntimeBinding{StatefulSetUID: request.StatefulSetUID, SourceClusterID: request.TargetClusterID}
	if err := e.claimArchiveScopeForRegistration(ctx, request.StatefulSetUID, fencing.Identity{SourceClusterID: request.TargetClusterID}, runtime); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if err := e.commitArchiveGenesisRef(ctx, request.StatefulSetUID, request.TargetClusterID, scope, ref); err != nil {
		return ArchiveForkGenesisResponse{}, err
	}
	if request.RecoveryMode == RecoveryModeFrozenSource {
		if err := e.verifyFrozenBucketEnvironment(ctx, scopeBucket, sourcePrefix, *request.BucketEnvironment); err != nil {
			return ArchiveForkGenesisResponse{}, err
		}
	}
	return ArchiveForkGenesisResponse{Profile: "independent", GenesisRegistered: true}, nil
}

func (e *FencingExecutor) readArchiveGenesisProfile(ctx context.Context, uid, source string) (fenceLedger, error) {
	if e == nil {
		return fenceLedger{}, fmt.Errorf("fencing: archive source authority is unavailable")
	}
	if e.AuthorityState != nil {
		state, _, err := e.AuthorityState.ReadRoot(ctx, uid, source)
		return state, err
	}
	if e.LegacyCooperativeControlState {
		return e.readLegacyCooperative(ctx, uid, source)
	}
	return fenceLedger{}, fmt.Errorf("fencing: persisted archive source authority is unavailable")
}

func (e *FencingExecutor) verifyCooperativeArchiveForkSource(ctx context.Context, request ArchiveForkGenesisRequest, sourceID string, state fenceLedger) error {
	if state.Ledger.ClosedBy != request.SourceFenceOperationID {
		return fmt.Errorf("fencing: cooperative source root is not closed by the requested fence")
	}
	fence, err := e.readCanonicalSourceFenceByOperation(ctx, request, sourceID)
	if err != nil {
		return err
	}
	hash, err := requestHash(&fence.Spec)
	if err != nil || fence.Spec.Scope != ScopeGeneration || state.Ledger.ClosedHash != hash || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 != nil {
		return fmt.Errorf("fencing: cooperative fence does not match the persisted source root")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return err
	}
	return nil
}

func (e *FencingExecutor) readCanonicalSourceFenceByOperation(ctx context.Context, request ArchiveForkGenesisRequest, sourceID string) (fenceResource, error) {
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + e.Kube.Namespace + "/rhizafences"
	var records struct {
		Items []fenceResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, endpoint, &records); err != nil {
		return fenceResource{}, err
	}
	var match *fenceResource
	for i := range records.Items {
		record := &records.Items[i]
		if record.Spec.OperationID != request.SourceFenceOperationID || record.Spec.StatefulSetUID != request.StatefulSetUID || record.Spec.SourceClusterID != sourceID {
			continue
		}
		if match != nil {
			return fenceResource{}, fmt.Errorf("fencing: multiple persisted source fences match the requested operation")
		}
		match = record
	}
	if match == nil || match.Metadata["deletionTimestamp"] != nil || match.Spec.Namespace != e.Kube.Namespace {
		return fenceResource{}, fmt.Errorf("fencing: persisted cooperative source fence is unavailable")
	}
	return *match, nil
}

func sourceRootBinding(state fenceLedger) (fencing.RuntimeBinding, string, error) {
	var binding string
	var representative fencing.RuntimeBinding
	for _, run := range state.Ledger.Runs {
		if run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil || run.Runtime.StatefulSetUID != state.StatefulSetUID || run.Runtime.SourceClusterID != state.Ledger.SourceClusterID {
			return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: source root contains an invalid runtime binding")
		}
		if binding != "" && binding != run.Runtime.BindingUID {
			return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: source root has conflicting workload bindings")
		}
		binding = run.Runtime.BindingUID
		if representative.RunID == "" {
			representative = *run.Runtime
		}
	}
	if binding == "" || representative.RunID == "" {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: source root has no enrolled runtime binding")
	}
	return representative, binding, nil
}

func (e *FencingExecutor) archiveForkStatefulSet(ctx context.Context, uid string) (object, error) {
	var workloads struct {
		Items []object `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+e.Kube.Namespace+"/statefulsets", &workloads); err != nil {
		return nil, err
	}
	for _, sts := range workloads.Items {
		if str(nested(sts, "metadata", "uid")) != uid {
			continue
		}
		containers := list(nested(sts, "spec", "template", "spec", "containers"))
		if len(containers) != 1 {
			return nil, fmt.Errorf("fencing: archive fork workload container is ambiguous")
		}
		return sts, nil
	}
	return nil, fmt.Errorf("fencing: archive fork StatefulSet UID is unavailable")
}

func (e *FencingExecutor) readCanonicalSourceFence(ctx context.Context, registration ArchiveForkGenesisRequest, sourceID, bindingUID string, state fenceLedger) (fenceResource, error) {
	name := "fence-" + hashJSON([]string{bindingUID, registration.SourceFenceOperationID})[:40]
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + e.Kube.Namespace + "/rhizafences/" + name
	var record fenceResource
	if err := e.Kube.Get(ctx, endpoint, &record); err != nil {
		return record, err
	}
	if record.Metadata["deletionTimestamp"] != nil || record.Spec.StatefulSetUID != registration.StatefulSetUID || record.Spec.SourceClusterID != sourceID || record.Spec.BindingUID != bindingUID || record.Spec.Namespace != e.Kube.Namespace {
		return record, fmt.Errorf("fencing: persisted source fence request identity differs")
	}
	return record, nil
}

type archiveForkIntentRecord struct {
	Version              int                           `json:"version"`
	OperationID          string                        `json:"operation_id"`
	SourcePrefix         string                        `json:"source_prefix"`
	TargetPrefix         string                        `json:"target_prefix"`
	MemberIDs            []string                      `json:"member_ids"`
	ManifestHash         string                        `json:"manifest_hash"`
	SourceBootstrapHash  string                        `json:"source_bootstrap_hash,omitempty"`
	TargetMembershipHash string                        `json:"target_membership_hash,omitempty"`
	FrozenSource         *recovery.FrozenSourceBinding `json:"frozen_source,omitempty"`
}
type archiveForkReservation struct {
	Version          int    `json:"version"`
	OperationID      string `json:"operation_id"`
	Target           string `json:"target"`
	Source           string `json:"source"`
	SourceMembership string `json:"source_membership"`
	StatefulSetUID   string `json:"statefulset_uid"`
	SpecHash         string `json:"spec_hash"`
	TargetBaseKind   string `json:"target_base_kind,omitempty"`
}
type archiveForkLineageRecord struct {
	Version          int                       `json:"version"`
	OperationID      string                    `json:"operation_id"`
	Source           string                    `json:"source"`
	SourceDurability string                    `json:"source_durability"`
	SourceMembership string                    `json:"source_membership"`
	TargetMembership recovery.MembershipRecord `json:"target_membership"`
	Fork             recovery.ForkResult       `json:"fork"`
}

func (e *FencingExecutor) verifyArchiveForkArtifacts(ctx context.Context, request ArchiveForkGenesisRequest, sourceID, sourcePrefix, targetPrefix string, scope fencing.ArchiveScope, sourceMembers []quepaxa.Member, sourceDurability string, frozen *recovery.FrozenSourceBinding) (fencing.ArchiveGenesisRef, error) {
	anchor, anchorHash, err := recovery.ReadGenerationAnchor(ctx, e.ArchiveBucket, targetPrefix)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	if anchor.OperationID != request.OperationID || anchor.SourcePrefix != sourcePrefix || anchor.TargetPrefix != targetPrefix || anchor.SourceTip == 0 ||
		(frozen == nil) != (anchor.FrozenSource == nil) || frozen != nil && (anchor.FrozenSource.Identity != frozen.Identity || anchor.FrozenSource.FenceProofDigest != frozen.FenceProofDigest || anchor.FrozenSource.ClosedRootDigest != frozen.ClosedRootDigest) {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: verified anchor does not match source, target, or recovery operation")
	}
	membershipData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(targetPrefix, "voters/membership.json"), 64<<10)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	var membership recovery.MembershipRecord
	if err := decodeCanonicalArchiveJSON(membershipData, &membership); err != nil || membership != anchor.TargetMembership {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: target membership differs from the verified anchor"))
	}
	if _, err := recovery.VerifyGenerationAnchor(ctx, e.ArchiveBucket, targetPrefix, membership, anchorHash); err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	intentData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(targetPrefix, "fork/intent.json"), 64<<10)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	var intent archiveForkIntentRecord
	validIntentVersion := func(version int) bool {
		if frozen != nil {
			return version == 3
		}
		return version == 1 || version == 2
	}
	if err := decodeCanonicalArchiveJSON(intentData, &intent); err != nil || !validIntentVersion(intent.Version) || intent.OperationID != request.OperationID || intent.SourcePrefix != sourcePrefix || intent.TargetPrefix != targetPrefix || intent.ManifestHash != anchor.SourceManifest ||
		(frozen == nil) != (intent.FrozenSource == nil) || frozen != nil && hashJSON(intent.FrozenSource) != hashJSON(anchor.FrozenSource) {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork intent differs from verified anchor"))
	}
	if (intent.Version != 2 && intent.Version != 3) || len(intent.MemberIDs) != len(sourceMembers) || intent.SourceBootstrapHash == "" || intent.TargetMembershipHash == "" {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: fork intent lacks anchored source and target membership bindings")
	}
	wantMembers := make([]string, 0, len(sourceMembers))
	for _, member := range sourceMembers {
		wantMembers = append(wantMembers, string(member.ID))
	}
	sort.Strings(wantMembers)
	if !sameStrings(intent.MemberIDs, wantMembers) {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: fork source members differ from enrolled source")
	}
	bootstrap, err := json.Marshal(struct {
		ConfigID uint             `json:"config_id"`
		Members  []quepaxa.Member `json:"members"`
	}{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers})
	if err != nil || intent.SourceBootstrapHash != authorityStateDigest(bootstrap) || anchor.SourceBootstrap != intent.SourceBootstrapHash {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork source bootstrap differs from enrolled source"))
	}
	membershipDigest, err := json.Marshal(membership)
	if err != nil || intent.TargetMembershipHash != authorityStateDigest(membershipDigest) {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork target membership hash differs"))
	}
	resultData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(targetPrefix, "fork/result.json"), 64<<10)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	var result recovery.ForkResult
	if err := decodeCanonicalArchiveJSON(resultData, &result); err != nil || result.Tip != anchor.SourceTip || result.ManifestHash != anchor.SourceManifest || result.PrefixHash != anchor.SourcePrefixHash ||
		(frozen == nil) != (result.FrozenSource == nil) || frozen != nil && hashJSON(result.FrozenSource) != hashJSON(anchor.FrozenSource) {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork result differs from verified anchor"))
	}
	lineageData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(targetPrefix, "recovery/generation.json"), 64<<10)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	var lineage archiveForkLineageRecord
	if err := decodeCanonicalArchiveJSON(lineageData, &lineage); err != nil || lineage.Version != 1 || lineage.OperationID != request.OperationID || lineage.Source != sourceID || lineage.SourceDurability != sourceDurability || lineage.SourceMembership != sourceFingerprint(sourceID, sourceDurability, sourceMembers, sourcePrefix) || lineage.TargetMembership != membership || !sameArchiveForkResult(lineage.Fork, result) {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork lineage differs from verified artifacts"))
	}
	manager := recovery.NewManager(e.ArchiveBucket, targetPrefix, recovery.ArchiveConfigID)
	defer manager.Close()
	if err := manager.Load(ctx); err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	if manager.Sealed() || uint64(manager.Tip()) != result.Tip {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: target archive is sealed or has a different tip")
	}
	if currentAnchor, ok := manager.GenerationAnchor(); !ok || currentAnchor != anchorHash {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: target archive head does not bind the verified generation anchor")
	}
	sourceArchive := recovery.NewManager(e.ArchiveBucket, sourcePrefix, recovery.ArchiveConfigID)
	defer sourceArchive.Close()
	if err := sourceArchive.Load(ctx); err != nil || uint64(sourceArchive.Tip()) != result.Tip {
		return fencing.ArchiveGenesisRef{}, errors.Join(err, fmt.Errorf("fencing: fork source archive is not the exact generation"))
	}
	if frozen == nil && !sourceArchive.Sealed() {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: legacy fork source archive is not sealed")
	}
	if frozen != nil {
		frontier := anchor.FrozenSource.Frontier
		if request.BucketEnvironment == nil || frontier.BucketMetageneration != request.BucketEnvironment.Metageneration || frontier.LifecyclePolicyDigest != request.BucketEnvironment.LifecycleDigest {
			return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: frozen source bucket environment differs from the materialized anchor")
		}
		attrs, attrErr := e.ArchiveBucket.Attributes(ctx, path.Join(sourcePrefix, "archive/head.bin"))
		data, readErr := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(sourcePrefix, "archive/head.bin"), 64<<10)
		if attrErr != nil || readErr != nil || attrs.Version == nil || *attrs.Version != frontier.HeadVersion || authorityStateDigest(data) != frontier.HeadDigest || frontier.Tip != uint64(sourceArchive.Tip()) || sourceArchive.Sealed() {
			return fencing.ArchiveGenesisRef{}, errors.Join(attrErr, readErr, fmt.Errorf("fencing: frozen source frontier differs from the current unsealed archive"))
		}
	}
	anchorDigest := anchorHash
	intentDigest, resultDigest, lineageDigest := authorityStateDigest(intentData), authorityStateDigest(resultData), authorityStateDigest(lineageData)
	fork := &fencing.ArchiveGenesisFork{
		AnchorKey: recovery.GenerationAnchorKey(targetPrefix), AnchorDigest: hex.EncodeToString(anchorDigest[:]),
		IntentDigest: intentDigest, ResultDigest: resultDigest, LineageDigest: lineageDigest,
		TargetPrefix: targetPrefix, MembershipHash: authorityStateDigest(membershipData), ConfigID: recovery.ArchiveConfigID,
		BaseTip: result.Tip, BasePrefixHash: result.PrefixHash, AnchorHash: hex.EncodeToString(anchorHash[:]),
	}
	ref := fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, Fork: fork}
	scopeHash, err := scope.Hash()
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	ref.ScopeHash = scopeHash
	return ref, ref.ValidateForScope(scope)
}

func sameArchiveForkResult(a, b recovery.ForkResult) bool {
	if a.Tip != b.Tip || a.PrefixHash != b.PrefixHash || a.ManifestHash != b.ManifestHash || (a.FrozenSource == nil) != (b.FrozenSource == nil) {
		return false
	}
	return a.FrozenSource == nil || hashJSON(a.FrozenSource) == hashJSON(b.FrozenSource)
}

func verifyIndependentProofCoverage(state fenceLedger, request FenceRequest, evidence *IndependentEvidenceV2) error {
	runs, err := independentClosedRuns(state, request, nil)
	if err != nil || evidence == nil || len(evidence.Terminals) != len(runs) {
		return errors.Join(err, fmt.Errorf("fencing: source proof does not cover the complete canonical run history"))
	}
	byRun := make(map[string]fencing.TerminalReference, len(evidence.Terminals))
	for _, ref := range evidence.Terminals {
		runID := ref.Request.Identity.RunID
		if _, exists := byRun[runID]; exists {
			return fmt.Errorf("fencing: source proof duplicates a terminal run")
		}
		byRun[runID] = ref
	}
	for _, run := range runs {
		ref, exists := byRun[run.Identity.RunID]
		if !exists || run.IndependentTerminal == nil || ref != *run.IndependentTerminal || run.Stop == nil || ref.Request != *run.Stop {
			return fmt.Errorf("fencing: source proof terminal reference differs from canonical run history")
		}
		if _, err := state.Ledger.IndependentTerminal(run.Identity.RunID, hashJSON(evidence.StorageScope)); err != nil {
			return err
		}
	}
	return nil
}

func readBoundedArchiveObject(ctx context.Context, bucket objstore.Bucket, key string, limit int64) ([]byte, error) {
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, limit+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("fencing: archive evidence object exceeds capacity")
	}
	return data, nil
}
func decodeCanonicalArchiveJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("fencing: trailing archive evidence JSON")
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, data) {
		return errors.Join(err, fmt.Errorf("fencing: noncanonical archive evidence JSON"))
	}
	return nil
}

func (e *FencingExecutor) authenticateController(ctx context.Context, authorization string) error {
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if e == nil || e.Kube == nil || !ok || token == "" || e.ControllerServiceAccount == "" {
		return fmt.Errorf("fencing: configured controller token required")
	}
	var review object
	if err := e.Kube.Post(ctx, "/apis/authentication.k8s.io/v1/tokenreviews", object{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "spec": object{"token": token, "audiences": []string{"rhiza-fencer"}}}, &review); err != nil {
		return err
	}
	if nested(review, "status", "authenticated") != true {
		return fmt.Errorf("fencing: controller token is unauthenticated")
	}
	validAudience := false
	for _, audience := range list(nested(review, "status", "audiences")) {
		validAudience = validAudience || str(audience) == "rhiza-fencer"
	}
	want := "system:serviceaccount:" + e.Kube.Namespace + ":" + e.ControllerServiceAccount
	if !validAudience || str(nested(review, "status", "user", "username")) != want {
		return fmt.Errorf("fencing: token is not from the configured controller service account")
	}
	return nil
}

func (e *FencingExecutor) handleArchiveForkGenesis(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/archive-genesis/fork/register" || r.URL.RawQuery != "" {
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
	var request ArchiveForkGenesisRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid archive fork registration", http.StatusBadRequest)
		return
	}
	result, err := e.registerArchiveForkGenesis(r.Context(), request)
	if err != nil {
		if transientFenceAuthority(err) || errors.Is(err, fencing.ErrPending) {
			http.Error(w, "archive fork registration pending", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "archive fork registration rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
