package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type manualRecoveryApprovalRecord struct {
	Version         int                             `json:"version"`
	ChallengeDigest string                          `json:"challengeDigest"`
	AcceptedAt      time.Time                       `json:"acceptedAt,omitempty"`
	Object          ManualRecoveryAttestationObject `json:"object"`
}

func (e *FencingExecutor) manualRecoveryTime() time.Time {
	if e != nil && e.manualRecoveryNow != nil {
		return e.manualRecoveryNow().UTC()
	}
	return time.Now().UTC()
}

func manualRecoveryChallengeIdentity(recoveryUID string) string {
	return hashJSON([]any{"rhiza-manual-recovery-challenge-v1", recoveryUID})
}

func manualRecoveryApprovalIdentity(recoveryUID string) string {
	return hashJSON([]any{"rhiza-manual-recovery-approval-v1", recoveryUID})
}

func (e *FencingExecutor) manualRecoveryChallengeForPreflight(
	ctx context.Context,
	stored Resource,
	env map[string]string,
	scope fencing.ArchiveScope,
	state fenceLedger,
	fence fenceResource,
	scopeHash string,
	currentUIDs []string,
) (ManualRecoveryChallenge, string, ManualRecoveryBinding, error) {
	var zero ManualRecoveryChallenge
	if e == nil || e.Kube == nil || stored.Status.TargetBaseKind != "fork" || stored.Status.FrozenBucketEnvironment == nil {
		return zero, "", ManualRecoveryBinding{}, fencing.ErrPending
	}
	actualScopeHash, err := scope.Hash()
	if err != nil || actualScopeHash != scopeHash {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
	}
	identity := recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: stored.Spec.SourceClusterID, StatefulSetUID: stored.Status.StatefulSetUID}
	targetID, targetErr := recovery.FrozenTargetID(identity)
	if targetErr != nil || (stored.Status.Target != "" && targetID != stored.Status.Target) {
		return zero, "", ManualRecoveryBinding{}, errors.Join(targetErr, fencing.ErrClosed)
	}
	if err := e.verifyFrozenBucketEnvironment(ctx, scope.Bucket, scope.Prefix, *stored.Status.FrozenBucketEnvironment); err != nil {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
	}
	currentUIDs, err = CanonicalizePodUIDs(currentUIDs)
	if err != nil {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return zero, "", ManualRecoveryBinding{}, err
	}
	durability := env["RHIZA_OBJSTORE_DURABILITY"]
	if (durability != "async" && durability != "before-ack") || stored.Status.SourceDurability != durability ||
		(durability == "async" && !stored.Spec.AllowDataLoss) ||
		stored.Status.RecoveryMode != RecoveryModeFrozenSource || stored.Spec.MaxArchiveAgeSeconds != 0 ||
		str(nested(stored.Metadata, "annotations", automaticOwner)) != "" || str(nested(stored.Metadata, "annotations", automaticLogical)) != "" ||
		stored.Spec.Membership != nil {
		return zero, "", ManualRecoveryBinding{}, fencing.ErrClosed
	}
	if state.Ledger.ClosedBy != stored.Spec.RecoveryID || state.Ledger.ClosedHash == "" ||
		fence.Spec.OperationID != stored.Spec.RecoveryID || fence.Spec.SourceClusterID != stored.Spec.SourceClusterID ||
		fence.Spec.StatefulSetUID != stored.Status.StatefulSetUID || fence.Spec.Scope != ScopeGeneration ||
		fence.Status.Proof != nil && fence.Status.Proof.IndependentV2 != nil {
		return zero, "", ManualRecoveryBinding{}, fencing.ErrClosed
	}
	if _, err := independentClosedRuns(state, fence.Spec, nil); err != nil {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
	}
	if e.ArchiveBucket == nil {
		return zero, "", ManualRecoveryBinding{}, fencing.ErrPending
	}
	sourceMembershipData, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, path.Join(scope.Prefix, "voters/membership.json"), 64<<10)
	if err != nil {
		return zero, "", ManualRecoveryBinding{}, err
	}
	var sourceMembership recovery.MembershipRecord
	durability = env["RHIZA_OBJSTORE_DURABILITY"]
	if err := decodeCanonicalArchiveJSON(sourceMembershipData, &sourceMembership); err != nil || sourceMembership != recovery.NewMembershipRecord(stored.Spec.SourceClusterID, members, durability) {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
	}
	archive := recovery.NewManager(e.ArchiveBucket, scope.Prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrPending)
	}
	headVersion, ok := archive.HeadVersion()
	if !ok || archive.Tip() == 0 {
		return zero, "", ManualRecoveryBinding{}, fencing.ErrPending
	}
	sourceArchiveDigest := hashJSON([]any{"rhiza-manual-source-archive-v1", scopeHash, headVersion, archive.Tip(),
		authorityStateDigest(sourceMembershipData), stored.Status.FrozenBucketEnvironment, state.Ledger.ArchiveGenesisRef, state.Ledger.ArchiveGenesisState})
	requestHash, err := requestHash(&fence.Spec)
	if err != nil {
		return zero, "", ManualRecoveryBinding{}, err
	}
	operationID := str(stored.Metadata["uid"]) + ":" + stored.Spec.RecoveryID
	cohortDigest := hashJSON([]any{"rhiza-manual-closed-cohort-v1", state.Ledger.ClosedHash, state.Ledger.Runs})
	current := ManualRecoveryBinding{
		EvidenceKind:      ManualTerminalEvidenceKind,
		RecoveryNamespace: e.Kube.Namespace, RecoveryName: resourceName(&stored), RecoveryUID: str(stored.Metadata["uid"]),
		RecoveryID: stored.Spec.RecoveryID, OperationID: operationID, SourceClusterID: stored.Spec.SourceClusterID,
		StatefulSetName: stored.Spec.StatefulSet, StatefulSetUID: stored.Status.StatefulSetUID,
		FenceRequestHash: requestHash, PolicyDigest: hashJSON(stored.Spec), RootDigest: e.ledgerHash(state), LedgerDigest: hashJSON(state.Ledger),
		CohortDigest: cohortDigest, StorageScopeDigest: scopeHash, SourceArchiveDigest: sourceArchiveDigest,
		TargetClusterID: targetID, TargetBaseKind: stored.Status.TargetBaseKind, CurrentPodUIDs: currentUIDs,
	}
	if err := validateManualRecoveryBinding(current); err != nil {
		return zero, "", ManualRecoveryBinding{}, err
	}
	challenge, digest, err := e.readManualRecoveryChallenge(ctx, current.RecoveryUID)
	if err != nil {
		if !errors.Is(err, errAuthorityStateNotFound) {
			return zero, "", ManualRecoveryBinding{}, err
		}
		now := e.manualRecoveryTime().Truncate(time.Second)
		challenge = ManualRecoveryChallenge{Version: manualAttestationVersion, ManualRecoveryBinding: current,
			IssuedAt: now, ExpiresAt: now.Add(manualAttestationMaxLifetime)}
		digest, err = e.persistManualRecoveryChallenge(ctx, challenge)
		if err != nil {
			// A concurrent identical request may win the create-only write.
			challenge, digest, err = e.readManualRecoveryChallenge(ctx, current.RecoveryUID)
			if err != nil {
				return zero, "", ManualRecoveryBinding{}, errors.Join(err, fencing.ErrClosed)
			}
		}
	}
	if !sameManualRecoveryBinding(challenge.ManualRecoveryBinding, current) {
		return zero, "", ManualRecoveryBinding{}, errors.Join(fencing.ErrClosed, fmt.Errorf("canonical manual recovery challenge binding changed"))
	}
	if digest == "" {
		digest, err = ManualRecoveryChallengeDigest(challenge)
	}
	return challenge, digest, current, err
}

// persistManualRecoveryChallenge stores the exact fencer-derived challenge in
// canonical ObjectStorage state. Replays are accepted only for identical bytes.
func (e *FencingExecutor) persistManualRecoveryChallenge(ctx context.Context, challenge ManualRecoveryChallenge) (string, error) {
	if e == nil || e.AuthorityState == nil {
		return "", errors.New("canonical manual recovery authority state is unavailable")
	}
	digest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		return "", err
	}
	identity := manualRecoveryChallengeIdentity(challenge.RecoveryUID)
	key := e.AuthorityState.key("manual-recovery-challenges", identity)
	if err := e.AuthorityState.write(ctx, key, identity, challenge, nil); err == nil {
		return digest, nil
	} else if !errors.Is(err, errAuthorityStateConflict) {
		return "", err
	}
	stored, storedDigest, err := e.readManualRecoveryChallenge(ctx, challenge.RecoveryUID)
	if err != nil || storedDigest != digest || hashJSON(stored) != hashJSON(challenge) {
		return "", errors.Join(err, fencing.ErrClosed)
	}
	return digest, nil
}

func (e *FencingExecutor) readManualRecoveryChallenge(ctx context.Context, recoveryUID string) (ManualRecoveryChallenge, string, error) {
	if e == nil || e.AuthorityState == nil || strings.TrimSpace(recoveryUID) == "" {
		return ManualRecoveryChallenge{}, "", errors.New("canonical manual recovery authority state is unavailable")
	}
	identity := manualRecoveryChallengeIdentity(recoveryUID)
	data, _, err := e.AuthorityState.read(ctx, e.AuthorityState.key("manual-recovery-challenges", identity), identity)
	if err != nil {
		return ManualRecoveryChallenge{}, "", err
	}
	var challenge ManualRecoveryChallenge
	if err := decodeAuthorityValue(data, &challenge); err != nil || challenge.RecoveryUID != recoveryUID {
		return ManualRecoveryChallenge{}, "", errors.Join(err, fencing.ErrClosed)
	}
	digest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		return ManualRecoveryChallenge{}, "", err
	}
	return challenge, digest, nil
}

// validateAndPersistManualRecoveryAttestation reads the exact namespaced
// approval selected by the canonical challenge, validates it against a fresh
// binding, and records the API-server UID/spec create-only in canonical state.
// Callers must invoke it again immediately before consuming the manual proof.
func (e *FencingExecutor) validateAndPersistManualRecoveryAttestation(
	ctx context.Context,
	challenge ManualRecoveryChallenge,
	current ManualRecoveryBinding,
	now time.Time,
) (ManualRecoveryAttestationObject, error) {
	storedChallenge, challengeDigest, err := e.readManualRecoveryChallenge(ctx, challenge.RecoveryUID)
	if err != nil || challengeDigest == "" || hashJSON(storedChallenge) != hashJSON(challenge) {
		return ManualRecoveryAttestationObject{}, errors.Join(err, fencing.ErrClosed)
	}
	name, err := ManualRecoveryAttestationName(challenge.RecoveryUID)
	if err != nil {
		return ManualRecoveryAttestationObject{}, err
	}
	apiPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(challenge.RecoveryNamespace) +
		"/rhizamanualrecoveryattestations/" + url.PathEscape(name)
	var raw object
	if err := e.Kube.Get(ctx, apiPath, &raw); err != nil {
		if apiNotFound(err) {
			return ManualRecoveryAttestationObject{}, fencing.ErrPending
		}
		return ManualRecoveryAttestationObject{}, err
	}
	approval, err := decodeManualRecoveryAttestationObject(raw)
	if err != nil {
		return ManualRecoveryAttestationObject{}, err
	}
	if err := ValidateManualRecoveryAttestation(challenge, approval, current, "", now); err != nil {
		return ManualRecoveryAttestationObject{}, errors.Join(fencing.ErrClosed, err)
	}

	record := manualRecoveryApprovalRecord{Version: manualAttestationVersion, ChallengeDigest: challengeDigest, AcceptedAt: now.UTC(), Object: approval}
	identity := manualRecoveryApprovalIdentity(challenge.RecoveryUID)
	key := e.AuthorityState.key("manual-recovery-approvals", identity)
	if err := e.AuthorityState.write(ctx, key, identity, record, nil); err != nil {
		if !errors.Is(err, errAuthorityStateConflict) {
			return ManualRecoveryAttestationObject{}, err
		}
		data, _, readErr := e.AuthorityState.read(ctx, key, identity)
		if readErr != nil {
			return ManualRecoveryAttestationObject{}, errors.Join(err, readErr)
		}
		var existing manualRecoveryApprovalRecord
		if decodeErr := decodeAuthorityValue(data, &existing); decodeErr != nil || existing.Version != manualAttestationVersion ||
			existing.ChallengeDigest != challengeDigest || hashJSON(existing.Object) != hashJSON(approval) {
			return ManualRecoveryAttestationObject{}, errors.Join(decodeErr, fencing.ErrClosed)
		}
	}
	return approval, nil
}

func (e *FencingExecutor) manualRecoveryApprovalDigest(
	ctx context.Context,
	challenge ManualRecoveryChallenge,
	current ManualRecoveryBinding,
	now time.Time,
) (string, error) {
	approval, err := e.validateAndPersistManualRecoveryAttestation(ctx, challenge, current, now)
	if err != nil {
		return "", err
	}
	return hashJSON(approval), nil
}

func (e *FencingExecutor) manualTerminalApprovalDigestForRecovery(
	ctx context.Context,
	request ArchiveForkGenesisRequest,
	scope fencing.ArchiveScope,
	state fenceLedger,
	fence fenceResource,
	recoveryUID string,
	challenge ManualRecoveryChallenge,
	challengeDigest string,
	current ManualRecoveryBinding,
) (string, error) {
	now := e.manualRecoveryTime()
	digest, err := e.manualRecoveryApprovalDigest(ctx, challenge, current, now)
	if err == nil || now.Before(challenge.ExpiresAt) {
		return digest, err
	}
	// A valid approval is durably accepted before the controller begins the
	// potentially long frozen-source copy. That exact acceptance may resume
	// after challenge expiry, but only while every live binding still matches.
	record, recordErr := e.readManualRecoveryApproval(ctx, recoveryUID)
	acceptedAt := record.AcceptedAt
	if recordErr == nil && acceptedAt.IsZero() {
		// Legacy create-only approval records were written only after the old
		// validator accepted the object inside the challenge window.
		acceptedAt = record.Object.CreatedAt
	}
	if recordErr == nil && record.Version == manualAttestationVersion && record.ChallengeDigest == challengeDigest &&
		!acceptedAt.Before(challenge.IssuedAt) && acceptedAt.Before(challenge.ExpiresAt) && !acceptedAt.After(now) &&
		hashJSON(record.Object) == request.ManualTerminalAttestationDigest {
		acceptedDigest, acceptedErr := e.manualRecoveryApprovalDigest(ctx, challenge, current, acceptedAt)
		if acceptedErr == nil && acceptedDigest == request.ManualTerminalAttestationDigest {
			return acceptedDigest, nil
		}
	}
	if recordErr != nil && !errors.Is(recordErr, errAuthorityStateNotFound) {
		return "", errors.Join(err, recordErr)
	}
	// Expiry may not strand a registration after its exact source claim has
	// consumed the accepted approval. A fresh or changed claim still requires a
	// currently valid challenge and approval.
	exactClaim, claimErr := e.exactManualTerminalSuccessorClaim(ctx, request, scope, state, fence, recoveryUID)
	if claimErr != nil || !exactClaim {
		return "", errors.Join(err, claimErr)
	}
	record, recordErr = e.readManualRecoveryApproval(ctx, recoveryUID)
	if recordErr != nil {
		return "", errors.Join(err, recordErr)
	}
	if record.Version != manualAttestationVersion || record.ChallengeDigest != challengeDigest || hashJSON(record.Object) != request.ManualTerminalAttestationDigest {
		return "", errors.Join(err, fmt.Errorf("fencing: expired manual approval differs from the accepted canonical claim"))
	}
	// Re-read the namespaced object and validate it at its immutable creation
	// instant. The canonical claim and accepted AuthorityState record make this
	// an exact continuation, not a new approval after expiry.
	return e.manualRecoveryApprovalDigest(ctx, challenge, current, record.Object.CreatedAt)
}

// validateManualTerminalEvidenceForRegistration revalidates the administrator
// attestation at the source-successor claim and archive-registration boundary.
// It never manufactures or accepts IndependentV2 proof fields.
func (e *FencingExecutor) validateManualTerminalEvidenceForRegistration(
	ctx context.Context,
	request ArchiveForkGenesisRequest,
	scope fencing.ArchiveScope,
	state fenceLedger,
	fence fenceResource,
) error {
	if request.EvidenceKind != ManualTerminalEvidenceKind || request.RecoveryMode != RecoveryModeFrozenSource ||
		request.RecoveryName == "" || !validEvidenceDigest(request.ManualRecoveryChallengeDigest) || !validEvidenceDigest(request.ManualTerminalAttestationDigest) ||
		request.ManualMembershipBindingDigest != "" || (fence.Status.Proof != nil && fence.Status.Proof.IndependentV2 != nil) {
		return fmt.Errorf("fencing: manual terminal registration evidence is incomplete or mixed")
	}
	var stored Resource
	if err := e.Kube.Get(ctx, e.controller().resourcePath(request.RecoveryName), &stored); err != nil {
		return err
	}
	uid := str(stored.Metadata["uid"])
	if uid == "" || uid+":"+stored.Spec.RecoveryID != request.OperationID || stored.Spec.RecoveryID != request.SourceFenceOperationID ||
		stored.Status.RecoveryMode != RecoveryModeFrozenSource || stored.Status.TargetBaseKind != "fork" || stored.Status.Target != request.TargetClusterID ||
		stored.Status.StatefulSetUID != request.StatefulSetUID || stored.Status.ManualRecoveryChallengeDigest != request.ManualRecoveryChallengeDigest ||
		stored.Status.ManualAttestationDigest != request.ManualTerminalAttestationDigest || stored.Status.FrozenBucketEnvironment == nil || request.BucketEnvironment == nil ||
		hashJSON(stored.Status.FrozenBucketEnvironment) != hashJSON(request.BucketEnvironment) || stored.Status.SpecHash != immutableSpecHash(stored.Spec) {
		return fmt.Errorf("fencing: manual terminal registration differs from persisted Recovery status")
	}
	sts, err := e.archiveForkStatefulSet(ctx, request.StatefulSetUID)
	if err != nil {
		return err
	}
	ctr, err := container(sts, stored.Spec.Container)
	if err != nil {
		return err
	}
	env, err := e.controller().environment(ctx, ctr)
	if err != nil || noPVC(sts, ctr, env) != nil || e.archiveStoreConfigMatches(env) != nil {
		return errors.Join(err, fmt.Errorf("fencing: manual terminal source workload is unavailable or unsafe"))
	}
	if err := e.verifyFrozenBucketEnvironment(ctx, scope.Bucket, scope.Prefix, *stored.Status.FrozenBucketEnvironment); err != nil {
		return errors.Join(err, fencing.ErrClosed)
	}
	currentUIDs, err := e.currentInventory(ctx, request.StatefulSetUID, nil)
	if err != nil {
		return err
	}
	challenge, challengeDigest, current, err := e.manualRecoveryChallengeForPreflight(ctx, stored, env, scope, state, fence, state.Ledger.ArchiveScopeHash, currentUIDs)
	if err != nil {
		return err
	}
	if challengeDigest != request.ManualRecoveryChallengeDigest || stored.Status.ManualRecoveryChallenge == nil ||
		hashJSON(stored.Status.ManualRecoveryChallenge) != hashJSON(challenge) {
		return fmt.Errorf("fencing: canonical manual terminal challenge differs from persisted Recovery status")
	}
	digest, err := e.manualTerminalApprovalDigestForRecovery(ctx, request, scope, state, fence, uid, challenge, challengeDigest, current)
	if err != nil || digest != request.ManualTerminalAttestationDigest {
		return errors.Join(err, fmt.Errorf("fencing: manual terminal attestation changed or expired before registration"))
	}
	return nil
}

func (e *FencingExecutor) readManualRecoveryApproval(ctx context.Context, recoveryUID string) (manualRecoveryApprovalRecord, error) {
	if e == nil || e.AuthorityState == nil || strings.TrimSpace(recoveryUID) == "" {
		return manualRecoveryApprovalRecord{}, errors.New("canonical manual recovery approval state is unavailable")
	}
	identity := manualRecoveryApprovalIdentity(recoveryUID)
	data, _, err := e.AuthorityState.read(ctx, e.AuthorityState.key("manual-recovery-approvals", identity), identity)
	if err != nil {
		return manualRecoveryApprovalRecord{}, err
	}
	var record manualRecoveryApprovalRecord
	if err := decodeAuthorityValue(data, &record); err != nil || record.Version != manualAttestationVersion || record.Object.UID == "" {
		return manualRecoveryApprovalRecord{}, errors.Join(err, fencing.ErrClosed)
	}
	return record, nil
}

func (e *FencingExecutor) exactManualTerminalSuccessorClaim(
	ctx context.Context,
	request ArchiveForkGenesisRequest,
	scope fencing.ArchiveScope,
	state fenceLedger,
	fence fenceResource,
	recoveryUID string,
) (bool, error) {
	if e == nil || e.AuthorityState == nil {
		return false, errors.New("canonical source successor authority is unavailable")
	}
	claim, err := e.AuthorityState.readSourceSuccessorClaim(ctx, scope, request.StatefulSetUID)
	if errors.Is(err, errAuthorityStateNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	scopeHash, err := scope.Hash()
	if err != nil {
		return false, err
	}
	want := sourceSuccessorClaim{
		Version: sourceSuccessorClaimVersion, SourceScopeHash: scopeHash, SourceClusterID: scope.ClusterID,
		StatefulSetUID: request.StatefulSetUID, RecoveryResourceUID: recoveryUID, RecoveryMode: RecoveryModeFrozenSource,
		TargetBaseKind: "fork", TargetClusterID: request.TargetClusterID, OperationID: request.OperationID,
		FenceOperationID: request.SourceFenceOperationID, ClosedRootDigest: e.ledgerHash(state),
		FenceDigest: stableSourceFenceEvidenceDigest(fence), EvidenceKind: ManualTerminalEvidenceKind,
		ManualRecoveryChallengeDigest:   request.ManualRecoveryChallengeDigest,
		ManualTerminalAttestationDigest: request.ManualTerminalAttestationDigest,
	}
	return claim == want, nil
}

func decodeManualRecoveryAttestationObject(raw object) (ManualRecoveryAttestationObject, error) {
	if str(raw["apiVersion"]) != "rhiza.mrchypark.dev/v1alpha1" || str(raw["kind"]) != "RhizaManualRecoveryAttestation" {
		return ManualRecoveryAttestationObject{}, errors.New("manual recovery approval API type differs")
	}
	metadata := asObject(raw["metadata"])
	createdAt, err := time.Parse(time.RFC3339Nano, str(metadata["creationTimestamp"]))
	if err != nil {
		return ManualRecoveryAttestationObject{}, fmt.Errorf("manual recovery approval creation time is invalid: %w", err)
	}
	var spec ManualRecoveryAttestationSpec
	encoded, err := json.Marshal(raw["spec"])
	if err != nil || decodeAuthorityValue(encoded, &spec) != nil {
		return ManualRecoveryAttestationObject{}, errors.New("manual recovery approval spec is invalid")
	}
	return ManualRecoveryAttestationObject{
		Namespace: str(metadata["namespace"]),
		Name:      str(metadata["name"]),
		UID:       str(metadata["uid"]),
		CreatedAt: createdAt,
		Spec:      spec,
	}, nil
}

func manualRecoveryAttestationPath(namespace, name string) string {
	return path.Join("/apis/rhiza.mrchypark.dev/v1alpha1/namespaces", url.PathEscape(namespace), "rhizamanualrecoveryattestations", url.PathEscape(name))
}
