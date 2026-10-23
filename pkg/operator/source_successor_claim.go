package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const sourceSuccessorClaimVersion = 1

const sourceSuccessorModeLegacy = "sealed-source-v1"

// sourceSuccessorClaim is the one-time arbitration record for recovery from a
// closed source generation. It lives under the protected authority prefix,
// never under the source archive data prefix.
type sourceSuccessorClaim struct {
	Version             int    `json:"version"`
	SourceScopeHash     string `json:"sourceScopeHash"`
	SourceClusterID     string `json:"sourceClusterID"`
	StatefulSetUID      string `json:"statefulSetUID"`
	RecoveryResourceUID string `json:"recoveryResourceUID"`
	RecoveryMode        string `json:"recoveryMode"`
	TargetBaseKind      string `json:"targetBaseKind"`
	TargetClusterID     string `json:"targetClusterID"`
	OperationID         string `json:"operationID"`
	FenceOperationID    string `json:"fenceOperationID"`
	ClosedRootDigest    string `json:"closedRootDigest"`
	FenceDigest         string `json:"fenceDigest"`
	ReservationDigest   string `json:"reservationDigest,omitempty"`
}

func (c sourceSuccessorClaim) validate(scope fencing.ArchiveScope) error {
	scopeHash, err := scope.Hash()
	if err != nil {
		return err
	}
	if c.Version != sourceSuccessorClaimVersion || c.SourceScopeHash != scopeHash || c.SourceClusterID != scope.ClusterID ||
		c.StatefulSetUID == "" || c.RecoveryResourceUID == "" ||
		(c.RecoveryMode != RecoveryModeFrozenSource && c.RecoveryMode != sourceSuccessorModeLegacy) ||
		(c.TargetBaseKind != "fork" && c.TargetBaseKind != "empty") || c.TargetClusterID == "" || c.OperationID == "" ||
		c.FenceOperationID == "" || !validEvidenceDigest(c.ClosedRootDigest) || !validEvidenceDigest(c.FenceDigest) {
		return fmt.Errorf("fencing: invalid canonical source successor claim")
	}
	if c.OperationID != c.RecoveryResourceUID+":"+c.FenceOperationID {
		return fmt.Errorf("fencing: source successor claim operation does not bind its recovery and fence identities")
	}
	if c.RecoveryMode == RecoveryModeFrozenSource {
		if c.ReservationDigest != "" {
			return fmt.Errorf("fencing: frozen successor claim cannot bind a legacy reservation")
		}
		identity := recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: scope.ClusterID, StatefulSetUID: c.StatefulSetUID}
		target, err := recovery.FrozenTargetID(identity)
		if err != nil || target != c.TargetClusterID {
			return fmt.Errorf("fencing: frozen successor claim target is not stable for its source")
		}
	} else if !validEvidenceDigest(c.ReservationDigest) {
		return fmt.Errorf("fencing: legacy successor claim does not bind its exact reservation")
	} else if c.TargetClusterID != "rhiza-r-"+shortID(c.OperationID) {
		return fmt.Errorf("fencing: legacy successor claim target differs from its operation")
	}
	return nil
}

func sourceSuccessorClaimIdentity(scope fencing.ArchiveScope, statefulSetUID string) (string, string, error) {
	scopeHash, err := scope.Hash()
	if err != nil {
		return "", "", err
	}
	identity := hashJSON([]any{"rhiza-source-successor-v1", scopeHash, scope.ClusterID, statefulSetUID})
	return identity, scopeHash, nil
}

func (s *AuthorityStateStore) sourceSuccessorClaimKey(scope fencing.ArchiveScope, statefulSetUID string) (string, string, error) {
	identity, _, err := sourceSuccessorClaimIdentity(scope, statefulSetUID)
	if err != nil || s == nil || s.bucket == nil {
		return "", "", errors.Join(err, fmt.Errorf("fencing: source successor authority is unavailable"))
	}
	return s.key("source-successors", identity), identity, nil
}

func (s *AuthorityStateStore) readSourceSuccessorClaim(ctx context.Context, scope fencing.ArchiveScope, statefulSetUID string) (sourceSuccessorClaim, error) {
	key, identity, err := s.sourceSuccessorClaimKey(scope, statefulSetUID)
	if err != nil {
		return sourceSuccessorClaim{}, err
	}
	data, _, err := s.read(ctx, key, identity)
	if err != nil {
		return sourceSuccessorClaim{}, err
	}
	var claim sourceSuccessorClaim
	if err := decodeAuthorityValue(data, &claim); err != nil {
		return sourceSuccessorClaim{}, err
	}
	if err := claim.validate(scope); err != nil || claim.StatefulSetUID != statefulSetUID {
		return sourceSuccessorClaim{}, errors.Join(err, fmt.Errorf("fencing: source successor claim identity differs"))
	}
	return claim, nil
}

func (s *AuthorityStateStore) claimSourceSuccessor(ctx context.Context, scope fencing.ArchiveScope, claim sourceSuccessorClaim) error {
	if err := claim.validate(scope); err != nil {
		return err
	}
	key, identity, err := s.sourceSuccessorClaimKey(scope, claim.StatefulSetUID)
	if err != nil {
		return err
	}
	if err := s.write(ctx, key, identity, claim, nil); err == nil {
		return nil
	}
	got, readErr := s.readSourceSuccessorClaim(ctx, scope, claim.StatefulSetUID)
	if readErr == nil && got == claim {
		return nil
	}
	return errors.Join(readErr, errAuthorityStateConflict, fmt.Errorf("fencing: source generation already chose a different successor"))
}

func (e *FencingExecutor) sourceSuccessorClaim(ctx context.Context, scope fencing.ArchiveScope, ownerUID, recoveryUID, operationID, fenceOperationID, target, targetBaseKind, mode, reservationDigest string) (sourceSuccessorClaim, error) {
	var empty sourceSuccessorClaim
	if e == nil || e.Kube == nil || e.AuthorityState == nil || ctx == nil || ownerUID == "" || recoveryUID == "" || operationID != recoveryUID+":"+fenceOperationID {
		return empty, fmt.Errorf("fencing: source successor claim identity is unavailable")
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, ownerUID, scope.ClusterID)
	if err != nil {
		return empty, err
	}
	if state.StatefulSetUID != ownerUID || state.Ledger.SourceClusterID != scope.ClusterID || state.Ledger.ClosedBy != fenceOperationID || state.Ledger.ClosedHash == "" {
		return empty, fmt.Errorf("fencing: source successor claim requires the exact closed authority root")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return empty, err
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil {
		return empty, err
	}
	request := ArchiveForkGenesisRequest{StatefulSetUID: ownerUID, SourceFenceOperationID: fenceOperationID}
	fence, err := e.readCanonicalSourceFence(ctx, request, scope.ClusterID, bindingUID, state)
	if err != nil {
		return empty, err
	}
	if fence.Spec.OperationID != fenceOperationID || fence.Spec.StatefulSetUID != ownerUID || fence.Spec.SourceClusterID != scope.ClusterID || fence.Spec.Scope != ScopeGeneration {
		return empty, fmt.Errorf("fencing: source successor claim fence identity differs")
	}
	requestHash, err := requestHash(&fence.Spec)
	if err != nil || requestHash != state.Ledger.ClosedHash {
		return empty, fmt.Errorf("fencing: source successor claim fence does not close the canonical root")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return empty, err
	}
	if proof := fence.Status.Proof.IndependentV2; proof != nil {
		if err := validateIndependentV2(fence.Spec, proof); err != nil || proof.LedgerHash != e.ledgerHash(state) {
			return empty, errors.Join(err, fmt.Errorf("fencing: source successor claim proof differs from the closed root"))
		}
		if err := verifyIndependentProofCoverage(state, fence.Spec, proof); err != nil {
			return empty, err
		}
		sts, err := e.archiveForkStatefulSet(ctx, ownerUID)
		if err != nil {
			return empty, err
		}
		storageScope, err := e.storageScope(ctx, sts, scope.ClusterID)
		if err != nil || proof.StorageScope != storageScope {
			return empty, errors.Join(err, fmt.Errorf("fencing: source successor claim proof names another ObjectStorage scope"))
		}
	} else if mode == RecoveryModeFrozenSource {
		return empty, fmt.Errorf("fencing: frozen source successor requires independent v2 proof")
	} else if err := e.verifyCooperativeArchiveForkSource(ctx, request, scope.ClusterID, state); err != nil {
		return empty, err
	}
	if mode != RecoveryModeFrozenSource && mode != sourceSuccessorModeLegacy {
		return empty, fmt.Errorf("fencing: unsupported source successor mode")
	}
	if mode == sourceSuccessorModeLegacy && target != "rhiza-r-"+shortID(operationID) {
		return empty, fmt.Errorf("fencing: legacy successor target differs from its operation")
	}
	scopeHash, _ := scope.Hash()
	claim := sourceSuccessorClaim{
		Version: sourceSuccessorClaimVersion, SourceScopeHash: scopeHash, SourceClusterID: scope.ClusterID,
		StatefulSetUID: ownerUID, RecoveryResourceUID: recoveryUID, RecoveryMode: mode,
		TargetBaseKind: targetBaseKind, TargetClusterID: target, OperationID: operationID,
		FenceOperationID: fenceOperationID, ClosedRootDigest: e.ledgerHash(state),
		FenceDigest: stableSourceFenceEvidenceDigest(fence), ReservationDigest: reservationDigest,
	}
	if err := claim.validate(scope); err != nil {
		return empty, err
	}
	return claim, nil
}

func (e *FencingExecutor) claimSourceSuccessor(ctx context.Context, scope fencing.ArchiveScope, ownerUID, recoveryUID, operationID, fenceOperationID, target, targetBaseKind, mode, reservationDigest string) error {
	claim, err := e.sourceSuccessorClaim(ctx, scope, ownerUID, recoveryUID, operationID, fenceOperationID, target, targetBaseKind, mode, reservationDigest)
	if err != nil {
		return err
	}
	return e.AuthorityState.claimSourceSuccessor(ctx, scope, claim)
}

func (e *FencingExecutor) verifySourceSuccessorClaim(ctx context.Context, scope fencing.ArchiveScope, ownerUID, recoveryUID, operationID, fenceOperationID, target, targetBaseKind, mode, reservationDigest string) error {
	expected, err := e.sourceSuccessorClaim(ctx, scope, ownerUID, recoveryUID, operationID, fenceOperationID, target, targetBaseKind, mode, reservationDigest)
	if err != nil {
		return err
	}
	actual, err := e.AuthorityState.readSourceSuccessorClaim(ctx, scope, ownerUID)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("fencing: existing source successor claim differs from this recovery")
	}
	return nil
}

func (c *Controller) verifyOrFindSuccessorReservation(ctx context.Context, sourcePrefix string, want object) (bool, string, error) {
	key := path.Join(sourcePrefix, "recovery/successor.json")
	exists, err := c.Bucket.Exists(ctx, key)
	if err != nil || !exists {
		if err != nil {
			return false, "", err
		}
		return false, authorityStateDigest(jsonBytes(want)), nil
	}
	data, err := readBoundedArchiveObject(ctx, c.Bucket, key, 64<<10)
	if err != nil {
		return true, "", err
	}
	var got archiveForkReservation
	if err := decodeLegacySuccessorReservation(data, &got); err != nil {
		return true, "", err
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		return true, "", err
	}
	var expected archiveForkReservation
	if err := json.Unmarshal(wantBytes, &expected); err != nil {
		return true, "", err
	}
	if got.Version != expected.Version || got.OperationID != expected.OperationID || got.Target != expected.Target || got.Source != expected.Source ||
		got.SourceMembership != expected.SourceMembership || got.StatefulSetUID != expected.StatefulSetUID || got.SpecHash != expected.SpecHash ||
		(got.TargetBaseKind != "" && got.TargetBaseKind != expected.TargetBaseKind) {
		return true, "", fmt.Errorf("existing reservation names a different recovery or target")
	}
	return true, authorityStateDigest(data), nil
}

func readLegacySuccessorReservation(ctx context.Context, bucket objstore.Bucket, sourcePrefix string, want archiveForkReservation) (string, error) {
	if bucket == nil {
		return "", fmt.Errorf("fencing: legacy successor reservation store is unavailable")
	}
	key := path.Join(sourcePrefix, "recovery/successor.json")
	data, err := readBoundedArchiveObject(ctx, bucket, key, 64<<10)
	if err != nil {
		return "", err
	}
	var got archiveForkReservation
	if err := decodeLegacySuccessorReservation(data, &got); err != nil {
		return "", err
	}
	if got.Version != want.Version || got.OperationID != want.OperationID || got.Target != want.Target || got.Source != want.Source ||
		got.SourceMembership != want.SourceMembership || got.StatefulSetUID != want.StatefulSetUID || got.SpecHash != want.SpecHash ||
		(got.TargetBaseKind != "" && got.TargetBaseKind != want.TargetBaseKind) {
		return "", fmt.Errorf("fencing: legacy successor reservation differs from the source generation claim")
	}
	return authorityStateDigest(data), nil
}

// decodeLegacySuccessorReservation preserves the map-key ordering emitted by
// Controller.reservation while still requiring an exact canonical payload.
func decodeLegacySuccessorReservation(data []byte, target *archiveForkReservation) error {
	if target == nil {
		return fmt.Errorf("fencing: legacy successor reservation target is unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("fencing: trailing legacy successor reservation JSON")
	}
	canonical := object{
		"version": target.Version, "operation_id": target.OperationID, "target": target.Target,
		"source": target.Source, "source_membership": target.SourceMembership,
		"statefulset_uid": target.StatefulSetUID, "spec_hash": target.SpecHash,
	}
	if target.TargetBaseKind != "" {
		canonical["target_base_kind"] = target.TargetBaseKind
	}
	encoded, err := json.Marshal(canonical)
	if err != nil || !bytes.Equal(encoded, data) {
		return errors.Join(err, fmt.Errorf("fencing: noncanonical legacy successor reservation JSON"))
	}
	return nil
}
