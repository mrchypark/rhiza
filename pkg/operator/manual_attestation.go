package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

const manualAttestationVersion = 1

const manualAttestationMaxLifetime = time.Hour

const ManualTerminalEvidenceKind = recovery.FrozenSourceManualTerminalEvidenceKind

// ManualRecoveryBinding is recomputed from canonical CONTROL state and live
// Kubernetes/archive reads at both attestation acceptance and proof use.
// Digests are lowercase SHA-256 hex strings; PodUIDs must be sorted uniquely.
type ManualRecoveryBinding struct {
	EvidenceKind         string   `json:"evidenceKind"`
	RecoveryNamespace    string   `json:"recoveryNamespace"`
	RecoveryName         string   `json:"recoveryName"`
	RecoveryUID          string   `json:"recoveryUID"`
	RecoveryID           string   `json:"recoveryID"`
	OperationID          string   `json:"operationID"`
	SourceClusterID      string   `json:"sourceClusterID"`
	StatefulSetName      string   `json:"statefulSetName"`
	StatefulSetUID       string   `json:"statefulSetUID"`
	FenceRequestHash     string   `json:"fenceRequestHash"`
	PolicyDigest         string   `json:"policyDigest"`
	RootDigest           string   `json:"rootDigest"`
	LedgerDigest         string   `json:"ledgerDigest"`
	CohortDigest         string   `json:"cohortDigest"`
	StorageScopeDigest   string   `json:"storageScopeDigest"`
	SourceArchiveDigest  string   `json:"sourceArchiveDigest"`
	TargetClusterID      string   `json:"targetClusterID"`
	TargetBaseKind       string   `json:"targetBaseKind"`
	TargetMembershipHash string   `json:"targetMembershipHash,omitempty"`
	CurrentPodUIDs       []string `json:"currentPodUIDs"`
}

// ManualRecoveryChallenge is the immutable challenge persisted by the fencer
// in canonical CONTROL evidence. It is not sourced from Recovery status.
type ManualRecoveryChallenge struct {
	Version int `json:"version"`
	ManualRecoveryBinding
	IssuedAt  time.Time `json:"issuedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// ManualRecoveryAttestationSpec is the immutable administrator-created CRD
// spec. Attester is descriptive; Kubernetes create authorization and audit are
// the identity boundary.
type ManualRecoveryAttestationSpec struct {
	Version         int    `json:"version"`
	ChallengeDigest string `json:"challengeDigest"`
	ManualRecoveryBinding
	Assertions   ManualRecoveryAssertions `json:"assertions"`
	Attester     string                   `json:"attester"`
	EvidenceRefs []string                 `json:"evidenceRefs"`
}

type ManualRecoveryAssertions struct {
	AllHistoricalRunsTerminated        bool `json:"allHistoricalRunsTerminated"`
	RecreationBlocked                  bool `json:"recreationBlocked"`
	OldPeerAndClientWriteAccessBlocked bool `json:"oldPeerAndClientWriteAccessBlocked"`
	OldArchiveWriterAccessRevoked      bool `json:"oldArchiveWriterAccessRevoked"`
	StorageQuiesced                    bool `json:"storageQuiesced"`
}

// ManualRecoveryAttestationObject contains the API-server identity metadata
// that must be retained in canonical CONTROL evidence after the exact GET.
type ManualRecoveryAttestationObject struct {
	Namespace string
	Name      string
	UID       string
	CreatedAt time.Time
	Spec      ManualRecoveryAttestationSpec
}

// ManualRecoveryAttestationName returns a deterministic, DNS-safe lookup name
// without embedding the Recovery UID itself. The exact UID remains in spec and
// is checked after the fencer's GET.
func ManualRecoveryAttestationName(recoveryUID string) (string, error) {
	if strings.TrimSpace(recoveryUID) == "" || len(recoveryUID) > 128 {
		return "", errors.New("invalid Recovery UID")
	}
	sum := sha256.Sum256([]byte(recoveryUID))
	return "mra-" + hex.EncodeToString(sum[:]), nil
}

// ManualRecoveryChallengeDigest returns the stable digest used by the
// attestation object and the typed HTTP 202 challenge response.
func ManualRecoveryChallengeDigest(challenge ManualRecoveryChallenge) (string, error) {
	if err := validateManualRecoveryBinding(challenge.ManualRecoveryBinding); err != nil {
		return "", fmt.Errorf("invalid challenge binding: %w", err)
	}
	if challenge.Version != manualAttestationVersion {
		return "", errors.New("unsupported manual recovery challenge version")
	}
	if challenge.IssuedAt.IsZero() || challenge.ExpiresAt.IsZero() || !challenge.ExpiresAt.After(challenge.IssuedAt) || challenge.ExpiresAt.Sub(challenge.IssuedAt) > manualAttestationMaxLifetime {
		return "", errors.New("invalid manual recovery challenge lifetime")
	}
	payload, err := json.Marshal(challenge)
	if err != nil {
		return "", fmt.Errorf("marshal manual recovery challenge: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateManualRecoveryAttestation verifies the immutable approval against
// the canonical challenge and a freshly recomputed source/target binding. The
// caller must run it both when accepting the approval and when consuming the
// resulting proof. expectedObjectUID is empty only before the first immutable
// approval record has been accepted; subsequent checks must pass the retained
// API-server object UID.
func ValidateManualRecoveryAttestation(
	challenge ManualRecoveryChallenge,
	approval ManualRecoveryAttestationObject,
	current ManualRecoveryBinding,
	expectedObjectUID string,
	now time.Time,
) error {
	challengeDigest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		return err
	}
	if now.Before(challenge.IssuedAt) || !now.Before(challenge.ExpiresAt) {
		return errors.New("manual recovery challenge is not currently valid")
	}
	if err := validateManualRecoveryBinding(current); err != nil {
		return fmt.Errorf("invalid current recovery binding: %w", err)
	}
	if !sameManualRecoveryBinding(challenge.ManualRecoveryBinding, current) {
		return errors.New("canonical recovery binding changed after challenge")
	}
	if approval.Namespace != challenge.RecoveryNamespace || approval.Name != mustManualRecoveryAttestationName(challenge.RecoveryUID) || approval.UID == "" {
		return errors.New("manual recovery attestation object identity mismatch")
	}
	if expectedObjectUID != "" && approval.UID != expectedObjectUID {
		return errors.New("manual recovery attestation object UID changed")
	}
	if approval.CreatedAt.IsZero() || approval.CreatedAt.Before(challenge.IssuedAt) || !approval.CreatedAt.Before(challenge.ExpiresAt) || approval.CreatedAt.After(now) {
		return errors.New("manual recovery attestation was created outside the challenge window")
	}
	spec := approval.Spec
	if spec.Version != manualAttestationVersion || spec.ChallengeDigest != challengeDigest {
		return errors.New("manual recovery attestation challenge mismatch")
	}
	if !sameManualRecoveryBinding(spec.ManualRecoveryBinding, challenge.ManualRecoveryBinding) {
		return errors.New("manual recovery attestation binding mismatch")
	}
	if !spec.Assertions.AllHistoricalRunsTerminated || !spec.Assertions.RecreationBlocked || !spec.Assertions.OldPeerAndClientWriteAccessBlocked || !spec.Assertions.OldArchiveWriterAccessRevoked || !spec.Assertions.StorageQuiesced {
		return errors.New("manual recovery attestation is incomplete")
	}
	if strings.TrimSpace(spec.Attester) == "" || len(spec.Attester) > 256 {
		return errors.New("manual recovery attestation requires a bounded attester reference")
	}
	if len(spec.EvidenceRefs) == 0 || len(spec.EvidenceRefs) > 32 {
		return errors.New("manual recovery attestation requires bounded evidence references")
	}
	for _, ref := range spec.EvidenceRefs {
		if strings.TrimSpace(ref) == "" || len(ref) > 512 {
			return errors.New("manual recovery attestation has an invalid evidence reference")
		}
	}
	return nil
}

func validateManualRecoveryBinding(binding ManualRecoveryBinding) error {
	required := []struct {
		name, value string
	}{
		{"Recovery namespace", binding.RecoveryNamespace},
		{"evidence kind", binding.EvidenceKind},
		{"Recovery name", binding.RecoveryName},
		{"Recovery UID", binding.RecoveryUID},
		{"Recovery ID", binding.RecoveryID},
		{"operation ID", binding.OperationID},
		{"source cluster ID", binding.SourceClusterID},
		{"StatefulSet name", binding.StatefulSetName},
		{"StatefulSet UID", binding.StatefulSetUID},
		{"target cluster ID", binding.TargetClusterID},
		{"target base kind", binding.TargetBaseKind},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is empty", field.name)
		}
	}
	for _, field := range []struct {
		name, value string
		max         int
	}{
		{"Recovery namespace", binding.RecoveryNamespace, 63},
		{"Recovery name", binding.RecoveryName, 253},
		{"Recovery UID", binding.RecoveryUID, 128},
		{"Recovery ID", binding.RecoveryID, 256},
		{"operation ID", binding.OperationID, 256},
		{"source cluster ID", binding.SourceClusterID, 256},
		{"StatefulSet name", binding.StatefulSetName, 253},
		{"StatefulSet UID", binding.StatefulSetUID, 128},
		{"target cluster ID", binding.TargetClusterID, 256},
		{"target base kind", binding.TargetBaseKind, 16},
	} {
		if len(field.value) > field.max {
			return fmt.Errorf("%s exceeds %d bytes", field.name, field.max)
		}
	}
	for _, field := range []struct{ name, value string }{
		{"policy digest", binding.PolicyDigest},
		{"root digest", binding.RootDigest},
		{"ledger digest", binding.LedgerDigest},
		{"cohort digest", binding.CohortDigest},
		{"storage scope digest", binding.StorageScopeDigest},
		{"source archive digest", binding.SourceArchiveDigest},
		{"fence request hash", binding.FenceRequestHash},
	} {
		if !isSHA256Hex(field.value) {
			return fmt.Errorf("%s is not a lowercase SHA-256 digest", field.name)
		}
	}
	if binding.EvidenceKind != ManualTerminalEvidenceKind {
		return errors.New("unsupported manual recovery evidence kind")
	}
	if binding.TargetBaseKind != "fork" {
		return errors.New("manual terminal recovery currently requires a non-empty fork base")
	}
	if binding.TargetMembershipHash != "" && !isSHA256Hex(binding.TargetMembershipHash) {
		return errors.New("target membership hash is not a lowercase SHA-256 digest")
	}
	if binding.CurrentPodUIDs == nil || len(binding.CurrentPodUIDs) > 64 {
		return errors.New("current Pod UID inventory must be present with at most 64 entries")
	}
	for i, uid := range binding.CurrentPodUIDs {
		if strings.TrimSpace(uid) == "" || len(uid) > 128 {
			return errors.New("current Pod UID inventory contains an invalid UID")
		}
		if i > 0 && binding.CurrentPodUIDs[i-1] >= uid {
			return errors.New("current Pod UID inventory must be sorted and unique")
		}
	}
	return nil
}

func sameManualRecoveryBinding(a, b ManualRecoveryBinding) bool {
	return a.RecoveryNamespace == b.RecoveryNamespace &&
		a.EvidenceKind == b.EvidenceKind &&
		a.RecoveryName == b.RecoveryName &&
		a.RecoveryUID == b.RecoveryUID &&
		a.RecoveryID == b.RecoveryID &&
		a.OperationID == b.OperationID &&
		a.SourceClusterID == b.SourceClusterID &&
		a.StatefulSetName == b.StatefulSetName &&
		a.StatefulSetUID == b.StatefulSetUID &&
		a.FenceRequestHash == b.FenceRequestHash &&
		a.PolicyDigest == b.PolicyDigest &&
		a.RootDigest == b.RootDigest &&
		a.LedgerDigest == b.LedgerDigest &&
		a.CohortDigest == b.CohortDigest &&
		a.StorageScopeDigest == b.StorageScopeDigest &&
		a.SourceArchiveDigest == b.SourceArchiveDigest &&
		a.TargetClusterID == b.TargetClusterID &&
		a.TargetBaseKind == b.TargetBaseKind &&
		a.TargetMembershipHash == b.TargetMembershipHash &&
		sameStrings(a.CurrentPodUIDs, b.CurrentPodUIDs)
}

func mustManualRecoveryAttestationName(recoveryUID string) string {
	name, _ := ManualRecoveryAttestationName(recoveryUID)
	return name
}

// CanonicalizePodUIDs produces the required deterministic inventory order for
// a challenge; duplicate UIDs are rejected rather than silently collapsed.
func CanonicalizePodUIDs(uids []string) ([]string, error) {
	if len(uids) > 64 {
		return nil, errors.New("current Pod UID inventory must contain at most 64 entries")
	}
	canonical := append([]string(nil), uids...)
	sort.Strings(canonical)
	for i, uid := range canonical {
		if strings.TrimSpace(uid) == "" || len(uid) > 128 || (i > 0 && canonical[i-1] == uid) {
			return nil, errors.New("current Pod UID inventory contains an invalid or duplicate UID")
		}
	}
	if canonical == nil {
		canonical = []string{}
	}
	return canonical, nil
}
