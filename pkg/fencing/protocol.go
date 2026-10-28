// Package fencing implements the cooperative process fencing protocol.
package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

const ProtocolVersion = 1

var (
	ErrPending     = errors.New("fencing: evidence pending")
	ErrClosed      = errors.New("fencing: incarnation terminally fenced")
	ErrUnavailable = errors.New("fencing: authority temporarily unavailable")
	ErrNotFound    = errors.New("fencing: immutable peer-closure request not found")
)

// Identity binds one supervisor launch; WALIdentity is assigned once, before writers start.
type Identity struct {
	SourceClusterID string `json:"sourceClusterID"`
	NodeID          string `json:"nodeID"`
	// PodName is empty only for legacy bootstrap identities, where NodeID must
	// remain the actual Pod name. Replacement identities bind the Kubernetes
	// name separately from their UID-derived consensus ID.
	PodName        string `json:"podName,omitempty"`
	PodUID         string `json:"podUID"`
	CredentialUID  string `json:"credentialUID,omitempty"`
	CredentialName string `json:"credentialName,omitempty"`
	RunID          string `json:"runID"`
	WALIdentity    string `json:"walIdentity"`
}

// ReplacementNodeID makes each Pod incarnation a distinct consensus identity.
// Kubernetes UIDs are preserved exactly; malformed values fail closed.
func ReplacementNodeID(podUID string) (string, error) {
	if podUID == "" || strings.TrimSpace(podUID) != podUID {
		return "", fmt.Errorf("fencing: invalid Pod UID")
	}
	for _, r := range podUID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-') {
			return "", fmt.Errorf("fencing: invalid Pod UID")
		}
	}
	return "pod-" + podUID, nil
}

// ValidatePodBinding accepts exactly the legacy name identity or the new
// UID-derived replacement identity.
func ValidatePodBinding(nodeID, podName, podUID string) error {
	if nodeID == "" || podName == "" || podUID == "" {
		return fmt.Errorf("fencing: incomplete Pod identity binding")
	}
	if nodeID == podName {
		return nil
	}
	want, err := ReplacementNodeID(podUID)
	if err != nil || nodeID != want {
		return fmt.Errorf("fencing: NodeID is not bound to the Pod name and UID")
	}
	return nil
}

// PodNameForIdentity keeps old ledger records readable without treating their
// empty field as authority for a new identity.
func PodNameForIdentity(id Identity) string {
	if id.PodName != "" {
		return id.PodName
	}
	return id.NodeID
}

// ValidateIdentity checks only the identity's self-contained fields. Legacy
// name-based records remain valid; new records must carry the exact UID-derived
// NodeID and the immutable credential object UID.
func (id Identity) Validate() error {
	if id.SourceClusterID == "" || id.NodeID == "" || id.PodUID == "" || id.RunID == "" {
		return fmt.Errorf("fencing: incomplete identity")
	}
	if id.PodName == "" {
		if id.CredentialUID != "" || id.CredentialName != "" {
			return fmt.Errorf("fencing: legacy identity cannot bind a replacement credential")
		}
		return nil
	}
	want, err := ReplacementNodeID(id.PodUID)
	if err != nil || id.NodeID != want || id.CredentialUID == "" || id.CredentialName == "" {
		return fmt.Errorf("fencing: replacement identity binding differs")
	}
	return nil
}

// Registration is returned by the authority before the child process starts.
// It contains only the new incarnation's scalar peer secret and self-member;
// neither is persisted in the fencing ledger.
type Registration struct {
	Identity  Identity `json:"identity"`
	PeerToken string   `json:"peerToken,omitempty"`
	Learner   []byte   `json:"learner,omitempty"`
}

// BootstrapPurpose identifies the existing authorization which permits one
// immutable pre-runtime assignment. It is not a caller-selected bootstrap
// mode: the authority derives it from retained operator/fence state.
type BootstrapPurpose string

const (
	BootstrapInitial     BootstrapPurpose = "initial"
	BootstrapLearner     BootstrapPurpose = "learner"
	BootstrapDRSuccessor BootstrapPurpose = "dr-successor"
)

// BootstrapAssignment binds an existing authorization to one exact Pod
// incarnation and credential. LedgerUID pins the Kubernetes ledger object;
// replacing that object is history loss, not a fresh bootstrap.
type BootstrapAssignment struct {
	OperationID       string           `json:"operationID"`
	RequestHash       string           `json:"requestHash"`
	AuthorizationHash string           `json:"authorizationHash"`
	Purpose           BootstrapPurpose `json:"purpose"`
	RootUID           string           `json:"rootUID"`
	StatefulSetUID    string           `json:"statefulSetUID"`
	SourceClusterID   string           `json:"sourceClusterID"`
	PodName           string           `json:"podName"`
	PodUID            string           `json:"podUID"`
	NodeID            string           `json:"nodeID"`
	CredentialName    string           `json:"credentialName"`
	CredentialUID     string           `json:"credentialUID"`
	MembershipHash    string           `json:"membershipHash"`
	ProfileHash       string           `json:"profileHash"`
	LedgerUID         string           `json:"ledgerUID"`
	// Learner bindings carry the exact public identity and source configuration
	// needed by pre-runtime restore. Scalar members keep assignment equality exact.
	PublicMember      BootstrapPublicMember `json:"publicMember,omitempty"`
	Durability        string                `json:"durability,omitempty"`
	StoreIdentityHash string                `json:"storeIdentityHash,omitempty"`
	// DRApproval carries the complete, externally authorized generation change
	// for a DR successor. Request and Receipt are value-comparable, so the
	// assignment remains safe to compare by value after ledger serialization.
	DRApproval BootstrapDRApproval `json:"drApproval,omitempty"`
}

// BootstrapDRApproval is the exact anchor request/receipt pair retained by
// the recovery controller. It does not replace RequestHash's existing
// bootstrap-operation meaning; the pair is covered by AuthorizationHash.
type BootstrapDRApproval struct {
	Request recoveryanchor.Request `json:"request"`
	Receipt recoveryanchor.Receipt `json:"receipt"`
}

type BootstrapPublicMember struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	PeerURL     string `json:"peerURL"`
	LogURL      string `json:"logURL"`
	PublicKey   string `json:"publicKey"`
	WALIdentity string `json:"walIdentity"`
}

func (a BootstrapAssignment) Validate() error {
	if a.OperationID == "" || a.RequestHash == "" || a.AuthorizationHash == "" ||
		(a.Purpose != BootstrapInitial && a.Purpose != BootstrapLearner && a.Purpose != BootstrapDRSuccessor) ||
		a.RootUID == "" || a.StatefulSetUID == "" || a.SourceClusterID == "" ||
		a.PodName == "" || a.PodUID == "" || a.NodeID == "" || a.CredentialName == "" ||
		a.CredentialUID == "" || a.MembershipHash == "" || a.ProfileHash == "" || a.LedgerUID == "" {
		return fmt.Errorf("fencing: incomplete bootstrap assignment")
	}
	if a.Purpose == BootstrapLearner {
		want, err := ReplacementNodeID(a.PodUID)
		if err != nil || a.NodeID != want {
			return fmt.Errorf("fencing: learner assignment NodeID is not UID-bound")
		}
		if a.PublicMember.ID != "" {
			key, keyErr := hex.DecodeString(a.PublicMember.PublicKey)
			if a.PublicMember.ID != a.NodeID || keyErr != nil || len(key) != len(quepaxa.PublicKey{}) || a.PublicMember.URL == "" || a.PublicMember.PeerURL == "" || a.PublicMember.WALIdentity != "" || a.Durability == "" || !validBootstrapHash(a.StoreIdentityHash) || !validBootstrapHash(a.MembershipHash) || !validBootstrapHash(a.ProfileHash) {
				return fmt.Errorf("fencing: learner assignment public member differs")
			}
		}
	}
	if a.Purpose == BootstrapDRSuccessor {
		approval := a.DRApproval
		r := approval.Request
		if r.AnchorID == "" || r.OperationID == "" || r.StatefulSetUID != a.StatefulSetUID || r.TargetClusterID != a.SourceClusterID ||
			r.TargetPrefix == "" || r.TargetAnchorHash == "" || r.Fork.Tip == 0 || r.Fork.PrefixHash == "" || r.Fork.ManifestHash == "" ||
			r.TargetMembership.Version == 0 || r.TargetMembership.Cluster != r.TargetClusterID || r.TargetMembership.Durability == "" || r.TargetMembership.Membership == "" ||
			approval.Receipt.AnchorID != r.AnchorID || approval.Receipt.OperationID != r.OperationID || approval.Receipt.Target.ClusterID != r.TargetClusterID ||
			approval.Receipt.Target.StorageID == "" || approval.Receipt.Generation == 0 {
			return fmt.Errorf("fencing: DR successor approval is incomplete or differs from assignment")
		}
		hash, err := recoveryanchor.RequestHash(r)
		if err != nil || approval.Receipt.RequestHash != hash {
			return fmt.Errorf("fencing: DR successor receipt does not bind the full request")
		}
	} else if a.DRApproval != (BootstrapDRApproval{}) {
		return fmt.Errorf("fencing: non-DR bootstrap assignment carries DR approval")
	}
	return nil
}

// BootstrapDRAuthorizationHash binds a DR approval to the existing
// operation-level target scope. Per-Pod fields are intentionally excluded so
// every Pod assignment in one operation shares the operation authorization.
func BootstrapDRAuthorizationHash(a BootstrapAssignment) string {
	data, _ := json.Marshal(struct {
		OperationID, RequestHash                                   string
		Purpose                                                    BootstrapPurpose
		RootUID, StatefulSetUID, SourceClusterID                   string
		MembershipHash, ProfileHash, StoreIdentityHash, Durability string
		DRApproval                                                 BootstrapDRApproval
	}{a.OperationID, a.RequestHash, a.Purpose, a.RootUID, a.StatefulSetUID, a.SourceClusterID,
		a.MembershipHash, a.ProfileHash, a.StoreIdentityHash, a.Durability, a.DRApproval})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// BootstrapAssignmentDigest is the canonical provenance digest written by an
// initial-genesis marker. It binds the digest to the exact retained assignment,
// not merely to a caller-selected materialization mode.
func BootstrapAssignmentDigest(assignment BootstrapAssignment) string {
	data, _ := json.Marshal(assignment)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// InitExecutionScope is populated by the authority from live Kubernetes
// runtime status, never trusted from an init caller. ContainerID identifies
// one actual execution; a new nonce cannot adopt that same execution.
type InitExecutionScope struct {
	PodName       string `json:"podName"`
	PodUID        string `json:"podUID"`
	ContainerName string `json:"containerName"`
	ContainerID   string `json:"containerID"`
	ImageID       string `json:"imageID"`
	NodeName      string `json:"nodeName"`
	NodeUID       string `json:"nodeUID"`
}

func (s InitExecutionScope) ValidateFor(a BootstrapAssignment) error {
	if s.PodName != a.PodName || s.PodUID != a.PodUID || s.ContainerName == "" ||
		s.ContainerID == "" || s.ImageID == "" || s.NodeName == "" || s.NodeUID == "" {
		return fmt.Errorf("fencing: init execution scope is incomplete or differs from assignment")
	}
	return nil
}

type InitAttemptState string

const (
	InitAttemptStarted      InitAttemptState = "started"
	InitAttemptMaterialized InitAttemptState = "materialized"
	InitAttemptFailed       InitAttemptState = "failed"
)

const (
	InitFailureValidation = "validation"
	InitFailureRestore    = "restore"
	InitFailureGenesis    = "genesis"
	InitFailureCleanup    = "cleanup"
	InitFailureOther      = "other"
)

// InitAttemptOutcome is a live local result, not process termination or
// storage-settlement evidence. FailureClass is bounded and contains no raw
// error text or secrets.
type InitAttemptOutcome struct {
	State         InitAttemptState `json:"state"`
	FailureClass  string           `json:"failureClass,omitempty"`
	GenesisDigest string           `json:"genesisDigest,omitempty"`
	LiveCleanup   InitLiveCleanup  `json:"liveCleanup"`
}

// InitLiveCleanup records bounded facts from this still-live Node process.
// These flags are not terminal process evidence or storage-settlement proof.
type InitLiveCleanup struct {
	Mode                 string `json:"mode"` // restore, marker-noop, or genesis
	BucketDrainSucceeded bool   `json:"bucketDrainSucceeded"`
	CleanupSucceeded     bool   `json:"cleanupSucceeded"`
	NamespaceQuiescent   bool   `json:"namespaceQuiescent"`
}

const (
	InitModeRestore    = "restore"
	InitModeMarkerNoop = "marker-noop"
	InitModeGenesis    = "genesis"
)

func (o InitAttemptOutcome) Validate() error {
	cleanup := o.LiveCleanup
	validMode := cleanup.Mode == InitModeRestore || cleanup.Mode == InitModeMarkerNoop || cleanup.Mode == InitModeGenesis
	if cleanup.Mode != "" && !validMode {
		return fmt.Errorf("fencing: unknown init cleanup mode")
	}
	if (cleanup.Mode == InitModeMarkerNoop || cleanup.Mode == InitModeGenesis) && cleanup.BucketDrainSucceeded {
		return fmt.Errorf("fencing: non-draining init mode claims bucket-drain success")
	}
	switch o.State {
	case InitAttemptMaterialized:
		if o.FailureClass != "" || !validMode || !cleanup.CleanupSucceeded || !cleanup.NamespaceQuiescent {
			return fmt.Errorf("fencing: materialized init outcome lacks successful cleanup and namespace quiescence")
		}
		if cleanup.Mode == InitModeRestore && !cleanup.BucketDrainSucceeded {
			return fmt.Errorf("fencing: restore init outcome lacks bucket-drain success")
		}
		if cleanup.Mode == InitModeGenesis && o.GenesisDigest == "" {
			return fmt.Errorf("fencing: genesis init outcome lacks assignment provenance")
		}
		if cleanup.Mode != InitModeGenesis && o.GenesisDigest != "" {
			return fmt.Errorf("fencing: non-genesis init outcome carries genesis provenance")
		}
		if o.GenesisDigest != "" {
			if len(o.GenesisDigest) != 64 {
				return fmt.Errorf("fencing: invalid init genesis digest")
			}
			for _, r := range o.GenesisDigest {
				if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
					return fmt.Errorf("fencing: invalid init genesis digest")
				}
			}
		}
	case InitAttemptFailed:
		if o.GenesisDigest != "" || (cleanup.Mode == "" && (cleanup.BucketDrainSucceeded || cleanup.CleanupSucceeded || cleanup.NamespaceQuiescent)) {
			return fmt.Errorf("fencing: failed init outcome cannot carry a genesis digest")
		}
		switch o.FailureClass {
		case InitFailureValidation, InitFailureRestore, InitFailureGenesis, InitFailureCleanup, InitFailureOther:
		default:
			return fmt.Errorf("fencing: unknown init failure class")
		}
	default:
		return fmt.Errorf("fencing: invalid init outcome state")
	}
	return nil
}

// ValidateForAssignment applies the purpose-specific provenance contract that
// cannot be checked by validating an outcome in isolation.
func (o InitAttemptOutcome) ValidateForAssignment(assignment BootstrapAssignment) error {
	if err := assignment.Validate(); err != nil {
		return err
	}
	if err := o.Validate(); err != nil {
		return err
	}
	if o.State != InitAttemptMaterialized {
		return nil
	}
	if assignment.Purpose == BootstrapInitial {
		if o.LiveCleanup.Mode != InitModeGenesis || o.GenesisDigest != BootstrapAssignmentDigest(assignment) {
			return fmt.Errorf("fencing: initial genesis outcome does not match its authorized assignment")
		}
		return nil
	}
	if o.LiveCleanup.Mode == InitModeGenesis || o.GenesisDigest != "" {
		return fmt.Errorf("fencing: genesis outcome is not authorized for this assignment purpose")
	}
	return nil
}

type InitTerminalEvidence struct {
	ContainerID string `json:"containerID"`
	ImageID     string `json:"imageID"`
	ExitCode    int32  `json:"exitCode"`
	Reason      string `json:"reason"`
}

// InitAttempt is append-only history for one actual init process. Outcome and
// terminal observation are separate; neither can be inferred from Pod absence.
type InitAttempt struct {
	AttemptID  string                `json:"attemptID"`
	Assignment BootstrapAssignment   `json:"assignment"`
	Scope      InitExecutionScope    `json:"scope"`
	Outcomes   []InitAttemptOutcome  `json:"outcomes,omitempty"`
	Terminal   *InitTerminalEvidence `json:"terminal,omitempty"`
}

type BootstrapStatusResult struct {
	Assignment        *BootstrapAssignment `json:"assignment,omitempty"`
	AssignmentSetHash string               `json:"assignmentSetHash,omitempty"`
	Attempts          []InitAttempt        `json:"attempts,omitempty"`
	AdmissionClosed   bool                 `json:"admissionClosed,omitempty"`
	Complete          bool                 `json:"complete,omitempty"`
}

// BootstrapAssignmentSetHash deterministically binds the complete retained
// assignment set returned by the authority; callers cannot invent completion
// scope from their local view of one Pod.
func BootstrapAssignmentSetHash(assignments []BootstrapAssignment) string {
	ordered := append([]BootstrapAssignment(nil), assignments...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].PodUID != ordered[j].PodUID {
			return ordered[i].PodUID < ordered[j].PodUID
		}
		return ordered[i].OperationID < ordered[j].OperationID
	})
	data, _ := json.Marshal(ordered)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validBootstrapHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

type BootstrapCompletion struct {
	OperationID         string `json:"operationID"`
	AuthorizationHash   string `json:"authorizationHash"`
	AssignmentSetHash   string `json:"assignmentSetHash"`
	CertifiedBarrierRef string `json:"certifiedBarrierRef"`
}

// MembershipRepairAuthorization is the immutable identity and membership
// scope approved for one ordinary same-generation repair. Runtime progress is
// kept separately so a retry cannot rewrite the request that authorized it.
type MembershipRepairAuthorization struct {
	OperationID             string          `json:"operationID"`
	RequestHash             string          `json:"requestHash"`
	AuthorizationHash       string          `json:"authorizationHash"`
	RootUID                 string          `json:"rootUID"`
	StatefulSetUID          string          `json:"statefulSetUID"`
	LedgerUID               string          `json:"ledgerUID"`
	SourceClusterID         string          `json:"sourceClusterID"`
	MembershipHash          string          `json:"membershipHash"`
	ExpectedAbortSlot       *quepaxa.Slot   `json:"expectedAbortSlot,omitempty"`
	BootstrapMembershipHash string          `json:"bootstrapMembershipHash,omitempty"`
	ProfileHash             string          `json:"profileHash,omitempty"`
	StoreIdentityHash       string          `json:"storeIdentityHash,omitempty"`
	Durability              string          `json:"durability,omitempty"`
	OldIdentity             Identity        `json:"oldIdentity"`
	Membership              quepaxa.Cluster `json:"membership"`
}

func (a MembershipRepairAuthorization) Validate() error {
	if a.OperationID == "" || !validBootstrapHash(a.RequestHash) || !validBootstrapHash(a.AuthorizationHash) ||
		a.RootUID == "" || a.StatefulSetUID == "" || a.LedgerUID == "" || a.SourceClusterID == "" || !validBootstrapHash(a.MembershipHash) ||
		a.OldIdentity.Validate() != nil || a.OldIdentity.WALIdentity == "" || a.OldIdentity.SourceClusterID != a.SourceClusterID ||
		a.Membership.ConfigID == 0 || len(a.Membership.Members) < 2 || canonicalJSONHash(a.Membership.Members) != a.MembershipHash {
		return fmt.Errorf("fencing: incomplete membership-repair authorization")
	}
	if (a.BootstrapMembershipHash != "" && !validBootstrapHash(a.BootstrapMembershipHash)) ||
		(a.ProfileHash != "" && !validBootstrapHash(a.ProfileHash)) ||
		(a.StoreIdentityHash != "" && !validBootstrapHash(a.StoreIdentityHash)) {
		return fmt.Errorf("fencing: invalid membership-repair source binding")
	}
	if a.BootstrapMembershipHash != "" && (a.ExpectedAbortSlot == nil || a.ProfileHash == "" || a.StoreIdentityHash == "" || (a.Durability != "async" && a.Durability != "before-ack")) {
		return fmt.Errorf("fencing: incomplete membership-repair source binding")
	}
	seen := make(map[quepaxa.NodeID]bool, len(a.Membership.Members))
	oldMember := false
	for _, member := range a.Membership.Members {
		if member.ID == "" || seen[member.ID] || member.PublicKey == (quepaxa.PublicKey{}) || member.URL == "" || member.LogURL == "" {
			return fmt.Errorf("fencing: invalid repair membership")
		}
		seen[member.ID] = true
		if string(member.ID) == a.OldIdentity.NodeID && member.WALIdentity == a.OldIdentity.WALIdentity {
			oldMember = true
		}
	}
	if !oldMember {
		return fmt.Errorf("fencing: old repair identity is absent from frozen membership")
	}
	return nil
}

func canonicalJSONHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// MembershipRepairAuthorizationHash returns the canonical digest bound into
// completion requests for one retained ordinary repair authorization.
func MembershipRepairAuthorizationHash(a MembershipRepairAuthorization) string {
	if a.ExpectedAbortSlot == nil {
		return ""
	}
	return canonicalJSONHash(struct {
		OperationID, RequestHash, RootUID, StatefulSetUID, LedgerUID, SourceClusterID       string
		MembershipHash, BootstrapMembershipHash, ProfileHash, StoreIdentityHash, Durability string
		ExpectedAbortSlot                                                                   uint64
	}{a.OperationID, a.RequestHash, a.RootUID, a.StatefulSetUID, a.LedgerUID, a.SourceClusterID, a.MembershipHash, a.BootstrapMembershipHash, a.ProfileHash, a.StoreIdentityHash, a.Durability, uint64(*a.ExpectedAbortSlot)})
}

// MembershipRepairCompletionScopeDigest hashes the exact completion request
// scope while omitting only its self-referential digest field.
func MembershipRepairCompletionScopeDigest(request network.MembershipRepairCompletionRequest) string {
	request.ScopeDigest = ""
	return canonicalJSONHash(request)
}

// BootstrapStoreIdentityHash hashes only the non-secret object-store location
// identity produced by the operator's StoreIdentity function.
func BootstrapStoreIdentityHash(identity map[string]string) string {
	return canonicalJSONHash(identity)
}

type MembershipRepairStage string

const (
	MembershipRepairAdmitted       MembershipRepairStage = "admitted"
	MembershipRepairCandidateBound MembershipRepairStage = "candidate-bound"
	MembershipRepairOldClosed      MembershipRepairStage = "old-closed"
	MembershipRepairRemoved        MembershipRepairStage = "removed"
	MembershipRepairAdded          MembershipRepairStage = "added"
	MembershipRepairComplete       MembershipRepairStage = "complete"
	MembershipRepairCancelled      MembershipRepairStage = "cancelled"
)

type MembershipRepairPhaseKind string

const (
	MembershipRepairRemove MembershipRepairPhaseKind = "remove"
	MembershipRepairAdd    MembershipRepairPhaseKind = "add"
)

// MembershipRepairPhase is one immutable consensus round request. AbortSlot
// preserves the distinction between missing and an explicit zero revision;
// Change is the existing network request consumed by Node.
type MembershipRepairPhase struct {
	Kind            MembershipRepairPhaseKind `json:"kind"`
	CandidatePodUID string                    `json:"candidatePodUID"`
	Change          network.MembershipChange  `json:"change"`
	AbortSlot       *quepaxa.Slot             `json:"abortSlot"`
	Hash            string                    `json:"hash"`
}

func (p MembershipRepairPhase) Validate(a MembershipRepairAuthorization, candidate MembershipRepairCandidate) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if p.CandidatePodUID != candidate.Assignment.PodUID || p.AbortSlot == nil || p.Change.ExpectedAbortSlot != *p.AbortSlot || p.Change.ClusterID != a.SourceClusterID || p.Change.OperationID == "" || p.Change.ExpectedConfigID == 0 || !validBootstrapHash(p.Hash) {
		return fmt.Errorf("fencing: membership-repair phase lacks exact round binding")
	}
	switch p.Kind {
	case MembershipRepairRemove:
		if (a.ExpectedAbortSlot != nil && *p.AbortSlot != *a.ExpectedAbortSlot) || p.Change.ExpectedConfigID != a.Membership.ConfigID || p.Change.Remove != quepaxa.NodeID(a.OldIdentity.NodeID) || p.Change.Add != nil || p.Change.Fence == nil || p.Change.Fence.WALIdentity != a.OldIdentity.WALIdentity || p.Change.Fence.WorkloadUID != a.StatefulSetUID || p.Change.Fence.NodeID != p.Change.Remove {
			return fmt.Errorf("fencing: repair removal differs from retained old identity")
		}
	case MembershipRepairAdd:
		member := candidate.Member
		if candidate.Assignment.Validate() != nil || member == nil || p.Change.ExpectedConfigID != a.Membership.ConfigID+1 || p.Change.Remove != "" || p.Change.Fence != nil || p.Change.Add == nil || *p.Change.Add != *member || string(member.ID) != candidate.Assignment.NodeID || member.WALIdentity == "" {
			return fmt.Errorf("fencing: repair addition differs from bound candidate")
		}
	default:
		return fmt.Errorf("fencing: unknown membership-repair phase")
	}
	want, err := MembershipRepairPhaseHash(p)
	if err != nil || want != p.Hash {
		return fmt.Errorf("fencing: membership-repair phase hash differs")
	}
	return nil
}

func MembershipRepairPhaseHash(p MembershipRepairPhase) (string, error) {
	data, err := json.Marshal(struct {
		Kind            MembershipRepairPhaseKind `json:"kind"`
		CandidatePodUID string                    `json:"candidatePodUID"`
		Change          network.MembershipChange  `json:"change"`
		AbortSlot       *quepaxa.Slot             `json:"abortSlot"`
	}{p.Kind, p.CandidatePodUID, p.Change, p.AbortSlot})
	if err != nil {
		return "", fmt.Errorf("fencing: encode membership-repair phase: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type MembershipRepairCandidate struct {
	Assignment BootstrapAssignment    `json:"assignment"`
	Member     *quepaxa.Member        `json:"member,omitempty"`
	Attempts   map[string]InitAttempt `json:"attempts,omitempty"`
}

type MembershipRepairProgress struct {
	Revision   uint64                        `json:"revision"`
	Stage      MembershipRepairStage         `json:"stage"`
	Candidates []MembershipRepairCandidate   `json:"candidates,omitempty"`
	Phases     []MembershipRepairPhase       `json:"phases,omitempty"`
	Results    []MembershipRepairPhaseResult `json:"results,omitempty"`
	Completion *MembershipRepairCompletion   `json:"completion,omitempty"`
}

// MembershipRepairCompletion retains the one authority-issued completion
// challenge and, after independent validation, the exact authenticated runtime
// result. The accepted result and Complete stage are written in one ledger CAS.
type MembershipRepairCompletion struct {
	Request  network.MembershipRepairCompletionRequest   `json:"request"`
	Accepted *network.MembershipRepairCompletionResponse `json:"accepted,omitempty"`
}

type MembershipRepairPhaseResult struct {
	PhaseHash          string          `json:"phaseHash"`
	JournalOperationID string          `json:"journalOperationID"`
	JournalRequestHash string          `json:"journalRequestHash"`
	Target             quepaxa.Cluster `json:"target"`
	FreezeSlot         uint64          `json:"freezeSlot"`
	FreezeHash         string          `json:"freezeHash"`
	TerminalSlot       uint64          `json:"terminalSlot"`
	TerminalHash       string          `json:"terminalHash"`
	Outcome            string          `json:"outcome"`
}

type MembershipRepairView struct {
	Authorization MembershipRepairAuthorization `json:"authorization"`
	Progress      MembershipRepairProgress      `json:"progress"`
	Preclosure    *MembershipRepairPreclosure   `json:"preclosure,omitempty"`
}

func (v MembershipRepairView) ValidateFor(caller Identity) error {
	if err := v.Authorization.Validate(); err != nil {
		return err
	}
	if caller.Validate() != nil || caller.WALIdentity == "" || caller.SourceClusterID != v.Authorization.SourceClusterID {
		return fmt.Errorf("fencing: membership-repair caller is not a bound source runtime")
	}
	memberFound := false
	for _, member := range v.Authorization.Membership.Members {
		if string(member.ID) == caller.NodeID && member.WALIdentity == caller.WALIdentity {
			memberFound = true
			break
		}
	}
	if !memberFound {
		return fmt.Errorf("fencing: membership-repair caller is not a frozen voter")
	}
	if v.Progress.Stage == "" || v.Progress.Revision == 0 {
		return fmt.Errorf("fencing: membership-repair progress is unavailable")
	}
	if v.Preclosure != nil {
		pre := v.Preclosure
		request, response := pre.Request, pre.Response
		barrierNonce := network.MembershipRepairBarrierNonce(request.Challenge)
		if network.ValidateMembershipRepairProbeRequest(request) != nil || request.SourceConfigID != uint(v.Authorization.Membership.ConfigID) ||
			v.Authorization.ExpectedAbortSlot == nil || request.AbortSlot == nil || *request.AbortSlot != *v.Authorization.ExpectedAbortSlot ||
			request.ExcludedOldMemberID != v.Authorization.OldIdentity.NodeID || response.Challenge != request.Challenge || response.ScopeDigest != request.ScopeDigest ||
			response.BarrierNonce != hex.EncodeToString(barrierNonce[:]) || response.BarrierSlot < response.ReadIndexSlot ||
			response.Version != request.Version || response.VerifierNodeID == "" || response.VerifierNodeID == v.Authorization.OldIdentity.NodeID ||
			response.VerifierWALIdentity == "" || !response.Ready || !response.Voting || response.Pending || response.SourceConfigID != request.SourceConfigID ||
			response.AbortSlot == nil || *response.AbortSlot != *request.AbortSlot || response.ExcludedOldMemberID != request.ExcludedOldMemberID ||
			response.CandidateMember != request.CandidateMember || response.CandidateBindRef != request.CandidateBindRef || response.PrefixHash == "" || response.CandidatePrefixHash != response.PrefixHash || response.MAC == "" {
			return fmt.Errorf("fencing: membership-repair preclosure differs from immutable source scope")
		}
		candidateFound := false
		for _, candidate := range v.Progress.Candidates {
			if candidate.Assignment.PodUID == pre.CandidatePodUID && candidate.Member != nil && *candidate.Member == request.CandidateMember && request.CandidateBindRef == BootstrapAssignmentDigest(candidate.Assignment) &&
				request.ScopeDigest == MembershipRepairProbeScopeDigest(request.Challenge, v.Authorization, candidate.Assignment, *candidate.Member) {
				candidateFound = true
			}
		}
		if !candidateFound {
			return fmt.Errorf("fencing: membership-repair preclosure differs from retained candidate")
		}
	}
	switch v.Progress.Stage {
	case MembershipRepairAdmitted, MembershipRepairCandidateBound, MembershipRepairOldClosed, MembershipRepairRemoved, MembershipRepairAdded, MembershipRepairComplete, MembershipRepairCancelled:
	default:
		return fmt.Errorf("fencing: unknown membership-repair stage")
	}
	for _, candidate := range v.Progress.Candidates {
		assignment := candidate.Assignment
		if err := assignment.Validate(); err != nil || assignment.RootUID != v.Authorization.RootUID || assignment.StatefulSetUID != v.Authorization.StatefulSetUID || assignment.LedgerUID != v.Authorization.LedgerUID || assignment.SourceClusterID != v.Authorization.SourceClusterID {
			return fmt.Errorf("fencing: membership-repair candidate assignment differs")
		}
		if v.Authorization.BootstrapMembershipHash != "" && (assignment.MembershipHash != v.Authorization.BootstrapMembershipHash || assignment.ProfileHash != v.Authorization.ProfileHash || assignment.StoreIdentityHash != v.Authorization.StoreIdentityHash || assignment.Durability != v.Authorization.Durability || assignment.PublicMember.ID != assignment.NodeID) {
			return fmt.Errorf("fencing: learner assignment source binding differs")
		}
		if candidate.Member != nil {
			member := candidate.Member
			if string(member.ID) != assignment.NodeID || member.WALIdentity == "" || member.PublicKey == (quepaxa.PublicKey{}) {
				return fmt.Errorf("fencing: membership-repair candidate member differs")
			}
		}
		for attemptID, attempt := range candidate.Attempts {
			if attemptID == "" || attempt.AttemptID != attemptID || attempt.Assignment != assignment || attempt.Scope.ValidateFor(assignment) != nil || len(attempt.Outcomes) > 1 {
				return fmt.Errorf("fencing: membership-repair init attempt differs")
			}
			for _, outcome := range attempt.Outcomes {
				if outcome.ValidateForAssignment(assignment) != nil {
					return fmt.Errorf("fencing: membership-repair init outcome differs")
				}
			}
			if attempt.Terminal != nil && (attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID || attempt.Terminal.Reason == "") {
				return fmt.Errorf("fencing: membership-repair init terminal evidence differs")
			}
		}
	}
	if len(v.Progress.Candidates) == 0 && v.Progress.Stage != MembershipRepairAdmitted && v.Progress.Stage != MembershipRepairCancelled {
		return fmt.Errorf("fencing: membership-repair progress lacks its candidate assignment")
	}
	for _, phase := range v.Progress.Phases {
		var candidate *MembershipRepairCandidate
		for i := range v.Progress.Candidates {
			if v.Progress.Candidates[i].Assignment.PodUID == phase.CandidatePodUID {
				candidate = &v.Progress.Candidates[i]
				break
			}
		}
		if candidate == nil || phase.Validate(v.Authorization, *candidate) != nil {
			return fmt.Errorf("fencing: membership-repair phase is invalid")
		}
	}
	for _, result := range v.Progress.Results {
		var phase *MembershipRepairPhase
		for i := range v.Progress.Phases {
			if v.Progress.Phases[i].Hash == result.PhaseHash {
				phase = &v.Progress.Phases[i]
				break
			}
		}
		if phase == nil || result.JournalOperationID != phase.Change.OperationID || result.JournalRequestHash != canonicalJSONHash(phase.Change) ||
			result.Target.ConfigID != phase.Change.ExpectedConfigID+1 || result.FreezeSlot == 0 || result.TerminalSlot != result.FreezeSlot+16 ||
			!validBootstrapHash(result.FreezeHash) || !validBootstrapHash(result.TerminalHash) || result.Outcome != "completed" {
			return fmt.Errorf("fencing: membership-repair certified phase result is invalid")
		}
		if phase.Kind == MembershipRepairAdd {
			found := false
			if phase.Change.Add != nil {
				for _, member := range result.Target.Members {
					if member == *phase.Change.Add {
						found = true
					}
				}
			}
			if !found {
				return fmt.Errorf("fencing: membership-repair ADD result differs from its exact target")
			}
		}
	}
	return nil
}

type MembershipRepairStatusRequest struct {
	// TargetPodName is an optional stable StatefulSet slot selected by the
	// caller. The authority resolves its retained voter identity and the live
	// replacement Pod UID; callers never supply a WAL identity or Pod UID.
	TargetPodName string `json:"targetPodName,omitempty"`
}

type MembershipRepairAuthorizationRequest struct {
	Phase MembershipRepairPhase `json:"phase"`
}

// MembershipRepairQualificationRequest asks the authority to qualify one
// exact retained candidate before any old-voter closure is requested.
type MembershipRepairQualificationRequest struct {
	OperationID     string `json:"operationID"`
	CandidatePodUID string `json:"candidatePodUID"`
}

// MembershipRepairQualificationResult is returned only after the authority
// has verified and retained the qualification against the certified source
// archive. The evidence fields are references/measurements, not booleans.
type MembershipRepairQualificationResult struct {
	OperationID string                                 `json:"operationID"`
	Preclosure  *network.MembershipRepairProbeResponse `json:"preclosure,omitempty"`
}

type MembershipRepairPreclosure struct {
	CandidatePodUID string                                `json:"candidatePodUID"`
	Request         network.MembershipRepairProbeRequest  `json:"request"`
	Response        network.MembershipRepairProbeResponse `json:"response"`
}

func MembershipRepairProbeScopeDigest(challenge string, authorization MembershipRepairAuthorization, assignment BootstrapAssignment, member quepaxa.Member) string {
	if challenge == "" || authorization.Validate() != nil || authorization.ExpectedAbortSlot == nil || assignment.Validate() != nil || assignment.PodUID == "" {
		return ""
	}
	return canonicalJSONHash(struct {
		Challenge, OperationID, RequestHash, AuthorizationHash, RootUID, StatefulSetUID, LedgerUID, SourceClusterID string
		CandidatePodUID, CandidateBindRef, OldNodeID, OldWALIdentity                                                string
		ConfigID                                                                                                    uint
		AbortSlot                                                                                                   quepaxa.Slot
		Member                                                                                                      quepaxa.Member
	}{challenge, authorization.OperationID, authorization.RequestHash, authorization.AuthorizationHash, authorization.RootUID,
		authorization.StatefulSetUID, authorization.LedgerUID, authorization.SourceClusterID, assignment.PodUID, BootstrapAssignmentDigest(assignment),
		authorization.OldIdentity.NodeID, authorization.OldIdentity.WALIdentity, uint(authorization.Membership.ConfigID),
		*authorization.ExpectedAbortSlot, member})
}

type MembershipRepairPhasePrepareRequest struct {
	OperationID string                    `json:"operationID"`
	Kind        MembershipRepairPhaseKind `json:"kind"`
}

type MembershipRepairPhasePrepareResult struct {
	Phase MembershipRepairPhase `json:"phase"`
}

type MembershipRepairPhaseObserveRequest struct {
	OperationID        string          `json:"operationID"`
	PhaseHash          string          `json:"phaseHash"`
	JournalOperationID string          `json:"journalOperationID"`
	JournalRequestHash string          `json:"journalRequestHash"`
	Target             quepaxa.Cluster `json:"target"`
	TerminalSlot       uint64          `json:"terminalSlot"`
}

type MembershipRepairPhaseObserveResult struct {
	OperationID string                `json:"operationID"`
	PhaseHash   string                `json:"phaseHash"`
	Stage       MembershipRepairStage `json:"stage"`
	Complete    bool                  `json:"complete"`
}

type MembershipRepairStatusResult struct {
	Repair      *MembershipRepairView              `json:"repair,omitempty"`
	Observation *MembershipRepairSourceObservation `json:"observation,omitempty"`
}

// MembershipRepairSourceObservation is a read-only, independently checked
// source/slot observation. It is not a reservation or permission to mutate.
type MembershipRepairSourceObservation struct {
	ConfigID          uint64 `json:"configID"`
	AbortSlot         uint64 `json:"abortSlot"`
	TargetPodName     string `json:"targetPodName"`
	TargetNodeID      string `json:"targetNodeID"`
	TargetPodUID      string `json:"targetPodUID"`
	TargetWALIdentity string `json:"targetWALIdentity"`
	ReplacementPodUID string `json:"replacementPodUID"`
	MembershipHash    string `json:"membershipHash"`
}

func validateMembershipRepairSourceObservation(targetPodName string, result MembershipRepairStatusResult) error {
	if result.Repair != nil && result.Observation != nil {
		return fmt.Errorf("fencing: membership-repair response mixes active repair and source observation")
	}
	if targetPodName == "" {
		if result.Observation != nil {
			return fmt.Errorf("fencing: unrequested membership-repair source observation")
		}
		return nil
	}
	if result.Repair != nil {
		return nil
	}
	observation := result.Observation
	if observation == nil || observation.ConfigID == 0 || observation.TargetPodName != targetPodName ||
		observation.TargetNodeID == "" || observation.TargetPodUID == "" || observation.TargetWALIdentity == "" ||
		observation.ReplacementPodUID == "" || observation.ReplacementPodUID == observation.TargetPodUID ||
		!validBootstrapHash(observation.MembershipHash) {
		return fmt.Errorf("fencing: membership-repair source observation is incomplete or differs from requested slot")
	}
	return nil
}

// MembershipRepairReserveRequest is a survivor-authorized preparation
// reservation. It deliberately reuses the exact source round tuple without
// requesting or initiating peer closure.
type MembershipRepairReserveRequest struct {
	ExpectedConfigID  uint64  `json:"expectedConfigID"`
	ExpectedAbortSlot *uint64 `json:"expectedAbortSlot"`
	TargetNodeID      string  `json:"targetNodeID"`
	TargetWALIdentity string  `json:"targetWALIdentity"`
}

type MembershipRepairReserveResult struct {
	Repair *MembershipRepairView `json:"repair,omitempty"`
}

// ParseBootstrapBarrierSlot accepts only the canonical positive decimal slot
// representation used as a lookup hint for authority-side archive verification.
func ParseBootstrapBarrierSlot(value string) (uint64, error) {
	slot, err := strconv.ParseUint(value, 10, 64)
	if err != nil || slot == 0 || strconv.FormatUint(slot, 10) != value {
		return 0, fmt.Errorf("fencing: bootstrap barrier slot is not canonical")
	}
	return slot, nil
}

// BootstrapBarrierNonce deterministically binds the initial read barrier to
// the retained bootstrap operation. The nonce does not prove that a barrier
// was certified; the authority must still read and verify its archive decision.
func BootstrapBarrierNonce(operationID string) [16]byte {
	digest := sha256.Sum256([]byte("rhiza/bootstrap/read-barrier/v1\x00" + operationID))
	var nonce [16]byte
	copy(nonce[:], digest[:len(nonce)])
	return nonce
}

type BootstrapStatusRequest struct {
	OperationID string `json:"operationID"`
}

type BootstrapRuntimeAdmissionResult struct {
	Assignment BootstrapAssignment `json:"assignment"`
}

type BeginInitRequest struct {
	Assignment BootstrapAssignment `json:"assignment"`
	AttemptID  string              `json:"attemptID"`
}

type RecordInitOutcomeRequest struct {
	Assignment BootstrapAssignment `json:"assignment"`
	AttemptID  string              `json:"attemptID"`
	Outcome    InitAttemptOutcome  `json:"outcome"`
}

func (r Registration) ValidateFor(candidate Identity) error {
	id := r.Identity
	if id.SourceClusterID != candidate.SourceClusterID || id.PodUID != candidate.PodUID || id.RunID != candidate.RunID || id.WALIdentity != "" {
		return fmt.Errorf("fencing: registration identity differs from startup candidate")
	}
	if id.PodName == "" {
		if id.NodeID != candidate.NodeID || r.PeerToken != "" || len(r.Learner) != 0 || id.CredentialUID != "" || id.CredentialName != "" {
			return fmt.Errorf("fencing: legacy registration changed identity")
		}
		return nil
	}
	if (candidate.PodName != "" && candidate.PodName != id.PodName) ||
		(candidate.PodName == "" && candidate.NodeID != id.NodeID && candidate.NodeID != id.PodName) || id.Validate() != nil {
		return fmt.Errorf("fencing: replacement registration is incomplete")
	}
	if r.PeerToken == "" && len(r.Learner) == 0 && candidate.NodeID == id.NodeID {
		return nil
	}
	if r.PeerToken == "" || len(r.Learner) == 0 || !json.Valid(r.Learner) {
		return fmt.Errorf("fencing: replacement registration credentials are incomplete")
	}
	return nil
}

// validateLocalStartup is deliberately narrower than ValidateFor: a local
// supervisor already knows the candidate's source and canonical NodeID, while
// PodUID and RunID are assigned by the supervisor itself. The returned
// registration must nevertheless be a complete, self-consistent identity.
// Remote authority responses continue to use ValidateFor with their exact
// request tuple.
func (r Registration) validateLocalStartup(candidate Identity) error {
	id := r.Identity
	if candidate.SourceClusterID == "" || candidate.NodeID == "" ||
		id.SourceClusterID != candidate.SourceClusterID || id.NodeID != candidate.NodeID {
		return fmt.Errorf("fencing: local startup registration differs from known candidate")
	}
	if candidate.PodName != "" && candidate.PodName != PodNameForIdentity(id) {
		return fmt.Errorf("fencing: local startup Pod name differs from candidate")
	}
	if id.WALIdentity != "" {
		return fmt.Errorf("fencing: local startup registration is already WAL-bound")
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("fencing: invalid local startup identity: %w", err)
	}
	if r.PeerToken != "" || len(r.Learner) != 0 {
		return fmt.Errorf("fencing: local startup registration must not carry child credentials")
	}
	return nil
}

type StopRequest struct {
	OperationID string   `json:"operationID"`
	RequestHash string   `json:"requestHash"`
	Identity    Identity `json:"identity"`
}

// PeerClosureRequest asks the authority to close one other stable voter slot
// for an exact source membership round. The caller identity is carried
// separately in AuthorityMessage and is never part of the round operation ID.
type PeerClosureRequest struct {
	ExpectedConfigID  uint64  `json:"expectedConfigID"`
	ExpectedAbortSlot *uint64 `json:"expectedAbortSlot"`
	TargetNodeID      string  `json:"targetNodeID"`
	TargetWALIdentity string  `json:"targetWALIdentity"`
}

// Validate requires an explicitly supplied abort slot; a pointer to zero is a
// valid round value, while omission and JSON null are not.
func (r PeerClosureRequest) Validate(caller Identity) error {
	if caller.SourceClusterID == "" || caller.NodeID == "" || caller.PodUID == "" || caller.RunID == "" || caller.WALIdentity == "" ||
		r.ExpectedConfigID == 0 || r.ExpectedAbortSlot == nil || r.TargetNodeID == "" || r.TargetNodeID == caller.NodeID || r.TargetWALIdentity == "" {
		return errors.New("fencing: complete bound caller and target round are required")
	}
	return nil
}

// PeerClosureTarget is a target incarnation captured by the authority from its
// durable ledger and current Pod inventory. Empty runtime fields keep
// independent evidence pending; they are never inferred from a replacement.
type PeerClosureTarget struct {
	NodeID      string `json:"nodeID"`
	Pod         string `json:"pod"`
	PodUID      string `json:"podUID"`
	WALIdentity string `json:"walIdentity"`
	NodeName    string `json:"nodeName,omitempty"`
	NodeUID     string `json:"nodeUID,omitempty"`
	ContainerID string `json:"containerID,omitempty"`
	ImageID     string `json:"imageID,omitempty"`
}

// PeerClosureReceipt is an authority-validated projection of the immutable
// RhizaFence proof. It is returned only after exact request and live-binding
// verification succeeds.
type PeerClosureReceipt struct {
	OperationID         string              `json:"operationID"`
	SourceClusterID     string              `json:"sourceClusterID"`
	BindingUID          string              `json:"bindingUID"`
	StatefulSetUID      string              `json:"statefulSetUID"`
	RequestHash         string              `json:"requestHash"`
	ExpectedConfigID    uint64              `json:"expectedConfigID"`
	ExpectedAbortSlot   *uint64             `json:"expectedAbortSlot"`
	TargetNodeID        string              `json:"targetNodeID"`
	TargetWALIdentity   string              `json:"targetWALIdentity"`
	Targets             []PeerClosureTarget `json:"targets"`
	ProofID             string              `json:"proofID"`
	ProcessesTerminated bool                `json:"processesTerminated"`
	RecreationBlocked   bool                `json:"recreationBlocked"`
	StorageQuiesced     bool                `json:"storageQuiesced"`
}

// PeerClosureAuthority is deliberately separate from Authority: only the new
// peer-closure action requires survivor authorization; normal register/bind and
// termination actions retain their existing caller contract.
type PeerClosureAuthority interface {
	RequestPeerClosure(context.Context, Identity, PeerClosureRequest) (PeerClosureReceipt, error)
	ReadPeerClosure(context.Context, Identity, PeerClosureRequest) (PeerClosureReceipt, error)
}

// DrainReceipt is engine evidence, not evidence that the application process exited.
// Any unknown mutation keeps the incarnation ineligible even after SDK retries succeed.
type DrainReceipt struct {
	Request          StopRequest `json:"request"`
	WritersJoined    bool        `json:"writersJoined"`
	PendingMutations uint64      `json:"pendingMutations"`
	UnknownMutations uint64      `json:"unknownMutations"`
}

func (r DrainReceipt) Clean() bool {
	return r.WritersJoined && r.PendingMutations == 0 && r.UnknownMutations == 0
}
