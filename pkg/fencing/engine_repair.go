package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	EngineRemovalBindingVersion        = 1
	EngineRemovalMode                  = "engine-owned-remove"
	EngineRepairRecordVersion          = 1
	StorageEffectsProofVersion         = 1
	MutationGenerationZeroGuardVersion = 1
	MaxEngineRepairIncarnations        = 64
)

// EngineRemovalBinding is the immutable context passed from the engine-owned
// membership transition to the storage authority. FreezeSlot and FreezeHash
// are empty only before a freeze is committed; afterward both are required.
// This is a binding, not a proof that the freeze or storage settlement exists.
type EngineRemovalBinding struct {
	Version           int    `json:"version"`
	Mode              string `json:"mode"`
	OperationID       string `json:"operationID"`
	RequestHash       string `json:"requestHash"`
	SourceClusterID   string `json:"sourceClusterID"`
	StatefulSetUID    string `json:"statefulSetUID"`
	OldConfigID       uint64 `json:"oldConfigID"`
	ExpectedAbortSlot uint64 `json:"expectedAbortSlot"`
	OldMemberID       string `json:"oldMemberID"`
	OldWALIdentity    string `json:"oldWALIdentity"`
	TargetMemberID    string `json:"targetMemberID"`
	FreezeSlot        uint64 `json:"freezeSlot,omitempty"`
	FreezeHash        string `json:"freezeHash,omitempty"`
}

func (b EngineRemovalBinding) Validate() error {
	if b.Version != EngineRemovalBindingVersion || b.Mode != EngineRemovalMode || b.OperationID == "" || len(b.OperationID) > 256 ||
		!mutationDigestValid(b.RequestHash) || b.SourceClusterID == "" || b.StatefulSetUID == "" || b.OldConfigID == 0 ||
		b.OldMemberID == "" || b.OldWALIdentity == "" || b.TargetMemberID == "" || b.TargetMemberID == b.OldMemberID {
		return fmt.Errorf("fencing: incomplete engine removal binding")
	}
	if (b.FreezeSlot == 0) != (b.FreezeHash == "") || (b.FreezeHash != "" && !mutationDigestValid(b.FreezeHash)) {
		return fmt.Errorf("fencing: engine removal freeze binding is incomplete")
	}
	return nil
}

// RepairIncarnation pins every historical run in the generation inventory.
// It intentionally contains identity/enrollment references only; callers must
// load and verify producer/archive state before issuing exclusion evidence.
type RepairIncarnation struct {
	Identity          Identity    `json:"identity"`
	Role              StartupRole `json:"role"`
	ProducerUID       string      `json:"producerUID,omitempty"`
	MutationScopeHash string      `json:"mutationScopeHash,omitempty"`
	MutationProfile   string      `json:"mutationProfile,omitempty"`
}

// EngineRepairCandidateContinuation records a fresh supervisor incarnation of
// the same reserved learner identity. Earlier runs remain in the canonical
// ledger and are never rewritten or removed.
type EngineRepairCandidateContinuation struct {
	PriorRunID           string         `json:"priorRunID"`
	ExpectedRecordDigest string         `json:"expectedRecordDigest"`
	Identity             Identity       `json:"identity"`
	RuntimeBinding       RuntimeBinding `json:"runtimeBinding"`
	RuntimeHash          string         `json:"runtimeHash"`
}

// EngineRepairRecord is one immutable, generation-scoped repair reservation.
// The canonical storage adapter must persist it with the expected ConfigID and
// abort-slot CAS, and must refuse a second live record for this generation.
// Membership decisions remain QuePaxa's responsibility.
type EngineRepairRecord struct {
	Version int                  `json:"version"`
	Binding EngineRemovalBinding `json:"binding"`
	// BootstrapRequestDigest binds adoption to the authority-recorded physical
	// request. It is immutable and contains no credential value.
	BootstrapRequestDigest string `json:"bootstrapRequestDigest,omitempty"`
	// SourceConfig and SourceAdminCredential are pinned at reservation time.
	// They let the authority query the original quorum without accepting a
	// candidate-supplied configuration or secret value.
	SourceConfig          quepaxa.Cluster             `json:"sourceConfig"`
	SourceAdminCredential EngineRepairAdminCredential `json:"sourceAdminCredential"`
	// Candidate runtime enrollment is pinned only after the trusted runtime
	// agent enrolls the exact authority-assigned candidate. It is a one-time
	// post-reservation fact, not caller-provided bootstrap request data.
	CandidateRuntimeBinding *RuntimeBinding `json:"candidateRuntimeBinding,omitempty"`
	// CandidateContinuations is append-only. CandidateIdentity remains the
	// immutable identity from the original reservation; the latest continuation
	// is the only candidate RunID eligible for a new startup grant.
	CandidateContinuations []EngineRepairCandidateContinuation `json:"candidateContinuations,omitempty"`
	// RemovalOperation is nil only before the freeze is committed. It may be
	// set once to the exact frozen operation and is immutable thereafter.
	RemovalOperation      *quepaxa.EngineRemoval `json:"removalOperation,omitempty"`
	Incarnations          []RepairIncarnation    `json:"incarnations"`
	CandidateIdentity     Identity               `json:"candidateIdentity"`
	CandidateMember       quepaxa.Member         `json:"candidateMember"`
	CandidateCredential   EngineRepairCredential `json:"candidateCredential"`
	CandidateRuntimeHash  string                 `json:"candidateRuntimeHash"`
	CandidateMemberDigest string                 `json:"candidateMemberDigest"`
	RemoveRequestHash     string                 `json:"removeRequestHash"`
	AddRequestHash        string                 `json:"addRequestHash,omitempty"`
	StorageProofDigest    string                 `json:"storageProofDigest,omitempty"`
	RemoveTerminalDigest  string                 `json:"removeTerminalDigest,omitempty"`
	Phase                 EngineRepairPhase      `json:"phase"`
	// ClosingFrom preserves the exact in-flight repair phase when a newer DR
	// generation supersedes this operation. It is populated only for
	// GenerationClosing; referenced evidence is retained, never synthesized.
	ClosingFrom EngineRepairPhase `json:"closingFrom,omitempty"`
}

// CurrentCandidateIdentity returns the latest exact learner incarnation while
// preserving CandidateIdentity as the immutable original reservation.
func (r EngineRepairRecord) CurrentCandidateIdentity() Identity {
	if len(r.CandidateContinuations) == 0 {
		return r.CandidateIdentity
	}
	return r.CandidateContinuations[len(r.CandidateContinuations)-1].Identity
}

// CurrentCandidateRuntime returns the trusted runtime enrollment for the
// latest candidate incarnation.
func (r EngineRepairRecord) CurrentCandidateRuntime() *RuntimeBinding {
	if len(r.CandidateContinuations) == 0 {
		return r.CandidateRuntimeBinding
	}
	return &r.CandidateContinuations[len(r.CandidateContinuations)-1].RuntimeBinding
}

func (r EngineRepairRecord) CurrentCandidateRuntimeHash() string {
	if len(r.CandidateContinuations) == 0 {
		return r.CandidateRuntimeHash
	}
	return r.CandidateContinuations[len(r.CandidateContinuations)-1].RuntimeHash
}

// EngineRepairAdminCredential pins the immutable Kubernetes Secret from
// which the fencer rereads the original cluster's admin token. Only the
// token's digest is persisted; the credential itself is never copied into
// canonical repair evidence.
type EngineRepairAdminCredential struct {
	Name             string `json:"name"`
	UID              string `json:"uid"`
	AdminTokenDigest string `json:"adminTokenDigest"`
}

// EngineRepairCredential pins an immutable peer credential object without
// copying its private peer token into repair evidence. The authority must
// reread this exact Secret UID and verify PeerTokenDigest before returning the
// token to the candidate runtime.
type EngineRepairCredential struct {
	Name            string `json:"name"`
	UID             string `json:"uid"`
	PeerTokenDigest string `json:"peerTokenDigest"`
}

// EngineRepairReference is the only authority-bearing pointer to an immutable
// repair record. It is stored inside the canonical root ledger CAS value; the
// referenced evidence object alone never grants repair authority.
type EngineRepairReference struct {
	Version           int                  `json:"version"`
	OperationID       string               `json:"operationID"`
	Binding           EngineRemovalBinding `json:"binding"`
	CandidateIdentity Identity             `json:"candidateIdentity"`
	Phase             EngineRepairPhase    `json:"phase"`
	ClosingFrom       EngineRepairPhase    `json:"closingFrom,omitempty"`
	IdentityDigest    string               `json:"identityDigest"`
	RecordDigest      string               `json:"recordDigest"`
	RecordKey         string               `json:"recordKey"`
}

func (r EngineRepairReference) Validate() error {
	if r.Version != EngineRepairRecordVersion || r.OperationID == "" || r.Binding.Validate() != nil || r.OperationID != r.Binding.OperationID ||
		!r.Phase.valid() || (r.Phase == RepairGenerationClosing && (!r.ClosingFrom.valid() || r.ClosingFrom == RepairGenerationClosing)) ||
		(r.Phase != RepairGenerationClosing && r.ClosingFrom != "") ||
		r.CandidateIdentity.SourceClusterID != r.Binding.SourceClusterID || r.CandidateIdentity.NodeID != r.Binding.TargetMemberID ||
		r.CandidateIdentity.PodUID == "" || r.CandidateIdentity.RunID == "" || r.CandidateIdentity.WALIdentity == "" ||
		r.CandidateIdentity.AssignmentID == "" || !mutationDigestValid(r.CandidateIdentity.AssignmentID) ||
		!mutationDigestValid(r.IdentityDigest) || !mutationDigestValid(r.RecordDigest) || r.RecordKey == "" {
		return fmt.Errorf("fencing: invalid engine repair root reference")
	}
	return nil
}

// EngineRepairGenerationKey allows only one repair reservation for the exact
// source root/configuration/abort generation. The returned key is opaque so it
// is safe to use directly in the bounded canonical-root map.
func (b EngineRemovalBinding) EngineRepairGenerationKey() (string, error) {
	if err := b.Validate(); err != nil {
		return "", err
	}
	return hashJSONValue([]any{"rhiza-engine-repair-generation-v1", b.StatefulSetUID, b.SourceClusterID, b.OldConfigID, b.ExpectedAbortSlot}), nil
}

type EngineRepairPhase string

const (
	RepairCandidatePrepared EngineRepairPhase = "CandidatePrepared"
	RepairRemoveFrozen      EngineRepairPhase = "RemoveFrozen"
	RepairStorageClosing    EngineRepairPhase = "StorageClosing"
	RepairRemoved           EngineRepairPhase = "Removed"
	RepairAdding            EngineRepairPhase = "Adding"
	RepairActive            EngineRepairPhase = "Active"
	RepairGenerationClosing EngineRepairPhase = "GenerationClosing"
)

func (r EngineRepairRecord) Validate() error {
	if err := r.validateCore(); err != nil {
		return err
	}
	return r.validateIncarnations()
}

// ValidateAdoptionProposal validates the record fields a survivor can submit
// before the authority attaches the canonical root incarnation inventory.
// Inventory-bearing records are not accepted on this path.
func (r EngineRepairRecord) ValidateAdoptionProposal() error {
	if len(r.Incarnations) != 0 || r.Phase != RepairCandidatePrepared || r.RemovalOperation != nil ||
		r.Binding.FreezeSlot != 0 || r.Binding.FreezeHash != "" || r.CandidateRuntimeBinding != nil || r.CandidateRuntimeHash != "" ||
		!mutationDigestValid(r.BootstrapRequestDigest) {
		return fmt.Errorf("fencing: adoption proposal contains authority-owned or post-reservation state")
	}
	return r.validateCore()
}

// AdoptionProposalDigest is stable when the authority replaces the empty
// caller inventory with the exact run snapshot from the canonical root.
func (r EngineRepairRecord) AdoptionProposalDigest() string {
	r.Incarnations = nil
	return hashJSONValue(r)
}

func (r EngineRepairRecord) validateCore() error {
	if r.Version != EngineRepairRecordVersion || r.Binding.Validate() != nil ||
		!validEngineRepairSourceConfig(r.SourceConfig, r.Binding) || !r.SourceAdminCredential.valid() ||
		(r.BootstrapRequestDigest != "" && !mutationDigestValid(r.BootstrapRequestDigest)) ||
		(r.evidencePhase().atLeast(RepairRemoveFrozen) && r.Binding.FreezeSlot == 0) ||
		(!r.evidencePhase().atLeast(RepairRemoveFrozen) && r.Binding.FreezeSlot != 0) ||
		!mutationDigestValid(r.RemoveRequestHash) || !r.Phase.valid() {
		return fmt.Errorf("fencing: invalid engine repair record")
	}
	if r.RemovalOperation == nil {
		if r.evidencePhase().atLeast(RepairRemoveFrozen) {
			return fmt.Errorf("fencing: frozen engine repair lacks its exact removal operation")
		}
	} else {
		if !r.evidencePhase().atLeast(RepairRemoveFrozen) {
			return fmt.Errorf("fencing: removal operation was pinned before its freeze")
		}
		if err := validateEngineRepairRemovalOperation(*r.RemovalOperation, r.SourceConfig, r.Binding); err != nil {
			return err
		}
		if r.RemovalOperation.Target.MemberSet()[quepaxa.NodeID(r.CandidateIdentity.NodeID)].ID != "" {
			return fmt.Errorf("fencing: engine removal target already contains the replacement candidate")
		}
	}
	if r.Phase == RepairGenerationClosing {
		if !r.ClosingFrom.valid() || r.ClosingFrom == RepairGenerationClosing {
			return fmt.Errorf("fencing: generation-closing record lacks its prior phase")
		}
	} else if r.ClosingFrom != "" {
		return fmt.Errorf("fencing: non-closing repair carries a prior phase")
	}
	if r.CandidateIdentity.SourceClusterID != r.Binding.SourceClusterID || r.CandidateIdentity.NodeID != r.Binding.TargetMemberID ||
		r.CandidateIdentity.PodUID == "" || r.CandidateIdentity.RunID == "" || r.CandidateIdentity.WALIdentity == "" ||
		r.CandidateIdentity.AssignmentID == "" || !mutationDigestValid(r.CandidateIdentity.AssignmentID) ||
		!mutationDigestValid(r.CandidateMemberDigest) {
		return fmt.Errorf("fencing: engine repair candidate identity is incomplete")
	}
	if r.CandidateRuntimeBinding == nil {
		if r.CandidateRuntimeHash != "" || r.evidencePhase().atLeast(RepairRemoveFrozen) {
			return fmt.Errorf("fencing: engine repair candidate runtime is not trusted and pinned")
		}
	} else if r.CandidateRuntimeBinding.Validate(r.CandidateIdentity) != nil || r.CandidateRuntimeBinding.StatefulSetUID != r.Binding.StatefulSetUID ||
		r.CandidateRuntimeHash != RuntimeBindingDigest(*r.CandidateRuntimeBinding) {
		return fmt.Errorf("fencing: engine repair candidate runtime differs from trusted enrollment")
	}
	if len(r.CandidateContinuations) > MaxEngineRepairIncarnations || len(r.CandidateContinuations) > 0 && r.CandidateRuntimeBinding == nil ||
		len(r.Incarnations)+1+len(r.CandidateContinuations) > MaxEngineRepairIncarnations {
		return fmt.Errorf("fencing: engine repair candidate continuation history is incomplete")
	}
	seenCandidateRuns := map[string]struct{}{r.CandidateIdentity.RunID: {}}
	previousIdentity := r.CandidateIdentity
	seenRuntimeBindings := make(map[RuntimeBinding]struct{}, len(r.CandidateContinuations)+1)
	if r.CandidateRuntimeBinding != nil {
		seenRuntimeBindings[*r.CandidateRuntimeBinding] = struct{}{}
	}
	for _, continuation := range r.CandidateContinuations {
		identity := continuation.Identity
		if continuation.PriorRunID != previousIdentity.RunID || !mutationDigestValid(continuation.ExpectedRecordDigest) ||
			identity.SourceClusterID != r.CandidateIdentity.SourceClusterID || identity.NodeID != r.CandidateIdentity.NodeID ||
			identity.PodName != r.CandidateIdentity.PodName || identity.PodUID != r.CandidateIdentity.PodUID ||
			identity.WALIdentity != r.CandidateIdentity.WALIdentity || identity.AssignmentID != r.CandidateIdentity.AssignmentID ||
			identity.RunID == "" || identity.RunID == previousIdentity.RunID || continuation.RuntimeBinding.Validate(identity) != nil ||
			continuation.RuntimeBinding.StatefulSetUID != r.Binding.StatefulSetUID ||
			continuation.RuntimeBinding.AuthorityID != r.CandidateRuntimeBinding.AuthorityID ||
			continuation.RuntimeBinding.NamespaceUID != r.CandidateRuntimeBinding.NamespaceUID ||
			continuation.RuntimeBinding.BindingUID != r.CandidateRuntimeBinding.BindingUID ||
			continuation.RuntimeHash != RuntimeBindingDigest(continuation.RuntimeBinding) {
			return fmt.Errorf("fencing: engine repair candidate continuation differs from its immutable reservation or fresh runtime")
		}
		if _, reused := seenRuntimeBindings[continuation.RuntimeBinding]; reused {
			return fmt.Errorf("fencing: candidate continuation reused an earlier runtime enrollment")
		}
		if _, duplicate := seenCandidateRuns[identity.RunID]; duplicate {
			return fmt.Errorf("fencing: duplicate candidate continuation run")
		}
		seenCandidateRuns[identity.RunID] = struct{}{}
		seenRuntimeBindings[continuation.RuntimeBinding] = struct{}{}
		previousIdentity = identity
	}
	if _, exists := r.SourceConfig.MemberSet()[quepaxa.NodeID(r.CandidateIdentity.NodeID)]; exists {
		return fmt.Errorf("fencing: replacement candidate is already in the pinned original configuration")
	}
	if r.CandidateMember.ID != quepaxa.NodeID(r.CandidateIdentity.NodeID) || r.CandidateMember.WALIdentity != r.CandidateIdentity.WALIdentity ||
		r.CandidateMember.PublicKey == (quepaxa.PublicKey{}) || !validRepairEndpoint(r.CandidateMember.URL, "http", "https") ||
		!validRepairEndpoint(r.CandidateMember.LogURL, "http", "https") ||
		!validRepairEndpoint(r.CandidateMember.PeerURL, "quic") ||
		r.CandidateMemberDigest != EngineRepairMemberDigest(r.CandidateMember) || !r.CandidateCredential.valid() {
		return fmt.Errorf("fencing: engine repair candidate configuration is incomplete or differs from its immutable digest")
	}
	if r.Binding.OldMemberID == r.CandidateIdentity.NodeID || r.Binding.OldWALIdentity == r.CandidateIdentity.WALIdentity {
		return fmt.Errorf("fencing: engine repair candidate reuses the old member identity")
	}
	evidencePhase := r.evidencePhase()
	if evidencePhase.atLeast(RepairAdding) && !mutationDigestValid(r.AddRequestHash) {
		return fmt.Errorf("fencing: engine repair ADD request is not pinned")
	}
	if evidencePhase.atLeast(RepairRemoved) && (!mutationDigestValid(r.StorageProofDigest) || !mutationDigestValid(r.RemoveTerminalDigest)) {
		return fmt.Errorf("fencing: engine repair removal lacks storage and terminal evidence")
	}
	return nil
}

func (r EngineRepairRecord) validateIncarnations() error {
	if len(r.Incarnations) == 0 || len(r.Incarnations) > MaxEngineRepairIncarnations {
		return fmt.Errorf("fencing: engine repair incarnation inventory is incomplete")
	}
	seen := make(map[string]struct{}, len(r.Incarnations))
	oldMemberFound := false
	for _, incarnation := range r.Incarnations {
		id := incarnation.Identity
		if id.SourceClusterID != r.Binding.SourceClusterID || id.NodeID == "" || id.PodUID == "" || id.RunID == "" ||
			(incarnation.Role != StartupRoleVoterActive && incarnation.Role != StartupRoleLearnerPendingRemove) {
			return fmt.Errorf("fencing: engine repair incarnation inventory is incomplete")
		}
		if _, exists := seen[id.RunID]; exists {
			return fmt.Errorf("fencing: duplicate run in engine repair inventory")
		}
		seen[id.RunID] = struct{}{}
		if id.NodeID == r.Binding.OldMemberID {
			if id.WALIdentity != "" && id.WALIdentity != r.Binding.OldWALIdentity {
				return fmt.Errorf("fencing: old member repair inventory has another WAL")
			}
			oldMemberFound = oldMemberFound || id.WALIdentity == r.Binding.OldWALIdentity
		}
		if incarnation.ProducerUID != "" && (!mutationDigestValid(incarnation.MutationScopeHash) || incarnation.MutationProfile == "") {
			return fmt.Errorf("fencing: repair producer enrollment is incomplete")
		}
		if incarnation.ProducerUID == "" && (incarnation.MutationScopeHash != "" || incarnation.MutationProfile != "") {
			return fmt.Errorf("fencing: unbound repair incarnation carries producer authority")
		}
	}
	if !oldMemberFound {
		return fmt.Errorf("fencing: engine repair inventory omits the old member")
	}
	return nil
}

func (c EngineRepairAdminCredential) valid() bool {
	return c.Name != "" && len(c.Name) <= 253 && !strings.ContainsAny(c.Name, "/\\\x00\r\n") &&
		c.UID != "" && len(c.UID) <= 256 && mutationDigestValid(c.AdminTokenDigest)
}

func EngineRepairAdminTokenDigest(token []byte) string {
	sum := sha256.Sum256(token)
	return hex.EncodeToString(sum[:])
}

func validEngineRepairSourceConfig(config quepaxa.Cluster, binding EngineRemovalBinding) bool {
	if config.ConfigID != uint(binding.OldConfigID) || len(config.Members) < 2 || len(config.Members) > MaxEngineRepairIncarnations {
		return false
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(config.Members))
	oldFound := false
	for _, member := range config.Members {
		peerAddressValid := validRepairEndpoint(member.PeerURL, "quic")
		if member.PeerURL == "" {
			peerAddressValid = validRepairEndpoint(member.URL, "http", "https")
		}
		if member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) || !peerAddressValid {
			return false
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return false
		}
		seen[member.ID] = struct{}{}
		if string(member.ID) == binding.OldMemberID {
			oldFound = member.WALIdentity == binding.OldWALIdentity
		}
	}
	return oldFound
}

func validateEngineRepairRemovalOperation(operation quepaxa.EngineRemoval, source quepaxa.Cluster, binding EngineRemovalBinding) error {
	if operation.Version != 1 || operation.Mode != EngineRemovalMode || operation.OperationID != binding.OperationID ||
		operation.ExpectedConfigID != uint(binding.OldConfigID) || operation.ExpectedAbortSlot != quepaxa.Slot(binding.ExpectedAbortSlot) ||
		operation.Removed.ID != quepaxa.NodeID(binding.OldMemberID) || operation.Removed.WALIdentity != binding.OldWALIdentity ||
		operation.Freeze != quepaxa.Slot(binding.FreezeSlot) || fmt.Sprintf("%x", operation.FreezeHash[:]) != binding.FreezeHash ||
		operation.StorageEffectsExcludedHash != (quepaxa.ValueHash{}) || operation.Target.ConfigID != source.ConfigID+1 ||
		len(operation.Target.Members) != len(source.Members)-1 {
		return fmt.Errorf("fencing: engine removal operation differs from its original-config repair reservation")
	}
	remaining := source.MemberSet()
	if _, exists := remaining[operation.Removed.ID]; !exists {
		return fmt.Errorf("fencing: original configuration omits the removed member")
	}
	delete(remaining, operation.Removed.ID)
	target := operation.Target.MemberSet()
	if len(remaining) != len(target) || target[operation.Removed.ID].ID != "" {
		return fmt.Errorf("fencing: engine removal target does not preserve the original survivors")
	}
	for id, member := range remaining {
		if target[id] != member {
			return fmt.Errorf("fencing: engine removal target changed a pinned original voter")
		}
	}
	originalRemoved := source.MemberSet()[operation.Removed.ID]
	if originalRemoved != operation.Removed {
		return fmt.Errorf("fencing: engine removal operation changed the original removed-member identity")
	}
	return nil
}

func SameEngineRemovalOperation(left, right quepaxa.EngineRemoval) bool {
	if left.Version != right.Version || left.Mode != right.Mode || left.OperationID != right.OperationID ||
		left.ExpectedConfigID != right.ExpectedConfigID || left.ExpectedAbortSlot != right.ExpectedAbortSlot ||
		left.Removed != right.Removed || left.Freeze != right.Freeze || left.FreezeHash != right.FreezeHash ||
		left.StorageEffectsExcludedHash != right.StorageEffectsExcludedHash || left.Target.ConfigID != right.Target.ConfigID ||
		len(left.Target.Members) != len(right.Target.Members) {
		return false
	}
	for id, member := range left.Target.MemberSet() {
		if right.Target.MemberSet()[id] != member {
			return false
		}
	}
	return true
}

func (c EngineRepairCredential) valid() bool {
	return c.Name != "" && len(c.Name) <= 253 && !strings.ContainsAny(c.Name, "/\\\x00\r\n") &&
		c.UID != "" && len(c.UID) <= 256 && mutationDigestValid(c.PeerTokenDigest)
}

func validRepairEndpoint(raw string, schemes ...string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	for _, scheme := range schemes {
		if parsed.Scheme == scheme {
			return true
		}
	}
	return false
}

// EngineRepairMemberDigest binds the exact public candidate member config.
// Private peer credentials are pinned separately by EngineRepairCredential.
func EngineRepairMemberDigest(member quepaxa.Member) string {
	return hashJSONValue([]any{"member", member})
}

// EngineRepairPeerTokenDigest pins a private peer credential without placing
// the token itself in the durable repair record or its evidence objects.
func EngineRepairPeerTokenDigest(token []byte) string {
	sum := sha256.Sum256(token)
	return hex.EncodeToString(sum[:])
}

func (r EngineRepairRecord) evidencePhase() EngineRepairPhase {
	if r.Phase == RepairGenerationClosing {
		return r.ClosingFrom
	}
	return r.Phase
}

func (r EngineRepairRecord) Digest() string { return hashJSONValue(r) }

// IdentityDigest remains stable as the operation advances through phases and
// gains evidence references. The exact removal operation is a separately
// one-time-locked fact; all original configuration and credential references
// remain part of the immutable reservation digest.
func (r EngineRepairRecord) IdentityDigest() string {
	r.AddRequestHash = ""
	r.StorageProofDigest = ""
	r.RemoveTerminalDigest = ""
	r.Phase = ""
	r.ClosingFrom = ""
	r.Binding.FreezeSlot = 0
	r.Binding.FreezeHash = ""
	r.RemovalOperation = nil
	r.CandidateRuntimeHash = ""
	r.CandidateRuntimeBinding = nil
	r.CandidateContinuations = nil
	return hashJSONValue(r)
}

func (p EngineRepairPhase) valid() bool {
	switch p {
	case RepairCandidatePrepared, RepairRemoveFrozen, RepairStorageClosing, RepairRemoved, RepairAdding, RepairActive, RepairGenerationClosing:
		return true
	default:
		return false
	}
}

func (p EngineRepairPhase) atLeast(want EngineRepairPhase) bool {
	order := map[EngineRepairPhase]int{
		RepairCandidatePrepared: 1, RepairRemoveFrozen: 2, RepairStorageClosing: 3, RepairRemoved: 4,
		RepairAdding: 5, RepairActive: 6, RepairGenerationClosing: 7,
	}
	return order[p] >= order[want]
}

// ValidateAgainstLedger requires the record to retain the complete old-member
// closure cohort and candidate history. It also permits only authenticated
// fresh runs of an already-recorded healthy survivor; those runs do not alter
// the immutable closure cohort. Producer and archive contents are verified
// separately before storage closure.
func (r EngineRepairRecord) ValidateAgainstLedger(l Ledger) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if l.SourceClusterID != r.Binding.SourceClusterID || len(l.Runs) == 0 || len(l.Runs) > MaxEngineRepairIncarnations ||
		len(l.Runs) < len(r.Incarnations) {
		return fmt.Errorf("fencing: engine repair inventory differs from canonical root")
	}
	seen := make(map[string]struct{}, len(r.Incarnations)+1)
	for _, incarnation := range r.Incarnations {
		run, exists := l.Runs[incarnation.Identity.RunID]
		if !exists || run.Identity != incarnation.Identity || RunRole(run) != incarnation.Role || run.MutationProducerUID != incarnation.ProducerUID ||
			run.MutationScopeHash != incarnation.MutationScopeHash || run.MutationProfile != incarnation.MutationProfile {
			return fmt.Errorf("fencing: engine repair incarnation differs from canonical root")
		}
		seen[incarnation.Identity.RunID] = struct{}{}
	}
	identities := []Identity{r.CandidateIdentity}
	runtimes := []*RuntimeBinding{r.CandidateRuntimeBinding}
	for i := range r.CandidateContinuations {
		continuation := &r.CandidateContinuations[i]
		identities = append(identities, continuation.Identity)
		runtimes = append(runtimes, &continuation.RuntimeBinding)
	}
	for i, identity := range identities {
		candidate, exists := l.Runs[identity.RunID]
		if !exists {
			if runtimes[i] != nil || i != len(identities)-1 {
				return fmt.Errorf("fencing: pinned candidate runtime is missing its canonical learner run")
			}
			continue
		}
		if runtimes[i] == nil || candidate.Runtime == nil || *candidate.Runtime != *runtimes[i] {
			return fmt.Errorf("fencing: candidate run is not bound to the pinned trusted runtime")
		}
		isCurrent := i == len(identities)-1
		if isCurrent && (repairRequiresActiveCandidateRun(r.Phase, r.ClosingFrom) ||
			repairAllowsActiveCandidateRun(r.Phase, r.ClosingFrom) && RunRole(candidate) == StartupRoleVoterActive) {
			if candidate.Identity != identity || RunRole(candidate) != StartupRoleVoterActive {
				return fmt.Errorf("fencing: completed engine repair candidate differs from its active canonical run")
			}
			if candidate.MutationProducerUID != "" && (!mutationDigestValid(candidate.MutationScopeHash) || candidate.MutationProfile == "") ||
				candidate.MutationProducerUID == "" && (candidate.MutationScopeHash != "" || candidate.MutationProfile != "") {
				return fmt.Errorf("fencing: active repair candidate producer binding is malformed")
			}
		} else if !repairCandidateRunIdentityMatches(identity, candidate.Identity) || RunRole(candidate) != StartupRoleLearnerPendingRemove || candidate.MutationProducerUID != "" ||
			candidate.MutationScopeHash != "" || candidate.MutationProfile != "" {
			return fmt.Errorf("fencing: registered engine repair candidate is not the exact unadmitted learner run")
		} else if candidate.Identity.WALIdentity != "" && candidate.Identity.WALIdentity != identity.WALIdentity {
			return fmt.Errorf("fencing: candidate run has a different bound WAL identity")
		}
		if _, duplicate := seen[identity.RunID]; duplicate {
			return fmt.Errorf("fencing: candidate run is duplicated in the historical incarnation inventory")
		}
		seen[identity.RunID] = struct{}{}
	}
	for runID, run := range l.Runs {
		if _, accounted := seen[runID]; accounted {
			continue
		}
		if !repairSurvivorRestartMatches(r, run) && !engineRepairActiveRestartMatches(r, l, run) {
			return fmt.Errorf("fencing: canonical root contains an unrecorded engine repair incarnation")
		}
		seen[runID] = struct{}{}
	}
	capacityUse, err := l.EngineRepairCapacityUse()
	if err != nil || capacityUse > MaxEngineRepairIncarnations {
		return fmt.Errorf("fencing: engine repair candidate exceeds the bounded incarnation inventory")
	}
	if len(seen) != len(l.Runs) {
		return fmt.Errorf("fencing: canonical root contains an unrecorded engine repair incarnation")
	}
	return nil
}

func repairSurvivorRestartMatches(record EngineRepairRecord, run Run) bool {
	identity := run.Identity
	if identity.SourceClusterID != record.Binding.SourceClusterID || identity.NodeID == "" || identity.PodName == "" ||
		identity.PodUID == "" || identity.RunID == "" || identity.WALIdentity == "" ||
		identity.NodeID == record.Binding.OldMemberID || identity.NodeID == record.CandidateIdentity.NodeID ||
		runRole(run) != StartupRoleVoterActive || run.Runtime == nil || run.Runtime.Validate(identity) != nil ||
		run.Runtime.StatefulSetUID != record.Binding.StatefulSetUID {
		return false
	}
	if run.MutationProducerUID != "" && (!mutationDigestValid(run.MutationScopeHash) || run.MutationProfile == "") ||
		run.MutationProducerUID == "" && (run.MutationScopeHash != "" || run.MutationProfile != "") {
		return false
	}
	for _, original := range record.Incarnations {
		prior := original.Identity
		if prior.NodeID != identity.NodeID || prior.PodName != identity.PodName || prior.PodUID != identity.PodUID ||
			prior.WALIdentity != identity.WALIdentity || prior.AssignmentID != identity.AssignmentID ||
			original.Role != StartupRoleVoterActive {
			continue
		}
		return true
	}
	return false
}

func repairIsActiveHistory(phase, closingFrom EngineRepairPhase) bool {
	return phase == RepairActive || phase == RepairGenerationClosing && closingFrom == RepairActive
}

func repairRequiresActiveCandidateRun(phase, closingFrom EngineRepairPhase) bool {
	return phase == RepairActive || phase == RepairGenerationClosing && closingFrom == RepairActive
}

func repairAllowsActiveCandidateRun(phase, closingFrom EngineRepairPhase) bool {
	return phase == RepairAdding || phase == RepairActive || phase == RepairGenerationClosing &&
		(closingFrom == RepairAdding || closingFrom == RepairActive)
}

// StorageRunSettlement is a reference to a separately verified producer
// snapshot and its complete terminal/future-effect-excluded history.
// SettlementDigest authenticates the exact contiguous settled history; it is
// not trusted without the verifier reloading that history and archive chain.
type StorageRunSettlement struct {
	RunID             string `json:"runID"`
	PodUID            string `json:"podUID"`
	WALIdentity       string `json:"walIdentity,omitempty"`
	ProducerUID       string `json:"producerUID,omitempty"`
	ClosedThrough     uint64 `json:"closedThrough"`
	SettledThrough    uint64 `json:"settledThrough"`
	ProducerStateHash string `json:"producerStateHash"`
	SettlementDigest  string `json:"settlementDigest"`
	NoProducerDigest  string `json:"noProducerDigest,omitempty"`
}

// StorageEffectsExcluded proves only that this exact old identity has no
// storage effects left that can alter the canonical source. It is not process
// termination evidence and must not be copied into Exited, Drain, or
// IndependentTerminal.
type StorageEffectsExcluded struct {
	Version          int                    `json:"version"`
	Binding          EngineRemovalBinding   `json:"binding"`
	RepairRecordHash string                 `json:"repairRecordHash"`
	RunSettlements   []StorageRunSettlement `json:"runSettlements"`
	ProofDigest      string                 `json:"proofDigest"`
}

func (p StorageEffectsExcluded) Validate(record EngineRepairRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if !record.evidencePhase().atLeast(RepairStorageClosing) || record.Binding.FreezeSlot == 0 {
		return fmt.Errorf("fencing: storage-effects proof precedes the frozen repair")
	}
	oldRuns := make(map[string]RepairIncarnation)
	for _, incarnation := range record.Incarnations {
		if incarnation.Identity.NodeID == record.Binding.OldMemberID {
			oldRuns[incarnation.Identity.RunID] = incarnation
		}
	}
	if len(oldRuns) == 0 || p.Version != StorageEffectsProofVersion || p.Binding != record.Binding || !mutationDigestValid(p.RepairRecordHash) ||
		p.RepairRecordHash != record.IdentityDigest() || !mutationDigestValid(p.ProofDigest) || p.ProofDigest != p.Digest() || len(p.RunSettlements) != len(oldRuns) {
		return fmt.Errorf("fencing: storage-effects proof is not bound to the repair record")
	}
	seen := make(map[string]struct{}, len(p.RunSettlements))
	for _, settlement := range p.RunSettlements {
		incarnation, exists := oldRuns[settlement.RunID]
		if !exists || settlement.PodUID != incarnation.Identity.PodUID || settlement.WALIdentity != incarnation.Identity.WALIdentity || settlement.ProducerUID != incarnation.ProducerUID {
			return fmt.Errorf("fencing: storage-effects proof names an unknown or changed run")
		}
		if _, duplicate := seen[settlement.RunID]; duplicate {
			return fmt.Errorf("fencing: storage-effects proof duplicates a run")
		}
		seen[settlement.RunID] = struct{}{}
		if !mutationDigestValid(settlement.ProducerStateHash) || !mutationDigestValid(settlement.SettlementDigest) {
			return fmt.Errorf("fencing: storage-effects settlement evidence is incomplete")
		}
		if incarnation.ProducerUID == "" {
			if settlement.ClosedThrough != 0 || settlement.SettledThrough != 0 || !mutationDigestValid(settlement.NoProducerDigest) {
				return fmt.Errorf("fencing: no-producer run lacks exact root evidence")
			}
		} else if settlement.NoProducerDigest != "" || settlement.SettledThrough != settlement.ClosedThrough {
			return fmt.Errorf("fencing: producer settlement does not cover its closed admission prefix")
		}
	}
	if record.StorageProofDigest != "" && record.StorageProofDigest != p.ProofDigest {
		return fmt.Errorf("fencing: repair record stores another storage-effects proof")
	}
	return nil
}

func (p StorageEffectsExcluded) Digest() string {
	p.ProofDigest = ""
	return hashJSONValue(p)
}

func hashJSONValue(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// SortedRepairIncarnations gives callers a deterministic snapshot to store
// before granting any repair or storage authority.
func SortedRepairIncarnations(l Ledger) ([]RepairIncarnation, error) {
	if l.SourceClusterID == "" || len(l.Runs) == 0 || len(l.Runs) > MaxEngineRepairIncarnations {
		return nil, fmt.Errorf("fencing: canonical incarnation inventory is empty or over capacity")
	}
	incarnations := make([]RepairIncarnation, 0, len(l.Runs))
	for _, run := range l.Runs {
		if run.Identity.SourceClusterID != l.SourceClusterID || run.Identity.RunID == "" {
			return nil, fmt.Errorf("fencing: canonical incarnation inventory is malformed")
		}
		incarnations = append(incarnations, RepairIncarnation{Identity: run.Identity, Role: RunRole(run), ProducerUID: run.MutationProducerUID,
			MutationScopeHash: run.MutationScopeHash, MutationProfile: run.MutationProfile})
	}
	sort.Slice(incarnations, func(i, j int) bool {
		if incarnations[i].Identity.NodeID != incarnations[j].Identity.NodeID {
			return incarnations[i].Identity.NodeID < incarnations[j].Identity.NodeID
		}
		return incarnations[i].Identity.RunID < incarnations[j].Identity.RunID
	})
	return incarnations, nil
}
