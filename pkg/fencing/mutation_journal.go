package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"strings"
)

const (
	MaxMutationJournalEntries = 64
	MaxMutationDispatches     = 16
	MutationIntentVersion     = 1
)

type MutationPhase string

const (
	MutationReserved MutationPhase = "Reserved"
	MutationOpen     MutationPhase = "Open"
	MutationClosed   MutationPhase = "Closed"
)

type MutationOutcome string

const (
	MutationCommitted MutationOutcome = "Committed"
	MutationRejected  MutationOutcome = "Rejected"
	MutationExcluded  MutationOutcome = "FutureEffectExcluded"
)

type MutationResolutionReason string

const (
	MutationResolutionReasonGenerationZeroPinned MutationResolutionReason = "GenerationZeroPinned"
)

type MutationConditionMode string

const (
	MutationUnconditional   MutationConditionMode = "Unconditional"
	MutationCreateOnly      MutationConditionMode = "CreateOnly"
	MutationMatchGeneration MutationConditionMode = "MatchGeneration"
)

// MutationCondition is the caller's original selection, not a condition chosen
// later by an executor while resolving an uncertain request.
type MutationCondition struct {
	Mode       MutationConditionMode `json:"mode"`
	Generation int64                 `json:"generation,omitempty"`
}

// MutationOwnerStep correlates accepted work with a durable caller workflow.
// It does not replace the structural producer-UID/sequence request identity.
type MutationOwnerStep struct {
	Owner    string `json:"owner"`
	Workflow string `json:"workflow"`
	Step     string `json:"step"`
	Attempt  uint64 `json:"attempt"`
}

type MutationIntent struct {
	ContractVersion int               `json:"contractVersion"`
	StoreScopeHash  string            `json:"storeScopeHash"`
	OwnerStep       MutationOwnerStep `json:"ownerStep"`
	Condition       MutationCondition `json:"condition"`
	OperationID     string            `json:"operationID"`
	Kind            string            `json:"kind"`
	Key             string            `json:"key"`
	PayloadRef      string            `json:"payloadRef,omitempty"`
	PayloadDigest   string            `json:"payloadDigest,omitempty"`
	PayloadSize     int64             `json:"payloadSize"`
}

// MutationPreparation is the one immutable descriptor authorized to execute.
// SessionRef points to a private, durable resumable-session record, not a URI
// or credential embedded in the public control journal.
type MutationPreparation struct {
	Profile        string `json:"profile"`
	StoreScopeHash string `json:"storeScopeHash"`
	GenerationSet  bool   `json:"generationSet"`
	Generation     int64  `json:"generation"`
	SessionRef     string `json:"sessionRef,omitempty"`
}

// IsHeldCreateEligible identifies zero-generation uploads protected by the
// held profile. The original condition remains in the intent and is never
// rewritten; unconditional writes are eligible only when preparation pinned
// the observed absence as generation zero.
func IsHeldCreateEligible(intent MutationIntent, preparation MutationPreparation) bool {
	return intent.Kind == "upload" && preparation.Profile == MutationHeldExecutionProfile && preparation.GenerationSet && preparation.Generation == 0 &&
		(intent.Condition.Mode == MutationCreateOnly || intent.Condition.Mode == MutationUnconditional)
}

// MutationResolution must be supplied only by the trusted provider worker after
// recording its immutable provider evidence. Exclusion is never a client ACK.
type MutationResolution struct {
	Outcome         MutationOutcome          `json:"outcome"`
	Reason          MutationResolutionReason `json:"reason,omitempty"`
	PreparationHash string                   `json:"preparationHash"`
	ReceiptDigest   string                   `json:"receiptDigest"`
	PayloadDigest   string                   `json:"payloadDigest,omitempty"`
	ConditionFailed bool                     `json:"conditionFailed,omitempty"`
	// AllCopiesTerminal is a trusted provider-worker assertion backed by the
	// receipt. In particular a successful create-if-absent response alone does
	// not establish this: a delayed copy can succeed after a later deletion.
	// Sealed dispatch admission and every granted permission's terminal receipt
	// are required; counting requests already observed on the wire is insufficient.
	AllCopiesTerminal bool `json:"allCopiesTerminal,omitempty"`
}

type MutationEntry struct {
	Intent                   MutationIntent                    `json:"intent"`
	Preparation              *MutationPreparation              `json:"preparation,omitempty"`
	Resolution               *MutationResolution               `json:"resolution,omitempty"`
	CopyCompletion           *MutationCopyCompletion           `json:"copyCompletion,omitempty"`
	GuardedLogicalResolution *MutationGuardedLogicalResolution `json:"guardedLogicalResolution,omitempty"`
	GenerationZeroGuard      *MutationGenerationZeroGuard      `json:"generationZeroGuard,omitempty"`
	GenerationZeroRights     *MutationGenerationZeroRights     `json:"generationZeroRights,omitempty"`
	DispatchClosed           bool                              `json:"dispatchClosed,omitempty"`
	Dispatches               map[string]MutationDispatch       `json:"dispatches,omitempty"`
}

// MutationGenerationZeroGuard is distinct from MutationGuardedLogicalResolution:
// it proves the live positive-generation held object and the repair-scoped
// future-effect fence for an uncertain generation-zero dispatch. It does not
// assert that the old physical request has terminated or that every copy is
// terminal. The storage authority must reload and verify GuardRef and the exact
// object metadata before trusting this reference.
type MutationGenerationZeroGuard struct {
	Version              int                  `json:"version"`
	RepairBinding        EngineRemovalBinding `json:"repairBinding"`
	RepairRecordHash     string               `json:"repairRecordHash"`
	StorageScopeHash     string               `json:"storageScopeHash"`
	Key                  string               `json:"key"`
	Runtime              RuntimeBinding       `json:"runtime"`
	WALIdentity          string               `json:"walIdentity"`
	ProducerUID          string               `json:"producerUID"`
	Sequence             uint64               `json:"sequence"`
	Intent               MutationIntent       `json:"intent"`
	Preparation          MutationPreparation  `json:"preparation"`
	PreparationHash      string               `json:"preparationHash"`
	DispatchID           string               `json:"dispatchID"`
	GuardRootIdentity    string               `json:"guardRootIdentity"`
	GuardRef             string               `json:"guardRef"`
	GuardUID             string               `json:"guardUID"`
	GuardDigest          string               `json:"guardDigest"`
	ObjectGeneration     int64                `json:"objectGeneration"`
	ObjectMetageneration int64                `json:"objectMetageneration"`
	ObjectSize           int64                `json:"objectSize"`
	PayloadDigest        string               `json:"payloadDigest"`
}

// MutationGuardedLogicalResolution resolves only the logical value of an
// uncertain held generation-zero create. It deliberately does not terminalize
// the original provider permission; the live held object fences later effects.
type MutationGuardedLogicalResolution struct {
	Version              int                     `json:"version"`
	Runtime              RuntimeBinding          `json:"runtime,omitzero"`
	AuthorityProfile     string                  `json:"authorityProfile,omitempty"`
	LogicalBinding       *LogicalWriterBindingV1 `json:"logicalBinding,omitempty"`
	WALIdentity          string                  `json:"walIdentity"`
	ProducerUID          string                  `json:"producerUID"`
	Sequence             uint64                  `json:"sequence"`
	Intent               MutationIntent          `json:"intent"`
	Preparation          MutationPreparation     `json:"preparation"`
	PreparationHash      string                  `json:"preparationHash"`
	DispatchID           string                  `json:"dispatchID"`
	Profile              string                  `json:"profile"`
	GuardRootIdentity    string                  `json:"guardRootIdentity"`
	GuardVersion         int                     `json:"guardVersion"`
	GuardRef             string                  `json:"guardRef"`
	GuardDigest          string                  `json:"guardDigest"`
	ObjectGeneration     int64                   `json:"objectGeneration"`
	ObjectMetageneration int64                   `json:"objectMetageneration"`
	ObjectSize           int64                   `json:"objectSize"`
	PayloadDigest        string                  `json:"payloadDigest"`
}

const MutationGuardedLogicalResolutionVersion = 1

// MutationDispatch is a durable executable permission, including a worker that
// paused before sending any bytes. Empty ReceiptDigest means unresolved. The
// trusted executor must settle the permission itself, not just observed HTTP
// attempts, and never reuse a settled permission for another physical dispatch.
type MutationDispatch struct {
	ReceiptDigest string `json:"receiptDigest,omitempty"`
}

// MutationCopyCompletion adds terminal-attempt evidence without overwriting
// the original commitment receipt or its earlier uncertainty.
type MutationCopyCompletion struct {
	PreparationHash            string `json:"preparationHash"`
	ReceiptDigest              string `json:"receiptDigest"`
	GenerationZeroRightsDigest string `json:"generationZeroRightsDigest,omitempty"`
}

// MutationGenerationZeroRights records every repair-helper create attempt
// separately from the original dispatch permission. Each right is persisted
// before its provider request. Sealed closes admission; it is not terminal
// evidence. A completion is valid only after every right and the original
// dispatch have exact terminal receipts.
type MutationGenerationZeroRights struct {
	RepairBinding    EngineRemovalBinding                  `json:"repairBinding"`
	RepairRecordHash string                                `json:"repairRecordHash"`
	Rights           []MutationGenerationZeroPhysicalRight `json:"rights"`
	Sealed           bool                                  `json:"sealed,omitempty"`
	// HelperRightsRetired closes only admission/use of this repair-helper
	// permission set. Keep the JSON name for compatibility. It does not retire
	// the original dispatch, its generation-zero guard, or the key barrier.
	HelperRightsRetired bool `json:"retired,omitempty"`
}

type MutationGenerationZeroPhysicalRight struct {
	Attempt               uint32 `json:"attempt"`
	RequestDigest         string `json:"requestDigest"`
	TerminalReceiptDigest string `json:"terminalReceiptDigest,omitempty"`
}

func (r MutationGenerationZeroRights) Digest() string {
	r.Sealed = true
	return hashJSONValue(r)
}

// MutationProducer is a pure CAS transition value. The service must persist
// each transition before dispatch or ACK; this type does not provide persistence.
// Missing state is Reserved/unknown, never an implicitly open producer.
type MutationProducer struct {
	Runtime          RuntimeBinding          `json:"runtime,omitzero"`
	Profile          string                  `json:"profile,omitempty"`
	LogicalBinding   *LogicalWriterBindingV1 `json:"logicalBinding,omitempty"`
	WALIdentity      string                  `json:"walIdentity,omitempty"`
	Phase            MutationPhase           `json:"phase"`
	LastAdmitted     uint64                  `json:"lastAdmitted"`
	ClosedThrough    uint64                  `json:"closedThrough,omitempty"`
	CloseOperationID string                  `json:"closeOperationID,omitempty"`
	CloseRequestHash string                  `json:"closeRequestHash,omitempty"`
	CompactedThrough uint64                  `json:"compactedThrough,omitempty"`
	// RetiredStageThrough is the contiguous archive prefix whose remote stage
	// payloads have been retired (or whose entries had no staged payload).
	// It is persisted separately from CompactedThrough so retirement can resume
	// after an archive commit and process restart.
	RetiredStageThrough uint64                    `json:"retiredStageThrough,omitempty"`
	ArchiveHead         *MutationArchiveReference `json:"archiveHead,omitempty"`
	ArchiveScan         *MutationArchiveScan      `json:"archiveScan,omitempty"`
	EvidenceFault       string                    `json:"evidenceFault,omitempty"`
	Entries             map[uint64]MutationEntry  `json:"entries"`
}

// Validate rejects incomplete or damaged state after loading it. This is not
// authentication of provider receipts: the persistence/executor boundary must
// authenticate those separately. Only the bounded live interval is validated;
// archive contents and chain coverage require the storage-backed verifier.
func (p *MutationProducer) Validate() error {
	if p == nil {
		return fmt.Errorf("fencing: missing mutation producer")
	}
	switch p.Profile {
	case LogicalWriterProfileV1:
		if p.Runtime != (RuntimeBinding{}) || p.LogicalBinding == nil || p.LogicalBinding.Validate() != nil ||
			p.LogicalBinding.Role != StartupRoleVoterActive || p.LogicalBinding.WALIdentity != p.WALIdentity {
			return fmt.Errorf("fencing: incomplete logical mutation producer binding")
		}
		if p.WALIdentity != "" && p.LogicalBinding.ValidateBound() != nil {
			return fmt.Errorf("fencing: bound logical mutation producer lacks writer scope")
		}
	case "", RuntimeWriterProfileV1:
		if p.LogicalBinding != nil {
			return fmt.Errorf("fencing: physical mutation producer contains logical binding")
		}
		id := Identity{SourceClusterID: p.Runtime.SourceClusterID, NodeID: p.Runtime.NodeID, PodUID: p.Runtime.PodUID, RunID: p.Runtime.RunID, AssignmentID: p.Runtime.AssignmentID}
		if err := p.Runtime.Validate(id); err != nil {
			return err
		}
	default:
		return fmt.Errorf("fencing: unknown mutation producer profile")
	}
	if p.CompactedThrough > p.LastAdmitted || p.RetiredStageThrough > p.CompactedThrough || p.Entries == nil || p.LastAdmitted-p.CompactedThrough > MaxMutationJournalEntries || uint64(len(p.Entries)) != p.LastAdmitted-p.CompactedThrough {
		return fmt.Errorf("fencing: mutation journal is incomplete or exceeds capacity")
	}
	if (p.CompactedThrough == 0) != (p.ArchiveHead == nil) || (p.ArchiveHead != nil && !p.ArchiveHead.Valid()) {
		return fmt.Errorf("fencing: mutation archive watermark lacks its exact head")
	}
	if p.EvidenceFault != "" && !mutationDigestValid(p.EvidenceFault) {
		return fmt.Errorf("fencing: malformed evidence fault")
	}
	if scan := p.ArchiveScan; scan != nil {
		if p.ArchiveHead == nil || scan.Head != *p.ArchiveHead || !mutationDigestValid(scan.VerificationID) || scan.Through > p.CompactedThrough || (scan.Next == nil) != (scan.Through == 0) || (scan.Next != nil && !scan.Next.Valid()) || scan.Links > p.CompactedThrough {
			return fmt.Errorf("fencing: archive verification cursor differs from retained history")
		}
		if (scan.Sought != nil && (!scan.Sought.Valid() || scan.Sequence != 0)) || (scan.Found && scan.Sought == nil && scan.Sequence == 0) || scan.Sequence > p.CompactedThrough || (scan.ResultArchive != nil && (!scan.ResultArchive.Valid() || scan.Sequence == 0 || !scan.Found)) {
			return fmt.Errorf("fencing: invalid archive lookup binding")
		}
		covered := p.CompactedThrough - scan.Through
		if scan.Links > covered || (covered == 0 && (scan.Links != 0 || scan.Next == nil || *scan.Next != scan.Head)) || (covered > 0 && (scan.Links == 0 || (covered-1)/MaxMutationJournalEntries+1 > scan.Links)) {
			return fmt.Errorf("fencing: archive cursor claims impossible range coverage")
		}
	}
	switch p.Phase {
	case MutationReserved:
		if p.LastAdmitted != 0 {
			return fmt.Errorf("fencing: reserved mutation producer has admitted work")
		}
	case MutationOpen:
		if p.WALIdentity == "" {
			return fmt.Errorf("fencing: open mutation producer lacks WAL binding")
		}
	case MutationClosed:
		if p.CloseOperationID == "" || !mutationDigestValid(p.CloseRequestHash) || p.ClosedThrough != p.LastAdmitted || (p.LastAdmitted != 0 && p.WALIdentity == "") {
			return fmt.Errorf("fencing: mutation closure is incomplete")
		}
	default:
		return fmt.Errorf("fencing: unknown mutation producer phase")
	}
	if p.Phase != MutationClosed && (p.ClosedThrough != 0 || p.CloseOperationID != "" || p.CloseRequestHash != "") {
		return fmt.Errorf("fencing: open mutation producer retains closure metadata")
	}
	seen := make(map[string]bool, len(p.Entries))
	for sequence := p.CompactedThrough; sequence < p.LastAdmitted; {
		sequence++
		entry, exists := p.Entries[sequence]
		if !exists || seen[entry.Intent.OperationID] {
			return fmt.Errorf("fencing: mutation journal sequence or identity is invalid")
		}
		seen[entry.Intent.OperationID] = true
		if err := p.validateEntry(sequence, entry); err != nil {
			return err
		}
	}
	return nil
}

func (p *MutationProducer) validateIdentity(id Identity) error {
	if p == nil {
		return fmt.Errorf("fencing: missing mutation producer")
	}
	if p.Profile == LogicalWriterProfileV1 {
		if p.LogicalBinding == nil || p.LogicalBinding.Validate() != nil || id.SourceClusterID != p.LogicalBinding.SourceClusterID ||
			id.NodeID != p.LogicalBinding.NodeID || id.RunID != p.LogicalBinding.RunID || id.AssignmentID != p.LogicalBinding.AssignmentID ||
			(p.LogicalBinding.WALIdentity != "" && id.WALIdentity != p.LogicalBinding.WALIdentity) {
			return fmt.Errorf("fencing: logical mutation producer identity differs")
		}
		return nil
	}
	return p.Runtime.Validate(id)
}

func (p *MutationProducer) identity(walIdentity string) Identity {
	if p != nil && p.Profile == LogicalWriterProfileV1 && p.LogicalBinding != nil {
		return Identity{SourceClusterID: p.LogicalBinding.SourceClusterID, NodeID: p.LogicalBinding.NodeID,
			RunID: p.LogicalBinding.RunID, AssignmentID: p.LogicalBinding.AssignmentID, WALIdentity: walIdentity}
	}
	if p == nil {
		return Identity{}
	}
	return Identity{SourceClusterID: p.Runtime.SourceClusterID, NodeID: p.Runtime.NodeID, PodUID: p.Runtime.PodUID,
		RunID: p.Runtime.RunID, AssignmentID: p.Runtime.AssignmentID, WALIdentity: walIdentity}
}

// AdvanceArchivedStageRetirement advances exactly one archived sequence after
// its external stage retirement has completed. Persist this transition only
// after retirement; callers can safely retry a failed retirement unchanged.
func (p *MutationProducer) AdvanceArchivedStageRetirement(sequence uint64) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if sequence == 0 || sequence > p.CompactedThrough {
		return fmt.Errorf("fencing: archived stage retirement cursor is not contiguous")
	}
	if sequence <= p.RetiredStageThrough {
		return nil
	}
	if sequence != p.RetiredStageThrough+1 {
		return fmt.Errorf("fencing: archived stage retirement cursor is not contiguous")
	}
	p.RetiredStageThrough = sequence
	return nil
}

func (p *MutationProducer) validateEntry(sequence uint64, entry MutationEntry) error {
	if err := validateMutationIntent(entry.Intent); err != nil {
		return err
	}
	if len(entry.Dispatches) > MaxMutationDispatches || (len(entry.Dispatches) != 0 && entry.Preparation == nil) {
		return fmt.Errorf("fencing: dispatch permission set is invalid")
	}
	for id, dispatch := range entry.Dispatches {
		if id == "" || len(id) > 128 || (dispatch.ReceiptDigest != "" && !mutationDigestValid(dispatch.ReceiptDigest)) {
			return fmt.Errorf("fencing: dispatch permission evidence is invalid")
		}
	}
	if entry.Preparation != nil {
		if err := validateMutationPreparation(entry.Intent, *entry.Preparation); err != nil {
			return err
		}
	}
	if entry.GenerationZeroRights != nil {
		if err := p.validateGenerationZeroRights(sequence, entry, *entry.GenerationZeroRights); err != nil {
			return err
		}
	}
	if entry.Resolution != nil {
		if err := p.validateResolution(sequence, entry, *entry.Resolution); err != nil {
			return err
		}
	}
	if entry.CopyCompletion != nil {
		if entry.Preparation == nil || entry.Resolution == nil || len(entry.Dispatches) == 0 || entry.CopyCompletion.PreparationHash != p.preparationHash(sequence, entry) || !mutationDigestValid(entry.CopyCompletion.ReceiptDigest) || !allDispatchesTerminal(entry) {
			return fmt.Errorf("fencing: copy completion lacks matching prepared terminal work")
		}
		if entry.GenerationZeroGuard != nil {
			if entry.CopyCompletion.GenerationZeroRightsDigest == "" || entry.GenerationZeroRights == nil ||
				!generationZeroRightsTerminal(*entry.GenerationZeroRights) || entry.CopyCompletion.GenerationZeroRightsDigest != entry.GenerationZeroRights.Digest() {
				return fmt.Errorf("fencing: guarded copy completion lacks its exact sealed physical-right set")
			}
		} else if entry.CopyCompletion.GenerationZeroRightsDigest != "" || entry.GenerationZeroRights != nil {
			return fmt.Errorf("fencing: generation-zero physical rights lack their immutable storage guard")
		}
	}
	if entry.GuardedLogicalResolution != nil {
		if entry.Resolution == nil {
			return fmt.Errorf("fencing: guarded logical evidence lacks a logical resolution")
		}
		if entry.GenerationZeroGuard != nil {
			return fmt.Errorf("fencing: generation-zero storage guard cannot reuse guarded logical evidence")
		}
		if err := p.validateGuardedLogicalResolution(sequence, entry, *entry.GuardedLogicalResolution); err != nil {
			return err
		}
	}
	if entry.GenerationZeroGuard != nil {
		if entry.GuardedLogicalResolution != nil || entry.Resolution == nil {
			return fmt.Errorf("fencing: generation-zero storage guard has an incompatible resolution")
		}
		if err := p.validateGenerationZeroGuard(sequence, entry, *entry.GenerationZeroGuard); err != nil {
			return err
		}
	}
	return nil
}

func NewMutationProducer(id Identity, runtime RuntimeBinding) (*MutationProducer, error) {
	if err := runtime.Validate(id); err != nil {
		return nil, err
	}
	return &MutationProducer{Runtime: runtime, Phase: MutationReserved, Entries: make(map[uint64]MutationEntry)}, nil
}

// NewLogicalMutationProducer constructs the existing durable journal value
// under an explicit logical-writer profile. Pending learners are ineligible.
func NewLogicalMutationProducer(binding LogicalWriterBindingV1) (*MutationProducer, error) {
	if binding.Validate() != nil || binding.Role != StartupRoleVoterActive || binding.ProducerUID == "" || !mutationDigestValid(binding.StoreScopeHash) {
		return nil, fmt.Errorf("fencing: invalid logical mutation producer binding")
	}
	return &MutationProducer{Profile: LogicalWriterProfileV1, LogicalBinding: &binding, WALIdentity: binding.WALIdentity,
		Phase: MutationReserved, Entries: make(map[uint64]MutationEntry)}, nil
}

// Activate is authorized only after the matching durable WAL bind. A closer
// can close a Reserved producer before it ever admits a physical mutation.
func (p *MutationProducer) Activate(id Identity) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	if p.Phase != MutationReserved && p.Phase != MutationOpen {
		return ErrClosed
	}
	if err := p.validateIdentity(id); err != nil {
		return err
	}
	if p.Profile == LogicalWriterProfileV1 && (p.LogicalBinding.ProducerUID == "" || !mutationDigestValid(p.LogicalBinding.StoreScopeHash)) {
		return fmt.Errorf("fencing: logical mutation producer lacks producer scope")
	}
	if id.WALIdentity == "" || (p.WALIdentity != "" && p.WALIdentity != id.WALIdentity) {
		return fmt.Errorf("fencing: mutation producer WAL differs")
	}
	p.WALIdentity, p.Phase = id.WALIdentity, MutationOpen
	if p.Profile == LogicalWriterProfileV1 {
		binding := *p.LogicalBinding
		binding.WALIdentity = id.WALIdentity
		p.LogicalBinding = &binding
	}
	return p.Validate()
}

// BindWAL pins the already-authorized WAL on a Reserved producer without
// opening admission. It is used when the authority already knows the
// candidate's immutable WAL before the child starts; Activate remains the
// later startup bind that opens mutation admission.
func (p *MutationProducer) BindWAL(id Identity) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	if p.Phase != MutationReserved || p.LastAdmitted != 0 {
		return ErrClosed
	}
	if err := p.validateIdentity(id); err != nil {
		return err
	}
	if p.Profile == LogicalWriterProfileV1 && (p.LogicalBinding.ProducerUID == "" || !mutationDigestValid(p.LogicalBinding.StoreScopeHash)) {
		return fmt.Errorf("fencing: logical mutation producer lacks producer scope")
	}
	if id.WALIdentity == "" || (p.WALIdentity != "" && p.WALIdentity != id.WALIdentity) {
		return fmt.Errorf("fencing: mutation producer WAL differs")
	}
	p.WALIdentity = id.WALIdentity
	if p.Profile == LogicalWriterProfileV1 {
		binding := *p.LogicalBinding
		binding.WALIdentity = id.WALIdentity
		p.LogicalBinding = &binding
	}
	return p.Validate()
}

func (p *MutationProducer) Admit(sequence uint64, intent MutationIntent) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	if sequence == 0 {
		return fmt.Errorf("fencing: mutation sequence is terminal or invalid")
	}
	if sequence <= p.CompactedThrough {
		return ErrMutationArchived
	}
	if old, exists := p.Entries[sequence]; exists {
		if old.Intent == intent {
			return nil
		}
		return fmt.Errorf("fencing: mutation intent changed")
	}
	if p.Phase != MutationOpen {
		return ErrClosed
	}
	if p.LastAdmitted == ^uint64(0) || sequence != p.LastAdmitted+1 || len(p.Entries) >= MaxMutationJournalEntries {
		return fmt.Errorf("fencing: mutation sequence gap or journal capacity exhausted")
	}
	if err := validateMutationIntent(intent); err != nil {
		return err
	}
	for _, entry := range p.Entries {
		if entry.Intent.OperationID == intent.OperationID {
			return fmt.Errorf("fencing: operation ID already belongs to another sequence")
		}
	}
	p.Entries[sequence] = MutationEntry{Intent: intent}
	p.LastAdmitted = sequence
	return nil
}

func validateMutationIntent(intent MutationIntent) error {
	if intent.ContractVersion != MutationIntentVersion || !mutationDigestValid(intent.StoreScopeHash) || intent.OperationID == "" || len(intent.OperationID) > 256 || intent.Key == "" || len(intent.Key) > 1024 || intent.Key == "." || strings.HasPrefix(intent.Key, "/") || intent.Key == ".." || strings.HasPrefix(intent.Key, "../") || path.Clean(intent.Key) != intent.Key || (intent.Kind != "upload" && intent.Kind != "delete") {
		return fmt.Errorf("fencing: mutation intent is incomplete")
	}
	owner := intent.OwnerStep
	if owner.Owner == "" || len(owner.Owner) > 128 || owner.Workflow == "" || len(owner.Workflow) > 128 || owner.Step == "" || len(owner.Step) > 128 || owner.Attempt == 0 {
		return fmt.Errorf("fencing: bounded durable owner step is required")
	}
	switch intent.Condition.Mode {
	case MutationUnconditional, MutationCreateOnly:
		if intent.Condition.Generation != 0 || intent.Kind == "delete" {
			return fmt.Errorf("fencing: deletion requires its selected generation")
		}
	case MutationMatchGeneration:
		if intent.Condition.Generation <= 0 {
			return fmt.Errorf("fencing: exact selected generation is required")
		}
	default:
		return fmt.Errorf("fencing: original mutation condition is required")
	}
	if intent.Kind == "upload" && (intent.PayloadRef == "" || len(intent.PayloadRef) > 1024 || !mutationDigestValid(intent.PayloadDigest) || intent.PayloadSize < 0) {
		return fmt.Errorf("fencing: immutable staged payload is required")
	}
	if intent.Kind == "delete" && (intent.PayloadRef != "" || intent.PayloadDigest != "" || intent.PayloadSize != 0) {
		return fmt.Errorf("fencing: deletion cannot carry a payload")
	}
	return nil
}

func (p *MutationProducer) Close(operationID, requestHash string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if operationID == "" || !mutationDigestValid(requestHash) {
		return fmt.Errorf("fencing: invalid mutation closure")
	}
	if p.Phase == MutationClosed {
		if p.CloseOperationID == operationID && p.CloseRequestHash == requestHash {
			for sequence, entry := range p.Entries {
				entry.DispatchClosed = true
				p.Entries[sequence] = entry
			}
			return nil
		}
		return ErrClosed
	}
	p.Phase, p.ClosedThrough = MutationClosed, p.LastAdmitted
	p.CloseOperationID, p.CloseRequestHash = operationID, requestHash
	// Close seals every already-admitted entry against new dispatch rights.
	// Existing grants remain obligations and can still be terminalized.
	for sequence, entry := range p.Entries {
		entry.DispatchClosed = true
		p.Entries[sequence] = entry
	}
	return nil
}

func (p *MutationProducer) Prepare(sequence uint64, prepared MutationPreparation) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Resolution != nil {
		return fmt.Errorf("fencing: mutation preparation has no executable admission")
	}
	if err := validateMutationPreparation(entry.Intent, prepared); err != nil {
		return err
	}
	if entry.Preparation != nil && *entry.Preparation != prepared {
		return fmt.Errorf("fencing: mutation preparation is immutable")
	}
	entry.Preparation = &prepared
	p.Entries[sequence] = entry
	return nil
}

func validateMutationPreparation(intent MutationIntent, prepared MutationPreparation) error {
	if !prepared.GenerationSet || prepared.Generation < 0 || !mutationDigestValid(prepared.StoreScopeHash) {
		return fmt.Errorf("fencing: fixed storage preconditions are required")
	}
	if prepared.StoreScopeHash != intent.StoreScopeHash {
		return fmt.Errorf("fencing: preparation changed the caller storage scope")
	}
	if intent.Condition.Mode == MutationCreateOnly && prepared.Generation != 0 || intent.Condition.Mode == MutationMatchGeneration && prepared.Generation != intent.Condition.Generation {
		return fmt.Errorf("fencing: preparation changed the original mutation condition")
	}
	if prepared.Profile != MutationExecutionProfile && prepared.Profile != MutationHeldExecutionProfile && prepared.Profile != "gcs-resumable-v1" {
		return fmt.Errorf("fencing: unsupported mutation profile")
	}
	if prepared.Profile == MutationHeldExecutionProfile && prepared.Generation == 0 && intent.Kind == "upload" && !IsHeldCreateEligible(intent, prepared) {
		return fmt.Errorf("fencing: zero-generation held upload is not eligible")
	}
	if prepared.Profile == "gcs-resumable-v1" && intent.Kind != "upload" {
		return fmt.Errorf("fencing: resumable profile only supports uploads")
	}
	if (prepared.Profile == "gcs-resumable-v1") != (prepared.SessionRef != "") {
		return fmt.Errorf("fencing: upload session preparation differs from profile")
	}
	if intent.Kind == "delete" && prepared.Generation == 0 {
		return fmt.Errorf("fencing: deletion requires an exact nonzero generation")
	}
	return nil
}

// PreparationHash binds evidence to this exact runtime/WAL/sequence, not just
// the object key. A receipt from another producer cannot settle this producer.
func (p *MutationProducer) PreparationHash(sequence uint64) (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil {
		return "", fmt.Errorf("fencing: mutation has no preparation")
	}
	return p.preparationHash(sequence, entry), nil
}

// ResolveGuardedLogical records a root-authenticated logical resolution without
// changing the original receipt or marking its physical dispatch terminal.
func (p *MutationProducer) ResolveGuardedLogical(producerUID string, sequence uint64, evidence MutationGuardedLogicalResolution) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists {
		return fmt.Errorf("fencing: guarded logical resolution lacks an admitted mutation")
	}
	if producerUID == "" || evidence.ProducerUID != producerUID {
		return fmt.Errorf("fencing: guarded logical resolution producer identity differs")
	}
	if err := p.validateGuardedLogicalResolution(sequence, entry, evidence); err != nil {
		return err
	}
	if entry.GuardedLogicalResolution != nil && *entry.GuardedLogicalResolution != evidence {
		return fmt.Errorf("fencing: guarded logical resolution is immutable")
	}
	if entry.Resolution == nil {
		entry.Resolution = &MutationResolution{Outcome: MutationCommitted, PreparationHash: evidence.PreparationHash,
			ReceiptDigest: evidence.GuardDigest, PayloadDigest: evidence.PayloadDigest}
	}
	entry.GuardedLogicalResolution = &evidence
	p.Entries[sequence] = entry
	return nil
}

// RecordGenerationZeroPhysicalRight durably accounts for one repair-helper
// request before it is sent. An existing exact attempt is not a reusable
// permission; callers must treat created=false as already consumed/ambiguous.
func (p *MutationProducer) RecordGenerationZeroPhysicalRight(producerUID string, sequence uint64, repair EngineRepairRecord, attempt uint32, requestDigest string) (bool, error) {
	if err := p.Validate(); err != nil {
		return false, err
	}
	entry, exists := p.Entries[sequence]
	if !exists || producerUID == "" || repair.Validate() != nil || repair.IdentityDigest() == "" ||
		!repair.evidencePhase().atLeast(RepairStorageClosing) || p.Phase != MutationClosed || sequence == 0 || sequence > p.ClosedThrough ||
		entry.Preparation == nil || !IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		!mutationDigestValid(requestDigest) {
		return false, fmt.Errorf("fencing: generation-zero helper right is not bound to a closed held-create dispatch")
	}
	if entry.GenerationZeroGuard != nil && (entry.GenerationZeroGuard.RepairRecordHash != repair.IdentityDigest() || entry.GenerationZeroGuard.RepairBinding != repair.Binding) {
		return false, fmt.Errorf("fencing: generation-zero helper right differs from its immutable guard")
	}
	if entry.GenerationZeroRights == nil {
		entry.GenerationZeroRights = &MutationGenerationZeroRights{RepairBinding: repair.Binding, RepairRecordHash: repair.IdentityDigest()}
	}
	rights := entry.GenerationZeroRights
	if rights.RepairBinding != repair.Binding || rights.RepairRecordHash != repair.IdentityDigest() || rights.Sealed {
		return false, fmt.Errorf("fencing: generation-zero helper-right admission is sealed or belongs to another repair")
	}
	if attempt == 0 || int(attempt) > len(rights.Rights)+1 || int(attempt) > MaxMutationDispatches {
		return false, fmt.Errorf("fencing: generation-zero helper-right attempt is not contiguous or exceeds capacity")
	}
	if int(attempt) <= len(rights.Rights) {
		if rights.Rights[attempt-1].RequestDigest != requestDigest {
			return false, fmt.Errorf("fencing: generation-zero helper-right attempt changed its request")
		}
		return false, nil
	}
	rights.Rights = append(rights.Rights, MutationGenerationZeroPhysicalRight{Attempt: attempt, RequestDigest: requestDigest})
	p.Entries[sequence] = entry
	if err := p.Validate(); err != nil {
		return false, err
	}
	return true, nil
}

// RecordGenerationZeroPhysicalTerminal binds an actual provider terminal
// receipt to exactly one pre-recorded helper request. Future-effect exclusion
// or a live object readback alone is not a physical terminal receipt.
func (p *MutationProducer) RecordGenerationZeroPhysicalTerminal(sequence uint64, attempt uint32, requestDigest, receiptDigest string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.GenerationZeroRights == nil || entry.GenerationZeroRights.HelperRightsRetired ||
		attempt == 0 || int(attempt) > len(entry.GenerationZeroRights.Rights) || !mutationDigestValid(requestDigest) || !mutationDigestValid(receiptDigest) {
		return fmt.Errorf("fencing: generation-zero terminal receipt lacks its pre-dispatch right")
	}
	right := &entry.GenerationZeroRights.Rights[attempt-1]
	if right.Attempt != attempt || right.RequestDigest != requestDigest ||
		(right.TerminalReceiptDigest != "" && right.TerminalReceiptDigest != receiptDigest) {
		return fmt.Errorf("fencing: generation-zero terminal receipt differs from its exact request")
	}
	right.TerminalReceiptDigest = receiptDigest
	p.Entries[sequence] = entry
	return p.Validate()
}

// SealGenerationZeroRights closes helper-right admission after the permanent
// guard is persisted. Sealing does not imply terminality; unresolved rights
// continue to block CopyCompletion and compaction.
func (p *MutationProducer) SealGenerationZeroRights(producerUID string, sequence uint64, repair EngineRepairRecord) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || producerUID == "" || entry.GenerationZeroGuard == nil || entry.GenerationZeroRights == nil ||
		repair.Validate() != nil || entry.GenerationZeroGuard.ProducerUID != producerUID ||
		entry.GenerationZeroGuard.RepairRecordHash != repair.IdentityDigest() || entry.GenerationZeroGuard.RepairBinding != repair.Binding ||
		entry.GenerationZeroRights.RepairRecordHash != repair.IdentityDigest() || entry.GenerationZeroRights.RepairBinding != repair.Binding {
		return fmt.Errorf("fencing: generation-zero helper-right seal is not bound to the exact guard")
	}
	entry.GenerationZeroRights.Sealed = true
	p.Entries[sequence] = entry
	return p.Validate()
}

// RetireGenerationZeroRights records that the sealed helper permission set is
// fully terminal. It preserves the rights and receipts as journal evidence;
// this retires helper rights only, not the original dispatch, G0 guard, or key
// barrier. Those remain governed by their own terminal evidence.
func (p *MutationProducer) RetireGenerationZeroRights(producerUID string, sequence uint64, repair EngineRepairRecord) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || producerUID == "" || entry.GenerationZeroGuard == nil || entry.GenerationZeroRights == nil ||
		repair.Validate() != nil || entry.GenerationZeroGuard.ProducerUID != producerUID ||
		entry.GenerationZeroGuard.RepairRecordHash != repair.IdentityDigest() || entry.GenerationZeroGuard.RepairBinding != repair.Binding ||
		entry.GenerationZeroRights.RepairRecordHash != repair.IdentityDigest() || entry.GenerationZeroRights.RepairBinding != repair.Binding ||
		!generationZeroRightsTerminal(*entry.GenerationZeroRights) {
		return fmt.Errorf("fencing: generation-zero rights are not sealed and terminal for the exact guard")
	}
	if entry.GenerationZeroRights.HelperRightsRetired {
		return nil
	}
	entry.GenerationZeroRights.HelperRightsRetired = true
	p.Entries[sequence] = entry
	return p.Validate()
}

// RecordGenerationZeroGuard records a distinct repair-scoped storage guard.
// It preserves an existing exact committed receipt and never terminalizes the
// original physical dispatch permission.
func (p *MutationProducer) RecordGenerationZeroGuard(producerUID string, sequence uint64, repair EngineRepairRecord, guard MutationGenerationZeroGuard) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || producerUID == "" || guard.ProducerUID != producerUID || repair.Validate() != nil ||
		repair.IdentityDigest() != guard.RepairRecordHash || repair.Binding != guard.RepairBinding || !repair.evidencePhase().atLeast(RepairStorageClosing) {
		return fmt.Errorf("fencing: generation-zero guard producer identity differs")
	}
	candidate := entry
	if candidate.Resolution == nil {
		candidate.Resolution = &MutationResolution{Outcome: MutationCommitted, PreparationHash: guard.PreparationHash,
			ReceiptDigest: guard.GuardDigest, PayloadDigest: guard.PayloadDigest}
	}
	if err := p.validateGenerationZeroGuard(sequence, candidate, guard); err != nil {
		return err
	}
	if entry.GenerationZeroGuard != nil && *entry.GenerationZeroGuard != guard {
		return fmt.Errorf("fencing: generation-zero storage guard is immutable")
	}
	if entry.Resolution != nil && (entry.Resolution.Outcome != MutationCommitted || entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest) {
		return fmt.Errorf("fencing: generation-zero guard conflicts with the original mutation resolution")
	}
	entry.Resolution = candidate.Resolution
	entry.GenerationZeroGuard = &guard
	p.Entries[sequence] = entry
	return p.Validate()
}

// ValidateGenerationZeroGuard rechecks the reference against an exact closed
// producer entry. The caller must independently load GuardRef, verify its UID
// and digest, and read back the held object's exact positive generation.
func (p *MutationProducer) ValidateGenerationZeroGuard(producerUID string, sequence uint64, repair EngineRepairRecord, entry MutationEntry) error {
	if p == nil || p.Profile == LogicalWriterProfileV1 || p.Phase != MutationClosed || p.WALIdentity == "" || producerUID == "" || entry.GenerationZeroGuard == nil || entry.GenerationZeroGuard.ProducerUID != producerUID ||
		repair.Validate() != nil || repair.IdentityDigest() != entry.GenerationZeroGuard.RepairRecordHash || repair.Binding != entry.GenerationZeroGuard.RepairBinding || !repair.evidencePhase().atLeast(RepairStorageClosing) {
		return fmt.Errorf("fencing: generation-zero guard producer identity differs")
	}
	id := Identity{SourceClusterID: p.Runtime.SourceClusterID, NodeID: p.Runtime.NodeID, PodUID: p.Runtime.PodUID, RunID: p.Runtime.RunID, AssignmentID: p.Runtime.AssignmentID, WALIdentity: p.WALIdentity}
	if err := p.Runtime.Validate(id); err != nil {
		return err
	}
	return p.validateGenerationZeroGuard(sequence, entry, *entry.GenerationZeroGuard)
}

func (p *MutationProducer) validateGenerationZeroGuard(sequence uint64, entry MutationEntry, guard MutationGenerationZeroGuard) error {
	if p == nil || p.Profile == LogicalWriterProfileV1 || p.Phase != MutationClosed || sequence == 0 || sequence > p.ClosedThrough || entry.Preparation == nil ||
		guard.Version != MutationGenerationZeroGuardVersion || guard.RepairBinding.Validate() != nil || guard.RepairBinding.FreezeSlot == 0 ||
		guard.RepairBinding.SourceClusterID != p.Runtime.SourceClusterID || guard.RepairBinding.OldMemberID != p.Runtime.NodeID ||
		guard.RepairBinding.OldWALIdentity != p.WALIdentity || guard.RepairBinding.StatefulSetUID != p.Runtime.StatefulSetUID ||
		guard.Runtime != p.Runtime || guard.WALIdentity != p.WALIdentity || guard.ProducerUID == "" || guard.Sequence != sequence ||
		guard.Intent != entry.Intent || guard.Preparation != *entry.Preparation || guard.StorageScopeHash != entry.Intent.StoreScopeHash ||
		guard.Key != entry.Intent.Key || guard.PreparationHash != p.preparationHash(sequence, entry) || guard.DispatchID == "" ||
		!mutationDigestValid(guard.RepairRecordHash) || !mutationDigestValid(guard.GuardRootIdentity) ||
		guard.GuardRef == "" || len(guard.GuardRef) > 512 || !strings.HasPrefix(guard.GuardRef, "mutation-effect-guards/") ||
		strings.HasPrefix(guard.GuardRef, "/") || path.Clean(guard.GuardRef) != guard.GuardRef || strings.HasPrefix(guard.GuardRef, "../") ||
		guard.GuardUID == "" || len(guard.GuardUID) > 128 || !mutationDigestValid(guard.GuardDigest) ||
		guard.GuardDigest != guard.Digest() ||
		guard.ObjectGeneration <= 0 || guard.ObjectMetageneration <= 0 || guard.ObjectSize != entry.Intent.PayloadSize ||
		guard.PayloadDigest != entry.Intent.PayloadDigest || !mutationDigestValid(guard.PayloadDigest) {
		return fmt.Errorf("fencing: generation-zero guard is not bound to the exact prepared producer tuple")
	}
	if !IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		entry.Resolution == nil || entry.Resolution.Outcome != MutationCommitted || entry.Resolution.AllCopiesTerminal ||
		entry.Resolution.PreparationHash != guard.PreparationHash || entry.Resolution.PayloadDigest != guard.PayloadDigest {
		return fmt.Errorf("fencing: generation-zero guard requires one unresolved committed held create")
	}
	rights := entry.GenerationZeroRights
	if rights == nil || rights.RepairRecordHash != guard.RepairRecordHash || rights.RepairBinding != guard.RepairBinding {
		return fmt.Errorf("fencing: generation-zero guard lacks its repair-bound physical-right set")
	}
	_, exists := entry.Dispatches[guard.DispatchID]
	if !exists {
		return fmt.Errorf("fencing: generation-zero guard does not bind the original dispatch")
	}
	return nil
}

func (p *MutationProducer) validateGenerationZeroRights(sequence uint64, entry MutationEntry, rights MutationGenerationZeroRights) error {
	if p == nil || p.Profile == LogicalWriterProfileV1 || p.Phase != MutationClosed || sequence == 0 || sequence > p.ClosedThrough || entry.Preparation == nil ||
		!IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		rights.RepairBinding.Validate() != nil || rights.RepairBinding.SourceClusterID != p.Runtime.SourceClusterID ||
		rights.RepairBinding.OldMemberID != p.Runtime.NodeID || rights.RepairBinding.OldWALIdentity != p.WALIdentity ||
		rights.RepairBinding.StatefulSetUID != p.Runtime.StatefulSetUID || !mutationDigestValid(rights.RepairRecordHash) || len(rights.Rights) == 0 ||
		len(rights.Rights) > MaxMutationDispatches {
		return fmt.Errorf("fencing: generation-zero physical rights are not bound to the exact closed held create")
	}
	if entry.GenerationZeroGuard != nil && (entry.GenerationZeroGuard.RepairRecordHash != rights.RepairRecordHash || entry.GenerationZeroGuard.RepairBinding != rights.RepairBinding) {
		return fmt.Errorf("fencing: generation-zero physical-right repair binding differs from its guard")
	}
	for index, right := range rights.Rights {
		if right.Attempt != uint32(index+1) || !mutationDigestValid(right.RequestDigest) ||
			(right.TerminalReceiptDigest != "" && !mutationDigestValid(right.TerminalReceiptDigest)) {
			return fmt.Errorf("fencing: generation-zero physical-right order or evidence is invalid")
		}
	}
	if rights.Sealed && entry.GenerationZeroGuard == nil {
		return fmt.Errorf("fencing: generation-zero physical-right set sealed before its storage guard")
	}
	if rights.HelperRightsRetired && (entry.GenerationZeroGuard == nil || !generationZeroRightsTerminal(rights)) {
		return fmt.Errorf("fencing: generation-zero physical-right set retired before exact terminal guard")
	}
	return nil
}

func generationZeroRightsTerminal(rights MutationGenerationZeroRights) bool {
	if !rights.Sealed || len(rights.Rights) == 0 {
		return false
	}
	for _, right := range rights.Rights {
		if !mutationDigestValid(right.TerminalReceiptDigest) {
			return false
		}
	}
	return true
}

func (g MutationGenerationZeroGuard) Digest() string {
	g.GuardDigest = ""
	return hashJSONValue(g)
}

// ValidateGuardedLogicalResolution checks archived or separately persisted
// evidence against an exact closed producer entry without changing that entry.
func (p *MutationProducer) ValidateGuardedLogicalResolution(producerUID string, sequence uint64, entry MutationEntry) error {
	if p == nil || p.Phase != MutationClosed || p.WALIdentity == "" || producerUID == "" || entry.GuardedLogicalResolution == nil || entry.GuardedLogicalResolution.ProducerUID != producerUID {
		return fmt.Errorf("fencing: guarded logical resolution producer identity differs")
	}
	id := p.identity(p.WALIdentity)
	if err := p.validateIdentity(id); err != nil {
		return err
	}
	return p.validateGuardedLogicalResolution(sequence, entry, *entry.GuardedLogicalResolution)
}

func (p *MutationProducer) validateGuardedLogicalResolution(sequence uint64, entry MutationEntry, evidence MutationGuardedLogicalResolution) error {
	if p == nil {
		return fmt.Errorf("fencing: missing mutation producer")
	}
	identityBindingMatches := evidence.AuthorityProfile == p.Profile && evidence.LogicalBinding == nil && evidence.Runtime == p.Runtime
	if p.Profile == LogicalWriterProfileV1 {
		identityBindingMatches = evidence.AuthorityProfile == LogicalWriterProfileV1 && evidence.Runtime == (RuntimeBinding{}) && sameLogicalBinding(evidence.LogicalBinding, p.LogicalBinding)
	}
	if p == nil || p.Phase != MutationClosed || sequence > p.ClosedThrough || entry.Preparation == nil || evidence.Version != MutationGuardedLogicalResolutionVersion ||
		!identityBindingMatches || evidence.WALIdentity != p.WALIdentity || evidence.ProducerUID == "" || evidence.Sequence != sequence ||
		evidence.Intent != entry.Intent || evidence.Preparation != *entry.Preparation || evidence.Profile != MutationHeldExecutionProfile ||
		evidence.Preparation.Profile != MutationHeldExecutionProfile || evidence.PreparationHash != p.preparationHash(sequence, entry) ||
		evidence.GuardRootIdentity == "" || !mutationDigestValid(evidence.GuardRootIdentity) || evidence.GuardVersion != MutationGuardedLogicalResolutionVersion || evidence.GuardRef == "" || len(evidence.GuardRef) > 512 ||
		!strings.HasPrefix(evidence.GuardRef, "mutation-guards/") || strings.HasPrefix(evidence.GuardRef, "/") || path.Clean(evidence.GuardRef) != evidence.GuardRef || strings.HasPrefix(evidence.GuardRef, "../") ||
		!mutationDigestValid(evidence.GuardDigest) || evidence.ObjectGeneration <= 0 || evidence.ObjectMetageneration <= 0 ||
		evidence.ObjectSize != entry.Intent.PayloadSize || evidence.PayloadDigest != entry.Intent.PayloadDigest || !mutationDigestValid(evidence.PayloadDigest) {
		return fmt.Errorf("fencing: guarded logical resolution is not bound to the exact prepared producer tuple")
	}
	if !IsHeldCreateEligible(entry.Intent, *entry.Preparation) ||
		(entry.Resolution != nil && (entry.Resolution.Outcome != MutationCommitted || entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest)) ||
		entry.DispatchClosed == false || len(entry.Dispatches) != 1 {
		return fmt.Errorf("fencing: guarded logical resolution is not eligible for an unresolved held create")
	}
	if _, ok := entry.Dispatches[evidence.DispatchID]; !ok || evidence.DispatchID == "" {
		return fmt.Errorf("fencing: guarded logical resolution does not bind the original dispatch grant")
	}
	for _, dispatch := range entry.Dispatches {
		if dispatch.ReceiptDigest != "" {
			return fmt.Errorf("fencing: guarded logical resolution cannot rewrite a terminal dispatch receipt")
		}
	}
	return nil
}

func (p *MutationProducer) preparationHash(sequence uint64, entry MutationEntry) string {
	if p.Profile == LogicalWriterProfileV1 {
		encoded, _ := json.Marshal(struct {
			Domain         string
			Profile        string
			LogicalBinding LogicalWriterBindingV1
			WALIdentity    string
			Sequence       uint64
			Intent         MutationIntent
			Prepared       MutationPreparation
		}{"rhiza-logical-writer-mutation-preparation-v1", p.Profile, *p.LogicalBinding, p.WALIdentity, sequence, entry.Intent, *entry.Preparation})
		digest := sha256.Sum256(encoded)
		return hex.EncodeToString(digest[:])
	}
	encoded, _ := json.Marshal(struct {
		Runtime     RuntimeBinding
		WALIdentity string
		Sequence    uint64
		Intent      MutationIntent
		Prepared    MutationPreparation
	}{p.Runtime, p.WALIdentity, sequence, entry.Intent, *entry.Preparation})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (p *MutationProducer) Resolve(sequence uint64, resolution MutationResolution) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil {
		return fmt.Errorf("fencing: mutation resolution lacks prepared work or durable evidence")
	}
	if err := p.validateResolution(sequence, entry, resolution); err != nil {
		return err
	}
	if entry.Resolution != nil && *entry.Resolution != resolution {
		return fmt.Errorf("fencing: mutation terminal result is immutable")
	}
	entry.Resolution = &resolution
	p.Entries[sequence] = entry
	return nil
}

func (p *MutationProducer) validateResolution(sequence uint64, entry MutationEntry, resolution MutationResolution) error {
	if !mutationDigestValid(resolution.ReceiptDigest) {
		return fmt.Errorf("fencing: resolution requires durable evidence")
	}
	if err := validateMutationResolutionReason(entry, resolution); err != nil {
		return err
	}
	if resolution.ConditionFailed && (resolution.Outcome != MutationRejected || entry.Intent.Condition.Mode == MutationUnconditional) {
		return fmt.Errorf("fencing: condition failure must be a conditional terminal rejection")
	}
	if resolution.AllCopiesTerminal && !allDispatchesTerminal(entry) {
		return fmt.Errorf("fencing: executable dispatch permissions remain unsealed or unresolved")
	}
	if resolution.AllCopiesTerminal && entry.Preparation != nil && len(entry.Dispatches) == 0 {
		closedCancellation := p.Phase == MutationClosed && sequence <= p.ClosedThrough && entry.DispatchClosed &&
			resolution.Outcome == MutationRejected && !resolution.ConditionFailed && resolution.PayloadDigest == ""
		if !closedCancellation {
			return fmt.Errorf("fencing: prepared terminal mutation lacks a granted dispatch or exact closed cancellation")
		}
	}
	if resolution.AllCopiesTerminal && len(entry.Dispatches) == 0 && resolution.Reason == MutationResolutionReasonGenerationZeroPinned &&
		(p.Phase != MutationClosed || sequence > p.ClosedThrough) {
		return fmt.Errorf("fencing: pinned-generation cancellation requires a closed producer tuple")
	}
	if entry.Preparation == nil {
		if resolution.Outcome != MutationRejected || resolution.PreparationHash != "" || resolution.PayloadDigest != "" || resolution.ConditionFailed || !resolution.AllCopiesTerminal {
			return fmt.Errorf("fencing: unprepared work can only be terminally cancelled")
		}
		return nil
	}
	if resolution.PreparationHash != p.preparationHash(sequence, entry) {
		return fmt.Errorf("fencing: mutation resolution belongs to another preparation")
	}
	switch resolution.Outcome {
	case MutationCommitted:
		if resolution.PayloadDigest != entry.Intent.PayloadDigest {
			return fmt.Errorf("fencing: committed mutation payload attribution differs")
		}
	case MutationRejected:
		if !resolution.AllCopiesTerminal || resolution.PayloadDigest != "" {
			return fmt.Errorf("fencing: rejection requires all copies terminal and no successful payload")
		}
	case MutationExcluded:
		if entry.Preparation.Generation == 0 {
			return fmt.Errorf("fencing: zero-generation future exclusion requires permanent guard evidence")
		}
		if resolution.PayloadDigest != "" {
			return fmt.Errorf("fencing: excluded work is not a committed payload")
		}
	default:
		return fmt.Errorf("fencing: mutation outcome is unresolved")
	}
	return nil
}

func validateMutationResolutionReason(entry MutationEntry, resolution MutationResolution) error {
	switch resolution.Reason {
	case "":
		return nil
	case MutationResolutionReasonGenerationZeroPinned:
		if resolution.Outcome != MutationRejected || resolution.ConditionFailed || !resolution.AllCopiesTerminal ||
			entry.Intent.Kind != "delete" || entry.Intent.Condition.Mode != MutationMatchGeneration || !entry.DispatchClosed || len(entry.Dispatches) != 0 {
			return fmt.Errorf("fencing: pinned-generation deferral is not a terminal no-dispatch delete")
		}
		if entry.Preparation == nil {
			if resolution.PreparationHash != "" {
				return fmt.Errorf("fencing: unprepared pinned-generation deferral has a preparation hash")
			}
			return nil
		}
		if !entry.Preparation.GenerationSet || entry.Preparation.Generation != entry.Intent.Condition.Generation ||
			resolution.PreparationHash == "" {
			return fmt.Errorf("fencing: prepared pinned-generation deferral differs from the exact selected generation")
		}
		return nil
	default:
		return fmt.Errorf("fencing: unknown mutation terminal resolution reason")
	}
}

// AdmitAndCancel consumes an allocated sequence without granting execution. The
// authority must persist this composed transition in one producer CAS.
func (p *MutationProducer) AdmitAndCancel(sequence uint64, intent MutationIntent, receiptDigest string) error {
	if p == nil {
		return fmt.Errorf("fencing: missing mutation producer")
	}
	next := *p
	next.Entries = maps.Clone(p.Entries)
	if err := next.Admit(sequence, intent); err != nil {
		return err
	}
	if err := next.CancelUnprepared(sequence, receiptDigest); err != nil {
		return err
	}
	*p = next
	return nil
}

// CancelUnprepared is serialized against Prepare on this same CAS value. A
// scheduler that created only an external preparation cannot dispatch it.
func (p *MutationProducer) CancelUnprepared(sequence uint64, receiptDigest string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation != nil || !mutationDigestValid(receiptDigest) {
		return fmt.Errorf("fencing: mutation may already be executable")
	}
	entry.DispatchClosed = true
	resolution := MutationResolution{Outcome: MutationRejected, ReceiptDigest: receiptDigest, AllCopiesTerminal: true}
	if entry.Resolution != nil && *entry.Resolution != resolution {
		return fmt.Errorf("fencing: mutation terminal result is immutable")
	}
	entry.Resolution = &resolution
	p.Entries[sequence] = entry
	return nil
}

// CancelPreparedNoDispatch settles a closed entry whose exact preparation was
// recorded but for which no executable dispatch permission was ever granted.
// The caller supplies a digest of the exact closed tuple; the journal CAS is
// the evidence that no dispatch existed and races atomically with grant.
func (p *MutationProducer) CancelPreparedNoDispatch(sequence uint64, preparationHash, receiptDigest string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if p.Phase != MutationClosed || sequence == 0 || sequence > p.ClosedThrough || !exists || entry.Preparation == nil ||
		entry.Resolution != nil || !entry.DispatchClosed || len(entry.Dispatches) != 0 || !mutationDigestValid(preparationHash) ||
		preparationHash != p.preparationHash(sequence, entry) || !mutationDigestValid(receiptDigest) {
		return fmt.Errorf("fencing: prepared mutation has dispatch rights or differs from closed cancellation")
	}
	entry.Resolution = &MutationResolution{Outcome: MutationRejected, PreparationHash: preparationHash, ReceiptDigest: receiptDigest, AllCopiesTerminal: true}
	p.Entries[sequence] = entry
	return nil
}

// CancelPinnedGenerationDelete records a distinct terminal result only after
// the trusted executor verified the canonical live generation pin. The journal
// method independently limits it to a closed no-dispatch delete of that exact
// selected generation; it does not itself authenticate the storage barrier.
func (p *MutationProducer) CancelPinnedGenerationDelete(sequence uint64, preparationHash, receiptDigest string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if p.Phase != MutationClosed || sequence == 0 || sequence > p.ClosedThrough || !exists || entry.Resolution != nil ||
		!entry.DispatchClosed || len(entry.Dispatches) != 0 || entry.Intent.Kind != "delete" ||
		entry.Intent.Condition.Mode != MutationMatchGeneration || !mutationDigestValid(receiptDigest) {
		return fmt.Errorf("fencing: pinned-generation delete cancellation differs from the closed no-dispatch tuple")
	}
	if entry.Preparation == nil {
		if preparationHash != "" {
			return fmt.Errorf("fencing: unprepared pinned-generation delete has a preparation hash")
		}
	} else if !entry.Preparation.GenerationSet || entry.Preparation.Generation != entry.Intent.Condition.Generation ||
		!mutationDigestValid(preparationHash) || preparationHash != p.preparationHash(sequence, entry) {
		return fmt.Errorf("fencing: prepared pinned-generation delete differs from the selected generation")
	}
	resolution := MutationResolution{Outcome: MutationRejected, PreparationHash: preparationHash, ReceiptDigest: receiptDigest,
		AllCopiesTerminal: true, Reason: MutationResolutionReasonGenerationZeroPinned}
	if err := validateMutationResolutionReason(entry, resolution); err != nil {
		return err
	}
	entry.Resolution = &resolution
	p.Entries[sequence] = entry
	return p.Validate()
}

// ConfirmCopiesTerminal is called only after the trusted executor verifies all
// copies/sessions AND every outstanding executable permission are terminal,
// with dispatch admission sealed. A timeout, a single response, or absence
// from a recent object listing cannot produce this evidence.
func (p *MutationProducer) ConfirmCopiesTerminal(sequence uint64, completion MutationCopyCompletion) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil || entry.Resolution == nil || len(entry.Dispatches) == 0 || completion.PreparationHash != p.preparationHash(sequence, entry) || !mutationDigestValid(completion.ReceiptDigest) || !allDispatchesTerminal(entry) {
		return fmt.Errorf("fencing: copy completion lacks matching prepared terminal work")
	}
	if entry.GenerationZeroGuard != nil {
		if entry.GenerationZeroRights == nil || !generationZeroRightsTerminal(*entry.GenerationZeroRights) ||
			completion.GenerationZeroRightsDigest != entry.GenerationZeroRights.Digest() {
			return fmt.Errorf("fencing: guarded copy completion lacks the exact terminal helper-right set")
		}
	} else if completion.GenerationZeroRightsDigest != "" || entry.GenerationZeroRights != nil {
		return fmt.Errorf("fencing: non-guarded copy completion contains generation-zero rights")
	}
	if entry.CopyCompletion != nil && *entry.CopyCompletion != completion {
		return fmt.Errorf("fencing: copy completion evidence is immutable")
	}
	entry.CopyCompletion = &completion
	p.Entries[sequence] = entry
	return nil
}

// GrantDispatch must be persisted before handing executable permission to any
// worker. Reading a prepared descriptor alone grants no right to send a request.
func (p *MutationProducer) GrantDispatch(sequence uint64, dispatchID string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil || entry.Resolution != nil || entry.DispatchClosed || dispatchID == "" || len(dispatchID) > 128 {
		return fmt.Errorf("fencing: dispatch admission is closed or unprepared")
	}
	if old, exists := entry.Dispatches[dispatchID]; exists {
		if old.ReceiptDigest == "" {
			return nil
		}
		return fmt.Errorf("fencing: settled dispatch cannot execute again")
	}
	if len(entry.Dispatches) >= MaxMutationDispatches {
		return fmt.Errorf("fencing: dispatch capacity exhausted")
	}
	if entry.Dispatches == nil {
		entry.Dispatches = make(map[string]MutationDispatch)
	}
	entry.Dispatches[dispatchID] = MutationDispatch{}
	p.Entries[sequence] = entry
	return nil
}

// SealDispatch permanently stops new permissions, not just new HTTP requests.
func (p *MutationProducer) SealDispatch(sequence uint64) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil {
		return fmt.Errorf("fencing: dispatch has no prepared admission")
	}
	entry.DispatchClosed = true
	p.Entries[sequence] = entry
	return nil
}

// RecordDispatchTerminal records a receipt only after a trusted executor verifies
// its producer/sequence/permission attribution and the end of executable rights.
// The digest's shape is not authentication of that underlying receipt.
func (p *MutationProducer) RecordDispatchTerminal(sequence uint64, dispatchID, receiptDigest string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || !mutationDigestValid(receiptDigest) {
		return fmt.Errorf("fencing: dispatch terminal evidence is incomplete")
	}
	dispatch, exists := entry.Dispatches[dispatchID]
	if !exists {
		return fmt.Errorf("fencing: unknown dispatch permission")
	}
	if dispatch.ReceiptDigest != "" && dispatch.ReceiptDigest != receiptDigest {
		return fmt.Errorf("fencing: dispatch receipt is immutable")
	}
	dispatch.ReceiptDigest = receiptDigest
	entry.Dispatches[dispatchID] = dispatch
	p.Entries[sequence] = entry
	return nil
}

func allDispatchesTerminal(entry MutationEntry) bool {
	if !entry.DispatchClosed {
		return false
	}
	for _, dispatch := range entry.Dispatches {
		if !mutationDigestValid(dispatch.ReceiptDigest) {
			return false
		}
	}
	return true
}

func (p *MutationProducer) BarrierComplete() bool {
	if p.Validate() != nil || p.Phase != MutationClosed || p.EvidenceFault != "" {
		return false
	}
	for seq := p.CompactedThrough; seq < p.ClosedThrough; {
		seq++
		entry, exists := p.Entries[seq]
		if !exists || !mutationEntryTerminal(entry) {
			return false
		}
	}
	return true
}

func mutationEntryTerminal(entry MutationEntry) bool {
	if entry.Resolution == nil {
		return false
	}
	if entry.GenerationZeroGuard != nil {
		return entry.CopyCompletion != nil && entry.GenerationZeroRights != nil && generationZeroRightsTerminal(*entry.GenerationZeroRights)
	}
	return entry.Preparation == nil || entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil ||
		entry.GuardedLogicalResolution != nil
}

func mutationDigestValid(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
