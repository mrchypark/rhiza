package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
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

// MutationResolution must be supplied only by the trusted provider worker after
// recording its immutable provider evidence. Exclusion is never a client ACK.
type MutationResolution struct {
	Outcome         MutationOutcome `json:"outcome"`
	PreparationHash string          `json:"preparationHash"`
	ReceiptDigest   string          `json:"receiptDigest"`
	PayloadDigest   string          `json:"payloadDigest,omitempty"`
	// AllCopiesTerminal is a trusted provider-worker assertion backed by the
	// receipt. In particular a successful create-if-absent response alone does
	// not establish this: a delayed copy can succeed after a later deletion.
	// Sealed dispatch admission and every granted permission's terminal receipt
	// are required; counting requests already observed on the wire is insufficient.
	AllCopiesTerminal bool `json:"allCopiesTerminal,omitempty"`
}

type MutationEntry struct {
	Intent         MutationIntent              `json:"intent"`
	Preparation    *MutationPreparation        `json:"preparation,omitempty"`
	Resolution     *MutationResolution         `json:"resolution,omitempty"`
	CopyCompletion *MutationCopyCompletion     `json:"copyCompletion,omitempty"`
	DispatchClosed bool                        `json:"dispatchClosed,omitempty"`
	Dispatches     map[string]MutationDispatch `json:"dispatches,omitempty"`
}

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
	PreparationHash string `json:"preparationHash"`
	ReceiptDigest   string `json:"receiptDigest"`
}

// MutationProducer is a pure CAS transition value. The service must persist
// each transition before dispatch or ACK; this type does not provide persistence.
// Missing state is Reserved/unknown, never an implicitly open producer.
type MutationProducer struct {
	Runtime          RuntimeBinding            `json:"runtime"`
	WALIdentity      string                    `json:"walIdentity,omitempty"`
	Phase            MutationPhase             `json:"phase"`
	LastAdmitted     uint64                    `json:"lastAdmitted"`
	ClosedThrough    uint64                    `json:"closedThrough,omitempty"`
	CloseOperationID string                    `json:"closeOperationID,omitempty"`
	CloseRequestHash string                    `json:"closeRequestHash,omitempty"`
	CompactedThrough uint64                    `json:"compactedThrough,omitempty"`
	ArchiveHead      *MutationArchiveReference `json:"archiveHead,omitempty"`
	ArchiveScan      *MutationArchiveScan      `json:"archiveScan,omitempty"`
	EvidenceFault    string                    `json:"evidenceFault,omitempty"`
	Entries          map[uint64]MutationEntry  `json:"entries"`
}

// Validate rejects incomplete or damaged state after loading it. This is not
// authentication of provider receipts: the persistence/executor boundary must
// authenticate those separately. Only the bounded live interval is validated;
// archive contents and chain coverage require the storage-backed verifier.
func (p *MutationProducer) Validate() error {
	if p == nil {
		return fmt.Errorf("fencing: missing mutation producer")
	}
	id := Identity{SourceClusterID: p.Runtime.SourceClusterID, NodeID: p.Runtime.NodeID, PodUID: p.Runtime.PodUID, RunID: p.Runtime.RunID}
	if err := p.Runtime.Validate(id); err != nil {
		return err
	}
	if p.CompactedThrough > p.LastAdmitted || p.Entries == nil || p.LastAdmitted-p.CompactedThrough > MaxMutationJournalEntries || uint64(len(p.Entries)) != p.LastAdmitted-p.CompactedThrough {
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
		if p.WALIdentity != "" || p.LastAdmitted != 0 {
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
		if !exists || validateMutationIntent(entry.Intent) != nil || seen[entry.Intent.OperationID] {
			return fmt.Errorf("fencing: mutation journal sequence or identity is invalid")
		}
		seen[entry.Intent.OperationID] = true
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
		if entry.Resolution != nil {
			if err := p.validateResolution(sequence, entry, *entry.Resolution); err != nil {
				return err
			}
		}
		if entry.CopyCompletion != nil {
			if entry.Preparation == nil || entry.Resolution == nil || entry.CopyCompletion.PreparationHash != p.preparationHash(sequence, entry) || !mutationDigestValid(entry.CopyCompletion.ReceiptDigest) || !allDispatchesTerminal(entry) {
				return fmt.Errorf("fencing: copy completion lacks matching prepared terminal work")
			}
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
	if err := p.Runtime.Validate(id); err != nil {
		return err
	}
	if id.WALIdentity == "" || (p.WALIdentity != "" && p.WALIdentity != id.WALIdentity) {
		return fmt.Errorf("fencing: mutation producer WAL differs")
	}
	p.WALIdentity, p.Phase = id.WALIdentity, MutationOpen
	return nil
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
	if intent.ContractVersion != MutationIntentVersion || !mutationDigestValid(intent.StoreScopeHash) || intent.OperationID == "" || len(intent.OperationID) > 256 || intent.Key == "" || len(intent.Key) > 1024 || (intent.Kind != "upload" && intent.Kind != "delete") {
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
			return nil
		}
		return ErrClosed
	}
	p.Phase, p.ClosedThrough = MutationClosed, p.LastAdmitted
	p.CloseOperationID, p.CloseRequestHash = operationID, requestHash
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
	if prepared.Profile != "gcs-json-v1" && prepared.Profile != "gcs-resumable-v1" {
		return fmt.Errorf("fencing: unsupported mutation profile")
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

func (p *MutationProducer) preparationHash(sequence uint64, entry MutationEntry) string {
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
	if resolution.AllCopiesTerminal && !allDispatchesTerminal(entry) {
		return fmt.Errorf("fencing: executable dispatch permissions remain unsealed or unresolved")
	}
	if entry.Preparation == nil {
		if resolution.Outcome != MutationRejected || resolution.PreparationHash != "" || resolution.PayloadDigest != "" || !resolution.AllCopiesTerminal {
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

// ConfirmCopiesTerminal is called only after the trusted executor verifies all
// copies/sessions AND every outstanding executable permission are terminal,
// with dispatch admission sealed. A timeout, a single response, or absence
// from a recent object listing cannot produce this evidence.
func (p *MutationProducer) ConfirmCopiesTerminal(sequence uint64, completion MutationCopyCompletion) error {
	if err := p.Validate(); err != nil {
		return err
	}
	entry, exists := p.Entries[sequence]
	if !exists || entry.Preparation == nil || entry.Resolution == nil || completion.PreparationHash != p.preparationHash(sequence, entry) || !mutationDigestValid(completion.ReceiptDigest) || !allDispatchesTerminal(entry) {
		return fmt.Errorf("fencing: copy completion lacks matching prepared terminal work")
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
	return entry.Preparation == nil || entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil
}

func mutationDigestValid(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
