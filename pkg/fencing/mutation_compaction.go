package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrMutationArchived requires a verified archived-result lookup. It is neither
// an admission success nor executable permission to replay the original work.
var ErrMutationArchived = errors.New("fencing: mutation archived; look up original result")

type MutationArchiveReference struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	Digest string `json:"digest"`
}

// MutationArchiveScan is a trusted backend's resumable verification position,
// bound to one exact archive head and fencing operation. A completed cursor is
// not authority to trust a new head or a new operation's history automatically.
type MutationArchiveScan struct {
	VerificationID string                    `json:"verificationID"`
	Head           MutationArchiveReference  `json:"head"`
	Next           *MutationArchiveReference `json:"next,omitempty"`
	Through        uint64                    `json:"through"`
	Links          uint64                    `json:"links"`
	Sought         *MutationArchiveReference `json:"sought,omitempty"`
	Found          bool                      `json:"found,omitempty"`
	Sequence       uint64                    `json:"sequence,omitempty"`
	ResultArchive  *MutationArchiveReference `json:"resultArchive,omitempty"`
}

func MutationArchiveName(digest string) string {
	return "rhiza-mutation-archive-" + digest[:min(len(digest), 40)]
}

func (r MutationArchiveReference) Valid() bool {
	return mutationDigestValid(r.Digest) && r.Name == MutationArchiveName(r.Digest) && r.UID != "" && len(r.UID) <= 128
}

// MutationRequestID makes producer UID + sequence the authoritative identity.
// A caller retains that tuple before its FIRST submission and never renumbers
// an uncertain request. Arbitrary operation-ID-only historical deduplication is
// not provided. Compaction accepts only entries using this structural identity.
func MutationRequestID(producerUID string, sequence uint64) string {
	data, _ := json.Marshal(struct {
		ProducerUID string
		Sequence    uint64
	}{producerUID, sequence})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// MutationCompactionPlan is the complete immutable archive payload. Producer CAS
// commits its reference; persisting this payload alone never advances coverage.
type MutationCompactionPlan struct {
	ProducerUID     string                    `json:"producerUID"`
	Runtime         RuntimeBinding            `json:"runtime"`
	WALIdentity     string                    `json:"walIdentity"`
	PreviousThrough uint64                    `json:"previousThrough"`
	Previous        *MutationArchiveReference `json:"previous,omitempty"`
	Through         uint64                    `json:"through"`
	Entries         map[uint64]MutationEntry  `json:"entries"`
}

func (plan MutationCompactionPlan) Digest() string {
	data, _ := json.Marshal(plan)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (plan MutationCompactionPlan) Validate() error {
	if plan.ProducerUID == "" || len(plan.ProducerUID) > 128 || plan.PreviousThrough >= plan.Through || plan.Through-plan.PreviousThrough > MaxMutationJournalEntries || plan.WALIdentity == "" {
		return fmt.Errorf("fencing: invalid archive range or producer identity")
	}
	p := MutationProducer{Runtime: plan.Runtime, WALIdentity: plan.WALIdentity, Phase: MutationOpen, LastAdmitted: plan.Through, CompactedThrough: plan.PreviousThrough, ArchiveHead: plan.Previous, Entries: plan.Entries}
	if err := p.Validate(); err != nil {
		return err
	}
	for sequence, entry := range plan.Entries {
		if !mutationEntryTerminal(entry) || entry.Intent.OperationID != MutationRequestID(plan.ProducerUID, sequence) {
			return fmt.Errorf("fencing: archive contains unresolved work or non-sequence-bound identity")
		}
	}
	return nil
}

// PlanMutationCompaction snapshots only an exact contiguous terminal prefix.
// A terminal suffix cannot conceal an earlier unresolved admission/permission.
func (p *MutationProducer) PlanMutationCompaction(producerUID string, through uint64) (MutationCompactionPlan, error) {
	if err := p.Validate(); err != nil {
		return MutationCompactionPlan{}, err
	}
	if p.EvidenceFault != "" {
		return MutationCompactionPlan{}, ErrUnavailable
	}
	if through <= p.CompactedThrough || through > p.LastAdmitted {
		return MutationCompactionPlan{}, fmt.Errorf("fencing: archive range is outside live journal")
	}
	plan := MutationCompactionPlan{ProducerUID: producerUID, Runtime: p.Runtime, WALIdentity: p.WALIdentity, PreviousThrough: p.CompactedThrough, Previous: p.ArchiveHead, Through: through, Entries: make(map[uint64]MutationEntry)}
	for sequence := p.CompactedThrough; sequence < through; {
		sequence++
		plan.Entries[sequence] = p.Entries[sequence]
	}
	if err := plan.Validate(); err != nil {
		return MutationCompactionPlan{}, err
	}
	// Snapshot nested dispatch/receipt maps too; later state changes cannot alter
	// an archive that was already prepared under its content-addressed name.
	data, _ := json.Marshal(plan)
	var copy MutationCompactionPlan
	if err := json.Unmarshal(data, &copy); err != nil {
		return MutationCompactionPlan{}, err
	}
	return copy, nil
}

// CommitMutationCompaction is a pure transition AFTER persist/readback verifies
// the immutable archive UID and digest. Tail-only CAS conflicts can reuse a plan
// if the exact predecessor and full prefix still match. Overlap requires replanning.
func (p *MutationProducer) CommitMutationCompaction(producerUID string, plan MutationCompactionPlan, reference MutationArchiveReference) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.EvidenceFault != "" {
		return ErrUnavailable
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	if producerUID != plan.ProducerUID || p.Runtime != plan.Runtime || p.WALIdentity != plan.WALIdentity || !reference.Valid() || reference.Digest != plan.Digest() {
		return fmt.Errorf("fencing: archive identity or persisted reference differs")
	}
	if p.CompactedThrough == plan.Through && p.ArchiveHead != nil && *p.ArchiveHead == reference {
		return nil
	}
	if p.CompactedThrough != plan.PreviousThrough || !sameMutationArchiveReference(p.ArchiveHead, plan.Previous) || plan.Through > p.LastAdmitted {
		return fmt.Errorf("fencing: stale compaction predecessor; replan exact remaining prefix")
	}
	for sequence, archived := range plan.Entries {
		entry, exists := p.Entries[sequence]
		actual, _ := json.Marshal(entry)
		wanted, _ := json.Marshal(archived)
		if !exists || string(actual) != string(wanted) || !mutationEntryTerminal(entry) {
			return fmt.Errorf("fencing: compaction prefix changed")
		}
	}
	for sequence := range plan.Entries {
		delete(p.Entries, sequence)
	}
	p.CompactedThrough = plan.Through
	copy := reference
	p.ArchiveHead = &copy
	p.ArchiveScan = nil
	return nil
}

func sameMutationArchiveReference(a, b *MutationArchiveReference) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
