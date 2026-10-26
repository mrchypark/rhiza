package fencing

import (
	"fmt"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const PendingEngineRepairRequestVersion = 1

// PendingEngineRepairProposal is the exact physical request before a survivor
// has selected the canonical ConfigID and abort slot. It conveys no role or
// execution permission. OperationID is assigned by the authority from Digest.
type PendingEngineRepairProposal struct {
	Version           int      `json:"version"`
	SourceClusterID   string   `json:"sourceClusterID"`
	StatefulSetUID    string   `json:"statefulSetUID"`
	BootstrapIdentity Identity `json:"bootstrapIdentity"`
	StableOrdinal     int      `json:"stableOrdinal"`
	// OldMemberID is the physical launch configuration's node ID in a
	// preflight proposal. The authority replaces it with the canonical current
	// occupant of StableOrdinal before constructing the accepted request.
	OldMemberID string `json:"oldMemberID"`
	// SourceOldWALIdentity is filled by the authority from the canonical
	// registered source member, even when the recovering Pod has no local WAL.
	SourceOldWALIdentity string `json:"sourceOldWALIdentity"`
	// IncumbentReferenceDigest is authority-filled evidence binding the
	// canonical incumbent to its immutable genesis identity or completed
	// repair mapping. It is empty in caller preflight and included in the
	// accepted request digest.
	IncumbentReferenceDigest string                      `json:"incumbentReferenceDigest,omitempty"`
	OldWALIdentity           string                      `json:"oldWALIdentity,omitempty"`
	Outcome                  RecoveryBootstrapOutcome    `json:"outcome"`
	LocalIdentityHash        string                      `json:"localIdentityHash"`
	CandidateIdentity        Identity                    `json:"candidateIdentity"`
	CandidateMember          quepaxa.Member              `json:"candidateMember"`
	CandidateCredential      EngineRepairCredential      `json:"candidateCredential"`
	SourceAdminCredential    EngineRepairAdminCredential `json:"sourceAdminCredential"`
}

// PendingEngineRepairRequest is the authority-assigned, create-only form of a
// proposal. Its stable ID and digest bind the physical incarnation and slot;
// it is deliberately separate from Ledger.Runs and StartupGrant.
type PendingEngineRepairRequest struct {
	PendingEngineRepairProposal
	OperationID   string `json:"operationID"`
	PendingID     string `json:"pendingID"`
	RequestDigest string `json:"requestDigest"`
}

func (p PendingEngineRepairProposal) Digest() string {
	return hashJSONValue(p)
}

func (p PendingEngineRepairProposal) Validate() error {
	bootstrap := p.BootstrapIdentity
	candidate := p.CandidateIdentity
	validOutcome := p.Outcome == RecoveryBootstrapLostWAL && p.OldWALIdentity != "" ||
		p.Outcome == RecoveryBootstrapCandidateWAL && p.OldWALIdentity == ""
	if p.Version != PendingEngineRepairRequestVersion || p.SourceClusterID == "" || p.StatefulSetUID == "" ||
		p.StableOrdinal < 0 || p.StableOrdinal > 65535 || p.OldMemberID == "" || p.SourceOldWALIdentity == "" ||
		!mutationDigestValid(p.IncumbentReferenceDigest) || !validOutcome ||
		!mutationDigestValid(p.LocalIdentityHash) || bootstrap.SourceClusterID != p.SourceClusterID ||
		bootstrap.NodeID == "" || bootstrap.PodName == "" || bootstrap.PodUID == "" || bootstrap.RunID == "" ||
		bootstrap.WALIdentity != "" || bootstrap.AssignmentID != "" || candidate.SourceClusterID != p.SourceClusterID ||
		candidate.NodeID == "" || candidate.NodeID == p.OldMemberID || candidate.PodName != bootstrap.PodName ||
		candidate.PodUID != bootstrap.PodUID || candidate.RunID != bootstrap.RunID || candidate.WALIdentity == "" ||
		candidate.AssignmentID != "" ||
		p.CandidateMember.ID != quepaxa.NodeID(candidate.NodeID) || p.CandidateMember.WALIdentity != candidate.WALIdentity ||
		p.CandidateMember.PublicKey == (quepaxa.PublicKey{}) || !validRepairEndpoint(p.CandidateMember.URL, "http", "https") ||
		!validRepairEndpoint(p.CandidateMember.LogURL, "http", "https") || !validRepairEndpoint(p.CandidateMember.PeerURL, "quic") ||
		!p.CandidateCredential.valid() || !p.SourceAdminCredential.valid() {
		return fmt.Errorf("fencing: invalid pending engine repair proposal")
	}
	return nil
}

// ValidatePreflight checks only caller-supplied public continuity and
// candidate material. Credential references are authority-enriched after the
// authenticated workload context is established.
func (p PendingEngineRepairProposal) ValidatePreflight() error {
	b, c := p.BootstrapIdentity, p.CandidateIdentity
	if p.Version != PendingEngineRepairRequestVersion || p.SourceClusterID == "" ||
		p.StableOrdinal < 0 || p.StableOrdinal > 65535 || p.OldMemberID == "" || p.SourceOldWALIdentity != "" || p.IncumbentReferenceDigest != "" ||
		p.Outcome != RecoveryBootstrapLostWAL || p.OldWALIdentity == "" || !mutationDigestValid(p.LocalIdentityHash) ||
		b.SourceClusterID != p.SourceClusterID || b.NodeID != p.OldMemberID || b.PodName == "" || b.PodUID == "" || b.RunID == "" ||
		b.WALIdentity != "" || b.AssignmentID != "" || c.SourceClusterID != p.SourceClusterID || c.NodeID == "" || c.NodeID == p.OldMemberID ||
		c.PodName != b.PodName || c.PodUID != b.PodUID || c.RunID != b.RunID || c.WALIdentity == "" || c.AssignmentID != "" ||
		p.CandidateMember.ID != quepaxa.NodeID(c.NodeID) || p.CandidateMember.WALIdentity != c.WALIdentity ||
		p.CandidateMember.PublicKey == (quepaxa.PublicKey{}) || !validRepairEndpoint(p.CandidateMember.URL, "http", "https") ||
		!validRepairEndpoint(p.CandidateMember.LogURL, "http", "https") || !validRepairEndpoint(p.CandidateMember.PeerURL, "quic") {
		return fmt.Errorf("fencing: invalid engine repair preflight proposal")
	}
	return nil
}

func (r PendingEngineRepairRequest) Validate() error {
	proposal := r.PendingEngineRepairProposal
	assignmentID := proposal.CandidateIdentity.AssignmentID
	proposal.CandidateIdentity.AssignmentID = ""
	if err := proposal.Validate(); err != nil {
		return err
	}
	digest := proposal.Digest()
	if assignmentID != PendingEngineRepairAssignmentID(digest) || r.RequestDigest != digest || r.PendingID != PendingEngineRepairID(digest) || r.OperationID != PendingEngineRepairOperationID(digest) {
		return fmt.Errorf("fencing: pending engine repair identity differs from its canonical request")
	}
	return nil
}

func (r PendingEngineRepairRequest) SlotKey() string {
	return PendingEngineRepairSlotKey(r.SourceClusterID, r.StatefulSetUID, r.OldMemberID, r.StableOrdinal)
}

func (r PendingEngineRepairRequest) MatchesRecord(record EngineRepairRecord) bool {
	return r.Validate() == nil && record.BootstrapRequestDigest == r.RequestDigest &&
		record.Binding.OperationID == r.OperationID && record.Binding.SourceClusterID == r.SourceClusterID &&
		record.Binding.StatefulSetUID == r.StatefulSetUID && record.Binding.OldMemberID == r.OldMemberID &&
		record.Binding.OldWALIdentity == r.SourceOldWALIdentity &&
		record.CandidateIdentity == r.CandidateIdentity && record.CandidateMember == r.CandidateMember &&
		record.CandidateCredential == r.CandidateCredential && record.SourceAdminCredential == r.SourceAdminCredential
}

func PendingEngineRepairID(requestDigest string) string {
	return hashJSONValue([]any{"rhiza-pending-engine-repair-v1", requestDigest})
}

func PendingEngineRepairOperationID(requestDigest string) string {
	return "repair-" + PendingEngineRepairID(requestDigest)
}

func PendingEngineRepairAssignmentID(requestDigest string) string {
	return hashJSONValue([]any{"rhiza-engine-repair-candidate-assignment-v1", requestDigest})
}

func PendingEngineRepairSlotKey(sourceClusterID, statefulSetUID, oldMemberID string, ordinal int) string {
	return hashJSONValue([]any{"rhiza-pending-engine-repair-slot-v1", sourceClusterID, statefulSetUID, oldMemberID, ordinal})
}

func NewPendingEngineRepairRequest(proposal PendingEngineRepairProposal) (PendingEngineRepairRequest, error) {
	if err := proposal.Validate(); err != nil {
		return PendingEngineRepairRequest{}, err
	}
	digest := proposal.Digest()
	proposal.CandidateIdentity.AssignmentID = PendingEngineRepairAssignmentID(digest)
	request := PendingEngineRepairRequest{PendingEngineRepairProposal: proposal, OperationID: PendingEngineRepairOperationID(digest),
		PendingID: PendingEngineRepairID(digest), RequestDigest: digest}
	return request, request.Validate()
}
