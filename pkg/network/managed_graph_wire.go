package network

import (
	"encoding/json"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ValidateManagedGraphWireBudget is a local-only startup check, not profile
// selection or permission to propose. Call after the exact transport identity
// and learner credential are installed, for every initial/derived/joint config.
// It uses the actual encoders and counts expanded aliases, not only wire bytes.
func (t *Transport) ValidateManagedGraphWireBudget() error {
	// Learned: root + decision + principal proposal + N vector slots,
	// N summaries and 2N proposal refs => at least 4N+3 materialized objects.
	if len(t.members) == 0 || len(t.members) > (peerObjectBudget-3)/4 {
		return fmt.Errorf("Graph wire membership exceeds object budget")
	}
	metadata := 0
	for _, n := range []int{len(t.clusterID), len(t.localID), len(t.adminToken)} {
		if n > maxPeerFrame-metadata {
			return fmt.Errorf("Graph wire identity metadata exceeds budget")
		}
		metadata += n
	}
	longest := t.localID
	certificateID := t.localID
	encodedID, _ := json.Marshal(string(certificateID))
	for id := range t.members {
		if len(id) > maxPeerFrame-metadata {
			return fmt.Errorf("Graph wire identity metadata exceeds budget")
		}
		metadata += len(id)
		if len(id) > len(longest) {
			longest = id
		}
		encoded, _ := json.Marshal(string(id))
		if len(encoded) > len(encodedID) {
			encodedID = encoded
			certificateID = id
		}
	}
	if metadata > maxPeerFrame {
		return fmt.Errorf("Graph wire identity metadata exceeds budget")
	}
	refs := 1 + 2*len(t.members)
	if len(longest) > 0 && refs > (maxPeerFrame-types.MaxManagedGraphExecutionBytes)/len(longest) {
		return fmt.Errorf("Graph repeated wire identities exceed budget")
	}
	if len(encodedID) > 0 && refs > (maxPeerFrame-types.MaxManagedGraphExecutionBytes)/len(encodedID) {
		return fmt.Errorf("Graph repeated certificate identities exceed budget")
	}
	value := make([]byte, types.MaxManagedGraphExecutionBytes)
	proposal := quepaxa.Proposal{ProposerID: longest, Value: value}
	for i := range proposal.Priority {
		proposal.Priority[i] = 255
		proposal.Hash[i] = 255
	}
	var summaries []quepaxa.Summary
	maxSlot, maxStep := ^uint64(0), ^uint64(0)
	for id := range t.members {
		p := proposal
		summaries = append(summaries, quepaxa.Summary{RecorderID: id, Step: quepaxa.Step(maxStep), FirstCurrent: &p, AggregatePrior: &p, ReconfigurationID: proposal.Hash})
	}
	decision := quepaxa.Decision{Slot: quepaxa.Slot(maxSlot), Step: quepaxa.Step(maxStep), ConfigID: t.configID, Proposal: proposal, Summaries: summaries}
	certificateDecision := decision
	certificateDecision.Proposal.ProposerID = certificateID
	certificateDecision.Summaries = make([]quepaxa.Summary, len(summaries))
	for i, s := range summaries {
		p := proposal
		p.ProposerID = certificateID
		s.FirstCurrent = &p
		s.AggregatePrior = &p
		certificateDecision.Summaries[i] = s
	}
	certificate, err := quepaxa.EncodeCertificateForBudget(t.configID, certificateDecision)
	if err != nil {
		return fmt.Errorf("Graph certificate encoding failed")
	}
	request := t.request(peerfb.OperationRecord)
	request.Record = &peerfb.RecordRequestT{Slot: maxSlot, Step: maxStep, Proposal: proposalToWire(proposal)}
	request.Hash = proposal.Hash[:]
	if err := graphCheckRequestBudget(request); err != nil {
		return fmt.Errorf("Graph Record request exceeds wire budget")
	}
	if err := graphCheckResponseBudget(&peerfb.ResponseT{Summary: summaryToWire(quepaxa.Summary{RecorderID: longest, Step: quepaxa.Step(maxStep), FirstCurrent: &proposal, AggregatePrior: &proposal, ReconfigurationID: proposal.Hash})}); err != nil {
		return fmt.Errorf("Graph decided Record summary exceeds wire budget")
	}
	request = t.request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	if err := graphCheckRequestBudget(request); err != nil {
		return fmt.Errorf("Graph Learned exceeds wire budget")
	}
	request = t.request(peerfb.OperationStageValue)
	request.Value = value
	request.Hash = proposal.Hash[:]
	if err := graphCheckRequestBudget(request); err != nil {
		return fmt.Errorf("Graph StageValue exceeds wire budget")
	}
	if err := graphCheckResponseBudget(&peerfb.ResponseT{Value: value}); err != nil {
		return fmt.Errorf("Graph FetchValue exceeds wire budget")
	}
	if err := graphCheckResponseBudget(&peerfb.ResponseT{ClusterId: string(t.clusterID), ProposerId: string(longest), ConfigId: uint64(t.configID), Tip: 1, Decisions: []*peerfb.DecidedValueT{{Slot: 1, Hash: proposal.Hash[:], Value: value, Certificate: certificate}}}); err != nil {
		return fmt.Errorf("Graph singleton Sync exceeds wire budget")
	}
	return nil
}

func graphCheckRequestBudget(request *peerfb.RequestT) error {
	data := encodePeerRequest(request)
	if len(data) > maxPeerFrame {
		return fmt.Errorf("frame too large")
	}
	return peerRequestBounds(data)
}
func graphCheckResponseBudget(response *peerfb.ResponseT) error {
	data := encodePeerResponse(response)
	if len(data) > maxPeerFrame {
		return fmt.Errorf("frame too large")
	}
	return peerResponseBounds(data)
}
