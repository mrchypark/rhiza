package network

//go:generate flatc --go --gen-object-api -o . peer.fbs

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	peerWireVersion = 1
	maxPeerFrame    = 1 << 20
)

func encodePeerRequest(request *peerfb.RequestT) []byte {
	builder := flatbuffers.NewBuilder(512)
	request.Version = peerWireVersion
	offset := request.Pack(builder)
	peerfb.FinishRequestBuffer(builder, offset)
	return append([]byte(nil), builder.FinishedBytes()...)
}

func decodePeerRequest(data []byte) (request *peerfb.RequestT, err error) {
	if len(data) < 8 || !peerfb.RequestBufferHasIdentifier(data) {
		return nil, fmt.Errorf("invalid peer frame")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			request = nil
			err = fmt.Errorf("invalid peer frame: %v", recovered)
		}
	}()
	request = peerfb.GetRootAsRequest(data, 0).UnPack()
	if request.Version != peerWireVersion {
		return nil, fmt.Errorf("unsupported peer wire version %d", request.Version)
	}
	return request, nil
}

func encodePeerResponse(response *peerfb.ResponseT) []byte {
	builder := flatbuffers.NewBuilder(512)
	response.Version = peerWireVersion
	offset := response.Pack(builder)
	peerfb.FinishResponseBuffer(builder, offset)
	return append([]byte(nil), builder.FinishedBytes()...)
}

func decodePeerResponse(data []byte) (response *peerfb.ResponseT, err error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid peer response")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			response = nil
			err = fmt.Errorf("invalid peer response: %v", recovered)
		}
	}()
	response = peerfb.GetRootAsResponse(data, 0).UnPack()
	if response.Version != peerWireVersion {
		return nil, fmt.Errorf("unsupported peer wire version %d", response.Version)
	}
	if response.Error != "" {
		return response, fmt.Errorf("peer rejected request: %s", response.Error)
	}
	return response, nil
}

func proposalToWire(value quepaxa.Proposal) *peerfb.ProposalT {
	return &peerfb.ProposalT{
		Priority: append([]byte(nil), value.Priority[:]...), ProposerId: string(value.ProposerID),
		Hash: append([]byte(nil), value.Hash[:]...), Value: append([]byte(nil), value.Value...),
	}
}

func proposalFromWire(value *peerfb.ProposalT) (quepaxa.Proposal, error) {
	if value == nil || len(value.Priority) != 32 || len(value.Hash) != 32 || value.ProposerId == "" {
		return quepaxa.Proposal{}, fmt.Errorf("invalid proposal")
	}
	result := quepaxa.Proposal{ProposerID: quepaxa.NodeID(value.ProposerId), Value: append([]byte(nil), value.Value...)}
	copy(result.Priority[:], value.Priority)
	copy(result.Hash[:], value.Hash)
	return result, nil
}

func summaryToWire(value quepaxa.Summary) *peerfb.SummaryT {
	result := &peerfb.SummaryT{RecorderId: string(value.RecorderID), Step: uint64(value.Step)}
	if value.FirstCurrent != nil {
		result.FirstCurrent = proposalToWire(*value.FirstCurrent)
	}
	if value.AggregatePrior != nil {
		result.AggregatePrior = proposalToWire(*value.AggregatePrior)
	}
	return result
}

func summaryFromWire(value *peerfb.SummaryT) (quepaxa.Summary, error) {
	if value == nil || value.RecorderId == "" {
		return quepaxa.Summary{}, fmt.Errorf("invalid summary")
	}
	result := quepaxa.Summary{RecorderID: quepaxa.NodeID(value.RecorderId), Step: quepaxa.Step(value.Step)}
	if value.FirstCurrent != nil {
		proposal, err := proposalFromWire(value.FirstCurrent)
		if err != nil {
			return quepaxa.Summary{}, err
		}
		result.FirstCurrent = &proposal
	}
	if value.AggregatePrior != nil {
		proposal, err := proposalFromWire(value.AggregatePrior)
		if err != nil {
			return quepaxa.Summary{}, err
		}
		result.AggregatePrior = &proposal
	}
	return result, nil
}

func decisionToWire(value quepaxa.Decision) *peerfb.DecisionT {
	summaries := make([]*peerfb.SummaryT, len(value.Summaries))
	for i := range value.Summaries {
		summaries[i] = summaryToWire(value.Summaries[i])
	}
	return &peerfb.DecisionT{Slot: uint64(value.Slot), Step: uint64(value.Step), Proposal: proposalToWire(value.Proposal), Summaries: summaries}
}

func decisionFromWire(value *peerfb.DecisionT) (quepaxa.Decision, error) {
	if value == nil || value.Slot == 0 {
		return quepaxa.Decision{}, fmt.Errorf("invalid decision")
	}
	proposal, err := proposalFromWire(value.Proposal)
	if err != nil {
		return quepaxa.Decision{}, err
	}
	result := quepaxa.Decision{Slot: quepaxa.Slot(value.Slot), Step: quepaxa.Step(value.Step), Proposal: proposal, Summaries: make([]quepaxa.Summary, len(value.Summaries))}
	for i := range value.Summaries {
		result.Summaries[i], err = summaryFromWire(value.Summaries[i])
		if err != nil {
			return quepaxa.Decision{}, err
		}
	}
	return result, nil
}

func decidedToWire(value quepaxa.DecidedValue) *peerfb.DecidedValueT {
	return &peerfb.DecidedValueT{Slot: uint64(value.Slot), Hash: append([]byte(nil), value.Hash[:]...), Value: append([]byte(nil), value.Value...), Certificate: append([]byte(nil), value.Certificate...)}
}

func decidedFromWire(value *peerfb.DecidedValueT) (quepaxa.DecidedValue, error) {
	if value == nil || value.Slot == 0 || len(value.Hash) != 32 {
		return quepaxa.DecidedValue{}, fmt.Errorf("invalid decided value")
	}
	result := quepaxa.DecidedValue{Slot: quepaxa.Slot(value.Slot), Value: append([]byte(nil), value.Value...), Certificate: append([]byte(nil), value.Certificate...)}
	copy(result.Hash[:], value.Hash)
	return result, nil
}
