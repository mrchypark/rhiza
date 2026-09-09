package quepaxa

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/mrchypark/rhiza/pkg/quepaxa/walfb"
)

func encodeRecorderEntry(slot Slot, state ISR, reconfiguration ...bool) []byte {
	builder := flatbuffers.NewBuilder(256)
	first := proposalToFlatBuffer(state.FirstCurrent).Pack(builder)
	current := first
	if !sameProposal(state.AggregateCurrent, state.FirstCurrent) {
		current = proposalToFlatBuffer(state.AggregateCurrent).Pack(builder)
	}
	prior := first
	if !sameProposal(state.AggregatePrior, state.FirstCurrent) {
		prior = current
		if !sameProposal(state.AggregatePrior, state.AggregateCurrent) {
			prior = proposalToFlatBuffer(state.AggregatePrior).Pack(builder)
		}
	}
	walfb.RecorderStateStart(builder)
	walfb.RecorderStateAddSlot(builder, uint64(slot))
	walfb.RecorderStateAddStep(builder, uint64(state.Step))
	walfb.RecorderStateAddFirstCurrent(builder, first)
	walfb.RecorderStateAddAggregateCurrent(builder, current)
	walfb.RecorderStateAddAggregatePrior(builder, prior)
	offset := walfb.RecorderStateEnd(builder)
	walfb.FinishRecorderStateBuffer(builder, offset)
	magic := isrEntryMagic
	if len(reconfiguration) != 0 && reconfiguration[0] {
		magic = isrEntryV2Magic
	}
	payload := make([]byte, len(magic)+len(builder.FinishedBytes()))
	copy(payload, magic)
	copy(payload[len(magic):], builder.FinishedBytes())
	return payload
}

func decodeRecorderEntry(payload []byte) (persisted recorderEntry, err error) {
	if bytes.Equal(payload, isrEntryV2Magic) {
		return recorderEntry{Reconfiguration: true}, nil
	}
	marker := false
	magic := isrEntryMagic
	if bytes.HasPrefix(payload, isrEntryV2Magic) {
		marker = true
		magic = isrEntryV2Magic
	} else if !bytes.HasPrefix(payload, magic) {
		return persisted, fmt.Errorf("unknown recorder WAL format")
	}

	data := payload[len(magic):]
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid FlatBuffers recorder WAL: %v", recovered)
		}
	}()
	if len(data) < 8 || !walfb.RecorderStateBufferHasIdentifier(data) {
		return persisted, fmt.Errorf("invalid FlatBuffers recorder WAL")
	}
	state := walfb.GetRootAsRecorderState(data, 0).UnPack()
	persisted.Slot = Slot(state.Slot)
	persisted.Reconfiguration = marker
	persisted.State.Step = Step(state.Step)
	if persisted.Slot == 0 {
		return recorderEntry{}, fmt.Errorf("invalid recorder slot")
	}
	if persisted.State.FirstCurrent, err = proposalFromFlatBuffer(state.FirstCurrent); err != nil {
		return recorderEntry{}, fmt.Errorf("first current: %w", err)
	}
	if persisted.State.AggregateCurrent, err = proposalFromFlatBuffer(state.AggregateCurrent); err != nil {
		return recorderEntry{}, fmt.Errorf("aggregate current: %w", err)
	}
	if persisted.State.AggregatePrior, err = proposalFromFlatBuffer(state.AggregatePrior); err != nil {
		return recorderEntry{}, fmt.Errorf("aggregate prior: %w", err)
	}
	return persisted, nil
}

func proposalToFlatBuffer(proposal *Proposal) *walfb.ProposalT {
	if proposal == nil {
		return nil
	}
	return &walfb.ProposalT{
		Priority:   proposal.Priority[:],
		ProposerId: string(proposal.ProposerID),
		Hash:       proposal.Hash[:],
		Value:      proposal.Value,
	}
}

func proposalFromFlatBuffer(proposal *walfb.ProposalT) (*Proposal, error) {
	if proposal == nil {
		return nil, nil
	}
	if len(proposal.Priority) != len(Priority{}) || len(proposal.Hash) != len(ValueHash{}) {
		return nil, fmt.Errorf("invalid proposal field length")
	}
	decoded := &Proposal{ProposerID: NodeID(proposal.ProposerId), Value: append([]byte(nil), proposal.Value...)}
	copy(decoded.Priority[:], proposal.Priority)
	copy(decoded.Hash[:], proposal.Hash)
	if len(decoded.Value) != 0 && sha256.Sum256(decoded.Value) != decoded.Hash {
		return nil, fmt.Errorf("proposal hash mismatch")
	}
	return decoded, nil
}
