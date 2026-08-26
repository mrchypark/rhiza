package quepaxa

import (
	"encoding/json"
	"fmt"
)

const certificateVersion = 2

type proposalRef struct {
	Priority   Priority  `json:"priority"`
	ProposerID NodeID    `json:"proposer_id"`
	Hash       ValueHash `json:"hash"`
}

type summaryRef struct {
	RecorderID     NodeID       `json:"recorder_id"`
	Step           Step         `json:"step"`
	FirstCurrent   *proposalRef `json:"first_current,omitempty"`
	AggregatePrior *proposalRef `json:"aggregate_prior,omitempty"`
}

type certificateV2 struct {
	Version   int          `json:"version"`
	ConfigID  uint         `json:"config_id"`
	Slot      Slot         `json:"slot"`
	Step      Step         `json:"step"`
	Proposal  proposalRef  `json:"proposal"`
	Summaries []summaryRef `json:"summaries"`
}

type decisionRecordV2 struct {
	Value       []byte          `json:"value"`
	Certificate json.RawMessage `json:"certificate"`
}

func ref(proposal *Proposal) *proposalRef {
	if proposal == nil {
		return nil
	}
	return &proposalRef{Priority: proposal.Priority, ProposerID: proposal.ProposerID, Hash: proposal.Hash}
}

func proposalFromRef(value *proposalRef) *Proposal {
	if value == nil {
		return nil
	}
	return &Proposal{Priority: value.Priority, ProposerID: value.ProposerID, Hash: value.Hash}
}

func encodeCertificate(configID uint, decision Decision) ([]byte, error) {
	certificate := certificateV2{
		Version: certificateVersion, ConfigID: configID, Slot: decision.Slot, Step: decision.Step,
		Proposal: *ref(&decision.Proposal), Summaries: make([]summaryRef, len(decision.Summaries)),
	}
	for i, summary := range decision.Summaries {
		certificate.Summaries[i] = summaryRef{
			RecorderID: summary.RecorderID, Step: summary.Step,
			FirstCurrent: ref(summary.FirstCurrent), AggregatePrior: ref(summary.AggregatePrior),
		}
	}
	return json.Marshal(certificate)
}

func decodeCertificate(data []byte) (uint, Decision, error) {
	if len(data) == 0 {
		return 0, Decision{}, fmt.Errorf("decision has no QuePaxa certificate")
	}
	var certificate certificateV2
	if err := json.Unmarshal(data, &certificate); err != nil {
		return 0, Decision{}, fmt.Errorf("decode QuePaxa certificate: %w", err)
	}
	if certificate.Version != certificateVersion {
		return 0, Decision{}, fmt.Errorf("unsupported QuePaxa certificate version %d", certificate.Version)
	}
	decision := Decision{
		Slot: certificate.Slot, Step: certificate.Step, Proposal: *proposalFromRef(&certificate.Proposal),
		Summaries: make([]Summary, len(certificate.Summaries)),
	}
	for i, summary := range certificate.Summaries {
		decision.Summaries[i] = Summary{
			RecorderID: summary.RecorderID, Step: summary.Step,
			FirstCurrent: proposalFromRef(summary.FirstCurrent), AggregatePrior: proposalFromRef(summary.AggregatePrior),
		}
	}
	return certificate.ConfigID, decision, nil
}

func encodeDecisionRecord(value []byte, certificate []byte) ([]byte, error) {
	return json.Marshal(decisionRecordV2{Value: value, Certificate: certificate})
}

func decodeDecisionRecord(data []byte) ([]byte, []byte, error) {
	var record decisionRecordV2
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, nil, fmt.Errorf("decode decision WAL record: %w", err)
	}
	if len(record.Certificate) == 0 {
		return nil, nil, fmt.Errorf("decision WAL record has no certificate")
	}
	return record.Value, record.Certificate, nil
}
