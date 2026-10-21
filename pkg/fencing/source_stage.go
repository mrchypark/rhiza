package fencing

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type sourceSealedPayload struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type sourceRetryStage struct {
	Ticket  string               `json:"ticket"`
	Draft   MutationIntent       `json:"draft"`
	Payload *sourceSealedPayload `json:"payload,omitempty"`
}

func uploadStageDraft(intent MutationIntent) MutationIntent {
	intent.OperationID = ""
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = "", "", 0
	return intent
}

func validateSourceStage(stage sourceRetryStage, scope string) error {
	if !mutationDigestValid(stage.Ticket) || stage.Draft.Kind != "upload" || stage.Draft != uploadStageDraft(stage.Draft) || stage.Draft.StoreScopeHash != scope {
		return fmt.Errorf("fencing: staging ticket original owner/target invalid")
	}
	intent := stage.Draft
	intent.OperationID = stage.Ticket
	if stage.Payload == nil {
		// Only validate the prospective caller fields; this placeholder is never
		// admitted, persisted as payload evidence or sent to the authority.
		intent.PayloadRef, intent.PayloadDigest = "receiving", stage.Ticket
	} else {
		intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = stage.Payload.Ref, stage.Payload.Digest, stage.Payload.Size
	}
	return validateMutationIntent(intent)
}

func (d sourceRetryData) validateStages() error {
	if len(d.Stages) > MaxMutationJournalEntries {
		return fmt.Errorf("fencing: source staging ticket capacity exceeded")
	}
	owners := make(map[MutationOwnerStep]bool, len(d.Stages))
	tickets := make(map[string]bool, len(d.Stages))
	for _, stage := range d.Stages {
		if err := validateSourceStage(stage, d.Binding.Producer.StoreScopeHash); err != nil {
			return err
		}
		if owners[stage.Draft.OwnerStep] || tickets[stage.Ticket] {
			return fmt.Errorf("fencing: duplicate source staging owner or ticket")
		}
		owners[stage.Draft.OwnerStep], tickets[stage.Ticket] = true, true
	}
	return nil
}

func (d sourceRetryData) hasSealedStage(intent MutationIntent) bool {
	for _, stage := range d.Stages {
		if stage.Draft == uploadStageDraft(intent) && stage.Payload != nil {
			return *stage.Payload == (sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize})
		}
	}
	return false
}

// reserveUpload fsyncs the original ticket and target before any reservation or
// payload transmission to the authority. It does not reserve a mutation sequence.
func (s *sourceRetryState) reserveUpload(draft MutationIntent) (sourceRetryStage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return sourceRetryStage{}, err
	}
	for _, stage := range s.data.Stages {
		if stage.Draft.OwnerStep == draft.OwnerStep {
			if stage.Draft != draft {
				return sourceRetryStage{}, fmt.Errorf("fencing: staging retry changed original target")
			}
			return cloneSourceStage(stage), nil
		}
	}
	if len(s.data.Stages) == MaxMutationJournalEntries {
		return sourceRetryStage{}, fmt.Errorf("fencing: source staging ticket capacity exhausted")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return sourceRetryStage{}, err
	}
	stage := sourceRetryStage{Ticket: hex.EncodeToString(nonce[:]), Draft: draft}
	if err := validateSourceStage(stage, s.data.Binding.Producer.StoreScopeHash); err != nil {
		return sourceRetryStage{}, err
	}
	next := s.data
	next.Stages = append(append([]sourceRetryStage(nil), next.Stages...), stage)
	if err := s.persist(next); err != nil {
		return sourceRetryStage{}, err
	}
	return stage, nil
}

// sealedUpload retains a descriptor obtained from the authenticated authority
// sealing response. It is not physical sealing or verification of that receipt.
func (s *sourceRetryState) sealedUpload(ticket string, payload sourceSealedPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	next := s.data
	next.Stages = append([]sourceRetryStage(nil), s.data.Stages...)
	for i := range next.Stages {
		stage := &next.Stages[i]
		if stage.Ticket != ticket {
			continue
		}
		if stage.Payload != nil {
			if *stage.Payload != payload {
				return fmt.Errorf("fencing: original sealed payload changed")
			}
			return nil
		}
		stage.Payload = &payload
		return s.persist(next)
	}
	return fmt.Errorf("fencing: original staging ticket missing")
}

func cloneSourceStage(stage sourceRetryStage) sourceRetryStage {
	if stage.Payload != nil {
		copy := *stage.Payload
		stage.Payload = &copy
	}
	return stage
}
