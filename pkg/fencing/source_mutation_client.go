package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

var (
	errMutationExcluded = errors.New("fencing: original mutation was excluded")
	errMutationRejected = errors.New("fencing: original mutation was rejected")
)

// sourceMutationAuthority is deliberately narrow and internal. Implementations
// must authenticate responses and make each context/deadline actually interrupt
// its transport; local validation below is not proof authentication.
type sourceMutationAuthority interface {
	ReserveUpload(context.Context, string, MutationIntent) error
	SealUpload(context.Context, string, []byte) (sourceSealedPayload, error)
	Admit(context.Context, uint64, MutationIntent) error
	Lookup(context.Context, uint64, MutationIntent) (MutationLookupResult, error)
}

// sourceMutationClient owns only the source-side orchestration. Its retry state
// must already be opened from the original durable producer directory; this
// client never creates recovery state or infers sequence numbers from authority.
type sourceMutationClient struct {
	state     *sourceRetryState
	authority sourceMutationAuthority
}

// Upload implements the first source vertical path for bounded bootstrap JSON
// payloads. Each caller supplies a stable owner step; intentional registrations
// must use distinct steps. Provider mutation and ACK proof remain out of scope.
func (c *sourceMutationClient) Upload(ctx context.Context, draft MutationIntent, payload []byte) error {
	if c == nil || c.state == nil || c.authority == nil || ctx == nil {
		return fmt.Errorf("fencing: source mutation client is not configured")
	}
	if int64(len(payload)) > maxStagePayloadBytes {
		return fmt.Errorf("fencing: bootstrap upload exceeds staging payload limit")
	}
	draft = uploadStageDraft(draft)
	stage, step, hasStep, err := c.state.owner(draft)
	if err != nil {
		return err
	}
	if !hasStep && c.state.atCapacity() {
		if err := c.refreshArchivedOwners(ctx); err != nil {
			return err
		}
		stage, step, hasStep, err = c.state.owner(draft)
		if err != nil {
			return err
		}
	}
	if hasStep {
		if !sameSourcePayload(step.Entry.Intent, payload) {
			return fmt.Errorf("fencing: retry changed original upload bytes")
		}
		if step.Entry.Resolution != nil {
			return c.finish(step)
		}
		return c.settle(ctx, step)
	}
	stage, err = c.state.reserveUploadWithPayload(draft, payload)
	if err != nil {
		return err
	}
	if stage.Payload == nil {
		if err := c.state.markStageRemoteStarted(stage.Ticket); err != nil {
			return err
		}
		if err := c.authority.ReserveUpload(ctx, stage.Ticket, stage.Draft); err != nil {
			return err
		}
		sealed, err := c.authority.SealUpload(ctx, stage.Ticket, append([]byte(nil), payload...))
		if err != nil {
			return err
		}
		digest := sha256.Sum256(payload)
		if sealed.Size != int64(len(payload)) || sealed.Digest != hex.EncodeToString(digest[:]) || sealed.Ref == "" {
			return fmt.Errorf("fencing: authority sealed payload differs from original bytes")
		}
		if err := c.state.sealedUpload(stage.Ticket, sealed); err != nil {
			return err
		}
		stage.Payload = &sealed
	} else {
		if !sameSourcePayload(MutationIntent{PayloadDigest: stage.Payload.Digest, PayloadSize: stage.Payload.Size}, payload) {
			return fmt.Errorf("fencing: retry changed original upload bytes")
		}
	}
	intent := draft
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = stage.Payload.Ref, stage.Payload.Digest, stage.Payload.Size
	step, err = c.state.allocate(intent)
	if err != nil {
		return err
	}
	return c.settle(ctx, step)
}

// refreshArchivedOwners is used only at the bounded local capacity edge. It
// resolves retained exact tuples and retires only the authority-authenticated
// contiguous archive prefix; it never infers a sequence from LastAdmitted.
func (c *sourceMutationClient) refreshArchivedOwners(ctx context.Context) error {
	for {
		step, ok := c.state.firstStep()
		if !ok {
			return nil
		}
		before := c.state.retiredThrough()
		err := c.settle(ctx, step)
		if err != nil && !errors.Is(err, errMutationExcluded) && !errors.Is(err, errMutationRejected) {
			return err
		}
		if c.state.retiredThrough() == before {
			return fmt.Errorf("fencing: source retry capacity remains full until the exact terminal prefix is archived")
		}
	}
}

func sameSourcePayload(intent MutationIntent, payload []byte) bool {
	digest := sha256.Sum256(payload)
	return intent.PayloadSize == int64(len(payload)) && intent.PayloadDigest == hex.EncodeToString(digest[:])
}

func (c *sourceMutationClient) settle(ctx context.Context, step sourceRetryStep) error {
	intent, sequence := step.Entry.Intent, step.Sequence
	result, err := c.authority.Lookup(ctx, sequence, intent)
	if err != nil {
		return err
	}
	if result.Found {
		if result.Entry == nil || result.Entry.Intent != intent {
			return fmt.Errorf("fencing: authority lookup changed original intent")
		}
		if err := c.state.admitted(sequence, intent); err != nil {
			return err
		}
	} else {
		if result.Archived {
			return fmt.Errorf("fencing: archived mutation lookup omitted its result")
		}
		if step.Admitted {
			return ErrPending
		}
		if err := c.authority.Admit(ctx, sequence, intent); err != nil {
			if errors.Is(err, ErrMutationArchived) {
				if persistErr := c.state.admitted(sequence, intent); persistErr != nil {
					return persistErr
				}
			}
			return err
		}
		if err := c.state.admitted(sequence, intent); err != nil {
			return err
		}
		result, err = c.authority.Lookup(ctx, sequence, intent)
		if err != nil {
			return err
		}
		if !result.Found || result.Entry == nil {
			return ErrPending
		}
	}
	entry := *result.Entry
	if entry.Intent != intent {
		return fmt.Errorf("fencing: authority result changed original intent")
	}
	if entry.Resolution == nil {
		return ErrPending
	}
	if err := c.state.resolved(sequence, entry); err != nil {
		return err
	}
	finishErr := c.finish(sourceRetryStep{Sequence: sequence, Entry: entry})
	if result.CompactedThrough > 0 {
		if err := c.state.retireArchived(result.CompactedThrough, result.ArchiveHead); err != nil {
			return err
		}
	}
	return finishErr
}

func (c *sourceMutationClient) finish(step sourceRetryStep) error {
	if !step.Advanced {
		if err := c.state.advance(step.Sequence, step.Entry.Intent); err != nil {
			return err
		}
	}
	switch step.Entry.Resolution.Outcome {
	case MutationCommitted:
		return nil
	case MutationExcluded:
		return errMutationExcluded
	case MutationRejected:
		return errMutationRejected
	default:
		return fmt.Errorf("fencing: authority returned unknown mutation outcome")
	}
}

// owner snapshots existing stage/step state under its mutex so callers never
// inspect mutable retry data outside the durable-state lock.
func (s *sourceRetryState) owner(draft MutationIntent) (sourceRetryStage, sourceRetryStep, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return sourceRetryStage{}, sourceRetryStep{}, false, err
	}
	for _, owner := range s.data.RetiredOwners {
		if owner == draft.OwnerStep {
			return sourceRetryStage{}, sourceRetryStep{}, false, fmt.Errorf("fencing: source owner was already retired; owner identities are never reusable")
		}
	}
	var stage sourceRetryStage
	for _, saved := range s.data.Stages {
		if saved.Draft.OwnerStep != draft.OwnerStep {
			continue
		}
		if saved.Draft != draft {
			return sourceRetryStage{}, sourceRetryStep{}, false, fmt.Errorf("fencing: staging retry changed original target")
		}
		stage = cloneSourceStage(saved)
		break
	}
	for _, saved := range s.data.Steps {
		if saved.Entry.Intent.OwnerStep != draft.OwnerStep {
			continue
		}
		if uploadStageDraft(saved.Entry.Intent) != draft {
			return sourceRetryStage{}, sourceRetryStep{}, false, fmt.Errorf("fencing: retry changed original owner intent")
		}
		if stage.Ticket == "" {
			return sourceRetryStage{}, sourceRetryStep{}, false, fmt.Errorf("fencing: original source staging ticket missing")
		}
		return stage, cloneSourceRetryStep(saved), true, nil
	}
	return stage, sourceRetryStep{}, false, nil
}
