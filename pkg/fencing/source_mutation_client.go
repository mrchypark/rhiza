package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrMutationExcluded = errors.New("fencing: original mutation was excluded")
	ErrMutationRejected = errors.New("fencing: original mutation was rejected")
)

// sourceMutationAuthority is deliberately narrow and internal. Implementations
// must authenticate responses and make each context/deadline actually interrupt
// its transport; local validation below is not proof authentication.
type sourceMutationAuthority interface {
	ReserveUpload(context.Context, string, MutationIntent) error
	SealUpload(context.Context, string, []byte) (sourceSealedPayload, error)
	Admit(context.Context, uint64, MutationIntent) error
	Lookup(context.Context, uint64, MutationIntent) (entry MutationEntry, found bool, archived bool, err error)
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
	if err := validateSourceStage(sourceRetryStage{Ticket: strings.Repeat("0", 64), Draft: draft}, c.state.data.Binding.Producer.StoreScopeHash); err != nil {
		return err
	}
	stage, err := c.state.reserveUpload(draft)
	if err != nil {
		return err
	}
	if err := c.authority.ReserveUpload(ctx, stage.Ticket, stage.Draft); err != nil {
		return err
	}
	if stage.Payload == nil {
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
		digest := sha256.Sum256(payload)
		if stage.Payload.Size != int64(len(payload)) || stage.Payload.Digest != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("fencing: retry changed original upload bytes")
		}
	}
	intent := draft
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = stage.Payload.Ref, stage.Payload.Digest, stage.Payload.Size
	step, err := c.state.allocate(intent)
	if err != nil {
		return err
	}
	return c.settle(ctx, step)
}

func (c *sourceMutationClient) settle(ctx context.Context, step sourceRetryStep) error {
	intent, sequence := step.Entry.Intent, step.Sequence
	entry, found, archived, err := c.authority.Lookup(ctx, sequence, intent)
	if err != nil {
		return err
	}
	if found {
		if entry.Intent != intent {
			return fmt.Errorf("fencing: authority lookup changed original intent")
		}
		if err := c.state.admitted(sequence, intent); err != nil {
			return err
		}
	} else {
		if archived {
			return fmt.Errorf("fencing: archived mutation lookup omitted its result")
		}
		if err := c.authority.Admit(ctx, sequence, intent); err != nil {
			return err
		}
		if err := c.state.admitted(sequence, intent); err != nil {
			return err
		}
		entry, found, archived, err = c.authority.Lookup(ctx, sequence, intent)
		if err != nil {
			return err
		}
		if !found {
			return ErrPending
		}
	}
	_ = archived // Found archived entries are lookup-only and never re-admitted.
	if entry.Intent != intent {
		return fmt.Errorf("fencing: authority result changed original intent")
	}
	if entry.Resolution == nil {
		return ErrPending
	}
	if err := c.state.resolved(sequence, entry); err != nil {
		return err
	}
	if err := c.state.advance(sequence, intent); err != nil {
		return err
	}
	switch entry.Resolution.Outcome {
	case MutationCommitted:
		return nil
	case MutationExcluded:
		return ErrMutationExcluded
	case MutationRejected:
		return ErrMutationRejected
	default:
		return fmt.Errorf("fencing: authority returned unknown mutation outcome")
	}
}
