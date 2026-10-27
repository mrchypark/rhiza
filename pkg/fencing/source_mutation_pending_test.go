package fencing

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIsMutationPreDispatchContentionRequiresLinearExactSubtype(t *testing.T) {
	if !IsMutationPreDispatchContention(fmt.Errorf("authority execute: %w", ErrMutationPreDispatchContention)) {
		t.Fatal("single wrapped exact subtype was not recognized")
	}
	if IsMutationPreDispatchContention(errors.Join(ErrMutationPreDispatchContention, errors.New("receipt cleanup failed"))) {
		t.Fatal("joined unrelated error was promoted to pre-dispatch contention")
	}
	if IsMutationPreDispatchContention(ErrPending) {
		t.Fatal("generic pending was promoted to pre-dispatch contention")
	}
}

type preDispatchContentionAuthority struct {
	sourceMutationAuthority
	sequence     uint64
	intent       MutationIntent
	admits       int
	lookups      int
	blockRetry   bool
	hideAdmitted bool
}

func (a *preDispatchContentionAuthority) Admit(ctx context.Context, sequence uint64, intent MutationIntent) error {
	a.admits++
	a.sequence, a.intent = sequence, intent
	if err := a.sourceMutationAuthority.Admit(ctx, sequence, intent); err != nil {
		return err
	}
	if a.admits == 1 {
		return ErrMutationPreDispatchContention
	}
	return nil
}

func (a *preDispatchContentionAuthority) Lookup(ctx context.Context, sequence uint64, intent MutationIntent) (MutationLookupResult, error) {
	a.lookups++
	if sequence != a.sequence && a.admits > 0 || (a.admits > 0 && intent != a.intent) {
		return MutationLookupResult{}, errors.New("retry changed the original sequence or intent")
	}
	if a.blockRetry && a.lookups > 1 {
		<-ctx.Done()
		return MutationLookupResult{}, ctx.Err()
	}
	if a.hideAdmitted && a.admits > 0 {
		return MutationLookupResult{}, nil
	}
	return a.sourceMutationAuthority.Lookup(ctx, sequence, intent)
}

func TestSourceMutationSettlementRetriesExactPreDispatchContention(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	authority := &preDispatchContentionAuthority{sourceMutationAuthority: fake}
	client.authority = authority
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	draft, payload := bootstrapDraft("run-contention", "registration"), []byte(`{"node":"n1"}`)
	if err := client.Upload(ctx, draft, payload); err != nil {
		t.Fatalf("settle exact contention: %v", err)
	}
	if authority.admits != 1 || authority.sequence != 1 || len(client.state.data.Steps) != 1 {
		t.Fatalf("allocated or admitted a replacement tuple: admits=%d seq=%d steps=%d", authority.admits, authority.sequence, len(client.state.data.Steps))
	}
	step := client.state.data.Steps[0]
	if step.Entry.Intent != authority.intent || !step.Admitted || !step.Advanced || step.Entry.Resolution == nil {
		t.Fatalf("original tuple was not durably settled: %+v", step)
	}
}

func TestSourceMutationSettlementWithoutDeadlineReturnsTypedContention(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	authority := &preDispatchContentionAuthority{sourceMutationAuthority: fake}
	client.authority = authority
	draft, payload := bootstrapDraft("run-unbounded", "registration"), []byte(`{"node":"n1"}`)
	err := client.Upload(context.Background(), draft, payload)
	if !errors.Is(err, ErrMutationPreDispatchContention) || !errors.Is(err, ErrPending) {
		t.Fatalf("unbounded call should return typed pending without waiting: %v", err)
	}
	if authority.admits != 1 || len(client.state.data.Steps) != 1 || !client.state.data.Steps[0].Admitted || client.state.data.Steps[0].Advanced {
		t.Fatal("contention did not preserve exactly one durably admitted pending tuple")
	}
}

func TestSourceMutationSettlementDoesNotReadmitTypedContentionWhenLookupMisses(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	authority := &preDispatchContentionAuthority{sourceMutationAuthority: fake, hideAdmitted: true}
	client.authority = authority
	draft, payload := bootstrapDraft("run-contention-missing-lookup", "registration"), []byte(`{"node":"n1"}`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Upload(ctx, draft, payload)
	if err != ErrPending {
		t.Fatalf("missing post-contention lookup should remain generic pending: %v", err)
	}
	if authority.admits != 1 || len(client.state.data.Steps) != 1 {
		t.Fatalf("missing lookup caused a second admission or tuple allocation: admits=%d steps=%d", authority.admits, len(client.state.data.Steps))
	}
	step := client.state.data.Steps[0]
	if !step.Admitted || step.Advanced || step.Entry.Resolution != nil {
		t.Fatal("missing lookup did not preserve the exact admitted tuple as pending")
	}
}

func TestSourceMutationSettlementCancellationPreservesExactPendingTuple(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	authority := &preDispatchContentionAuthority{sourceMutationAuthority: fake, blockRetry: true}
	client.authority = authority
	draft, payload := bootstrapDraft("run-cancel", "registration"), []byte(`{"node":"n1"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()
	err := client.Upload(ctx, draft, payload)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled settlement: %v", err)
	}
	if authority.admits != 1 || len(client.state.data.Steps) != 1 || !client.state.data.Steps[0].Admitted || client.state.data.Steps[0].Advanced {
		t.Fatal("cancellation changed, acknowledged, or discarded the original pending tuple")
	}
}

func TestSourceMutationSettlementDoesNotRetryJoinedContention(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	joined := errors.Join(ErrMutationPreDispatchContention, errors.New("unrelated failure"))
	authority := &joinedContentionAuthority{sourceMutationAuthority: fake, err: joined}
	client.authority = authority
	draft, payload := bootstrapDraft("run-joined", "registration"), []byte(`{"node":"n1"}`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Upload(ctx, draft, payload)
	if !errors.Is(err, ErrMutationPreDispatchContention) || authority.admits != 1 {
		t.Fatalf("joined unrelated error was not returned immediately: err=%v admits=%d", err, authority.admits)
	}
}

func TestSourceMutationSettlementDoesNotRetryGenericPending(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	authority := &joinedContentionAuthority{sourceMutationAuthority: fake, err: ErrPending}
	client.authority = authority
	draft, payload := bootstrapDraft("run-generic-pending", "registration"), []byte(`{"node":"n1"}`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Upload(ctx, draft, payload)
	if err != ErrPending || authority.admits != 1 {
		t.Fatalf("generic pending was not returned immediately: err=%v admits=%d", err, authority.admits)
	}
}

type joinedContentionAuthority struct {
	sourceMutationAuthority
	err    error
	admits int
}

func (a *joinedContentionAuthority) Admit(ctx context.Context, sequence uint64, intent MutationIntent) error {
	a.admits++
	if err := a.sourceMutationAuthority.Admit(ctx, sequence, intent); err != nil {
		return err
	}
	return a.err
}
