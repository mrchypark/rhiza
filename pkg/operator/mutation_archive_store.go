package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

var errMutationArchiveEvidence = errors.New("fencing: retained mutation archive is missing or differs")

func (e *FencingExecutor) decodeMutationArchive(data []byte, version *objstore.ObjectVersion, reference fencing.MutationArchiveReference, runtime fencing.RuntimeBinding, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	return e.decodeMutationArchiveForAuthority(data, version, reference, runtimeMutationAuthority(runtime), producerUID, wal)
}

func (e *FencingExecutor) decodeMutationArchiveForAuthority(data []byte, version *objstore.ObjectVersion,
	reference fencing.MutationArchiveReference, authority mutationExecutionAuthority, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	var plan fencing.MutationCompactionPlan
	if !reference.Valid() || version == nil || version.Type != objstore.Generation || version.Value != reference.UID || len(data) > 700000 {
		return plan, errMutationArchiveEvidence
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&plan) != nil || d.Decode(new(any)) != io.EOF || plan.Validate() != nil || plan.Digest() != reference.Digest ||
		!mutationArchiveMatchesAuthority(plan, authority, producerUID, wal) {
		return fencing.MutationCompactionPlan{}, errMutationArchiveEvidence
	}
	canonical, err := json.Marshal(plan)
	if err != nil || !bytes.Equal(canonical, data) {
		return fencing.MutationCompactionPlan{}, errMutationArchiveEvidence
	}
	return plan, nil
}

func (e *FencingExecutor) readMutationArchive(ctx context.Context, reference fencing.MutationArchiveReference, runtime fencing.RuntimeBinding, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	return e.readMutationArchiveForAuthority(ctx, runtimeMutationAuthority(runtime), reference, producerUID, wal)
}

// Creation alone never advances coverage. Readback resolves an uncertain create
// of the same immutable content; an existing object's data is never overwritten.
func (e *FencingExecutor) persistMutationArchive(ctx context.Context, plan fencing.MutationCompactionPlan) (fencing.MutationArchiveReference, error) {
	return e.persistMutationArchiveForAuthority(ctx, runtimeMutationAuthority(plan.Runtime), plan)
}

func (e *FencingExecutor) verifyMutationArchiveReceipts(ctx context.Context, plan fencing.MutationCompactionPlan) error {
	verify := e.VerifyMutationEvidence
	if verify == nil {
		verify = e.VerifySourceMutationEvidence
	}
	for sequence, entry := range plan.Entries {
		// The archive's cancelled-unprepared CAS record itself proves no
		// executable descriptor existed. Prepared work needs external evidence.
		if entry.Preparation == nil {
			continue
		}
		if err := verify(ctx, plan.Runtime, sequence, entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *FencingExecutor) validateMutationArchiveHead(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, producer fencing.MutationProducer) error {
	if producer.ArchiveHead == nil {
		return nil
	}
	plan, err := e.readMutationArchive(ctx, *producer.ArchiveHead, runtime, producerUID, producer.WALIdentity)
	if err != nil {
		return err
	}
	if plan.Through != producer.CompactedThrough {
		return errMutationArchiveEvidence
	}
	return nil
}

// Normal admission checks only the pinned head. A definite evidence fault is
// latched; transient transport/API failures are not classified as corruption.
func (e *FencingExecutor) checkMutationArchiveHead(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, producer fencing.MutationProducer) error {
	return e.mutationArchiveFailure(ctx, runtime, e.validateMutationArchiveHead(ctx, runtime, producerUID, producer))
}

func (e *FencingExecutor) mutationArchiveFailure(ctx context.Context, runtime fencing.RuntimeBinding, err error) error {
	if !errors.Is(err, errMutationArchiveEvidence) {
		return err
	}
	_, latchErr := e.mutateProducer(ctx, runtime, false, true, func(p *fencing.MutationProducer) error {
		if p.EvidenceFault == "" {
			p.EvidenceFault = hashJSON([]any{"mutation-archive-evidence-fault", runtime, p.ArchiveHead})
		}
		return nil
	})
	return errors.Join(err, latchErr)
}

// A larger watermark cannot resolve a lost CAS response. Search the currently
// committed chain for this exact UID/digest instead of silently rebasing a plan.
func (e *FencingExecutor) commitMutationArchive(ctx context.Context, runtime fencing.RuntimeBinding, plan fencing.MutationCompactionPlan) (fencing.MutationProducer, error) {
	return e.commitMutationArchiveForAuthority(ctx, runtimeMutationAuthority(runtime), plan)
}

func (e *FencingExecutor) verifyMutationArchiveHistory(ctx context.Context, runtime fencing.RuntimeBinding, verificationID string, budget int) (bool, error) {
	done, _, err := e.scanMutationArchiveHistory(ctx, runtime, verificationID, budget, nil, 0)
	return done, err
}

// Persist the bounded scan position so restarts can finish arbitrarily long
// retained chains. New heads/operations reset it. Head-only admission cannot
// detect arbitrary ancestor loss; retained archives must not be deleted outside
// this protocol. Every new fencing operation verifies full history coverage.
func (e *FencingExecutor) scanMutationArchiveHistory(ctx context.Context, runtime fencing.RuntimeBinding, verificationID string, budget int, sought *fencing.MutationArchiveReference, sequence uint64) (bool, bool, error) {
	return e.scanMutationArchiveHistoryForAuthority(ctx, runtimeMutationAuthority(runtime), verificationID, budget, sought, sequence)
}

// lookupArchivedMutation is read-only with respect to executable permission.
// It requires the original UID/sequence and complete intent, verifies retained
// history, then returns the recorded outcome unchanged (including Excluded).
func (e *FencingExecutor) lookupArchivedMutation(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) (fencing.MutationEntry, error) {
	return e.lookupArchivedMutationForAuthority(ctx, runtimeMutationAuthority(runtime), producerUID, sequence, intent)
}
