package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

var errMutationArchiveEvidence = errors.New("fencing: retained mutation archive is missing or differs")

func (e *FencingExecutor) mutationArchivePath(name string) string {
	return e.controller().corePath("configmaps", name)
}

func (e *FencingExecutor) decodeMutationArchive(resource object, reference fencing.MutationArchiveReference, runtime fencing.RuntimeBinding, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	var plan fencing.MutationCompactionPlan
	if !reference.Valid() || str(nested(resource, "metadata", "name")) != reference.Name || str(nested(resource, "metadata", "namespace")) != e.Kube.Namespace || str(nested(resource, "metadata", "uid")) != reference.UID || nested(resource, "metadata", "deletionTimestamp") != nil || resource["immutable"] != true {
		return plan, errMutationArchiveEvidence
	}
	raw := str(nested(resource, "data", "archive.json"))
	if len(raw) > 700000 {
		return plan, errMutationArchiveEvidence
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&plan) != nil || d.Decode(new(any)) != io.EOF || plan.Validate() != nil || plan.Digest() != reference.Digest || plan.ProducerUID != producerUID || plan.Runtime != runtime || plan.WALIdentity != wal {
		return fencing.MutationCompactionPlan{}, errMutationArchiveEvidence
	}
	return plan, nil
}

func (e *FencingExecutor) readMutationArchive(ctx context.Context, reference fencing.MutationArchiveReference, runtime fencing.RuntimeBinding, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	if !reference.Valid() {
		return fencing.MutationCompactionPlan{}, errMutationArchiveEvidence
	}
	var resource object
	if err := e.Kube.Get(ctx, e.mutationArchivePath(reference.Name), &resource); err != nil {
		if apiNotFound(err) {
			return fencing.MutationCompactionPlan{}, errMutationArchiveEvidence
		}
		return fencing.MutationCompactionPlan{}, err
	}
	return e.decodeMutationArchive(resource, reference, runtime, producerUID, wal)
}

// Creation alone never advances coverage. Readback resolves an uncertain create
// of the same immutable content; an existing object's data is never overwritten.
func (e *FencingExecutor) persistMutationArchive(ctx context.Context, plan fencing.MutationCompactionPlan) (fencing.MutationArchiveReference, error) {
	if err := plan.Validate(); err != nil {
		return fencing.MutationArchiveReference{}, err
	}
	if err := e.verifyMutationArchiveReceipts(ctx, plan); err != nil {
		return fencing.MutationArchiveReference{}, err
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return fencing.MutationArchiveReference{}, err
	}
	if len(data) > 700000 {
		return fencing.MutationArchiveReference{}, fmt.Errorf("fencing: archive capacity exhausted")
	}
	ref := fencing.MutationArchiveReference{Digest: plan.Digest()}
	ref.Name = fencing.MutationArchiveName(ref.Digest)
	resource := object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": ref.Name, "namespace": e.Kube.Namespace}, "immutable": true, "data": object{"archive.json": string(data)}}
	postErr := e.Kube.Post(ctx, e.controller().corePath("configmaps", ""), resource, nil)
	var stored object
	if err := e.Kube.Get(ctx, e.mutationArchivePath(ref.Name), &stored); err != nil {
		return ref, errors.Join(postErr, err)
	}
	ref.UID = str(nested(stored, "metadata", "uid"))
	if _, err := e.decodeMutationArchive(stored, ref, plan.Runtime, plan.ProducerUID, plan.WALIdentity); err != nil {
		return ref, err
	}
	return ref, nil
}

func (e *FencingExecutor) verifyMutationArchiveReceipts(ctx context.Context, plan fencing.MutationCompactionPlan) error {
	for sequence, entry := range plan.Entries {
		// The archive's cancelled-unprepared CAS record itself proves no
		// executable descriptor existed. Prepared work needs external evidence.
		if entry.Preparation == nil {
			continue
		}
		if e.VerifyMutationEvidence == nil {
			return fmt.Errorf("fencing: prepared mutation evidence verifier is not configured")
		}
		if err := e.VerifyMutationEvidence(ctx, plan.Runtime, sequence, entry); err != nil {
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
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return fencing.MutationProducer{}, err
	}
	if plan.ProducerUID != run.MutationProducerUID || plan.Runtime != runtime || plan.WALIdentity != run.Identity.WALIdentity {
		return fencing.MutationProducer{}, fmt.Errorf("fencing: compaction identity differs from enrollment")
	}
	ref, err := e.persistMutationArchive(ctx, plan)
	if err != nil {
		return fencing.MutationProducer{}, err
	}
	state, commitErr := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		return p.CommitMutationCompaction(run.MutationProducerUID, plan, ref)
	})
	if commitErr == nil {
		return state, nil
	}
	done, found, verifyErr := e.scanMutationArchiveHistory(ctx, runtime, hashJSON([]any{"resolve-compaction", ref}), 32, &ref, 0)
	if verifyErr != nil {
		return state, errors.Join(commitErr, verifyErr)
	}
	if !done {
		return state, ErrFencePending
	}
	if !found {
		return state, commitErr
	}
	return e.reserveMutationProducer(ctx, runtime)
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
	if budget < 1 || budget > 64 || len(verificationID) != 64 || (sought != nil && (!sought.Valid() || sequence != 0)) {
		return false, false, fmt.Errorf("fencing: invalid archive verification request")
	}
	producer, err := e.reserveMutationProducer(ctx, runtime)
	if err != nil {
		return false, false, err
	}
	if producer.ArchiveHead == nil {
		return true, false, nil
	}
	if sequence > producer.CompactedThrough {
		return false, false, fmt.Errorf("fencing: result sequence is not archived")
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return false, false, err
	}
	head := *producer.ArchiveHead
	scan := fencing.MutationArchiveScan{VerificationID: verificationID, Head: head, Next: &head, Through: producer.CompactedThrough, Sought: sought, Sequence: sequence}
	if old := producer.ArchiveScan; old != nil && old.VerificationID == verificationID && old.Head == head && hashJSON(old.Sought) == hashJSON(sought) && old.Sequence == sequence {
		scan = *old
	}
	for i := 0; i < budget && scan.Next != nil; i++ {
		ref := *scan.Next
		plan, err := e.readMutationArchive(ctx, ref, runtime, run.MutationProducerUID, producer.WALIdentity)
		if err == nil && plan.Through != scan.Through {
			err = errMutationArchiveEvidence
		}
		if err == nil {
			err = e.verifyMutationArchiveReceipts(ctx, plan)
		}
		if err != nil {
			return false, false, e.mutationArchiveFailure(ctx, runtime, err)
		}
		if sought != nil && ref == *sought {
			scan.Found = true
		}
		if sequence != 0 && plan.PreviousThrough < sequence && sequence <= plan.Through {
			scan.Found = true
			copy := ref
			scan.ResultArchive = &copy
		}
		scan.Through, scan.Next = plan.PreviousThrough, plan.Previous
		scan.Links++
	}
	if (scan.Next == nil) != (scan.Through == 0) {
		return false, false, e.mutationArchiveFailure(ctx, runtime, errMutationArchiveEvidence)
	}
	_, err = e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		if p.ArchiveHead == nil || *p.ArchiveHead != head || p.CompactedThrough != producer.CompactedThrough {
			return ErrFencePending
		}
		p.ArchiveScan = &scan
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return scan.Next == nil, scan.Found, nil
}

// lookupArchivedMutation is read-only with respect to executable permission.
// It requires the original UID/sequence and complete intent, verifies retained
// history, then returns the recorded outcome unchanged (including Excluded).
func (e *FencingExecutor) lookupArchivedMutation(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) (fencing.MutationEntry, error) {
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if producerUID == "" || producerUID != run.MutationProducerUID || sequence == 0 || intent.OperationID != fencing.MutationRequestID(producerUID, sequence) {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: archived-result request identity differs")
	}
	verificationID := hashJSON([]any{"lookup-archived-mutation", producerUID, sequence, intent})
	done, found, err := e.scanMutationArchiveHistory(ctx, runtime, verificationID, 32, nil, sequence)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if !done {
		return fencing.MutationEntry{}, ErrFencePending
	}
	if !found {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: original mutation is not archived")
	}
	p, err := e.reserveMutationProducer(ctx, runtime)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if p.ArchiveScan == nil || p.ArchiveScan.VerificationID != verificationID || p.ArchiveScan.Sequence != sequence || p.ArchiveScan.ResultArchive == nil {
		return fencing.MutationEntry{}, ErrFencePending
	}
	plan, err := e.readMutationArchive(ctx, *p.ArchiveScan.ResultArchive, runtime, producerUID, p.WALIdentity)
	if err != nil {
		return fencing.MutationEntry{}, e.mutationArchiveFailure(ctx, runtime, err)
	}
	entry, exists := plan.Entries[sequence]
	if !exists || hashJSON(entry.Intent) != hashJSON(intent) {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: archived retry changed original intent")
	}
	if err := e.verifyMutationArchiveReceipts(ctx, plan); err != nil {
		return fencing.MutationEntry{}, e.mutationArchiveFailure(ctx, runtime, err)
	}
	return entry, nil
}
