package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

type sourceMutationFake struct {
	producer        *MutationProducer
	producerUID     string
	reserves        int
	reserveErr      error
	seals           int
	admits          int
	loseAdmit       bool
	archiveOnAdmit  bool
	result          *MutationResolution
	archivedEntries map[uint64]MutationEntry
	generation      int64
	pendingDispatch bool
	lookupErr       error
	beforeAdmit     func() error
}

func (f *sourceMutationFake) ReserveUpload(ctx context.Context, _ string, _ MutationIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.reserves++
	return f.reserveErr
}

func (f *sourceMutationFake) SealUpload(ctx context.Context, ticket string, payload []byte) (sourceSealedPayload, error) {
	if err := ctx.Err(); err != nil {
		return sourceSealedPayload{}, err
	}
	f.seals++
	digest := sha256.Sum256(payload)
	return sourceSealedPayload{Ref: "sealed/" + ticket, Digest: hex.EncodeToString(digest[:]), Size: int64(len(payload))}, nil
}

func (f *sourceMutationFake) Admit(ctx context.Context, sequence uint64, intent MutationIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.beforeAdmit != nil {
		if err := f.beforeAdmit(); err != nil {
			return err
		}
	}
	f.admits++
	if err := f.producer.Admit(sequence, intent); err != nil {
		return err
	}
	if f.archiveOnAdmit {
		if err := f.saveArchived(sequence); err != nil {
			return err
		}
		return ErrMutationArchived
	}
	if f.loseAdmit {
		return errors.New("simulated lost admission response")
	}
	return nil
}

func (f *sourceMutationFake) Lookup(ctx context.Context, sequence uint64, intent MutationIntent) (MutationEntry, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return MutationEntry{}, false, false, err
	}
	if f.lookupErr != nil && f.producer.LastAdmitted > 0 {
		return MutationEntry{}, false, false, f.lookupErr
	}
	if entry, ok := f.archivedEntries[sequence]; ok {
		if entry.Intent != intent {
			return MutationEntry{}, false, true, errors.New("archived tuple mismatch")
		}
		return entry, true, true, nil
	}
	entry, ok := f.producer.Entries[sequence]
	if !ok {
		return MutationEntry{}, false, false, nil
	}
	if entry.Intent != intent {
		return MutationEntry{}, false, false, errors.New("tuple mismatch")
	}
	entry, err := f.resolve(sequence)
	if err != nil {
		return MutationEntry{}, false, false, err
	}
	return entry, true, false, nil
}

func (f *sourceMutationFake) resolve(sequence uint64) (MutationEntry, error) {
	entry := f.producer.Entries[sequence]
	if f.result == nil || entry.Resolution != nil {
		return entry, nil
	}
	if entry.Preparation == nil {
		if err := f.producer.Prepare(sequence, journalPreparation(f.generation)); err != nil {
			return MutationEntry{}, err
		}
	}
	if f.pendingDispatch {
		if err := f.producer.GrantDispatch(sequence, "copy-1"); err != nil {
			return MutationEntry{}, err
		}
	}
	resolution := *f.result
	if resolution.AllCopiesTerminal {
		if err := f.producer.SealDispatch(sequence); err != nil {
			return MutationEntry{}, err
		}
		entry = f.producer.Entries[sequence]
		for dispatchID, dispatch := range entry.Dispatches {
			if dispatch.ReceiptDigest == "" {
				if err := f.producer.RecordDispatchTerminal(sequence, dispatchID, journalTestDigest); err != nil {
					return MutationEntry{}, err
				}
			}
		}
	}
	hash, err := f.producer.PreparationHash(sequence)
	if err != nil {
		return MutationEntry{}, err
	}
	resolution.PreparationHash = hash
	if resolution.Outcome == MutationCommitted {
		resolution.PayloadDigest = entry.Intent.PayloadDigest
	} else {
		resolution.PayloadDigest = ""
	}
	if err := f.producer.Resolve(sequence, resolution); err != nil {
		return MutationEntry{}, err
	}
	return f.producer.Entries[sequence], nil
}

func (f *sourceMutationFake) saveArchived(sequence uint64) error {
	entry, err := f.resolve(sequence)
	if err != nil {
		return err
	}
	if f.archivedEntries == nil {
		f.archivedEntries = make(map[uint64]MutationEntry)
	}
	f.archivedEntries[sequence] = cloneSourceRetryStep(sourceRetryStep{Entry: entry}).Entry
	plan, err := f.producer.PlanMutationCompaction(f.producerUID, sequence)
	if err != nil {
		return err
	}
	if err := f.producer.CommitMutationCompaction(f.producerUID, plan, archiveReference(plan)); err != nil {
		return err
	}
	return nil
}

func newSourceMutationClientTest(t *testing.T) (*sourceMutationClient, *sourceMutationFake, BindReceipt, string) {
	t.Helper()
	state, binding, dir := newSourceRetryTest(t)
	producer, err := NewMutationProducer(binding.Identity, binding.Producer.Runtime)
	mustJournal(t, err)
	mustJournal(t, producer.Activate(binding.Identity))
	fake := &sourceMutationFake{producer: producer, producerUID: binding.Producer.ProducerUID}
	return &sourceMutationClient{state: state, authority: fake}, fake, binding, dir
}

func bootstrapDraft(owner, step string) MutationIntent {
	intent := sourceRetryIntent(step)
	intent.OwnerStep.Owner = owner
	intent.OwnerStep.Workflow = "bootstrap"
	intent.OwnerStep.Step = step
	intent.Key = "bootstrap/" + step + ".json"
	return intent
}

func TestSourceMutationClientLostAdmissionResponseReopensSameTuple(t *testing.T) {
	client, fake, binding, dir := newSourceMutationClientTest(t)
	fake.loseAdmit = true
	draft, body := bootstrapDraft("run-1", "registration-a"), []byte(`{"node":"a"}`)
	if err := client.Upload(context.Background(), draft, body); err == nil {
		t.Fatal("lost response was treated as success")
	}
	if fake.admits != 1 {
		t.Fatalf("admit count = %d", fake.admits)
	}
	client.state = reopenSourceRetryTest(t, client.state, binding, dir)
	fake.loseAdmit = false
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if fake.admits != 1 {
		t.Fatalf("known admission was resubmitted %d times", fake.admits-1)
	}
	if len(client.state.data.Steps) != 1 || client.state.data.Steps[0].Sequence != 1 || !client.state.data.Steps[0].Advanced {
		t.Fatal("reopen did not retain and advance the original tuple")
	}
}

func TestSourceMutationClientFsyncsTupleBeforeFirstAdmission(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.beforeAdmit = func() error {
		data, err := os.ReadFile(client.state.dir + "/retry.json")
		if err != nil {
			return err
		}
		var saved sourceRetryFile
		if err := json.Unmarshal(data, &saved); err != nil {
			return err
		}
		if len(saved.Data.Steps) != 1 || saved.Data.Steps[0].Sequence != 1 || saved.Data.Steps[0].Entry.Intent.PayloadDigest == "" {
			return errors.New("original full tuple not durable before Admit")
		}
		return nil
	}
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration"), []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func TestSourceMutationClientUncertainAdmissionBlocksAnotherOwner(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.loseAdmit = true
	if err := client.Upload(context.Background(), bootstrapDraft("run", "one"), []byte("one")); err == nil {
		t.Fatal("lost admission response succeeded")
	}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "two"), []byte("two")); !errors.Is(err, ErrPending) {
		t.Fatalf("second owner passed uncertain admission: %v", err)
	}
	if fake.admits != 1 || len(client.state.data.Steps) != 1 {
		t.Fatal("uncertain owner advanced or consumed another sequence")
	}
}

func TestSourceMutationClientResultSyncFailurePreventsAcknowledgement(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	draft := bootstrapDraft("run", "registration")
	// Admit normally, then surface a terminal result and fail its durable write.
	fake.beforeAdmit = nil
	if err := client.Upload(context.Background(), draft, []byte("{}")); !errors.Is(err, ErrPending) {
		t.Fatalf("initial pending result: %v", err)
	}
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	var syncs int
	client.state.syncDir = func(string) error {
		syncs++
		if syncs == 2 {
			return errors.New("injected result fsync failure")
		}
		return syncSourceRetryDir(client.state.dir)
	}
	if err := client.Upload(context.Background(), draft, []byte("{}")); err == nil {
		t.Fatal("result sync failure returned success")
	}
	step := client.state.data.Steps[0]
	if step.Advanced || step.Entry.Resolution != nil {
		t.Fatal("failed result fsync acknowledged or advanced owner")
	}
}

func TestSourceMutationClientCancellationDoesNotCancelOrAcknowledgeWork(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Upload(ctx, bootstrapDraft("run", "registration"), []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request: %v", err)
	}
	if fake.admits != 0 || len(client.state.data.Steps) != 0 {
		t.Fatal("cancellation implied admission or ACK")
	}
}

func TestSourceMutationClientLookupTimeoutLeavesAdmittedWorkPending(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.lookupErr = context.DeadlineExceeded
	err := client.Upload(context.Background(), bootstrapDraft("run", "registration"), []byte("{}"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lookup timeout: %v", err)
	}
	step := client.state.data.Steps[0]
	if fake.admits != 1 || !step.Admitted || step.Entry.Resolution != nil || step.Advanced {
		t.Fatal("timeout implied provider cancellation, result, or owner ACK")
	}
	fake.lookupErr = nil
	fake.reserveErr = errors.New("stage service unavailable")
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration"), []byte("{}")); err != nil {
		t.Fatalf("known admitted work was gated by remote staging: %v", err)
	}
	if fake.admits != 1 {
		t.Fatal("known admission was resubmitted after timeout")
	}
}

func TestSourceMutationClientArchivedResultIsLookupOnly(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.loseAdmit = true
	draft, body := bootstrapDraft("run", "registration"), []byte("{}")
	if err := client.Upload(context.Background(), draft, body); err == nil {
		t.Fatal("expected lost response")
	}
	fake.loseAdmit = false
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := fake.saveArchived(1); err != nil {
		t.Fatal(err)
	}
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatal(err)
	}
	if fake.admits != 1 || len(fake.producer.Entries) != 0 || fake.producer.CompactedThrough != 1 || fake.archivedEntries[1].Resolution == nil {
		t.Fatal("compacted archived result was not lookup-only")
	}
}

func TestSourceMutationClientAdmitArchivedPersistsLookupOnlyStatus(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.archiveOnAdmit = true
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	draft, body := bootstrapDraft("run", "registration"), []byte("{}")
	if err := client.Upload(context.Background(), draft, body); !errors.Is(err, ErrMutationArchived) {
		t.Fatalf("archived admission response: %v", err)
	}
	if !client.state.data.Steps[0].Admitted || len(fake.producer.Entries) != 0 {
		t.Fatal("archived admission response was not retained as known admitted")
	}
	fake.archiveOnAdmit = false
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatal(err)
	}
	if fake.admits != 1 {
		t.Fatal("known archived tuple was re-admitted")
	}
}

func TestSourceMutationClientReopensDurableResultWithoutStagingOrLookup(t *testing.T) {
	client, fake, binding, dir := newSourceMutationClientTest(t)
	draft, body := bootstrapDraft("run", "registration"), []byte(`{"node":"n"}`)
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatal(err)
	}
	client.state = reopenSourceRetryTest(t, client.state, binding, dir)
	fake.reserveErr = errors.New("staging service unavailable")
	fake.lookupErr = errors.New("authority lookup unavailable")
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatalf("durable original result was not returned locally: %v", err)
	}
	if fake.admits != 1 || fake.reserves != 1 {
		t.Fatal("reopened durable result initiated new remote work")
	}
}

func TestSourceMutationClientCommitCanRetainUnresolvedCopies(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.pendingDispatch = true
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration"), []byte("{}")); err != nil {
		t.Fatalf("committed result with pending copy: %v", err)
	}
	entry := fake.producer.Entries[1]
	if entry.Resolution == nil || entry.Resolution.AllCopiesTerminal || entry.DispatchClosed || entry.Dispatches["copy-1"].ReceiptDigest != "" {
		t.Fatal("fixture forced outstanding copy work to terminal before commitment")
	}
}

func TestSourceMutationClientNegativeResultsAreNotSuccessAndBootstrapOwnersDiffer(t *testing.T) {
	for _, outcome := range []MutationOutcome{MutationExcluded, MutationRejected} {
		t.Run(string(outcome), func(t *testing.T) {
			client, fake, _, _ := newSourceMutationClientTest(t)
			draft := bootstrapDraft("run", "registration-a")
			if outcome == MutationExcluded {
				// Exclusion is meaningful only for a positive-generation preparation;
				// zero-generation future-effect exclusion is invalid by contract.
				draft.Condition = MutationCondition{Mode: MutationUnconditional}
				fake.generation = 7
			}
			fake.result = &MutationResolution{Outcome: outcome, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
			err := client.Upload(context.Background(), draft, []byte(`{"a":1}`))
			want := errMutationRejected
			if outcome == MutationExcluded {
				want = errMutationExcluded
			}
			if !errors.Is(err, want) {
				t.Fatalf("outcome %s returned %v", outcome, err)
			}
			if client.state.data.Steps[0].Entry.Resolution == nil || !client.state.data.Steps[0].Advanced {
				t.Fatal("negative result was not durably retained")
			}
		})
	}
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration-a"), []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration-b"), []byte(`{"b":2}`)); err != nil {
		t.Fatal(err)
	}
	fake.reserveErr = errors.New("stage service unavailable")
	if err := client.Upload(context.Background(), bootstrapDraft("run", "registration-b"), []byte(`{"b":2}`)); err != nil {
		t.Fatalf("durable original result was gated by remote stage reservation: %v", err)
	}
	if fake.admits != 2 || len(client.state.data.Steps) != 2 || client.state.data.Steps[0].Entry.Intent.OwnerStep == client.state.data.Steps[1].Entry.Intent.OwnerStep {
		t.Fatal("distinct bootstrap registrations did not retain distinct owner tuples")
	}
}
