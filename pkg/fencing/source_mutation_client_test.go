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
	state       *sourceRetryState
	producer    *MutationProducer
	reserves    int
	seals       int
	admits      int
	loseAdmit   bool
	result      *MutationResolution
	archived    bool
	generation  int64
	lookupErr   error
	beforeAdmit func() error
}

func (f *sourceMutationFake) ReserveUpload(ctx context.Context, _ string, _ MutationIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.reserves++
	return nil
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
	entry, ok := f.producer.Entries[sequence]
	if !ok {
		return MutationEntry{}, false, false, nil
	}
	if entry.Intent != intent {
		return MutationEntry{}, false, f.archived, errors.New("tuple mismatch")
	}
	if f.result != nil && entry.Resolution == nil {
		if err := f.producer.Prepare(sequence, journalPreparation(f.generation)); err != nil {
			return MutationEntry{}, false, false, err
		}
		hash, err := f.producer.PreparationHash(sequence)
		if err != nil {
			return MutationEntry{}, false, false, err
		}
		resolution := *f.result
		resolution.PreparationHash = hash
		if resolution.Outcome == MutationCommitted {
			resolution.PayloadDigest = intent.PayloadDigest
		} else {
			resolution.PayloadDigest = ""
		}
		if err := f.producer.Resolve(sequence, resolution); err != nil {
			return MutationEntry{}, false, false, err
		}
		entry = f.producer.Entries[sequence]
	}
	return entry, true, f.archived, nil
}

func newSourceMutationClientTest(t *testing.T) (*sourceMutationClient, *sourceMutationFake, BindReceipt, string) {
	t.Helper()
	state, binding, dir := newSourceRetryTest(t)
	producer, err := NewMutationProducer(binding.Identity, binding.Producer.Runtime)
	mustJournal(t, err)
	mustJournal(t, producer.Activate(binding.Identity))
	fake := &sourceMutationFake{state: state, producer: producer}
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
	fake.state = client.state
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
}

func TestSourceMutationClientArchivedResultIsLookupOnly(t *testing.T) {
	client, fake, _, _ := newSourceMutationClientTest(t)
	fake.loseAdmit = true
	draft, body := bootstrapDraft("run", "registration"), []byte("{}")
	if err := client.Upload(context.Background(), draft, body); err == nil {
		t.Fatal("expected lost response")
	}
	fake.loseAdmit = false
	fake.archived = true
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := client.Upload(context.Background(), draft, body); err != nil {
		t.Fatal(err)
	}
	if fake.admits != 1 {
		t.Fatal("archived response was re-admitted")
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
			want := ErrMutationRejected
			if outcome == MutationExcluded {
				want = ErrMutationExcluded
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
	if fake.admits != 2 || len(client.state.data.Steps) != 2 || client.state.data.Steps[0].Entry.Intent.OwnerStep == client.state.data.Steps[1].Entry.Intent.OwnerStep {
		t.Fatal("distinct bootstrap registrations did not retain distinct owner tuples")
	}
}
