package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type mutationExecutionTestProvider struct {
	payload      []byte
	result       mutationProviderResult
	observeCalls int
	uploadCalls  int
	deleteCalls  int
	commitFound  bool
	uploadErr    error
	committedErr error
}

func (p *mutationExecutionTestProvider) observe(context.Context, string) (int64, bool, error) {
	p.observeCalls++
	return 0, false, nil
}

func (p *mutationExecutionTestProvider) upload(_ context.Context, _ string, generation int64, intent fencing.MutationIntent, payload io.Reader) (mutationProviderResult, error) {
	p.uploadCalls++
	if generation != 0 {
		return mutationProviderResult{}, errors.New("unexpected generation for create-only upload")
	}
	data, err := io.ReadAll(payload)
	if err != nil {
		return mutationProviderResult{}, err
	}
	p.payload = append([]byte(nil), data...)
	if p.uploadErr != nil {
		return mutationProviderResult{}, p.uploadErr
	}
	result := p.result
	if result.PayloadDigest == "" {
		result.PayloadDigest = intent.PayloadDigest
	}
	return result, nil
}

func (p *mutationExecutionTestProvider) delete(context.Context, string, int64) (mutationProviderResult, error) {
	p.deleteCalls++
	return mutationProviderResult{}, errors.New("unexpected delete")
}

func (p *mutationExecutionTestProvider) committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	return p.result, p.commitFound, p.committedErr
}

func mutationExecutionFixture(t *testing.T, conditions ...fencing.MutationCondition) (*FencingExecutor, fencing.RuntimeBinding, fencing.MutationIntent, []byte, AttestationStoreScope, *MutationExecutionReceiptStore) {
	t.Helper()
	e, _, _, runtime := newMutationStoreFixture(t)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	scopeHash := hashJSON(scope)
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodUID: "pod-0", RunID: "run-0", WALIdentity: "wal"}
	incarnation := stages.incarnation
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.BindMutationScope(id, scopeHash, fencing.MutationExecutionProfile); err != nil {
			return err
		}
		return l.BindMutationStage(id, incarnation)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error { return l.Bind(id) }); err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) }); err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	e.SourceStages = stages

	draft := stageStoreIntent()
	draft.StoreScopeHash = scopeHash
	if len(conditions) != 0 {
		draft.Condition = conditions[0]
	}
	const ticket = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	if err := stages.Reserve(run.MutationProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"membership":"original bytes"}`)
	descriptor, err := stages.Seal(run.MutationProducerUID, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(run.MutationProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = descriptor.Ref, descriptor.Digest, descriptor.Size
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }); err != nil {
		t.Fatal(err)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return e, runtime, intent, payload, scope, receipts
}

func TestExecuteSourceMutationConsumesSealedBytesAndCommitsReceiptBeforeResolve(t *testing.T) {
	e, runtime, intent, payload, scope, receipts := mutationExecutionFixture(t)
	provider := &mutationExecutionTestProvider{result: mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 21, Metageneration: 1, AllCopiesTerminal: true}}
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); err != nil {
		t.Fatal(err)
	}
	if provider.uploadCalls != 1 || !bytes.Equal(provider.payload, payload) {
		t.Fatalf("provider did not receive exactly the sealed bytes once: calls=%d payload=%q", provider.uploadCalls, provider.payload)
	}
	lookup, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 1, Intent: intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationCommitted || !lookup.Entry.Resolution.AllCopiesTerminal {
		t.Fatalf("committed tuple lacks terminal durable resolution: %+v %v", lookup, err)
	}
	dispatchesTerminal := len(lookup.Entry.Dispatches) == 1 && lookup.Entry.DispatchClosed
	for _, dispatch := range lookup.Entry.Dispatches {
		_, digestErr := hex.DecodeString(dispatch.ReceiptDigest)
		dispatchesTerminal = dispatchesTerminal && len(dispatch.ReceiptDigest) == sha256.Size*2 && digestErr == nil
	}
	if !dispatchesTerminal {
		t.Fatalf("provider receipt did not terminalize the one dispatch: %+v", lookup.Entry.Dispatches)
	}
	owner := hashJSON([]any{runtime, "wal", producerUID, uint64(1), intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil || receipt.Generation != 21 || receipt.PayloadDigest != intent.PayloadDigest || receipt.Intent != intent {
		t.Fatalf("immutable provider receipt missing or mismatched: %+v %v", receipt, err)
	}
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); err != nil {
		t.Fatal(err)
	}
	if provider.uploadCalls != 1 {
		t.Fatalf("resolved retry dispatched again: %d calls", provider.uploadCalls)
	}
}

func TestCommittedCreateWithUnresolvedCopiesRetainsKeyGuardAndBlocksBarrier(t *testing.T) {
	e, runtime, intent, _, scope, receipts := mutationExecutionFixture(t)
	provider := &mutationExecutionTestProvider{result: mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 21, PayloadDigest: intent.PayloadDigest}}
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); err != nil {
		t.Fatalf("original durable commitment was not returned: %v", err)
	}
	identity := fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}
	lookup, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, identity, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 1, Intent: intent})
	if err != nil || lookup.Entry == nil || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationCommitted || lookup.Entry.Resolution.AllCopiesTerminal {
		t.Fatalf("original commitment or unresolved-copy state changed: %+v %v", lookup, err)
	}
	closed, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Close("close", hashBytes([]byte("close"))) })
	if err != nil || closed.BarrierComplete() {
		t.Fatalf("zero-generation close barrier ignored unresolved copies: complete=%t err=%v", closed.BarrierComplete(), err)
	}
	lock, err := readMutationKeyForTest(t.Context(), e, hashJSON(scope), intent.Key)
	if err != nil || lock.Owner == "" {
		t.Fatalf("zero-generation per-key guard was released before copy completion: %+v %v", lock, err)
	}
}

func TestExecuteSourceMutationUnknownDispatchIsNeverCopiedOrAcknowledged(t *testing.T) {
	e, runtime, intent, payload, scope, receipts := mutationExecutionFixture(t)
	provider := &mutationExecutionTestProvider{uploadErr: errors.New("response lost")}
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("unknown provider result was not left pending: %v", err)
	}
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("unresolved retry was not left pending: %v", err)
	}
	if provider.uploadCalls != 1 || !bytes.Equal(provider.payload, payload) {
		t.Fatalf("uncertain dispatch was copied or changed: calls=%d payload=%q", provider.uploadCalls, provider.payload)
	}
	owner := hashJSON([]any{runtime, "wal", producerUID, uint64(1), intent.OperationID})
	lock, err := readMutationKeyForTest(t.Context(), e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != owner {
		t.Fatalf("uncertain provider outcome released its durable key lock: %+v %v", lock, err)
	}
	// Canonical lock truth survives loss of the Kubernetes index/cache entirely.
	restarted := &FencingExecutor{AuthorityState: e.AuthorityState}
	if err := restarted.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, false); err != nil {
		t.Fatalf("same unresolved tuple could not recover its object-store lock without Kubernetes: %v", err)
	}
	if err := restarted.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner+"other", false); !errors.Is(err, ErrFencePending) {
		t.Fatalf("object-store lock did not exclude another owner without Kubernetes: %v", err)
	}
	lookup, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 1, Intent: intent})
	dispatchesUnresolved := lookup.Entry != nil && len(lookup.Entry.Dispatches) == 1
	if lookup.Entry != nil {
		for _, dispatch := range lookup.Entry.Dispatches {
			dispatchesUnresolved = dispatchesUnresolved && dispatch.ReceiptDigest == ""
		}
	}
	if err != nil || lookup.Entry == nil || lookup.Entry.Resolution != nil || !dispatchesUnresolved {
		t.Fatalf("unknown dispatch was incorrectly resolved: %+v %v", lookup, err)
	}
}

func readMutationKeyForTest(ctx context.Context, e *FencingExecutor, scopeHash, key string) (mutationKeyState, error) {
	path, identity := e.mutationKeyIdentity(scopeHash, key)
	data, _, err := e.AuthorityState.read(ctx, path, identity)
	if err != nil {
		return mutationKeyState{}, err
	}
	var state mutationKeyState
	if err := decodeAuthorityValue(data, &state); err != nil {
		return mutationKeyState{}, err
	}
	return state, nil
}

type concurrentMutationProvider struct {
	observed     chan struct{}
	entered      chan struct{}
	release      chan struct{}
	observeCalls atomic.Int32
	uploadCalls  atomic.Int32
	closeEntered sync.Once
}

func (p *concurrentMutationProvider) observe(context.Context, string) (int64, bool, error) {
	if p.observeCalls.Add(1) == 2 {
		close(p.observed)
	}
	<-p.observed
	return 0, false, nil
}

func (p *concurrentMutationProvider) upload(ctx context.Context, _ string, _ int64, intent fencing.MutationIntent, _ io.Reader) (mutationProviderResult, error) {
	p.uploadCalls.Add(1)
	p.closeEntered.Do(func() { close(p.entered) })
	select {
	case <-p.release:
		return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 21, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}, nil
	case <-ctx.Done():
		return mutationProviderResult{}, ctx.Err()
	}
}

func (p *concurrentMutationProvider) delete(context.Context, string, int64) (mutationProviderResult, error) {
	return mutationProviderResult{}, errors.New("unexpected delete")
}

func (p *concurrentMutationProvider) committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	return mutationProviderResult{}, false, nil
}

func TestConcurrentExecutorsGrantOnlyOnePhysicalDispatch(t *testing.T) {
	condition := fencing.MutationCondition{Mode: fencing.MutationUnconditional}
	e, runtime, intent, _, scope, receipts := mutationExecutionFixture(t, condition)
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	provider := &concurrentMutationProvider{observed: make(chan struct{}), entered: make(chan struct{}), release: make(chan struct{})}
	secondExecutor := *e
	type result struct{ err error }
	results := make(chan result, 2)
	go func() {
		results <- result{err: secondExecutor.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts)}
	}()
	go func() {
		results <- result{err: e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts)}
	}()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("neither executor reached the physical provider call")
	}
	var loser result
	select {
	case loser = <-results:
		if loser.err == nil {
			t.Fatal("executor returned before the blocked physical call completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("competing executor did not resolve to pending while the winner was blocked")
	}
	if !errors.Is(loser.err, ErrFencePending) {
		t.Fatalf("competing executor did not fail closed: %v", loser.err)
	}
	close(provider.release)
	var winner result
	select {
	case winner = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("winning executor did not complete after provider release")
	}
	if winner.err != nil || !errors.Is(loser.err, ErrFencePending) {
		t.Fatalf("expected one committed winner and one pending loser, got %v and %v", loser.err, winner.err)
	}
	if calls := provider.uploadCalls.Load(); calls != 1 {
		t.Fatalf("competing executors issued %d physical provider calls, want exactly one", calls)
	}
}

func TestGrantMutationDispatchRequiresExactLostResponseProof(t *testing.T) {
	tests := []struct {
		name       string
		dispatches map[string]fencing.MutationDispatch
		closed     bool
		wantErr    bool
	}{
		{name: "missing dispatch map", closed: true, wantErr: true},
		{name: "different dispatch ID", dispatches: map[string]fencing.MutationDispatch{"other": {}}, closed: true, wantErr: true},
		{name: "exact unresolved dispatch", dispatches: map[string]fencing.MutationDispatch{"ours": {}}, closed: true},
		{name: "exact dispatch already terminal", dispatches: map[string]fencing.MutationDispatch{"ours": {ReceiptDigest: strings.Repeat("a", 64)}}, closed: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := &fencing.MutationProducer{Entries: map[uint64]fencing.MutationEntry{
				1: {Preparation: &fencing.MutationPreparation{}, DispatchClosed: tt.closed, Dispatches: tt.dispatches},
			}}
			err := grantMutationDispatch(producer, 1, "ours")
			if tt.wantErr && !errors.Is(err, ErrFencePending) {
				t.Fatalf("grant result=%v, want pending", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("exact lost-response proof was rejected: %v", err)
			}
		})
	}
}

func TestVerifySourceMutationEvidenceRejectsChangedDispatchID(t *testing.T) {
	e, runtime, intent, _, _, _ := mutationExecutionFixture(t)
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	preparation := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true}
	receipt := mutationExecutionReceipt{
		Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: 1,
		Intent: intent, Preparation: preparation, DispatchID: "tampered-dispatch-id",
		Outcome: fencing.MutationCommitted, Generation: 21, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true,
	}
	receipt.ReceiptDigest = receipt.digest()
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := NewRemoteMutationExecutionReceiptStore(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipts.put(receipt); err != nil {
		t.Fatal(err)
	}
	entry := fencing.MutationEntry{
		Intent: intent, Preparation: &preparation, DispatchClosed: true,
		Dispatches: map[string]fencing.MutationDispatch{"journal-dispatch-id": {ReceiptDigest: receipt.ReceiptDigest}},
		Resolution: &fencing.MutationResolution{
			Outcome: receipt.Outcome, ReceiptDigest: receipt.ReceiptDigest, PayloadDigest: receipt.PayloadDigest, AllCopiesTerminal: true,
		},
	}
	if err := e.VerifySourceMutationEvidence(t.Context(), runtime, 1, entry); err == nil {
		t.Fatal("receipt with a changed, ungranted dispatch ID was accepted")
	}
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
