package operator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type logicalContentionStorage struct {
	mu         sync.Mutex
	generation int64
	writes     int
	payload    []byte
}

type logicalContentionProvider struct {
	storage    *logicalContentionStorage
	entered    chan struct{}
	release    <-chan struct{}
	blockAt    string
	unknown    bool
	calls      atomic.Int32
	enteredOne sync.Once
}

func (p *logicalContentionProvider) observe(context.Context, string) (int64, bool, error) {
	p.storage.mu.Lock()
	defer p.storage.mu.Unlock()
	return p.storage.generation, p.storage.generation != 0, nil
}

func (p *logicalContentionProvider) upload(ctx context.Context, _ string, generation int64, intent fencing.MutationIntent, payload io.Reader) (mutationProviderResult, error) {
	p.calls.Add(1)
	data, err := io.ReadAll(payload)
	if err != nil {
		return mutationProviderResult{}, err
	}
	block := func() error {
		if p.blockAt == "" {
			return nil
		}
		p.enteredOne.Do(func() { close(p.entered) })
		select {
		case <-p.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.blockAt == "before-write" {
		if err := block(); err != nil {
			return mutationProviderResult{}, err
		}
	}

	p.storage.mu.Lock()
	if p.storage.generation != generation {
		p.storage.mu.Unlock()
		return mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true,
			Generation: generation, AllCopiesTerminal: true}, nil
	}
	p.storage.generation++
	p.storage.writes++
	p.storage.payload = append([]byte(nil), data...)
	resultGeneration := p.storage.generation
	p.storage.mu.Unlock()

	if p.blockAt == "after-write" {
		if err := block(); err != nil {
			return mutationProviderResult{}, err
		}
	}
	if p.unknown {
		return mutationProviderResult{}, errors.New("injected unknown provider outcome")
	}
	return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: resultGeneration, Metageneration: 1,
		PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}, nil
}

func (p *logicalContentionProvider) delete(context.Context, string, int64) (mutationProviderResult, error) {
	return mutationProviderResult{}, errors.New("unexpected delete")
}

func (p *logicalContentionProvider) committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	return mutationProviderResult{}, false, nil
}

type logicalContentionFixture struct {
	httpFixture  *logicalManifestHTTPFixture
	identity     fencing.Identity
	ownerBinding fencing.LogicalWriterBindingV1
	waiterBind   fencing.LogicalWriterBindingV1
	scope        AttestationStoreScope
	ownerIntent  fencing.MutationIntent
	waiterIntent fencing.MutationIntent
}

func newLogicalContentionFixture(t *testing.T) logicalContentionFixture {
	t.Helper()
	ctx := t.Context()
	httpFixture := newLogicalManifestHTTPFixture(t, nil)
	authority := &fencing.AuthorityClient{URL: httpFixture.server.URL + "/v1/supervisor", TokenFile: httpFixture.podTokenFile,
		Client: httpFixture.server.Client(), AuthorityProfile: fencing.LogicalWriterProfileV1}
	identity := httpFixture.identity
	if _, err := authority.RegisterStartup(ctx, identity); err != nil {
		t.Fatal(err)
	}
	identity.WALIdentity = hashJSON("contention-owner-wal")
	bound, err := authority.Bind(ctx, identity)
	if err != nil || bound.LogicalBinding == nil || bound.Producer == nil {
		t.Fatalf("bind owner logical producer: receipt=%+v err=%v", bound, err)
	}
	scope, err := httpFixture.e.sourceMutationStorageScopeForRoot(ctx, logicalManifestSTSUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	waiterID, waiterBinding := registerTerminalCleanupReplacement(t, httpFixture, identity, *bound.LogicalBinding)
	_ = waiterID
	ownerIntent := admitLogicalContentionMutation(t, httpFixture, *bound.LogicalBinding, "owner", []byte("owner bytes"))
	waiterIntent := admitLogicalContentionMutation(t, httpFixture, waiterBinding, "waiter", []byte("waiter bytes"))
	return logicalContentionFixture{httpFixture: httpFixture, identity: identity, ownerBinding: *bound.LogicalBinding,
		waiterBind: waiterBinding, scope: scope, ownerIntent: ownerIntent, waiterIntent: waiterIntent}
}

func admitLogicalContentionMutation(t *testing.T, fixture *logicalManifestHTTPFixture, binding fencing.LogicalWriterBindingV1,
	step string, payload []byte) fencing.MutationIntent {
	t.Helper()
	ctx := t.Context()
	stages, err := fixture.e.sourceStagesForRoot(ctx, logicalManifestSTSUID, binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	draft := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "upload", Key: "archive/PUBLISH_LOCK"})
	draft.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 14}
	draft.OwnerStep = fencing.MutationOwnerStep{Owner: binding.ProducerUID, Workflow: "archive-sync", Step: step, Attempt: 1}
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	ticket := hashJSON([]any{"logical-contention-stage", binding.ProducerUID, step, draft, hashBytes(payload)})
	if err := stages.ReserveContext(ctx, binding.ProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := stages.SealContext(ctx, binding.ProducerUID, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	producer, uid, version, err := fixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, binding)
	if err != nil || uid != binding.ProducerUID {
		t.Fatalf("read admitted logical producer: uid=%q err=%v", uid, err)
	}
	if err := producer.Admit(1, intent); err != nil {
		t.Fatal(err)
	}
	if err := fixture.state.WriteLogicalProducer(ctx, logicalManifestSTSUID, binding, producer, version); err != nil {
		t.Fatal(err)
	}
	return intent
}

func logicalContentionReceipts(t *testing.T, fixture *logicalManifestHTTPFixture, binding fencing.LogicalWriterBindingV1) *MutationExecutionReceiptStore {
	t.Helper()
	receipts, err := fixture.e.receiptsForLogicalBinding(logicalManifestSTSUID, binding)
	if err != nil {
		t.Fatal(err)
	}
	return receipts
}

func TestLogicalMutationForeignOwnerContentionKeepsExactWaiterTuple(t *testing.T) {
	for _, blockAt := range []string{"before-write", "after-write"} {
		t.Run(blockAt, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			fixture := newLogicalContentionFixture(t)
			storage := &logicalContentionStorage{generation: 14}
			entered, release := make(chan struct{}), make(chan struct{})
			ownerProvider := &logicalContentionProvider{storage: storage, entered: entered, release: release, blockAt: blockAt}
			ownerReceipts := logicalContentionReceipts(t, fixture.httpFixture, fixture.ownerBinding)
			ownerResult := make(chan error, 1)
			go func() {
				ownerResult <- fixture.httpFixture.e.ExecuteLogicalSourceMutation(ctx, logicalManifestSTSUID, fixture.ownerBinding,
					fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent, fixture.scope, ownerProvider, ownerReceipts)
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatalf("owner did not reach provider barrier: %v", ctx.Err())
			}

			waiterProvider := &logicalContentionProvider{storage: storage}
			waiterReceipts := logicalContentionReceipts(t, fixture.httpFixture, fixture.waiterBind)
			waitErr := fixture.httpFixture.e.ExecuteLogicalSourceMutation(ctx, logicalManifestSTSUID, fixture.waiterBind,
				fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, waiterProvider, waiterReceipts)
			if !errors.Is(waitErr, fencing.ErrMutationPreDispatchContention) || !errors.Is(waitErr, fencing.ErrPending) {
				t.Fatalf("waiter error=%v, want the exact pre-dispatch contention subtype", waitErr)
			}
			waiter, waiterUID, _, err := fixture.httpFixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, fixture.waiterBind)
			if err != nil || waiterUID != fixture.waiterBind.ProducerUID {
				t.Fatalf("read waiter before owner settles: uid=%q err=%v", waiterUID, err)
			}
			entry := waiter.Entries[1]
			if entry.Preparation != nil || entry.Resolution != nil || entry.DispatchClosed || len(entry.Dispatches) != 0 ||
				entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil || entry.GenerationZeroGuard != nil || entry.GenerationZeroRights != nil || waiterProvider.calls.Load() != 0 {
				t.Fatalf("contention changed waiter permission or provider state: entry=%+v calls=%d", entry, waiterProvider.calls.Load())
			}

			close(release)
			select {
			case err := <-ownerResult:
				if err != nil {
					t.Fatalf("owner failed after releasing provider barrier: %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("owner did not settle: %v", ctx.Err())
			}
			storage.mu.Lock()
			generation, writes := storage.generation, storage.writes
			storage.mu.Unlock()
			if generation != 15 || writes != 1 || ownerProvider.calls.Load() != 1 {
				t.Fatalf("owner provider effects differ: generation=%d writes=%d calls=%d", generation, writes, ownerProvider.calls.Load())
			}

			// Retry the same producer sequence and exact intent after the old
			// owner completed. Its original MatchGeneration is now stale; the
			// normal provider 412 path must terminalize it without changing bytes.
			waitErr = fixture.httpFixture.e.ExecuteLogicalSourceMutation(ctx, logicalManifestSTSUID, fixture.waiterBind,
				fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, waiterProvider, waiterReceipts)
			if !errors.Is(waitErr, fencing.ErrMutationConditionFailed) {
				t.Fatalf("exact waiter retry result=%v, want its original generation condition to fail terminally", waitErr)
			}
			waiter, _, _, err = fixture.httpFixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, fixture.waiterBind)
			if err != nil || waiter.LastAdmitted != 1 || waiter.Entries[1].Intent != fixture.waiterIntent || waiter.Entries[1].Resolution == nil ||
				!waiter.Entries[1].Resolution.ConditionFailed || waiterProvider.calls.Load() != 1 {
				t.Fatalf("retry changed identity or failed to settle the exact stale tuple: producer=%+v calls=%d err=%v", waiter, waiterProvider.calls.Load(), err)
			}
			storage.mu.Lock()
			defer storage.mu.Unlock()
			if storage.writes != 1 || string(storage.payload) != "owner bytes" {
				t.Fatalf("stale waiter overwrote owner bytes: writes=%d payload=%q", storage.writes, storage.payload)
			}
		})
	}
}

func TestLogicalMutationForeignOwnerPendingAndCleanupFailuresStayGeneric(t *testing.T) {
	t.Run("unknown owner dispatch", func(t *testing.T) {
		fixture := newLogicalContentionFixture(t)
		storage := &logicalContentionStorage{generation: 14}
		unknownProvider := &logicalContentionProvider{storage: storage, unknown: true}
		ownerReceipts := logicalContentionReceipts(t, fixture.httpFixture, fixture.ownerBinding)
		ownerErr := fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.ownerBinding,
			fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent, fixture.scope, unknownProvider, ownerReceipts)
		if !errors.Is(ownerErr, ErrFencePending) {
			t.Fatalf("unknown owner dispatch error=%v, want generic pending", ownerErr)
		}
		ownerRun := producerRunForTerminalCleanup(t, fixture.httpFixture, fixture.identity)
		owner := logicalMutationAuthority(logicalManifestSTSUID, fixture.ownerBinding).mutationOwner(ownerRun,
			fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent)
		keyState, err := fixture.httpFixture.e.readMutationKeyState(t.Context(), fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
		if err != nil || keyState.Owner != owner {
			t.Fatalf("unknown owner dispatch lost its key: state=%+v owner=%q err=%v", keyState, owner, err)
		}
		waiterProvider := &logicalContentionProvider{storage: storage}
		waitErr := fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.waiterBind,
			fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, waiterProvider,
			logicalContentionReceipts(t, fixture.httpFixture, fixture.waiterBind))
		if !errors.Is(waitErr, fencing.ErrMutationPreDispatchContention) || waiterProvider.calls.Load() != 0 {
			t.Fatalf("pristine waiter did not receive safe retry signal without dispatch: err=%v calls=%d", waitErr, waiterProvider.calls.Load())
		}
		// The same owner tuple has already dispatched. Its retry is generic
		// uncertainty and may not be rewritten as foreign-owner contention.
		ownerErr = fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.ownerBinding,
			fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent, fixture.scope, unknownProvider, ownerReceipts)
		if !errors.Is(ownerErr, ErrFencePending) || errors.Is(ownerErr, fencing.ErrMutationPreDispatchContention) {
			t.Fatalf("owner's unknown dispatched retry was misclassified: %v", ownerErr)
		}
		keyState, err = fixture.httpFixture.e.readMutationKeyState(t.Context(), fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
		if err != nil || keyState.Owner != owner {
			t.Fatalf("waiter altered unresolved owner key: state=%+v owner=%q err=%v", keyState, owner, err)
		}
	})

	t.Run("joined terminal cleanup error", func(t *testing.T) {
		fixture := newLogicalContentionFixture(t)
		storage := &logicalContentionStorage{generation: 14}
		ownerProvider := &logicalContentionProvider{storage: storage}
		ownerReceipts := logicalContentionReceipts(t, fixture.httpFixture, fixture.ownerBinding)
		if err := fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.ownerBinding,
			fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent, fixture.scope, ownerProvider, ownerReceipts); err != nil {
			t.Fatalf("settle canonical owner before cleanup fault: %v", err)
		}
		ownerRun := producerRunForTerminalCleanup(t, fixture.httpFixture, fixture.identity)
		owner := logicalMutationAuthority(logicalManifestSTSUID, fixture.ownerBinding).mutationOwner(ownerRun,
			fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent)
		if err := fixture.httpFixture.e.acquireMutationKey(t.Context(), fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key, owner, false); err != nil {
			t.Fatalf("recreate only exact terminal owner key for the recovery fault: %v", err)
		}
		key, _ := fixture.httpFixture.e.mutationKeyIdentity(fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
		fault := &terminalOwnerClearFaultBucket{Bucket: fixture.httpFixture.state.bucket, key: key, remaining: 1}
		fixture.httpFixture.state.bucket = fault
		waiterProvider := &logicalContentionProvider{storage: storage}
		waitErr := fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.waiterBind,
			fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, waiterProvider,
			logicalContentionReceipts(t, fixture.httpFixture, fixture.waiterBind))
		if !errors.Is(waitErr, ErrFencePending) || errors.Is(waitErr, fencing.ErrMutationPreDispatchContention) || waiterProvider.calls.Load() != 0 {
			t.Fatalf("joined cleanup error was promoted or dispatched: err=%v calls=%d", waitErr, waiterProvider.calls.Load())
		}
		state, err := fixture.httpFixture.e.readMutationKeyState(t.Context(), fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
		if err != nil || state.Owner != owner || fault.remaining != 0 {
			t.Fatalf("cleanup failure changed exact predecessor owner: state=%+v owner=%q remaining=%d err=%v", state, owner, fault.remaining, err)
		}
	})
}

func TestLogicalMutationForeignOwnerSpecialKeyStatesStayGeneric(t *testing.T) {
	for _, mode := range []string{"pending-generation-zero", "barrier", "retirement"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newLogicalContentionFixture(t)
			ownerRun := producerRunForTerminalCleanup(t, fixture.httpFixture, fixture.identity)
			owner := logicalMutationAuthority(logicalManifestSTSUID, fixture.ownerBinding).mutationOwner(ownerRun,
				fixture.ownerBinding.ProducerUID, 1, fixture.ownerIntent)
			if err := fixture.httpFixture.e.acquireMutationKey(t.Context(), fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key, owner, true); err != nil {
				t.Fatal(err)
			}
			endpoint, identity := fixture.httpFixture.e.mutationKeyIdentity(fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
			data, version, err := fixture.httpFixture.state.read(t.Context(), endpoint, identity)
			if err != nil {
				t.Fatal(err)
			}
			var state mutationKeyState
			if err := decodeAuthorityValue(data, &state); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pending-generation-zero":
				state.PendingGenerationZero = &mutationGenerationZeroRight{}
			case "barrier":
				state.Barrier = &mutationGenerationZeroBarrier{}
			case "retirement":
				state.Retirement = &mutationGenerationZeroRetirementReference{}
			}
			if err := fixture.httpFixture.state.write(t.Context(), endpoint, identity, state, version); err != nil {
				t.Fatal(err)
			}
			provider := &logicalContentionProvider{storage: &logicalContentionStorage{generation: 14}}
			err = fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.waiterBind,
				fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, provider,
				logicalContentionReceipts(t, fixture.httpFixture, fixture.waiterBind))
			if !errors.Is(err, ErrFencePending) || errors.Is(err, fencing.ErrMutationPreDispatchContention) || provider.calls.Load() != 0 {
				t.Fatalf("special key state was promoted or dispatched: err=%v calls=%d", err, provider.calls.Load())
			}
		})
	}

	t.Run("malformed key record", func(t *testing.T) {
		fixture := newLogicalContentionFixture(t)
		endpoint, identity := fixture.httpFixture.e.mutationKeyIdentity(fixture.ownerBinding.StoreScopeHash, fixture.ownerIntent.Key)
		if err := fixture.httpFixture.state.bucket.Upload(t.Context(), endpoint, bytes.NewReader([]byte(`{"value":{"scopeHash":"partial"`))); err != nil {
			t.Fatal(err)
		}
		provider := &logicalContentionProvider{storage: &logicalContentionStorage{generation: 14}}
		err := fixture.httpFixture.e.ExecuteLogicalSourceMutation(t.Context(), logicalManifestSTSUID, fixture.waiterBind,
			fixture.waiterBind.ProducerUID, 1, fixture.waiterIntent, fixture.scope, provider,
			logicalContentionReceipts(t, fixture.httpFixture, fixture.waiterBind))
		if err == nil || errors.Is(err, fencing.ErrMutationPreDispatchContention) || provider.calls.Load() != 0 {
			t.Fatalf("partial key decode was promoted or dispatched: err=%v calls=%d identity=%s", err, provider.calls.Load(), identity)
		}
	})
}

func TestSourceMutationPendingReasonIsOnlySetForExactSubtype(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantValue string
	}{
		{name: "typed contention", err: fencing.ErrMutationPreDispatchContention, wantValue: fencing.MutationPreDispatchContentionReason},
		{name: "joined contention and cleanup error", err: errors.Join(fencing.ErrMutationPreDispatchContention, errors.New("cleanup failed"))},
		{name: "generic pending", err: ErrFencePending},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			(&FencingExecutor{}).writeSourceStageError(response, test.err)
			if response.Code != http.StatusAccepted {
				t.Fatalf("status=%d, want 202", response.Code)
			}
			if got := response.Header().Get(fencing.MutationPendingReasonHeader); got != test.wantValue {
				t.Fatalf("pending reason=%q, want %q", got, test.wantValue)
			}
		})
	}
}
