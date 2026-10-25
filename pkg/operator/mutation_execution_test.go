package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type generationZeroRetirementFaultBucket struct {
	objstore.Bucket
	proofKey          string
	stateKey          string
	proofResponseLost bool
	casResponseLost   bool
	failStateRead     bool
}

func (b *generationZeroRetirementFaultBucket) Upload(ctx context.Context, name string, input io.Reader, opts ...objstore.ObjectUploadOption) error {
	body, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(body), opts...); err != nil {
		return err
	}
	if name == b.proofKey && !b.proofResponseLost {
		b.proofResponseLost = true
		return errors.New("injected lost immutable retirement-proof response")
	}
	if name == b.stateKey && !b.casResponseLost && bytes.Contains(body, []byte(`"generationZeroRetirement"`)) {
		b.casResponseLost = true
		b.failStateRead = true
		return errors.New("injected lost generation-zero retirement CAS response")
	}
	return nil
}

func (b *generationZeroRetirementFaultBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.stateKey && b.failStateRead {
		b.failStateRead = false
		return nil, errors.New("injected retirement CAS readback failure")
	}
	return b.Bucket.Get(ctx, name)
}

type mutationExecutionTestProvider struct {
	payload              []byte
	result               mutationProviderResult
	delegate             *gcsMutationProvider
	observeCalls         int
	uploadCalls          int
	committedCalls       int
	uploadGenerations    []int64
	expectedGeneration   int64
	observedGeneration   int64
	observedObjectExists bool
	deleteCalls          int
	commitFound          bool
	uploadErr            error
	committedErr         error
}

func (p *mutationExecutionTestProvider) observe(context.Context, string) (int64, bool, error) {
	p.observeCalls++
	return p.observedGeneration, p.observedObjectExists, nil
}

func (p *mutationExecutionTestProvider) upload(ctx context.Context, key string, generation int64, intent fencing.MutationIntent, payload io.Reader) (mutationProviderResult, error) {
	p.uploadCalls++
	p.uploadGenerations = append(p.uploadGenerations, generation)
	if generation != p.expectedGeneration {
		return mutationProviderResult{}, fmt.Errorf("unexpected upload generation: got %d want %d", generation, p.expectedGeneration)
	}
	data, err := io.ReadAll(payload)
	if err != nil {
		return mutationProviderResult{}, err
	}
	p.payload = append([]byte(nil), data...)
	if p.uploadErr != nil {
		return mutationProviderResult{}, p.uploadErr
	}
	if p.delegate != nil {
		return p.delegate.upload(ctx, key, generation, intent, bytes.NewReader(data))
	}
	result := p.result
	if result.Outcome == fencing.MutationCommitted && result.PayloadDigest == "" {
		result.PayloadDigest = intent.PayloadDigest
	}
	return result, nil
}

func prepareGrantedUnsentHeldMutation(t *testing.T, f heldProfileFixture, intent fencing.MutationIntent, generation int64) {
	t.Helper()
	if generation <= 0 {
		t.Fatal("test requires a positive fixed generation")
	}
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatalf("failed to establish the original tuple's durable key lock: %v", err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: generation}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "original-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("close-held", hashBytes([]byte("close-held")))
	}); err != nil {
		t.Fatal(err)
	}
}

func testEngineRepairRecord(t *testing.T, f heldProfileFixture) fencing.EngineRepairRecord {
	t.Helper()
	state, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	old := state.Ledger.Runs[f.runtime.RunID].Identity
	incarnations, err := fencing.SortedRepairIncarnations(state.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "engine-repair-test", RequestHash: hashBytes([]byte("remove-request")), SourceClusterID: f.runtime.SourceClusterID,
		StatefulSetUID: f.runtime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 1, OldMemberID: old.NodeID,
		OldWALIdentity: old.WALIdentity, TargetMemberID: "replacement"}
	candidateWAL := hashBytes([]byte("replacement-wal"))
	candidate := fencing.Identity{SourceClusterID: old.SourceClusterID, NodeID: "replacement", PodName: "replacement-pod", PodUID: "replacement-uid",
		RunID: "replacement-run", WALIdentity: candidateWAL, AssignmentID: hashBytes([]byte("replacement-assignment"))}
	candidateRuntime := f.runtime
	candidateRuntime.NodeID, candidateRuntime.PodName, candidateRuntime.PodUID = candidate.NodeID, candidate.PodName, candidate.PodUID
	candidateRuntime.RunID, candidateRuntime.AssignmentID, candidateRuntime.ContainerID = candidate.RunID, candidate.AssignmentID, "replacement-container"
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(old.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", PublicKey: quepaxa.PublicKey{1}, WALIdentity: old.WALIdentity}
	survivorMember := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "survivor-wal"}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidate.NodeID), URL: "http://replacement:8080", PeerURL: "quic://replacement:9090", LogURL: "http://replacement:8081", PublicKey: quepaxa.PublicKey{3}, WALIdentity: candidate.WALIdentity}
	record := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding, Incarnations: incarnations,
		SourceConfig:          quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivorMember}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-admin", UID: "source-admin-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		CandidateIdentity:     candidate,
		CandidateMember:       candidateMember, CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember), RemoveRequestHash: binding.RequestHash, Phase: fencing.RepairCandidatePrepared}
	if err := record.ValidateAgainstLedger(state.Ledger); err != nil {
		t.Fatalf("test candidate-prepared repair reservation is invalid: %v", err)
	}
	store, err := NewEngineRepairStore(f.e.AuthorityState, binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateOrReadEngineRepair(t.Context(), record)
	if err != nil {
		t.Fatalf("persist canonical candidate-prepared reservation: %v", err)
	}
	pinned, err := store.PinCandidateRuntime(t.Context(), binding.SourceClusterID, binding.OperationID, created.Digest(), candidateRuntime)
	if err != nil {
		t.Fatalf("pin enrolled candidate runtime: %v", err)
	}
	frozen := freezeEngineRepairRecord(pinned)
	frozen.Phase = fencing.RepairRemoveFrozen
	frozen, err = store.CompareAndSwapEngineRepair(t.Context(), binding.SourceClusterID, pinned.Digest(), frozen)
	if err != nil {
		t.Fatalf("persist frozen exact REMOVE operation: %v", err)
	}
	closing := frozen
	closing.Phase = fencing.RepairStorageClosing
	closing, err = store.CompareAndSwapEngineRepair(t.Context(), binding.SourceClusterID, frozen.Digest(), closing)
	if err != nil {
		t.Fatalf("persist storage-closing phase: %v", err)
	}
	return closing
}

func noProducerEngineRepairFixture(t *testing.T) (*FencingExecutor, *EngineRepairStore, fencing.EngineRepairRecord, fencing.RuntimeBinding) {
	t.Helper()
	e, _, oldIdentity, oldRuntime := newMutationStoreFixture(t)
	oldIdentity.WALIdentity = "wal-no-producer"
	if _, err := e.change(t.Context(), oldRuntime.StatefulSetUID, oldRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.RequireMutationJournal = false
		if err := ledger.Bind(oldIdentity); err != nil {
			return err
		}
		survivorIdentity := fencing.Identity{SourceClusterID: oldIdentity.SourceClusterID, NodeID: "survivor", PodName: "survivor-0",
			PodUID: "survivor-pod", RunID: "survivor-run", WALIdentity: "survivor-wal", AssignmentID: hashBytes([]byte("survivor-assignment"))}
		survivorRuntime := oldRuntime
		survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorIdentity.NodeID, survivorIdentity.PodName, survivorIdentity.PodUID
		survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorIdentity.RunID, survivorIdentity.AssignmentID, "survivor-container"
		ledger.Runs[survivorIdentity.RunID] = fencing.Run{Identity: survivorIdentity, Role: fencing.StartupRoleVoterActive, Runtime: &survivorRuntime}
		ledger.RequireMutationJournal = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, _, err := e.AuthorityState.ReadRoot(t.Context(), oldRuntime.StatefulSetUID, oldRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fencing.SortedRepairIncarnations(state.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(oldIdentity.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081",
		WALIdentity: oldIdentity.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivor := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081",
		WALIdentity: "survivor-wal", PublicKey: quepaxa.PublicKey{2}}
	candidateWAL := hashBytes([]byte("candidate-wal"))
	candidate := fencing.Identity{SourceClusterID: oldIdentity.SourceClusterID, NodeID: "candidate", PodName: "candidate-0", PodUID: "candidate-pod",
		RunID: "candidate-run", WALIdentity: candidateWAL, AssignmentID: hashBytes([]byte("candidate-assignment"))}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidate.NodeID), URL: "http://candidate:8080", PeerURL: "quic://candidate:9090",
		LogURL: "http://candidate:8081", WALIdentity: candidate.WALIdentity, PublicKey: quepaxa.PublicKey{3}}
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "repair-no-producer", RequestHash: hashBytes([]byte("remove-no-producer")), SourceClusterID: oldIdentity.SourceClusterID,
		StatefulSetUID: oldRuntime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: oldIdentity.NodeID,
		OldWALIdentity: oldIdentity.WALIdentity, TargetMemberID: candidate.NodeID}
	repair := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding,
		SourceConfig: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivor}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid",
			AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations: incarnations, CandidateIdentity: candidate, CandidateMember: candidateMember,
		CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid",
			PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember), RemoveRequestHash: binding.RequestHash,
		Phase: fencing.RepairCandidatePrepared}
	store, err := NewEngineRepairStore(e.AuthorityState, binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateOrReadEngineRepair(t.Context(), repair)
	if err != nil {
		t.Fatal(err)
	}
	runtime := engineRepairStoreCandidateRuntime(created)
	pinned, err := store.PinCandidateRuntime(t.Context(), binding.SourceClusterID, binding.OperationID, created.Digest(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	frozen := freezeEngineRepairRecord(pinned)
	frozen.Phase = fencing.RepairRemoveFrozen
	frozen, err = store.CompareAndSwapEngineRepair(t.Context(), binding.SourceClusterID, pinned.Digest(), frozen)
	if err != nil {
		t.Fatal(err)
	}
	closing := frozen
	closing.Phase = fencing.RepairStorageClosing
	closing, err = store.CompareAndSwapEngineRepair(t.Context(), binding.SourceClusterID, frozen.Digest(), closing)
	if err != nil {
		t.Fatal(err)
	}
	e.SourceMutationStorage = &storage.Client{}
	if err := e.closeEngineRepairOldCohort(t.Context(), closing); err != nil {
		t.Fatal(err)
	}
	return e, store, closing, oldRuntime
}

type engineRepairFreezeVerifierFunc func(context.Context, fencing.RuntimeBinding, fencing.EngineRepairRecord) error

func (verify engineRepairFreezeVerifierFunc) VerifyEngineRemovalFreeze(ctx context.Context, runtime fencing.RuntimeBinding, repair fencing.EngineRepairRecord) error {
	return verify(ctx, runtime, repair)
}

func TestEngineRepairStorageEndpointReplaysStoredProofAfterSurvivorRestart(t *testing.T) {
	e, _, closing, baseRuntime := noProducerEngineRepairFixture(t)
	e.Kube = &Kubernetes{}
	caller := fencing.Identity{SourceClusterID: closing.Binding.SourceClusterID, NodeID: "survivor", PodName: "survivor-0",
		PodUID: "survivor-pod", RunID: "survivor-run", WALIdentity: "survivor-wal", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	callerRuntime := baseRuntime
	callerRuntime.NodeID, callerRuntime.PodName, callerRuntime.PodUID = caller.NodeID, caller.PodName, caller.PodUID
	callerRuntime.RunID, callerRuntime.AssignmentID, callerRuntime.ContainerID = caller.RunID, caller.AssignmentID, "survivor-container"
	verifyCalls := 0
	verifier := engineRepairFreezeVerifierFunc(func(_ context.Context, runtime fencing.RuntimeBinding, repair fencing.EngineRepairRecord) error {
		verifyCalls++
		if runtime != engineRepairStoreCandidateRuntime(repair) {
			return fmt.Errorf("freeze verifier received a runtime other than the pinned candidate")
		}
		return nil
	})

	first, err := e.executeEngineRepairStorageEffectsWithVerifier(t.Context(), caller, callerRuntime, closing.Binding.OperationID, verifier)
	if err != nil {
		t.Fatalf("public storage-closure endpoint: %v", err)
	}
	if len(first.RunSettlements) != 1 || first.RunSettlements[0].NoProducerDigest == "" {
		t.Fatalf("public endpoint did not return the exact no-producer proof: %+v", first)
	}
	if verifyCalls != 1 {
		t.Fatalf("public endpoint skipped its authenticated freeze-verifier seam: calls=%d", verifyCalls)
	}
	// Simulate losing the endpoint response after its proof digest was persisted
	// in the canonical repair root. A normal same-survivor restart changes only
	// the runtime/run identity, not the immutable source voter binding.

	current, version, err := e.AuthorityState.ReadRoot(t.Context(), closing.Binding.StatefulSetUID, closing.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	nextCaller := caller
	nextCaller.RunID = "survivor-run-2"
	nextRuntime := callerRuntime
	nextRuntime.RunID, nextRuntime.ContainerID = nextCaller.RunID, "survivor-container-2"
	current.Ledger.Runs[nextCaller.RunID] = fencing.Run{Identity: nextCaller, Role: fencing.StartupRoleVoterActive, Runtime: &nextRuntime}
	if err := e.AuthorityState.WriteRoot(t.Context(), closing.Binding.StatefulSetUID, closing.Binding.SourceClusterID, current, version); err != nil {
		t.Fatalf("append fresh runtime for the same original survivor: %v", err)
	}

	replayed, err := e.executeEngineRepairStorageEffectsWithVerifier(t.Context(), nextCaller, nextRuntime, closing.Binding.OperationID, verifier)
	if err != nil || replayed.ProofDigest != first.ProofDigest || replayed.Digest() != first.Digest() {
		t.Fatalf("public endpoint did not return the same root-pinned proof after survivor restart: first=%+v replayed=%+v err=%v", first, replayed, err)
	}
	if verifyCalls != 2 {
		t.Fatalf("public retry skipped its authenticated freeze-verifier seam: calls=%d", verifyCalls)
	}
	root := mustReadRoot(t, e, closing)
	canonical, err := readEngineRepairStorageRecordByOperation(t.Context(), mustEngineRepairStore(t, e, closing), root, closing.Binding.SourceClusterID, closing.Binding.OperationID)
	if err != nil || canonical.StorageProofDigest != first.ProofDigest {
		t.Fatalf("canonical repair root lost the immutable proof reference: proof=%q record=%+v err=%v", first.ProofDigest, canonical, err)
	}
}

func mustEngineRepairStore(t *testing.T, e *FencingExecutor, repair fencing.EngineRepairRecord) *EngineRepairStore {
	t.Helper()
	store, err := NewEngineRepairStore(e.AuthorityState, repair.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestEngineRepairNoProducerProofRetryAfterRootCASAndUnrelatedSurvivorRun(t *testing.T) {
	e, store, closing, oldRuntime := noProducerEngineRepairFixture(t)
	first, err := e.settleEngineRepairStorageEffects(t.Context(), closing)
	if err != nil {
		t.Fatalf("commit storage proof before simulated lost caller response: %v", err)
	}
	if len(first.RunSettlements) != 1 || first.RunSettlements[0].ProducerUID != "" || first.RunSettlements[0].NoProducerDigest == "" {
		t.Fatalf("fixture did not produce an exact no-producer settlement: %+v", first.RunSettlements)
	}
	// The caller loses the first response after the root CAS. Keep the local
	// value only as a test oracle, then retry from the pre-CAS request after an
	// unrelated original survivor has registered a fresh run.

	var originalSurvivor fencing.Identity
	for _, incarnation := range closing.Incarnations {
		if incarnation.Identity.NodeID == "survivor" {
			originalSurvivor = incarnation.Identity
			break
		}
	}
	if originalSurvivor.RunID == "" {
		t.Fatal("fixture omitted the original survivor incarnation")
	}
	survivorIdentity := originalSurvivor
	survivorIdentity.RunID = "survivor-run-2"
	survivorRuntime := oldRuntime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorIdentity.NodeID, survivorIdentity.PodName, survivorIdentity.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorIdentity.RunID, survivorIdentity.AssignmentID, "survivor-container-2"
	state, version, err := e.AuthorityState.ReadRoot(t.Context(), closing.Binding.StatefulSetUID, closing.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	state.Ledger.Runs[survivorIdentity.RunID] = fencing.Run{Identity: survivorIdentity, Role: fencing.StartupRoleVoterActive, Runtime: &survivorRuntime}
	if err := e.AuthorityState.WriteRoot(t.Context(), closing.Binding.StatefulSetUID, closing.Binding.SourceClusterID, state, version); err != nil {
		t.Fatalf("append unrelated active survivor run: %v", err)
	}
	oldRun := state.Ledger.Runs[closing.Incarnations[0].Identity.RunID]
	stableSettlement, err := engineRepairNoProducerSettlement(closing, state.Ledger, closing.Incarnations[0], oldRun)
	if err != nil || stableSettlement != first.RunSettlements[0] {
		t.Fatalf("unrelated survivor run changed the no-producer projection: before=%+v after=%+v err=%v", first.RunSettlements[0], stableSettlement, err)
	}

	replayed, err := e.settleEngineRepairStorageEffects(t.Context(), closing)
	if err != nil || replayed.ProofDigest != first.ProofDigest || replayed.Digest() != first.Digest() {
		t.Fatalf("retry did not return the same root-pinned proof after the unrelated run append: first=%+v replayed=%+v err=%v", first, replayed, err)
	}
	canonical, err := readEngineRepairStorageRecordByOperation(t.Context(), store, mustReadRoot(t, e, closing), closing.Binding.SourceClusterID, closing.Binding.OperationID)
	if err != nil || canonical.StorageProofDigest != first.ProofDigest {
		t.Fatalf("canonical repair record lost its proof reference: record=%+v err=%v", canonical, err)
	}

	badLedger := state.Ledger
	badLedger.ClosedBy, badLedger.ClosedHash = "dr-close", hashBytes([]byte("dr-close"))
	if err := validateEngineRepairStorageInventory(canonical, badLedger); err == nil {
		t.Fatal("storage proof accepted a generation-wide DR-closed root")
	}
	unrecordedOld := state.Ledger
	unrecordedOld.Runs["old-run-unrecorded"] = fencing.Run{Identity: fencing.Identity{
		SourceClusterID: closing.Binding.SourceClusterID, NodeID: closing.Binding.OldMemberID, PodUID: "old-pod-unrecorded",
		RunID: "old-run-unrecorded", WALIdentity: closing.Binding.OldWALIdentity,
	}}
	if err := validateEngineRepairStorageInventory(canonical, unrecordedOld); err == nil {
		t.Fatal("storage proof accepted an unrecorded old-member incarnation")
	}
	oldRun = state.Ledger.Runs[closing.Incarnations[0].Identity.RunID]
	if oldRun.Stop == nil {
		t.Fatal("fixture lost the canonical old-member stop")
	}
	oldRun.Stop.RequestHash = hashBytes([]byte("stale-stop"))
	if _, err := engineRepairNoProducerSettlement(canonical, state.Ledger, closing.Incarnations[0], oldRun); err == nil {
		t.Fatal("no-producer proof accepted a changed old-member stop")
	}
}

func mustReadRoot(t *testing.T, e *FencingExecutor, repair fencing.EngineRepairRecord) fenceLedger {
	t.Helper()
	state, err := e.read(t.Context(), repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestClosedNonzeroDispatchSettlesOnceFromRemoteSealedPayload(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		result                mutationProviderResult
		observedGeneration    int64
		observedExists        bool
		wantOutcome           fencing.MutationOutcome
		wantConditionFailed   bool
		wantAllCopiesTerminal bool
	}{
		{name: "actual commit", result: mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 38, PayloadDigest: hashBytes([]byte(`{"membership":"settled bytes"}`))}, wantOutcome: fencing.MutationCommitted, wantAllCopiesTerminal: true},
		{name: "future fixed-generation effect excluded", result: mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true, Generation: 37, AllCopiesTerminal: true}, observedGeneration: 38, observedExists: true,
			wantOutcome: fencing.MutationRejected, wantConditionFailed: true, wantAllCopiesTerminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHeldProfileFixture(t)
			const originalGeneration int64 = 37
			intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: originalGeneration}, []byte(`{"membership":"settled bytes"}`))
			prepareGrantedUnsentHeldMutation(t, f, intent, originalGeneration)
			// Canonical authority selects remote staging; settlement must not rely on
			// the source Pod's emptyDir/local stage copy.
			f.e.SourceStages = nil
			gcsState, client := newHeldGCSServer(t, f)
			f.e.SourceMutationStorage = client
			provider := &mutationExecutionTestProvider{expectedGeneration: originalGeneration, result: tc.result,
				observedGeneration: tc.observedGeneration, observedObjectExists: tc.observedExists}
			if tc.result.Outcome == fencing.MutationCommitted {
				const objectName = "root/source/membership/current"
				gcsState.mu.Lock()
				gcsState.nextGeneration = originalGeneration
				gcsState.loseCreate = false
				gcsState.objects[objectName] = heldGCSObject{name: objectName, generation: originalGeneration, metageneration: 1,
					payload: []byte(`{"membership":"before mutation"}`), metadata: map[string]string{}}
				gcsState.mu.Unlock()
				actualProvider, err := newGCSHeldCreateMutationProvider(client, f.scope)
				if err != nil {
					t.Fatal(err)
				}
				provider.delegate = actualProvider
			}
			repair := testEngineRepairRecord(t, f)
			if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); err != nil {
				t.Fatalf("closed fixed-generation dispatch did not settle from its canonical remote payload: %v", err)
			}
			if provider.uploadCalls != 1 || len(provider.uploadGenerations) != 1 || provider.uploadGenerations[0] != originalGeneration || !bytes.Equal(provider.payload, []byte(`{"membership":"settled bytes"}`)) {
				t.Fatalf("settlement changed the original generation or payload: calls=%d generations=%v payload=%q", provider.uploadCalls, provider.uploadGenerations, provider.payload)
			}
			producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer {
				t.Fatalf("settlement producer readback failed: uid=%q err=%v", uid, err)
			}
			entry := producer.Entries[1]
			if entry.Resolution == nil || entry.Resolution.Outcome != tc.wantOutcome || entry.Resolution.ConditionFailed != tc.wantConditionFailed ||
				entry.Resolution.AllCopiesTerminal != tc.wantAllCopiesTerminal {
				t.Fatalf("settlement outcome or dispatch terminality changed: %+v", entry)
			}
			for _, dispatch := range entry.Dispatches {
				if tc.wantAllCopiesTerminal != (dispatch.ReceiptDigest != "") {
					t.Fatalf("settlement dispatch receipt terminality mismatch: %+v", entry.Dispatches)
				}
			}
			if err := f.e.VerifySourceMutationEvidence(t.Context(), f.runtime, 1, entry); err != nil {
				t.Fatalf("remote settlement authorization/result did not verify from canonical evidence: %v", err)
			}
			lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
			if err != nil || lock.Owner != "" {
				t.Fatalf("settled fixed-generation key remained locked: %+v err=%v", lock, err)
			}
			if provider.uploadCalls != 1 {
				t.Fatalf("resumed original tuple issued another successful-effect opportunity: calls=%d", provider.uploadCalls)
			}
			// A retry after the producer CAS and original unlock may observe a
			// lost unlock response must release only the original exact owner.
			originalOwner := hashJSON([]any{f.runtime, "wal-held-profile", f.producer, uint64(1), intent.OperationID})
			if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, originalOwner, false); err != nil {
				t.Fatal(err)
			}
			if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); err != nil {
				t.Fatalf("resolved settlement did not resume its lost unlock: %v", err)
			}
			lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
			if err != nil || lock.Owner != "" {
				t.Fatalf("resolved retry did not release the original owner: %+v err=%v", lock, err)
			}
			// A later retry may then observe a successor owner. It must return
			// the saved settlement without clearing that owner's lock.
			nextOwner := hashJSON([]any{"successor", f.runtime, intent.Key})
			if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner, false); err != nil {
				t.Fatal(err)
			}
			if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); err != nil {
				t.Fatalf("resolved settlement could not resume after unlock: %v", err)
			}
			lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
			if err != nil || lock.Owner != nextOwner || provider.uploadCalls != 1 {
				t.Fatalf("resolved retry changed successor ownership or repeated provider work: lock=%+v calls=%d err=%v", lock, provider.uploadCalls, err)
			}
			if err := f.e.releaseMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEngineRepairResumesTerminalRejectedReceiptAcrossJournalAndSettlementGaps(t *testing.T) {
	for _, journalResolved := range []bool{false, true} {
		name := "receipt before journal CAS"
		if journalResolved {
			name = "journal before settlement"
		}
		t.Run(name, func(t *testing.T) {
			f := newHeldProfileFixture(t)
			payload := []byte(`{"membership":"rejected original tuple"}`)
			intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 37}, payload)
			prepareGrantedUnsentHeldMutation(t, f, intent, 37)
			run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer {
				t.Fatalf("producer readback failed: uid=%q err=%v", uid, err)
			}
			entry := producer.Entries[1]
			dispatchID := "original-dispatch"
			receipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer, Sequence: 1,
				Intent: intent, Preparation: *entry.Preparation, DispatchID: dispatchID, Outcome: fencing.MutationRejected, ConditionFailed: true,
				Generation: entry.Preparation.Generation, AllCopiesTerminal: true}
			receipt.ReceiptDigest = receipt.digest()
			if digest, err := f.receipts.put(receipt); err != nil || digest != receipt.ReceiptDigest {
				t.Fatalf("could not persist exact terminal 412 receipt: digest=%q err=%v", digest, err)
			}
			if journalResolved {
				if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
					preparationHash, err := p.PreparationHash(1)
					if err != nil {
						return err
					}
					if err := p.RecordDispatchTerminal(1, dispatchID, receipt.ReceiptDigest); err != nil {
						return err
					}
					return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationRejected, PreparationHash: preparationHash,
						ReceiptDigest: receipt.ReceiptDigest, ConditionFailed: true, AllCopiesTerminal: true})
				}); err != nil {
					t.Fatalf("could not establish journal-CAS-complete crash state: %v", err)
				}
			}
			_, client := newHeldGCSServer(t, f)
			f.e.SourceMutationStorage = client
			f.e.SourceStages = nil
			provider := &mutationExecutionTestProvider{expectedGeneration: receipt.Generation, result: mutationProviderResult{Outcome: fencing.MutationRejected,
				ConditionFailed: true, Generation: receipt.Generation, AllCopiesTerminal: true}, observedGeneration: receipt.Generation + 1, observedObjectExists: true}
			repair := testEngineRepairRecord(t, f)
			settlement, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider)
			if err != nil {
				t.Fatalf("exact terminal rejection did not resume to immutable settlement: %v", err)
			}
			if provider.uploadCalls != 0 || provider.committedCalls == 0 || provider.observeCalls == 0 || settlement.SettledThrough != 1 {
				t.Fatalf("rejected recovery repeated provider work or skipped exclusion proof: uploads=%d committed=%d observe=%d settlement=%+v",
					provider.uploadCalls, provider.committedCalls, provider.observeCalls, settlement)
			}
			producer, uid, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer {
				t.Fatalf("settled producer readback failed: uid=%q err=%v", uid, err)
			}
			entry = producer.Entries[1]
			dispatch := entry.Dispatches[dispatchID]
			if entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.Resolution.ConditionFailed ||
				!entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != "" || entry.Resolution.ReceiptDigest != receipt.ReceiptDigest ||
				dispatch.ReceiptDigest != receipt.ReceiptDigest {
				t.Fatalf("rejection was not preserved as the exact terminal producer outcome: entry=%+v", entry)
			}
			if err := f.e.VerifySourceMutationEvidence(t.Context(), f.runtime, 1, entry); err != nil {
				t.Fatalf("terminal rejection receipt did not verify against the journal: %v", err)
			}
			store, err := f.e.authorityEvidenceFor(f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			settlementKey := engineRepairMutationSettlementKey(repair.IdentityDigest(), f.runtime, f.producer, 1)
			if _, err := f.e.readEngineRepairRunSettlement(t.Context(), store, producer, f.producer, 1, settlementKey, repair.IdentityDigest()); err != nil {
				t.Fatalf("immutable rejected settlement did not survive exact readback: %v", err)
			}
			savedReader, _, err := store.Read(t.Context(), settlementKey)
			if err != nil {
				t.Fatal(err)
			}
			var saved engineRepairMutationSettlement
			data, readErr := io.ReadAll(savedReader)
			if err := errors.Join(readErr, savedReader.Close()); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &saved); err != nil || saved.Outcome != fencing.MutationRejected || !saved.ConditionFailed ||
				!saved.AllCopiesTerminal || saved.ActualCommitProven || !saved.FutureEffectsExcluded || saved.PayloadDigest != "" ||
				saved.ReceiptDigest != receipt.ReceiptDigest {
				t.Fatalf("saved settlement changed rejection into commit/exclusion: %+v err=%v", saved, err)
			}
		})
	}
}

func TestEngineRepairRejectedReceiptDoesNotSettleWithoutExactTerminalProof(t *testing.T) {
	for _, tc := range []struct {
		name               string
		clearTerminalFlag  bool
		observedGeneration int64
	}{
		{name: "receipt lacks terminal-copy proof", clearTerminalFlag: true, observedGeneration: 38},
		{name: "selected generation remains live", observedGeneration: 37},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHeldProfileFixture(t)
			intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 37}, []byte(`{"membership":"reject safely"}`))
			prepareGrantedUnsentHeldMutation(t, f, intent, 37)
			run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			preparation := *producer.Entries[1].Preparation
			receipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer, Sequence: 1,
				Intent: intent, Preparation: preparation, DispatchID: "original-dispatch", Outcome: fencing.MutationRejected, ConditionFailed: true,
				Generation: preparation.Generation, AllCopiesTerminal: !tc.clearTerminalFlag}
			receipt.ReceiptDigest = receipt.digest()
			if _, err := f.receipts.put(receipt); err != nil {
				t.Fatal(err)
			}
			_, client := newHeldGCSServer(t, f)
			f.e.SourceMutationStorage = client
			provider := &mutationExecutionTestProvider{expectedGeneration: preparation.Generation, result: mutationProviderResult{Outcome: fencing.MutationRejected,
				ConditionFailed: true, Generation: preparation.Generation, AllCopiesTerminal: true}, observedGeneration: tc.observedGeneration, observedObjectExists: true}
			repair := testEngineRepairRecord(t, f)
			if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); !errors.Is(err, ErrFencePending) {
				t.Fatalf("unproven rejection did not remain pending: err=%v", err)
			}
			producer, _, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || producer.Entries[1].Resolution != nil || provider.uploadCalls != 0 {
				t.Fatalf("unproven rejection changed the journal or repeated work: resolution=%+v uploads=%d err=%v",
					producer.Entries[1].Resolution, provider.uploadCalls, err)
			}
		})
	}
}

func TestEngineRepairCancelsClosedMutationWithoutDispatch(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := "unprepared"
		if prepared {
			name = "prepared"
		}
		t.Run(name, func(t *testing.T) {
			f := newHeldProfileFixture(t)
			intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 37}, []byte(`{"membership":"no dispatch"}`))
			if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
				if prepared {
					if err := p.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
						GenerationSet: true, Generation: 37}); err != nil {
						return err
					}
				}
				return p.Close("close-before-dispatch", hashBytes([]byte("close-before-dispatch")))
			}); err != nil {
				t.Fatal(err)
			}
			run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.e.settleEngineRepairProducer(t.Context(), fencing.EngineRepairRecord{}, run, f.runtime); err != nil {
				t.Fatalf("closed entry without dispatch did not settle by cancellation: %v", err)
			}
			producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer || !producer.BarrierComplete() {
				t.Fatalf("closed producer barrier did not read back: uid=%q err=%v producer=%+v", uid, err, producer)
			}
			entry := producer.Entries[1]
			if !isEngineRepairNoDispatchCancellation(f.runtime, producer.WALIdentity, f.producer, 1, entry) {
				t.Fatalf("closed no-dispatch cancellation did not bind the exact tuple: %+v", entry)
			}
			if err := f.e.VerifySourceMutationEvidence(t.Context(), f.runtime, 1, entry); err != nil {
				t.Fatalf("closed no-dispatch cancellation did not verify: %v", err)
			}
		})
	}
}

func TestEngineRepairResumesPersistedReceiptBeforeJournalCAS(t *testing.T) {
	f := newHeldProfileFixture(t)
	payload := []byte(`{"membership":"receipt already durable"}`)
	intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 37}, payload)
	prepareGrantedUnsentHeldMutation(t, f, intent, 37)
	receipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: "wal-held-profile", ProducerUID: f.producer, Sequence: 1,
		Intent: intent, Preparation: fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 37},
		DispatchID: "original-dispatch", Outcome: fencing.MutationCommitted, Generation: 38, Metageneration: 2, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}
	receipt.ReceiptDigest = receipt.digest()
	if digest, err := f.receipts.put(receipt); err != nil || digest != receipt.ReceiptDigest {
		t.Fatalf("could not establish response-lost durable receipt: digest=%q err=%v", digest, err)
	}
	_, client := newHeldGCSServer(t, f)
	f.e.SourceMutationStorage = client
	f.e.SourceStages = nil // recovery must use the receipt, not the removed local stage
	provider := &mutationExecutionTestProvider{commitFound: true, payload: payload, result: mutationProviderResult{Outcome: fencing.MutationCommitted,
		Generation: receipt.Generation, Metageneration: receipt.Metageneration, PayloadDigest: receipt.PayloadDigest, AllCopiesTerminal: true}}
	repair := testEngineRepairRecord(t, f)
	if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); err != nil {
		t.Fatalf("settlement did not resume the persisted provider receipt: %v", err)
	}
	if provider.uploadCalls != 0 || provider.committedCalls != 1 {
		t.Fatalf("receipt recovery repeated or skipped provider verification: uploads=%d committed=%d", provider.uploadCalls, provider.committedCalls)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("receipt-bound journal readback failed: uid=%q err=%v", uid, err)
	}
	entry := producer.Entries[1]
	dispatch, dispatchExists := entry.Dispatches[receipt.DispatchID]
	if entry.Resolution == nil || entry.Resolution.ReceiptDigest != receipt.ReceiptDigest ||
		!entry.Resolution.AllCopiesTerminal || len(entry.Dispatches) != 1 ||
		!dispatchExists || dispatch.ReceiptDigest != receipt.ReceiptDigest {
		t.Fatalf("receipt-bound journal CAS did not read back: uid=%q entry=%+v", uid, entry)
	}
	if err := f.e.VerifySourceMutationEvidence(t.Context(), f.runtime, 1, entry); err != nil {
		t.Fatalf("terminal receipt recovery did not verify against the canonical journal: %v", err)
	}
}

func TestEngineRepairProducerRetryReleasesResolvedDispatchOwner(t *testing.T) {
	f := newHeldProfileFixture(t)
	payload := []byte(`{"membership":"terminal receipt"}`)
	intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 37}, payload)
	prepareGrantedUnsentHeldMutation(t, f, intent, 37)
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	repair := testEngineRepairRecord(t, f)
	_, client := newHeldGCSServer(t, f)
	f.e.SourceMutationStorage = client
	provider := &mutationExecutionTestProvider{expectedGeneration: 37, result: mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 38,
		Metageneration: 2, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}}
	if _, err := f.e.settleEngineRepairMutation(t.Context(), repair, f.runtime, f.producer, 1, f.scope, provider); err != nil {
		t.Fatalf("initial closed dispatch settlement failed: %v", err)
	}
	if provider.uploadCalls != 1 {
		t.Fatalf("initial settlement provider call count = %d, want 1", provider.uploadCalls)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, false); err != nil {
		t.Fatalf("could not model a persisted original key owner after lost unlock response: %v", err)
	}
	if err := f.e.settleEngineRepairProducer(t.Context(), repair, run, f.runtime); err != nil {
		t.Fatalf("outer producer recovery did not release the resolved dispatch owner: %v", err)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != "" || provider.uploadCalls != 1 {
		t.Fatalf("resolved producer retry failed to release only its old owner: lock=%+v uploads=%d err=%v", lock, provider.uploadCalls, err)
	}
	successor := hashJSON([]any{"successor", f.runtime, intent.Key})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, successor, false); err != nil {
		t.Fatal(err)
	}
	if err := f.e.settleEngineRepairProducer(t.Context(), repair, run, f.runtime); err != nil {
		t.Fatalf("outer retry after terminal settlement failed: %v", err)
	}
	lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != successor || provider.uploadCalls != 1 {
		t.Fatalf("outer retry cleared a successor owner or repeated provider work: lock=%+v uploads=%d err=%v", lock, provider.uploadCalls, err)
	}
	if err := f.e.releaseMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, successor); err != nil {
		t.Fatal(err)
	}
}

func TestEngineRepairZeroGenerationSettlementReleasesKeyAfterGuardReadback(t *testing.T) {
	e, api, id, runtime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	preparePendingArchiveGenesis(t, e, api, id, runtime)
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatalf("reserve original producer before WAL binding: %v", err)
	}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatalf("open original producer staging store: %v", err)
	}
	id.WALIdentity = "wal-held-profile"
	configureMutationWorkloadFixture(t, e, api, id, runtime)
	if _, err := e.bindWALAtScopeWithStage(t.Context(), runtime.StatefulSetUID, id, hashJSON(scope), stages.incarnation); err != nil {
		t.Fatalf("bind and activate original producer after archive registration: %v", err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatalf("read original enrolled producer: %v", err)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatalf("open original producer receipt store: %v", err)
	}
	f := heldProfileFixture{e: e, api: api, id: id, runtime: runtime, producer: run.MutationProducerUID,
		scope: scope, stages: stages, receipts: receipts}
	survivorID := fencing.Identity{SourceClusterID: f.id.SourceClusterID, NodeID: "survivor", PodName: "survivor-0", PodUID: "survivor-pod",
		RunID: "survivor-run", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	survivorRuntime := f.runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorID.RunID, survivorID.AssignmentID, "survivor-container"
	if _, err := f.e.change(t.Context(), survivorRuntime.StatefulSetUID, survivorRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("register canonical survivor before the repair snapshot: %v", err)
	}
	if _, err := f.e.reserveMutationProducer(t.Context(), survivorRuntime); err != nil {
		t.Fatalf("reserve canonical survivor producer before WAL binding: %v", err)
	}
	survivorStages, err := f.e.sourceStagesFor(t.Context(), survivorRuntime)
	if err != nil {
		t.Fatalf("open canonical survivor staging store: %v", err)
	}
	survivorID.WALIdentity = "survivor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, survivorID, survivorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(t.Context(), survivorRuntime.StatefulSetUID, survivorID, hashJSON(f.scope), survivorStages.incarnation); err != nil {
		t.Fatalf("bind and activate canonical survivor producer before the repair snapshot: %v", err)
	}
	payload := []byte(`{"membership":"held bootstrap"}`)
	intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	deleteIntent := f.admitDelete(t, 2, intent.Key, 1)
	updatePayload := []byte(`{"membership":"successor generation"}`)
	recreatePayload := []byte(`{"membership":"recreated successor"}`)
	oldGenerationZeroRecreate := f.admitUpload(t, 3, intent.Key, fencing.MutationCondition{Mode: fencing.MutationUnconditional}, recreatePayload)
	run, err = f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "original-zero-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		if err := p.Prepare(2, fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
			GenerationSet: true, Generation: 1}); err != nil {
			return err
		}
		if err := p.Prepare(3, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(3, "recreate-zero-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(3); err != nil {
			return err
		}
		return p.Close("close-zero", hashBytes([]byte("close-zero")))
	}); err != nil {
		t.Fatal(err)
	}
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	server.loseCreate = false
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	originalProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	// Capture a genuine successful response from the original dispatch before
	// the recovery helper retries the exact create-only tuple. Persist its
	// receipt only after recovery to model the original response arriving late;
	// the helper's separate 412/attribute-read receipt is never used for the
	// original dispatch.
	originalResult, err := originalProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload))
	if err != nil || originalResult.Outcome != fencing.MutationCommitted || !originalResult.AllCopiesTerminal ||
		originalResult.Generation <= 0 || originalResult.Metageneration <= 0 || originalResult.PayloadDigest != intent.PayloadDigest {
		t.Fatalf("original dispatch did not return its exact terminal create receipt: result=%+v err=%v", originalResult, err)
	}
	originalReceipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: "original-zero-dispatch", Outcome: originalResult.Outcome,
		ConditionFailed: originalResult.ConditionFailed, Generation: originalResult.Generation, Metageneration: originalResult.Metageneration,
		PayloadDigest: originalResult.PayloadDigest, AllCopiesTerminal: originalResult.AllCopiesTerminal}
	originalReceipt.ReceiptDigest = originalReceipt.digest()
	repair := testEngineRepairRecord(t, f)
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("zero-generation dispatch did not settle: %v", err)
	}
	if guard.ObjectGeneration != originalResult.Generation || guard.ObjectMetageneration != originalResult.Metageneration {
		t.Fatalf("settlement omitted exact guarded generation: %+v", guard)
	}
	if digest, err := f.receipts.put(originalReceipt); err != nil || digest != originalReceipt.ReceiptDigest {
		t.Fatalf("could not persist the original dispatch's own terminal receipt: digest=%q err=%v", digest, err)
	}
	storedOriginalReceipt, err := f.receipts.get(mutationExecutionReceiptName(originalReceipt))
	if err != nil || hashJSON(storedOriginalReceipt) != hashJSON(originalReceipt) ||
		storedOriginalReceipt.DispatchID != "original-zero-dispatch" || !storedOriginalReceipt.AllCopiesTerminal {
		t.Fatalf("original dispatch receipt readback lost its exact terminal binding: receipt=%+v err=%v", storedOriginalReceipt, err)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != "" {
		t.Fatalf("verified zero-generation settlement retained the key lock: %+v err=%v", lock, err)
	}

	// The original create-only right remains harmless after the lock is released.
	oldProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	oldResult, err := oldProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload))
	if err != nil || oldResult.Outcome != fencing.MutationRejected || !oldResult.ConditionFailed {
		t.Fatalf("stale generation-zero create was not denied by its original precondition: result=%+v err=%v", oldResult, err)
	}
	if guard.ObjectGeneration != 1 || deleteIntent.Condition.Generation != guard.ObjectGeneration {
		t.Fatalf("fixture delete does not name the exact guarded generation: guard=%+v delete=%+v", guard, deleteIntent.Condition)
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 2, deleteIntent, f.scope, oldProvider, f.receipts); !errors.Is(err, fencing.ErrMutationConditionFailed) {
		t.Fatalf("verified current-generation delete was not terminally cancelled: %v", err)
	}
	producerAfterDelete, uidAfterDelete, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	deletedEntry := producerAfterDelete.Entries[2]
	if err != nil || uidAfterDelete != f.producer || deletedEntry.Resolution == nil || deletedEntry.Resolution.Outcome != fencing.MutationRejected ||
		deletedEntry.Resolution.Reason != fencing.MutationResolutionReasonGenerationZeroPinned || !deletedEntry.Resolution.AllCopiesTerminal || len(deletedEntry.Dispatches) != 0 ||
		!isEngineRepairNoDispatchCancellation(f.runtime, producerAfterDelete.WALIdentity, f.producer, 2, deletedEntry) {
		t.Fatalf("verified pin cancellation was not durably terminal: uid=%q entry=%+v err=%v", uidAfterDelete, deletedEntry, err)
	}
	// Continue the exact pre-existing survivor under a fresh run/container. The
	// repair inventory permits this same logical voter restart but not a new
	// old-member incarnation.
	successorID := survivorID
	successorID.RunID = "survivor-run-restarted"
	successorID.WALIdentity = ""
	successorRuntime := survivorRuntime
	successorRuntime.RunID, successorRuntime.ContainerID = successorID.RunID, "survivor-container-restarted"
	if _, err := f.e.change(t.Context(), successorRuntime.StatefulSetUID, successorRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(successorID, successorRuntime)
	}); err != nil {
		t.Fatalf("register open successor runtime: %v", err)
	}
	if _, err := f.e.reserveMutationProducer(t.Context(), successorRuntime); err != nil {
		t.Fatalf("reserve open successor producer: %v", err)
	}
	successorStages, err := f.e.sourceStagesFor(t.Context(), successorRuntime)
	if err != nil {
		t.Fatalf("open successor staging store: %v", err)
	}
	successorID.WALIdentity = "survivor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, successorID, successorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(t.Context(), successorRuntime.StatefulSetUID, successorID, hashJSON(f.scope), successorStages.incarnation); err != nil {
		t.Fatalf("bind and activate successor producer: %v", err)
	}
	successorReceipts, err := f.e.receiptsForRuntime(successorRuntime)
	if err != nil {
		t.Fatalf("open successor receipt store: %v", err)
	}
	successor := f
	successor.id, successor.runtime, successor.producer = successorID, successorRuntime, stableMutationProducerUID(successorRuntime)
	successor.stages, successor.receipts = successorStages, successorReceipts
	updateIntent := successor.admitUpload(t, 1, intent.Key, fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 1}, updatePayload)
	successorProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), successorRuntime, successor.producer, 1, updateIntent, f.scope, successorProvider, successorReceipts); err != nil {
		keyState, stateErr := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
		var barrier any
		if keyState.Barrier != nil {
			barrier = struct {
				Generation, Metageneration int64
				OperationID, PayloadDigest string
				InitialHoldReleased        bool
				LineageRef, LineageDigest  string
			}{keyState.Barrier.CurrentGeneration, keyState.Barrier.CurrentMetageneration, keyState.Barrier.CurrentOperationID,
				keyState.Barrier.CurrentPayloadDigest, keyState.Barrier.InitialHoldReleased, keyState.Barrier.LineageRef, keyState.Barrier.LineageDigest}
		}
		t.Fatalf("successor positive-generation update did not advance the pinned lineage: %v; fake GCS: %s; barrier=%+v lockOwner=%q stateErr=%v",
			err, server.diagnostic(pathJoin(f.scope.Prefix, intent.Key)), barrier, keyState.Owner, stateErr)
	}
	lineageState, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || lineageState.Barrier == nil || lineageState.Barrier.CurrentGeneration <= guard.ObjectGeneration ||
		lineageState.Barrier.CurrentOperationID != updateIntent.OperationID || lineageState.Barrier.LineageRef == "" || !lineageState.Barrier.InitialHoldReleased {
		t.Fatalf("successful successor did not create a verifiable latest lineage: state=%+v err=%v", lineageState, err)
	}
	server.mu.Lock()
	updatedObject, updatedObjectExists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !updatedObjectExists || updatedObject.generation != lineageState.Barrier.CurrentGeneration || updatedObject.temporaryHold ||
		!bytes.Equal(updatedObject.payload, updatePayload) {
		t.Fatalf("origin-hold cleanup affected or failed to advance the successor generation: exists=%t object=%+v barrier=%+v",
			updatedObjectExists, updatedObject, lineageState.Barrier)
	}
	recreateDelete := successor.admitDelete(t, 2, intent.Key, lineageState.Barrier.CurrentGeneration)

	// The first executor gate for any distinct positive-generation tuple is this
	// durable cross-producer key lock. It must be available after exact guard
	// readback, before a replacement producer can issue its guarded update.
	nextOwner := hashJSON([]any{successorRuntime, "survivor-wal", successor.producer, uint64(1), updateIntent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner, false); err != nil {
		t.Fatalf("later positive-generation producer could not acquire the guarded key: %v", err)
	}
	lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != nextOwner {
		t.Fatalf("later producer key ownership was not durably read back before replay: %+v err=%v", lock, err)
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, oldProvider, f.receipts); err != nil {
		t.Fatalf("replay of the old guarded tuple failed after successor acquisition: %v", err)
	}
	lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != nextOwner {
		t.Fatalf("old guarded replay disturbed the successor key owner: %+v err=%v", lock, err)
	}
	stateAfterReplay, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || stateAfterReplay.Barrier == nil || *stateAfterReplay.Barrier != *lineageState.Barrier {
		t.Fatalf("old guarded replay changed the successor's current barrier lineage: before=%+v after=%+v err=%v",
			lineageState.Barrier, stateAfterReplay.Barrier, err)
	}
	if err := f.e.releaseMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner); err != nil {
		t.Fatalf("later producer could not release the key after its update: %v", err)
	}

	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer || producer.Phase != fencing.MutationClosed || producer.ClosedThrough != 3 || len(producer.Entries) != 3 {
		t.Fatalf("original producer readback failed: uid=%q err=%v", uid, err)
	}
	entry := producer.Entries[1]
	if entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard || producer.ValidateGenerationZeroGuard(f.producer, 1, repair, entry) != nil {
		t.Fatalf("later producer removed or changed the original generation-zero guard: %+v", entry.GenerationZeroGuard)
	}
	if entry.GenerationZeroRights == nil || !entry.GenerationZeroRights.Sealed || !entry.GenerationZeroRights.HelperRightsRetired || len(entry.GenerationZeroRights.Rights) != 1 ||
		entry.GenerationZeroRights.Rights[0].Attempt != 1 || !mutationDigestValidOperator(entry.GenerationZeroRights.Rights[0].RequestDigest) ||
		!mutationDigestValidOperator(entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest) || entry.CopyCompletion != nil ||
		entry.Resolution == nil || entry.Resolution.AllCopiesTerminal {
		t.Fatalf("helper right was not journaled separately from the unresolved original dispatch: %+v", entry)
	}
	if err := f.e.verifyGenerationZeroGuard(t.Context(), f.runtime, run, f.producer, 1, entry); err != nil {
		t.Fatalf("original exact generation proof no longer verifies after the later producer: %v", err)
	}
	lock, err = readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != "" {
		t.Fatalf("positive-generation update left the key locked: %+v err=%v", lock, err)
	}

	// Helper-right retirement and future-effect exclusion do not prove the
	// original create dispatch terminal. Until its own receipt and copy
	// completion are durable, the name-presence barrier must remain installed.
	retired, err := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
	if err != nil || retired {
		t.Fatalf("unresolved original dispatch retired its barrier: retired=%t err=%v", retired, err)
	}
	keyState, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || keyState.Barrier == nil || keyState.Retirement != nil {
		t.Fatalf("unknown original rights did not remain pinned: state=%+v err=%v", keyState, err)
	}

	// Persist the original dispatch's actual terminal receipt independently of
	// the helper receipt. The immutable completion binds the original dispatch,
	// original logical resolution, and sealed helper-right set.
	if entry.GenerationZeroRights == nil || len(entry.GenerationZeroRights.Rights) != 1 ||
		!mutationDigestValidOperator(entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest) ||
		entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest == originalReceipt.ReceiptDigest {
		t.Fatalf("settlement did not retain the exact helper terminal receipt: %+v", entry.GenerationZeroRights)
	}
	originalResolution := *entry.Resolution
	terminalReceiptDigest := originalReceipt.ReceiptDigest
	completion := fencing.MutationCopyCompletion{PreparationHash: guard.PreparationHash,
		ReceiptDigest:              terminalReceiptDigest,
		GenerationZeroRightsDigest: entry.GenerationZeroRights.Digest()}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.RecordDispatchTerminal(1, guard.DispatchID, terminalReceiptDigest); err != nil {
			return err
		}
		return p.ConfirmCopiesTerminal(1, completion)
	}); err != nil {
		t.Fatalf("could not persist exact terminal completion: %v", err)
	}
	producer, uid, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("terminal completion producer readback failed: uid=%q err=%v", uid, err)
	}
	terminalEntry := producer.Entries[1]
	if terminalEntry.CopyCompletion == nil || terminalEntry.Resolution == nil || *terminalEntry.Resolution != originalResolution ||
		terminalEntry.Resolution.AllCopiesTerminal || len(terminalEntry.Dispatches) != 1 ||
		terminalEntry.Dispatches[guard.DispatchID].ReceiptDigest != terminalReceiptDigest {
		t.Fatalf("fixture did not preserve distinct logical and physical terminal evidence: %+v", terminalEntry)
	}

	// Lose the immutable proof-create response after the exact bytes are stored.
	evidence, err := f.e.authorityEvidenceFor(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := generationZeroRetirementProofFor(f.runtime, producer.WALIdentity, f.producer, 1, terminalEntry)
	if err != nil {
		t.Fatalf("canonical terminal entry did not produce retirement proof: %v", err)
	}
	badProof := proof
	badProof.RightsDigest = hashBytes([]byte("different helper-right set"))
	badProof.Digest = badProof.digest()
	if err := badProof.validate(guard, terminalEntry); err == nil {
		t.Fatal("retirement proof accepted a different sealed helper-right set")
	}
	proofObject, err := evidence.objectKey(proof.ref())
	if err != nil {
		t.Fatal(err)
	}
	stateObject, _ := f.e.mutationKeyIdentity(intent.StoreScopeHash, intent.Key)
	faults := &generationZeroRetirementFaultBucket{Bucket: f.e.AuthorityState.bucket, proofKey: proofObject, stateKey: stateObject}
	// Install the lost-response wrapper before the first proof write. That call
	// must persist/read back the proof, then stop at the active owner; the retry
	// below exercises the separate lost-CAS response.
	f.e.AuthorityState.bucket = faults
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner, false); err != nil {
		t.Fatalf("could not establish a concurrent key owner: %v", err)
	}
	retired, err = f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, terminalEntry)
	if err != nil || retired {
		t.Fatalf("retirement crossed an active successor owner: retired=%t err=%v", retired, err)
	}
	keyState, err = f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || keyState.Owner != nextOwner || keyState.Barrier == nil || keyState.Barrier.CurrentOperationID != updateIntent.OperationID {
		t.Fatalf("blocked retirement changed the successor owner or lineage: state=%+v err=%v", keyState, err)
	}
	if err := f.e.releaseMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, nextOwner); err != nil {
		t.Fatalf("could not release test successor owner: %v", err)
	}
	barrierBefore, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || barrierBefore.Barrier == nil {
		t.Fatalf("pre-retirement barrier readback failed: state=%+v err=%v", barrierBefore, err)
	}
	server.mu.Lock()
	objectBefore, objectExists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !objectExists || objectBefore.generation != barrierBefore.Barrier.CurrentGeneration || objectBefore.metageneration != barrierBefore.Barrier.CurrentMetageneration ||
		objectBefore.temporaryHold || !bytes.Equal(objectBefore.payload, updatePayload) {
		t.Fatalf("fixture did not retain the exact released latest object before retirement: exists=%t object=%+v barrier=%+v",
			objectExists, objectBefore, barrierBefore.Barrier)
	}
	retired, err = f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, terminalEntry)
	if err != nil || !retired || !faults.proofResponseLost || !faults.casResponseLost {
		t.Fatalf("exact proof/CAS readback did not resume retirement: retired=%t proofLost=%t casLost=%t err=%v",
			retired, faults.proofResponseLost, faults.casResponseLost, err)
	}
	keyState, err = f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || keyState.Barrier != nil || keyState.Owner != "" || keyState.PendingGenerationZero != nil || keyState.Retirement == nil ||
		keyState.Retirement.ProofRef != proof.ref() || keyState.Retirement.Barrier != *barrierBefore.Barrier {
		t.Fatalf("retirement did not preserve the exact latest barrier lineage: state=%+v err=%v", keyState, err)
	}
	server.mu.Lock()
	objectAfter, objectStillExists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !objectStillExists || objectAfter.generation != objectBefore.generation || objectAfter.metageneration != objectBefore.metageneration || objectAfter.temporaryHold {
		t.Fatalf("retirement changed or deleted the exact released origin object: exists=%t before=%+v after=%+v", objectStillExists, objectBefore, objectAfter)
	}
	server.mu.Lock()
	postsBeforeReplay := server.posts
	server.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, oldProvider, f.receipts); err != nil {
		t.Fatalf("late original replay did not resolve from terminal retirement evidence: %v", err)
	}
	server.mu.Lock()
	postsAfterReplay := server.posts
	server.mu.Unlock()
	if postsAfterReplay != postsBeforeReplay {
		t.Fatalf("late original replay repeated provider effects: create posts before=%d after=%d", postsBeforeReplay, postsAfterReplay)
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), successorRuntime, successor.producer, 2, recreateDelete, f.scope, successorProvider, successorReceipts); err != nil {
		t.Fatalf("successor exact-generation delete after retirement failed: %v", err)
	}
	server.mu.Lock()
	_, existsAfterDelete := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if existsAfterDelete {
		t.Fatal("successor delete did not remove its exact current generation")
	}
	guardB, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 3, f.scope)
	if err != nil || guardB.ObjectGeneration <= guard.ObjectGeneration {
		t.Fatalf("authorized second closed generation-zero dispatch did not recreate under a new barrier: guard=%+v err=%v", guardB, err)
	}
	stateWithBarrierB, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || stateWithBarrierB.Barrier == nil || stateWithBarrierB.Barrier.CurrentOperationID != oldGenerationZeroRecreate.OperationID ||
		stateWithBarrierB.Barrier.CurrentGeneration != guardB.ObjectGeneration || stateWithBarrierB.Barrier.GuardDigest != guardB.GuardDigest ||
		stateWithBarrierB.Barrier.GuardRef != guardB.GuardRef || stateWithBarrierB.Barrier.Runtime != guardB.Runtime ||
		stateWithBarrierB.Barrier.WALIdentity != guardB.WALIdentity || stateWithBarrierB.Barrier.ProducerUID != f.producer ||
		stateWithBarrierB.Barrier.Sequence != 3 || stateWithBarrierB.Barrier.OriginOperationID != oldGenerationZeroRecreate.OperationID {
		t.Fatalf("recreated object did not install its exact second barrier: state=%+v guard=%+v err=%v", stateWithBarrierB, guardB, err)
	}
	// Directly retry retirement of A while B is active. It must recognize the
	// stale guard as a no-op, preserving both B's lineage and the recreated
	// object without issuing another storage mutation.
	server.mu.Lock()
	postsBeforeStaleRetirement := server.posts
	objectBeforeStaleRetirement := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	retired, err = f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, terminalEntry)
	stateAfterStaleRetirement, stateErr := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	server.mu.Lock()
	postsAfterStaleRetirement := server.posts
	objectAfterStaleRetirement := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if err != nil || retired || stateErr != nil || stateAfterStaleRetirement.Barrier == nil ||
		*stateAfterStaleRetirement.Barrier != *stateWithBarrierB.Barrier ||
		postsAfterStaleRetirement != postsBeforeStaleRetirement ||
		objectAfterStaleRetirement.generation != objectBeforeStaleRetirement.generation ||
		objectAfterStaleRetirement.metageneration != objectBeforeStaleRetirement.metageneration ||
		objectAfterStaleRetirement.temporaryHold != objectBeforeStaleRetirement.temporaryHold ||
		!bytes.Equal(objectAfterStaleRetirement.payload, objectBeforeStaleRetirement.payload) {
		t.Fatalf("stale A retirement affected active barrier B or its object: retired=%t before=%+v after=%+v posts=%d->%d err=%v stateErr=%v",
			retired, stateWithBarrierB.Barrier, stateAfterStaleRetirement.Barrier,
			postsBeforeStaleRetirement, postsAfterStaleRetirement, err, stateErr)
	}
	// A late replay of the first, retired tuple must not clear the new barrier.
	server.mu.Lock()
	postsBeforeBarrierReplay := server.posts
	server.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, oldProvider, f.receipts); err != nil {
		t.Fatalf("late first-generation replay did not resolve after recreation: %v", err)
	}
	stateAfterBarrierReplay, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	server.mu.Lock()
	postsAfterBarrierReplay := server.posts
	server.mu.Unlock()
	if err != nil || stateAfterBarrierReplay.Barrier == nil || *stateAfterBarrierReplay.Barrier != *stateWithBarrierB.Barrier ||
		postsAfterBarrierReplay != postsBeforeBarrierReplay {
		t.Fatalf("old replay changed the recreated successor barrier or repeated storage effects: before=%+v after=%+v posts=%d->%d err=%v",
			stateWithBarrierB.Barrier, stateAfterBarrierReplay.Barrier, postsBeforeBarrierReplay, postsAfterBarrierReplay, err)
	}
	postRecreatePayload := []byte(`{"membership":"post-recreation successor generation"}`)
	postRecreateIntent := successor.admitUpload(t, 3, intent.Key,
		fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: guardB.ObjectGeneration}, postRecreatePayload)
	if err := f.e.ExecuteSourceMutation(t.Context(), successorRuntime, successor.producer, 3, postRecreateIntent, f.scope, successorProvider, successorReceipts); err != nil {
		t.Fatalf("open successor could not advance the recreated barrier lineage: %v", err)
	}
	latestBarrierState, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || latestBarrierState.Barrier == nil || latestBarrierState.Barrier.CurrentOperationID != postRecreateIntent.OperationID ||
		latestBarrierState.Barrier.CurrentGeneration <= guardB.ObjectGeneration || latestBarrierState.Barrier.LineageRef == "" {
		t.Fatalf("post-recreation positive update did not preserve a new exact barrier lineage: state=%+v err=%v", latestBarrierState, err)
	}
	server.mu.Lock()
	recreatedObject, recreated := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !recreated || recreatedObject.generation != latestBarrierState.Barrier.CurrentGeneration || recreatedObject.temporaryHold ||
		!bytes.Equal(recreatedObject.payload, postRecreatePayload) {
		t.Fatalf("successor recreation did not create a distinct object generation: exists=%t object=%+v", recreated, recreatedObject)
	}
	// Compaction must preserve the immutable original receipt and retirement
	// reference; a replay through the archived lookup path cannot recreate it.
	f.e.AuthorityState.bucket = &archiveTestBucket{Bucket: f.e.AuthorityState.bucket}
	if _, err := f.e.commitMutationArchive(t.Context(), f.runtime, storeCompactionPlan(t, f.e, f.runtime, 1)); err != nil {
		t.Fatalf("compact terminal generation-zero history after retirement: %v", err)
	}
	identity := run.Identity
	archived, err := f.e.lookupMutation(t.Context(), f.runtime.StatefulSetUID, identity,
		fencing.MutationLookupRequest{ProducerUID: f.producer, Sequence: 1, Intent: intent})
	if err != nil || !archived.Archived || archived.Entry == nil || archived.Entry.CopyCompletion == nil {
		t.Fatalf("compaction lost terminal generation-zero history: result=%+v err=%v", archived, err)
	}
	server.mu.Lock()
	postsBeforeArchivedReplay := server.posts
	recreatedBeforeReplay := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, oldProvider, f.receipts); err != nil {
		t.Fatalf("archived old tuple did not resolve from retirement proof: %v", err)
	}
	server.mu.Lock()
	postsAfterArchivedReplay := server.posts
	recreatedAfterReplay := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	finalBarrierState, barrierErr := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if postsAfterArchivedReplay != postsBeforeArchivedReplay || recreatedAfterReplay.generation != recreatedBeforeReplay.generation ||
		recreatedAfterReplay.metageneration != recreatedBeforeReplay.metageneration || recreatedAfterReplay.temporaryHold != recreatedBeforeReplay.temporaryHold ||
		!bytes.Equal(recreatedAfterReplay.payload, postRecreatePayload) || barrierErr != nil || finalBarrierState.Barrier == nil ||
		*finalBarrierState.Barrier != *latestBarrierState.Barrier {
		t.Fatalf("archived old replay changed the recreated successor: posts=%d->%d before=%+v after=%+v",
			postsBeforeArchivedReplay, postsAfterArchivedReplay, recreatedBeforeReplay, recreatedAfterReplay)
	}
}

func TestEngineRepairZeroGenerationLostCreateResponseRemainsPending(t *testing.T) {
	f := newHeldProfileFixture(t)
	payload := []byte(`{"membership":"uncertain held bootstrap"}`)
	intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "lost-zero-dispatch-response"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("close-lost-zero", hashBytes([]byte("close-lost-zero")))
	}); err != nil {
		t.Fatal(err)
	}
	server, client := newHeldGCSServer(t, f) // This fixture commits the object, then loses the create response.
	server.mu.Lock()
	server.nextGeneration = 0
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	repair := testEngineRepairRecord(t, f)
	if _, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope); !errors.Is(err, ErrFencePending) {
		t.Fatalf("lost helper response was treated as terminal: %v", err)
	}

	server.mu.Lock()
	object, exists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !exists || !object.temporaryHold || !bytes.Equal(object.payload, payload) ||
		object.metadata["rhiza-operation-id"] != intent.OperationID || object.metadata["rhiza-payload-sha256"] != intent.PayloadDigest {
		t.Fatalf("uncertain create did not remain exactly held: exists=%t object=%+v", exists, object)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("uncertain producer readback failed: uid=%q err=%v", uid, err)
	}
	entry := producer.Entries[1]
	if entry.Resolution != nil || entry.GenerationZeroGuard != nil || entry.CopyCompletion != nil ||
		len(entry.Dispatches) != 1 || entry.Dispatches["lost-zero-dispatch-response"].ReceiptDigest != "" ||
		entry.GenerationZeroRights == nil || entry.GenerationZeroRights.Sealed || len(entry.GenerationZeroRights.Rights) != 1 ||
		entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest != "" {
		t.Fatalf("uncertain helper response gained terminal or guard evidence: %+v", entry)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != owner {
		t.Fatalf("uncertain helper response released its exact key owner: lock=%+v err=%v", lock, err)
	}
}

func TestHeldMutationDeleteOfVerifiedCurrentPinnedGenerationIsTerminallyCancelled(t *testing.T) {
	f := newHeldProfileFixture(t)
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	// This positive barrier fixture needs a confirmed helper create. The shared
	// fake server defaults to losing its first create response, which is tested
	// separately as a pending, unguarded outcome.
	server.loseCreate = false
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	create := f.admitUpload(t, 1, "objects/current-pin", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, []byte("pinned"))
	intent := f.admitDelete(t, 2, create.Key, 1)
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), create.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), create.StoreScopeHash, create.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile,
			StoreScopeHash: create.StoreScopeHash, GenerationSet: true, Generation: 0}); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "pin-create-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		if err := p.Prepare(2, fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile,
			StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 1}); err != nil {
			return err
		}
		return p.Close("pin-delete-close", hashBytes([]byte("pin-delete-close")))
	}); err != nil {
		t.Fatal(err)
	}
	repair := testEngineRepairRecord(t, f)
	if _, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope); err != nil {
		t.Fatalf("could not install an authentic current generation barrier: %v", err)
	}
	provider := heldProfileProvider(t, f)
	err = f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 2, intent, f.scope, provider, f.receipts)
	if !errors.Is(err, fencing.ErrMutationConditionFailed) {
		t.Fatalf("delete of the currently pinned generation was not terminally rejected: %v", err)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	entry := producer.Entries[2]
	if err != nil || uid != f.producer || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected ||
		!entry.Resolution.AllCopiesTerminal || len(entry.Dispatches) != 0 || !isEngineRepairNoDispatchCancellation(f.runtime, producer.WALIdentity, f.producer, 2, entry) {
		t.Fatalf("pinned delete remained unresolved or received a dispatch: uid=%q entry=%+v err=%v", uid, entry, err)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != "" || lock.Barrier == nil || lock.Barrier.CurrentGeneration != 1 {
		t.Fatalf("terminal cancellation released or changed the live pin: lock=%+v err=%v", lock, err)
	}
	if server.deletes != 0 {
		t.Fatalf("current pinned generation reached storage delete: deletes=%d", server.deletes)
	}
}

func TestHeldMutationDeleteWithUnverifiedCurrentBarrierDoesNotCancel(t *testing.T) {
	f := newHeldProfileFixture(t)
	server, client := newHeldGCSServer(t, f)
	f.e.SourceMutationStorage = client
	intent := f.admitDelete(t, 1, "objects/unverified-pin", 7)
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile,
			StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 7}); err != nil {
			return err
		}
		return p.Close("unverified-pin-delete-close", hashBytes([]byte("unverified-pin-delete-close")))
	}); err != nil {
		t.Fatal(err)
	}
	endpoint, identity := f.e.mutationKeyIdentity(intent.StoreScopeHash, intent.Key)
	barrier := mutationGenerationZeroBarrier{ScopeHash: intent.StoreScopeHash, Key: intent.Key, CurrentGeneration: 7}
	if err := f.e.AuthorityState.write(t.Context(), endpoint, identity, mutationKeyState{ScopeHash: intent.StoreScopeHash, Key: intent.Key, Barrier: &barrier}, nil); err != nil {
		t.Fatal(err)
	}
	err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, heldProfileProvider(t, f), f.receipts)
	if !errors.Is(err, ErrFencePending) || errors.Is(err, fencing.ErrMutationConditionFailed) {
		t.Fatalf("unverified barrier produced a terminal cancellation: %v", err)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	entry := producer.Entries[1]
	if err != nil || uid != f.producer || entry.Resolution != nil || !entry.DispatchClosed || len(entry.Dispatches) != 0 {
		t.Fatalf("unverified barrier changed the unresolved journal entry: uid=%q entry=%+v err=%v", uid, entry, err)
	}
	if server.deletes != 0 {
		t.Fatalf("unverified barrier reached storage delete: deletes=%d", server.deletes)
	}
}

func (p *mutationExecutionTestProvider) delete(context.Context, string, int64) (mutationProviderResult, error) {
	p.deleteCalls++
	return mutationProviderResult{}, errors.New("unexpected delete")
}

func (p *mutationExecutionTestProvider) committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	p.committedCalls++
	return p.result, p.commitFound, p.committedErr
}

func (p *mutationExecutionTestProvider) verifyRepairCommit(_ context.Context, _ string, intent fencing.MutationIntent, preparation fencing.MutationPreparation, result mutationProviderResult) error {
	if result.Outcome != fencing.MutationCommitted || result.Generation <= preparation.Generation || result.PayloadDigest != intent.PayloadDigest || hashBytes(p.payload) != intent.PayloadDigest {
		return fmt.Errorf("fake repair provider could not verify the exact sealed commit")
	}
	return nil
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
