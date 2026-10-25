package operator

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type pauseEngineRepairRootCASBucket struct {
	objstore.Bucket
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (b *pauseEngineRepairRootCASBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/rhiza-authority/v1/roots/") && b.armed.CompareAndSwap(true, false) {
		close(b.started)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func engineRepairStoreFixture(t *testing.T) (*FencingExecutor, *EngineRepairStore, fencing.EngineRepairRecord) {
	t.Helper()
	e, fixture, id, runtime := newMutationStoreFixture(t)
	scopeHash := preparePendingArchiveGenesis(t, e, fixture, id, runtime)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	if _, err := e.bindWALAtScope(t.Context(), runtime.StatefulSetUID, id, scopeHash); err != nil {
		t.Fatalf("bind source producer scope and WAL through startup path: %v", err)
	}
	root, _, err := e.AuthorityState.ReadRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(id.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", WALIdentity: id.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivor := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", WALIdentity: "survivor-wal", PublicKey: quepaxa.PublicKey{2}}
	sourceConfig := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivor}}
	binding := fencing.EngineRemovalBinding{
		Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "repair-operation", RequestHash: strings.Repeat("a", 64),
		SourceClusterID: runtime.SourceClusterID, StatefulSetUID: runtime.StatefulSetUID,
		OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: id.NodeID, OldWALIdentity: id.WALIdentity, TargetMemberID: "candidate",
	}
	candidateWAL := strings.Repeat("c", 64)
	candidateMember := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", WALIdentity: candidateWAL, PublicKey: quepaxa.PublicKey{3}}
	guard := fencing.EngineRepairRecord{
		Version: fencing.EngineRepairRecordVersion, Binding: binding, SourceConfig: sourceConfig,
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations:          incarnations,
		CandidateIdentity:     fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: "candidate", PodName: "candidate-0", PodUID: "candidate-pod", RunID: "candidate-run", WALIdentity: candidateWAL, AssignmentID: strings.Repeat("b", 64)},
		CandidateMember:       candidateMember,
		CandidateCredential:   fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember),
		RemoveRequestHash:     strings.Repeat("e", 64), Phase: fencing.RepairCandidatePrepared,
	}
	store, err := NewEngineRepairStore(e.AuthorityState, runtime.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	return e, store, guard
}

func pinEngineRepairStoreCandidate(t *testing.T, store *EngineRepairStore, record fencing.EngineRepairRecord) fencing.EngineRepairRecord {
	t.Helper()
	runtime := engineRepairStoreCandidateRuntime(record)
	pinned, err := store.PinCandidateRuntime(t.Context(), record.Binding.SourceClusterID, record.Binding.OperationID, record.Digest(), runtime)
	if err != nil {
		t.Fatalf("pin trusted candidate runtime: %v", err)
	}
	return pinned
}

func engineRepairStoreCandidateRuntime(record fencing.EngineRepairRecord) fencing.RuntimeBinding {
	id := record.CandidateIdentity
	return fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "root",
		StatefulSetUID: record.Binding.StatefulSetUID, SourceClusterID: id.SourceClusterID, NodeID: id.NodeID,
		PodName: id.PodName, PodUID: id.PodUID, RunID: id.RunID, AssignmentID: id.AssignmentID,
		NodeName: "worker", NodeUID: "worker-uid", MachineID: "machine", BootID: "boot",
		RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: "candidate-container", ImageID: "image"}
}

func enrollActiveEngineRepairMutationProducer(t *testing.T, e *FencingExecutor, identity fencing.Identity, runtime fencing.RuntimeBinding) {
	t.Helper()
	producerUID := stableMutationProducerUID(runtime)
	scopeHash := hashJSON(AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/" + identity.SourceClusterID})
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		run := ledger.Runs[identity.RunID]
		if run.Identity != identity || run.Runtime == nil || *run.Runtime != runtime || run.Promotion == nil {
			return fencing.ErrClosed
		}
		run.MutationProducerUID = producerUID
		run.MutationScopeHash = scopeHash
		run.MutationProfile = e.sourceMutationProfile()
		run.MutationStageIncarnation = stages.incarnation
		ledger.Runs[identity.RunID] = run
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	producer, err := fencing.NewMutationProducer(identity, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Activate(identity); err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.WriteProducer(t.Context(), runtime, *producer, nil); err != nil {
		t.Fatal(err)
	}
}

func assertEngineRepairRunProvenance(t *testing.T, run fencing.Run, record fencing.EngineRepairRecord) {
	t.Helper()
	want := fencing.EngineRepairRunProvenance{
		Version: fencing.EngineRepairRunProvenanceVersion, OperationID: record.Binding.OperationID,
		IdentityDigest: record.IdentityDigest(), CandidateIdentity: record.CurrentCandidateIdentity(),
		RuntimeBindingDigest: record.CurrentCandidateRuntimeHash(),
	}
	if run.EngineRepairProvenance == nil || *run.EngineRepairProvenance != want {
		t.Fatalf("canonical candidate Run lacks exact server-pinned repair provenance: got=%+v want=%+v", run.EngineRepairProvenance, want)
	}
}

func freezeEngineRepairRecord(record fencing.EngineRepairRecord) fencing.EngineRepairRecord {
	record.Binding.FreezeSlot = 17
	record.Binding.FreezeHash = strings.Repeat("f", 64)
	freezeHash, _ := hex.DecodeString(record.Binding.FreezeHash)
	var hash quepaxa.ValueHash
	copy(hash[:], freezeHash)
	removed := record.SourceConfig.MemberSet()[quepaxa.NodeID(record.Binding.OldMemberID)]
	target := quepaxa.Cluster{ConfigID: record.SourceConfig.ConfigID + 1}
	for _, member := range record.SourceConfig.Members {
		if member.ID != removed.ID {
			target.Members = append(target.Members, member)
		}
	}
	record.RemovalOperation = &quepaxa.EngineRemoval{Version: 1, Mode: fencing.EngineRemovalMode, OperationID: record.Binding.OperationID,
		ExpectedConfigID: uint(record.Binding.OldConfigID), ExpectedAbortSlot: quepaxa.Slot(record.Binding.ExpectedAbortSlot),
		Removed: removed, Target: target, Freeze: quepaxa.Slot(record.Binding.FreezeSlot), FreezeHash: hash}
	return record
}

func pendingEngineRepairProposalForRecord(record fencing.EngineRepairRecord) fencing.PendingEngineRepairProposal {
	bootstrap := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: record.Binding.OldMemberID,
		PodName: "replacement-0", PodUID: "replacement-pod", RunID: "replacement-run"}
	candidate := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: record.Binding.TargetMemberID,
		PodName: bootstrap.PodName, PodUID: bootstrap.PodUID, RunID: bootstrap.RunID, WALIdentity: record.CandidateMember.WALIdentity}
	return fencing.PendingEngineRepairProposal{
		Version: fencing.PendingEngineRepairRequestVersion, SourceClusterID: record.Binding.SourceClusterID,
		StatefulSetUID: record.Binding.StatefulSetUID, BootstrapIdentity: bootstrap, StableOrdinal: 0,
		OldMemberID: record.Binding.OldMemberID, SourceOldWALIdentity: record.Binding.OldWALIdentity,
		IncumbentReferenceDigest: strings.Repeat("d", 64),
		OldWALIdentity:           record.Binding.OldWALIdentity, Outcome: fencing.RecoveryBootstrapLostWAL,
		LocalIdentityHash: strings.Repeat("c", 64), CandidateIdentity: candidate,
		CandidateMember: record.CandidateMember, CandidateCredential: record.CandidateCredential,
		SourceAdminCredential: record.SourceAdminCredential,
	}
}

func TestEngineRepairStoreRootCASReservationAndPhaseAdvance(t *testing.T) {
	e, store, candidate := engineRepairStoreFixture(t)
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, candidate)
	if err != nil || created.Digest() != candidate.Digest() {
		t.Fatalf("create repair record=%+v err=%v", created, err)
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := root.Ledger.Runs[candidate.CandidateIdentity.RunID]; exists {
		t.Fatal("reservation registered a candidate run before trusted runtime enrollment")
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	root, _, err = e.AuthorityState.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	generationKey, _ := candidate.Binding.EngineRepairGenerationKey()
	reference, ok := root.Ledger.EngineRepairs[generationKey]
	if !ok || reference.Validate() != nil || reference.Binding != candidate.Binding || reference.CandidateIdentity != candidate.CandidateIdentity || reference.RecordDigest != pinned.Digest() {
		t.Fatalf("canonical root did not pin exact reservation: %+v", reference)
	}
	candidateRun, exists := root.Ledger.Runs[candidate.CandidateIdentity.RunID]
	wantUnbound := candidate.CandidateIdentity
	wantUnbound.WALIdentity = ""
	if !exists || candidateRun.Identity != wantUnbound || candidateRun.Runtime == nil || *candidateRun.Runtime != *pinned.CandidateRuntimeBinding ||
		fencing.RunRole(candidateRun) != fencing.StartupRoleLearnerPendingRemove {
		t.Fatalf("trusted runtime and exact WAL-unbound learner run were not pinned together: run=%+v exists=%t", candidateRun, exists)
	}
	assertEngineRepairRunProvenance(t, candidateRun, pinned)

	loaded, err := store.ReadEngineRepair(ctx, candidate.Binding.SourceClusterID, candidate.Binding.OperationID)
	if err != nil || loaded.Digest() != pinned.Digest() {
		t.Fatalf("read referenced record digest=%s err=%v", loaded.Digest(), err)
	}

	next := pinned
	next = freezeEngineRepairRecord(next)
	next.Phase = fencing.RepairRemoveFrozen
	advanced, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, pinned.Digest(), next)
	if err != nil || advanced.Digest() != next.Digest() || advanced.IdentityDigest() != pinned.IdentityDigest() {
		t.Fatalf("advance record=%+v err=%v", advanced, err)
	}

	readback, err := store.CreateOrReadEngineRepair(ctx, candidate)
	if err != nil || readback.Digest() != advanced.Digest() {
		t.Fatalf("idempotent read of advanced reservation digest=%s err=%v, want canonical advanced digest %s", readback.Digest(), err, advanced.Digest())
	}
	if _, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, candidate.Digest(), candidate); err == nil {
		t.Fatal("stale CAS rewrote or regressed the canonical repair record")
	}
	loaded, err = store.ReadEngineRepair(ctx, candidate.Binding.SourceClusterID, candidate.Binding.OperationID)
	if err != nil || loaded.Digest() != next.Digest() {
		t.Fatalf("stale CAS changed root-referenced record: digest=%s err=%v", loaded.Digest(), err)
	}
	closedState, closeVersion, err := e.AuthorityState.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closedState.Ledger.ClosedBy = "fixture-close"
	if err := e.AuthorityState.WriteRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID, closedState, closeVersion); err != nil {
		t.Fatalf("close root after successful runtime pin: %v", err)
	}
	closedState, _, err = e.AuthorityState.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closedRun, exists := closedState.Ledger.Runs[candidate.CandidateIdentity.RunID]
	if !exists || closedRun.Runtime == nil || *closedRun.Runtime != *pinned.CandidateRuntimeBinding {
		t.Fatal("close CAS omitted the runtime pin that committed first")
	}
}

func TestEngineRepairStoreContinuesCandidateWithFreshRunID(t *testing.T) {
	e, store, reservation := engineRepairStoreFixture(t)
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	originalIdentity := pinned.CandidateIdentity
	originalRuntime := *pinned.CandidateRuntimeBinding
	identityDigest := pinned.IdentityDigest()
	continuationIdentity := originalIdentity
	continuationIdentity.RunID = "candidate-run-restarted"
	continuationRuntime := engineRepairStoreCandidateRuntime(reservation)
	continuationRuntime.RunID = continuationIdentity.RunID
	continuationRuntime.RuntimeID = "containerd-restarted"
	continuationRuntime.ContainerID = "candidate-container-restarted"
	invalid := []struct {
		name     string
		identity fencing.Identity
		runtime  fencing.RuntimeBinding
	}{
		{name: "assignment", identity: continuationIdentity, runtime: continuationRuntime},
		{name: "pod uid", identity: continuationIdentity, runtime: continuationRuntime},
		{name: "wal", identity: continuationIdentity, runtime: continuationRuntime},
	}
	invalid[0].identity.AssignmentID = strings.Repeat("9", 64)
	invalid[0].runtime.AssignmentID = invalid[0].identity.AssignmentID
	invalid[1].identity.PodUID = "replacement-pod"
	invalid[1].runtime.PodUID = invalid[1].identity.PodUID
	invalid[2].identity.WALIdentity = "different-wal"
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
				pinned.Digest(), originalIdentity.RunID, tc.identity, tc.runtime); err == nil {
				t.Fatal("candidate continuation changed an immutable candidate binding")
			}
		})
	}

	continued, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		pinned.Digest(), originalIdentity.RunID, continuationIdentity, continuationRuntime)
	if err != nil {
		t.Fatalf("continue candidate with a fresh RunID: %v", err)
	}
	if continued.CandidateIdentity != originalIdentity || continued.CurrentCandidateIdentity() != continuationIdentity ||
		continued.CurrentCandidateRuntime() == nil || *continued.CurrentCandidateRuntime() != continuationRuntime ||
		continued.CurrentCandidateRuntimeHash() != fencing.RuntimeBindingDigest(continuationRuntime) ||
		continued.IdentityDigest() != identityDigest || len(continued.CandidateContinuations) != 1 ||
		continued.CandidateContinuations[0].PriorRunID != originalIdentity.RunID ||
		continued.CandidateContinuations[0].ExpectedRecordDigest != pinned.Digest() {
		t.Fatalf("continuation did not preserve reservation identity and append exact runtime: %+v", continued)
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	generationKey, _ := reservation.Binding.EngineRepairGenerationKey()
	reference := root.Ledger.EngineRepairs[generationKey]
	if reference.CandidateIdentity != continuationIdentity || reference.RecordDigest != continued.Digest() || reference.IdentityDigest != identityDigest {
		t.Fatalf("root did not pin the fresh current RunID and immutable reservation digest: %+v", reference)
	}
	priorRun, priorExists := root.Ledger.Runs[originalIdentity.RunID]
	currentRun, currentExists := root.Ledger.Runs[continuationIdentity.RunID]
	wantPrior := originalIdentity
	wantPrior.WALIdentity = ""
	wantCurrent := continuationIdentity
	wantCurrent.WALIdentity = ""
	if !priorExists || priorRun.Identity != wantPrior || priorRun.Runtime == nil || *priorRun.Runtime != originalRuntime ||
		fencing.RunRole(priorRun) != fencing.StartupRoleLearnerPendingRemove ||
		!currentExists || currentRun.Identity != wantCurrent || currentRun.Runtime == nil || *currentRun.Runtime != continuationRuntime ||
		fencing.RunRole(currentRun) != fencing.StartupRoleLearnerPendingRemove || currentRun.MutationProducerUID != "" {
		t.Fatalf("continuation changed the old run or failed to register the exact new learner: prior=%+v current=%+v", priorRun, currentRun)
	}
	assertEngineRepairRunProvenance(t, priorRun, pinned)
	assertEngineRepairRunProvenance(t, currentRun, continued)
	if err := continued.ValidateAgainstLedger(root.Ledger); err != nil {
		t.Fatalf("continued record does not validate against canonical run history: %v", err)
	}

	retried, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		pinned.Digest(), originalIdentity.RunID, continuationIdentity, continuationRuntime)
	if err != nil || retried.Digest() != continued.Digest() {
		t.Fatalf("lost continuation response was not idempotent: digest=%s err=%v", retried.Digest(), err)
	}
	wrongNext := continuationIdentity
	wrongNext.RunID = "candidate-run-forged"
	if _, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		pinned.Digest(), originalIdentity.RunID, wrongNext, continuationRuntime); err == nil {
		t.Fatal("stale prior RunID authorized a second continuation")
	}
	rootAfter, _, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := rootAfter.Ledger.Runs[wrongNext.RunID]; exists || rootAfter.Ledger.EngineRepairs[generationKey] != reference {
		t.Fatal("rejected stale continuation mutated canonical runs or reference")
	}
	closeState, closeVersion, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closeState.Ledger.ClosedBy = "fixture-close"
	if err := e.AuthorityState.WriteRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID, closeState, closeVersion); err != nil {
		t.Fatal(err)
	}
	thirdIdentity := continuationIdentity
	thirdIdentity.RunID = "candidate-run-after-close"
	thirdRuntime := continuationRuntime
	thirdRuntime.RunID = thirdIdentity.RunID
	thirdRuntime.RuntimeID = "containerd-after-close"
	thirdRuntime.ContainerID = "candidate-container-after-close"
	if _, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		continued.Digest(), continuationIdentity.RunID, thirdIdentity, thirdRuntime); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("continuation after root close returned %v, want ErrClosed", err)
	}
	closedRoot, _, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := closedRoot.Ledger.Runs[thirdIdentity.RunID]; exists || closedRoot.Ledger.EngineRepairs[generationKey] != reference {
		t.Fatal("closed root accepted a new candidate RunID or changed its repair reference")
	}
}

func TestEngineRepairStoreContinuesBoundCandidateWithFreshRunID(t *testing.T) {
	e, store, reservation := engineRepairStoreFixture(t)
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	original := pinned.CandidateIdentity
	runtime := *pinned.CandidateRuntimeBinding

	root, version, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.Bind(original); err != nil {
		t.Fatalf("bind exact learner WAL in canonical root: %v", err)
	}
	if err := e.AuthorityState.WriteRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID, root, version); err != nil {
		t.Fatalf("persist exact bound learner identity: %v", err)
	}

	nextIdentity := original
	nextIdentity.RunID = "candidate-run-bound-restarted"
	nextRuntime := engineRepairStoreCandidateRuntime(reservation)
	nextRuntime.RunID = nextIdentity.RunID
	nextRuntime.RuntimeID = "containerd-bound-restarted"
	nextRuntime.ContainerID = "candidate-bound-restarted"
	continued, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		pinned.Digest(), original.RunID, nextIdentity, nextRuntime)
	if err != nil {
		t.Fatalf("continue exactly bound candidate with fresh RunID: %v", err)
	}
	if continued.CandidateIdentity != original || continued.CurrentCandidateIdentity() != nextIdentity ||
		continued.CurrentCandidateRuntime() == nil || *continued.CurrentCandidateRuntime() != nextRuntime {
		t.Fatalf("bound candidate continuation changed immutable identity or failed to select fresh runtime: %+v", continued)
	}
	root, _, err = e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	priorRun := root.Ledger.Runs[original.RunID]
	currentRun := root.Ledger.Runs[nextIdentity.RunID]
	wantCurrent := nextIdentity
	wantCurrent.WALIdentity = ""
	if priorRun.Identity != original || priorRun.Runtime == nil || *priorRun.Runtime != runtime ||
		fencing.RunRole(priorRun) != fencing.StartupRoleLearnerPendingRemove ||
		currentRun.Identity != wantCurrent ||
		currentRun.Runtime == nil || *currentRun.Runtime != nextRuntime || fencing.RunRole(currentRun) != fencing.StartupRoleLearnerPendingRemove {
		t.Fatalf("bound prior run or fresh prebind learner run differs from canonical continuation: prior=%+v current=%+v", priorRun, currentRun)
	}
	assertEngineRepairRunProvenance(t, priorRun, pinned)
	assertEngineRepairRunProvenance(t, currentRun, continued)
	if err := continued.ValidateAgainstLedger(root.Ledger); err != nil {
		t.Fatalf("bound continuation does not validate against canonical run history: %v", err)
	}
	if _, found, err := e.engineRepairRecordForRuntime(ctx, reservation.Binding.StatefulSetUID, original); !found || !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("stale prior candidate identity remains eligible for startup binding: found=%t err=%v", found, err)
	}
}

func TestEngineRepairStoreRejectsContinuationFromClosedCandidateRun(t *testing.T) {
	e, store, reservation := engineRepairStoreFixture(t)
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	root, version, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.CloseNodes("candidate-run-retired", strings.Repeat("d", 64), []string{pinned.CandidateIdentity.NodeID}); err != nil {
		t.Fatalf("record exact candidate slot retirement: %v", err)
	}
	if err := e.AuthorityState.WriteRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID, root, version); err != nil {
		t.Fatalf("persist canonical stopped learner run: %v", err)
	}

	nextIdentity := pinned.CandidateIdentity
	nextIdentity.RunID = "candidate-run-after-closed-incarnation"
	nextRuntime := engineRepairStoreCandidateRuntime(reservation)
	nextRuntime.RunID = nextIdentity.RunID
	nextRuntime.RuntimeID = "containerd-after-closed"
	nextRuntime.ContainerID = "candidate-after-closed"
	if _, err := store.ContinueCandidateRuntime(ctx, reservation.Binding.SourceClusterID, reservation.Binding.OperationID,
		pinned.Digest(), pinned.CandidateIdentity.RunID, nextIdentity, nextRuntime); err == nil {
		t.Fatal("closed canonical candidate run authorized a fresh continuation")
	}
	closedRoot, _, err := e.AuthorityState.ReadRoot(ctx, reservation.Binding.StatefulSetUID, reservation.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := closedRoot.Ledger.Runs[nextIdentity.RunID]; exists {
		t.Fatal("rejected closed-run continuation registered a fresh candidate")
	}
}

func TestEngineRepairStorePinRejectsCloseThatWinsRootCAS(t *testing.T) {
	e, _, candidate := engineRepairStoreFixture(t)
	bucket := &pauseEngineRepairRootCASBucket{Bucket: e.AuthorityState.bucket, started: make(chan struct{}), release: make(chan struct{})}
	authority, err := NewAuthorityStateStore(bucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	releasePin := func() { releaseOnce.Do(func() { close(bucket.release) }) }
	defer releasePin()
	store, err := NewEngineRepairStore(authority, candidate.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	bucket.armed.Store(true)
	type pinResult struct {
		record fencing.EngineRepairRecord
		err    error
	}
	result := make(chan pinResult, 1)
	go func() {
		record, err := store.PinCandidateRuntime(ctx, candidate.Binding.SourceClusterID, candidate.Binding.OperationID,
			created.Digest(), engineRepairStoreCandidateRuntime(candidate))
		result <- pinResult{record: record, err: err}
	}()
	select {
	case <-bucket.started:
	case <-time.After(5 * time.Second):
		t.Fatal("candidate pin did not reach its root CAS")
	}
	closer, err := NewAuthorityStateStore(e.AuthorityState.bucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	closedState, closeVersion, err := closer.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closedState.Ledger.ClosedBy = "fixture-close"
	if err := closer.WriteRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID, closedState, closeVersion); err != nil {
		t.Fatalf("close canonical root before releasing candidate CAS: %v", err)
	}
	closing := created
	closing.Phase = fencing.RepairGenerationClosing
	closing.ClosingFrom = fencing.RepairCandidatePrepared
	if _, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, created.Digest(), closing); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("repair advance after root close returned %v, want ErrClosed", err)
	}
	releasePin()
	select {
	case pinned := <-result:
		if !errors.Is(pinned.err, fencing.ErrClosed) {
			t.Fatalf("pin after close returned record=%+v err=%v, want ErrClosed", pinned.record, pinned.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("candidate pin did not finish after the close won")
	}
	closedState, _, err = closer.ReadRoot(ctx, candidate.Binding.StatefulSetUID, candidate.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if closedState.Ledger.ClosedBy != "fixture-close" {
		t.Fatalf("root close marker changed: %q", closedState.Ledger.ClosedBy)
	}
	if _, exists := closedState.Ledger.Runs[candidate.CandidateIdentity.RunID]; exists {
		t.Fatal("candidate run was registered after close won the root CAS")
	}
	generationKey, _ := candidate.Binding.EngineRepairGenerationKey()
	if reference := closedState.Ledger.EngineRepairs[generationKey]; reference.RecordDigest != created.Digest() || reference.Phase != fencing.RepairCandidatePrepared {
		t.Fatalf("close-winning race changed canonical repair reference: %+v", reference)
	}
}

func TestEngineRepairStoreOrphanEvidenceIsNotAuthority(t *testing.T) {
	_, store, candidate := engineRepairStoreFixture(t)
	if err := store.persistImmutableRecord(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadEngineRepair(t.Context(), candidate.Binding.SourceClusterID, candidate.Binding.OperationID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("unreferenced evidence was treated as authority: %v", err)
	}
}

func TestEngineRepairStoreAdoptionUsesCanonicalRootInventory(t *testing.T) {
	e, store, record := engineRepairStoreFixture(t)
	ctx := t.Context()
	proposal := fencing.PendingEngineRepairProposal{
		Version:         fencing.PendingEngineRepairRequestVersion,
		SourceClusterID: record.Binding.SourceClusterID, StatefulSetUID: record.Binding.StatefulSetUID,
		BootstrapIdentity: fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: record.Binding.OldMemberID,
			PodName: "old-0", PodUID: "replacement-pod", RunID: "replacement-run"},
		StableOrdinal: 0, OldMemberID: record.Binding.OldMemberID, SourceOldWALIdentity: record.Binding.OldWALIdentity,
		IncumbentReferenceDigest: strings.Repeat("d", 64),
		Outcome:                  fencing.RecoveryBootstrapCandidateWAL, LocalIdentityHash: strings.Repeat("c", 64),
		CandidateIdentity: fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: record.Binding.TargetMemberID,
			PodName: "old-0", PodUID: "replacement-pod", RunID: "replacement-run", WALIdentity: record.CandidateIdentity.WALIdentity},
		CandidateMember: record.CandidateMember, CandidateCredential: record.CandidateCredential,
		SourceAdminCredential: record.SourceAdminCredential,
	}
	pending, err := store.CreateOrReadPendingEngineRepair(ctx, proposal)
	if err != nil {
		t.Fatalf("record pending physical request: %v", err)
	}
	record.Binding.OperationID = pending.OperationID
	record.Binding.RequestHash = pending.RequestDigest
	record.CandidateIdentity = pending.CandidateIdentity
	record.BootstrapRequestDigest = pending.RequestDigest
	record.Incarnations = nil // Only the authority may attach this root snapshot.
	if err := record.ValidateAdoptionProposal(); err != nil {
		t.Fatalf("empty-inventory adoption proposal: %v", err)
	}
	if err := record.Validate(); err == nil {
		t.Fatal("unpopulated adoption proposal unexpectedly passed full record validation")
	}
	callerSuppliedInventory, _, err := e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	callerSuppliedInventorySnapshot, err := fencing.SortedRepairIncarnations(callerSuppliedInventory.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	malformedProposal := record
	malformedProposal.Incarnations = callerSuppliedInventorySnapshot
	if err := malformedProposal.ValidateAdoptionProposal(); err == nil {
		t.Fatal("adoption accepted caller-supplied canonical inventory")
	}
	adopted, err := store.AdoptPendingEngineRepair(ctx, pending.PendingID, pending.RequestDigest, record)
	if err != nil {
		t.Fatalf("adopt pending request from canonical root inventory: %v", err)
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	want, err := fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted.Incarnations) != len(want) {
		t.Fatalf("adopted inventory length=%d, canonical root length=%d", len(adopted.Incarnations), len(want))
	}
	for i := range want {
		if adopted.Incarnations[i] != want[i] {
			t.Fatalf("adopted inventory[%d]=%+v, want canonical root entry %+v", i, adopted.Incarnations[i], want[i])
		}
	}
	if _, exists := root.Ledger.Runs[pending.BootstrapIdentity.RunID]; exists || len(root.Ledger.PendingEngineRepairs) != 0 {
		t.Fatal("adoption registered a candidate run or retained its pending request")
	}
	if adopted.CandidateRuntimeBinding != nil || adopted.CandidateRuntimeHash != "" {
		t.Fatal("adoption pinned a runtime before trusted enrollment")
	}
}

func TestEngineRepairStoreRejectsSecondOperationForSameGeneration(t *testing.T) {
	_, store, candidate := engineRepairStoreFixture(t)
	if _, err := store.CreateOrReadEngineRepair(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	other := candidate
	other.Binding.OperationID = "different-operation"
	other.Binding.RequestHash = strings.Repeat("9", 64)
	other.RemoveRequestHash = strings.Repeat("8", 64)
	if _, err := store.CreateOrReadEngineRepair(t.Context(), other); err == nil {
		t.Fatal("second operation replaced the canonical reservation for one generation")
	}
	loaded, err := store.ReadEngineRepair(t.Context(), candidate.Binding.SourceClusterID, candidate.Binding.OperationID)
	if err != nil || loaded.Digest() != candidate.Digest() {
		t.Fatalf("original canonical reservation changed: digest=%s err=%v", loaded.Digest(), err)
	}
}

func TestEngineRepairStoreSerializesLiveRepairsPerSourceAndRetainsHistory(t *testing.T) {
	_, store, first := engineRepairStoreFixture(t)
	ctx := t.Context()
	if _, err := store.CreateOrReadEngineRepair(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := first
	second.Binding.OperationID = "repair-operation-next"
	second.Binding.RequestHash = strings.Repeat("9", 64)
	second.Binding.OldConfigID++
	second.Binding.ExpectedAbortSlot++
	second.Binding.TargetMemberID = "candidate-next"
	second.SourceConfig.ConfigID++
	second.CandidateIdentity.NodeID = "candidate-next"
	second.CandidateIdentity.RunID = "candidate-run-next"
	second.CandidateIdentity.WALIdentity = "candidate-wal-next"
	second.CandidateIdentity.AssignmentID = strings.Repeat("7", 64)
	second.CandidateMember = quepaxa.Member{ID: quepaxa.NodeID(second.CandidateIdentity.NodeID), URL: "http://candidate-next:8080",
		PeerURL: "quic://candidate-next:9090", LogURL: "http://candidate-next:8081", WALIdentity: second.CandidateIdentity.WALIdentity, PublicKey: quepaxa.PublicKey{4}}
	second.CandidateMemberDigest = fencing.EngineRepairMemberDigest(second.CandidateMember)
	second.CandidateRuntimeBinding = nil
	second.CandidateRuntimeHash = ""
	second.RemoveRequestHash = strings.Repeat("8", 64)
	if _, err := store.CreateOrReadEngineRepair(ctx, second); err == nil {
		t.Fatal("different ConfigID/abort-slot repair overlapped the source's live reservation")
	}

	closing := first
	closing.Phase = fencing.RepairGenerationClosing
	closing.ClosingFrom = fencing.RepairCandidatePrepared
	closed, err := store.CompareAndSwapEngineRepair(ctx, first.Binding.SourceClusterID, first.Digest(), closing)
	if err != nil {
		t.Fatalf("close superseded live reservation: %v", err)
	}
	if _, err := store.CreateOrReadEngineRepair(ctx, second); err == nil {
		t.Fatal("GenerationClosing released the source before canonical root closure")
	}
	if _, err := store.CreateOrReadPendingEngineRepair(ctx, pendingEngineRepairProposalForRecord(first)); err == nil {
		t.Fatal("GenerationClosing allowed a new pending physical request for the same source")
	}
	if _, err := store.PinCandidateRuntime(ctx, first.Binding.SourceClusterID, first.Binding.OperationID, closed.Digest(), engineRepairStoreCandidateRuntime(first)); err == nil {
		t.Fatal("GenerationClosing allowed candidate runtime activation")
	}
	loaded, err := store.ReadEngineRepair(ctx, first.Binding.SourceClusterID, first.Binding.OperationID)
	if err != nil || loaded.Digest() != closed.Digest() || loaded.Phase != fencing.RepairGenerationClosing {
		t.Fatalf("prior repair evidence was not retained: phase=%s err=%v", loaded.Phase, err)
	}
	// The lock policy releases only on Active; the separate store transition
	// test proves generic CAS cannot manufacture that phase.
	activeReference := engineRepairRootReference(first)
	activeReference.Phase = fencing.RepairActive
	activeLedger := fencing.Ledger{EngineRepairs: map[string]fencing.EngineRepairReference{"active-history": activeReference}}
	if err := validateNoLiveEngineRepairForSource(activeLedger, first.Binding.SourceClusterID, ""); err != nil {
		t.Fatalf("completed Active history kept the source locked: %v", err)
	}
}

func TestEngineRepairStoreGenericCASCannotCreateActivePhase(t *testing.T) {
	_, store, record := engineRepairStoreFixture(t)
	ctx := t.Context()
	if _, err := store.CreateOrReadEngineRepair(ctx, record); err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, record)
	forgedActive := freezeEngineRepairRecord(pinned)
	forgedActive.Phase = fencing.RepairActive
	forgedActive.AddRequestHash = strings.Repeat("1", 64)
	forgedActive.StorageProofDigest = strings.Repeat("2", 64)
	forgedActive.RemoveTerminalDigest = strings.Repeat("3", 64)
	if _, err := store.CompareAndSwapEngineRepair(ctx, record.Binding.SourceClusterID, pinned.Digest(), forgedActive); err == nil {
		t.Fatal("generic CAS created Active without the verified activation path")
	}
	loaded, err := store.ReadEngineRepair(ctx, record.Binding.SourceClusterID, record.Binding.OperationID)
	if err != nil || loaded.Digest() != pinned.Digest() || loaded.Phase != fencing.RepairCandidatePrepared {
		t.Fatalf("rejected Active request changed the canonical record: phase=%s err=%v", loaded.Phase, err)
	}
}

func TestEngineRepairStoreGenerationClosingPreservesExactPriorPhase(t *testing.T) {
	_, store, candidate := engineRepairStoreFixture(t)
	ctx := t.Context()
	if _, err := store.CreateOrReadEngineRepair(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, candidate)
	frozen := freezeEngineRepairRecord(pinned)
	frozen.Phase = fencing.RepairRemoveFrozen
	if _, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, pinned.Digest(), frozen); err != nil {
		t.Fatalf("advance to frozen repair: %v", err)
	}

	wrongEntry := frozen
	wrongEntry.Phase = fencing.RepairGenerationClosing
	wrongEntry.ClosingFrom = fencing.RepairRemoved
	if _, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, frozen.Digest(), wrongEntry); err == nil {
		t.Fatal("generation-closing CAS accepted a caller-invented earlier prior phase")
	}

	closing := frozen
	closing.Phase = fencing.RepairGenerationClosing
	closing.ClosingFrom = fencing.RepairRemoveFrozen
	closed, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, frozen.Digest(), closing)
	if err != nil {
		t.Fatalf("close active repair generation: %v", err)
	}

	changedPrior := closed
	changedPrior.ClosingFrom = fencing.RepairRemoved
	if _, err := store.CompareAndSwapEngineRepair(ctx, candidate.Binding.SourceClusterID, closed.Digest(), changedPrior); err == nil {
		t.Fatal("generation-closing CAS rewrote its original prior phase")
	}
	loaded, err := store.ReadEngineRepair(ctx, candidate.Binding.SourceClusterID, candidate.Binding.OperationID)
	if err != nil || loaded.Digest() != closed.Digest() || loaded.ClosingFrom != fencing.RepairRemoveFrozen {
		t.Fatalf("invalid close phase changed canonical record: record=%+v err=%v", loaded, err)
	}
}

func seedActiveEngineRepairForRestart(t *testing.T) (*FencingExecutor, *EngineRepairStore, fencing.EngineRepairRecord) {
	t.Helper()
	e, store, record := engineRepairStoreFixture(t)
	ctx := t.Context()
	created, err := store.CreateOrReadEngineRepair(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	active := freezeEngineRepairRecord(pinned)
	active.StorageProofDigest = strings.Repeat("2", 64)
	active.RemoveTerminalDigest = strings.Repeat("3", 64)
	request := network.MembershipChange{OperationID: engineRepairAddOperationID(active.Binding.OperationID), ClusterID: active.Binding.SourceClusterID,
		ExpectedConfigID: active.RemovalOperation.Target.ConfigID, ExpectedAbortSlot: 0, Add: &active.CandidateMember}
	active.AddRequestHash = hashJSON(request)
	active.Phase = fencing.RepairActive
	if err := active.Validate(); err != nil {
		t.Fatalf("active record fixture is invalid: %v", err)
	}
	state, version, err := e.AuthorityState.ReadRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Ledger.Bind(active.CandidateIdentity); err != nil {
		t.Fatalf("bind exact candidate WAL through the canonical ledger: %v", err)
	}
	if err := state.Ledger.PromoteLearner(active.CandidateIdentity, engineRepairAddOperationID(active.Binding.OperationID), strings.Repeat("4", 64)); err != nil {
		t.Fatalf("promote exact candidate Run through the canonical ledger: %v", err)
	}
	if err := store.persistImmutableRecord(ctx, active); err != nil {
		t.Fatal(err)
	}
	key, err := active.Binding.EngineRepairGenerationKey()
	if err != nil {
		t.Fatal(err)
	}
	state.Ledger.EngineRepairs[key] = engineRepairRootReference(active)
	if err := active.ValidateAgainstLedger(state.Ledger); err != nil {
		t.Fatalf("active record does not match the exact canonical candidate Run: %v", err)
	}
	if err := e.AuthorityState.WriteRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID, state, version); err != nil {
		t.Fatal(err)
	}
	enrollActiveEngineRepairMutationProducer(t, e, active.CandidateIdentity, engineRepairStoreCandidateRuntime(active))
	return e, store, active
}

func TestEngineRepairStoreAdvancesActiveRestartRunInRootCASAndRetainsHistory(t *testing.T) {
	e, store, active := seedActiveEngineRepairForRestart(t)
	ctx := t.Context()
	initialDigest := active.Digest()
	root, _, err := e.AuthorityState.ReadRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	prior := root.Ledger.Runs[active.CandidateIdentity.RunID]
	priorRuntime := engineRepairStoreCandidateRuntime(active)
	priorProducer, priorProducerUID, _, err := e.AuthorityState.ReadProducer(ctx, priorRuntime)
	if err != nil || prior.MutationProducerUID != stableMutationProducerUID(priorRuntime) || priorProducerUID != prior.MutationProducerUID ||
		prior.MutationScopeHash != hashJSON(AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/" + active.Binding.SourceClusterID}) ||
		prior.MutationProfile != e.sourceMutationProfile() || prior.MutationStageIncarnation == "" ||
		priorProducer.Runtime != priorRuntime || priorProducer.WALIdentity != active.CandidateIdentity.WALIdentity ||
		priorProducer.Phase != fencing.MutationOpen || priorProducer.Validate() != nil {
		t.Fatalf("active restart source fixture lacks its exact already-bound producer: run=%+v producer=%+v uid=%q err=%v", prior, priorProducer, priorProducerUID, err)
	}

	restart := func(prior fencing.Identity, suffix string) (fencing.Identity, fencing.RuntimeBinding) {
		t.Helper()
		next := prior
		next.RunID = "candidate-run-" + suffix
		runtime := engineRepairStoreCandidateRuntime(active)
		runtime.RunID = next.RunID
		runtime.RuntimeID += "-" + suffix
		runtime.ContainerID += "-" + suffix
		runtime.BootID += "-" + suffix
		got, err := store.RestartActiveCandidateRuntime(ctx, active.Binding.SourceClusterID, active.Binding.OperationID,
			initialDigest, prior.RunID, next, runtime)
		if err != nil || got.Digest() != initialDigest {
			t.Fatalf("advance active restart %s: digest=%s err=%v", suffix, got.Digest(), err)
		}
		return next, runtime
	}

	firstIdentity, firstRuntime := restart(active.CandidateIdentity, "restart-1")
	loaded, currentIdentity, currentRuntime, err := store.ReadEngineRepairCurrentCandidate(ctx, active.Binding.SourceClusterID, active.Binding.OperationID)
	if err != nil || loaded.Digest() != initialDigest || currentIdentity != firstIdentity || currentRuntime != firstRuntime {
		t.Fatalf("first current-run readback differs: identity=%+v runtime=%+v err=%v", currentIdentity, currentRuntime, err)
	}
	secondIdentity, secondRuntime := restart(firstIdentity, "restart-2")
	loaded, currentIdentity, currentRuntime, err = store.ReadEngineRepairCurrentCandidate(ctx, active.Binding.SourceClusterID, active.Binding.OperationID)
	if err != nil || loaded.Digest() != initialDigest || currentIdentity != secondIdentity || currentRuntime != secondRuntime {
		t.Fatalf("second current-run readback differs: identity=%+v runtime=%+v err=%v", currentIdentity, currentRuntime, err)
	}

	root, _, err = e.AuthorityState.ReadRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := active.Binding.EngineRepairGenerationKey()
	reference := root.Ledger.EngineRepairs[key]
	if reference.CandidateIdentity != secondIdentity || reference.RecordDigest != initialDigest || len(root.Ledger.Runs) < 3 {
		t.Fatalf("current pointer did not advance while immutable history remained: ref=%+v runs=%d", reference, len(root.Ledger.Runs))
	}
	for _, identity := range []fencing.Identity{active.CandidateIdentity, firstIdentity, secondIdentity} {
		if run, ok := root.Ledger.Runs[identity.RunID]; !ok || run.Identity != identity {
			t.Fatalf("restart history omitted or rewrote Run %q: %+v", identity.RunID, run)
		}
	}

	staleIdentity := secondIdentity
	staleIdentity.RunID = "candidate-run-stale"
	staleRuntime := secondRuntime
	staleRuntime.RunID = staleIdentity.RunID
	if _, err := store.RestartActiveCandidateRuntime(ctx, active.Binding.SourceClusterID, active.Binding.OperationID,
		initialDigest, active.CandidateIdentity.RunID, staleIdentity, staleRuntime); err == nil {
		t.Fatal("stale first incarnation advanced a later current root reference")
	}

	closed, version, err := e.AuthorityState.ReadRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closed.Ledger.ClosedBy = "fixture-close"
	closed.Ledger.ClosedHash = hashBytes([]byte("fixture-close"))
	if err := e.AuthorityState.WriteRoot(ctx, active.Binding.StatefulSetUID, active.Binding.SourceClusterID, closed, version); err != nil {
		t.Fatal(err)
	}
	closedTarget := secondIdentity
	closedTarget.RunID = "candidate-run-after-close"
	closedRuntime := secondRuntime
	closedRuntime.RunID = closedTarget.RunID
	if _, err := store.RestartActiveCandidateRuntime(ctx, active.Binding.SourceClusterID, active.Binding.OperationID,
		initialDigest, secondIdentity.RunID, closedTarget, closedRuntime); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("closed root accepted active candidate restart: %v", err)
	}
}
