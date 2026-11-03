//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type managedGraphStrictAdmissionFixtureChecker struct {
	identity ManagedStartupIdentity
	wal      string
	tip      uint64
}

func (c managedGraphStrictAdmissionFixtureChecker) ValidateManagedStartup(_ context.Context, identity ManagedStartupIdentity) error {
	if identity != c.identity {
		return errors.New("fixture managed startup identity differs")
	}
	return nil
}

func (c managedGraphStrictAdmissionFixtureChecker) ValidateManagedAdmission(ctx context.Context, identity ManagedStartupIdentity, state ManagedRuntimeState) error {
	if err := c.ValidateManagedStartup(ctx, identity); err != nil {
		return err
	}
	if state.WALIdentity != c.wal || state.CertifiedTip != c.tip || state.AppliedTip != c.tip || state.RecorderTip != c.tip || state.ExistingRegisteredWAL {
		return errors.New("fixture managed admission state differs from the exact certified applied prefix")
	}
	return nil
}

func requireManagedGraphStrictAdmissionFixture(fixture *managedGraphAdmissionRaceFixture) {
	fixture.node.managedStartup.Checker = managedGraphStrictAdmissionFixtureChecker{
		identity: fixture.node.managedStartup.Identity,
		wal:      fixture.node.core.WALIdentity(),
		tip:      9,
	}
}

func TestManagedGraphAdmissionAfterControlCertifiesDuringObservation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	releaseObserver := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseObserver)
	var observed atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && observed.CompareAndSwap(false, true) {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	t.Cleanup(restoreObserver)

	var graphProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		_, graph, err := types.DecodeManagedGraphExecution(value)
		if err == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)

	barrier := managedGraphRaceControlValue()
	owner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	pending := fixture.node.core.RuntimeProgressSnapshot()
	if pending.CertifiedTip != 8 || pending.RecorderTip != 9 || !pending.RecorderHashPresent || pending.RecorderHashIsCertified || pending.RecorderHash != quepaxa.ValueHash(sha256.Sum256(barrier)) {
		t.Fatalf("fixture lacks the exact real R=C+1 pending control: %+v", pending)
	}
	if _, decided := fixture.node.core.CertifiedValue(9); decided {
		t.Fatal("pending control was already certified before observation")
	}

	executed := make(chan error, 1)
	go func() {
		executed <- fixture.node.admitInternalApplication(ctx)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Core observer did not reach its deterministic final-snapshot boundary", ctx.Err())
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHash != pending.RecorderHash || progress.RecorderHashIsCertified {
		t.Fatalf("observer gate did not retain the exact uncertified control: %+v", progress)
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual original control did not certify while observation was paused: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != pending.RecorderHash || !bytes.Equal(decision.Value, barrier) {
		t.Fatalf("slot 9 did not certify the exact observed control: found=%t hash=%x", ok, decision.Hash)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 9 || fixture.node.material.Tip() > uint64(progress.CertifiedTip) {
		t.Fatalf("the observed recorder gap did not close cleanly: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}

	releaseObserver()
	select {
	case err := <-executed:
		if err != nil {
			t.Fatalf("Graph write admission rejected after the exact pending value became certified: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Graph write admission did not finish after observer revalidation", ctx.Err())
	}
	if graphProposals.Load() != 0 || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("admission-only fixture did not replay exactly the certified prefix: graph_proposals=%d certified=%d applied=%d", graphProposals.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphInitialAdmissionAfterControlCertifiesDuringObservation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	requireManagedGraphStrictAdmissionFixture(fixture)
	t.Cleanup(fixture.transport.releaseHeld)
	t.Cleanup(func() {
		if binding := fixture.node.stopInternalApplication(); binding != nil {
			binding.active.Wait()
		}
		fixture.node.wg.Wait()
	})
	fixture.node.managedInitiallyAdmitted.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resume) })
	var observed atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && observed.CompareAndSwap(false, true) {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	defer restoreObserver()
	owner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	result := make(chan error, 1)
	go func() { result <- fixture.node.EnsureInitialManagedAdmission(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("initial admission did not reach full observer validation", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual initial control did not certify while validation was paused: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != quepaxa.ValueHash(sha256.Sum256(managedGraphRaceControlValue())) || !bytes.Equal(decision.Value, managedGraphRaceControlValue()) {
		t.Fatalf("initial admission fixture lacks the exact certified control: found=%t decision=%+v", ok, decision)
	}
	resumeOnce.Do(func() { close(resume) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("real initial admission rejected the typed prefix-advance recovery: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("real initial admission did not finish after recheck", ctx.Err())
	}
	if !fixture.node.managedInitiallyAdmitted.Load() || fixture.node.core.Tip() < 9 || fixture.node.material.Tip() < 9 {
		t.Fatalf("initial voter admission did not complete through certified replay: admitted=%t certified=%d applied=%d", fixture.node.managedInitiallyAdmitted.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionRejectsBorrowedBindingDriftAfterPrefixAdvance(t *testing.T) {
	for _, initial := range []bool{false, true} {
		name := "already-admitted"
		if initial {
			name = "initial-admission"
		}
		t.Run(name, func(t *testing.T) { testManagedGraphBindingDriftAfterPrefixAdvance(t, initial) })
	}
}

func testManagedGraphBindingDriftAfterPrefixAdvance(t *testing.T, initial bool) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	if initial {
		requireManagedGraphStrictAdmissionFixture(fixture)
		fixture.node.managedInitiallyAdmitted.Store(false)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resume) })
	var observed atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && observed.CompareAndSwap(false, true) {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	defer restoreObserver()
	var graphProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(proposal []byte) {
		if _, graph, decodeErr := types.DecodeManagedGraphExecution(proposal); decodeErr == nil && graph {
			graphProposals.Add(1)
		}
	})
	defer restoreProposal()
	owner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	type result struct {
		err error
	}
	executed := make(chan result, 1)
	go func() {
		var err error
		if initial {
			err = fixture.node.EnsureInitialManagedAdmission(ctx)
		} else {
			_, err = fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{RequestID: "binding-drift-prefix-advance", Cypher: "CREATE (:MustNotRun {once:true})"})
		}
		executed <- result{err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("observer did not reach full validation before binding drift", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual control did not certify: %v", err)
	}
	original := fixture.node.internalApplication.Load()
	if original == nil {
		t.Fatal("fixture binding disappeared")
	}
	replacement := &internalApplicationBinding{lifetime: original.lifetime, identity: original.identity, policy: original.policy, window: original.window, wal: original.wal, walStore: original.walStore, member: original.member, core: original.core, material: original.material}
	t.Cleanup(func() { fixture.node.internalApplication.Store(original) })
	fixture.node.internalApplication.Store(replacement)
	resumeOnce.Do(func() { close(resume) })
	select {
	case got := <-executed:
		fixture.node.internalApplication.Store(original)
		if got.err == nil {
			t.Fatal("prefix advance admitted through a replacement binding pointer")
		}
	case <-ctx.Done():
		fixture.node.internalApplication.Store(original)
		t.Fatal("binding-drift admission did not return", ctx.Err())
	}
	if graphProposals.Load() != 0 || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 8 {
		t.Fatalf("binding drift allowed application work or replay: proposals=%d certified=%d applied=%d", graphProposals.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionAfterObjectCheckpointCertifiesDuringObservation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := fixture.node.archive.SyncThrough(ctx, fixture.node.core, 8); err != nil {
		t.Fatalf("archive actual certified prefix: %v", err)
	}
	files, cut, releaseFiles, err := fixture.node.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFiles()
	if cut != 8 || len(files) != 2 {
		t.Fatalf("fixture lacks the actual paired Graph cut: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := fixture.node.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatalf("upload actual Graph checkpoint pair: %v", err)
	}
	prefix, ok := fixture.node.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual candidate cut prefix is missing")
	}
	next, following, err := fixture.node.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.transport.graphReviewTransport["n2"].PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("persist real prepare marker for object-backed QCKP: %v", err)
	}
	owner := fixture.startHeldValue(ctx, value)
	fixture.awaitPendingControl(t)
	pending := fixture.node.core.RuntimeProgressSnapshot()
	if pending.CertifiedTip != 8 || pending.RecorderTip != 9 || !pending.RecorderHashPresent || pending.RecorderHashIsCertified || pending.RecorderHash != quepaxa.ValueHash(sha256.Sum256(value)) {
		t.Fatalf("fixture lacks the actual pending object-backed QCKP: %+v", pending)
	}

	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resume) })
	var observed atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && observed.CompareAndSwap(false, true) {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	defer restoreObserver()
	var graphProposals, barriers atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(proposal []byte) {
		if _, graph, decodeErr := types.DecodeManagedGraphExecution(proposal); decodeErr == nil && graph {
			graphProposals.Add(1)
		}
		if isBarrier, decodeErr := types.DecodeReadBarrier(proposal); decodeErr == nil && isBarrier {
			barriers.Add(1)
		}
	})
	defer restoreProposal()
	executed := make(chan error, 1)
	go func() { executed <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("observer did not reach full object-backed QCKP final recheck", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual object-backed QCKP did not certify: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != pending.RecorderHash || !bytes.Equal(decision.Value, value) {
		t.Fatalf("slot 9 lacks the exact actual QCKP certificate: found=%t hash=%x", ok, decision.Hash)
	}
	resumeOnce.Do(func() { close(resume) })
	select {
	case err := <-executed:
		if err != nil {
			t.Fatalf("Graph admission did not continue through existing guards after QCKP recheck: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Graph admission did not finish after QCKP certification", ctx.Err())
	}
	if graphProposals.Load() != 0 || barriers.Load() != 0 || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("fallback proposed extra work or failed to replay: graph_proposals=%d barriers=%d certified=%d applied=%d", graphProposals.Load(), barriers.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionKeepsUncertifiedOrdinaryAndCanceledRequestsClosed(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{
		RequestID: "ordinary-not-control",
		Cypher:    "CREATE (:OrdinaryNotControl {once:true})",
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(value)
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(hash), Value: bytes.Clone(value)}
	if _, err := fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("record actual unquorate Graph proposal locally: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 8 || progress.RecorderTip != 9 || !progress.RecorderHashPresent || progress.RecorderHashIsCertified || progress.RecorderHash != quepaxa.ValueHash(hash) {
		t.Fatalf("ordinary Graph value did not remain an uncertified next-slot record: %+v", progress)
	}
	if _, decided := fixture.node.core.CertifiedValue(9); decided {
		t.Fatal("ordinary Graph proposal unexpectedly has a certificate")
	}

	var graphProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		_, graph, decodeErr := types.DecodeManagedGraphExecution(value)
		if decodeErr == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)
	response, executeErr := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{
		RequestID: "foreground-after-ordinary-record",
		Cypher:    "CREATE (:ForegroundMustNotPass {once:true})",
	})
	if executeErr == nil || response.MutationReceipt.Slot != 0 {
		t.Fatalf("uncertified ordinary recorder value admitted a Graph request: response=%+v error=%v", response, executeErr)
	}
	if graphProposals.Load() != 0 {
		t.Fatalf("Graph application proposed despite an uncertified ordinary record: count=%d", graphProposals.Load())
	}

	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	response, executeErr = fixture.node.ExecuteManagedGraph(canceled, types.GraphCommand{
		RequestID: "canceled-after-ordinary-record",
		Cypher:    "CREATE (:CanceledMustNotPass {once:true})",
	})
	if !errors.Is(executeErr, context.Canceled) || response.MutationReceipt.Slot != 0 {
		t.Fatalf("cancellation did not remain fail-closed: response=%+v error=%v", response, executeErr)
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHashIsCertified || fixture.node.material.Tip() != 8 {
		t.Fatalf("ordinary unquorate state advanced during rejected/canceled admission: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	if graphProposals.Load() != 0 {
		t.Fatalf("canceled Graph request was proposed: count=%d", graphProposals.Load())
	}
}
