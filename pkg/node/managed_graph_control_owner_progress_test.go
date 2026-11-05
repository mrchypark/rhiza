//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphAdmissionAcceptsSuccessfulOwnedBarrierBelowLaterCertifiedTip(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	durabilityReached := make(chan struct{})
	releaseDurability := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDurability) }) }
	t.Cleanup(release)
	fixture.node.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 9 {
			return nil
		}
		close(durabilityReached)
		select {
		case <-releaseDurability:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	select {
	case <-durabilityReached:
	case <-ctx.Done():
		t.Fatal("owned control did not reach actual durability boundary", ctx.Err())
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("durability hold is not post-certificate/apply: C=%d A=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	ownedDecision, ok := fixture.node.core.CertifiedValue(9)
	if !ok {
		t.Fatal("actual n1 slot-9 owner was not certified before durability hold")
	}
	if isBarrier, err := types.DecodeReadBarrier(ownedDecision.Value); err != nil || !isBarrier {
		t.Fatalf("slot-9 owner is not a certified pure barrier: barrier=%t err=%v", isBarrier, err)
	}

	var nextNonce [types.ReadBarrierNonceSize]byte
	for i := range nextNonce {
		nextNonce[i] = byte(90 + i)
	}
	nextValue := types.EncodeReadBarrier(nextNonce)
	type proposalResult struct {
		slot quepaxa.Slot
		err  error
	}
	remoteNext := make(chan proposalResult, 1)
	go func() {
		slot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, nextValue)
		remoteNext <- proposalResult{slot: slot, err: err}
	}()
	select {
	case result := <-remoteNext:
		if result.err != nil || result.slot != 10 {
			t.Fatalf("other voter did not certify the actual next slot: slot=%d err=%v", result.slot, result.err)
		}
	case <-ctx.Done():
		t.Fatal("other voter did not finish slot-10 barrier", ctx.Err())
	}
	if fixture.node.core.Tip() != 10 {
		t.Fatalf("n1 did not learn the later certificate before owner completion: C=%d", fixture.node.core.Tip())
	}
	later, ok := fixture.node.core.CertifiedValue(10)
	if !ok || !bytes.Equal(later.Value, nextValue) {
		t.Fatalf("n1 did not retain the exact actual slot-10 certificate: ok=%t decision=%+v", ok, later)
	}
	shortDone := make(chan struct{})
	close(shortDone)
	shortRemoteOperation := &managedGraphControlOperation{
		value: bytes.Clone(ownedDecision.Value), hash: sha256.Sum256(ownedDecision.Value),
		done: shortDone, slot: 9,
	}
	if err := fixture.node.waitManagedAppendOnlyBarrier(ctx, 10, shortRemoteOperation, shortDone); err == nil {
		t.Fatal("a valid slot-9 certificate satisfied the actual slot-10 remote minimum")
	}

	waiterEntered := make(chan struct{})
	var waiterOnce sync.Once
	restoreWaiter := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:waiter-entered" {
			waiterOnce.Do(func() { close(waiterEntered) })
		}
	})
	t.Cleanup(restoreWaiter)
	foreground := make(chan error, 1)
	go func() { foreground <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-waiterEntered:
	case err := <-foreground:
		t.Fatalf("foreground returned before joining the successful slot-9 owner: %v", err)
	case <-ctx.Done():
		t.Fatal("foreground did not reach the concrete owner waiter", ctx.Err())
	}
	release()
	select {
	case err := <-owner:
		if err != nil {
			t.Fatalf("successful slot-9 owner failed after durability release: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("slot-9 owner did not finish after durability release", ctx.Err())
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground rejected successful owner slot 9 after later certified slot 10: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("foreground did not finish after owner completion", ctx.Err())
	}
	if fixture.node.core.Tip() != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("successful owner join failed to preserve and replay the later certificate: C=%d A=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphPostReadIndexJoinsExactObservedBarrierOwner(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(fixture.transport.releaseHeld)
	owner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)

	binding := fixture.node.internalApplication.Load()
	if binding == nil {
		t.Fatal("managed application binding is unavailable")
	}
	observedAdmission := make(chan error, 1)
	var proposalEntries atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if barrier, decodeErr := types.DecodeReadBarrier(value); decodeErr == nil && barrier {
			proposalEntries.Add(1)
		}
	})
	t.Cleanup(restoreProposal)
	firstWaiting := make(chan struct{})
	var firstWaitOnce sync.Once
	restoreFirstWait := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:waiter-entered" {
			firstWaitOnce.Do(func() { close(firstWaiting) })
		}
	})
	t.Cleanup(restoreFirstWait)
	go func() { observedAdmission <- fixture.node.waitManagedAppendOnlyControl(ctx, binding) }()
	awaitManagedGraphAdmissionOwner(t, fixture, observedAdmission, 9)
	select {
	case <-firstWaiting:
	case <-ctx.Done():
		t.Fatal("first owner waiter did not enter", ctx.Err())
	}
	restoreFirstWait()
	fixture.node.managedGraphControlMu.Lock()
	operation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if operation == nil || operation.binding != binding {
		t.Fatalf("Node did not retain the sealed R=9 observation: %+v", operation)
	}

	readIndex, _, err := fixture.node.core.ReadIndex(ctx)
	if err != nil || readIndex != 8 {
		t.Fatalf("actual pre-certificate ReadIndex returned %d, err=%v; want C=8", readIndex, err)
	}
	selected, resume := make(chan struct{}), make(chan struct{})
	var selectedOnce sync.Once
	restoreHook := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:waiter-entered" {
			selectedOnce.Do(func() { close(selected) })
			<-resume
		}
	})
	t.Cleanup(func() {
		restoreHook()
		select {
		case <-resume:
		default:
			close(resume)
		}
	})
	joined := make(chan error, 1)
	go func() { joined <- fixture.node.waitManagedAppendOnlyControl(ctx, binding) }()
	select {
	case <-selected:
	case err := <-joined:
		t.Fatalf("post-ReadIndex owner selection returned before its deterministic hook: %v", err)
	case <-ctx.Done():
		t.Fatal("post-ReadIndex waiter did not select the exact observed owner", ctx.Err())
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual slot-9 owner failed: %v", err)
	}
	select {
	case err := <-observedAdmission:
		if err != nil {
			progress := fixture.node.core.RuntimeProgressSnapshot()
			isBarrier, decodeErr := types.DecodeReadBarrier(operation.value)
			t.Fatalf("owner rejected certified R=9: wait_err=%v worker_err=%v slot=%d barrier=%t decode_err=%v hash_matches=%t C=%d R=%d", err, operation.err, operation.slot, isBarrier, decodeErr, operation.hash == quepaxa.ValueHash(sha256.Sum256(operation.value)), progress.CertifiedTip, progress.RecorderTip)
		}
	case <-ctx.Done():
		t.Fatal("observation worker did not complete after R=9 certification", ctx.Err())
	}
	if operation.slot <= 9 || operation.err != nil || bytes.Equal(operation.value, managedGraphRaceControlValue()) || proposalEntries.Load() != 1 {
		t.Fatalf("observed foreign R=9 control was not followed by exactly one own-nonce barrier: operation=%+v barrier_proposals=%d", operation, proposalEntries.Load())
	}
	close(resume)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("post-ReadIndex waiter rejected its exact observed R=9 certificate: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("post-ReadIndex waiter did not join the observed owner", ctx.Err())
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || !bytes.Equal(decision.Value, managedGraphRaceControlValue()) {
		t.Fatalf("observed owner did not retain its exact original R=9 certificate: ok=%t decision=%+v", ok, decision)
	}
	owned, ok := fixture.node.core.CertifiedValue(operation.slot)
	if !ok || owned.Hash != operation.hash || !bytes.Equal(owned.Value, operation.value) {
		t.Fatalf("observed owner lacks its own exact S=%d barrier certificate: ok=%t decision=%+v", operation.slot, ok, owned)
	}
	if fixture.node.core.Tip() != operation.slot || fixture.node.material.Tip() != uint64(operation.slot) {
		t.Fatalf("post-ReadIndex own-certificate join failed to replay the observed suffix: certified=%d applied=%d owner_slot=%d", fixture.node.core.Tip(), fixture.node.material.Tip(), operation.slot)
	}
}

func TestManagedGraphPostReadIndexAdoptsOriginalOrdinaryAtOriginalSlot(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(fixture.transport.releaseHeld)
	foreignOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)

	binding := fixture.node.internalApplication.Load()
	if binding == nil {
		t.Fatal("managed application binding is unavailable")
	}
	readIndex, _, err := fixture.node.core.ReadIndex(ctx)
	if err != nil || readIndex != 8 {
		t.Fatalf("actual pre-certificate ReadIndex returned %d, err=%v; want C=8", readIndex, err)
	}
	ownedProposalEntered := make(chan []byte, 1)
	ownedProposalRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseProposal := func() { releaseOnce.Do(func() { close(ownedProposalRelease) }) }
	t.Cleanup(releaseProposal)
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		barrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr != nil || !barrier || bytes.Equal(value, managedGraphRaceControlValue()) {
			return
		}
		ownedProposalEntered <- bytes.Clone(value)
		<-ownedProposalRelease
	})
	t.Cleanup(restoreProposal)
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- fixture.node.waitManagedAppendOnlyControl(ctx, binding) }()
	awaitManagedGraphAdmissionOwner(t, fixture, ownerResult, 9)
	fixture.node.managedGraphControlMu.Lock()
	operation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if operation == nil || operation.binding != binding {
		t.Fatalf("Node did not retain the exact original R=9 observation: %+v", operation)
	}
	select {
	case value := <-ownedProposalEntered:
		if !bytes.Equal(value, operation.value) || operation.slot != 0 {
			t.Fatalf("proposal hold did not capture the unreserved owned barrier: matches=%t slot=%d", bytes.Equal(value, operation.value), operation.slot)
		}
	case <-ctx.Done():
		t.Fatal("owned barrier did not reach the proposal-entry hold", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, foreignOwner); err != nil {
		t.Fatalf("actual foreign R=9 barrier did not certify: %v", err)
	}
	command := types.GraphCommand{
		RequestID: "post-readindex-fresh-ordinary-gap",
		Cypher:    "CREATE (:PostReadIndexFreshOrdinary {original:true})",
	}
	ordinary, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryHash := quepaxa.ValueHash(sha256.Sum256(ordinary))
	for _, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		if _, err := fixture.transport.graphReviewTransport[id].Record(ctx, quepaxa.RecordRequest{
			Slot: 10, Step: 4, ConfigID: 1,
			Proposal: quepaxa.Proposal{ProposerID: "n2", Hash: ordinaryHash, Value: bytes.Clone(ordinary)},
		}); err != nil {
			t.Fatalf("record actual ordinary R=10 at %s: %v", id, err)
		}
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || !progress.RecorderHashPresent || progress.RecorderHashIsCertified || progress.RecorderHash != ordinaryHash {
		t.Fatalf("fixture lacks the exact fresh ordinary R=10 gap: %+v", progress)
	}
	stored, found := fixture.node.core.Value(ordinaryHash)
	if !found || !bytes.Equal(stored, ordinary) {
		t.Fatal("ordinary R=10 original bytes were not retained")
	}
	joined := make(chan error, 1)
	go func() { joined <- fixture.node.waitManagedAppendOnlyControl(ctx, binding) }()
	select {
	case err := <-joined:
		t.Fatalf("caller did not wait for actual owner and X10 certificate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseProposal()
	select {
	case err := <-ownerResult:
		if err != nil {
			t.Fatalf("ordinary consensus did not finish X10 and its own barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("ordinary consensus did not complete original X10", ctx.Err())
	}
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("joined owner did not finish original X10: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("joined owner did not complete", ctx.Err())
	}
	decision, certified := fixture.node.core.CertifiedValue(10)
	if !certified || decision.Hash != ordinaryHash || !bytes.Equal(decision.Value, ordinary) || len(decision.Certificate) == 0 {
		t.Fatalf("X10 did not retain its original bytes and certificate: certified=%t decision=%+v", certified, decision)
	}
	barrier, certified := fixture.node.core.CertifiedValue(operation.slot)
	if !certified || operation.slot <= 10 || !managedGraphControlCertificateMatches(fixture.node.core, operation, barrier) {
		t.Fatalf("owner lacks its separate exact barrier certificate: slot=%d certified=%t", operation.slot, certified)
	}
	progress = fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip < operation.slot || fixture.node.material.Tip() < uint64(operation.slot) {
		t.Fatalf("owner returned before contiguous apply: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil || !found || !matches || !receipt.Applied || receipt.Slot != 10 {
		t.Fatalf("original X10 receipt differs: receipt=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
	}
	query, err := fixture.node.material.GraphQuery(ctx, "MATCH (n:PostReadIndexFreshOrdinary) RETURN n.original", nil)
	if err != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] != true {
		t.Fatalf("original X10 was not applied exactly once: rows=%v err=%v", query.Rows, err)
	}
	decisions, _, err := fixture.node.core.DecisionsFrom(9, int(progress.CertifiedTip-9)+1)
	if err != nil {
		t.Fatal(err)
	}
	applications := 0
	for _, decided := range decisions {
		if bytes.Equal(decided.Value, ordinary) {
			applications++
			if decided.Slot != 10 {
				t.Fatalf("original application was resubmitted at slot %d", decided.Slot)
			}
		}
	}
	if applications != 1 {
		t.Fatalf("original application appeared at %d slots, want exactly one", applications)
	}
	if receipt, found, _, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, "post-readindex-never-submitted", fingerprint); err != nil || found {
		t.Fatalf("consensus recovery fabricated another request receipt: receipt=%+v found=%t err=%v", receipt, found, err)
	}
}

func TestManagedGraphAdmissionStartsFreshOwnerAfterCompletedRemoteControl(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	firstOperation := completeRemoteObservedBarrierForStalePointerTest(t, fixture, ctx)

	var nextNonce [types.ReadBarrierNonceSize]byte
	for i := range nextNonce {
		nextNonce[i] = byte(111 + i)
	}
	nextValue := types.EncodeReadBarrier(nextNonce)
	recordRemotePendingControl(t, fixture, ctx, 11, nextValue)
	awaitManagedGraphActualRecorderGap(t, fixture, 11, nextValue)

	secondOperationReady := make(chan struct{})
	var secondOperationMu sync.Mutex
	var secondOperation *managedGraphControlOperation
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, err := types.DecodeReadBarrier(value)
		if err != nil || !isBarrier {
			return
		}
		fixture.node.managedGraphControlMu.Lock()
		candidate := fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		if candidate == nil || candidate.binding != fixture.node.internalApplication.Load() {
			return
		}
		secondOperationMu.Lock()
		if secondOperation == nil {
			secondOperation = candidate
			close(secondOperationReady)
		}
		secondOperationMu.Unlock()
	})
	t.Cleanup(restoreProposal)
	secondAdmission := make(chan error, 1)
	go func() { secondAdmission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-secondOperationReady:
	case <-ctx.Done():
		t.Fatal("fresh R=11 observation did not launch a Node-owned barrier", ctx.Err())
	}
	secondOperationMu.Lock()
	operation := secondOperation
	secondOperationMu.Unlock()
	select {
	case err := <-secondAdmission:
		if err != nil {
			t.Fatalf("second admission failed after its fresh R=11 observation and certified continuation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("second admission did not complete after R=11 certification", ctx.Err())
	}
	decision11, ok := fixture.node.core.CertifiedValue(11)
	if !ok {
		t.Fatal("second observation did not close the actual R=11 certificate")
	}
	isBarrier, decodeErr := types.DecodeReadBarrier(decision11.Value)
	if decodeErr != nil || !isBarrier || operation.slot < 11 {
		t.Fatalf("second observation's exact-slot/later barrier contract differs: decode_err=%v is_barrier=%t operation=%+v", decodeErr, isBarrier, operation)
	}
	decision, ok := fixture.node.core.CertifiedValue(operation.slot)
	if !ok || decision.Hash != operation.hash || !bytes.Equal(decision.Value, operation.value) {
		t.Fatalf("second observation's Node-owned barrier lacks its exact certificate: ok=%t decision=%+v operation=%+v", ok, decision, operation)
	}
	if fixture.node.material.Tip() < uint64(operation.slot) {
		t.Fatalf("second admission returned before replaying its Node-owned continuation: applied=%d", fixture.node.material.Tip())
	}
	if fixture.node.core.Tip() < 10 || fixture.node.material.Tip() < 10 || firstOperation.slot != 10 {
		t.Fatalf("completed first prefix was not preserved: first=%+v certified=%d applied=%d", firstOperation, fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionRecoversOrdinaryGapAfterCompletedRemoteControl(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	firstOperation := completeRemoteObservedBarrierForStalePointerTest(t, fixture, ctx)

	command := types.GraphCommand{RequestID: "stale-operation-ordinary-gap", Cypher: "CREATE (:RecoveredPending {id:'ordinary'})"}
	ordinaryValue, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	originalHash := quepaxa.ValueHash(sha256.Sum256(ordinaryValue))
	// A local minority record need not win. Persist the original Step-5
	// proposal on a real quorum before the next normal proposal starts.
	for _, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		summary, err := fixture.transport.SendRecord(ctx, id, quepaxa.RecordRequest{Slot: 11, Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: "n2", Hash: originalHash, Value: bytes.Clone(ordinaryValue)}})
		if err != nil || summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != originalHash {
			t.Fatalf("persist original R11 on real recorder %s: summary=%+v err=%v", id, summary, err)
		}
	}
	if _, certified := fixture.node.core.CertifiedValue(11); certified {
		t.Fatal("fixture manufactured a certificate before normal consensus")
	}
	awaitManagedGraphActualRecorderGap(t, fixture, 11, ordinaryValue)

	var barrierProposals atomic.Int32
	var ownBarrier []byte
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if barrier, err := types.DecodeReadBarrier(value); err == nil && barrier {
			barrierProposals.Add(1)
			ownBarrier = bytes.Clone(value)
		}
	})
	t.Cleanup(restoreProposal)
	result := make(chan error, 1)
	go func() { result <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("ordinary R=11 consensus recovery failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("ordinary R=11 recovery did not complete", ctx.Err())
	}
	decision, found := fixture.node.core.CertifiedValue(11)
	if !found || !bytes.Equal(decision.Value, ordinaryValue) || len(decision.Certificate) == 0 {
		t.Fatalf("ordinary R=11 original certificate differs: found=%t decision=%+v", found, decision)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if barrierProposals.Load() != 1 || progress.CertifiedTip <= 11 || fixture.node.material.Tip() < uint64(progress.CertifiedTip) {
		t.Fatalf("fresh owner lacks its own barrier and replay: proposals=%d progress=%+v applied=%d", barrierProposals.Load(), progress, fixture.node.material.Tip())
	}
	ownedSlot, found := fixture.node.core.DecidedSlot(ownBarrier)
	owned, certified := fixture.node.core.CertifiedValue(ownedSlot)
	if !found || !certified || ownedSlot <= 11 || !bytes.Equal(owned.Value, ownBarrier) || len(owned.Certificate) == 0 {
		t.Fatalf("new owner lacks its own exact certificate: slot=%d found=%t certified=%t", ownedSlot, found, certified)
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil || !found || !matches || !receipt.Applied || receipt.Slot != 11 {
		t.Fatalf("original receipt differs: receipt=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
	}
	rows, err := fixture.node.material.GraphQuery(ctx, "MATCH (n:RecoveredPending) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "ordinary" {
		t.Fatalf("original R11 was not applied once: rows=%v err=%v", rows.Rows, err)
	}
	decisions, _, err := fixture.node.core.DecisionsFrom(11, int(progress.CertifiedTip-11)+1)
	if err != nil {
		t.Fatal(err)
	}
	originals := 0
	for _, retained := range decisions {
		if bytes.Equal(retained.Value, ordinaryValue) {
			originals++
			if retained.Slot != 11 {
				t.Fatalf("original R11 was resubmitted at %d", retained.Slot)
			}
		}
	}
	if originals != 1 {
		t.Fatalf("original R11 appeared at %d slots", originals)
	}
	if _, found, _, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, "never-submitted-after-completion", fingerprint); err != nil || found {
		t.Fatalf("normal recovery fabricated another request receipt: found=%t err=%v", found, err)
	}
	if firstOperation.slot != 10 {
		t.Fatalf("initial completed-operation schedule was not actual R=9/S=10: %+v", firstOperation)
	}
}

func completeRemoteObservedBarrierForStalePointerTest(t *testing.T, fixture *managedGraphAdmissionRaceFixture, ctx context.Context) *managedGraphControlOperation {
	t.Helper()
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	durabilityReached := make(chan struct{})
	releaseDurability := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseDurability) }) })
	fixture.node.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 10 {
			return nil
		}
		close(durabilityReached)
		select {
		case <-releaseDurability:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	remoteOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	firstAdmission := make(chan error, 1)
	go func() { firstAdmission <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionOwner(t, fixture, firstAdmission, 9)
	fixture.node.managedGraphControlMu.Lock()
	firstOperation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if firstOperation == nil || firstOperation.binding != fixture.node.internalApplication.Load() {
		t.Fatalf("first admission did not retain its actual R=9 observation: %+v", firstOperation)
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, remoteOwner); err != nil {
		t.Fatalf("actual remote slot-9 control did not certify: %v", err)
	}
	select {
	case <-durabilityReached:
	case <-ctx.Done():
		t.Fatal("Node-owned slot-10 barrier did not reach its actual durability boundary", ctx.Err())
	}
	firstDecision, ok := fixture.node.core.CertifiedValue(10)
	isBarrier, decodeErr := types.DecodeReadBarrier(firstDecision.Value)
	if !ok || decodeErr != nil || !isBarrier {
		t.Fatalf("first observation's Node-owned slot-10 barrier is not certified: ok=%t decision=%+v", ok, firstDecision)
	}
	// Unblock the real Node transport request before waiting for its owner.
	releaseOnce.Do(func() { close(releaseDurability) })
	select {
	case err := <-firstAdmission:
		if err != nil {
			t.Fatalf("first admission did not finish after actual R=9/S=10 certification: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first admission did not finish after the slot-10 durability release", ctx.Err())
	}
	if firstOperation.slot != 10 || firstOperation.err != nil {
		t.Fatalf("first success did not complete its actual R=9/S=10 observation: %+v", firstOperation)
	}
	select {
	case <-firstOperation.done:
	default:
		t.Fatal("first successful operation was not completed")
	}
	fixture.node.managedGraphControlMu.Lock()
	registered := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if registered != nil && registered != firstOperation {
		t.Fatalf("first completion was replaced by an unrelated registered operation: old=%p current=%p", firstOperation, registered)
	}
	return firstOperation
}

func awaitManagedGraphActualRecorderGap(t *testing.T, fixture *managedGraphAdmissionRaceFixture, slot quepaxa.Slot, want []byte) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		progress := fixture.node.core.RuntimeProgressSnapshot()
		if progress.RecorderTip == slot && progress.CertifiedTip+1 == slot && progress.RecorderHashPresent {
			value, found := fixture.node.core.Value(progress.RecorderHash)
			if found && bytes.Equal(value, want) && !progress.RecorderHashIsCertified {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	t.Fatalf("n1 did not receive the exact actual remote slot-%d recorder value: progress=%+v", slot, progress)
}

func recordRemotePendingControl(t *testing.T, fixture *managedGraphAdmissionRaceFixture, ctx context.Context, slot quepaxa.Slot, value []byte) {
	t.Helper()
	proposal := quepaxa.Proposal{
		ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(value)), Value: bytes.Clone(value),
	}
	if _, err := fixture.transport.SendRecord(ctx, "n1", quepaxa.RecordRequest{Slot: slot, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("record actual remote control at slot %d through n1's Record endpoint: %v", slot, err)
	}
}
