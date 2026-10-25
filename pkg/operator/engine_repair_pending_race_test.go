package operator

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func engineRepairPendingRaceProposal(record fencing.EngineRepairRecord, ordinal int, suffix string) fencing.PendingEngineRepairProposal {
	proposal := pendingEngineRepairProposalForRecord(record)
	proposal.StableOrdinal = ordinal
	proposal.BootstrapIdentity.PodName = "candidate-" + suffix
	proposal.BootstrapIdentity.PodUID = "replacement-pod-" + suffix
	proposal.BootstrapIdentity.RunID = "replacement-run-" + suffix
	proposal.CandidateIdentity.NodeID = "candidate-" + suffix
	proposal.CandidateIdentity.PodName = proposal.BootstrapIdentity.PodName
	proposal.CandidateIdentity.PodUID = proposal.BootstrapIdentity.PodUID
	proposal.CandidateIdentity.RunID = proposal.BootstrapIdentity.RunID
	proposal.CandidateIdentity.WALIdentity = "candidate-wal-" + suffix
	proposal.CandidateMember.ID = quepaxa.NodeID(proposal.CandidateIdentity.NodeID)
	proposal.CandidateMember.WALIdentity = proposal.CandidateIdentity.WALIdentity
	proposal.LocalIdentityHash = strings.Repeat("c", 64)
	return proposal
}

func TestPendingEngineRepairLostCASResponseIsIdempotentAndConflictsStayRejected(t *testing.T) {
	e, _, record := engineRepairStoreFixture(t)
	proposal := engineRepairPendingRaceProposal(record, 0, "lost-response")

	faultBucket := &mutationCASReplyFaultBucket{Bucket: e.AuthorityState.bucket, loseCASReply: true}
	authority, err := NewAuthorityStateStore(faultBucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewEngineRepairStore(authority, record.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}

	created, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal)
	if err != nil {
		t.Fatalf("lost root-CAS response was not resolved by exact canonical readback: %v", err)
	}
	retried, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal)
	if err != nil || retried != created {
		t.Fatalf("same physical request retry changed the pending reservation: equal=%t err=%v", retried == created, err)
	}

	conflict := proposal
	conflict.LocalIdentityHash = strings.Repeat("e", 64)
	if _, err := store.CreateOrReadPendingEngineRepair(t.Context(), conflict); err == nil {
		t.Fatal("different request for the same physical RunID/slot replaced the pinned request")
	}
	root, _, err := authority.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Ledger.PendingEngineRepairs) != 1 || root.Ledger.PendingEngineRepairs[created.SlotKey()] != created {
		t.Fatalf("lost-response retry or conflict changed canonical pending state: %+v", root.Ledger.PendingEngineRepairs)
	}
	if _, exists := root.Ledger.Runs[proposal.BootstrapIdentity.RunID]; exists {
		t.Fatal("pending request created an executable Run before adoption")
	}
}

func TestConcurrentPendingEngineRepairsAtCapacityAdmitOnlyOne(t *testing.T) {
	e, _, record := engineRepairStoreFixture(t)
	state, version, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; len(state.Ledger.Runs) < fencing.MaxEngineRepairIncarnations-1; index++ {
		nodeID := fmt.Sprintf("capacity-filler-%02d", index)
		identity := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: nodeID, PodName: nodeID,
			PodUID: nodeID + "-pod", RunID: nodeID + "-run"}
		runtime := fencing.RuntimeBinding{
			AuthorityID: "capacity-authority", NamespaceUID: "capacity-namespace", BindingUID: "capacity-root",
			StatefulSetUID: record.Binding.StatefulSetUID, SourceClusterID: identity.SourceClusterID,
			NodeID: identity.NodeID, PodName: identity.PodName, PodUID: identity.PodUID, RunID: identity.RunID,
			NodeName: "capacity-worker", NodeUID: "capacity-worker-uid", MachineID: nodeID + "-machine",
			BootID: nodeID + "-boot", RuntimeID: nodeID + "-runtime", SandboxID: nodeID + "-sandbox",
			ContainerID: nodeID + "-container", ImageID: "capacity-image",
		}
		if err := state.Ledger.RegisterEnrolledWithRole(identity, runtime, fencing.StartupRoleVoterActive); err != nil {
			t.Fatalf("register trusted canonical capacity filler %d: %v", index, err)
		}
	}
	if len(state.Ledger.Runs) != fencing.MaxEngineRepairIncarnations-1 {
		t.Fatalf("fixture has %d canonical runs, want %d", len(state.Ledger.Runs), fencing.MaxEngineRepairIncarnations-1)
	}
	if err := e.AuthorityState.WriteRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, state, version); err != nil {
		t.Fatalf("persist 63-run capacity fixture: %v", err)
	}

	bucket := &pauseEngineRepairRootCASBucket{Bucket: e.AuthorityState.bucket, started: make(chan struct{}), release: make(chan struct{})}
	bucket.armed.Store(true)
	authority, err := NewAuthorityStateStore(bucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewEngineRepairStore(authority, record.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	proposals := []fencing.PendingEngineRepairProposal{
		engineRepairPendingRaceProposal(record, 0, "capacity-a"),
		engineRepairPendingRaceProposal(record, 1, "capacity-b"),
	}
	type result struct {
		request fencing.PendingEngineRepairRequest
		err     error
	}
	results := make(chan result, len(proposals))
	go func() {
		request, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposals[0])
		results <- result{request: request, err: err}
	}()
	select {
	case <-bucket.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first capacity reservation did not reach root CAS")
	}
	go func() {
		request, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposals[1])
		results <- result{request: request, err: err}
	}()
	var second result
	select {
	case second = <-results:
	case <-time.After(5 * time.Second):
		bucket.release <- struct{}{}
		t.Fatal("second capacity reservation did not finish while first CAS was paused")
	}
	var releaseOnce sync.Once
	releaseOnce.Do(func() { close(bucket.release) })
	var first result
	select {
	case first = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("first capacity reservation did not retry after concurrent CAS")
	}

	accepted := 0
	for _, outcome := range []result{first, second} {
		if outcome.err == nil {
			accepted++
		} else if !strings.Contains(outcome.err.Error(), "capacity") &&
			!strings.Contains(outcome.err.Error(), "source already has a pending engine repair") {
			t.Fatalf("losing request failed for neither capacity nor the source-level single-pending invariant: %v", outcome.err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d concurrent pending requests at one remaining capacity slot, want exactly one", accepted)
	}
	root, _, err := authority.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	used, err := root.Ledger.EngineRepairCapacityUse()
	if err != nil || used != fencing.MaxEngineRepairIncarnations || len(root.Ledger.PendingEngineRepairs) != 1 {
		t.Fatalf("capacity root used=%d pending=%d err=%v, want 64 and one pending request", used, len(root.Ledger.PendingEngineRepairs), err)
	}
	overCapacity := root.Ledger
	if err := overCapacity.Register(fencing.Identity{SourceClusterID: record.Binding.SourceClusterID,
		NodeID: "capacity-overflow", PodName: "capacity-overflow", PodUID: "capacity-overflow-pod", RunID: "capacity-overflow-run"}); err == nil {
		t.Fatal("canonical root admitted a 65th repair incarnation")
	}
}

func TestPendingEngineRepairAndBootstrapRunRegistrationAreMutuallyExclusive(t *testing.T) {
	t.Run("reservation first blocks run registration", func(t *testing.T) {
		e, store, record := engineRepairStoreFixture(t)
		proposal := engineRepairPendingRaceProposal(record, 0, "registration-order")
		if _, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal); err != nil {
			t.Fatal(err)
		}
		identity := proposal.CandidateIdentity
		identity.WALIdentity = ""
		if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Register(identity)
		}); err == nil {
			t.Fatal("pending bootstrap RunID was registered before adoption")
		}
	})

	t.Run("run registration first blocks reservation", func(t *testing.T) {
		e, store, record := engineRepairStoreFixture(t)
		proposal := engineRepairPendingRaceProposal(record, 0, "registered-first")
		identity := proposal.CandidateIdentity
		identity.WALIdentity = ""
		if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Register(identity)
		}); err != nil {
			t.Fatalf("register candidate RunID first: %v", err)
		}
		if _, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal); err == nil {
			t.Fatal("pending request reused an already registered physical RunID")
		}
	})
}

func TestPendingEngineRepairCloseRaceDoesNotCreateReservationAfterRootClose(t *testing.T) {
	e, _, record := engineRepairStoreFixture(t)
	baseBucket := e.AuthorityState.bucket
	bucket := &pauseEngineRepairRootCASBucket{Bucket: baseBucket, started: make(chan struct{}), release: make(chan struct{})}
	bucket.armed.Store(true)
	authority, err := NewAuthorityStateStore(bucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	contenderStore, err := NewEngineRepairStore(authority, record.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	contender := engineRepairPendingRaceProposal(record, 1, "raced-with-close")
	contenderRequest, err := fencing.NewPendingEngineRepairRequest(contender)
	if err != nil {
		t.Fatalf("build exact contender reference for race assertion: %v", err)
	}
	type result struct {
		request fencing.PendingEngineRepairRequest
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		request, err := contenderStore.CreateOrReadPendingEngineRepair(t.Context(), contender)
		resultCh <- result{request: request, err: err}
	}()
	select {
	case <-bucket.started:
	case <-time.After(5 * time.Second):
		t.Fatal("contending pending request did not reach root CAS")
	}

	closer, err := NewAuthorityStateStore(baseBucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	root, version, err := closer.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	root.Ledger.ClosedBy = "fixture-disaster-close"
	if err := closer.WriteRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, root, version); err != nil {
		t.Fatalf("close root before contending reservation CAS: %v", err)
	}
	var releaseOnce sync.Once
	releaseOnce.Do(func() { close(bucket.release) })
	contending := <-resultCh
	if !errors.Is(contending.err, fencing.ErrClosed) {
		t.Fatalf("pending reservation after close returned %v, want ErrClosed", contending.err)
	}

	closed, _, err := closer.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Ledger.ClosedBy != "fixture-disaster-close" || len(closed.Ledger.PendingEngineRepairs) != 0 {
		t.Fatalf("close race changed the closed root or retained a new pending request: close=%q pending=%+v", closed.Ledger.ClosedBy, closed.Ledger.PendingEngineRepairs)
	}
	if _, exists := closed.Ledger.PendingEngineRepairs[contenderRequest.SlotKey()]; exists {
		t.Fatal("close-losing request remained in the canonical root")
	}
}
