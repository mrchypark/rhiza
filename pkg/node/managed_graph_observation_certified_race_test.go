//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
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
		tip:      10,
	}
}

func TestManagedGraphInitialAdmissionWaitsForOwnCertifiedBarrier(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requireManagedGraphStrictAdmissionFixture(fixture)
	fixture.node.managedInitiallyAdmitted.Store(false)
	fixture.node.managedAdmitted.Store(false)
	foreign := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	result := make(chan error, 1)
	go func() { result <- fixture.node.EnsureInitialManagedAdmission(ctx) }()
	awaitManagedGraphAdmissionOwner(t, fixture, result, 9)
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, foreign); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("initial voter admission failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("initial voter admission did not complete", ctx.Err())
	}
	if !fixture.node.managedInitiallyAdmitted.Load() || fixture.node.core.Tip() < 10 || fixture.node.material.Tip() < 10 {
		t.Fatalf("initial voter admission lacks own certified replay: admitted=%t C=%d A=%d", fixture.node.managedInitiallyAdmitted.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	assertManagedGraphRaceBarrierSuffix(t, fixture, managedGraphRaceControlValue())
}

func TestManagedGraphAdmissionFixtureJoinsPublisherBeforeTempDirCleanup(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan error, 1)
	var fixture *managedGraphAdmissionRaceFixture
	var binding *internalApplicationBinding
	restore := func() {}
	// On the old broken fixture, the worker remains parked after t.Run returns.
	// Parent-owned cancellation/join prevents this negative from leaking or
	// hanging; restore the global hook only after that exact worker has joined.
	defer func() {
		if fixture != nil {
			fixture.transport.releaseHeld()
			if binding != nil {
				binding.cancel()
			}
			if current := fixture.node.stopInternalApplication(); current != nil {
				current.active.Wait()
			}
			fixture.node.wg.Wait()
		}
		restore()
	}()
	if !t.Run("actual-initial-admission", func(t *testing.T) {
		fixture = newManagedGraphAdmissionRaceFixture(t)
		binding = fixture.node.internalApplication.Load()
		if binding == nil {
			t.Fatal("fixture binding missing")
		}
		ownedLifetime, materialDir := binding.lifetime, fixture.materialDir
		restore = localtesthooks.SetBackgroundPublication(func(ctx context.Context, phase string) {
			if phase != "checkpoint" || ctx != ownedLifetime {
				return
			}
			close(entered)
			<-ctx.Done()
			// Capture directory existence at cancellation, not after cleanup.
			_, err := os.Stat(materialDir)
			canceled <- err
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		requireManagedGraphStrictAdmissionFixture(fixture)
		fixture.node.managedInitiallyAdmitted.Store(false)
		fixture.node.managedAdmitted.Store(false)
		foreign := fixture.startHeldControl(ctx)
		fixture.awaitPendingControl(t)
		result := make(chan error, 1)
		go func() { result <- fixture.node.EnsureInitialManagedAdmission(ctx) }()
		awaitManagedGraphAdmissionOwner(t, fixture, result, 9)
		fixture.transport.releaseHeld()
		if err := awaitProposal(t, foreign); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("actual initial admission: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("actual initial admission did not finish", ctx.Err())
		}
		assertManagedGraphRaceBarrierSuffix(t, fixture, managedGraphRaceControlValue())
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("owned checkpoint publisher did not enter", ctx.Err())
		}
	}) {
		return
	}
	select {
	case err := <-canceled:
		if err != nil {
			t.Errorf("publisher cancellation occurred after material TempDir removal: %v", err)
		}
	default:
		t.Error("fixture cleanup returned without canceling and joining its checkpoint publisher")
	}
	if _, err := os.Stat(fixture.materialDir); !os.IsNotExist(err) {
		t.Errorf("subtest material TempDir cleanup did not complete: %v", err)
	}
}

func TestManagedGraphAdmissionRejectsBindingDriftBeforePendingRecovery(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	foreign := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	binding := fixture.node.internalApplication.Load()
	fixture.node.internalApplication.Store(nil)
	var proposals atomic.Int32
	restore := localtesthooks.SetProposalEntry(func([]byte) { proposals.Add(1) })
	err := fixture.node.waitManagedAppendOnlyControl(ctx, binding)
	restore()
	fixture.node.internalApplication.Store(binding)
	fixture.transport.releaseHeld()
	if ownerErr := awaitProposal(t, foreign); ownerErr != nil {
		t.Fatal(ownerErr)
	}
	if err == nil || proposals.Load() != 0 {
		t.Fatalf("binding drift allowed recovery: err=%v proposals=%d", err, proposals.Load())
	}
}

func TestManagedGraphAdmissionWithoutRecorderQuorumCannotAcknowledgeNewRequest(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	foreign := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeldUncertified()
	_ = awaitProposal(t, foreign)
	var applications atomic.Int32
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		if _, graph, err := types.DecodeManagedGraphExecution(value); err == nil && graph {
			applications.Add(1)
		}
	})
	defer restore()
	response, err := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{RequestID: "no-quorum-no-ack", Cypher: "CREATE (:NoQuorum {id:'never'})"})
	if err == nil || response.MutationReceipt.Slot != 0 || applications.Load() != 0 {
		t.Fatalf("failed quorum admitted another mutation: response=%+v err=%v proposals=%d", response, err, applications.Load())
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := fixture.node.admitInternalApplication(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission lost cancellation: %v", err)
	}
}
