package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
)

func managedControlRef(value []byte) Proposal {
	return Proposal{Priority: highestPriority, ProposerID: "remote", Hash: ValueHash(sha256.Sum256(value))}
}

func installManagedControlISR(core *Core, slot Slot, state ISR, values ...[]byte) {
	core.mu.Lock()
	defer core.mu.Unlock()
	for _, value := range values {
		hash := ValueHash(sha256.Sum256(value))
		core.values[hash] = bytes.Clone(value)
	}
	core.recorders[slot] = state
}

func TestObserveManagedAppendOnlyControlReadBarrier(t *testing.T) {
	core, wal := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error {
		t.Fatal("ReadBarrier must not call the QCKP verifier")
		return nil
	})
	if slot, _, err := core.Propose(context.Background(), []byte("committed prefix")); err != nil || slot != 1 {
		t.Fatalf("build committed prefix slot=%d err=%v", slot, err)
	}
	barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{1, 2, 3})
	proposal := managedControlRef(barrier)
	installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, barrier)
	before := wal.Bytes()
	observation, err := core.ObserveManagedAppendOnlyControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if observation.CommittedTip != 1 || observation.ObservedSlot != 2 || observation.ConfigID != 1 || observation.Step != 4 || observation.ApplicationProtocol != managedRequestAppendOnlyApplicationProtocol || observation.Generation != 0 || !observation.BelongsTo(core) {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	if len(observation.Proposals) != 2 || observation.Proposals[0].Role != ManagedAppendOnlyFirstCurrent || observation.Proposals[1].Role != ManagedAppendOnlyAggregateCurrent || observation.Proposals[0].Kind != ManagedAppendOnlyReadBarrier || observation.Proposals[1].Hash != proposal.Hash {
		t.Fatalf("incomplete control identity observation: %+v", observation.Proposals)
	}
	if wal.Bytes() != before {
		t.Fatalf("read-only classification appended WAL: %d -> %d", before, wal.Bytes())
	}
	core.mu.RLock()
	_, prepared := core.preparedCheckpoints[1]
	state := core.recorders[2]
	core.mu.RUnlock()
	if prepared || !sameProposal(state.FirstCurrent, &proposal) || !sameProposal(state.AggregateCurrent, &proposal) {
		t.Fatal("classification created checkpoint intent or mutated the ISR")
	}
	other, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if observation.BelongsTo(other) {
		t.Fatal("observation was transferable to another Core instance")
	}
}

func TestObserveManagedAppendOnlyControlValidatesEveryQCKPReference(t *testing.T) {
	core, wal := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if slot, _, err := core.Propose(context.Background(), []byte("committed prefix")); err != nil || slot != 1 {
		t.Fatalf("build committed prefix slot=%d err=%v", slot, err)
	}
	seal := managedAppendOnlyTestSeal()
	seal.Index = 1
	seal.PrefixHash, _ = core.PrefixHash(1)
	seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(1)
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal := managedControlRef(value)
	installManagedControlISR(core, 2, ISR{Step: 6, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal), AggregatePrior: cloneProposal(&proposal)}, value)
	calls := 0
	core.managedAppendOnly.Verify = func(ctx context.Context, candidate ManagedCheckpointCandidate) error {
		calls++
		if !bytes.Equal(candidate.Value, value) || candidate.Hash != proposal.Hash || candidate.DecisionSlot != 2 || candidate.ConfigID != 1 || !sameManagedCheckpointSeal(candidate.Seal, seal) {
			t.Fatal("verifier did not receive exact original QCKP identity")
		}
		if !core.mu.TryLock() {
			t.Fatal("QCKP verifier ran while Core.mu was held")
		}
		core.mu.Unlock()
		return nil
	}
	before := wal.Bytes()
	observation, err := core.ObserveManagedAppendOnlyControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(observation.Proposals) != 3 {
		t.Fatalf("all F/A/P references must be verified: calls=%d refs=%+v", calls, observation.Proposals)
	}
	for _, ref := range observation.Proposals {
		if ref.Kind != ManagedAppendOnlyCheckpointSeal || ref.Hash != proposal.Hash {
			t.Fatalf("QCKP reference changed: %+v", ref)
		}
	}
	if wal.Bytes() != before {
		t.Fatal("QCKP classification wrote to WAL")
	}
}

func TestObserveManagedAppendOnlyControlRejectsInvalidISRShapes(t *testing.T) {
	barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{9})
	app := []byte("ordinary application")
	barrierRef, appRef := managedControlRef(barrier), managedControlRef(app)
	for _, tc := range []struct {
		name  string
		state ISR
		vals  [][]byte
	}{
		{"missing_first", ISR{Step: 4, AggregateCurrent: cloneProposal(&barrierRef)}, [][]byte{barrier}},
		{"missing_value", ISR{Step: 4, FirstCurrent: cloneProposal(&barrierRef), AggregateCurrent: cloneProposal(&barrierRef)}, nil},
		{"mixed_current_application", ISR{Step: 4, FirstCurrent: cloneProposal(&barrierRef), AggregateCurrent: cloneProposal(&appRef)}, [][]byte{barrier, app}},
		{"mixed_prior_application", ISR{Step: 5, FirstCurrent: cloneProposal(&barrierRef), AggregateCurrent: cloneProposal(&barrierRef), AggregatePrior: cloneProposal(&appRef)}, [][]byte{barrier, app}},
		{"aggregate_below_first", ISR{Step: 4, FirstCurrent: cloneProposal(&appRef), AggregateCurrent: cloneProposal(&barrierRef)}, [][]byte{barrier, app}},
		{"unreachable_step", ISR{Step: 3, FirstCurrent: cloneProposal(&barrierRef), AggregateCurrent: cloneProposal(&barrierRef)}, [][]byte{barrier}},
		{"prior_at_step_four", ISR{Step: 4, FirstCurrent: cloneProposal(&barrierRef), AggregateCurrent: cloneProposal(&barrierRef), AggregatePrior: cloneProposal(&barrierRef)}, [][]byte{barrier}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error {
				t.Fatal("invalid ISR invoked QCKP verifier")
				return nil
			})
			if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
				t.Fatalf("build prefix slot=%d err=%v", slot, err)
			}
			installManagedControlISR(core, 2, tc.state, tc.vals...)
			before := core.wal.Bytes()
			if _, err := core.ObserveManagedAppendOnlyControl(context.Background()); err == nil {
				t.Fatal("invalid pending ISR classified as control")
			}
			if core.wal.Bytes() != before {
				t.Fatal("invalid classification mutated WAL")
			}
		})
	}
}

func TestObserveManagedAppendOnlyControlRejectsUnknownAndWiderPendingRange(t *testing.T) {
	barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{4})
	proposal := managedControlRef(barrier)
	for _, wider := range []bool{true} {
		core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
		if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
			t.Fatalf("build prefix slot=%d err=%v", slot, err)
		}
		installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, barrier)
		if wider {
			installManagedControlISR(core, 3, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, barrier)
		}
		if _, err := core.ObserveManagedAppendOnlyControl(context.Background()); err == nil {
			t.Fatalf("wider pending ISR range accepted: wider=%v", wider)
		}
	}
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
		t.Fatalf("build prefix slot=%d err=%v", slot, err)
	}
	unknown := []byte("unknown marker")
	ref := managedControlRef(unknown)
	installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&ref), AggregateCurrent: cloneProposal(&ref)}, unknown)
	if _, err := core.ObserveManagedAppendOnlyControl(context.Background()); err == nil {
		t.Fatal("unknown control value was accepted")
	}
}

func TestObserveManagedAppendOnlyControlRejectsISRDriftDuringVerification(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
		t.Fatalf("build prefix slot=%d err=%v", slot, err)
	}
	seal := managedAppendOnlyTestSeal()
	seal.Index = 1
	seal.PrefixHash, _ = core.PrefixHash(1)
	seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(1)
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal := managedControlRef(value)
	installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, value)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce sync.Once
	core.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := core.ObserveManagedAppendOnlyControl(context.Background())
		result <- err
	}()
	<-entered
	core.mu.Lock()
	core.recorders[2] = ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}
	core.recorders[2].AggregateCurrent.Priority[0]--
	core.mu.Unlock()
	close(release)
	if err := <-result; err == nil {
		t.Fatal("ISR drift during out-of-lock QCKP validation was accepted")
	}
}

func TestObserveManagedAppendOnlyControlRejectsCommitOrBindingDrift(t *testing.T) {
	for _, change := range []string{"committed_tip", "binding_generation"} {
		t.Run(change, func(t *testing.T) {
			core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
			if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
				t.Fatalf("build prefix slot=%d err=%v", slot, err)
			}
			seal := managedAppendOnlyTestSeal()
			seal.Index = 1
			seal.PrefixHash, _ = core.PrefixHash(1)
			seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(1)
			value, err := EncodeCheckpointSeal(seal)
			if err != nil {
				t.Fatal(err)
			}
			proposal := managedControlRef(value)
			installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, value)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			core.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error {
				once.Do(func() { close(entered) })
				<-release
				return nil
			}
			result := make(chan error, 1)
			go func() {
				_, err := core.ObserveManagedAppendOnlyControl(context.Background())
				result <- err
			}()
			<-entered
			switch change {
			case "committed_tip":
				core.mu.Lock()
				core.tip++
				core.mu.Unlock()
			case "binding_generation":
				core.managedAppendOnly.Binding.Generation++
			}
			close(release)
			if err := <-result; err == nil {
				t.Fatal("Core identity drift during QCKP verification was accepted")
			}
		})
	}
}

func TestObserveManagedAppendOnlyControlHonorsCancellation(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if slot, _, err := core.Propose(context.Background(), []byte("prefix")); err != nil || slot != 1 {
		t.Fatalf("build prefix slot=%d err=%v", slot, err)
	}
	seal := managedAppendOnlyTestSeal()
	seal.Index = 1
	seal.PrefixHash, _ = core.PrefixHash(1)
	seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(1)
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal := managedControlRef(value)
	installManagedControlISR(core, 2, ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}, value)
	started := make(chan struct{})
	core.managedAppendOnly.Verify = func(ctx context.Context, _ ManagedCheckpointCandidate) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := core.ObserveManagedAppendOnlyControl(ctx)
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("classification error=%v, want context canceled", err)
	}
}
