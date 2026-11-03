//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

type managedControlObserverContextKey struct{}

type managedControlHeldTransport struct {
	*clusterTransport
	held        chan struct{}
	release     chan struct{}
	heldOnce    sync.Once
	releaseOnce sync.Once
}

func (t *managedControlHeldTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	if request.Slot == 9 {
		if to == "n3" {
			select {
			case <-t.release:
			case <-ctx.Done():
				return Summary{}, ctx.Err()
			}
		}
	}
	summary, err := t.clusterTransport.SendRecord(ctx, to, request)
	if err != nil {
		return summary, err
	}
	if request.Slot == 9 && to == "n2" {
		t.heldOnce.Do(func() { close(t.held) })
		select {
		case <-t.release:
		case <-ctx.Done():
			return Summary{}, ctx.Err()
		}
	}
	return summary, nil
}

func (t *managedControlHeldTransport) releaseHeld() {
	t.releaseOnce.Do(func() { close(t.release) })
}

type managedControlProposalResult struct {
	slot Slot
	err  error
}

type managedControlProposal struct {
	done   chan managedControlProposalResult
	result managedControlProposalResult
	got    atomic.Bool
}

func (p *managedControlProposal) wait(ctx context.Context) managedControlProposalResult {
	if p.got.Load() {
		return p.result
	}
	select {
	case p.result = <-p.done:
		p.got.Store(true)
	case <-ctx.Done():
		p.result.err = ctx.Err()
	case <-time.After(5 * time.Second):
		p.result.err = errors.New("proposal did not finish after release")
	}
	return p.result
}

func newManagedControlRaceCluster(t *testing.T, verifier ManagedCheckpointCandidateVerifier) (map[NodeID]*Core, *managedControlHeldTransport) {
	t.Helper()
	cluster := Cluster{ConfigID: 1}
	ids := []NodeID{"n1", "n2", "n3"}
	type registeredWAL struct {
		wal      *qlog.WAL
		identity [32]byte
	}
	wals := make(map[NodeID]registeredWAL, len(ids))
	for i, id := range ids {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(id)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		identity := make([]byte, 32)
		for j := range identity {
			identity[j] = byte(i + 1)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		registeredIdentity, registered := wal.Identity()
		if !registered || len(registeredIdentity) != 32 {
			t.Fatalf("WAL %s did not retain its registered identity", id)
		}
		var identityArray [32]byte
		copy(identityArray[:], registeredIdentity)
		wals[id] = registeredWAL{wal: wal, identity: identityArray}
		cluster.Members = append(cluster.Members, Member{ID: id, WALIdentity: hex.EncodeToString(registeredIdentity)})
	}
	base := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	transport := &managedControlHeldTransport{clusterTransport: base, held: make(chan struct{}), release: make(chan struct{})}
	cores := make(map[NodeID]*Core, len(ids))
	for _, id := range ids {
		registered := wals[id]
		verify := ManagedCheckpointCandidateVerifier(func(context.Context, ManagedCheckpointCandidate) error { return nil })
		if id == "n1" && verifier != nil {
			verify = verifier
		}
		core, err := New(Config{
			NodeID: id, Cluster: cluster, WAL: registered.wal, Transport: transport,
			ManagedAppendOnly: &ManagedAppendOnlyConfig{Binding: ManagedAppendOnlyBinding{
				ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol,
				BootstrapPlanDigest: hex.EncodeToString(make([]byte, 32)),
				MemberConfigDigest:  hex.EncodeToString(registered.identity[:]),
				Generation:          0,
				WALIdentity:         registered.identity,
			}, Verify: verify},
		})
		if err != nil {
			t.Fatal(err)
		}
		cores[id] = core
		base.cores[id] = core
	}
	t.Cleanup(transport.releaseHeld)
	return cores, transport
}

func managedControlRacePrefix(t *testing.T, core *Core) {
	t.Helper()
	for slot := uint64(1); slot <= 8; slot++ {
		var nonce [ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(slot + uint64(i))
		}
		got, _, err := core.Propose(context.Background(), EncodeReadBarrier(nonce))
		if err != nil || uint64(got) != slot {
			t.Fatalf("establish actual prefix slot=%d got=%d err=%v", slot, got, err)
		}
	}
}

func startManagedControlProposal(t *testing.T, ctx context.Context, core *Core, transport *managedControlHeldTransport, value []byte) *managedControlProposal {
	t.Helper()
	proposal := &managedControlProposal{done: make(chan managedControlProposalResult, 1)}
	go func() {
		slot, _, err := core.Propose(ctx, value)
		proposal.done <- managedControlProposalResult{slot: slot, err: err}
	}()
	select {
	case <-transport.held:
	case <-ctx.Done():
		t.Fatalf("proposal did not reach actual held recorder response: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach actual held recorder response")
	}
	t.Cleanup(func() {
		transport.releaseHeld()
		if !proposal.got.Load() {
			select {
			case proposal.result = <-proposal.done:
				proposal.got.Store(true)
			case <-time.After(5 * time.Second):
				t.Error("held proposal did not terminate during cleanup")
			}
		}
	})
	return proposal
}

func TestObserveManagedControlReturnsTypedErrorOnlyForValidatedPrefixAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cores, transport := newManagedControlRaceCluster(t, nil)
	core := cores["n1"]
	managedControlRacePrefix(t, core)
	var nonce [ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(200 + i)
	}
	barrier := EncodeReadBarrier(nonce)
	proposal := startManagedControlProposal(t, ctx, core, transport, barrier)
	var proposalResult managedControlProposalResult
	var advanced atomic.Bool
	restore := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && advanced.CompareAndSwap(false, true) {
			transport.releaseHeld()
			proposalResult = proposal.wait(ctx)
		}
	})
	_, observationErr := core.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if proposalResult.err != nil || proposalResult.slot != 9 || !advanced.Load() {
		t.Fatalf("actual control certification=%+v hook=%v", proposalResult, advanced.Load())
	}
	if !IsManagedControlPrefixAdvance(observationErr) || !errors.Is(observationErr, ErrManagedControlObservationUnavailable) || observationErr.Error() != "managed control observation unavailable: Core state changed during validation" {
		t.Fatalf("observation error=%v, want compatible typed prefix-advance error", observationErr)
	}
	certified, ok := core.CertifiedValue(9)
	if !ok || certified.Hash != ValueHash(sha256.Sum256(barrier)) {
		t.Fatalf("actual certified control mismatch: present=%v hash=%x", ok, certified.Hash)
	}
	if ok, err := core.RecheckManagedControlPrefixAdvance(ctx, observationErr, 8, 9, managedRequestAppendOnlyApplicationProtocol, 0); err != nil || !ok {
		t.Fatalf("continuous prefix recheck=%v err=%v", ok, err)
	}
	if ok, err := cores["n2"].RecheckManagedControlPrefixAdvance(ctx, observationErr, 8, 9, managedRequestAppendOnlyApplicationProtocol, 0); err != nil || ok {
		t.Fatalf("different-Core recheck=%v err=%v, want false,nil", ok, err)
	}
}

func TestObserveManagedControlStillGapAuthorityDriftAndCancellationStayUnaccepted(t *testing.T) {
	t.Run("still_gap", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cores, transport := newManagedControlRaceCluster(t, nil)
		core := cores["n1"]
		managedControlRacePrefix(t, core)
		barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{4})
		startManagedControlProposal(t, ctx, core, transport, barrier)
		observation, err := core.ObserveManagedAppendOnlyControl(ctx)
		if err != nil || observation.ObservedSlot != 9 || IsManagedControlPrefixAdvance(err) {
			t.Fatalf("still-gap observation=%+v err=%v", observation, err)
		}
	})
	t.Run("authority_drift", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cores, transport := newManagedControlRaceCluster(t, nil)
		core := cores["n1"]
		managedControlRacePrefix(t, core)
		barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{5})
		startManagedControlProposal(t, ctx, core, transport, barrier)
		var hooked atomic.Bool
		restore := localtesthooks.Set(func(event string) {
			if event == "core:managed-control:before-snapshot-recheck" && hooked.CompareAndSwap(false, true) {
				core.mu.Lock()
				core.membershipVersion++
				core.mu.Unlock()
			}
		})
		_, err := core.ObserveManagedAppendOnlyControl(ctx)
		restore()
		if !hooked.Load() || err == nil || IsManagedControlPrefixAdvance(err) {
			t.Fatalf("authority-drift error=%v hook=%v", err, hooked.Load())
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cores, transport := newManagedControlRaceCluster(t, nil)
		core := cores["n1"]
		managedControlRacePrefix(t, core)
		barrier := EncodeReadBarrier([ReadBarrierNonceSize]byte{6})
		startManagedControlProposal(t, ctx, core, transport, barrier)
		observerCtx, cancelObserver := context.WithCancel(ctx)
		var hooked atomic.Bool
		restore := localtesthooks.Set(func(event string) {
			if event == "core:managed-control:before-snapshot-recheck" && hooked.CompareAndSwap(false, true) {
				cancelObserver()
			}
		})
		_, err := core.ObserveManagedAppendOnlyControl(observerCtx)
		restore()
		if !hooked.Load() || !errors.Is(err, context.Canceled) || IsManagedControlPrefixAdvance(err) {
			t.Fatalf("cancellation error=%v hook=%v", err, hooked.Load())
		}
	})
}

func TestObserveManagedControlVerifierFailureStaysGenericAfterActualCertification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var transport *managedControlHeldTransport
	var proposal *managedControlProposal
	var proposalResult managedControlProposalResult
	var injected atomic.Bool
	verify := func(ctx context.Context, _ ManagedCheckpointCandidate) error {
		if ctx.Value(managedControlObserverContextKey{}) == true && injected.CompareAndSwap(false, true) {
			transport.releaseHeld()
			proposalResult = proposal.wait(ctx)
			proposalResult = proposal.wait(ctx)
			return errors.New("observer-only verifier failure")
		}
		return nil
	}
	cores, held := newManagedControlRaceCluster(t, verify)
	transport = held
	core := cores["n1"]
	managedControlRacePrefix(t, core)
	seal := managedAppendOnlyTestSeal()
	seal.Index = 8
	seal.GenerationAnchorHash = core.GenerationAnchorHash()
	seal.PrefixHash, _ = core.PrefixHash(8)
	seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(8)
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal = startManagedControlProposal(t, ctx, core, held, value)
	observerCtx := context.WithValue(ctx, managedControlObserverContextKey{}, true)
	_, observationErr := core.ObserveManagedAppendOnlyControl(observerCtx)
	if !injected.Load() || observationErr == nil || IsManagedControlPrefixAdvance(observationErr) {
		t.Fatalf("observation error=%v verifier-called=%v", observationErr, injected.Load())
	}
	if proposalResult.err != nil || proposalResult.slot != 9 || core.Tip() != 9 {
		t.Fatalf("verifier-negative race failed to certify original QCKP: proposal=%+v tip=%d", proposalResult, core.Tip())
	}
	if _, ok := core.CertifiedValue(9); !ok {
		t.Fatal("verifier-negative race lacks the actual slot-9 certificate")
	}
}
