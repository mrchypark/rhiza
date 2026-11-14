//go:build rhiza_local_testhooks

package node

import (
	"context"
	"crypto/sha256"
	"errors"
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

func TestManagedSharedBarrierKeepsFirstShortTarget(t *testing.T) {
	f := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(f.node.core, f.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	f.node.server = server
	t.Cleanup(server.Close)
	f.transport.releaseHeld()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	gate9, gate11 := make(chan struct{}), make(chan struct{})
	entered9, entered11 := make(chan struct{}), make(chan struct{})
	var once9, once11, release9, release11 sync.Once
	defer release9.Do(func() { close(gate9) })
	defer release11.Do(func() { close(gate11) })
	server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		var gate <-chan struct{}
		switch slot {
		case 9:
			once9.Do(func() { close(entered9) })
			gate = gate9
		case 11:
			once11.Do(func() { close(entered11) })
			gate = gate11
		default:
			return nil
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	owner9 := make(chan error, 1)
	go func() { owner9 <- f.node.advanceManagedGraphCheckpointCut(ctx) }()
	select {
	case <-entered9:
	case <-ctx.Done():
		t.Fatal("S9 owner not held")
	}
	recordFreshAdmissionControl(t, ctx, f, 10, freshAdmissionControlValue(77), true)
	waiting, joined11, short := make(chan struct{}), make(chan struct{}), make(chan struct{})
	resume := make(chan struct{})
	var waitOnce, joinedOnce, shortOnce, resumeOnce sync.Once
	var waiterCount atomic.Int32
	defer resumeOnce.Do(func() { close(resume) })
	restore := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-control:waiter-entered":
			if waiterCount.Add(1) == 1 {
				waitOnce.Do(func() { close(waiting) })
			} else {
				joinedOnce.Do(func() { close(joined11) })
			}
		case "node:managed-control:short-before-retry":
			shortOnce.Do(func() { close(short); <-resume })
		}
	})
	defer restore()
	type outcome struct {
		slot quepaxa.Slot
		err  error
	}
	result := make(chan outcome, 1)
	go func() {
		slot, err := f.node.waitManagedAppendOnlyControlProof(ctx, f.node.internalApplication.Load())
		result <- outcome{slot, err}
	}()
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("R10 waiter did not join S9")
	}
	release9.Do(func() { close(gate9) })
	select {
	case err := <-owner9:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("S9 owner timeout")
	}
	select {
	case <-short:
	case <-ctx.Done():
		t.Fatal("R10 short proof not observed")
	}
	var proposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if yes, _ := types.DecodeReadBarrier(value); yes {
			proposals.Add(1)
		}
	})
	defer restoreProposal()
	owner11 := make(chan error, 1)
	go func() { owner11 <- f.node.advanceManagedGraphCheckpointCut(ctx) }()
	select {
	case <-entered11:
	case <-ctx.Done():
		t.Fatal("S11 owner did not reach durable boundary")
	}
	if f.node.core.Tip() != 11 {
		t.Fatalf("expected S11, got %d", f.node.core.Tip())
	}
	recordFreshAdmissionControl(t, ctx, f, 12, freshAdmissionControlValue(99), true)
	resumeOnce.Do(func() { close(resume) })
	select {
	case <-joined11:
	case <-ctx.Done():
		t.Fatal("waiter did not join S11")
	}
	release11.Do(func() { close(gate11) })
	select {
	case err := <-owner11:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("S11 owner did not finish")
	}
	select {
	case got := <-result:
		if got.err != nil || got.slot != 11 {
			t.Fatalf("fixed R10 expected S11, got %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("fixed proof did not complete")
	}
	if got := proposals.Load(); got != 1 {
		t.Fatalf("expected only S11 owner, got %d barrier proposals", got)
	}
}

func TestManagedSharedBarrierTerminalAndMissingCertificateNeverSpawnSuccessor(t *testing.T) {
	for _, mode := range []string{"terminal-error", "missing-certificate", "zero-slot"} {
		t.Run(mode, func(t *testing.T) {
			f := newManagedGraphAdmissionRaceFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			value := freshAdmissionControlValue(231)
			done := make(chan struct{})
			op := &managedGraphControlOperation{value: value, hash: sha256.Sum256(value), binding: f.node.internalApplication.Load(), done: done, slot: 999}
			if mode == "zero-slot" {
				op.slot = 0
			}
			if mode == "terminal-error" {
				op.err = errors.New("injected terminal failure")
			}
			f.node.managedGraphControlMu.Lock()
			f.node.managedGraphControl = op
			f.node.managedGraphControlMu.Unlock()
			waiting := make(chan struct{})
			var once sync.Once
			var proposals atomic.Int32
			restore := localtesthooks.Set(func(event string) {
				if event == "node:managed-control:waiter-entered" {
					once.Do(func() { close(waiting) })
				}
			})
			defer restore()
			restoreProposal := localtesthooks.SetProposalEntry(func([]byte) { proposals.Add(1) })
			defer restoreProposal()
			result := make(chan error, 1)
			go func() { _, err := f.node.waitManagedAppendOnlyControlProof(ctx, op.binding); result <- err }()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("shared owner not selected")
			}
			close(done)
			select {
			case err := <-result:
				if err == nil || errors.Is(err, errManagedBarrierTooShort) {
					t.Fatalf("terminal refusal became retryable: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("terminal refusal blocked")
			}
			if proposals.Load() != 0 {
				t.Fatal("terminal refusal spawned a successor")
			}
		})
	}
}

func TestManagedSharedBarrierRetirementDrainsBeforeReplacement(t *testing.T) {
	for _, mode := range []string{"binding-retired", "caller-cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := newManagedGraphAdmissionRaceFixture(t)
			server, err := network.NewServerForPolicy(f.node.core, f.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
			if err != nil {
				t.Fatal(err)
			}
			f.node.server = server
			t.Cleanup(server.Close)
			f.transport.releaseHeld()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
				if slot != 9 {
					return nil
				}
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			owner := make(chan error, 1)
			go func() { owner <- f.node.advanceManagedGraphCheckpointCut(ctx) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("owner not held")
			}
			recordFreshAdmissionControl(t, ctx, f, 10, freshAdmissionControlValue(77), true)
			binding, scoped, releaseBorrow, err := f.node.borrowInternalApplication(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request, stopRequest := context.WithCancel(scoped)
			defer stopRequest()
			waiting, short, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var waitOnce, shortOnce, resumeOnce sync.Once
			defer resumeOnce.Do(func() { close(resume) })
			restore := localtesthooks.Set(func(event string) {
				if event == "node:managed-control:waiter-entered" {
					waitOnce.Do(func() { close(waiting) })
				}
				if event == "node:managed-control:short-before-retry" {
					shortOnce.Do(func() { close(short); <-resume })
				}
			})
			defer restore()
			var proposals atomic.Int32
			restoreProposal := localtesthooks.SetProposalEntry(func([]byte) { proposals.Add(1) })
			defer restoreProposal()
			result := make(chan error, 1)
			go func() {
				defer releaseBorrow()
				_, err := f.node.waitManagedAppendOnlyControlProof(request, binding)
				result <- err
			}()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("waiter did not join owner")
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-owner:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("owner timeout")
			}
			select {
			case <-short:
			case <-ctx.Done():
				t.Fatal("short-to-replacement gate not reached")
			}
			retired := binding
			if mode == "binding-retired" {
				retired = f.node.stopInternalApplication()
				if retired != binding {
					t.Fatal("unexpected binding retired")
				}
			} else {
				stopRequest()
			}
			drained := make(chan struct{})
			go func() { retired.active.Wait(); close(drained) }()
			select {
			case <-drained:
				t.Fatal("borrower drained before it left the short boundary")
			default:
			}
			resumeOnce.Do(func() { close(resume) })
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("retired binding returned proof")
				}
			case <-ctx.Done():
				t.Fatal("retired waiter leaked")
			}
			select {
			case <-drained:
			case <-ctx.Done():
				t.Fatal("retired binding did not drain")
			}
			if proposals.Load() != 0 {
				t.Fatal("retired binding spawned successor")
			}
		})
	}
}

func TestManagedKVSharedShortBarrierAndCancellation(t *testing.T) {
	for _, mode := range []string{"complete", "cancel-growing-target", "durability-error"} {
		t.Run(mode, func(t *testing.T) {
			n, _, fixture := newKVWrapperFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			base := n.core.Tip()
			ownerSlot := base + 1
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			n.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
				if slot == ownerSlot {
					enteredOnce.Do(func() { close(entered) })
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					if mode == "durability-error" {
						return errors.New("injected durability barrier error")
					}
				}
				return kvWrapperDurable(ctx, n, slot)
			})
			owner := make(chan error, 1)
			go func() { owner <- n.advanceManagedGraphCheckpointCut(ctx) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("shared owner did not reach durability")
			}
			recordFreshAdmissionControl(t, ctx, fixture, ownerSlot+1, freshAdmissionControlValue(111), true)
			progress := n.core.RuntimeProgressSnapshot()
			if progress.RecorderTip != ownerSlot+1 || progress.CertifiedTip != ownerSlot {
				t.Fatalf("missing real recorder gap: %+v", progress)
			}
			waiting := make(chan struct{})
			var waitOnce sync.Once
			restore := localtesthooks.Set(func(event string) {
				if event == "node:managed-control:waiter-entered" {
					waitOnce.Do(func() { close(waiting) })
				}
			})
			defer restore()
			requestCtx, stop := context.WithCancel(ctx)
			defer stop()
			type result struct {
				r   network.KVMutationResponse
				err error
			}
			resultCh := make(chan result, 1)
			go func() {
				r, err := n.ExecuteManagedKV(requestCtx, "put", network.KVMutationRequest{RequestID: "kv-short-owner", Key: "kv-short-owner", Value: []byte("retained")})
				resultCh <- result{r, err}
			}()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("KV write did not join shared owner")
			}
			if mode == "cancel-growing-target" {
				for i := 2; i <= 4; i++ {
					recordFreshAdmissionControl(t, ctx, fixture, ownerSlot+quepaxa.Slot(i), freshAdmissionControlValue(byte(111+i)), true)
				}
				stop()
				select {
				case got := <-resultCh:
					if got.err == nil || got.r.Status == types.MutationCommitted {
						t.Fatalf("cancelled KV incorrectly acknowledged: %+v", got)
					}
				case <-ctx.Done():
					t.Fatal("cancelled KV blocked")
				}
				releaseOnce.Do(func() { close(release) })
				select {
				case err := <-owner:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("owner did not drain")
				}
				return
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-owner:
				if mode == "durability-error" && err == nil {
					t.Fatal("owner hid injected durability error")
				}
				if mode != "durability-error" && err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("owner did not finish")
			}
			select {
			case got := <-resultCh:
				if mode == "durability-error" {
					if got.err == nil || got.r.Status == types.MutationCommitted {
						t.Fatalf("durability failure produced KV ACK: %+v", got)
					}
					return
				}
				if got.err != nil || got.r.Status != types.MutationCommitted || got.r.Slot <= uint64(ownerSlot+1) {
					t.Fatalf("KV was not applied after fresh proof: %+v", got)
				}
				original, found, err := n.RetainedRequest(ctx, "kv", "kv-short-owner")
				if err != nil || !found || original.Slot != got.r.Slot {
					t.Fatalf("exact retained KV missing: %+v %v", original, err)
				}
			case <-ctx.Done():
				t.Fatal("KV write did not finish")
			}
		})
	}
}

func TestManagedSharedBarrierMismatchedCertificateNeverRetries(t *testing.T) {
	f := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	recordFreshAdmissionControl(t, ctx, f, 9, freshAdmissionControlValue(202), false)
	before := f.node.core.RuntimeProgressSnapshot()
	if before.CertifiedTip != 8 || before.RecorderTip != 9 {
		t.Fatalf("expected actual pending R9 over C8: %+v", before)
	}
	value := freshAdmissionControlValue(201)
	done := make(chan struct{})
	op := &managedGraphControlOperation{value: value, hash: sha256.Sum256(value), binding: f.node.internalApplication.Load(), done: done, slot: 8}
	if cert, ok := f.node.core.CertifiedValue(op.slot); !ok || managedGraphControlCertificateMatches(f.node.core, op, cert) {
		t.Fatal("fixture lacks an actual mismatched slot-8 certificate")
	}
	f.node.managedGraphControlMu.Lock()
	f.node.managedGraphControl = op
	f.node.managedGraphControlMu.Unlock()
	waiting := make(chan struct{})
	var once sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:waiter-entered" {
			once.Do(func() { close(waiting) })
		}
	})
	defer restore()
	var proposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func([]byte) { proposals.Add(1) })
	defer restoreProposal()
	result := make(chan error, 1)
	go func() { _, err := f.node.waitManagedAppendOnlyControlProof(ctx, op.binding); result <- err }()
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("waiter did not select shared owner")
	}
	close(done)
	select {
	case err := <-result:
		if err == nil || errors.Is(err, errManagedBarrierTooShort) {
			t.Fatalf("mismatched certificate treated as retryable: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("waiter did not finish")
	}
	after := f.node.core.RuntimeProgressSnapshot()
	if after.CertifiedTip != before.CertifiedTip || proposals.Load() != 0 {
		t.Fatalf("mismatch started a new proposal: before=%+v after=%+v proposals=%d", before, after, proposals.Load())
	}
}

func TestManagedSharedBarrierShorterThanLiveRecorderGetsFreshProof(t *testing.T) {
	f := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(f.node.core, f.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	f.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f.transport.releaseHeld()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 9 {
			return nil
		}
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	owner := make(chan error, 1)
	go func() { owner <- f.node.advanceManagedGraphCheckpointCut(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("owner did not reach durable boundary")
	}
	if f.node.core.Tip() != 9 {
		t.Fatalf("owner tip=%d", f.node.core.Tip())
	}
	value := freshAdmissionControlValue(77)
	recordFreshAdmissionControl(t, ctx, f, 10, value, true)
	progress := f.node.core.RuntimeProgressSnapshot()
	if progress.RecorderTip != 10 || progress.CertifiedTip != 9 {
		t.Fatalf("not an actual recorder gap: %+v", progress)
	}
	waiting := make(chan struct{})
	var waitOnce sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:waiter-entered" {
			waitOnce.Do(func() { close(waiting) })
		}
	})
	defer restore()
	result := make(chan struct {
		slot quepaxa.Slot
		err  error
	}, 1)
	go func() {
		slot, err := f.node.waitManagedAppendOnlyControlProof(ctx, f.node.internalApplication.Load())
		result <- struct {
			slot quepaxa.Slot
			err  error
		}{slot, err}
	}()
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("waiter did not join owner")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-owner:
		if err != nil {
			t.Fatalf("original owner: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("owner timeout")
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("short owner prevented fresh proof: %v", got.err)
		}
		if got.slot <= 10 {
			t.Fatalf("proof %d does not follow target 10", got.slot)
		}
		v, ok := f.node.core.CertifiedValue(got.slot)
		if !ok || len(v.Certificate) == 0 || f.node.material.Tip() < uint64(got.slot) {
			t.Fatal("fresh proof not certified and applied")
		}
	case <-ctx.Done():
		t.Fatal("fresh proof timed out")
	}
}
