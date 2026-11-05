//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The original passes real quorum certification, Node's certified replay,
// native materialization and the Server's actual before-ACK archive barrier.
// Only the later minority Record is fault input; no certificate is fabricated.
func managedGraphCommittedResolutionFixture(t *testing.T, ctx context.Context) (*managedGraphAdmissionRaceFixture, types.GraphCommand, network.GraphExecuteResponse) {
	t.Helper()
	f := newManagedGraphAdmissionRaceFixture(t)
	f.transport.releaseHeld()
	n := f.node
	n.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if err := n.core.EnsureDurable(slot); err != nil {
			return err
		}
		return n.archive.SyncThrough(ctx, n.core, slot)
	})
	original := types.GraphCommand{RequestID: "resolve-known-graph", Cypher: "CREATE (:ResolveKnown {value:'retained'})"}
	frame, err := types.EncodeManagedGraphExecution(original)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := n.core.Propose(ctx, frame)
	if err != nil {
		t.Fatal("actual Graph certification", err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal("actual certified native replay", err)
	}
	ack, err := n.server.GraphExecute(ctx, original)
	if err != nil || ack.Slot != uint64(slot) || ack.Status != types.MutationCommitted || !ack.Applied {
		t.Fatalf("actual retained Graph ACK unavailable: %+v %v", ack, err)
	}
	return f, original, ack
}

func TestManagedGraphResolutionCutTrustBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f, command, _ := managedGraphCommittedResolutionFixture(t, ctx)
	n := f.node
	original, found, err := n.material.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID)
	if err != nil || !found {
		t.Fatal(err)
	}
	cut, err := n.captureManagedGraphResolutionCut(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	state := ManagedRuntimeState{CertifiedTip: uint64(n.core.Tip()), AppliedTip: n.material.Tip()}
	if !n.managedGraphResolutionCutCovered(cut, state) {
		t.Fatal("valid original cut was not covered")
	}
	for _, name := range []string{"missing-certificate", "wrong-decision-hash", "wrong-execution"} {
		t.Run(name, func(t *testing.T) {
			bad := original
			switch name {
			case "missing-certificate":
				bad.Slot = uint64(n.core.Tip() + 1)
				bad.Receipt.Slot = bad.Slot
			case "wrong-decision-hash":
				bad.DecisionHash[0] ^= 1
			case "wrong-execution":
				bad.ExecutionItem = bytes.Clone(bad.ExecutionItem)
				bad.ExecutionItem[0] ^= 1
			}
			if _, err := n.captureManagedGraphResolutionCut(ctx, bad); err == nil {
				t.Fatal("uncertified or mismatched original established a cut")
			}
		})
	}
	for _, name := range []string{"prefix-drift", "root-drift", "floor-drift", "empty-proof", "unapplied-original"} {
		t.Run(name, func(t *testing.T) {
			bad, s := cut, state
			switch name {
			case "prefix-drift":
				bad.prefix[0] ^= 1
			case "root-drift":
				bad.root[0] ^= 1
			case "floor-drift":
				bad.floor++
			case "empty-proof":
				bad = managedGraphResolutionCut{}
			case "unapplied-original":
				s.AppliedTip = uint64(cut.through - 1)
			}
			if n.managedGraphResolutionCutCovered(bad, s) {
				t.Fatal("changed trust anchor or unapplied original was covered")
			}
		})
	}
	// Publish a real paired QCKP, then restore a passive Core from that exact
	// quorum certificate. This tests the compacted-original proof helper, not
	// public Host startup or an invented floor/certificate.
	n.managedGraphInitialRootDone = make(chan struct{})
	if err := n.publishManagedAppendOnlyCheckpoint(ctx); err != nil {
		t.Fatal("actual paired checkpoint", err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal(err)
	}
	seal, found, err := n.core.LatestManagedCheckpointSeal()
	if err != nil || !found || uint64(seal.Index) < original.Slot {
		t.Fatal("actual covering QCKP missing", err)
	}
	decision, found := n.core.CertifiedValue(seal.DecisionSlot)
	if !found {
		t.Fatal("actual QCKP certificate missing")
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	nonce, err := hex.DecodeString(n.core.WALIdentity())
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity(nonce); err != nil {
		t.Fatal(err)
	}
	observer, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "resolution-cut-observer", Cluster: n.core.CurrentCluster(), WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	observer.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return n.checkpoints.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	if err := observer.RestoreCheckpointBase(ctx, seal.CheckpointSeal, decision); err != nil {
		t.Fatal(err)
	}
	if err := observer.AcceptCertifiedValues([]quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	if _, found := observer.CertifiedValue(quepaxa.Slot(original.Slot)); found {
		t.Fatal("original was not actually below the restored floor")
	}
	probe := New(n.config)
	probe.core = observer
	covered, err := probe.captureManagedGraphResolutionCut(ctx, original)
	if err != nil || covered.through != observer.CompactionFloor() || !probe.managedGraphResolutionCutCovered(covered, ManagedRuntimeState{CertifiedTip: uint64(observer.Tip()), AppliedTip: n.material.Tip()}) {
		t.Fatal("verified compacted original lost its root/prefix proof", err)
	}
}

func TestManagedGraphResolutionPostRecheckRejectsRuntimeChanges(t *testing.T) {
	for _, name := range []string{"caller-canceled", "binding-changed", "nonvoter-core"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			f, command, _ := managedGraphCommittedResolutionFixture(t, ctx)
			n := f.node
			core, binding := n.core, n.internalApplication.Load()
			defer func() { n.core = core; n.internalApplication.Store(binding) }()
			values, tip, err := core.DecisionsFrom(1, 256)
			if err != nil {
				t.Fatal(err)
			}
			var observer *quepaxa.Core
			if name == "nonvoter-core" {
				wal, err := qlog.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer wal.Close()
				observer, err = quepaxa.NewObserver(quepaxa.Config{NodeID: "resolution-nonvoter", Cluster: core.CurrentCluster(), WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
				if err != nil {
					t.Fatal(err)
				}
				if err := observer.AcceptCertifiedValues(values); err != nil {
					t.Fatal(err)
				}
			}
			hit := false
			restore := localtesthooks.Set(func(event string) {
				if event != "node:managed-request:original-rechecked" {
					return
				}
				hit = true
				switch name {
				case "caller-canceled":
					cancel()
				case "binding-changed":
					n.internalApplication.Store(nil)
				case "nonvoter-core":
					n.core = observer
				}
			})
			defer restore()
			_, err = n.ResolveRequest(ctx, "graph", command.RequestID)
			if !hit || !errors.Is(err, network.ErrCommitUnknown) {
				t.Fatal("post-recheck runtime change returned a successful result", err)
			}
			if name == "caller-canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("own cancellation identity lost", err)
			}
			after, afterTip, e := core.DecisionsFrom(1, 256)
			if e != nil || afterTip != tip || !reflect.DeepEqual(values, after) || n.material.Tip() != uint64(tip) {
				t.Fatal("failed post-recheck changed certified/application state")
			}
		})
	}
}

func TestManagedGraphResolveCommittedOriginalWithLateControlISR(t *testing.T) {
	t.Run("minority-isr", func(t *testing.T) { testManagedGraphResolveCommittedOriginalWithLateControl(t, false) })
	t.Run("certified-unapplied-suffix", func(t *testing.T) { testManagedGraphResolveCommittedOriginalWithLateControl(t, true) })
}

func testManagedGraphResolveCommittedOriginalWithLateControl(t *testing.T, certify bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f, command, ack := managedGraphCommittedResolutionFixture(t, ctx)
	n := f.node
	before, found, err := n.material.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID)
	if err != nil || !found || before.Receipt != ack.MutationReceipt {
		t.Fatal("known exact original missing", err)
	}
	certified, tip, err := n.core.DecisionsFrom(1, 256)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := n.core.PrefixHash(tip)
	if !ok {
		t.Fatal("known certified prefix missing")
	}
	var nonce [types.ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(181 + i)
	}
	fresh := types.EncodeReadBarrier(nonce)
	freshSlot := tip + 1
	injected := false
	restore := localtesthooks.Set(func(event string) {
		if event != "node:managed-request:resolved-before-runtime-recheck" {
			return
		}
		if injected {
			t.Fatal("resolution repeated its final boundary")
		}
		injected = true
		if n.core.Tip() != tip || n.material.Tip() != uint64(tip) {
			t.Fatal("late ISR preceded the known certified/applied result")
		}
		if certify {
			slot, _, err := n.core.Propose(ctx, fresh)
			if err != nil || slot != freshSlot {
				t.Fatalf("actual late control certification: slot=%d err=%v", slot, err)
			}
		} else {
			recordFreshAdmissionControl(t, ctx, f, freshSlot, fresh, false)
		}
	})
	defer restore()
	resolved, err := n.ResolveRequest(ctx, "graph", command.RequestID)
	if !injected {
		t.Fatal("final resolution boundary was not reached", err)
	}
	if err != nil {
		// Intended RED must be this exact post-resolution global-quiescence
		// refusal, not admission, source, fingerprint or durability failure.
		if !errors.Is(err, network.ErrCommitUnknown) || !strings.Contains(err.Error(), "request runtime changed during resolution") {
			t.Fatalf("wrong failure boundary: %v", err)
		}
		t.Errorf("known committed original rejected only by later unrelated ISR: %v", err)
	} else if resolved.Receipt != ack.MutationReceipt || !reflect.DeepEqual(resolved.Original, before) {
		t.Error("recovery changed the original slot/bytes/receipt")
	}
	after, found, lookupErr := n.material.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID)
	if lookupErr != nil || !found || !reflect.DeepEqual(before, after) {
		t.Fatal("late ISR changed retained original", lookupErr)
	}
	afterCertified, afterTip, e := n.core.DecisionsFrom(1, 256)
	afterPrefix, ok := n.core.PrefixHash(tip)
	expected, expectedTip := certified, tip
	if certify {
		decision, found := n.core.CertifiedValue(freshSlot)
		if !found || !bytes.Equal(decision.Value, fresh) || len(decision.Certificate) == 0 {
			t.Fatal("actual late certificate missing")
		}
		expected = append(append([]quepaxa.DecidedValue(nil), certified...), decision)
		expectedTip = freshSlot
	}
	if e != nil || afterTip != expectedTip || !reflect.DeepEqual(expected, afterCertified) || !ok || afterPrefix != prefix || n.material.Tip() != uint64(tip) {
		t.Fatal("resolution changed the known certified/applied prefix")
	}
	if _, ok := n.core.CertifiedValue(freshSlot); ok && !certify {
		t.Fatal("late minority ISR received a fake certificate")
	}
	progress := n.core.RuntimeProgressSnapshot()
	if !certify && (progress.RecorderTip != freshSlot || progress.RecorderHash != sha256.Sum256(fresh)) {
		t.Fatal("actual late minority ISR was not retained")
	}
	frame, e := types.EncodeManagedGraphExecution(command)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, d := range afterCertified {
		if bytes.Equal(d.Value, frame) {
			count++
			if uint64(d.Slot) != ack.Slot {
				t.Fatal("original resubmitted at another slot")
			}
		}
	}
	if count != 1 {
		t.Fatal("original was not applied from one exact certified position")
	}
}

func TestManagedGraphResolveOriginalSafetyNeighbors(t *testing.T) {
	for _, name := range []string{"missing-original", "durability-unavailable", "wrong-fingerprint"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			f, command, ack := managedGraphCommittedResolutionFixture(t, ctx)
			n := f.node
			before, found, err := n.material.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID)
			if err != nil || !found {
				t.Fatal(err)
			}
			values, tip, err := n.core.DecisionsFrom(1, 256)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "missing-original":
				result, err := n.ResolveRequest(ctx, "graph", "absent-original")
				if !errors.Is(err, network.ErrCommitUnknown) || result.Receipt.Slot != 0 {
					t.Fatal("missing original was resolved", err)
				}
			case "durability-unavailable":
				n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return network.ErrDurabilityUnavailable })
				result, err := n.ResolveRequest(ctx, "graph", command.RequestID)
				if !errors.Is(err, network.ErrCommitUnknown) || !errors.Is(err, network.ErrDurabilityUnavailable) || result.Receipt.Slot != 0 {
					t.Fatal("unconfirmed durability produced a receipt", err)
				}
			case "wrong-fingerprint":
				wrong := command
				wrong.Cypher = "CREATE (:ResolveWrong)"
				result, err := n.server.ResolveGraph(ctx, wrong)
				if !errors.Is(err, network.ErrRequestConflict) || result.Receipt != nil || result.Durability == network.ResolveKVDurabilityConfirmed {
					t.Fatal("conflicting original was confirmed", err)
				}
			}
			after, found, err := n.material.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID)
			afterValues, afterTip, e := n.core.DecisionsFrom(1, 256)
			if err != nil || !found || !reflect.DeepEqual(before, after) || after.Receipt != ack.MutationReceipt || e != nil || afterTip != tip || !reflect.DeepEqual(values, afterValues) || n.material.Tip() != uint64(tip) {
				t.Fatal("refused resolution changed original or certified/application state")
			}
		})
	}
}
