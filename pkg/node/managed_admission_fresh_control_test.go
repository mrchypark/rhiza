//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// All decisions and recorder entries below pass through real Core methods.
// Fresh Records deliberately have no decision certificate: they must neither
// invalidate the already certified read cut nor become read authority. The
// first fresh value has quorum recorder support; the old recovery path then
// encounters a later minority value at its final observation instead.
func TestManagedAdmissionCapturedBarrierSurvivesFreshControlISR(t *testing.T) {
	f := newManagedGraphAdmissionRaceFixture(t)
	f.transport.releaseHeld()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seed := freshAdmissionControlValue(91)
	recordFreshAdmissionControl(t, ctx, f, 9, seed, true)
	if _, ok := f.node.core.CertifiedValue(9); ok {
		t.Fatal("preexisting ISR already has a certificate")
	}
	var firstOwner, finalOwner *managedGraphControlOperation
	// Capture the actual owner while its own bytes enter Core.Propose. The
	// admission waiter may retire the shared pointer before the later hook.
	var ownerMu sync.Mutex
	var capturedOwner *managedGraphControlOperation
	var capturedValue []byte
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		f.node.managedGraphControlMu.Lock()
		op := f.node.managedGraphControl
		if op != nil && op.binding == f.node.internalApplication.Load() && bytes.Equal(value, op.value) && managedGraphControlOwnBarrierMatches(op, sha256.Sum256(value)) {
			ownerMu.Lock()
			capturedOwner, capturedValue = op, bytes.Clone(value)
			ownerMu.Unlock()
		}
		f.node.managedGraphControlMu.Unlock()
	})
	t.Cleanup(restoreProposal)
	lastOwner := func() (*managedGraphControlOperation, []byte) {
		ownerMu.Lock()
		defer ownerMu.Unlock()
		return capturedOwner, bytes.Clone(capturedValue)
	}
	var middle, fresh []byte
	var middleSlot, freshSlot quepaxa.Slot
	var certifiedCut []quepaxa.DecidedValue
	afterReadIndex, middleRecorded, finalObserved := false, false, false
	restore := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-control:post-readindex-owner-selected":
			if !afterReadIndex {
				op, value := lastOwner()
				firstOwner = requireFreshAdmissionOwnCertificate(t, f, 9, seed, op, value)
				afterReadIndex = true
			}
		case "node:managed-runtime-state:after-certified-tip":
			if afterReadIndex && !middleRecorded {
				// This sample follows the first ReadIndex/replay. The fixed
				// path keeps this new quorum-backed ISR uncertified; the old
				// path unnecessarily starts a second recovery pass for it.
				middleRecorded = true
				middleSlot = f.node.core.Tip() + 1
				if firstOwner == nil || f.node.material.Tip() != uint64(middleSlot-1) {
					t.Fatal("fresh ISR preceded the completed first owned read cut")
				}
				var err error
				certifiedCut, _, err = f.node.core.DecisionsFrom(1, int(middleSlot-1))
				if err != nil || len(certifiedCut) != int(middleSlot-1) {
					t.Fatal("cannot capture first exact certified read prefix", err)
				}
				middle = freshAdmissionControlValue(113)
				freshSlot, fresh = middleSlot, middle
				recordFreshAdmissionControl(t, ctx, f, middleSlot, middle, true)
			}
		case "node:managed-admission:before-final-runtime-observation":
			if finalObserved {
				t.Fatal("unexpected repeated final runtime boundary")
			}
			finalObserved = true
			if !middleRecorded {
				t.Fatal("final boundary preceded actual second-pass gap")
			}
			op, value := lastOwner()
			finalOwner = requireFreshAdmissionOwnCertificate(t, f, middleSlot, middle, op, value)
			if finalOwner == firstOwner || finalOwner.slot <= firstOwner.slot || bytes.Equal(finalOwner.value, firstOwner.value) {
				t.Fatal("second recovery lacks a separate own-nonce certificate")
			}
			if f.node.material.Tip() != uint64(f.node.core.Tip()) {
				t.Fatal("captured certified prefix was not applied before fresh ISR")
			}
			freshSlot = f.node.core.Tip() + 1
			var err error
			certifiedCut, _, err = f.node.core.DecisionsFrom(1, int(freshSlot-1))
			if err != nil || len(certifiedCut) != int(freshSlot-1) {
				t.Fatal("cannot capture exact certified read prefix", err)
			}
			fresh = freshAdmissionControlValue(137)
			// A legitimate local recorder entry, not a fabricated decision.
			// No proposer is started, so it remains deterministically held.
			recordFreshAdmissionControl(t, ctx, f, freshSlot, fresh, false)
		}
	})
	t.Cleanup(restore)
	err := f.node.admitInternalApplication(ctx)
	if !afterReadIndex || !middleRecorded {
		t.Fatalf("required admission boundaries were not reached: read_index=%t middle=%t final=%t err=%v", afterReadIndex, middleRecorded, finalObserved, err)
	}
	if !finalObserved {
		op, value := lastOwner()
		if op != firstOwner {
			t.Fatal("fresh ISR started an unobserved replacement barrier")
		}
		finalOwner = requireFreshAdmissionOwnCertificate(t, f, 9, seed, op, value)
		if freshSlot != middleSlot || !bytes.Equal(fresh, middle) {
			t.Fatal("first completed cut lost the exact fresh quorum-backed ISR")
		}
	}
	progress := f.node.core.RuntimeProgressSnapshot()
	if finalOwner == nil || progress.CertifiedTip != freshSlot-1 || progress.RecorderTip != freshSlot || f.node.material.Tip() != uint64(progress.CertifiedTip) || progress.RecorderHash != sha256.Sum256(fresh) || progress.RecorderHashIsCertified {
		t.Fatalf("fresh control is not the exact pending ISR above applied read cut: %+v", progress)
	}
	if _, ok := f.node.core.CertifiedValue(freshSlot); ok {
		t.Fatal("fresh unresolved ISR became certified")
	}
	stored, found := f.node.core.Value(sha256.Sum256(fresh))
	if !found || !bytes.Equal(stored, fresh) || bytes.Equal(stored, finalOwner.value) {
		t.Fatal("fresh pending control original was lost or replaced by the owned barrier")
	}
	after, _, readErr := f.node.core.DecisionsFrom(1, int(freshSlot-1))
	if readErr != nil || !reflect.DeepEqual(after, certifiedCut) {
		t.Fatal("fresh ISR changed the captured certified decision/certificate bytes", readErr)
	}
	if err != nil {
		t.Fatalf("fresh unrelated control ISR invalidated completed captured-prefix admission: %v", err)
	}
}

func TestManagedAdmissionPreexistingOriginalStillRequiresQuorumCertificate(t *testing.T) {
	f := newManagedGraphAdmissionRaceFixture(t)
	// Existing fixture withholds both remote recorder replies at slot 9.
	// Set this before any proposal, not concurrently with recorder traffic.
	f.transport.releaseHeldUncertified()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	command := types.GraphCommand{RequestID: "fresh-isr-unresolved-original", Cypher: "CREATE (:FreshISRUnresolved {id:'must-not-apply'})"}
	seed, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	recordFreshAdmissionControl(t, ctx, f, 9, seed, false)
	before, _, err := f.node.core.DecisionsFrom(1, 8)
	if err != nil {
		t.Fatal(err)
	}
	err = f.node.admitInternalApplication(ctx)
	if err == nil {
		t.Fatal("preexisting unresolved ISR passed without a quorum certificate")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "internal application has unresolved certified history") {
		t.Fatalf("quorum refusal failed at an unrelated boundary: %v", err)
	}
	if f.node.core.Tip() != 8 || f.node.material.Tip() != 8 {
		t.Fatal("uncertified recorder state became applied read authority")
	}
	if _, ok := f.node.core.CertifiedValue(9); ok {
		t.Fatal("missing quorum manufactured a slot-9 certificate")
	}
	original, found := f.node.core.Value(sha256.Sum256(seed))
	if !found || !bytes.Equal(original, seed) {
		t.Fatal("quorum refusal lost the unresolved original bytes")
	}
	// Inspect local state on a fresh context after the operation deadline;
	// this is observation only, not a retry of the refused admission.
	inspect, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	fingerprint, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	_, found, _, err = f.node.material.GraphRequestStatusFingerprint(inspect, command.RequestID, fingerprint)
	if err != nil || found {
		t.Fatal("uncertified original acquired an application receipt", err)
	}
	rows, err := f.node.material.GraphQuery(inspect, "MATCH (n:FreshISRUnresolved) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 0 {
		t.Fatal("uncertified original changed application data", err)
	}
	after, _, err := f.node.core.DecisionsFrom(1, 8)
	if err != nil || len(after) != len(before) {
		t.Fatal("quorum refusal changed the certified prefix", err)
	}
	for i := range before {
		if before[i].Slot != after[i].Slot || before[i].Hash != after[i].Hash || !bytes.Equal(before[i].Value, after[i].Value) || !bytes.Equal(before[i].Certificate, after[i].Certificate) {
			t.Fatal("quorum refusal changed certified decision bytes")
		}
	}
}

func freshAdmissionControlValue(start byte) []byte {
	var nonce [types.ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = start + byte(i)
	}
	return types.EncodeReadBarrier(nonce)
}

func recordFreshAdmissionControl(t *testing.T, ctx context.Context, f *managedGraphAdmissionRaceFixture, slot quepaxa.Slot, value []byte, quorum bool) {
	t.Helper()
	ids := []quepaxa.NodeID{"n1"}
	if quorum {
		ids = append(ids, "n2", "n3")
	}
	hash := sha256.Sum256(value)
	for _, id := range ids {
		summary, err := f.transport.graphReviewTransport[id].Record(ctx, quepaxa.RecordRequest{Slot: slot, Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: "n2", Hash: hash, Value: bytes.Clone(value)}})
		if err != nil || summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash {
			t.Fatalf("actual control ISR was not recorded at original slot %d on %s: %v", slot, id, err)
		}
	}
}

func requireFreshAdmissionOwnCertificate(t *testing.T, f *managedGraphAdmissionRaceFixture, captured quepaxa.Slot, original []byte, op *managedGraphControlOperation, proposed []byte) *managedGraphControlOperation {
	t.Helper()
	decision, ok := f.node.core.CertifiedValue(captured)
	if !ok || !bytes.Equal(decision.Value, original) || len(decision.Certificate) == 0 {
		t.Fatal("captured ISR original lacks its exact original-slot certificate")
	}
	if op == nil || op.binding != f.node.internalApplication.Load() || !bytes.Equal(op.value, proposed) || !managedGraphControlOwnBarrierMatches(op, sha256.Sum256(proposed)) {
		t.Fatal("captured ISR recovery lacks the actual proposal-entry owner and nonce bytes")
	}
	select {
	case <-op.done:
	default:
		t.Fatal("owned barrier has not completed")
	}
	owned, certified := f.node.core.CertifiedValue(op.slot)
	if op.err != nil || op.slot <= captured || !certified || len(owned.Certificate) == 0 || !managedGraphControlCertificateMatches(f.node.core, op, owned) || f.node.material.Tip() < uint64(op.slot) {
		t.Fatal("captured ISR recovery lacks its exact own-nonce certificate and apply")
	}
	return op
}
