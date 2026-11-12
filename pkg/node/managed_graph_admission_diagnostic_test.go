package node

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphAdmissionDiagnosticRecoversRemoteControlAtExactSlot(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	binding := fixture.node.internalApplication.Load()
	if binding == nil {
		t.Fatal("fixture has no installed application binding")
	}
	result := make(chan error, 1)
	var proofSlot quepaxa.Slot
	go func() {
		slot, err := fixture.node.waitManagedAppendOnlyControlProof(ctx, binding)
		proofSlot = slot
		result <- err
	}()
	awaitManagedGraphAdmissionOwner(t, fixture, result, 9)
	fixture.node.managedGraphControlMu.Lock()
	owned := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if owned == nil {
		t.Fatal("actual own-nonce owner missing")
	}
	fixture.transport.releaseHeld()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("remote control was not recovered through verified observation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("verified remote control recovery did not finish", ctx.Err())
	}
	proof, exists := fixture.node.core.CertifiedValue(proofSlot)
	if proofSlot == 0 || proofSlot != owned.slot || !exists || !managedGraphControlCertificateMatches(fixture.node.core, owned, proof) || fixture.node.material.Tip() < uint64(proofSlot) {
		t.Fatal("returned proof is not the exact certified/applied own-nonce barrier")
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	want := managedGraphRaceControlValue()
	if !ok || string(decision.Value) != string(want) {
		t.Fatalf("slot 9 winner was not the exact remote control: ok=%t hash=%x", ok, decision.Hash)
	}
	if fixture.node.material.Tip() < 9 {
		t.Fatalf("admission returned before the observed control was replayed: applied=%d", fixture.node.material.Tip())
	}
}

func TestManagedAppendOnlyControlNoOpDoesNotReturnCertificateProof(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	before := fixture.node.core.Tip()
	slot, err := fixture.node.waitManagedAppendOnlyControlProof(context.Background(), fixture.node.internalApplication.Load())
	if err != nil || slot != 0 || fixture.node.core.Tip() != before {
		t.Fatalf("no-op invented certificate proof: slot=%d err=%v", slot, err)
	}
	slot, err = fixture.node.waitManagedAppendOnlyControlProof(context.Background(), nil)
	if err == nil || slot != 0 || fixture.node.core.Tip() != before {
		t.Fatalf("invalid binding returned certificate proof: slot=%d err=%v", slot, err)
	}
}
