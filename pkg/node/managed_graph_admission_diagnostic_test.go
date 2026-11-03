package node

import (
	"context"
	"testing"
	"time"
)

func TestManagedGraphAdmissionDiagnosticRecoversRemoteControlAtExactSlot(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = fixture.startHeldControl(ctx)
	if fixture.node.core.Tip() != 8 || fixture.node.core.RecorderTip() != 9 {
		t.Fatalf("fixture did not hold the exact next slot: certified=%d recorded=%d", fixture.node.core.Tip(), fixture.node.core.RecorderTip())
	}
	binding := fixture.node.internalApplication.Load()
	if binding == nil {
		t.Fatal("fixture has no installed application binding")
	}
	result := make(chan error, 1)
	go func() { result <- fixture.node.waitManagedGraphOwnedControl(ctx, binding, 9) }()
	awaitManagedGraphAdmissionObservation(t, fixture, result, 9)
	fixture.transport.releaseHeld()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("remote control was not recovered through verified observation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("verified remote control recovery did not finish", ctx.Err())
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
