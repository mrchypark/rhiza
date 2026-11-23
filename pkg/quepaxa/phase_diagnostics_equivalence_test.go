package quepaxa

import (
	"bytes"
	"context"
	"testing"
)

// The same assertions run with and without the diagnostic build tag in CI.
func TestPhaseDiagnosticBehavior(t *testing.T) {
	cores, transport := newTestCluster(t)
	value := []byte("phase diagnostic behavior")
	slot, _, err := cores["n2"].Propose(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := cores["n2"].CertifiedValue(slot)
	if !ok || !bytes.Equal(decision.Value, value) {
		t.Fatalf("decision not preserved: slot=%d", slot)
	}
	transport.fail("n1", "n3")
	if _, err := cores["n2"].runSlot(context.Background(), 100, []byte("unavailable"), true); err == nil {
		t.Fatal("quorum loss must fail")
	}
}
