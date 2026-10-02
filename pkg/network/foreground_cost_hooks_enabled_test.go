//go:build rhiza_local_testhooks

package network

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func installForegroundLearnHook(t testing.TB, observation *foregroundLearnObservation) {
	t.Helper()
	restore := localtesthooks.Set(observation.capture)
	t.Cleanup(restore)
}

func TestForegroundLearnFailureDiagnosticIsBounded(t *testing.T) {
	if !localtesthooks.Enabled {
		t.Skip("requires rhiza_local_testhooks build tag")
	}
	root := t.TempDir()
	peers := newForegroundAPIPeers(t, root)
	observation := &foregroundLearnObservation{}
	var closeClientTransport sync.Once
	restore := localtesthooks.Set(func(event string) {
		observation.capture(event)
		if strings.Contains(event, "network:learned:phase=received:node=n") {
			closeClientTransport.Do(func() { _ = peers.transports[0].Close() })
		}
	})
	t.Cleanup(restore)

	value := quepaxa.EncodeReadBarrier([quepaxa.ReadBarrierNonceSize]byte{1, 2, 3, 4})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := peers.servers["n1"].ProposeControl(ctx, value)
	if err == nil {
		t.Fatal("ProposeControl unexpectedly succeeded after the learner transport was closed")
	}

	event := observation.firstEvent()
	for _, want := range []string{
		"network:foreground-learn-failure:", "node=n1", "kind=read-barrier",
		"phase=read-barrier-advance", "slot=", fmt.Sprintf("hash=%x", sha256.Sum256(value)),
		"peers=", "n2=", "n3=",
	} {
		if !strings.Contains(event, want) {
			t.Fatalf("first event %q missing %q", event, want)
		}
	}
	if strings.Contains(event, "nonce") || strings.Contains(event, "payload=") {
		t.Fatalf("diagnostic exposed a payload or nonce: %q", event)
	}
	if got := observation.eventCount(); got != 1 {
		t.Fatalf("emitted failure event count=%d, want 1", got)
	}

	summary := foregroundLearnObservationSummary(peers, observation)
	if !strings.Contains(summary, "slot=1") || !strings.Contains(summary, "observation_time_local_certified=") {
		t.Fatalf("post-return local snapshot missing or not tied to first failure: %q", summary)
	}
	t.Logf("forced ProposeControl failure event: %s", event)
	t.Logf("post-return local peer state: %s", summary)
}
