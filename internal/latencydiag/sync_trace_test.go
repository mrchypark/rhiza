//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func TestSyncTraceIdentityAndBoundedLoss(t *testing.T) {
	ctx := WithSync(context.Background(), 9)
	if SyncID(ctx) != 9 {
		t.Fatal("lost request identity")
	}
	peer := "private-peer-token"
	start := time.Now()
	event := syncEvent("peer_sync_fetch", 9, 3, 128, peer, errors.New("private error"), start)
	want := sha256.Sum256([]byte(peer))
	if event.batch != 9 || event.stage != "peer_sync_fetch" || event.slot != 3 || event.count != 128 || event.peer != [8]byte(want[:8]) || event.outcome != "error" {
		t.Fatalf("wrong bounded trace: %+v", event)
	}
	var bounded eventRing
	for i := 0; i < capacity+1; i++ {
		bounded.add(event)
	}
	items, dropped := bounded.drain()
	if len(items) != capacity || dropped != 1 {
		t.Fatalf("len=%d dropped=%d", len(items), dropped)
	}
}
