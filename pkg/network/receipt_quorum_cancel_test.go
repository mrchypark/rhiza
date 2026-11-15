//go:build rhiza_local_testhooks && rhiza_receipt_quorum_fixture && rhiza_receipt_legacy_repro

package network

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedQuorumCancelsReceiptDML(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE receipt_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	// This lightweight fixture does not start Node's background catch-up.
	if err := p.servers["n3"].catchUpFrom(ctx, "n1", 1, true); err != nil {
		t.Fatalf("prepare certified schema on slow peer: %v", err)
	}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	if err := p.materials["n3"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	fastAllowed := make(chan struct{})
	var fastOnce sync.Once
	allowFast := func() { fastOnce.Do(func() { close(fastAllowed) }) }
	t.Cleanup(allowFast)
	var mu sync.Mutex
	var failure string
	var slowHandler string
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:sender=n1:") {
			select {
			case <-fastAllowed:
			case <-ctx.Done():
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(event, "network:learned:phase=apply-failed:node=n3:sender=n1:") {
			failure = event
		}
		if strings.Contains(event, "network:learned:phase=handler-result:node=n3:sender=n1:") {
			slowHandler = event
		}
	})
	t.Cleanup(restore)
	type reply struct {
		response ExecuteResponse
		err      error
	}
	result := make(chan reply, 1)
	go func() {
		response, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "probe", SQL: "INSERT INTO receipt_probe VALUES(1)"})
		result <- reply{response, err}
	}()
	var slowCtx context.Context
	select {
	case slowCtx = <-entered:
	case early := <-result:
		t.Fatalf("request finished before receipt gate: %v", early.err)
	case <-ctx.Done():
		t.Fatal("receipt gate not reached")
	}
	allowFast()
	select {
	case <-slowCtx.Done():
	case <-ctx.Done():
		t.Fatal("slow peer context not canceled by fast quorum")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("parent context canceled: %v", err)
	}
	releaseGate()
	select {
	case got := <-result:
		if got.err != nil || got.response.Status != types.MutationCommitted || got.response.Slot != 2 {
			t.Fatalf("client request: receipt=%+v err=%v", got.response.MutationReceipt, got.err)
		}
	case <-ctx.Done():
		t.Fatal("client did not return")
	}
	for {
		mu.Lock()
		observed, handler := failure, slowHandler
		mu.Unlock()
		if observed != "" && handler != "" {
			if strings.HasSuffix(handler, `:error=""`) {
				t.Fatal("slow handler returned success")
			}
			if !strings.Contains(observed, "ctx_canceled=true") || !strings.Contains(observed, "record SQL receipts: sqlite3: interrupted") || !strings.Contains(observed, "commit outcome is unknown") {
				t.Fatalf("unexpected slow apply failure: %s", observed)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("slow apply failure not observed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	for _, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		if !p.cores[id].IsDecided(2) {
			t.Fatalf("%s missing certified decision", id)
		}
	}
	// PeerServer.Close joins all stream handlers, so the final observation is
	// made only after the slow Learned handler can no longer send a response.
	if err := p.peerServers[2].Close(); err != nil {
		t.Fatalf("close slow peer: %v", err)
	}
	mu.Lock()
	handler := slowHandler
	mu.Unlock()
	if handler == "" || strings.HasSuffix(handler, `:error=""`) {
		t.Fatalf("slow handler returned success: %q", handler)
	}
}
