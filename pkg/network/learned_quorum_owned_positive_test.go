//go:build rhiza_local_testhooks && rhiza_receipt_quorum_fixture

package network

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestLearnedQuorumOwnsCertifiedReceiptApplyAfterStreamCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE receipt_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if err := p.servers["n3"].catchUpFrom(ctx, "n1", 1, true); err != nil {
		t.Fatal(err)
	}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	if err := p.materials["n3"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	fast := make(chan struct{})
	var fastOnce sync.Once
	allowFast := func() { fastOnce.Do(func() { close(fast) }) }
	t.Cleanup(allowFast)
	streamEvents := make(chan learnedStreamEvent, 16)
	testLearnedStreamEvents.Store(&streamEvents)
	t.Cleanup(func() { testLearnedStreamEvents.Store(nil) })
	var mu sync.Mutex
	var slowAck bool
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:sender=n1:") {
			select {
			case <-fast:
			case <-ctx.Done():
			}
		}
		if strings.Contains(event, "network:learned:phase=response-written:node=n3:sender=n1:") {
			mu.Lock()
			slowAck = true
			mu.Unlock()
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
	case got := <-result:
		t.Fatalf("client returned before slow receipt gate: %v", got.err)
	case <-ctx.Done():
		t.Fatal("slow receipt gate not reached")
	}
	allowFast()
	var remoteStream context.Context
	for remoteStream == nil {
		select {
		case event := <-streamEvents:
			if event.node == "n3" && event.sender == "n1" && event.slot == 2 {
				remoteStream = event.ctx
			}
		case <-ctx.Done():
			t.Fatal("slow stream context not observed")
		}
	}
	select {
	case <-remoteStream.Done():
	case <-ctx.Done():
		t.Fatal("slow QUIC stream not canceled")
	}
	select {
	case <-slowCtx.Done():
		t.Fatal("remote cancel interrupted owned receipt SQL")
	default:
	}
	select {
	case got := <-result:
		if got.err != nil || got.response.Status != types.MutationCommitted || got.response.Slot != 2 {
			t.Fatalf("client: %+v err=%v", got.response.MutationReceipt, got.err)
		}
	case <-ctx.Done():
		t.Fatal("quorum did not complete while slow apply held")
	}
	releaseGate()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, found, err := p.materials["n3"].MutationReceipt(ctx, types.MutationSQL, "probe")
		if err != nil {
			t.Fatalf("slow replica health or receipt: %v", err)
		}
		if found {
			if receipt.Status != types.MutationCommitted || receipt.Slot != 2 {
				t.Fatalf("slow replica receipt: %+v", receipt)
			}
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("owned apply did not complete")
		}
	}
	primary, _, err := p.cores["n1"].DecisionsFrom(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	slow, _, err := p.cores["n3"].DecisionsFrom(2, 1)
	if err != nil || len(primary) != 1 || len(slow) != 1 || primary[0].Slot != slow[0].Slot || primary[0].Hash != slow[0].Hash || !bytes.Equal(primary[0].Value, slow[0].Value) || !bytes.Equal(primary[0].Certificate, slow[0].Certificate) {
		t.Fatalf("certified history changed during handoff: primary=%d slow=%d err=%v", len(primary), len(slow), err)
	}
	rows, err := p.materials["n3"].QueryResult(ctx, "SELECT id FROM receipt_probe", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatalf("slow replica data=%+v err=%v", rows, err)
	}
	if err := p.peerServers[2].Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ack := slowAck
	mu.Unlock()
	if ack {
		t.Fatal("canceled slow stream emitted an ACK")
	}
}

func TestLearnedOwnedApplyServerCloseDuringReceipt(t *testing.T) {
	testLearnedOwnedApplyCloseDuringReceipt(t, false)
}

func TestLearnedOwnedApplyPeerCloseDuringReceipt(t *testing.T) {
	testLearnedOwnedApplyCloseDuringReceipt(t, true)
}

func testLearnedOwnedApplyCloseDuringReceipt(t *testing.T, peerClose bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE receipt_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if err := p.servers["n3"].catchUpFrom(ctx, "n1", 1, true); err != nil {
		t.Fatal(err)
	}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	releaseGate := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	if err := p.materials["n3"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	fast := make(chan struct{})
	var fastOnce sync.Once
	allowFast := func() { fastOnce.Do(func() { close(fast) }) }
	t.Cleanup(allowFast)
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:sender=n1:") {
			select {
			case <-fast:
			case <-ctx.Done():
			}
		}
	})
	t.Cleanup(restore)
	result := make(chan error, 1)
	go func() {
		_, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "probe", SQL: "INSERT INTO receipt_probe VALUES(1)"})
		result <- err
	}()
	var slowCtx context.Context
	select {
	case slowCtx = <-entered:
	case err := <-result:
		t.Fatalf("client returned before receipt gate: %v", err)
	case <-ctx.Done():
		t.Fatal("receipt gate not reached")
	}
	closed := make(chan error, 1)
	go func() {
		if peerClose {
			closed <- p.peerServers[2].Close()
			return
		}
		p.servers["n3"].Close()
		closed <- nil
	}()
	select {
	case <-slowCtx.Done():
	case <-ctx.Done():
		t.Fatal("server close did not cancel owned SQL")
	}
	select {
	case <-closed:
		t.Fatal("server close returned before receipt gate released")
	default:
	}
	releaseGate()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("server close did not join owned SQL")
	}
	if err := p.materials["n3"].Health(ctx); !errors.Is(err, materializer.ErrGraphCommitUnknown) {
		t.Fatalf("local interruption must remain fail closed: %v", err)
	}
	allowFast()
	select {
	case <-result:
	case <-ctx.Done():
		t.Fatal("client did not finish")
	}
}
