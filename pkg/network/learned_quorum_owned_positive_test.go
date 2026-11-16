//go:build rhiza_local_testhooks && rhiza_receipt_quorum_fixture

package network

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedQuorumOwnsCertifiedReceiptApplyAfterStreamCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE receipt_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if err := p.servers["n3"].catchUpFrom(ctx, "n1", p.cores["n1"].Tip(), true); err != nil {
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
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "seed", SQL: "INSERT INTO receipt_probe VALUES(7)"}); err != nil {
		t.Fatal(err)
	}
	if err := p.cores["n1"].WaitTip(ctx, 2); err != nil {
		t.Fatalf("wait for locally certified seed: %v", err)
	}
	if err := p.servers["n3"].catchUpFrom(ctx, "n1", 2, true); err != nil {
		t.Fatal(err)
	}
	seedDecisions, certifiedTip, err := p.cores["n3"].DecisionsFrom(1, 2)
	if err != nil || certifiedTip != 2 || len(seedDecisions) != 2 {
		t.Fatalf("seed decisions tip=%d count=%d err=%v", certifiedTip, len(seedDecisions), err)
	}
	if err := p.materials["n3"].ApplyBatch(ctx, seedDecisions); err != nil {
		t.Fatalf("apply seed decisions: %v", err)
	}
	beforeTip := p.materials["n3"].Tip()
	beforeStateTip := p.materials["n3"].StateTip()
	if beforeTip != 2 || beforeStateTip != 2 {
		t.Fatalf("slow peer initial tip=%d state_tip=%d", beforeTip, beforeStateTip)
	}
	beforeRows, err := p.materials["n3"].QueryResult(ctx, "SELECT id FROM receipt_probe ORDER BY id", nil)
	if err != nil || len(beforeRows.Rows) != 1 || beforeRows.Rows[0][0] != int64(7) {
		t.Fatalf("slow peer initial rows=%+v err=%v", beforeRows.Rows, err)
	}
	if _, found, err := p.materials["n3"].MutationReceipt(ctx, types.MutationSQL, "probe"); err != nil || found {
		t.Fatalf("probe receipt before apply found=%t err=%v", found, err)
	}
	writerID, err := p.materials["n3"].TestWriterConnectionID(ctx)
	if err != nil {
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
	var mu sync.Mutex
	var retired, physicallyClosed, closeFailed bool
	var witnessEvent, applyFailedEvent, applyCompleteEvent, handlerResultEvent string
	var observedEvents []string
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:sender=n1:") {
			select {
			case <-fast:
			case <-ctx.Done():
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if event == fmt.Sprintf("materializer:sqlite:writer:retire:id=%d", writerID) {
			retired = true
		}
		if strings.HasPrefix(event, fmt.Sprintf("materializer:sqlite:writer:auto-rollback:id=%d:", writerID)) {
			witnessEvent = event
		}
		if strings.HasPrefix(event, fmt.Sprintf("materializer:sqlite:writer:close:id=%d:", writerID)) {
			physicallyClosed = true
			closeFailed = strings.HasSuffix(event, ":failed=true")
		}
		if strings.Contains(event, "network:learned:phase=apply-failed:node=n3:sender=n1:slot=3:") {
			applyFailedEvent = event
		}
		if strings.Contains(event, "network:learned:phase=apply-complete:node=n3:sender=n1:slot=3") {
			applyCompleteEvent = event
		}
		if strings.Contains(event, "network:learned:phase=handler-result:node=n3:sender=n1:slot=3:") {
			handlerResultEvent = event
		}
		if strings.Contains(event, ":node=n3:") || strings.HasPrefix(event, "materializer:sqlite:writer:") {
			observedEvents = append(observedEvents, event)
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
	closeDone := make(chan error, 1)
	go func() {
		if peerClose {
			closeDone <- p.peerServers[2].Close()
			return
		}
		p.servers["n3"].Close()
		closeDone <- nil
	}()
	select {
	case <-slowCtx.Done():
	case <-ctx.Done():
		t.Fatal("server close did not cancel owned SQL")
	}
	select {
	case <-closeDone:
		t.Fatal("server close returned before receipt gate released")
	default:
	}
	releaseGate()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("server close did not join owned SQL")
	}
	if err := p.materials["n3"].Health(ctx); err != nil {
		t.Fatalf("confirmed auto-rollback poisoned slow materializer: %v", err)
	}
	if tip, stateTip := p.materials["n3"].Tip(), p.materials["n3"].StateTip(); tip != beforeTip || stateTip != beforeStateTip {
		t.Fatalf("slow peer tip changed across close: %d/%d, want %d/%d", tip, stateTip, beforeTip, beforeStateTip)
	}
	afterCloseRows, err := p.materials["n3"].QueryResult(ctx, "SELECT id FROM receipt_probe ORDER BY id", nil)
	if err != nil || len(afterCloseRows.Rows) != 1 || afterCloseRows.Rows[0][0] != int64(7) {
		t.Fatalf("slow peer rows changed across close=%+v err=%v", afterCloseRows.Rows, err)
	}
	if _, found, err := p.materials["n3"].MutationReceipt(ctx, types.MutationSQL, "probe"); err != nil || found {
		t.Fatalf("interrupted probe exposed receipt found=%t err=%v", found, err)
	}
	mu.Lock()
	gotRetired, gotClosed, gotCloseFailed := retired, physicallyClosed, closeFailed
	gotWitness, gotApplyFailed := witnessEvent, applyFailedEvent
	gotApplyComplete, gotHandlerResult := applyCompleteEvent, handlerResultEvent
	gotEvents := append([]string(nil), observedEvents...)
	mu.Unlock()
	if !gotRetired || !gotClosed || gotCloseFailed {
		t.Fatalf("writer retirement retired=%t closed=%t close_failed=%t", gotRetired, gotClosed, gotCloseFailed)
	}
	if gotWitness == "" || !strings.Contains(gotWitness, ":class=sqlite:code_known=true:code=9") {
		t.Fatalf("auto-rollback witness=%q", gotWitness)
	}
	if gotApplyFailed == "" || !strings.Contains(gotApplyFailed, "sqlite3: interrupted") {
		t.Fatalf("apply failure=%q events=%q", gotApplyFailed, gotEvents)
	}
	if gotApplyComplete != "" {
		t.Fatalf("closed slow peer emitted a success ACK: %q events=%q", gotApplyComplete, gotEvents)
	}
	if gotHandlerResult == "" || strings.HasSuffix(gotHandlerResult, `:error=""`) {
		t.Fatalf("closed slow peer returned success: %q events=%q", gotHandlerResult, gotEvents)
	}
	if err := p.materials["n3"].TestRemoveReceiptGate(ctx); err != nil {
		t.Fatal(err)
	}
	replacementID, err := p.materials["n3"].TestWriterConnectionID(ctx)
	if err != nil || replacementID == writerID {
		t.Fatalf("old writer connection reused: old=%d new=%d err=%v", writerID, replacementID, err)
	}
	decision, ok := p.cores["n1"].CertifiedValue(3)
	if !ok || decision.Slot != 3 || len(decision.Certificate) == 0 {
		t.Fatalf("certified replay missing: found=%t slot=%d certificate=%d", ok, decision.Slot, len(decision.Certificate))
	}
	if err := p.materials["n3"].ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatalf("reapply certified slot: %v", err)
	}
	if receipt, found, err := p.materials["n3"].MutationReceipt(ctx, types.MutationSQL, "probe"); err != nil || !found || receipt.Slot != 3 || receipt.Status != types.MutationCommitted {
		t.Fatalf("replayed receipt=%+v found=%t err=%v", receipt, found, err)
	}
	replayedRows, err := p.materials["n3"].QueryResult(ctx, "SELECT id FROM receipt_probe ORDER BY id", nil)
	if err != nil || len(replayedRows.Rows) != 2 || replayedRows.Rows[0][0] != int64(1) || replayedRows.Rows[1][0] != int64(7) {
		t.Fatalf("replayed rows=%+v err=%v", replayedRows.Rows, err)
	}
	if tip, stateTip := p.materials["n3"].Tip(), p.materials["n3"].StateTip(); tip != 3 || stateTip != 3 {
		t.Fatalf("replayed tip=%d state_tip=%d, want 3/3", tip, stateTip)
	}
	allowFast()
	select {
	case <-result:
	case <-ctx.Done():
		t.Fatal("client did not finish")
	}
}
