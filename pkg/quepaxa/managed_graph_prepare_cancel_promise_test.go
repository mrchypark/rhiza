package quepaxa

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestPrepareCheckpointCanceledSyncKeepsLowerSealBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{ConfigID: 1, Members: members}
	transport := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	cores := transport.cores
	walDirs := make(map[NodeID]string, len(members))
	wals := make(map[NodeID]*qlog.WAL, len(members))
	for _, member := range members {
		dir := filepath.Join(t.TempDir(), string(member.ID))
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		walDirs[member.ID] = dir
		wals[member.ID] = wal
		cores[member.ID] = newCore(member.ID, config, wal, transport)
	}
	t.Cleanup(func() {
		for _, wal := range wals {
			if wal != nil {
				_ = wal.Close()
			}
		}
	})

	source, target := cores["n1"], cores["n3"]
	for i := 0; i < 7; i++ {
		if slot, _, err := source.Propose(ctx, []byte{byte(i + 1)}); err != nil || slot != Slot(i+1) {
			t.Fatalf("certify prefix slot %d: returned slot=%d err=%v", i+1, slot, err)
		}
	}
	for id, core := range cores {
		if err := core.WaitTip(ctx, 7); err != nil {
			t.Fatalf("core %s did not learn certified prefix: %v", id, err)
		}
	}
	if _, ok := target.CertifiedValue(7); !ok {
		t.Fatal("target lacks actual certified prefix decision at slot 7")
	}

	makeSeal := func(index Slot, label string) CheckpointSeal {
		t.Helper()
		prefix, ok := source.PrefixHash(index)
		if !ok {
			t.Fatalf("certified prefix %d missing", index)
		}
		next, following, err := source.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatalf("checkpoint leader order at %d: %v", index, err)
		}
		return CheckpointSeal{
			ConfigID: 1, Index: index, RootHash: sha256.Sum256([]byte(label)),
			StateHash: sha256.Sum256([]byte("state-" + label)), PrefixHash: prefix,
			NextLeaderOrder: next, FollowingLeaderOrder: following,
		}
	}
	lowSeal := makeSeal(5, "cancel-pending-low")
	higherSeal := makeSeal(7, "cancel-pending-higher")
	// This callback stands in for the external paired-root verifier. It only
	// accepts the exact test identities; Core still checks certified prefix,
	// config, and leader-order fields through the ordinary prepare path.
	for _, core := range cores {
		core.SetCheckpointValidator(func(_ context.Context, seal CheckpointSeal) error {
			if (seal.Index == lowSeal.Index && seal.RootHash == lowSeal.RootHash && seal.StateHash == lowSeal.StateHash) ||
				(seal.Index == higherSeal.Index && seal.RootHash == higherSeal.RootHash && seal.StateHash == higherSeal.StateHash) {
				return nil
			}
			return errors.New("unknown test root identity")
		})
		if err := core.PrepareCheckpoint(ctx, lowSeal); err != nil {
			t.Fatalf("prepare lower seal at %s: %v", core.nodeID, err)
		}
	}
	lowValue, err := EncodeCheckpointSeal(lowSeal)
	if err != nil {
		t.Fatal(err)
	}
	lowHash := ValueHash(sha256.Sum256(lowValue))
	if _, root, ok := target.LatestPreparedCheckpoint(); !ok || root != lowSeal.RootHash {
		t.Fatal("lower prepared marker was not established before higher prepare")
	}

	markerSyncEntered := make(chan struct{})
	releaseMarkerSync := make(chan struct{})
	markerSyncDone := make(chan struct{})
	var releaseOnce sync.Once
	var syncCalls atomic.Int32
	defer releaseOnce.Do(func() { close(releaseMarkerSync) })
	target.commits = newGroupCommit(func() error {
		if syncCalls.Add(1) == 1 {
			close(markerSyncEntered)
			<-releaseMarkerSync
			err := target.wal.Sync()
			close(markerSyncDone)
			return err
		}
		return target.wal.Sync()
	})

	higherCtx, cancelHigher := context.WithCancel(ctx)
	higherDone := make(chan error, 1)
	go func() { higherDone <- target.PrepareCheckpoint(higherCtx, higherSeal) }()
	select {
	case <-markerSyncEntered:
	case <-ctx.Done():
		t.Fatal("higher prepare marker Sync did not enter the blocking callback")
	}

	readEvidence := func(wal *qlog.WAL) (bool, bool, int, error) {
		t.Helper()
		entries, err := wal.Read()
		if err != nil {
			return false, false, 0, err
		}
		var sawLowMarker, sawHigherMarker bool
		lowReceiptCount := 0
		for _, entry := range entries {
			if entry.Type == qlog.EntryCheckpointVerified {
				seal, checkpoint, decodeErr := DecodeCheckpointSeal(entry.Payload)
				if decodeErr != nil || !checkpoint || entry.Slot != uint64(seal.Index) || entry.Hash != seal.RootHash {
					return false, false, 0, errors.New("invalid checkpoint marker in actual WAL")
				}
				if seal.Index == lowSeal.Index && seal.RootHash == lowSeal.RootHash {
					sawLowMarker = true
				}
				if seal.Index == higherSeal.Index && seal.RootHash == higherSeal.RootHash {
					sawHigherMarker = true
				}
			}
			if entry.Type == qlog.EntryReceipt {
				persisted, decodeErr := decodeRecorderEntry(entry.Payload)
				if decodeErr != nil {
					return false, false, 0, decodeErr
				}
				if persisted.Slot == 8 {
					for _, proposal := range []*Proposal{persisted.State.FirstCurrent, persisted.State.AggregateCurrent, persisted.State.AggregatePrior} {
						if proposal != nil && proposal.Hash == lowHash {
							lowReceiptCount++
							break
						}
					}
				}
			}
		}
		return sawLowMarker, sawHigherMarker, lowReceiptCount, nil
	}
	lowMarkerBefore, higherMarkerBefore, receiptsBefore, err := readEvidence(target.wal)
	if err != nil {
		t.Fatal(err)
	}
	if !lowMarkerBefore || !higherMarkerBefore || receiptsBefore != 0 {
		t.Fatalf("pre-cancel WAL evidence lowMarker=%t higherMarker=%t lowReceipts=%d", lowMarkerBefore, higherMarkerBefore, receiptsBefore)
	}
	if index, root, ok := target.LatestPreparedCheckpoint(); !ok || index != lowSeal.Index || root != lowSeal.RootHash {
		t.Fatalf("blocked higher prepare exposed authorization before Sync: index=%d root=%x present=%t", index, root, ok)
	}
	cancelHigher()
	select {
	case err := <-higherDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("higher prepare returned %v, want caller cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled higher prepare did not return")
	}
	if index, root, ok := target.LatestPreparedCheckpoint(); !ok || index != lowSeal.Index || root != lowSeal.RootHash {
		t.Fatalf("canceled higher prepare published authorization: index=%d root=%x present=%t", index, root, ok)
	}
	if _, ok := target.CertifiedValue(8); ok {
		t.Fatal("slot 8 was certified before the lower-seal Record attempt")
	}

	// While the appended marker's real Sync remains unresolved, a lower seal
	// Record must not append a receipt or acknowledge the caller.
	recordCtx, cancelRecord := context.WithCancel(ctx)
	type recordResult struct {
		summary Summary
		err     error
	}
	recordDone := make(chan recordResult, 1)
	request := RecordRequest{Slot: 8, Step: 4, ConfigID: 1, Proposal: newProposal(highestPriority, "n3", lowValue)}
	go func() {
		summary, err := target.Record(recordCtx, request)
		recordDone <- recordResult{summary: summary, err: err}
	}()
	var pendingRecordResult recordResult
	var pendingRecordFinished, receiptWhilePending bool
	pendingDeadline := time.NewTimer(5 * time.Second)
	pendingTicker := time.NewTicker(time.Millisecond)
	for !pendingRecordFinished && !receiptWhilePending {
		select {
		case pendingRecordResult = <-recordDone:
			pendingRecordFinished = true
		case <-pendingTicker.C:
			_, _, count, readErr := readEvidence(target.wal)
			if readErr != nil {
				cancelRecord()
				pendingTicker.Stop()
				pendingDeadline.Stop()
				t.Fatal(readErr)
			}
			receiptWhilePending = count > 0
		case <-pendingDeadline.C:
			cancelRecord()
			pendingRecordResult = <-recordDone
			pendingRecordFinished = true
		case <-ctx.Done():
			cancelRecord()
			pendingRecordResult = <-recordDone
			pendingRecordFinished = true
		}
	}
	pendingTicker.Stop()
	pendingDeadline.Stop()
	if receiptWhilePending || pendingRecordResult.err == nil || pendingRecordResult.summary.FirstCurrent != nil {
		cancelRecord()
		if !pendingRecordFinished {
			pendingRecordResult = <-recordDone
			pendingRecordFinished = true
		}
	}
	cancelRecord()
	if pendingRecordFinished && pendingRecordResult.err == nil {
		t.Errorf("lower checkpoint Record ACKed while higher marker Sync was unresolved")
	}
	if receiptWhilePending {
		t.Errorf("lower checkpoint receipt was appended while higher marker Sync was unresolved; caller result=%v", pendingRecordResult.err)
	}
	if _, ok := target.CertifiedValue(8); ok {
		t.Errorf("slot 8 became certified during the pending lower-seal Record attempt")
	}

	releaseOnce.Do(func() { close(releaseMarkerSync) })
	select {
	case <-markerSyncDone:
	case <-ctx.Done():
		t.Fatal("late actual WAL Sync did not complete")
	}
	tick := time.NewTicker(time.Millisecond)
	for {
		target.commits.mu.Lock()
		running := target.commits.running
		target.commits.mu.Unlock()
		if !running {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			tick.Stop()
			t.Fatal("group commit did not become idle after late flush")
		}
	}
	tick.Stop()
	lowMarkerAfter, higherMarkerAfter, receiptsAfter, err := readEvidence(target.wal)
	if err != nil {
		t.Fatal(err)
	}
	if !lowMarkerAfter || !higherMarkerAfter {
		t.Fatalf("synced WAL missing prepared markers low=%t higher=%t", lowMarkerAfter, higherMarkerAfter)
	}

	// Once the delayed flush has completed, another lower-seal Record must
	// still fail. Record its outcome, but continue to reopen the actual WAL even
	// on the baseline so marker, certificate, and recovery evidence stay apart.
	postFlushSummary, postFlushErr := target.Record(ctx, request)
	_, _, receiptsAfterPostFlushRecord, err := readEvidence(target.wal)
	if err != nil {
		t.Fatal(err)
	}
	if postFlushErr == nil || postFlushSummary.FirstCurrent != nil {
		t.Errorf("lower checkpoint Record passed after marker flush: err=%v", postFlushErr)
	}
	if receiptsAfterPostFlushRecord > receiptsAfter {
		t.Errorf("lower checkpoint appended another receipt after marker flush: before=%d after=%d", receiptsAfter, receiptsAfterPostFlushRecord)
	}
	if _, ok := target.CertifiedValue(8); ok {
		t.Errorf("slot 8 became certified after the lower-seal Record following marker flush")
	}

	if err := wals["n3"].Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := qlog.Open(walDirs["n3"])
	if err != nil {
		t.Fatal(err)
	}
	wals["n3"] = reopenedWAL
	reopened := newCore("n3", config, reopenedWAL, transport)
	if err := reopened.recover(); err != nil {
		t.Fatalf("recover actual WAL after canceled prepare Sync: %v", err)
	}
	transport.setCore("n3", reopened)
	cores["n3"] = reopened
	if index, root, ok := reopened.LatestPreparedCheckpoint(); !ok || index != higherSeal.Index || root != higherSeal.RootHash {
		t.Fatalf("reopened durable marker fence=(%d,%x,%t), want higher index/root", index, root, ok)
	}
	if reopened.Tip() != 7 {
		t.Fatalf("reopened certified prefix tip=%d, want 7", reopened.Tip())
	}
	if _, ok := reopened.CertifiedValue(8); ok {
		t.Errorf("reopen recovered an unexpected slot-8 certificate")
	}
	certified, ok := reopened.CertifiedValue(7)
	if !ok || sha256.Sum256(certified.Value) != certified.Hash {
		t.Fatal("reopened WAL lacks the actual slot-7 prefix certificate/value")
	}
	recoveredLowReceipt := false
	reopened.mu.RLock()
	state := reopened.recorders[8]
	for _, proposal := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
		recoveredLowReceipt = recoveredLowReceipt || proposal != nil && proposal.Hash == lowHash
	}
	reopened.mu.RUnlock()
	if recoveredLowReceipt {
		t.Errorf("reopen restored a lower-seal slot-8 receipt alongside the higher prepared fence")
	}
	beforeReopenRecord, _, _, err := readEvidence(reopened.wal)
	if err != nil || !beforeReopenRecord {
		t.Fatalf("reopened WAL marker evidence low=%t err=%v", beforeReopenRecord, err)
	}
	reopenSummary, reopenErr := reopened.Record(ctx, request)
	if reopenErr == nil || reopenSummary.FirstCurrent != nil {
		t.Errorf("reopened higher fence did not reject lower seal Record: err=%v", reopenErr)
	}
	if _, ok := reopened.CertifiedValue(8); ok {
		t.Errorf("slot 8 became certified after the lower-seal Record against the reopened higher fence")
	}
	_, _, receiptsAfterReopenRecord, err := readEvidence(reopened.wal)
	if err != nil {
		t.Fatal(err)
	}
	if receiptsAfterReopenRecord > receiptsAfterPostFlushRecord {
		t.Errorf("reopened lower Record appended another receipt: before=%d after=%d", receiptsAfterPostFlushRecord, receiptsAfterReopenRecord)
	}

	if receiptsAfter > 0 || receiptsAfterPostFlushRecord > 0 {
		t.Errorf("lower-seal receipts persisted: while-pending=%t after-flush=%d", receiptWhilePending, receiptsAfterPostFlushRecord)
	}
}
