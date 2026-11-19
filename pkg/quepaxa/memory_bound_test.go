package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func numberedTestPayload(seq int, size int) []byte {
	payload := make([]byte, max(16, size))
	binary.BigEndian.PutUint64(payload[:8], uint64(seq))
	for i := 8; i < len(payload); i++ {
		payload[i] = byte(seq*31 + i)
	}
	return payload
}

func TestCoreEvictsColdDurableDecisionsAndServesFromWAL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	const (
		proposals = 40
		maxSlots  = 8
		maxBytes  = int64(8 * 1024)
	)
	core, err := New(Config{
		NodeID:                   "n1",
		Cluster:                  Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}},
		WAL:                      wal,
		MaxRetainedDecisionSlots: maxSlots,
		MaxRetainedDecisionBytes: maxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}

	userPayloads := make(map[Slot][]byte, proposals)
	for seq := 1; seq <= proposals; seq++ {
		payload := numberedTestPayload(seq, 512)
		slot, _, err := core.Propose(ctx, payload)
		if err != nil {
			t.Fatalf("propose %d: %v", seq, err)
		}
		userPayloads[slot] = payload
	}
	totalSlots := core.Tip()
	if totalSlots < proposals {
		t.Fatalf("totalSlots=%d, want >= %d", totalSlots, proposals)
	}

	core.mu.RLock()
	hotCount := len(core.decided)
	hotBytes := core.hotDecisionBytes
	evictedThrough := core.evictedThrough
	coldCount := len(core.cold)
	durableCount := len(core.durable)
	loggedCount := len(core.logged)
	_, slot1Hot := core.decided[1]
	_, tipHot := core.decided[totalSlots]
	core.mu.RUnlock()

	if evictedThrough < 20 || coldCount != int(evictedThrough) {
		t.Fatalf("evictedThrough=%d coldCount=%d, want substantial cold eviction", evictedThrough, coldCount)
	}
	if hotCount > maxSlots || hotBytes > maxBytes {
		t.Fatalf("hot retention exceeded budget: hotCount=%d (max %d) hotBytes=%d (max %d)", hotCount, maxSlots, hotBytes, maxBytes)
	}
	if durableCount > hotCount || loggedCount > hotCount {
		t.Fatalf("metadata maps leaked evicted slots: durable=%d logged=%d hot=%d", durableCount, loggedCount, hotCount)
	}
	if slot1Hot || !tipHot {
		t.Fatalf("unexpected hot residency: slot1Hot=%t tipHot=%t", slot1Hot, tipHot)
	}

	all, tip, err := core.DecisionsFrom(1, int(totalSlots))
	if err != nil || tip != totalSlots || len(all) != int(totalSlots) {
		t.Fatalf("DecisionsFrom(1, %d): len=%d tip=%d err=%v", totalSlots, len(all), tip, err)
	}
	expectedHashes := make(map[Slot]ValueHash, totalSlots)
	expectedPrefixes := make(map[Slot][32]byte, totalSlots)
	var runningPrefix [32]byte
	for i, decision := range all {
		wantSlot := Slot(i + 1)
		if decision.Slot != wantSlot {
			t.Fatalf("DecisionsFrom slot mismatch at index %d: got %d, want %d", i, decision.Slot, wantSlot)
		}
		if expected, ok := userPayloads[wantSlot]; ok && !bytes.Equal(decision.Value, expected) {
			t.Fatalf("DecisionsFrom user payload mismatch at slot %d", wantSlot)
		}
		expectedHashes[wantSlot] = decision.Hash
		runningPrefix = AdvancePrefixHash(runningPrefix, wantSlot, decision.Hash)
		expectedPrefixes[wantSlot] = runningPrefix
	}

	var paged []DecidedValue
	for next := Slot(1); next <= totalSlots; {
		page, _, err := core.DecisionsFromBounded(next, 10, 4*1024)
		if err != nil {
			t.Fatalf("DecisionsFromBounded(%d): %v", next, err)
		}
		if len(page) == 0 {
			t.Fatalf("DecisionsFromBounded(%d) returned empty page", next)
		}
		paged = append(paged, page...)
		next = page[len(page)-1].Slot + 1
	}
	if len(paged) != int(totalSlots) {
		t.Fatalf("bounded pages collected %d decisions, want %d", len(paged), totalSlots)
	}

	for _, slot := range []Slot{1, evictedThrough, evictedThrough + 1, totalSlots} {
		val, ok := core.CertifiedValue(slot)
		if !ok || val.Hash != expectedHashes[slot] {
			t.Fatalf("CertifiedValue(%d) ok=%t match=%t", slot, ok, val.Hash == expectedHashes[slot])
		}
		if expected, isUser := userPayloads[slot]; isUser && !bytes.Equal(val.Value, expected) {
			t.Fatalf("CertifiedValue(%d) payload mismatch", slot)
		}
	}
	for slot := Slot(1); slot <= totalSlots; slot++ {
		prefix, ok := core.PrefixHash(slot)
		if !ok || prefix != expectedPrefixes[slot] {
			t.Fatalf("PrefixHash(%d) ok=%t match=%t", slot, ok, prefix == expectedPrefixes[slot])
		}
	}

	learnCtx, learnCancel := context.WithCancel(ctx)
	defer learnCancel()
	ch, err := core.Learn(learnCtx, 1)
	if err != nil {
		t.Fatalf("Learn(1): %v", err)
	}
	seenLearn := make(map[Slot]ValueHash, totalSlots)
	for len(seenLearn) < int(totalSlots) {
		select {
		case item := <-ch:
			seenLearn[item.Slot] = item.Hash
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Learn events: got %d of %d", len(seenLearn), totalSlots)
		}
	}
	for slot := Slot(1); slot <= totalSlots; slot++ {
		if seenLearn[slot] != expectedHashes[slot] {
			t.Fatalf("Learn hash mismatch at slot %d", slot)
		}
	}
}

func TestCoreColdEvictionSurvivesCompactionAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		NodeID:                   "n1",
		Cluster:                  Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}},
		WAL:                      wal,
		MaxRetainedDecisionSlots: 6,
		MaxRetainedDecisionBytes: 1 << 20,
	}
	core, err := New(cfg)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}

	payloads := make(map[Slot][]byte)
	for seq := 1; seq <= 12; seq++ {
		p := numberedTestPayload(seq, 256)
		slot, _, err := core.Propose(ctx, p)
		if err != nil {
			_ = wal.Close()
			t.Fatalf("propose %d: %v", seq, err)
		}
		payloads[slot] = p
	}

	core.mu.RLock()
	preEvicted := core.evictedThrough
	core.mu.RUnlock()
	if preEvicted < 4 {
		_ = wal.Close()
		t.Fatalf("expected cold eviction before compaction, got evictedThrough=%d", preEvicted)
	}

	const compactFloor Slot = 3
	prefix3, ok := core.PrefixHash(compactFloor)
	if !ok {
		_ = wal.Close()
		t.Fatalf("PrefixHash(%d) unavailable for cold slot", compactFloor)
	}
	order, err := core.LeaderOrder(compactFloor + 1)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID:        1,
		Index:           compactFloor,
		RootHash:        sha256.Sum256([]byte("compact-root-3")),
		StateHash:       sha256.Sum256([]byte("compact-state-3")),
		PrefixHash:      prefix3,
		NextLeaderOrder: order,
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		_ = wal.Close()
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(ctx, encodedSeal)
	if err != nil {
		_ = wal.Close()
		t.Fatalf("propose seal: %v", err)
	}
	payloads[sealSlot] = encodedSeal

	if err := core.CompactThrough(compactFloor, seal.RootHash); err != nil {
		_ = wal.Close()
		t.Fatalf("CompactThrough(%d): %v", compactFloor, err)
	}
	floor, _, ok := core.RecoveryRoot()
	if !ok || floor != compactFloor || core.Tip() != sealSlot {
		_ = wal.Close()
		t.Fatalf("after compaction floor=%d ok=%t tip=%d, want floor=%d tip=%d", floor, ok, core.Tip(), compactFloor, sealSlot)
	}

	for seq := int(sealSlot) + 1; seq <= 25; seq++ {
		p := numberedTestPayload(seq, 256)
		slot, _, err := core.Propose(ctx, p)
		if err != nil {
			_ = wal.Close()
			t.Fatalf("post-compaction propose %d: %v", seq, err)
		}
		payloads[slot] = p
	}

	core.mu.RLock()
	postHotCount := len(core.decided)
	postEvicted := core.evictedThrough
	core.mu.RUnlock()
	if postHotCount > cfg.MaxRetainedDecisionSlots || postEvicted <= 13 {
		_ = wal.Close()
		t.Fatalf("post-compaction eviction failed: hotCount=%d evictedThrough=%d", postHotCount, postEvicted)
	}

	wantTipPrefix, ok := core.PrefixHash(25)
	if !ok {
		_ = wal.Close()
		t.Fatal("tip prefix unavailable after post-compaction eviction")
	}
	wantMidPrefix, ok := core.PrefixHash(10)
	if !ok {
		_ = wal.Close()
		t.Fatal("evicted suffix prefix unavailable after post-compaction eviction")
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedWAL, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopenedWAL.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	}()
	cfg.WAL = reopenedWAL
	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen Core: %v", err)
	}
	reopenedFloor, _, ok := reopened.RecoveryRoot()
	if !ok || reopenedFloor != compactFloor || reopened.Tip() != 25 {
		t.Fatalf("reopened floor=%d ok=%t tip=%d, want %d / 25", reopenedFloor, ok, reopened.Tip(), compactFloor)
	}
	if got, ok := reopened.PrefixHash(25); !ok || got != wantTipPrefix {
		t.Fatalf("reopened tip prefix ok=%t match=%t", ok, got == wantTipPrefix)
	}
	if got, ok := reopened.PrefixHash(10); !ok || got != wantMidPrefix {
		t.Fatalf("reopened cold suffix prefix ok=%t match=%t", ok, got == wantMidPrefix)
	}
	suffix, tip, err := reopened.DecisionsFrom(compactFloor+1, 30)
	if err != nil || tip != 25 || len(suffix) != 22 {
		t.Fatalf("reopened DecisionsFrom: len=%d tip=%d err=%v", len(suffix), tip, err)
	}
	for _, d := range suffix {
		if !bytes.Equal(d.Value, payloads[d.Slot]) {
			t.Fatalf("reopened payload mismatch at slot %d", d.Slot)
		}
	}
}

func TestCoreColdEvictionPreservesManagedCheckpointSealAndReconstructionCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, wal := managedAppendOnlyTestConfig(t, managedGraphRequestAppendOnlyApplicationProtocol, func(context.Context, ManagedCheckpointCandidate) error {
		return nil
	})
	config.MaxRetainedDecisionSlots = 4
	config.MaxRetainedDecisionBytes = 1 << 20
	core, err := New(config)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	if slot, _, err := core.Propose(ctx, numberedTestPayload(1, 128)); err != nil || slot != 1 {
		t.Fatalf("propose slot 1: slot=%d err=%v", slot, err)
	}
	prefix1, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing prefix at slot 1")
	}
	next1, err := core.LeaderOrder(2)
	if err != nil {
		t.Fatal(err)
	}
	seal1 := CheckpointSeal{
		ConfigID:        1,
		Index:           1,
		RootHash:        sha256.Sum256([]byte("managed-cold-root-1")),
		StateHash:       sha256.Sum256([]byte("managed-cold-state-1")),
		PrefixHash:      prefix1,
		NextLeaderOrder: next1,
	}
	encodedSeal1, err := EncodeCheckpointSeal(seal1)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, encodedSeal1); err != nil || slot != 2 {
		t.Fatalf("propose seal1: slot=%d err=%v", slot, err)
	}

	for seq := 3; seq <= 12; seq++ {
		if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 128)); err != nil {
			t.Fatalf("propose slot %d: %v", seq, err)
		}
	}

	core.mu.RLock()
	evictedThrough := core.evictedThrough
	_, seal2Hot := core.decided[2]
	core.mu.RUnlock()
	if evictedThrough < 2 || seal2Hot {
		t.Fatalf("expected seal slot 2 to be evicted (evictedThrough=%d seal2Hot=%t)", evictedThrough, seal2Hot)
	}

	selected, found, err := core.LatestManagedCheckpointSeal()
	if err != nil || !found || selected.DecisionSlot != 2 || selected.RootHash != seal1.RootHash {
		t.Fatalf("LatestManagedCheckpointSeal: selected=%+v found=%t err=%v", selected, found, err)
	}
	cut, err := core.CaptureManagedReconstructionCut(selected, 0, [32]byte{})
	if err != nil {
		t.Fatalf("CaptureManagedReconstructionCut failed for evicted seal: %v", err)
	}
	tip, err := core.RecheckManagedReconstructionCut(ctx, cut)
	if err != nil || tip != 12 {
		t.Fatalf("RecheckManagedReconstructionCut tip=%d err=%v", tip, err)
	}
}

func TestCoreColdEvictionPrunesSupersededSealedRoots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, wal := managedAppendOnlyTestConfig(t, managedGraphRequestAppendOnlyApplicationProtocol, func(context.Context, ManagedCheckpointCandidate) error {
		return nil
	})
	config.MaxRetainedDecisionSlots = 4
	config.MaxRetainedDecisionBytes = 1 << 20
	core, err := New(config)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	proposeSealAtNext := func(cutIdx Slot, label string) CheckpointSeal {
		prefix, ok := core.PrefixHash(cutIdx)
		if !ok {
			t.Fatalf("missing prefix at cut %d", cutIdx)
		}
		order, err := core.LeaderOrder(cutIdx + 1)
		if err != nil {
			t.Fatal(err)
		}
		seal := CheckpointSeal{
			ConfigID:        1,
			Index:           cutIdx,
			RootHash:        sha256.Sum256([]byte(label + "-root")),
			StateHash:       sha256.Sum256([]byte(label + "-state")),
			PrefixHash:      prefix,
			NextLeaderOrder: order,
		}
		encoded, err := EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := core.Propose(ctx, encoded); err != nil {
			t.Fatalf("propose %s: %v", label, err)
		}
		return seal
	}

	if _, _, err := core.Propose(ctx, numberedTestPayload(1, 128)); err != nil {
		t.Fatal(err)
	}
	seal1 := proposeSealAtNext(1, "seal1") // slot 2
	if _, _, err := core.Propose(ctx, numberedTestPayload(3, 128)); err != nil {
		t.Fatal(err)
	}
	seal2 := proposeSealAtNext(3, "seal2") // slot 4

	for seq := 5; seq <= 12; seq++ {
		if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 128)); err != nil {
			t.Fatal(err)
		}
	}

	core.mu.RLock()
	evictedThrough := core.evictedThrough
	_, hasSeal1 := core.sealedRoots[seal1.RootHash]
	_, hasSeal2 := core.sealedRoots[seal2.RootHash]
	core.mu.RUnlock()

	if evictedThrough < 4 {
		t.Fatalf("expected both seal slots evicted, evictedThrough=%d", evictedThrough)
	}
	if hasSeal1 {
		t.Fatalf("superseded evicted seal1 root %x remained in sealedRoots", seal1.RootHash[:8])
	}
	if _, hasPrefix1 := core.prefixes[seal1.Index]; hasPrefix1 {
		t.Fatalf("superseded evicted seal1 retained orphaned prefix at slot %d", seal1.Index)
	}
	if !hasSeal2 {
		t.Fatalf("latest seal2 root %x missing from sealedRoots", seal2.RootHash[:8])
	}
	selected, found, err := core.LatestManagedCheckpointSeal()
	if err != nil || !found || selected.RootHash != seal2.RootHash || selected.DecisionSlot != 4 {
		t.Fatalf("LatestManagedCheckpointSeal got (%+v, %t, %v), want seal2", selected, found, err)
	}
}

func TestPruneDeadValuesPreservesRecentStagedAndPinned(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{NodeID: "n1", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	recentValue := []byte("recently-staged")
	recentHash := ValueHash(sha256.Sum256(recentValue))
	if err := core.StageValue(recentHash, recentValue); err != nil {
		t.Fatal(err)
	}
	pinnedValue := []byte("actively-recorded")
	pinnedHash := ValueHash(sha256.Sum256(pinnedValue))
	if err := core.StageValue(pinnedHash, pinnedValue); err != nil {
		t.Fatal(err)
	}
	core.pinValue(pinnedHash)
	defer core.unpinValue(pinnedHash)

	core.mu.Lock()
	for i := 0; i < 300; i++ {
		value := []byte{byte(i), byte(i >> 8)}
		core.values[ValueHash(sha256.Sum256(value))] = value
	}
	core.pruneDeadValuesLocked(map[ValueHash]struct{}{recentHash: {}, pinnedHash: {}})
	_, hasRecent := core.values[recentHash]
	_, hasPinned := core.values[pinnedHash]
	core.mu.Unlock()
	if !hasRecent || !hasPinned {
		t.Fatalf("prune removed in-flight values recent=%t pinned=%t", hasRecent, hasPinned)
	}
}

func TestPruneDeadValuesSuppressesFullSweepDuringRecovery(t *testing.T) {
	core := &Core{
		values:           map[ValueHash][]byte{},
		valueDurable:     map[ValueHash]bool{},
		recorders:        map[Slot]ISR{},
		decided:          map[Slot]DecidedValue{},
		recovering:       true,
		maxRetainedSlots: 8,
	}
	sentinel := []byte("scan-still-references")
	sentinelHash := ValueHash(sha256.Sum256(sentinel))
	core.values[sentinelHash] = sentinel
	for i := 0; i < 300; i++ {
		value := []byte{0x80, byte(i), byte(i >> 8)}
		core.values[ValueHash(sha256.Sum256(value))] = value
	}
	core.pruneDeadValuesLocked(nil)
	if _, ok := core.values[sentinelHash]; !ok {
		t.Fatal("mid-recovery prune performed a full values sweep")
	}
}

func TestColdEvictionIgnoresFutureSealWhenPruningSupersededRoots(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{
		NodeID: "n1", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}}, WAL: wal,
		MaxRetainedDecisionSlots: 100, MaxRetainedDecisionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 12; i++ {
		if _, _, err := core.Propose(context.Background(), numberedTestPayload(i, 128)); err != nil {
			t.Fatal(err)
		}
	}
	oldRoot := sha256.Sum256([]byte("old-active-seal"))
	futureRoot := sha256.Sum256([]byte("future-sparse-seal"))
	core.mu.Lock()
	core.sealedRoots[oldRoot] = SealedCheckpoint{CheckpointSeal: CheckpointSeal{Index: 2, RootHash: oldRoot}, DecisionSlot: 2}
	core.sealedRoots[futureRoot] = SealedCheckpoint{CheckpointSeal: CheckpointSeal{Index: 99, RootHash: futureRoot}, DecisionSlot: 100}
	core.durable[100] = true
	core.maxRetainedSlots = 4
	core.evictColdDecisionsLocked()
	_, hasOld := core.sealedRoots[oldRoot]
	core.mu.Unlock()
	if !hasOld {
		t.Fatal("future seal beyond tip pruned a durable active seal")
	}
}

func TestColdEvictionCachesLeaderScheduleForDiskFreeLookup(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	core, err := New(Config{
		NodeID: "n1", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}}, WAL: wal,
		MaxRetainedDecisionSlots: 2, MaxRetainedDecisionBytes: 1 << 20, EnableReconfiguration: true,
	})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := core.RecoverThrough(ctx, 49); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	core.mu.RLock()
	_, cached := core.coldLeaderSchedules[33]
	core.mu.RUnlock()
	if !cached {
		_ = wal.Close()
		t.Fatal("leader-schedule control slot was not cached during cold eviction")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	order, err := core.LeaderOrder(49)
	if err != nil || len(order) != 1 || order[0] != "n1" {
		t.Fatalf("LeaderOrder after WAL close order=%v err=%v", order, err)
	}
}

func TestInstallBasePrunesEpochStartsBehindBase(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{NodeID: "n1", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	core.epochStart[1] = core.now()
	core.epochStart[2] = core.now()
	core.epochStart[3] = core.now()
	core.installBaseLocked(consensusBase{ClosedThrough: 2, LeaderEpoch: 2, PrefixHash: [32]byte{1}})
	_, has1 := core.epochStart[1]
	_, has2 := core.epochStart[2]
	_, has3 := core.epochStart[3]
	core.mu.Unlock()
	if has1 || !has2 || !has3 {
		t.Fatalf("epoch starts after base installation: 1=%t 2=%t 3=%t", has1, has2, has3)
	}
}
