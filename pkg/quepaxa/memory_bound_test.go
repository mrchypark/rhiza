package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"strings"
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
	if !hasSeal2 {
		t.Fatalf("latest seal2 root %x missing from sealedRoots", seal2.RootHash[:8])
	}
	selected, found, err := core.LatestManagedCheckpointSeal()
	if err != nil || !found || selected.RootHash != seal2.RootHash || selected.DecisionSlot != 4 {
		t.Fatalf("LatestManagedCheckpointSeal got (%+v, %t, %v), want seal2", selected, found, err)
	}
}

func TestCoreStagedValuePinSurvivesColdPruneSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var core *Core
	var wal *qlog.WAL
	sweptDuringVerify := false

	config, wal := managedAppendOnlyTestConfig(t, managedGraphRequestAppendOnlyApplicationProtocol, func(ctx context.Context, candidate ManagedCheckpointCandidate) error {
		core.mu.Lock()
		// Populate stale values well above sweepThreshold (256) and run full sweep
		// while Core.record() has released Core.mu during candidate verification.
		for i := range 300 {
			h := ValueHash(sha256.Sum256([]byte{byte(i), byte(i >> 8), 0xaa}))
			core.values[h] = []byte("orphan")
		}
		core.pruneDeadValuesLocked(map[ValueHash]struct{}{})
		_, stillPresent := core.values[candidate.Hash]
		core.mu.Unlock()
		if !stillPresent {
			return context.Canceled
		}
		sweptDuringVerify = true
		return nil
	})
	var err error
	core, err = New(config)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	if _, _, err := core.Propose(ctx, numberedTestPayload(1, 128)); err != nil {
		t.Fatal(err)
	}
	prefix1, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing prefix at slot 1")
	}
	next1, err := core.LeaderOrder(2)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: 1, Index: 1,
		RootHash: sha256.Sum256([]byte("pin-root")), StateHash: sha256.Sum256([]byte("pin-state")),
		PrefixHash: prefix1, NextLeaderOrder: next1,
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	hash := ValueHash(sha256.Sum256(value))
	if _, err := core.Record(ctx, RecordRequest{
		Slot: 2, Step: 4, ConfigID: 1,
		Proposal: Proposal{Priority: highestPriority, ProposerID: "n1", Hash: hash, Value: value},
	}); err != nil {
		t.Fatalf("Record with concurrent prune sweep failed: %v", err)
	}
	if !sweptDuringVerify {
		t.Fatal("expected verifier sweep callback to run")
	}
}

func TestCoreColdEvictionCachesLeaderSchedulesUnderTightByteBudget(t *testing.T) {
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

	core, err := New(Config{
		NodeID:                   "n1",
		Cluster:                  Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}},
		WAL:                      wal,
		MaxRetainedDecisionSlots: 8,
		MaxRetainedDecisionBytes: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 1-node explorationEpochs() = 3 (48 slots). Slot 33 is control slot for epoch 3 (slots 49..64),
	// and slot 49 is control slot for epoch 4 (slots 65..80).
	for seq := 1; seq <= 65; seq++ {
		if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 256)); err != nil {
			t.Fatalf("propose %d: %v", seq, err)
		}
	}

	core.mu.RLock()
	_, ctrl33Hot := core.decided[33]
	_, ctrl33Cached := core.coldLeaderSchedules[33]
	_, ctrl49Cached := core.coldLeaderSchedules[49]
	core.mu.RUnlock()
	if ctrl33Hot {
		t.Fatal("expected leader schedule control slot 33 to be cold-evicted under tight byte budget")
	}
	if !ctrl33Cached || !ctrl49Cached {
		t.Fatalf("expected coldLeaderSchedules to cache evicted schedule slots: s33=%t s49=%t", ctrl33Cached, ctrl49Cached)
	}
	if order, err := core.LeaderOrder(core.Tip() + 1); err != nil || len(order) != 1 || order[0] != "n1" {
		t.Fatalf("LeaderOrder after cold eviction=%v err=%v", order, err)
	}
}

func TestCoreColdEvictionCleansSupersededSealPrefixAndIgnoresUntippedFutureSeal(t *testing.T) {
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
	seal1 := proposeSealAtNext(1, "seal1") // slot 2, non-stride index 1
	wantPrefix1, ok := core.PrefixHash(seal1.Index)
	if !ok {
		t.Fatal("missing prefix1")
	}

	if _, _, err := core.Propose(ctx, numberedTestPayload(3, 128)); err != nil {
		t.Fatal(err)
	}
	seal2 := proposeSealAtNext(3, "seal2") // slot 4

	// Plant a sparse untipped future durable seal entry (DecisionSlot > c.tip)
	// and verify it never counts as latestSeal to prune seal2 before c.tip.
	futureRoot := sha256.Sum256([]byte("future-seal-root"))
	core.mu.Lock()
	core.sealedRoots[futureRoot] = SealedCheckpoint{
		CheckpointSeal: CheckpointSeal{ConfigID: 1, Index: 500, RootHash: futureRoot},
		DecisionSlot:   501,
	}
	core.durable[501] = true
	core.mu.Unlock()

	for seq := 5; seq <= 12; seq++ {
		if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 128)); err != nil {
			t.Fatal(err)
		}
	}

	core.mu.RLock()
	_, hasSeal1 := core.sealedRoots[seal1.RootHash]
	_, hasSeal2 := core.sealedRoots[seal2.RootHash]
	_, hasSeal1Prefix := core.prefixes[seal1.Index]
	core.mu.RUnlock()

	if hasSeal1 {
		t.Fatal("expected seal1 to be pruned once seal2 superseded it")
	}
	if !hasSeal2 {
		t.Fatal("untipped future seal improperly pruned active latestSeal seal2")
	}
	if hasSeal1Prefix {
		t.Fatalf("expected pruned non-stride prefix at slot %d to be deleted from c.prefixes", seal1.Index)
	}
	if recomputed, ok := core.PrefixHash(seal1.Index); !ok || recomputed != wantPrefix1 {
		t.Fatalf("PrefixHash(%d) recompute failed: ok=%t match=%t", seal1.Index, ok, recomputed == wantPrefix1)
	}
}

func TestCoreRecoverBoundsHotDecisionsDuringWALScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("fixed-membership", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			NodeID:                   "n1",
			Cluster:                  Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}},
			WAL:                      wal,
			MaxRetainedDecisionSlots: 8,
			MaxRetainedDecisionBytes: 1 << 20,
		}
		core, err := New(cfg)
		if err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		for seq := 1; seq <= 100; seq++ {
			if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 256)); err != nil {
				_ = wal.Close()
				t.Fatalf("propose %d: %v", seq, err)
			}
		}
		wantTip := core.Tip()
		wantPrefix, ok := core.PrefixHash(wantTip)
		if !ok {
			_ = wal.Close()
			t.Fatal("missing tip prefix")
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}

		reopenedWAL, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reopenedWAL.Close() }()
		cfg.WAL = reopenedWAL
		reopened, err := New(cfg)
		if err != nil {
			t.Fatalf("reopen bounded WAL: %v", err)
		}
		reopened.mu.RLock()
		hotCount := len(reopened.decided)
		evictedThrough := reopened.evictedThrough
		reopened.mu.RUnlock()
		if hotCount > 8 || evictedThrough < 90 {
			t.Fatalf("expected bounded hot decisions during recover(): hot=%d evicted=%d", hotCount, evictedThrough)
		}
		if gotTip := reopened.Tip(); gotTip != wantTip {
			t.Fatalf("reopened tip=%d want=%d", gotTip, wantTip)
		}
		if gotPrefix, ok := reopened.PrefixHash(wantTip); !ok || gotPrefix != wantPrefix {
			t.Fatalf("reopened prefix ok=%t match=%t", ok, gotPrefix == wantPrefix)
		}
		if order, err := reopened.LeaderOrder(wantTip + 1); err != nil || len(order) != 1 || order[0] != "n1" {
			t.Fatalf("LeaderOrder after bounded reopen=%v err=%v", order, err)
		}
	})

	t.Run("reconfiguration-enabled", func(t *testing.T) {
		initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
		dir := t.TempDir()
		transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
		wals := make(map[NodeID]*qlog.WAL, len(initial.Members))
		for _, member := range initial.Members {
			wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
			if err != nil {
				t.Fatal(err)
			}
			core, err := New(Config{
				NodeID:                   member.ID,
				Cluster:                  initial,
				WAL:                      wal,
				Transport:                transport,
				EnableReconfiguration:    true,
				MaxRetainedDecisionSlots: 8,
				MaxRetainedDecisionBytes: 1 << 20,
			})
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			wals[member.ID] = wal
			transport.cores[member.ID] = core
		}
		target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
		if _, err := transport.cores["a"].BeginReconfiguration(ctx, target); err != nil {
			t.Fatal(err)
		}
		if err := transport.cores["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatal(err)
		}
		for seq := 1; seq <= 80; seq++ {
			if _, _, err := transport.cores["a"].Propose(ctx, numberedTestPayload(seq, 256)); err != nil {
				t.Fatalf("post-reconfig propose %d: %v", seq, err)
			}
		}
		wantTip := transport.cores["a"].Tip()
		wantPrefix, ok := transport.cores["a"].PrefixHash(wantTip)
		if !ok {
			t.Fatal("missing post-reconfig tip prefix")
		}
		for _, wal := range wals {
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
		}

		reopenedWAL, err := qlog.Open(filepath.Join(dir, "a"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reopenedWAL.Close() }()
		reopened, err := New(Config{
			NodeID:                   "a",
			Cluster:                  initial,
			WAL:                      reopenedWAL,
			Transport:                &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}},
			EnableReconfiguration:    true,
			MaxRetainedDecisionSlots: 8,
			MaxRetainedDecisionBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("reopen reconfig WAL: %v", err)
		}
		reopened.mu.RLock()
		hotCount := len(reopened.decided)
		evictedThrough := reopened.evictedThrough
		reopened.mu.RUnlock()
		if hotCount > 8 || evictedThrough < 80 {
			t.Fatalf("expected bounded hot decisions in reconfig recover(): hot=%d evicted=%d", hotCount, evictedThrough)
		}
		if reopened.Tip() != wantTip || reopened.ConfigID() != 2 {
			t.Fatalf("reopened tip=%d config=%d, want %d / 2", reopened.Tip(), reopened.ConfigID(), wantTip)
		}
		if gotPrefix, ok := reopened.PrefixHash(wantTip); !ok || gotPrefix != wantPrefix {
			t.Fatalf("reopened prefix ok=%t match=%t", ok, gotPrefix == wantPrefix)
		}
		history, err := reopened.MembershipHistory()
		if err != nil || len(history.Transitions) != 1 {
			t.Fatalf("MembershipHistory after bounded reopen=%+v err=%v", history, err)
		}
		if _, err := validateMembershipHistory(initial, history); err != nil {
			t.Fatalf("validate reopened history: %v", err)
		}
	})
}

func TestCoreCompactThroughWithTransitionInRetainedSuffixAndEpochStartPrune(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	dir := t.TempDir()
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	for _, member := range initial.Members {
		wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := New(Config{
			NodeID:                member.ID,
			Cluster:               initial,
			WAL:                   wal,
			Transport:             transport,
			EnableReconfiguration: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		transport.cores[member.ID] = core
	}
	core := transport.cores["a"]
	// Propose slots spanning past one leaderEpochSize before creating checkpoint seal at cut=18.
	for seq := 1; seq <= 18; seq++ {
		if _, _, err := core.Propose(ctx, numberedTestPayload(seq, 64)); err != nil {
			t.Fatal(err)
		}
	}
	cut := core.Tip()
	prefix, ok := core.PrefixHash(cut)
	if !ok {
		t.Fatal("missing prefix at cut")
	}
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: 1, Index: cut,
		RootHash: sha256.Sum256([]byte("pre-transition-root")), StateHash: sha256.Sum256([]byte("pre-transition-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership,
	}
	for _, member := range initial.Members {
		voter := transport.cores[member.ID]
		if err := voter.WaitTip(ctx, cut); err != nil {
			t.Fatal(err)
		}
		if err := voter.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encodedSeal); err != nil {
		t.Fatal(err)
	}

	// Execute a full certified membership transition AFTER the checkpoint cut.
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("post-transition-write")); err != nil {
		t.Fatal(err)
	}

	core.mu.Lock()
	core.epochStart[0] = core.now()
	core.mu.Unlock()
	if err := core.CompactThrough(cut, seal.RootHash); err != nil {
		t.Fatalf("CompactThrough with transition in retained suffix failed: %v", err)
	}
	core.mu.RLock()
	_, hasEpochZero := core.epochStart[0]
	core.mu.RUnlock()
	if hasEpochZero {
		t.Fatal("installBaseLocked did not prune epochStart entries below base.LeaderEpoch")
	}
}

func TestManagedAppendOnlyFailsClosedOnCorruptedCheckpointHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	dir := t.TempDir()
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	identity := bytes.Repeat([]byte{0x42}, 32)
	for _, member := range initial.Members {
		wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		core, err := New(Config{
			NodeID:                member.ID,
			Cluster:               initial,
			WAL:                   wal,
			Transport:             transport,
			EnableReconfiguration: true,
			ManagedAppendOnly: &ManagedAppendOnlyConfig{
				Binding: ManagedAppendOnlyBinding{
					ApplicationProtocol: managedGraphRequestAppendOnlyApplicationProtocol,
					BootstrapPlanDigest: strings.Repeat("11", 32),
					MemberConfigDigest:  strings.Repeat("22", 32),
					Generation:          0,
					WALIdentity:         [32]byte(identity),
				},
				Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil },
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	core := transport.cores["a"]
	freezeSlot, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("after-reconfig")); err != nil {
		t.Fatal(err)
	}
	cut := core.Tip()
	prefix, _ := core.PrefixHash(cut)
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: core.ConfigID(), Index: cut,
		RootHash: sha256.Sum256([]byte("managed-fail-closed-root")), StateHash: sha256.Sum256([]byte("managed-fail-closed-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership,
	}
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, encodedSeal)
	if err != nil {
		t.Fatal(err)
	}
	candidate := ManagedCheckpointCandidate{
		Value: encodedSeal, Hash: sha256.Sum256(encodedSeal), Seal: seal, DecisionSlot: slot, ConfigID: core.ConfigID(),
	}

	// Mark the freeze decision undurable so checkpointMembershipAtLocked returns an error.
	core.mu.Lock()
	core.durable[freezeSlot] = false
	core.mu.Unlock()

	if err := core.ValidateManagedCheckpointRoot(ctx, candidate); err == nil || !strings.Contains(err.Error(), "not durable") {
		t.Fatalf("ValidateManagedCheckpointRoot swallowed membership history error: %v", err)
	}
	if err := core.validateManagedCandidateLocal(candidate); err == nil || !strings.Contains(err.Error(), "not durable") {
		t.Fatalf("validateManagedCandidateLocal swallowed membership history error: %v", err)
	}
}
