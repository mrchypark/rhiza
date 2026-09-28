package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestLocalWALCostsAndPureValueValidation(t *testing.T) {
	const nodeID NodeID = "local\"id\n\\"
	core, wal := localReserveCore(t, t.TempDir(), nodeID)
	defer wal.Close()
	initialBytes := wal.Bytes()
	value := bytes.Repeat([]byte{0xa5}, MaxReplicatedValueBytes)
	if err := core.ValidateLocalValueCost(value); err != nil {
		t.Fatalf("max value validation: %v", err)
	}
	if !bytes.Equal(value, bytes.Repeat([]byte{0xa5}, MaxReplicatedValueBytes)) {
		t.Fatal("validation changed supplied value")
	}
	if err := core.ValidateLocalValueCost(append(value, 1)); err == nil {
		t.Fatal("value over cap passed cost validation")
	}
	if _, err := core.LocalWALCosts(-1); err == nil {
		t.Fatal("negative value length passed cost calculation")
	}
	if wal.Bytes() != initialBytes {
		t.Fatal("pure validation changed WAL")
	}

	model := core.localCost
	if model.nodeID != nodeID || model.configID != core.ConfigID() {
		t.Fatalf("Local cost identity node=%q config=%d, Core node=%q config=%d", model.nodeID, model.configID, nodeID, core.ConfigID())
	}
	for _, length := range []int{0, 1, 2, 3, MaxReplicatedValueBytes - 2, MaxReplicatedValueBytes - 1, MaxReplicatedValueBytes} {
		got, err := core.LocalWALCosts(length)
		if err != nil {
			t.Fatalf("cost for value length %d: %v", length, err)
		}
		v := uint64(length)
		wantProposal := uint64(53) + v
		wantDecision := uint64(85) + 4*((v+2)/3) + model.certificateBytes
		if v == 0 {
			wantDecision += 2 // The length-only estimate also covers nil []byte => null.
		}
		wantSlot := wantProposal + 3*model.receiptEntryBytes + wantDecision
		if got.ProposalEntryBytes != wantProposal || got.DecisionEntryBytes != wantDecision || got.SlotBytes != wantSlot {
			t.Fatalf("cost(%d)=%+v, want P=%d D=%d B=%d", length, got, wantProposal, wantDecision, wantSlot)
		}
		if got.ForegroundBytes != wantSlot+model.maxSlotBytes || got.RecoveryBytes != model.maxSlotBytes || got.CheckpointBytes != uint64(53+MaxReplicatedValueBytes)+2*model.maxSlotBytes {
			t.Fatalf("cost(%d) aggregate bounds=%+v", length, got)
		}
	}
	if model.leaderScheduleBytes == 0 || model.leaderScheduleBytes > localValueLimit || model.checkpointSealBytes == 0 || model.checkpointSealBytes > localValueLimit {
		t.Fatalf("invalid configured schedule/seal bounds: schedule=%d seal=%d", model.leaderScheduleBytes, model.checkpointSealBytes)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := core.LocalWALCosts(128); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("hot cost calculation allocations=%.1f, want 0", allocs)
	}

	normalWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer normalWAL.Close()
	normal, err := New(Config{NodeID: "ordinary", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "ordinary"}}}, WAL: normalWAL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normal.LocalWALCosts(1); !errors.Is(err, errLocalCostsUnavailable) {
		t.Fatalf("non-Local cost query error=%v", err)
	}
}

func TestLocalCostRejectsInvalidOrImpossibleIDsBeforeWALMutation(t *testing.T) {
	for _, nodeID := range []NodeID{
		NodeID(string([]byte{0xff, 'x'})),
		NodeID(strings.Repeat("x", MaxReplicatedValueBytes/2+1)),
		NodeID(strings.Repeat("<", MaxReplicatedValueBytes/6+1)),
	} {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		core, err := New(Config{NodeID: nodeID, Cluster: Cluster{ConfigID: 7, Members: []Member{{ID: nodeID}}}, WAL: wal, LocalMode: true})
		if err == nil {
			_ = wal.Close()
			t.Fatalf("New accepted invalid/impossible Local ID %q", nodeID)
		}
		if wal.Bytes() != 0 {
			t.Fatalf("invalid Local ID %q mutated WAL: %d bytes", nodeID, wal.Bytes())
		}
		if core != nil {
			t.Fatalf("New returned Core with invalid Local ID %q", nodeID)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLocalUndurableDecisionBytesMatchesAppendSerializer(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	certOne := []byte(" { \"unknown\" : \"<keep & exact>\" } \n")
	certTwo := append([]byte(`{"unknown":"`), bytes.Repeat([]byte("x"), 1500)...)
	certTwo = append(certTwo, []byte(`"}`)...)
	valueOne := []byte("one")
	valueTwo := []byte("two-two")
	core.mu.Lock()
	core.decided[1] = DecidedValue{Slot: 1, Hash: sha256.Sum256(valueOne), Value: valueOne, Certificate: certOne}
	core.decided[2] = DecidedValue{Slot: 2, Hash: sha256.Sum256(valueTwo), Value: valueTwo, Certificate: certTwo}
	core.mu.Unlock()
	before := wal.Bytes()
	wantOne := uint64(len(mustDecisionEntry(t, core.decided[1]).Encode()))
	wantTwo := uint64(len(mustDecisionEntry(t, core.decided[2]).Encode()))
	staticDecisionCost, err := core.LocalWALCosts(len(valueTwo))
	if err != nil {
		t.Fatal(err)
	}
	if wantTwo <= staticDecisionCost.DecisionEntryBytes {
		t.Fatalf("oversized original certificate entry=%d did not exceed structural per-instance bound=%d", wantTwo, staticDecisionCost.DecisionEntryBytes)
	}
	got, err := core.LocalUndurableDecisionBytes(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if got != wantOne+wantTwo {
		t.Fatalf("recovery cost=%d, want exact appended QDEC sum %d", got, wantOne+wantTwo)
	}
	if wal.Bytes() != before {
		t.Fatal("read-only recovery cost changed WAL")
	}
	entryOne := mustDecisionEntry(t, core.decided[1])
	wantRecordOne, err := encodeDecisionRecord(valueOne, certOne)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(entryOne.Payload[len(decisionEntryMagic):], wantRecordOne) {
		t.Fatal("recovery estimate did not use the existing append serializer")
	}
	if !bytes.Equal(core.decided[1].Certificate, certOne) {
		t.Fatal("read-only recovery estimate mutated the retained certificate")
	}

	core.mu.Lock()
	core.logged[1] = true
	core.durable[1] = false
	core.mu.Unlock()
	got, err = core.LocalUndurableDecisionBytes(context.Background(), 2)
	if err != nil || got != wantTwo {
		t.Fatalf("logged-but-undurable decision cost=%d error=%v, want only slot 2 (%d)", got, err, wantTwo)
	}
	core.mu.Lock()
	core.durable[1] = true
	core.mu.Unlock()
	got, err = core.LocalUndurableDecisionBytes(context.Background(), 2)
	if err != nil || got != wantTwo {
		t.Fatalf("logged-and-durable decision cost=%d error=%v, want only slot 2 (%d)", got, err, wantTwo)
	}
	core.mu.Lock()
	core.floor = 1
	core.mu.Unlock()
	if got, err = core.LocalUndurableDecisionBytes(context.Background(), 1); err != nil || got != 0 {
		t.Fatalf("floor-covered recovery cost=%d error=%v, want zero", got, err)
	}
	if wal.Bytes() != before {
		t.Fatal("read-only recovery cost changed WAL in logged/floor cases")
	}
}

func TestLocalUndurableDecisionBytesFailsClosedWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		fill func(*Core)
	}{
		{name: "gap", fill: func(core *Core) {
			core.decided[1] = DecidedValue{Slot: 1, Certificate: []byte(`{}`)}
			core.decided[3] = DecidedValue{Slot: 3, Certificate: []byte(`{}`)}
		}},
		{name: "invalid certificate", fill: func(core *Core) {
			core.decided[1] = DecidedValue{Slot: 1, Certificate: []byte(`{`)}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, wal := localReserveCore(t, t.TempDir(), "local")
			defer wal.Close()
			core.mu.Lock()
			test.fill(core)
			core.mu.Unlock()
			before := wal.Bytes()
			if _, err := core.LocalUndurableDecisionBytes(context.Background(), 3); err == nil {
				t.Fatal("invalid/sparse target unexpectedly produced a recovery cost")
			}
			if wal.Bytes() != before {
				t.Fatal("failed cost calculation mutated WAL")
			}
		})
	}
}

func TestRecoveryBytesIsSingleSlotBoundNotSuffixBound(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := bytes.Repeat([]byte{0x5a}, MaxReplicatedValueBytes)
	var expected uint64
	core.mu.Lock()
	for slot := Slot(1); slot <= 3; slot++ {
		decision := DecidedValue{Slot: slot, Hash: sha256.Sum256(value), Value: value, Certificate: []byte(`{}`)}
		core.decided[slot] = decision
		entry := mustDecisionEntry(t, decision)
		expected += uint64(len(entry.Encode()))
	}
	core.mu.Unlock()
	got, err := core.LocalUndurableDecisionBytes(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	cost, err := core.LocalWALCosts(MaxReplicatedValueBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got != expected || got <= cost.RecoveryBytes {
		t.Fatalf("multi-slot exact suffix=%d want=%d, single-slot RecoveryBytes=%d; API must not imply suffix bound", got, expected, cost.RecoveryBytes)
	}
}

func TestLocalProposalAndCheckpointActualBytesStayWithinModel(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local\"id\n\\")
	defer wal.Close()
	value := []byte("small local proposal")
	before := wal.Bytes()
	slot, _, err := core.Propose(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	afterPropose := wal.Bytes()
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	if actual := uint64(afterPropose - before); actual > cost.SlotBytes {
		t.Fatalf("actual Local proposal bytes=%d exceed B(v)=%d", actual, cost.SlotBytes)
	}
	if core.logged[slot] == false {
		t.Fatal("ordinary Local Propose did not durably log its decision")
	}

	seal := testLocalSeal(t, core, slot)
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	beforePrepare := wal.Bytes()
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	prepareDelta := uint64(wal.Bytes() - beforePrepare)
	expectedCheckpointEntry := uint64(len((qlog.Entry{Slot: uint64(slot), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: encodedSeal}).Encode()))
	if prepareDelta != expectedCheckpointEntry {
		t.Fatalf("clean checkpoint delta=%d, want exact checkpoint entry %d", prepareDelta, expectedCheckpointEntry)
	}
	if prepareDelta > cost.CheckpointBytes {
		t.Fatalf("actual checkpoint bytes=%d exceed conservative checkpoint cost=%d", prepareDelta, cost.CheckpointBytes)
	}
	beforeReuse := wal.Bytes()
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	if wal.Bytes() != beforeReuse {
		t.Fatal("prepared checkpoint reuse appended bytes")
	}
}

func TestLocalDirtyCheckpointCostIncludesExactUndurablePrefix(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := []byte("certified but not marker-logged")
	slot, _, err := core.ProposeCertified(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	logged := core.logged[slot]
	core.mu.RUnlock()
	if logged {
		t.Fatal("ProposeCertified unexpectedly marker-logged the decision")
	}
	h, err := core.LocalUndurableDecisionBytes(context.Background(), slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, slot)
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	before := wal.Bytes()
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	expectedDelta := h + uint64(len((qlog.Entry{Slot: uint64(slot), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: encodedSeal}).Encode()))
	actualDelta := uint64(wal.Bytes() - before)
	if actualDelta != expectedDelta {
		t.Fatalf("dirty checkpoint WAL delta=%d, want exact H+checkpoint=%d (H=%d)", actualDelta, expectedDelta, h)
	}
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	if actualDelta > h+cost.CheckpointBytes {
		t.Fatalf("actual checkpoint delta=%d exceeds H+checkpoint bound=%d", actualDelta, h+cost.CheckpointBytes)
	}
}

func TestLocalRecoveryAppendDeltaMatchesExactReadOnlyCost(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := []byte("local recovery value")
	slot, _, err := core.ProposeCertified(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := core.LocalUndurableDecisionBytes(context.Background(), slot)
	if err != nil {
		t.Fatal(err)
	}
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	if expected > cost.RecoveryBytes {
		t.Fatalf("exact recovery bytes=%d exceed B(V)=%d", expected, cost.RecoveryBytes)
	}
	before := wal.Bytes()
	if err := core.EnsureDurableThrough(context.Background(), slot); err != nil {
		t.Fatal(err)
	}
	if actual := uint64(wal.Bytes() - before); actual != expected {
		t.Fatalf("recovery append delta=%d, exact read-only H=%d", actual, expected)
	}
}

func TestLocalPendingRecordRecoveryActualBytesFitSlotCost(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := []byte("pending local record")
	proposal := newProposal(highestPriority, core.nodeID, value)
	if _, err := localRecordForTest(t, core, RecordRequest{
		Slot: 1, Step: 4, ConfigID: core.ConfigID(), Proposal: proposal,
	}); err != nil {
		t.Fatal(err)
	}
	cost, err := core.LocalWALCosts(MaxReplicatedValueBytes)
	if err != nil {
		t.Fatal(err)
	}
	before := wal.Bytes()
	if err := core.RecoverThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	actual := uint64(wal.Bytes() - before)
	if actual > cost.RecoveryBytes {
		t.Fatalf("actual pending-record recovery bytes=%d exceed B(V)=%d", actual, cost.RecoveryBytes)
	}
	if core.Tip() != 1 {
		t.Fatalf("recovery tip=%d, want 1", core.Tip())
	}
}

func TestLocalScheduledProposeActualBytesFitForegroundCost(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	for i := 0; i < 32; i++ {
		if _, _, err := core.Propose(context.Background(), []byte("advance")); err != nil {
			t.Fatalf("advance proposal %d: %v", i, err)
		}
	}
	value := []byte("proposal following schedule")
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	before := wal.Bytes()
	if _, _, err := core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	actual := uint64(wal.Bytes() - before)
	if actual > cost.ForegroundBytes {
		t.Fatalf("scheduled foreground WAL delta=%d exceeds B(v)+B(V)=%d", actual, cost.ForegroundBytes)
	}
	var foundSchedule bool
	if err := wal.Scan(func(entry qlog.Entry) error {
		if entry.Type != qlog.EntryProposal {
			return nil
		}
		order, schedule, err := DecodeLeaderSchedule(entry.Payload)
		if err != nil {
			return err
		}
		if schedule {
			foundSchedule = true
			if len(order) != 1 || order[0] != core.nodeID || uint64(len(entry.Payload)) != core.localCost.leaderScheduleBytes {
				t.Errorf("scheduled value order=%v bytes=%d model=%d", order, len(entry.Payload), core.localCost.leaderScheduleBytes)
			}
			if got, want := uint64(len(entry.Encode())), core.localCost.entryHeaderBytes+core.localCost.leaderScheduleBytes; got != want {
				t.Errorf("scheduled proposal entry bytes=%d, want exact E(schedule)=%d", got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !foundSchedule {
		t.Fatal("scheduled proposal path did not append a leader schedule")
	}
}

func TestLocalPendingPrefixThenScheduleAndOfferedValueCosts(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := []byte("pending record before epoch schedule")
	proposal := newProposal(highestPriority, core.nodeID, value)
	for slot := Slot(1); slot <= 32; slot++ {
		if _, err := localRecordForTest(t, core, RecordRequest{
			Slot: slot, Step: 4, ConfigID: core.ConfigID(), Proposal: proposal,
		}); err != nil {
			t.Fatalf("seed pending slot %d: %v", slot, err)
		}
	}
	if got := core.RecorderTip(); got != 32 {
		t.Fatalf("pending recorder tip=%d, want 32", got)
	}
	beforeEntries := len(localReserveEntries(t, wal))
	before := wal.Bytes()
	value = []byte("offered at the schedule boundary")
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	returnedSlot, _, err := core.Propose(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if returnedSlot != 34 {
		t.Fatalf("Propose returned slot %d, want offered value after schedule at 33", returnedSlot)
	}
	if got := core.Tip(); got != 34 {
		t.Fatalf("tip after pending recovery and schedule=%d, want 34", got)
	}
	actual := uint64(wal.Bytes() - before)
	var recoveryDelta, foregroundDelta uint64
	var recovered uint64
	var scheduleSeen, offeredSeen, scheduleDecisionSeen, offeredDecisionSeen bool
	entries := localReserveEntries(t, wal)
	for i, entry := range entries {
		if i < beforeEntries {
			continue
		}
		if entry.Type == qlog.EntryDecide && entry.Slot >= 1 && entry.Slot <= 32 {
			recoveryDelta += uint64(len(entry.Encode()))
			recovered++
		} else {
			foregroundDelta += uint64(len(entry.Encode()))
		}
		if entry.Type == qlog.EntryDecide && entry.Slot == 33 {
			scheduleDecisionSeen = true
		}
		if entry.Type == qlog.EntryDecide && entry.Slot == 34 {
			offeredDecisionSeen = true
		}
		if entry.Type == qlog.EntryProposal {
			order, isSchedule, err := DecodeLeaderSchedule(entry.Payload)
			if err == nil && isSchedule && len(order) == 1 && order[0] == core.nodeID {
				scheduleSeen = true
			}
			if bytes.Equal(entry.Payload, value) {
				offeredSeen = true
			}
		}
	}
	if recovered != 32 || !scheduleSeen || !offeredSeen || !scheduleDecisionSeen || !offeredDecisionSeen {
		t.Fatalf("trace: recovered pending slots=%d schedule33=%v/decision33=%v offered34=%v/decision34=%v", recovered, scheduleSeen, scheduleDecisionSeen, offeredSeen, offeredDecisionSeen)
	}
	if actual != recoveryDelta+foregroundDelta {
		t.Fatalf("WAL delta=%d does not equal recovery=%d + foreground=%d", actual, recoveryDelta, foregroundDelta)
	}
	if foregroundDelta > cost.ForegroundBytes {
		t.Fatalf("normalized prefix/schedule + offered slot bytes=%d exceed ForegroundBytes=%d", foregroundDelta, cost.ForegroundBytes)
	}
	t.Logf("illustrative trace only: recovered slots 1..32 (%d bytes), scheduled slot 33 and offered value slot 34 (%d bytes); total WAL delta %d; RecoveryBytes=%d is a one-slot bound and is not asserted against this multi-slot prefix", recoveryDelta, foregroundDelta, actual, cost.RecoveryBytes)
}

func TestLocalTip31MaxPending32ThenSchedule33AndOffered34(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	for slot := 1; slot <= 31; slot++ {
		if decided, _, err := core.Propose(context.Background(), []byte("clean prefix")); err != nil || decided != Slot(slot) {
			t.Fatalf("clean prefix proposal %d returned slot=%d err=%v", slot, decided, err)
		}
	}
	if got := core.Tip(); got != 31 {
		t.Fatalf("clean tip=%d, want 31", got)
	}
	pendingValue := bytes.Repeat([]byte{0x6b}, MaxReplicatedValueBytes)
	pendingProposal := newProposal(highestPriority, core.nodeID, pendingValue)
	if _, err := localRecordForTest(t, core, RecordRequest{
		Slot: 32, Step: 4, ConfigID: core.ConfigID(), Proposal: pendingProposal,
	}); err != nil {
		t.Fatalf("seed sole max-value pending slot 32: %v", err)
	}
	if got := core.RecorderTip(); got != 32 {
		t.Fatalf("recorder tip=%d, want only pending slot 32", got)
	}
	beforeEntries := len(localReserveEntries(t, wal))
	before := wal.Bytes()
	offeredValue := []byte("offered after slot-33 schedule")
	cost, err := core.LocalWALCosts(len(offeredValue))
	if err != nil {
		t.Fatal(err)
	}
	returnedSlot, _, err := core.Propose(context.Background(), offeredValue)
	if err != nil {
		t.Fatal(err)
	}
	if returnedSlot != 34 || core.Tip() != 34 {
		t.Fatalf("Propose returned slot=%d tip=%d; want offered value at 34 after schedule at 33", returnedSlot, core.Tip())
	}
	actual := uint64(wal.Bytes() - before)
	var recoveryDelta, foregroundDelta uint64
	var recovered32, scheduleSeen, scheduleDecisionSeen, offeredSeen, offeredDecisionSeen bool
	entries := localReserveEntries(t, wal)
	for i, entry := range entries {
		if i < beforeEntries {
			continue
		}
		if entry.Type == qlog.EntryDecide && entry.Slot == 32 {
			recoveryDelta += uint64(len(entry.Encode()))
			recovered32 = true
		} else {
			foregroundDelta += uint64(len(entry.Encode()))
		}
		if entry.Type == qlog.EntryDecide && entry.Slot == 33 {
			scheduleDecisionSeen = true
		}
		if entry.Type == qlog.EntryDecide && entry.Slot == 34 {
			offeredDecisionSeen = true
		}
		if entry.Type == qlog.EntryProposal {
			order, isSchedule, err := DecodeLeaderSchedule(entry.Payload)
			if err == nil && isSchedule && len(order) == 1 && order[0] == core.nodeID {
				scheduleSeen = true
			}
			if bytes.Equal(entry.Payload, offeredValue) {
				offeredSeen = true
			}
		}
	}
	if !recovered32 || !scheduleSeen || !scheduleDecisionSeen || !offeredSeen || !offeredDecisionSeen {
		t.Fatalf("trace: recovered32=%v schedule33=%v/%v offered34=%v/%v", recovered32, scheduleSeen, scheduleDecisionSeen, offeredSeen, offeredDecisionSeen)
	}
	if actual != recoveryDelta+foregroundDelta {
		t.Fatalf("WAL delta=%d does not equal recovery=%d + foreground=%d", actual, recoveryDelta, foregroundDelta)
	}
	if recoveryDelta > cost.RecoveryBytes {
		t.Fatalf("sole max-value pending slot recovery=%d exceeds one-slot RecoveryBytes=%d", recoveryDelta, cost.RecoveryBytes)
	}
	if foregroundDelta > cost.ForegroundBytes {
		t.Fatalf("schedule33 + offered slot34 bytes=%d exceed ForegroundBytes=%d", foregroundDelta, cost.ForegroundBytes)
	}
	if actual > cost.ForegroundBytes+cost.RecoveryBytes {
		t.Fatalf("WAL delta=%d exceeds ForegroundBytes+RecoveryBytes=%d+%d", actual, cost.ForegroundBytes, cost.RecoveryBytes)
	}
	t.Logf("trace: clean tip31; recovered only max-value pending slot32 (%d bytes); schedule33 + offered value34 (%d bytes); total %d <= foreground %d + recovery %d", recoveryDelta, foregroundDelta, actual, cost.ForegroundBytes, cost.RecoveryBytes)
}

func TestLocalCheckpointSyncFailureDeltaFitsHPlusCheckpointBound(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	value := []byte("prefix before checkpoint")
	slot, _, err := core.ProposeCertified(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	h, err := core.LocalUndurableDecisionBytes(context.Background(), slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, slot)
	sealBytes, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	core.commits = newGroupCommit(func() error { return errors.New("injected sync failure") })
	before := wal.Bytes()
	if err := core.PrepareCheckpoint(context.Background(), seal); err == nil {
		t.Fatal("PrepareCheckpoint succeeded despite injected sync failure")
	}
	actual := uint64(wal.Bytes() - before)
	checkpointEntry := uint64(len((qlog.Entry{Slot: uint64(slot), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: sealBytes}).Encode()))
	cost, err := core.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	if actual != h+checkpointEntry {
		t.Fatalf("sync-failed checkpoint WAL delta=%d, want H+checkpoint entry=%d", actual, h+checkpointEntry)
	}
	if actual > h+cost.CheckpointBytes {
		t.Fatalf("sync-failed checkpoint WAL delta=%d exceeds H+checkpoint bound=%d", actual, h+cost.CheckpointBytes)
	}
}

func testLocalSeal(t *testing.T, core *Core, slot Slot) CheckpointSeal {
	t.Helper()
	core.mu.RLock()
	prefix := core.prefixes[slot]
	core.mu.RUnlock()
	next, following, err := core.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	return CheckpointSeal{
		ConfigID: core.ConfigID(), Index: slot,
		RootHash: sha256.Sum256([]byte("local root")), StateHash: sha256.Sum256([]byte("local state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
}

func mustDecisionEntry(t *testing.T, value DecidedValue) qlog.Entry {
	t.Helper()
	entry, err := decisionEntry(value)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestDecisionEntryMatchesExistingAppendSerializer(t *testing.T) {
	value := []byte{1, 2, 3, 4}
	cert := []byte(" { \"k\" : \"<&>\" } ")
	decision := DecidedValue{Slot: 9, Hash: sha256.Sum256(value), Value: value, Certificate: cert}
	entry, err := decisionEntry(decision)
	if err != nil {
		t.Fatal(err)
	}
	if got := uint64(len(entry.Encode())); got != mustDecisionEntryLength(t, decision) {
		t.Fatalf("encoded QDEC entry bytes=%d, calculated=%d", got, mustDecisionEntryLength(t, decision))
	}
	wantRecord, err := json.Marshal(decisionRecord{Value: value, Certificate: cert})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(entry.Payload, append(append([]byte(nil), decisionEntryMagic...), wantRecord...)) {
		t.Fatal("QDEC payload differs from existing decision-record JSON encoding")
	}
	if !bytes.Equal(cert, []byte(" { \"k\" : \"<&>\" } ")) {
		t.Fatal("decision serializer mutated caller certificate")
	}
	if _, _, err := decodeDecisionRecord(entry.Payload[len(decisionEntryMagic):]); err != nil {
		t.Fatalf("preserved decision record is not decodable: %v", err)
	}
}

func TestZeroLengthValueCostCoversNilAndEmptySerialization(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	cost, err := core.LocalWALCosts(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{nil, {}} {
		decision := DecidedValue{Slot: 1, Value: value, Certificate: []byte(`{}`)}
		entry := mustDecisionEntry(t, decision)
		if actual := uint64(len(entry.Encode())); actual > cost.DecisionEntryBytes {
			t.Fatalf("zero-length value entry bytes=%d exceed DecisionEntryBytes=%d (nil=%v)", actual, cost.DecisionEntryBytes, value == nil)
		}
	}
	nilRecord, err := encodeDecisionRecord(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	emptyRecord, err := encodeDecisionRecord([]byte{}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(nilRecord) != len(emptyRecord)+2 {
		t.Fatalf("nil/empty JSON sizes=%d/%d, want null to require 2 extra bytes", len(nilRecord), len(emptyRecord))
	}
}

func mustDecisionEntryLength(t *testing.T, value DecidedValue) uint64 {
	t.Helper()
	return uint64(len(mustDecisionEntry(t, value).Encode()))
}

func TestLocalCostCheckedArithmetic(t *testing.T) {
	if _, err := checkedAdd(math.MaxUint64, 1); err == nil {
		t.Fatal("checkedAdd accepted uint64 overflow")
	}
	if _, err := checkedMul(math.MaxUint64, 2); err == nil {
		t.Fatal("checkedMul accepted uint64 overflow")
	}
	if _, err := decisionEntry(DecidedValue{Certificate: []byte("not-json")}); err == nil {
		t.Fatal("decision serializer accepted invalid certificate JSON")
	}
}
