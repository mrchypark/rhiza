package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func localReserveCore(t *testing.T, dir string, nodeID NodeID) (*Core, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core, err := New(Config{
		NodeID:    nodeID,
		Cluster:   Cluster{ConfigID: 1, Members: []Member{{ID: nodeID}}},
		WAL:       wal,
		LocalMode: true,
	})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	return core, wal
}

func localReserveEntries(t *testing.T, wal *qlog.WAL) []qlog.Entry {
	t.Helper()
	var entries []qlog.Entry
	if err := wal.Scan(func(entry qlog.Entry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return entries
}

func localReserveBytes(entries []qlog.Entry) int64 {
	var total int64
	for _, entry := range entries {
		total += int64(len(entry.Encode()))
	}
	return total
}

func localReserveReceiptCount(entries []qlog.Entry) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == qlog.EntryReceipt && entry.Slot != 0 {
			count++
		}
	}
	return count
}

func localPriority(value byte) Priority {
	var priority Priority
	priority[0] = value
	return priority
}

func TestLocalReserveRecorderBounds(t *testing.T) {
	t.Run("fresh fast path uses one receipt", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local\"id\n\\")
		defer wal.Close()
		if _, _, err := core.Propose(context.Background(), []byte("fresh")); err != nil {
			t.Fatal(err)
		}
		entries := localReserveEntries(t, wal)
		if got := localReserveReceiptCount(entries); got != 1 {
			t.Fatalf("receipt entries=%d, want 1", got)
		}
		if got := wal.Bytes(); got != localReserveBytes(entries) {
			t.Fatalf("WAL bytes=%d, sum of encoded entries=%d", got, localReserveBytes(entries))
		}
	})

	t.Run("empty recovery uses three receipts", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local\"id\n\\")
		defer wal.Close()
		if err := core.RecoverThrough(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		entries := localReserveEntries(t, wal)
		if got := localReserveReceiptCount(entries); got != 3 {
			t.Fatalf("receipt entries=%d, want 3", got)
		}
		if got := wal.Bytes(); got != localReserveBytes(entries) {
			t.Fatalf("WAL bytes=%d, sum of encoded entries=%d", got, localReserveBytes(entries))
		}
	})

	t.Run("reachable partial phases recover with bounded receipt counts", func(t *testing.T) {
		const nodeID NodeID = "local\"id\n\\"
		for _, through := range []Step{4, 5, 6} {
			t.Run(fmt.Sprintf("step-%d", through), func(t *testing.T) {
				dir := t.TempDir()
				core, wal := localReserveCore(t, dir, nodeID)
				proposal := newProposal(highestPriority, nodeID, []byte("partial"))
				for step := Step(4); step <= through; step++ {
					if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: step, Proposal: proposal}); err != nil {
						t.Fatalf("record partial phase %d: %v", step, err)
					}
				}
				if got := localReserveReceiptCount(localReserveEntries(t, wal)); got != int(through-3) {
					t.Fatalf("persisted phase %d receipt count=%d", through, got)
				}
				if err := wal.Close(); err != nil {
					t.Fatal(err)
				}

				core, wal = localReserveCore(t, dir, nodeID)
				defer wal.Close()
				core.priority = func() (Priority, error) { return localPriority(1), nil }
				beforeEntries := localReserveEntries(t, wal)
				beforeBytes := wal.Bytes()
				if err := core.RecoverThrough(context.Background(), 1); err != nil {
					t.Fatal(err)
				}
				entries := localReserveEntries(t, wal)
				added := entries[len(beforeEntries):]
				wantReceipts := 2
				if through == 4 {
					wantReceipts = 1
				} else if through == 5 {
					wantReceipts = 3
				}
				if got := localReserveReceiptCount(added); got != wantReceipts {
					t.Fatalf("phase-%d recovery appended %d receipts, want %d; steps=%v", through, got, wantReceipts, localReceiptSteps(added))
				}
				if got := len(added); got != wantReceipts+1 {
					t.Fatalf("recovery appended %d entries, want %d receipts plus one decision", got, wantReceipts)
				}
				if got, want := wal.Bytes()-beforeBytes, localReserveBytes(added); got != want {
					t.Fatalf("recovery WAL byte delta=%d, appended-entry bytes=%d", got, want)
				}
				if got := core.Tip(); got != 1 {
					t.Fatalf("recovered tip=%d, want 1", got)
				}
			})
		}
	})
}

func localReceiptSteps(entries []qlog.Entry) []uint64 {
	var steps []uint64
	for _, entry := range entries {
		if entry.Type != qlog.EntryReceipt || entry.Slot == 0 {
			continue
		}
		persisted, err := decodeRecorderEntry(entry.Payload)
		if err == nil {
			steps = append(steps, uint64(persisted.State.Step))
		}
	}
	return steps
}

func TestLocalReserveScheduledRecoveryCounts(t *testing.T) {
	const nodeID NodeID = "local\"id\n\\"
	core, wal := localReserveCore(t, t.TempDir(), nodeID)
	defer wal.Close()
	ctx := context.Background()
	for slot := 1; slot <= 32; slot++ {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("value-%d", slot))); err != nil {
			t.Fatalf("propose before schedule slot %d: %v", slot, err)
		}
	}
	if err := core.RecoverThrough(ctx, 33); err != nil {
		t.Fatal(err)
	}
	for slot := 34; slot <= 48; slot++ {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("value-%d", slot))); err != nil {
			t.Fatalf("propose before schedule slot %d: %v", slot, err)
		}
	}
	if err := core.RecoverThrough(ctx, 49); err != nil {
		t.Fatal(err)
	}
	entries := localReserveEntries(t, wal)
	for _, slot := range []uint64{33, 49} {
		var count int
		var scheduleDecision bool
		for _, entry := range entries {
			if entry.Slot != slot {
				continue
			}
			if entry.Type == qlog.EntryReceipt {
				count++
			}
			if entry.Type == qlog.EntryDecide {
				value, certificate, err := decodeDecisionRecord(entry.Payload[len(decisionEntryMagic):])
				if err != nil {
					t.Fatal(err)
				}
				_, decision, err := decodeCertificate(certificate)
				if err != nil {
					t.Fatal(err)
				}
				decision.Proposal.Value = value
				_, scheduleDecision, err = DecodeLeaderSchedule(value)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		if count != 3 || !scheduleDecision {
			t.Fatalf("schedule slot %d: receipt count=%d schedule decision=%v, want 3 and true", slot, count, scheduleDecision)
		}
	}
	if got := wal.Bytes(); got != localReserveBytes(entries) {
		t.Fatalf("WAL bytes=%d, sum of encoded entries=%d", got, localReserveBytes(entries))
	}
}

func TestLocalReserveDivergentPhaseTwoPartialISRCounts(t *testing.T) {
	const nodeID NodeID = "local\"id\n\\"
	dir := t.TempDir()
	core, wal := localReserveCore(t, dir, nodeID)
	value := []byte("partial phase two")
	requests := []RecordRequest{
		{Slot: 1, Step: 4, Proposal: newProposal(localPriority(80), nodeID, value)},
		{Slot: 1, Step: 5, Proposal: newProposal(localPriority(40), nodeID, value)},
		{Slot: 1, Step: 6, Proposal: newProposal(localPriority(60), nodeID, value)},
	}
	for _, request := range requests {
		if _, err := core.Record(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	core, wal = localReserveCore(t, dir, nodeID)
	defer wal.Close()
	priorities := []Priority{localPriority(10), localPriority(30)}
	core.priority = func() (Priority, error) {
		if len(priorities) == 0 {
			return Priority{}, fmt.Errorf("unexpected extra priority request")
		}
		priority := priorities[0]
		priorities = priorities[1:]
		return priority, nil
	}
	beforeEntries := localReserveEntries(t, wal)
	beforeBytes := wal.Bytes()
	if err := core.RecoverThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	entries := localReserveEntries(t, wal)
	added := entries[len(beforeEntries):]
	if got := localReserveReceiptCount(added); got != 6 {
		t.Fatalf("divergent phase-2 recovery appended %d receipts, want 6; steps=%v", got, localReceiptSteps(added))
	}
	if got := len(added); got != 7 {
		t.Fatalf("recovery appended %d entries, want 6 receipts plus one decision", got)
	}
	if got, want := wal.Bytes()-beforeBytes, localReserveBytes(added); got != want {
		t.Fatalf("recovery WAL byte delta=%d, appended-entry bytes=%d", got, want)
	}
}

func TestLocalReserveMaxNumericAndEscapedIDEncodedLengths(t *testing.T) {
	const nodeID NodeID = "local\"id\n\\"
	core, wal := localReserveCore(t, t.TempDir(), nodeID)
	defer wal.Close()
	value := []byte("max numeric field")
	proposal := newProposal(highestPriority, nodeID, value)
	if _, err := core.Record(context.Background(), RecordRequest{
		Slot: Slot(math.MaxUint64), Step: Step(math.MaxUint64), Proposal: proposal,
	}); err != nil {
		t.Fatal(err)
	}
	entries := localReserveEntries(t, wal)
	var receipt *qlog.Entry
	for i := range entries {
		if entries[i].Type == qlog.EntryReceipt {
			receipt = &entries[i]
			break
		}
	}
	if receipt == nil {
		t.Fatal("missing receipt entry")
	}
	state := core.recorders[Slot(math.MaxUint64)]
	if got, want := len(receipt.Encode()), qlogEntryHeaderBytes+len(encodeRecorderEntry(Slot(math.MaxUint64), state, false)); got != want {
		t.Fatalf("receipt encoded length=%d, formula=%d", got, want)
	}

	var maxPriority Priority
	var maxHash ValueHash
	for i := range maxPriority {
		maxPriority[i] = 0xff
		maxHash[i] = 0xff
	}
	first := &Proposal{Priority: maxPriority, ProposerID: nodeID, Hash: maxHash}
	secondPriority := maxPriority
	secondPriority[len(secondPriority)-1]--
	second := &Proposal{Priority: secondPriority, ProposerID: nodeID, Hash: maxHash}
	decision := Decision{
		ConfigID: ^uint(0), Slot: Slot(math.MaxUint64), Step: Step(math.MaxUint64),
		Proposal:  *first,
		Summaries: []Summary{{RecorderID: nodeID, Step: Step(math.MaxUint64), FirstCurrent: first, AggregatePrior: second}},
	}
	certificate, err := encodeCertificate(^uint(0), decision)
	if err != nil {
		t.Fatal(err)
	}
	maxValue := bytes.Repeat([]byte{0xff}, MaxReplicatedValueBytes)
	record, err := encodeDecisionRecord(maxValue, certificate)
	if err != nil {
		t.Fatal(err)
	}
	entry := qlog.Entry{Slot: math.MaxUint64, Hash: sha256.Sum256(maxValue), Type: qlog.EntryDecide, Payload: append(append([]byte(nil), decisionEntryMagic...), record...)}
	if err := wal.Append(entry); err != nil {
		t.Fatal(err)
	}
	entries = localReserveEntries(t, wal)
	if got := wal.Bytes(); got != localReserveBytes(entries) {
		t.Fatalf("WAL bytes=%d, sum of encoded entries=%d", got, localReserveBytes(entries))
	}
}

const qlogEntryHeaderBytes = 53
