package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func localReserveCore(t *testing.T, dir string, nodeID NodeID) (*Core, *qlog.WAL) {
	return localReserveCoreWithLimit(t, dir, nodeID, math.MaxInt64)
}

func localReserveCoreWithLimit(t *testing.T, dir string, nodeID NodeID, limit int64) (*Core, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.SetMaxBytes(limit); err != nil {
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

func localCoreFromEntries(t *testing.T, dir string, nodeID NodeID, entries []qlog.Entry, limit int64) (*Core, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
	}
	if err := wal.Sync(); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.SetMaxBytes(limit); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	core, err := New(Config{NodeID: nodeID, Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: nodeID}}}, WAL: wal, LocalMode: true})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	return core, wal
}

func localRecordForTest(t *testing.T, core *Core, request RecordRequest) (Summary, error) {
	t.Helper()
	if err := core.acquireLocalExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer core.releaseLocalExecution()
	return core.recordLocalOwned(context.Background(), request)
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
					if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: step, Proposal: proposal}); err != nil {
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

func TestLocalAdmissionDeniesBeforeMutationAndAllowsExactFit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		short bool
	}{
		{name: "one byte short", short: true},
		{name: "exact fit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, err := newLocalCostModel("admission", 1)
			if err != nil {
				t.Fatal(err)
			}
			value := []byte("offered-value")
			cost, err := model.estimate(len(value))
			if err != nil {
				t.Fatal(err)
			}
			limit := cost.ForegroundBytes + cost.RecoveryBytes + cost.CheckpointBytes
			if tc.short {
				limit--
			}
			core, wal := localReserveCoreWithLimit(t, t.TempDir(), "admission", int64(limit))
			defer wal.Close()
			if _, _, err := core.Propose(context.Background(), value); err != nil {
				if !tc.short || !IsLocalAdmissionDenial(err) {
					t.Fatalf("Propose() error=%v", err)
				}
				var denial *localAdmissionDenial
				wantProtected := cost.RecoveryBytes + cost.CheckpointBytes
				if !errors.As(err, &denial) || denial.operation != "propose" || denial.reason != "protected_reserve" || denial.used != 0 || denial.limit != limit || denial.immediate != cost.ForegroundBytes || denial.protected != wantProtected {
					t.Fatalf("prewrite denial details=%+v, err=%v", denial, err)
				}
				if got := len(localReserveEntries(t, wal)); got != 0 || core.Tip() != 0 || core.RecorderTip() != 0 {
					t.Fatalf("denial mutated state: entries=%d tip=%d recorder=%d", got, core.Tip(), core.RecorderTip())
				}
				return
			} else if tc.short {
				t.Fatal("one-byte-short proposal unexpectedly succeeded")
			}
			if wal.Bytes() > int64(limit) {
				t.Fatalf("exact-fit proposal exceeded limit: used=%d limit=%d", wal.Bytes(), limit)
			}
		})
	}
}

func TestLocalCoreRequiresConfiguredFiniteWALLimit(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	config := Config{NodeID: "finite", Cluster: Cluster{Members: []Member{{ID: "finite"}}}, WAL: wal, LocalMode: true}
	if _, err := New(config); err == nil {
		t.Fatal("Local Core accepted an unbounded WAL")
	}
	if err := wal.SetMaxBytes(1 << 20); err != nil {
		t.Fatal(err)
	}
	if _, err := New(config); err != nil {
		t.Fatalf("Local Core rejected a finite positive WAL limit: %v", err)
	}
}

func TestLocalRecoveryAdmissionPlansEntirePendingRangeBeforeWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		short bool
	}{
		{name: "one byte short", short: true},
		{name: "whole range fits"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seed, seedWAL := localReserveCore(t, dir+"/seed", "recovery-budget")
			proposal := newProposal(highestPriority, seed.nodeID, []byte("pending slot 32"))
			if _, err := localRecordForTest(t, seed, RecordRequest{Slot: 32, Step: 4, ConfigID: seed.ConfigID(), Proposal: proposal}); err != nil {
				t.Fatal(err)
			}
			before := localReserveEntries(t, seedWAL)
			used := seedWAL.Bytes()
			unknown, err := checkedMul(32, seed.localCost.maxSlotBytes)
			if err != nil {
				t.Fatal(err)
			}
			need, err := checkedAdd(unknown, seed.localCost.maxCheckpointCostBytes)
			if err != nil {
				t.Fatal(err)
			}
			limit := uint64(used) + need
			if tc.short {
				limit--
			}
			_ = seedWAL.Close()
			core, wal := localCoreFromEntries(t, dir+"/target", "recovery-budget", before, int64(limit))
			defer wal.Close()
			err = core.RecoverThrough(context.Background(), 32)
			if tc.short {
				if !IsLocalAdmissionDenial(err) {
					t.Fatalf("RecoverThrough error=%v, want prewrite admission denial", err)
				}
				if wal.Bytes() != used || !reflect.DeepEqual(before, localReserveEntries(t, wal)) || core.Tip() != 0 || core.RecorderTip() != 32 {
					t.Fatalf("denial changed pending evidence: used=%d/%d tip=%d recorder=%d", used, wal.Bytes(), core.Tip(), core.RecorderTip())
				}
				return
			}
			if err != nil {
				t.Fatalf("adequately budgeted recovery failed: %v", err)
			}
			if wal.Bytes() > int64(limit) || core.Tip() != 32 {
				t.Fatalf("recovery exceeded budget or missed target: used=%d limit=%d tip=%d", wal.Bytes(), limit, core.Tip())
			}
		})
	}
}

func TestLocalForegroundAdmissionIncludesUnloggedHistoricalDecision(t *testing.T) {
	dir := t.TempDir()
	seed, seedWAL := localReserveCore(t, dir+"/seed", "unlogged-history")
	if _, _, err := seed.ProposeCertified(context.Background(), []byte("certified but not logged")); err != nil {
		t.Fatal(err)
	}
	first, ok := seed.CertifiedValue(1)
	if !ok {
		t.Fatal("first proposal was not retained")
	}
	h := uint64(len(mustDecisionEntry(t, first).Encode()))
	if h == 0 {
		t.Fatal("empty missing QDEC cost")
	}
	value := []byte("next foreground proposal")
	cost, err := seed.LocalWALCosts(len(value))
	if err != nil {
		t.Fatal(err)
	}
	entries := localReserveEntries(t, seedWAL)
	used := seedWAL.Bytes()
	limit := uint64(used) + cost.ForegroundBytes + cost.RecoveryBytes + cost.CheckpointBytes
	_ = seedWAL.Close()
	core, wal := localCoreFromEntries(t, dir+"/target", "unlogged-history", entries, int64(limit))
	defer wal.Close()
	decision, err := decodeDecision(first.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = append([]byte(nil), first.Value...)
	if err := core.acquireLocalExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	core.acceptDecision(decision)
	core.releaseLocalExecution()
	before := localReserveEntries(t, wal)
	if _, _, err := core.Propose(context.Background(), value); !IsLocalAdmissionDenial(err) {
		t.Fatalf("proposal omitting historical QDEC reserve error=%v, want prewrite denial", err)
	}
	if wal.Bytes() != used || !reflect.DeepEqual(before, localReserveEntries(t, wal)) || core.Tip() != 1 {
		t.Fatalf("denial changed retained history: used=%d/%d tip=%d", used, wal.Bytes(), core.Tip())
	}
}

func TestLocalDecisionCompletionReservesExactMissingQDECAndCheckpoint(t *testing.T) {
	seed, seedWAL := localReserveCore(t, t.TempDir()+"/seed", "completion-cost")
	slot, _, err := seed.ProposeCertified(context.Background(), []byte("needs QDEC"))
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := seed.CertifiedValue(slot)
	if !ok {
		t.Fatal("certified decision was not retained")
	}
	missing := uint64(len(mustDecisionEntry(t, decision).Encode()))
	_ = seedWAL.Close()
	core, wal := localReserveCoreWithLimit(t, t.TempDir(), "completion-cost", int64(missing+seed.localCost.maxCheckpointCostBytes-1))
	defer wal.Close()
	decoded, err := decodeDecision(decision.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Proposal.Value = append([]byte(nil), decision.Value...)
	if err := core.acquireLocalExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	core.acceptDecision(decoded)
	core.releaseLocalExecution()
	used := wal.Bytes()
	before := localReserveEntries(t, wal)
	if _, err := core.CompleteDecision(context.Background(), slot); !IsLocalAdmissionDenial(err) {
		t.Fatalf("CompleteDecision error=%v, want protected-reserve prewrite denial", err)
	}
	if wal.Bytes() != used || !reflect.DeepEqual(before, localReserveEntries(t, wal)) {
		t.Fatalf("completion denial appended QDEC: bytes=%d/%d", used, wal.Bytes())
	}
}

func TestLocalRecoveryPrevalidatesLatePendingISRBeforeAnyAppend(t *testing.T) {
	dir := t.TempDir()
	seed, seedWAL := localReserveCore(t, dir+"/seed", "late-isr")
	proposal := newProposal(highestPriority, seed.nodeID, []byte("pending"))
	if _, err := localRecordForTest(t, seed, RecordRequest{Slot: 32, Step: 4, ConfigID: seed.ConfigID(), Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	entries := localReserveEntries(t, seedWAL)
	_ = seedWAL.Close()
	for i := range entries {
		if entries[i].Type == qlog.EntryReceipt && entries[i].Slot == 32 {
			entries[i].Payload = encodeRecorderEntry(32, ISR{Step: 7}, false)
		}
	}
	core, wal := localCoreFromEntries(t, dir+"/target", "late-isr", entries, math.MaxInt64)
	defer wal.Close()
	before := localReserveEntries(t, wal)
	used := wal.Bytes()
	if err := core.RecoverThrough(context.Background(), 32); err == nil {
		t.Fatal("recovery accepted unsupported late-range ISR")
	}
	if wal.Bytes() != used || !reflect.DeepEqual(before, localReserveEntries(t, wal)) || core.Tip() != 0 {
		t.Fatalf("late ISR validation partially recovered: bytes=%d/%d tip=%d", used, wal.Bytes(), core.Tip())
	}
}

func TestLocalRecordAttemptLimitStopsBeforeThirdRoundRecord(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	core.localRecordAttemptLimit = 2
	err := core.RecoverThrough(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "attempt limit exceeded") {
		t.Fatalf("RecoverThrough error=%v, want local attempt-limit error", err)
	}
	entries := localReserveEntries(t, wal)
	if got := localReserveReceiptCount(entries); got != 2 {
		t.Fatalf("receipt count=%d, want two records before limit", got)
	}
	for _, entry := range entries {
		if entry.Type == qlog.EntryDecide {
			t.Fatal("attempt limit was checked after decision append")
		}
	}
}

func TestLocalRecordAttemptCounterAllowsThreeAndRejectsFourth(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	attempts := 0
	for i := 1; i <= 4; i++ {
		got := core.takeLocalRecordAttempt(&attempts)
		if want := i <= 3; got != want {
			t.Fatalf("attempt %d allowed=%v, want %v", i, got, want)
		}
	}
	if attempts != 3 {
		t.Fatalf("attempt counter=%d, want 3", attempts)
	}
}

func TestLocalRecordFailuresExposeOnlyCompletedWALBoundaries(t *testing.T) {
	t.Run("stage append failure", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		proposal := newProposal(highestPriority, "local", []byte("stage failure"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err == nil {
			t.Fatal("record succeeded with closed WAL")
		}
		if core.recorders[1].Step != 0 {
			t.Fatalf("recorder state=%+v after stage failure", core.recorders[1])
		}
		if _, ok := core.Value(proposal.Hash); ok {
			t.Fatal("stage failure installed value")
		}
	})
	t.Run("receipt append failure after partial stage", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		defer wal.Close()
		var entriesAtFailure []qlog.Entry
		core.recordBeforeAppend = func() { entriesAtFailure = localReserveEntries(t, wal); _ = wal.Close() }
		proposal := newProposal(highestPriority, "local", []byte("receipt failure"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err == nil {
			t.Fatal("record succeeded after WAL closed before receipt")
		}
		if core.recorders[1].Step != 0 {
			t.Fatalf("recorder state=%+v after receipt failure", core.recorders[1])
		}
		if value, ok := core.stagedValue(proposal.Hash); !ok || !bytes.Equal(value, proposal.Value) {
			t.Fatal("partial stage boundary did not retain staged value")
		}
		if got := localReserveReceiptCount(entriesAtFailure); got != 0 {
			t.Fatalf("receipt count=%d, want 0", got)
		}
	})
	t.Run("sync failure after receipt append", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		defer wal.Close()
		core.commits = newGroupCommit(func() error { return fmt.Errorf("injected sync failure") })
		proposal := newProposal(highestPriority, "local", []byte("sync failure"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err == nil {
			t.Fatal("record succeeded despite sync failure")
		}
		if core.recorders[1].Step != 4 {
			t.Fatalf("recorder state=%+v after sync failure, want appended state", core.recorders[1])
		}
		if got := localReserveReceiptCount(localReserveEntries(t, wal)); got != 1 {
			t.Fatalf("receipt count=%d, want appended receipt", got)
		}
	})
}

func TestLocalPendingISRValidationPrecedesStaging(t *testing.T) {
	t.Run("invalid current reference", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		defer wal.Close()
		bad := newProposal(highestPriority, "foreign", []byte("not retained"))
		bad.Value = nil
		core.recorders[1] = ISR{Step: 4, FirstCurrent: &bad, AggregateCurrent: &bad}
		before := localReserveEntries(t, wal)
		if err := core.RecoverThrough(context.Background(), 1); err == nil {
			t.Fatal("recovered unsupported current ISR")
		}
		if !reflect.DeepEqual(localReserveEntries(t, wal), before) || wal.Bytes() != 0 {
			t.Fatal("invalid current reference changed WAL")
		}
		if _, ok := core.Value(bad.Hash); ok {
			t.Fatal("invalid current reference staged its value")
		}
	})
	t.Run("invalid next reference", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		defer wal.Close()
		proposal := newProposal(highestPriority, "foreign", []byte("not local proposer"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err == nil {
			t.Fatal("recorded invalid next ISR")
		}
		if got := len(localReserveEntries(t, wal)); got != 0 || wal.Bytes() != 0 {
			t.Fatalf("invalid next reference changed WAL: entries=%d bytes=%d", got, wal.Bytes())
		}
		if _, ok := core.Value(proposal.Hash); ok {
			t.Fatal("invalid next reference staged its value")
		}
	})
}

func TestLocalGapRecoveryPreservesCertifiedLaterSlotWithUnsupportedISR(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	ctx := context.Background()
	if slot, _, err := core.Propose(ctx, []byte("prefix")); err != nil || slot != 1 {
		t.Fatalf("prefix proposal slot=%d err=%v", slot, err)
	}
	if err := core.acquireLocalExecution(ctx); err != nil {
		t.Fatal(err)
	}
	decision, err := core.runSlot(ctx, 3, []byte("already certified later"), false)
	if err == nil {
		err = core.acceptDecisionDurable(decision)
	}
	core.releaseLocalExecution()
	if err != nil {
		t.Fatalf("create sparse later decision: %v", err)
	}
	core.mu.Lock()
	certified := core.decided[3]
	core.recorders[3] = ISR{Step: 7}
	core.mu.Unlock()
	beforeReceipts := localReceiptCountForSlot(localReserveEntries(t, wal), 3)
	if err := core.RecoverThrough(ctx, 3); err != nil {
		t.Fatalf("recover gap through certified slot: %v", err)
	}
	after, ok := core.decision(3)
	if !ok || !bytes.Equal(after.Certificate, certified.Certificate) {
		t.Fatal("gap recovery replaced the later slot certificate")
	}
	if got := localReceiptCountForSlot(localReserveEntries(t, wal), 3); got != beforeReceipts {
		t.Fatalf("later-slot receipts=%d after recovery, want unchanged %d", got, beforeReceipts)
	}
	if core.Tip() != 3 {
		t.Fatalf("tip=%d after filling gap, want 3", core.Tip())
	}
}

func localReceiptCountForSlot(entries []qlog.Entry, slot Slot) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == qlog.EntryReceipt && Slot(entry.Slot) == slot {
			count++
		}
	}
	return count
}

func TestLocalRunSlotReturnsExistingDecisionBeforeUnsupportedISRValidation(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if slot, _, err := core.Propose(context.Background(), []byte("certified")); err != nil || slot != 1 {
		t.Fatalf("Propose slot=%d err=%v", slot, err)
	}
	core.mu.Lock()
	certified := core.decided[1]
	core.recorders[1] = ISR{Step: 7}
	core.mu.Unlock()
	beforeReceipts := localReceiptCountForSlot(localReserveEntries(t, wal), 1)
	decision, err := core.runSlot(context.Background(), 1, []byte("replacement"), true)
	if err != nil {
		t.Fatalf("runSlot existing certified decision: %v", err)
	}
	if decision.Proposal.Hash != certified.Hash {
		t.Fatal("runSlot returned a replacement value")
	}
	if after, _ := core.decision(1); !bytes.Equal(after.Certificate, certified.Certificate) {
		t.Fatal("runSlot replaced the stored certificate")
	}
	if got := localReceiptCountForSlot(localReserveEntries(t, wal), 1); got != beforeReceipts {
		t.Fatalf("receipts=%d after existing-decision shortcut, want %d", got, beforeReceipts)
	}
}

func TestLocalSupportedStepFourAggregateMayExceedFirst(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	first := newProposal(localPriority(10), "local", []byte("first"))
	higher := newProposal(localPriority(20), "local", []byte("higher aggregate"))
	for _, proposal := range []Proposal{first, higher} {
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
			t.Fatal(err)
		}
	}
	state := core.recorders[1]
	if state.Step != 4 || compareProposal(state.AggregateCurrent, state.FirstCurrent) <= 0 {
		t.Fatalf("step-4 state=%+v, want A > F", state)
	}
	if err := core.RecoverThrough(context.Background(), 1); err != nil {
		t.Fatalf("recover supported A>F step 4: %v", err)
	}
}

func TestLocalSupportedStepFivePriorMayExceedFirst(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	low := newProposal(localPriority(10), "local", []byte("low"))
	high := newProposal(localPriority(20), "local", []byte("high"))
	for _, request := range []RecordRequest{
		{Slot: 1, Step: 4, Proposal: low},
		{Slot: 1, Step: 4, Proposal: high},
		{Slot: 1, Step: 5, Proposal: low},
	} {
		if _, err := localRecordForTest(t, core, request); err != nil {
			t.Fatal(err)
		}
	}
	state := core.recorders[1]
	if state.Step != 5 || !sameProposal(state.FirstCurrent, state.AggregateCurrent) || compareProposal(state.AggregatePrior, state.FirstCurrent) <= 0 {
		t.Fatalf("step-5 state=%+v, want F=A and P>F", state)
	}
	if err := core.RecoverThrough(context.Background(), 1); err != nil {
		t.Fatalf("recover supported P>F step 5: %v", err)
	}
}

func TestLocalPublicRecorderMutatorsRejectWithoutMutation(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	proposal := newProposal(highestPriority, "local", []byte("must stay unstaged"))
	before := localReserveEntries(t, wal)
	beforeBytes := wal.Bytes()
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err == nil {
			t.Fatal("Record accepted an external Local mutation")
		}
		if err := core.StageValue(proposal.Hash, proposal.Value); err == nil {
			t.Fatal("StageValue accepted an external Local mutation")
		}
		if err := core.StoreValue(proposal.Hash, proposal.Value); err == nil {
			t.Fatal("StoreValue accepted an external Local mutation")
		}
	}
	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{"AcceptDecision", func() error { return core.AcceptDecision(Decision{}) }},
		{"AcceptDecisionHint", func() error { return core.AcceptDecisionHint(Decision{}) }},
		{"AcceptCertifiedValue", func() error { return core.AcceptCertifiedValue(DecidedValue{}) }},
		{"AcceptCertifiedValueForAck", func() error { return core.AcceptCertifiedValueForAck(DecidedValue{}) }},
		{"AcceptCertifiedValues", func() error { return core.AcceptCertifiedValues([]DecidedValue{{}}) }},
		{"AcceptCertifiedHints", func() error { return core.AcceptCertifiedHints([]DecidedValue{{}}) }},
	} {
		t.Run(call.name, func(t *testing.T) {
			for attempt := 0; attempt < 2; attempt++ {
				if err := call.fn(); err == nil {
					t.Fatal("accepted external Local mutation")
				}
			}
		})
	}
	if err := core.RestoreCheckpointBase(context.Background(), CheckpointSeal{}, DecidedValue{}); err == nil {
		t.Fatal("RestoreCheckpointBase accepted external Local authority")
	}
	if err := core.InstallFencedGenerationBase(context.Background(), FencedGenerationBase{}); err == nil {
		t.Fatal("InstallFencedGenerationBase accepted external Local authority")
	}
	after := localReserveEntries(t, wal)
	if !reflect.DeepEqual(after, before) || wal.Bytes() != beforeBytes {
		t.Fatal("public Local mutation changed WAL")
	}
	if _, ok := core.Value(proposal.Hash); ok {
		t.Fatal("public Local mutation installed value")
	}
	if core.Tip() != 0 || core.RecorderTip() != 0 {
		t.Fatalf("tips changed: tip=%d recorder=%d", core.Tip(), core.RecorderTip())
	}
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
	proposal80 := newProposal(localPriority(80), nodeID, value)
	proposal40 := newProposal(localPriority(40), nodeID, value)
	proposal60 := newProposal(localPriority(60), nodeID, value)
	if err := wal.Append(qlog.Entry{Hash: proposal80.Hash, Type: qlog.EntryProposal, Payload: value}); err != nil {
		t.Fatal(err)
	}
	state := ISR{}
	for _, item := range []struct {
		step     Step
		proposal Proposal
	}{{4, proposal80}, {5, proposal40}, {6, proposal60}} {
		state, _ = state.Record(item.step, item.proposal)
		payload := encodeRecorderEntry(1, state, false)
		if err := wal.Append(qlog.Entry{Slot: 1, Hash: item.proposal.Hash, Type: qlog.EntryReceipt, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	core, wal = localReserveCore(t, dir, nodeID)
	defer wal.Close()
	beforeEntries := localReserveEntries(t, wal)
	beforeBytes := wal.Bytes()
	if err := core.RecoverThrough(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "unsupported pending local ISR") {
		t.Fatalf("RecoverThrough error=%v, want unsupported pending ISR", err)
	}
	entries := localReserveEntries(t, wal)
	if !reflect.DeepEqual(entries, beforeEntries) || wal.Bytes() != beforeBytes {
		t.Fatal("unsupported ISR recovery mutated the WAL")
	}
}

func TestLocalReserveMaxNumericAndEscapedIDEncodedLengths(t *testing.T) {
	const nodeID NodeID = "local\"id\n\\"
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	core, err := New(Config{NodeID: nodeID, Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: nodeID}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
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
