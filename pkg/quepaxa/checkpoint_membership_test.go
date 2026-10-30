package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func checkpointMembershipSource(t *testing.T) (*Core, string, Cluster, CheckpointSeal, DecidedValue) {
	t.Helper()
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	dir := t.TempDir()
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	cores := make(map[NodeID]*Core, len(initial.Members))
	for _, member := range initial.Members {
		wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		core := newCore(member.ID, &initial, wal, transport)
		core.reconfigEnabled = true
		core.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
		cores[member.ID] = core
		transport.cores[member.ID] = core
	}
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	transport.fail("c")
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 3, Members: []Member{{ID: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("after-two-transitions")); err != nil {
		t.Fatal(err)
	}
	index := core.Tip()
	membership, err := core.CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(index)
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{ConfigID: core.ConfigID(), Index: index, RootHash: sha256.Sum256([]byte("membership-root")), StateHash: sha256.Sum256([]byte("membership-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("missing checkpoint decision")
	}
	return core, filepath.Join(dir, "a"), initial, seal, decision
}

func TestCheckpointMembershipCompactionRestartsAfterTransitions(t *testing.T) {
	core, walDir, initial, seal, _ := checkpointMembershipSource(t)
	if err := core.CompactThrough(seal.Index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if got, err := core.CheckpointMembership(seal.Index); err != nil || !sameMembershipRecord(got, *seal.Membership) {
		t.Fatalf("compacted membership=%+v err=%v", got, err)
	}
	entries, err := core.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	base, err := decodeConsensusBase(entries[0].Payload)
	if err != nil || base.Membership == nil || !sameMembershipRecord(*base.Membership, *seal.Membership) {
		t.Fatalf("base membership=%+v err=%v", base.Membership, err)
	}
	if core.ConfigID() != 3 || core.ClusterForSlot(seal.Index+1).ConfigID != 3 || len(initial.Members) != 3 {
		t.Fatal("compaction lost current membership")
	}
	if err := core.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := qlog.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedWAL.Close()
	restarted := newCore("a", &initial, reopenedWAL, nil)
	restarted.reconfigEnabled = true
	if err := restarted.recover(); err != nil {
		t.Fatal(err)
	}
	if restarted.ConfigID() != 3 || restarted.CompactionFloor() != seal.Index {
		t.Fatalf("restart config=%d floor=%d", restarted.ConfigID(), restarted.CompactionFloor())
	}
}

func TestCheckpointMembershipColdObserverRestoreAndRejectsTampering(t *testing.T) {
	source, _, initial, seal, decision := checkpointMembershipSource(t)
	observerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer observerWAL.Close()
	observer, err := NewObserver(Config{NodeID: "observer", Cluster: initial, WAL: observerWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	observer.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := observer.RestoreCheckpointBase(context.Background(), seal, decision); err != nil {
		t.Fatal(err)
	}
	if observer.IsVoter() || observer.ConfigID() != 3 {
		t.Fatalf("observer voter=%v config=%d", observer.IsVoter(), observer.ConfigID())
	}
	if prefix, ok := observer.PrefixHash(seal.Index); !ok || prefix != seal.PrefixHash || observer.Tip() != seal.Index {
		t.Fatalf("restored observer prefix=%x ok=%v tip=%d", prefix, ok, observer.Tip())
	}
	if _, retired := observer.retiredIDs["c"]; !retired {
		t.Fatal("restore lost retired member")
	}
	tampered := seal
	membership := cloneMembershipRecord(*seal.Membership)
	membership.Genesis.Members[0].PublicKey = PublicKey{9}
	tampered.Membership = &membership
	if err := observer.ValidateCheckpointBase(context.Background(), tampered, decision); err == nil {
		t.Fatal("accepted tampered membership history")
	}
	membership = cloneMembershipRecord(*seal.Membership)
	membership.Genesis.Members[0].PeerURL = "quic://tampered"
	tampered.Membership = &membership
	if err := observer.ValidateCheckpointBase(context.Background(), tampered, decision); err == nil {
		t.Fatal("accepted tampered bootstrap peer URL")
	}
	if source.ConfigID() != 3 {
		t.Fatal("source changed during observer restore")
	}
}

func TestCheckpointMembershipModeMismatchFailsClosed(t *testing.T) {
	_, _, initial, seal, decision := checkpointMembershipSource(t)
	for _, enabled := range []bool{false, true} {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		observer, err := NewObserver(Config{NodeID: NodeID(fmt.Sprintf("observer-%t", enabled)), Cluster: initial, WAL: wal, EnableReconfiguration: enabled})
		if err != nil {
			t.Fatal(err)
		}
		observer.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		candidate := seal
		if enabled {
			candidate.Membership = nil
		}
		if err := observer.ValidateCheckpointBase(context.Background(), candidate, decision); err == nil {
			t.Fatalf("mode enabled=%t accepted incompatible checkpoint", enabled)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadOnlyObserverWithReconfigurationNeverWritesMarker(t *testing.T) {
	cluster := Cluster{ConfigID: 1, Members: []Member{{ID: "voter"}}}

	legacyDir := t.TempDir()
	legacy, err := qlog.Open(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err = qlog.OpenReadOnly(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	if !legacy.ReadOnly() {
		t.Fatal("OpenReadOnly WAL did not report its mode")
	}
	observer, err := NewObserver(Config{NodeID: "observer", Cluster: cluster, WAL: legacy, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("open read-only observer on legacy WAL: %v", err)
	}
	if observer.reconfigWAL {
		t.Fatal("legacy WAL unexpectedly reports an absent marker")
	}
	entries, err := legacy.Read()
	if err != nil || len(entries) != 0 {
		t.Fatalf("read-only observer changed legacy WAL: entries=%d err=%v", len(entries), err)
	}
	if _, err := New(Config{NodeID: "voter", Cluster: cluster, WAL: legacy, EnableReconfiguration: true}); err == nil {
		t.Fatal("accepted read-only WAL for a voter")
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	markedDir := t.TempDir()
	marked, err := qlog.Open(markedDir)
	if err != nil {
		t.Fatal(err)
	}
	identity := bytes.Repeat([]byte{0x5a}, 32)
	if err := marked.BindIdentity(identity); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{NodeID: "voter", Cluster: cluster, WAL: marked, EnableReconfiguration: true}); err != nil {
		t.Fatalf("writable voter did not initialize reconfiguration marker: %v", err)
	}
	if err := marked.Close(); err != nil {
		t.Fatal(err)
	}
	marked, err = qlog.OpenReadOnly(markedDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{NodeID: "voter", Cluster: cluster, WAL: marked, EnableReconfiguration: true}); err == nil {
		t.Fatal("accepted identity-bound read-only WAL for a voter")
	}
	if _, err := NewLearner(Config{NodeID: "learner", Cluster: cluster, WAL: marked, EnableReconfiguration: true}); err == nil {
		t.Fatal("accepted identity-bound read-only WAL for a learner")
	}
	if _, err := NewObserver(Config{NodeID: "observer", Cluster: cluster, WAL: marked}); err == nil {
		t.Fatal("disabled read-only observer accepted a reconfiguration marker")
	}
	defer marked.Close()
	observer, err = NewObserver(Config{NodeID: "observer", Cluster: cluster, WAL: marked, EnableReconfiguration: true})
	if err != nil || !observer.reconfigWAL {
		t.Fatalf("read-only observer rejected marker/history: reconfigWAL=%v err=%v", observer != nil && observer.reconfigWAL, err)
	}
	entries, err = marked.Read()
	if err != nil || len(entries) != 1 {
		t.Fatalf("read-only observer changed marked WAL: entries=%d err=%v", len(entries), err)
	}

	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	historyDirs := make(map[NodeID]string, len(initial.Members))
	for _, member := range initial.Members {
		dir := filepath.Join(t.TempDir(), string(member.ID))
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		core := newCore(member.ID, &initial, wal, transport)
		core.reconfigEnabled = true
		core.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
		transport.cores[member.ID] = core
		historyDirs[member.ID] = dir
	}
	targetCluster := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := transport.cores["a"].BeginReconfiguration(context.Background(), targetCluster); err != nil {
		t.Fatal(err)
	}
	if err := transport.cores["a"].FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, core := range transport.cores {
		if err := core.wal.Close(); err != nil {
			t.Fatal(err)
		}
	}
	history, err := qlog.OpenReadOnly(historyDirs["a"])
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	observer, err = NewObserver(Config{NodeID: "history-observer", Cluster: initial, WAL: history, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("read-only observer rejected genuine reconfiguration history: %v", err)
	}
	if got := observer.ConfigID(); got != 2 {
		t.Fatalf("read-only observer recovered config=%d, want genuine history config=2", got)
	}
	if _, err := observer.MembershipHistory(); err != nil {
		t.Fatalf("read-only observer lost membership history: %v", err)
	}
}

func TestWritableCoreRolesInstallReconfigurationMarkerOnce(t *testing.T) {
	cluster := Cluster{ConfigID: 1, Members: []Member{{ID: "voter"}}}
	for _, role := range []string{"voter", "learner", "observer"} {
		t.Run(role, func(t *testing.T) {
			dir := t.TempDir()
			openCore := func(wal *qlog.WAL) (*Core, error) {
				config := Config{NodeID: "voter", Cluster: cluster, WAL: wal, EnableReconfiguration: true}
				switch role {
				case "learner":
					config.NodeID = "learner"
					return NewLearner(config)
				case "observer":
					config.NodeID = "observer"
					return NewObserver(config)
				default:
					return New(config)
				}
			}
			wal, err := qlog.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := openCore(wal); err != nil {
				t.Fatalf("initial writable %s: %v", role, err)
			}
			if got := reconfigurationMarkerCount(t, wal); got != 1 {
				t.Fatalf("initial marker count=%d, want 1", got)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			wal, err = qlog.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			if _, err := openCore(wal); err != nil {
				t.Fatalf("writable %s restart: %v", role, err)
			}
			if got := reconfigurationMarkerCount(t, wal); got != 1 {
				t.Fatalf("restart marker count=%d, want exactly 1", got)
			}
		})
	}
}

func TestReconfigurationMarkerInstallPropagatesAppendAndSyncErrors(t *testing.T) {
	appendErr := errors.New("marker append failed")
	syncErr := errors.New("marker sync failed")
	synced := false
	if err := installReconfigurationMarker(func() error { return appendErr }, func() error {
		synced = true
		return nil
	}); !errors.Is(err, appendErr) || synced {
		t.Fatalf("append failure result=%v syncCalled=%v", err, synced)
	}
	appended := false
	if err := installReconfigurationMarker(func() error { appended = true; return nil }, func() error { return syncErr }); !errors.Is(err, syncErr) || !appended {
		t.Fatalf("sync failure result=%v appendCalled=%v", err, appended)
	}
}

func reconfigurationMarkerCount(t *testing.T, wal *qlog.WAL) int {
	t.Helper()
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Type == qlog.EntryReceipt && bytes.Equal(entry.Payload, isrEntryV2Magic) {
			count++
		}
	}
	return count
}

func TestRestoreCheckpointCompactsMarkerForReadOnlyObserver(t *testing.T) {
	source, _, initial, seal, decision := checkpointMembershipSource(t)
	sourceTransport := source.transport.(*clusterTransport)
	for _, core := range sourceTransport.cores {
		if err := core.wal.Close(); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Repeat([]byte{0x83}, 32)
	if err := wal.BindIdentity(nonce); err != nil {
		t.Fatal(err)
	}
	observer, err := NewObserver(Config{NodeID: "restore-observer", Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := reconfigurationMarkerCount(t, wal); got != 1 {
		t.Fatalf("pre-restore marker count=%d, want 1", got)
	}
	observer.SetCheckpointValidator(func(_ context.Context, candidate CheckpointSeal) error {
		if !reflect.DeepEqual(candidate, seal) {
			return errors.New("checkpoint differs from fixture")
		}
		return nil
	})
	if err := observer.RestoreCheckpointBase(context.Background(), seal, decision); err != nil {
		t.Fatalf("restore verified checkpoint base: %v", err)
	}
	if observer.Tip() != seal.Index {
		t.Fatalf("restored tip=%d, want %d", observer.Tip(), seal.Index)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	segments, err := filepath.Glob(filepath.Join(dir, "seg_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("WAL segments=%v err=%v", segments, err)
	}
	active, err := os.OpenFile(segments[len(segments)-1], os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := active.Write([]byte{0xde, 0xad}); err != nil {
		_ = active.Close()
		t.Fatal(err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := func() map[string][32]byte {
		t.Helper()
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		result := make(map[string][32]byte, len(files))
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil {
				t.Fatal(err)
			}
			result[file.Name()] = sha256.Sum256(data)
		}
		return result
	}
	before := snapshot()
	readonly, err := qlog.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("open compacted WAL read-only with torn active tail: %v", err)
	}
	if !readonly.ReadOnly() {
		t.Fatal("compacted WAL lost its read-only mode")
	}
	if got, ok := readonly.Identity(); !ok || !bytes.Equal(got, nonce) {
		t.Fatalf("compacted WAL nonce=%x present=%v", got, ok)
	}
	restarted, err := NewObserver(Config{NodeID: "restore-observer", Cluster: initial, WAL: readonly, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("open read-only observer after checkpoint compaction: %v", err)
	}
	if restarted.WALIdentity() != hex.EncodeToString(nonce) {
		t.Fatalf("observer WAL identity=%q, want nonce %x", restarted.WALIdentity(), nonce)
	}
	if got := reconfigurationMarkerCount(t, readonly); got != 0 {
		t.Fatalf("restore compaction left %d marker entries, want none", got)
	}
	if restarted.reconfigWAL {
		t.Fatal("observer inferred a marker from checkpoint membership")
	}
	if restarted.Tip() != seal.Index || restarted.ConfigID() != seal.ConfigID {
		t.Fatalf("restored observer tip/config=%d/%d, want %d/%d", restarted.Tip(), restarted.ConfigID(), seal.Index, seal.ConfigID)
	}
	if prefix, ok := restarted.PrefixHash(seal.Index); !ok || prefix != seal.PrefixHash {
		t.Fatalf("restored observer prefix=%x present=%v, want %x", prefix, ok, seal.PrefixHash)
	}
	membership, err := restarted.MembershipHistory()
	if err != nil || !sameMembershipRecord(membership, *seal.Membership) {
		t.Fatalf("restored membership=%+v err=%v", membership, err)
	}
	if _, err := NewObserver(Config{NodeID: "disabled-observer", Cluster: initial, WAL: readonly}); err == nil {
		t.Fatal("disabled observer accepted compacted reconfiguration history")
	}
	if err := readonly.Close(); err != nil {
		t.Fatal(err)
	}
	after := snapshot()
	if len(after) != len(before) {
		t.Fatalf("read-only observers changed WAL file set: before=%v after=%v", before, after)
	}
	for name, hash := range before {
		if after[name] != hash {
			t.Fatalf("read-only observer changed WAL file %s: before=%x after=%x", name, hash, after[name])
		}
	}
}

func TestCheckpointMembershipRejectsUnboundFreeze(t *testing.T) {
	_, _, initial, seal, _ := checkpointMembershipSource(t)
	history := cloneMembershipRecord(*seal.Membership)
	freeze := &history.Transitions[1].Freeze
	id, decision, err := decodeCertificate(freeze.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Summaries[0].ReconfigurationID = ValueHash{9}
	freeze.Certificate, err = encodeCertificate(id, decision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateMembershipHistory(initial, history); err == nil {
		t.Fatal("accepted a freeze not bound to its own proposal")
	}
}

func TestCheckpointMembershipRejectsPendingTerminal(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CheckpointMembership(core.Tip()); err == nil {
		t.Fatal("checkpoint membership accepted active freeze")
	}
	pending, ok := core.PendingReconfiguration()
	if !ok || pending.ConfigID != 2 {
		t.Fatalf("pending reconfiguration=%+v ok=%v", pending, ok)
	}
	pending.Members[0].ID = "mutated"
	if fresh, ok := core.PendingReconfiguration(); !ok || fresh.Members[0].ID != "a" {
		t.Fatal("pending reconfiguration exposed core state")
	}
}

func TestCheckpointMembershipProofRespectsValueLimit(t *testing.T) {
	seal := CheckpointSeal{
		ConfigID: 1, Index: 1, RootHash: sha256.Sum256([]byte("root")), StateHash: sha256.Sum256([]byte("state")), PrefixHash: sha256.Sum256([]byte("prefix")), NextLeaderOrder: []NodeID{"a"},
		Membership: &MembershipRecord{Genesis: Cluster{
			ConfigID: 1,
			Members:  []Member{{ID: "a", WALIdentity: string(make([]byte, MaxReplicatedValueBytes))}},
		}},
	}
	if _, err := EncodeCheckpointSeal(seal); err == nil {
		t.Fatal("accepted an oversized checkpoint membership proof")
	}
}

func TestRestoreReplaysAbortAboveCheckpoint(t *testing.T) {
	core, _, initial, seal, sealed := checkpointMembershipSource(t)
	target := core.CurrentCluster()
	target.ConfigID++
	target.Members = append(target.Members, Member{ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"})
	freeze, err := core.BeginReconfiguration(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	tip := core.Tip()
	prefix, _ := core.PrefixHash(tip)
	walDir := t.TempDir()
	restoreWAL, err := qlog.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	restored := newCore("a", &initial, restoreWAL, nil)
	restored.reconfigEnabled = true
	restored.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	// Retain the certified suffix while the earlier prefix is still absent.
	for slot := seal.Index + 1; slot <= tip; slot++ {
		value := mustCertified(t, core, slot)
		restored.decided[slot] = value
		if err := restored.appendDecision(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreCheckpointBase(context.Background(), seal, sealed); err != nil {
		t.Fatal(err)
	}
	core = restored
	if got, ok := core.PrefixHash(tip); core.Tip() != tip || !ok || got != prefix {
		t.Fatal("compaction changed checked prefix")
	}
	if slot, _, ok := core.LastReconfigurationAbort(); !ok || slot != freeze+16 {
		t.Fatal("compaction lost suffix abort")
	}
	if err := core.wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	restarted := newCore("a", &initial, wal, nil)
	restarted.reconfigEnabled = true
	if err := restarted.recover(); err != nil {
		t.Fatal(err)
	}
	if slot, _, ok := restarted.LastReconfigurationAbort(); !ok || slot != freeze+16 || restarted.Tip() != tip {
		t.Fatal("restart lost suffix state")
	}
}
