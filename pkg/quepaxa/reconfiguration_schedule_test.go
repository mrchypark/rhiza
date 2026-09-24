package quepaxa

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReconfigurationScheduleBoundaryAbort(t *testing.T) {
	for _, tip := range []Slot{80, 95, 96, 97, 112} {
		t.Run(fmt.Sprint(tip), func(t *testing.T) {
			cores, _ := reconfigCluster(t)
			c := cores["a"]
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := c.RecoverThrough(ctx, tip); err != nil {
				t.Fatal(err)
			}
			if c.Tip() != tip {
				t.Fatalf("starting tip=%d want=%d", c.Tip(), tip)
			}
			target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
			slot, err := c.BeginReconfiguration(ctx, target)
			if err != nil {
				t.Fatal(err)
			}
			want := tip + 1
			if tip == 80 || tip == 96 || tip == 112 {
				want++
			}
			if slot != want {
				t.Fatalf("freeze=%d want=%d", slot, want)
			}
			if err = c.AbortReconfiguration(ctx); err != nil {
				t.Fatal(err)
			}
			for c.Tip() < slot+32 {
				if _, _, err = c.Propose(ctx, []byte("after")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReconfigurationSparseScheduleFreezeFailsClosed(t *testing.T) {
	for _, certified := range []bool{false, true} {
		t.Run(fmt.Sprint(certified), func(t *testing.T) {
			initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
			dir := t.TempDir()
			transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
			cores := transport.cores
			for _, member := range initial.Members {
				wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = wal.Close() })
				core, err := New(Config{NodeID: member.ID, Cluster: initial, WAL: wal, Transport: transport, EnableReconfiguration: true})
				if err != nil {
					t.Fatal(err)
				}
				cores[member.ID] = core
			}
			c := cores["a"]
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := c.RecoverThrough(ctx, 81); err != nil {
				t.Fatal(err)
			}
			target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
			value, err := encodeReconfiguration(reconfigurationValue{Freeze: 97, TerminalSlot: 113, Target: target, PrefixHash: [32]byte{3}})
			if err != nil {
				t.Fatal(err)
			}
			late, err := c.runSlot(ctx, 97, value, false)
			if err != nil {
				t.Fatal(err)
			}
			if certified {
				if err := c.AcceptDecision(late); err != nil {
					t.Fatal(err)
				}
			}
			for id, core := range cores {
				if err := core.wal.Close(); err != nil {
					t.Fatal(err)
				}
				wal, err := qlog.Open(filepath.Join(dir, string(id)))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = wal.Close() })
				restarted, err := New(Config{NodeID: id, Cluster: initial, WAL: wal, Transport: transport, EnableReconfiguration: true})
				if err != nil {
					t.Fatal(err)
				}
				cores[id] = restarted
			}
			c = cores["a"]
			if _, err := c.BeginReconfiguration(ctx, target); err != nil {
				t.Fatal(err)
			}
			if err := c.AbortReconfiguration(ctx); err == nil {
				t.Fatal("abort accepted incompatible schedule history")
			}
			if c.Tip() >= 97 {
				t.Fatalf("advanced through poisoned schedule: %d", c.Tip())
			}
			if got, ok := c.decision(97); ok && !bytes.Equal(got.Value, value) {
				t.Fatal("poisoned evidence overwritten")
			}
		})
	}
}

func TestLegacyContiguousScheduleWALRejected(t *testing.T) {
	for _, phase := range []string{"drain", "abort"} {
		t.Run(phase, func(t *testing.T) {
			source := filepath.Join("..", "..", "testdata", "reconfiguration-schedule", phase)
			entries, err := os.ReadDir(source)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			before := map[string][]byte{}
			for _, entry := range entries {
				data, err := os.ReadFile(filepath.Join(source, entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				before[entry.Name()] = data
				if err := os.WriteFile(filepath.Join(dir, entry.Name()), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			wal, err := qlog.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(Config{NodeID: "a", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}, WAL: wal, Transport: &clusterTransport{}, EnableReconfiguration: true})
			if err == nil || !strings.Contains(err.Error(), "incompatible leader schedule history at slot 97") {
				t.Fatalf("recovery error=%v", err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(before) {
				t.Fatal("recovery changed WAL file set")
			}
			for name, expected := range before {
				actual, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(actual, expected) {
					t.Fatalf("recovery changed evidence %s", name)
				}
			}
		})
	}
}

func TestCompactionCannotAdvanceRejectedSchedule(t *testing.T) {
	cores, _ := reconfigCluster(t)
	c := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.RecoverThrough(ctx, 64); err != nil {
		t.Fatal(err)
	}
	membership, err := c.CheckpointMembership(64)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := c.PrefixHash(64)
	next, following, err := c.CheckpointLeaderOrders(64)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{ConfigID: 1, Index: 64, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	for _, core := range cores {
		if err := core.RecoverThrough(ctx, 64); err != nil {
			t.Fatal(err)
		}
		core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	if err := c.RecoverThrough(ctx, 81); err != nil {
		t.Fatal(err)
	}
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: strings.Repeat("0", 64)}}}
	value, err := encodeReconfiguration(reconfigurationValue{Freeze: 97, TerminalSlot: 113, Target: target, PrefixHash: [32]byte{3}})
	if err != nil {
		t.Fatal(err)
	}
	late, err := c.runSlot(ctx, 97, value, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptDecision(late); err != nil {
		t.Fatal(err)
	}
	if err := c.RecoverThrough(ctx, 96); err == nil || !strings.Contains(err.Error(), "incompatible leader schedule") {
		t.Fatalf("expected schedule rejection, got %v", err)
	}
	if c.Tip() != 96 {
		t.Fatalf("tip before compaction = %d", c.Tip())
	}
	t.Run("restore", func(t *testing.T) {
		restored, _ := reconfigCluster(t)
		r := restored["a"]
		r.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		for slot := Slot(65); slot <= 97; slot++ {
			decision, err := historyDecision(1, mustCertified(t, c, slot))
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AcceptDecision(decision); err != nil {
				t.Fatal(err)
			}
		}
		before, err := r.wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.RestoreCheckpointBase(ctx, seal, mustCertified(t, c, 65)); err == nil || !strings.Contains(err.Error(), "incompatible leader schedule") {
			t.Fatalf("restore accepted rejected suffix: %v", err)
		}
		if r.Tip() != 0 || r.CompactionFloor() != 0 {
			t.Fatal("failed restore changed base")
		}
		if got, ok := r.CertifiedValue(97); !ok || !bytes.Equal(got.Value, value) {
			t.Fatal("restore lost rejected certificate")
		}
		after, err := r.wal.Read()
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("restore changed WAL evidence: %v", err)
		}
	})
	before, err := c.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CompactThrough(64, seal.RootHash); err == nil || !strings.Contains(err.Error(), "incompatible leader schedule") {
		t.Fatalf("compaction accepted rejected suffix: %v", err)
	}
	if c.CompactionFloor() != 0 {
		t.Fatal("failed compaction changed floor")
	}
	if c.Tip() != 96 {
		t.Fatalf("compaction advanced rejected schedule: tip=%d", c.Tip())
	}
	got, ok := c.CertifiedValue(97)
	if !ok || !bytes.Equal(got.Value, value) {
		t.Fatal("compaction lost rejected certificate")
	}
	after, err := c.wal.Read()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("compaction changed WAL evidence: %v", err)
	}
}
