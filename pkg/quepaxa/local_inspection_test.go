package quepaxa

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestOpenLocalReadOnlyInspectionRequiresReadOnlyLocalWAL(t *testing.T) {
	dir := t.TempDir()
	writable, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := writable.SetMaxBytes(1 << 20); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocalReadOnlyInspection(Config{
		NodeID: "local", Cluster: Cluster{Members: []Member{{ID: "local"}}}, WAL: writable, LocalMode: true,
	}); err == nil {
		t.Fatal("accepted writable WAL for read-only Local inspection")
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err := qlog.OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	if err := readonly.SetMaxBytes(1 << 20); err != nil {
		t.Fatal(err)
	}
	cluster := Cluster{Members: []Member{{ID: "local"}}}
	if _, err := OpenLocalReadOnlyInspection(Config{NodeID: "local", Cluster: cluster, WAL: readonly}); err == nil {
		t.Fatal("accepted read-only inspection without explicit LocalMode")
	}
	for name, config := range map[string]Config{
		"multiple members": {
			NodeID: "local", Cluster: Cluster{Members: []Member{{ID: "local"}, {ID: "other"}}}, WAL: readonly, LocalMode: true,
		},
		"reconfiguration": {
			NodeID: "local", Cluster: cluster, WAL: readonly, LocalMode: true, EnableReconfiguration: true,
		},
		"different local member": {
			NodeID: "local", Cluster: Cluster{Members: []Member{{ID: "other"}}}, WAL: readonly, LocalMode: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenLocalReadOnlyInspection(config); err == nil {
				t.Fatal("accepted configuration outside exact LocalMode inspection")
			}
		})
	}
	view, err := OpenLocalReadOnlyInspection(Config{NodeID: "local", Cluster: cluster, WAL: readonly, LocalMode: true})
	if err != nil {
		t.Fatalf("open Local read-only inspection: %v", err)
	}
	if view.Tip() != 0 || view.RecorderTip() != 0 {
		t.Fatalf("empty inspection tip/recorder tip = %d/%d", view.Tip(), view.RecorderTip())
	}
	if roots, err := view.LocalCheckpointRoots(context.Background()); err != nil || len(roots) != 0 {
		t.Fatalf("empty inspection roots=%v err=%v", roots, err)
	}
}

func TestLocalReadOnlyInspectionPreservesNormalAndCheckpointWALs(t *testing.T) {
	for _, state := range []string{"normal", "compacted base", "prepared root", "pending recorder"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			core, wal := localReserveCore(t, dir, "local")
			switch state {
			case "normal":
				if _, _, err := core.Propose(context.Background(), []byte("local read-only inspection")); err != nil {
					t.Fatal(err)
				}
			case "compacted base":
				if _, _, err := core.Propose(context.Background(), []byte("local checkpoint base")); err != nil {
					t.Fatal(err)
				}
				seal := testLocalSeal(t, core, 1)
				if _, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(context.Context, Slot) error { return nil }); err != nil {
					t.Fatal(err)
				}
			case "prepared root":
				if _, _, err := core.Propose(context.Background(), []byte("prepared local checkpoint")); err != nil {
					t.Fatal(err)
				}
				if err := core.PrepareCheckpoint(context.Background(), testLocalSeal(t, core, 1)); err != nil {
					t.Fatal(err)
				}
			case "pending recorder":
				proposal := newProposal(highestPriority, "local", []byte("pending local recorder"))
				if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, ConfigID: core.ConfigID(), Proposal: proposal}); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown Local WAL state %q", state)
			}
			wantTip, wantRecorderTip, wantFloor := core.Tip(), core.RecorderTip(), core.CompactionFloor()
			wantRecoveryFloor, wantRecoveryRoot, wantHasRecoveryRoot := core.RecoveryRoot()
			wantPrefix, wantPrefixOK := core.PrefixHash(wantTip)
			wantRoots, err := core.LocalCheckpointRoots(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			wantValue, wantValueOK := core.CertifiedValue(wantFloor + 1)
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			before := localInspectionFiles(t, dir)
			readonly, err := qlog.OpenReadOnly(dir)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = readonly.Close()
				}
			})
			if err := readonly.SetMaxBytes(1 << 20); err != nil {
				t.Fatal(err)
			}
			view, err := OpenLocalReadOnlyInspection(Config{NodeID: "local", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "local"}}}, WAL: readonly, LocalMode: true})
			if err != nil {
				t.Fatalf("open read-only Local inspection: %v", err)
			}
			if view.Tip() != wantTip || view.RecorderTip() != wantRecorderTip || view.CompactionFloor() != wantFloor {
				t.Fatalf("tip/recorder/floor=%d/%d/%d, want %d/%d/%d", view.Tip(), view.RecorderTip(), view.CompactionFloor(), wantTip, wantRecorderTip, wantFloor)
			}
			if gotFloor, gotRoot, gotBase := view.RecoveryRoot(); gotFloor != wantRecoveryFloor || gotRoot != wantRecoveryRoot || gotBase != wantHasRecoveryRoot {
				t.Fatalf("recovery root=(%d,%x,%v), want (%d,%x,%v)", gotFloor, gotRoot, gotBase, wantRecoveryFloor, wantRecoveryRoot, wantHasRecoveryRoot)
			}
			if got, ok := view.PrefixHash(wantTip); got != wantPrefix || ok != wantPrefixOK {
				t.Fatalf("tip prefix=(%x,%v), want (%x,%v)", got, ok, wantPrefix, wantPrefixOK)
			}
			gotValue, gotValueOK := view.CertifiedValue(wantFloor + 1)
			if gotValueOK != wantValueOK || gotValueOK && !reflect.DeepEqual(gotValue, wantValue) {
				t.Fatalf("certified value at %d differs from recovered Local core", wantFloor+1)
			}
			roots, err := view.LocalCheckpointRoots(context.Background())
			if err != nil || !slices.Equal(roots, wantRoots) {
				t.Fatalf("checkpoint roots=%v err=%v, want %v", roots, err, wantRoots)
			}
			if len(roots) > 0 {
				roots[0].Index++
				again, err := view.LocalCheckpointRoots(context.Background())
				if err != nil || !slices.Equal(again, wantRoots) {
					t.Fatalf("mutating returned roots changed inspection state: roots=%v err=%v", again, err)
				}
			}
			if err := readonly.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			if after := localInspectionFiles(t, dir); !reflect.DeepEqual(before, after) {
				t.Fatal("read-only Local inspection changed WAL files or identity bytes")
			}
		})
	}
}

func localInspectionFiles(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[relative] = bytes.Clone(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
