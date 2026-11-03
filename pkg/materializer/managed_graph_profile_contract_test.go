package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

type managedGraphContractFile struct {
	mode fs.FileMode
	size int64
	hash [32]byte
	link string
}

func managedGraphContractInventory(t *testing.T, root string) map[string]managedGraphContractFile {
	t.Helper()
	out := make(map[string]managedGraphContractFile)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		identity := managedGraphContractFile{mode: info.Mode()}
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			hash := sha256.New()
			size, readErr := io.Copy(hash, file)
			closeErr := file.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			identity.size = size
			copy(identity.hash[:], hash.Sum(nil))
		} else if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			identity.link = link
		}
		out[relative] = identity
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// These sizes are pinned-fixture observations, not general SQLite limits.
// Only the exact source DB's absent readonly sidecars may be newly created.
func managedGraphContractReadOnlyChange(before, after map[string]managedGraphContractFile, db string) error {
	for path, original := range before {
		current, exists := after[path]
		if !exists {
			return fmt.Errorf("original path disappeared: %s", path)
		}
		if path == db+"-shm" && original.mode.IsRegular() && current.mode == original.mode && current.size == original.size {
			continue // SHM bytes are reader coordination, not application bytes.
		}
		if current != original {
			return fmt.Errorf("original state changed: %s", path)
		}
	}
	for path, current := range after {
		if _, existed := before[path]; existed {
			continue
		}
		if current.mode.IsRegular() && (path == db+"-wal" && current.size == 0 || path == db+"-shm" && current.size == 32768) {
			continue
		}
		return fmt.Errorf("unexpected new state: %s", path)
	}
	return nil
}

// The selected policy is explicit, and both physical stores carry its marker.
// Wrong-profile refusal must precede a writable open of either original store.
func TestManagedGraphMaterializationMarker(t *testing.T) {
	ctx := context.Background()
	t.Run("readonly_sidecar_oracle", func(t *testing.T) {
		base := map[string]managedGraphContractFile{
			"sqlite.db":                       {mode: 0600, size: 4096, hash: [32]byte{1}},
			"latticedb/graph.ltdb/state.json": {mode: 0600, size: 4096, hash: [32]byte{2}},
		}
		for _, tc := range []struct {
			name   string
			change func(map[string]managedGraphContractFile)
			allow  bool
		}{
			{"exact_readonly_sidecars", func(after map[string]managedGraphContractFile) {
				after["sqlite.db-wal"] = managedGraphContractFile{mode: 0600}
				after["sqlite.db-shm"] = managedGraphContractFile{mode: 0600, size: 32768}
			}, true},
			{"nonempty_new_WAL", func(after map[string]managedGraphContractFile) {
				after["sqlite.db-wal"] = managedGraphContractFile{mode: 0600, size: 1}
			}, false},
			{"new_WAL_symlink", func(after map[string]managedGraphContractFile) {
				after["sqlite.db-wal"] = managedGraphContractFile{mode: os.ModeSymlink, link: "foreign"}
			}, false},
			{"wrong_SHM_size", func(after map[string]managedGraphContractFile) {
				after["sqlite.db-shm"] = managedGraphContractFile{mode: 0600, size: 32767}
			}, false},
			{"other_DB_sidecar", func(after map[string]managedGraphContractFile) {
				after["other.db-wal"] = managedGraphContractFile{mode: 0600}
			}, false},
			{"backup", func(after map[string]managedGraphContractFile) { after["sqlite.db.restore-backup"] = base["sqlite.db"] }, false},
			{"journal", func(after map[string]managedGraphContractFile) {
				after["sqlite.db-journal"] = managedGraphContractFile{mode: 0600}
			}, false},
			{"qlog", func(after map[string]managedGraphContractFile) {
				after["qlog/new"] = managedGraphContractFile{mode: 0600}
			}, false},
			{"main_bytes", func(after map[string]managedGraphContractFile) {
				file := after["sqlite.db"]
				file.hash[0]++
				after["sqlite.db"] = file
			}, false},
			{"native_bytes", func(after map[string]managedGraphContractFile) {
				file := after["latticedb/graph.ltdb/state.json"]
				file.hash[0]++
				after["latticedb/graph.ltdb/state.json"] = file
			}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				after := maps.Clone(base)
				tc.change(after)
				if err := managedGraphContractReadOnlyChange(base, after, "sqlite.db"); (err == nil) != tc.allow {
					t.Fatalf("readonly exception classification: allow=%t error=%v", tc.allow, err)
				}
			})
		}
		existing := maps.Clone(base)
		existing["sqlite.db-wal"] = managedGraphContractFile{mode: 0600, size: 123, hash: [32]byte{3}}
		existing["sqlite.db-shm"] = managedGraphContractFile{mode: 0600, size: 32768, hash: [32]byte{4}}
		after := maps.Clone(existing)
		file := after["sqlite.db-shm"]
		file.hash[0]++
		after["sqlite.db-shm"] = file
		if err := managedGraphContractReadOnlyChange(existing, after, "sqlite.db"); err != nil {
			t.Fatal("existing SHM reader-coordination change rejected", err)
		}
		file = after["sqlite.db-wal"]
		file.hash[0]++
		after["sqlite.db-wal"] = file
		if err := managedGraphContractReadOnlyChange(existing, after, "sqlite.db"); err == nil {
			t.Fatal("existing committed WAL bytes were exempted")
		}
		delete(after, "sqlite.db-wal")
		if err := managedGraphContractReadOnlyChange(existing, after, "sqlite.db"); err == nil {
			t.Fatal("existing WAL disappearance was exempted")
		}
	})
	open := map[int]func(string) (*Materializer, error){
		7: func(path string) (*Materializer, error) { return Open(path, 1, 1024) },
		8: func(path string) (*Materializer, error) { return OpenManagedSQLKV(path, 1, 1024) },
		9: func(path string) (*Materializer, error) { return OpenManagedSQLKVWithOriginals(path, 1, 1024) },
		10: func(path string) (*Materializer, error) {
			return OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
		},
	}
	t.Run("actual_pair_reopens", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sqlite.db")
		original := types.GraphCommand{RequestID: "\xff", Cypher: "CREATE (:MarkerItem {n: 10})"}
		value, err := types.EncodeManagedGraphExecution(original)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			m, err := open[10](path)
			if err != nil {
				t.Fatal(err)
			}
			func() {
				defer func() {
					if err := m.Close(); err != nil {
						t.Error(err)
					}
				}()
				if m.MaterializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || m.managedProtocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || m.graph == nil {
					t.Fatal("explicit Graph constructor selected a different physical profile")
				}
				var policy, protocol string
				if err := m.writer.QueryRowContext(ctx, "SELECT value FROM _rhiza_meta WHERE key='sql_execution_policy'").Scan(&policy); err != nil || policy != "10" {
					t.Fatalf("SQLite policy marker=%q error=%v", policy, err)
				}
				if err := m.writer.QueryRowContext(ctx, "SELECT value FROM _rhiza_meta WHERE key='managed_application_protocol'").Scan(&protocol); err != nil || protocol != sqlpolicy.ManagedGraphRequestApplicationProtocol {
					t.Fatalf("SQLite protocol marker=%q error=%v", protocol, err)
				}
				marker, err := m.graph.getMetadata(graphFormatKey)
				if err != nil || !bytes.Equal(marker, graphOriginalStoragePolicy) {
					t.Fatalf("native policy marker mismatch: %v", err)
				}
				if attempt == 0 {
					if err := m.Apply(ctx, 1, value); err != nil {
						t.Fatal(err)
					}
				}
				rows, err := m.GraphQuery(ctx, "MATCH (n:MarkerItem) RETURN n.n", nil)
				if err != nil || len(rows.Rows) != 1 {
					t.Fatalf("actual/reopened native row missing: rows=%+v error=%v", rows, err)
				}
				receipt, found, err := m.GraphMutationReceipt(ctx, original.RequestID)
				if err != nil || !found || receipt.Slot != 1 || receipt.Status != types.MutationCommitted {
					t.Fatalf("actual/reopened raw receipt mismatch: found=%t receipt=%+v error=%v", found, receipt, err)
				}
			}()
		}
	})
	for _, pair := range [][2]int{{7, 10}, {8, 10}, {9, 10}, {10, 7}, {10, 8}, {10, 9}} {
		t.Run(fmt.Sprintf("refuse_%d_as_%d", pair[0], pair[1]), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sqlite.db")
			m, err := open[pair[0]](path)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			before := managedGraphContractInventory(t, dir)
			wrong, err := open[pair[1]](path)
			if wrong != nil {
				_ = wrong.Close()
			}
			if err == nil || wrong != nil {
				t.Fatal("wrong profile reached an opened materializer")
			}
			// The constructors can reject forbidden Graph artifacts even before
			// marker inspection. Independently classify the stored marker using
			// the actual live checker, without claiming writer-call count proof.
			if err := sqlpolicy.CheckFileForPolicy(ctx, path, sqlpolicy.MaterializationPolicy(pair[1])); !errors.Is(err, sqlpolicy.ErrIncompatible) {
				t.Fatalf("actual wrong marker was not incompatible: %v", err)
			}
			if err := managedGraphContractReadOnlyChange(before, managedGraphContractInventory(t, dir), "sqlite.db"); err != nil {
				t.Fatalf("wrong-profile refusal changed application state: %v", err)
			}
		})
	}
	if !t.Failed() {
		fmt.Println("RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10")
	}
}
