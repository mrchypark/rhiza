package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func localBackupStagingFixture(t *testing.T) (types.ExecutionConfig, *recovery.LocalArchive) {
	t.Helper()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "staging", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	seed := New(&config)
	if err := seed.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seed.Shutdown() })
	for _, req := range []network.ExecuteRequest{{RequestID: "schema", SQL: "CREATE TABLE staging_rows (id INTEGER)"}, {RequestID: "seed", SQL: "INSERT INTO staging_rows VALUES (1)"}} {
		if _, err := seed.server.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.reclaimLocalCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.server.Execute(context.Background(), network.ExecuteRequest{RequestID: "tail", SQL: "INSERT INTO staging_rows VALUES (2)"}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Shutdown(); err != nil {
		t.Fatal(err)
	}
	config.DataDir = t.TempDir()
	reader := New(&config)
	if err := reader.openLocalBackupBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.bucket.Close() })
	archive, err := recovery.OpenLocalArchive(context.Background(), reader.bucket, reader.localBackupPrefix())
	if err != nil {
		t.Fatal(err)
	}
	return config, archive
}

func stagingImage(t *testing.T, root string) map[string]string {
	t.Helper()
	image := map[string]string{}
	if err := filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			image[relative+"/"] = ""
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(name)
			image[relative] = "symlink:" + target
			return err
		}
		data, err := os.ReadFile(name)
		image[relative] = string(data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return image
}

func writeStageFile(t *testing.T, name, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func materializeStageOwner(t *testing.T, n *Node, binding recovery.LocalBinding) {
	t.Helper()
	owner, err := n.localBackupStageOwnership(binding)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicLocalFile(n.config.DataDir, n.localBackupStageOwnerPath(), data); err != nil {
		t.Fatal(err)
	}
}

func TestLocalBackupStagingPreinstallCrashPhases(t *testing.T) {
	base, archive := localBackupStagingFixture(t)
	for _, phase := range []string{"intent", "journal", "next", "policy", "wal", "checkpoint", "install_temp"} {
		t.Run(phase, func(t *testing.T) {
			config := base
			config.DataDir = t.TempDir()
			n := New(&config)
			if err := n.openLocalBackupBucket(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer n.bucket.Close()
			original := filepath.Join(config.DataDir, "sqlite.db")
			writeStageFile(t, original, "original authority")
			for retry := 0; retry < 2; retry++ {
				materializeStageOwner(t, n, archive.Binding)
				journal := filepath.Join(config.DataDir, ".local-backup-import")
				if phase != "intent" {
					if err := os.Mkdir(journal, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if phase != "intent" && phase != "journal" {
					if err := os.Mkdir(filepath.Join(journal, "next"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				switch phase {
				case "policy":
					writeStageFile(t, filepath.Join(journal, "next/local-store.json"), "partial policy")
				case "wal":
					writeStageFile(t, filepath.Join(journal, "next/qlog/seg_1.log"), "partial WAL")
				case "checkpoint":
					writeStageFile(t, filepath.Join(journal, "next/local-checkpoints/partial/sqlite.db"), "partial native checkpoint")
				case "install_temp":
					writeStageFile(t, filepath.Join(journal, ".local-policy-crashed"), "partial install JSON")
				}
				if err := n.finishLocalBackupImport(); err != nil {
					t.Fatal(err)
				}
				if err := n.finishLocalBackupImport(); err != nil {
					t.Fatalf("repeated retry: %v", err)
				}
				for _, name := range []string{journal, n.localBackupStageOwnerPath()} {
					if _, err := os.Lstat(name); !os.IsNotExist(err) {
						t.Fatalf("owned pre-install evidence remains: %s: %v", name, err)
					}
				}
				data, err := os.ReadFile(original)
				if err != nil || string(data) != "original authority" {
					t.Fatal("pre-install retry changed original authority")
				}
			}
		})
	}
}

func TestLocalBackupStagingForeignEvidenceUnchanged(t *testing.T) {
	base, archive := localBackupStagingFixture(t)
	for _, mode := range []string{"unknown", "foreign_node", "foreign_path", "foreign_binding", "malformed", "previous", "unexpected", "symlink", "malformed_install"} {
		t.Run(mode, func(t *testing.T) {
			config := base
			config.DataDir = t.TempDir()
			n := New(&config)
			if err := n.openLocalBackupBucket(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer n.bucket.Close()
			journal := filepath.Join(config.DataDir, ".local-backup-import")
			writeStageFile(t, filepath.Join(config.DataDir, "sqlite.db"), "original authority")
			if mode == "symlink" {
				foreign := t.TempDir()
				writeStageFile(t, filepath.Join(foreign, "next/payload"), "foreign")
				if err := os.Symlink(foreign, journal); err != nil {
					t.Fatal(err)
				}
			} else {
				writeStageFile(t, filepath.Join(journal, "next/payload"), "unfinished")
			}
			if mode != "unknown" && mode != "malformed_install" {
				materializeStageOwner(t, n, archive.Binding)
				var owner localBackupStageOwner
				data, err := os.ReadFile(n.localBackupStageOwnerPath())
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &owner); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "foreign_node":
					owner.Node = "foreign"
				case "foreign_path":
					owner.DataDir = t.TempDir()
				case "foreign_binding":
					owner.Binding.StoreUUID = "11111111111111111111111111111111"
				case "previous":
					writeStageFile(t, filepath.Join(journal, "previous/sqlite.db"), "original saved authority")
				case "unexpected":
					writeStageFile(t, filepath.Join(journal, "foreign"), "unknown")
				}
				encoded, err := json.Marshal(owner)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "malformed" {
					encoded = []byte("{")
				}
				if err := os.WriteFile(n.localBackupStageOwnerPath(), encoded, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "malformed_install" {
				writeStageFile(t, filepath.Join(journal, "install.json"), `{`)
			}
			before := stagingImage(t, config.DataDir)
			if err := n.finishLocalBackupImport(); err == nil {
				t.Fatal("unknown or foreign evidence was accepted")
			}
			if !reflect.DeepEqual(before, stagingImage(t, config.DataDir)) {
				t.Fatal("refusal modified evidence")
			}
		})
	}
}

func TestLocalBackupStagingCommittedRetryAndOpen(t *testing.T) {
	config, archive := localBackupStagingFixture(t)
	n := New(&config)
	if err := n.openLocalBackupBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n.bucket.Close()
	// The crash happened after a complete install.json publication and after the
	// original file move, but before installing its staged replacement.
	if err := n.beginLocalBackupStage(archive.Binding); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(config.DataDir, ".local-backup-import")
	writeStageFile(t, filepath.Join(config.DataDir, "sqlite.db"), "old authority")
	writeStageFile(t, filepath.Join(journal, "next/sqlite.db"), "new authority")
	if err := n.writeLocalBackupInstallJournal(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(journal, "previous"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(config.DataDir, "sqlite.db"), filepath.Join(journal, "previous/sqlite.db")); err != nil {
		t.Fatal(err)
	}
	for retry := 0; retry < 2; retry++ {
		if err := n.finishLocalBackupImport(); err != nil {
			t.Fatal(err)
		}
		original, err := os.ReadFile(filepath.Join(journal, "previous/sqlite.db"))
		if err != nil || string(original) != "old authority" {
			t.Fatal("committed retry lost original authority")
		}
		installed, err := os.ReadFile(filepath.Join(config.DataDir, "sqlite.db"))
		if err != nil || string(installed) != "new authority" {
			t.Fatal("committed retry lost replacement")
		}
	}
	// Committed journals from before ownership intents remain retryable.
	if err := os.Remove(n.localBackupStageOwnerPath()); err != nil {
		t.Fatal(err)
	}
	if err := n.finishLocalBackupImport(); err != nil {
		t.Fatalf("legacy committed retry: %v", err)
	}
	// Independent real import exercises pre-install retry followed by normal Open.
	config.DataDir = t.TempDir()
	n2 := New(&config)
	if err := n2.openLocalBackupBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n2.bucket.Close()
	if err := n2.beginLocalBackupStage(archive.Binding); err != nil {
		t.Fatal(err)
	}
	writeStageFile(t, filepath.Join(config.DataDir, ".local-backup-import/next/qlog/partial"), "interrupted download")
	reopened := New(&config)
	if err := reopened.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Parent integrates this call into Node.Open's existing success cleanup.
	if err := reopened.completeLocalBackupImport(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(reopened.localBackupStageOwnerPath()); !os.IsNotExist(err) {
		t.Fatalf("successful startup retained owner intent: %v", err)
	}
	rows, err := reopened.server.Query(context.Background(), network.QueryRequest{SQL: "SELECT id FROM staging_rows ORDER BY id"})
	closeErr := reopened.Shutdown()
	if err != nil || closeErr != nil || len(rows.Rows) != 2 {
		t.Fatalf("restored rows=%v query=%v close=%v", rows, err, closeErr)
	}
	// Both committed staging and ownership intent are gone after success.
	again := New(&config)
	if err := again.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := again.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
