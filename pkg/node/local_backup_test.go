package node

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestLocalBackupWholeLossCheckpointTailAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	remote := t.TempDir()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: root, ObjStoreProvider: "filesystem", ObjStoreDir: remote, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	if config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		t.Fatal("Local backup default is not before-ack")
	}
	execute := func(id, sql string) {
		t.Helper()
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: id, SQL: sql}); err != nil {
			t.Fatal(err)
		}
	}
	execute("schema", "CREATE TABLE backup_rows (id INTEGER PRIMARY KEY)")
	execute("first", "INSERT INTO backup_rows VALUES (1)")
	if _, err := n.server.GraphExecute(ctx, types.GraphCommand{RequestID: "graph", Cypher: "CREATE (:BackupRow {id: 1})", Events: []types.GraphStreamEvent{{Stream: "backup-events", Kind: "created", Payload: "one"}}}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	base := n.localBackup.Snapshot().Seal.Index
	if _, ok := n.localBackup.PrefixHash(1); base > 1 && ok {
		t.Fatal("snapshot retained pre-base archive proof")
	}
	for _, value := range n.localBackup.DecisionsFrom(1, 256) {
		if value.Slot <= base {
			t.Fatal("snapshot retained pre-base payload")
		}
	}
	execute("tail", "INSERT INTO backup_rows VALUES (2)")
	if n.localBackup.Tip() != n.core.Tip() {
		t.Fatal("acknowledged tail is not remotely durable before shutdown")
	}
	uuid := n.localStore.policy.StoreUUID
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("whole loss restore: %v", err)
	}
	if n.localStore.policy.StoreUUID != uuid {
		t.Fatal("restore replaced store UUID")
	}
	result, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM backup_rows ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("restored rows=%v", result.Rows)
	}
	graph, err := n.server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:BackupRow) RETURN n.id"})
	if err != nil || len(graph.Rows) != 1 {
		t.Fatalf("restored graph=%v err=%v", graph, err)
	}
	stream, err := n.server.GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "backup-events", Limit: 10})
	if err != nil || len(stream.Records) != 1 {
		t.Fatalf("restored stream=%v err=%v", stream, err)
	}
	execute("tail", "INSERT INTO backup_rows VALUES (2)")
	execute("postrestore", "INSERT INTO backup_rows VALUES (3)")
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("second restart: %v", err)
	}
	result, err = n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM backup_rows ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("second restart rows=%v", result.Rows)
	}
}

func TestLocalBackupDivergencePreservesWAL(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE diverge (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	branch := t.TempDir()
	copyLocalBackupTestDir(t, config.DataDir, branch)
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "remote", SQL: "INSERT INTO diverge VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	localConfig := config
	localConfig.DataDir = branch
	localConfig.ObjStoreProvider, localConfig.ObjStoreDir, localConfig.ObjStoreDurability = "", "", ""
	n = New(&localConfig)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "local", SQL: "INSERT INTO diverge VALUES (2)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	before := make(map[string][32]byte)
	err := filepath.WalkDir(filepath.Join(branch, "qlog"), func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "lock.qlog" {
			return nil
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		before[name] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config.DataDir = branch
	n = New(&config)
	if err := n.Open(ctx); err == nil {
		_ = n.Shutdown()
		t.Fatal("divergent archive accepted")
	}
	for name, want := range before {
		data, err := os.ReadFile(name)
		if err != nil || sha256.Sum256(data) != want {
			t.Fatalf("divergence changed WAL %s: %v", name, err)
		}
	}
}

func TestLocalBackupMissingBoundRemoteFails(t *testing.T) {
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
	n := New(&config)
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(config.ObjStoreDir, "local/v1/BINDING")); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(context.Background()); err == nil {
		_ = n.Shutdown()
		t.Fatal("missing bound remote was bootstrapped")
	}
}

func copyLocalBackupTestDir(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, name)
		if err != nil {
			return err
		}
		out := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(out, 0700)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalBackupReconcilesRemoteAheadAndLocalExtension(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	execute := func(id, sql string) {
		t.Helper()
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: id, SQL: sql}); err != nil {
			t.Fatal(err)
		}
	}
	execute("schema", "CREATE TABLE reconcile (id INTEGER PRIMARY KEY)")
	execute("one", "INSERT INTO reconcile VALUES (1)")
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	stale := t.TempDir()
	copyLocalBackupTestDir(t, config.DataDir, stale)
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	execute("two", "INSERT INTO reconcile VALUES (2)")
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	config.DataDir = stale
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("remote ahead: %v", err)
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM reconcile"})
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("remote ahead rows=%v err=%v", rows, err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	provider, remote := config.ObjStoreProvider, config.ObjStoreDir
	config.ObjStoreProvider, config.ObjStoreDir, config.ObjStoreDurability = "", "", ""
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	execute("three", "INSERT INTO reconcile VALUES (3)")
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	config.ObjStoreProvider, config.ObjStoreDir, config.ObjStoreDurability = provider, remote, ""
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("local extension: %v", err)
	}
	if n.localBackup.Snapshot() == nil {
		t.Fatal("local extension did not publish fresh snapshot")
	}
}

func TestLocalBackupFirstAttachCompactedStoreAndRestore(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE attach (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	config.ObjStoreProvider, config.ObjStoreDir = "filesystem", t.TempDir()
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	if err := os.RemoveAll(config.DataDir); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("compacted archive restore: %v", err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "after", SQL: "INSERT INTO attach VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
}

func TestLocalBackupRemoteAheadPreservesRestoreIntent(t *testing.T) {
	for _, checkpointed := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpointed-%v", checkpointed), func(t *testing.T) {
			config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
			ctx := context.Background()
			n := New(&config)
			if err := n.Open(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE pending_restore (id INTEGER)"}); err != nil {
				t.Fatal(err)
			}
			if checkpointed {
				if err := n.reclaimLocalCheckpoint(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := n.Shutdown(); err != nil {
				t.Fatal(err)
			}
			stale := t.TempDir()
			copyLocalBackupTestDir(t, config.DataDir, stale)
			n = New(&config)
			if err := n.Open(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "ahead", SQL: "INSERT INTO pending_restore VALUES (1)"}); err != nil {
				t.Fatal(err)
			}
			remoteTip := n.localBackup.Tip()
			if err := n.Shutdown(); err != nil {
				t.Fatal(err)
			}
			config.DataDir = stale
			if err := os.WriteFile(filepath.Join(stale, "sqlite.db.local-restore.json"), []byte("malformed existing restore intent"), 0600); err != nil {
				t.Fatal(err)
			}
			image := func() map[string]string {
				t.Helper()
				result := map[string]string{}
				err := filepath.WalkDir(config.DataDir, func(name string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					rel, err := filepath.Rel(config.DataDir, name)
					if err != nil {
						return err
					}
					if rel == "qlog/lock.qlog" {
						return nil
					}
					if entry.IsDir() {
						result[rel+"/"] = ""
						return nil
					}
					data, err := os.ReadFile(name)
					if err != nil {
						return err
					}
					result[rel] = string(data)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			before := image()
			n = New(&config)
			if err := n.Open(context.Background()); err == nil {
				_ = n.Shutdown()
				t.Fatal("remote-ahead replacement bypassed restore intent")
			}
			after := image()
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("remote-ahead startup changed original authority before validating restore intent (remote tip %d)", remoteTip)
			}
			if err := os.Remove(filepath.Join(stale, "sqlite.db.local-restore.json")); err != nil {
				t.Fatal(err)
			}
			n = New(&config)
			if err := n.Open(ctx); err != nil {
				t.Fatalf("remote-ahead retry after journal repair: %v", err)
			}
			rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM pending_restore"})
			closeErr := n.Shutdown()
			if err != nil || closeErr != nil || len(rows.Rows) != 1 {
				t.Fatalf("repaired restore rows=%v query=%v close=%v", rows, err, closeErr)
			}
		})
	}
}
