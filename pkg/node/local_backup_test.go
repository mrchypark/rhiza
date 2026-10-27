package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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

func TestLocalBackupHeldMaintenanceEqualTipReplaysBeforeRelease(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{
		Local: true, LocalArchiveProfile: types.LocalArchiveProfileFilesystem,
		NodeID: "local", ClusterID: "held-equal-tip", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(),
		ObjStoreDurability:   types.ObjectStoreDurabilityBeforeAck,
		ObjStoreSyncInterval: time.Hour, ObjStoreGCInterval: time.Hour, CheckpointInterval: time.Hour,
	}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	for _, request := range []network.ExecuteRequest{
		{RequestID: "schema", SQL: "CREATE TABLE equal_tip (id INTEGER PRIMARY KEY)"},
		{RequestID: "row", SQL: "INSERT INTO equal_tip VALUES (7)"},
	} {
		if _, err := n.server.Execute(ctx, request); err != nil {
			_ = n.Shutdown()
			t.Fatal(err)
		}
	}
	prefix := n.localBackupPrefix()
	if n.localBackup.Tip() != n.core.Tip() {
		_ = n.Shutdown()
		t.Fatal("fixture archive and local WAL tips differ")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}

	controlPath := filepath.Join(config.ObjStoreDir, filepath.FromSlash(prefix), "local-gc-control")
	data, err := os.ReadFile(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	var control map[string]any
	if err := json.Unmarshal(data, &control); err != nil {
		t.Fatal(err)
	}
	control["token"] = "interrupted-maintenance"
	data, err = json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(controlPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("replay equal-tip archive before clearing held token: %v", err)
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM equal_tip"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(7) {
		_ = n.Shutdown()
		t.Fatalf("restored equal-tip rows=%v err=%v", rows.Rows, err)
	}
	resolved, err := n.localBackup.MaintenanceResolved(ctx)
	if err != nil || !resolved {
		_ = n.Shutdown()
		t.Fatalf("maintenance control resolved=%v err=%v", resolved, err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestFilesystemV2ArchiveNamespaceContainment(t *testing.T) {
	invalid := []struct {
		name    string
		prefix  func(string) string
		cluster func(string) string
		outside func(string) string
	}{
		{
			name:   "prefix traversal",
			prefix: func(string) string { return "../outside" }, cluster: func(string) string { return "cluster" },
			outside: func(base string) string { return filepath.Join(base, "outside", "cluster", "local", "v1") },
		},
		{
			name:   "cluster traversal",
			prefix: func(string) string { return "nested" }, cluster: func(string) string { return "../../outside" },
			outside: func(base string) string { return filepath.Join(base, "outside", "local", "v1") },
		},
		{
			name:   "absolute prefix",
			prefix: func(base string) string { return filepath.Join(base, "absolute-prefix") }, cluster: func(string) string { return "cluster" },
			outside: func(base string) string { return filepath.Join(base, "absolute-prefix", "cluster", "local", "v1") },
		},
		{
			name:   "absolute cluster ID",
			prefix: func(string) string { return "nested" }, cluster: func(base string) string { return filepath.Join(base, "absolute-cluster") },
			outside: func(base string) string { return filepath.Join(base, "absolute-cluster", "local", "v1") },
		},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			dataDir := filepath.Join(base, "data")
			root := filepath.Join(base, "objects")
			if err := os.Mkdir(dataDir, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(test.outside(base), "sentinel")
			if err := os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
				t.Fatal(err)
			}
			const sentinel = "preserve-outside-root"
			if err := os.WriteFile(marker, []byte(sentinel), 0600); err != nil {
				t.Fatal(err)
			}
			config := types.ExecutionConfig{
				Local: true, LocalArchiveProfile: types.LocalArchiveProfileFilesystem,
				NodeID: "local", ClusterID: types.ClusterID(test.cluster(base)), DataDir: dataDir,
				ObjStoreProvider: "filesystem", ObjStoreDir: root,
				ObjStorePrefix: test.prefix(base), ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
			}
			n := New(&config)
			if err := n.openLocalBackupBucket(context.Background()); err == nil {
				if n.localOwner != nil {
					_ = n.localOwner.Close(time.Second)
				}
				t.Fatal("escaping archive namespace was accepted")
			}
			if n.localOwner != nil {
				t.Fatal("rejected namespace acquired an owner")
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("archive root was created before rejection: %v", err)
			}
			got, err := os.ReadFile(marker)
			if err != nil || string(got) != sentinel {
				t.Fatalf("outside-root sentinel=%q err=%v", got, err)
			}
		})
	}

	t.Run("symlink escape under root", func(t *testing.T) {
		base := t.TempDir()
		dataDir, root := filepath.Join(base, "data"), filepath.Join(base, "objects")
		outside := filepath.Join(base, "outside")
		if err := os.Mkdir(dataDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, "organization"), 0700); err != nil {
			t.Fatal(err)
		}
		outsideArchive := filepath.Join(outside, "team", "cluster", "local", "v1")
		if err := os.MkdirAll(outsideArchive, 0700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(outsideArchive, "sentinel")
		const sentinel = "preserve-symlink-target"
		if err := os.WriteFile(marker, []byte(sentinel), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(root, "organization")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "organization")); err != nil {
			t.Fatal(err)
		}
		config := types.ExecutionConfig{
			Local: true, LocalArchiveProfile: types.LocalArchiveProfileFilesystem,
			NodeID: "local", ClusterID: "cluster", DataDir: dataDir,
			ObjStoreProvider: "filesystem", ObjStoreDir: root,
			ObjStorePrefix: "organization/team", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
		}
		n := New(&config)
		if err := n.openLocalBackupBucket(context.Background()); err == nil {
			t.Fatal("namespace symlink escape was accepted")
		}
		if n.localOwner != nil {
			t.Fatal("symlink escape acquired an owner")
		}
		rootReal, err := filepath.EvalSymlinks(root)
		if err != nil {
			t.Fatal(err)
		}
		ownerHash := sha256.Sum256([]byte(rootReal))
		anchor := filepath.Join(filepath.Dir(rootReal), fmt.Sprintf(".rhiza-local-owner-%x", ownerHash[:8]))
		if _, err := os.Stat(anchor); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owner anchor created before symlink rejection: %v", err)
		}
		got, err := os.ReadFile(marker)
		if err != nil || string(got) != sentinel {
			t.Fatalf("symlink target sentinel=%q err=%v", got, err)
		}
	})

	t.Run("valid nested prefix", func(t *testing.T) {
		base := t.TempDir()
		dataDir, root := filepath.Join(base, "data"), filepath.Join(base, "objects")
		if err := os.Mkdir(dataDir, 0700); err != nil {
			t.Fatal(err)
		}
		config := types.ExecutionConfig{
			Local: true, LocalArchiveProfile: types.LocalArchiveProfileFilesystem,
			NodeID: "local", ClusterID: "cluster", DataDir: dataDir,
			ObjStoreProvider: "filesystem", ObjStoreDir: root,
			ObjStorePrefix: "organization/team", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
		}
		n := New(&config)
		if err := n.openLocalBackupBucket(context.Background()); err != nil {
			t.Fatalf("valid nested prefix rejected: %v", err)
		}
		if n.localBackupPrefix() != "organization/team/cluster/local/v1" {
			t.Fatalf("archive prefix = %q", n.localBackupPrefix())
		}
		if err := n.bucket.Close(); err != nil {
			t.Fatal(err)
		}
		if err := n.localOwner.Close(time.Second); err != nil {
			t.Fatal(err)
		}
	})
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
