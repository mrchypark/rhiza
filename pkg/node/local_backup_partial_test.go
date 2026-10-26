package node

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/ncruces/go-sqlite3/driver"
)

func TestLocalBackupRemoteAheadPreservesPartialMaterializedEvidence(t *testing.T) {
	for _, kind := range []string{"ahead", "hash-conflict"} {
		t.Run(kind, func(t *testing.T) { testLocalBackupPartialMaterializedEvidence(t, kind) })
	}
}

func testLocalBackupPartialMaterializedEvidence(t *testing.T, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "partial", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreDurability: types.ObjectStoreDurabilityAsync, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	var n *Node
	t.Cleanup(func() {
		if n != nil {
			_ = n.Shutdown()
		}
	})
	open := func() {
		t.Helper()
		n = New(&config)
		if err := n.Open(ctx); err != nil {
			t.Fatal(err)
		}
	}
	closeNode := func() {
		t.Helper()
		if err := n.Shutdown(); err != nil {
			t.Fatal(err)
		}
		n = nil
	}
	execute := func(id, query string) {
		t.Helper()
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: id, SQL: query}); err != nil {
			t.Fatal(err)
		}
	}
	open()
	execute("schema", "CREATE TABLE partial_rows (id INTEGER)")
	execute("one", "INSERT INTO partial_rows VALUES (1)")
	execute("two", "INSERT INTO partial_rows VALUES (2)")
	if n.core.Tip() != 3 || n.material.Tip() != 3 {
		t.Fatal("expected original tip 3")
	}
	closeNode()
	sqliteAtThree, err := os.ReadFile(filepath.Join(config.DataDir, "sqlite.db"))
	if err != nil {
		t.Fatal(err)
	}
	oldWAL := t.TempDir()
	copyLocalBackupTestDir(t, filepath.Join(config.DataDir, "qlog"), oldWAL)
	open()
	execute("three", "INSERT INTO partial_rows VALUES (3)")
	if n.core.Tip() != 4 {
		t.Fatal("expected remote tip 4")
	}
	closeNode()
	remote := config.ObjStoreDir
	config.ObjStoreProvider, config.ObjStoreDir = "", ""
	open()
	execute("four", "INSERT INTO partial_rows VALUES (4)")
	if n.core.Tip() != 5 || n.material.Tip() != 5 {
		t.Fatal("expected surviving materialized tip 5")
	}
	closeNode()
	config.ObjStoreProvider, config.ObjStoreDir = "filesystem", remote
	// Simulate independent partial loss: retain SQLite 5, valid WAL 3, remote 4.
	if err := os.RemoveAll(filepath.Join(config.DataDir, "qlog")); err != nil {
		t.Fatal(err)
	}
	copyLocalBackupTestDir(t, oldWAL, filepath.Join(config.DataDir, "qlog"))
	wantRefusal := "materialized slot 5"
	if kind == "hash-conflict" {
		sqlitePath := filepath.Join(config.DataDir, "sqlite.db")
		if err := os.WriteFile(sqlitePath, sqliteAtThree, 0600); err != nil {
			t.Fatal(err)
		}
		sqlite, err := driver.Open((&url.URL{Scheme: "file", Path: filepath.ToSlash(sqlitePath)}).String())
		if err != nil {
			t.Fatal(err)
		}
		_, updateErr := sqlite.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_hash'`, strings.Repeat("ab", 32))
		closeErr := sqlite.Close()
		if updateErr != nil || closeErr != nil {
			t.Fatalf("inject conflicting materialized hash: update=%v close=%v", updateErr, closeErr)
		}
		wantRefusal = "materialized slot 3 hash conflicts"
	}
	before, remoteBefore := stagingImage(t, config.DataDir), stagingImage(t, config.ObjStoreDir)
	n = New(&config)
	err = n.Open(ctx)
	if err == nil {
		t.Fatal("remote replacement erased surviving materialized tip 5")
	}
	if !strings.Contains(err.Error(), wantRefusal) {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if after := stagingImage(t, config.DataDir); !reflect.DeepEqual(before, after) {
		t.Fatal("refusal changed original local evidence")
	}
	if after := stagingImage(t, config.ObjStoreDir); !reflect.DeepEqual(remoteBefore, after) {
		t.Fatal("refusal changed remote evidence")
	}
}
