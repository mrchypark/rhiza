package sqlpolicy

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3/driver"
)

// The marker update is deliberately WAL-only while its actual writer remains
// open. A live provenance check must not inspect the stale main DB in isolation.
// Marker Q alone is not a claim that this fixture is a complete profileQ DB.
func TestLivePolicyCheckReadsCommittedWALWithoutChangingSource(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "live.db")
	writer, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	writer.SetMaxOpenConns(1)
	var mode string
	if err := writer.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("actual WAL fixture unavailable: mode=%q error=%v", mode, err)
	}
	if _, err := writer.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO _rhiza_meta VALUES('sql_execution_policy','7')"); err != nil {
		t.Fatal(err)
	}
	var busy, frames, checkpointed int
	if err := writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &frames, &checkpointed); err != nil || busy != 0 {
		t.Fatalf("actual P marker checkpoint failed: busy=%d error=%v", busy, err)
	}
	mainP, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE _rhiza_meta SET value='10' WHERE key='sql_execution_policy'"); err != nil {
		t.Fatal(err)
	}
	mainBefore, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(mainBefore, mainP) {
		t.Fatal("Q was not isolated to the actual WAL", err)
	}
	walBefore, err := os.ReadFile(path + "-wal")
	if err != nil || len(walBefore) == 0 {
		t.Fatal("actual committed WAL is absent", err)
	}
	if err := CheckFileForPolicy(ctx, path, LegacyMaterializationPolicy); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("live checker accepted stale main-only markerP: %v", err)
	}
	if err := CheckFileForPolicy(ctx, path, ManagedGraphRequestMaterializationPolicy); err != nil {
		t.Fatalf("live checker did not observe committed WAL markerQ: %v", err)
	}
	mainAfter, mainErr := os.ReadFile(path)
	walAfter, walErr := os.ReadFile(path + "-wal")
	if mainErr != nil || walErr != nil || !bytes.Equal(mainBefore, mainAfter) || !bytes.Equal(walBefore, walAfter) {
		t.Fatalf("live checks changed main/existing WAL bytes: main=%v WAL=%v", mainErr, walErr)
	}
	// Only after capturing the source-preservation evidence may its owner close
	// and perform SQLite's normal final checkpoint/sidecar lifecycle.
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
