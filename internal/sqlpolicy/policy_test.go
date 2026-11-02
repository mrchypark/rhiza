package sqlpolicy

import (
	"bytes"
	"context"
	"errors"
	"github.com/ncruces/go-sqlite3/driver"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestMaterializationPolicyExactExpected(t *testing.T) {
	if Version != 7 || Marker() != "7" || PeerALPN != "rhiza-peer-v7" {
		t.Fatal("legacy SQL semantics changed")
	}
	ctx := context.Background()
	for _, stored := range []string{"7", "8", "08", "9", ""} {
		t.Run("stored-"+stored, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			db, err := driver.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO _rhiza_meta VALUES('sql_execution_policy',?)`, stored); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range []MaterializationPolicy{LegacyMaterializationPolicy, ManagedSQLKVMaterializationPolicy} {
				wantOK := stored == strconv.Itoa(int(expected))
				for _, check := range []func(context.Context, string, MaterializationPolicy) error{CheckFileForPolicy, CheckSnapshotFileForPolicy, CheckExistingForPolicy} {
					err := check(ctx, path, expected)
					if (err == nil) != wantOK || (err != nil && !errors.Is(err, ErrIncompatible)) {
						t.Fatalf("stored=%q expected=%d: %v", stored, expected, err)
					}
				}
			}
			if err := CheckFile(ctx, path); (err == nil) != (stored == "7") {
				t.Fatalf("legacy facade accepted %q: %v", stored, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("policy check changed source")
			}
		})
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	for _, bad := range []MaterializationPolicy{0, 6, 9, -1} {
		if err := CheckExistingForPolicy(ctx, missing, bad); !errors.Is(err, ErrIncompatible) {
			t.Fatalf("unknown expected policy accepted: %d %v", bad, err)
		}
	}
	if err := CheckExistingForPolicy(ctx, missing, ManagedSQLKVMaterializationPolicy); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFileRelativePath(t *testing.T) {
	dir, err := os.MkdirTemp(".", "policy-path-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path, err := filepath.Abs(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + Marker() + `')`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, path)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckExisting(context.Background(), relative); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFileRejectsPreviousExecutionPolicyWithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := strconv.Itoa(Version - 1)
	if _, err := db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + previous + `')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := CheckExisting(context.Background(), path); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("previous policy error=%v", err)
	}
}
