package rhiza_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

// This exercises only the exported embedded API. The backup lives outside the
// disposable local root, so deleting DataDir cannot accidentally preserve a WAL.
func TestEmbeddedObjectStorageRepeatedTotalLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := embeddedStoreConfig(t)

	var db *rhiza.DB
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	open := func() {
		t.Helper()
		var err error
		db, err = rhiza.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if !db.Ready() {
			t.Fatal("open returned unready database")
		}
	}
	closeDB := func() {
		t.Helper()
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		db = nil
	}
	open()
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE backup_items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	request := rhiza.ExecuteRequest{RequestID: "original", SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{"original"}}
	original, err := db.ExecuteReturning(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(original.Statements) != 1 || len(original.Statements[0].Rows) != 1 {
		t.Fatalf("missing original RETURNING result: %+v", original)
	}
	check := func(want int) {
		t.Helper()
		rows, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT id, name FROM backup_items ORDER BY id"})
		if err != nil || len(rows.Rows) != want {
			t.Fatalf("rows=%+v want=%d err=%v", rows.Rows, want, err)
		}
		retry, err := db.ExecuteReturning(ctx, request)
		if err != nil || retry.Slot != original.Slot || !reflect.DeepEqual(retry.Statements, original.Statements) {
			t.Fatalf("idempotent retry=%+v original=%+v err=%v", retry, original, err)
		}
	}
	for loss := 1; loss <= 2; loss++ {
		// Freeze the acknowledged remote image before Close can flush anything.
		published := embeddedRemoteImage(t, cfg)
		if len(published) == 0 {
			t.Fatal("acknowledged mutations have no remote backup")
		}
		closeDB()
		embeddedReplaceRemote(t, cfg, published)
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(cfg.DataDir); !os.IsNotExist(err) {
			t.Fatalf("local root still exists: %v", err)
		}
		open()
		check(loss)
		name := "after-first-restore"
		if loss == 2 {
			name = "after-second-restore"
		}
		if _, err := db.ExecuteReturning(ctx, rhiza.ExecuteRequest{RequestID: name, SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{name}}); err != nil {
			t.Fatalf("write after loss %d: %v", loss, err)
		}
		check(loss + 1)
		closeDB()
		open()
		check(loss + 1)
	}
	closeDB()
}
