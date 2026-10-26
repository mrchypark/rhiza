package rhiza_test

import (
	"context"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// This exercises only the exported embedded API. The backup lives outside the
// disposable local root, so deleting DataDir cannot accidentally preserve a WAL.
func TestEmbeddedObjectStorageRepeatedTotalLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := embeddedStoreConfig(t)
	cfg.ClusterID = "embedded-repeated-total-loss"

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
	acknowledged := original.Slot
	for loss := 1; loss <= 2; loss++ {
		// Prove remote durability before Close can flush any missing writes.
		readOnly := &embeddedReadOnlyBucket{Bucket: embeddedBucket(t, cfg)}
		archive, err := recovery.OpenLocalArchive(ctx, readOnly, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1"))
		if err != nil {
			t.Fatalf("read acknowledged archive before Close: %v", err)
		}
		if uint64(archive.Tip()) < acknowledged {
			t.Fatalf("remote tip=%d before Close, want acknowledged slot >=%d", archive.Tip(), acknowledged)
		}
		if readOnly.mutations != 0 {
			t.Fatalf("archive validation attempted %d remote mutations", readOnly.mutations)
		}
		closeDB()
		// Capture all keys only after the writer and its gate have drained.
		published := embeddedRemoteImage(t, cfg)
		if len(published) == 0 {
			t.Fatal("acknowledged mutations have no remote backup")
		}
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
		written, err := db.ExecuteReturning(ctx, rhiza.ExecuteRequest{RequestID: name, SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{name}})
		if err != nil {
			t.Fatalf("write after loss %d: %v", loss, err)
		}
		acknowledged = written.Slot
		check(loss + 1)
		closeDB()
		open()
		check(loss + 1)
	}
	closeDB()
}
