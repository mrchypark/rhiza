package rhiza_test

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

// This checks a same-directory restart, not recovery after data-directory loss.
func TestWindowsPublicAPIRestart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local bool
	}{{"local", true}, {"single-voter", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cfg := rhiza.Config{NodeID: "restart", DataDir: t.TempDir(), Local: tc.local}
			db, err := rhiza.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() {
				if db != nil {
					if err := db.Close(); err != nil {
						t.Errorf("close: %v", err)
					}
				}
			}()
			for _, req := range []rhiza.ExecuteRequest{
				{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
				{RequestID: "insert", SQL: "INSERT INTO items (id, name) VALUES (1, 'persisted')"},
			} {
				if _, err := db.Execute(ctx, req); err != nil {
					t.Fatalf("execute %s: %v", req.RequestID, err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("first close: %v", err)
			}
			db = nil
			db, err = rhiza.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			result, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items WHERE id = 1"})
			if err != nil {
				t.Fatalf("query after reopen: %v", err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "persisted" {
				t.Fatalf("unexpected rows after reopen: %#v", result.Rows)
			}
		})
	}
}
