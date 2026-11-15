//go:build rhiza_local_testhooks

package materializer

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func TestUserPolicyRestoreFailureCleanup(t *testing.T) {
	if os.Getenv("RHIZA_POLICY_RESTORE_CHILD") == "1" {
		testUserPolicyRestoreFailureChild(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUserPolicyRestoreFailureCleanup$", "-test.v")
	cmd.Env = append(os.Environ(), "RHIZA_POLICY_RESTORE_CHILD=1", "RHIZA_POLICY_RESTORE_DIR="+t.TempDir())
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("cleanup stalled: %v\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("child failed: %v\n%s", err, output)
	}
}

func testUserPolicyRestoreFailureChild(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(os.Getenv("RHIZA_POLICY_RESTORE_DIR"), "state.sqlite")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	schema, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE probe(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, schema); err != nil {
		t.Fatal(err)
	}
	if err := m.writer.Close(); err != nil {
		t.Fatal(err)
	}
	var closes atomic.Int32
	m.writer, err = sqlite3driver.Open(path, func(conn *sqlite3.Conn) error {
		return setSQLAuthorizer(conn, true, false, false)
	}, func(*sqlite3.Conn) error {
		closes.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	m.writer.SetMaxOpenConns(1)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "write", SQL: "INSERT INTO probe VALUES (7)"}})
	if err != nil {
		t.Fatal(err)
	}
	failUserPolicyRestore.Store(true)
	applyErr := m.Apply(ctx, 2, value)
	failUserPolicyRestore.Store(false)
	if !errors.Is(applyErr, errSQLPolicyState) || !errors.Is(applyErr, ErrGraphCommitUnknown) || !errors.Is(applyErr, sqlite3.AUTH) {
		t.Fatalf("restore failure must preserve policy, rollback and unknown errors: %v", applyErr)
	}
	if m.Tip() != 1 {
		t.Fatalf("tip advanced: %d", m.Tip())
	}
	if _, found, err := m.MutationReceipt(ctx, types.MutationSQL, "write"); found || !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("receipt exposed: found=%t err=%v", found, err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("expected SQLite TRACE_CLOSE before returning, got %d", got)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows, err := reopened.QueryResult(ctx, "SELECT count(*) FROM probe", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(0) || reopened.Tip() != 1 {
		t.Fatalf("unexpected durable state: rows=%+v tip=%d err=%v", rows.Rows, reopened.Tip(), err)
	}
	if _, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "write"); err != nil || found {
		t.Fatalf("unexpected durable receipt: found=%t err=%v", found, err)
	}
}
