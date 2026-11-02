package network_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/ncruces/go-sqlite3"
)

const cancellationQuery = `WITH RECURSIVE counter(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM counter WHERE x<100000000) SELECT sum(x) FROM counter`

func TestQueryCancellationCauseDriverAndPublicBoundary(t *testing.T) {
	m, err := materializer.Open(filepath.Join(t.TempDir(), "driver.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	driverCtx, driverCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer driverCancel()
	_, _, driverErr := m.QueryResultAt(driverCtx, cancellationQuery, nil)
	var driverCause sqlite3.ExtendedErrorCode
	t.Logf("materializer: error_type=%T sqlite_interrupt=%t context_error=%v is_deadline=%t driver_as=%t", driverErr, errors.Is(driverErr, sqlite3.INTERRUPT), driverCtx.Err(), errors.Is(driverErr, context.DeadlineExceeded), errors.As(driverErr, &driverCause))
	if !errors.Is(driverErr, sqlite3.INTERRUPT) || !errors.Is(driverCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("did not reach actual SQLite interruption: type=%T error=%v context=%v", driverErr, driverErr, driverCtx.Err())
	}

	db, err := rhiza.Open(context.Background(), rhiza.Config{Local: true, NodeID: "local", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Run("ordinary-select", func(t *testing.T) {
		result, err := db.Query(context.Background(), rhiza.QueryRequest{SQL: "SELECT 1"})
		if err != nil || len(result.Rows) != 1 {
			t.Fatalf("normal neighbor: rows=%v error=%v", result.Rows, err)
		}
	})
	t.Run("syntax-before-late-cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT FROM"})
		cancel()
		if err == nil || !errors.Is(err, network.ErrInvalidRequest) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("syntax error reclassified after cancellation: type=%T error=%v", err, err)
		}
	})
	t.Run("actual-deadline-retains-driver-cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := db.Query(ctx, rhiza.QueryRequest{SQL: cancellationQuery})
		var cause sqlite3.ExtendedErrorCode
		hasDriver := errors.As(err, &cause)
		t.Logf("public Query: error_type=%T error=%v context_error=%v is_deadline=%t sqlite_interrupt=%t driver_as=%t", err, err, ctx.Err(), errors.Is(err, context.DeadlineExceeded), errors.Is(err, sqlite3.INTERRUPT), hasDriver)
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, sqlite3.INTERRUPT) || !hasDriver {
			t.Fatalf("public query lost cancellation or actual driver cause: type=%T error=%v context=%v", err, err, ctx.Err())
		}
		if !errors.Is(err, network.ErrInvalidRequest) {
			t.Fatal("existing query classification changed")
		}
	})
	t.Run("actual-cancel-retains-driver-cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		timer := time.AfterFunc(50*time.Millisecond, cancel)
		defer timer.Stop()
		_, err := db.Query(ctx, rhiza.QueryRequest{SQL: cancellationQuery})
		var cause sqlite3.ExtendedErrorCode
		if !errors.Is(ctx.Err(), context.Canceled) || !errors.Is(err, context.Canceled) || !errors.Is(err, sqlite3.INTERRUPT) || !errors.As(err, &cause) || !errors.Is(err, network.ErrInvalidRequest) {
			t.Fatalf("public query lost canceled context or driver cause: type=%T error=%v context=%v", err, err, ctx.Err())
		}
	})
}
