package materializer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func TestQueryContextCauseOnlyJoinsInterruptWithCompletedCall(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, stop := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer stop()
	interrupt := error(sqlite3.ExtendedErrorCode(sqlite3.INTERRUPT))
	wrapped := fmt.Errorf("driver: %w", interrupt)
	syntax := error(sqlite3.ExtendedErrorCode(sqlite3.ERROR))
	other := errors.New("unrelated I/O")
	for _, test := range []struct {
		name             string
		ctx              context.Context
		err, wantContext error
	}{
		{"canceled-interrupt", canceled, interrupt, context.Canceled},
		{"deadline-interrupt", deadline, interrupt, context.DeadlineExceeded},
		{"wrapped-interrupt", canceled, wrapped, context.Canceled},
		{"live-interrupt", context.Background(), interrupt, nil},
		{"syntax-late-cancel", canceled, syntax, nil},
		{"other-late-deadline", deadline, other, nil},
		{"nil-late-cancel", canceled, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := queryContextCause(test.ctx, test.err)
			if test.wantContext == nil {
				if got != test.err {
					t.Fatalf("changed unrelated error: %v", got)
				}
				return
			}
			var code sqlite3.ExtendedErrorCode
			if !errors.Is(got, test.wantContext) || !errors.Is(got, test.err) || !errors.Is(got, sqlite3.INTERRUPT) || !errors.As(got, &code) {
				t.Fatalf("lost context or actual driver cause: %v", got)
			}
		})
	}
}

func TestQueryResultAtPreparationPreservesActualInterruption(t *testing.T) {
	// Make only the applied-slot lookup expensive. The public query is SELECT 1,
	// so interruption must occur during snapshot preparation, not row collection.
	db, err := sqlite3driver.Open(filepath.Join(t.TempDir(), "preparation.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err = db.Exec(`CREATE VIEW _rhiza_meta AS
		WITH RECURSIVE counter(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM counter WHERE x<100000000)
		SELECT 'applied_slot' AS key, CAST(sum(x) AS TEXT) AS value FROM counter`)
	if err != nil {
		t.Fatal(err)
	}
	m := &Materializer{readers: []*sql.DB{db}}
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			want := context.DeadlineExceeded
			if canceled {
				cancel()
				ctx, cancel = context.WithCancel(context.Background())
				timer := time.AfterFunc(50*time.Millisecond, cancel)
				defer timer.Stop()
				want = context.Canceled
			}
			defer cancel()
			_, _, err := m.QueryResultAt(ctx, "SELECT 1", nil)
			var code sqlite3.ExtendedErrorCode
			if !errors.Is(err, sqlite3.INTERRUPT) || !errors.Is(ctx.Err(), want) || !errors.As(err, &code) {
				t.Fatalf("did not reach actual preparation interruption: type=%T error=%v context=%v", err, err, ctx.Err())
			}
			if !errors.Is(err, want) {
				t.Fatalf("preparation lost completed context: type=%T error=%v context=%v", err, err, ctx.Err())
			}
		})
	}
}

func TestQueryResultsPreserveActualDriverInterruption(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "query.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := m.QueryResult(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	for _, at := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("at=%t/cancel=%t", at, canceled), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				want := context.DeadlineExceeded
				if canceled {
					cancel()
					ctx, cancel = context.WithCancel(context.Background())
					timer := time.AfterFunc(50*time.Millisecond, cancel)
					defer timer.Stop()
					want = context.Canceled
				}
				defer cancel()
				const query = `WITH RECURSIVE counter(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM counter WHERE x<100000000) SELECT sum(x) FROM counter`
				if at {
					_, _, err = m.QueryResultAt(ctx, query, nil)
				} else {
					_, err = m.QueryResult(ctx, query, nil)
				}
				var code sqlite3.ExtendedErrorCode
				if !errors.Is(err, want) || !errors.Is(err, sqlite3.INTERRUPT) || !errors.As(err, &code) {
					t.Fatalf("driver/context cause lost: type=%T error=%v context=%v", err, err, ctx.Err())
				}
			})
		}
	}
}
