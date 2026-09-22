package materializer

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
)

var errSQLRowIDExhausted = errors.New("maximum SQLite rowid is reserved by execution policy")

// watchRowID watches effective native writes, including transient trigger writes.
// Classification is needed only for maximum-rowid events: WITHOUT ROWID hook
// arguments are undefined and must never be interpreted as actual rowids.
func watchRowID(ctx context.Context, conn *sql.Conn, tx *sql.Tx) (func() error, error) {
	var targets map[string]bool
	if err := conn.Raw(func(raw any) error {
		raw.(driver.Conn).Raw().PreUpdateHook(func(p sqlite3.PreUpdateData) {
			if (p.Op == sqlite3.AUTH_INSERT || p.Op == sqlite3.AUTH_UPDATE) && p.NewRowID == math.MaxInt64 {
				if targets == nil {
					targets = make(map[string]bool)
				}
				targets[p.Table] = true
			}
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return func() error {
		if err := conn.Raw(func(raw any) error {
			raw.(driver.Conn).Raw().PreUpdateHook(nil)
			return nil
		}); err != nil {
			return err
		}
		if len(targets) == 0 {
			return nil
		}
		rows, err := tx.QueryContext(ctx, "PRAGMA main.table_list")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var schema, name, kind string
			var columns, withoutRowID, strict int
			if err := rows.Scan(&schema, &name, &kind, &columns, &withoutRowID, &strict); err != nil {
				return err
			}
			if targets[name] {
				if kind != "table" || withoutRowID == 0 {
					return errSQLRowIDExhausted
				}
				delete(targets, name)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// An unknown target cannot establish that the undefined WITHOUT ROWID
		// argument was harmless. Never silently accept unclassified writes.
		if len(targets) != 0 {
			return errSQLRowIDExhausted
		}
		return nil
	}, nil
}
