package materializer

import (
	"github.com/ncruces/go-sqlite3"
	"strings"
)

func setSQLAuthorizer(conn *sqlite3.Conn, writer, readOnly, allowMainMaintenance bool) error {
	unsupported := map[string]bool{}
	if writer {
		query, _, err := conn.Prepare(`PRAGMA main.table_list`)
		if err != nil {
			return err
		}
		for query.Step() {
			if kind := query.ColumnText(2); kind == "virtual" || kind == "shadow" {
				unsupported[query.ColumnText(1)] = true
			}
		}
		err = query.Err()
		closeErr := query.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	mainAlter := false
	mainDrop := false
	return conn.SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, name, database, inner string) sqlite3.AuthorizerReturnCode {
		if (writer || readOnly) && (action == sqlite3.AUTH_ATTACH || action == sqlite3.AUTH_DETACH) {
			return sqlite3.AUTH_DENY
		}
		if action == sqlite3.AUTH_FUNCTION && (strings.EqualFold(name, "load_extension") || writer && nondeterministicSQLFunction(name)) {
			return sqlite3.AUTH_DENY
		}
		if writer {
			switch action {
			case sqlite3.AUTH_CREATE_VTABLE, sqlite3.AUTH_ANALYZE:
				return sqlite3.AUTH_DENY
			case sqlite3.AUTH_DROP_TABLE:
				mainDrop = allowMainMaintenance && database == "main"
			case sqlite3.AUTH_INSERT, sqlite3.AUTH_UPDATE, sqlite3.AUTH_DELETE:
				// A single main-schema ALTER renames AUTOINCREMENT bookkeeping.
				if mainAlter && inner == "" && action == sqlite3.AUTH_UPDATE && object == "sqlite_sequence" && name == "name" {
					return sqlite3.AUTH_OK
				}
				if mainDrop && inner == "" && action == sqlite3.AUTH_DELETE && object == "sqlite_sequence" {
					return sqlite3.AUTH_OK
				}
				if unsupported[object] || strings.HasPrefix(object, "sqlite_") && object != "sqlite_master" && object != "sqlite_schema" && object != "sqlite_temp_master" && object != "sqlite_temp_schema" {
					return sqlite3.AUTH_DENY
				}
			case sqlite3.AUTH_CREATE_TEMP_INDEX, sqlite3.AUTH_CREATE_TEMP_TABLE, sqlite3.AUTH_CREATE_TEMP_TRIGGER, sqlite3.AUTH_CREATE_TEMP_VIEW, sqlite3.AUTH_DROP_TEMP_INDEX, sqlite3.AUTH_DROP_TEMP_TABLE, sqlite3.AUTH_DROP_TEMP_TRIGGER, sqlite3.AUTH_DROP_TEMP_VIEW:
				return sqlite3.AUTH_DENY
			case sqlite3.AUTH_ALTER_TABLE:
				if object != "main" {
					return sqlite3.AUTH_DENY
				}
				mainAlter = allowMainMaintenance
			}
			if strings.EqualFold(database, "temp") {
				// SQLite's main-schema ALTER maintenance reads/updates this catalog.
				// The caller scopes this authorizer to one ALTER, including reprepare.
				if mainAlter && (object == "sqlite_temp_master" || object == "sqlite_temp_schema") && (action == sqlite3.AUTH_READ || action == sqlite3.AUTH_UPDATE) {
					return sqlite3.AUTH_OK
				}
				return sqlite3.AUTH_DENY
			}
		}
		return sqlite3.AUTH_OK
	})
}
