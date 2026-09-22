package materializer

import (
	"github.com/ncruces/go-sqlite3"
	"strings"
)

func setSQLAuthorizer(conn *sqlite3.Conn, writer, readOnly, allowMainAlter bool) error {
	mainAlter := false
	return conn.SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, name, database, _ string) sqlite3.AuthorizerReturnCode {
		if (writer || readOnly) && (action == sqlite3.AUTH_ATTACH || action == sqlite3.AUTH_DETACH) {
			return sqlite3.AUTH_DENY
		}
		if action == sqlite3.AUTH_FUNCTION && (strings.EqualFold(name, "load_extension") || writer && nondeterministicSQLFunction(name)) {
			return sqlite3.AUTH_DENY
		}
		if writer {
			switch action {
			case sqlite3.AUTH_CREATE_TEMP_INDEX, sqlite3.AUTH_CREATE_TEMP_TABLE, sqlite3.AUTH_CREATE_TEMP_TRIGGER, sqlite3.AUTH_CREATE_TEMP_VIEW, sqlite3.AUTH_DROP_TEMP_INDEX, sqlite3.AUTH_DROP_TEMP_TABLE, sqlite3.AUTH_DROP_TEMP_TRIGGER, sqlite3.AUTH_DROP_TEMP_VIEW:
				return sqlite3.AUTH_DENY
			case sqlite3.AUTH_ALTER_TABLE:
				if object != "main" {
					return sqlite3.AUTH_DENY
				}
				mainAlter = allowMainAlter
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
