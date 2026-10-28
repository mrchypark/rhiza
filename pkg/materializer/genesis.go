package materializer

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	sqlite "github.com/ncruces/go-sqlite3/driver"
)

var emptyGenesisTables = map[string][]string{
	"_rhiza_meta":        {"key", "value"},
	"_rhiza_kv":          {"key", "value", "expires_at_unix_ms"},
	"_rhiza_idempotency": {"kind", "request_id", "fingerprint", "commit_slot", "status", "error_code", "rows_affected", "last_insert_id", "applied", "sql_result"},
	"_rhiza_migrations":  {"version", "name", "checksum"},
}

var emptyGenesisIndexes = map[string]string{
	"_rhiza_kv_expiry": "_rhiza_kv", "_rhiza_idempotency_slot": "_rhiza_idempotency",
}

var emptyGenesisSchema = map[string]string{
	"_rhiza_meta":        `CREATE TABLE _rhiza_meta ( key TEXT PRIMARY KEY, value TEXT )`,
	"_rhiza_kv":          `CREATE TABLE _rhiza_kv ( key TEXT PRIMARY KEY, value BLOB NOT NULL, expires_at_unix_ms INTEGER NOT NULL DEFAULT 0 )`,
	"_rhiza_idempotency": `CREATE TABLE _rhiza_idempotency ( kind INTEGER NOT NULL, request_id BLOB NOT NULL, fingerprint BLOB NOT NULL CHECK(length(fingerprint) = 32), commit_slot INTEGER NOT NULL, status TEXT NOT NULL, error_code TEXT NOT NULL, rows_affected INTEGER NOT NULL, last_insert_id INTEGER NOT NULL, applied INTEGER NOT NULL, sql_result BLOB NOT NULL DEFAULT X'', PRIMARY KEY(kind, request_id) ) WITHOUT ROWID`,
	"_rhiza_migrations":  migrationTableDDL,
}

var emptyGenesisIndexSchema = map[string]string{
	"_rhiza_kv_expiry":        `CREATE INDEX _rhiza_kv_expiry ON _rhiza_kv(expires_at_unix_ms) WHERE expires_at_unix_ms > 0`,
	"_rhiza_idempotency_slot": `CREATE INDEX _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot)`,
}

// ValidateEmptyGenesisState inspects existing materialized files read-only.
// It refuses application data and unknown SQLite objects before Open can
// create, migrate, or otherwise mutate either database.
func ValidateEmptyGenesisState(sqlitePath, graphPath string) error {
	if err := ValidateCleanRestoreState(sqlitePath); err != nil {
		return err
	}
	if err := validateEmptyGenesisSQLite(sqlitePath); err != nil {
		return err
	}
	if err := ValidateLocalGraphStorage(graphPath, true); err != nil {
		return err
	}
	if _, err := os.Lstat(graphPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return validateEmptyGenesisGraph(graphPath)
}

func validateEmptyGenesisSQLite(path string) (resultErr error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("initial genesis SQLite path is not a regular file; preserve DataDir")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(absPath)}).String()
	db, err := sqlite.Open(fileURL + "?mode=ro")
	if err != nil {
		return fmt.Errorf("open initial genesis SQLite read-only: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	objects, err := db.Query(`SELECT type, name, tbl_name FROM sqlite_schema WHERE substr(name, 1, 7) != 'sqlite_' ORDER BY type, name`)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, objects.Close()) }()
	tables := map[string]bool{}
	indexes := map[string]bool{}
	for objects.Next() {
		var kind, name, table string
		if err := objects.Scan(&kind, &name, &table); err != nil {
			return err
		}
		switch kind {
		case "table":
			if _, ok := emptyGenesisTables[name]; ok {
				tables[name] = true
			} else {
				return fmt.Errorf("initial genesis SQLite catalog contains unknown table %q; preserve DataDir", name)
			}
		case "index":
			if emptyGenesisIndexes[name] != table {
				return fmt.Errorf("initial genesis SQLite catalog contains unknown index %q; preserve DataDir", name)
			}
			indexes[name] = true
		default:
			return fmt.Errorf("initial genesis SQLite catalog contains unknown %s %q; preserve DataDir", kind, name)
		}
	}
	if err := objects.Err(); err != nil {
		return err
	}
	if len(tables) != len(emptyGenesisTables) || len(indexes) != len(emptyGenesisIndexes) {
		return fmt.Errorf("initial genesis SQLite schema is incomplete; preserve DataDir")
	}
	for table, want := range emptyGenesisTables {
		if err := validateGenesisSQLiteDefinition(db, "table", table, emptyGenesisSchema[table]); err != nil {
			return err
		}
		columns, err := genesisSQLiteColumns(db, table)
		if err != nil {
			return err
		}
		if len(columns) != len(want) {
			return fmt.Errorf("initial genesis SQLite table %q has an unexpected schema; preserve DataDir", table)
		}
		for i := range want {
			if columns[i] != want[i] {
				return fmt.Errorf("initial genesis SQLite table %q has an unexpected schema; preserve DataDir", table)
			}
		}
	}
	for index, want := range emptyGenesisIndexSchema {
		if err := validateGenesisSQLiteDefinition(db, "index", index, want); err != nil {
			return err
		}
	}
	for _, table := range []string{"_rhiza_kv", "_rhiza_idempotency", "_rhiza_migrations"} {
		var count int64
		if err := db.QueryRow(`SELECT COUNT(*) FROM "` + table + `"`).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("initial genesis SQLite table %q is not empty; preserve DataDir", table)
		}
	}
	if tables["_rhiza_meta"] {
		rows, err := db.Query(`SELECT key, value FROM _rhiza_meta ORDER BY key`)
		if err != nil {
			return err
		}
		seen := make(map[string]bool)
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				_ = rows.Close()
				return err
			}
			if seen[key] || !validEmptyGenesisMetadata(key, value) {
				_ = rows.Close()
				return fmt.Errorf("initial genesis SQLite metadata %q is unknown or nonempty; preserve DataDir", key)
			}
			seen[key] = true
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if len(seen) != 7 {
			return fmt.Errorf("initial genesis SQLite metadata is incomplete; preserve DataDir")
		}
	}
	return nil
}

func validateGenesisSQLiteDefinition(db *sql.DB, kind, name, want string) error {
	var got string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?`, kind, name).Scan(&got); err != nil {
		return err
	}
	if strings.Join(strings.Fields(got), " ") != strings.Join(strings.Fields(want), " ") {
		return fmt.Errorf("initial genesis SQLite %s %q has an unexpected definition; preserve DataDir", kind, name)
	}
	return nil
}

func genesisSQLiteColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA table_info("` + table + `")`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notNull, primary int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

func validEmptyGenesisMetadata(key, value string) bool {
	switch key {
	case "sql_execution_policy":
		return value == sqlpolicy.Marker()
	case "sql_idempotency_window":
		window, err := strconv.ParseUint(value, 10, 64)
		return err == nil && window >= 1024 && window <= 1_048_576
	case "sql_receipt_result_bytes", "sql_receipt_count", "applied_slot", "state_slot":
		return value == "0"
	case "applied_hash":
		decoded, err := hex.DecodeString(value)
		return err == nil && len(decoded) == 32 && value == "0000000000000000000000000000000000000000000000000000000000000000"
	default:
		return false
	}
}

func validateEmptyGenesisGraph(path string) (resultErr error) {
	db, err := latticedb.Open(path, latticedb.OpenOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	result, err := db.Query("MATCH (n) RETURN n LIMIT 1", nil)
	if err != nil {
		return err
	}
	if len(result.Rows) != 0 {
		return fmt.Errorf("initial genesis graph contains application nodes; preserve DataDir")
	}
	return nil
}
