package materializer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

// SQLApplicationState describes SQL logical state, not WAL history or authority.
type SQLApplicationState struct {
	Digest     [32]byte
	FreshEmpty bool
}

type sqlEvidenceObject struct {
	Type, Name, Table, SQL string
	Rows                   []string
}

// InspectSQLApplicationState reads one apply-locked SQLite snapshot. FreshEmpty
// is only a local materializer fact; the caller must separately inspect WAL history.
func (m *Materializer) InspectSQLApplicationState(ctx context.Context) (SQLApplicationState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.writer == nil {
		return SQLApplicationState{}, sql.ErrConnDone
	}
	if m.graph == nil {
		return SQLApplicationState{}, fmt.Errorf("graph state unavailable")
	}
	m.graph.mu.RLock()
	unsafeGraph := m.graph.commitUnknown || m.graph.pending != nil
	m.graph.mu.RUnlock()
	if unsafeGraph {
		return SQLApplicationState{}, ErrGraphCommitUnknown
	}
	tx, err := m.writer.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SQLApplicationState{}, err
	}
	return inspectSQLApplicationTransaction(ctx, tx, m.stateTip)
}

// InspectSQLApplicationFile copies a stable SQLite database and its WAL into a
// private directory, then inspects that copy read-only. Callers must ensure
// source files remain stable during the copy (for example, by holding the
// materializer's WAL exclusion). Source sidecars are never created, changed,
// or removed by this function.
func InspectSQLApplicationFile(ctx context.Context, dbPath string) (state SQLApplicationState, appliedTip uint64, resultErr error) {
	absolute, err := filepath.Abs(dbPath)
	if err != nil {
		return state, 0, err
	}
	privateDir, err := os.MkdirTemp("", "rhiza-sql-evidence-")
	if err != nil {
		return state, 0, err
	}
	defer func() {
		if err := os.RemoveAll(privateDir); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove private SQL evidence snapshot: %w", err))
		}
	}()
	privateDB := filepath.Join(privateDir, "application.sqlite")
	if err := copyEvidenceFile(absolute, privateDB); err != nil {
		return state, 0, err
	}
	if err := copyEvidenceFileIfPresent(absolute+"-wal", privateDB+"-wal"); err != nil {
		return state, 0, err
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(privateDB)}).String() + "?mode=ro&_pragma=busy_timeout(5000)"
	db, err := openSQLite(dsn, false)
	if err != nil {
		return state, 0, fmt.Errorf("open SQL state read-only: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return state, 0, err
	}
	defer tx.Rollback()
	var policy string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='sql_execution_policy'`).Scan(&policy); err != nil {
		return state, 0, fmt.Errorf("read SQL materialization policy: %w", err)
	}
	if policy != sqlpolicy.Marker() {
		return state, 0, fmt.Errorf("incompatible SQL materialization policy marker %q", policy)
	}
	var appliedValue, stateValue string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='applied_slot'`).Scan(&appliedValue); err != nil {
		return state, 0, fmt.Errorf("read applied SQL tip: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='state_slot'`).Scan(&stateValue); err != nil {
		return state, 0, fmt.Errorf("read SQL state tip: %w", err)
	}
	appliedTip, err = strconv.ParseUint(appliedValue, 10, 64)
	if err != nil {
		return state, 0, fmt.Errorf("invalid applied SQL tip: %w", err)
	}
	stateTip, err := strconv.ParseUint(stateValue, 10, 64)
	if err != nil || stateTip > appliedTip {
		return state, 0, fmt.Errorf("invalid SQL state tip")
	}
	state, err = inspectSQLApplicationTransaction(ctx, tx, stateTip)
	return state, appliedTip, err
}

func copyEvidenceFileIfPresent(source, destination string) error {
	if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return copyEvidenceFile(source, destination)
}

func copyEvidenceFile(source, destination string) (resultErr error) {
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open SQL evidence source %s: %w", source, err)
	}
	defer func() { resultErr = errors.Join(resultErr, in.Close()) }()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create private SQL evidence file: %w", err)
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("copy SQL evidence source %s: %w", source, err)
	}
	return nil
}

func inspectSQLApplicationTransaction(ctx context.Context, tx *sql.Tx, stateTip uint64) (state SQLApplicationState, resultErr error) {
	defer tx.Rollback()
	// Keep canonical sorting on disk, not in a total-database-sized Go slice.
	private, err := os.MkdirTemp("", "rhiza-sql-evidence-sort-")
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(private)) }()
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(private, "sort.sqlite"))}).String() + "?_pragma=cache_size(-2048)&_pragma=mmap_size(0)&_pragma=temp_store(FILE)"
	spool, err := sqlite3driver.Open(dsn)
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, spool.Close()) }()
	spool.SetMaxOpenConns(1)
	sortTx, err := spool.BeginTx(ctx, nil)
	if err != nil {
		return state, err
	}
	defer sortTx.Rollback()
	if _, err := sortTx.ExecContext(ctx, `CREATE TABLE objects (id INTEGER PRIMARY KEY, metadata BLOB NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL); CREATE TABLE evidence_rows (object INTEGER NOT NULL, data BLOB NOT NULL); CREATE INDEX evidence_order ON evidence_rows(object,data)`); err != nil {
		return state, err
	}
	insertObject, err := sortTx.PrepareContext(ctx, `INSERT INTO objects(id,metadata,kind,name) VALUES(?,?,?,?)`)
	if err != nil {
		return state, err
	}
	defer insertObject.Close()
	insertRow, err := sortTx.PrepareContext(ctx, `INSERT INTO evidence_rows(object,data) VALUES(?,?)`)
	if err != nil {
		return state, err
	}
	defer insertRow.Close()
	rows, err := tx.QueryContext(ctx, `SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name`)
	if err != nil {
		return SQLApplicationState{}, err
	}
	internal := map[string]string{
		"_rhiza_meta":             `CREATE TABLE _rhiza_meta ( key TEXT PRIMARY KEY, value TEXT )`,
		"_rhiza_kv":               `CREATE TABLE _rhiza_kv ( key TEXT PRIMARY KEY, value BLOB NOT NULL, expires_at_unix_ms INTEGER NOT NULL DEFAULT 0 )`,
		"_rhiza_idempotency":      `CREATE TABLE _rhiza_idempotency ( kind INTEGER NOT NULL, request_id BLOB NOT NULL, fingerprint BLOB NOT NULL CHECK(length(fingerprint) = 32), commit_slot INTEGER NOT NULL, status TEXT NOT NULL, error_code TEXT NOT NULL, rows_affected INTEGER NOT NULL, last_insert_id INTEGER NOT NULL, applied INTEGER NOT NULL, sql_result BLOB NOT NULL DEFAULT X'', PRIMARY KEY(kind, request_id) ) WITHOUT ROWID`,
		"_rhiza_migrations":       `CREATE TABLE _rhiza_migrations ( version INTEGER PRIMARY KEY CHECK(version > 0), name TEXT NOT NULL, checksum TEXT NOT NULL CHECK(length(checksum) = 64) ) STRICT`,
		"_rhiza_kv_expiry":        `CREATE INDEX _rhiza_kv_expiry ON _rhiza_kv(expires_at_unix_ms) WHERE expires_at_unix_ms > 0`,
		"_rhiza_idempotency_slot": `CREATE INDEX _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot)`,
	}
	objectCount := 0
	for rows.Next() {
		var o sqlEvidenceObject
		if err := rows.Scan(&o.Type, &o.Name, &o.Table, &o.SQL); err != nil {
			rows.Close()
			return SQLApplicationState{}, err
		}
		if schema, ok := internal[o.Name]; ok {
			if strings.Join(strings.Fields(o.SQL), " ") != schema {
				rows.Close()
				return SQLApplicationState{}, fmt.Errorf("invalid internal schema %q", o.Name)
			}
			delete(internal, o.Name)
			continue
		}
		if strings.HasPrefix(strings.ToLower(o.Name), "_rhiza_") {
			rows.Close()
			return SQLApplicationState{}, fmt.Errorf("unknown internal schema %q", o.Name)
		}
		if strings.HasPrefix(o.Name, "sqlite_autoindex_") && o.Type == "index" && o.SQL == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(o.Name), "sqlite_") && o.Name != "sqlite_sequence" {
			rows.Close()
			return SQLApplicationState{}, fmt.Errorf("unsupported SQLite schema %q", o.Name)
		}
		metadata, err := json.Marshal(o)
		if err != nil {
			rows.Close()
			return state, err
		}
		if _, err := insertObject.ExecContext(ctx, objectCount, metadata, o.Type, o.Name); err != nil {
			rows.Close()
			return state, err
		}
		objectCount++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return SQLApplicationState{}, err
	}
	if err := rows.Close(); err != nil {
		return SQLApplicationState{}, err
	}
	if len(internal) != 0 {
		return SQLApplicationState{}, fmt.Errorf("missing internal schema")
	}
	tableList, err := tx.QueryContext(ctx, `PRAGMA main.table_list`)
	if err != nil {
		return SQLApplicationState{}, err
	}
	for tableList.Next() {
		var schema, name, kind string
		var columns, wr, strict int
		if err := tableList.Scan(&schema, &name, &kind, &columns, &wr, &strict); err != nil {
			tableList.Close()
			return SQLApplicationState{}, err
		}
		if kind == "virtual" || kind == "shadow" {
			tableList.Close()
			return SQLApplicationState{}, fmt.Errorf("unsupported SQLite table kind %q", kind)
		}
	}
	if err := tableList.Err(); err != nil {
		tableList.Close()
		return SQLApplicationState{}, err
	}
	if err := tableList.Close(); err != nil {
		return SQLApplicationState{}, err
	}
	objectRows, err := sortTx.QueryContext(ctx, `SELECT id,kind,name FROM objects ORDER BY id`)
	if err != nil {
		return state, err
	}
	defer objectRows.Close()
	for objectRows.Next() {
		var id int
		var kind, objectName string
		if err := objectRows.Scan(&id, &kind, &objectName); err != nil {
			return state, err
		}
		if kind != "table" {
			continue
		}
		name := `"` + strings.ReplaceAll(objectName, `"`, `""`) + `"`
		var wr int
		if err := tx.QueryRowContext(ctx, `SELECT wr FROM pragma_table_list WHERE schema='main' AND type='table' AND name=?`, objectName).Scan(&wr); err != nil {
			return state, fmt.Errorf("unsupported table %q: %w", objectName, err)
		}
		projection := "*"
		if wr == 0 {
			info, err := tx.QueryContext(ctx, "PRAGMA main.table_xinfo("+name+")")
			if err != nil {
				return SQLApplicationState{}, err
			}
			shadowed := map[string]bool{}
			for info.Next() {
				var cid, notnull, pk, hidden int
				var column, typ string
				var dflt any
				if err := info.Scan(&cid, &column, &typ, &notnull, &dflt, &pk, &hidden); err != nil {
					info.Close()
					return SQLApplicationState{}, err
				}
				shadowed[strings.ToLower(column)] = true
			}
			if err := info.Err(); err != nil {
				info.Close()
				return SQLApplicationState{}, err
			}
			if err := info.Close(); err != nil {
				return SQLApplicationState{}, err
			}
			alias := ""
			for _, candidate := range []string{"rowid", "_rowid_", "oid"} {
				if !shadowed[candidate] {
					alias = candidate
					break
				}
			}
			if alias == "" {
				return SQLApplicationState{}, fmt.Errorf("rowid unavailable for table %q", objectName)
			}
			projection = alias + ",*"
		}
		r, err := tx.QueryContext(ctx, "SELECT "+projection+" FROM "+name)
		if err != nil {
			return SQLApplicationState{}, err
		}
		columns, err := r.Columns()
		if err != nil {
			r.Close()
			return SQLApplicationState{}, err
		}
		for r.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for j := range values {
				destinations[j] = &values[j]
			}
			if err := r.Scan(destinations...); err != nil {
				r.Close()
				return SQLApplicationState{}, err
			}
			encoded := make([][2]string, len(values))
			for j, value := range values {
				switch v := value.(type) {
				case nil:
					encoded[j] = [2]string{"null", ""}
				case int64:
					encoded[j] = [2]string{"integer", strconv.FormatInt(v, 10)}
				case float64:
					encoded[j] = [2]string{"real", fmt.Sprintf("%016x", math.Float64bits(v))}
				case string:
					encoded[j] = [2]string{"text", v}
				case []byte:
					encoded[j] = [2]string{"blob", hex.EncodeToString(v)}
				default:
					r.Close()
					return SQLApplicationState{}, fmt.Errorf("unsupported SQLite value %T", value)
				}
			}
			b, err := json.Marshal(encoded)
			if err != nil {
				r.Close()
				return SQLApplicationState{}, err
			}
			if _, err := insertRow.ExecContext(ctx, id, b); err != nil {
				r.Close()
				return state, err
			}
		}
		if err := r.Err(); err != nil {
			r.Close()
			return SQLApplicationState{}, err
		}
		if err := r.Close(); err != nil {
			return SQLApplicationState{}, err
		}
	}
	if err := objectRows.Err(); err != nil {
		return state, err
	}
	if err := objectRows.Close(); err != nil {
		return state, err
	}
	var receipts, kv, migrations int64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM _rhiza_idempotency), (SELECT COUNT(*) FROM _rhiza_kv), (SELECT COUNT(*) FROM _rhiza_migrations)`).Scan(&receipts, &kv, &migrations); err != nil {
		return SQLApplicationState{}, err
	}
	if err := tx.Commit(); err != nil {
		return SQLApplicationState{}, err
	}
	digest, err := hashSQLApplicationEvidence(ctx, sortTx, objectCount)
	if err != nil {
		return SQLApplicationState{}, err
	}
	return SQLApplicationState{Digest: digest, FreshEmpty: objectCount == 0 && receipts == 0 && kv == 0 && migrations == 0 && stateTip == 0}, nil
}

// hashSQLApplicationEvidence emits exactly encoding/json's historical v1 bytes.
// BLOB ordering is bytewise, matching sort.Strings on canonical row JSON.
func hashSQLApplicationEvidence(ctx context.Context, tx *sql.Tx, objectCount int) ([32]byte, error) {
	var digest [32]byte
	h := sha256.New()
	io.WriteString(h, `{"Version":"rhiza-sql-state-v1","Objects":`)
	if objectCount == 0 {
		io.WriteString(h, "null}")
		copy(digest[:], h.Sum(nil))
		return digest, ctx.Err()
	}
	io.WriteString(h, "[")
	objects, err := tx.QueryContext(ctx, `SELECT id,metadata FROM objects ORDER BY id`)
	if err != nil {
		return digest, err
	}
	defer objects.Close()
	count := 0
	for objects.Next() {
		var id int
		var metadata []byte
		if err := objects.Scan(&id, &metadata); err != nil {
			return digest, err
		}
		if !strings.HasSuffix(string(metadata), `,"Rows":null}`) {
			return digest, fmt.Errorf("invalid SQL evidence metadata")
		}
		if count != 0 {
			io.WriteString(h, ",")
		}
		h.Write(metadata[:len(metadata)-len("null}")])
		rows, err := tx.QueryContext(ctx, `SELECT data FROM evidence_rows WHERE object=? ORDER BY data`, id)
		if err != nil {
			return digest, err
		}
		hasRows := false
		for rows.Next() {
			var row []byte
			if err := rows.Scan(&row); err != nil {
				rows.Close()
				return digest, err
			}
			if hasRows {
				io.WriteString(h, ",")
			} else {
				io.WriteString(h, "[")
			}
			hasRows = true
			encoded, err := json.Marshal(string(row))
			if err != nil {
				rows.Close()
				return digest, err
			}
			h.Write(encoded)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return digest, err
		}
		if err := rows.Close(); err != nil {
			return digest, err
		}
		if hasRows {
			io.WriteString(h, "]")
		} else {
			io.WriteString(h, "null")
		}
		io.WriteString(h, "}")
		count++
	}
	if err := objects.Err(); err != nil {
		return digest, err
	}
	if count != objectCount {
		return digest, fmt.Errorf("SQL evidence object count changed")
	}
	io.WriteString(h, "]}")
	copy(digest[:], h.Sum(nil))
	return digest, ctx.Err()
}
