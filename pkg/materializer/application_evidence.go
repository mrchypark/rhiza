package materializer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// SQLApplicationState describes SQL logical state, not WAL history or authority.
type SQLApplicationState struct {
	Digest     [32]byte
	FreshEmpty bool
}

const maxSQLEvidenceBytes = 64 << 20

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
	defer tx.Rollback()
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
	var objects []sqlEvidenceObject
	totalBytes := 0
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
		objects = append(objects, o)
		totalBytes += len(o.Type) + len(o.Name) + len(o.Table) + len(o.SQL)
		if totalBytes > maxSQLEvidenceBytes {
			rows.Close()
			return SQLApplicationState{}, fmt.Errorf("SQL evidence exceeds size limit")
		}
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
	withoutRowID := make(map[string]bool)
	for tableList.Next() {
		var schema, name, kind string
		var columns, wr, strict int
		if err := tableList.Scan(&schema, &name, &kind, &columns, &wr, &strict); err != nil {
			tableList.Close()
			return SQLApplicationState{}, err
		}
		if kind == "table" {
			withoutRowID[name] = wr != 0
		}
	}
	if err := tableList.Err(); err != nil {
		tableList.Close()
		return SQLApplicationState{}, err
	}
	if err := tableList.Close(); err != nil {
		return SQLApplicationState{}, err
	}
	for i := range objects {
		if objects[i].Type != "table" {
			continue
		}
		name := `"` + strings.ReplaceAll(objects[i].Name, `"`, `""`) + `"`
		wr, ok := withoutRowID[objects[i].Name]
		if !ok {
			return SQLApplicationState{}, fmt.Errorf("unsupported table %q", objects[i].Name)
		}
		projection := "*"
		if !wr {
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
				return SQLApplicationState{}, fmt.Errorf("rowid unavailable for table %q", objects[i].Name)
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
			objects[i].Rows = append(objects[i].Rows, string(b))
			totalBytes += len(b)
			if totalBytes > maxSQLEvidenceBytes {
				r.Close()
				return SQLApplicationState{}, fmt.Errorf("SQL evidence exceeds size limit")
			}
		}
		if err := r.Err(); err != nil {
			r.Close()
			return SQLApplicationState{}, err
		}
		if err := r.Close(); err != nil {
			return SQLApplicationState{}, err
		}
		sort.Strings(objects[i].Rows)
	}
	var receipts, kv, migrations int64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM _rhiza_idempotency), (SELECT COUNT(*) FROM _rhiza_kv), (SELECT COUNT(*) FROM _rhiza_migrations)`).Scan(&receipts, &kv, &migrations); err != nil {
		return SQLApplicationState{}, err
	}
	if err := tx.Commit(); err != nil {
		return SQLApplicationState{}, err
	}
	b, err := json.Marshal(struct {
		Version string
		Objects []sqlEvidenceObject
	}{"rhiza-sql-state-v1", objects})
	if err != nil {
		return SQLApplicationState{}, err
	}
	return SQLApplicationState{Digest: sha256.Sum256(b), FreshEmpty: len(objects) == 0 && receipts == 0 && kv == 0 && migrations == 0 && m.stateTip == 0}, nil
}
