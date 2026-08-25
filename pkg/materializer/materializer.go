package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
)

const (
	MaxSQLBytes      = 256 << 10
	MaxSQLStatements = 64
	MaxSQLArgs       = 999
	MaxReturningRows = 10_000
)

// Materializer applies decided values to SQLite.
// Uses single writer, multiple readers pattern like Hiqlite.
type Materializer struct {
	db       *sql.DB
	writer   *sql.DB
	readers  []*sql.DB
	mu       sync.RWMutex
	tip      uint64
	tipHash  [32]byte
	dbPath   string
	readersN int
	graph    *graphState
	notifyMu sync.Mutex
	nextSub  uint64
	subs     map[uint64]notificationSubscription
}

type notificationSubscription struct {
	topic string
	ch    chan []byte
}

type snapshotParts struct {
	sqlite   []byte
	graphDir string
	cleanup  func()
}

// Open opens or creates a materializer.
func Open(dbPath string, readerCount int) (*Materializer, error) {
	if readerCount < 1 {
		return nil, fmt.Errorf("reader count must be positive")
	}
	existing := false
	if info, err := os.Stat(dbPath); err == nil {
		existing = info.Size() > 0
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat database: %w", err)
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}).String()
	// QLog is the durable source of truth; SQLite is replayable materialized
	// state, so NORMAL avoids a redundant per-command durability barrier.
	writerDSN := fileURL + "?_pragma=journal_mode(wal)&_pragma=synchronous(normal)&_pragma=busy_timeout(5000)"
	readerDSN := fileURL + "?mode=ro&_pragma=busy_timeout(5000)"

	// Open main database for metadata
	db, err := openSQLite(writerDSN, false)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Open writer connection (single)
	writer, err := openSQLite(writerDSN, true)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)

	// Open reader connections (pool)
	readers := make([]*sql.DB, readerCount)
	for i := 0; i < readerCount; i++ {
		r, err := openSQLite(readerDSN, false)
		if err != nil {
			db.Close()
			writer.Close()
			for j := 0; j < i; j++ {
				readers[j].Close()
			}
			return nil, fmt.Errorf("open reader %d: %w", i, err)
		}
		readers[i] = r
	}

	m := &Materializer{
		db:       db,
		writer:   writer,
		readers:  readers,
		dbPath:   dbPath,
		readersN: readerCount,
		subs:     make(map[uint64]notificationSubscription),
	}

	// Initialize schema
	if err := m.initSchema(); err != nil {
		m.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if err := m.loadTip(existing); err != nil {
		m.Close()
		return nil, fmt.Errorf("load applied slot: %w", err)
	}
	graph, err := openGraph(filepath.Join(filepath.Dir(dbPath), "goraphdb"), m.tip)
	if err != nil {
		m.Close()
		return nil, fmt.Errorf("open graph materializer: %w", err)
	}
	m.graph = graph

	return m, nil
}

func openSQLite(dsn string, writer bool) (*sql.DB, error) {
	readOnly := strings.Contains(dsn, "mode=ro")
	return sqlite3driver.Open(dsn, func(conn *sqlite3.Conn) error {
		if err := fts5.Register(conn); err != nil {
			return err
		}
		conn.Limit(sqlite3.LIMIT_SQL_LENGTH, MaxSQLBytes)
		conn.Limit(sqlite3.LIMIT_LENGTH, 16<<20)
		conn.Limit(sqlite3.LIMIT_VARIABLE_NUMBER, MaxSQLArgs)
		if _, err := conn.Config(sqlite3.DBCONFIG_DEFENSIVE, true); err != nil {
			return err
		}
		return conn.SetAuthorizer(func(action sqlite3.AuthorizerActionCode, _, name string, _, _ string) sqlite3.AuthorizerReturnCode {
			if (writer || readOnly) && (action == sqlite3.AUTH_ATTACH || action == sqlite3.AUTH_DETACH) {
				return sqlite3.AUTH_DENY
			}
			if action == sqlite3.AUTH_FUNCTION && (strings.EqualFold(name, "load_extension") || writer && nondeterministicSQLFunction(name)) {
				return sqlite3.AUTH_DENY
			}
			return sqlite3.AUTH_OK
		})
	})
}

func nondeterministicSQLFunction(name string) bool {
	switch strings.ToLower(name) {
	case "random", "randomblob", "now", "current_time", "current_date", "current_timestamp",
		"date", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff",
		"load_extension":
		return true
	default:
		return false
	}
}

func (m *Materializer) loadTip(existing bool) error {
	var value string
	err := m.writer.QueryRow(`SELECT value FROM _rhiza_meta WHERE key = 'applied_slot'`).Scan(&value)
	if err == sql.ErrNoRows {
		if existing {
			return fmt.Errorf("existing database has no applied slot; rebuild it from the decision log")
		}
		zeroHash := hex.EncodeToString(make([]byte, sha256.Size))
		if _, err := m.writer.Exec(`INSERT INTO _rhiza_meta(key, value) VALUES ('applied_slot', '0'), ('applied_hash', ?)`, zeroHash); err != nil {
			return err
		}
		value = "0"
		err = nil
	}
	if err != nil {
		return err
	}
	tip, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return err
	}
	m.tip = tip
	var hashValue string
	if err := m.writer.QueryRow(`SELECT value FROM _rhiza_meta WHERE key = 'applied_hash'`).Scan(&hashValue); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(hashValue)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("invalid applied hash")
	}
	copy(m.tipHash[:], decoded)
	return nil
}

// ValidateTip checks that materialized state agrees with the recovered log.
func (m *Materializer) ValidateTip(slot uint64, value []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.tip != slot {
		return fmt.Errorf("materialized tip %d does not match recovered tip %d", m.tip, slot)
	}
	if sha256.Sum256(value) != m.tipHash {
		return fmt.Errorf("materialized slot %d hash conflicts with recovered decision", slot)
	}
	return nil
}

// initSchema creates the metadata table.
func (m *Materializer) initSchema() error {
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS _rhiza_meta (
			key TEXT PRIMARY KEY,
			value TEXT
		);
		CREATE TABLE IF NOT EXISTS _rhiza_requests (
				request_id TEXT PRIMARY KEY,
				sql TEXT NOT NULL,
				result_json BLOB
			);
		CREATE TABLE IF NOT EXISTS _rhiza_kv (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL,
			expires_at_unix_ms INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS _rhiza_kv_requests (
			request_id TEXT PRIMARY KEY,
			command_json BLOB NOT NULL,
			result_json BLOB NOT NULL
		);
		CREATE TABLE IF NOT EXISTS _rhiza_notify_requests (
			request_id TEXT PRIMARY KEY
		);
		`)
	if err != nil {
		return err
	}
	var resultColumn int
	if err := m.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('_rhiza_requests') WHERE name = 'result_json'`).Scan(&resultColumn); err != nil {
		return err
	}
	if resultColumn == 0 {
		_, err = m.db.Exec(`ALTER TABLE _rhiza_requests ADD COLUMN result_json BLOB`)
	}
	return err
}

// Apply applies a decided value to SQLite.
func (m *Materializer) Apply(ctx context.Context, slot uint64, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	hash := sha256.Sum256(value)

	if slot <= m.tip {
		if slot == m.tip && hash != m.tipHash {
			return fmt.Errorf("applied slot %d hash conflict", slot)
		}
		return nil
	}
	if slot != m.tip+1 {
		return fmt.Errorf("apply slot gap: have %d, got %d", m.tip, slot)
	}
	graphCommand, graph, err := types.DecodeGraphCommand(value)
	if err != nil {
		return fmt.Errorf("decode graph command: %w", err)
	}
	if err := m.applyGraph(ctx, slot, value, graphCommand, graph); err != nil {
		return err
	}

	tx, err := m.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply: %w", err)
	}
	defer tx.Rollback()
	notifyCommand, notify, err := types.DecodeNotifyCommand(value)
	if err != nil {
		return fmt.Errorf("decode notification: %w", err)
	}
	kvCommand, kv, err := types.DecodeKVCommand(value)
	if err != nil {
		return fmt.Errorf("decode KV command: %w", err)
	}
	commands, batched, err := types.DecodeSQLBatch(value)
	if err != nil {
		return fmt.Errorf("decode SQL batch: %w", err)
	}
	if barrier, err := types.DecodeReadBarrier(value); err != nil {
		return fmt.Errorf("decode read barrier: %w", err)
	} else if barrier {
		commands = nil
		batched = true
	} else if _, schedule, err := types.DecodeLeaderSchedule(value); err != nil {
		return fmt.Errorf("decode leader schedule: %w", err)
	} else if schedule {
		commands = nil
		batched = true
	} else if graph {
		commands = nil
		batched = true
	}
	publish := false
	if notify {
		if notifyCommand.RequestID == "" || notifyCommand.Topic == "" || len(notifyCommand.Topic) > 256 || len(notifyCommand.Payload) > 1<<20 {
			return fmt.Errorf("invalid notification")
		}
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO _rhiza_notify_requests(request_id) VALUES (?)`, notifyCommand.RequestID)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		publish = rows > 0
		commands = nil
		batched = true
	} else if kv {
		if err := m.applyKV(ctx, tx, kvCommand); err != nil {
			return err
		}
		commands = nil
		batched = true
	} else if !batched {
		commands = []types.SQLCommand{{SQL: string(value)}}
	}
	for _, command := range commands {
		if err := ValidateSQLCommand(command); err != nil {
			return fmt.Errorf("validate SQL request %q: %w", command.RequestID, err)
		}
		commandJSON, err := json.Marshal(command)
		if err != nil {
			return fmt.Errorf("encode SQL request %q: %w", command.RequestID, err)
		}
		if command.RequestID != "" {
			var existing string
			err := tx.QueryRowContext(ctx, `SELECT sql FROM _rhiza_requests WHERE request_id = ?`, command.RequestID).Scan(&existing)
			switch {
			case err == nil:
				if existing != string(commandJSON) && !(len(command.Args) == 0 && len(command.Statements) == 0 && !command.WantRows && existing == command.SQL) {
					continue
				}
				continue
			case err != sql.ErrNoRows:
				return fmt.Errorf("check SQL request %q: %w", command.RequestID, err)
			}
		}
		if _, err := tx.ExecContext(ctx, "SAVEPOINT rhiza_command"); err != nil {
			return err
		}
		result, executeErr := executeSQLCommand(ctx, tx, command)
		if executeErr != nil {
			if command.RequestID == "" {
				return executeErr
			}
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO rhiza_command"); err != nil {
				return err
			}
			result = types.SQLCommandResult{Error: executeErr.Error()}
		}
		if _, err := tx.ExecContext(ctx, "RELEASE rhiza_command"); err != nil {
			return err
		}
		if command.RequestID != "" {
			resultJSON, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("encode SQL result %q: %w", command.RequestID, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_requests(request_id, sql, result_json) VALUES (?, ?, ?)`, command.RequestID, commandJSON, resultJSON); err != nil {
				return fmt.Errorf("record SQL request %q: %w", command.RequestID, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_meta(key, value) VALUES ('applied_slot', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, strconv.FormatUint(slot, 10)); err != nil {
		return fmt.Errorf("persist applied slot: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_meta(key, value) VALUES ('applied_hash', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, hex.EncodeToString(hash[:])); err != nil {
		return fmt.Errorf("persist applied hash: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply: %w", err)
	}
	m.tip = slot
	m.tipHash = hash
	if publish {
		m.publishNotification(notifyCommand.Topic, notifyCommand.Payload)
	}
	return nil
}

func (m *Materializer) publishNotification(topic string, payload []byte) {
	m.notifyMu.Lock()
	defer m.notifyMu.Unlock()
	for _, sub := range m.subs {
		if sub.topic != topic {
			continue
		}
		select {
		case sub.ch <- append([]byte(nil), payload...):
		default: // bounded at-most-once delivery: slow subscribers observe a gap.
		}
	}
}

// Subscribe returns live, bounded, at-most-once notifications for a topic.
func (m *Materializer) Subscribe(topic string) (<-chan []byte, func()) {
	m.notifyMu.Lock()
	id := m.nextSub
	m.nextSub++
	ch := make(chan []byte, 64)
	m.subs[id] = notificationSubscription{topic: topic, ch: ch}
	m.notifyMu.Unlock()
	return ch, func() {
		m.notifyMu.Lock()
		delete(m.subs, id)
		m.notifyMu.Unlock()
	}
}

func (m *Materializer) applyKV(ctx context.Context, tx *sql.Tx, command types.KVCommand) error {
	if command.RequestID == "" || command.Key == "" || len(command.Key) > 1024 || len(command.Value) > 16<<20 {
		return fmt.Errorf("invalid KV command")
	}
	switch command.Operation {
	case "put", "delete", "cas":
	default:
		return fmt.Errorf("invalid KV operation %q", command.Operation)
	}
	commandJSON, err := json.Marshal(command)
	if err != nil {
		return err
	}
	var existingCommand, existingResult []byte
	err = tx.QueryRowContext(ctx, `SELECT command_json, result_json FROM _rhiza_kv_requests WHERE request_id = ?`, command.RequestID).Scan(&existingCommand, &existingResult)
	if err == nil {
		if !bytes.Equal(existingCommand, commandJSON) {
			return nil
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	result := types.KVCommandResult{Applied: true}
	switch command.Operation {
	case "put":
		_, err = tx.ExecContext(ctx, `INSERT INTO _rhiza_kv(key, value, expires_at_unix_ms) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at_unix_ms=excluded.expires_at_unix_ms`, command.Key, command.Value, command.ExpiresAtUnixMS)
	case "delete":
		var affected sql.Result
		affected, err = tx.ExecContext(ctx, `DELETE FROM _rhiza_kv WHERE key = ?`, command.Key)
		if err == nil {
			rows, _ := affected.RowsAffected()
			result.Applied = rows > 0
		}
	case "cas":
		var current []byte
		var expires int64
		lookupErr := tx.QueryRowContext(ctx, `SELECT value, expires_at_unix_ms FROM _rhiza_kv WHERE key = ?`, command.Key).Scan(&current, &expires)
		exists := lookupErr == nil && (expires == 0 || expires > command.ObservedAtUnixMS)
		if lookupErr != nil && lookupErr != sql.ErrNoRows {
			return lookupErr
		}
		result.Applied = exists == command.ExpectedExists && (!exists || bytes.Equal(current, command.Expected))
		if result.Applied {
			_, err = tx.ExecContext(ctx, `INSERT INTO _rhiza_kv(key, value, expires_at_unix_ms) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at_unix_ms=excluded.expires_at_unix_ms`, command.Key, command.Value, command.ExpiresAtUnixMS)
		}
	}
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO _rhiza_kv_requests(request_id, command_json, result_json) VALUES (?, ?, ?)`, command.RequestID, commandJSON, resultJSON)
	return err
}

// KVGet reads a non-expired value from the local materialized state.
func (m *Materializer) KVGet(ctx context.Context, key string, now time.Time) ([]byte, bool, error) {
	var value []byte
	err := m.reader().QueryRowContext(ctx, `SELECT value FROM _rhiza_kv WHERE key = ? AND (expires_at_unix_ms = 0 OR expires_at_unix_ms > ?)`, key, now.UnixMilli()).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	return value, err == nil, err
}

func (m *Materializer) KVRequestResult(ctx context.Context, requestID string) (types.KVCommandResult, error) {
	var data []byte
	if err := m.reader().QueryRowContext(ctx, `SELECT result_json FROM _rhiza_kv_requests WHERE request_id = ?`, requestID).Scan(&data); err != nil {
		return types.KVCommandResult{}, err
	}
	var result types.KVCommandResult
	err := json.Unmarshal(data, &result)
	return result, err
}

func (m *Materializer) KVRequestMatches(ctx context.Context, command types.KVCommand) (bool, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return false, err
	}
	var existing []byte
	err = m.reader().QueryRowContext(ctx, `SELECT command_json FROM _rhiza_kv_requests WHERE request_id = ?`, command.RequestID).Scan(&existing)
	if err == sql.ErrNoRows {
		return true, nil
	}
	return bytes.Equal(existing, encoded), err
}

// ValidateSQLCommand validates the replicated SQL trust boundary.
func ValidateSQLCommand(command types.SQLCommand) error {
	statements := command.Statements
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	if len(statements) == 0 || len(statements) > MaxSQLStatements {
		return fmt.Errorf("statement count must be between 1 and %d", MaxSQLStatements)
	}
	for _, statement := range statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("SQL is required")
		}
		if len(statement.SQL) > MaxSQLBytes {
			return fmt.Errorf("SQL exceeds %d bytes", MaxSQLBytes)
		}
		if len(statement.Args) > MaxSQLArgs {
			return fmt.Errorf("SQL has more than %d arguments", MaxSQLArgs)
		}
		for _, arg := range statement.Args {
			if _, err := sqlArg(arg); err != nil {
				return err
			}
		}
		keyword := strings.ToUpper(strings.Fields(strings.TrimSpace(statement.SQL))[0])
		switch keyword {
		case "PRAGMA", "ATTACH", "DETACH", "VACUUM", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE":
			return fmt.Errorf("%s is not allowed on the replicated write API", keyword)
		}
	}
	return nil
}

func executeSQLCommand(ctx context.Context, tx *sql.Tx, command types.SQLCommand) (types.SQLCommandResult, error) {
	statements := command.Statements
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	result := types.SQLCommandResult{Statements: make([]types.SQLStatementResult, 0, len(statements))}
	for _, statement := range statements {
		args := make([]any, len(statement.Args))
		for i, arg := range statement.Args {
			value, err := sqlArg(arg)
			if err != nil {
				return result, err
			}
			args[i] = value
		}
		if statement.WantRows {
			rows, err := tx.QueryContext(ctx, statement.SQL, args...)
			if err != nil {
				return result, err
			}
			statementResult, err := collectRows(rows, MaxReturningRows)
			rows.Close()
			if err != nil {
				return result, err
			}
			result.Statements = append(result.Statements, statementResult)
			continue
		}
		execResult, err := tx.ExecContext(ctx, statement.SQL, args...)
		if err != nil {
			return result, err
		}
		statementResult := types.SQLStatementResult{}
		statementResult.RowsAffected, _ = execResult.RowsAffected()
		statementResult.LastInsertID, _ = execResult.LastInsertId()
		result.Statements = append(result.Statements, statementResult)
	}
	return result, nil
}

func sqlArg(arg any) (any, error) {
	switch value := arg.(type) {
	case nil, bool, string, []byte, int64, float64:
		return value, nil
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return nil, fmt.Errorf("SQL argument overflows int64")
		}
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf("SQL argument overflows int64")
		}
		return int64(value), nil
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer, nil
		}
		floating, err := value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid numeric SQL argument %q", value)
		}
		return floating, nil
	default:
		return nil, fmt.Errorf("unsupported SQL argument type %T", arg)
	}
}

// NormalizeSQLArgs converts JSON numbers into database/sql argument types.
func NormalizeSQLArgs(args []any) ([]any, error) {
	result := make([]any, len(args))
	for i, arg := range args {
		value, err := sqlArg(arg)
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

func collectRows(rows *sql.Rows, limit int) (types.SQLStatementResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	result := types.SQLStatementResult{Columns: columns}
	for rows.Next() {
		if len(result.Rows) >= limit {
			return result, fmt.Errorf("result exceeds %d rows", limit)
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		result.Rows = append(result.Rows, values)
	}
	return result, rows.Err()
}

// RequestResult returns the persisted result for an idempotent request.
func (m *Materializer) RequestResult(ctx context.Context, requestID string) (types.SQLCommandResult, error) {
	var data []byte
	if err := m.reader().QueryRowContext(ctx, `SELECT result_json FROM _rhiza_requests WHERE request_id = ?`, requestID).Scan(&data); err != nil {
		return types.SQLCommandResult{}, err
	}
	if len(data) == 0 {
		return types.SQLCommandResult{Statements: []types.SQLStatementResult{}}, nil
	}
	var result types.SQLCommandResult
	if err := json.Unmarshal(data, &result); err != nil {
		return types.SQLCommandResult{}, err
	}
	return result, nil
}

func (m *Materializer) SQLRequestMatches(ctx context.Context, command types.SQLCommand) (bool, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return false, err
	}
	var existing string
	err = m.reader().QueryRowContext(ctx, `SELECT sql FROM _rhiza_requests WHERE request_id = ?`, command.RequestID).Scan(&existing)
	if err == sql.ErrNoRows {
		return true, nil
	}
	legacy := len(command.Args) == 0 && len(command.Statements) == 0 && !command.WantRows && existing == command.SQL
	return existing == string(encoded) || legacy, err
}

// Query executes a read query.
func (m *Materializer) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return m.reader().QueryContext(ctx, query, args...)
}

// QueryResult executes a bounded read query and materializes its result.
func (m *Materializer) QueryResult(ctx context.Context, query string, args []any) (types.SQLStatementResult, error) {
	if strings.TrimSpace(query) == "" || len(query) > MaxSQLBytes {
		return types.SQLStatementResult{}, fmt.Errorf("invalid SQL query")
	}
	normalized, err := NormalizeSQLArgs(args)
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	rows, err := m.Query(ctx, query, normalized...)
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	defer rows.Close()
	return collectRows(rows, MaxReturningRows)
}

func (m *Materializer) reader() *sql.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readers[0]
}

// QueryRow executes a single-row read query.
func (m *Materializer) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	m.mu.RLock()
	reader := m.readers[0]
	m.mu.RUnlock()

	return reader.QueryRowContext(ctx, query, args...)
}

// Exec executes a write query directly.
func (m *Materializer) Exec(ctx context.Context, query string, args ...interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.writer.ExecContext(ctx, query, args...)
	return err
}

// Snapshot creates a backup of the database.
func (m *Materializer) Snapshot(ctx context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	file, err := os.CreateTemp(filepath.Dir(m.dbPath), ".rhiza-snapshot-*.db")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	file.Close()
	os.Remove(path)
	defer os.Remove(path)
	if _, err := m.db.ExecContext(ctx, "VACUUM INTO ?", path); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	return m.encodeSnapshot(data)
}

// Restore restores from a snapshot.
func (m *Materializer) Restore(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty snapshot")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := filepath.Dir(m.dbPath)
	parts, err := prepareSnapshot(data, dir)
	if err != nil {
		return err
	}
	if parts.cleanup != nil {
		defer parts.cleanup()
	}
	file, err := os.CreateTemp(dir, ".rhiza-restore-*.db")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err == nil {
		_, err = file.Write(parts.sqlite)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close snapshot: %w", closeErr)
	}
	check, err := openSQLite((&url.URL{Scheme: "file", Path: filepath.ToSlash(tempPath)}).String()+"?mode=ro", false)
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	var status string
	err = check.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&status)
	check.Close()
	if err != nil || status != "ok" {
		return fmt.Errorf("invalid snapshot: quick_check=%q err=%v", status, err)
	}
	backupPath := m.dbPath + ".restore-backup"
	graphPath := filepath.Join(dir, "goraphdb")
	graphBackupPath := graphPath + ".restore-backup"
	os.Remove(backupPath)
	os.RemoveAll(graphBackupPath)
	if err := m.closeConnections(); err != nil {
		return err
	}
	os.Remove(m.dbPath + "-wal")
	os.Remove(m.dbPath + "-shm")
	if err := os.Rename(m.dbPath, backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("backup database: %w", err)
	}
	graphBackedUp := false
	graphInstalled := false
	if parts.graphDir != "" {
		if err := os.Rename(graphPath, graphBackupPath); err == nil {
			graphBackedUp = true
		} else if !os.IsNotExist(err) {
			_ = os.Rename(backupPath, m.dbPath)
			return fmt.Errorf("backup graph database: %w", err)
		}
		if err := os.Rename(parts.graphDir, graphPath); err != nil {
			if graphBackedUp {
				_ = os.Rename(graphBackupPath, graphPath)
			}
			_ = os.Rename(backupPath, m.dbPath)
			return fmt.Errorf("install graph snapshot: %w", err)
		}
		graphInstalled = true
	}
	if err := os.Rename(tempPath, m.dbPath); err != nil {
		if graphInstalled {
			_ = os.RemoveAll(graphPath)
		}
		if graphBackedUp {
			_ = os.Rename(graphBackupPath, graphPath)
		}
		_ = os.Rename(backupPath, m.dbPath)
		if reopenErr := m.reopen(); reopenErr != nil {
			return fmt.Errorf("install snapshot: %w; reopen original: %v", err, reopenErr)
		}
		return fmt.Errorf("install snapshot: %w", err)
	}
	restored, err := Open(m.dbPath, m.readersN)
	if err == nil {
		err = restored.validateRestoredSnapshot()
	}
	if err != nil {
		if restored != nil {
			_ = restored.Close()
		}
		os.Remove(m.dbPath)
		if graphInstalled {
			_ = os.RemoveAll(graphPath)
		}
		if graphBackedUp {
			_ = os.Rename(graphBackupPath, graphPath)
		}
		_ = os.Rename(backupPath, m.dbPath)
		if reopenErr := m.reopen(); reopenErr != nil {
			return fmt.Errorf("open restored snapshot: %w; reopen original: %v", err, reopenErr)
		}
		return fmt.Errorf("open restored snapshot: %w", err)
	}
	os.Remove(backupPath)
	os.RemoveAll(graphBackupPath)
	m.db, m.writer, m.readers, m.graph = restored.db, restored.writer, restored.readers, restored.graph
	m.tip, m.tipHash = restored.tip, restored.tipHash
	restored.db, restored.writer, restored.readers, restored.graph = nil, nil, nil, nil
	return nil
}

func (m *Materializer) reopen() error {
	reopened, err := Open(m.dbPath, m.readersN)
	if err != nil {
		return err
	}
	m.db, m.writer, m.readers, m.graph = reopened.db, reopened.writer, reopened.readers, reopened.graph
	m.tip, m.tipHash = reopened.tip, reopened.tipHash
	reopened.db, reopened.writer, reopened.readers, reopened.graph = nil, nil, nil, nil
	return nil
}

// Tip returns the last applied slot.
func (m *Materializer) Tip() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tip
}

// Close closes all connections.
func (m *Materializer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeConnections()
}

func (m *Materializer) closeConnections() error {
	if m.graph != nil {
		m.graph.close()
		m.graph = nil
	}
	if m.db != nil {
		m.db.Close()
		m.db = nil
	}
	if m.writer != nil {
		m.writer.Close()
		m.writer = nil
	}
	for _, r := range m.readers {
		if r != nil {
			r.Close()
		}
	}
	m.readers = nil
	return nil
}

// Health check
func (m *Materializer) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := m.db.PingContext(ctx); err != nil {
		return err
	}
	return m.graphHealth()
}
