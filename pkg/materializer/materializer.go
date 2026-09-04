package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
)

const (
	MaxSQLBytes            = 256 << 10
	MaxSQLStatements       = 64
	MaxSQLArgs             = 999
	MaxReturningRows       = 10_000
	MaxResultBytes         = 16 << 20
	MaxMutationResultBytes = 1 << 20
	MaxCellBytes           = 1 << 20

	notificationSubscriberLimit = 64
	notificationQueueDepth      = 1
	notificationDispatchDepth   = 64
	recentSQLReceiptLimit       = 4096
	// ponytail: fixed-size epochs bound memory; saturation only adds SQLite fallbacks.
	sqlReceiptBloomBytes = 4 << 20
)

const migrationTableDDL = `CREATE TABLE IF NOT EXISTS _rhiza_migrations (
	version INTEGER PRIMARY KEY CHECK(version > 0),
	name TEXT NOT NULL,
	checksum TEXT NOT NULL CHECK(length(checksum) = 64)
) STRICT`

// Materializer applies decided values to SQLite.
// Uses single writer, multiple readers pattern like Hiqlite.
type Materializer struct {
	db                 *sql.DB
	writer             *sql.DB
	readers            []*sql.DB
	mu                 sync.RWMutex
	tip                uint64
	stateTip           uint64
	tipHash            [32]byte
	dbPath             string
	readersN           int
	idempotencyWindow  uint64
	recentSQLReceipts  map[string]storedReceipt
	pendingSQLReceipts []pendingSQLReceipt
	sqlReceipts        sqlReceiptBloom
	graph              *graphState
	notifyMu           sync.Mutex
	nextSub            uint64
	subs               map[uint64]notificationSubscription
	notifyQueue        chan pendingNotification
	notifyStop         chan struct{}
	notifyStopOnce     sync.Once
	notifyWG           sync.WaitGroup
	notifyDrops        atomic.Uint64
	walCheckpointStop  chan struct{}
	walCheckpointOnce  sync.Once
	walCheckpointWG    sync.WaitGroup
}

type notificationSubscription struct {
	topic string
	ch    chan []byte
}

type snapshotParts struct {
	sqlitePath string
	graphDir   string
	cleanup    func()
}

type restoreState struct {
	Phase            string `json:"phase"`
	InstallGraph     bool   `json:"install_graph"`
	GraphHadOriginal bool   `json:"graph_had_original"`
}

type CheckpointRole string

const (
	CheckpointSQLite    CheckpointRole = "sqlite"
	CheckpointGraphData CheckpointRole = "graph-data"
)

type CheckpointFile struct {
	Role CheckpointRole
	Path string
}

type sqliteSnapshot struct {
	conn  *sql.Conn
	index uint64
}

// Open opens or creates a materializer.
func Open(dbPath string, readerCount int, idempotencyWindow ...uint64) (*Materializer, error) {
	if err := recoverRestore(dbPath); err != nil {
		return nil, fmt.Errorf("recover interrupted restore: %w", err)
	}
	return openMaterializer(dbPath, readerCount, idempotencyWindow...)
}

func openMaterializer(dbPath string, readerCount int, idempotencyWindow ...uint64) (*Materializer, error) {
	if readerCount < 1 {
		return nil, fmt.Errorf("reader count must be positive")
	}
	window := uint64(types.DefaultIdempotencyWindowSlots)
	if len(idempotencyWindow) != 0 {
		window = idempotencyWindow[0]
	}
	if window < 1024 || window > 1_048_576 {
		return nil, fmt.Errorf("idempotency window must be between 1024 and 1048576 slots")
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
	writerDSN := fileURL + "?_pragma=journal_mode(wal)&_pragma=synchronous(normal)&_pragma=wal_autocheckpoint(0)&_pragma=busy_timeout(5000)"
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

	// database/sql already provides the reader pool; one handle avoids opening
	// N independent pools while only ever selecting readers[0].
	reader, err := openSQLite(readerDSN, false)
	if err != nil {
		db.Close()
		writer.Close()
		return nil, fmt.Errorf("open reader: %w", err)
	}
	reader.SetMaxOpenConns(readerCount)
	reader.SetMaxIdleConns(readerCount)
	readers := []*sql.DB{reader}

	m := &Materializer{
		db:                db,
		writer:            writer,
		readers:           readers,
		dbPath:            dbPath,
		readersN:          readerCount,
		idempotencyWindow: window,
		recentSQLReceipts: make(map[string]storedReceipt),
		subs:              make(map[uint64]notificationSubscription),
		notifyQueue:       make(chan pendingNotification, notificationDispatchDepth),
		notifyStop:        make(chan struct{}),
		walCheckpointStop: make(chan struct{}),
	}
	m.notifyWG.Add(1)
	go m.runNotifications()

	// Initialize schema
	if err := m.initSchema(); err != nil {
		m.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if err := m.loadTip(existing); err != nil {
		m.Close()
		return nil, fmt.Errorf("load applied slot: %w", err)
	}
	if err := m.loadSQLReceiptBloom(); err != nil {
		m.Close()
		return nil, fmt.Errorf("load SQL receipts: %w", err)
	}
	graph, err := openGraph(filepath.Join(filepath.Dir(dbPath), "latticedb"), m.tip, window)
	if err != nil {
		m.Close()
		return nil, fmt.Errorf("open graph materializer: %w", err)
	}
	m.graph = graph
	m.walCheckpointWG.Add(1)
	go m.runSQLiteCheckpoints()

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
		if _, err := m.writer.Exec(`INSERT INTO _rhiza_meta(key, value) VALUES ('applied_slot', '0'), ('state_slot', '0'), ('applied_hash', ?)`, zeroHash); err != nil {
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
	var stateValue string
	if err := m.writer.QueryRow(`SELECT value FROM _rhiza_meta WHERE key = 'state_slot'`).Scan(&stateValue); err != nil {
		return fmt.Errorf("load state slot: %w", err)
	}
	m.stateTip, err = strconv.ParseUint(stateValue, 10, 64)
	if err != nil || m.stateTip > m.tip {
		return fmt.Errorf("invalid state slot")
	}
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
		CREATE TABLE IF NOT EXISTS _rhiza_kv (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL,
			expires_at_unix_ms INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS _rhiza_kv_expiry
		ON _rhiza_kv(expires_at_unix_ms) WHERE expires_at_unix_ms > 0;
		CREATE TABLE IF NOT EXISTS _rhiza_idempotency (
			kind INTEGER NOT NULL,
			request_id BLOB NOT NULL,
			fingerprint BLOB NOT NULL CHECK(length(fingerprint) = 32),
			commit_slot INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL,
			rows_affected INTEGER NOT NULL,
			last_insert_id INTEGER NOT NULL,
			applied INTEGER NOT NULL,
			sql_result BLOB NOT NULL DEFAULT X'',
			PRIMARY KEY(kind, request_id)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot);
		`)
	if err != nil {
		return err
	}
	var hasSQLResult int
	if err := m.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('_rhiza_idempotency') WHERE name = 'sql_result'`).Scan(&hasSQLResult); err != nil {
		return err
	}
	if hasSQLResult == 0 {
		if _, err := m.db.Exec(`ALTER TABLE _rhiza_idempotency ADD COLUMN sql_result BLOB NOT NULL DEFAULT X''`); err != nil {
			return err
		}
	}
	if _, err := m.db.Exec(migrationTableDDL); err != nil {
		return err
	}
	if err := validateMigrationTable(m.db); err != nil {
		return err
	}
	var incompatible int
	if err := m.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('_rhiza_requests','_rhiza_kv_requests','_rhiza_notify_requests')`).Scan(&incompatible); err != nil {
		return err
	}
	if incompatible != 0 {
		return fmt.Errorf("incompatible idempotency schema; rebuild from the certified log")
	}
	return nil
}

func validateMigrationTable(db *sql.DB) error {
	var schema string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='_rhiza_migrations'`).Scan(&schema); err != nil {
		return err
	}
	wantSchema := `CREATE TABLE _rhiza_migrations ( version INTEGER PRIMARY KEY CHECK(version > 0), name TEXT NOT NULL, checksum TEXT NOT NULL CHECK(length(checksum) = 64) ) STRICT`
	if strings.Join(strings.Fields(schema), " ") != wantSchema {
		return fmt.Errorf("incompatible _rhiza_migrations schema")
	}
	rows, err := db.Query(`PRAGMA table_info('_rhiza_migrations')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type column struct {
		name, kind       string
		notNull, primary int
	}
	var got []column
	for rows.Next() {
		var cid int
		var item column
		var defaultValue any
		if err := rows.Scan(&cid, &item.name, &item.kind, &item.notNull, &defaultValue, &item.primary); err != nil {
			return err
		}
		got = append(got, item)
	}
	want := []column{{"version", "INTEGER", 0, 1}, {"name", "TEXT", 1, 0}, {"checksum", "TEXT", 1, 0}}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(got) != len(want) {
		return fmt.Errorf("incompatible _rhiza_migrations schema")
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("incompatible _rhiza_migrations schema")
		}
	}
	return nil
}

type storedReceipt struct {
	fingerprint [32]byte
	receipt     types.MutationReceipt
	sqlResult   types.SQLCommandResult
}

type sqlReceiptBloomEpoch struct {
	epoch uint64
	valid bool
	bits  []byte
}

type sqlReceiptBloom struct {
	epochs [2]sqlReceiptBloomEpoch
	window uint64
}

func (b *sqlReceiptBloom) add(requestID string, slot uint64) {
	epoch := slot / b.window
	filter := &b.epochs[epoch%uint64(len(b.epochs))]
	if !filter.valid || filter.epoch != epoch {
		if filter.bits == nil {
			filter.bits = make([]byte, sqlReceiptBloomBytes)
		} else {
			clear(filter.bits)
		}
		filter.epoch, filter.valid = epoch, true
	}
	hash := sha256.Sum256([]byte(requestID))
	for i := range 3 {
		bit := bloomBit(hash, i)
		filter.bits[bit>>3] |= 1 << (bit & 7)
	}
}

func (b *sqlReceiptBloom) mightContain(requestID string, tip uint64) bool {
	epoch := tip / b.window
	hash := sha256.Sum256([]byte(requestID))
	for i := range b.epochs {
		filter := &b.epochs[i]
		if !filter.valid || filter.epoch > epoch || epoch-filter.epoch > 1 {
			continue
		}
		found := true
		for j := range 3 {
			bit := bloomBit(hash, j)
			if filter.bits[bit>>3]&(1<<(bit&7)) == 0 {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func bloomBit(hash [32]byte, index int) uint32 {
	offset := index * 4
	value := uint32(hash[offset]) | uint32(hash[offset+1])<<8 | uint32(hash[offset+2])<<16 | uint32(hash[offset+3])<<24
	return value & (sqlReceiptBloomBytes*8 - 1)
}

func (m *Materializer) loadSQLReceiptBloom() error {
	m.sqlReceipts.window = m.idempotencyWindow
	floor := uint64(0)
	if m.tip >= m.idempotencyWindow {
		floor = m.tip - m.idempotencyWindow + 1
	}
	rows, err := m.db.Query(`SELECT request_id, commit_slot FROM _rhiza_idempotency WHERE kind = ? AND commit_slot >= ?`, types.MutationSQL, floor)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var requestID string
		var slot uint64
		if err := rows.Scan(&requestID, &slot); err != nil {
			return err
		}
		m.sqlReceipts.add(requestID, slot)
	}
	return rows.Err()
}

func scanReceipt(scanner interface{ Scan(...any) error }, window uint64) (storedReceipt, error) {
	var record storedReceipt
	var fingerprint []byte
	var encodedResult []byte
	var applied int
	err := scanner.Scan(&fingerprint, &record.receipt.Slot, &record.receipt.Status, &record.receipt.ErrorCode, &record.receipt.RowsAffected, &record.receipt.LastInsertID, &applied, &encodedResult)
	if err != nil {
		return storedReceipt{}, err
	}
	if len(fingerprint) != sha256.Size {
		return storedReceipt{}, fmt.Errorf("invalid idempotency fingerprint")
	}
	copy(record.fingerprint[:], fingerprint)
	record.receipt.Applied = applied != 0
	record.receipt.RetryThroughSlot = record.receipt.Slot + window - 1
	if len(encodedResult) != 0 {
		var err error
		record.sqlResult, err = decodeSQLResult(encodedResult)
		if err != nil {
			return storedReceipt{}, fmt.Errorf("decode SQL result: %w", err)
		}
	}
	return record, nil
}

func receiptQuery() string {
	return `SELECT fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result FROM _rhiza_idempotency WHERE kind = ? AND request_id = ?`
}

func (m *Materializer) MutationReceipt(ctx context.Context, kind types.MutationKind, requestID string) (types.MutationReceipt, bool, error) {
	reader, err := m.reader()
	if err != nil {
		return types.MutationReceipt{}, false, err
	}
	record, err := scanReceipt(reader.QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return types.MutationReceipt{}, false, nil
	}
	if err != nil {
		return types.MutationReceipt{}, false, err
	}
	if m.Tip() > record.receipt.RetryThroughSlot {
		return types.MutationReceipt{}, false, nil
	}
	return record.receipt, true, nil
}

func (m *Materializer) requestMatches(ctx context.Context, kind types.MutationKind, requestID string, fingerprint [32]byte) (bool, bool, error) {
	reader, err := m.reader()
	if err != nil {
		return false, false, err
	}
	record, err := scanReceipt(reader.QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if m.Tip() > record.receipt.RetryThroughSlot {
		return true, false, nil
	}
	return record.fingerprint == fingerprint, true, nil
}

func (m *Materializer) receiptInTx(ctx context.Context, tx *sql.Tx, kind types.MutationKind, requestID string) (storedReceipt, bool, error) {
	record, err := scanReceipt(tx.QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return storedReceipt{}, false, nil
	}
	return record, err == nil, err
}

func insertReceipt(ctx context.Context, tx *sql.Tx, kind types.MutationKind, requestID string, fingerprint [32]byte, receipt types.MutationReceipt) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, X'')`, kind, requestID, fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied)
	return err
}

func encodeSQLResult(result types.SQLCommandResult) ([]byte, error) {
	if len(result.Statements) == 0 {
		return nil, nil
	}
	var encoded bytes.Buffer
	encoded.WriteString("RSQL")
	writeUint32(&encoded, uint32(len(result.Statements)))
	for _, statement := range result.Statements {
		writeInt64(&encoded, statement.RowsAffected)
		writeInt64(&encoded, statement.LastInsertID)
		writeUint32(&encoded, uint32(len(statement.Columns)))
		for _, column := range statement.Columns {
			writeBytes(&encoded, []byte(column))
		}
		writeUint32(&encoded, uint32(len(statement.Rows)))
		for _, row := range statement.Rows {
			writeUint32(&encoded, uint32(len(row)))
			for _, value := range row {
				if err := writeSQLValue(&encoded, value); err != nil {
					return nil, err
				}
			}
		}
	}
	writeBytes(&encoded, []byte(result.Error))
	if encoded.Len() > MaxMutationResultBytes {
		return nil, fmt.Errorf("stored SQL result exceeds %d bytes", MaxMutationResultBytes)
	}
	return encoded.Bytes(), nil
}

func writeUint32(dst *bytes.Buffer, value uint32) { _ = binary.Write(dst, binary.BigEndian, value) }
func writeInt64(dst *bytes.Buffer, value int64)   { _ = binary.Write(dst, binary.BigEndian, value) }
func writeBytes(dst *bytes.Buffer, value []byte) {
	writeUint32(dst, uint32(len(value)))
	dst.Write(value)
}

func writeSQLValue(dst *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case nil:
		dst.WriteByte(0)
	case int64:
		dst.WriteByte(1)
		writeInt64(dst, value)
	case float64:
		dst.WriteByte(2)
		writeInt64(dst, int64(math.Float64bits(value)))
	case string:
		dst.WriteByte(3)
		writeBytes(dst, []byte(value))
	case []byte:
		dst.WriteByte(4)
		writeBytes(dst, value)
	default:
		return fmt.Errorf("unsupported SQL result type %T", value)
	}
	return nil
}

func decodeSQLResult(encoded []byte) (types.SQLCommandResult, error) {
	reader := bytes.NewReader(encoded)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != "RSQL" {
		return types.SQLCommandResult{}, fmt.Errorf("invalid result encoding")
	}
	count, err := readUint32(reader)
	if err != nil || count > MaxSQLStatements {
		return types.SQLCommandResult{}, fmt.Errorf("invalid statement count")
	}
	result := types.SQLCommandResult{Statements: make([]types.SQLStatementResult, count)}
	for i := range result.Statements {
		statement := &result.Statements[i]
		if statement.RowsAffected, err = readInt64(reader); err != nil {
			return types.SQLCommandResult{}, err
		}
		if statement.LastInsertID, err = readInt64(reader); err != nil {
			return types.SQLCommandResult{}, err
		}
		columns, readErr := readUint32(reader)
		if readErr != nil || columns > MaxSQLArgs {
			return types.SQLCommandResult{}, fmt.Errorf("invalid column count")
		}
		statement.Columns = make([]string, columns)
		for j := range statement.Columns {
			value, readErr := readBytes(reader)
			if readErr != nil {
				return types.SQLCommandResult{}, readErr
			}
			statement.Columns[j] = string(value)
		}
		rowCount, readErr := readUint32(reader)
		if readErr != nil || rowCount > MaxReturningRows {
			return types.SQLCommandResult{}, fmt.Errorf("invalid row count")
		}
		statement.Rows = make([][]any, rowCount)
		for j := range statement.Rows {
			cellCount, readErr := readUint32(reader)
			if readErr != nil || cellCount != columns {
				return types.SQLCommandResult{}, fmt.Errorf("invalid cell count")
			}
			statement.Rows[j] = make([]any, cellCount)
			for k := range statement.Rows[j] {
				statement.Rows[j][k], readErr = readSQLValue(reader)
				if readErr != nil {
					return types.SQLCommandResult{}, readErr
				}
			}
		}
	}
	errorText, err := readBytes(reader)
	if err != nil || reader.Len() != 0 {
		return types.SQLCommandResult{}, fmt.Errorf("invalid trailing result data")
	}
	result.Error = string(errorText)
	return result, nil
}

func readUint32(reader *bytes.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}
func readInt64(reader *bytes.Reader) (int64, error) {
	var value int64
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}
func readBytes(reader *bytes.Reader) ([]byte, error) {
	size, err := readUint32(reader)
	if err != nil || uint64(size) > uint64(reader.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	value := make([]byte, size)
	_, err = io.ReadFull(reader, value)
	return value, err
}
func readSQLValue(reader *bytes.Reader) (any, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		return nil, nil
	case 1:
		return readInt64(reader)
	case 2:
		bits, err := readInt64(reader)
		return math.Float64frombits(uint64(bits)), err
	case 3:
		value, err := readBytes(reader)
		return string(value), err
	case 4:
		return readBytes(reader)
	default:
		return nil, fmt.Errorf("invalid SQL value tag %d", tag)
	}
}

func insertReceiptsIfAbsent(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, kind types.MutationKind, receipts []pendingSQLReceipt) (bool, error) {
	if len(receipts) == 1 {
		pending := receipts[0]
		receipt := pending.record.receipt
		encodedResult, err := encodeSQLResult(pending.record.sqlResult)
		if err != nil {
			return false, err
		}
		result, err := execPrepared(ctx, tx, prepared, `INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(kind, request_id) DO NOTHING`, kind, pending.requestID, pending.record.fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied, encodedResult)
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		return rows == 1, err
	}
	const receiptArgs = 10
	if len(receipts) > MaxSQLArgs/receiptArgs {
		for len(receipts) != 0 {
			n := min(len(receipts), MaxSQLArgs/receiptArgs)
			inserted, err := insertReceiptsIfAbsent(ctx, tx, prepared, kind, receipts[:n])
			if err != nil || !inserted {
				return inserted, err
			}
			receipts = receipts[n:]
		}
		return true, nil
	}
	var query strings.Builder
	query.WriteString(`INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES `)
	args := make([]any, 0, len(receipts)*receiptArgs)
	for i, pending := range receipts {
		if i != 0 {
			query.WriteByte(',')
		}
		query.WriteString(`(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		receipt := pending.record.receipt
		encodedResult, err := encodeSQLResult(pending.record.sqlResult)
		if err != nil {
			return false, err
		}
		args = append(args, kind, pending.requestID, pending.record.fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied, encodedResult)
	}
	query.WriteString(` ON CONFLICT(kind, request_id) DO NOTHING`)
	result, err := execPrepared(ctx, tx, prepared, query.String(), args...)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == int64(len(receipts)), err
}

func (m *Materializer) pruneReceipts(ctx context.Context, tx *sql.Tx, tip uint64) error {
	if tip <= m.idempotencyWindow {
		return nil
	}
	floor := tip - m.idempotencyWindow + 1
	_, err := tx.ExecContext(ctx, `DELETE FROM _rhiza_idempotency WHERE commit_slot < ?`, floor)
	return err
}

type pendingNotification struct {
	topic   string
	payload []byte
}

type pendingSQLReceipt struct {
	requestID string
	record    storedReceipt
}

// Apply applies one decided value.
func (m *Materializer) Apply(ctx context.Context, slot uint64, value []byte) error {
	return m.ApplyBatch(ctx, []quepaxa.DecidedValue{{Slot: quepaxa.Slot(slot), Value: value}})
}

// ApplyBatch materializes a contiguous decision page with one SQLite commit.
func (m *Materializer) ApplyBatch(ctx context.Context, decisions []quepaxa.DecidedValue) error {
	if len(decisions) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return sql.ErrConnDone
	}
	tx, err := m.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply batch: %w", err)
	}
	defer tx.Rollback()
	statements := make(map[string]*sql.Stmt)
	defer func() {
		for _, statement := range statements {
			statement.Close()
		}
	}()
	oldTip, oldStateTip, oldHash := m.tip, m.stateTip, m.tipHash
	pending := make([]pendingNotification, 0)
	m.pendingSQLReceipts = m.pendingSQLReceipts[:0]
	for _, decision := range decisions {
		slot := uint64(decision.Slot)
		hash := sha256.Sum256(decision.Value)
		if slot <= m.tip {
			if slot == m.tip && hash != m.tipHash {
				m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
				return fmt.Errorf("applied slot %d hash conflict", slot)
			}
			continue
		}
		if slot != m.tip+1 {
			m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
			return fmt.Errorf("apply slot gap: have %d, got %d", m.tip, slot)
		}
		// oldTip is the last SQLite-durable tip; m.tip advances before this batch commits.
		if err := m.applyValueLocked(ctx, tx, statements, slot, decision.Value, hash, oldTip, &pending); err != nil {
			m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
			return err
		}
		m.tip, m.tipHash = slot, hash
	}
	if err := tx.Commit(); err != nil {
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		return fmt.Errorf("commit apply batch: %w", err)
	}
	for _, pending := range m.pendingSQLReceipts {
		m.sqlReceipts.add(pending.requestID, pending.record.receipt.Slot)
	}
	pendingReceipts := m.pendingSQLReceipts
	if len(pendingReceipts) > recentSQLReceiptLimit {
		pendingReceipts = pendingReceipts[len(pendingReceipts)-recentSQLReceiptLimit:]
	}
	if len(m.recentSQLReceipts)+len(pendingReceipts) > recentSQLReceiptLimit {
		clear(m.recentSQLReceipts)
	}
	for _, pending := range pendingReceipts {
		record := pending.record
		record.sqlResult = types.SQLCommandResult{}
		m.recentSQLReceipts[pending.requestID] = record
	}
	for _, notification := range pending {
		m.enqueueNotification(notification)
	}
	return nil
}

func (m *Materializer) runNotifications() {
	defer m.notifyWG.Done()
	for {
		select {
		case <-m.notifyStop:
			return
		case notification := <-m.notifyQueue:
			m.publishNotification(notification.topic, notification.payload)
		}
	}
}

func (m *Materializer) runSQLiteCheckpoints() {
	defer m.walCheckpointWG.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.walCheckpointStop:
			return
		case <-ticker.C:
			m.mu.RLock()
			db := m.db
			m.mu.RUnlock()
			if db != nil {
				_, _ = db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
			}
		}
	}
}

func (m *Materializer) stopSQLiteCheckpoints() {
	m.walCheckpointOnce.Do(func() { close(m.walCheckpointStop) })
	m.walCheckpointWG.Wait()
}

func (m *Materializer) enqueueNotification(notification pendingNotification) {
	select {
	case m.notifyQueue <- notification:
	default:
		m.notifyDrops.Add(1)
	}
}

func (m *Materializer) applyValueLocked(ctx context.Context, tx *sql.Tx, statements map[string]*sql.Stmt, slot uint64, value []byte, hash [32]byte, confirmedGraphThrough uint64, pending *[]pendingNotification) error {
	if err := m.pruneReceipts(ctx, tx, slot); err != nil {
		return fmt.Errorf("prune idempotency receipts: %w", err)
	}
	graphCommands, graph, err := types.DecodeGraphBatch(value)
	if err != nil {
		return fmt.Errorf("decode graph batch: %w", err)
	}
	if err := m.applyGraph(ctx, slot, value, graphCommands, graph, confirmedGraphThrough); err != nil {
		return err
	}

	notifyCommand, notify, err := types.DecodeNotifyCommand(value)
	if err != nil {
		return fmt.Errorf("decode notification: %w", err)
	}
	kvCommand, kv, err := types.DecodeKVCommand(value)
	if err != nil {
		return fmt.Errorf("decode KV command: %w", err)
	}
	kvCommands, kvBatch, err := types.DecodeKVBatch(value)
	if err != nil {
		return fmt.Errorf("decode KV batch: %w", err)
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
	} else if _, checkpoint, err := types.DecodeCheckpointSeal(value); err != nil {
		return fmt.Errorf("decode checkpoint seal: %w", err)
	} else if checkpoint {
		commands = nil
		batched = true
	} else if graph {
		commands = nil
		batched = true
	}
	mutatesState := graph || notify || kv || kvBatch || len(commands) != 0 || !batched
	publish := false
	if notify {
		if notifyCommand.RequestID == "" || len(notifyCommand.RequestID) > types.MaxRequestIDBytes || notifyCommand.Topic == "" || len(notifyCommand.Topic) > 256 || len(notifyCommand.Payload) > 1<<20 {
			return fmt.Errorf("invalid notification")
		}
		fingerprint, err := types.NotifyFingerprint(notifyCommand)
		if err != nil {
			return err
		}
		_, found, err := m.receiptInTx(ctx, tx, types.MutationNotify, notifyCommand.RequestID)
		if err != nil {
			return err
		}
		if !found {
			receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted, Applied: true}
			if err := insertReceipt(ctx, tx, types.MutationNotify, notifyCommand.RequestID, fingerprint, receipt); err != nil {
				return err
			}
			publish = true
		}
		commands = nil
		batched = true
	} else if kv {
		if err := m.applyKV(ctx, tx, slot, kvCommand); err != nil {
			return err
		}
		commands = nil
		batched = true
	} else if kvBatch {
		for _, command := range kvCommands {
			if err := m.applyKV(ctx, tx, slot, command); err != nil {
				return err
			}
		}
		commands = nil
		batched = true
	} else if !batched {
		commands = []types.SQLCommand{{SQL: string(value)}}
	}
	pendingStart := len(m.pendingSQLReceipts)
	for _, command := range commands {
		if err := ValidateSQLCommand(command); err != nil {
			return fmt.Errorf("validate SQL request %q: %w", command.RequestID, err)
		}
		fingerprint, err := types.SQLFingerprint(command)
		if err != nil {
			return fmt.Errorf("encode SQL request %q: %w", command.RequestID, err)
		}
		if command.RequestID != "" {
			duplicate := false
			for _, pending := range m.pendingSQLReceipts[pendingStart:] {
				if pending.requestID == command.RequestID {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			mightExist := m.sqlReceipts.mightContain(command.RequestID, m.tip)
			if !mightExist {
				for _, pending := range m.pendingSQLReceipts[:pendingStart] {
					if pending.requestID == command.RequestID {
						mightExist = true
						break
					}
				}
			}
			if mightExist {
				if _, found, err := m.receiptInTx(ctx, tx, types.MutationSQL, command.RequestID); err != nil {
					return err
				} else if found {
					continue
				}
			}
		}
		if _, err := execPrepared(ctx, tx, statements, "SAVEPOINT rhiza_command"); err != nil {
			return err
		}
		result, executeErr := executeSQLCommand(ctx, tx, statements, command)
		receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted}
		if executeErr != nil {
			if command.RequestID == "" {
				return executeErr
			}
			if _, err := execPrepared(ctx, tx, statements, "ROLLBACK TO rhiza_command"); err != nil {
				return err
			}
			result = types.SQLCommandResult{}
			receipt.Status = types.MutationRejected
			receipt.ErrorCode = "execution_failed"
		} else {
			for _, statement := range result.Statements {
				receipt.RowsAffected += statement.RowsAffected
				receipt.LastInsertID = statement.LastInsertID
			}
		}
		if command.RequestID != "" {
			receipt.RetryThroughSlot = slot + m.idempotencyWindow - 1
			storedResult := types.SQLCommandResult{}
			for _, statement := range command.Statements {
				if statement.WantRows {
					storedResult = result
					break
				}
			}
			if len(command.Statements) == 0 && command.WantRows {
				storedResult = result
			}
			m.pendingSQLReceipts = append(m.pendingSQLReceipts, pendingSQLReceipt{requestID: command.RequestID, record: storedReceipt{fingerprint: fingerprint, receipt: receipt, sqlResult: storedResult}})
		}
		if _, err := execPrepared(ctx, tx, statements, "RELEASE rhiza_command"); err != nil {
			return err
		}
	}
	pendingReceipts := m.pendingSQLReceipts[pendingStart:]
	if len(pendingReceipts) != 0 {
		inserted, err := insertReceiptsIfAbsent(ctx, tx, statements, types.MutationSQL, pendingReceipts)
		if err != nil {
			return fmt.Errorf("record SQL receipts: %w", err)
		}
		if !inserted {
			return fmt.Errorf("SQL request appeared during apply")
		}
	}
	slotValue := strconv.FormatUint(slot, 10)
	hashValue := hex.EncodeToString(hash[:])
	if mutatesState {
		if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_meta(key, value) VALUES ('applied_slot', ?), ('applied_hash', ?), ('state_slot', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, slotValue, hashValue, slotValue); err != nil {
			return fmt.Errorf("persist applied state: %w", err)
		}
		m.stateTip = slot
	} else if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_meta(key, value) VALUES ('applied_slot', ?), ('applied_hash', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, slotValue, hashValue); err != nil {
		return fmt.Errorf("persist applied position: %w", err)
	}
	if publish {
		*pending = append(*pending, pendingNotification{topic: notifyCommand.Topic, payload: notifyCommand.Payload})
	}
	return nil
}

func (m *Materializer) publishNotification(topic string, payload []byte) {
	m.notifyMu.Lock()
	channels := make([]chan []byte, 0, len(m.subs))
	for _, sub := range m.subs {
		if sub.topic == topic {
			channels = append(channels, sub.ch)
		}
	}
	m.notifyMu.Unlock()
	for _, ch := range channels {
		if len(ch) == cap(ch) {
			m.notifyDrops.Add(1)
			continue
		}
		select {
		case ch <- append([]byte(nil), payload...):
		default: // A receiver raced another delivery; slow subscribers observe a gap.
			m.notifyDrops.Add(1)
		}
	}
}

// Subscribe returns live, bounded, at-most-once notifications for a topic.
func (m *Materializer) Subscribe(topic string) (<-chan []byte, func(), error) {
	m.notifyMu.Lock()
	select {
	case <-m.notifyStop:
		m.notifyMu.Unlock()
		return nil, nil, fmt.Errorf("materializer is closed")
	default:
	}
	if len(m.subs) >= notificationSubscriberLimit {
		m.notifyMu.Unlock()
		return nil, nil, fmt.Errorf("notification subscriber limit reached")
	}
	id := m.nextSub
	m.nextSub++
	ch := make(chan []byte, notificationQueueDepth)
	m.subs[id] = notificationSubscription{topic: topic, ch: ch}
	m.notifyMu.Unlock()
	return ch, func() {
		m.notifyMu.Lock()
		delete(m.subs, id)
		m.notifyMu.Unlock()
	}, nil
}

// NotificationDrops reports live at-most-once deliveries discarded before
// delivery, including confirmation failures and saturated queues.
func (m *Materializer) NotificationDrops() uint64 {
	return m.notifyDrops.Load()
}

func (m *Materializer) applyKV(ctx context.Context, tx *sql.Tx, slot uint64, command types.KVCommand) error {
	if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes || command.Key == "" || len(command.Key) > 1024 || len(command.Value) > 16<<20 {
		return fmt.Errorf("invalid KV command")
	}
	switch command.Operation {
	case "put", "delete", "cas":
	default:
		return fmt.Errorf("invalid KV operation %q", command.Operation)
	}
	fingerprint, err := types.KVFingerprint(command)
	if err != nil {
		return err
	}
	if _, found, err := m.receiptInTx(ctx, tx, types.MutationKV, command.RequestID); err != nil {
		return err
	} else if found {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _rhiza_kv WHERE rowid IN (SELECT rowid FROM _rhiza_kv WHERE expires_at_unix_ms > 0 AND expires_at_unix_ms <= ? LIMIT 256)`, command.ObservedAtUnixMS); err != nil {
		return fmt.Errorf("prune expired KV entries: %w", err)
	}
	receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted, Applied: true}
	switch command.Operation {
	case "put":
		_, err = tx.ExecContext(ctx, `INSERT INTO _rhiza_kv(key, value, expires_at_unix_ms) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at_unix_ms=excluded.expires_at_unix_ms`, command.Key, command.Value, command.ExpiresAtUnixMS)
	case "delete":
		var affected sql.Result
		affected, err = tx.ExecContext(ctx, `DELETE FROM _rhiza_kv WHERE key = ?`, command.Key)
		if err == nil {
			rows, _ := affected.RowsAffected()
			receipt.Applied = rows > 0
		}
	case "cas":
		var current []byte
		var expires int64
		lookupErr := tx.QueryRowContext(ctx, `SELECT value, expires_at_unix_ms FROM _rhiza_kv WHERE key = ?`, command.Key).Scan(&current, &expires)
		exists := lookupErr == nil && (expires == 0 || expires > command.ObservedAtUnixMS)
		if lookupErr != nil && lookupErr != sql.ErrNoRows {
			return lookupErr
		}
		receipt.Applied = exists == command.ExpectedExists && (!exists || bytes.Equal(current, command.Expected))
		if receipt.Applied {
			_, err = tx.ExecContext(ctx, `INSERT INTO _rhiza_kv(key, value, expires_at_unix_ms) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at_unix_ms=excluded.expires_at_unix_ms`, command.Key, command.Value, command.ExpiresAtUnixMS)
		}
	}
	if err != nil {
		return err
	}
	return insertReceipt(ctx, tx, types.MutationKV, command.RequestID, fingerprint, receipt)
}

// KVGet reads a non-expired value from the local materialized state.
func (m *Materializer) KVGet(ctx context.Context, key string, now time.Time) ([]byte, bool, error) {
	reader, err := m.reader()
	if err != nil {
		return nil, false, err
	}
	var value []byte
	err = reader.QueryRowContext(ctx, `SELECT value FROM _rhiza_kv WHERE key = ? AND (expires_at_unix_ms = 0 OR expires_at_unix_ms > ?)`, key, now.UnixMilli()).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	return value, err == nil, err
}

// KVGetAt reads a value and the applied slot from one SQLite snapshot.
func (m *Materializer) KVGetAt(ctx context.Context, key string, now time.Time) ([]byte, bool, uint64, error) {
	reader, err := m.reader()
	if err != nil {
		return nil, false, 0, err
	}
	var slotText string
	var found bool
	var value []byte
	err = reader.QueryRowContext(ctx, `
		SELECT meta.value, kv.key IS NOT NULL, kv.value
		FROM _rhiza_meta AS meta
		LEFT JOIN _rhiza_kv AS kv
		  ON kv.key = ? AND (kv.expires_at_unix_ms = 0 OR kv.expires_at_unix_ms > ?)
		WHERE meta.key = 'applied_slot'`, key, now.UnixMilli()).Scan(&slotText, &found, &value)
	if err != nil {
		return nil, false, 0, err
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		return nil, false, 0, err
	}
	return value, found, slot, nil
}

func (m *Materializer) KVRequestMatches(ctx context.Context, command types.KVCommand) (bool, error) {
	fingerprint, err := types.KVFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationKV, command.RequestID, fingerprint)
	return matches, err
}

// NotifyRequestMatches checks whether a request ID is unused or has identical content.
func (m *Materializer) NotifyRequestMatches(ctx context.Context, command types.NotifyCommand) (bool, error) {
	fingerprint, err := types.NotifyFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationNotify, command.RequestID, fingerprint)
	return matches, err
}

// ValidateSQLCommand validates the replicated SQL trust boundary.
func ValidateSQLCommand(command types.SQLCommand) error {
	if len(command.RequestID) > types.MaxRequestIDBytes {
		return fmt.Errorf("request_id must not exceed %d bytes", types.MaxRequestIDBytes)
	}
	statements := command.Statements
	if command.RequireOne && (!command.WantRows || len(statements) != 0) {
		return fmt.Errorf("require_one is only valid for one row-returning statement")
	}
	if command.Migration != nil {
		if command.SQL != "" || len(command.Args) != 0 || command.WantRows || command.Migration.Version <= 0 || command.Migration.Name == "" || len(command.Migration.Checksum) != 64 {
			return fmt.Errorf("invalid migration command")
		}
		if _, err := hex.DecodeString(command.Migration.Checksum); err != nil {
			return fmt.Errorf("invalid migration checksum")
		}
	}
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	if len(statements) == 0 || len(statements) > MaxSQLStatements {
		return fmt.Errorf("statement count must be between 1 and %d", MaxSQLStatements)
	}
	for statementIndex, statement := range statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("SQL is required")
		}
		if strings.IndexByte(statement.SQL, 0) >= 0 {
			return fmt.Errorf("SQL contains a null byte")
		}
		if len(statement.SQL) > MaxSQLBytes {
			return fmt.Errorf("SQL exceeds %d bytes", MaxSQLBytes)
		}
		argCount := len(statement.Args)
		if len(statement.OutputRefs) > MaxSQLArgs {
			return fmt.Errorf("SQL has more than %d output references", MaxSQLArgs)
		}
		if command.Migration != nil && (len(statement.Args) != 0 || statement.WantRows || len(statement.OutputRefs) != 0) {
			return fmt.Errorf("migration statements do not support arguments, returned rows, or output references")
		}
		if len(statement.OutputRefs) != 0 {
			parameterCount, err := countPlainParameters(statement.SQL)
			if err != nil {
				return err
			}
			if parameterCount != len(statement.Args) {
				return fmt.Errorf("output references require one argument for every positional parameter")
			}
		}
		seenArgs := make(map[int]struct{}, len(statement.OutputRefs))
		for _, ref := range statement.OutputRefs {
			if ref.ArgIndex < 0 || ref.ArgIndex >= len(statement.Args) {
				return fmt.Errorf("output reference argument index is out of bounds")
			}
			if statement.Args[ref.ArgIndex] != nil {
				return fmt.Errorf("output reference argument %d must be null", ref.ArgIndex)
			}
			if _, exists := seenArgs[ref.ArgIndex]; exists {
				return fmt.Errorf("duplicate output reference argument index %d", ref.ArgIndex)
			}
			seenArgs[ref.ArgIndex] = struct{}{}
			if ref.StatementIndex < 0 || ref.StatementIndex >= statementIndex || !statements[ref.StatementIndex].WantRows {
				return fmt.Errorf("output reference must target an earlier row-returning statement")
			}
			if (ref.ColumnName == "") == (ref.ColumnIndex == nil) {
				return fmt.Errorf("output reference requires exactly one column name or index")
			}
			if ref.ColumnIndex != nil && *ref.ColumnIndex < 0 {
				return fmt.Errorf("output reference column index is out of bounds")
			}
		}
		if argCount > MaxSQLArgs {
			return fmt.Errorf("SQL has more than %d arguments", MaxSQLArgs)
		}
		if err := validatePublicSQL(statement.SQL); err != nil {
			return err
		}
		for _, arg := range statement.Args {
			if _, err := sqlArg(arg); err != nil {
				return err
			}
		}
		keyword := firstSQLKeyword(statement.SQL)
		if keyword == "" {
			return fmt.Errorf("SQL statement has no valid leading keyword")
		}
		switch keyword {
		case "PRAGMA", "ATTACH", "DETACH", "VACUUM", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE":
			return fmt.Errorf("%s is not allowed on the replicated write API", keyword)
		}
	}
	return nil
}

func countPlainParameters(query string) (int, error) {
	count := 0
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'', '"', '`':
			quote := query[i]
			i++
			for i < len(query) {
				if query[i] == quote {
					if i+1 < len(query) && query[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '[':
			i++
			for i < len(query) && query[i] != ']' {
				i++
			}
			if i < len(query) {
				i++
			}
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i += 2
				for i < len(query) && query[i] != '\n' {
					i++
				}
			} else {
				i++
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i += 2
				for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
					i++
				}
				if i+1 < len(query) {
					i += 2
				}
			} else {
				i++
			}
		case '?':
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				return 0, fmt.Errorf("output references require unnumbered ? parameters")
			}
			count++
			i++
		case ':', '@', '$':
			if i+1 < len(query) && (query[i+1] == '_' || query[i+1] >= 'A' && query[i+1] <= 'Z' || query[i+1] >= 'a' && query[i+1] <= 'z') {
				return 0, fmt.Errorf("output references do not support named parameters")
			}
			i++
		default:
			i++
		}
	}
	return count, nil
}

func firstSQLKeyword(query string) string {
	for {
		query = strings.TrimSpace(query)
		switch {
		case strings.HasPrefix(query, "--"):
			if end := strings.IndexByte(query, '\n'); end >= 0 {
				query = query[end+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "/*"):
			if end := strings.Index(query[2:], "*/"); end >= 0 {
				query = query[end+4:]
				continue
			}
			return ""
		}
		break
	}
	end := 0
	for end < len(query) && (query[end] >= 'A' && query[end] <= 'Z' || query[end] >= 'a' && query[end] <= 'z' || query[end] == '_') {
		end++
	}
	return strings.ToUpper(query[:end])
}

func validatePublicSQL(query string) error {
	if strings.Contains(strings.ToLower(query), "_rhiza_") {
		return fmt.Errorf("the _rhiza_ namespace is reserved")
	}
	return nil
}

func executeSQLCommand(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, command types.SQLCommand) (types.SQLCommandResult, error) {
	statements := command.Statements
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	result := types.SQLCommandResult{Statements: make([]types.SQLStatementResult, 0, len(statements))}
	if command.Migration != nil {
		var name, checksum string
		err := tx.QueryRowContext(ctx, `SELECT name, checksum FROM _rhiza_migrations WHERE version = ?`, command.Migration.Version).Scan(&name, &checksum)
		if err == nil {
			if name == command.Migration.Name && checksum == command.Migration.Checksum {
				return result, nil
			}
			return result, fmt.Errorf("migration %d conflicts with the applied definition", command.Migration.Version)
		}
		if err != sql.ErrNoRows {
			return result, err
		}
		var maxVersion sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM _rhiza_migrations`).Scan(&maxVersion); err != nil {
			return result, err
		}
		expectedVersion := int64(1)
		if maxVersion.Valid {
			expectedVersion = maxVersion.Int64 + 1
		}
		if command.Migration.Version != expectedVersion {
			return result, fmt.Errorf("migration version %d does not follow %d", command.Migration.Version, expectedVersion-1)
		}
		if _, err := execPrepared(ctx, tx, prepared, `INSERT INTO _rhiza_migrations(version, name, checksum) VALUES (?, ?, ?)`, command.Migration.Version, command.Migration.Name, command.Migration.Checksum); err != nil {
			return result, err
		}
	}
	budget := resultBudget{rows: MaxReturningRows, bytes: MaxMutationResultBytes, limit: MaxMutationResultBytes}
	resolvedBytes := 0
	wantsRows := command.WantRows
	for _, statement := range statements {
		args := make([]any, len(statement.Args))
		for i, arg := range statement.Args {
			value, err := sqlArg(arg)
			if err != nil {
				return result, err
			}
			args[i] = value
		}
		for _, ref := range statement.OutputRefs {
			prior := result.Statements[ref.StatementIndex]
			if len(prior.Rows) != 1 {
				return result, fmt.Errorf("statement %d must return exactly one row", ref.StatementIndex)
			}
			column := -1
			if ref.ColumnIndex != nil {
				column = *ref.ColumnIndex
			} else {
				matches := 0
				for i, name := range prior.Columns {
					if name == ref.ColumnName {
						column = i
						matches++
					}
				}
				if matches != 1 {
					return result, fmt.Errorf("statement %d output column %q must be unique", ref.StatementIndex, ref.ColumnName)
				}
			}
			if column < 0 || column >= len(prior.Rows[0]) {
				return result, fmt.Errorf("statement %d output column is out of bounds", ref.StatementIndex)
			}
			value := prior.Rows[0][column]
			resolvedBytes += sqlValueSize(value)
			if resolvedBytes > MaxMutationResultBytes {
				return result, fmt.Errorf("resolved output references exceed %d bytes", MaxMutationResultBytes)
			}
			args[ref.ArgIndex] = value
		}
		query, err := preparedStatement(ctx, tx, prepared, statement.SQL)
		if err != nil {
			return result, err
		}
		if statement.WantRows {
			wantsRows = true
			rows, err := query.QueryContext(ctx, args...)
			if err != nil {
				return result, err
			}
			statementResult, err := collectRowsWithBudget(rows, &budget)
			rows.Close()
			if err != nil {
				return result, err
			}
			if command.RequireOne && len(statementResult.Rows) != 1 {
				return result, fmt.Errorf("statement must return exactly one row")
			}
			result.Statements = append(result.Statements, statementResult)
			continue
		}
		execResult, err := query.ExecContext(ctx, args...)
		if err != nil {
			return result, err
		}
		statementResult := types.SQLStatementResult{}
		statementResult.RowsAffected, _ = execResult.RowsAffected()
		statementResult.LastInsertID, _ = execResult.LastInsertId()
		result.Statements = append(result.Statements, statementResult)
	}
	if wantsRows {
		if _, err := encodeSQLResult(result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func preparedStatement(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, query string) (*sql.Stmt, error) {
	if statement := prepared[query]; statement != nil {
		return statement, nil
	}
	statement, err := tx.PrepareContext(ctx, query)
	if err == nil {
		prepared[query] = statement
	}
	return statement, err
}

func execPrepared(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, query string, args ...any) (sql.Result, error) {
	statement, err := preparedStatement(ctx, tx, prepared, query)
	if err != nil {
		return nil, err
	}
	return statement.ExecContext(ctx, args...)
}

func sqlArg(arg any) (any, error) {
	switch value := arg.(type) {
	case nil, bool, string, []byte, int64, float64:
		return value, nil
	case map[string]any:
		encoded, ok := value["$rhiza_blob"]
		if !ok || len(value) != 1 {
			return nil, fmt.Errorf("unsupported SQL argument type %T", arg)
		}
		text, ok := encoded.(string)
		if !ok {
			return nil, fmt.Errorf("invalid SQL blob argument")
		}
		blob, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, fmt.Errorf("invalid SQL blob argument: %w", err)
		}
		return blob, nil
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

type resultBudget struct {
	rows  int
	bytes int
	limit int
}

type countingWriter int

func (w *countingWriter) Write(p []byte) (int, error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

func encodedJSONSize(value any) (int, error) {
	var size countingWriter
	if err := json.NewEncoder(&size).Encode(value); err != nil {
		return 0, err
	}
	return int(size) - 1, nil
}

func collectRows(rows *sql.Rows, limit int) (types.SQLStatementResult, error) {
	return collectRowsWithBudget(rows, &resultBudget{rows: limit, bytes: MaxResultBytes, limit: MaxResultBytes})
}

func collectRowsWithBudget(rows *sql.Rows, budget *resultBudget) (types.SQLStatementResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	result := types.SQLStatementResult{Columns: columns}
	for _, column := range columns {
		budget.bytes -= len(column)
	}
	if budget.bytes < 0 {
		return result, fmt.Errorf("result exceeds %d bytes", budget.limit)
	}
	for rows.Next() {
		if budget.rows == 0 {
			return result, fmt.Errorf("result exceeds %d rows", MaxReturningRows)
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		for _, value := range values {
			size := sqlValueSize(value)
			if size > MaxCellBytes {
				return result, fmt.Errorf("result cell exceeds %d bytes", MaxCellBytes)
			}
			budget.bytes -= size
			if budget.bytes < 0 {
				return result, fmt.Errorf("result exceeds %d bytes", budget.limit)
			}
		}
		result.Rows = append(result.Rows, values)
		budget.rows--
	}
	return result, rows.Err()
}

func sqlValueSize(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	case nil:
		return 1
	default:
		return 16
	}
}

func (m *Materializer) SQLRequestMatches(ctx context.Context, command types.SQLCommand) (bool, error) {
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationSQL, command.RequestID, fingerprint)
	return matches, err
}

// SQLRequestStatus returns the retained receipt and fingerprint match.
func (m *Materializer) SQLRequestStatus(ctx context.Context, command types.SQLCommand) (types.MutationReceipt, bool, bool, error) {
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	return m.SQLRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
}

// SQLRequestResultFingerprint returns the retained receipt and row results for
// an idempotent SQL mutation.
func (m *Materializer) SQLRequestResultFingerprint(ctx context.Context, requestID string, fingerprint [32]byte, wantRows bool) (types.MutationReceipt, types.SQLCommandResult, bool, bool, error) {
	m.mu.RLock()
	record, cached := m.recentSQLReceipts[requestID]
	tip := m.tip
	mightContain := m.sqlReceipts.mightContain(requestID, tip)
	m.mu.RUnlock()
	if cached {
		if tip > record.receipt.RetryThroughSlot {
			return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
		}
		if !wantRows {
			return record.receipt, types.SQLCommandResult{}, true, record.fingerprint == fingerprint, nil
		}
	}
	if !mightContain {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	reader, err := m.reader()
	if err != nil {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, false, err
	}
	record, err = scanReceipt(reader.QueryRowContext(ctx, receiptQuery(), types.MutationSQL, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	if err != nil {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, false, err
	}
	if m.Tip() > record.receipt.RetryThroughSlot {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	return record.receipt, record.sqlResult, true, record.fingerprint == fingerprint, nil
}

// SQLRequestStatusFingerprint returns the retained receipt for a precomputed fingerprint.
func (m *Materializer) SQLRequestStatusFingerprint(ctx context.Context, requestID string, fingerprint [32]byte) (types.MutationReceipt, bool, bool, error) {
	receipt, _, found, matches, err := m.SQLRequestResultFingerprint(ctx, requestID, fingerprint, false)
	return receipt, found, matches, err
}

// Query executes a read query.
func (m *Materializer) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if err := validatePublicSQL(query); err != nil {
		return nil, err
	}
	reader, err := m.reader()
	if err != nil {
		return nil, err
	}
	return reader.QueryContext(ctx, query, args...)
}

// QueryResult executes a bounded read query and materializes its result.
func (m *Materializer) QueryResult(ctx context.Context, query string, args []any) (types.SQLStatementResult, error) {
	if strings.TrimSpace(query) == "" || len(query) > MaxSQLBytes {
		return types.SQLStatementResult{}, fmt.Errorf("invalid SQL query")
	}
	if err := validatePublicSQL(query); err != nil {
		return types.SQLStatementResult{}, err
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
	result, err := collectRows(rows, MaxReturningRows)
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	encodedBytes, err := encodedJSONSize(result)
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	if encodedBytes > MaxResultBytes {
		return types.SQLStatementResult{}, fmt.Errorf("result exceeds %d encoded bytes", MaxResultBytes)
	}
	return result, nil
}

// QueryResultAt executes a bounded read query and returns its applied slot from
// the same SQLite snapshot.
func (m *Materializer) QueryResultAt(ctx context.Context, query string, args []any) (types.SQLStatementResult, uint64, error) {
	if strings.TrimSpace(query) == "" || len(query) > MaxSQLBytes {
		return types.SQLStatementResult{}, 0, fmt.Errorf("invalid SQL query")
	}
	if err := validatePublicSQL(query); err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	normalized, err := NormalizeSQLArgs(args)
	if err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	snapshot, err := m.beginSQLiteReadSnapshot(ctx)
	if err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	defer snapshot.Close()
	rows, err := snapshot.conn.QueryContext(ctx, query, normalized...)
	if err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	defer rows.Close()
	result, err := collectRows(rows, MaxReturningRows)
	if err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	encodedBytes, err := encodedJSONSize(result)
	if err != nil {
		return types.SQLStatementResult{}, 0, err
	}
	if encodedBytes > MaxResultBytes {
		return types.SQLStatementResult{}, 0, fmt.Errorf("result exceeds %d encoded bytes", MaxResultBytes)
	}
	return result, snapshot.index, nil
}

func (m *Materializer) reader() (*sql.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.readers) == 0 || m.readers[0] == nil {
		return nil, sql.ErrConnDone
	}
	return m.readers[0], nil
}

func (m *Materializer) queryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	m.mu.RLock()
	reader := m.readers[0]
	m.mu.RUnlock()

	return reader.QueryRowContext(ctx, query, args...)
}

// Snapshot is unavailable for the combined store; use CheckpointFilesAt.
func (m *Materializer) Snapshot(ctx context.Context) ([]byte, error) {
	data, _, err := m.SnapshotAt(ctx)
	return data, err
}

// SnapshotAt is unavailable for the combined store; use CheckpointFilesAt.
func (m *Materializer) SnapshotAt(ctx context.Context) ([]byte, uint64, error) {
	var data bytes.Buffer
	index, err := m.SnapshotTo(ctx, &data)
	return data.Bytes(), index, err
}

// SnapshotTo is unavailable for the combined store; use CheckpointFilesAt.
func (m *Materializer) SnapshotTo(ctx context.Context, writer io.Writer) (uint64, error) {
	m.mu.RLock()
	closed := m.db == nil
	m.mu.RUnlock()
	if closed {
		return 0, sql.ErrConnDone
	}
	return 0, fmt.Errorf("combined SQL and Graph snapshots require CheckpointFilesAt")
}

func (m *Materializer) beginSQLiteSnapshot(ctx context.Context) (*sqliteSnapshot, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	return beginSQLiteSnapshot(ctx, db)
}

func (m *Materializer) beginSQLiteReadSnapshot(ctx context.Context) (*sqliteSnapshot, error) {
	m.mu.RLock()
	var reader *sql.DB
	if len(m.readers) != 0 {
		reader = m.readers[0]
	}
	m.mu.RUnlock()
	return beginSQLiteSnapshot(ctx, reader)
}

// beginSQLiteSnapshotLocked is for callers that already hold m.mu.
func (m *Materializer) beginSQLiteSnapshotLocked(ctx context.Context) (*sqliteSnapshot, error) {
	return beginSQLiteSnapshot(ctx, m.db)
}

func beginSQLiteSnapshot(ctx context.Context, db *sql.DB) (*sqliteSnapshot, error) {
	if db == nil {
		return nil, sql.ErrConnDone
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var value string
	if err := conn.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key = 'applied_slot'`).Scan(&value); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		return nil, err
	}
	index, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		return nil, err
	}
	return &sqliteSnapshot{conn: conn, index: index}, nil
}

func (snapshot *sqliteSnapshot) Backup(path string) error {
	destination := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
	return snapshot.conn.Raw(func(driverConn any) error {
		raw, ok := driverConn.(sqlite3driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected SQLite driver connection %T", driverConn)
		}
		return raw.Raw().Backup("main", destination)
	})
}

func (snapshot *sqliteSnapshot) Close() error {
	if snapshot == nil || snapshot.conn == nil {
		return nil
	}
	_, rollbackErr := snapshot.conn.ExecContext(context.Background(), "ROLLBACK")
	closeErr := snapshot.conn.Close()
	snapshot.conn = nil
	if rollbackErr != nil {
		return rollbackErr
	}
	return closeErr
}

// CheckpointFilesAt captures fixed-role database files without packaging them.
func (m *Materializer) CheckpointFilesAt(ctx context.Context) ([]CheckpointFile, uint64, func(), error) {
	dir := filepath.Dir(m.dbPath)
	sqlite, err := os.CreateTemp(dir, ".rhiza-checkpoint-sqlite-*")
	if err != nil {
		return nil, 0, nil, err
	}
	sqlitePath := sqlite.Name()
	_ = sqlite.Close()
	_ = os.Remove(sqlitePath)
	paths := []string{sqlitePath}
	cleanup := func() {
		for _, path := range paths {
			_ = os.Remove(path)
		}
	}
	graph, err := os.CreateTemp(dir, ".rhiza-checkpoint-graph-*")
	if err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	graphPath := graph.Name()
	_ = graph.Close()
	_ = os.Remove(graphPath)
	paths = append(paths, graphPath)
	m.mu.Lock()
	sqliteSnapshot, err := m.beginSQLiteSnapshotLocked(ctx)
	index := uint64(0)
	if err == nil {
		index = sqliteSnapshot.index
	}
	if err == nil && (m.tip != index || m.graphTip() != index) {
		err = fmt.Errorf("checkpoint stores disagree at slot %d", index)
	}
	var graphSnap *graphSnapshot
	if err == nil {
		graphSnap, err = m.beginGraphSnapshot()
	}
	m.mu.Unlock()
	if err != nil {
		if sqliteSnapshot != nil {
			_ = sqliteSnapshot.Close()
		}
		cleanup()
		return nil, 0, nil, err
	}
	if err = sqliteSnapshot.Backup(sqlitePath); err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = graphSnap.Backup(graphPath)
	}
	if closeErr := graphSnap.Close(); err == nil {
		err = closeErr
	}
	if closeErr := sqliteSnapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	return []CheckpointFile{{Role: CheckpointSQLite, Path: sqlitePath}, {Role: CheckpointGraphData, Path: graphPath}}, index, cleanup, nil
}

// Restore is unavailable for the combined store; use RestoreCheckpoint.
func (m *Materializer) Restore(ctx context.Context, data []byte) error {
	return fmt.Errorf("combined SQL and Graph restore requires fixed-role checkpoint files")
}

// RestoreFile is unavailable for the combined store; use RestoreCheckpoint.
func (m *Materializer) RestoreFile(ctx context.Context, snapshotPath string) error {
	return fmt.Errorf("combined SQL and Graph restore requires fixed-role checkpoint files")
}

// RestoreCheckpoint atomically installs the exact fixed-role checkpoint set.
func (m *Materializer) RestoreCheckpoint(ctx context.Context, files []CheckpointFile) error {
	var parts snapshotParts
	seen := make(map[CheckpointRole]bool, len(files))
	for _, file := range files {
		if seen[file.Role] || file.Path == "" {
			return fmt.Errorf("invalid checkpoint file set")
		}
		seen[file.Role] = true
		switch file.Role {
		case CheckpointSQLite:
			parts.sqlitePath = file.Path
		case CheckpointGraphData:
			parts.graphDir = filepath.Dir(file.Path)
		default:
			return fmt.Errorf("unknown checkpoint role %q", file.Role)
		}
	}
	if parts.sqlitePath == "" || !seen[CheckpointGraphData] {
		return fmt.Errorf("checkpoint requires SQLite and Graph files")
	}
	root, err := os.MkdirTemp(filepath.Dir(m.dbPath), ".rhiza-graph-restore-*")
	if err != nil {
		return err
	}
	graphDir := filepath.Join(root, "latticedb")
	if err := os.MkdirAll(graphDir, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	source := ""
	for _, file := range files {
		if file.Role == CheckpointGraphData {
			source = file.Path
		}
	}
	if err := copyFile(source, filepath.Join(graphDir, "graph.ltdb")); err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	parts.graphDir = graphDir
	parts.cleanup = func() { _ = os.RemoveAll(root) }
	return m.restoreParts(ctx, parts)
}

func copyFile(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(target, source)
	if err == nil {
		err = target.Sync()
	}
	if closeErr := target.Close(); err == nil {
		err = closeErr
	}
	return err
}

func restoreStatePath(dbPath string) string { return dbPath + ".restore-state.json" }

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeRestoreState(dbPath string, state restoreState) error {
	dir := filepath.Dir(dbPath)
	file, err := os.CreateTemp(dir, ".rhiza-restore-state-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0o600); err == nil {
		err = json.NewEncoder(file).Encode(state)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(name, restoreStatePath(dbPath)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func recoverRestore(dbPath string) error {
	journal := restoreStatePath(dbPath)
	data, err := os.ReadFile(journal)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var state restoreState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode restore journal: %w", err)
	}
	dir := filepath.Dir(dbPath)
	backupPath := dbPath + ".restore-backup"
	graphPath := filepath.Join(dir, "latticedb")
	graphBackupPath := graphPath + ".restore-backup"
	if state.Phase != "committed" {
		if _, err := os.Stat(backupPath); err == nil {
			_ = os.Remove(dbPath)
			if err := os.Rename(backupPath, dbPath); err != nil {
				return fmt.Errorf("rollback SQLite restore: %w", err)
			}
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		if state.InstallGraph {
			if state.GraphHadOriginal {
				if _, err := os.Stat(graphBackupPath); err == nil {
					_ = os.RemoveAll(graphPath)
					if err := os.Rename(graphBackupPath, graphPath); err != nil {
						return fmt.Errorf("rollback graph restore: %w", err)
					}
				} else if !os.IsNotExist(err) {
					return err
				}
			} else {
				_ = os.RemoveAll(graphPath)
			}
		}
	}
	_ = os.Remove(backupPath)
	_ = os.RemoveAll(graphBackupPath)
	if err := syncDirectory(dir); err != nil {
		return err
	}
	if err := os.Remove(journal); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(dir)
}

func (m *Materializer) restoreParts(ctx context.Context, parts snapshotParts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := filepath.Dir(m.dbPath)
	if parts.cleanup != nil {
		defer parts.cleanup()
	}
	file, err := os.CreateTemp(dir, ".rhiza-restore-*.db")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err = file.Chmod(0o600); err == nil {
		var source *os.File
		source, err = os.Open(parts.sqlitePath)
		if err == nil {
			_, err = io.Copy(file, source)
			if closeErr := source.Close(); err == nil {
				err = closeErr
			}
		}
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
	graphPath := filepath.Join(dir, "latticedb")
	graphBackupPath := graphPath + ".restore-backup"
	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		return fmt.Errorf("stale SQLite restore backup exists")
	}
	if _, err := os.Stat(graphBackupPath); !os.IsNotExist(err) {
		return fmt.Errorf("stale graph restore backup exists")
	}
	_, graphStatErr := os.Stat(graphPath)
	state := restoreState{Phase: "prepared", InstallGraph: parts.graphDir != "", GraphHadOriginal: graphStatErr == nil}
	if graphStatErr != nil && !os.IsNotExist(graphStatErr) {
		return graphStatErr
	}
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return fmt.Errorf("prepare restore journal: %w", err)
	}
	rollback := func(cause error) error {
		var recoveryErr, reopenErr error
		if err := recoverRestore(m.dbPath); err != nil {
			recoveryErr = fmt.Errorf("recover restore: %w", err)
		}
		if err := m.reopen(); err != nil {
			reopenErr = fmt.Errorf("reopen materializer: %w", err)
		}
		return errors.Join(cause, recoveryErr, reopenErr)
	}
	if err := m.closeConnections(); err != nil {
		return rollback(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(m.dbPath + suffix); err != nil && !os.IsNotExist(err) {
			return rollback(fmt.Errorf("remove SQLite sidecar %s: %w", suffix, err))
		}
	}
	if err := os.Rename(m.dbPath, backupPath); err != nil && !os.IsNotExist(err) {
		return rollback(fmt.Errorf("backup database: %w", err))
	}
	if err := syncDirectory(dir); err != nil {
		return rollback(err)
	}
	state.Phase = "sqlite-backed-up"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return rollback(err)
	}
	graphBackedUp := false
	graphInstalled := false
	if parts.graphDir != "" {
		if err := os.Rename(graphPath, graphBackupPath); err == nil {
			graphBackedUp = true
		} else if !os.IsNotExist(err) {
			_ = os.Rename(backupPath, m.dbPath)
			return rollback(fmt.Errorf("backup graph database: %w", err))
		}
		if err := os.Rename(parts.graphDir, graphPath); err != nil {
			if graphBackedUp {
				_ = os.Rename(graphBackupPath, graphPath)
			}
			_ = os.Rename(backupPath, m.dbPath)
			return rollback(fmt.Errorf("install graph snapshot: %w", err))
		}
		graphInstalled = true
		if err := syncDirectory(dir); err != nil {
			return rollback(err)
		}
		state.Phase = "graph-installed"
		if err := writeRestoreState(m.dbPath, state); err != nil {
			return rollback(err)
		}
	}
	if err := os.Rename(tempPath, m.dbPath); err != nil {
		if graphInstalled {
			_ = os.RemoveAll(graphPath)
		}
		if graphBackedUp {
			_ = os.Rename(graphBackupPath, graphPath)
		}
		_ = os.Rename(backupPath, m.dbPath)
		return rollback(fmt.Errorf("install snapshot: %w", err))
	}
	if err := syncDirectory(dir); err != nil {
		return rollback(err)
	}
	state.Phase = "sqlite-installed"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return rollback(err)
	}
	restored, err := openMaterializer(m.dbPath, m.readersN, m.idempotencyWindow)
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
		return rollback(fmt.Errorf("open restored snapshot: %w", err))
	}
	state.Phase = "committed"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		_ = restored.Close()
		return rollback(fmt.Errorf("commit restore journal: %w", err))
	}
	if err := recoverRestore(m.dbPath); err != nil {
		_ = restored.Close()
		return rollback(fmt.Errorf("finalize restore: %w", err))
	}
	m.adopt(restored)
	return nil
}

func (m *Materializer) reopen() error {
	reopened, err := Open(m.dbPath, m.readersN, m.idempotencyWindow)
	if err != nil {
		return err
	}
	m.adopt(reopened)
	return nil
}

func (m *Materializer) adopt(source *Materializer) {
	source.stopSQLiteCheckpoints()
	m.db, m.writer, m.readers, m.graph = source.db, source.writer, source.readers, source.graph
	m.tip, m.stateTip, m.tipHash = source.tip, source.stateTip, source.tipHash
	m.recentSQLReceipts = source.recentSQLReceipts
	m.sqlReceipts = source.sqlReceipts
	source.db, source.writer, source.readers, source.graph = nil, nil, nil, nil
	_ = source.Close()
}

// Tip returns the last applied slot.
func (m *Materializer) Tip() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tip
}

// StateTip returns the last slot that changed user-visible state.
func (m *Materializer) StateTip() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stateTip
}

// Close closes all connections.
func (m *Materializer) Close() error {
	m.stopSQLiteCheckpoints()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifyStopOnce.Do(func() { close(m.notifyStop) })
	m.notifyWG.Wait()
	m.notifyMu.Lock()
	for id, sub := range m.subs {
		close(sub.ch)
		delete(m.subs, id)
	}
	m.notifyMu.Unlock()
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
	m.mu.RLock()
	db := m.db
	if db == nil {
		m.mu.RUnlock()
		return fmt.Errorf("SQLite materializer is not open")
	}
	err := db.PingContext(ctx)
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	return m.graphHealth()
}
