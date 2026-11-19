package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
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
	MaxSQLResultColumns    = 2000
	MaxReturningRows       = 10_000
	MaxResultBytes         = 16 << 20
	MaxMutationResultBytes = 1 << 20
	MaxCellBytes           = 1 << 20
	maxSQLColumnCharge     = 32
	maxSQLStatementCharge  = 256
	maxSQLRowCharge        = 64
	maxSQLCellCharge       = 64
	MaxGraphReachableDepth = 64
	MaxGraphReachableEdges = 1_000_000
	// MaxGraphQueryWork matches LatticeDB's default per-query execution budget.
	// It does not bound parsing time or wall-clock duration.
	MaxGraphQueryWork       = 10_000_000
	MaxGraphPropertyIndexes = 64

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

var errSQLPreconditionFailed = errors.New("SQL precondition failed")

// Materializer applies decided values to SQLite.
// Uses single writer, multiple readers pattern like Hiqlite.
type Materializer struct {
	db                       *sql.DB
	writer                   *sql.DB
	readers                  []*sql.DB
	mu                       sync.RWMutex
	tip                      uint64
	stateTip                 uint64
	tipHash                  [32]byte
	dbPath                   string
	readersN                 int
	idempotencyWindow        uint64
	managedProtocol          string
	materializationPolicy    sqlpolicy.MaterializationPolicy
	commitUnknown            bool
	sqlReceiptRetention      sqlReceiptRetentionUsage
	afterApplyCommitError    func() error
	recentSQLReceipts        map[string]storedReceipt
	pendingSQLReceipts       []pendingSQLReceipt
	pendingSQLResultBytes    int
	sqlReceipts              sqlReceiptBloom
	graph                    *graphState
	graphNodePropertyIndexes []types.GraphNodePropertyIndex
	notifyMu                 sync.Mutex
	nextSub                  uint64
	subs                     map[uint64]notificationSubscription
	notifyQueue              chan pendingNotification
	notifyStop               chan struct{}
	notifyStopOnce           sync.Once
	notifyWG                 sync.WaitGroup
	notifyDrops              atomic.Uint64
	walCheckpointStop        chan struct{}
	walCheckpointOnce        sync.Once
	walCheckpointWG          sync.WaitGroup
	walCheckpointIO          sync.WaitGroup
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
	Role           CheckpointRole
	Path           string
	ExpectedLength uint64
	ExpectedSHA256 string
}

type sqliteSnapshot struct {
	conn    *sql.Conn
	index   uint64
	release func()
}

// Open opens or creates a materializer.
func Open(dbPath string, readerCount int, idempotencyWindow ...uint64) (*Materializer, error) {
	return openWithProtocol(dbPath, readerCount, "", idempotencyWindow...)
}

func openWithProtocol(dbPath string, readerCount int, protocol string, idempotencyWindow ...uint64) (*Materializer, error) {
	if protocol != "" && protocol != types.ManagedApplicationProtocolSQLKV && !originalsProtocol(protocol) {
		return nil, fmt.Errorf("unsupported managed application protocol")
	}
	// SQLite receives this path as a file: URL, where a relative path lands in the
	// authority part and resolves against the filesystem root. Resolve it first so
	// a relative data directory stays where the caller's working directory is.
	absolute, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	dbPath = absolute
	if graphProtocol(protocol) {
		if err := ValidateCleanRestoreState(dbPath); err != nil {
			return nil, err
		}
	}
	if protocol != "" && !graphProtocol(protocol) {
		if err := validateManagedArtifacts(dbPath); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(dbPath); err == nil {
		if err := sqlpolicy.CheckFileForPolicy(context.Background(), dbPath, policyForProtocol(protocol)); err != nil {
			return nil, err
		}
		if err := checkManagedPolicyFile(context.Background(), dbPath, protocol, idempotencyWindow...); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if _, err := os.Lstat(localRestoreJournalPath(dbPath)); err == nil {
		return nil, fmt.Errorf("Local restore journal requires exact validated WAL-base recovery")
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := recoverRestore(dbPath); err != nil {
		return nil, fmt.Errorf("recover interrupted restore: %w", err)
	}
	return openMaterializerPolicy(dbPath, readerCount, protocol, idempotencyWindow...)
}

// ValidateCleanRestoreState is a read-only Local startup guard. Restore
// journals do not identify the exact certified checkpoint root, so Local
// startup preserves and refuses interrupted restore state instead of allowing
// Open to roll it forward or back after WAL repair has begun.
func ValidateCleanRestoreState(dbPath string) error {
	absolute, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	for _, path := range []string{restoreStatePath(absolute), absolute + ".restore-backup", filepath.Join(filepath.Dir(absolute), "latticedb.restore-backup")} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("Local startup found interrupted materializer restore state at %s; preserve it for exact-root recovery", path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ConfigureLocalGraphNodePropertyIndexes declares node-local derived indexes.
// The declaration is retained and reconciled after materializer replacement.
func (m *Materializer) ConfigureLocalGraphNodePropertyIndexes(indexes []types.GraphNodePropertyIndex) error {
	if m.managedProtocol != "" {
		if len(indexes) != 0 {
			return fmt.Errorf("mixed materialization does not support Graph indexes")
		}
		return nil
	}
	deduped, err := ValidateLocalGraphNodePropertyIndexes(indexes)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ensureGraphNodePropertyIndexes(m.graph, deduped); err != nil {
		return err
	}
	m.graphNodePropertyIndexes = deduped
	return nil
}

// ValidateLocalGraphNodePropertyIndexes validates and canonicalizes the
// configured derived-index policy without opening or mutating materializer
// files. Duplicate declarations are harmless and collapse to one entry.
func ValidateLocalGraphNodePropertyIndexes(indexes []types.GraphNodePropertyIndex) ([]types.GraphNodePropertyIndex, error) {
	if len(indexes) > MaxGraphPropertyIndexes {
		return nil, fmt.Errorf("at most %d local graph property indexes are allowed", MaxGraphPropertyIndexes)
	}
	deduped := make([]types.GraphNodePropertyIndex, 0, len(indexes))
	seen := make(map[types.GraphNodePropertyIndex]struct{}, len(indexes))
	for _, index := range indexes {
		if index.Label == "" || index.Property == "" {
			return nil, fmt.Errorf("graph index label and property are required")
		}
		if !utf8.ValidString(index.Label) || !utf8.ValidString(index.Property) {
			return nil, fmt.Errorf("graph index label and property must be valid UTF-8")
		}
		if _, ok := seen[index]; ok {
			continue
		}
		seen[index] = struct{}{}
		deduped = append(deduped, index)
	}
	return deduped, nil
}

func openMaterializer(dbPath string, readerCount int, idempotencyWindow ...uint64) (*Materializer, error) {
	return openMaterializerPolicy(dbPath, readerCount, "", idempotencyWindow...)
}

func openMaterializerPolicy(dbPath string, readerCount int, protocol string, idempotencyWindow ...uint64) (*Materializer, error) {
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
	if _, err := os.Stat(dbPath); err == nil {
		existing = true
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat database: %w", err)
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}).String()
	if existing {
		if err := sqlpolicy.CheckFileForPolicy(context.Background(), dbPath, policyForProtocol(protocol)); err != nil {
			return nil, err
		}
		if err := checkManagedPolicyFile(context.Background(), dbPath, protocol, window); err != nil {
			return nil, err
		}
	}
	if protocol != "" {
		if graphProtocol(protocol) {
			if existing {
				if _, _, err := inspectApplicationFile(context.Background(), dbPath, window, protocol); err != nil {
					return nil, fmt.Errorf("validate stored graph SQL projection: %w", err)
				}
			}
		} else {
			if err := validateManagedGraphAbsence(dbPath); err != nil {
				return nil, err
			}
			if existing {
				if _, _, err := inspectApplicationFile(context.Background(), dbPath, window, protocol); err != nil {
					return nil, fmt.Errorf("validate stored mixed application: %w", err)
				}
			}
		}
	}
	// Check an existing graph policy before opening SQLite with a writer or
	// opening LatticeDB writable. Old graph state is incompatible, not migrated.
	if protocol == "" {
		if err := ValidateLocalGraphStorage(filepath.Join(filepath.Dir(dbPath), "latticedb", "graph.ltdb"), true); err != nil {
			return nil, fmt.Errorf("incompatible graph materialization: %w", err)
		}
	}
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
		db:                    db,
		writer:                writer,
		readers:               readers,
		dbPath:                dbPath,
		readersN:              readerCount,
		idempotencyWindow:     window,
		managedProtocol:       protocol,
		materializationPolicy: policyForProtocol(protocol),
		recentSQLReceipts:     make(map[string]storedReceipt),
		subs:                  make(map[uint64]notificationSubscription),
		notifyQueue:           make(chan pendingNotification, notificationDispatchDepth),
		notifyStop:            make(chan struct{}),
		walCheckpointStop:     make(chan struct{}),
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
	if err := m.loadSQLReceiptRetention(); err != nil {
		m.Close()
		return nil, fmt.Errorf("load SQL receipt retention: %w", err)
	}
	if err := m.loadSQLReceiptBloom(); err != nil {
		m.Close()
		return nil, fmt.Errorf("load SQL receipts: %w", err)
	}
	if protocol == "" || graphProtocol(protocol) {
		graph, err := openGraphForPolicy(filepath.Join(filepath.Dir(dbPath), "latticedb"), m.tip, window, graphProtocol(protocol))
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("open graph materializer: %w", err)
		}
		m.graph = graph
		if graphProtocol(protocol) {
			if err := m.validateGraphHistoryLocked(context.Background(), false); err != nil {
				m.Close()
				return nil, err
			}
		}
	}
	m.walCheckpointWG.Add(1)
	go m.runSQLiteCheckpoints()

	return m, nil
}

func openSQLite(dsn string, writer bool) (*sql.DB, error) {
	readOnly := strings.Contains(dsn, "mode=ro")
	init := func(conn *sqlite3.Conn) error {
		if err := fts5.Register(conn); err != nil {
			return err
		}
		conn.Limit(sqlite3.LIMIT_SQL_LENGTH, MaxSQLBytes)
		conn.Limit(sqlite3.LIMIT_LENGTH, 16<<20)
		conn.Limit(sqlite3.LIMIT_VARIABLE_NUMBER, MaxSQLArgs)
		// SQLite also applies this limit to schema columns and ORDER BY terms.
		conn.Limit(sqlite3.LIMIT_COLUMN, MaxSQLResultColumns)
		if conn.Limit(sqlite3.LIMIT_COLUMN, -1) != MaxSQLResultColumns {
			return fmt.Errorf("SQLite does not support %d result columns", MaxSQLResultColumns)
		}
		if _, err := conn.Config(sqlite3.DBCONFIG_DEFENSIVE, true); err != nil {
			return err
		}
		if writer {
			// The authorizer below never sees a function that is only resolved
			// when a column DEFAULT is applied, so such a write was certified and
			// then materialized a different durable value on a voter whose writer
			// connection a restart had recreated (issue #152). Replacing the
			// functions themselves also rejects those scalar-function evaluations
			// the authorizer cannot observe. Name-based restrictions also cover
			// fixed-input date/time defaults, not just clock-dependent arguments.
			if err := denySQLFunctions(conn); err != nil {
				return err
			}
		}
		return setSQLAuthorizer(conn, writer, readOnly, false)
	}
	if writer {
		return openRollbackWitnessSQLite(dsn, init)
	}
	return sqlite3driver.Open(dsn, init)
}

// sqlFunctionDenylist holds functions a replicated write must not use. The
// clock and randomness entries read the host, and the three connection-local
// entries return counters owned by the writer connection, so a voter whose
// writer connection a restart had recreated would materialize a different
// durable value for the same certified command (issue #152).
var sqlFunctionDenylist = []string{
	"random", "randomblob", "now", "current_time", "current_date", "current_timestamp",
	"date", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff",
	"load_extension", "changes", "total_changes", "last_insert_rowid",
}

func nondeterministicSQLFunction(name string) bool {
	return slices.Contains(sqlFunctionDenylist, strings.ToLower(name))
}

// denySQLFunctions replaces every denylisted function with one that fails, so
// the write fails instead of storing a value that is not reproducible from the
// certified log. Any arity is accepted because the caller never supplies a
// valid result.
func denySQLFunctions(conn *sqlite3.Conn) error {
	for _, name := range sqlFunctionDenylist {
		if err := conn.CreateFunction(name, -1, 0, func(ctx sqlite3.Context, _ ...sqlite3.Value) {
			ctx.ResultError(fmt.Errorf("function %s is not reproducible on the replicated write API", name))
		}); err != nil {
			return fmt.Errorf("deny SQL function %s: %w", name, err)
		}
	}
	return nil
}

func (m *Materializer) loadTip(existing bool) error {
	var value string
	err := m.writer.QueryRow(`SELECT value FROM _rhiza_meta WHERE key = 'applied_slot'`).Scan(&value)
	if err == sql.ErrNoRows {
		if existing {
			return fmt.Errorf("existing database has no applied slot; rebuild it from the decision log")
		}
		zeroHash := hex.EncodeToString(make([]byte, sha256.Size))
		query := `INSERT INTO _rhiza_meta(key, value) VALUES ('sql_execution_policy', ?), ('sql_idempotency_window', ?), ('sql_receipt_result_bytes', '0'), ('sql_receipt_count', '0'), ('applied_slot', '0'), ('state_slot', '0'), ('applied_hash', ?)`
		args := []any{strconv.Itoa(int(m.materializationPolicy)), strconv.FormatUint(m.idempotencyWindow, 10), zeroHash}
		if m.managedProtocol != "" {
			query += `, ('managed_application_protocol', ?), ('managed_application_policy', ?)`
			args = append(args, m.managedProtocol, managedApplicationPolicy)
		}
		if originalsPolicy(m.materializationPolicy) {
			query += `, ('request_original_bytes','0')`
		}
		if graphProtocol(m.managedProtocol) {
			query += `, ('last_graph_slot','0'), ('graph_history_hash',?)`
			args = append(args, zeroHash)
		}
		if _, err := m.writer.Exec(query, args...); err != nil {
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
	if err := m.validateExecutionPolicy(value); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return err
	}
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
	if originalsPolicy(m.materializationPolicy) {
		_, err = m.db.Exec(requestOriginalDDL)
		return err
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

// sqlReceiptReadLocked checks the uncertainty state while m.mu is held.
func (m *Materializer) sqlReceiptReadLocked() error {
	if graphProtocol(m.managedProtocol) {
		return m.graphReadableLocked()
	}
	if m.commitUnknown {
		return ErrGraphCommitUnknown
	}
	if m.graph == nil {
		return nil
	}
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	if m.graph.commitUnknown {
		return ErrGraphCommitUnknown
	}
	return nil
}

func (m *Materializer) MutationReceipt(ctx context.Context, kind types.MutationKind, requestID string) (types.MutationReceipt, bool, error) {
	if kind == types.MutationGraph && graphProtocol(m.managedProtocol) {
		return m.GraphMutationReceipt(ctx, requestID)
	}
	if kind == types.MutationSQL || m.managedProtocol != "" && kind != types.MutationGraph {
		m.mu.RLock()
		defer m.mu.RUnlock()
		if err := m.sqlReceiptReadLocked(); err != nil {
			return types.MutationReceipt{}, false, err
		}
		if len(m.readers) == 0 || m.readers[0] == nil {
			return types.MutationReceipt{}, false, sql.ErrConnDone
		}
		record, err := scanReceipt(m.readers[0].QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
		if err == sql.ErrNoRows {
			return types.MutationReceipt{}, false, nil
		}
		if err != nil {
			return types.MutationReceipt{}, false, err
		}
		if m.tip > record.receipt.RetryThroughSlot {
			return types.MutationReceipt{}, false, nil
		}
		return record.receipt, true, nil
	}
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
	if kind == types.MutationSQL || m.managedProtocol != "" && kind != types.MutationGraph {
		m.mu.RLock()
		defer m.mu.RUnlock()
		if err := m.sqlReceiptReadLocked(); err != nil {
			return false, false, err
		}
		if len(m.readers) == 0 || m.readers[0] == nil {
			return false, false, sql.ErrConnDone
		}
		record, err := scanReceipt(m.readers[0].QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
		if err == sql.ErrNoRows {
			return true, false, nil
		}
		if err != nil {
			return false, false, err
		}
		if m.tip > record.receipt.RetryThroughSlot {
			return true, false, nil
		}
		return record.fingerprint == fingerprint, true, nil
	}
	_, found, matches, err := m.requestStatus(ctx, kind, requestID, fingerprint)
	return matches, found, err
}

// requestStatus returns the retained receipt and fingerprint match from one lookup.
func (m *Materializer) requestStatus(ctx context.Context, kind types.MutationKind, requestID string, fingerprint [32]byte) (types.MutationReceipt, bool, bool, error) {
	reader, err := m.reader()
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	record, err := scanReceipt(reader.QueryRowContext(ctx, receiptQuery(), kind, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return types.MutationReceipt{}, false, true, nil
	}
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	if m.Tip() > record.receipt.RetryThroughSlot {
		return types.MutationReceipt{}, false, true, nil
	}
	return record.receipt, true, record.fingerprint == fingerprint, nil
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
		if result.Error != "" {
			return nil, fmt.Errorf("SQL result error without statements")
		}
		return nil, nil
	}
	if len(result.Statements) > MaxSQLStatements {
		return nil, fmt.Errorf("invalid statement count")
	}
	var encoded bytes.Buffer
	encoded.WriteString("RSQL")
	writeUint32(&encoded, uint32(len(result.Statements)))
	for _, statement := range result.Statements {
		if len(statement.Columns) > MaxSQLResultColumns {
			return nil, fmt.Errorf("invalid column count")
		}
		if len(statement.Rows) > MaxReturningRows {
			return nil, fmt.Errorf("invalid row count")
		}
		writeInt64(&encoded, statement.RowsAffected)
		writeInt64(&encoded, statement.LastInsertID)
		writeUint32(&encoded, uint32(len(statement.Columns)))
		for _, column := range statement.Columns {
			if err := writeBytes(&encoded, []byte(column)); err != nil {
				return nil, err
			}
		}
		writeUint32(&encoded, uint32(len(statement.Rows)))
		for _, row := range statement.Rows {
			if len(row) != len(statement.Columns) {
				return nil, fmt.Errorf("invalid cell count")
			}
			writeUint32(&encoded, uint32(len(row)))
			for _, value := range row {
				if err := writeSQLValue(&encoded, value); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := writeBytes(&encoded, []byte(result.Error)); err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

func writeUint32(dst *bytes.Buffer, value uint32) { _ = binary.Write(dst, binary.BigEndian, value) }
func writeInt64(dst *bytes.Buffer, value int64)   { _ = binary.Write(dst, binary.BigEndian, value) }
func writeBytes(dst *bytes.Buffer, value []byte) error {
	if len(value) > MaxMutationResultBytes-4 || dst.Len() > MaxMutationResultBytes-4-len(value) {
		return fmt.Errorf("stored SQL result exceeds %d bytes", MaxMutationResultBytes)
	}
	writeUint32(dst, uint32(len(value)))
	dst.Write(value)
	return nil
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
		return writeBytes(dst, []byte(value))
	case []byte:
		dst.WriteByte(4)
		return writeBytes(dst, value)
	default:
		return fmt.Errorf("unsupported SQL result type %T", value)
	}
	if dst.Len() > MaxMutationResultBytes {
		return fmt.Errorf("stored SQL result exceeds %d bytes", MaxMutationResultBytes)
	}
	return nil
}

func decodeSQLResult(encoded []byte) (types.SQLCommandResult, error) {
	if len(encoded) > MaxMutationResultBytes {
		return types.SQLCommandResult{}, fmt.Errorf("stored SQL result exceeds %d bytes", MaxMutationResultBytes)
	}
	reader := bytes.NewReader(encoded)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != "RSQL" {
		return types.SQLCommandResult{}, fmt.Errorf("invalid result encoding")
	}
	count, err := readUint32(reader)
	if err != nil || count > MaxSQLStatements {
		return types.SQLCommandResult{}, fmt.Errorf("invalid statement count")
	}
	budget := MaxResultBytes - int(count)*maxSQLStatementCharge
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
		if readErr != nil || columns > MaxSQLResultColumns {
			return types.SQLCommandResult{}, fmt.Errorf("invalid column count")
		}
		statement.Columns = make([]string, 0, columns)
		for range columns {
			value, readErr := readBudgetBytes(reader, &budget, maxSQLColumnCharge, MaxMutationResultBytes)
			if readErr != nil {
				return types.SQLCommandResult{}, readErr
			}
			statement.Columns = append(statement.Columns, string(value))
		}
		rowCount, readErr := readUint32(reader)
		if readErr != nil || rowCount > MaxReturningRows || int(rowCount) > budget/maxSQLRowCharge {
			return types.SQLCommandResult{}, fmt.Errorf("invalid row count")
		}
		budget -= int(rowCount) * maxSQLRowCharge
		statement.Rows = make([][]any, rowCount)
		for j := range statement.Rows {
			cellCount, readErr := readUint32(reader)
			if readErr != nil || cellCount != columns {
				return types.SQLCommandResult{}, fmt.Errorf("invalid cell count")
			}
			if int(cellCount) > budget/maxSQLCellCharge {
				return types.SQLCommandResult{}, fmt.Errorf("stored SQL result exceeds working-memory budget")
			}
			budget -= int(cellCount) * maxSQLCellCharge
			statement.Rows[j] = make([]any, cellCount)
			for k := range statement.Rows[j] {
				statement.Rows[j][k], readErr = readSQLValueBudget(reader, &budget)
				if readErr != nil {
					return types.SQLCommandResult{}, readErr
				}
			}
		}
	}
	errorText, err := readBudgetBytes(reader, &budget, 0, MaxMutationResultBytes)
	if err != nil || reader.Len() != 0 {
		return types.SQLCommandResult{}, fmt.Errorf("invalid trailing result data")
	}
	result.Error = string(errorText)
	return result, nil
}

func readBudgetBytes(reader *bytes.Reader, budget *int, overhead, maximum int) ([]byte, error) {
	size, err := readUint32(reader)
	if err != nil || int(size) > maximum || uint64(size) > uint64(reader.Len()) || int(size)+overhead > *budget {
		return nil, fmt.Errorf("stored SQL result exceeds working-memory budget")
	}
	*budget -= int(size) + overhead
	value := make([]byte, int(size))
	_, err = io.ReadFull(reader, value)
	return value, err
}

func readSQLValueBudget(reader *bytes.Reader, budget *int) (any, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		if *budget < 65 {
			return nil, fmt.Errorf("stored SQL result exceeds working-memory budget")
		}
		*budget -= 65
		return nil, nil
	case 1:
		if *budget < 80 {
			return nil, fmt.Errorf("stored SQL result exceeds working-memory budget")
		}
		*budget -= 80
		return readInt64(reader)
	case 2:
		if *budget < 80 {
			return nil, fmt.Errorf("stored SQL result exceeds working-memory budget")
		}
		*budget -= 80
		bits, err := readInt64(reader)
		return math.Float64frombits(uint64(bits)), err
	case 3:
		value, err := readBudgetBytes(reader, budget, 0, MaxCellBytes)
		return string(value), err
	case 4:
		return readBudgetBytes(reader, budget, 0, MaxCellBytes)
	default:
		return nil, fmt.Errorf("invalid SQL result tag %d", tag)
	}
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
		result, err := execPrepared(ctx, tx, prepared, `INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(kind, request_id) DO NOTHING`, kind, pending.requestID, pending.record.fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied, pending.encodedResult)
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
		args = append(args, kind, pending.requestID, pending.record.fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied, pending.encodedResult)
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
	var resultBytes, receipts int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(sql_result)), 0), COUNT(*) FROM _rhiza_idempotency WHERE kind = ? AND commit_slot < ?`, types.MutationSQL, floor).Scan(&resultBytes, &receipts); err != nil {
		return fmt.Errorf("count expired SQL receipts: %w", err)
	}
	if resultBytes < 0 || receipts < 0 {
		return fmt.Errorf("invalid expired SQL receipt accounting")
	}
	if err := m.sqlReceiptRetention.remove(uint64(resultBytes), uint64(receipts)); err != nil {
		return err
	}
	if originalsPolicy(m.materializationPolicy) {
		var expiredBytes int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(execution)),0) FROM _rhiza_request_original WHERE commit_slot < ?`, floor).Scan(&expiredBytes); err != nil {
			return err
		}
		if err := adjustOriginalBytes(ctx, tx, -expiredBytes); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM _rhiza_request_original WHERE commit_slot < ?`, floor); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM _rhiza_idempotency WHERE commit_slot < ?`, floor)
	return err
}

type pendingNotification struct {
	topic   string
	payload []byte
}

type pendingSQLReceipt struct {
	requestID     string
	record        storedReceipt
	encodedResult []byte
	persisted     bool
	executionItem []byte
	batchIndex    uint32
}

// Apply applies one decided value.
func (m *Materializer) Apply(ctx context.Context, slot uint64, value []byte) error {
	return m.ApplyBatch(ctx, []quepaxa.DecidedValue{{Slot: quepaxa.Slot(slot), Value: value}})
}

// ApplyBatch materializes a contiguous decision page with one SQLite commit.
func (m *Materializer) ApplyBatch(ctx context.Context, decisions []quepaxa.DecidedValue) (applyErr error) {
	for _, decision := range decisions {
		if err := m.validateExecutionPolicy(decision.Value); err != nil {
			return err
		}
	}
	if len(decisions) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return sql.ErrConnDone
	}
	if err := m.fatalHealthLocked(); err != nil {
		return err
	}
	if err := m.validateGraphHistoryLocked(ctx, false); err != nil {
		return err
	}
	if m.graph != nil {
		m.graph.mu.RLock()
		commitUnknown := m.graph.commitUnknown
		m.graph.mu.RUnlock()
		if commitUnknown {
			return ErrGraphCommitUnknown
		}
	}
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire apply connection: %w", err)
	}
	defer conn.Close()
	// Own rollback synchronously: automatic context rollback can close conn
	// while the per-statement Raw callback is acquiring it. SQL operations
	// still use ctx, and cancellation is checked again before committing.
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return fmt.Errorf("begin apply batch: %w", err)
	}
	oldTip, oldStateTip, oldHash := m.tip, m.stateTip, m.tipHash
	oldRetention := m.sqlReceiptRetention
	commitAttempted := false
	commitFailed := false
	var rollbackWitness *rollbackWitnessConn
	var statementCloseErr error
	defer func() {
		if rollbackWitness == nil {
			_ = conn.Raw(func(driverConn any) error {
				rollbackWitness, _ = driverConn.(*rollbackWitnessConn)
				return nil
			})
		}
		if applyErr != nil && !commitAttempted {
			if rollbackWitness != nil {
				rollbackWitness.armAutoRollback()
			}
		}
		rollbackErr := tx.Rollback()
		if commitAttempted && errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		retire := commitFailed || rollbackErr != nil || statementCloseErr != nil
		if rollbackWitness != nil && rollbackWitness.takeRetire() {
			retire = true
		}
		if retire {
			// The transaction is finished; database/sql owns final physical close.
			retireErr := conn.Raw(func(any) error { return sqldriver.ErrBadConn })
			if !errors.Is(retireErr, sqldriver.ErrBadConn) {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("retire apply connection: %w", retireErr))
			}
		}
		if applyErr != nil {
			m.sqlReceiptRetention = oldRetention
			if !commitAttempted {
				if rollbackErr != nil {
					m.commitUnknown = true
					if m.graph != nil {
						m.graph.mu.Lock()
						m.graph.poisonCommitOutcome()
						m.graph.mu.Unlock()
					}
					applyErr = errors.Join(applyErr, ErrGraphCommitUnknown, fmt.Errorf("rollback apply batch: %w", rollbackErr))
				} else if m.graph != nil {
					m.graph.mu.Lock()
					// Only a confirmed rollback permits discarding control-slot progress.
					m.graph.tip = min(m.graph.tip, max(oldTip, m.graph.durableTip))
					m.graph.mu.Unlock()
				}
			}
		} else if rollbackErr != nil {
			m.commitUnknown = true
			if m.graph != nil {
				m.graph.mu.Lock()
				m.graph.poisonCommitOutcome()
				m.graph.mu.Unlock()
			}
			applyErr = errors.Join(applyErr, ErrGraphCommitUnknown, fmt.Errorf("close apply batch: %w", rollbackErr))
		}
		if errors.Is(applyErr, errSQLPolicyState) {
			// Discard only after rollback releases the transaction's Conn lock.
			// ErrBadConn from Raw while the transaction is active deadlocks Close.
			_ = conn.Raw(func(any) error { return sqldriver.ErrBadConn })
		}
	}()
	if m.managedProtocol != "" {
		if err := checkManagedPolicy(ctx, tx, m.managedProtocol, m.idempotencyWindow); err != nil {
			return err
		}
	}
	statements := make(map[string]*sql.Stmt)
	defer func() {
		for _, statement := range statements {
			if err := statement.Close(); err != nil {
				statementCloseErr = errors.Join(statementCloseErr, fmt.Errorf("close prepared statement: %w", err))
			}
		}
	}()
	pending := make([]pendingNotification, 0)
	m.pendingSQLReceipts = m.pendingSQLReceipts[:0]
	m.pendingSQLResultBytes = 0
	defer func() {
		clear(m.pendingSQLReceipts)
		m.pendingSQLReceipts = m.pendingSQLReceipts[:0]
		m.pendingSQLResultBytes = 0
	}()
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
		if err := m.applyValueLocked(ctx, conn, tx, statements, slot, decision.Value, hash, oldTip, &pending); err != nil {
			m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
			return err
		}
		m.tip, m.tipHash = slot, hash
	}
	if err := m.flushPendingSQLReceipts(ctx, tx, nil); err != nil {
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		return err
	}
	if err := m.persistSQLReceiptRetention(ctx, tx); err != nil {
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		return err
	}
	if err := ctx.Err(); err != nil {
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		return err
	}
	commitAttempted = true
	commitErr := tx.Commit()
	if commitErr == nil && m.afterApplyCommitError != nil {
		// Test seam: model a lost acknowledgement after SQLite committed.
		commitErr = m.afterApplyCommitError()
	}
	if commitErr != nil {
		commitFailed = true
		m.commitUnknown = true
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		if m.graph != nil {
			m.graph.mu.Lock()
			m.graph.poisonCommitOutcome()
			m.graph.mu.Unlock()
		}
		return fmt.Errorf("%w: SQLite commit apply batch: %w", ErrGraphCommitUnknown, commitErr)
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
		m.recentSQLReceipts[pending.requestID] = pending.record
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

const sqliteWALTruncatePages = 4096

func (m *Materializer) checkpointSQLiteWAL(db *sql.DB, truncateThreshold int) {
	var busy, logPages, checkpointedPages int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logPages, &checkpointedPages); err != nil {
		return
	}
	if busy == 0 && logPages >= truncateThreshold && checkpointedPages == logPages {
		// TRUNCATE requires exclusive SQLite locking; skip it unless the materializer is idle.
		if !m.mu.TryLock() {
			return
		}
		defer m.mu.Unlock()
		if m.db == db && m.db != nil {
			_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
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
			if db != nil {
				m.walCheckpointIO.Add(1)
			}
			m.mu.RUnlock()
			if db != nil {
				m.checkpointSQLiteWAL(db, sqliteWALTruncatePages)
				m.walCheckpointIO.Done()
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

func (m *Materializer) applyValueLocked(ctx context.Context, conn *sql.Conn, tx *sql.Tx, statements map[string]*sql.Stmt, slot uint64, value []byte, hash [32]byte, confirmedGraphThrough uint64, pending *[]pendingNotification) error {
	if m.managedProtocol != "" {
		application, err := managedApplicationValue(value)
		if graphProtocol(m.managedProtocol) {
			_, isGraph, decodeErr := types.DecodeManagedGraphExecution(value)
			if decodeErr != nil {
				return decodeErr
			}
			if isGraph {
				application = true
				err = nil
			}
		}
		if err != nil {
			return err
		}
		if application {
			// Earlier values in this same batch must be visible to pruning and its
			// accounting before their retained window can expire.
			if err := m.flushPendingSQLReceipts(ctx, tx, statements); err != nil {
				return err
			}
			if err := m.pruneReceipts(ctx, tx, slot); err != nil {
				return err
			}
		}
	} else if err := m.pruneReceipts(ctx, tx, slot); err != nil {
		return fmt.Errorf("prune idempotency receipts: %w", err)
	}
	graphCommands, graph, err := types.DecodeGraphBatch(value)
	if graphProtocol(m.managedProtocol) {
		var command types.GraphCommand
		command, graph, err = types.DecodeManagedGraphExecution(value)
		graphCommands = nil
		if graph {
			graphCommands = []types.GraphCommand{command}
		}
	}
	if err != nil {
		return fmt.Errorf("decode graph batch: %w", err)
	}
	if m.managedProtocol == "" || graphProtocol(m.managedProtocol) {
		if err := m.applyGraph(ctx, slot, value, graphCommands, graph, confirmedGraphThrough); err != nil {
			return err
		}
		if graph && graphProtocol(m.managedProtocol) {
			if localtesthooks.Enabled {
				localtesthooks.Hit("materializer:graph:after-native-before-sqlite")
			}
			if err := foldSQLiteGraphHistory(ctx, tx, slot, hash); err != nil {
				return err
			}
		}
	}

	notifyCommand, notify, err := types.DecodeNotifyCommand(value)
	if err != nil {
		return fmt.Errorf("decode notification: %w", err)
	}
	kvCommand, kv, err := types.DecodeKVCommand(value)
	if m.managedProtocol != "" {
		kvCommand, kv, err = types.DecodeManagedKVCommand(value)
	}
	if err != nil {
		return fmt.Errorf("decode KV command: %w", err)
	}
	kvCommands, kvBatch, err := types.DecodeKVBatch(value)
	if m.managedProtocol != "" {
		kvCommands, kvBatch, err = types.DecodeManagedKVBatch(value)
	}
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
	} else if reconfiguration, err := types.DecodeReconfiguration(value); err != nil {
		return fmt.Errorf("decode reconfiguration: %w", err)
	} else if reconfiguration {
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
		if err := m.applyKV(ctx, tx, nil, slot, kvCommand, 0); err != nil {
			return err
		}
		commands = nil
		batched = true
	} else if kvBatch {
		var kvStatements map[string]*sql.Stmt
		if len(kvCommands) > 1 {
			// Statements prepared as internal KV must never be reused as user SQL.
			kvStatements = make(map[string]*sql.Stmt)
			defer func() {
				for _, statement := range kvStatements {
					statement.Close()
				}
			}()
		}
		for index, command := range kvCommands {
			if err := m.applyKV(ctx, tx, kvStatements, slot, command, uint32(index)); err != nil {
				return err
			}
		}
		commands = nil
		batched = true
	} else if !batched {
		return sqlpolicy.ErrIncompatible
	}
	pendingStart := len(m.pendingSQLReceipts)
	for commandIndex, command := range commands {
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
			pendingEarlier := false
			for _, pending := range m.pendingSQLReceipts[:pendingStart] {
				if pending.requestID == command.RequestID {
					mightExist = true
					pendingEarlier = true
					break
				}
			}
			if pendingEarlier {
				continue
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
		var encodedResult []byte
		result, executeErr := executeSQLCommand(ctx, conn, tx, statements, command, &encodedResult)
		if errors.Is(executeErr, errSQLPolicyState) {
			return executeErr
		}
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
			receipt.ErrorCode = types.MutationErrorCodeExecutionFailed
			if errors.Is(executeErr, errSQLPreconditionFailed) {
				receipt.ErrorCode = types.MutationErrorCodePreconditionFailed
			}
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
			if len(storedResult.Statements) == 0 {
				encodedResult = nil
			}
			if !m.sqlReceiptRetention.admits(0, m.idempotencyWindow) {
				return fmt.Errorf("SQL receipt envelope counter exceeds policy bounds")
			}
			if !m.sqlReceiptRetention.admits(uint64(len(encodedResult)), m.idempotencyWindow) {
				if _, err := execPrepared(ctx, tx, statements, "ROLLBACK TO rhiza_command"); err != nil {
					return err
				}
				result = types.SQLCommandResult{}
				storedResult = types.SQLCommandResult{}
				encodedResult = nil
				receipt = types.MutationReceipt{Slot: slot, Status: types.MutationRejected, ErrorCode: types.MutationErrorCodeResultRetentionFull, RetryThroughSlot: slot + m.idempotencyWindow - 1}
			}
			m.sqlReceiptRetention.add(uint64(len(encodedResult)))
			resultCharge := max(sqlResultWorkingBytes(storedResult), len(encodedResult))
			if cap(encodedResult) > resultCharge {
				// Do not retain spare encoder capacity beyond the working-set charge.
				compact := make([]byte, len(encodedResult))
				copy(compact, encodedResult)
				encodedResult = compact
			}
			pendingReceipt := pendingSQLReceipt{requestID: command.RequestID, record: storedReceipt{fingerprint: fingerprint, receipt: receipt}, encodedResult: encodedResult}
			if originalsPolicy(m.materializationPolicy) {
				pendingReceipt.executionItem, err = types.EncodeSQLBatchItem(command)
				if err != nil {
					return err
				}
				pendingReceipt.batchIndex = uint32(commandIndex)
			}
			m.pendingSQLReceipts = append(m.pendingSQLReceipts, pendingReceipt)
			m.pendingSQLResultBytes += resultCharge
			if m.pendingSQLResultBytes >= MaxMutationResultBytes {
				if err := m.flushPendingSQLReceipts(ctx, tx, statements); err != nil {
					return err
				}
			}
		}
		if _, err := execPrepared(ctx, tx, statements, "RELEASE rhiza_command"); err != nil {
			return err
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

func execKV(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, query string, args ...any) (sql.Result, error) {
	if prepared == nil {
		return tx.ExecContext(ctx, query, args...)
	}
	return execPrepared(ctx, tx, prepared, query, args...)
}

func (m *Materializer) applyKV(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, slot uint64, command types.KVCommand, batchIndex uint32) error {
	if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes || command.Key == "" || len(command.Key) > 1024 || len(command.Value) > 16<<20 {
		return fmt.Errorf("invalid KV command")
	}
	switch command.Operation {
	case "put", "delete", "cas":
	default:
		return fmt.Errorf("invalid KV operation %q", command.Operation)
	}
	fingerprint, err := m.kvFingerprint(command)
	if err != nil {
		return err
	}
	if _, found, err := m.receiptInTx(ctx, tx, types.MutationKV, command.RequestID); err != nil {
		return err
	} else if found {
		return nil
	}
	if _, err := execKV(ctx, tx, prepared, `DELETE FROM _rhiza_kv WHERE rowid IN (SELECT rowid FROM _rhiza_kv WHERE expires_at_unix_ms > 0 AND expires_at_unix_ms <= ? LIMIT 256)`, command.ObservedAtUnixMS); err != nil {
		return fmt.Errorf("prune expired KV entries: %w", err)
	}
	receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted, Applied: true}
	const upsert = `INSERT INTO _rhiza_kv(key, value, expires_at_unix_ms) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at_unix_ms=excluded.expires_at_unix_ms`
	switch command.Operation {
	case "put":
		_, err = execKV(ctx, tx, prepared, upsert, command.Key, command.Value, command.ExpiresAtUnixMS)
	case "delete":
		var affected sql.Result
		affected, err = execKV(ctx, tx, prepared, `DELETE FROM _rhiza_kv WHERE key = ?`, command.Key)
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
			_, err = execKV(ctx, tx, prepared, upsert, command.Key, command.Value, command.ExpiresAtUnixMS)
		}
	}
	if err != nil {
		return err
	}
	if err := insertReceipt(ctx, tx, types.MutationKV, command.RequestID, fingerprint, receipt); err != nil {
		return err
	}
	if originalsPolicy(m.materializationPolicy) {
		item, err := types.EncodeManagedKVBatchItem(command)
		if err != nil {
			return err
		}
		return insertRequestOriginal(ctx, tx, types.MutationKV, command.RequestID, slot, batchIndex, item)
	}
	return nil
}

// KVGet reads a non-expired value from the local materialized state.
func (m *Materializer) KVGet(ctx context.Context, key string, now time.Time) ([]byte, bool, error) {
	if m.managedProtocol != "" {
		value, found, _, err := m.KVGetAt(ctx, key, now)
		return value, found, err
	}
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
	if m.managedProtocol != "" {
		return m.managedKVGetAt(ctx, key, now)
	}
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
	fingerprint, err := m.kvFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationKV, command.RequestID, fingerprint)
	return matches, err
}

// KVRequestStatus returns the retained receipt and fingerprint match in one lookup.
func (m *Materializer) KVRequestStatus(ctx context.Context, command types.KVCommand) (types.MutationReceipt, bool, bool, error) {
	fingerprint, err := types.KVFingerprint(command)
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	return m.requestStatus(ctx, types.MutationKV, command.RequestID, fingerprint)
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
func (m *Materializer) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if m.managedProtocol != "" {
		return nil, fmt.Errorf("mixed reads require bounded QueryResult")
	}
	if err := validatePublicSQL(query); err != nil {
		return nil, err
	}
	reader, err := m.reader()
	if err != nil {
		return nil, err
	}
	return reader.QueryContext(ctx, query, args...)
}

// queryContextCause preserves both actual SQLite interruption and its completed
// call context. Other driver errors must not become cancellation after the fact.
func queryContextCause(ctx context.Context, err error) error {
	if errors.Is(err, sqlite3.INTERRUPT) && ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

// QueryResult executes a bounded read query and materializes its result.
func (m *Materializer) QueryResult(ctx context.Context, query string, args []any) (types.SQLStatementResult, error) {
	if m.managedProtocol != "" {
		result, _, err := m.QueryResultAt(ctx, query, args)
		return result, err
	}
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
		return types.SQLStatementResult{}, queryContextCause(ctx, err)
	}
	defer rows.Close()
	result, err := collectRows(rows, MaxReturningRows)
	if err != nil {
		return types.SQLStatementResult{}, queryContextCause(ctx, err)
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
		return types.SQLStatementResult{}, 0, queryContextCause(ctx, err)
	}
	defer snapshot.Close()
	rows, err := snapshot.conn.QueryContext(ctx, query, normalized...)
	if err != nil {
		return types.SQLStatementResult{}, 0, queryContextCause(ctx, err)
	}
	defer rows.Close()
	result, err := collectRows(rows, MaxReturningRows)
	if err != nil {
		return types.SQLStatementResult{}, 0, queryContextCause(ctx, err)
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
	var reader *sql.DB
	if len(m.readers) > 0 && m.readers[0] != nil {
		reader = m.readers[0]
	} else {
		reader = m.db
	}
	m.mu.RUnlock()

	return reader.QueryRowContext(ctx, query, args...)
}

// Snapshot is unavailable for the combined store; use CheckpointFilesAt.
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
	err := m.closeConnections()
	if localtesthooks.Enabled {
		localtesthooks.Hit("materializer:close:complete")
	}
	return err
}

func (m *Materializer) closeConnections() error {
	// Caller owns m.mu exclusively, so no new checkpoint can register while
	// retirement waits. Completion does not need m.mu or the whole worker.
	m.walCheckpointIO.Wait()
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
	if m.managedProtocol != "" {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		m.mu.RLock()
		defer m.mu.RUnlock()
		if err := m.sqlReceiptReadLocked(); err != nil {
			return err
		}
		if m.db == nil {
			return sql.ErrConnDone
		}
		return m.db.PingContext(ctx)
	}
	if err := m.graphHealth(); err != nil {
		return err
	}
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
