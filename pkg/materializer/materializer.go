package materializer

import (
	"bytes"
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
	MaxSQLBytes      = 256 << 10
	MaxSQLStatements = 128
	MaxSQLArgs       = 999
	MaxReturningRows = 10_000
	MaxResultBytes   = 16 << 20
	MaxCellBytes     = 1 << 20

	notificationSubscriberLimit = 64
	notificationQueueDepth      = 1
	notificationDispatchDepth   = 64
)

// Materializer applies decided values to SQLite.
// Uses single writer, multiple readers pattern like Hiqlite.
type Materializer struct {
	db                *sql.DB
	writer            *sql.DB
	readers           []*sql.DB
	mu                sync.RWMutex
	tip               uint64
	stateTip          uint64
	tipHash           [32]byte
	dbPath            string
	readersN          int
	idempotencyWindow uint64
	graph             *graphState
	notifyMu          sync.Mutex
	nextSub           uint64
	subs              map[uint64]notificationSubscription
	notifyQueue       chan pendingNotification
	notifyStop        chan struct{}
	notifyStopOnce    sync.Once
	notifyWG          sync.WaitGroup
	notifyDrops       atomic.Uint64
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
	writerDSN := fileURL + "?_pragma=journal_mode(wal)&_pragma=synchronous(normal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	readerDSN := fileURL + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"

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
		subs:              make(map[uint64]notificationSubscription),
		notifyQueue:       make(chan pendingNotification, notificationDispatchDepth),
		notifyStop:        make(chan struct{}),
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
	graph, err := openGraph(filepath.Join(filepath.Dir(dbPath), "latticedb"), m.tip, window)
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
			PRIMARY KEY(kind, request_id)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot);
		`)
	if err != nil {
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

type storedReceipt struct {
	fingerprint [32]byte
	receipt     types.MutationReceipt
}

func scanReceipt(scanner interface{ Scan(...any) error }, window uint64) (storedReceipt, error) {
	var record storedReceipt
	var fingerprint []byte
	var applied int
	err := scanner.Scan(&fingerprint, &record.receipt.Slot, &record.receipt.Status, &record.receipt.ErrorCode, &record.receipt.RowsAffected, &record.receipt.LastInsertID, &applied)
	if err != nil {
		return storedReceipt{}, err
	}
	if len(fingerprint) != sha256.Size {
		return storedReceipt{}, fmt.Errorf("invalid idempotency fingerprint")
	}
	copy(record.fingerprint[:], fingerprint)
	record.receipt.Applied = applied != 0
	record.receipt.RetryThroughSlot = record.receipt.Slot + window - 1
	return record, nil
}

func receiptQuery() string {
	return `SELECT fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied FROM _rhiza_idempotency WHERE kind = ? AND request_id = ?`
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
	_, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, kind, requestID, fingerprint[:], receipt.Slot, receipt.Status, receipt.ErrorCode, receipt.RowsAffected, receipt.LastInsertID, receipt.Applied)
	return err
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
	oldTip, oldStateTip, oldHash := m.tip, m.stateTip, m.tipHash
	pending := make([]pendingNotification, 0)
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
		if err := m.applyValueLocked(ctx, tx, slot, decision.Value, hash, oldTip, &pending); err != nil {
			m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
			return err
		}
		m.tip, m.tipHash = slot, hash
	}
	if err := tx.Commit(); err != nil {
		m.tip, m.stateTip, m.tipHash = oldTip, oldStateTip, oldHash
		return fmt.Errorf("commit apply batch: %w", err)
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

func (m *Materializer) enqueueNotification(notification pendingNotification) {
	select {
	case m.notifyQueue <- notification:
	default:
		m.notifyDrops.Add(1)
	}
}

func (m *Materializer) applyValueLocked(ctx context.Context, tx *sql.Tx, slot uint64, value []byte, hash [32]byte, confirmedGraphThrough uint64, pending *[]pendingNotification) error {
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
	for _, command := range commands {
		if err := ValidateSQLCommand(command); err != nil {
			return fmt.Errorf("validate SQL request %q: %w", command.RequestID, err)
		}
		fingerprint, err := types.SQLFingerprint(command)
		if err != nil {
			return fmt.Errorf("encode SQL request %q: %w", command.RequestID, err)
		}
		if command.RequestID != "" {
			_, found, err := m.receiptInTx(ctx, tx, types.MutationSQL, command.RequestID)
			if err != nil {
				return fmt.Errorf("check SQL request %q: %w", command.RequestID, err)
			}
			if found {
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, "SAVEPOINT rhiza_command"); err != nil {
			return err
		}
		result, executeErr := executeSQLCommand(ctx, tx, command)
		receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted}
		if executeErr != nil {
			if command.RequestID == "" {
				return executeErr
			}
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO rhiza_command"); err != nil {
				return err
			}
			receipt.Status = types.MutationRejected
			receipt.ErrorCode = "execution_failed"
		} else {
			for _, statement := range result.Statements {
				receipt.RowsAffected += statement.RowsAffected
				receipt.LastInsertID = statement.LastInsertID
			}
		}
		if _, err := tx.ExecContext(ctx, "RELEASE rhiza_command"); err != nil {
			return err
		}
		if command.RequestID != "" {
			if err := insertReceipt(ctx, tx, types.MutationSQL, command.RequestID, fingerprint, receipt); err != nil {
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
	if mutatesState {
		if _, err := tx.ExecContext(ctx, `UPDATE _rhiza_meta SET value = ? WHERE key = 'state_slot'`, strconv.FormatUint(slot, 10)); err != nil {
			return fmt.Errorf("persist state slot: %w", err)
		}
		m.stateTip = slot
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
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	if len(statements) == 0 || len(statements) > MaxSQLStatements {
		return fmt.Errorf("statement count must be between 1 and %d", MaxSQLStatements)
	}
	for _, statement := range statements {
		if statement.WantRows {
			return fmt.Errorf("mutation row results are unsupported; issue a query after the returned slot")
		}
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("SQL is required")
		}
		if strings.IndexByte(statement.SQL, 0) >= 0 {
			return fmt.Errorf("SQL contains a null byte")
		}
		if len(statement.SQL) > MaxSQLBytes {
			return fmt.Errorf("SQL exceeds %d bytes", MaxSQLBytes)
		}
		if len(statement.Args) > MaxSQLArgs {
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

func executeSQLCommand(ctx context.Context, tx *sql.Tx, command types.SQLCommand) (types.SQLCommandResult, error) {
	statements := command.Statements
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	result := types.SQLCommandResult{Statements: make([]types.SQLStatementResult, 0, len(statements))}
	budget := resultBudget{rows: MaxReturningRows, bytes: MaxResultBytes}
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
			prepared, err := tx.PrepareContext(ctx, statement.SQL)
			if err != nil {
				return result, err
			}
			rows, err := prepared.QueryContext(ctx, args...)
			if err != nil {
				prepared.Close()
				return result, err
			}
			statementResult, err := collectRowsWithBudget(rows, &budget)
			rows.Close()
			prepared.Close()
			if err != nil {
				return result, err
			}
			result.Statements = append(result.Statements, statementResult)
			continue
		}
		prepared, err := tx.PrepareContext(ctx, statement.SQL)
		if err != nil {
			return result, err
		}
		execResult, err := prepared.ExecContext(ctx, args...)
		prepared.Close()
		if err != nil {
			return result, err
		}
		statementResult := types.SQLStatementResult{}
		statementResult.RowsAffected, _ = execResult.RowsAffected()
		statementResult.LastInsertID, _ = execResult.LastInsertId()
		result.Statements = append(result.Statements, statementResult)
	}
	encodedBytes, err := encodedJSONSize(result)
	if err != nil {
		return result, err
	}
	if encodedBytes > MaxResultBytes {
		return result, fmt.Errorf("result exceeds %d encoded bytes", MaxResultBytes)
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

type resultBudget struct {
	rows  int
	bytes int
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
	return collectRowsWithBudget(rows, &resultBudget{rows: limit, bytes: MaxResultBytes})
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
		return result, fmt.Errorf("result exceeds %d bytes", MaxResultBytes)
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
			size := 16
			switch value := value.(type) {
			case string:
				size = len(value)
			case []byte:
				size = len(value)
			case nil:
				size = 1
			}
			if size > MaxCellBytes {
				return result, fmt.Errorf("result cell exceeds %d bytes", MaxCellBytes)
			}
			budget.bytes -= size
			if budget.bytes < 0 {
				return result, fmt.Errorf("result exceeds %d bytes", MaxResultBytes)
			}
		}
		result.Rows = append(result.Rows, values)
		budget.rows--
	}
	return result, rows.Err()
}

func (m *Materializer) SQLRequestMatches(ctx context.Context, command types.SQLCommand) (bool, error) {
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationSQL, command.RequestID, fingerprint)
	return matches, err
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

// OpenLocalSQLReader returns a caller-owned read-only handle to the local
// materialized SQLite state. HA callers that require freshness must establish
// a linearizable barrier through the Rhiza API before reading.
func (m *Materializer) OpenLocalSQLReader() (*sql.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.writer == nil {
		return nil, sql.ErrConnDone
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(m.dbPath)}).String()
	return openSQLite(fileURL+"?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", false)
}

// SpeculateSQL runs a transaction against the current local materialization
// and always rolls it back. It exists for embedded adapters that must plan one
// deterministic Statements batch; only Execute may commit replicated state.
func (m *Materializer) SpeculateSQL(ctx context.Context, fn func(*sql.Tx) error) error {
	if fn == nil {
		return fmt.Errorf("speculative SQL callback is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return sql.ErrConnDone
	}
	tx, err := m.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
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
	m.db, m.writer, m.readers, m.graph = source.db, source.writer, source.readers, source.graph
	m.tip, m.stateTip, m.tipHash = source.tip, source.stateTip, source.tipHash
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
