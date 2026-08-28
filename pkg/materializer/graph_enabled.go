//go:build graph

package materializer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
	"github.com/mrchypark/rhiza/internal/types"
)

func graphArgs(args map[string]any) (map[string]any, error) {
	result := make(map[string]any, len(args))
	for key, value := range args {
		converted, err := graphArg(value)
		if err != nil {
			return nil, err
		}
		result[key] = converted
	}
	return result, nil
}

var graphTipKey = []byte("rhiza/applied_slot")
var graphJournalKey = []byte("rhiza/recovery_journal")

type graphState struct {
	db                *latticedb.DB
	mu                sync.RWMutex
	tip               uint64
	idempotencyWindow uint64
}

type graphSnapshot struct {
	snapshot *latticedb.Snapshot
}

func (m *Materializer) beginGraphSnapshot() (*graphSnapshot, error) {
	m.graph.mu.Lock()
	defer m.graph.mu.Unlock()
	snapshot, err := m.graph.db.BeginSnapshot()
	if err != nil {
		return nil, err
	}
	return &graphSnapshot{snapshot: snapshot}, nil
}

func (snapshot *graphSnapshot) Backup(path string) error { return snapshot.snapshot.Backup(path) }
func (snapshot *graphSnapshot) Close() error             { return snapshot.snapshot.Close() }
func (m *Materializer) graphTip() uint64                 { return m.graph.tip }

type graphRequest struct {
	Fingerprint [32]byte
	Receipt     types.MutationReceipt
}

type graphJournalEntry struct {
	Slot uint64
	Hash [32]byte
}

func BuildProfile() types.Profile { return types.ProfileGraph }
func GraphEnabled() bool          { return true }

func openGraph(path string, sqliteTip, idempotencyWindow uint64) (*graphState, error) {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(path, "graph.ltdb")
	existing := false
	if info, err := os.Stat(dbPath); err == nil {
		existing = info.Size() > 0
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	db, err := latticedb.Open(dbPath, latticedb.OpenOptions{
		Create:               true,
		CacheSizeMB:          32,
		EnableAdjacencyCache: true,
		NoSync:               true,
	})
	if err != nil {
		return nil, err
	}
	g := &graphState{db: db, idempotencyWindow: idempotencyWindow}
	encodedTip, err := g.getMetadata(graphTipKey)
	if err != nil {
		g.close()
		return nil, err
	}
	if encodedTip == nil {
		if existing || sqliteTip != 0 {
			g.close()
			return nil, fmt.Errorf("existing graph state has no applied slot; rebuild from the decision log")
		}
		if err := db.Update(func(tx *latticedb.Tx) error {
			if err := tx.PutAppMetadata(graphTipKey, encodeGraphTip(0)); err != nil {
				return err
			}
			return tx.PutAppMetadata(graphJournalKey, nil)
		}); err != nil {
			g.close()
			return nil, err
		}
	} else {
		g.tip, err = decodeGraphTip(encodedTip)
		if err != nil {
			g.close()
			return nil, err
		}
	}
	encodedJournal, err := g.getMetadata(graphJournalKey)
	if err != nil {
		g.close()
		return nil, err
	}
	journal, err := decodeGraphJournal(encodedJournal)
	if err != nil {
		g.close()
		return nil, err
	}
	if g.tip < sqliteTip {
		g.close()
		return nil, fmt.Errorf("graph applied slot %d is behind SQLite slot %d; rebuild from the decision log", g.tip, sqliteTip)
	}
	journal = pendingGraphJournal(journal, sqliteTip)
	if g.tip > sqliteTip && (len(journal) != int(g.tip-sqliteTip) || journal[0].Slot != sqliteTip+1 || journal[len(journal)-1].Slot != g.tip) {
		g.close()
		return nil, fmt.Errorf("graph recovery journal does not cover SQLite gap %d..%d", sqliteTip+1, g.tip)
	}
	if err := db.Update(func(tx *latticedb.Tx) error {
		return tx.PutAppMetadata(graphJournalKey, encodeGraphJournal(journal))
	}); err != nil {
		g.close()
		return nil, err
	}
	return g, nil
}

func (g *graphState) getMetadata(key []byte) ([]byte, error) {
	var value []byte
	err := g.db.View(func(tx *latticedb.Tx) error {
		var ok bool
		var err error
		value, ok, err = tx.GetAppMetadata(key)
		if err != nil || ok {
			return err
		}
		value = nil
		return nil
	})
	return value, err
}

func encodeGraphJournal(entries []graphJournalEntry) []byte {
	data := make([]byte, len(entries)*40)
	for i, entry := range entries {
		offset := i * 40
		binary.BigEndian.PutUint64(data[offset:offset+8], entry.Slot)
		copy(data[offset+8:offset+40], entry.Hash[:])
	}
	return data
}

func decodeGraphJournal(data []byte) ([]graphJournalEntry, error) {
	if len(data)%40 != 0 {
		return nil, fmt.Errorf("invalid graph recovery journal")
	}
	entries := make([]graphJournalEntry, len(data)/40)
	for i := range entries {
		offset := i * 40
		entries[i].Slot = binary.BigEndian.Uint64(data[offset : offset+8])
		copy(entries[i].Hash[:], data[offset+8:offset+40])
		if entries[i].Slot == 0 || i > 0 && entries[i-1].Slot+1 != entries[i].Slot {
			return nil, fmt.Errorf("invalid graph recovery journal")
		}
	}
	return entries, nil
}

func pendingGraphJournal(entries []graphJournalEntry, through uint64) []graphJournalEntry {
	for len(entries) > 0 && entries[0].Slot <= through {
		entries = entries[1:]
	}
	return entries
}

func encodeGraphTip(tip uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, tip)
	return data
}

func decodeGraphTip(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid graph applied slot")
	}
	return binary.BigEndian.Uint64(data), nil
}

func graphRequestKey(id string) []byte { return []byte("rhiza/idem/by-id/" + id) }

func graphSlotKey(slot uint64) []byte {
	key := make([]byte, len("rhiza/idem/by-slot/")+8)
	copy(key, "rhiza/idem/by-slot/")
	binary.BigEndian.PutUint64(key[len("rhiza/idem/by-slot/"):], slot)
	return key
}

func (g *graphState) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.db != nil {
		_ = g.db.Close()
		g.db = nil
	}
}

func (m *Materializer) applyGraph(ctx context.Context, slot uint64, value []byte, commands []types.GraphCommand, graph bool) error {
	g := m.graph
	if g == nil || g.db == nil {
		return fmt.Errorf("LatticeDB is not open")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	valueHash := sha256.Sum256(value)
	if slot <= g.tip {
		encoded, err := g.getMetadata(graphJournalKey)
		if err != nil {
			return err
		}
		journal, err := decodeGraphJournal(encoded)
		if err != nil {
			return err
		}
		for _, entry := range journal {
			if entry.Slot == slot {
				if entry.Hash != valueHash {
					return fmt.Errorf("graph applied slot %d hash conflict", slot)
				}
				return nil
			}
		}
		return fmt.Errorf("graph applied slot %d is missing from recovery journal", slot)
	}
	if slot != g.tip+1 {
		return fmt.Errorf("graph apply slot gap: have %d, got %d", g.tip, slot)
	}
	if commands, sqlBatch, err := types.DecodeSQLBatch(value); err != nil {
		return err
	} else if sqlBatch && len(commands) > 0 {
		return fmt.Errorf("SQL command is not supported by the graph-kv build")
	}
	if !graph {
		known, err := knownNonGraphValue(value)
		if err != nil {
			return err
		}
		if !known {
			return fmt.Errorf("unrecognized command is not supported by the graph-kv build")
		}
		if err := g.advanceTip(ctx, slot, valueHash); err != nil {
			return err
		}
		g.tip = slot
		return nil
	}

	for i, command := range commands {
		advance := i == len(commands)-1
		fingerprint, err := prepareGraphCommand(command)
		if err == nil {
			err = g.applyCommand(ctx, slot, valueHash, command, fingerprint, advance)
		}
		if err != nil {
			if recordErr := g.recordFailure(ctx, slot, valueHash, command, fingerprint, advance); recordErr != nil {
				return recordErr
			}
		}
	}
	g.tip = slot
	return nil
}

func prepareGraphCommand(command types.GraphCommand) ([32]byte, error) {
	if err := ValidateGraphCommand(command); err != nil {
		return [32]byte{}, err
	}
	return types.GraphFingerprint(command)
}

func (g *graphState) advanceTip(ctx context.Context, slot uint64, valueHash [32]byte) error {
	return g.db.Update(func(tx *latticedb.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return advanceGraphMetadata(tx, slot, valueHash, g.idempotencyWindow)
	})
}

func (g *graphState) applyCommand(ctx context.Context, slot uint64, valueHash [32]byte, command types.GraphCommand, fingerprint [32]byte, advance bool) error {
	args, err := graphArgs(command.Args)
	if err != nil {
		return err
	}
	return g.db.Update(func(tx *latticedb.Tx) error {
		existing, found, err := requestInTx(tx, command.RequestID)
		if err != nil {
			return err
		}
		if found {
			if existing.Fingerprint != fingerprint {
				return fmt.Errorf("request_id was already used for a different graph command")
			}
			if advance {
				return advanceGraphMetadata(tx, slot, valueHash, g.idempotencyWindow)
			}
			return nil
		}
		_, err = tx.QueryContext(ctx, command.Cypher, args, MaxReturningRows, MaxResultBytes)
		if err != nil {
			return err
		}
		for _, event := range command.Events {
			payload, err := graphArg(event.Payload)
			if err != nil {
				return err
			}
			if err := tx.PublishStream(event.Stream, event.Kind, payload); err != nil {
				return err
			}
		}
		receipt := types.MutationReceipt{Slot: slot, Status: types.MutationCommitted, Applied: true, RetryThroughSlot: slot + g.idempotencyWindow - 1}
		if err := putRequest(tx, command.RequestID, graphRequest{Fingerprint: fingerprint, Receipt: receipt}); err != nil {
			return err
		}
		if advance {
			return advanceGraphMetadata(tx, slot, valueHash, g.idempotencyWindow)
		}
		return nil
	})
}

func (g *graphState) recordFailure(ctx context.Context, slot uint64, valueHash [32]byte, command types.GraphCommand, fingerprint [32]byte, advance bool) error {
	if fingerprint == ([32]byte{}) {
		var err error
		fingerprint, err = types.GraphFingerprint(command)
		if err != nil {
			return err
		}
	}
	return g.db.Update(func(tx *latticedb.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, found, err := requestInTx(tx, command.RequestID)
		if err != nil {
			return err
		}
		if !found && command.RequestID != "" {
			receipt := types.MutationReceipt{Slot: slot, Status: types.MutationRejected, ErrorCode: "execution_failed", RetryThroughSlot: slot + g.idempotencyWindow - 1}
			if err := putRequest(tx, command.RequestID, graphRequest{Fingerprint: fingerprint, Receipt: receipt}); err != nil {
				return err
			}
		}
		if advance {
			return advanceGraphMetadata(tx, slot, valueHash, g.idempotencyWindow)
		}
		return nil
	})
}

func advanceGraphMetadata(tx *latticedb.Tx, slot uint64, hash [32]byte, window uint64) error {
	journalData, _, err := tx.GetAppMetadata(graphJournalKey)
	if err != nil {
		return err
	}
	journal, err := decodeGraphJournal(journalData)
	if err != nil {
		return err
	}
	if len(journal) != 0 && journal[len(journal)-1].Slot+1 != slot {
		return fmt.Errorf("graph recovery journal slot gap")
	}
	journal = append(journal, graphJournalEntry{Slot: slot, Hash: hash})
	if err := tx.PutAppMetadata(graphJournalKey, encodeGraphJournal(journal)); err != nil {
		return err
	}
	if slot >= window {
		if err := pruneGraphRequests(tx, slot-window); err != nil {
			return err
		}
	}
	return tx.PutAppMetadata(graphTipKey, encodeGraphTip(slot))
}

func (m *Materializer) confirmGraphThrough(ctx context.Context, through uint64) error {
	g := m.graph
	if g == nil || g.db == nil {
		return fmt.Errorf("LatticeDB is not open")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.db.Update(func(tx *latticedb.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		journalData, _, err := tx.GetAppMetadata(graphJournalKey)
		if err != nil {
			return err
		}
		journal, err := decodeGraphJournal(journalData)
		if err != nil {
			return err
		}
		return tx.PutAppMetadata(graphJournalKey, encodeGraphJournal(pendingGraphJournal(journal, through)))
	})
}

func putRequest(tx *latticedb.Tx, id string, request graphRequest) error {
	if id == "" || len(id) > types.MaxRequestIDBytes {
		return fmt.Errorf("invalid graph request ID length")
	}
	if err := tx.PutAppMetadata(graphRequestKey(id), encodeGraphRequest(request)); err != nil {
		return err
	}
	key := graphSlotKey(request.Receipt.Slot)
	ids, _, err := tx.GetAppMetadata(key)
	if err != nil {
		return err
	}
	ids = binary.AppendUvarint(ids, uint64(len(id)))
	ids = append(ids, id...)
	return tx.PutAppMetadata(key, ids)
}

func requestInTx(tx *latticedb.Tx, id string) (graphRequest, bool, error) {
	data, _, err := tx.GetAppMetadata(graphRequestKey(id))
	if err != nil {
		return graphRequest{}, false, err
	}
	return decodeGraphRequest(data)
}

func (g *graphState) request(id string) (graphRequest, bool, error) {
	data, err := g.getMetadata(graphRequestKey(id))
	if err != nil {
		return graphRequest{}, false, err
	}
	request, found, err := decodeGraphRequest(data)
	if found {
		request.Receipt.RetryThroughSlot = request.Receipt.Slot + g.idempotencyWindow - 1
	}
	return request, found, err
}

func decodeGraphRequest(data []byte) (graphRequest, bool, error) {
	if data == nil {
		return graphRequest{}, false, nil
	}
	if len(data) < 42 {
		return graphRequest{}, false, fmt.Errorf("invalid graph request record")
	}
	var request graphRequest
	copy(request.Fingerprint[:], data[:32])
	request.Receipt.Slot = binary.BigEndian.Uint64(data[32:40])
	request.Receipt.Applied = data[41]&1 != 0
	switch data[40] {
	case 1:
		request.Receipt.Status = types.MutationCommitted
	case 2:
		request.Receipt.Status = types.MutationRejected
		request.Receipt.ErrorCode = "execution_failed"
	default:
		return graphRequest{}, false, fmt.Errorf("invalid graph request status")
	}
	return request, true, nil
}

func encodeGraphRequest(request graphRequest) []byte {
	data := make([]byte, 42)
	copy(data, request.Fingerprint[:])
	binary.BigEndian.PutUint64(data[32:40], request.Receipt.Slot)
	if request.Receipt.Status == types.MutationRejected {
		data[40] = 2
	} else {
		data[40] = 1
	}
	if request.Receipt.Applied {
		data[41] = 1
	}
	return data
}

func pruneGraphRequests(tx *latticedb.Tx, slot uint64) error {
	key := graphSlotKey(slot)
	ids, _, err := tx.GetAppMetadata(key)
	if err != nil {
		return err
	}
	for len(ids) > 0 {
		encodedLength, n := binary.Uvarint(ids)
		if n <= 0 || encodedLength == 0 || encodedLength > types.MaxRequestIDBytes {
			return fmt.Errorf("invalid graph idempotency slot index")
		}
		ids = ids[n:]
		length := int(encodedLength)
		if length > len(ids) {
			return fmt.Errorf("invalid graph idempotency slot index")
		}
		if err := tx.DeleteAppMetadata(graphRequestKey(string(ids[:length]))); err != nil {
			return err
		}
		ids = ids[length:]
	}
	return tx.DeleteAppMetadata(key)
}

func knownNonGraphValue(value []byte) (bool, error) {
	if _, ok, err := types.DecodeKVBatch(value); ok || err != nil {
		return ok, err
	}
	if _, ok, err := types.DecodeKVCommand(value); ok || err != nil {
		return ok, err
	}
	if _, ok, err := types.DecodeNotifyCommand(value); ok || err != nil {
		return ok, err
	}
	if ok, err := types.DecodeReadBarrier(value); ok || err != nil {
		return ok, err
	}
	if _, ok, err := types.DecodeCheckpointSeal(value); ok || err != nil {
		return ok, err
	}
	_, ok, err := types.DecodeLeaderSchedule(value)
	return ok, err
}

func (m *Materializer) GraphQuery(ctx context.Context, cypher string, args map[string]any) (types.GraphCommandResult, error) {
	m.mu.RLock()
	if err := ctx.Err(); err != nil {
		m.mu.RUnlock()
		return types.GraphCommandResult{}, err
	}
	if len(cypher) == 0 || len(cypher) > MaxSQLBytes || len(args) > MaxSQLArgs {
		m.mu.RUnlock()
		return types.GraphCommandResult{}, fmt.Errorf("invalid graph query")
	}
	converted, err := graphArgs(args)
	if err != nil {
		m.mu.RUnlock()
		return types.GraphCommandResult{}, err
	}
	g := m.graph
	g.mu.RLock()
	if g.tip != m.tip {
		g.mu.RUnlock()
		m.mu.RUnlock()
		return types.GraphCommandResult{}, fmt.Errorf("graph materializer tip %d does not match SQLite tip %d", g.tip, m.tip)
	}
	var result latticedb.QueryResult
	err = g.db.View(func(tx *latticedb.Tx) error {
		var queryErr error
		result, queryErr = tx.QueryContext(ctx, cypher, converted, MaxReturningRows, MaxResultBytes)
		return queryErr
	})
	g.mu.RUnlock()
	m.mu.RUnlock()
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	return collectLatticeRows(result)
}

func (m *Materializer) GraphReadStream(ctx context.Context, stream string, afterSequence uint64, limit uint, wait time.Duration) ([]types.GraphStreamRecord, error) {
	if err := validateGraphStreamName(stream, true); err != nil {
		return nil, err
	}
	if limit == 0 || limit > MaxGraphStreamRecords || wait < 0 {
		return nil, fmt.Errorf("invalid graph stream read options")
	}
	deadline := time.Now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.RLock()
		g := m.graph
		if g == nil || g.db == nil {
			m.mu.RUnlock()
			return nil, fmt.Errorf("LatticeDB is not open")
		}
		g.mu.RLock()
		records, err := g.db.ReadStream(stream, afterSequence, limit, 0)
		g.mu.RUnlock()
		m.mu.RUnlock()
		if err != nil || len(records) > 0 || wait == 0 || !time.Now().Before(deadline) {
			out := make([]types.GraphStreamRecord, len(records))
			for i, record := range records {
				out[i] = types.GraphStreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: record.Payload}
			}
			return out, err
		}
		delay := min(25*time.Millisecond, time.Until(deadline))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func validateGraphStreamConsumer(consumer string) error {
	if !utf8.ValidString(consumer) || consumer == "" || len(consumer) > 255 {
		return fmt.Errorf("consumer is required and must be valid UTF-8 not exceeding 255 bytes")
	}
	return nil
}

func (m *Materializer) GraphStreamOffset(ctx context.Context, stream, consumer string) (uint64, bool, error) {
	if err := validateGraphStreamName(stream, true); err != nil {
		return 0, false, err
	}
	if err := validateGraphStreamConsumer(consumer); err != nil {
		return 0, false, err
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.graph == nil || m.graph.db == nil {
		return 0, false, fmt.Errorf("LatticeDB is not open")
	}
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	return m.graph.db.GetStreamOffset(stream, consumer)
}

func (m *Materializer) SetGraphStreamOffset(ctx context.Context, stream, consumer string, sequence uint64) error {
	if err := validateGraphStreamName(stream, true); err != nil {
		return err
	}
	if err := validateGraphStreamConsumer(consumer); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.graph == nil || m.graph.db == nil {
		return fmt.Errorf("LatticeDB is not open")
	}
	m.graph.mu.Lock()
	defer m.graph.mu.Unlock()
	return m.graph.db.Update(func(tx *latticedb.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return tx.SetStreamOffset(stream, consumer, sequence)
	})
}

func (m *Materializer) TrimGraphStream(ctx context.Context, stream string, throughSequence uint64) error {
	if err := validateGraphStreamName(stream, true); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.graph == nil || m.graph.db == nil {
		return fmt.Errorf("LatticeDB is not open")
	}
	m.graph.mu.Lock()
	defer m.graph.mu.Unlock()
	return m.graph.db.Update(func(tx *latticedb.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return tx.TrimStream(stream, throughSequence)
	})
}

func collectLatticeRows(result latticedb.QueryResult) (types.GraphCommandResult, error) {
	response := types.GraphCommandResult{Columns: append([]string(nil), result.Columns...)}
	remaining := MaxResultBytes
	for _, column := range response.Columns {
		remaining -= len(column)
	}
	for _, source := range result.Rows {
		if len(response.Rows) == MaxReturningRows {
			return types.GraphCommandResult{}, fmt.Errorf("graph result exceeds %d rows", MaxReturningRows)
		}
		row := make([]any, len(result.Columns))
		for i, column := range result.Columns {
			value := source[column]
			encodedBytes, err := encodedJSONSize(value)
			if err != nil {
				return types.GraphCommandResult{}, err
			}
			if encodedBytes > MaxCellBytes {
				return types.GraphCommandResult{}, fmt.Errorf("graph result cell exceeds %d bytes", MaxCellBytes)
			}
			remaining -= encodedBytes
			if remaining < 0 {
				return types.GraphCommandResult{}, fmt.Errorf("graph result exceeds %d bytes", MaxResultBytes)
			}
			row[i] = value
		}
		response.Rows = append(response.Rows, row)
	}
	return response, nil
}

func (m *Materializer) GraphMutationReceipt(_ context.Context, requestID string) (types.MutationReceipt, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g := m.graph
	g.mu.RLock()
	defer g.mu.RUnlock()
	request, found, err := g.request(requestID)
	return request.Receipt, found, err
}

func (m *Materializer) GraphRequestMatches(_ context.Context, command types.GraphCommand) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fingerprint, err := types.GraphFingerprint(command)
	if err != nil {
		return false, err
	}
	g := m.graph
	g.mu.RLock()
	defer g.mu.RUnlock()
	request, found, err := g.request(command.RequestID)
	if err != nil || !found {
		return !found, err
	}
	return request.Fingerprint == fingerprint, nil
}

func (m *Materializer) graphRequestExists(requestID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	_, found, err := m.graph.request(requestID)
	return found, err
}

func (m *Materializer) graphHealth() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.graph == nil || m.graph.db == nil {
		return fmt.Errorf("LatticeDB is not open")
	}
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	if m.graph.tip != m.tip {
		return fmt.Errorf("graph materializer tip %d does not match SQLite tip %d", m.graph.tip, m.tip)
	}
	return nil
}

func (*Materializer) writeSnapshot(string, io.Writer) error {
	return fmt.Errorf("graph snapshots require CheckpointFilesAt")
}

func prepareSnapshotFile(string, string) (snapshotParts, error) {
	return snapshotParts{}, fmt.Errorf("graph snapshots require fixed-role files")
}

func (m *Materializer) validateRestoredSnapshot() error {
	if m.graph == nil {
		return fmt.Errorf("LatticeDB checkpoint is missing")
	}
	if m.graph.tip != m.tip {
		return fmt.Errorf("checkpoint materializer tips differ: SQLite=%d LatticeDB=%d", m.tip, m.graph.tip)
	}
	return nil
}
