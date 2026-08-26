//go:build graph

package materializer

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mrchypark/rhiza/internal/types"
	graphdb "github.com/mstrYoda/goraphdb"
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

type graphState struct {
	db  *graphdb.DB
	mu  sync.Mutex
	tip uint64
}

type graphRequest struct {
	Hash   string                   `json:"hash"`
	Result types.GraphCommandResult `json:"result"`
}

func BuildProfile() types.Profile { return types.ProfileGraph }
func GraphEnabled() bool          { return true }

func openGraph(path string, sqliteTip uint64) (*graphState, error) {
	existing := false
	if entries, err := os.ReadDir(path); err == nil {
		existing = len(entries) > 0
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	opts := graphdb.DefaultOptions()
	opts.ShardCount = 1
	opts.NoSync = false
	opts.EnableWAL = false
	opts.Role = "standalone"
	opts.MaxResultRows = MaxReturningRows
	db, err := graphdb.Open(path, opts)
	if err != nil {
		return nil, err
	}
	g := &graphState{db: db}
	encodedTip, err := db.GetMetadata(graphTipKey)
	if err != nil {
		g.close()
		return nil, err
	}
	if encodedTip == nil {
		if existing || sqliteTip != 0 {
			g.close()
			return nil, fmt.Errorf("existing graph state has no applied slot; rebuild from the decision log")
		}
		if err := db.UpdateAtomic(context.Background(), func(tx *graphdb.AtomicTx) error {
			return tx.PutMetadata(graphTipKey, encodeGraphTip(0))
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
	if g.tip < sqliteTip {
		g.close()
		return nil, fmt.Errorf("graph applied slot %d is behind SQLite slot %d; rebuild from the decision log", g.tip, sqliteTip)
	}
	return g, nil
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

func graphRequestKey(id string) []byte { return []byte("rhiza/request/" + id) }

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

func (m *Materializer) applyGraph(ctx context.Context, slot uint64, value []byte, command types.GraphCommand, graph bool) error {
	g := m.graph
	if g == nil || g.db == nil {
		return fmt.Errorf("GoraphDB is not open")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if slot <= g.tip {
		return nil
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
		if err := g.advanceTip(ctx, slot); err != nil {
			return err
		}
		g.tip = slot
		return nil
	}

	result, hash, err := prepareGraphCommand(command)
	if err == nil {
		err = g.applyCommand(ctx, slot, command, hash, &result)
	}
	if err != nil {
		result = types.GraphCommandResult{Error: err.Error()}
		if recordErr := g.recordFailure(ctx, slot, command, hash, result); recordErr != nil {
			return recordErr
		}
	}
	g.tip = slot
	return nil
}

func prepareGraphCommand(command types.GraphCommand) (types.GraphCommandResult, string, error) {
	if err := ValidateGraphCommand(command); err != nil {
		return types.GraphCommandResult{}, "", err
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return types.GraphCommandResult{}, "", err
	}
	hash := sha256.Sum256(encoded)
	return types.GraphCommandResult{}, hex.EncodeToString(hash[:]), nil
}

func (g *graphState) advanceTip(ctx context.Context, slot uint64) error {
	return g.db.UpdateAtomic(ctx, func(tx *graphdb.AtomicTx) error {
		return tx.PutMetadata(graphTipKey, encodeGraphTip(slot))
	})
}

func (g *graphState) applyCommand(ctx context.Context, slot uint64, command types.GraphCommand, hash string, result *types.GraphCommandResult) error {
	args, err := graphArgs(command.Args)
	if err != nil {
		return err
	}
	return g.db.UpdateAtomic(ctx, func(tx *graphdb.AtomicTx) error {
		existing, found, err := requestInTx(tx, command.RequestID)
		if err != nil {
			return err
		}
		if found {
			if existing.Hash != hash {
				return fmt.Errorf("request_id was already used for a different graph command")
			}
			*result = existing.Result
			return tx.PutMetadata(graphTipKey, encodeGraphTip(slot))
		}
		queryResult, err := tx.CypherWithParams(ctx, command.Cypher, args)
		if err != nil {
			return err
		}
		*result = collectGoraphRows(queryResult)
		if err := putRequest(tx, command.RequestID, graphRequest{Hash: hash, Result: *result}); err != nil {
			return err
		}
		return tx.PutMetadata(graphTipKey, encodeGraphTip(slot))
	})
}

func (g *graphState) recordFailure(ctx context.Context, slot uint64, command types.GraphCommand, hash string, result types.GraphCommandResult) error {
	if hash == "" {
		encoded, err := json.Marshal(command)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(encoded)
		hash = hex.EncodeToString(sum[:])
	}
	return g.db.UpdateAtomic(ctx, func(tx *graphdb.AtomicTx) error {
		_, found, err := requestInTx(tx, command.RequestID)
		if err != nil {
			return err
		}
		if !found && command.RequestID != "" {
			if err := putRequest(tx, command.RequestID, graphRequest{Hash: hash, Result: result}); err != nil {
				return err
			}
		}
		return tx.PutMetadata(graphTipKey, encodeGraphTip(slot))
	})
}

func putRequest(tx *graphdb.AtomicTx, id string, request graphRequest) error {
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return tx.PutMetadata(graphRequestKey(id), encoded)
}

func requestInTx(tx *graphdb.AtomicTx, id string) (graphRequest, bool, error) {
	return decodeGraphRequest(tx.GetMetadata(graphRequestKey(id)))
}

func (g *graphState) request(id string) (graphRequest, bool, error) {
	data, err := g.db.GetMetadata(graphRequestKey(id))
	if err != nil {
		return graphRequest{}, false, err
	}
	return decodeGraphRequest(data)
}

func decodeGraphRequest(data []byte) (graphRequest, bool, error) {
	if data == nil {
		return graphRequest{}, false, nil
	}
	var request graphRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return graphRequest{}, false, fmt.Errorf("invalid graph request record: %w", err)
	}
	return request, true, nil
}

func knownNonGraphValue(value []byte) (bool, error) {
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
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return types.GraphCommandResult{}, err
	}
	if len(cypher) == 0 || len(cypher) > MaxSQLBytes || len(args) > MaxSQLArgs {
		return types.GraphCommandResult{}, fmt.Errorf("invalid graph query")
	}
	converted, err := graphArgs(args)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	g := m.graph
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tip != m.tip {
		return types.GraphCommandResult{}, fmt.Errorf("graph materializer tip %d does not match SQLite tip %d", g.tip, m.tip)
	}
	result, err := g.db.CypherReadWithParams(ctx, cypher, converted)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	response := collectGoraphRows(result)
	if len(response.Rows) > MaxReturningRows {
		return types.GraphCommandResult{}, fmt.Errorf("graph result exceeds %d rows", MaxReturningRows)
	}
	return response, nil
}

func collectGoraphRows(result *graphdb.CypherResult) types.GraphCommandResult {
	response := types.GraphCommandResult{Columns: append([]string(nil), result.Columns...)}
	for _, source := range result.Rows {
		row := make([]any, len(result.Columns))
		for i, column := range result.Columns {
			row[i] = source[column]
		}
		response.Rows = append(response.Rows, row)
	}
	return response
}

func (m *Materializer) GraphRequestResult(_ context.Context, requestID string) (types.GraphCommandResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g := m.graph
	g.mu.Lock()
	defer g.mu.Unlock()
	request, found, err := g.request(requestID)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	if !found {
		return types.GraphCommandResult{}, fmt.Errorf("graph request not found")
	}
	return request.Result, nil
}

func (m *Materializer) GraphRequestMatches(_ context.Context, command types.GraphCommand) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	encoded, err := json.Marshal(command)
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256(encoded)
	g := m.graph
	g.mu.Lock()
	defer g.mu.Unlock()
	request, found, err := g.request(command.RequestID)
	if err != nil || !found {
		return !found, err
	}
	return request.Hash == hex.EncodeToString(hash[:]), nil
}

func (m *Materializer) graphHealth() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.graph == nil || m.graph.db == nil {
		return fmt.Errorf("GoraphDB is not open")
	}
	if m.graph.tip != m.tip {
		return fmt.Errorf("graph materializer tip %d does not match SQLite tip %d", m.graph.tip, m.tip)
	}
	return nil
}

const graphSnapshotMagic = "RHIZA-GORAPHDB-SNAPSHOT-1\n"

const (
	maxGraphSnapshotFiles     = 65536
	maxGraphSnapshotExpansion = 128
	minGraphSnapshotExtracted = 64 << 20
)

func (m *Materializer) writeSnapshot(sqlitePath string, output io.Writer) error {
	g := m.graph
	if g == nil {
		return fmt.Errorf("GoraphDB is not open")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tip != m.tip {
		return fmt.Errorf("cannot checkpoint mismatched materializers: SQLite=%d GoraphDB=%d", m.tip, g.tip)
	}
	if err := g.db.Close(); err != nil {
		return err
	}
	g.db = nil
	graphPath := filepath.Join(filepath.Dir(m.dbPath), "goraphdb")
	snapshotErr := encodeGraphSnapshotFile(sqlitePath, graphPath, output)
	reopened, reopenErr := openGraph(graphPath, m.tip)
	if reopenErr != nil {
		if snapshotErr != nil {
			return fmt.Errorf("snapshot graph: %v; reopen graph: %w", snapshotErr, reopenErr)
		}
		return fmt.Errorf("reopen graph after snapshot: %w", reopenErr)
	}
	g.db, g.tip = reopened.db, reopened.tip
	reopened.db = nil
	return snapshotErr
}

func encodeGraphSnapshotFile(sqlitePath, graphPath string, output io.Writer) error {
	if _, err := io.WriteString(output, graphSnapshotMagic); err != nil {
		return err
	}
	zw := zip.NewWriter(output)
	sqliteWriter, err := zw.CreateHeader(&zip.FileHeader{Name: "sqlite.db", Method: zip.Deflate})
	if err == nil {
		var file *os.File
		file, err = os.Open(sqlitePath)
		if err == nil {
			_, err = io.Copy(sqliteWriter, file)
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
	}
	if err == nil {
		err = writeGraphFiles(zw, graphPath)
	}
	if closeErr := zw.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeGraphFiles(zw *zip.Writer, graphPath string) error {
	return filepath.WalkDir(graphPath, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported GoraphDB snapshot file %s", path)
		}
		rel, err := filepath.Rel(graphPath, path)
		if err != nil {
			return err
		}
		writer, err := zw.CreateHeader(&zip.FileHeader{Name: "goraphdb/" + filepath.ToSlash(rel), Method: zip.Deflate})
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(writer, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func prepareSnapshot(data []byte, dir string) (snapshotParts, error) {
	if !bytes.HasPrefix(data, []byte(graphSnapshotMagic)) {
		return snapshotParts{}, fmt.Errorf("graph build requires a GoraphDB Graph/KV checkpoint")
	}
	archive := data[len(graphSnapshotMagic):]
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return snapshotParts{}, fmt.Errorf("open graph checkpoint: %w", err)
	}
	maxExtracted := int64(len(archive)) * maxGraphSnapshotExpansion
	if maxExtracted < minGraphSnapshotExtracted {
		maxExtracted = minGraphSnapshotExtracted
	}
	if len(zr.File) > maxGraphSnapshotFiles {
		return snapshotParts{}, fmt.Errorf("graph checkpoint exceeds entry limit")
	}
	var extracted uint64
	for _, file := range zr.File {
		if file.FileInfo().IsDir() {
			continue
		}
		if file.UncompressedSize64 > uint64(maxExtracted)-extracted {
			return snapshotParts{}, fmt.Errorf("graph checkpoint exceeds extraction limits")
		}
		extracted += file.UncompressedSize64
	}
	root, err := os.MkdirTemp(dir, ".rhiza-graph-restore-*")
	if err != nil {
		return snapshotParts{}, err
	}
	parts := snapshotParts{graphDir: filepath.Join(root, "goraphdb"), cleanup: func() { _ = os.RemoveAll(root) }}
	graphFiles := 0
	for _, file := range zr.File {
		name := filepath.ToSlash(filepath.Clean(file.Name))
		if name == "sqlite.db" {
			reader, err := file.Open()
			if err != nil {
				parts.cleanup()
				return snapshotParts{}, err
			}
			parts.sqlite, err = io.ReadAll(io.LimitReader(reader, int64(file.UncompressedSize64)+1))
			closeErr := reader.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				parts.cleanup()
				return snapshotParts{}, err
			}
			if uint64(len(parts.sqlite)) != file.UncompressedSize64 {
				parts.cleanup()
				return snapshotParts{}, fmt.Errorf("invalid sqlite checkpoint size")
			}
			continue
		}
		if file.FileInfo().IsDir() {
			continue
		}
		if !file.Mode().IsRegular() || !strings.HasPrefix(name, "goraphdb/") {
			parts.cleanup()
			return snapshotParts{}, fmt.Errorf("invalid graph checkpoint path %q", file.Name)
		}
		rel := strings.TrimPrefix(name, "goraphdb/")
		target := filepath.Join(parts.graphDir, filepath.FromSlash(rel))
		cleanRel, err := filepath.Rel(parts.graphDir, target)
		if err != nil || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
			parts.cleanup()
			return snapshotParts{}, fmt.Errorf("invalid graph checkpoint path %q", file.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			parts.cleanup()
			return snapshotParts{}, err
		}
		reader, err := file.Open()
		if err != nil {
			parts.cleanup()
			return snapshotParts{}, err
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			var copied int64
			copied, err = io.Copy(output, io.LimitReader(reader, int64(file.UncompressedSize64)+1))
			if err == nil && uint64(copied) != file.UncompressedSize64 {
				err = fmt.Errorf("invalid graph checkpoint size for %q", file.Name)
			}
		}
		_ = reader.Close()
		if output != nil {
			if closeErr := output.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			parts.cleanup()
			return snapshotParts{}, err
		}
		graphFiles++
	}
	if len(parts.sqlite) == 0 || graphFiles == 0 {
		parts.cleanup()
		return snapshotParts{}, fmt.Errorf("incomplete Graph/KV checkpoint")
	}
	return parts, nil
}

func (m *Materializer) validateRestoredSnapshot() error {
	if m.graph == nil {
		return fmt.Errorf("GoraphDB checkpoint is missing")
	}
	if m.graph.tip != m.tip {
		return fmt.Errorf("checkpoint materializer tips differ: SQLite=%d GoraphDB=%d", m.tip, m.graph.tip)
	}
	return nil
}
