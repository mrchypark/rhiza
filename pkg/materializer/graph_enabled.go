//go:build graph

package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	lbug "github.com/LadybugDB/go-ladybug"
	"github.com/mrchypark/rhiza/internal/types"
)

type graphState struct {
	db   *lbug.Database
	conn *lbug.Connection
	mu   sync.Mutex
	tip  uint64
}

func BuildProfile() types.Profile { return types.ProfileGraph }
func GraphEnabled() bool          { return true }

func openGraph(path string, sqliteTip uint64) (*graphState, error) {
	_, statErr := os.Stat(path)
	existing := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	db, err := lbug.OpenDatabase(path, lbug.DefaultSystemConfig())
	if err != nil {
		return nil, err
	}
	conn, err := lbug.OpenConnection(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	g := &graphState{db: db, conn: conn}
	for _, schema := range []string{
		`CREATE NODE TABLE IF NOT EXISTS _RhizaMeta(key STRING, value INT64, PRIMARY KEY(key))`,
		`CREATE NODE TABLE IF NOT EXISTS _RhizaRequest(id STRING, command_hash STRING, result STRING, PRIMARY KEY(id))`,
	} {
		if err := g.queryClose(schema); err != nil {
			g.close()
			return nil, fmt.Errorf("initialize Ladybug schema: %w", err)
		}
	}
	result, err := conn.Query(`MATCH (m:_RhizaMeta {key: 'applied_slot'}) RETURN m.value`)
	if err != nil {
		g.close()
		return nil, err
	}
	rows, err := collectGraphRows(result)
	result.Close()
	if err != nil {
		g.close()
		return nil, err
	}
	if len(rows.Rows) == 0 {
		if existing || sqliteTip != 0 {
			g.close()
			return nil, fmt.Errorf("existing state has no Ladybug applied slot; rebuild from the decision log")
		}
		if err := g.queryClose(`CREATE (m:_RhizaMeta {key: 'applied_slot', value: 0})`); err != nil {
			g.close()
			return nil, err
		}
	} else {
		tip, ok := rows.Rows[0][0].(int64)
		if !ok || tip < 0 {
			g.close()
			return nil, fmt.Errorf("invalid Ladybug applied slot")
		}
		g.tip = uint64(tip)
	}
	if g.tip < sqliteTip {
		g.close()
		return nil, fmt.Errorf("Ladybug applied slot %d is behind SQLite slot %d; rebuild from the decision log", g.tip, sqliteTip)
	}
	return g, nil
}

func (g *graphState) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		g.conn.Close()
		g.conn = nil
	}
	if g.db != nil {
		g.db.Close()
		g.db = nil
	}
}

func (g *graphState) queryClose(query string) error {
	result, err := g.conn.Query(query)
	if result != nil {
		result.Close()
	}
	return err
}

func (m *Materializer) applyGraph(slot uint64, value []byte, command types.GraphCommand, graph bool) error {
	g := m.graph
	if g == nil {
		return fmt.Errorf("Ladybug is not open")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if slot <= g.tip {
		return nil
	}
	if slot != g.tip+1 {
		return fmt.Errorf("Ladybug apply slot gap: have %d, got %d", g.tip, slot)
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
	}
	if err := g.queryClose("BEGIN TRANSACTION"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = g.queryClose("ROLLBACK")
		}
	}()
	if graph {
		if err := g.applyCommand(command); err != nil {
			return err
		}
	}
	if err := g.executeClose(`MATCH (m:_RhizaMeta {key: 'applied_slot'}) SET m.value = $slot`, map[string]any{"slot": int64(slot)}); err != nil {
		return err
	}
	if err := g.queryClose("COMMIT"); err != nil {
		return err
	}
	committed = true
	g.tip = slot
	return nil
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
	_, ok, err := types.DecodeLeaderSchedule(value)
	return ok, err
}

func (g *graphState) applyCommand(command types.GraphCommand) error {
	if err := ValidateGraphCommand(command); err != nil {
		return err
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(encoded)
	hashString := hex.EncodeToString(hash[:])
	existing, found, err := g.request(command.RequestID)
	if err != nil {
		return err
	}
	if found {
		if existing.hash != hashString {
			return fmt.Errorf("request_id was already used for a different graph command")
		}
		return nil
	}
	args, err := graphArgs(command.Args)
	if err != nil {
		return err
	}
	statement, err := g.conn.Prepare(command.Cypher)
	if err != nil {
		return err
	}
	defer statement.Close()
	if statement.IsReadOnly() {
		return fmt.Errorf("graph execute requires a mutation")
	}
	result, err := g.conn.Execute(statement, args)
	if err != nil {
		return err
	}
	commandResult, err := collectGraphRows(result)
	result.Close()
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(commandResult)
	if err != nil {
		return err
	}
	return g.executeClose(`CREATE (r:_RhizaRequest {id: $id, command_hash: $hash, result: $result})`, map[string]any{
		"id": command.RequestID, "hash": hashString, "result": string(resultJSON),
	})
}

type graphRequest struct {
	hash   string
	result string
}

func (g *graphState) request(id string) (graphRequest, bool, error) {
	statement, err := g.conn.Prepare(`MATCH (r:_RhizaRequest {id: $id}) RETURN r.command_hash, r.result`)
	if err != nil {
		return graphRequest{}, false, err
	}
	defer statement.Close()
	result, err := g.conn.Execute(statement, map[string]any{"id": id})
	if err != nil {
		return graphRequest{}, false, err
	}
	defer result.Close()
	rows, err := collectGraphRows(result)
	if err != nil || len(rows.Rows) == 0 {
		return graphRequest{}, false, err
	}
	hash, hashOK := rows.Rows[0][0].(string)
	encoded, resultOK := rows.Rows[0][1].(string)
	if !hashOK || !resultOK {
		return graphRequest{}, false, fmt.Errorf("invalid graph request record")
	}
	return graphRequest{hash: hash, result: encoded}, true, nil
}

func (g *graphState) executeClose(query string, args map[string]any) error {
	statement, err := g.conn.Prepare(query)
	if err != nil {
		return err
	}
	defer statement.Close()
	result, err := g.conn.Execute(statement, args)
	if result != nil {
		result.Close()
	}
	return err
}

func (m *Materializer) GraphQuery(ctx context.Context, cypher string, args map[string]any) (types.GraphCommandResult, error) {
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
	statement, err := g.conn.Prepare(cypher)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	defer statement.Close()
	if !statement.IsReadOnly() {
		return types.GraphCommandResult{}, fmt.Errorf("graph query must be read-only")
	}
	result, err := g.conn.Execute(statement, converted)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	defer result.Close()
	return collectGraphRows(result)
}

func collectGraphRows(result *lbug.QueryResult) (types.GraphCommandResult, error) {
	response := types.GraphCommandResult{Columns: append([]string(nil), result.GetColumnNames()...)}
	for result.HasNext() {
		if len(response.Rows) >= MaxReturningRows {
			return response, fmt.Errorf("graph result exceeds %d rows", MaxReturningRows)
		}
		tuple, err := result.Next()
		if err != nil {
			return response, err
		}
		row, err := tuple.GetAsSlice()
		tuple.Close()
		if err != nil {
			return response, err
		}
		response.Rows = append(response.Rows, row)
	}
	return response, nil
}

func (m *Materializer) GraphRequestResult(_ context.Context, requestID string) (types.GraphCommandResult, error) {
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
	var result types.GraphCommandResult
	err = json.Unmarshal([]byte(request.result), &result)
	return result, err
}

func (m *Materializer) GraphRequestMatches(_ context.Context, command types.GraphCommand) (bool, error) {
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
	return bytes.Equal([]byte(request.hash), []byte(hex.EncodeToString(hash[:]))), nil
}

func (m *Materializer) graphHealth() error {
	if m.graph == nil {
		return fmt.Errorf("Ladybug is not open")
	}
	m.graph.mu.Lock()
	defer m.graph.mu.Unlock()
	if m.graph.conn == nil {
		return fmt.Errorf("Ladybug is not open")
	}
	return nil
}
