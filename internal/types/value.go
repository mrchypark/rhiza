package types

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var sqlBatchMagic = []byte("QBAT\x00")
var kvCommandMagic = []byte("QKVC\x00")
var kvBatchMagic = []byte("QKVB\x00")
var notifyCommandMagic = []byte("QNTF\x00")
var graphBatchMagic = []byte("QGRB\x00")

const ReadBarrierNonceSize = quepaxa.ReadBarrierNonceSize
const MaxRequestIDBytes = 64
const DefaultIdempotencyWindowSlots = 65_536

type MutationKind uint8

const (
	MutationSQL MutationKind = iota + 1
	MutationKV
	MutationNotify
	MutationGraph
)

type MutationStatus string

const (
	MutationCommitted MutationStatus = "committed"
	MutationRejected  MutationStatus = "rejected"
)

// MutationReceipt is the bounded result retained for idempotent retries.
type MutationReceipt struct {
	Slot             uint64         `json:"slot"`
	Status           MutationStatus `json:"status"`
	ErrorCode        string         `json:"error_code,omitempty"`
	RowsAffected     int64          `json:"rows_affected,omitempty"`
	LastInsertID     int64          `json:"last_insert_id,omitempty"`
	Applied          bool           `json:"applied,omitempty"`
	RetryThroughSlot uint64         `json:"retry_through_slot"`
}

// SQLCommand is one submitter command inside a proposer batch.
type SQLCommand struct {
	RequestID string `json:"request_id"`
	SQL       string `json:"sql,omitempty"`
	Args      []any  `json:"args,omitempty"`
	// WantRows requests bounded rows from a replicated mutation.
	WantRows   bool           `json:"want_rows,omitempty"`
	RequireOne bool           `json:"require_one,omitempty"`
	Statements []SQLStatement `json:"statements,omitempty"`
	Migration  *SQLMigration  `json:"migration,omitempty"`
}

// SQLMigration marks an engine-owned, atomically applied migration command.
type SQLMigration struct {
	Version  int64  `json:"version"`
	Name     string `json:"name"`
	Checksum string `json:"checksum"`
}

// SQLStatement is one statement in a replicated client transaction.
type SQLStatement struct {
	SQL        string                  `json:"sql"`
	Args       []any                   `json:"args,omitempty"`
	WantRows   bool                    `json:"want_rows,omitempty"`
	OutputRefs []SQLStatementOutputRef `json:"output_refs,omitempty"`
}

// SQLStatementOutputRef replaces a null positional argument with a column
// from the single row returned by an earlier statement in the transaction.
type SQLStatementOutputRef struct {
	ArgIndex       int    `json:"arg_index"`
	StatementIndex int    `json:"statement_index"`
	ColumnName     string `json:"column_name,omitempty"`
	ColumnIndex    *int   `json:"column_index,omitempty"`
}

// SQLStatementResult is the deterministic result returned by one statement.
type SQLStatementResult struct {
	RowsAffected int64    `json:"rows_affected,omitempty"`
	LastInsertID int64    `json:"last_insert_id,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
}

// SQLCommandResult is retained with the bounded MutationReceipt for retries.
type SQLCommandResult struct {
	Statements []SQLStatementResult `json:"statements"`
	Error      string               `json:"error,omitempty"`
}

// KVCommand is a replicated mutation of the typed key/value store.
type KVCommand struct {
	RequestID        string `json:"request_id"`
	Operation        string `json:"operation"`
	Key              string `json:"key"`
	Value            []byte `json:"value,omitempty"`
	Expected         []byte `json:"expected,omitempty"`
	ExpectedExists   bool   `json:"expected_exists,omitempty"`
	TTLMS            int64  `json:"ttl_ms,omitempty"`
	ExpiresAtUnixMS  int64  `json:"expires_at_unix_ms,omitempty"`
	ObservedAtUnixMS int64  `json:"observed_at_unix_ms,omitempty"`
}

// KVRequestMatches compares client intent, excluding first-admission time.
func KVRequestMatches(stored, request KVCommand) bool {
	ttl := stored.TTLMS
	if ttl == 0 && stored.ExpiresAtUnixMS > stored.ObservedAtUnixMS {
		ttl = stored.ExpiresAtUnixMS - stored.ObservedAtUnixMS
	}
	return stored.RequestID == request.RequestID && stored.Operation == request.Operation &&
		stored.Key == request.Key && bytes.Equal(stored.Value, request.Value) &&
		bytes.Equal(stored.Expected, request.Expected) && stored.ExpectedExists == request.ExpectedExists &&
		ttl == request.TTLMS
}

type KVCommandResult struct {
	Applied bool `json:"applied"`
}

type NotifyCommand struct {
	RequestID string `json:"request_id"`
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
}

// GraphCommand is one idempotent, replicated Cypher mutation.
type GraphCommand struct {
	RequestID    string                     `json:"request_id"`
	Cypher       string                     `json:"cypher,omitempty"`
	Args         map[string]any             `json:"args,omitempty"`
	Events       []GraphStreamEvent         `json:"events,omitempty"`
	StreamOffset *GraphStreamOffsetMutation `json:"stream_offset,omitempty"`
	StreamTrim   *GraphStreamTrimMutation   `json:"stream_trim,omitempty"`
}

type GraphStreamOffsetMutation struct {
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	Sequence uint64 `json:"sequence"`
}

type GraphStreamTrimMutation struct {
	Stream          string `json:"stream"`
	ThroughSequence uint64 `json:"through_sequence"`
}

// GraphStreamEvent is published atomically with its GraphCommand.
type GraphStreamEvent struct {
	Stream  string `json:"stream"`
	Kind    string `json:"kind"`
	Payload any    `json:"payload"`
}

// GraphStreamRecord is one cursor-addressable LatticeDB stream record.
type GraphStreamRecord struct {
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Payload  any    `json:"payload"`
}

type GraphCommandResult struct {
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	Error        string   `json:"error,omitempty"`
	AppliedSlot  uint64   `json:"applied_slot,omitempty"`
	ConsensusTip uint64   `json:"consensus_tip,omitempty"`
}

// GraphReachableRequest describes one bounded outgoing traversal. Matching
// nodes are both returned and expanded; the start node is never returned.
type GraphReachableRequest struct {
	StartLabel     string         `json:"start_label"`
	StartProperty  string         `json:"start_property"`
	StartValue     any            `json:"start_value"`
	EdgeType       string         `json:"edge_type"`
	NodeLabel      string         `json:"node_label,omitempty"`
	NodeFilters    map[string]any `json:"node_filters,omitempty"`
	ResultProperty string         `json:"result_property"`
	MaxDepth       uint32         `json:"max_depth"`
	MaxResults     uint           `json:"max_results"`
	// MaxScannedEdges counts fetched outgoing edges, including duplicate
	// targets and targets rejected by NodeLabel or NodeFilters.
	MaxScannedEdges uint `json:"max_scanned_edges"`
	// MaxBytes bounds the JSON encoding of Nodes.
	MaxBytes    uint   `json:"max_bytes"`
	Consistency string `json:"consistency,omitempty"`
	// RequireAppliedSlot requires an exact local materializer version.
	RequireAppliedSlot *uint64 `json:"require_applied_slot,omitempty"`
}

type GraphReachableNode struct {
	Value    any    `json:"value"`
	Distance uint32 `json:"distance"`
}

type GraphReachableResult struct {
	Nodes        []GraphReachableNode `json:"nodes"`
	StartFound   bool                 `json:"start_found"`
	ScannedEdges uint                 `json:"scanned_edges"`
	AppliedSlot  uint64               `json:"applied_slot"`
	ConsensusTip uint64               `json:"consensus_tip"`
}

func EncodeGraphCommand(command GraphCommand) ([]byte, error) {
	return EncodeGraphBatch([]GraphCommand{command})
}

func EncodeGraphBatch(commands []GraphCommand) ([]byte, error) {
	items := make([][]byte, len(commands))
	for i := range commands {
		var err error
		items[i], err = EncodeGraphBatchItem(commands[i])
		if err != nil {
			return nil, err
		}
	}
	return AssembleGraphBatch(items), nil
}

func EncodeGraphBatchItem(command GraphCommand) ([]byte, error) {
	return json.Marshal(command)
}

func AssembleGraphBatch(items [][]byte) []byte {
	return assembleBatch(graphBatchMagic, items)
}

func assembleBatch(magic []byte, items [][]byte) []byte {
	size := len(magic) + 2
	for _, item := range items {
		size += len(item)
	}
	if len(items) > 1 {
		size += len(items) - 1
	}
	payload := make([]byte, 0, size)
	payload = append(payload, magic...)
	payload = append(payload, '[')
	for i, item := range items {
		if i != 0 {
			payload = append(payload, ',')
		}
		payload = append(payload, item...)
	}
	payload = append(payload, ']')
	return payload
}

func BatchEncodedSize(magicBytes int, items [][]byte) int {
	size := magicBytes + 2
	for _, item := range items {
		size += len(item)
	}
	if len(items) > 1 {
		size += len(items) - 1
	}
	return size
}

func FingerprintBatchItem(domain string, item []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write([]byte{0})
	h.Write(item)
	var fingerprint [32]byte
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

func SQLFingerprint(command SQLCommand) ([32]byte, error) {
	command.RequestID = ""
	item, err := EncodeSQLBatchItem(command)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/sql", item), nil
}

func GraphFingerprint(command GraphCommand) ([32]byte, error) {
	command.RequestID = ""
	item, err := EncodeGraphBatchItem(command)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/graph", item), nil
}

func KVFingerprint(command KVCommand) ([32]byte, error) {
	command.RequestID = ""
	command.ExpiresAtUnixMS = 0
	command.ObservedAtUnixMS = 0
	item, err := json.Marshal(command)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/kv", item), nil
}

func NotifyFingerprint(command NotifyCommand) ([32]byte, error) {
	command.RequestID = ""
	item, err := json.Marshal(command)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/notify", item), nil
}

func EncodeSQLBatchItem(command SQLCommand) ([]byte, error) {
	wire := command
	wire.Args, _ = encodeSQLArgs(command.Args)
	statementsCloned := false
	for i, statement := range command.Statements {
		args, changed := encodeSQLArgs(statement.Args)
		if !changed {
			continue
		}
		if !statementsCloned {
			wire.Statements = append([]SQLStatement(nil), command.Statements...)
			statementsCloned = true
		}
		wire.Statements[i].Args = args
	}
	return json.Marshal(wire)
}

func encodeSQLArgs(args []any) ([]any, bool) {
	var result []any
	for i, arg := range args {
		if value, ok := arg.([]byte); ok {
			if result == nil {
				result = append([]any(nil), args...)
			}
			result[i] = map[string]string{"$rhiza_blob": base64.StdEncoding.EncodeToString(value)}
		}
	}
	if result == nil {
		return args, false
	}
	return result, true
}

func decodeSQLArgs(args []any) error {
	for i, arg := range args {
		object, ok := arg.(map[string]any)
		if !ok {
			continue
		}
		encoded, tagged := object["$rhiza_blob"]
		if !tagged || len(object) != 1 {
			continue
		}
		text, ok := encoded.(string)
		if !ok {
			return fmt.Errorf("invalid SQL blob argument")
		}
		value, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return fmt.Errorf("invalid SQL blob argument: %w", err)
		}
		args[i] = value
	}
	return nil
}

func AssembleSQLBatch(items [][]byte) []byte {
	return assembleBatch(sqlBatchMagic, items)
}

func SQLBatchEncodedSize(items [][]byte) int {
	return BatchEncodedSize(len(sqlBatchMagic), items)
}

func EncodeSQLBatch(commands []SQLCommand) ([]byte, error) {
	items := make([][]byte, len(commands))
	for i := range commands {
		var err error
		items[i], err = EncodeSQLBatchItem(commands[i])
		if err != nil {
			return nil, err
		}
	}
	return AssembleSQLBatch(items), nil
}

func DecodeGraphBatch(value []byte) ([]GraphCommand, bool, error) {
	if !bytes.HasPrefix(value, graphBatchMagic) {
		return nil, false, nil
	}
	var commands []GraphCommand
	decoder := json.NewDecoder(bytes.NewReader(value[len(graphBatchMagic):]))
	decoder.UseNumber()
	if err := decoder.Decode(&commands); err != nil {
		return nil, true, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, true, err
	}
	if len(commands) == 0 {
		return nil, true, fmt.Errorf("empty graph command batch")
	}
	return commands, true, nil
}

func EncodeNotifyCommand(command NotifyCommand) ([]byte, error) {
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), notifyCommandMagic...), payload...), nil
}

func DecodeNotifyCommand(value []byte) (NotifyCommand, bool, error) {
	if !bytes.HasPrefix(value, notifyCommandMagic) {
		return NotifyCommand{}, false, nil
	}
	var command NotifyCommand
	if err := json.Unmarshal(value[len(notifyCommandMagic):], &command); err != nil {
		return NotifyCommand{}, true, err
	}
	return command, true, nil
}

func EncodeKVCommand(command KVCommand) ([]byte, error) {
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), kvCommandMagic...), payload...), nil
}

func EncodeKVBatchItem(command KVCommand) ([]byte, error) {
	return json.Marshal(command)
}

func AssembleKVBatch(items [][]byte) []byte {
	return assembleBatch(kvBatchMagic, items)
}

func DecodeKVBatch(value []byte) ([]KVCommand, bool, error) {
	if !bytes.HasPrefix(value, kvBatchMagic) {
		return nil, false, nil
	}
	var commands []KVCommand
	if err := json.Unmarshal(value[len(kvBatchMagic):], &commands); err != nil {
		return nil, true, err
	}
	if len(commands) == 0 {
		return nil, true, fmt.Errorf("empty KV command batch")
	}
	return commands, true, nil
}

func DecodeKVCommand(value []byte) (KVCommand, bool, error) {
	if !bytes.HasPrefix(value, kvCommandMagic) {
		return KVCommand{}, false, nil
	}
	var command KVCommand
	if err := json.Unmarshal(value[len(kvCommandMagic):], &command); err != nil {
		return KVCommand{}, true, err
	}
	return command, true, nil
}

func DecodeSQLBatch(value []byte) ([]SQLCommand, bool, error) {
	if !bytes.HasPrefix(value, sqlBatchMagic) {
		return nil, false, nil
	}
	var commands []SQLCommand
	decoder := json.NewDecoder(bytes.NewReader(value[len(sqlBatchMagic):]))
	decoder.UseNumber()
	if err := decoder.Decode(&commands); err != nil {
		return nil, true, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, true, err
	}
	if len(commands) == 0 {
		return nil, true, fmt.Errorf("empty SQL command batch")
	}
	for i := range commands {
		if err := decodeSQLArgs(commands[i].Args); err != nil {
			return nil, true, err
		}
		for j := range commands[i].Statements {
			if err := decodeSQLArgs(commands[i].Statements[j].Args); err != nil {
				return nil, true, err
			}
		}
	}
	return commands, true, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func EncodeLeaderSchedule(members []NodeID) ([]byte, error) {
	return quepaxa.EncodeLeaderSchedule(members)
}

func DecodeLeaderSchedule(value []byte) ([]NodeID, bool, error) {
	return quepaxa.DecodeLeaderSchedule(value)
}

func EncodeReadBarrier(nonce [ReadBarrierNonceSize]byte) []byte {
	return quepaxa.EncodeReadBarrier(nonce)
}

func DecodeReadBarrier(value []byte) (bool, error) {
	return quepaxa.DecodeReadBarrier(value)
}

func DecodeCheckpointSeal(value []byte) (quepaxa.CheckpointSeal, bool, error) {
	return quepaxa.DecodeCheckpointSeal(value)
}

// Proposal is a value proposed for a slot.
type Proposal struct {
	// RequestID is the client's idempotency key.
	RequestID string

	// Payload contains the encoded commands (SQL, Graph, KV).
	Payload []byte
}

// Hash returns the SHA-256 hash of the proposal.
func (p *Proposal) Hash() ValueHash {
	h := sha256.New()
	h.Write([]byte(p.RequestID))
	h.Write(p.Payload)
	var out ValueHash
	copy(out[:], h.Sum(nil))
	return out
}

// SlotValue is a decided value bound to a specific slot.
type SlotValue = quepaxa.SlotValue

// DecidedValue is the durable value bound to a decided slot.
type DecidedValue = quepaxa.DecidedValue

// Receipt is a recorder's confirmation of a proposal.
type Receipt = quepaxa.Receipt
