package types

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var sqlBatchMagic = []byte("QBAT1\x00")
var kvCommandMagic = []byte("QKVC1\x00")
var notifyCommandMagic = []byte("QNTF1\x00")
var graphCommandMagic = []byte("QGRF1\x00")

const ReadBarrierNonceSize = quepaxa.ReadBarrierNonceSize
const MaxRequestIDBytes = 256

// SQLCommand is one submitter command inside a proposer batch.
type SQLCommand struct {
	RequestID  string         `json:"request_id"`
	SQL        string         `json:"sql,omitempty"`
	Args       []any          `json:"args,omitempty"`
	WantRows   bool           `json:"want_rows,omitempty"`
	Statements []SQLStatement `json:"statements,omitempty"`
}

// SQLStatement is one statement in a replicated client transaction.
type SQLStatement struct {
	SQL      string `json:"sql"`
	Args     []any  `json:"args,omitempty"`
	WantRows bool   `json:"want_rows,omitempty"`
}

// SQLStatementResult is the deterministic result returned by one statement.
type SQLStatementResult struct {
	RowsAffected int64    `json:"rows_affected,omitempty"`
	LastInsertID int64    `json:"last_insert_id,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
}

// SQLCommandResult is persisted with the request ID for idempotent retries.
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
	RequestID string         `json:"request_id"`
	Cypher    string         `json:"cypher"`
	Args      map[string]any `json:"args,omitempty"`
}

type GraphCommandResult struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
	Error   string   `json:"error,omitempty"`
}

func EncodeGraphCommand(command GraphCommand) ([]byte, error) {
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), graphCommandMagic...), payload...), nil
}

func DecodeGraphCommand(value []byte) (GraphCommand, bool, error) {
	if !bytes.HasPrefix(value, graphCommandMagic) {
		return GraphCommand{}, false, nil
	}
	var command GraphCommand
	decoder := json.NewDecoder(bytes.NewReader(value[len(graphCommandMagic):]))
	decoder.UseNumber()
	if err := decoder.Decode(&command); err != nil {
		return GraphCommand{}, true, err
	}
	return command, true, nil
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

func EncodeSQLBatch(commands []SQLCommand) ([]byte, error) {
	payload, err := json.Marshal(commands)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), sqlBatchMagic...), payload...), nil
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
	if len(commands) == 0 {
		return nil, true, fmt.Errorf("empty SQL command batch")
	}
	return commands, true, nil
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

	// Profile indicates the runtime profile: "sql", "graph", or "kv".
	Profile string
}

// Hash returns the SHA-256 hash of the proposal.
func (p *Proposal) Hash() ValueHash {
	h := sha256.New()
	h.Write([]byte(p.RequestID))
	h.Write(p.Payload)
	h.Write([]byte(p.Profile))
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
