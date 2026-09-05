// Package rhiza provides the primary in-process Go API. HTTP is an optional adapter.
package rhiza

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type Member = quepaxa.Member
type NodeID = quepaxa.NodeID
type ReplicaMember = network.PeerIdentity

// NewReplicaMember removes a voter's secret while retaining its pinned peer identity.
func NewReplicaMember(clusterID string, member Member) (ReplicaMember, error) {
	return network.NewPeerIdentity(types.ClusterID(clusterID), member)
}

type SQLStatement = types.SQLStatement
type SQLStatementOutputRef = types.SQLStatementOutputRef
type SQLStatementResult = types.SQLStatementResult
type GraphCommand = types.GraphCommand
type GraphResult = types.GraphCommandResult
type GraphStreamEvent = types.GraphStreamEvent
type GraphStreamRecord = types.GraphStreamRecord
type NotifyCommand = types.NotifyCommand
type MutationReceipt = types.MutationReceipt
type MutationStatus = types.MutationStatus

type ExecuteRequest = network.ExecuteRequest
type ExecuteResponse = network.ExecuteResponse
type HTTPErrorResponse = network.ErrorResponse
type QueryRequest = network.QueryRequest
type QueryResponse = network.QueryResponse
type KVGetRequest = network.KVGetRequest
type KVGetResponse = network.KVGetResponse
type KVMutationRequest = network.KVMutationRequest
type KVMutationResponse = network.KVMutationResponse
type GraphQueryRequest = network.GraphQueryRequest
type GraphExecuteResponse = network.GraphExecuteResponse
type GraphStreamReadRequest = network.GraphStreamReadRequest
type GraphStreamReadResponse = network.GraphStreamReadResponse
type GraphStreamOffsetRequest = network.GraphStreamOffsetRequest
type GraphStreamOffsetResponse = network.GraphStreamOffsetResponse
type GraphStreamTrimRequest = network.GraphStreamTrimRequest
type GraphReachableRequest = types.GraphReachableRequest
type GraphReachableNode = types.GraphReachableNode
type GraphReachableResult = types.GraphReachableResult
type GraphNodePropertyIndex = types.GraphNodePropertyIndex
type RequestStatusRequest = network.RequestStatusRequest
type RequestStatusResponse = network.RequestStatusResponse
type ObjectStoreStats = objstore.Stats
type ObjectStoreDurability = types.ObjectStoreDurability

// Config contains the durable local path, fixed membership, and peer endpoint.
type Config struct {
	ClusterID                      string
	NodeID                         string
	DataDir                        string
	BindAddr                       string
	PeerAddr                       string
	AdminToken                     string
	Members                        []Member
	ObjStoreEndpoint               string
	ObjStoreBucket                 string
	ObjStoreProvider               string
	ObjStoreDir                    string
	ObjStorePrefix                 string
	ObjStoreRegion                 string
	ObjStoreInsecure               bool
	ObjStoreRetries                int
	ObjStoreAccessKey              string
	ObjStoreSecretKey              string
	ObjStoreSessionToken           string
	ObjStoreServiceAccount         string
	ObjStoreAzureTenantID          string
	ObjStoreAzureClientID          string
	ObjStoreAzureClientSecret      string
	ObjStoreAzureStorageAccount    string
	ObjStoreAzureStorageAccountKey string
	ObjStoreAzureConnectionString  string
	ObjStoreAzureUserAssignedID    string
	ObjStoreDurability             ObjectStoreDurability
	ObjStoreSyncInterval           time.Duration
	ObjStoreBatchDelay             time.Duration
	ObjStoreGCInterval             time.Duration
	ObjStoreGCGracePeriod          time.Duration
	CheckpointInterval             time.Duration
	CheckpointTailBytes            int64
	MaxWALBytes                    int64
	// Both zero use 64 concurrent reads / 8 long-poll reads. With an explicit
	// total, zero MaxLongPollReads disables waiting stream reads.
	MaxConcurrentReads int
	MaxLongPollReads   int
	// LocalGraphNodePropertyIndexes are node-local derived indexes. Rhiza
	// reconciles them at open and after checkpoint restore; they are not replicated.
	LocalGraphNodePropertyIndexes []GraphNodePropertyIndex
}

const (
	ConsistencyLocal                    = "local"
	ConsistencyLinearizable             = "linearizable"
	ObjectStoreDurabilityAsync          = types.ObjectStoreDurabilityAsync
	ObjectStoreDurabilityBeforeAck      = types.ObjectStoreDurabilityBeforeAck
	MutationCommitted                   = types.MutationCommitted
	MutationRejected                    = types.MutationRejected
	MutationErrorCodeExecutionFailed    = types.MutationErrorCodeExecutionFailed
	MutationErrorCodePreconditionFailed = types.MutationErrorCodePreconditionFailed
	RequestKindSQL                      = "sql"
	RequestKindKV                       = "kv"
	RequestKindNotify                   = "notify"
	RequestKindGraph                    = "graph"
	RequestStateCommitted               = "committed"
	RequestStateRejected                = "rejected"
	RequestStateUnknownOrExpired        = "unknown_or_expired"
	ObjectStoreProviderFilesystem       = "filesystem"
	ObjectStoreProviderS3               = "s3"
	ObjectStoreProviderGCS              = "gcs"
	ObjectStoreProviderAzure            = "azure"
	HTTPErrorCodeInvalidRequest         = "invalid_request"
	HTTPErrorCodeRequestConflict        = "request_conflict"
	HTTPErrorCodeNotReady               = "not_ready"
	HTTPErrorCodeOverloaded             = "overloaded"
	HTTPErrorCodeDurabilityUnavailable  = "durability_unavailable"
	HTTPErrorCodeCommitUnknown          = "commit_unknown"
	HTTPErrorCodeQuorumUnavailable      = "quorum_unavailable"
	HTTPErrorCodeMethodNotAllowed       = "method_not_allowed"
	HTTPErrorCodeNotFound               = "not_found"
	// MaxReplicatedMutationBytes is the encoded consensus-value limit.
	MaxReplicatedMutationBytes = quepaxa.MaxReplicatedValueBytes
	// MaxHTTPBodyBytes is the optional HTTP adapter's larger JSON envelope limit.
	MaxHTTPBodyBytes              = network.MaxRequestBodyBytes
	MaxGraphReachableDepth        = materializer.MaxGraphReachableDepth
	MaxGraphReachableResults      = materializer.MaxReturningRows
	MaxGraphReachableScannedEdges = materializer.MaxGraphReachableEdges
	MaxGraphReachableBytes        = materializer.MaxResultBytes
	MaxLocalGraphPropertyIndexes  = materializer.MaxGraphPropertyIndexes
)

var (
	ErrNotReady              = network.ErrNotReady
	ErrRequestConflict       = network.ErrRequestConflict
	ErrInvalidRequest        = network.ErrInvalidRequest
	ErrQuorumUnavailable     = quepaxa.ErrQuorumUnavailable
	ErrDurabilityUnavailable = network.ErrDurabilityUnavailable
	ErrCommitUnknown         = network.ErrCommitUnknown
	ErrGraphResourceLimit    = network.ErrGraphResourceLimit
	ErrReadVersionMismatch   = network.ErrReadVersionMismatch
)

// DB owns one embedded Rhiza node and its private QUIC peer endpoint.
type DB struct {
	node      *node.Node
	api       *network.Server
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

// Migration is one ordered, repeatable schema change.
type Migration struct {
	Version    int64          `json:"version"`
	Name       string         `json:"name"`
	Statements []SQLStatement `json:"statements"`
}

// Open starts the embedded engine. It does not start a public HTTP listener.
func Open(ctx context.Context, config Config) (*DB, error) {
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if config.BindAddr == "" {
		config.BindAddr = "127.0.0.1:0"
	}
	if config.PeerAddr == "" {
		config.PeerAddr = "127.0.0.1:0"
	}
	if config.ClusterID == "" {
		config.ClusterID = "cluster-a"
	}
	childCtx, cancel := context.WithCancel(ctx)
	internalConfig := &types.ExecutionConfig{
		ClusterID: types.ClusterID(config.ClusterID), NodeID: types.NodeID(config.NodeID),
		DataDir: config.DataDir, BindAddr: config.BindAddr, PeerAddr: config.PeerAddr,
		AdminToken: config.AdminToken, Members: config.Members,
		ObjStoreEndpoint: config.ObjStoreEndpoint, ObjStoreBucket: config.ObjStoreBucket,
		ObjStoreProvider: config.ObjStoreProvider, ObjStoreDir: config.ObjStoreDir, ObjStorePrefix: config.ObjStorePrefix,
		ObjStoreRegion: config.ObjStoreRegion, ObjStoreInsecure: config.ObjStoreInsecure, ObjStoreRetries: config.ObjStoreRetries,
		ObjStoreAccessKey: config.ObjStoreAccessKey, ObjStoreSecretKey: config.ObjStoreSecretKey, ObjStoreSessionToken: config.ObjStoreSessionToken,
		ObjStoreServiceAccount: config.ObjStoreServiceAccount, ObjStoreAzureTenantID: config.ObjStoreAzureTenantID,
		ObjStoreAzureClientID: config.ObjStoreAzureClientID, ObjStoreAzureClientSecret: config.ObjStoreAzureClientSecret,
		ObjStoreAzureStorageAccount: config.ObjStoreAzureStorageAccount, ObjStoreAzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
		ObjStoreAzureConnectionString: config.ObjStoreAzureConnectionString, ObjStoreAzureUserAssignedID: config.ObjStoreAzureUserAssignedID,
		ObjStoreDurability: config.ObjStoreDurability, ObjStoreSyncInterval: config.ObjStoreSyncInterval,
		ObjStoreBatchDelay: config.ObjStoreBatchDelay,
		ObjStoreGCInterval: config.ObjStoreGCInterval, ObjStoreGCGracePeriod: config.ObjStoreGCGracePeriod,
		CheckpointInterval: config.CheckpointInterval, CheckpointTailBytes: config.CheckpointTailBytes, MaxWALBytes: config.MaxWALBytes,
		MaxConcurrentReads: config.MaxConcurrentReads, MaxLongPollReads: config.MaxLongPollReads,
		LocalGraphNodePropertyIndexes: slices.Clone(config.LocalGraphNodePropertyIndexes),
	}
	n := node.New(internalConfig)
	if err := n.Open(childCtx); err != nil {
		cancel()
		return nil, err
	}
	api, err := n.API()
	if err != nil {
		cancel()
		_ = n.Shutdown()
		return nil, err
	}
	return &DB{node: n, api: api, cancel: cancel}, nil
}

func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closeErr = db.node.Shutdown()
		db.cancel()
	})
	return db.closeErr
}

// Ready reports whether local recovery and catch-up completed. It is not a
// live quorum probe: an isolated peer may remain locally ready. Mutations and
// linearizable queries still fail closed when quorum is unavailable.
func (db *DB) Ready() bool { return db.node.Ready() }

// ValidateExecuteRequest applies the replicated SQL contract and encoded-size
// limit without submitting the mutation.
func ValidateExecuteRequest(req ExecuteRequest) error { return network.ValidateExecuteRequest(req) }

// ValidateGraphReachableRequest checks limits and value types without reading data.
func ValidateGraphReachableRequest(req GraphReachableRequest) error {
	if err := materializer.ValidateGraphReachableRequest(req); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

// Handler exposes the optional HTTP server API without opening a listener.
func (db *DB) Handler() http.Handler                            { return db.api }
func (db *DB) ServeHTTP(w http.ResponseWriter, r *http.Request) { db.api.ServeHTTP(w, r) }

func (db *DB) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	return db.api.Execute(ctx, req)
}

// ExecuteReturning executes one replicated mutation and returns its bounded rows.
func (db *DB) ExecuteReturning(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	return db.api.ExecuteReturning(ctx, req)
}

// ExecuteReturningOne commits only when exactly one row is returned.
func (db *DB) ExecuteReturningOne(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	return db.api.ExecuteReturningOne(ctx, req)
}

// SQLRow is one immutable row passed to a typed mapping callback.
type SQLRow struct {
	columns []string
	values  []any
}

// Len returns the number of columns in the row.
func (r SQLRow) Len() int { return len(r.values) }

// Columns returns a copy of the column names in result order.
func (r SQLRow) Columns() []string { return slices.Clone(r.columns) }

// Values returns a copy of the values in result order.
func (r SQLRow) Values() []any {
	values := slices.Clone(r.values)
	for i := range values {
		values[i] = cloneSQLRowValue(values[i])
	}
	return values
}

// Value returns one column by index.
func (r SQLRow) Value(index int) (any, error) {
	if index < 0 || index >= len(r.values) {
		return nil, fmt.Errorf("SQL row column index %d is out of bounds", index)
	}
	return cloneSQLRowValue(r.values[index]), nil
}

// Named returns a uniquely named column.
func (r SQLRow) Named(name string) (any, error) {
	index := -1
	for i, column := range r.columns {
		if column == name {
			if index >= 0 {
				return nil, fmt.Errorf("SQL row column %q is not unique", name)
			}
			index = i
		}
	}
	if index < 0 || index >= len(r.values) {
		return nil, fmt.Errorf("SQL row column %q does not exist", name)
	}
	return cloneSQLRowValue(r.values[index]), nil
}

func cloneSQLRowValue(value any) any {
	if bytes, ok := value.([]byte); ok {
		return slices.Clone(bytes)
	}
	return value
}

// ExecuteReturningMap maps replicated RETURNING rows to an application type.
func ExecuteReturningMap[T any](ctx context.Context, db *DB, req ExecuteRequest, mapper func(SQLRow) (T, error)) (ExecuteResponse, []T, error) {
	if db == nil || mapper == nil {
		return ExecuteResponse{}, nil, fmt.Errorf("%w: database and row mapper are required", ErrInvalidRequest)
	}
	response, err := db.ExecuteReturning(ctx, req)
	if err != nil {
		return response, nil, err
	}
	if response.Status != types.MutationCommitted {
		return response, nil, fmt.Errorf("SQL mutation was rejected: %s", response.ErrorCode)
	}
	if len(response.Statements) != 1 {
		return response, nil, fmt.Errorf("SQL RETURNING result is unavailable")
	}
	statement := response.Statements[0]
	result := make([]T, len(statement.Rows))
	for i, values := range statement.Rows {
		result[i], err = mapper(SQLRow{columns: statement.Columns, values: values})
		if err != nil {
			return response, nil, fmt.Errorf("map returned row %d: %w", i, err)
		}
	}
	return response, result, nil
}

// ExecuteReturningMapOne maps one row and rolls back unless exactly one exists.
func ExecuteReturningMapOne[T any](ctx context.Context, db *DB, req ExecuteRequest, mapper func(SQLRow) (T, error)) (ExecuteResponse, T, error) {
	var zero T
	if db == nil || mapper == nil {
		return ExecuteResponse{}, zero, fmt.Errorf("%w: database and row mapper are required", ErrInvalidRequest)
	}
	response, err := db.ExecuteReturningOne(ctx, req)
	if err != nil {
		return response, zero, err
	}
	if response.Status != types.MutationCommitted {
		return response, zero, fmt.Errorf("SQL mutation was rejected: %s", response.ErrorCode)
	}
	if len(response.Statements) != 1 || len(response.Statements[0].Rows) != 1 {
		return response, zero, fmt.Errorf("SQL RETURNING result is unavailable")
	}
	statement := response.Statements[0]
	result, err := mapper(SQLRow{columns: statement.Columns, values: statement.Rows[0]})
	if err != nil {
		return response, zero, fmt.Errorf("map returned row: %w", err)
	}
	return response, result, nil
}

// Migrate applies strictly ordered migrations exactly once. A repeated version
// must have the same name and statements.
func (db *DB) Migrate(ctx context.Context, migrations []Migration) error {
	if len(migrations) == 0 {
		return nil
	}
	prepared := make([]struct {
		migration Migration
		request   network.MigrationRequest
	}, len(migrations))
	var previous int64
	for i, migration := range migrations {
		if migration.Version <= 0 || migration.Version != previous+1 || migration.Name == "" || len(migration.Statements) == 0 {
			return fmt.Errorf("%w: migrations require a name, statements, and contiguous versions starting at 1", ErrInvalidRequest)
		}
		previous = migration.Version
		for _, statement := range migration.Statements {
			if len(statement.Args) != 0 || statement.WantRows || len(statement.OutputRefs) != 0 {
				return fmt.Errorf("%w: migration statements do not support arguments, returned rows, or output references", ErrInvalidRequest)
			}
		}
		digest := migrationChecksum(migration)
		checksum := hex.EncodeToString(digest[:])
		requestID := checksum
		if err := ValidateExecuteRequest(ExecuteRequest{RequestID: requestID, Statements: migration.Statements}); err != nil {
			return fmt.Errorf("migration %d: %w", migration.Version, err)
		}
		request := network.MigrationRequest{RequestID: requestID, Version: migration.Version, Name: migration.Name, Checksum: checksum, Statements: migration.Statements}
		prepared[i] = struct {
			migration Migration
			request   network.MigrationRequest
		}{migration, request}
	}
	for _, item := range prepared {
		result, err := db.api.Migrate(ctx, item.request)
		if err != nil {
			return err
		}
		if result.Status != types.MutationCommitted {
			return fmt.Errorf("migration %d failed: %s", item.migration.Version, result.ErrorCode)
		}
	}
	return nil
}

func migrationChecksum(migration Migration) [32]byte {
	var canonical bytes.Buffer
	canonical.WriteString("rhiza/migration\x00")
	_ = binary.Write(&canonical, binary.BigEndian, migration.Version)
	writeMigrationField := func(value string) {
		_ = binary.Write(&canonical, binary.BigEndian, uint64(len(value)))
		canonical.WriteString(value)
	}
	writeMigrationField(migration.Name)
	_ = binary.Write(&canonical, binary.BigEndian, uint64(len(migration.Statements)))
	for _, statement := range migration.Statements {
		writeMigrationField(statement.SQL)
	}
	return sha256.Sum256(canonical.Bytes())
}
func (db *DB) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	return db.api.Query(ctx, req)
}
func (db *DB) KVGet(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	return db.api.KVGet(ctx, req)
}
func (db *DB) KVPut(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return db.api.KVPut(ctx, req)
}
func (db *DB) KVDelete(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return db.api.KVDelete(ctx, req)
}
func (db *DB) KVCAS(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return db.api.KVCAS(ctx, req)
}
func (db *DB) GraphExecute(ctx context.Context, req GraphCommand) (GraphExecuteResponse, error) {
	return db.api.GraphExecute(ctx, req)
}
func (db *DB) GraphQuery(ctx context.Context, req GraphQueryRequest) (GraphResult, error) {
	return db.api.GraphQuery(ctx, req)
}

// GraphReachable performs a bounded, deterministic outgoing traversal on one
// immutable local graph snapshot. Results are ordered by distance, then node ID.
func (db *DB) GraphReachable(ctx context.Context, req GraphReachableRequest) (GraphReachableResult, error) {
	return db.api.GraphReachable(ctx, req)
}

// GraphChanges reads the node-local LatticeDB semantic graph changefeed.
func (db *DB) GraphChanges(ctx context.Context, req GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	return db.api.GraphChanges(ctx, req)
}

// GraphStreamRead reads a replicated named stream after its per-stream cursor.
func (db *DB) GraphStreamRead(ctx context.Context, req GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	return db.api.GraphStreamRead(ctx, req)
}

// GraphStreamOffset returns a replicated durable consumer offset.
func (db *DB) GraphStreamOffset(ctx context.Context, req GraphStreamOffsetRequest) (GraphStreamOffsetResponse, error) {
	return db.api.GraphStreamOffset(ctx, req)
}

// SetGraphStreamOffset stores a replicated durable consumer offset.
func (db *DB) SetGraphStreamOffset(ctx context.Context, req GraphStreamOffsetRequest) error {
	return db.api.SetGraphStreamOffset(ctx, req)
}

// TrimGraphStream replicates deletion of records through the supplied sequence.
func (db *DB) TrimGraphStream(ctx context.Context, req GraphStreamTrimRequest) error {
	return db.api.TrimGraphStream(ctx, req)
}
func (db *DB) NotifyPublish(ctx context.Context, req NotifyCommand) (MutationReceipt, error) {
	return db.api.NotifyPublish(ctx, req)
}
func (db *DB) NotifySubscribe(topic string) (<-chan []byte, func(), error) {
	return db.api.NotifySubscribe(topic)
}
func (db *DB) NotificationDrops() uint64 {
	return db.api.NotificationDrops()
}
func (db *DB) RequestStatus(ctx context.Context, req RequestStatusRequest) (RequestStatusResponse, error) {
	return db.api.RequestStatus(ctx, req)
}

func (db *DB) ObjectStoreStats() (ObjectStoreStats, bool) {
	return db.node.ObjectStoreStats()
}
