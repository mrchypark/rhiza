// Package rhiza provides the primary in-process Go API. HTTP is an optional adapter.
package rhiza

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type Member = quepaxa.Member
type SQLStatement = types.SQLStatement
type GraphCommand = types.GraphCommand
type GraphResult = types.GraphCommandResult
type GraphStreamEvent = types.GraphStreamEvent
type GraphStreamRecord = types.GraphStreamRecord
type NotifyCommand = types.NotifyCommand
type MutationReceipt = types.MutationReceipt

type ExecuteRequest = network.ExecuteRequest
type ExecuteResponse = network.ExecuteResponse
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
type RequestStatusRequest = network.RequestStatusRequest
type RequestStatusResponse = network.RequestStatusResponse
type ObjectStoreStats = objstore.Stats
type ObjectStoreDurability = types.ObjectStoreDurability

// Config contains the durable local path, fixed membership, and peer endpoint.
type Config struct {
	ClusterID             string
	NodeID                string
	DataDir               string
	BindAddr              string
	PeerAddr              string
	AdminToken            string
	Members               []Member
	ObjStoreEndpoint      string
	ObjStoreBucket        string
	ObjStoreProvider      string
	ObjStoreDir           string
	ObjStorePrefix        string
	ObjStoreRegion        string
	ObjStoreInsecure      bool
	ObjStoreRetries       int
	ObjStoreAccessKey     string
	ObjStoreSecretKey     string
	ObjStoreSessionToken  string
	ObjStoreDurability    ObjectStoreDurability
	ObjStoreSyncInterval  time.Duration
	ObjStoreBatchDelay    time.Duration
	ObjStoreGCInterval    time.Duration
	ObjStoreGCGracePeriod time.Duration
	CheckpointInterval    time.Duration
	CheckpointTailBytes   int64
	MaxWALBytes           int64
	// HedgeDelay delays each lower-priority proposer. Nil uses
	// DefaultHedgeDelay; a pointer to zero explicitly enables eager hedging.
	HedgeDelay *time.Duration
}

const (
	ConsistencyLocal               = "local"
	ConsistencyLinearizable        = "linearizable"
	ObjectStoreDurabilityAsync     = types.ObjectStoreDurabilityAsync
	ObjectStoreDurabilityBeforeAck = types.ObjectStoreDurabilityBeforeAck
	DefaultHedgeDelay              = 5 * time.Millisecond
)

var (
	ErrNotReady              = network.ErrNotReady
	ErrRequestConflict       = network.ErrRequestConflict
	ErrInvalidRequest        = network.ErrInvalidRequest
	ErrQuorumUnavailable     = quepaxa.ErrQuorumUnavailable
	ErrDurabilityUnavailable = network.ErrDurabilityUnavailable
	ErrCommitUnknown         = network.ErrCommitUnknown
)

// DB owns one embedded Rhiza node and its private QUIC peer endpoint.
type DB struct {
	node      *node.Node
	api       *network.Server
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
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
	hedgeDelay, err := configuredHedgeDelay(config.HedgeDelay)
	if err != nil {
		return nil, err
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
		ObjStoreDurability: config.ObjStoreDurability, ObjStoreSyncInterval: config.ObjStoreSyncInterval,
		ObjStoreBatchDelay: config.ObjStoreBatchDelay,
		ObjStoreGCInterval: config.ObjStoreGCInterval, ObjStoreGCGracePeriod: config.ObjStoreGCGracePeriod,
		CheckpointInterval: config.CheckpointInterval, CheckpointTailBytes: config.CheckpointTailBytes, MaxWALBytes: config.MaxWALBytes, HedgeDelay: hedgeDelay,
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

func configuredHedgeDelay(delay *time.Duration) (time.Duration, error) {
	if delay == nil {
		return DefaultHedgeDelay, nil
	}
	if *delay < 0 {
		return 0, fmt.Errorf("hedge delay must not be negative")
	}
	return *delay, nil
}

func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closeErr = db.node.Shutdown()
		db.cancel()
	})
	return db.closeErr
}

// Handler exposes the optional HTTP server API without opening a listener.
func (db *DB) Handler() http.Handler                            { return db.api }
func (db *DB) ServeHTTP(w http.ResponseWriter, r *http.Request) { db.api.ServeHTTP(w, r) }

func (db *DB) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	return db.api.Execute(ctx, req)
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
