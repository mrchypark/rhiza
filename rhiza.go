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
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type Member = quepaxa.Member
type Profile = types.Profile
type SQLStatement = types.SQLStatement
type GraphCommand = types.GraphCommand
type GraphResult = types.GraphCommandResult
type NotifyCommand = types.NotifyCommand

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
type ObjectStoreStats = objstore.Stats
type ObjectStoreDurability = types.ObjectStoreDurability

// Config contains the durable local path, fixed membership, and peer endpoint.
type Config struct {
	ClusterID             string
	NodeID                string
	Profile               Profile
	DataDir               string
	BindAddr              string
	PeerAddr              string
	AdminToken            string
	ClientToken           string
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
	ObjStoreGCInterval    time.Duration
	ObjStoreGCGracePeriod time.Duration
	CheckpointInterval    time.Duration
	ReadTimeout           time.Duration
	HedgeDelay            time.Duration
}

const (
	ProfileSQL                     = types.ProfileSQL
	ProfileGraph                   = types.ProfileGraph
	ProfileKV                      = types.ProfileKV
	ConsistencyLocal               = "local"
	ConsistencyLinearizable        = "linearizable"
	ObjectStoreDurabilityAsync     = types.ObjectStoreDurabilityAsync
	ObjectStoreDurabilityBeforeAck = types.ObjectStoreDurabilityBeforeAck
)

var (
	ErrNotReady              = network.ErrNotReady
	ErrRequestConflict       = network.ErrRequestConflict
	ErrInvalidRequest        = network.ErrInvalidRequest
	ErrQuorumUnavailable     = quepaxa.ErrQuorumUnavailable
	ErrDurabilityUnavailable = network.ErrDurabilityUnavailable
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
	if config.Profile == "" {
		config.Profile = materializer.BuildProfile()
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
		ClusterID: types.ClusterID(config.ClusterID), NodeID: types.NodeID(config.NodeID), Profile: config.Profile,
		DataDir: config.DataDir, BindAddr: config.BindAddr, PeerAddr: config.PeerAddr,
		AdminToken: config.AdminToken, ClientToken: config.ClientToken, Members: config.Members,
		ObjStoreEndpoint: config.ObjStoreEndpoint, ObjStoreBucket: config.ObjStoreBucket,
		ObjStoreProvider: config.ObjStoreProvider, ObjStoreDir: config.ObjStoreDir, ObjStorePrefix: config.ObjStorePrefix,
		ObjStoreRegion: config.ObjStoreRegion, ObjStoreInsecure: config.ObjStoreInsecure, ObjStoreRetries: config.ObjStoreRetries,
		ObjStoreAccessKey: config.ObjStoreAccessKey, ObjStoreSecretKey: config.ObjStoreSecretKey, ObjStoreSessionToken: config.ObjStoreSessionToken,
		ObjStoreDurability: config.ObjStoreDurability, ObjStoreSyncInterval: config.ObjStoreSyncInterval,
		ObjStoreGCInterval: config.ObjStoreGCInterval, ObjStoreGCGracePeriod: config.ObjStoreGCGracePeriod,
		CheckpointInterval: config.CheckpointInterval, ReadTimeout: config.ReadTimeout, HedgeDelay: config.HedgeDelay,
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
	db.closeOnce.Do(func() { db.cancel(); db.closeErr = db.node.Shutdown() })
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
func (db *DB) NotifyPublish(ctx context.Context, req NotifyCommand) (uint64, error) {
	return db.api.NotifyPublish(ctx, req)
}
func (db *DB) NotifySubscribe(topic string) (<-chan []byte, func(), error) {
	return db.api.NotifySubscribe(topic)
}

func (db *DB) ObjectStoreStats() (ObjectStoreStats, bool) {
	return db.node.ObjectStoreStats()
}
