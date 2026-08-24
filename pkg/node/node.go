package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Node is the main runtime container.
type Node struct {
	config       *types.ExecutionConfig
	core         *quepaxa.Core
	material     *materializer.Materializer
	server       *network.Server
	peer         *network.PeerServer
	transport    *network.Transport
	wal          *qlog.WAL
	lock         *qlog.LockFile
	bucket       *objectstore.MeteredBucket
	qlogSync     *qlog.ObjStoreSyncer
	checkpoints  *checkpoint.Manager
	checkpointer *checkpoint.AutoCheckpointer
	ready        atomic.Bool
	opened       atomic.Bool
}

// New creates a new Node.
func New(config *types.ExecutionConfig) *Node {
	return &Node{
		config: config,
	}
}

// Open starts the embedded engine and its private peer transport without serving HTTP.
func (n *Node) Open(ctx context.Context) (err error) {
	if n.config == nil || n.config.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if n.config.Profile != materializer.BuildProfile() {
		return fmt.Errorf("execution profile %q does not match %s build", n.config.Profile, materializer.BuildProfile())
	}
	if !n.opened.CompareAndSwap(false, true) {
		return fmt.Errorf("node is already open")
	}
	defer func() {
		if err != nil {
			_ = n.Shutdown()
		}
	}()

	// 1. Acquire lock file
	cleanStart, lock, err := qlog.Acquire(n.config.DataDir + "/qlog")
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	n.lock = lock

	// 2. Open WAL
	wal, err := qlog.Open(n.config.DataDir + "/qlog")
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	n.wal = wal
	if n.config.ObjStoreProvider != "" || n.config.ObjStoreEndpoint != "" || n.config.ObjStoreBucket != "" {
		provider := objectstore.Provider(n.config.ObjStoreProvider)
		if provider == "" {
			provider = objectstore.ProviderS3
		}
		bucket, bucketErr := objectstore.NewBucket(objectstore.Config{
			Provider: provider, FilesystemDir: n.config.ObjStoreDir, Prefix: n.config.ObjStorePrefix,
			Endpoint: n.config.ObjStoreEndpoint, Bucket: n.config.ObjStoreBucket, Region: n.config.ObjStoreRegion,
			Insecure: n.config.ObjStoreInsecure, MaxRetries: n.config.ObjStoreRetries,
			AccessKey: n.config.ObjStoreAccessKey, SecretKey: n.config.ObjStoreSecretKey, SessionToken: n.config.ObjStoreSessionToken,
		})
		if bucketErr != nil {
			return fmt.Errorf("open object store: %w", bucketErr)
		}
		n.bucket = bucket
		prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), string(n.config.NodeID))
		manifest := qlog.NewManifest()
		n.qlogSync = qlog.NewObjStoreSyncer(wal, manifest, bucket, prefix, 0)
		if _, recoveryErr := qlog.NewRecovery(wal, manifest, bucket, prefix).Recover(ctx); recoveryErr != nil {
			return recoveryErr
		}
		n.checkpoints = checkpoint.NewManager(bucket, prefix, n.config.DataDir)
		if loadErr := n.checkpoints.Load(ctx); loadErr != nil {
			return fmt.Errorf("load checkpoint manifest: %w", loadErr)
		}
	}

	// 3. Recovery is completed below by Core.Recover plus deterministic replay.
	if !cleanStart {
		log.Println("non-clean start detected, recovering from WAL...")
	}

	// 4. Open materializer
	material, err := materializer.Open(
		n.config.DataDir+"/sqlite.db",
		4, // reader count
	)
	if err != nil {
		// ponytail: qlog is currently never compacted, so rebuilding SQLite from it
		// is safer and smaller than database-specific corruption repair.
		if rebuildErr := quarantineSQLite(n.config.DataDir + "/sqlite.db"); rebuildErr != nil {
			return fmt.Errorf("open materializer: %w; quarantine: %v", err, rebuildErr)
		}
		if rebuildErr := quarantineGraph(n.config.DataDir + "/ladybug"); rebuildErr != nil {
			return fmt.Errorf("open materializer: %w; quarantine graph: %v", err, rebuildErr)
		}
		material, err = materializer.Open(n.config.DataDir+"/sqlite.db", 4)
		if err != nil {
			return fmt.Errorf("rebuild materializer: %w", err)
		}
	}
	n.material = material

	// 5. Create consensus core through the same public API available to external users.
	cluster := n.loadClusterConfig()
	if len(cluster.Members) > 1 {
		for _, member := range cluster.Members {
			if member.Token == "" && n.config.AdminToken == "" {
				return fmt.Errorf("peer token is required for multi-node cluster member %s", member.ID)
			}
		}
	}
	transport := network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.AdminToken)
	n.transport = transport
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport,
	})
	if err != nil {
		return fmt.Errorf("create QuePaxa core: %w", err)
	}
	n.core = core
	if n.checkpoints != nil {
		if latest := n.checkpoints.Latest(); latest != nil && latest.Index > material.Tip() {
			if quepaxa.Slot(latest.Index) > core.Tip() {
				return fmt.Errorf("checkpoint slot %d is ahead of certified log tip %d", latest.Index, core.Tip())
			}
			data, readErr := n.checkpoints.Read(ctx, latest.Index)
			if readErr != nil {
				return readErr
			}
			if restoreErr := material.Restore(ctx, data); restoreErr != nil {
				return fmt.Errorf("restore checkpoint %d: %w", latest.Index, restoreErr)
			}
		}
	}

	// 6. Replay the materializer after New has recovered consensus state.
	if quepaxa.Slot(material.Tip()) > core.Tip() {
		return fmt.Errorf("materialized slot %d is ahead of certified log tip %d", material.Tip(), core.Tip())
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return fmt.Errorf("replay local decisions: %w", err)
	}
	// 7. Create HTTP server
	server := network.NewServer(core, material, n.config.ClusterID, true, transport, cluster.Members, n.config.HedgeDelay, n.ready.Load)
	n.server = server
	peer, err := network.StartPeerServer(ctx, n.peerAddr(), server, cluster.Members, n.config.AdminToken)
	if err != nil {
		return fmt.Errorf("listen peer QUIC: %w", err)
	}
	n.peer = peer
	if len(cluster.Members) == 1 {
		n.ready.Store(true)
	} else {
		go n.startCatchUp(ctx, transport, cluster)
	}

	// 8. Start periodic WAL sync
	core.StartPeriodicSync(ctx, 1*time.Second)
	if n.checkpoints != nil {
		interval := n.config.CheckpointInterval
		if interval <= 0 {
			interval = time.Minute
		}
		n.checkpointer = checkpoint.NewAutoCheckpointer(n.checkpoints, material, 1, interval)
		n.checkpointer.Start(ctx, func() uint64 { return uint64(core.Tip()) }, n.qlogSync.Sync)
	}
	return nil
}

// Start opens the embedded engine and serves the optional public HTTP adapter.
func (n *Node) Start(ctx context.Context) error {
	if err := n.Open(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", n.config.BindAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	httpServer := &http.Server{
		Handler:      n.server,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("node starting HTTP/TCP on %s and peer QUIC/UDP on %s", n.config.BindAddr, n.peerAddr())
	if err := httpServer.Serve(listener); errors.Is(err, http.ErrServerClosed) {
		return nil
	} else {
		return err
	}
}

// Handler returns the optional HTTP server adapter after Open succeeds.
func (n *Node) Handler() (http.Handler, error) {
	if n.server == nil {
		return nil, fmt.Errorf("node is not open")
	}
	return n.server, nil
}

// API returns the in-process Go API used by the HTTP adapter.
func (n *Node) API() (*network.Server, error) {
	if n.server == nil {
		return nil, fmt.Errorf("node is not open")
	}
	return n.server, nil
}

func quarantineSQLite(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	suffix := fmt.Sprintf(".corrupt-%d", time.Now().UnixNano())
	if err := os.Rename(path, path+suffix); err != nil {
		return err
	}
	for _, extra := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + extra); err == nil {
			_ = os.Rename(path+extra, filepath.Clean(path+extra+suffix))
		}
	}
	return nil
}

func (n *Node) replayLocalDecisions(ctx context.Context) error {
	for {
		from := quepaxa.Slot(n.material.Tip() + 1)
		decisions, tip, err := n.core.DecisionsFrom(from, 256)
		if err != nil {
			return err
		}
		for _, decision := range decisions {
			if err := n.material.Apply(ctx, uint64(decision.Slot), decision.Value); err != nil {
				return err
			}
		}
		if quepaxa.Slot(n.material.Tip()) >= tip {
			return nil
		}
	}
}

func (n *Node) startCatchUp(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) {
	for {
		if err := n.catchUp(ctx, transport, cluster); err != nil {
			n.ready.Store(false)
			log.Printf("quorum catch-up failed: %v", err)
		} else {
			n.ready.Store(true)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (n *Node) catchUp(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	for {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return err
		}
		applied := quepaxa.Slot(n.material.Tip())
		type result struct {
			response network.DecisionsResponse
			err      error
		}
		results := make(chan result, len(cluster.Members)-1)
		roundCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pending := 0
		for _, member := range cluster.Members {
			if member.ID == n.config.NodeID {
				continue
			}
			pending++
			go func(source quepaxa.NodeID) {
				response, err := transport.FetchDecisions(roundCtx, source, applied+1, 256)
				results <- result{response: response, err: err}
			}(member.ID)
		}
		successes := 1 // local recorder
		var best *network.DecisionsResponse
		for pending > 0 {
			select {
			case <-roundCtx.Done():
				pending = 0
			case result := <-results:
				pending--
				if result.err == nil {
					successes++
					if best == nil || result.response.Tip > best.Tip {
						copy := result.response
						best = &copy
					}
				}
			}
		}
		cancel()
		if successes < cluster.QuorumSize() {
			return quepaxa.ErrQuorumUnavailable
		}
		if best == nil || best.Tip <= applied {
			return nil
		}
		expected := applied + 1
		for _, decision := range best.Decisions {
			if decision.Slot != expected {
				return fmt.Errorf("catch-up gap: expected=%d got=%d", expected, decision.Slot)
			}
			if err := n.core.AcceptCertifiedValue(decision); err != nil {
				return err
			}
			if err := n.material.Apply(ctx, uint64(decision.Slot), decision.Value); err != nil {
				return err
			}
			expected++
		}
		if len(best.Decisions) == 0 {
			return fmt.Errorf("catch-up source reported tip %d without slot %d", best.Tip, expected)
		}
	}
}

// Shutdown gracefully shuts down the node.
func (n *Node) Shutdown() error {
	n.opened.Store(false)
	if n.checkpointer != nil {
		n.checkpointer.Stop()
	}
	if n.peer != nil {
		n.peer.Close()
		n.peer = nil
	}
	if n.transport != nil {
		n.transport.Close()
		n.transport = nil
	}
	var shutdownErr error
	if n.qlogSync != nil && n.material != nil && n.core != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := n.qlogSync.Sync(shutdownCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else if n.checkpointer != nil {
			shutdownErr = errors.Join(shutdownErr, n.checkpointer.CheckpointOnShutdown(shutdownCtx, uint64(n.core.Tip())))
		}
		cancel()
		n.qlogSync.Stop()
		n.qlogSync = nil
	}
	// Close WAL
	if n.wal != nil {
		shutdownErr = errors.Join(shutdownErr, n.wal.Sync(), n.wal.Close())
		n.wal = nil
	}

	// Close materializer
	if n.material != nil {
		shutdownErr = errors.Join(shutdownErr, n.material.Close())
		n.material = nil
	}

	// Release lock
	if n.lock != nil {
		shutdownErr = errors.Join(shutdownErr, n.lock.Release())
		n.lock = nil
	}

	if n.bucket != nil {
		shutdownErr = errors.Join(shutdownErr, n.bucket.Close())
		n.bucket = nil
	}
	return shutdownErr
}

func (n *Node) ObjectStoreStats() (objectstore.Stats, bool) {
	if n.bucket == nil {
		return objectstore.Stats{}, false
	}
	return n.bucket.Stats(), true
}

// loadClusterConfig loads cluster configuration.
func (n *Node) loadClusterConfig() *quepaxa.Cluster {
	if len(n.config.Members) > 0 {
		return &quepaxa.Cluster{ConfigID: 1, Members: n.config.Members}
	}
	return &quepaxa.Cluster{
		ConfigID: 1,
		Members: []quepaxa.Member{
			{ID: n.config.NodeID, URL: "http://" + n.config.BindAddr, PeerURL: "quic://" + n.peerAddr()},
		},
	}
}

func (n *Node) peerAddr() string {
	if n.config.PeerAddr != "" {
		return n.config.PeerAddr
	}
	host, _, err := net.SplitHostPort(n.config.BindAddr)
	if err != nil {
		return n.config.BindAddr
	}
	return net.JoinHostPort(host, "9090")
}

func main() {
	// Parse config from environment
	config := &types.ExecutionConfig{
		ClusterID: types.ClusterID(os.Getenv("RHIZA_CLUSTER_ID")),
		Profile:   types.Profile(os.Getenv("RHIZA_EXECUTION_PROFILE")),
		DataDir:   getEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"),
		BindAddr:  getEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
	}

	node := New(config)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		log.Fatalf("node error: %v", err)
	}

	node.Shutdown()
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
