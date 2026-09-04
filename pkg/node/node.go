package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Node is the main runtime container.
type Node struct {
	config       *types.ExecutionConfig
	core         *quepaxa.Core
	material     *materializer.Materializer
	server       *network.Server
	peer         *network.PeerServer
	transport    *network.Transport
	catchUp      *network.Transport
	wal          *qlog.WAL
	lock         *qlog.LockFile
	bucket       *objectstore.MeteredBucket
	checkpoints  *checkpoint.Manager
	archive      *recovery.Manager
	checkpointer *checkpoint.AutoCheckpointer
	ready        atomic.Bool
	opened       atomic.Bool
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	recoveryMu   sync.Mutex
	compactionMu sync.Mutex
	catchUpWake  chan struct{}
}

// New creates a new Node.
func New(config *types.ExecutionConfig) *Node {
	return &Node{
		config: config,
	}
}

func validateObjectStoreConfig(config *types.ExecutionConfig) (bool, error) {
	configured := config.ObjStoreProvider != "" || config.ObjStoreEndpoint != "" || config.ObjStoreBucket != "" || config.ObjStoreDir != ""
	provider := objectstore.Provider(config.ObjStoreProvider)
	if len(config.Members) > 1 && configured {
		if provider == "" || provider == objectstore.ProviderFilesystem || config.ObjStoreDir != "" {
			return false, fmt.Errorf("multi-node clusters require explicit shared object storage")
		}
		if provider != objectstore.ProviderS3 && provider != objectstore.ProviderGCS && provider != objectstore.ProviderAzure {
			return false, fmt.Errorf("multi-node object-store provider %q is unsupported", provider)
		}
	}
	if (provider == objectstore.ProviderS3 || provider == objectstore.ProviderGCS || provider == objectstore.ProviderAzure) && config.ObjStoreBucket == "" {
		return false, fmt.Errorf("object-store bucket is required")
	}
	if provider != "" {
		if err := objectstore.ValidateConfig(objectstore.Config{
			Provider: provider, Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Insecure: config.ObjStoreInsecure,
			AzureStorageAccount: config.ObjStoreAzureStorageAccount,
		}); err != nil {
			return false, err
		}
	}
	return configured, nil
}

// Open starts the embedded engine and its private peer transport without serving HTTP.
func (n *Node) Open(ctx context.Context) (err error) {
	if n.config == nil || n.config.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if len(n.config.Members) > 1 {
		for _, member := range n.config.Members {
			if member.Token == "" {
				return fmt.Errorf("voter token is required for multi-node cluster member %s", member.ID)
			}
			if n.config.AdminToken != "" && member.Token == n.config.AdminToken {
				return fmt.Errorf("admin token must differ from voter token for %s", member.ID)
			}
		}
	}
	if n.config.ObjStoreDurability == "" {
		n.config.ObjStoreDurability = types.ObjectStoreDurabilityAsync
	}
	if n.config.ObjStoreDurability != types.ObjectStoreDurabilityAsync && n.config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return fmt.Errorf("invalid object-store durability %q", n.config.ObjStoreDurability)
	}
	if n.config.ObjStoreSyncInterval < 0 {
		return fmt.Errorf("object-store sync interval must not be negative")
	}
	if n.config.ObjStoreBatchDelay < 0 {
		return fmt.Errorf("object-store batch delay must not be negative")
	}
	if n.config.ObjStoreGCInterval < 0 || n.config.ObjStoreGCGracePeriod < 0 {
		return fmt.Errorf("object-store GC durations must not be negative")
	}
	objectStoreConfigured, configErr := validateObjectStoreConfig(n.config)
	if configErr != nil {
		return configErr
	}
	if n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck && !objectStoreConfigured {
		return fmt.Errorf("before-ack durability requires object storage")
	}
	if len(n.config.Members) > 1 && !objectStoreConfigured {
		return fmt.Errorf("multi-node clusters require shared object storage")
	}
	if n.config.MaxWALBytes < 0 {
		return fmt.Errorf("max WAL bytes must not be negative")
	}
	if !n.opened.CompareAndSwap(false, true) {
		return fmt.Errorf("node is already open")
	}
	defer func() {
		if err != nil {
			_ = n.Shutdown()
		}
	}()
	ctx, n.cancel = context.WithCancel(ctx)

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
	maxWALBytes := n.config.MaxWALBytes
	if maxWALBytes == 0 {
		maxWALBytes = 4 << 30
	}
	if maxWALBytes > 0 {
		if err := wal.SetMaxBytes(maxWALBytes); err != nil {
			return err
		}
	}
	if objectStoreConfigured {
		provider := objectstore.Provider(n.config.ObjStoreProvider)
		if provider == "" {
			provider = objectstore.ProviderS3
		}
		bucket, bucketErr := objectstore.NewBucket(objectstore.Config{
			Provider: provider, FilesystemDir: n.config.ObjStoreDir, Prefix: n.config.ObjStorePrefix,
			Endpoint: n.config.ObjStoreEndpoint, Bucket: n.config.ObjStoreBucket, Region: n.config.ObjStoreRegion,
			Insecure: n.config.ObjStoreInsecure, MaxRetries: n.config.ObjStoreRetries,
			AccessKey: n.config.ObjStoreAccessKey, SecretKey: n.config.ObjStoreSecretKey, SessionToken: n.config.ObjStoreSessionToken,
			ServiceAccount: n.config.ObjStoreServiceAccount, AzureTenantID: n.config.ObjStoreAzureTenantID,
			AzureClientID: n.config.ObjStoreAzureClientID, AzureClientSecret: n.config.ObjStoreAzureClientSecret,
			AzureStorageAccount: n.config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: n.config.ObjStoreAzureStorageAccountKey,
			AzureConnectionString: n.config.ObjStoreAzureConnectionString, AzureUserAssignedID: n.config.ObjStoreAzureUserAssignedID,
		})
		if bucketErr != nil {
			return fmt.Errorf("open object store: %w", bucketErr)
		}
		n.bucket = bucket
		clusterPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		n.checkpoints = checkpoint.NewManager(bucket, clusterPrefix, n.config.DataDir, 1)
		if loadErr := n.checkpoints.Load(ctx); loadErr != nil {
			return fmt.Errorf("load checkpoint manifest: %w", loadErr)
		}
		n.archive = recovery.NewManager(bucket, clusterPrefix, 1)
		n.archive.SetGroupDelay(n.config.ObjStoreBatchDelay)
		if len(n.config.Members) > 1 && !n.archive.CASSupported() {
			return fmt.Errorf("multi-node shared archive requires conditional object writes")
		}
		if loadErr := n.archive.Load(ctx); loadErr != nil {
			if n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck {
				return fmt.Errorf("load shared decision archive: %w", loadErr)
			}
			log.Printf("shared archive unavailable during async startup: %v", loadErr)
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
		// Rebuilding from the retained consensus base and suffix is safer and
		// smaller than database-specific corruption repair.
		if rebuildErr := quarantineSQLite(n.config.DataDir + "/sqlite.db"); rebuildErr != nil {
			return fmt.Errorf("open materializer: %w; quarantine: %v", err, rebuildErr)
		}
		if rebuildErr := quarantineGraph(n.config.DataDir + "/latticedb"); rebuildErr != nil {
			return fmt.Errorf("open materializer: %w; quarantine graph: %v", err, rebuildErr)
		}
		material, err = materializer.Open(n.config.DataDir+"/sqlite.db", 4)
		if err != nil {
			return fmt.Errorf("rebuild materializer: %w", err)
		}
	}
	n.material = material
	if err := material.ConfigureLocalGraphNodePropertyIndexes(n.config.LocalGraphNodePropertyIndexes); err != nil {
		_ = material.Close()
		return fmt.Errorf("configure local graph indexes: %w", err)
	}

	// 5. Create consensus core through the same public API available to external users.
	cluster := n.loadClusterConfig()
	transport := network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.AdminToken)
	n.transport = transport
	if len(cluster.Members) > 1 {
		n.catchUpWake = make(chan struct{}, 1)
		n.catchUp = network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.AdminToken)
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport,
	})
	if err != nil {
		return fmt.Errorf("create QuePaxa core: %w", err)
	}
	n.core = core
	if n.checkpoints != nil {
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
	}
	if n.archive != nil {
		if seal, decision, ok := n.archive.RecoveryBase(); ok && core.Tip() < seal.Index {
			if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
				return fmt.Errorf("restore checkpoint recovery base: %w", err)
			}
		}
		for core.Tip() < n.archive.Tip() {
			values, _, archiveErr := n.archive.DecisionsFrom(ctx, core.Tip()+1, 256)
			if archiveErr != nil || len(values) == 0 {
				if archiveErr == nil {
					archiveErr = fmt.Errorf("shared archive omitted slot %d", core.Tip()+1)
				}
				return archiveErr
			}
			if archiveErr := core.AcceptCertifiedValues(values); archiveErr != nil {
				return fmt.Errorf("recover shared archive: %w", archiveErr)
			}
		}
	}
	// Record RPCs must be available while every replica is recovering. Public
	// proposals and learned decisions remain gated by ready=false.
	server := network.NewServer(core, material, n.config.ClusterID, true, transport, n.ready.Load)
	if n.checkpoints != nil {
		server.SetCheckpointPrepare(func(ctx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) error {
			if err := n.checkpoints.ValidatePublisherClaim(ctx, string(sender), uint64(seal.Index), seal.RootHash); err != nil {
				return err
			}
			return core.PrepareCheckpoint(ctx, seal)
		})
	}
	server.SetObjectStoreStats(func() (map[string]uint64, bool) {
		stats, ok := n.ObjectStoreStats()
		return map[string]uint64{
			"uploads": stats.Uploads, "gets": stats.Gets, "lists": stats.Lists, "heads": stats.Heads,
			"deletes": stats.Deletes, "failures": stats.Failures, "bytes_uploaded": stats.BytesUploaded,
			"bytes_downloaded": stats.BytesDownloaded, "s3_http_requests": stats.S3HTTPRequests,
			"s3_http_failures":    stats.S3HTTPFailures,
			"condition_conflicts": stats.ConditionConflicts, "dedup_hits": stats.DedupHits,
			"sdk_retries": stats.SDKRetries, "transport_failures": stats.TransportFailures,
			"http_4xx_unexpected": stats.Unexpected4xx, "http_5xx": stats.HTTP5xx,
		}, ok
	})
	if n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck {
		server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
			if err := core.EnsureDurable(slot); err != nil {
				return err
			}
			if err := n.archive.SyncThrough(ctx, core, slot); err != nil {
				return err
			}
			return nil
		})
	}
	n.server = server
	server.SetCompactedHandler(n.wakeCatchUp)
	peer, err := network.StartPeerServer(ctx, n.peerAddr(), server, cluster.Members, n.config.AdminToken)
	if err != nil {
		return fmt.Errorf("listen peer QUIC: %w", err)
	}
	n.peer = peer

	var certifiedCheckpoint *checkpoint.Checkpoint
	if n.checkpoints != nil {
		seal, sealed, sealErr := core.LatestCheckpointSeal()
		if sealErr != nil {
			return sealErr
		}
		if current := n.checkpoints.Latest(); current != nil && (!sealed || current.Index > uint64(seal.Index) || current.Index == uint64(seal.Index) && current.RootHash != seal.RootHash) {
			return fmt.Errorf("checkpoint CURRENT is not backed by the certified seal; start with fresh object storage")
		}
		if sealed {
			certifiedCheckpoint, err = n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
			if err != nil || certifiedCheckpoint.Hash != seal.StateHash {
				if err == nil {
					err = fmt.Errorf("certified checkpoint state hash mismatch")
				}
				return err
			}
			if err := n.checkpoints.PromoteCertifiedCurrent(ctx, certifiedCheckpoint); err != nil {
				return fmt.Errorf("promote certified checkpoint: %w", err)
			}
		}
	}
	recoveryTarget := quepaxa.Slot(material.Tip())
	if recorderTip := core.RecorderTip(); recorderTip > recoveryTarget {
		recoveryTarget = recorderTip
	}
	if certifiedCheckpoint != nil && quepaxa.Slot(certifiedCheckpoint.Index) > recoveryTarget {
		recoveryTarget = quepaxa.Slot(certifiedCheckpoint.Index)
	}
	if recoveryTarget > core.Tip() {
		if err := core.RecoverThrough(ctx, recoveryTarget); err != nil {
			return fmt.Errorf("recover certified log through %d: %w", recoveryTarget, err)
		}
	}
	if certifiedCheckpoint != nil {
		if certifiedCheckpoint.Index > material.Tip() {
			if quepaxa.Slot(certifiedCheckpoint.Index) > core.Tip() {
				return fmt.Errorf("checkpoint slot %d is ahead of certified log tip %d", certifiedCheckpoint.Index, core.Tip())
			}
			dir, fileErr := os.MkdirTemp(n.config.DataDir, ".rhiza-checkpoint-restore-*")
			if fileErr != nil {
				return fileErr
			}
			defer os.RemoveAll(dir)
			files, readErr := n.checkpoints.DownloadRootFiles(ctx, certifiedCheckpoint.Index, certifiedCheckpoint.RootHash, dir)
			if readErr != nil {
				return readErr
			}
			materialFiles := make([]materializer.CheckpointFile, 0, len(files))
			for _, file := range files {
				materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			if restoreErr := material.RestoreCheckpoint(ctx, materialFiles); restoreErr != nil {
				return fmt.Errorf("restore checkpoint %d: %w", certifiedCheckpoint.Index, restoreErr)
			}
		}
	}

	// 6. Verify a materializer that survived beyond the local decision marker,
	// then replay the remaining recovered decisions.
	if quepaxa.Slot(material.Tip()) > core.Tip() {
		return fmt.Errorf("materialized slot %d is ahead of certified log tip %d", material.Tip(), core.Tip())
	}
	if material.Tip() > uint64(core.CompactionFloor()) {
		decision, ok := core.CertifiedValue(quepaxa.Slot(material.Tip()))
		if !ok {
			return fmt.Errorf("materialized slot %d has no recovered decision", material.Tip())
		}
		if err := material.ValidateTip(material.Tip(), decision.Value); err != nil {
			return err
		}
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return fmt.Errorf("replay local decisions: %w", err)
	}
	if len(cluster.Members) == 1 {
		n.ready.Store(true)
	} else {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.startCatchUp(ctx, n.catchUp, cluster)
		}()
	}

	// 8. Start periodic WAL sync
	core.StartPeriodicSync(ctx, 1*time.Second)
	if n.archive != nil {
		interval := n.config.ObjStoreSyncInterval
		if interval == 0 {
			interval = time.Minute
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := n.catchUpArchive(ctx); err != nil {
						log.Printf("shared archive catch-up error: %v", err)
					} else if err := n.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
						log.Printf("shared archive sync error: %v", err)
					} else if err := n.compactCertifiedCheckpoint(ctx); err != nil {
						log.Printf("certified checkpoint compaction skipped: %v", err)
					}
				}
			}
		}()
	}
	if n.checkpoints != nil {
		interval := n.config.CheckpointInterval
		if interval <= 0 {
			interval = 15 * time.Minute
		}
		n.checkpointer = checkpoint.NewAutoCheckpointer(n.checkpoints, material, 1, interval)
		tailBytes := n.config.CheckpointTailBytes
		if tailBytes <= 0 {
			tailBytes = 512 << 20
		}
		n.checkpointer.ConfigureTail(n.wal.Bytes, tailBytes)
		n.checkpointer.ConfigurePublisher(string(core.NodeID()), func() uint64 {
			floor := material.Tip()
			if index, _, ok := core.LatestPreparedCheckpoint(); ok {
				floor = max(floor, uint64(index))
			}
			if seal, ok, err := core.LatestCheckpointSeal(); err == nil && ok {
				floor = max(floor, uint64(seal.Index))
			}
			if latest := n.checkpoints.Latest(); latest != nil {
				floor = max(floor, latest.Index)
			}
			return floor
		}, func(ctx context.Context, reserved uint64) error {
			for material.Tip() < reserved {
				var nonce [types.ReadBarrierNonceSize]byte
				if _, err := rand.Read(nonce[:]); err != nil {
					return err
				}
				if _, err := server.ProposeControl(ctx, types.EncodeReadBarrier(nonce)); err != nil {
					return err
				}
			}
			return nil
		})
		n.checkpointer.ConfigurePublication(
			func() bool {
				seal, ok, err := core.LatestCheckpointSeal()
				return err == nil && (!ok || material.Tip() > uint64(seal.Index))
			},
			func(ctx context.Context, root *checkpoint.Checkpoint) error {
				prefix, ok := core.PrefixHash(quepaxa.Slot(root.Index))
				if !ok {
					return fmt.Errorf("checkpoint prefix %d is unavailable", root.Index)
				}
				order, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(root.Index))
				if err != nil {
					return err
				}
				seal := quepaxa.CheckpointSeal{
					ConfigID: core.ConfigID(), Index: quepaxa.Slot(root.Index), RootHash: root.RootHash,
					StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: order, FollowingLeaderOrder: following,
				}
				if err := n.checkpoints.ValidatePublisherClaim(ctx, string(core.NodeID()), root.Index, root.RootHash); err != nil {
					return err
				}
				if err := core.PrepareCheckpoint(ctx, seal); err != nil {
					return err
				}
				if err := transport.PrepareCheckpoint(ctx, seal); err != nil {
					return err
				}
				value, err := quepaxa.EncodeCheckpointSeal(seal)
				if err != nil {
					return err
				}
				_, _, err = core.Propose(ctx, value)
				if err != nil {
					return err
				}
				if err := n.replayLocalDecisions(ctx); err != nil {
					return err
				}
				if err := n.checkpoints.PromoteCertifiedCurrent(ctx, root); err != nil {
					return err
				}
				return n.compactCertifiedCheckpoint(ctx)
			},
		)
		n.checkpointer.Start(ctx, material.StateTip, func(ctx context.Context) error {
			if err := n.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
				return err
			}
			return nil
		})
	}
	if n.checkpoints != nil && n.config.ObjStoreGCInterval > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ticker := time.NewTicker(n.config.ObjStoreGCInterval)
			defer ticker.Stop()
			var archiveFloor uint64
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					order := core.ProposerOrder()
					if len(order) == 0 || order[0] != core.NodeID() {
						continue
					}
					retain := make(map[[32]byte]struct{}, 2)
					if _, root, ok := core.RecoveryRoot(); ok {
						retain[root] = struct{}{}
					}
					if seal, ok, err := core.LatestCheckpointSeal(); err == nil && ok {
						retain[seal.RootHash] = struct{}{}
					}
					if n.archive != nil {
						if err := n.archive.Load(ctx); err != nil {
							log.Printf("load shared archive before checkpoint GC failed: %v", err)
							continue
						}
						seal, _, ok := n.archive.RecoveryBase()
						archiveFloor, ok = advanceArchiveFloor(archiveFloor, uint64(seal.Index), ok)
						if !ok {
							continue
						}
						retain[seal.RootHash] = struct{}{}
					}
					if err := n.checkpoints.GarbageCollectFrom(ctx, retain, 2, archiveFloor, n.config.ObjStoreGCGracePeriod); err != nil {
						log.Printf("checkpoint GC failed: %v", err)
					} else if n.archive != nil {
						if err := n.archive.Cleanup(ctx, n.config.ObjStoreGCGracePeriod); err != nil {
							log.Printf("archive GC failed: %v", err)
						}
					}
				}
			}
		}()
	}
	return nil
}

func advanceArchiveFloor(current, base uint64, ok bool) (uint64, bool) {
	if !ok {
		return current, false
	}
	return max(current, base), true
}

func (n *Node) compactCertifiedCheckpoint(ctx context.Context) error {
	// Auto-checkpoint completion and the archive ticker can observe the same
	// seal. Keep the whole trim/compact transition single-flight so the later
	// caller rechecks the newly installed floor instead of compacting it twice.
	n.compactionMu.Lock()
	defer n.compactionMu.Unlock()
	if n.core == nil || n.archive == nil || n.checkpoints == nil {
		return nil
	}
	seal, ok, err := n.core.LatestCheckpointSeal()
	if err != nil {
		return err
	}
	if !ok || seal.Index <= n.core.CompactionFloor() {
		return nil
	}
	if quepaxa.Slot(n.material.Tip()) < seal.Index {
		return fmt.Errorf("materializer tip %d is behind checkpoint %d", n.material.Tip(), seal.Index)
	}
	if err := n.core.VerifyCheckpoint(ctx, seal.CheckpointSeal); err != nil {
		return err
	}
	if err := n.archive.SyncThrough(ctx, n.core, seal.DecisionSlot); err != nil {
		return err
	}
	decision, ok := n.core.CertifiedValue(seal.DecisionSlot)
	if !ok {
		return fmt.Errorf("checkpoint seal decision %d is unavailable", seal.DecisionSlot)
	}
	if err := n.archive.TrimThrough(ctx, seal, decision); err != nil {
		return err
	}
	return n.core.CompactThrough(seal.Index, seal.RootHash)
}

func (n *Node) catchUpArchive(ctx context.Context) error {
	if n.archive == nil || n.core == nil {
		return nil
	}
	if err := n.archive.Load(ctx); err != nil {
		return err
	}
	if seal, _, ok := n.archive.RecoveryBase(); ok && archiveRecoveryRequired(n.core.Tip(), seal.Index) {
		// The archive has trimmed the local next slot. Waiting for a peer to
		// report ErrCompacted only turns this into a periodic retry loop.
		return n.restoreArchiveCatchUp(ctx)
	}
	for n.core.Tip() < n.archive.Tip() {
		values, _, err := n.archive.DecisionsFrom(ctx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	return n.replayLocalDecisions(ctx)
}

func archiveRecoveryRequired(localTip, archiveBase quepaxa.Slot) bool {
	return archiveBase != 0 && localTip < archiveBase
}

func (n *Node) restoreArchiveCatchUp(ctx context.Context) (resultErr error) {
	n.recoveryMu.Lock()
	defer n.recoveryMu.Unlock()
	// A compacted peer means local history may no longer be sufficient for a
	// linearizable operation. Reject new client traffic before any remote I/O.
	n.ready.Store(false)
	if n.archive == nil || n.checkpoints == nil || n.core == nil || n.material == nil || n.server == nil {
		return fmt.Errorf("live checkpoint recovery is unavailable")
	}
	var pinNonce [8]byte
	if _, err := rand.Read(pinNonce[:]); err != nil {
		return err
	}
	owner := fmt.Sprintf("%s-%x", n.config.NodeID, pinNonce)
	snapshot, err := n.archive.BeginRecoverySnapshot(ctx, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = snapshot.Close(closeCtx)
	}()
	seal, baseDecision, ok := snapshot.RecoveryBase()
	if !ok {
		return fmt.Errorf("shared archive has no checkpoint recovery base")
	}
	root, err := n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil {
		return err
	}
	if root.Hash != seal.StateHash {
		return fmt.Errorf("certified checkpoint state hash mismatch")
	}
	checkpointPin, err := n.checkpoints.PinRecoveryRoot(ctx, root, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = checkpointPin.Close(closeCtx)
	}()
	root, err = checkpointPin.Root()
	if err != nil {
		return err
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var renewMu sync.Mutex
	var renewFailure error
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(40 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := checkpointPin.Renew(workCtx, 2*time.Minute); err != nil {
					renewMu.Lock()
					renewFailure = err
					renewMu.Unlock()
					cancel()
					return
				}
				if err := snapshot.Renew(workCtx, 2*time.Minute); err != nil {
					renewMu.Lock()
					renewFailure = err
					renewMu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-renewDone
	}()
	checkRenewFailure := func(err error) error {
		renewMu.Lock()
		defer renewMu.Unlock()
		if renewFailure != nil {
			return renewFailure
		}
		return err
	}
	defer func() { resultErr = checkRenewFailure(resultErr) }()
	dir, err := os.MkdirTemp(n.config.DataDir, ".rhiza-live-restore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	files, err := n.checkpoints.DownloadAndVerifyRootFiles(workCtx, root, dir)
	if err != nil {
		return checkRenewFailure(err)
	}
	materialFiles := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	resume, err := n.server.Quiesce(workCtx)
	if err != nil {
		return checkRenewFailure(err)
	}
	defer resume()
	if n.core.Tip() < seal.Index {
		if err := n.core.RestoreCheckpointBase(workCtx, seal, baseDecision); err != nil {
			return err
		}
	} else if prefix, ok := n.core.PrefixHash(seal.Index); !ok || prefix != seal.PrefixHash {
		return fmt.Errorf("local consensus prefix does not match checkpoint recovery base")
	}
	if n.material.Tip() < uint64(seal.Index) {
		if err := n.material.RestoreCheckpoint(workCtx, materialFiles); err != nil {
			return err
		}
	}
	for n.core.Tip() < snapshot.Tip() {
		values, _, err := snapshot.DecisionsFrom(workCtx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	if err := n.archive.Load(workCtx); err != nil {
		return err
	}
	for n.core.Tip() < n.archive.Tip() {
		values, _, err := n.archive.DecisionsFrom(workCtx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	if quepaxa.Slot(n.material.Tip()) > n.core.Tip() {
		return fmt.Errorf("materialized slot %d is ahead of certified log tip %d", n.material.Tip(), n.core.Tip())
	}
	if err := n.replayLocalDecisions(workCtx); err != nil {
		return err
	}
	if n.material.Tip() > uint64(n.core.CompactionFloor()) {
		decision, ok := n.core.CertifiedValue(quepaxa.Slot(n.material.Tip()))
		if !ok {
			return fmt.Errorf("materialized slot %d has no recovered decision", n.material.Tip())
		}
		if err := n.material.ValidateTip(n.material.Tip(), decision.Value); err != nil {
			return err
		}
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
		return errors.Join(fmt.Errorf("listen: %w", err), n.Shutdown())
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

// Ready reports whether local startup recovery and catch-up have completed.
// It does not prove current quorum availability.
func (n *Node) Ready() bool { return n.ready.Load() }

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
		if err := n.material.ApplyBatch(ctx, decisions); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) >= tip {
			return nil
		}
	}
}

func (n *Node) startCatchUp(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) {
	n.observeCatchUp(n.catchUpQuorum(ctx, transport, cluster))
	var round uint64
	for {
		delay := syncInterval(n.config.NodeID, round)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		case <-n.catchUpWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if !n.ready.Load() {
			n.observeCatchUp(n.catchUpQuorum(ctx, transport, cluster))
		} else if sources := syncSources(n.config.NodeID, cluster.Members, round); len(sources) > 0 {
			var syncErr error
			for _, source := range sources {
				if syncErr = n.catchUpPeer(ctx, transport, source); syncErr == nil {
					break
				}
			}
			if syncErr != nil {
				log.Printf("operation sync deferred: %v", syncErr)
			}
			// A compacted peer can force checkpoint recovery from catchUpPeer.
			// Only a fresh quorum round may make that node ready again.
			if !n.ready.Load() {
				n.observeCatchUp(n.catchUpQuorum(ctx, transport, cluster))
			}
		}
		round++
	}
}

// wakeCatchUp is called by the foreground server when a peer has compacted
// history needed by this node. The one-slot channel coalesces concurrent
// callers; startCatchUp is the only recovery worker.
func (n *Node) wakeCatchUp() {
	n.ready.Store(false)
	if n.catchUpWake == nil {
		return
	}
	select {
	case n.catchUpWake <- struct{}{}:
	default:
	}
}

func syncInterval(nodeID quepaxa.NodeID, round uint64) time.Duration {
	hash := sha256.Sum256([]byte(fmt.Sprintf("rhiza-sync-jitter:%s:%d", nodeID, round)))
	return time.Duration(900+binary.BigEndian.Uint16(hash[:2])%201) * time.Millisecond
}

func syncSources(nodeID quepaxa.NodeID, members []quepaxa.Member, round uint64) []quepaxa.NodeID {
	peers := make([]quepaxa.NodeID, 0, len(members)-1)
	for _, member := range members {
		if member.ID != nodeID {
			peers = append(peers, member.ID)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	seed := sha256.Sum256([]byte(fmt.Sprintf("rhiza-sync-peer:%s:%d", nodeID, round/uint64(len(peers)))))
	for i := len(peers) - 1; i > 0; i-- {
		j := int(binary.BigEndian.Uint64(seed[(i*8)%24:]) % uint64(i+1))
		peers[i], peers[j] = peers[j], peers[i]
	}
	first := int(round % uint64(len(peers)))
	return append(peers[first:], peers[:first]...)
}

func (n *Node) observeCatchUp(err error) {
	if err != nil {
		// A transient peer timeout must not make an already caught-up node
		// reject traffic; quorum operations enforce their own availability.
		log.Printf("quorum catch-up failed: %v", err)
		return
	}
	n.ready.Store(true)
}

func hasUsablePeerSuffix(applied quepaxa.Slot, successes, quorum int, best *network.DecisionsResponse) bool {
	return successes >= quorum && best != nil && best.Tip > applied
}

func (n *Node) catchUpQuorum(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
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
		// Initial QUIC connection establishment can exceed the steady-state RPC
		// budget after all peers restart and DNS endpoints are republished.
		roundCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pending := 0
		for _, member := range cluster.Members {
			if member.ID == n.config.NodeID {
				continue
			}
			pending++
			go func(source quepaxa.NodeID) {
				response, err := transport.FetchDecisions(roundCtx, source, applied+1, 128)
				results <- result{response: response, err: err}
			}(member.ID)
		}
		successes := 1 // local recorder
		var best *network.DecisionsResponse
		compacted := false
		var grace *time.Timer
		for pending > 0 {
			var graceC <-chan time.Time
			if successes >= cluster.QuorumSize() {
				if grace == nil {
					grace = time.NewTimer(10 * time.Millisecond)
				}
				graceC = grace.C
			}
			select {
			case <-roundCtx.Done():
				pending = 0
			case <-graceC:
				pending = 0
			case result := <-results:
				pending--
				if errors.Is(result.err, quepaxa.ErrCompacted) {
					compacted = true
				} else if result.err == nil {
					successes++
					if best == nil || result.response.Tip > best.Tip {
						copy := result.response
						best = &copy
					}
				}
			}
		}
		if grace != nil {
			grace.Stop()
		}
		cancel()
		peerSuffix := hasUsablePeerSuffix(applied, successes, cluster.QuorumSize(), best)
		if compacted && !peerSuffix {
			if err := n.restoreArchiveCatchUp(ctx); err != nil {
				return err
			}
			continue
		}
		if successes < cluster.QuorumSize() {
			return quepaxa.ErrQuorumUnavailable
		}
		if !peerSuffix {
			return nil
		}
		expected := applied + 1
		for _, decision := range best.Decisions {
			if decision.Slot != expected {
				return fmt.Errorf("catch-up gap: expected=%d got=%d", expected, decision.Slot)
			}
			expected++
		}
		if err := n.core.AcceptCertifiedValues(best.Decisions); err != nil {
			return err
		}
		if err := n.material.ApplyBatch(ctx, best.Decisions); err != nil {
			return err
		}
		if len(best.Decisions) == 0 {
			return fmt.Errorf("catch-up source reported tip %d without slot %d", best.Tip, expected)
		}
	}
}

func (n *Node) catchUpPeer(ctx context.Context, transport *network.Transport, source quepaxa.NodeID) error {
	for {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return err
		}
		from := quepaxa.Slot(n.material.Tip()) + 1
		pageCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		response, err := transport.FetchDecisions(pageCtx, source, from, 128)
		cancel()
		if err != nil {
			if errors.Is(err, quepaxa.ErrCompacted) {
				if err := n.restoreArchiveCatchUp(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if response.Tip < from {
			return nil
		}
		if len(response.Decisions) == 0 || response.Decisions[0].Slot != from {
			return fmt.Errorf("operation sync source %s omitted slot %d", source, from)
		}
		if err := n.core.AcceptCertifiedValues(response.Decisions); err != nil {
			return err
		}
		if err := n.material.ApplyBatch(ctx, response.Decisions); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) >= response.Tip {
			return nil
		}
	}
}

// Shutdown gracefully shuts down the node.
func (n *Node) Shutdown() error {
	n.opened.Store(false)
	n.ready.Store(false)
	var shutdownErr error
	if n.checkpointer != nil {
		n.checkpointer.Stop()
	}
	if n.archive != nil && n.material != nil && n.core != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := n.archive.SyncThrough(shutdownCtx, n.core, n.core.Tip()); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else if n.checkpointer != nil {
			shutdownErr = errors.Join(shutdownErr, n.checkpointer.CheckpointOnShutdown(shutdownCtx, n.material.StateTip()))
		}
		cancel()
	}
	n.checkpointer = nil
	if n.server != nil {
		n.server.Close()
		n.server = nil
	}
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.wg.Wait()
	if n.archive != nil {
		n.archive.Close()
		n.archive = nil
	}
	if n.peer != nil {
		n.peer.Close()
		n.peer = nil
	}
	if n.transport != nil {
		n.transport.Close()
		n.transport = nil
	}
	if n.catchUp != nil {
		n.catchUp.Close()
		n.catchUp = nil
	}
	if n.core != nil {
		n.core.StopPeriodicSync()
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
