package rhiza

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type ReplicaMode string

const (
	ReplicaModeObjectStore ReplicaMode = "object-store"
	ReplicaModeLearner     ReplicaMode = "learner"
)

// ReplicaConfig configures a non-voting, read-only follower. Members contains
// only the fixed voters whose certificates the follower verifies.
type ReplicaConfig struct {
	ClusterID    string
	ReplicaID    string
	DataDir      string
	AdminToken   string
	Members      []network.PeerIdentity
	SyncInterval time.Duration

	ObjStoreEndpoint     string
	ObjStoreBucket       string
	ObjStoreProvider     string
	ObjStoreDir          string
	ObjStorePrefix       string
	ObjStoreRegion       string
	ObjStoreInsecure     bool
	ObjStoreRetries      int
	ObjStoreAccessKey    string
	ObjStoreSecretKey    string
	ObjStoreSessionToken string
}

type ReplicaStatus struct {
	Mode        ReplicaMode
	AppliedSlot uint64
	SourceTip   uint64
	Source      string
	LastSync    time.Time
	LastError   string
}

type replicaIdentity struct {
	ClusterID string   `json:"cluster_id"`
	ConfigID  uint     `json:"config_id"`
	ReplicaID string   `json:"replica_id"`
	Voters    []string `json:"voters"`
	Provider  string   `json:"provider"`
	Endpoint  string   `json:"endpoint"`
	Bucket    string   `json:"bucket"`
	Directory string   `json:"directory"`
	Prefix    string   `json:"prefix"`
}

// ReadReplica is an eventual, read-only copy. It never proposes, votes,
// acknowledges decisions, or participates in quorum/read-index operations.
type ReadReplica struct {
	mode        ReplicaMode
	config      ReplicaConfig
	core        *quepaxa.Core
	material    *materializer.Materializer
	api         *network.Server
	wal         *qlog.WAL
	lock        *qlog.LockFile
	bucket      *objstore.MeteredBucket
	checkpoints *checkpoint.Manager
	archive     *recovery.Manager
	transport   *network.Transport
	fetch       func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error)
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	syncMu      sync.Mutex
	statusMu    sync.RWMutex
	status      ReplicaStatus
	ready       atomic.Bool
	closeOnce   sync.Once
	closeErr    error
	peerCursor  int
}

// OpenReadReplica follows certified checkpoint/archive state only.
func OpenReadReplica(ctx context.Context, config ReplicaConfig) (*ReadReplica, error) {
	return openReplica(ctx, config, ReplicaModeObjectStore)
}

// OpenLearner follows voter peer logs first and falls back to certified object
// storage after compaction or peer unavailability. It is not cluster membership.
func OpenLearner(ctx context.Context, config ReplicaConfig) (*ReadReplica, error) {
	return openReplica(ctx, config, ReplicaModeLearner)
}

func openReplica(ctx context.Context, config ReplicaConfig, mode ReplicaMode) (_ *ReadReplica, resultErr error) {
	if config.DataDir == "" || config.ReplicaID == "" {
		return nil, fmt.Errorf("replica ID and data directory are required")
	}
	if config.ClusterID == "" {
		config.ClusterID = "cluster-a"
	}
	if len(config.Members) == 0 {
		return nil, fmt.Errorf("voter membership is required")
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(config.Members))
	for _, member := range config.Members {
		if member.ID == "" {
			return nil, fmt.Errorf("voter ID is required")
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return nil, fmt.Errorf("duplicate voter %q", member.ID)
		}
		seen[member.ID] = struct{}{}
		if member.ID == quepaxa.NodeID(config.ReplicaID) {
			return nil, fmt.Errorf("replica ID must not be a voter")
		}
		if mode == ReplicaModeLearner && (member.PeerURL == "" || member.PublicKey == ([32]byte{})) {
			return nil, fmt.Errorf("learner voter %q requires a peer URL and pinned public key", member.ID)
		}
	}
	if config.ObjStoreProvider == "" {
		config.ObjStoreProvider = string(objstore.ProviderS3)
	}
	if config.SyncInterval < 0 {
		return nil, fmt.Errorf("replica sync interval must not be negative")
	}
	if mode == ReplicaModeLearner && config.AdminToken == "" {
		return nil, fmt.Errorf("learner requires the voter admin token for read-only sync")
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return nil, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	r := &ReadReplica{mode: mode, config: config, status: ReplicaStatus{Mode: mode}, ctx: childCtx, cancel: cancel}
	defer func() {
		if resultErr != nil {
			_ = r.Close()
		}
	}()
	_, lock, err := qlog.Acquire(path.Join(config.DataDir, "qlog"))
	if err != nil {
		return nil, fmt.Errorf("acquire replica lock: %w", err)
	}
	r.lock = lock
	if err := ensureReplicaIdentity(config); err != nil {
		return nil, err
	}
	r.wal, err = qlog.Open(path.Join(config.DataDir, "qlog"))
	if err != nil {
		return nil, fmt.Errorf("open replica WAL: %w", err)
	}
	r.material, err = materializer.Open(path.Join(config.DataDir, "sqlite.db"), 4)
	if err != nil {
		return nil, fmt.Errorf("open replica materializer: %w", err)
	}
	r.bucket, err = objstore.NewBucket(objstore.Config{
		Provider: objstore.Provider(config.ObjStoreProvider), FilesystemDir: config.ObjStoreDir,
		Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Region: config.ObjStoreRegion,
		Insecure: config.ObjStoreInsecure, MaxRetries: config.ObjStoreRetries, AccessKey: config.ObjStoreAccessKey,
		SecretKey: config.ObjStoreSecretKey, SessionToken: config.ObjStoreSessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("open replica object store: %w", err)
	}
	prefix := path.Join(config.ObjStorePrefix, config.ClusterID)
	r.checkpoints = checkpoint.NewManager(r.bucket, prefix, config.DataDir, 1)
	if err := r.checkpoints.Load(childCtx); err != nil {
		return nil, fmt.Errorf("load replica checkpoints: %w", err)
	}
	r.archive = recovery.NewManager(r.bucket, prefix, 1)
	if err := r.archive.Load(childCtx); err != nil {
		return nil, fmt.Errorf("load replica archive: %w", err)
	}
	voters := make([]quepaxa.Member, 0, len(config.Members))
	for _, member := range config.Members {
		voters = append(voters, quepaxa.Member{ID: member.ID, PeerURL: member.PeerURL})
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: voters}
	r.core, err = quepaxa.NewObserver(quepaxa.Config{NodeID: quepaxa.NodeID(config.ReplicaID), Cluster: cluster, WAL: r.wal})
	if err != nil {
		return nil, fmt.Errorf("open replica verifier: %w", err)
	}
	r.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return r.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if mode == ReplicaModeLearner {
		r.transport = network.NewLearnerTransport(types.ClusterID(config.ClusterID), quepaxa.NodeID(config.ReplicaID), cluster.ConfigID, config.Members, config.AdminToken)
		r.fetch = r.transport.FetchDecisions
	}
	if quepaxa.Slot(r.material.Tip()) > r.core.Tip() {
		return nil, fmt.Errorf("replica materialized slot %d is ahead of certified tip %d", r.material.Tip(), r.core.Tip())
	}
	objectErr := r.syncObjectStore(childCtx)
	if mode == ReplicaModeObjectStore {
		if objectErr != nil {
			return nil, objectErr
		}
	} else if peerErr := r.syncPeer(childCtx); peerErr != nil && objectErr != nil {
		return nil, errors.Join(objectErr, peerErr)
	}
	r.statusMu.Lock()
	r.status.AppliedSlot, r.status.LastSync = r.material.Tip(), time.Now()
	r.statusMu.Unlock()
	r.ready.Store(true)
	r.api = network.NewServer(r.core, r.material, types.ClusterID(config.ClusterID), false, nil, nil, 0, r.ready.Load)
	r.api.SetObjectStoreStats(func() (map[string]uint64, bool) { return objectStatsMap(r.bucket.Stats()), true })
	r.core.StartPeriodicSync(childCtx, time.Second)
	interval := config.SyncInterval
	if interval == 0 {
		if mode == ReplicaModeLearner {
			interval = 100 * time.Millisecond
		} else {
			interval = time.Second
		}
	}
	r.wg.Add(1)
	go r.run(childCtx, interval)
	return r, nil
}

func ensureReplicaIdentity(config ReplicaConfig) error {
	voters := make([]string, 0, len(config.Members))
	for _, member := range config.Members {
		voters = append(voters, fmt.Sprintf("%s:%x", member.ID, member.PublicKey))
	}
	slices.Sort(voters)
	directory := config.ObjStoreDir
	if directory != "" {
		var err error
		directory, err = filepath.Abs(directory)
		if err != nil {
			return err
		}
	}
	want := replicaIdentity{ClusterID: config.ClusterID, ConfigID: 1, ReplicaID: config.ReplicaID, Voters: voters,
		Provider: config.ObjStoreProvider, Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket,
		Directory: directory, Prefix: path.Clean(config.ObjStorePrefix)}
	manifest := filepath.Join(config.DataDir, "replica-identity.json")
	data, err := os.ReadFile(manifest)
	if err == nil {
		var got replicaIdentity
		if err := json.Unmarshal(data, &got); err != nil {
			return fmt.Errorf("decode replica identity: %w", err)
		}
		if !slices.Equal(got.Voters, want.Voters) || got.ClusterID != want.ClusterID || got.ConfigID != want.ConfigID ||
			got.ReplicaID != want.ReplicaID || got.Provider != want.Provider || got.Endpoint != want.Endpoint ||
			got.Bucket != want.Bucket || got.Directory != want.Directory || got.Prefix != want.Prefix {
			return fmt.Errorf("replica data directory identity mismatch")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(config.DataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != "qlog" {
			return fmt.Errorf("replica state exists without identity manifest")
		}
		qlogEntries, err := os.ReadDir(filepath.Join(config.DataDir, "qlog"))
		if err != nil {
			return err
		}
		for _, qlogEntry := range qlogEntries {
			if qlogEntry.Name() != "lock.qlog" {
				return fmt.Errorf("replica state exists without identity manifest")
			}
		}
	}
	data, err = json.MarshalIndent(want, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(config.DataDir, ".replica-identity-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, manifest); err != nil {
		return err
	}
	dir, err := os.Open(config.DataDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (r *ReadReplica) run(ctx context.Context, interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.Sync(ctx)
		}
	}
}

// Sync performs one bounded catch-up pass from the configured source.
func (r *ReadReplica) Sync(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if !r.ready.Load() {
		return ErrNotReady
	}
	var err error
	if r.mode == ReplicaModeLearner {
		err = r.syncPeer(ctx)
		if err != nil && ctx.Err() == nil {
			err = r.syncObjectStore(ctx)
		}
	} else {
		err = r.syncObjectStore(ctx)
	}
	r.statusMu.Lock()
	r.status.AppliedSlot = r.material.Tip()
	r.status.LastSync = time.Now()
	if err != nil {
		r.status.LastError = err.Error()
	} else {
		r.status.LastError = ""
	}
	r.statusMu.Unlock()
	return err
}

func (r *ReadReplica) syncPeer(ctx context.Context) error {
	if r.fetch == nil {
		return fmt.Errorf("learner peer transport is unavailable")
	}
	var firstErr error
	for offset := range len(r.config.Members) {
		index := (r.peerCursor + offset) % len(r.config.Members)
		member := r.config.Members[index]
		var target quepaxa.Slot
		targetSet := false
		for {
			response, err := r.fetch(ctx, member.ID, r.core.Tip()+1, 128)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
			if !targetSet {
				target, targetSet = response.Tip, true
			}
			if r.core.Tip() >= target {
				r.peerCursor = (index + 1) % len(r.config.Members)
				r.setSource("peer:"+string(member.ID), uint64(target))
				return r.applyCertified(ctx)
			}
			if len(response.Decisions) == 0 {
				if firstErr == nil {
					firstErr = fmt.Errorf("peer %s omitted slot %d", member.ID, r.core.Tip()+1)
				}
				break
			}
			values := response.Decisions
			for i, value := range values {
				if value.Slot > target {
					values = values[:i]
					break
				}
			}
			before := r.core.Tip()
			if err := r.core.AcceptCertifiedValues(values); err != nil {
				return err
			}
			if r.core.Tip() <= before {
				return fmt.Errorf("peer %s made no progress from slot %d", member.ID, before)
			}
			if err := r.applyCertified(ctx); err != nil {
				return err
			}
			if r.core.Tip() >= target {
				r.peerCursor = (index + 1) % len(r.config.Members)
				r.setSource("peer:"+string(member.ID), uint64(target))
				return nil
			}
		}
	}
	if firstErr == nil {
		firstErr = quepaxa.ErrQuorumUnavailable
	}
	return firstErr
}

func (r *ReadReplica) syncObjectStore(ctx context.Context) (resultErr error) {
	if err := r.archive.Load(ctx); err != nil {
		return err
	}
	seal, baseDecision, hasBase := r.archive.RecoveryBase()
	if !hasBase {
		if r.archive.Tip() < r.core.Tip() {
			return fmt.Errorf("object-store archive tip %d is behind replica tip %d", r.archive.Tip(), r.core.Tip())
		}
		if err := r.acceptArchive(ctx, r.archive.DecisionsFrom, r.archive.Tip()); err != nil {
			return err
		}
		r.setSource("object-store", uint64(r.archive.Tip()))
		return r.applyCertified(ctx)
	}
	if floor := r.core.CompactionFloor(); seal.Index < floor {
		return fmt.Errorf("object-store checkpoint base %d regressed behind replica floor %d", seal.Index, floor)
	}
	owner, err := replicaOwner(r.config.ReplicaID)
	if err != nil {
		return err
	}
	snapshot, err := r.archive.BeginRecoverySnapshot(ctx, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer closeRecoverySnapshot(snapshot)
	seal, baseDecision, hasBase = snapshot.RecoveryBase()
	if !hasBase {
		return fmt.Errorf("recovery snapshot omitted checkpoint base")
	}
	if floor := r.core.CompactionFloor(); seal.Index < floor {
		return fmt.Errorf("recovery snapshot checkpoint base %d regressed behind replica floor %d", seal.Index, floor)
	}
	var checkpointPin *checkpoint.RecoveryPin
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.core.Tip() < seal.Index || r.material.Tip() < uint64(seal.Index) {
		root, err := r.checkpoints.OpenRoot(workCtx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return err
		}
		if root.Hash != seal.StateHash {
			return fmt.Errorf("certified checkpoint state hash mismatch")
		}
		checkpointPin, err = r.checkpoints.PinRecoveryRoot(workCtx, root, owner, 2*time.Minute)
		if err != nil {
			return err
		}
		defer closeCheckpointPin(checkpointPin)
	}
	renewDone := make(chan error, 1)
	go renewReplicaPins(workCtx, cancel, snapshot, checkpointPin, renewDone)
	defer func() {
		cancel()
		resultErr = errors.Join(resultErr, <-renewDone)
	}()
	if checkpointPin != nil {
		root, err := checkpointPin.Root()
		if err != nil {
			return err
		}
		dir, err := os.MkdirTemp(r.config.DataDir, ".rhiza-replica-restore-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		files, err := r.checkpoints.DownloadAndVerifyRootFiles(workCtx, root, dir)
		if err != nil {
			return err
		}
		if r.core.Tip() < seal.Index {
			if err := r.core.RestoreCheckpointBase(workCtx, seal, baseDecision); err != nil {
				return err
			}
		} else if err := r.core.ValidateCheckpointBase(workCtx, seal, baseDecision); err != nil {
			return err
		}
		if r.material.Tip() < uint64(seal.Index) {
			materialFiles := make([]materializer.CheckpointFile, 0, len(files))
			for _, file := range files {
				materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			if err := r.material.RestoreCheckpoint(workCtx, materialFiles); err != nil {
				return err
			}
		}
	} else if err := r.core.ValidateCheckpointBase(workCtx, seal, baseDecision); err != nil {
		return err
	}
	if err := r.acceptArchive(workCtx, snapshot.DecisionsFrom, snapshot.Tip()); err != nil {
		return err
	}
	if err := r.applyCertified(workCtx); err != nil {
		return err
	}
	if r.core.CompactionFloor() < seal.Index && r.core.Tip() >= baseDecision.Slot {
		if err := r.core.PrepareCheckpoint(workCtx, seal); err != nil {
			return err
		}
		if err := r.core.CompactThrough(seal.Index, seal.RootHash); err != nil {
			return err
		}
	}
	r.setSource("object-store", uint64(snapshot.Tip()))
	return nil
}

type archiveReader func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error)

func (r *ReadReplica) acceptArchive(ctx context.Context, read archiveReader, tip quepaxa.Slot) error {
	for r.core.Tip() < tip {
		values, _, err := read(ctx, r.core.Tip()+1, 256)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return fmt.Errorf("shared archive omitted slot %d", r.core.Tip()+1)
		}
		if err := r.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReadReplica) applyCertified(ctx context.Context) error {
	for quepaxa.Slot(r.material.Tip()) < r.core.Tip() {
		values, _, err := r.core.DecisionsFrom(quepaxa.Slot(r.material.Tip())+1, 256)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return fmt.Errorf("replica decision gap at slot %d", r.material.Tip()+1)
		}
		if err := r.material.ApplyBatch(ctx, values); err != nil {
			return err
		}
	}
	return nil
}

func replicaOwner(id string) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("replica-%s-%x", id, nonce), nil
}

func renewReplicaPins(ctx context.Context, cancel context.CancelFunc, snapshot *recovery.RecoverySnapshot, pin *checkpoint.RecoveryPin, done chan<- error) {
	ticker := time.NewTicker(40 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := snapshot.Renew(ctx, 2*time.Minute); err != nil {
				cancel()
				done <- err
				return
			}
			if pin != nil {
				if err := pin.Renew(ctx, 2*time.Minute); err != nil {
					cancel()
					done <- err
					return
				}
			}
		}
	}
}

func closeRecoverySnapshot(snapshot *recovery.RecoverySnapshot) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = snapshot.Close(ctx)
}

func closeCheckpointPin(pin *checkpoint.RecoveryPin) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pin.Close(ctx)
}

func (r *ReadReplica) setSource(source string, tip uint64) {
	r.statusMu.Lock()
	r.status.Source, r.status.SourceTip = source, tip
	r.statusMu.Unlock()
}

func objectStatsMap(stats objstore.Stats) map[string]uint64 {
	return map[string]uint64{
		"uploads": stats.Uploads, "gets": stats.Gets, "lists": stats.Lists, "heads": stats.Heads,
		"deletes": stats.Deletes, "failures": stats.Failures, "bytes_uploaded": stats.BytesUploaded,
		"bytes_downloaded": stats.BytesDownloaded, "s3_http_requests": stats.S3HTTPRequests,
		"s3_http_failures": stats.S3HTTPFailures, "condition_conflicts": stats.ConditionConflicts,
		"dedup_hits": stats.DedupHits, "sdk_retries": stats.SDKRetries,
		"transport_failures": stats.TransportFailures, "http_4xx_unexpected": stats.Unexpected4xx,
		"http_5xx": stats.HTTP5xx,
	}
}

func (r *ReadReplica) Ready() bool { return r.ready.Load() }

func (r *ReadReplica) Status() ReplicaStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	status := r.status
	status.AppliedSlot = r.material.Tip()
	return status
}

func (r *ReadReplica) Handler() http.Handler                              { return r.api }
func (r *ReadReplica) ServeHTTP(w http.ResponseWriter, req *http.Request) { r.api.ServeHTTP(w, req) }
func (r *ReadReplica) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	return r.api.Query(ctx, req)
}
func (r *ReadReplica) KVGet(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	return r.api.KVGet(ctx, req)
}
func (r *ReadReplica) GraphQuery(ctx context.Context, req GraphQueryRequest) (GraphResult, error) {
	return r.api.GraphQuery(ctx, req)
}
func (r *ReadReplica) GraphChanges(ctx context.Context, req GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	return r.api.GraphChanges(ctx, req)
}
func (r *ReadReplica) GraphStreamRead(ctx context.Context, req GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	return r.api.GraphStreamRead(ctx, req)
}
func (r *ReadReplica) GraphStreamOffset(ctx context.Context, req GraphStreamOffsetRequest) (GraphStreamOffsetResponse, error) {
	return r.api.GraphStreamOffset(ctx, req)
}
func (r *ReadReplica) RequestStatus(ctx context.Context, req RequestStatusRequest) (RequestStatusResponse, error) {
	return r.api.RequestStatus(ctx, req)
}
func (r *ReadReplica) ObjectStoreStats() ObjectStoreStats { return r.bucket.Stats() }

func (r *ReadReplica) Close() error {
	r.closeOnce.Do(func() {
		r.ready.Store(false)
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
		r.syncMu.Lock()
		defer r.syncMu.Unlock()
		if r.api != nil {
			r.api.Close()
		}
		if r.archive != nil {
			r.archive.Close()
		}
		if r.transport != nil {
			r.closeErr = errors.Join(r.closeErr, r.transport.Close())
		}
		if r.core != nil {
			r.core.StopPeriodicSync()
		}
		if r.wal != nil {
			r.closeErr = errors.Join(r.closeErr, r.wal.Sync(), r.wal.Close())
		}
		if r.material != nil {
			r.closeErr = errors.Join(r.closeErr, r.material.Close())
		}
		if r.lock != nil {
			r.closeErr = errors.Join(r.closeErr, r.lock.Release())
		}
		if r.bucket != nil {
			r.closeErr = errors.Join(r.closeErr, r.bucket.Close())
		}
	})
	return r.closeErr
}
