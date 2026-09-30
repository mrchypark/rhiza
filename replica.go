package rhiza

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
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
	Members      []ReplicaMember
	SyncInterval time.Duration
	// Both zero use 64 concurrent reads / 8 long-poll reads. With an explicit
	// total, zero MaxLongPollReads disables waiting stream reads.
	MaxConcurrentReads int
	MaxLongPollReads   int

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
}

type ReplicaStatus struct {
	Mode        ReplicaMode
	AppliedSlot uint64
	SourceTip   uint64
	LagSlots    uint64
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
	Account   string   `json:"account,omitempty"`
}

var (
	learnerCheckpointLease = 2 * time.Minute
	learnerCheckpointRenew = 40 * time.Second
	learnerCheckpointProbe = 30 * time.Second
)

type replicaFailure struct{ err error }

type learnerCheckpointCandidate struct {
	seal     quepaxa.CheckpointSeal
	decision quepaxa.DecidedValue
	manager  *checkpoint.Manager
}

type learnerCheckpointResult struct {
	candidate *learnerCheckpointCandidate
	err       error
}

type learnerCheckpointJob struct {
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	result        chan learnerCheckpointResult
	finish        chan struct{}
	stopRenew     chan struct{}
	renewDone     chan struct{}
	stopRenewOnce sync.Once
	candidate     *learnerCheckpointCandidate

	mu         sync.Mutex
	phase      uint8
	leaseLost  bool
	leaseErr   error
	deadline   time.Time
	cleanupErr error
}

const (
	learnerCheckpointVerifying uint8 = iota
	learnerCheckpointPreparing
	learnerCheckpointCompacting
)

// ReadReplica is an eventual, read-only copy. It never proposes, votes,
// acknowledges decisions, or participates in quorum/read-index operations.
type ReadReplica struct {
	mode                    ReplicaMode
	config                  ReplicaConfig
	core                    *quepaxa.Core
	material                *materializer.Materializer
	api                     *network.Server
	wal                     *qlog.WAL
	lock                    *qlog.LockFile
	bucket                  *objstore.MeteredBucket
	checkpoints             *checkpoint.Manager
	archive                 *recovery.Manager
	transport               *network.Transport
	fetch                   func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error)
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	syncMu                  sync.Mutex
	statusMu                sync.RWMutex
	status                  ReplicaStatus
	ready                   atomic.Bool
	closeOnce               sync.Once
	closeErr                error
	peerCursor              int
	pinOwner                string
	syncedHead              *thanosobjstore.ObjectVersion
	checkpointJob           *learnerCheckpointJob
	nextCheckpointProbe     time.Time
	checkpointProbeFailures int
	checkpointError         error
	checkpointFailure       atomic.Pointer[replicaFailure]
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
	if config.MaxConcurrentReads < 0 || config.MaxLongPollReads < 0 || config.MaxLongPollReads > config.MaxConcurrentReads {
		return nil, fmt.Errorf("read admission requires 0 <= max long-poll reads <= max concurrent reads")
	}
	if config.DataDir == "" || config.ReplicaID == "" {
		return nil, fmt.Errorf("replica ID and data directory are required")
	}
	if err := sqlpolicy.CheckExisting(ctx, path.Join(config.DataDir, "sqlite.db")); err != nil {
		return nil, err
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
	bucketConfig := objstore.Config{
		Provider: objstore.Provider(config.ObjStoreProvider), FilesystemDir: config.ObjStoreDir,
		Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Region: config.ObjStoreRegion,
		Insecure: config.ObjStoreInsecure, MaxRetries: config.ObjStoreRetries, AccessKey: config.ObjStoreAccessKey,
		SecretKey: config.ObjStoreSecretKey, SessionToken: config.ObjStoreSessionToken,
		ServiceAccount: config.ObjStoreServiceAccount, AzureTenantID: config.ObjStoreAzureTenantID,
		AzureClientID: config.ObjStoreAzureClientID, AzureClientSecret: config.ObjStoreAzureClientSecret,
		AzureStorageAccount: config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
		AzureConnectionString: config.ObjStoreAzureConnectionString, AzureUserAssignedID: config.ObjStoreAzureUserAssignedID,
	}
	if err := objstore.ValidateConfig(bucketConfig); err != nil {
		return nil, fmt.Errorf("invalid replica object store: %w", err)
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
	r.bucket, err = objstore.NewBucket(bucketConfig)
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
	if errors.Is(objectErr, sqlpolicy.ErrIncompatible) {
		return nil, objectErr
	}
	if mode == ReplicaModeObjectStore {
		if objectErr != nil {
			return nil, objectErr
		}
	} else if peerErr := r.syncPeer(childCtx); errors.Is(peerErr, sqlpolicy.ErrIncompatible) || (peerErr != nil && objectErr != nil) {
		return nil, errors.Join(objectErr, peerErr)
	}
	r.statusMu.Lock()
	r.status.AppliedSlot, r.status.LastSync = r.material.Tip(), time.Now()
	r.statusMu.Unlock()
	r.ready.Store(true)
	if mode == ReplicaModeLearner {
		r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
	}
	r.api = network.NewServer(r.core, r.material, types.ClusterID(config.ClusterID), false, nil, r.ready.Load)
	if config.MaxConcurrentReads != 0 {
		if err := r.api.SetReadAdmissionLimits(network.ReadAdmissionLimits{
			MaxConcurrent: config.MaxConcurrentReads, MaxLongPoll: config.MaxLongPollReads,
		}); err != nil {
			return nil, err
		}
	}
	r.api.SetObjectStoreStats(func() (map[string]uint64, bool) { return objectStatsMap(r.bucket.Stats()), true })
	r.api.SetReplicaStatus(func() network.ReplicaStatus {
		status := r.Status()
		lag := uint64(0)
		if status.SourceTip > status.AppliedSlot {
			lag = status.SourceTip - status.AppliedSlot
		}
		return network.ReplicaStatus{Mode: string(status.Mode), AppliedSlot: status.AppliedSlot, SourceTip: status.SourceTip,
			LagSlots: lag, Source: status.Source, LastSync: status.LastSync, LastError: status.LastError}
	})
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
		Directory: directory, Prefix: path.Clean(config.ObjStorePrefix), Account: config.ObjStoreAzureStorageAccount}
	manifest := filepath.Join(config.DataDir, "replica-identity.json")
	data, err := os.ReadFile(manifest)
	if err == nil {
		var got replicaIdentity
		if err := json.Unmarshal(data, &got); err != nil {
			return fmt.Errorf("decode replica identity: %w", err)
		}
		if !slices.Equal(got.Voters, want.Voters) || got.ClusterID != want.ClusterID || got.ConfigID != want.ConfigID ||
			got.ReplicaID != want.ReplicaID || got.Provider != want.Provider || got.Endpoint != want.Endpoint ||
			got.Bucket != want.Bucket || got.Directory != want.Directory || got.Prefix != want.Prefix || got.Account != want.Account {
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
		if failure := r.checkpointFailure.Load(); failure != nil {
			return failure.err
		}
		return ErrNotReady
	}
	var err error
	if r.mode == ReplicaModeLearner {
		err = r.syncPeer(ctx)
		if err == nil {
			err = r.advanceLearnerCheckpoint(ctx)
		} else if !errors.Is(err, sqlpolicy.ErrIncompatible) && ctx.Err() == nil {
			if failure := r.checkpointFailure.Load(); failure != nil {
				err = failure.err
			} else {
				r.stopLearnerCheckpointJob()
				if failure := r.checkpointFailure.Load(); failure != nil {
					err = failure.err
				} else {
					err = r.syncObjectStore(ctx)
				}
			}
		}
	} else {
		err = r.syncObjectStore(ctx)
	}
	r.statusMu.Lock()
	r.status.AppliedSlot = r.material.Tip()
	r.status.LastSync = time.Now()
	if err != nil {
		r.status.LastError = err.Error()
	} else if r.checkpointError != nil {
		r.status.LastError = r.checkpointError.Error()
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
			for _, decision := range values {
				if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
					return err
				}
			}
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

func (r *ReadReplica) advanceLearnerCheckpoint(ctx context.Context) error {
	if failure := r.checkpointFailure.Load(); failure != nil {
		return failure.err
	}
	if r.checkpointJob == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Before(r.nextCheckpointProbe) {
			return nil
		}
		r.startLearnerCheckpointProbe()
		return nil
	}
	job := r.checkpointJob
	if job.candidate == nil {
		select {
		case result := <-job.result:
			if result.err != nil {
				r.checkpointProbeFailed(result.err)
				r.stopLearnerCheckpointJob()
				return nil
			}
			if result.candidate == nil {
				r.checkpointProbeFailures = 0
				r.checkpointError = nil
				r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
				r.stopLearnerCheckpointJob()
				return nil
			}
			job.candidate = result.candidate
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return r.finishLearnerCheckpoint(ctx, job)
}

func (r *ReadReplica) startLearnerCheckpointProbe() {
	jobCtx, cancel := context.WithCancel(r.ctx)
	job := &learnerCheckpointJob{
		ctx: jobCtx, cancel: cancel, done: make(chan struct{}), result: make(chan learnerCheckpointResult, 1), finish: make(chan struct{}),
	}
	r.checkpointJob = job
	r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
	go r.verifyLearnerCheckpoint(jobCtx, job)
}

func (r *ReadReplica) verifyLearnerCheckpoint(ctx context.Context, job *learnerCheckpointJob) {
	defer close(job.done)
	owner, err := replicaOwner(r.config.ReplicaID)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	snapshot, err := r.archive.BeginRecoverySnapshot(ctx, owner, learnerCheckpointLease)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	seal, decision, ok := snapshot.RecoveryBase()
	if !ok {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published archive has no recovery checkpoint base")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	if seal.Index == 0 || decision.Slot <= seal.Index {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published recovery base has invalid seal decision slot")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	seal = cloneReplicaSeal(seal)
	decision = cloneReplicaDecision(decision)
	manager := checkpoint.NewManager(r.bucket, path.Join(r.config.ObjStorePrefix, r.config.ClusterID), "", seal.ConfigID)
	root, err := manager.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	if root.Index != uint64(seal.Index) || root.ConfigID != seal.ConfigID || root.Hash != seal.StateHash {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published recovery root does not match its certified seal")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	owner, err = replicaOwner(r.config.ReplicaID)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	leaseStart := time.Now()
	pin, err := manager.PinRecoveryRoot(ctx, root, owner, learnerCheckpointLease)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	job.mu.Lock()
	job.deadline = leaseStart.Add(learnerCheckpointLease)
	job.mu.Unlock()
	archiveCloseErr := closeRecoverySnapshot(snapshot)
	renewDone := make(chan struct{})
	job.stopRenew, job.renewDone = make(chan struct{}), renewDone
	go r.renewLearnerCheckpointPin(ctx, job, pin, renewDone)
	closePin := func() {
		job.cancel()
		r.haltLearnerCheckpointRenewal(job)
		job.cleanupErr = errors.Join(job.cleanupErr, closeCheckpointPin(pin), archiveCloseErr)
	}
	if archiveCloseErr != nil {
		closePin()
		job.result <- learnerCheckpointResult{err: archiveCloseErr}
		return
	}
	if err := manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash); err != nil {
		closePin()
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	candidate := &learnerCheckpointCandidate{seal: seal, decision: cloneReplicaDecision(decision), manager: manager}
	job.result <- learnerCheckpointResult{candidate: candidate}
	select {
	case <-job.finish:
	case <-ctx.Done():
		job.mu.Lock()
		compacting := job.phase == learnerCheckpointCompacting
		job.mu.Unlock()
		if compacting {
			<-job.finish
		}
	}
	closePin()
}

func (r *ReadReplica) renewLearnerCheckpointPin(ctx context.Context, job *learnerCheckpointJob, pin *checkpoint.RecoveryPin, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(learnerCheckpointRenew)
	defer ticker.Stop()
	ctxDone := ctx.Done()
	for {
		select {
		case <-ctxDone:
			job.mu.Lock()
			compacting := job.phase == learnerCheckpointCompacting
			job.mu.Unlock()
			if !compacting {
				return
			}
			ctxDone = nil
		case <-job.stopRenew:
			return
		case <-ticker.C:
			started := time.Now()
			localtesthooks.Hit("learner:checkpoint-pin-renewal-started")
			err := pin.Renew(ctx, learnerCheckpointLease)
			job.mu.Lock()
			if err == nil && started.Before(job.deadline) && time.Now().Before(job.deadline) {
				job.deadline = started.Add(learnerCheckpointLease)
				job.mu.Unlock()
				continue
			}
			if err == nil {
				err = fmt.Errorf("checkpoint recovery pin renewal completed after the confirmed lease deadline")
			}
			localtesthooks.Hit("learner:checkpoint-pin-renewal-failed")
			job.leaseLost = true
			job.leaseErr = err
			phase := job.phase
			job.mu.Unlock()
			if phase == learnerCheckpointCompacting {
				r.latchLearnerCheckpointFailure(fmt.Errorf("checkpoint pin lost during WAL compaction: %w", err))
			}
			if phase != learnerCheckpointCompacting {
				job.cancel()
			}
			return
		}
	}
}

func (r *ReadReplica) haltLearnerCheckpointRenewal(job *learnerCheckpointJob) {
	if job.renewDone == nil {
		return
	}
	job.stopRenewOnce.Do(func() { close(job.stopRenew) })
	<-job.renewDone
}

func cloneReplicaSeal(seal quepaxa.CheckpointSeal) quepaxa.CheckpointSeal {
	seal.NextLeaderOrder = slices.Clone(seal.NextLeaderOrder)
	seal.FollowingLeaderOrder = slices.Clone(seal.FollowingLeaderOrder)
	if seal.Membership != nil {
		membership := *seal.Membership
		membership.Genesis.Members = slices.Clone(membership.Genesis.Members)
		membership.Transitions = slices.Clone(membership.Transitions)
		for i := range membership.Transitions {
			membership.Transitions[i].Freeze = cloneReplicaDecision(membership.Transitions[i].Freeze)
			membership.Transitions[i].Terminal = cloneReplicaDecision(membership.Transitions[i].Terminal)
		}
		if membership.Abort != nil {
			abort := *membership.Abort
			abort.Freeze = cloneReplicaDecision(abort.Freeze)
			abort.Terminal = cloneReplicaDecision(abort.Terminal)
			membership.Abort = &abort
		}
		seal.Membership = &membership
	}
	return seal
}

func cloneReplicaDecision(value quepaxa.DecidedValue) quepaxa.DecidedValue {
	value.Value = append([]byte(nil), value.Value...)
	value.Certificate = append(value.Certificate[:0:0], value.Certificate...)
	return value
}

func (r *ReadReplica) finishLearnerCheckpoint(ctx context.Context, job *learnerCheckpointJob) error {
	candidate := job.candidate
	if candidate == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	job.mu.Lock()
	leaseErr := job.leaseErr
	leaseOK := !job.leaseLost && time.Now().Before(job.deadline)
	job.mu.Unlock()
	if !leaseOK {
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired before adoption")
		}
		r.checkpointProbeFailed(leaseErr)
		r.stopLearnerCheckpointJob()
		return nil
	}
	if r.core.Tip() < candidate.decision.Slot || quepaxa.Slot(r.material.Tip()) < candidate.decision.Slot {
		return nil // The seal is certified remotely but not yet applied locally.
	}
	if err := r.archive.Load(ctx); err != nil {
		r.checkpointError = err
		return nil
	}
	currentSeal, _, hasCurrent := r.archive.RecoveryBase()
	if !hasCurrent || currentSeal.ConfigID != candidate.seal.ConfigID || currentSeal.Index < candidate.seal.Index ||
		(currentSeal.Index == candidate.seal.Index && currentSeal.RootHash != candidate.seal.RootHash) {
		r.checkpointProbeFailed(fmt.Errorf("published archive recovery base no longer includes the pinned candidate"))
		r.stopLearnerCheckpointJob()
		return nil
	}
	if candidate.seal.Index <= r.core.CompactionFloor() {
		r.checkpointProbeFailures = 0
		r.checkpointError = nil
		r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
		r.stopLearnerCheckpointJob()
		return nil
	}
	localDecision, exists := r.core.CertifiedValue(candidate.decision.Slot)
	if !exists || localDecision.Hash != candidate.decision.Hash || !slices.Equal(localDecision.Value, candidate.decision.Value) ||
		!slices.Equal(localDecision.Certificate, candidate.decision.Certificate) {
		r.checkpointProbeFailed(fmt.Errorf("checkpoint seal decision is not the locally applied certified value"))
		r.stopLearnerCheckpointJob()
		return nil
	}
	previousManager := r.checkpoints
	r.checkpoints = candidate.manager
	defer func() { r.checkpoints = previousManager }()
	if err := r.core.ValidateCheckpointBase(job.ctx, candidate.seal, candidate.decision); err != nil {
		r.checkpointProbeFailed(err)
		r.stopLearnerCheckpointJob()
		return nil
	}
	job.mu.Lock()
	if job.leaseLost || !time.Now().Before(job.deadline) {
		leaseErr := job.leaseErr
		job.mu.Unlock()
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired before prepare")
		}
		r.checkpointProbeFailed(leaseErr)
		r.stopLearnerCheckpointJob()
		return nil
	}
	job.phase = learnerCheckpointPreparing
	job.mu.Unlock()
	if err := r.core.PrepareCheckpoint(job.ctx, candidate.seal); err != nil {
		failure := fmt.Errorf("checkpoint prepare outcome requires reopen: %w", err)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	job.mu.Lock()
	if job.leaseLost || !time.Now().Before(job.deadline) {
		leaseErr := job.leaseErr
		job.mu.Unlock()
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired after prepare")
		}
		failure := fmt.Errorf("checkpoint prepared but pin was lost before compaction; reopen required: %w", leaseErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	job.phase = learnerCheckpointCompacting
	job.mu.Unlock()
	compactErr := r.core.CompactThrough(candidate.seal.Index, candidate.seal.RootHash)
	localtesthooks.Hit("learner:checkpoint-compaction-finished")
	r.haltLearnerCheckpointRenewal(job)
	job.mu.Lock()
	leaseLost, leaseErr := job.leaseLost, job.leaseErr
	job.phase = learnerCheckpointVerifying
	job.mu.Unlock()
	if compactErr != nil {
		failure := fmt.Errorf("checkpoint compaction outcome requires reopen: %w", compactErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	r.syncedHead = nil
	if leaseLost {
		failure := fmt.Errorf("checkpoint floor committed after pin lease loss; reopen required: %w", leaseErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	r.checkpointProbeFailures = 0
	r.checkpointError = nil
	r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
	r.stopLearnerCheckpointJob()
	return nil
}

func (r *ReadReplica) checkpointProbeFailed(err error) {
	r.checkpointError = err
	r.checkpointProbeFailures++
	delays := [...]time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second}
	index := r.checkpointProbeFailures - 1
	if index >= len(delays) {
		index = len(delays) - 1
	}
	r.nextCheckpointProbe = time.Now().Add(delays[index])
}

func (r *ReadReplica) stopLearnerCheckpointJob() error {
	job := r.checkpointJob
	if job == nil {
		return nil
	}
	r.checkpointJob = nil
	job.cancel()
	close(job.finish)
	<-job.done
	if job.cleanupErr != nil {
		r.checkpointError = job.cleanupErr
	}
	return job.cleanupErr
}

func (r *ReadReplica) latchLearnerCheckpointFailure(err error) {
	if err == nil {
		return
	}
	r.checkpointFailure.CompareAndSwap(nil, &replicaFailure{err: err})
	r.ready.Store(false)
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
	version, versioned := r.archive.HeadVersion()
	// The previous successful pass verified this exact history. No object data
	// is consumed here, so there is nothing to protect from GC with a pin.
	if versioned && r.syncedHead != nil && *r.syncedHead == version &&
		r.core.Tip() == r.archive.Tip() && r.material.Tip() == uint64(r.core.Tip()) &&
		r.core.CompactionFloor() == seal.Index {
		r.setSource("object-store", uint64(r.archive.Tip()))
		return nil
	}
	r.syncedHead = nil
	if r.pinOwner == "" {
		var err error
		r.pinOwner, err = replicaOwner(r.config.ReplicaID)
		if err != nil {
			return err
		}
	}
	owner := r.pinOwner
	// Run after pin cleanup and renewal have completed. A failed release must
	// not allow reuse of an owner whose previous lease may still be active.
	defer func() {
		if resultErr != nil {
			r.pinOwner = ""
		} else if versioned {
			r.syncedHead = &version
		}
	}()
	snapshot, err := r.archive.BeginRecoverySnapshot(ctx, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeRecoverySnapshot(snapshot)) }()
	version, versioned = r.archive.HeadVersion()
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
		defer func() { resultErr = errors.Join(resultErr, closeCheckpointPin(checkpointPin)) }()
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
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
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

func closeRecoverySnapshot(snapshot *recovery.RecoverySnapshot) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return snapshot.Close(ctx)
}

func closeCheckpointPin(pin *checkpoint.RecoveryPin) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pin.Close(ctx)
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
		"http_5xx":      stats.HTTP5xx,
		"http_requests": stats.HTTPRequests, "http_failures": stats.HTTPFailures,
		"http_get_requests": stats.HTTPGetRequests, "http_put_requests": stats.HTTPPutRequests,
		"http_head_requests": stats.HTTPHeadRequests, "http_delete_requests": stats.HTTPDeleteRequests,
		"http_other_requests": stats.HTTPOtherRequests,
	}
}

func (r *ReadReplica) Ready() bool { return r.ready.Load() }

func (r *ReadReplica) Status() ReplicaStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	status := r.status
	status.AppliedSlot = r.material.Tip()
	if status.SourceTip > status.AppliedSlot {
		status.LagSlots = status.SourceTip - status.AppliedSlot
	}
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
func (r *ReadReplica) GraphReachable(ctx context.Context, req GraphReachableRequest) (GraphReachableResult, error) {
	return r.api.GraphReachable(ctx, req)
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
		if err := r.stopLearnerCheckpointJob(); err != nil {
			r.closeErr = errors.Join(r.closeErr, err)
		}
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
