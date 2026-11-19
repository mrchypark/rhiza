package node

import (
	"context"
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

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Node is the main runtime container.
type Node struct {
	config                       *types.ExecutionConfig
	core                         *quepaxa.Core
	material                     *materializer.Materializer
	server                       *network.Server
	peerControl                  *network.Server
	peer                         *network.PeerServer
	transport                    *network.Transport
	catchUp                      *network.Transport
	wal                          *qlog.WAL
	lock                         *qlog.LockFile
	bucket                       *objectstore.MeteredBucket
	checkpoints                  *checkpoint.Manager
	archive                      *recovery.Manager
	checkpointer                 *checkpoint.AutoCheckpointer
	localStore                   *localCheckpointIdentity
	localBackup                  *recovery.LocalArchive
	localOwner                   *objectstore.LocalFilesystemOwner
	localBackupReady             bool
	localBackupNeedsSnapshot     bool
	localGCContext               context.Context
	localGCCancel                context.CancelFunc
	localSnapshotMu              sync.Mutex
	localBackupRestorePins       *localRestorePins
	localRoot                    *localCheckpointDescriptor
	localRestoreSuffixPending    bool
	recoveryPhases               nodeRecoveryPhases
	ready                        atomic.Bool
	membershipMu                 sync.Mutex
	opened                       atomic.Bool
	cancel                       context.CancelFunc
	wg                           sync.WaitGroup
	recoveryMu                   sync.Mutex
	replayMu                     sync.Mutex
	compactionMu                 sync.Mutex
	catchUpWake                  chan struct{}
	shutdownMu                   sync.Mutex
	terminalMu                   sync.Mutex
	terminalIdentity             network.TerminalDrainRequest
	terminalRequested            atomic.Bool
	terminalDone                 chan struct{}
	terminalResult               TerminalDrainResult
	terminalErr                  error
	fenceSession                 *fencing.Session
	preparedStartup              *PreparedStartupConfig
	managedStartup               *ManagedStartupConfig
	managedAdmitted              atomic.Bool
	managedInitiallyAdmitted     atomic.Bool
	managedGraphPublisherStarted atomic.Bool
	managedGraphPublisherBinding atomic.Pointer[internalApplicationBinding]
	managedGraphInitialRootDone  chan struct{}
	managedGraphInitialRootOnce  sync.Once
	managedGraphControlMu        sync.Mutex
	managedGraphControl          *managedGraphControlOperation
	managedPeerVerified          atomic.Bool
	internalApplication          atomic.Pointer[internalApplicationBinding]
}

type managedGraphControlOperation struct {
	value   []byte
	hash    quepaxa.ValueHash
	binding *internalApplicationBinding
	done    chan struct{}
	slot    quepaxa.Slot
	err     error
}

// nodeRecoveryPhases records the selected certified checkpoint restore path.
// It is written synchronously during open and observed after Open returns.
type nodeRecoveryPhases struct {
	selectedCheckpoint               bool
	checkpointIndex                  uint64
	checkpointRootHash               [32]byte
	checkpointDownloadVerify         time.Duration
	checkpointDownloadVerifyStarted  bool
	checkpointDownloadVerifyComplete bool
	materializerRestore              time.Duration
	materializerRestoreStarted       bool
	materializerRestoreComplete      bool
}

// New creates a new Node.
func New(config *types.ExecutionConfig) *Node {
	return &Node{
		managedGraphInitialRootDone: make(chan struct{}),
		config:                      config,
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

func validateReadAdmissionConfig(config *types.ExecutionConfig) error {
	if config.MaxConcurrentReads < 0 || config.MaxLongPollReads < 0 {
		return fmt.Errorf("read admission limits must not be negative")
	}
	if config.MaxConcurrentReads == 0 && config.MaxLongPollReads == 0 {
		return nil
	}
	if config.MaxConcurrentReads == 0 || config.MaxLongPollReads > config.MaxConcurrentReads {
		return fmt.Errorf("max long-poll reads must not exceed max concurrent reads")
	}
	return nil
}

// validateVoterMembership mirrors Core's fixed-membership checks before
// offline enrollment can write a remote registration.
func validateVoterMembership(config *types.ExecutionConfig) error {
	if config.EnableReconfiguration && (len(config.Members) < 2 || config.AdminToken == "") {
		return fmt.Errorf("managed membership requires bootstrap voters and an admin token")
	}
	if config.Learner != nil && len(config.Members) == 0 {
		return fmt.Errorf("learner requires bootstrap members")
	}
	if len(config.Members) == 0 {
		return nil // loadClusterConfig supplies the single-node default later.
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(config.Members))
	local := false
	for _, member := range config.Members {
		if member.ID == "" {
			return fmt.Errorf("voter member ID is required")
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("duplicate voter member %q", member.ID)
		}
		seen[member.ID] = struct{}{}
		local = local || member.ID == config.NodeID
	}
	if config.Learner != nil {
		if !config.EnableReconfiguration || local || config.Learner.ID != config.NodeID || config.Learner.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("learner requires reconfiguration, a new local ID, and its own public key")
		}
	} else if !local {
		return fmt.Errorf("local node %q is not a voter member", config.NodeID)
	}
	return nil
}

// Open starts the embedded engine and its private peer transport without serving HTTP.
func (n *Node) Open(ctx context.Context) error {
	if n.preparedStartup != nil {
		return n.openPrepared(ctx, *n.preparedStartup)
	}
	return n.open(ctx, false)
}

func managedMaterializerFilesAbsent(dataDir string) (bool, error) {
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "sqlite.db-journal", "latticedb"} {
		_, err := os.Lstat(filepath.Join(dataDir, name))
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

// EnrollExistingVoter registers a pre-registration WAL while all voters are
// offline. It neither restores missing voting state nor starts any listeners.
func (n *Node) EnrollExistingVoter(ctx context.Context) error {
	return n.open(ctx, true)
}

func (n *Node) open(ctx context.Context, enroll bool) error {
	return n.openInternal(ctx, enroll, nil, false)
}

func (n *Node) openWithWALLock(ctx context.Context, enroll, cleanStart bool, lock *qlog.LockFile) (err error) {
	if lock == nil {
		return fmt.Errorf("preacquired WAL lock is required")
	}
	defer func() {
		if n.lock != lock {
			err = errors.Join(err, lock.Release())
		}
	}()
	return n.openInternal(ctx, enroll, lock, cleanStart)
}

func refusePreparedUncertifiedRecorder(core *quepaxa.Core) error {
	if core == nil {
		return fmt.Errorf("managed prepared startup has no verified Core")
	}
	recordedTip, certifiedTip := core.RecorderTip(), core.Tip()
	if recordedTip > certifiedTip {
		return fmt.Errorf("managed prepared startup has uncertified recorded progress: recorded=%d certified=%d", recordedTip, certifiedTip)
	}
	return nil
}

func (n *Node) openInternal(ctx context.Context, enroll bool, prelocked *qlog.LockFile, cleanStart bool) (err error) {
	openStage := "runtime_config"
	defer func() {
		logManagedNodeOpenFailure(n.managedStartup != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol), openStage, err)
	}()
	if n.terminalRequested.Load() {
		return fmt.Errorf("terminally drained node cannot reopen")
	}
	var localRestoreAuthority *materializer.LocalRestoreAuthority
	var localRestoreFiles []materializer.CheckpointFile
	managedMaterializerWasAbsent := false
	var managedGraphTarget materializer.ManagedGraphTargetState
	var managedGraphSelectedSeal *quepaxa.SealedCheckpoint
	var managedGraphSelectedCheckpoint *checkpoint.Checkpoint
	var managedGraphReplayThrough quepaxa.Slot
	var managedPreparedReplayThrough quepaxa.Slot
	if n.config == nil || n.config.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	preparedLearner := n.managedStartup != nil && (n.managedStartup.Prepared != nil || n.managedStartup.verifiedAdoption != nil) && n.config.Learner != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol)
	preparedResume := preparedLearner && n.managedStartup.verifiedAdoption != nil
	if n.opened.Load() {
		return fmt.Errorf("node is already open")
	}
	if err := validateLocalGCConfig(n.config); err != nil {
		return err
	}
	openStage = "runtime_optional_supervisor"
	if err := n.connectFenceSession(ctx); err != nil {
		return err
	}
	if n.config.Local && (enroll || len(n.config.Members) != 0 || n.config.Learner != nil || n.config.EnableReconfiguration ||
		n.config.BindAddr != "" || n.config.PeerAddr != "" || n.config.PeerToken != "" || n.config.AdminToken != "") {
		return fmt.Errorf("local mode requires standalone storage without listeners, membership, or enrollment")
	}
	if !n.config.Local {
		openStage = "runtime_policy"
		if err := sqlpolicy.CheckExistingForPolicy(ctx, n.config.DataDir+"/sqlite.db", n.materializationPolicy()); err != nil {
			return err
		}
	}
	if err := validateVoterMembership(n.config); err != nil {
		return err
	}
	if len(n.config.Members) > 1 {
		if n.config.PeerToken == "" {
			return fmt.Errorf("peer token is required for multi-node clusters")
		}
		if n.config.PeerToken == n.config.AdminToken {
			return fmt.Errorf("admin token must differ from the peer token")
		}
		for _, member := range n.config.Members {
			if member.PublicKey == (quepaxa.PublicKey{}) {
				return fmt.Errorf("voter public key is required for multi-node cluster member %s", member.ID)
			}
		}
	}
	if n.config.ObjStoreDurability == "" {
		n.config.ObjStoreDurability = types.ObjectStoreDurabilityAsync
		if n.config.Local && (n.config.ObjStoreProvider != "" || n.config.ObjStoreEndpoint != "" || n.config.ObjStoreBucket != "" || n.config.ObjStoreDir != "") {
			n.config.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
		}
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
	if err := validateReadAdmissionConfig(n.config); err != nil {
		return err
	}
	if _, err := materializer.ValidateLocalGraphNodePropertyIndexes(n.config.LocalGraphNodePropertyIndexes); err != nil {
		return err
	}
	if !n.opened.CompareAndSwap(false, true) {
		return fmt.Errorf("node is already open")
	}
	n.recoveryPhases = nodeRecoveryPhases{}
	defer func() {
		if err != nil {
			_ = n.abortOpen()
		}
	}()
	ctx, n.cancel = context.WithCancel(ctx)
	defer func() {
		if pins := n.localBackupRestorePins; pins != nil {
			err = errors.Join(err, context.Cause(pins.ctx), pins.close())
			n.localBackupRestorePins = nil
		}
	}()

	// 1. Acquire lock file
	openStage = "runtime_wal_lock"
	lock := prelocked
	if lock == nil {
		var acquireErr error
		cleanStart, lock, acquireErr = qlog.Acquire(n.config.DataDir + "/qlog")
		if acquireErr != nil {
			return fmt.Errorf("acquire lock: %w", acquireErr)
		}
	}
	n.lock = lock
	localVoter, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	if n.managedStartup != nil {
		n.managedStartup.existingRegisteredWAL = localVoter.identity != nil && localVoter.identity.Registered
	}
	if n.config.Local && localVoter.identity != nil {
		return fmt.Errorf("%w: registered voter cannot be reopened as standalone Local data", ErrVoterStateLost)
	}
	if n.config.Local && objectStoreConfigured {
		if err := n.prepareLocalBackup(ctx); err != nil {
			return fmt.Errorf("reconcile Local backup: %w", err)
		}
		if pins := n.localBackupRestorePins; pins != nil {
			stop := context.AfterFunc(pins.ctx, n.cancel)
			defer stop()
		}
	}
	if !n.config.Local {
		if _, err := os.Lstat(filepath.Join(n.config.DataDir, "local-store.json")); err == nil {
			return fmt.Errorf("Local store cannot be opened in non-Local mode; preserve existing data")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		identity, identityErr := openLocalCheckpointIdentity(n.config)
		if identityErr != nil {
			return identityErr
		}
		n.localStore = &identity
		readOnlyWAL, inspectErr := qlog.OpenReadOnly(n.config.DataDir + "/qlog")
		if inspectErr != nil {
			return inspectErr
		}
		maxWALBytes := n.config.MaxWALBytes
		if maxWALBytes == 0 {
			maxWALBytes = 4 << 30
		}
		if inspectErr = readOnlyWAL.SetMaxBytes(maxWALBytes); inspectErr == nil {
			inspectCore, coreErr := quepaxa.OpenLocalReadOnlyInspection(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *n.loadClusterConfig(), WAL: readOnlyWAL, LocalMode: true})
			inspectErr = coreErr
			if inspectErr == nil {
				inspectErr = n.validateLocalRoots(inspectCore)
			}
			if inspectErr == nil {
				if floor, root, hasBase := inspectCore.RecoveryRoot(); hasBase {
					prefix, prefixOK := inspectCore.PrefixHash(floor)
					if !prefixOK {
						inspectErr = fmt.Errorf("Local recovery base prefix is unavailable")
					} else {
						roots, rootsErr := inspectCore.LocalCheckpointRoots(ctx)
						if rootsErr != nil {
							inspectErr = rootsErr
						} else {
							var expected *quepaxa.LocalCheckpointRoot
							for i := range roots {
								if roots[i].RootHash == root && roots[i].Index == floor && roots[i].PrefixHash == prefix {
									copy := roots[i]
									expected = &copy
									break
								}
							}
							if expected == nil {
								inspectErr = fmt.Errorf("Local recovery base has no complete WAL authority tuple")
							} else {
								descriptor, sourceFiles, openErr := n.localStore.OpenExpected(*expected)
								if openErr != nil {
									inspectErr = openErr
								} else {
									authority := materializer.LocalRestoreAuthority{StoreUUID: n.localStore.policy.StoreUUID, ConfigDigest: n.localStore.configDigest, Index: uint64(expected.Index), ConfigID: expected.ConfigID, RootHash: descriptor.RootHash, PrefixHash: descriptor.Prefix, StateHash: descriptor.StateHash}
									if prepareErr := materializer.PrepareLocalRestore(filepath.Join(n.config.DataDir, "sqlite.db"), authority, sourceFiles); prepareErr != nil {
										inspectErr = prepareErr
									} else {
										localRestoreAuthority = &authority
										localRestoreFiles = sourceFiles
									}
								}
							}
						}
					}
				} else {
					inspectErr = materializer.ValidateNoLocalRestore(filepath.Join(n.config.DataDir, "sqlite.db"))
					if inspectErr == nil {
						inspectErr = sqlpolicy.CheckExisting(ctx, filepath.Join(n.config.DataDir, "sqlite.db"))
					}
					if inspectErr == nil {
						_, sqliteErr := os.Lstat(filepath.Join(n.config.DataDir, "sqlite.db"))
						allowAbsentGraph := inspectCore.Tip() == 0 && errors.Is(sqliteErr, os.ErrNotExist)
						inspectErr = materializer.ValidateLocalGraphStorage(filepath.Join(n.config.DataDir, "latticedb", "graph.ltdb"), allowAbsentGraph)
					}
				}
			}
		}
		closeErr := readOnlyWAL.Close()
		if inspectErr != nil || closeErr != nil {
			return errors.Join(inspectErr, closeErr)
		}
	}

	// 2. Open WAL
	var wal *qlog.WAL
	if n.config.Local {
		wal, err = qlog.OpenLocalDeferredCleanup(n.config.DataDir + "/qlog")
	} else {
		openStage = "runtime_wal_open"
		wal, err = qlog.Open(n.config.DataDir + "/qlog")
	}
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
	if objectStoreConfigured && !n.config.Local {
		openStage = "runtime_source_open"
		provider := objectstore.Provider(n.config.ObjStoreProvider)
		if provider == "" {
			provider = objectstore.ProviderS3
		}
		newBucket := objectstore.NewBucketWithContext
		if n.fenceSession != nil {
			newBucket = objectstore.NewBucketForSupervisedStartup
		}
		if n.managedStartup != nil {
			finalPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
			newBucket = func(ctx context.Context, cfg objectstore.Config) (*objectstore.MeteredBucket, error) {
				return objectstore.NewBucketForNamespace(ctx, cfg, finalPrefix)
			}
		}
		bucket, bucketErr := newBucket(ctx, objectstore.Config{
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
		var beforeWrite []func(context.Context, string) error
		if n.fenceSession != nil {
			beforeWrite = append(beforeWrite, n.fenceSession.BindWAL)
		}
		if n.managedStartup != nil {
			openStage = "runtime_registration"
			if err := n.managedStartup.Checker.ValidateManagedStartup(ctx, n.managedStartup.Identity); err != nil {
				return fmt.Errorf("managed registration authority: %w", err)
			}
			if !n.managedStartup.Identity.Fresh {
				exists, err := bucket.Exists(ctx, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "archive/head.bin"))
				if err != nil || !exists && !n.managedStartup.PeerRecovery {
					return errors.Join(fmt.Errorf("managed recovery requires existing archive head"), err)
				}
			}
		}
		profile, profileErr := n.managedVoterProfileFor(n.config)
		if profileErr != nil {
			return profileErr
		}
		var identityErr error
		if profile == nil {
			identityErr = ensureVoterIdentity(ctx, n.config, bucket, n.wal, localVoter, enroll, beforeWrite...)
		} else {
			identityErr = ensureVoterIdentityWithProfile(ctx, n.config, bucket, n.wal, localVoter, enroll, false, profile, beforeWrite...)
		}
		if identityErr != nil {
			return identityErr
		}
		if n.fenceSession != nil && n.fenceSession.Identity().WALIdentity == "" {
			return fmt.Errorf("supervised fencing requires registered voter WAL identity")
		}
		if enroll {
			return n.Shutdown()
		}
		clusterPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		if n.config.Local {
			n.checkpoints = checkpoint.NewManager(bucket, clusterPrefix, n.config.DataDir, 1)
			n.archive = recovery.NewManager(bucket, clusterPrefix, 1)
		} else {
			n.checkpoints, err = checkpoint.NewAppendOnlyManagerForPolicy(bucket, clusterPrefix, n.config.DataDir, n.materializationPolicy(), 1)
			if err != nil {
				return err
			}
			var validateValue func([]byte) error
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				validateValue = types.ValidateManagedGraphExecutionPolicy
			}
			n.archive, err = recovery.NewAppendOnlyManagerWithValueValidator(bucket, clusterPrefix, 1, validateValue)
			if err != nil {
				return err
			}
		}
		openStage = "runtime_checkpoint_load"
		if loadErr := n.checkpoints.Load(ctx); loadErr != nil {
			return fmt.Errorf("load checkpoint manifest: %w", loadErr)
		}
		n.archive.SetGroupDelay(n.config.ObjStoreBatchDelay)
		if len(n.config.Members) > 1 && !n.archive.CASSupported() {
			return fmt.Errorf("multi-node shared archive requires conditional object writes")
		}
		openStage = "runtime_archive_load"
		loadArchive := true
		if n.managedStartup != nil && n.managedStartup.PeerRecovery {
			loadArchive, err = bucket.Exists(ctx, path.Join(clusterPrefix, "archive/head.bin"))
			if err != nil {
				return err
			}
		}
		if loadArchive {
			if loadErr := n.archive.Load(ctx); loadErr != nil {
				if n.managedStartup != nil || n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck {
					return fmt.Errorf("load shared decision archive: %w", loadErr)
				}
				log.Printf("shared archive unavailable during async startup: %v", loadErr)
			}
		}
		if n.archive.Sealed() {
			return recovery.ErrArchiveSealed
		}
	}
	if !objectStoreConfigured && !n.config.Local {
		if err := ensureVoterIdentity(ctx, n.config, nil, n.wal, localVoter, enroll); err != nil {
			return err
		}
	}
	startupRecovery := &startupRecoveryGuard{ctx: ctx}
	if n.checkpoints != nil && n.managedStartup == nil {
		owner, ownerErr := startupRecoveryOwner(string(n.config.NodeID))
		if ownerErr != nil {
			return ownerErr
		}
		startupRecovery, err = newStartupRecoveryGuard(ctx, n.archive, owner)
		if err != nil {
			return fmt.Errorf("pin startup recovery evidence: %w", err)
		}
		defer startupRecovery.Close()
	}

	// 3. Recovery is completed below by Core.Recover plus deterministic replay.
	if !cleanStart {
		log.Println("non-clean start detected, recovering from WAL...")
	}

	openStage = "runtime_materializer"
	var material *materializer.Materializer
	deferredManagedGraph := n.managedStartup != nil && n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy
	if deferredManagedGraph {
		if localtesthooks.Enabled {
			localtesthooks.Hit("node:managed-graph-startup:target_inspect")
		}
		managedMaterializerWasAbsent, err = managedMaterializerFilesAbsent(n.config.DataDir)
		if err != nil {
			return err
		}
		managedGraphTarget, err = materializer.InspectManagedGraphInstallTarget(ctx, filepath.Join(n.config.DataDir, "sqlite.db"), n.receiptWindow(), n.managedGraphInstallProtocol())
		if err != nil {
			return err
		}
	}
	if localRestoreAuthority == nil && !deferredManagedGraph {
		if n.managedStartup != nil {
			managedMaterializerWasAbsent, err = managedMaterializerFilesAbsent(n.config.DataDir)
			if err != nil {
				return err
			}
		}
		material, err = n.openMaterializer(filepath.Join(n.config.DataDir, "sqlite.db"), 4)
		if err != nil {
			if n.managedStartup != nil {
				return fmt.Errorf("open managed materializer without replacing application evidence: %w", err)
			}
			if errors.Is(err, sqlpolicy.ErrIncompatible) {
				return fmt.Errorf("open materializer: %w", err)
			}
			if rebuildErr := quarantineSQLite(n.config.DataDir + "/sqlite.db"); rebuildErr != nil {
				return fmt.Errorf("open materializer: %w; quarantine: %v", err, rebuildErr)
			}
			if rebuildErr := quarantineGraph(n.config.DataDir + "/latticedb"); rebuildErr != nil {
				return fmt.Errorf("open materializer: %w; quarantine graph: %v", err, rebuildErr)
			}
			material, err = n.openMaterializer(filepath.Join(n.config.DataDir, "sqlite.db"), 4)
			if err != nil {
				return fmt.Errorf("rebuild materializer: %w", err)
			}
		}
		if err := material.ConfigureLocalGraphNodePropertyIndexes(n.config.LocalGraphNodePropertyIndexes); err != nil {
			_ = material.Close()
			return fmt.Errorf("configure local graph indexes: %w", err)
		}
		n.material = material
	}

	// 5. Create consensus core through the same public API available to external users.
	openStage = "runtime_core_open"
	cluster := n.loadClusterConfig()
	var transport *network.Transport
	var coreTransport quepaxa.Transport
	managedPeerProfile, profileErr := n.managedAppendOnlyProfile()
	if profileErr != nil {
		return profileErr
	}
	if !n.config.Local {
		if managedPeerProfile != nil {
			transport, err = network.NewTransportForManagedProfile(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, *managedPeerProfile)
		} else {
			transport, err = network.NewTransportForPolicy(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, n.materializationPolicy())
		}
		if err != nil {
			return err
		}
		coreTransport = transport
	}
	n.transport = transport
	if len(cluster.Members) > 1 {
		n.catchUpWake = make(chan struct{}, 1)
		if managedPeerProfile != nil {
			n.catchUp, err = network.NewTransportForManagedProfile(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, *managedPeerProfile)
		} else {
			n.catchUp, err = network.NewTransportForPolicy(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, n.materializationPolicy())
		}
		if err != nil {
			return err
		}
	}
	// An unpromoted learner holds no voting identity, so it authenticates its
	// read-only sync with the cluster admin token until its promotion applies.
	if n.config.Learner != nil {
		transport.SetAdminToken(n.config.AdminToken)
		if n.catchUp != nil {
			n.catchUp.SetAdminToken(n.config.AdminToken)
		}
	}
	makeCore := quepaxa.New
	if n.config.Learner != nil {
		makeCore = quepaxa.NewLearner
	}
	var validateValue func([]byte) error
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		validateValue = types.ValidateManagedGraphExecutionPolicy
		for _, peer := range []*network.Transport{transport, n.catchUp} {
			if peer != nil {
				if err := peer.ValidateManagedGraphWireBudget(); err != nil {
					return fmt.Errorf("managed Graph wire budget: %w", err)
				}
			}
		}
	}
	if deferredManagedGraph && localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:core_open")
	}
	coreConfig := quepaxa.Config{
		NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: coreTransport, LocalMode: n.config.Local,
		EnableReconfiguration:    n.config.EnableReconfiguration,
		ReconfigurationAdmission: n.verifyMembershipAdmission,
		ValidateValue:            validateValue,
	}
	if managedPeerProfile != nil {
		coreConfig.ManagedAppendOnly, err = n.managedAppendOnlyCoreConfig()
		if err != nil {
			return err
		}
	}
	core, err := makeCore(coreConfig)
	if err != nil {
		return fmt.Errorf("create QuePaxa core: %w", err)
	}
	n.core = core
	if n.config.Local {
		recovered := core.CurrentCluster()
		if len(recovered.Members) != 1 || recovered.Members[0].ID != n.config.NodeID {
			return fmt.Errorf("local mode cannot open a multi-voter or foreign WAL")
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			prefix, ok := core.PrefixHash(seal.Index)
			if !ok || prefix != seal.PrefixHash {
				return fmt.Errorf("Local checkpoint prefix is not in the certified history")
			}
			descriptor, _, err := n.localStore.OpenExpected(quepaxa.LocalCheckpointRoot{RootHash: seal.RootHash, Index: seal.Index, PrefixHash: prefix, StateHash: seal.StateHash, ConfigID: seal.ConfigID})
			if err != nil {
				return err
			}
			if descriptor.StateHash != fmt.Sprintf("%x", seal.StateHash) {
				return fmt.Errorf("Local checkpoint state hash mismatch")
			}
			return ctx.Err()
		})
	}
	if n.config.Learner != nil && n.config.Learner.WALIdentity != "" && n.config.Learner.WALIdentity != core.WALIdentity() {
		return fmt.Errorf("learner WAL identity differs from configured incarnation")
	}
	if n.config.EnableReconfiguration {
		transport.BindCore(core)
		if n.catchUp != nil {
			n.catchUp.BindCore(core)
		}
	}
	// The writable WAL has now been scanned into Core. Confirm that the exact
	// base authorized by the read-only pass is still selected before installing
	// the snapshot pair.
	if localRestoreAuthority != nil {
		if pins := n.localBackupRestorePins; pins != nil {
			if err := pins.confirm(); err != nil {
				return err
			}
		}
		floor, root, ok := core.RecoveryRoot()
		prefix, prefixOK := core.PrefixHash(quepaxa.Slot(localRestoreAuthority.Index))
		if !ok || !prefixOK || uint64(floor) != localRestoreAuthority.Index || fmt.Sprintf("%x", root) != localRestoreAuthority.RootHash || fmt.Sprintf("%x", prefix) != localRestoreAuthority.PrefixHash {
			return fmt.Errorf("writable WAL recovery base differs from authorized Local restore")
		}
		material, err = materializer.OpenLocalFromBase(filepath.Join(n.config.DataDir, "sqlite.db"), 4, *localRestoreAuthority, localRestoreFiles, n.config.LocalGraphNodePropertyIndexes)
		if err != nil {
			return err
		}
	} else if material == nil && !deferredManagedGraph {
		return fmt.Errorf("materializer was not opened before Core initialization")
	}
	n.material = material
	if localRestoreAuthority != nil {
		if err := materializer.FinalizeLocalRestore(filepath.Join(n.config.DataDir, "sqlite.db"), *localRestoreAuthority); err != nil {
			return fmt.Errorf("finalize exact-base Local restore before WAL suffix replay: %w", err)
		}
		n.localRestoreSuffixPending = true
	}
	if index, root, ok := core.LatestPreparedCheckpoint(); ok {
		log.Printf("checkpoint recovered: state=prepared index=%d root=%x", index, root)
	} else if index, root, ok := core.RecoveryRoot(); ok {
		log.Printf("checkpoint recovered: state=compacted index=%d root=%x", index, root)
	}
	if n.checkpoints != nil {
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
	}
	if managedSQLKVPolicy(n.materializationPolicy()) {
		if core.GenerationAnchorHash() != ([32]byte{}) {
			return fmt.Errorf("mixed generation anchors are not supported")
		}
		if n.archive != nil {
			if _, anchored := n.archive.GenerationAnchor(); anchored {
				return fmt.Errorf("mixed generation anchors are not supported")
			}
		}
	}
	openStage = "runtime_archive_recover"
	generationCheckpoint, err := n.installGenerationAnchor(startupRecovery)
	if err != nil {
		return fmt.Errorf("restore generation anchor: %w", startupRecovery.Check(err))
	}
	if n.archive != nil {
		if deferredManagedGraph && localtesthooks.Enabled {
			localtesthooks.Hit("node:managed-graph-startup:archive_recover")
		}
		archiveTip := n.archive.Tip()
		archiveDecisions := n.archive.DecisionsFrom
		archiveBase := n.archive.RecoveryBase
		if snapshot := startupRecovery.Snapshot(); snapshot != nil {
			archiveTip = snapshot.Tip()
			archiveDecisions = snapshot.DecisionsFrom
			archiveBase = snapshot.RecoveryBase
		}
		if !preparedLearner {
			if seal, decision, ok := archiveBase(); ok && core.Tip() < seal.Index {
				if managedSQLKVPolicy(n.materializationPolicy()) {
					dir, err := os.MkdirTemp(n.config.DataDir, ".rhiza-managed-base-*")
					if err != nil {
						return err
					}
					files, readErr := n.checkpoints.DownloadRootFiles(startupRecovery.Context(), uint64(seal.Index), seal.RootHash, dir)
					if readErr == nil {
						readErr = n.validateCheckpointFiles(startupRecovery.Context(), files, uint64(seal.Index))
					}
					cleanupErr := os.RemoveAll(dir)
					if err := errors.Join(readErr, cleanupErr); err != nil {
						return err
					}
				}
				if err := core.RestoreCheckpointBase(startupRecovery.Context(), seal, decision); err != nil {
					return fmt.Errorf("restore checkpoint recovery base: %w", startupRecovery.Check(err))
				}
			}
			for core.Tip() < archiveTip {
				values, _, archiveErr := archiveDecisions(startupRecovery.Context(), core.Tip()+1, 256)
				if archiveErr != nil || len(values) == 0 {
					if archiveErr == nil {
						archiveErr = fmt.Errorf("shared archive omitted slot %d", core.Tip()+1)
					}
					return startupRecovery.Check(archiveErr)
				}
				for _, decision := range values {
					if err := n.validateExecutionPolicy(decision.Value); err != nil {
						return err
					}
				}
				if archiveErr := core.AcceptCertifiedValues(values); archiveErr != nil {
					return fmt.Errorf("recover shared archive: %w", archiveErr)
				}
			}
		}
	}
	openStage = "runtime_pair_reconstruct"
	if deferredManagedGraph {
		if localtesthooks.Enabled {
			localtesthooks.Hit("node:managed-graph-startup:pair_select")
		}
		hasSeal := false
		if managedPeerProfile != nil {
			openStage = "" // This selector supplies its own fixed failure reason.
			selectedRoot, selected, selectErr := n.selectManagedGraphStartupRoot(ctx)
			if selectErr != nil {
				return selectErr
			}
			openStage = "runtime_pair_reconstruct"
			hasSeal = selected != nil
			if selected != nil {
				selectedCopy := *selected
				managedGraphSelectedSeal = &selectedCopy
				managedGraphSelectedCheckpoint = selectedRoot
			}
		} else {
			selected, found, selectErr := core.LatestCheckpointSeal()
			if selectErr != nil {
				return selectErr
			}
			hasSeal = found
			if found {
				selectedCopy := selected
				managedGraphSelectedSeal = &selectedCopy
				managedGraphSelectedCheckpoint, err = n.checkpoints.OpenRoot(ctx, uint64(selected.Index), selected.RootHash)
				if err != nil || managedGraphSelectedCheckpoint == nil || managedGraphSelectedCheckpoint.Hash != selected.StateHash {
					if err == nil {
						err = fmt.Errorf("selected managed Graph checkpoint state hash mismatch")
					}
					return err
				}
			}
		}
		if hasSeal {
			material, managedGraphReplayThrough, err = n.installManagedGraphStartup(ctx, core, managedGraphTarget, managedGraphSelectedSeal)
		} else {
			// The append-only selector already authenticated any observed CURRENT.
			// Legacy Graph still needs its separate locator guard.
			if managedPeerProfile == nil {
				_, hasLocator, locatorErr := n.checkpoints.LoadManagedGraphLocator(ctx)
				if locatorErr != nil {
					return locatorErr
				}
				if hasLocator {
					return fmt.Errorf("managed Graph recovery requires an authenticated producer root")
				}
			}
			if !managedMaterializerWasAbsent || n.managedStartup.existingRegisteredWAL || !n.managedStartup.BootstrapMember || core.CompactionFloor() != 0 {
				return fmt.Errorf("managed Graph recovery requires an authenticated producer root")
			}
			tip := core.Tip()
			if n.managedApplicationProtocol() == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol &&
				(!n.managedStartup.Identity.Fresh || tip != 0) {
				if !managedGraphTarget.Absent || managedGraphTarget.InstallAttempt != nil {
					return fmt.Errorf("managed Graph recovery requires an authenticated producer root")
				}
				if proofErr := n.verifyManagedGraphBootstrapControl(startupRecovery.Context(), tip); proofErr != nil {
					return fmt.Errorf("managed Graph recovery requires an authenticated producer root: %w", proofErr)
				}
			} else {
				if !n.managedStartup.Identity.Fresh {
					return fmt.Errorf("managed Graph recovery requires an authenticated producer root")
				}
				if _, historyErr := certifiedSuffixThrough(core, 1, tip); historyErr != nil {
					return historyErr
				}
			}
			// Actual Core has authenticated the complete genesis prefix. No old
			// native target exists and no installer authority is fabricated.
			material, err = n.openMaterializer(filepath.Join(n.config.DataDir, "sqlite.db"), 4)
			managedGraphReplayThrough = tip
		}
		if err != nil {
			return fmt.Errorf("open actual reconstructed managed Graph pair: %w", err)
		}
		n.material = material
	}
	// Record RPCs must be available while every replica is recovering. Public
	// proposals and learned decisions remain gated by ready=false.
	openStage = "runtime_peer_open"
	var server *network.Server
	if managedPeerProfile != nil {
		server, err = network.NewServerForManagedProfile(core, material, n.config.ClusterID, true, transport, *managedPeerProfile, n.Ready)
	} else {
		server, err = network.NewServerForPolicy(core, material, n.config.ClusterID, true, transport, n.materializationPolicy(), n.Ready)
	}
	if err != nil {
		return err
	}
	server.SetTerminalDrain(func(ctx context.Context, expected network.TerminalDrainRequest) (any, error) {
		return n.TerminalDrain(ctx, expected)
	})
	if n.config.Local {
		server.SetLocalFailureHandler(func(error) { n.ready.Store(false) })
		server.SetLocalReclaimHandler(n.reclaimLocalCheckpoint)
	}
	if n.config.MaxConcurrentReads != 0 {
		if err := server.SetReadAdmissionLimits(network.ReadAdmissionLimits{
			MaxConcurrent: n.config.MaxConcurrentReads,
			MaxLongPoll:   n.config.MaxLongPollReads,
		}); err != nil {
			return fmt.Errorf("configure read admission: %w", err)
		}
	}
	if n.checkpoints != nil {
		server.SetCheckpointPrepare(n.preparePeerCheckpoint)
	}
	server.SetObjectStoreStats(func() (map[string]uint64, bool) {
		stats, ok := n.ObjectStoreStats()
		replayGroupingEnabled := uint64(0)
		if stats.ReplayGroupingEnabled {
			replayGroupingEnabled = 1
		}
		return map[string]uint64{
			"replay_grouping_enabled": replayGroupingEnabled,
			"uploads":                 stats.Uploads, "gets": stats.Gets, "lists": stats.Lists, "heads": stats.Heads,
			"deletes": stats.Deletes, "failures": stats.Failures, "bytes_uploaded": stats.BytesUploaded,
			"bytes_downloaded": stats.BytesDownloaded, "s3_http_requests": stats.S3HTTPRequests,
			"s3_http_failures": stats.S3HTTPFailures,
			"http_requests":    stats.HTTPRequests, "http_failures": stats.HTTPFailures,
			"http_get_requests": stats.HTTPGetRequests, "http_put_requests": stats.HTTPPutRequests,
			"http_head_requests": stats.HTTPHeadRequests, "http_delete_requests": stats.HTTPDeleteRequests,
			"http_other_requests": stats.HTTPOtherRequests,
			"condition_conflicts": stats.ConditionConflicts, "dedup_hits": stats.DedupHits,
			"sdk_retries": stats.SDKRetries, "transport_failures": stats.TransportFailures,
			"http_4xx_unexpected": stats.Unexpected4xx, "http_5xx": stats.HTTP5xx,
			"observed_request_identities":      stats.ObservedRequestIdentities,
			"observed_request_repeats":         stats.ObservedRequestRepeats,
			"request_grouping_unknown":         stats.RequestGroupingUnknown,
			"replay_tracker_capacity_misses":   stats.ReplayTrackerCapacityMisses,
			"replay_identity_capacity_misses":  stats.ReplayIdentityCapacityMisses,
			"replay_incomplete_operations":     stats.ReplayIncompleteOperations,
			"replay_tracked_operations_active": stats.ReplayTrackedOperationsActive,
			"replay_open_readers":              stats.ReplayOpenReaders,
		}, ok
	})
	archive := n.archive
	if n.config.EnableReconfiguration {
		server.SetMembershipToken(n.config.AdminToken)
		server.SetMembershipChange(n.changeMembership)
		server.SetMembershipAbort(n.abortMembership)
	}
	server.SetVoterRecoveryStatus(func(ctx context.Context) network.VoterRecoveryStatus {
		_, _, quorumErr := core.ReadIndex(ctx)
		archiveTip := uint64(0)
		if archive != nil {
			archiveTip = uint64(archive.Tip())
		}
		return network.VoterRecoveryStatus{
			NodeID:       string(n.config.NodeID),
			ClusterID:    string(n.config.ClusterID),
			Durability:   string(n.config.ObjStoreDurability),
			Ready:        n.Ready(),
			Quorum:       quorumErr == nil,
			CertifiedTip: uint64(core.Tip()),
			AppliedTip:   material.Tip(),
			ArchiveTip:   archiveTip,
		}
	})
	if archive != nil {
		server.SetRecoveryArchive(n.config.AdminToken, func(ctx context.Context) error {
			if n.config.EnableReconfiguration && !core.IsVoter() {
				return network.ErrNotReady
			}
			return archive.SyncThrough(ctx, core, core.Tip())
		})
	}
	if n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck {
		server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
			if err := core.EnsureDurable(slot); err != nil {
				return err
			}
			if n.config.Local {
				return n.syncLocalBackup(ctx)
			}
			if err := n.archive.SyncThrough(ctx, core, slot); err != nil {
				return err
			}
			return nil
		})
	}
	n.server = server
	server.SetCompactedHandler(n.wakeCatchUp)
	peerServer := server
	if n.managedStartup != nil {
		// Peer ReadIndex and learned/control traffic must not depend on the
		// application evidence CAS. This server is never returned by API/Handler.
		if managedPeerProfile != nil {
			peerServer, err = network.NewServerForManagedProfile(core, material, n.config.ClusterID, false, transport, *managedPeerProfile, n.ready.Load)
		} else {
			peerServer, err = network.NewServerForPolicy(core, material, n.config.ClusterID, false, transport, n.materializationPolicy(), n.ready.Load)
		}
		if err != nil {
			return err
		}
		peerServer.SetLearnReady(n.managedLearnReady)
		peerServer.SetCompactedHandler(n.wakeCatchUp)
		if n.checkpoints != nil {
			peerServer.SetCheckpointPrepare(n.preparePeerCheckpoint)
		}
		n.peerControl = peerServer
	}
	var peer *network.PeerServer
	if n.config.Learner != nil {
		if n.managedStartup != nil && n.managedStartup.promotionReady != nil {
			peer, err = network.StartLearnerPeerServerWithPromotionGate(ctx, n.peerAddr(), peerServer, cluster.Members, n.config.PeerToken, n.config.AdminToken, *n.config.Learner, n.managedStartup.promotionReady)
		} else {
			peer, err = network.StartLearnerPeerServer(ctx, n.peerAddr(), peerServer, cluster.Members, n.config.PeerToken, n.config.AdminToken, *n.config.Learner)
		}
	} else if !n.config.Local {
		peer, err = network.StartPeerServer(ctx, n.peerAddr(), peerServer, cluster.Members, n.config.PeerToken, n.config.AdminToken)
	}
	if err != nil {
		return fmt.Errorf("listen peer QUIC: %w", err)
	}
	n.peer = peer

	certifiedCheckpoint := generationCheckpoint
	var managedSelectedSeal *quepaxa.SealedCheckpoint
	restoreCheckpoint := certifiedCheckpoint
	if n.checkpoints != nil {
		if deferredManagedGraph {
			certifiedCheckpoint = managedGraphSelectedCheckpoint
		} else if managedPeerProfile != nil {
			certifiedCheckpoint, managedSelectedSeal, err = n.selectManagedAppendOnlyRoot(startupRecovery.Context())
			if err != nil {
				return err
			}
			localtesthooks.Hit("node:startup:managed-checkpoint-selected")
		} else {
			seal, sealed, sealErr := core.LatestCheckpointSeal()
			if sealErr != nil {
				return sealErr
			}
			if current := n.checkpoints.Latest(); current != nil {
				anchorCurrent := !sealed && generationCheckpoint != nil && current.Index == generationCheckpoint.Index && current.RootHash == generationCheckpoint.RootHash
				if !anchorCurrent && (!sealed || current.Index > uint64(seal.Index) || current.Index == uint64(seal.Index) && current.RootHash != seal.RootHash) {
					return fmt.Errorf("checkpoint CURRENT is not backed by certified recovery evidence")
				}
			}
			if sealed {
				certifiedCheckpoint, err = n.checkpoints.OpenRoot(startupRecovery.Context(), uint64(seal.Index), seal.RootHash)
				if err != nil || certifiedCheckpoint.Hash != seal.StateHash {
					if err == nil {
						err = fmt.Errorf("certified checkpoint state hash mismatch")
					}
					return err
				}
				if n.managedStartup == nil {
					certifiedCheckpoint, err = startupRecovery.PinRoot(startupRecovery.Context(), n.checkpoints, certifiedCheckpoint, startupRecovery.Owner())
					if err != nil {
						return fmt.Errorf("pin certified checkpoint recovery root: %w", startupRecovery.Check(err))
					}
				}
			} else if certifiedCheckpoint != nil {
				certifiedCheckpoint, err = startupRecovery.PinRoot(startupRecovery.Context(), n.checkpoints, certifiedCheckpoint, startupRecovery.Owner())
				if err != nil {
					return fmt.Errorf("pin generation recovery root: %w", startupRecovery.Check(err))
				}
			}
		}
	}
	// Normal startup must restore from the checkpoint selected by certified
	// recovery evidence, not the generation anchor captured before the seal was
	// opened. Replay begins after materializer.Tip and cannot cross a compacted
	// WAL floor to reconstruct that missing base.
	if n.managedStartup == nil {
		restoreCheckpoint = certifiedCheckpoint
	}
	if deferredManagedGraph {
		// The exact selected root and its fixed certified cut were already
		// consumed by the paired Graph reconstruction above.
		restoreCheckpoint = nil
	}
	openStage = "runtime_post_checkpoint"
	if n.managedStartup != nil && n.managedStartup.Prepared != nil && !preparedResume {
		openStage = "runtime_post_prepared_view"
		view := n.managedStartup.Prepared.View()
		if view == nil {
			return fmt.Errorf("managed prepared checkpoint view is unavailable")
		}
		openStage = "runtime_post_prepared_base"
		seal, decision, hasBase := view.RecoveryBase()
		if !hasBase {
			if core.CompactionFloor() != 0 {
				return fmt.Errorf("managed prepared WAL has a checkpoint base absent from verified source")
			}
			restoreCheckpoint = nil
		} else {
			if n.checkpoints == nil {
				return fmt.Errorf("managed prepared checkpoint store is unavailable")
			}
			floor, root, ok := core.RecoveryRoot()
			prefix, prefixOK := core.PrefixHash(seal.Index)
			if !ok || floor != seal.Index || root != seal.RootHash || !prefixOK || prefix != seal.PrefixHash {
				return fmt.Errorf("managed prepared WAL base differs from verified checkpoint")
			}
			openStage = "runtime_post_prepared_validate_base"
			if err := core.ValidateCheckpointBase(startupRecovery.Context(), seal, decision); err != nil {
				return fmt.Errorf("validate managed prepared checkpoint: %w", err)
			}
			openStage = "runtime_post_prepared_root"
			preparedCheckpoint, openErr := n.checkpoints.OpenRoot(startupRecovery.Context(), uint64(seal.Index), seal.RootHash)
			if openErr != nil || preparedCheckpoint.Hash != seal.StateHash {
				if openErr == nil {
					openErr = fmt.Errorf("managed prepared checkpoint state hash mismatch")
				}
				return openErr
			}
			restoreCheckpoint = preparedCheckpoint
		}
		openStage = "runtime_post_prepared_prefix"
		if core.Tip() != view.Tip() {
			return fmt.Errorf("managed prepared WAL tip differs from verified source")
		}
		prefix, prefixOK := core.PrefixHash(view.Tip())
		if !prefixOK || prefix != view.PrefixHash() {
			return fmt.Errorf("managed prepared WAL suffix differs from verified source")
		}
	}
	if preparedResume {
		// The existing H adoption, not a second prepared restore, is the
		// checkpoint authority. Preserve the local materializer (which may be
		// between H and the certified WAL tip) and never replace it from H.
		restoreCheckpoint = nil
	}
	if n.config.Local && localRestoreAuthority == nil {
		openStage = "runtime_post_local_resume"
		if err := n.resumePreparedLocalCheckpoint(startupRecovery.Context()); err != nil {
			return fmt.Errorf("resume prepared Local checkpoint: %w", startupRecovery.Check(err))
		}
	}
	recoveryTarget := quepaxa.Slot(material.Tip())
	if n.config.Local {
		recoveryTarget = core.Tip()
	}
	recordedTip := core.RecorderTip()
	certifiedTip := core.Tip()
	// An adoption receipt protects the existing materializer for its lifetime,
	// but only the certified current membership determines the runtime role.
	preparedReadOnly := preparedLearner && !core.IsVoter()
	preparedReadOnlyResume := preparedResume && preparedReadOnly
	if preparedReadOnly {
		openStage = "runtime_post_recorder"
		if err := refusePreparedUncertifiedRecorder(core); err != nil {
			return err
		}
	}
	if preparedResume {
		openStage = "runtime_post_adopted_tip"
		managedPreparedReplayThrough = certifiedTip
		if managedPreparedReplayThrough < n.managedStartup.verifiedAdoption.Tip {
			return fmt.Errorf("managed adopted WAL is behind its durable H receipt")
		}
		recoveryTarget = managedPreparedReplayThrough
	}
	if !preparedReadOnlyResume && recordedTip > recoveryTarget {
		recoveryTarget = recordedTip
	}
	if !preparedReadOnlyResume && certifiedCheckpoint != nil && quepaxa.Slot(certifiedCheckpoint.Index) > recoveryTarget {
		recoveryTarget = quepaxa.Slot(certifiedCheckpoint.Index)
	}
	if !preparedReadOnlyResume && recoveryTarget > core.Tip() {
		openStage = "runtime_post_recover"
		if err := core.RecoverThrough(startupRecovery.Context(), recoveryTarget); err != nil {
			return fmt.Errorf("recover certified log through %d: %w", recoveryTarget, startupRecovery.Check(err))
		}
	}
	if preparedResume && !preparedReadOnly {
		// Recover only through the captured target above, then replay a fixed
		// certified cut. Never restore the adopted materializer from H again.
		managedPreparedReplayThrough = core.Tip()
	}
	if restoreCheckpoint != nil {
		if restoreCheckpoint.Index > material.Tip() {
			openStage = "runtime_post_restore"
			if n.managedStartup != nil && (n.managedStartup.Prepared != nil || n.managedStartup.verifiedAdoption != nil) {
				_, freshEmpty, inspectErr := n.inspectApplicationState(startupRecovery.Context(), material)
				if inspectErr != nil {
					return fmt.Errorf("inspect existing managed SQLite before prepared restore: %w", inspectErr)
				}
				if !managedMaterializerWasAbsent || material.Tip() != 0 || !freshEmpty {
					return fmt.Errorf("refuse to replace non-empty managed SQLite with prepared checkpoint")
				}
			}
			if quepaxa.Slot(restoreCheckpoint.Index) > core.Tip() {
				return fmt.Errorf("checkpoint slot %d is ahead of certified log tip %d", restoreCheckpoint.Index, core.Tip())
			}
			n.recoveryPhases.selectedCheckpoint = true
			n.recoveryPhases.checkpointIndex = restoreCheckpoint.Index
			n.recoveryPhases.checkpointRootHash = restoreCheckpoint.RootHash
			dir, fileErr := os.MkdirTemp(n.config.DataDir, ".rhiza-checkpoint-restore-*")
			if fileErr != nil {
				return fileErr
			}
			defer os.RemoveAll(dir)
			n.recoveryPhases.checkpointDownloadVerifyStarted = true
			downloadStarted := time.Now()
			files, readErr := n.checkpoints.DownloadRootFiles(startupRecovery.Context(), restoreCheckpoint.Index, restoreCheckpoint.RootHash, dir)
			n.recoveryPhases.checkpointDownloadVerify = time.Since(downloadStarted)
			n.recoveryPhases.checkpointDownloadVerifyComplete = readErr == nil
			if readErr != nil {
				return readErr
			}
			materialFiles := make([]materializer.CheckpointFile, 0, len(files))
			for _, file := range files {
				materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			n.recoveryPhases.materializerRestoreStarted = true
			restoreStarted := time.Now()
			if restoreErr := material.RestoreCheckpoint(startupRecovery.Context(), materialFiles); restoreErr != nil {
				n.recoveryPhases.materializerRestore = time.Since(restoreStarted)
				return fmt.Errorf("restore checkpoint %d: %w", restoreCheckpoint.Index, restoreErr)
			}
			n.recoveryPhases.materializerRestore = time.Since(restoreStarted)
			n.recoveryPhases.materializerRestoreComplete = true
		}
	}

	// 6. Verify a materializer that survived beyond the local decision marker,
	// then replay the remaining recovered decisions.
	openStage = "runtime_post_material_tip"
	if quepaxa.Slot(material.Tip()) > core.Tip() {
		return fmt.Errorf("materialized slot %d is ahead of certified log tip %d", material.Tip(), core.Tip())
	}
	if material.Tip() > uint64(core.CompactionFloor()) {
		openStage = "runtime_post_material_decision"
		decision, ok := core.CertifiedValue(quepaxa.Slot(material.Tip()))
		if !ok {
			return fmt.Errorf("materialized slot %d has no recovered decision", material.Tip())
		}
		openStage = "runtime_post_material_validate_tip"
		if err := material.ValidateTip(material.Tip(), decision.Value); err != nil {
			return err
		}
	}
	if deferredManagedGraph {
		if preparedResume && !preparedReadOnly {
			managedGraphReplayThrough = managedPreparedReplayThrough
		}
		if managedGraphReplayThrough > 0 {
			openStage = "runtime_post_replay_graph"
			if err := n.replayLocalDecisionsThrough(startupRecovery.Context(), managedGraphReplayThrough); err != nil {
				return fmt.Errorf("replay managed Graph startup prefix through %d: %w", managedGraphReplayThrough, startupRecovery.Check(err))
			}
		}
	} else if preparedResume {
		if managedPreparedReplayThrough > 0 {
			openStage = "runtime_post_replay_adopted"
			if err := n.replayLocalDecisionsThrough(startupRecovery.Context(), managedPreparedReplayThrough); err != nil {
				return fmt.Errorf("replay managed adopted prefix through %d: %w", managedPreparedReplayThrough, startupRecovery.Check(err))
			}
		}
	} else {
		openStage = "runtime_post_replay_local"
		if err := n.replayLocalDecisions(startupRecovery.Context()); err != nil {
			return fmt.Errorf("replay local decisions: %w", startupRecovery.Check(err))
		}
	}
	openStage = "runtime_post_recovery_evidence"
	if err := startupRecovery.Check(nil); err != nil {
		return fmt.Errorf("renew startup recovery evidence: %w", err)
	}
	// The selected archive head and checkpoint root have been consumed. Release
	// their GC pins before a newly sealed checkpoint attempts the next trim.
	if n.checkpoints != nil && n.managedStartup == nil {
		startupRecovery.Close()
	}
	if certifiedCheckpoint != nil {
		openStage = "runtime_post_selected_seal"
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			if deferredManagedGraph {
				if managedGraphSelectedSeal == nil {
					return fmt.Errorf("managed Graph startup lost its fixed selected checkpoint")
				}
				// Root selection and archive verification were completed before
				// the captured cut. Do not refresh or promote CURRENT after that cut.
				n.markManagedGraphInitialRootComplete()
			} else if managedPeerProfile != nil {
				seal := managedGraphSelectedSeal
				if seal == nil {
					latest, exists, err := core.LatestManagedCheckpointSeal()
					if err != nil || !exists || uint64(latest.Index) != certifiedCheckpoint.Index || latest.RootHash != certifiedCheckpoint.RootHash {
						return errors.Join(fmt.Errorf("managed append-only Graph startup certified root differs from exact QCKP"), err)
					}
					seal = &latest
				}
				if !preparedLearner {
					openStage = "runtime_post_checkpoint_promote"
					if err := n.promoteManagedAppendOnlySeal(ctx, *seal); err != nil {
						return fmt.Errorf("promote managed append-only Graph checkpoint: %w", err)
					}
				}
			} else {
				seal := managedGraphSelectedSeal
				if seal == nil {
					latest, exists, err := core.LatestCheckpointSeal()
					if err != nil || !exists || uint64(latest.Index) != certifiedCheckpoint.Index || latest.RootHash != certifiedCheckpoint.RootHash {
						return errors.Join(fmt.Errorf("managed Graph startup certified root differs from exact seal"), err)
					}
					seal = &latest
				}
				openStage = "runtime_post_checkpoint_promote"
				if err := n.promoteManagedGraphSeal(ctx, *seal); err != nil {
					return fmt.Errorf("promote managed Graph certified checkpoint: %w", err)
				}
			}
		} else if managedPeerProfile != nil {
			// Keep the verified selection paired with its restored root: peer
			// recovery may have learned a later authentic QCKP since selection.
			seal := managedSelectedSeal
			if seal == nil || uint64(seal.Index) != certifiedCheckpoint.Index || seal.RootHash != certifiedCheckpoint.RootHash {
				return fmt.Errorf("managed request startup certified root differs from exact QCKP")
			}
			if !preparedLearner {
				openStage = "runtime_post_checkpoint_promote"
				if err := n.promoteManagedAppendOnlySeal(ctx, *seal); err != nil {
					return fmt.Errorf("promote managed append-only checkpoint: %w", err)
				}
			}
		} else if err := n.publishCertifiedCheckpoint(ctx, certifiedCheckpoint); err != nil {
			return fmt.Errorf("publish certified checkpoint: %w", err)
		}
	}
	if n.config.Local {
		if err := wal.FinalizeDeferredCleanup(); err != nil {
			log.Printf("Local WAL committed-file cleanup deferred: %v", err)
		}
	}
	if n.config.Local && objectStoreConfigured {
		openStage = "runtime_post_local_backup"
		if err := n.initializeLocalBackup(ctx); err != nil {
			return fmt.Errorf("initialize Local backup: %w", err)
		}
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
	if n.localBackupReady {
		openStage = "runtime_post_local_backup"
		if n.localBackupNeedsSnapshot {
			if err := n.reclaimLocalCheckpoint(ctx); err != nil {
				return fmt.Errorf("snapshot Local extension: %w", err)
			}
			n.localBackupNeedsSnapshot = false
		}
		if err := n.completeLocalBackupImport(); err != nil {
			return fmt.Errorf("finish Local backup import: %w", err)
		}
		n.startLocalBackup(ctx)
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
			if err := localtesthooks.HitBackgroundPublication(ctx, "archive"); err != nil {
				return
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if n.config.EnableReconfiguration && !core.IsVoter() {
						continue
					}
					if err := n.catchUpArchive(ctx); err != nil {
						log.Printf("shared archive catch-up error: %v", err)
					} else if err := n.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
						log.Printf("shared archive sync error: %v", err)
					} else if n.managedStartup == nil {
						if err := n.compactCertifiedCheckpoint(ctx); err != nil {
							log.Printf("certified checkpoint compaction skipped: %v", err)
						}
					}
				}
			}
		}()
	}
	// Managed HA retains append-only evidence; legacy checkpoint maintenance
	// requires GC leases and is deliberately not started by this runtime.
	if n.checkpoints != nil && n.managedStartup == nil {
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
			return advanceCheckpointReadBarriers(ctx, reserved, material, server)
		})
		n.checkpointer.ConfigurePublication(
			func() bool {
				seal, ok, err := core.LatestCheckpointSeal()
				return core.IsVoter() && err == nil && (!ok || material.Tip() > uint64(seal.Index))
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
					ConfigID: core.ClusterForSlot(quepaxa.Slot(root.Index) + 1).ConfigID, Index: quepaxa.Slot(root.Index), RootHash: root.RootHash,
					StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: order, FollowingLeaderOrder: following,
					GenerationAnchorHash: core.GenerationAnchorHash(),
				}
				if n.config.EnableReconfiguration {
					history, err := core.CheckpointMembership(quepaxa.Slot(root.Index))
					if err != nil {
						return err
					}
					seal.Membership = &history
				}
				if err := n.checkpoints.ValidatePublisherClaim(ctx, string(core.NodeID()), root.Index, root.RootHash); err != nil {
					return err
				}
				if err := core.PrepareCheckpoint(ctx, seal); err != nil {
					return err
				}
				log.Printf("checkpoint prepared: index=%d root=%x", seal.Index, seal.RootHash)
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
				if err := n.publishCertifiedCheckpoint(ctx, root); err != nil {
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
	// n.checkpoints is currently initialized only for non-Local append-only
	// managers. Keep this legacy worker Local-only so HA never starts it.
	if n.config.Local && n.checkpoints != nil && n.config.ObjStoreGCInterval > 0 {
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
					// Checkpoint deletion is safe only while a validated, published
					// archive recovery base is available and retained below.
					if n.archive == nil {
						continue
					}
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
					seal, ok := n.validatedArchiveRecoveryBase(ctx)
					archiveFloor, ok = advanceArchiveFloor(archiveFloor, uint64(seal.Index), ok)
					if !ok {
						continue
					}
					retain[seal.RootHash] = struct{}{}
					if err := n.checkpoints.GarbageCollectFrom(ctx, retain, 2, archiveFloor, n.config.ObjStoreGCGracePeriod); err != nil {
						log.Printf("checkpoint GC failed: %v", err)
					}
					if n.archive != nil {
						if err := n.archive.Cleanup(ctx, n.config.ObjStoreGCGracePeriod); err != nil {
							log.Printf("archive GC failed: %v", err)
						}
					}
				}
			}
		}()
	}
	n.startFenceWatcher(ctx)
	return nil
}

func (n *Node) Start(ctx context.Context) error {
	if n.managedStartup != nil {
		return fmt.Errorf("managed runtime requires its private application host")
	}
	if n.config != nil && n.config.Local {
		return fmt.Errorf("local mode does not support an HTTP listener; use Open")
	}
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
	if n.managedStartup != nil {
		return nil, network.ErrNotReady
	}
	return n.server, nil
}

// API returns the in-process Go API used by the HTTP adapter.
func (n *Node) API() (*network.Server, error) {
	if n.server == nil {
		return nil, fmt.Errorf("node is not open")
	}
	if n.managedStartup != nil {
		return nil, network.ErrNotReady
	}
	return n.server, nil
}

// Ready reports whether local startup recovery and catch-up have completed.
// It does not prove current quorum availability.
func (n *Node) Ready() bool {
	if !n.ready.Load() {
		return false
	}
	if n.managedStartup == nil {
		return true
	}
	if n.internalApplicationProfile() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return n.CheckManagedLocalRead(ctx) == nil
	}
	if !n.managedAdmitted.Load() || !n.opened.Load() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := n.managedRuntimeStateWithContext(ctx)
	if err != nil {
		return false
	}
	if readChecker, ok := n.managedStartup.Checker.(ManagedSQLReadChecker); ok {
		if !n.core.IsVoter() {
			return false
		}
		return readChecker.ValidateManagedSQLRead(ctx, n.managedStartup.Identity, state) == nil
	}
	return n.managedStartup.Checker.ValidateManagedAdmission(ctx, n.managedStartup.Identity, state) == nil
}

// StartupCaughtUp reports whether Node's initial catch-up has marked the
// local runtime ready for foreground writes.
func (n *Node) StartupCaughtUp() bool {
	return n != nil && n.ready.Load()
}

func (n *Node) Shutdown() error {
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	return n.shutdown()
}

func (n *Node) shutdown() error {
	return n.shutdownEngine(true)
}

func (n *Node) shutdownEngine(closeBucket bool) error {
	return n.shutdownEngineWithArchiveSync(closeBucket, true)
}

// abortOpen closes a failed append-only startup without publishing local
// decisions that its read-only archive selection rejected.
func (n *Node) abortOpen() error {
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	syncArchive := n.managedStartup == nil || !isManagedAppendOnlyProtocol(n.managedApplicationProtocol())
	return n.shutdownEngineWithArchiveSync(true, syncArchive)
}

func (n *Node) shutdownEngineWithArchiveSync(closeBucket, syncArchive bool) error {
	internal := n.stopInternalApplication()
	n.managedPeerVerified.Store(false)
	n.managedAdmitted.Store(false)
	n.opened.Store(false)
	n.ready.Store(false)
	var shutdownErr error
	if n.localGCCancel != nil {
		n.localGCCancel()
	}
	// Stop Local maintenance retries before Close waits on the apply lock.
	if n.config != nil && n.config.Local && n.cancel != nil {
		n.cancel()
	}
	// End admission and join foreground work before choosing the durable tip.
	// Keep the server reachable until the peer handlers that reference it join.
	if n.server != nil {
		n.server.Close()
	}
	if internal != nil {
		internal.active.Wait()
	}
	if n.peerControl != nil {
		n.peerControl.Close()
	}
	if n.checkpointer != nil {
		n.checkpointer.Stop()
	}
	n.checkpointer = nil
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.wg.Wait()
	n.localGCContext = nil
	n.localGCCancel = nil
	if n.peer != nil {
		shutdownErr = errors.Join(shutdownErr, n.peer.Close())
		n.peer = nil
	}
	if n.transport != nil {
		shutdownErr = errors.Join(shutdownErr, n.transport.Close())
		n.transport = nil
	}
	if n.catchUp != nil {
		shutdownErr = errors.Join(shutdownErr, n.catchUp.Close())
		n.catchUp = nil
	}
	n.server = nil
	n.peerControl = nil
	n.ready.Store(false)
	if n.core != nil {
		n.core.StopPeriodicSync()
	}
	if n.localBackupReady && n.localBackup != nil && n.core != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr = errors.Join(shutdownErr, n.syncLocalBackup(shutdownCtx))
		cancel()
	}
	n.localBackupReady = false
	// Existing certified decisions can be archived without a live quorum.
	// Creating a new checkpoint here would instead require new consensus and
	// make simultaneous or last-voter shutdown depend on peers staying alive.
	if n.archive != nil {
		if syncArchive && n.core != nil && (!n.config.EnableReconfiguration || n.core.IsVoter()) {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			shutdownErr = errors.Join(shutdownErr, n.archive.SyncThrough(shutdownCtx, n.core, n.core.Tip()))
			cancel()
		}
		n.archive.Close()
		n.archive = nil
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

	if n.localOwner != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if n.bucket != nil {
			if err := n.bucket.DrainMutations(drainCtx); err != nil {
				n.localOwner.Poison()
				shutdownErr = errors.Join(shutdownErr, err)
			}
		}
		cancel()
		if n.localBackup != nil {
			checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resolved, err := n.localBackup.MaintenanceResolved(checkCtx)
			cancel()
			if err != nil || !resolved {
				n.localOwner.Poison()
				shutdownErr = errors.Join(shutdownErr, err, fmt.Errorf("v2 Local maintenance is unresolved"))
			}
		}
	}
	if n.bucket != nil && closeBucket {
		if err := n.bucket.Close(); err != nil {
			if n.localOwner != nil {
				n.localOwner.Poison()
			}
			shutdownErr = errors.Join(shutdownErr, err)
		}
		n.bucket = nil
	}
	if n.localOwner != nil {
		if err := n.localOwner.Close(30 * time.Second); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.localOwner = nil
		}
	}
	n.localBackup = nil
	// Release the DataDir lock only after filesystem archive ownership drains.
	if n.lock != nil {
		shutdownErr = errors.Join(shutdownErr, n.lock.Release())
		n.lock = nil
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
