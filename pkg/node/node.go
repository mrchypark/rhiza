package node

import (
	"bytes"
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
	thanosobjstore "github.com/thanos-io/objstore"
)

type nodeObjectBucket interface {
	thanosobjstore.Bucket
	Stats() objectstore.Stats
	DrainMutations(context.Context) error
	Close() error
}

const (
	nodeOpenPhaseConfiguration = "configuration"
	nodeOpenPhaseFencing       = "fencing"
	nodeOpenPhaseLocalState    = "local_state"
	nodeOpenPhaseWAL           = "wal"
	nodeOpenPhaseIdentityStore = "identity_storage"
	nodeOpenPhaseArchive       = "archive"
	nodeOpenPhaseMaterializer  = "materializer"
	nodeOpenPhaseConsensus     = "consensus"
	nodeOpenPhaseRecovery      = "recovery"
	nodeOpenPhaseNetwork       = "network"
	nodeOpenPhaseServices      = "services"
)

// Node is the main runtime container.
type Node struct {
	config                    *types.ExecutionConfig
	core                      *quepaxa.Core
	material                  *materializer.Materializer
	server                    *network.Server
	peer                      *network.PeerServer
	transport                 *network.Transport
	catchUp                   *network.Transport
	wal                       *qlog.WAL
	lock                      *qlog.LockFile
	bucket                    nodeObjectBucket
	protectedBucket           *fencedBucket
	checkpoints               *checkpoint.Manager
	archive                   *recovery.Manager
	checkpointer              *checkpoint.AutoCheckpointer
	localStore                *localCheckpointIdentity
	localRoot                 *localCheckpointDescriptor
	localRestoreSuffixPending bool
	recoveryPhases            nodeRecoveryPhases
	ready                     atomic.Bool
	readyClusterMu            sync.RWMutex
	readyCluster              *quepaxa.Cluster
	fenceWriterActive         atomic.Bool
	membershipMu              sync.Mutex
	opened                    atomic.Bool
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
	recoveryMu                sync.Mutex
	replayMu                  sync.Mutex
	compactionMu              sync.Mutex
	catchUpWake               chan struct{}
	archiveHintMu             sync.Mutex
	archiveHintWake           chan struct{}
	archiveHintLatest         quepaxa.TerminalGapKey
	archiveHintPending        quepaxa.TerminalGapKey
	archiveHintLastAttempted  quepaxa.TerminalGapKey
	archiveStartupCheck       archiveStartupCheck
	shutdownMu                sync.Mutex
	terminalMu                sync.Mutex
	terminalIdentity          network.TerminalDrainRequest
	terminalRequested         atomic.Bool
	terminalDone              chan struct{}
	terminalResult            TerminalDrainResult
	terminalErr               error
	fenceSession              *fencing.Session
	authorityClient           *fencing.AuthorityClient
	authorityGrant            fencing.StartupGrant
	authorityIdentity         fencing.Identity
	authorityPromotionReceipt *fencing.BindReceipt
	recoveryWALIdentity       string
}

// nodeRecoveryPhases records the selected certified checkpoint restore path.
// It is written synchronously during open and observed after Open returns.
type nodeRecoveryPhases struct {
	selectedCheckpoint               bool
	haStageAdopted                   bool
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
	if config.EngineOwnedRemovals && (!config.EnableReconfiguration || config.Local || len(config.Members) < 2) {
		return fmt.Errorf("engine-owned removals require multi-voter non-Local reconfiguration")
	}
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
	return n.open(ctx, false)
}

// EnrollExistingVoter registers a pre-registration WAL while all voters are
// offline. It neither restores missing voting state nor starts any listeners.
func (n *Node) EnrollExistingVoter(ctx context.Context) error {
	return n.open(ctx, true)
}

func (n *Node) open(ctx context.Context, enroll bool) (err error) {
	phase := nodeOpenPhaseConfiguration
	defer func() {
		if err != nil {
			err = fmt.Errorf("node open phase %s: %w", phase, err)
		}
	}()
	if n.terminalRequested.Load() {
		return fmt.Errorf("terminally drained node cannot reopen")
	}
	var localRestoreAuthority *materializer.LocalRestoreAuthority
	var localRestoreFiles []materializer.CheckpointFile
	if n.config == nil || n.config.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if n.opened.Load() {
		return fmt.Errorf("node is already open")
	}
	socket, ticket := os.Getenv("RHIZA_FENCE_SOCKET"), os.Getenv("RHIZA_FENCE_TICKET")
	supervised := socket != "" || ticket != "" || n.config.AuthorityProfile != ""
	if err := validateAuthorityProfile(n.config, socket, ticket); err != nil {
		return err
	}
	directAuthority := n.config.AuthorityProfile == fencing.LogicalWriterProfileV1
	if directAuthority && enroll {
		return fmt.Errorf("fencing: logical authority does not support offline voter enrollment")
	}
	if n.config.Local && (enroll || len(n.config.Members) != 0 || n.config.Learner != nil || n.config.EnableReconfiguration ||
		n.config.BindAddr != "" || n.config.PeerAddr != "" || n.config.PeerToken != "" || n.config.AdminToken != "" ||
		n.config.ObjStoreProvider != "" || n.config.ObjStoreEndpoint != "" || n.config.ObjStoreBucket != "" || n.config.ObjStoreDir != "") {
		return fmt.Errorf("local mode requires standalone local storage without listeners, membership, enrollment, or object storage")
	}
	if n.config.HAInitRestoreDir != "" && n.config.Local {
		return fmt.Errorf("HA init restore staging is unavailable in Local mode")
	}
	if !n.config.Local {
		if err := sqlpolicy.CheckExisting(ctx, n.config.DataDir+"/sqlite.db"); err != nil {
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
	if supervised && !objectStoreConfigured {
		return fmt.Errorf("supervised fencing requires a shared decision archive")
	}
	if n.config.ObjStoreDurability == types.ObjectStoreDurabilityBeforeAck && !objectStoreConfigured {
		return fmt.Errorf("before-ack durability requires object storage")
	}
	if len(n.config.Members) > 1 && !objectStoreConfigured {
		return fmt.Errorf("multi-node clusters require shared object storage")
	}
	if n.config.HAInitRestoreDir != "" && !objectStoreConfigured {
		return fmt.Errorf("HA init restore staging requires shared object storage")
	}
	if n.config.MaxWALBytes < 0 {
		return fmt.Errorf("max WAL bytes must not be negative")
	}
	if err := validateReadAdmissionConfig(n.config); err != nil {
		return err
	}
	if n.config.Local {
		if _, err := materializer.ValidateLocalGraphNodePropertyIndexes(n.config.LocalGraphNodePropertyIndexes); err != nil {
			return err
		}
	}
	if !n.opened.CompareAndSwap(false, true) {
		return fmt.Errorf("node is already open")
	}
	n.recoveryPhases = nodeRecoveryPhases{}
	defer func() {
		if err != nil {
			_ = n.Shutdown()
		}
	}()
	// Do not durably enroll a process until all pure configuration and
	// admission checks have passed and failure cleanup is installed.
	phase = nodeOpenPhaseFencing
	if !directAuthority {
		if err := n.connectFenceSession(ctx); err != nil {
			return err
		}
		if n.fenceSession != nil {
			if err := n.applyFenceStartupEnvironment(); err != nil {
				return err
			}
		}
		if n.fenceSession != nil && n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove &&
			(n.config.Learner == nil || n.config.Learner.ID != n.config.NodeID || len(n.config.Members) < 2) {
			return fmt.Errorf("fencing: pending-removal startup requires an explicit nonvoting learner configuration")
		}
	}
	supervised = n.hasFencingAuthority()
	ctx, n.cancel = context.WithCancel(ctx)

	// 1. Acquire lock file
	phase = nodeOpenPhaseLocalState
	cleanStart, lock, err := qlog.Acquire(n.config.DataDir + "/qlog")
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	n.lock = lock
	localVoter, err := loadVoterIdentity(n.config)
	localVoter, err = n.prepareEngineRepairLearnerBootstrap(localVoter, err)
	if err == nil && localVoter.identity == nil && n.recoveryWALIdentity != "" {
		err = validateVoterMembership(n.config)
	}
	if err != nil {
		return fmt.Errorf("load voter identity: %w", err)
	}
	haStageEligible := cleanStart && localVoter.identity == nil && !localVoter.hadWAL
	if haStageEligible && n.config.HAInitRestoreDir != "" {
		// Permit only a strictly parsed same-Pod pending Run left by an uncertain
		// pre-WAL registration; all other pre-existing DataDir state remains stale.
		if err := validateHAStageFreshness(n.config.DataDir, string(n.config.ClusterID), string(n.config.NodeID), os.Getenv("POD_UID"), directAuthority, localVoter); err != nil {
			return fmt.Errorf("HA init restore requires a fresh DataDir: %w", err)
		}
	}
	if directAuthority {
		if err := n.connectFenceSession(ctx); err != nil {
			return err
		}
		if err := n.applyAuthorityStartupEnvironment(); err != nil {
			return err
		}
		supervised = n.hasFencingAuthority()
		if n.authorityGrant.Role != fencing.StartupRoleVoterActive && n.authorityGrant.Role != fencing.StartupRoleLearnerPendingRemove {
			return fmt.Errorf("fencing: unsupported direct logical startup role %q", n.authorityGrant.Role)
		}
		n.recoveryWALIdentity, err = logicalAssignedWALForStartup(n.authorityGrant, localVoter)
		if err != nil {
			return err
		}
		if n.authorityGrant.Role == fencing.StartupRoleLearnerPendingRemove && (n.config.Learner == nil || n.config.Learner.ID != n.config.NodeID || len(n.config.Members) < 2) {
			return fmt.Errorf("fencing: pending logical startup requires authority-issued nonvoting learner configuration")
		}
	}
	var haStageDir string
	var haStageIndex *haRestoreStageIndex
	if n.config.HAInitRestoreDir != "" {
		haStageDir, err = resolveHAStagePath(n.config.DataDir, n.config.HAInitRestoreDir)
		if err != nil {
			return fmt.Errorf("resolve HA init restore stage: %w", err)
		}
		if haStageEligible {
			var present bool
			haStageIndex, present, err = readHAStageIndex(haStageDir)
			if err != nil {
				return fmt.Errorf("read HA init restore stage: %w", err)
			}
			if !present {
				return fmt.Errorf("HA init restore stage is missing")
			}
			bootstrap := n.loadClusterConfig()
			wantMembership := recovery.NewMembershipRecord(string(n.config.ClusterID), bootstrap.Members, string(n.config.ObjStoreDurability))
			if haStageIndex.BootstrapMembership != wantMembership || haStageIndex.BootstrapConfigID != bootstrap.ConfigID {
				return fmt.Errorf("HA init restore bootstrap membership differs from current configuration")
			}
		}
	}
	if n.config.Local && localVoter.identity != nil {
		return fmt.Errorf("%w: registered voter cannot be reopened as standalone Local data", ErrVoterStateLost)
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
			inspectCore, coreErr := quepaxa.New(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *n.loadClusterConfig(), WAL: readOnlyWAL, LocalMode: true})
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
	phase = nodeOpenPhaseWAL
	var wal *qlog.WAL
	if n.config.Local {
		wal, err = qlog.OpenLocalDeferredCleanup(n.config.DataDir + "/qlog")
	} else {
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
	if objectStoreConfigured {
		phase = nodeOpenPhaseIdentityStore
		provider := objectstore.Provider(n.config.ObjStoreProvider)
		if provider == "" {
			provider = objectstore.ProviderS3
		}
		if n.hasFencingAuthority() && provider == objectstore.ProviderGCS {
			if err := rejectSupervisedWriterCredentials(n.config); err != nil {
				return err
			}
		}
		newBucket := objectstore.NewBucketWithContext
		if n.hasFencingAuthority() {
			newBucket = objectstore.NewBucketForSupervisedStartup
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
		var runtimeBucket nodeObjectBucket = bucket
		if n.hasFencingAuthority() {
			protected := &fencedBucket{MeteredBucket: bucket, prefix: path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))}
			n.protectedBucket = protected
			runtimeBucket = protected
		}
		n.bucket = runtimeBucket
		var beforeWrite []func(context.Context, string) error
		var pendingIdentityWrite []func(context.Context, string, []byte) error
		if n.hasFencingAuthority() {
			beforeWrite = append(beforeWrite, n.bindFenceWAL)
			if n.fenceSession != nil && n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove ||
				n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleLearnerPendingRemove {
				pendingIdentityWrite = append(pendingIdentityWrite, n.registerLearnerIdentity)
			}
		}
		assignedRestart := n.fenceSession != nil && n.fenceSession.StartupRole() == fencing.StartupRoleVoterActive && n.fenceSession.Identity().AssignmentID != "" ||
			n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleVoterActive && n.authorityIdentity.AssignmentID != ""
		if err := ensureVoterIdentityWithHooks(ctx, n.config, runtimeBucket, n.wal, localVoter, enroll, beforeWrite, pendingIdentityWrite, assignedRestart, n.recoveryWALIdentity); err != nil {
			return fmt.Errorf("ensure voter identity: %w", err)
		}
		if n.fenceSession != nil && n.fenceSession.Identity().WALIdentity == "" {
			return fmt.Errorf("supervised fencing requires registered voter WAL identity")
		}
		if n.authorityClient != nil && n.authorityIdentity.WALIdentity == "" {
			return fmt.Errorf("fencing: logical authority did not bind the voter WAL")
		}
		if enroll {
			return n.Shutdown()
		}
		phase = nodeOpenPhaseArchive
		clusterPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		n.checkpoints = checkpoint.NewManager(runtimeBucket, clusterPrefix, n.config.DataDir, 1)
		if loadErr := n.checkpoints.Load(ctx); loadErr != nil {
			return fmt.Errorf("load checkpoint manifest: %w", loadErr)
		}
		n.archive = recovery.NewManager(runtimeBucket, clusterPrefix, recovery.ArchiveConfigID)
		n.archive.SetGroupDelay(n.config.ObjStoreBatchDelay)
		if len(n.config.Members) > 1 && !n.archive.CASSupported() {
			return fmt.Errorf("multi-node shared archive requires conditional object writes")
		}
		if err := loadNodeArchive(ctx, n.archive, n.config.ObjStoreDurability, supervised); err != nil {
			return fmt.Errorf("load shared decision archive: %w", err)
		}
	}
	phase = nodeOpenPhaseIdentityStore
	if !objectStoreConfigured {
		if err := ensureVoterIdentity(ctx, n.config, nil, n.wal, localVoter, enroll); err != nil {
			return err
		}
	}
	phase = nodeOpenPhaseRecovery
	startupRecovery := &startupRecoveryGuard{ctx: ctx}
	if n.checkpoints != nil {
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

	phase = nodeOpenPhaseMaterializer
	var material *materializer.Materializer
	if localRestoreAuthority == nil {
		material, err = materializer.Open(filepath.Join(n.config.DataDir, "sqlite.db"), 4)
		if err != nil {
			if errors.Is(err, sqlpolicy.ErrIncompatible) {
				return fmt.Errorf("open materializer: %w", err)
			}
			if rebuildErr := quarantineSQLite(n.config.DataDir + "/sqlite.db"); rebuildErr != nil {
				return fmt.Errorf("open materializer: %w; quarantine: %v", err, rebuildErr)
			}
			if rebuildErr := quarantineGraph(n.config.DataDir + "/latticedb"); rebuildErr != nil {
				return fmt.Errorf("open materializer: %w; quarantine graph: %v", err, rebuildErr)
			}
			material, err = materializer.Open(filepath.Join(n.config.DataDir, "sqlite.db"), 4)
			if err != nil {
				return fmt.Errorf("rebuild materializer: %w", err)
			}
		}
		if err := material.ConfigureLocalGraphNodePropertyIndexes(n.config.LocalGraphNodePropertyIndexes); err != nil {
			_ = material.Close()
			return fmt.Errorf("configure local graph indexes: %w", err)
		}
	}

	// 5. Create consensus core through the same public API available to external users.
	phase = nodeOpenPhaseConsensus
	cluster := n.loadClusterConfig()
	var transport *network.Transport
	var coreTransport quepaxa.Transport
	if !n.config.Local {
		transport = network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken)
		coreTransport = transport
	}
	n.transport = transport
	if len(cluster.Members) > 1 {
		n.catchUpWake = make(chan struct{}, 1)
		n.catchUp = network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken)
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
	core, err := makeCore(quepaxa.Config{
		NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: coreTransport, LocalMode: n.config.Local,
		EnableReconfiguration:            n.config.EnableReconfiguration,
		EngineOwnedRemovals:              n.config.EngineOwnedRemovals,
		ReconfigurationAdmission:         n.verifyMembershipAdmission,
		ReconfigurationPrefixPublication: n.publishReconfigurationPrefix,
	})
	if err != nil {
		return fmt.Errorf("create QuePaxa core: %w", err)
	}
	n.core = core
	n.initArchiveCatchUpHints()
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
		floor, root, ok := core.RecoveryRoot()
		prefix, prefixOK := core.PrefixHash(quepaxa.Slot(localRestoreAuthority.Index))
		if !ok || !prefixOK || uint64(floor) != localRestoreAuthority.Index || fmt.Sprintf("%x", root) != localRestoreAuthority.RootHash || fmt.Sprintf("%x", prefix) != localRestoreAuthority.PrefixHash {
			return fmt.Errorf("writable WAL recovery base differs from authorized Local restore")
		}
		material, err = materializer.OpenLocalFromBase(filepath.Join(n.config.DataDir, "sqlite.db"), 4, *localRestoreAuthority, localRestoreFiles, n.config.LocalGraphNodePropertyIndexes)
	} else if material == nil {
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
			if haStageIndex != nil && haStageIndex.HasCheckpoint && uint64(seal.Index) == haStageIndex.Index &&
				seal.RootHash == hashFromStage(haStageIndex.RootHash) {
				root, err := n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
				if err != nil {
					return err
				}
				if root.Hash != seal.StateHash || haStageIndex.StateHash != fmt.Sprintf("%x", seal.StateHash) {
					return fmt.Errorf("HA init restore checkpoint state differs from certified seal")
				}
				_, err = verifyHAStagedFilesAgainstRoot(haStageDir, *haStageIndex, root)
				return err
			}
			return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
	}
	phase = nodeOpenPhaseRecovery
	generationCheckpoint, err := n.installGenerationAnchor(startupRecovery)
	if err != nil {
		return fmt.Errorf("restore generation anchor: %w", startupRecovery.Check(err))
	}
	if n.archive != nil {
		archiveTip := n.archive.Tip()
		archiveDecisions := n.archive.DecisionsFrom
		archiveBase := n.archive.RecoveryBase
		if snapshot := startupRecovery.Snapshot(); snapshot != nil {
			archiveTip = snapshot.Tip()
			archiveDecisions = snapshot.DecisionsFrom
			archiveBase = snapshot.RecoveryBase
		}
		if seal, decision, ok := archiveBase(); ok && core.Tip() < seal.Index {
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
				if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
					return err
				}
			}
			if archiveErr := core.AcceptCertifiedValues(values); archiveErr != nil {
				return fmt.Errorf("recover shared archive: %w", archiveErr)
			}
		}
	}
	// Record RPCs must be available while every replica is recovering. Public
	// proposals and learned decisions remain gated by ready=false.
	server := network.NewServer(core, material, n.config.ClusterID, true, transport, n.apiReady)
	n.installArchiveCatchUpHint(server, core)
	if n.config.EnableReconfiguration || (n.config.EngineOwnedRemovals && n.config.Learner != nil && n.fenceSession != nil) {
		server.SetPeerLearnReady(n.peerLearnReady)
		server.SetPeerLearnDecisionAllowed(n.peerLearnDecisionAllowed)
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
			Ready:        n.apiReady(),
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
	phase = nodeOpenPhaseNetwork
	if err := installArchiveDurabilityBarriers(server, core, n.archive, n.config.ObjStoreDurability, n.config.EnableReconfiguration, supervised); err != nil {
		return err
	}
	n.server = server
	server.SetCompactedHandler(n.wakeCatchUp)
	var peer *network.PeerServer
	if n.config.Learner != nil {
		peer, err = network.StartLearnerPeerServer(ctx, n.peerAddr(), server, cluster.Members, n.config.PeerToken, n.config.AdminToken, *n.config.Learner)
	} else if !n.config.Local {
		peer, err = network.StartPeerServer(ctx, n.peerAddr(), server, cluster.Members, n.config.PeerToken, n.config.AdminToken)
	}
	if err != nil {
		return fmt.Errorf("listen peer QUIC: %w", err)
	}
	n.peer = peer

	phase = nodeOpenPhaseRecovery
	certifiedCheckpoint := generationCheckpoint
	if n.checkpoints != nil {
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
			certifiedCheckpoint, err = startupRecovery.PinRoot(startupRecovery.Context(), n.checkpoints, certifiedCheckpoint, startupRecovery.Owner())
			if err != nil {
				return fmt.Errorf("pin certified checkpoint recovery root: %w", startupRecovery.Check(err))
			}
		} else if certifiedCheckpoint != nil {
			certifiedCheckpoint, err = startupRecovery.PinRoot(startupRecovery.Context(), n.checkpoints, certifiedCheckpoint, startupRecovery.Owner())
			if err != nil {
				return fmt.Errorf("pin generation recovery root: %w", startupRecovery.Check(err))
			}
		}
	}
	if n.config.Local && localRestoreAuthority == nil {
		if err := n.resumePreparedLocalCheckpoint(startupRecovery.Context()); err != nil {
			return fmt.Errorf("resume prepared Local checkpoint: %w", startupRecovery.Check(err))
		}
	}
	recoveryTarget := quepaxa.Slot(material.Tip())
	if n.config.Local {
		recoveryTarget = core.Tip()
	}
	if recorderTip := core.RecorderTip(); recorderTip > recoveryTarget {
		recoveryTarget = recorderTip
	}
	if certifiedCheckpoint != nil && quepaxa.Slot(certifiedCheckpoint.Index) > recoveryTarget {
		recoveryTarget = quepaxa.Slot(certifiedCheckpoint.Index)
	}
	if recoveryTarget > core.Tip() {
		if err := core.RecoverThrough(startupRecovery.Context(), recoveryTarget); err != nil {
			return fmt.Errorf("recover certified log through %d: %w", recoveryTarget, startupRecovery.Check(err))
		}
	}
	if certifiedCheckpoint != nil {
		if certifiedCheckpoint.Index > material.Tip() {
			if quepaxa.Slot(certifiedCheckpoint.Index) > core.Tip() {
				return fmt.Errorf("checkpoint slot %d is ahead of certified log tip %d", certifiedCheckpoint.Index, core.Tip())
			}
			n.recoveryPhases.selectedCheckpoint = true
			n.recoveryPhases.checkpointIndex = certifiedCheckpoint.Index
			n.recoveryPhases.checkpointRootHash = certifiedCheckpoint.RootHash
			var materialFiles []materializer.CheckpointFile
			if haStageIndex != nil && haStageIndex.HasCheckpoint && haStageIndex.Index == certifiedCheckpoint.Index &&
				haStageIndex.RootHash == fmt.Sprintf("%x", certifiedCheckpoint.RootHash) &&
				haStageEligible && haStageDir != "" {
				prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
				archiveTip := n.archive.Tip()
				if snapshot := startupRecovery.Snapshot(); snapshot != nil {
					archiveTip = snapshot.Tip()
				}
				anchorHash := core.GenerationAnchorHash()
				materialFiles, err = verifyHAStagedCheckpointFiles(haStageDir, *haStageIndex, prefix, recovery.ArchiveConfigID, 1,
					certifiedCheckpoint, core, archiveTip, anchorHash)
				if err != nil {
					return fmt.Errorf("verify HA init restore stage against current history: %w", err)
				}
				n.recoveryPhases.haStageAdopted = true
			} else {
				dir, fileErr := os.MkdirTemp(n.config.DataDir, ".rhiza-checkpoint-restore-*")
				if fileErr != nil {
					return fileErr
				}
				defer os.RemoveAll(dir)
				n.recoveryPhases.checkpointDownloadVerifyStarted = true
				downloadStarted := time.Now()
				files, readErr := n.checkpoints.DownloadRootFiles(startupRecovery.Context(), certifiedCheckpoint.Index, certifiedCheckpoint.RootHash, dir)
				n.recoveryPhases.checkpointDownloadVerify = time.Since(downloadStarted)
				n.recoveryPhases.checkpointDownloadVerifyComplete = readErr == nil
				if readErr != nil {
					return readErr
				}
				materialFiles = make([]materializer.CheckpointFile, 0, len(files))
				for _, file := range files {
					materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
				}
			}
			n.recoveryPhases.materializerRestoreStarted = true
			restoreStarted := time.Now()
			if restoreErr := material.RestoreCheckpoint(startupRecovery.Context(), materialFiles); restoreErr != nil {
				n.recoveryPhases.materializerRestore = time.Since(restoreStarted)
				return fmt.Errorf("restore checkpoint %d: %w", certifiedCheckpoint.Index, restoreErr)
			}
			n.recoveryPhases.materializerRestore = time.Since(restoreStarted)
			n.recoveryPhases.materializerRestoreComplete = true
		}
	}
	if haStageIndex != nil && !haStageIndex.HasCheckpoint && haStageEligible {
		prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		archiveTip := n.archive.Tip()
		if snapshot := startupRecovery.Snapshot(); snapshot != nil {
			archiveTip = snapshot.Tip()
		}
		if err := verifyHAStagedHistoryOnly(*haStageIndex, prefix, recovery.ArchiveConfigID, 1, core, archiveTip, core.GenerationAnchorHash()); err != nil {
			return fmt.Errorf("verify checkpointless HA init history against current archive: %w", err)
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
	if err := n.replayLocalDecisions(startupRecovery.Context()); err != nil {
		return fmt.Errorf("replay local decisions: %w", startupRecovery.Check(err))
	}
	if err := startupRecovery.Check(nil); err != nil {
		return fmt.Errorf("renew startup recovery evidence: %w", err)
	}
	// The selected archive head and checkpoint root have been consumed. Release
	// their GC pins before a newly sealed checkpoint attempts the next trim.
	if n.checkpoints != nil {
		startupRecovery.Close()
	}
	if certifiedCheckpoint != nil {
		if err := n.publishCertifiedCheckpoint(ctx, certifiedCheckpoint); err != nil {
			return fmt.Errorf("publish certified checkpoint: %w", err)
		}
	}
	phase = nodeOpenPhaseServices
	if n.config.Local {
		if err := wal.FinalizeDeferredCleanup(); err != nil {
			log.Printf("Local WAL committed-file cleanup deferred: %v", err)
		}
	}
	currentCluster := core.CurrentCluster()
	if n.maySkipCatchUp(currentCluster) {
		if n.config.EnableReconfiguration {
			n.rememberReadyCluster(&currentCluster)
		}
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
		n.startArchiveWorker(ctx, core)
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
					ConfigID: core.ConfigID(), Index: quepaxa.Slot(root.Index), RootHash: root.RootHash,
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
					} else if n.archive != nil {
						if err := n.archive.Cleanup(ctx, n.config.ObjStoreGCGracePeriod); err != nil {
							log.Printf("archive GC failed: %v", err)
						}
					}
				}
			}
		}()
	}
	n.startFenceWatcher(ctx)
	n.startEngineRepairRunner(ctx)
	return nil
}

func (n *Node) maySkipCatchUp(cluster quepaxa.Cluster) bool {
	if len(cluster.Members) != 1 || (n.config.EngineOwnedRemovals && n.config.Learner != nil) {
		return false
	}
	return !n.config.EnableReconfiguration || (n.core != nil && n.core.StartupCatchUpRoundEligible(cluster))
}

func (n *Node) registerLearnerIdentity(ctx context.Context, key string, data []byte) error {
	if n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleLearnerPendingRemove {
		if n.authorityIdentity.WALIdentity == "" {
			return fmt.Errorf("fencing: logical learner identity registration precedes authority WAL binding")
		}
		return n.authorityClient.RegisterLearnerIdentity(ctx, n.authorityIdentity, fencing.LearnerIdentityRegistration{Key: key, Data: data})
	}
	if n.fenceSession == nil || n.fenceSession.StartupRole() != fencing.StartupRoleLearnerPendingRemove {
		return fmt.Errorf("fencing: learner identity registration is unavailable")
	}
	return n.fenceSession.RegisterLearnerIdentity(ctx, key, data)
}

func voterReadDurabilityBarrier(isVoter func() bool, barrier func(context.Context, quepaxa.Slot) error) func(context.Context, quepaxa.Slot) error {
	return func(ctx context.Context, slot quepaxa.Slot) error {
		if !isVoter() {
			return nil
		}
		return barrier(ctx, slot)
	}
}

func loadNodeArchive(ctx context.Context, archive *recovery.Manager, durability types.ObjectStoreDurability, supervised bool) error {
	if archive == nil {
		if supervised || durability == types.ObjectStoreDurabilityBeforeAck {
			return fmt.Errorf("shared decision archive is required")
		}
		return nil
	}
	if err := archive.Load(ctx); err != nil {
		if supervised || durability == types.ObjectStoreDurabilityBeforeAck {
			return fmt.Errorf("load shared decision archive: %w", err)
		}
		log.Printf("shared archive unavailable during async startup: %v", err)
	}
	if archive.Sealed() {
		return recovery.ErrArchiveSealed
	}
	return nil
}

func installArchiveDurabilityBarriers(server *network.Server, core *quepaxa.Core, archive *recovery.Manager, durability types.ObjectStoreDurability, reconfiguration, supervised bool) error {
	if archive == nil {
		if supervised || durability == types.ObjectStoreDurabilityBeforeAck {
			return fmt.Errorf("shared decision archive is required")
		}
		return nil
	}
	if !supervised && durability != types.ObjectStoreDurabilityBeforeAck {
		return nil
	}
	barrier := func(ctx context.Context, slot quepaxa.Slot) error {
		if supervised && durability == types.ObjectStoreDurabilityAsync {
			if err := archive.Load(ctx); err != nil {
				return err
			}
			if archive.Sealed() {
				return recovery.ErrArchiveSealed
			}
			return nil
		}
		if slot == 0 {
			if err := archive.Load(ctx); err != nil {
				return err
			}
			if archive.Sealed() {
				return recovery.ErrArchiveSealed
			}
			return nil
		}
		if err := core.EnsureDurable(slot); err != nil {
			return err
		}
		return archive.SyncThrough(ctx, core, slot)
	}
	server.SetDurabilityBarrier(barrier)
	if supervised {
		server.SetReadDurabilityBarrier(barrier)
	} else if durability == types.ObjectStoreDurabilityBeforeAck && reconfiguration {
		server.SetReadDurabilityBarrier(voterReadDurabilityBarrier(core.IsVoter, barrier))
	}
	return nil
}

func rejectSupervisedWriterCredentials(config *types.ExecutionConfig) error {
	if config.ObjStoreServiceAccount != "" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return fmt.Errorf("supervised node must use its read-only workload identity; key-file credentials belong only to the fencer")
	}
	return nil
}

func (n *Node) preparePeerCheckpoint(ctx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) error {
	if err := n.checkpoints.ValidatePublisherClaim(ctx, string(sender), uint64(seal.Index), seal.RootHash); err != nil {
		return err
	}
	waited, err := n.core.WaitCheckpointPrefixIfLagging(ctx, seal)
	if err != nil {
		return err
	}
	if waited {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := n.checkpoints.ValidatePublisherClaim(ctx, string(sender), uint64(seal.Index), seal.RootHash); err != nil {
			return err
		}
	}
	if err := n.core.PrepareCheckpoint(ctx, seal); err != nil {
		return err
	}
	log.Printf("checkpoint prepared: index=%d root=%x", seal.Index, seal.RootHash)
	return nil
}

func advanceCheckpointReadBarriers(ctx context.Context, reserved uint64, material *materializer.Materializer, server *network.Server) error {
	for material.Tip() < reserved {
		var nonce [types.ReadBarrierNonceSize]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		if _, err := server.ProposeCheckpointBarrier(ctx, types.EncodeReadBarrier(nonce)); err != nil {
			return err
		}
	}
	return nil
}

// validatedArchiveRecoveryBase returns an archive floor only after both the
// archive chain and its certified checkpoint base have been checked against
// this Core's checkpoint verifier and local lineage. Archive metadata alone
// is not authority to delete checkpoint roots.
func (n *Node) validatedArchiveRecoveryBase(ctx context.Context) (quepaxa.CheckpointSeal, bool) {
	if n.archive == nil || n.core == nil {
		return quepaxa.CheckpointSeal{}, false
	}
	if err := n.archive.Load(ctx); err != nil {
		log.Printf("load shared archive before checkpoint GC failed: %v", err)
		return quepaxa.CheckpointSeal{}, false
	}
	seal, decision, ok := n.archive.RecoveryBase()
	if !ok || decision.Slot <= seal.Index {
		return quepaxa.CheckpointSeal{}, false
	}
	if local, exists := n.core.CertifiedValue(decision.Slot); exists &&
		(local.Hash != decision.Hash || !bytes.Equal(local.Value, decision.Value) || !bytes.Equal(local.Certificate, decision.Certificate)) {
		log.Printf("archive recovery base decision conflicts with local certified history")
		return quepaxa.CheckpointSeal{}, false
	}
	if err := n.core.ValidateCheckpointBase(ctx, seal, decision); err != nil {
		log.Printf("validate shared archive checkpoint base before GC failed: %v", err)
		return quepaxa.CheckpointSeal{}, false
	}
	return seal, true
}

func advanceArchiveFloor(current, base uint64, ok bool) (uint64, bool) {
	if !ok {
		return current, false
	}
	return max(current, base), true
}

// publishCertifiedCheckpoint only makes a checkpoint discoverable after its
// decision archive has been made durable. Startup does not require compaction;
// another recovering voter may still pin the retained history.
func (n *Node) publishCertifiedCheckpoint(ctx context.Context, root *checkpoint.Checkpoint) error {
	if n.archive != nil {
		if err := n.archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
			return err
		}
	}
	return n.checkpoints.PromoteCertifiedCurrent(ctx, root)
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
		if err == recovery.ErrActiveRecoveryPin || err == recovery.ErrActiveGCLock {
			// The checkpoint is already certified and published. Keep the
			// compaction floor and protected history intact; the existing archive
			// ticker will retry this seal after the reader or GC holder releases.
			return nil
		}
		return err
	}
	return n.core.CompactThrough(seal.Index, seal.RootHash)
}

func (n *Node) reclaimLocalCheckpoint(ctx context.Context) (resultErr error) {
	if n.config == nil || !n.config.Local || n.server == nil || n.core == nil || n.material == nil || n.localStore == nil {
		return network.ErrLocalMaintenanceRefused
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: canceled before Local maintenance admission", network.ErrLocalMaintenanceRefused)
	}
	resume, err := n.server.Quiesce(ctx)
	if err != nil {
		if errors.Is(err, network.ErrNotReady) {
			return network.ErrNotReady
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: canceled while quiescing before Local maintenance mutation", network.ErrLocalMaintenanceRefused)
		}
		return err
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, network.ErrLocalMaintenanceRefused) && !quepaxa.IsLocalAdmissionDenial(resultErr) && !errors.Is(resultErr, network.ErrNotReady) {
			n.server.FailLocalLifecycle(resultErr)
		}
		resume()
	}()
	if err := n.localCheckpointSpacePreflight(); err != nil {
		return fmt.Errorf("%w: %v", network.ErrLocalMaintenanceRefused, err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("%w: canceled before Local state replay", network.ErrLocalMaintenanceRefused)
	}
	if n.material.Tip() < uint64(n.core.Tip()) {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return fmt.Errorf("replay Local state before checkpoint: %w", err)
		}
	}
	index := n.material.Tip()
	if index == 0 || quepaxa.Slot(index) != n.core.Tip() || quepaxa.Slot(index) <= n.core.CompactionFloor() {
		return fmt.Errorf("%w: no reclaimable Local prefix", network.ErrLocalMaintenanceRefused)
	}
	prefix, ok := n.core.PrefixHash(quepaxa.Slot(index))
	if !ok {
		return fmt.Errorf("Local checkpoint prefix is unavailable")
	}
	files, capturedIndex, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		return fmt.Errorf("capture Local checkpoint: %w", err)
	}
	defer cleanup()
	if capturedIndex != index {
		return fmt.Errorf("Local checkpoint snapshot moved from slot %d to %d", index, capturedIndex)
	}
	var sourceBytes, graphBytes uint64
	for _, file := range files {
		info, statErr := os.Lstat(file.Path)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("Local checkpoint capture produced an invalid fixed-role file")
		}
		sourceBytes += uint64(info.Size())
		if file.Role == materializer.CheckpointGraphData {
			graphBytes = uint64(info.Size())
		}
	}
	if sourceBytes > localCheckpointMaxBytes {
		return fmt.Errorf("Local checkpoint exceeds the combined %d-byte bound", localCheckpointMaxBytes)
	}
	if err := n.localStore.requireFreeBytes(n.config.DataDir, sourceBytes, graphBytes, uint64(max(int64(0), n.wal.Bytes()))); err != nil {
		return fmt.Errorf("Local checkpoint space changed during capture: %w", err)
	}
	descriptor, err := n.localStore.Publish(ctx, files, index, prefix, n.core.ConfigIDForSlot(quepaxa.Slot(index)))
	if err != nil {
		return fmt.Errorf("publish Local checkpoint: %w", err)
	}
	hitLocalNodeCheckpointCrashBoundary("after-checkpoint-root-durable-before-prepare")
	seal, err := sealFromLocalDescriptor(n.core, descriptor)
	if err != nil {
		return err
	}
	slot, err := n.core.ReclaimLocalCheckpointWithSpacePreflight(ctx, seal, func(rewriteBytes, remainingBytes uint64) error {
		return n.localCheckpointSpacePreflight(rewriteBytes, remainingBytes)
	}, func(applyCtx context.Context, appliedThrough quepaxa.Slot) error {
		if err := n.replayLocalDecisions(applyCtx); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) < appliedThrough {
			return fmt.Errorf("Local materializer stopped at %d before seal slot %d", n.material.Tip(), appliedThrough)
		}
		return n.localCheckpointSpacePreflight()
	})
	if err != nil {
		if quepaxa.IsLocalAdmissionDenial(err) {
			return err
		}
		return fmt.Errorf("reclaim Local checkpoint: %w", err)
	}
	if floor, _, ok := n.core.RecoveryRoot(); !ok || floor != seal.Index {
		return fmt.Errorf("Local checkpoint compacted through unexpected floor after seal slot %d", slot)
	}
	n.localRoot = &descriptor
	// Collection is post-commit cleanup: authority and materialized state are
	// already durable. A deletion failure is retained as cleanup debt and must
	// not turn a successful checkpoint into an ambiguous proposal result.
	if roots, rootErr := n.core.LocalCheckpointRoots(ctx); rootErr != nil {
		log.Printf("Local checkpoint GC deferred: cannot resolve live roots: %v", rootErr)
	} else if gcErr := n.localStore.Collect(roots); gcErr != nil {
		log.Printf("Local checkpoint GC deferred: %v", gcErr)
	}
	return nil
}

func (n *Node) resumePreparedLocalCheckpoint(ctx context.Context) error {
	index, root, prepared := n.core.LatestPreparedCheckpoint()
	if !prepared {
		return nil
	}
	if sealed, ok, err := n.core.LatestCheckpointSeal(); err != nil {
		return err
	} else if ok && sealed.Index == index && sealed.RootHash == root && n.core.CompactionFloor() >= index {
		floor, floorRoot, hasFloor := n.core.RecoveryRoot()
		if hasFloor && floor == index && floorRoot == root {
			return nil
		}
	}
	prefix, ok := n.core.PrefixHash(index)
	if !ok {
		return fmt.Errorf("prepared Local checkpoint has no certified prefix")
	}
	descriptor, _, err := n.localStore.OpenExpected(quepaxa.LocalCheckpointRoot{RootHash: root, Index: index, PrefixHash: prefix, ConfigID: n.core.ConfigIDForSlot(index)})
	if err != nil {
		return fmt.Errorf("open exact prepared Local checkpoint: %w", err)
	}
	if n.localRoot == nil || descriptor.Slot >= n.localRoot.Slot {
		n.localRoot = &descriptor
	}
	if err := n.localCheckpointSpacePreflight(); err != nil {
		return err
	}
	resume, err := n.server.Quiesce(ctx)
	if err != nil {
		return err
	}
	defer resume()
	seal, err := sealFromLocalDescriptor(n.core, descriptor)
	if err != nil {
		return err
	}
	_, err = n.core.ReclaimLocalCheckpointWithSpacePreflight(ctx, seal, func(rewriteBytes, remainingBytes uint64) error {
		return n.localCheckpointSpacePreflight(rewriteBytes, remainingBytes)
	}, func(applyCtx context.Context, through quepaxa.Slot) error {
		if err := n.replayLocalDecisions(applyCtx); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) < through {
			return fmt.Errorf("resumed Local materializer stopped at %d before seal slot %d", n.material.Tip(), through)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("complete prepared Local checkpoint: %w", err)
	}
	n.localRoot = &descriptor
	return nil
}

func (n *Node) localCheckpointSpacePreflight(reserve ...uint64) error {
	if n.localStore == nil || n.wal == nil {
		return fmt.Errorf("Local checkpoint storage is unavailable")
	}
	var live uint64
	for _, path := range []string{filepath.Join(n.config.DataDir, "sqlite.db"), filepath.Join(n.config.DataDir, "latticedb", "graph.ltdb")} {
		size, err := localTreeBytes(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("invalid Local materializer data for disk estimate at %s: %w", path, err)
		}
		if live > ^uint64(0)-size {
			return fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		live += size
	}
	if live > localCheckpointMaxBytes {
		return fmt.Errorf("Local state exceeds the supported checkpoint size bound")
	}
	// Backup output, durable publication copy, possible graph restore copy, and
	// a conservative WAL rewrite are all included; existing root files remain
	// charged as cleanup debt until deletion and directory sync succeed.
	values := []uint64{live, live, live, uint64(max(int64(0), n.wal.Bytes()))}
	values = append(values, reserve...)
	required, err := checkedLocalDiskAdd(values...)
	if err != nil {
		return err
	}
	return n.localStore.requireFreeBytes(n.config.DataDir, required)
}

func localTreeBytes(root string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in Local materializer data")
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("non-regular file in Local materializer data")
		}
		size := uint64(info.Size())
		if total > ^uint64(0)-size {
			return fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		total += size
		return nil
	})
	return total, err
}

func checkedLocalDiskAdd(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if total > ^uint64(0)-value {
			return 0, fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		total += value
	}
	return total, nil
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
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	return n.replayLocalDecisions(ctx)
}

func (n *Node) queueArchiveCatchUpHint(key quepaxa.TerminalGapKey) {
	if key.ConfigID == 0 || key.Freeze == 0 || key.Terminal <= key.Freeze || key.FreezeHash == (quepaxa.ValueHash{}) {
		return
	}
	n.archiveHintMu.Lock()
	wake := n.archiveHintWake
	if wake == nil || key == n.archiveHintLastAttempted || key == n.archiveHintPending {
		n.archiveHintMu.Unlock()
		return
	}
	if latest := n.archiveHintLatest; latest.Terminal != 0 && !newerArchiveCatchUpHint(key, latest) {
		n.archiveHintMu.Unlock()
		return
	}
	n.archiveHintLatest = key
	n.archiveHintPending = key
	n.archiveHintMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (n *Node) initArchiveCatchUpHints() {
	if n.archive == nil {
		return
	}
	// Install the signal state before the peer listener can expose Learned.
	// The one-slot channel coalesces concurrent hints without a retry queue.
	n.archiveHintMu.Lock()
	n.archiveHintWake = make(chan struct{}, 1)
	n.archiveHintLatest = quepaxa.TerminalGapKey{}
	n.archiveHintPending = quepaxa.TerminalGapKey{}
	n.archiveHintLastAttempted = quepaxa.TerminalGapKey{}
	n.archiveStartupCheck = archiveStartupCheck{}
	n.archiveHintMu.Unlock()
}

func (n *Node) installArchiveCatchUpHint(server *network.Server, core *quepaxa.Core) {
	if n.archive == nil || server == nil || core == nil {
		return
	}
	server.SetArchiveCatchUpHint(func(key quepaxa.TerminalGapKey) {
		if core.IsVoter() {
			n.queueArchiveCatchUpHint(key)
		}
	})
	server.SetArchiveCatchUpContinuation(func(ctx context.Context, key quepaxa.TerminalGapKey) error {
		return n.catchUpArchivePrefix(ctx, core, key)
	})
}

func newerArchiveCatchUpHint(candidate, current quepaxa.TerminalGapKey) bool {
	if candidate.ConfigID < current.ConfigID || candidate.Terminal < current.Terminal {
		return false
	}
	return candidate.ConfigID > current.ConfigID || candidate.Freeze > current.Freeze || candidate.Terminal > current.Terminal
}

func (n *Node) takeArchiveCatchUpHint() (quepaxa.TerminalGapKey, bool) {
	n.archiveHintMu.Lock()
	defer n.archiveHintMu.Unlock()
	key := n.archiveHintPending
	if key.Terminal == 0 {
		return quepaxa.TerminalGapKey{}, false
	}
	n.archiveHintPending = quepaxa.TerminalGapKey{}
	n.archiveHintLastAttempted = key
	return key, true
}

func (n *Node) catchUpArchiveHint(ctx context.Context, key quepaxa.TerminalGapKey) error {
	if n.archive == nil || n.core == nil || !n.core.IsVoter() || !n.core.PendingTerminalGap(key) {
		return nil
	}
	if err := n.archive.LoadOnce(ctx); err != nil {
		return err
	}
	if !n.core.PendingTerminalGap(key) {
		return nil
	}
	archiveTip := n.archive.Tip()
	if archiveTip < key.Terminal {
		return nil
	}
	localTip := n.core.Tip()
	if localTip >= key.Terminal {
		return nil
	}
	if localTip < key.Freeze || key.Terminal-localTip > 16 {
		return fmt.Errorf("pending archive terminal range is invalid")
	}
	values, readTip, err := n.archive.DecisionsFrom(ctx, localTip+1, int(key.Terminal-localTip))
	if err != nil {
		return err
	}
	count := int(key.Terminal - localTip)
	if readTip < key.Terminal || len(values) != count {
		return fmt.Errorf("shared archive omitted pending terminal range")
	}
	for i, value := range values {
		if value.Slot != localTip+1+quepaxa.Slot(i) {
			return fmt.Errorf("shared archive returned a non-contiguous pending terminal range")
		}
		if err := types.ValidateExecutionPolicy(value.Value); err != nil {
			return err
		}
	}
	if !n.core.PendingTerminalGap(key) {
		return nil
	}
	return n.core.AcceptCertifiedValues(values)
}

func (n *Node) startArchiveWorker(ctx context.Context, core *quepaxa.Core) {
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
			case <-n.archiveHintWake:
				if key, ok := n.takeArchiveCatchUpHint(); ok {
					if err := n.catchUpArchiveHint(ctx, key); err != nil {
						log.Printf("shared archive terminal catch-up deferred: %v", err)
					}
				}
				if captured, ok := n.takeStartupArchiveCheck(); ok {
					if err := n.catchUpStartupArchive(ctx, captured); err != nil {
						log.Printf("shared archive startup catch-up deferred: %v", err)
					}
				}
			case <-ticker.C:
				if n.config.EnableReconfiguration && !core.IsVoter() {
					continue
				}
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

func archiveRecoveryRequired(localTip, archiveBase quepaxa.Slot) bool {
	return archiveBase != 0 && localTip < archiveBase
}

func (n *Node) restoreArchiveCatchUp(ctx context.Context) (resultErr error) {
	ctx = localtesthooks.WithRecoveryPinCategory(ctx, localtesthooks.RecoveryPinOwnerNodeCatchup)
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
		// Carry only the recovery pin observer onto the fresh close context. The
		// background semantics, deadline, cancellation, and discarded error are
		// unchanged.
		closeCtx = localtesthooks.CarryRecoveryPinObserver(closeCtx, ctx)
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
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
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
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
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
	return n.server, nil
}

// API returns the in-process Go API used by the HTTP adapter.
func (n *Node) API() (*network.Server, error) {
	if n.server == nil {
		return nil, fmt.Errorf("node is not open")
	}
	return n.server, nil
}

// Ready reports public serving readiness, including supervised candidate
// activation. It does not prove current quorum availability.
func (n *Node) Ready() bool { return n.apiReady() }

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
	// Archive catch-up and checkpoint verification can both request local
	// replay. Keep the tip read, decision selection, apply, and restore-suffix
	// state transition under one owner so neither path races or replays stale
	// state concurrently.
	n.replayMu.Lock()
	defer n.replayMu.Unlock()
	for {
		from := quepaxa.Slot(n.material.Tip() + 1)
		decisions, tip, err := n.core.DecisionsFrom(from, 256)
		if err != nil {
			return err
		}
		if n.localRestoreSuffixPending && localRestoreSuffixCrashBoundary != nil {
			for _, decision := range decisions {
				if err := n.material.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
					return err
				}
				localRestoreSuffixCrashBoundary(uint64(decision.Slot))
			}
		} else if err := n.material.ApplyBatch(ctx, decisions); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) >= tip {
			n.localRestoreSuffixPending = false
			return nil
		}
	}
}

// A learner pulls certified history without running voter recovery. Its WAL
// identity remains distinct, and only a terminal decision grants voting rights.
func (n *Node) catchUpMembership(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	if n.config.EnableReconfiguration {
		current := n.core.CurrentCluster()
		cluster = &current
	}
	if n.config.Learner == nil || n.core.IsVoter() {
		if n.config.EnableReconfiguration {
			return n.catchUpStartupVoterQuorum(ctx, transport)
		}
		return n.catchUpQuorum(ctx, transport, cluster)
	}
	peerErr := n.catchUpLearnerPeers(ctx, transport, cluster)
	if peerErr == nil || !n.config.EnableReconfiguration || n.archive == nil {
		return peerErr
	}
	// A stale learner configuration can be rejected before the peer returns
	// any decisions, so peer catch-up alone cannot reveal the transition. Read
	// the existing certified archive once per worker pass, then retry against
	// the configuration it authenticated and applied.
	if err := n.catchUpArchive(ctx); err != nil {
		return fmt.Errorf("refresh learner membership from shared archive: %w", err)
	}
	current := n.core.CurrentCluster()
	if current.ConfigID == cluster.ConfigID {
		return peerErr
	}
	if n.core.IsVoter() {
		return n.catchUpQuorum(ctx, transport, &current)
	}
	return n.catchUpLearnerPeers(ctx, transport, &current)
}

func (n *Node) catchUpLearnerPeers(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	var last error = quepaxa.ErrQuorumUnavailable
	for _, member := range cluster.Members {
		if member.ID == n.core.NodeID() {
			continue
		}
		if last = n.catchUpPeer(ctx, transport, member.ID); last == nil {
			if err := n.core.EnsureDurableThrough(ctx, n.core.Tip()); err != nil {
				return err
			}
			return n.replayLocalDecisions(ctx)
		}
	}
	return last
}

func (n *Node) startCatchUp(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) {
	var initialCluster *quepaxa.Cluster
	if n.config.EnableReconfiguration {
		current := n.core.CurrentCluster()
		cluster = &current
		captured := cloneStartupCluster(current)
		initialCluster = &captured
	}
	initialErr := n.catchUpMembership(ctx, transport, cluster)
	n.observeCatchUpRound(initialErr, initialCluster)
	n.queueInitialStartupArchiveCheck(initialErr)
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
		if n.config.EnableReconfiguration {
			current := n.core.CurrentCluster()
			cluster = &current
		}
		if n.ready.Load() && !n.readyForCurrentCluster() {
			n.ready.Store(false)
		}
		if !n.apiReady() {
			var captured *quepaxa.Cluster
			if n.config.EnableReconfiguration {
				current := cloneStartupCluster(*cluster)
				captured = &current
			}
			n.observeCatchUpRound(n.catchUpMembership(ctx, transport, cluster), captured)
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
				var captured *quepaxa.Cluster
				if n.config.EnableReconfiguration {
					current := cloneStartupCluster(*cluster)
					captured = &current
				}
				n.observeCatchUpRound(n.catchUpMembership(ctx, transport, cluster), captured)
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
	n.observeCatchUpRound(err, nil)
}

func (n *Node) observeCatchUpRound(err error, captured *quepaxa.Cluster) {
	if err != nil {
		// A transient peer timeout must not make an already caught-up node
		// reject traffic; quorum operations enforce their own availability.
		log.Printf("quorum catch-up failed: %v", err)
		return
	}
	if n.hasFencingAuthority() && n.core != nil && n.core.IsVoter() &&
		((n.fenceSession != nil && n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove) || !n.fenceWriterActive.Load()) {
		// The ADD may be locally applied before its canonical journal is durably
		// acknowledged. Keep client writes unavailable until the authority changes
		// the startup role and returns the active producer receipt.
		n.ready.Store(false)
		return
	}
	if n.config != nil && n.config.EnableReconfiguration && n.core != nil &&
		(n.config.Learner == nil || n.core.IsVoter()) {
		if captured == nil || !n.core.StartupCatchUpRoundEligible(*captured) {
			n.ready.Store(false)
			n.rememberReadyCluster(nil)
			return
		}
		n.rememberReadyCluster(captured)
	} else {
		n.rememberReadyCluster(nil)
	}
	n.ready.Store(true)
}

func (n *Node) rememberReadyCluster(cluster *quepaxa.Cluster) {
	n.readyClusterMu.Lock()
	defer n.readyClusterMu.Unlock()
	if cluster == nil || cluster.ConfigID == 0 {
		n.readyCluster = nil
		return
	}
	captured := cloneStartupCluster(*cluster)
	n.readyCluster = &captured
}

func (n *Node) readyForCurrentCluster() bool {
	if n == nil || n.config == nil || !n.config.EnableReconfiguration || n.core == nil ||
		(n.config.Learner != nil && !n.core.IsVoter()) {
		return true
	}
	n.readyClusterMu.RLock()
	if n.readyCluster == nil {
		n.readyClusterMu.RUnlock()
		return false
	}
	captured := cloneStartupCluster(*n.readyCluster)
	n.readyClusterMu.RUnlock()
	return n.core.StartupCatchUpRoundEligible(captured)
}

func (n *Node) apiReady() bool {
	if n == nil || !n.ready.Load() || !n.readyForCurrentCluster() || n.supervisedEngineRepairCandidatePending() || n.logicalPendingLearner() {
		return false
	}
	if n.authorityClient != nil {
		return n.authorityGrant.Role == fencing.StartupRoleVoterActive && n.fenceWriterActive.Load()
	}
	return n.fenceSession == nil || n.fenceSession.StartupRole() != fencing.StartupRoleVoterActive || n.fenceWriterActive.Load()
}

func (n *Node) logicalPendingLearner() bool {
	return n != nil && n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleLearnerPendingRemove
}

func (n *Node) supervisedEngineRepairCandidatePending() bool {
	return n != nil && n.config != nil && n.config.EngineOwnedRemovals && n.config.Learner != nil && n.fenceSession != nil &&
		n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove
}

func (n *Node) peerLearnReady() bool {
	if n == nil || n.terminalRequested.Load() {
		return false
	}
	if n.supervisedEngineRepairCandidatePending() || n.logicalPendingLearner() {
		return true
	}
	return n.apiReady()
}

func (n *Node) peerLearnDecisionAllowed(decision quepaxa.Decision) bool {
	if n == nil || n.config == nil || n.core == nil || n.terminalRequested.Load() {
		return false
	}
	if n.supervisedEngineRepairCandidatePending() || n.logicalPendingLearner() {
		return true
	}
	if (n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleVoterActive ||
		n.fenceSession != nil && n.fenceSession.StartupRole() == fencing.StartupRoleVoterActive) && !n.fenceWriterActive.Load() {
		return false
	}
	if !n.opened.Load() {
		return false
	}
	if n.core.AllowsInstalledTerminalLearn(decision) {
		return true
	}
	if n.config.EnableReconfiguration && n.ready.Load() && n.core.AllowsNextReconfigurationFreeze(decision) {
		return true
	}
	return n.config.EnableReconfiguration && n.core.AllowsPendingReconfigurationLearn(decision)
}

func hasUsablePeerSuffix(applied quepaxa.Slot, successes, quorum int, best *network.DecisionsResponse) bool {
	return successes >= quorum && best != nil && best.Tip > applied
}

func (n *Node) catchUpQuorum(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	startupRound := false
	for {
		if n.config.EnableReconfiguration && startupRound && !n.core.StartupCatchUpRoundEligible(*cluster) {
			n.queueStartupTerminalGap()
			return quepaxa.ErrQuorumUnavailable
		}
		startupRound = true
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
		for _, decision := range best.Decisions {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(best.Decisions); err != nil {
			return err
		}
		if err := n.material.ApplyBatch(ctx, best.Decisions); err != nil {
			return err
		}
		if n.config.EnableReconfiguration && !n.core.StartupCatchUpRoundEligible(*cluster) {
			n.queueStartupTerminalGap()
			return quepaxa.ErrQuorumUnavailable
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
		for _, decision := range response.Decisions {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
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
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	return n.shutdown()
}

func (n *Node) shutdown() error {
	return n.shutdownEngine(true)
}

func (n *Node) shutdownEngine(closeBucket bool) error {
	n.opened.Store(false)
	n.ready.Store(false)
	var shutdownErr error
	// End admission and join foreground work before choosing the durable tip.
	// Keep the server reachable until the peer handlers that reference it join.
	if n.server != nil {
		n.server.Close()
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
	n.ready.Store(false)
	if n.core != nil {
		n.core.StopPeriodicSync()
	}
	// Existing certified decisions can be archived without a live quorum.
	// Creating a new checkpoint here would instead require new consensus and
	// make simultaneous or last-voter shutdown depend on peers staying alive.
	if n.archive != nil {
		if n.core != nil && (!n.config.EnableReconfiguration || n.core.IsVoter()) {
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

	// Release lock
	if n.lock != nil {
		shutdownErr = errors.Join(shutdownErr, n.lock.Release())
		n.lock = nil
	}

	if n.bucket != nil && closeBucket {
		shutdownErr = errors.Join(shutdownErr, n.bucket.Close())
		n.bucket = nil
	}
	if n.authorityClient != nil && n.authorityClient.Client != nil {
		n.authorityClient.Client.CloseIdleConnections()
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
