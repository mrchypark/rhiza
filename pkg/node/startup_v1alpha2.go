package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// StartupChecker checks current authority for this prepared identity. It is not
// a reservation or permission for future application writes.
type StartupChecker interface {
	ValidatePreparedStartup(context.Context, recovery.PreparedStartupIdentity) error
}

type PreparedStartupConfig struct {
	Prepared *recovery.VerifiedPreparedStartup
	Checker  StartupChecker
	// Identity is required for a restart after adoption when Prepared is nil.
	Identity recovery.PreparedStartupIdentity
}

var ErrPreparedWriteAdmissionUnavailable = errors.New("prepared startup write admission is unavailable")

const managedRepairOriginMaxBytes = (128 << 10) + 4096

// ManagedStartupIdentity binds an explicit, live initialization assignment to
// this exact execution member. A missing archive is never an assignment.
type ManagedStartupIdentity struct {
	Member              recovery.PreparedStartupIdentity
	AssignmentID        string
	IntentHash          string
	StorageID           string
	PhysicalBinding     string
	BootstrapPlanDigest string
	EvidenceFormat      string
	Fresh               bool
}

// ManagedRuntimeState is local continuity, not application-state evidence.
type ManagedRuntimeState struct {
	WALIdentity           string
	CertifiedTip          uint64
	AppliedTip            uint64
	RecorderTip           uint64
	ExistingRegisteredWAL bool
}

// ManagedStartupChecker is implemented by the private application host. Startup
// validates the exact assignment without requiring application evidence; live
// admission validates current evidence and binding, never a cached receipt.
type ManagedStartupChecker interface {
	ValidateManagedStartup(context.Context, ManagedStartupIdentity) error
	ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error
}

// managedRepairOriginChecker validates an immutable repair handoff against the
// current managed identity and Kubernetes owner chain. It is optional for
// ordinary prepared startups; repair adoption requires it.
type managedRepairOriginChecker interface {
	ValidateManagedRepairOrigin(context.Context, []byte) error
}

type ManagedStartupConfig struct {
	StorageRoot string
	// PeerRecovery permits retaining a non-serving runtime when HEAD is
	// unavailable. It never authorizes an empty application baseline.
	PeerRecovery          bool
	existingRegisteredWAL bool
	Identity              ManagedStartupIdentity
	Checker               ManagedStartupChecker
	Prepared              *recovery.VerifiedPreparedStartup
	// RepairOrigin is the exact bounded repair.json byte sequence. Its digest is
	// bound to prepared-adoption.json so a later process cannot substitute it.
	RepairOrigin   []byte
	promotionReady chan struct{}
	// BootstrapMember opens an original member using immutable voter registration
	// continuity instead of a learner adoption receipt. It grants no empty source.
	BootstrapMember  bool
	verifiedAdoption *preparedAdoption
}

type ManagedNamespace struct{ ConfigPrefix, FinalPrefix string }

func ManagedNamespaceFor(root, clusterUID string, generation int64, assignmentID, clusterID string) (ManagedNamespace, error) {
	prefix, final, err := objectstore.ManagedNamespacePrefixes(root, clusterUID, generation, assignmentID, clusterID)
	return ManagedNamespace{ConfigPrefix: prefix, FinalPrefix: final}, err
}

// CloneManagedRuntimeConfig detaches mutable caller-owned membership/config
// slices. Running handles never consult successor configuration on retry.
func CloneManagedRuntimeConfig(c types.ExecutionConfig) types.ExecutionConfig {
	c.Members = slices.Clone(c.Members)
	if c.Learner != nil {
		copy := *c.Learner
		c.Learner = &copy
	}
	c.LocalGraphNodePropertyIndexes = slices.Clone(c.LocalGraphNodePropertyIndexes)
	return c
}

// ManagedBootstrapPlanDigest is common to every initial voter. PhysicalBinding
// is the SourceBinding returned by PreparedStartupBindingDigests, not its
// member-specific ConfigDigest. The host must also recognize evidenceFormat.
func (n *Node) ManagedBootstrapPlanDigest(physicalBinding, evidenceFormat string) (string, error) {
	if n == nil || n.config == nil || n.config.Local || physicalBinding == "" || evidenceFormat == "" {
		return "", fmt.Errorf("invalid managed bootstrap binding")
	}
	source, _, err := n.preparedBindingDigests()
	if err != nil || source != physicalBinding {
		return "", errors.Join(fmt.Errorf("physical binding differs from configured storage"), err)
	}
	return types.ManagedBootstrapPlanDigest(string(n.config.ClusterID), n.config.Members, physicalBinding, evidenceFormat, n.config.ObjStoreDurability, n.config.EnableReconfiguration)
}

// OpenManaged starts an owned WAL/core/materializer and private peers while
// application admission stays closed. The caller must Shutdown even when the
// later evidence CAS or admission fails.
func (n *Node) OpenManaged(ctx context.Context, cfg ManagedStartupConfig) (resultErr error) {
	if n == nil || n.config == nil || n.config.Local || n.opened.Load() || n.preparedStartup != nil || n.managedStartup != nil || cfg.Checker == nil {
		return fmt.Errorf("managed startup requires a new non-Local node and live checker")
	}
	configCopy := CloneManagedRuntimeConfig(*n.config)
	n.config = &configCopy
	i := cfg.Identity
	namespace, err := ManagedNamespaceFor(cfg.StorageRoot, i.Member.ClusterUID, i.Member.Generation, i.AssignmentID, string(n.config.ClusterID))
	if err != nil || n.config.ObjStorePrefix != namespace.ConfigPrefix {
		return errors.Join(fmt.Errorf("managed runtime final namespace mismatch"), err)
	}
	if i.Member.ClusterUID == "" || i.Member.Generation < 0 || i.Member.DeploymentUID == "" || i.Member.PodUID == "" || i.Member.NodeID == "" || i.Member.NodeID != string(n.config.NodeID) || i.AssignmentID == "" || i.IntentHash == "" || i.StorageID == "" || i.PhysicalBinding == "" || i.BootstrapPlanDigest == "" || i.EvidenceFormat == "" {
		return fmt.Errorf("managed startup requires exact member and assignment bindings")
	}
	if len(n.config.Members) < 2 || n.config.ClusterID == "" || n.config.PeerToken == "" || n.config.PeerToken == n.config.AdminToken {
		return fmt.Errorf("managed startup requires explicit HA members and distinct peer identity")
	}
	if n.config.ObjStoreDurability == "" {
		n.config.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	}
	source, config, err := n.preparedBindingDigests()
	if err != nil {
		return err
	}
	if i.Member.SourceBinding != source || i.Member.ConfigDigest != config {
		return fmt.Errorf("managed storage or configuration binding mismatch")
	}
	bootstrap, err := n.ManagedBootstrapPlanDigest(i.PhysicalBinding, i.EvidenceFormat)
	if err != nil || bootstrap != i.BootstrapPlanDigest {
		return errors.Join(fmt.Errorf("managed common bootstrap plan mismatch"), err)
	}
	configured, err := validateObjectStoreConfig(n.config)
	if err != nil || !configured {
		return errors.Join(fmt.Errorf("managed startup requires object storage"), err)
	}
	if err := validateVoterMembership(n.config); err != nil {
		return err
	}
	if err := cfg.Checker.ValidateManagedStartup(ctx, i); err != nil {
		return fmt.Errorf("managed startup authority: %w", err)
	}
	if len(cfg.RepairOrigin) > managedRepairOriginMaxBytes {
		return fmt.Errorf("repair origin exceeds bounded file budget")
	}
	if n.config.Learner != nil && (cfg.Prepared != nil || !i.Fresh && !cfg.BootstrapMember) {
		cfg.promotionReady = make(chan struct{})
	}
	if len(cfg.RepairOrigin) != 0 {
		if n.config.Learner == nil || cfg.BootstrapMember || i.Fresh {
			return fmt.Errorf("repair origin requires a non-fresh learner")
		}
		if _, ok := cfg.Checker.(managedRepairOriginChecker); !ok {
			return fmt.Errorf("repair origin checker is unavailable")
		}
		cfg.RepairOrigin = bytes.Clone(cfg.RepairOrigin)
	}
	if cfg.Prepared != nil && (i.Fresh || cfg.Prepared.Identity() != i.Member) {
		return fmt.Errorf("managed prepared identity mismatch")
	}
	n.managedStartup = &cfg
	var startupLock *qlog.LockFile
	var cleanStart bool
	defer func() {
		if startupLock != nil {
			resultErr = errors.Join(resultErr, startupLock.Release())
		}
	}()
	if cfg.Prepared != nil || !i.Fresh && !cfg.BootstrapMember {
		cleanStart, startupLock, err = qlog.Acquire(filepath.Join(n.config.DataDir, "qlog"))
		if err != nil {
			return fmt.Errorf("acquire managed startup lock: %w", err)
		}
	}
	if cfg.Prepared != nil {
		if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
			return err
		}
		if err := n.verifyPreparedWALBeforeRestore(ctx, cfg.Prepared); err != nil {
			return err
		}
		absent, err := managedMaterializerFilesAbsent(n.config.DataDir)
		if err != nil {
			return err
		}
		if !absent {
			if err := n.verifyExistingManagedPreparedSQLBeforeOpen(ctx, cfg.Prepared); err != nil {
				return err
			}
		}
		if err := n.recoverPreparedV1Alpha2WithLock(ctx, cfg.Prepared, startupLock); err != nil {
			return err
		}
	} else if !i.Fresh && !cfg.BootstrapMember {
		if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
			return err
		}
		if err := n.verifyPreparedAdoptionWithLock(ctx, i.Member); err != nil {
			return err
		}
		adoption, err := loadPreparedAdoption(filepath.Join(n.config.DataDir, "prepared-adoption.json"))
		if err != nil {
			return err
		}
		if adoption.OriginDigest != managedRepairOriginDigest(cfg.RepairOrigin) {
			return fmt.Errorf("prepared repair origin does not match durable adoption")
		}
		cfg.verifiedAdoption = &adoption
	}
	// Revalidate authority and owner-chain identity after read-only WAL/SQL
	// preflight (or restore), immediately before opening writable runtime files.
	if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
		return err
	}
	var openErr error
	if startupLock == nil {
		openErr = n.open(ctx, false)
	} else {
		lockForOpen := startupLock
		startupLock = nil // openWithWALLock assumes ownership and releases on failure.
		openErr = n.openWithWALLock(ctx, false, cleanStart, lockForOpen)
	}
	if openErr != nil {
		return openErr
	}
	postOpenFailure := func(err error) error { return errors.Join(err, n.Shutdown()) }
	if cfg.Prepared != nil {
		adoption, err := n.verifyManagedPreparedRestore(ctx, cfg.Prepared)
		if err != nil {
			return postOpenFailure(err)
		}
		if err := persistPreparedAdoption(n.config.DataDir, i.Member, n.core.WALIdentity(), cfg.Prepared.View(), adoption.Membership, managedRepairOriginDigest(cfg.RepairOrigin)); err != nil {
			return postOpenFailure(fmt.Errorf("persist verified prepared adoption: %w", err))
		}
		cfg.verifiedAdoption = &adoption
	} else if cfg.verifiedAdoption != nil {
		if err := n.verifyManagedAdoptionAfterOpen(ctx, *cfg.verifiedAdoption); err != nil {
			return postOpenFailure(err)
		}
	}
	if cfg.promotionReady != nil {
		close(cfg.promotionReady)
	}
	n.managedStartup = &cfg
	n.managedPeerVerified.Store(true)
	return nil
}

func validateManagedRepairOrigin(ctx context.Context, checker ManagedStartupChecker, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	validator, ok := checker.(managedRepairOriginChecker)
	if !ok {
		return fmt.Errorf("repair origin checker is unavailable")
	}
	if err := validator.ValidateManagedRepairOrigin(ctx, raw); err != nil {
		return fmt.Errorf("validate immutable managed repair origin: %w", err)
	}
	return nil
}

func managedRepairOriginDigest(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func (n *Node) managedRuntimeState() ManagedRuntimeState {
	existing := n.managedStartup != nil && n.managedStartup.existingRegisteredWAL
	return ManagedRuntimeState{WALIdentity: n.core.WALIdentity(), CertifiedTip: uint64(n.core.Tip()), AppliedTip: n.material.Tip(), RecorderTip: uint64(n.core.RecorderTip()), ExistingRegisteredWAL: existing}
}

func (n *Node) ManagedState() (ManagedRuntimeState, error) {
	if n == nil || n.managedStartup == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		return ManagedRuntimeState{}, fmt.Errorf("managed runtime is not open")
	}
	return n.managedRuntimeState(), nil
}

// AdmitManaged performs a fresh quorum/replay round before checking current
// application authority. Failure preserves the paused runtime and its data.
func (n *Node) AdmitManaged(ctx context.Context) error {
	if n == nil || n.managedStartup == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		return fmt.Errorf("managed runtime is not open")
	}
	n.managedAdmitted.Store(false)
	target, _, err := n.core.ReadIndex(ctx)
	if err != nil {
		return err
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return err
	}
	state := n.managedRuntimeState()
	if state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		return fmt.Errorf("managed runtime has unapplied or unresolved history")
	}
	if err := n.managedStartup.Checker.ValidateManagedAdmission(ctx, n.managedStartup.Identity, state); err != nil {
		return err
	}
	n.managedAdmitted.Store(true)
	// Initial application admission requires quorum/catch-up once per owned
	// runtime. Later failed quorum rounds cannot revoke safe local reads;
	// their live authority/history checks remain mandatory.
	n.managedInitiallyAdmitted.Store(true)
	return nil
}

// OpenPrepared performs bounded local recovery but cannot serve until the
// actual application mutation gate is wired.
func (n *Node) OpenPrepared(ctx context.Context, cfg PreparedStartupConfig) error {
	if n == nil || n.opened.Load() || n.preparedStartup != nil || n.managedStartup != nil {
		return fmt.Errorf("node already opened or configured")
	}
	n.preparedStartup = &cfg
	return n.Open(ctx)
}

func (n *Node) openPrepared(ctx context.Context, cfg PreparedStartupConfig) error {
	if n == nil || n.config == nil || n.config.Local || cfg.Checker == nil {
		return fmt.Errorf("prepared startup requires a verified handoff and live checker")
	}
	identity := cfg.Identity
	if cfg.Prepared != nil {
		if cfg.Prepared.View() == nil {
			return fmt.Errorf("invalid prepared handoff")
		}
		identity = cfg.Prepared.Identity()
	}
	if identity.NodeID == "" || identity.NodeID != string(n.config.NodeID) {
		return fmt.Errorf("prepared node identity mismatch")
	}
	if n.config.Learner == nil {
		return fmt.Errorf("prepared startup currently requires a new learner")
	}
	if err := validateVoterMembership(n.config); err != nil {
		return err
	}
	if n.config.PeerToken == "" || n.config.PeerToken == n.config.AdminToken {
		return fmt.Errorf("prepared learner requires distinct peer and admin tokens")
	}
	source, config, err := n.preparedBindingDigests()
	if err != nil {
		return err
	}
	if identity.SourceBinding != source || identity.ConfigDigest != config {
		return fmt.Errorf("prepared storage or configuration binding mismatch")
	}
	if err := cfg.Checker.ValidatePreparedStartup(ctx, identity); err != nil {
		return fmt.Errorf("prepared startup authority: %w", err)
	}
	if cfg.Prepared == nil {
		if err := n.verifyPreparedAdoption(ctx, identity); err != nil {
			return err
		}
		return ErrPreparedWriteAdmissionUnavailable
	}
	if err := n.recoverPreparedV1Alpha2(ctx, cfg.Prepared); err != nil {
		return err
	}
	return ErrPreparedWriteAdmissionUnavailable
}

func (n *Node) verifyPreparedWALBeforeRestore(ctx context.Context, prepared *recovery.VerifiedPreparedStartup) (resultErr error) {
	if prepared == nil || prepared.View() == nil {
		return fmt.Errorf("verified prepared history is unavailable")
	}
	walPath := filepath.Join(n.config.DataDir, "qlog")
	wal, err := qlog.OpenReadOnly(walPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing prepared WAL read-only: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	local, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	bound, hasIdentity := wal.Identity()
	if local.identity != nil {
		nonce, decodeErr := hex.DecodeString(local.identity.Nonce)
		if decodeErr != nil || !hasIdentity || !bytes.Equal(nonce, bound) {
			return fmt.Errorf("existing prepared WAL differs from local voter identity")
		}
	} else if hasIdentity {
		return fmt.Errorf("existing prepared WAL has no local voter identity; preserve it")
	}
	entries, err := wal.Read()
	if err != nil {
		return fmt.Errorf("read existing prepared WAL: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	cluster := n.loadClusterConfig()
	core, err := newManagedReadOnlyCore(quepaxa.NodeID("prepared-wal-verifier"), *cluster, wal, n.config.EnableReconfiguration)
	if err != nil {
		return fmt.Errorf("verify existing prepared WAL: %w", err)
	}
	view := prepared.View()
	if core.CompactionFloor() > view.Base() || core.Tip() > view.Tip() {
		return fmt.Errorf("existing WAL is ahead of verified prepared history; preserve it")
	}
	if core.Tip() < view.Base() && core.Tip() != 0 {
		return fmt.Errorf("existing WAL ends before prepared checkpoint base; preserve it")
	}
	if view.Base() > 0 && core.Tip() >= view.Base() {
		seal, _, hasBase := view.RecoveryBase()
		prefix, ok := core.PrefixHash(view.Base())
		if !hasBase || !ok || prefix != seal.PrefixHash {
			return fmt.Errorf("existing WAL checkpoint prefix differs from prepared history")
		}
	}
	for _, value := range view.Decisions() {
		if value.Slot > core.Tip() {
			break
		}
		actual, ok := core.CertifiedValue(value.Slot)
		if !ok || !reflect.DeepEqual(actual, value) {
			return fmt.Errorf("existing WAL certified prefix differs from prepared history at slot %d", value.Slot)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func newManagedReadOnlyCore(nodeID quepaxa.NodeID, cluster quepaxa.Cluster, wal *qlog.WAL, enableReconfiguration bool) (*quepaxa.Core, error) {
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: nodeID, Cluster: cluster, WAL: wal})
	if err == nil || !enableReconfiguration {
		return core, err
	}
	// An old but otherwise compatible WAL may not yet contain the protocol
	// marker. Keep this preflight read-only; normal startup may add the marker
	// only after all existing SQL/WAL validation has passed.
	return quepaxa.NewObserver(quepaxa.Config{NodeID: nodeID, Cluster: cluster, WAL: wal, EnableReconfiguration: true})
}

func (n *Node) verifyPreparedAdoption(ctx context.Context, identity recovery.PreparedStartupIdentity) (resultErr error) {
	_, lock, err := qlog.Acquire(filepath.Join(n.config.DataDir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Release()) }()
	return n.verifyPreparedAdoptionWithLock(ctx, identity)
}

func (n *Node) verifyPreparedAdoptionWithLock(ctx context.Context, identity recovery.PreparedStartupIdentity) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := n.config.DataDir
	data, err := readPreparedAdoption(filepath.Join(dir, "prepared-adoption.json"))
	if err != nil {
		return fmt.Errorf("prepared adoption is missing or invalid")
	}
	var adoption preparedAdoption
	if err := json.Unmarshal(data, &adoption); err != nil || adoption.Identity != identity || adoption.WALIdentity == "" || adoption.Membership == nil {
		return fmt.Errorf("prepared adoption identity mismatch")
	}
	local, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	if local.identity == nil || !local.identity.Registered || local.identity.Nonce != adoption.WALIdentity {
		return fmt.Errorf("prepared WAL registration mismatch")
	}
	expected, err := hex.DecodeString(adoption.WALIdentity)
	if err != nil || len(expected) != 32 {
		return fmt.Errorf("invalid prepared WAL identity")
	}
	inspection, err := qlog.OpenReadOnly(filepath.Join(dir, "qlog"))
	if err != nil {
		return fmt.Errorf("inspect prepared WAL: %w", err)
	}
	bound, exists := inspection.Identity()
	closeErr := inspection.Close()
	if !exists || !bytes.Equal(bound, expected) {
		return errors.Join(fmt.Errorf("prepared WAL binding mismatch"), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close prepared WAL inspection: %w", closeErr)
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(dir, "qlog"))
	if err != nil {
		return fmt.Errorf("open managed WAL read-only: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	maxBytes := n.config.MaxWALBytes
	if maxBytes == 0 {
		maxBytes = 4 << 30
	}
	if maxBytes < 0 {
		return fmt.Errorf("invalid WAL limit")
	}
	if err := wal.SetMaxBytes(maxBytes); err != nil {
		return err
	}
	cluster := n.loadClusterConfig()
	core, err := newManagedReadOnlyCore(quepaxa.NodeID("managed-adoption-verifier"), *cluster, wal, n.config.EnableReconfiguration)
	if err != nil {
		return err
	}
	prefix, ok := core.PrefixHash(adoption.Tip)
	if core.WALIdentity() != adoption.WALIdentity || core.Tip() < adoption.Tip || !ok || prefix != adoption.PrefixHash {
		return fmt.Errorf("prepared WAL continuity mismatch")
	}
	if err := verifyAdoptedWALBase(core, adoption); err != nil {
		return err
	}
	if n.managedStartup != nil {
		absent, err := managedMaterializerFilesAbsent(dir)
		if err != nil {
			return fmt.Errorf("inspect managed SQLite paths before startup: %w", err)
		}
		if !absent {
			if err := n.verifyExistingManagedSQLBeforeOpen(ctx, core, adoption); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) verifyExistingManagedSQLBeforeOpen(ctx context.Context, core *quepaxa.Core, adoption preparedAdoption) (resultErr error) {
	dbPath := filepath.Join(n.config.DataDir, "sqlite.db")
	if err := materializer.ValidateCleanRestoreState(dbPath); err != nil {
		return fmt.Errorf("managed materializer has an interrupted restore; preserve it: %w", err)
	}
	actual, appliedTip, err := materializer.InspectSQLApplicationFile(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("inspect existing managed SQL before startup: %w", err)
	}
	floor := core.CompactionFloor()
	if quepaxa.Slot(appliedTip) < floor {
		return fmt.Errorf("managed SQLite tip %d is below verified checkpoint base %d; preserve existing data", appliedTip, floor)
	}
	if appliedTip > uint64(core.Tip()) {
		return fmt.Errorf("managed SQLite tip %d is ahead of certified WAL tip %d; preserve existing data", appliedTip, core.Tip())
	}
	if adoption.BaseSeal != nil {
		base, root, ok := core.RecoveryRoot()
		prefix, prefixOK := core.PrefixHash(adoption.BaseSeal.Index)
		if !ok || base != floor || base != adoption.BaseSeal.Index || root != adoption.BaseSeal.RootHash || !prefixOK || prefix != adoption.BaseSeal.PrefixHash {
			return fmt.Errorf("managed SQLite verification base differs from adopted WAL")
		}
	} else if floor != 0 {
		return fmt.Errorf("managed SQLite verification has no checkpoint for WAL floor %d", floor)
	}
	if quepaxa.Slot(appliedTip) > adoption.Tip {
		adoptedPrefix, ok := core.PrefixHash(adoption.Tip)
		if core.Tip() < adoption.Tip || !ok || adoptedPrefix != adoption.PrefixHash {
			return fmt.Errorf("managed SQLite verification WAL does not extend adopted prefix")
		}
	}

	bucket, err := n.newManagedStartupBucket(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	tmp, err := os.MkdirTemp(n.config.DataDir, ".rhiza-managed-sql-state-verify-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove managed SQL verification directory: %w", err))
		}
	}()
	var checkpointFiles []materializer.CheckpointFile
	if adoption.BaseSeal != nil {
		clusterPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		cacheDir := filepath.Join(tmp, "checkpoint-cache")
		if err := os.Mkdir(cacheDir, 0o700); err != nil {
			return err
		}
		manager := checkpoint.NewManager(bucket, clusterPrefix, cacheDir, 1)
		root, err := manager.OpenRoot(ctx, uint64(adoption.BaseSeal.Index), adoption.BaseSeal.RootHash)
		if err != nil || root.Hash != adoption.BaseSeal.StateHash {
			if err == nil {
				err = fmt.Errorf("checkpoint state hash differs from adopted seal")
			}
			return fmt.Errorf("verify adopted SQL checkpoint: %w", err)
		}
		checkpointDir := filepath.Join(tmp, "checkpoint")
		if err := os.Mkdir(checkpointDir, 0o700); err != nil {
			return err
		}
		files, err := manager.DownloadRootFiles(ctx, uint64(adoption.BaseSeal.Index), adoption.BaseSeal.RootHash, checkpointDir)
		if err != nil {
			return fmt.Errorf("download adopted SQL checkpoint: %w", err)
		}
		checkpointFiles = checkpointMaterializerFiles(files)
	}
	suffix, err := certifiedSuffixThrough(core, floor+1, quepaxa.Slot(appliedTip))
	if err != nil {
		return fmt.Errorf("verify certified SQL prefix at applied tip %d: %w", appliedTip, err)
	}
	return compareManagedSQLFileAtTip(ctx, dbPath, actual, appliedTip, checkpointFiles, suffix, filepath.Join(tmp, "expected.db"))
}

func (n *Node) verifyExistingManagedPreparedSQLBeforeOpen(ctx context.Context, prepared *recovery.VerifiedPreparedStartup) (resultErr error) {
	view := prepared.View()
	if view == nil {
		return fmt.Errorf("verified prepared view is unavailable")
	}
	dbPath := filepath.Join(n.config.DataDir, "sqlite.db")
	if err := materializer.ValidateCleanRestoreState(dbPath); err != nil {
		return fmt.Errorf("managed materializer has an interrupted restore; preserve it: %w", err)
	}
	actual, appliedTip, err := materializer.InspectSQLApplicationFile(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("inspect existing managed SQL before prepared restore: %w", err)
	}
	if quepaxa.Slot(appliedTip) < view.Base() || quepaxa.Slot(appliedTip) > view.Tip() {
		return fmt.Errorf("existing managed SQL tip %d is outside verified prepared history [%d,%d]", appliedTip, view.Base(), view.Tip())
	}
	tmp, err := os.MkdirTemp(n.config.DataDir, ".rhiza-managed-prepared-sql-verify-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove prepared SQL verification directory: %w", err))
		}
	}()
	var checkpointFiles []materializer.CheckpointFile
	checkpointDir := filepath.Join(tmp, "checkpoint")
	if err := os.Mkdir(checkpointDir, 0o700); err != nil {
		return err
	}
	files, err := view.DownloadCheckpointFiles(ctx, checkpointDir)
	if err != nil {
		return fmt.Errorf("download verified prepared checkpoint: %w", err)
	}
	checkpointFiles = checkpointMaterializerFiles(files)
	var suffix []quepaxa.DecidedValue
	var last = view.Base()
	for _, decision := range view.Decisions() {
		if decision.Slot > quepaxa.Slot(appliedTip) {
			break
		}
		if decision.Slot != last+1 {
			return fmt.Errorf("verified prepared suffix has a gap at slot %d", last+1)
		}
		suffix = append(suffix, decision)
		last = decision.Slot
	}
	if last != quepaxa.Slot(appliedTip) {
		return fmt.Errorf("verified prepared history does not reach existing SQL tip %d", appliedTip)
	}
	return compareManagedSQLFileAtTip(ctx, dbPath, actual, appliedTip, checkpointFiles, suffix, filepath.Join(tmp, "expected.db"))
}

func compareManagedSQLFileAtTip(ctx context.Context, dbPath string, actual materializer.SQLApplicationState, appliedTip uint64, checkpointFiles []materializer.CheckpointFile, suffix []quepaxa.DecidedValue, expectedPath string) (resultErr error) {
	expected, err := materializer.Open(expectedPath, 4)
	if err != nil {
		return fmt.Errorf("open isolated managed SQL verifier: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, expected.Close()) }()
	if len(checkpointFiles) != 0 {
		if err := expected.RestoreCheckpoint(ctx, checkpointFiles); err != nil {
			return fmt.Errorf("restore verified SQL checkpoint: %w", err)
		}
	}
	if len(suffix) != 0 {
		if err := expected.ApplyBatch(ctx, suffix); err != nil {
			return fmt.Errorf("apply certified SQL prefix: %w", err)
		}
	}
	if expected.Tip() != appliedTip {
		return fmt.Errorf("reconstructed SQL tip %d differs from actual applied tip %d", expected.Tip(), appliedTip)
	}
	want, err := expected.InspectSQLApplicationState(ctx)
	if err != nil {
		return fmt.Errorf("inspect reconstructed managed SQL: %w", err)
	}
	if actual.Digest != want.Digest {
		return fmt.Errorf("actual SQLite contents differ from verified checkpoint and certified WAL prefix at tip %d", appliedTip)
	}
	return nil
}

func (n *Node) newManagedStartupBucket(ctx context.Context) (*objectstore.MeteredBucket, error) {
	provider := objectstore.Provider(n.config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	finalPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
	return objectstore.NewBucketForNamespace(ctx, objectstore.Config{
		Provider: provider, FilesystemDir: n.config.ObjStoreDir, Prefix: n.config.ObjStorePrefix,
		Endpoint: n.config.ObjStoreEndpoint, Bucket: n.config.ObjStoreBucket, Region: n.config.ObjStoreRegion,
		Insecure: n.config.ObjStoreInsecure, MaxRetries: n.config.ObjStoreRetries,
		AccessKey: n.config.ObjStoreAccessKey, SecretKey: n.config.ObjStoreSecretKey, SessionToken: n.config.ObjStoreSessionToken,
		ServiceAccount: n.config.ObjStoreServiceAccount, AzureTenantID: n.config.ObjStoreAzureTenantID,
		AzureClientID: n.config.ObjStoreAzureClientID, AzureClientSecret: n.config.ObjStoreAzureClientSecret,
		AzureStorageAccount: n.config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: n.config.ObjStoreAzureStorageAccountKey,
		AzureConnectionString: n.config.ObjStoreAzureConnectionString,
	}, finalPrefix)
}

func loadPreparedAdoption(name string) (preparedAdoption, error) {
	data, err := readPreparedAdoption(name)
	if err != nil {
		return preparedAdoption{}, fmt.Errorf("prepared adoption is missing or invalid: %w", err)
	}
	var adoption preparedAdoption
	if err := json.Unmarshal(data, &adoption); err != nil || adoption.WALIdentity == "" || adoption.Membership == nil {
		return preparedAdoption{}, fmt.Errorf("prepared adoption is invalid")
	}
	return adoption, nil
}

func verifyAdoptedWALBase(core *quepaxa.Core, adoption preparedAdoption) error {
	if adoption.BaseSeal == nil {
		if core.CompactionFloor() != 0 {
			return fmt.Errorf("prepared WAL has an unrecorded checkpoint base")
		}
	} else {
		floor, root, ok := core.RecoveryRoot()
		prefix, prefixOK := core.PrefixHash(adoption.BaseSeal.Index)
		if !ok || floor != adoption.BaseSeal.Index || root != adoption.BaseSeal.RootHash || !prefixOK || prefix != adoption.BaseSeal.PrefixHash {
			return fmt.Errorf("prepared WAL checkpoint base mismatch")
		}
		if adoption.BaseDecision == nil {
			return fmt.Errorf("prepared checkpoint decision is missing")
		}
	}
	actual, err := core.MembershipHistory()
	if err != nil || !membershipHistoryHasPrefix(*adoption.Membership, actual) {
		return errors.Join(fmt.Errorf("prepared WAL membership history mismatch"), err)
	}
	return nil
}

func membershipHistoryHasPrefix(want, got quepaxa.MembershipRecord) bool {
	if !reflect.DeepEqual(want.Genesis, got.Genesis) || len(want.Transitions) > len(got.Transitions) {
		return false
	}
	for i := range want.Transitions {
		if !reflect.DeepEqual(want.Transitions[i], got.Transitions[i]) {
			return false
		}
	}
	return true
}

func (n *Node) verifyManagedPreparedRestore(ctx context.Context, prepared *recovery.VerifiedPreparedStartup) (preparedAdoption, error) {
	if n == nil || n.managedStartup == nil || prepared == nil || prepared.View() == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		return preparedAdoption{}, fmt.Errorf("prepared restore is not open")
	}
	view := prepared.View()
	if n.core.WALIdentity() == "" || n.core.Tip() != view.Tip() || n.material.Tip() != uint64(view.Tip()) {
		return preparedAdoption{}, fmt.Errorf("prepared WAL or materialized tip differs from verified source")
	}
	actualPrefix, ok := n.core.PrefixHash(view.Tip())
	if !ok || actualPrefix != view.PrefixHash() {
		return preparedAdoption{}, fmt.Errorf("prepared certified suffix differs from verified source")
	}
	for _, decision := range view.Decisions() {
		actual, ok := n.core.CertifiedValue(decision.Slot)
		if !ok || !reflect.DeepEqual(actual, decision) {
			return preparedAdoption{}, fmt.Errorf("prepared certified decision %d differs from verified source", decision.Slot)
		}
	}
	membership, err := n.core.MembershipHistory()
	if err != nil {
		return preparedAdoption{}, fmt.Errorf("verify prepared membership history: %w", err)
	}
	seal, decision, hasBase := view.RecoveryBase()
	adoption := preparedAdoption{Identity: n.managedStartup.Identity.Member, WALIdentity: n.core.WALIdentity(), Tip: view.Tip(), PrefixHash: view.PrefixHash(), Membership: &membership}
	if hasBase {
		if n.core.CompactionFloor() != seal.Index || decision.Slot == 0 {
			return preparedAdoption{}, fmt.Errorf("prepared WAL did not restore the verified checkpoint base")
		}
		if err := n.core.ValidateCheckpointBase(ctx, seal, decision); err != nil {
			return preparedAdoption{}, fmt.Errorf("verify restored checkpoint base: %w", err)
		}
		adoption.BaseSeal = &seal
		adoption.BaseDecision = &decision
	} else if n.core.CompactionFloor() != 0 {
		return preparedAdoption{}, fmt.Errorf("prepared WAL has a checkpoint base absent from verified source")
	}
	if view.Tip() > view.Base() {
		last, ok := n.core.CertifiedValue(view.Tip())
		if !ok {
			return preparedAdoption{}, fmt.Errorf("prepared suffix tip has no certified decision")
		}
		if err := n.material.ValidateTip(uint64(view.Tip()), last.Value); err != nil {
			return preparedAdoption{}, fmt.Errorf("prepared SQLite does not match certified suffix: %w", err)
		}
	} else if uint64(view.Base()) != n.material.Tip() {
		return preparedAdoption{}, fmt.Errorf("prepared SQLite does not match restored checkpoint index")
	}
	if err := verifyAdoptedWALBase(n.core, adoption); err != nil {
		return preparedAdoption{}, err
	}
	return adoption, nil
}

func (n *Node) verifyManagedAdoptionAfterOpen(ctx context.Context, adoption preparedAdoption) error {
	if n == nil || n.core == nil || n.material == nil || !n.opened.Load() || n.core.WALIdentity() != adoption.WALIdentity {
		return fmt.Errorf("managed adoption WAL is not open")
	}
	if err := verifyAdoptedWALBase(n.core, adoption); err != nil {
		return err
	}
	if adoption.BaseSeal != nil {
		if n.checkpoints == nil || adoption.BaseDecision == nil {
			return fmt.Errorf("managed adoption checkpoint verifier is unavailable")
		}
		if err := n.core.ValidateCheckpointBase(ctx, *adoption.BaseSeal, *adoption.BaseDecision); err != nil {
			return fmt.Errorf("revalidate managed checkpoint base: %w", err)
		}
	}
	if quepaxa.Slot(n.material.Tip()) > n.core.Tip() {
		return fmt.Errorf("managed SQLite tip is ahead of its certified WAL")
	}
	if n.material.Tip() > uint64(n.core.CompactionFloor()) {
		decision, ok := n.core.CertifiedValue(quepaxa.Slot(n.material.Tip()))
		if !ok {
			return fmt.Errorf("managed SQLite tip has no certified decision")
		}
		if err := n.material.ValidateTip(n.material.Tip(), decision.Value); err != nil {
			return err
		}
	}
	return nil
}

func checkpointMaterializerFiles(files []checkpoint.File) []materializer.CheckpointFile {
	result := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		result = append(result, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	return result
}

func certifiedSuffixThrough(core *quepaxa.Core, from, through quepaxa.Slot) ([]quepaxa.DecidedValue, error) {
	var values []quepaxa.DecidedValue
	for from <= through {
		page, _, err := core.DecisionsFrom(from, 256)
		if err != nil || len(page) == 0 {
			return nil, errors.Join(fmt.Errorf("certified SQL suffix is unavailable at slot %d", from), err)
		}
		last := from - 1
		for _, value := range page {
			if value.Slot > through {
				break
			}
			if value.Slot != last+1 {
				return nil, fmt.Errorf("certified SQL suffix has a gap at slot %d", last+1)
			}
			values = append(values, value)
			last = value.Slot
		}
		if last < from {
			return nil, fmt.Errorf("certified SQL suffix ends before slot %d", from)
		}
		from = last + 1
	}
	return values, nil
}

// recoverPreparedV1Alpha2 installs verified history in the actual local WAL.
// It does not open listeners, admit writes, or grant authority.
// The caller must control DataDir and prevent changes after loading prepared.
func (n *Node) recoverPreparedV1Alpha2(ctx context.Context, prepared *recovery.VerifiedPreparedStartup) (resultErr error) {
	return n.recoverPreparedV1Alpha2WithLock(ctx, prepared, nil)
}

func (n *Node) recoverPreparedV1Alpha2WithLock(ctx context.Context, prepared *recovery.VerifiedPreparedStartup, heldLock *qlog.LockFile) (resultErr error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.DataDir == "" || prepared == nil || prepared.View() == nil || n.opened.Load() {
		return fmt.Errorf("invalid non-serving prepared recovery")
	}
	dataDir, err := filepath.Abs(n.config.DataDir)
	if err != nil {
		return err
	}
	preparedDir, err := filepath.Abs(prepared.Directory())
	if err != nil || dataDir != preparedDir {
		return fmt.Errorf("prepared directory does not match DataDir")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if heldLock == nil {
		_, lock, err := qlog.Acquire(filepath.Join(dataDir, "qlog"))
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Release()) }()
	}
	localVoter, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	maxBytes := n.config.MaxWALBytes
	if maxBytes == 0 {
		maxBytes = 4 << 30
	}
	if maxBytes < 0 {
		return fmt.Errorf("invalid WAL limit")
	}
	if err := wal.SetMaxBytes(maxBytes); err != nil {
		return err
	}
	if configured, err := validateObjectStoreConfig(n.config); err != nil {
		return err
	} else if !configured {
		return fmt.Errorf("prepared startup requires a configured target store")
	}
	provider := objectstore.Provider(n.config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	newBucket := objectstore.NewBucketWithoutProvisioning
	if n.managedStartup != nil {
		finalPrefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
		newBucket = func(ctx context.Context, cfg objectstore.Config) (*objectstore.MeteredBucket, error) {
			return objectstore.NewBucketForNamespace(ctx, cfg, finalPrefix)
		}
	}
	bucket, err := newBucket(ctx, objectstore.Config{
		Provider: provider, FilesystemDir: n.config.ObjStoreDir, Prefix: n.config.ObjStorePrefix,
		Endpoint: n.config.ObjStoreEndpoint, Bucket: n.config.ObjStoreBucket, Region: n.config.ObjStoreRegion,
		Insecure: n.config.ObjStoreInsecure, MaxRetries: n.config.ObjStoreRetries,
		AccessKey: n.config.ObjStoreAccessKey, SecretKey: n.config.ObjStoreSecretKey, SessionToken: n.config.ObjStoreSessionToken,
		ServiceAccount: n.config.ObjStoreServiceAccount, AzureTenantID: n.config.ObjStoreAzureTenantID,
		AzureClientID: n.config.ObjStoreAzureClientID, AzureClientSecret: n.config.ObjStoreAzureClientSecret,
		AzureStorageAccount: n.config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: n.config.ObjStoreAzureStorageAccountKey,
		AzureConnectionString: n.config.ObjStoreAzureConnectionString, AzureUserAssignedID: n.config.ObjStoreAzureUserAssignedID,
	})
	if err != nil {
		return fmt.Errorf("open target store: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	if err := ensureVoterIdentityManaged(ctx, n.config, bucket, wal, localVoter, false, true); err != nil {
		return err
	}
	cluster := n.loadClusterConfig()
	transport := network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken)
	transport.SetAdminToken(n.config.AdminToken)
	defer func() { resultErr = errors.Join(resultErr, transport.Close()) }()
	newCore := quepaxa.New
	if n.config.Learner != nil {
		newCore = quepaxa.NewLearner
	}
	core, err := newCore(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport, EnableReconfiguration: n.config.EnableReconfiguration})
	if err != nil {
		return err
	}
	view := prepared.View()
	persistLegacyAdoption := func() error {
		if n.managedStartup != nil {
			return nil
		}
		membership, err := core.MembershipHistory()
		if err != nil {
			return err
		}
		return persistPreparedAdoption(dataDir, prepared.Identity(), core.WALIdentity(), view, &membership, "")
	}
	if core.Tip() > view.Tip() {
		return fmt.Errorf("existing WAL is ahead of prepared history; preserve it for restart")
	}
	if core.Tip() == view.Tip() {
		prefix, ok := core.PrefixHash(view.Tip())
		if !ok || prefix != view.PrefixHash() {
			return fmt.Errorf("existing WAL differs from prepared history")
		}
		return persistLegacyAdoption()
	}
	seal, decision, hasBase := view.RecoveryBase()
	if core.Tip() < view.Base() {
		if core.Tip() != 0 {
			return fmt.Errorf("existing WAL ends before prepared checkpoint base; preserve it for reconciliation")
		}
		if !hasBase {
			return fmt.Errorf("prepared history requires an unsupported generation base")
		}
		core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !reflect.DeepEqual(candidate, seal) {
				return fmt.Errorf("checkpoint differs from verified source")
			}
			return nil
		})
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return fmt.Errorf("install certified base: %w", err)
		}
	}
	if view.Base() > 0 {
		if !hasBase {
			return fmt.Errorf("prepared history requires an unsupported generation base")
		}
		prefix, ok := core.PrefixHash(view.Base())
		if !ok || prefix != seal.PrefixHash {
			return fmt.Errorf("existing WAL checkpoint prefix differs from prepared history")
		}
	}
	for _, value := range view.Decisions() {
		if value.Slot > core.Tip() {
			break
		}
		existing, ok := core.CertifiedValue(value.Slot)
		if !ok || !reflect.DeepEqual(existing, value) {
			return fmt.Errorf("existing WAL certified prefix differs from prepared history")
		}
	}
	for _, value := range view.Decisions() {
		if value.Slot <= core.Tip() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := core.AcceptCertifiedValues([]quepaxa.DecidedValue{value}); err != nil {
			return fmt.Errorf("install certified suffix: %w", err)
		}
	}
	prefix, ok := core.PrefixHash(view.Tip())
	if core.Tip() != view.Tip() || !ok || prefix != view.PrefixHash() {
		return fmt.Errorf("local WAL does not match prepared history")
	}
	return persistLegacyAdoption()
}

type preparedAdoption struct {
	Identity     recovery.PreparedStartupIdentity `json:"identity"`
	WALIdentity  string                           `json:"walIdentity"`
	Tip          quepaxa.Slot                     `json:"tip"`
	PrefixHash   [32]byte                         `json:"prefixHash"`
	BaseSeal     *quepaxa.CheckpointSeal          `json:"baseSeal,omitempty"`
	BaseDecision *quepaxa.DecidedValue            `json:"baseDecision,omitempty"`
	Membership   *quepaxa.MembershipRecord        `json:"membership,omitempty"`
	OriginDigest string                           `json:"originDigest,omitempty"`
}

func persistPreparedAdoption(dir string, identity recovery.PreparedStartupIdentity, walID string, view *recovery.ReadOnlyView, membership *quepaxa.MembershipRecord, originDigest string) error {
	if walID == "" {
		return fmt.Errorf("real WAL identity is missing")
	}
	if membership == nil {
		return fmt.Errorf("verified membership history is missing")
	}
	adoption := preparedAdoption{Identity: identity, WALIdentity: walID, Tip: view.Tip(), PrefixHash: view.PrefixHash(), Membership: membership, OriginDigest: originDigest}
	if seal, decision, ok := view.RecoveryBase(); ok {
		adoption.BaseSeal = &seal
		adoption.BaseDecision = &decision
	}
	want, err := json.Marshal(adoption)
	if err != nil {
		return err
	}
	name := filepath.Join(dir, "prepared-adoption.json")
	if existing, err := readPreparedAdoption(name); err == nil {
		if !bytes.Equal(existing, want) {
			return fmt.Errorf("prepared adoption identity mismatch")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "prepared-adoption.pending"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(want)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	if err := os.Link(filepath.Join(dir, "prepared-adoption.pending"), name); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("adoption outcome unknown: %w", err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		return fmt.Errorf("adoption outcome unknown: %w", err)
	}
	return nil
}

func readPreparedAdoption(name string) (data []byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16384 {
		return nil, fmt.Errorf("invalid adoption file")
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("adoption file changed")
	}
	data, err = io.ReadAll(io.LimitReader(f, 16385))
	if err != nil {
		return nil, err
	}
	if len(data) > 16384 {
		return nil, fmt.Errorf("adoption file too large")
	}
	return data, nil
}

// PreparedBindingDigests identifies the configured source and consensus inputs;
// no credentials are embedded in the returned digests.
func (n *Node) preparedBindingDigests() (string, string, error) {
	source, err := types.ManagedStorageBinding(*n.config)
	if err != nil {
		return "", "", err
	}
	config, _ := json.Marshal(struct {
		Cluster         string
		Node            string
		Members         []quepaxa.Member
		Learner         *quepaxa.Member
		Durability      string
		Reconfiguration bool
	}{string(n.config.ClusterID), string(n.config.NodeID), n.config.Members, n.config.Learner, string(n.config.ObjStoreDurability), n.config.EnableReconfiguration})
	return source, fmt.Sprintf("%x", sha256.Sum256(config)), nil
}

func (n *Node) PreparedStartupBindingDigests() (string, string, error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.ClusterID == "" {
		return "", "", fmt.Errorf("invalid prepared startup configuration")
	}
	return n.preparedBindingDigests()
}
