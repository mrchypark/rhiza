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
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sync"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
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
	ApplicationProtocol string `json:",omitempty"`
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

// internalApplicationBinding is published only after actual startup/adoption
// validation. It is a runtime identity, not a substitute for quorum or ACK.
type internalApplicationBinding struct {
	mu       sync.Mutex
	closing  bool
	active   sync.WaitGroup
	lifetime context.Context
	cancel   context.CancelFunc
	identity ManagedStartupIdentity
	policy   sqlpolicy.MaterializationPolicy
	window   uint64
	wal      string
	walStore *qlog.WAL
	member   quepaxa.Member
	core     *quepaxa.Core
	material *materializer.Materializer
}

// Borrowing is scoped to one operation, not permission to skip its runtime
// checks. Closing cancels borrowers and drains them before objects are closed.
func (n *Node) borrowInternalApplication(ctx context.Context) (*internalApplicationBinding, context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, ctx, nil, err
	}
	if !n.internalApplicationProfile() {
		return nil, ctx, nil, fmt.Errorf("internal application profile required")
	}
	b := n.internalApplication.Load()
	if b == nil {
		return nil, ctx, nil, fmt.Errorf("internal application binding unavailable")
	}
	b.mu.Lock()
	if b.closing || n.internalApplication.Load() != b {
		b.mu.Unlock()
		return nil, ctx, nil, fmt.Errorf("internal application is closing")
	}
	b.active.Add(1)
	b.mu.Unlock()
	op, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.lifetime, cancel)
	return b, op, func() { stop(); cancel(); b.active.Done() }, nil
}

func (n *Node) stopInternalApplication() *internalApplicationBinding {
	b := n.internalApplication.Swap(nil)
	if b != nil {
		b.mu.Lock()
		b.closing = true
		b.cancel()
		b.mu.Unlock()
	}
	return b
}

func (n *Node) internalApplicationProfile() bool {
	return n != nil && n.managedStartup != nil && (n.materializationPolicy() == sqlpolicy.ManagedRequestMaterializationPolicy || n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy)
}

func inspectManagedFileForPolicy(ctx context.Context, file string, window uint64, expected sqlpolicy.MaterializationPolicy, protocols ...string) (materializer.ManagedApplicationState, uint64, error) {
	protocol := ""
	if len(protocols) > 1 {
		return materializer.ManagedApplicationState{}, 0, sqlpolicy.ErrIncompatible
	}
	if len(protocols) == 1 {
		protocol = protocols[0]
	}
	switch expected {
	case sqlpolicy.ManagedGraphRequestMaterializationPolicy:
		// This inspects only the SQLite component. Paired native validation
		// remains mandatory at the checkpoint/source boundary.
		if protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
			return materializer.InspectManagedAppendOnlyGraphSQLFile(ctx, file, window)
		}
		if protocol != "" && protocol != sqlpolicy.ManagedGraphRequestApplicationProtocol {
			return materializer.ManagedApplicationState{}, 0, sqlpolicy.ErrIncompatible
		}
		return materializer.InspectManagedGraphSQLFile(ctx, file, window)
	case sqlpolicy.ManagedRequestMaterializationPolicy:
		if protocol == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol {
			return materializer.InspectManagedAppendOnlyRequestFile(ctx, file, window)
		}
		if protocol != "" && protocol != sqlpolicy.ManagedRequestApplicationProtocol {
			return materializer.ManagedApplicationState{}, 0, sqlpolicy.ErrIncompatible
		}
		return materializer.InspectManagedRequestFile(ctx, file, window)
	case sqlpolicy.ManagedSQLKVMaterializationPolicy:
		return materializer.InspectManagedApplicationFile(ctx, file, window)
	default:
		return materializer.ManagedApplicationState{}, 0, sqlpolicy.ErrIncompatible
	}
}

func managedSQLKVPolicy(policy sqlpolicy.MaterializationPolicy) bool {
	return policy == sqlpolicy.ManagedSQLKVMaterializationPolicy || policy == sqlpolicy.ManagedRequestMaterializationPolicy || policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy
}

func (n *Node) managedRuntimeHealth(ctx context.Context) error {
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return n.material.RuntimeHealth(ctx)
	}
	return n.material.Health(ctx)
}

func (n *Node) installInternalApplicationBinding(ctx context.Context) error {
	if !n.internalApplicationProfile() || n.internalApplication.Load() != nil || n.core == nil || n.material == nil || n.wal == nil || !n.opened.Load() {
		return fmt.Errorf("internal application binding cannot be installed")
	}
	i := n.managedStartup.Identity
	expectedProtocol := sqlpolicy.ManagedRequestApplicationProtocol
	switch {
	case n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy:
		expectedProtocol = sqlpolicy.ManagedGraphRequestApplicationProtocol
	}
	switch i.ApplicationProtocol {
	case sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol:
		expectedProtocol = i.ApplicationProtocol
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := n.material.ProjectionReadable(ctx); err != nil {
			return err
		}
	}
	if i.Member.Generation != 0 || i.ApplicationProtocol != expectedProtocol || n.material.MaterializationPolicy() != n.materializationPolicy() || n.core.WALIdentity() == "" {
		return fmt.Errorf("internal application startup identity or profile differs")
	}
	source, config, err := n.preparedBindingDigestsForProtocol(i.ApplicationProtocol)
	if err != nil || source != i.Member.SourceBinding || config != i.Member.ConfigDigest {
		return errors.Join(fmt.Errorf("internal application source binding differs"), err)
	}
	var member quepaxa.Member
	for _, candidate := range n.config.Members {
		if candidate.ID == n.config.NodeID {
			member = candidate
		}
	}
	if n.config.Learner != nil && n.config.Learner.ID == n.config.NodeID {
		member = *n.config.Learner
	}
	if member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) {
		return fmt.Errorf("internal application member identity unavailable")
	}
	if err := errors.Join(n.core.Health(), n.managedRuntimeHealth(ctx)); err != nil {
		return err
	}
	if fatal := n.wal.Capacity().Fatal; fatal != nil {
		return fatal
	}
	nonce, registered := n.wal.Identity()
	if !registered || hex.EncodeToString(nonce) != n.core.WALIdentity() {
		return fmt.Errorf("internal application WAL manifest is not registered")
	}
	lifetime, cancel := context.WithCancel(context.Background())
	b := &internalApplicationBinding{identity: i, policy: n.materializationPolicy(), window: n.receiptWindow(), wal: n.core.WALIdentity(), walStore: n.wal, member: member, core: n.core, material: n.material, lifetime: lifetime, cancel: cancel}
	if !n.internalApplication.CompareAndSwap(nil, b) {
		cancel()
		return fmt.Errorf("internal application binding already installed")
	}
	return nil
}

func (n *Node) checkInternalApplicationRuntime(ctx context.Context) (ManagedRuntimeState, error) {
	return n.checkInternalApplicationRuntimeObserved(ctx, true)
}

func (n *Node) checkInternalApplicationRuntimeOnce(ctx context.Context) (ManagedRuntimeState, error) {
	return n.checkInternalApplicationRuntimeObserved(ctx, false)
}

func (n *Node) checkInternalApplicationRuntimeObserved(ctx context.Context, retryProgress bool) (ManagedRuntimeState, error) {
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return ManagedRuntimeState{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return ManagedRuntimeState{}, err
	}
	if !n.internalApplicationProfile() || !n.opened.Load() || n.terminalRequested.Load() || n.wal == nil || n.config == nil || n.core == nil || n.material == nil {
		return ManagedRuntimeState{}, fmt.Errorf("internal application runtime unavailable")
	}
	b := n.internalApplication.Load()
	if b == nil || n.core != b.core || n.material != b.material || n.wal != b.walStore || n.managedStartup.Identity != b.identity || n.materializationPolicy() != b.policy || n.receiptWindow() != b.window || n.core.WALIdentity() != b.wal || n.material.MaterializationPolicy() != b.policy {
		return ManagedRuntimeState{}, fmt.Errorf("internal application runtime binding changed")
	}
	nonce, registered := n.wal.Identity()
	if !registered || hex.EncodeToString(nonce) != b.wal {
		return ManagedRuntimeState{}, fmt.Errorf("internal application WAL manifest identity changed")
	}
	source, config, err := n.preparedBindingDigestsForProtocol(b.identity.ApplicationProtocol)
	if err != nil || source != b.identity.Member.SourceBinding || config != b.identity.Member.ConfigDigest {
		return ManagedRuntimeState{}, errors.Join(fmt.Errorf("internal application current source differs"), err)
	}
	memberFound := false
	for _, member := range n.core.CurrentCluster().Members {
		if member.ID == b.member.ID {
			if member.PublicKey != b.member.PublicKey || member.WALIdentity != "" && member.WALIdentity != b.wal {
				return ManagedRuntimeState{}, fmt.Errorf("internal application current member differs")
			}
			memberFound = true
		}
	}
	if !memberFound && (n.managedInitiallyAdmitted.Load() || n.config.Learner == nil || n.config.Learner.ID != b.member.ID || n.config.Learner.PublicKey != b.member.PublicKey) {
		return ManagedRuntimeState{}, fmt.Errorf("internal application member is retired")
	}
	if err := errors.Join(n.core.Health(), n.managedRuntimeHealth(ctx)); err != nil {
		return ManagedRuntimeState{}, err
	}
	if fatal := n.wal.Capacity().Fatal; fatal != nil {
		return ManagedRuntimeState{}, fatal
	}
	var state ManagedRuntimeState
	if retryProgress {
		state, err = n.managedRuntimeStateWithContext(ctx)
		if err != nil {
			return state, err
		}
	} else {
		var stable bool
		state, stable = n.managedRuntimeStateOnce(ctx)
		if err := ctx.Err(); err != nil {
			return state, err
		}
		if !stable {
			return ManagedRuntimeState{}, network.ErrNotReady
		}
	}
	if state.WALIdentity != b.wal || state.AppliedTip > state.CertifiedTip {
		return state, fmt.Errorf("internal application applied prefix is invalid")
	}
	return state, nil
}

func (n *Node) admitInternalApplication(ctx context.Context) error {
	binding, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		if n != nil && n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("admitted", "runtime_pre", err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_runtime_pre", err)
	}
	defer release()
	if _, err := n.checkInternalApplicationRuntime(ctx); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("admitted", "runtime_pre", err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_runtime_pre", err)
	}
	if !n.managedInitiallyAdmitted.Load() || !n.core.CanParticipateReadIndex() {
		err := fmt.Errorf("internal application has no active voter admission")
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("admitted", "runtime_pre", err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_runtime_pre", err)
	}
	var verifiedBarrier quepaxa.Slot
	if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		verifiedBarrier, err = n.waitManagedAppendOnlyControlProof(ctx, binding)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionStageFailure("admitted", "pre_control", err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_pre_control", err)
		}
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := n.ensureManagedGraphInitialRoot(ctx); err != nil {
			logManagedGraphAdmissionStageFailure("admitted", "owned_wait", err)
			return logManagedSQLQueryFailure(ctx, "admission_owned_wait", err)
		}
	}
	target, _, err := n.core.ReadIndex(ctx)
	if err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("admitted", "read_index", err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_read_index", err)
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		localtesthooks.Hit("node:managed-control:post-readindex-owner-selected")
		if verifiedBarrier == 0 {
			verifiedBarrier, err = n.waitManagedAppendOnlyControlProof(ctx, binding)
		}
		if err != nil {
			logManagedGraphAdmissionTargetFailure("admitted", "owned_wait", target, err)
			return logManagedSQLQueryFailure(ctx, "admission_owned_wait", err)
		}
	}
	// A successful wait returns only the exact own-nonce barrier certificate.
	// Later recorder activity is not a reason to move this completed boundary.
	target = max(target, verifiedBarrier)
	// ReadIndex may observe a peer's certified tip before its Learned delivery
	// reaches this Core. Replay must not finish against an older local tip and
	// then reject the very target it was meant to synchronize. Wait only for
	// actual local certification; never reinterpret recorder data as decided.
	if err := n.core.WaitTip(ctx, target); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("admitted", "wait_tip", target, err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_wait_tip", err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("admitted", "replay", target, err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_replay", err)
	}
	state, err := n.checkInternalApplicationRuntime(ctx)
	if err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("admitted", "runtime_post", target, err)
		}
		return logManagedSQLQueryFailure(ctx, "admission_runtime_post", err)
	}
	// Without an owned proof, preserve recovery for a recorder/certificate gap.
	// Certified but unapplied decisions are handled separately below with a
	// fixed local prefix, so recorder data never becomes admission authority.
	if verifiedBarrier == 0 && state.RecorderTip > state.CertifiedTip && isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		verifiedBarrier, err = n.waitManagedAppendOnlyControlProof(ctx, binding)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionStageFailureRound("admitted", "pre_control", 2, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_pre_control", err)
		}
		target, _, err = n.core.ReadIndex(ctx)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionStageFailureRound("admitted", "read_index", 2, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_read_index", err)
		}
		target = max(target, verifiedBarrier)
		if err := n.core.WaitTip(ctx, target); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailureRound("admitted", "wait_tip", 2, target, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_wait_tip", err)
		}
		if err := n.replayLocalDecisions(ctx); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailureRound("admitted", "replay", 2, target, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_replay", err)
		}
		if localtesthooks.Enabled {
			localtesthooks.Hit("node:managed-admission:before-final-runtime-observation")
		}
		state, err = n.checkInternalApplicationRuntime(ctx)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailureRound("admitted", "runtime_post", 2, target, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_runtime_post", err)
		}
	}
	if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) && uint64(target) <= state.CertifiedTip &&
		(verifiedBarrier != 0 && state.AppliedTip < uint64(target) || verifiedBarrier == 0 && state.RecorderTip == state.CertifiedTip && state.AppliedTip < state.CertifiedTip) {
		// An owned proof needs replay only through its covered ReadIndex cut.
		// Otherwise retain the existing single captured certified-tip replay.
		through := quepaxa.Slot(state.CertifiedTip)
		if verifiedBarrier != 0 {
			through = target
		}
		if err := n.replayLocalDecisionsThrough(ctx, through); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailure("admitted", "fixed_replay", target, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_fixed_replay", err)
		}
		state, err = n.checkInternalApplicationRuntimeOnce(ctx)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailure("admitted", "runtime_post", target, err)
			}
			return logManagedSQLQueryFailure(ctx, "admission_runtime_post", err)
		}
	}
	historyIncomplete := state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip
	if verifiedBarrier != 0 {
		if err := ctx.Err(); err != nil {
			return logManagedSQLQueryFailure(ctx, "admission_runtime_post", err)
		}
		if binding.lifetime.Err() != nil || n.internalApplication.Load() != binding || !n.core.CanParticipateReadIndex() {
			return logManagedSQLQueryFailure(ctx, "admission_runtime_post", network.ErrNotReady)
		}
		historyIncomplete = state.CertifiedTip < uint64(target) || state.AppliedTip < uint64(target)
	}
	if historyIncomplete {
		if line := managedSQLQueryHistoryDiagnostic(ctx, uint64(target), state.RecorderTip, state.CertifiedTip, state.AppliedTip); line != "" {
			log.Print(line)
		}
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			log.Printf("MANAGED_GRAPH_ADMISSION_REFUSED target=%d recorded=%d certified=%d applied=%d", target, state.RecorderTip, state.CertifiedTip, state.AppliedTip)
			logManagedGraphHistoryRefused("admitted", uint64(target), state)
		}
		return logManagedSQLQueryFailure(ctx, "admission_unresolved_history", fmt.Errorf("internal application has unresolved certified history: target=%d recorded=%d certified=%d applied=%d", target, state.RecorderTip, state.CertifiedTip, state.AppliedTip))
	}
	return nil
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
	expectedPolicy    sqlpolicy.MaterializationPolicy
	idempotencyWindow uint64
	StorageRoot       string
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
	return n.ManagedBootstrapPlanDigestForProtocol(physicalBinding, evidenceFormat, "")
}

func (n *Node) ManagedBootstrapPlanDigestForProtocol(physicalBinding, evidenceFormat, protocol string) (string, error) {
	if n == nil || n.config == nil || n.config.Local || physicalBinding == "" || evidenceFormat == "" {
		return "", fmt.Errorf("invalid managed bootstrap binding")
	}
	source, _, err := n.preparedBindingDigests()
	if err != nil || source != physicalBinding {
		return "", errors.Join(fmt.Errorf("physical binding differs from configured storage"), err)
	}
	return types.ManagedBootstrapPlanDigestForProtocol(string(n.config.ClusterID), n.config.Members, physicalBinding, evidenceFormat, n.config.ObjStoreDurability, n.config.EnableReconfiguration, protocol)
}

func managedSQLQueryHistoryDiagnostic(ctx context.Context, target, recorded, certified, applied uint64) string {
	if managedSQLQueryDiagnostic(ctx, "admission_unresolved_history") == "" {
		return ""
	}
	return fmt.Sprintf("MANAGED_SQL_QUERY_HISTORY_REFUSED target=%d recorded=%d certified=%d applied=%d", target, recorded, certified, applied)
}

func managedNodeOpenDiagnostic(stage string) string {
	switch stage {
	case "prepared_config", "prepared_authority", "prepared_identity", "prepared_lock", "prepared_origin", "prepared_adoption", "prepared_wal", "prepared_materializer", "prepared_restore", "prepared_runtime_open",
		"adoption_identity", "adoption_wal_history", "adoption_sql_reconstruct", "adoption_sql_digest",
		"runtime_config", "runtime_optional_supervisor", "runtime_policy", "runtime_wal_lock", "runtime_wal_open", "runtime_source_open", "runtime_registration", "runtime_checkpoint_load", "runtime_archive_load", "runtime_materializer", "runtime_core_open", "runtime_archive_recover",
		"runtime_pair_reconstruct", "runtime_peer_open", "runtime_post_checkpoint", "prepared_runtime_adoption":
		return "MANAGED_HOST_OPEN_FAILURE stage=" + stage
	default:
		return ""
	}
}

func logManagedNodeOpenFailure(enabled bool, stage string, err error) error {
	if enabled && err != nil {
		if line := managedNodeOpenDiagnostic(stage); line != "" {
			log.Print(line)
		}
	}
	return err
}

// OpenManaged starts an owned WAL/core/materializer and private peers while
// application admission stays closed. The caller must Shutdown even when the
// later evidence CAS or admission fails.
func (n *Node) OpenManaged(ctx context.Context, cfg ManagedStartupConfig) (resultErr error) {
	openStage := "prepared_config"
	defer func() {
		logManagedNodeOpenFailure(isManagedAppendOnlyProtocol(cfg.Identity.ApplicationProtocol), openStage, resultErr)
	}()
	if n == nil || n.config == nil || n.config.Local || n.opened.Load() || n.preparedStartup != nil || n.managedStartup != nil || cfg.Checker == nil {
		return fmt.Errorf("managed startup requires a new non-Local node and live checker")
	}
	configCopy := CloneManagedRuntimeConfig(*n.config)
	n.config = &configCopy
	i := cfg.Identity
	policy, window, err := types.ManagedApplicationPolicy(i.ApplicationProtocol)
	if err != nil {
		return err
	}
	requestProtocol := i.ApplicationProtocol == sqlpolicy.ManagedRequestApplicationProtocol || i.ApplicationProtocol == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	graphProtocol := i.ApplicationProtocol == sqlpolicy.ManagedGraphRequestApplicationProtocol || i.ApplicationProtocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	if policy == sqlpolicy.ManagedRequestMaterializationPolicy && (i.Member.Generation != 0 || !requestProtocol || i.EvidenceFormat != i.ApplicationProtocol) {
		return fmt.Errorf("unsupported internal application generation or evidence format")
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy && (i.Member.Generation != 0 || !graphProtocol || i.EvidenceFormat != i.ApplicationProtocol) {
		return fmt.Errorf("unsupported Graph application generation or evidence format")
	}
	if managedSQLKVPolicy(policy) && (i.Member.Generation != 0 || i.EvidenceFormat != i.ApplicationProtocol) {
		return fmt.Errorf("unsupported managed application generation or evidence format")
	}
	cfg.expectedPolicy, cfg.idempotencyWindow = policy, window
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
	source, config, err := n.preparedBindingDigestsForProtocol(i.ApplicationProtocol)
	if err != nil {
		return err
	}
	if i.Member.SourceBinding != source || i.Member.ConfigDigest != config {
		return fmt.Errorf("managed storage or configuration binding mismatch")
	}
	bootstrap, err := n.ManagedBootstrapPlanDigestForProtocol(i.PhysicalBinding, i.EvidenceFormat, i.ApplicationProtocol)
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
	openStage = "prepared_authority"
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
	openStage = "prepared_identity"
	if cfg.Prepared != nil && (i.Fresh || cfg.Prepared.Identity() != i.Member) {
		return fmt.Errorf("managed prepared identity mismatch")
	}
	if cfg.Prepared != nil && (cfg.Prepared.View() == nil || cfg.Prepared.View().MaterializationPolicy() != policy) {
		return fmt.Errorf("managed prepared application profile mismatch")
	}
	// Publish once before restore/open can start runtime readers. The pointer,
	// expectedPolicy and idempotencyWindow stay fixed for this Node; adoption
	// and admission state still follow their existing lifecycle.
	n.managedStartup = &cfg
	var startupLock *qlog.LockFile
	var cleanStart bool
	defer func() {
		if startupLock != nil {
			resultErr = errors.Join(resultErr, startupLock.Release())
		}
	}()
	openStage = "prepared_lock"
	if cfg.Prepared != nil || !i.Fresh && !cfg.BootstrapMember {
		cleanStart, startupLock, err = qlog.Acquire(filepath.Join(n.config.DataDir, "qlog"))
		if err != nil {
			return fmt.Errorf("acquire managed startup lock: %w", err)
		}
	}
	openStage = "prepared_origin"
	if cfg.Prepared != nil {
		if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
			return err
		}
		openStage = "prepared_adoption"
		adoptionPath := filepath.Join(n.config.DataDir, "prepared-adoption.json")
		_, adoptionErr := os.Lstat(adoptionPath)
		if adoptionErr == nil {
			// A durable H receipt distinguishes a retry from first installation.
			// Verify it under the WAL lock before any exact-H preflight can reject
			// certified local suffix progress or inspect a partially replayed DB.
			if err := n.verifyPreparedAdoptionWithLock(ctx, i.Member); err != nil {
				return err
			}
			adoption, err := loadPreparedAdoption(adoptionPath)
			if err != nil {
				return err
			}
			view := cfg.Prepared.View()
			if view == nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() || adoption.OriginDigest != managedRepairOriginDigest(cfg.RepairOrigin) {
				return fmt.Errorf("prepared retry differs from durable adoption")
			}
			seal, decision, hasBase := view.RecoveryBase()
			baseMismatch := hasBase != (adoption.BaseSeal != nil) ||
				(hasBase && (!reflect.DeepEqual(seal, *adoption.BaseSeal) || adoption.BaseDecision == nil || !reflect.DeepEqual(decision, *adoption.BaseDecision))) ||
				(!hasBase && adoption.BaseDecision != nil)
			if baseMismatch {
				return fmt.Errorf("prepared retry checkpoint differs from durable adoption")
			}
			cfg.verifiedAdoption = &adoption
		} else if errors.Is(adoptionErr, os.ErrNotExist) {
			// Only an absent durable receipt permits the original exact-H install
			// path. Any present but unreadable or malformed receipt fails closed.
			openStage = "prepared_wal"
			if err := n.verifyPreparedWALBeforeRestore(ctx, cfg.Prepared); err != nil {
				return err
			}
			openStage = "prepared_materializer"
			if managedSQLKVPolicy(policy) {
				_, tip, err := inspectManagedFileForPolicy(ctx, filepath.Join(cfg.Prepared.Directory(), "sqlite.db"), window, policy, i.ApplicationProtocol)
				if err != nil || tip != uint64(cfg.Prepared.View().Tip()) {
					return errors.Join(fmt.Errorf("managed prepared application window or tip mismatch"), err)
				}
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
			openStage = "prepared_restore"
			if err := n.recoverPreparedV1Alpha2WithLock(ctx, cfg.Prepared, startupLock); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("inspect durable prepared adoption: %w", adoptionErr)
		}
	} else if !i.Fresh && !cfg.BootstrapMember {
		openStage = "prepared_origin"
		if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
			return err
		}
		openStage = "prepared_adoption"
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
	openStage = "prepared_origin"
	if err := validateManagedRepairOrigin(ctx, cfg.Checker, cfg.RepairOrigin); err != nil {
		return err
	}
	openStage = "prepared_runtime_open"
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
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy && localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:runtime_verify")
	}
	openStage = "prepared_runtime_adoption"
	postOpenFailure := func(err error) error { return errors.Join(err, n.abortOpen()) }
	if cfg.Prepared != nil && cfg.verifiedAdoption == nil {
		adoption, err := n.verifyManagedPreparedRestore(ctx, cfg.Prepared)
		if err != nil {
			return postOpenFailure(err)
		}
		if err := persistPreparedAdoption(n.config.DataDir, i.Member, n.core.WALIdentity(), cfg.Prepared.View(), adoption.Membership, managedRepairOriginDigest(cfg.RepairOrigin)); err != nil {
			return postOpenFailure(fmt.Errorf("persist verified prepared adoption: %w", err))
		}
		cfg.verifiedAdoption = &adoption
		if n.config.Learner != nil && isManagedAppendOnlyProtocol(i.ApplicationProtocol) {
			if localtesthooks.Enabled {
				localtesthooks.Hit("node:managed-prepared:adopted-before-suffix-catchup")
			}
			if err := n.catchUpPreparedArchiveSuffix(ctx, cfg.Prepared.View().Tip()); err != nil {
				return postOpenFailure(fmt.Errorf("catch up prepared learner certified suffix: %w", err))
			}
		}
	} else if cfg.verifiedAdoption != nil {
		if err := n.verifyManagedAdoptionAfterOpen(ctx, *cfg.verifiedAdoption); err != nil {
			return postOpenFailure(err)
		}
		if n.config.Learner != nil && isManagedAppendOnlyProtocol(i.ApplicationProtocol) {
			if err := n.replayAdoptedCertifiedSuffix(ctx, *cfg.verifiedAdoption); err != nil {
				return postOpenFailure(err)
			}
		}
	}
	if cfg.promotionReady != nil {
		close(cfg.promotionReady)
	}
	n.managedPeerVerified.Store(true)
	if n.internalApplicationProfile() {
		if err := n.installInternalApplicationBinding(ctx); err != nil {
			return postOpenFailure(err)
		}
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy && localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:open_complete")
	}
	return nil
}

// Ordinary nodes retain policy7. Managed selection is frozen before open and
// never inferred from files, a handoff Version, or successor configuration.
func (n *Node) materializationPolicy() sqlpolicy.MaterializationPolicy {
	if n.managedStartup != nil && n.managedStartup.expectedPolicy != 0 {
		return n.managedStartup.expectedPolicy
	}
	return sqlpolicy.LegacyMaterializationPolicy
}

func (n *Node) receiptWindow() uint64 {
	if n.managedStartup != nil && n.managedStartup.idempotencyWindow != 0 {
		return n.managedStartup.idempotencyWindow
	}
	return types.DefaultIdempotencyWindowSlots
}

func (n *Node) validateExecutionPolicy(value []byte) error {
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return types.ValidateManagedGraphExecutionPolicy(value)
	}
	if managedSQLKVPolicy(n.materializationPolicy()) {
		return types.ValidateManagedKVExecutionPolicy(value)
	}
	return types.ValidateExecutionPolicy(value)
}

func (n *Node) openMaterializer(path string, readers int) (*materializer.Materializer, error) {
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if n.managedApplicationProtocol() == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
			return materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, readers, n.receiptWindow())
		}
		return materializer.OpenManagedSQLKVGraphWithOriginals(path, readers, n.receiptWindow())
	}
	if n.internalApplicationProfile() {
		if n.managedApplicationProtocol() == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol {
			return materializer.OpenManagedSQLKVWithAppendOnlyOriginals(path, readers, n.receiptWindow())
		}
		return materializer.OpenManagedSQLKVWithOriginals(path, readers, n.receiptWindow())
	}
	if managedSQLKVPolicy(n.materializationPolicy()) {
		return materializer.OpenManagedSQLKV(path, readers, n.receiptWindow())
	}
	return materializer.Open(path, readers)
}

// Actual file semantics must precede every mixed Core checkpoint base install.
func (n *Node) validateCheckpointFiles(ctx context.Context, files []checkpoint.File, tip uint64) (resultErr error) {
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if len(files) != 2 || files[0].Role != checkpoint.RoleSQLite || files[1].Role != checkpoint.RoleGraphData {
			return fmt.Errorf("managed Graph checkpoint requires exact SQLite and Graph roles")
		}
		stage, err := os.MkdirTemp("", "rhiza-graph-checkpoint-validation-*")
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(stage)) }()
		mat, err := n.openMaterializer(filepath.Join(stage, "sqlite.db"), 1)
		if err != nil {
			return err
		}
		restore := make([]materializer.CheckpointFile, len(files))
		for i, file := range files {
			restore[i] = materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path}
		}
		if err := mat.RestoreCheckpoint(ctx, restore); err != nil {
			return errors.Join(err, mat.Close())
		}
		_, historyErr := mat.ManagedGraphHistory(ctx)
		if mat.Tip() != tip {
			historyErr = errors.Join(historyErr, fmt.Errorf("managed Graph checkpoint tip differs from certified base"))
		}
		return errors.Join(historyErr, mat.ProjectionReadable(ctx), mat.Close())
	}
	if !managedSQLKVPolicy(n.materializationPolicy()) {
		return nil
	}
	if len(files) != 1 || files[0].Role != checkpoint.RoleSQLite {
		return fmt.Errorf("mixed checkpoint requires exactly one SQLite file")
	}
	// The publisher's completed snapshot is deliberately adjacent to the live
	// database and retains its .rhiza-checkpoint-* name until publication ends.
	// Validate it through production restore in an isolated stage, just like the
	// Graph branch, without relaxing the live startup artifact guard.
	stage, err := os.MkdirTemp("", "rhiza-managed-checkpoint-validation-*")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(stage)) }()
	mat, err := n.openMaterializer(filepath.Join(stage, "sqlite.db"), 1)
	if err != nil {
		return err
	}
	restore := []materializer.CheckpointFile{{Role: materializer.CheckpointSQLite, Path: files[0].Path}}
	if err := mat.RestoreCheckpoint(ctx, restore); err != nil {
		return errors.Join(err, mat.Close())
	}
	_, inspectErr := mat.InspectManagedApplicationState(ctx)
	if mat.Tip() != tip {
		inspectErr = errors.Join(inspectErr, fmt.Errorf("mixed checkpoint applied tip differs from certified base"))
	}
	return errors.Join(inspectErr, mat.Close())
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

func (n *Node) managedRuntimeStateOnce(ctx context.Context) (ManagedRuntimeState, bool) {
	existing := n.managedStartup != nil && n.managedStartup.existingRegisteredWAL
	wal := n.core.WALIdentity()
	before := n.core.RuntimeProgressSnapshot()
	if localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-runtime-state:after-certified-tip")
		localtesthooks.HitManagedRuntimeStateContext(ctx)
	}
	applied := n.material.Tip()
	after := n.core.RuntimeProgressSnapshot()
	state := ManagedRuntimeState{WALIdentity: wal, CertifiedTip: uint64(after.CertifiedTip), AppliedTip: applied, RecorderTip: uint64(after.RecorderTip), ExistingRegisteredWAL: existing}
	return state, before.CertifiedTip == after.CertifiedTip
}

// managedRuntimeStateWithContext brackets the materializer tip with two
// observations from the same Core. If the certified prefix advanced during
// that interval, it discards the mixed sample and observes again under the
// caller's context. Recorder-only advancement remains visible and fail-closed.
func (n *Node) managedRuntimeStateWithContext(ctx context.Context) (ManagedRuntimeState, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ManagedRuntimeState{}, err
		}
		state, stable := n.managedRuntimeStateOnce(ctx)
		if !stable {
			continue
		}
		if err := ctx.Err(); err != nil {
			return state, err
		}
		return state, nil
	}
}

func (n *Node) ManagedState() (ManagedRuntimeState, error) {
	if n.internalApplicationProfile() {
		return n.checkInternalApplicationRuntimeOnce(context.Background())
	}
	if n == nil || n.managedStartup == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		return ManagedRuntimeState{}, fmt.Errorf("managed runtime is not open")
	}
	state, stable := n.managedRuntimeStateOnce(context.Background())
	if !stable {
		return ManagedRuntimeState{}, network.ErrNotReady
	}
	return state, nil
}

func (n *Node) managedStateWithContext(ctx context.Context) (ManagedRuntimeState, error) {
	if n.internalApplicationProfile() {
		return n.checkInternalApplicationRuntime(ctx)
	}
	if n == nil || n.managedStartup == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		return ManagedRuntimeState{}, fmt.Errorf("managed runtime is not open")
	}
	return n.managedRuntimeStateWithContext(ctx)
}

// EnsureInitialManagedAdmission preserves the first real quorum/live-authority
// boundary without requiring a new quorum round for already admitted local
// reads/status/readiness. It installs no caller-supplied permission.
func (n *Node) EnsureInitialManagedAdmission(ctx context.Context) error {
	_, scoped, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		if n != nil && n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("initial", "runtime_pre", err)
		}
		return err
	}
	defer release()
	if err := n.ensureInitialManagedAdmission(scoped); err != nil {
		return err
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy && !isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		return n.ensureManagedGraphInitialRoot(scoped)
	}
	return nil
}

// ensureInitialManagedAdmission is also used by the Graph publisher. It must
// not wait for that publisher's own initial-root completion.
func (n *Node) ensureInitialManagedAdmission(ctx context.Context) error {
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		if n != nil && n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			route := "initial"
			if n.managedInitiallyAdmitted.Load() {
				route = "admitted"
			}
			logManagedGraphAdmissionStageFailure(route, "runtime_pre", err)
		}
		return err
	}
	defer release()
	if _, err := n.checkInternalApplicationRuntime(ctx); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			route := "initial"
			if n.managedInitiallyAdmitted.Load() {
				route = "admitted"
			}
			logManagedGraphAdmissionStageFailure(route, "runtime_pre", err)
		}
		return err
	}
	if n.managedInitiallyAdmitted.Load() {
		return nil
	}
	return n.AdmitManaged(ctx)
}

// markManagedGraphInitialRootComplete is called only after startup or the
// publisher has authenticated the certified seal and paired root.
func (n *Node) markManagedGraphInitialRootComplete() {
	n.managedGraphInitialRootOnce.Do(func() { close(n.managedGraphInitialRootDone) })
}

func (n *Node) managedGraphInitialRootComplete() bool {
	select {
	case <-n.managedGraphInitialRootDone:
		return true
	default:
		return false
	}
}

// AdmitManaged performs a fresh quorum/replay round before checking current
// application authority. Failure preserves the paused runtime and its data.
func (n *Node) AdmitManaged(ctx context.Context) error {
	var binding *internalApplicationBinding
	if n.internalApplicationProfile() {
		borrowed, scoped, release, err := n.borrowInternalApplication(ctx)
		if err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionStageFailure("initial", "runtime_pre", err)
			}
			return err
		}
		defer release()
		binding = borrowed
		ctx = scoped
	}
	if n.internalApplicationProfile() && n.managedInitiallyAdmitted.Load() {
		return n.admitInternalApplication(ctx)
	}
	if n == nil || n.managedStartup == nil || !n.opened.Load() || n.core == nil || n.material == nil {
		err := fmt.Errorf("managed runtime is not open")
		if n != nil && n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("initial", "runtime_pre", err)
		}
		return err
	}
	n.managedAdmitted.Store(false)
	if binding != nil && isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		if err := n.waitManagedAppendOnlyControl(ctx, binding); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionStageFailure("initial", "pre_control", err)
			}
			return err
		}
	}
	target, _, err := n.core.ReadIndex(ctx)
	if err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionStageFailure("initial", "read_index", err)
		}
		return err
	}
	if binding != nil && isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		if err := n.core.WaitTip(ctx, target); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailure("initial", "wait_tip", target, err)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailure("initial", "wait_tip", target, err)
			}
			return err
		}
		binding.mu.Lock()
		bindingCurrent := !binding.closing && n.internalApplication.Load() == binding
		binding.mu.Unlock()
		if !bindingCurrent || !n.core.CanParticipateReadIndex() {
			if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				logManagedGraphAdmissionTargetFailure("initial", "runtime_post", target, network.ErrNotReady)
			}
			return network.ErrNotReady
		}
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("initial", "replay", target, err)
		}
		return err
	}
	state, err := n.managedRuntimeStateWithContext(ctx)
	if err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("initial", "runtime_post", target, err)
		}
		return err
	}
	if state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphHistoryRefused("initial", uint64(target), state)
		}
		return fmt.Errorf("managed runtime has unapplied or unresolved history")
	}
	if err := n.managedStartup.Checker.ValidateManagedAdmission(ctx, n.managedStartup.Identity, state); err != nil {
		if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			logManagedGraphAdmissionTargetFailure("initial", "authority", target, err)
		}
		return err
	}
	n.managedAdmitted.Store(true)
	// Initial application admission requires quorum/catch-up once per owned
	// runtime. Later failed quorum rounds cannot revoke safe local reads;
	// their live authority/history checks remain mandatory.
	n.managedInitiallyAdmitted.Store(true)
	n.startManagedGraphPublisherAfterAdmission(binding)
	return nil
}

func logManagedGraphAdmissionStageFailure(route, stage string, err error) {
	log.Printf("MANAGED_GRAPH_ADMISSION_FAILURE route=%s stage=%s read_target_known=false cause=%s", route, stage, managedMembershipCauseClass(err))
}

func logManagedGraphAdmissionTargetFailure(route, stage string, target quepaxa.Slot, err error) {
	log.Printf("MANAGED_GRAPH_ADMISSION_FAILURE route=%s stage=%s read_target_known=true target=%d cause=%s", route, stage, target, managedMembershipCauseClass(err))
}

func logManagedGraphAdmissionStageFailureRound(route, stage string, round int, err error) {
	log.Printf("MANAGED_GRAPH_ADMISSION_FAILURE route=%s stage=%s round=%d read_target_known=false cause=%s", route, stage, round, managedMembershipCauseClass(err))
}

func logManagedGraphAdmissionTargetFailureRound(route, stage string, round int, target quepaxa.Slot, err error) {
	log.Printf("MANAGED_GRAPH_ADMISSION_FAILURE route=%s stage=%s round=%d read_target_known=true target=%d cause=%s", route, stage, round, target, managedMembershipCauseClass(err))
}

func logManagedGraphHistoryRefused(route string, target uint64, state ManagedRuntimeState) {
	log.Printf("MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=%s read_target_known=true target=%d recorded=%d certified=%d applied=%d c_below_target=%t applied_differs_certified=%t recorded_above_certified=%t",
		route, target, state.RecorderTip, state.CertifiedTip, state.AppliedTip,
		state.CertifiedTip < target, state.AppliedTip != state.CertifiedTip, state.RecorderTip > state.CertifiedTip)
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
	core, err := newManagedReadOnlyCore(quepaxa.NodeID("prepared-wal-verifier"), *cluster, wal, n.config.EnableReconfiguration, n.materializationPolicy())
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

func newManagedReadOnlyCore(nodeID quepaxa.NodeID, cluster quepaxa.Cluster, wal *qlog.WAL, enableReconfiguration bool, expected sqlpolicy.MaterializationPolicy) (*quepaxa.Core, error) {
	var validateValue func([]byte) error
	if expected == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		validateValue = types.ValidateManagedGraphExecutionPolicy
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: nodeID, Cluster: cluster, WAL: wal, ValidateValue: validateValue})
	if err == nil || !enableReconfiguration {
		return core, err
	}
	// An old but otherwise compatible WAL may not yet contain the protocol
	// marker. Keep this preflight read-only; normal startup may add the marker
	// only after all existing SQL/WAL validation has passed.
	return quepaxa.NewObserver(quepaxa.Config{NodeID: nodeID, Cluster: cluster, WAL: wal, EnableReconfiguration: true, ValidateValue: validateValue})
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
	openStage := "adoption_identity"
	defer func() {
		logManagedNodeOpenFailure(n.managedStartup != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol), openStage, resultErr)
	}()
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
	openStage = "adoption_wal_history"
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
	core, err := newManagedReadOnlyCore(quepaxa.NodeID("managed-adoption-verifier"), *cluster, wal, n.config.EnableReconfiguration, n.materializationPolicy())
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
	openStage = ""
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
	openStage := "adoption_sql_reconstruct"
	defer func() {
		logManagedNodeOpenFailure(n.managedStartup != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol), openStage, resultErr)
	}()
	dbPath := filepath.Join(n.config.DataDir, "sqlite.db")
	if err := materializer.ValidateCleanRestoreState(dbPath); err != nil {
		return fmt.Errorf("managed materializer has an interrupted restore; preserve it: %w", err)
	}
	actual, appliedTip, err := n.inspectApplicationFile(ctx, dbPath)
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
		manager, err := checkpoint.NewManagerForPolicy(bucket, clusterPrefix, cacheDir, n.materializationPolicy(), 1)
		if err != nil {
			return err
		}
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
	return n.compareManagedFileAtTip(ctx, actual, appliedTip, checkpointFiles, suffix, filepath.Join(tmp, "expected.db"))
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
	actual, appliedTip, err := n.inspectApplicationFile(ctx, dbPath)
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
	return n.compareManagedFileAtTip(ctx, actual, appliedTip, checkpointFiles, suffix, filepath.Join(tmp, "expected.db"))
}

func (n *Node) inspectApplicationFile(ctx context.Context, path string) ([32]byte, uint64, error) {
	if managedSQLKVPolicy(n.materializationPolicy()) {
		state, tip, err := inspectManagedFileForPolicy(ctx, path, n.receiptWindow(), n.materializationPolicy(), n.managedApplicationProtocol())
		return state.Digest, tip, err
	}
	state, tip, err := materializer.InspectSQLApplicationFile(ctx, path)
	return state.Digest, tip, err
}

func (n *Node) compareManagedFileAtTip(ctx context.Context, actual [32]byte, appliedTip uint64, checkpointFiles []materializer.CheckpointFile, suffix []quepaxa.DecidedValue, expectedPath string) (resultErr error) {
	openStage := "adoption_sql_reconstruct"
	defer func() {
		logManagedNodeOpenFailure(n.managedStartup != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol), openStage, resultErr)
	}()
	expected, err := n.openMaterializer(expectedPath, 4)
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
	want, _, err := n.inspectApplicationState(ctx, expected)
	if err != nil {
		return fmt.Errorf("inspect reconstructed managed SQL: %w", err)
	}
	openStage = "adoption_sql_digest"
	if actual != want {
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
	prefix, ok := n.core.PrefixHash(adoption.Tip)
	if n.core.Tip() < adoption.Tip || !ok || prefix != adoption.PrefixHash {
		return fmt.Errorf("managed adoption certified prefix changed after open")
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

// replayAdoptedCertifiedSuffix resumes only the contiguous certified prefix
// already present in the adopted WAL. The bound is captured once; ordinary
// recorder progress must not extend startup recovery.
func (n *Node) replayAdoptedCertifiedSuffix(ctx context.Context, adoption preparedAdoption) error {
	if n == nil || n.core == nil || n.material == nil || !n.opened.Load() || n.core.WALIdentity() != adoption.WALIdentity {
		return fmt.Errorf("managed adopted suffix is unavailable")
	}
	if err := verifyAdoptedWALBase(n.core, adoption); err != nil {
		return err
	}
	through := n.core.Tip()
	if through < adoption.Tip {
		return fmt.Errorf("managed adopted WAL is behind its durable H receipt")
	}
	if n.core.RecorderTip() > through {
		return fmt.Errorf("managed adopted WAL has uncertified recorded progress beyond its fixed recovery prefix")
	}
	floor := n.core.CompactionFloor()
	applied := quepaxa.Slot(n.material.Tip())
	if applied < floor || applied > through {
		return fmt.Errorf("managed materialized tip is outside the certified recovery prefix")
	}
	for slot := applied + 1; slot <= through; slot++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		decision, ok := n.core.CertifiedValue(slot)
		if !ok || decision.Slot != slot {
			return fmt.Errorf("managed adopted certified suffix is unavailable at slot %d", slot)
		}
		if err := n.validateExecutionPolicy(decision.Value); err != nil {
			return fmt.Errorf("validate managed adopted suffix slot %d: %w", slot, err)
		}
	}
	if applied < through {
		if err := n.replayLocalDecisionsThrough(ctx, through); err != nil {
			return fmt.Errorf("replay managed adopted certified suffix through %d: %w", through, err)
		}
	}
	if quepaxa.Slot(n.material.Tip()) != through {
		return fmt.Errorf("managed adopted suffix replay stopped before fixed certified tip %d", through)
	}
	if through > floor {
		decision, ok := n.core.CertifiedValue(through)
		if !ok {
			return fmt.Errorf("managed adopted tip has no certified decision")
		}
		if err := n.material.ValidateTip(uint64(through), decision.Value); err != nil {
			return fmt.Errorf("validate managed adopted materialized tip: %w", err)
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
	if n == nil || prepared == nil || prepared.View() == nil || prepared.View().MaterializationPolicy() != n.materializationPolicy() {
		return fmt.Errorf("prepared application profile mismatch")
	}
	view := prepared.View()
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
	profile, profileErr := n.managedVoterProfileFor(n.config)
	if profileErr != nil {
		return profileErr
	}
	var identityErr error
	if profile == nil {
		identityErr = ensureVoterIdentityManaged(ctx, n.config, bucket, wal, localVoter, false, true)
	} else {
		identityErr = ensureVoterIdentityWithProfile(ctx, n.config, bucket, wal, localVoter, false, true, profile)
	}
	if identityErr != nil {
		return identityErr
	}
	cluster := n.loadClusterConfig()
	var transport *network.Transport
	peerProfile, err := n.managedAppendOnlyProfile()
	if err != nil {
		return err
	}
	if peerProfile != nil {
		transport, err = network.NewTransportForManagedProfile(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, *peerProfile)
	} else {
		transport, err = network.NewTransportForPolicy(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken, n.materializationPolicy())
	}
	if err != nil {
		return err
	}
	transport.SetAdminToken(n.config.AdminToken)
	defer func() { resultErr = errors.Join(resultErr, transport.Close()) }()
	var validateValue func([]byte) error
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		validateValue = types.ValidateManagedGraphExecutionPolicy
		if err := transport.ValidateManagedGraphWireBudget(); err != nil {
			return err
		}
	}
	newCore := quepaxa.New
	if n.config.Learner != nil {
		newCore = quepaxa.NewLearner
	}
	coreConfig := quepaxa.Config{NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport, EnableReconfiguration: n.config.EnableReconfiguration, ValidateValue: validateValue}
	if peerProfile != nil {
		coreConfig.ManagedAppendOnly, err = n.managedAppendOnlyCoreConfig()
		if err != nil {
			return err
		}
	}
	core, err := newCore(coreConfig)
	if err != nil {
		return err
	}
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
		if managedSQLKVPolicy(n.materializationPolicy()) {
			dir, err := os.MkdirTemp(dataDir, ".rhiza-prepared-base-*")
			if err != nil {
				return err
			}
			files, readErr := view.DownloadCheckpointFiles(ctx, dir)
			if readErr == nil {
				readErr = n.validateCheckpointFiles(ctx, files, uint64(view.Base()))
			}
			if err := errors.Join(readErr, os.RemoveAll(dir)); err != nil {
				return err
			}
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
	return n.preparedBindingDigestsForProtocol("")
}

func (n *Node) PreparedStartupBindingDigestsForProtocol(protocol string) (string, string, error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.ClusterID == "" {
		return "", "", fmt.Errorf("invalid prepared startup configuration")
	}
	return n.preparedBindingDigestsForProtocol(protocol)
}

func (n *Node) preparedBindingDigestsForProtocol(protocol string) (string, string, error) {
	source, err := types.ManagedStorageBinding(*n.config)
	if err != nil {
		return "", "", err
	}
	config, err := types.ManagedMemberConfigDigest(*n.config, protocol)
	return source, config, err
}

func (n *Node) PreparedStartupBindingDigests() (string, string, error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.ClusterID == "" {
		return "", "", fmt.Errorf("invalid prepared startup configuration")
	}
	return n.preparedBindingDigests()
}
