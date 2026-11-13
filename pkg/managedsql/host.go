// Package managedsql owns a private managed application runtime. It exposes no
// Node, raw network server, graph, notification, migration, or HTTP surface.
package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"reflect"
	"slices"
	"sync"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const EvidenceFormat = "rhiza-managed-sql/v1"

type RuntimeConfig = types.ExecutionConfig
type ExecuteRequest = network.ExecuteRequest
type ExecuteResponse = network.ExecuteResponse
type QueryRequest = network.QueryRequest
type QueryResponse = network.QueryResponse

type Config struct {
	StorageRoot string
	Runtime     RuntimeConfig
	Store       *operator.V1Alpha2StatusStore
	Ref         operator.V1Alpha2ClusterRef
	Plan        operator.V1Alpha2ApplicationInitPlan
	PodUID      string
	Prepared    *recovery.VerifiedPreparedStartup
	// RepairOrigin carries the exact immutable repair.json bytes, never a path
	// that could be replaced between verification and adoption.
	RepairOrigin []byte
}

type sqlEvidence struct {
	Format string `json:"format"`
	Digest string `json:"digest"`
}

type Host struct {
	mu sync.Mutex
	// replicaMu serializes both public membership entry points and the Host's
	// external scale coordinator. Status CAS remains the durable authority.
	replicaMu sync.Mutex
	// Initial admission is the only quorum-dependent setup step. Serialize it
	// independently so local reads and readiness never queue behind mutations.
	admissionMu sync.Mutex
	node        *node.Node
	cfg         Config
	identity    node.ManagedStartupIdentity
	authority   operator.V1Alpha2AuthorityView
	closed      bool
	// Immutable after Open publishes the Host. Close cancels admission waits
	// before acquiring mu, then retains the existing serialized Node drain.
	admissionLifetime context.Context
	stopAdmission     context.CancelFunc
	// recoveryTargets are caller-local selectors, never execution authority.
	recoveryTargets        map[network.RequestStatusRequest]requestRecoveryHint
	recoveryTargetOverflow bool
}

// Open returns an owned paused host after initialization, without waiting for
// quorum. Evidence/CAS failures after runtime creation return the host as well:
// callers must retain it for diagnosis/recovery or call Close.
func Open(ctx context.Context, cfg Config) (*Host, error) {
	openStage := "host_config"
	cfg.Runtime = node.CloneManagedRuntimeConfig(cfg.Runtime)
	cfg.RepairOrigin = bytes.Clone(cfg.RepairOrigin)
	if cfg.Runtime.ObjStoreProvider == "" {
		cfg.Runtime.ObjStoreProvider = "s3"
	}
	if err := operator.ValidateV1Alpha2ApplicationProfile(cfg.Plan.ApplicationProtocol, cfg.Plan.EvidenceFormat); err != nil {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("unsupported managed startup profile: %w", err))
	}
	if cfg.Store == nil || cfg.PodUID == "" {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("exact SQL initialization plan, store, and Pod UID required"))
	}
	if cfg.Plan.ApplicationProtocol != "" && cfg.Plan.Generation != 0 {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("mixed managed generation is not supported"))
	}
	if cfg.Runtime.ObjStoreDurability == "" {
		cfg.Runtime.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	}
	if cfg.Runtime.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("managed SQL requires configured before-ack durability"))
	}
	openStage = "host_binding"
	namespace, err := node.ManagedNamespaceFor(cfg.StorageRoot, cfg.Ref.UID, cfg.Plan.Generation, cfg.Plan.AssignmentID, string(cfg.Runtime.ClusterID))
	if err != nil || cfg.Runtime.ObjStorePrefix != namespace.ConfigPrefix {
		return nil, logManagedHostOpenFailure(openStage, errors.Join(fmt.Errorf("installed runtime final namespace differs from exact assignment"), err))
	}
	cluster, err := types.ManagedClusterID(cfg.Ref.UID, cfg.Plan.Generation)
	memberID, memberErr := types.ManagedNodeID(cfg.PodUID)
	if err != nil || memberErr != nil || cfg.Runtime.ClusterID != cluster || cfg.Runtime.NodeID != memberID {
		return nil, logManagedHostOpenFailure(openStage, errors.Join(fmt.Errorf("runtime member identity differs from CR generation or actual Pod UID"), err, memberErr))
	}
	h := &Host{cfg: cfg, node: node.New(&cfg.Runtime)}
	h.admissionLifetime, h.stopAdmission = context.WithCancel(context.Background())
	source, member, err := h.node.PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	common, err := h.node.ManagedBootstrapPlanDigestForProtocol(source, cfg.Plan.EvidenceFormat, cfg.Plan.ApplicationProtocol)
	if err != nil || cfg.Plan.PhysicalBinding != source || cfg.Plan.BootstrapPlanDigest != common || cfg.Plan.ClusterUID != cfg.Ref.UID {
		return nil, logManagedHostOpenFailure(openStage, errors.Join(fmt.Errorf("installed runtime differs from initialization plan"), err))
	}
	openStage = "host_plan"
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
	if err != nil || initial.Assignment().Plan != cfg.Plan {
		return nil, logManagedHostOpenFailure(openStage, errors.Join(fmt.Errorf("live initialization assignment mismatch"), err))
	}
	status, err := cfg.Store.Load(ctx, cfg.Ref)
	if err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	openStage = "host_bootstrap"
	bootstrap, err := status.BootstrapPlan()
	if err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	if bootstrap.ClusterID != cfg.Runtime.ClusterID || bootstrap.PhysicalBinding != source || bootstrap.EvidenceFormat != cfg.Plan.EvidenceFormat || bootstrap.Durability != cfg.Runtime.ObjStoreDurability || bootstrap.Reconfiguration != cfg.Runtime.EnableReconfiguration {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("installed runtime differs from full bootstrap plan"))
	}
	initialMember := false
	for _, m := range bootstrap.Members {
		if m.PodUID == cfg.PodUID && m.Member.ID == cfg.Runtime.NodeID && m.Member.PublicKey == quepaxa.PublicKey(network.PeerPublicKey(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, cfg.Runtime.PeerToken)) {
			initialMember = true
		}
	}
	if cfg.Runtime.Learner == nil && !initialMember {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("exact initial Pod/member/key is not in bootstrap plan"))
	}
	openStage = "host_prepared_identity"
	if cfg.Prepared != nil && cfg.Runtime.Learner != nil {
		admission, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
		if err != nil {
			return nil, logManagedHostOpenFailure(openStage, err)
		}
		if repair, ok := admission.Repair(); ok && repairPlanMatchesConfiguredLearner(repair.Plan, cfg) {
			if repair.State != "Closed" {
				return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("prepared repair learner requires the exact current closed repair"))
			}
			if len(cfg.RepairOrigin) == 0 {
				return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("closed repair learner requires immutable repair origin"))
			}
		}
	}
	if initial.Assignment().State == "Unused" && !initialMember {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("only an exact initial member may consume E0"))
	}
	openStage = "host_authority"
	h.authority = initial.Authority()
	ever := initial.EverApplicationReserved()
	if h.authority.Generation != cfg.Plan.Generation || h.authority.Binding.DeploymentUID != cfg.Plan.DeploymentUID || h.authority.Binding.StorageID != cfg.Plan.StorageID {
		return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("current authority differs from installed generation and storage binding"))
	}
	h.identity = node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: source, ConfigDigest: member}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: source, BootstrapPlanDigest: common, EvidenceFormat: cfg.Plan.EvidenceFormat, ApplicationProtocol: cfg.Plan.ApplicationProtocol}
	openStage = "host_prepared_identity"
	if cfg.Prepared != nil {
		if cfg.Prepared.Identity() != h.identity.Member {
			return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("prepared handoff differs from exact configured execution identity"))
		}
		h.identity.Fresh = false
	}
	openStage = "host_authority"
	if err := h.ValidateManagedStartup(ctx, h.identity); err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	openStage = "host_source"
	birthAuthority, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
	if err != nil || birthAuthority.Assignment().Plan != cfg.Plan || birthAuthority.Authority() != h.authority {
		return nil, logManagedHostOpenFailure(openStage, errors.Join(fmt.Errorf("birth assignment changed"), err))
	}
	emptySource, err := h.node.PrepareManagedSQLSource(ctx, h.identity, birthAuthority.Assignment().State == "Unused" && !birthAuthority.EverApplicationReserved() && cfg.Prepared == nil)
	if err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	h.identity.Fresh = emptySource && !ever && cfg.Prepared == nil && initialMember
	if localtesthooks.Enabled && h.identity.Fresh && cfg.Plan.ApplicationProtocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
		localtesthooks.Hit("managed-host:graph-bootstrap:source-checked")
	}
	graphBootstrap := cfg.Plan.ApplicationProtocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	if graphBootstrap && initial.Assignment().State == "Unused" {
		localtesthooks.Hit("managed-host:graph-bootstrap:before-unused-source-check")
		initial, err = h.loadStartupInitialization(ctx)
		if err != nil {
			return nil, logManagedHostOpenFailure(openStage, err)
		}
		ever = initial.EverApplicationReserved()
	}
	if h.identity.Fresh && initial.Assignment().State == "Consumed" {
		a, err := h.live(ctx)
		if err != nil {
			return nil, logManagedHostOpenFailure(openStage, err)
		}
		reserved := a.EverApplicationReserved()
		if _, pending := a.Pending(); reserved || pending {
			h.identity.Fresh = false
		}
	}
	if initial.Assignment().State == "Unused" {
		if ever {
			return nil, logManagedHostOpenFailure(openStage, fmt.Errorf("reserved application history prevents E0 consumption"))
		}
		if err := h.node.VerifyManagedSQLFreshSource(ctx, h.identity); err != nil {
			return nil, logManagedHostOpenFailure(openStage, err)
		}
	}
	openStage = "host_node_open"
	if err := h.node.OpenManaged(ctx, node.ManagedStartupConfig{StorageRoot: cfg.StorageRoot, PeerRecovery: true, Identity: h.identity, Checker: h, Prepared: cfg.Prepared, RepairOrigin: cfg.RepairOrigin, BootstrapMember: cfg.Runtime.Learner == nil}); err != nil {
		return nil, logManagedHostOpenFailure(openStage, err)
	}
	openStage = "host_initialization"
	if initial.Assignment().State == "Unused" {
		if graphBootstrap {
			localtesthooks.Hit("managed-host:graph-bootstrap:before-unused-evidence")
			initial, err = h.loadStartupInitialization(ctx)
			if err != nil {
				return h, logManagedHostOpenFailure(openStage, err)
			}
			if initial.Assignment().State == "Consumed" {
				// Do not inspect a now-live runtime as empty or attempt another
				// E0. Admission remains an independent live check.
				if _, err := h.live(ctx); err != nil {
					return h, logManagedHostOpenFailure(openStage, err)
				}
				return h, nil
			}
		}
		evidence, err := h.inspectEvidence(ctx, true, false)
		if err != nil {
			return h, logManagedHostOpenFailure(openStage, err)
		}
		liveInit, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
		if err != nil {
			return h, logManagedHostOpenFailure(openStage, err)
		}
		if liveInit.EverApplicationReserved() {
			return h, logManagedHostOpenFailure(openStage, fmt.Errorf("reservation raced initial E0 inspection"))
		}
		if err := h.node.VerifyManagedSQLFreshSource(ctx, h.identity); err != nil {
			return h, logManagedHostOpenFailure(openStage, err)
		}
		_, _, err = operator.CommitV1Alpha2ApplicationInit(ctx, cfg.Store, initial, cfg.Plan, evidence)
		if err != nil {
			live, e := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
			if e != nil || live.Assignment().Plan != cfg.Plan || live.Assignment().State != "Consumed" || live.Assignment().InitialEvidenceDigest != evidenceHash(evidence) {
				return h, logManagedHostOpenFailure(openStage, errors.Join(err, e))
			}
			// Matching consumption establishes initialization only. Admission is
			// always loaded again and local state checked by Ready/Execute.
		}
	}
	return h, nil
}

func (h *Host) loadStartupInitialization(ctx context.Context) (operator.V1Alpha2ApplicationInitSnapshot, error) {
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, h.cfg.Store, h.cfg.Ref)
	if err != nil {
		return init, err
	}
	if init.Assignment().Plan != h.cfg.Plan || init.Authority() != h.authority {
		return init, fmt.Errorf("live initialization binding changed")
	}
	return init, nil
}

func managedHostOpenDiagnostic(stage string) string {
	switch stage {
	case "host_config", "host_binding", "host_plan", "host_bootstrap", "host_authority", "host_prepared_identity", "host_source", "host_node_open", "host_initialization":
		return "MANAGED_HOST_OPEN_FAILURE stage=" + stage
	default:
		return ""
	}
}

func logManagedHostOpenFailure(stage string, err error) error {
	if err != nil {
		if line := managedHostOpenDiagnostic(stage); line != "" {
			log.Print(line)
		}
	}
	return err
}

func repairPlanMatchesConfiguredLearner(plan operator.V1Alpha2RepairPlan, cfg Config) bool {
	return cfg.Runtime.Learner != nil && plan.ClusterUID == cfg.Ref.UID && plan.Generation == cfg.Plan.Generation && plan.DeploymentUID == cfg.Plan.DeploymentUID && plan.StorageID == cfg.Plan.StorageID && plan.BootstrapPlanDigest == cfg.Plan.BootstrapPlanDigest && plan.Replacement.PodUID == cfg.PodUID && plan.Replacement.NodeID == cfg.Runtime.NodeID && plan.Replacement.PublicKey == cfg.Runtime.Learner.PublicKey && plan.Replacement.PeerURL == cfg.Runtime.Learner.PeerURL
}

func evidenceHash(raw json.RawMessage) string {
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func (h *Host) live(ctx context.Context) (operator.V1Alpha2AdmissionSnapshot, error) {
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, h.cfg.Store, h.cfg.Ref)
	if err != nil || initial.Assignment().Plan != h.cfg.Plan || initial.Assignment().State != "Consumed" {
		return operator.V1Alpha2AdmissionSnapshot{}, errors.Join(fmt.Errorf("current consumed initialization binding changed"), err)
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, h.cfg.Store, h.cfg.Ref)
	if err != nil {
		return a, err
	}
	if a.Authority() != h.authority || len(a.Transition()) > 0 && !bytes.Equal(bytes.TrimSpace(a.Transition()), []byte("null")) {
		return a, fmt.Errorf("managed authority or transition changed")
	}
	return a, nil
}

// ValidateManagedGraphBootstrapControl verifies live authority for an exact
// original opening a certified control prefix before the first producer root.
func (h *Host) ValidateManagedGraphBootstrapControl(ctx context.Context, i node.ManagedStartupIdentity) error {
	// Open already checked this exact Pod UID, node ID and public key
	// against the original plan before installing h.identity.
	if i != h.identity || i.Member.Generation != 0 || h.cfg.Runtime.Learner != nil || h.cfg.Prepared != nil {
		return fmt.Errorf("managed Graph control bootstrap requires the original startup identity")
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if _, pending := a.Pending(); pending || a.EverApplicationReserved() || a.SQLReservationsClosed() {
		return fmt.Errorf("managed Graph control bootstrap requires no application reservation or closed admission")
	}
	if _, repair := a.Repair(); repair {
		return fmt.Errorf("managed Graph control bootstrap requires no repair history")
	}
	return ctx.Err()
}

func (h *Host) ValidateManagedStartup(ctx context.Context, i node.ManagedStartupIdentity) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	s, err := operator.LoadV1Alpha2ApplicationInit(ctx, h.cfg.Store, h.cfg.Ref)
	if err != nil || s.Assignment().Plan != h.cfg.Plan || s.Authority() != h.authority {
		return errors.Join(fmt.Errorf("live initialization binding changed"), err)
	}
	if s.Assignment().State == "Unused" {
		if s.EverApplicationReserved() {
			return fmt.Errorf("unused assignment cannot have application reservation history")
		}
		return nil
	}
	_, err = h.live(ctx)
	if err != nil {
		return err
	}
	if i.Fresh {
		a, err := h.live(ctx)
		if err != nil {
			return err
		}
		if _, pending := a.Pending(); a.EverApplicationReserved() || pending {
			// Keep the owner and private peers alive; live application admission
			// below requires actual history rather than this stale startup hint.
			return nil
		}
		if i.ApplicationProtocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
			// Once E0 is consumed, another original may publish a READ
			// between source inspection and registration. Startup authenticates
			// a producer root or checks the actual rootless Core prefix; the
			// public archive restore view intentionally requires a locator.
			return ctx.Err()
		}
		return h.node.VerifyManagedSQLFreshSource(ctx, i)
	}
	if h.cfg.Runtime.Learner != nil && h.cfg.Prepared != nil {
		a, err := h.live(ctx)
		if err != nil {
			return err
		}
		r, ok := a.Repair()
		if !ok && len(h.cfg.RepairOrigin) == 0 {
			// Trusted prepared startup is also used outside managed replacement.
			return nil
		}
		if !ok || r.PreparedHandoff == nil || r.Plan.Replacement.PodUID != h.cfg.PodUID || r.Plan.Replacement.NodeID != h.cfg.Runtime.NodeID || r.Plan.Replacement.PublicKey != h.cfg.Runtime.Learner.PublicKey || r.Plan.Replacement.PeerURL != h.cfg.Runtime.Learner.PeerURL || r.PreparedHandoff.PhysicalBinding != i.PhysicalBinding || r.PreparedHandoff.SourceRoot != h.cfg.StorageRoot || r.PreparedHandoff.SourcePrefix != path.Join(h.cfg.Runtime.ObjStorePrefix, string(h.cfg.Runtime.ClusterID)) || !bytes.Equal(r.PreparedHandoff.Head, h.cfg.Prepared.View().CapturedHead()) || (r.State != "Closed" && r.State != "Completed") {
			return fmt.Errorf("prepared learner lacks exact live repair handoff")
		}
		if r.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove {
			view := h.cfg.Prepared.View()
			if r.PreparedHandoff.PreparedConfig == nil || uint64(r.PreparedHandoff.PreparedConfig.ConfigID) != r.Plan.ExpectedConfigID || uint64(view.Tip()) != r.PreparedHandoff.PreparedSlot || fmt.Sprintf("%x", view.PrefixHash()) != r.PreparedHandoff.PreparedHash {
				return fmt.Errorf("prepared learner source differs from exact ADD-first H")
			}
		}
	}
	return nil
}

func managedInitialAuthorityDiagnostic(leaf string, err error) string {
	if err == nil {
		return ""
	}
	switch leaf {
	case "identity", "live", "pending_closed", "evidence":
		return "MANAGED_INITIAL_AUTHORITY_FAILURE leaf=" + leaf
	default:
		return ""
	}
}
func (h *Host) logInitialAuthorityFailure(leaf string, err error) error {
	if h != nil && h.internalRequests() {
		if line := managedInitialAuthorityDiagnostic(leaf, err); line != "" {
			log.Print(line)
		}
	}
	return err
}

func (h *Host) ValidateManagedAdmission(ctx context.Context, i node.ManagedStartupIdentity, state node.ManagedRuntimeState) error {
	if i != h.identity {
		return h.logInitialAuthorityFailure("identity", fmt.Errorf("execution identity changed"))
	}
	a, err := h.live(ctx)
	if err != nil {
		return h.logInitialAuthorityFailure("live", err)
	}
	if _, pending := a.Pending(); pending || a.SQLReservationsClosed() {
		return h.logInitialAuthorityFailure("pending_closed", fmt.Errorf("initial admission has pending control or closed repair"))
	}
	localtesthooks.Hit("managed-host:initial-authority:before-evidence")
	if h.internalRequests() {
		err = h.node.ValidateManagedInitialAdmission(ctx, i, state)
	} else {
		_, err = h.inspectEvidence(ctx, false, false)
	}
	return h.logInitialAuthorityFailure("evidence", err)
}

func (h *Host) ValidateManagedPeerLearn(ctx context.Context, i node.ManagedStartupIdentity) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	return ctx.Err()
}

func (h *Host) ValidateManagedSQLRead(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState) error {
	return h.validateManagedState(ctx, i, runtime, false)
}

func (h *Host) validateManagedState(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState, _ bool) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	return ctx.Err()
}

func (h *Host) ValidateManagedSQLWrite(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState, req network.ExecuteRequest) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return network.ValidateExecuteRequest(req)
}

func (h *Host) ValidateManagedSQLRecovery(ctx context.Context, i node.ManagedStartupIdentity, req network.ExecuteRequest) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return network.ValidateExecuteRequest(req)
}

func (h *Host) Ready(ctx context.Context) error {
	operationCtx, n, release, err := h.localReadNode(ctx)
	if err != nil {
		return err
	}
	defer release()
	return n.CheckManagedLocalRead(operationCtx)
}

// RemoveRepairVoter excludes the old voter from consensus only. It does not
// claim physical process death, stop old I/O, publish a learner handoff, or
// complete/open the logical repair after its REMOVE-only intermediate state.
func (h *Host) RemoveRepairVoter(ctx context.Context, repairID string) error {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return fmt.Errorf("SQL host closed")
	}
	r, err := h.closedRepair(ctx, repairID)
	if err != nil {
		return err
	}
	if r.Plan.Sequence == operator.V1Alpha2RepairAdd {
		return fmt.Errorf("add-only enrollment has no voter to remove")
	}
	expectedConfig := r.Plan.ExpectedConfigID
	if r.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove {
		if r.AddCompletion == nil || r.AddCompletion.Kind != "Terminal" || r.AddCompletion.ConfigID != r.Plan.ExpectedConfigID+1 {
			return fmt.Errorf("verified ADD terminal required before removing old voter")
		}
		if r.PreparedHandoff == nil {
			return fmt.Errorf("exact ADD-first source required before removing old voter")
		}
		registered, err := h.node.ManagedRepairSnapshot(ctx, r.Plan.Replacement.NodeID, r.Plan.Replacement.PublicKey)
		if err != nil || registered.LearnerWALIdentity == "" {
			return errors.Join(fmt.Errorf("registered ADD-first voter required before removing old voter"), err)
		}
		request, err := managedRepairAddRequest(r, r.PreparedHandoff, registered.LearnerWALIdentity)
		if err != nil {
			return err
		}
		proof, err := h.node.VerifyManagedRepairAddition(ctx, request)
		if err != nil {
			return err
		}
		completion, err := managedRepairAdditionCompletion(r.Plan, *r.PreparedHandoff, proof)
		if err != nil || !reflect.DeepEqual(completion, *r.AddCompletion) {
			return errors.Join(fmt.Errorf("actual ADD terminal differs from recorded proof"), err)
		}
		expectedConfig = r.AddCompletion.ConfigID
	}
	request := node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(expectedConfig), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
	return h.node.ChangeManagedMembership(ctx, request)
}

// CaptureRepairHandoff pins the verified source in the same closed operation.
// ADD-first replacement captures after REMOVE; add-only enrollment captures
// the current roster before its sole ADD. A later live HEAD is never
// substituted for an already captured one.
func (h *Host) CaptureRepairHandoff(ctx context.Context, repairID string) (operator.V1Alpha2RepairHandoff, error) {
	a, err := h.live(ctx)
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	r, err := h.closedRepair(ctx, repairID)
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	if r.PreparedHandoff != nil {
		return *r.PreparedHandoff, nil
	}
	var source node.ManagedRepairSource
	if r.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove || r.Plan.Sequence == operator.V1Alpha2RepairAdd {
		request := node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipAdd, AddOnly: r.Plan.Sequence == operator.V1Alpha2RepairAdd, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
		source, err = h.node.CaptureManagedPreparedRepairSource(ctx, request)
	} else {
		request := node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
		source, err = h.node.CaptureManagedRepairSource(ctx, request)
	}
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	namespace, err := node.ManagedNamespaceFor(h.cfg.StorageRoot, h.cfg.Ref.UID, r.Plan.Generation, h.cfg.Plan.AssignmentID, string(h.cfg.Runtime.ClusterID))
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	capture := operator.V1Alpha2RepairHandoff{PlanDigest: r.Plan.Digest(), SourceRoot: h.cfg.StorageRoot, SourcePrefix: namespace.FinalPrefix, PhysicalBinding: h.identity.PhysicalBinding, Head: source.Head, HeadHash: fmt.Sprintf("%x", source.HeadHash)}
	if r.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove || r.Plan.Sequence == operator.V1Alpha2RepairAdd {
		if source.PreparedConfig == nil || source.PreparedSlot == 0 || source.PreparedHash == ([32]byte{}) {
			return operator.V1Alpha2RepairHandoff{}, fmt.Errorf("verified ADD-first prepared source required")
		}
		capture.PreparedConfig = source.PreparedConfig
		capture.PreparedSlot = uint64(source.PreparedSlot)
		capture.PreparedHash = fmt.Sprintf("%x", source.PreparedHash)
	} else {
		capture.RemoveTerminalSlot = uint64(source.RemoveTerminal.Slot)
		capture.RemoveTerminalHash = fmt.Sprintf("%x", source.RemoveTerminal.Hash)
		capture.Survivors = source.Survivors
	}
	_, observation, err := operator.CaptureV1Alpha2RepairHandoff(ctx, h.cfg.Store, a, h.authority, capture)
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	if observation != operator.V1Alpha2CASApplied {
		return operator.V1Alpha2RepairHandoff{}, fmt.Errorf("repair capture CAS outcome %s", observation)
	}
	return capture, nil
}

// AddRepairVoter accepts only the real learner's already registered nonce. The
// Node independently checks that registration and its QUIC durable-prefix proof
// for both replacement and add-only enrollment.
func (h *Host) AddRepairVoter(ctx context.Context, repairID, registeredWALIdentity string) error {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return fmt.Errorf("SQL host closed")
	}
	r, err := h.closedRepair(ctx, repairID)
	if err != nil {
		return err
	}
	if r.PreparedHandoff == nil || registeredWALIdentity == "" {
		return fmt.Errorf("captured handoff and registered learner WAL required")
	}
	member := quepaxa.Member{ID: r.Plan.Replacement.NodeID, PublicKey: r.Plan.Replacement.PublicKey, PeerURL: r.Plan.Replacement.PeerURL, WALIdentity: registeredWALIdentity}
	expectedConfig := r.PreparedHandoff.Survivors.ConfigID
	expectedAbort := quepaxa.Slot(0)
	if r.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove || r.Plan.Sequence == operator.V1Alpha2RepairAdd {
		if r.PreparedHandoff.PreparedConfig == nil || uint64(r.PreparedHandoff.PreparedConfig.ConfigID) != r.Plan.ExpectedConfigID {
			return fmt.Errorf("exact ADD-first prepared configuration required")
		}
		expectedConfig = uint(r.PreparedHandoff.PreparedConfig.ConfigID)
		expectedAbort = quepaxa.Slot(r.Plan.ExpectedAbortSlot)
	}
	return h.node.ChangeManagedMembership(ctx, node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipAdd, AddOnly: r.Plan.Sequence == operator.V1Alpha2RepairAdd, ExpectedConfigID: expectedConfig, ExpectedAbortSlot: expectedAbort, OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity, Addition: &member, HandoffHeadHash: r.PreparedHandoff.HeadHash})
}

func (h *Host) closedRepair(ctx context.Context, repairID string) (operator.V1Alpha2RepairRecord, error) {
	a, err := h.live(ctx)
	if err != nil {
		return operator.V1Alpha2RepairRecord{}, err
	}
	r, ok := a.Repair()
	if !ok || !a.SQLReservationsClosed() || r.Plan.OperationID != repairID || r.Plan.BootstrapPlanDigest != h.cfg.Plan.BootstrapPlanDigest || uint64(uint(r.Plan.ExpectedConfigID)) != r.Plan.ExpectedConfigID {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("exact live closed repair required")
	}
	if err := validateRepairSequenceProfile(r.Plan.Sequence, h.cfg.Plan.ApplicationProtocol); err != nil {
		return r, err
	}
	namespace, err := node.ManagedNamespaceFor(h.cfg.StorageRoot, h.cfg.Ref.UID, r.Plan.Generation, h.cfg.Plan.AssignmentID, string(h.cfg.Runtime.ClusterID))
	if err != nil || h.cfg.Runtime.ObjStorePrefix != namespace.ConfigPrefix {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("managed membership namespace changed")
	}
	if r.PreparedHandoff != nil && (r.PreparedHandoff.SourceRoot != h.cfg.StorageRoot || r.PreparedHandoff.SourcePrefix != namespace.FinalPrefix || r.PreparedHandoff.PhysicalBinding != h.identity.PhysicalBinding) {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("captured repair physical profile changed")
	}
	return r, nil
}

// ValidateManagedMembership is the private Node checker, not a bearer handler.
// Pending SQL/uncertain outcomes deliberately do not require new write admission.
func (h *Host) ValidateManagedMembership(ctx context.Context, i node.ManagedStartupIdentity, request node.ManagedMembershipRequest, current quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	if i != h.identity {
		return fmt.Errorf("managed membership execution identity changed")
	}
	r, err := h.closedRepair(ctx, request.RepairOperationID)
	if err != nil {
		return err
	}
	p := r.Plan
	if p.Sequence == operator.V1Alpha2RepairLeave {
		return h.validateManagedLeave(r, request, current, abortSlot)
	}
	if request.OldNodeID != p.OldNodeID || request.OldWALIdentity != p.OldWALIdentity || i.Member.Generation != p.Generation || i.Member.DeploymentUID != p.DeploymentUID || i.StorageID != p.StorageID {
		return fmt.Errorf("managed membership repair binding changed")
	}
	if p.Sequence == operator.V1Alpha2RepairAdd && request.Action == node.ManagedMembershipAdd {
		return h.validateAddOnlyEnrollment(r, request, current, abortSlot)
	}
	if p.Sequence == operator.V1Alpha2RepairAddThenRemove && request.Action == node.ManagedMembershipAdd {
		return h.validateAddFirstRepairAdd(r, request, current, abortSlot)
	}
	foundOld := false
	ids := make([]types.NodeID, 0, len(current.Members)+1)
	for _, member := range current.Members {
		if member.ID == p.OldNodeID {
			foundOld = true
			if member.WALIdentity != "" && member.WALIdentity != p.OldWALIdentity {
				return fmt.Errorf("old voter WAL identity changed")
			}
			continue
		}
		if member.ID == p.Replacement.NodeID {
			if request.Action == node.ManagedMembershipAdd {
				if request.Addition == nil || member != *request.Addition {
					return fmt.Errorf("replacement already differs from exact managed ADD")
				}
			} else if request.Action == node.ManagedMembershipRemove && p.Sequence == operator.V1Alpha2RepairAddThenRemove && r.AddCompletion != nil {
				if member.PublicKey != p.Replacement.PublicKey || member.PeerURL != p.Replacement.PeerURL || member.WALIdentity == "" {
					return fmt.Errorf("registered ADD-first replacement changed before REMOVE")
				}
			} else {
				return fmt.Errorf("replacement already differs from exact managed ADD")
			}
			continue
		}
		ids = append(ids, member.ID)
	}
	ids = append(ids, p.Replacement.NodeID)
	slices.Sort(ids)
	if !slices.Equal(ids, p.FinalVoterIDs) {
		return fmt.Errorf("current membership differs from repair target")
	}
	switch request.Action {
	case node.ManagedMembershipRemove:
		expectedConfig := p.ExpectedConfigID
		if p.Sequence == operator.V1Alpha2RepairAddThenRemove {
			if r.AddCompletion == nil || r.AddCompletion.ConfigID != p.ExpectedConfigID+1 || r.AddCompletion.Kind != "Terminal" {
				return fmt.Errorf("ADD terminal not durably recorded before REMOVE")
			}
			expectedConfig = r.AddCompletion.ConfigID
		}
		if uint64(request.ExpectedConfigID) != expectedConfig || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot || (current.ConfigID != request.ExpectedConfigID && current.ConfigID != request.ExpectedConfigID+1) || (current.ConfigID == request.ExpectedConfigID) != foundOld || current.ConfigID == request.ExpectedConfigID && abortSlot != request.ExpectedAbortSlot {
			return fmt.Errorf("managed REMOVE configuration/revision changed")
		}
	case node.ManagedMembershipAdd:
		handoff := r.PreparedHandoff
		if handoff == nil || request.HandoffHeadHash != handoff.HeadHash || request.Addition == nil || request.Addition.ID != p.Replacement.NodeID || request.Addition.PublicKey != p.Replacement.PublicKey || request.Addition.PeerURL != p.Replacement.PeerURL || request.Addition.WALIdentity == "" || request.Addition.URL != "" || request.Addition.LogURL != "" || request.ExpectedConfigID != handoff.Survivors.ConfigID || request.ExpectedAbortSlot != 0 || foundOld {
			return fmt.Errorf("managed ADD handoff or learner binding changed")
		}
		if current.ConfigID == request.ExpectedConfigID {
			if abortSlot != request.ExpectedAbortSlot || !reflect.DeepEqual(current, handoff.Survivors) {
				return fmt.Errorf("managed ADD survivor configuration/revision changed")
			}
		} else if current.ConfigID != request.ExpectedConfigID+1 || len(current.Members) != len(p.FinalVoterIDs) {
			return fmt.Errorf("managed ADD terminal configuration changed")
		}
	default:
		return fmt.Errorf("unsupported managed membership action")
	}
	return nil
}

func (h *Host) validateAddOnlyEnrollment(r operator.V1Alpha2RepairRecord, request node.ManagedMembershipRequest, current quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	p, handoff := r.Plan, r.PreparedHandoff
	if !request.AddOnly || handoff == nil || handoff.PreparedConfig == nil || request.HandoffHeadHash != handoff.HeadHash || request.Addition == nil || request.Addition.ID != p.Replacement.NodeID || request.Addition.PublicKey != p.Replacement.PublicKey || request.Addition.PeerURL != p.Replacement.PeerURL || request.Addition.WALIdentity == "" || request.Addition.URL != "" || request.Addition.LogURL != "" || uint64(request.ExpectedConfigID) != p.ExpectedConfigID || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot {
		return fmt.Errorf("managed enrollment handoff or learner binding changed")
	}
	if uint64(handoff.PreparedConfig.ConfigID) != p.ExpectedConfigID || handoff.PreparedSlot <= p.ExpectedAbortSlot || handoff.PreparedHash == "" {
		return fmt.Errorf("managed enrollment prepared source changed")
	}
	ids := make([]types.NodeID, 0, len(handoff.PreparedConfig.Members)+1)
	oldFound, replacementFound := false, false
	for _, member := range handoff.PreparedConfig.Members {
		if member.ID == p.OldNodeID {
			oldFound = member.WALIdentity == "" || member.WALIdentity == p.OldWALIdentity
		}
		if member.ID == p.Replacement.NodeID {
			replacementFound = true
		}
		ids = append(ids, types.NodeID(member.ID))
	}
	ids = append(ids, p.Replacement.NodeID)
	slices.Sort(ids)
	if !oldFound || replacementFound || !slices.Equal(ids, p.FinalVoterIDs) || (current.ConfigID == request.ExpectedConfigID && len(current.Members) > len(handoff.PreparedConfig.Members)) {
		return fmt.Errorf("managed enrollment source or final roster changed")
	}
	if current.ConfigID == request.ExpectedConfigID {
		if abortSlot != request.ExpectedAbortSlot || !reflect.DeepEqual(current, *handoff.PreparedConfig) {
			return fmt.Errorf("managed enrollment source configuration/revision changed")
		}
	} else {
		expected := quepaxa.Cluster{ConfigID: request.ExpectedConfigID + 1, Members: append(append([]quepaxa.Member(nil), handoff.PreparedConfig.Members...), *request.Addition)}
		if !sameRepairClusterMembers(current, expected) {
			return fmt.Errorf("managed enrollment target configuration changed")
		}
	}
	return nil
}

func (h *Host) ValidateManagedRepairPreparation(ctx context.Context, i node.ManagedStartupIdentity, request node.ManagedMembershipRequest, current quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	if i != h.identity || request.Action != node.ManagedMembershipAdd || request.Addition != nil || request.HandoffHeadHash != "" {
		return fmt.Errorf("managed ADD-first preparation identity changed")
	}
	r, err := h.closedRepair(ctx, request.RepairOperationID)
	if err != nil {
		return err
	}
	p := r.Plan
	if err := validateRepairSequenceProfile(p.Sequence, h.cfg.Plan.ApplicationProtocol); err != nil {
		return err
	}
	if (p.Sequence != operator.V1Alpha2RepairAddThenRemove && p.Sequence != operator.V1Alpha2RepairAdd) || request.AddOnly != (p.Sequence == operator.V1Alpha2RepairAdd) || r.PreparedHandoff != nil || r.AddCompletion != nil || r.Completion != nil || request.OldNodeID != p.OldNodeID || request.OldWALIdentity != p.OldWALIdentity || uint64(request.ExpectedConfigID) != p.ExpectedConfigID || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot || uint64(current.ConfigID) != p.ExpectedConfigID || uint64(abortSlot) != p.ExpectedAbortSlot || i.Member.Generation != p.Generation || i.Member.DeploymentUID != p.DeploymentUID || i.StorageID != p.StorageID {
		return fmt.Errorf("exact open ADD-first source intent required")
	}
	oldFound := false
	ids := make([]types.NodeID, 0, len(current.Members))
	for _, member := range current.Members {
		if member.ID == p.OldNodeID {
			oldFound = true
			if member.WALIdentity != "" && member.WALIdentity != p.OldWALIdentity {
				return fmt.Errorf("old voter WAL identity changed before ADD-first capture")
			}
			if p.Sequence != operator.V1Alpha2RepairAdd {
				continue
			}
		}
		if member.ID == p.Replacement.NodeID {
			return fmt.Errorf("replacement already appears in ADD-first source configuration")
		}
		ids = append(ids, member.ID)
	}
	if !oldFound {
		return fmt.Errorf("old voter missing from ADD-first source configuration")
	}
	ids = append(ids, p.Replacement.NodeID)
	slices.Sort(ids)
	if !slices.Equal(ids, p.FinalVoterIDs) {
		return fmt.Errorf("ADD-first source configuration differs from exact repair target")
	}
	return nil
}

func (h *Host) validateAddFirstRepairAdd(r operator.V1Alpha2RepairRecord, request node.ManagedMembershipRequest, current quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	p, handoff := r.Plan, r.PreparedHandoff
	if handoff == nil || handoff.PreparedConfig == nil || request.HandoffHeadHash != handoff.HeadHash || request.Addition == nil || request.Addition.ID != p.Replacement.NodeID || request.Addition.PublicKey != p.Replacement.PublicKey || request.Addition.PeerURL != p.Replacement.PeerURL || request.Addition.WALIdentity == "" || request.Addition.URL != "" || request.Addition.LogURL != "" || uint64(request.ExpectedConfigID) != p.ExpectedConfigID || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot {
		return fmt.Errorf("managed ADD-first handoff or learner binding changed")
	}
	if uint64(handoff.PreparedConfig.ConfigID) != p.ExpectedConfigID || handoff.PreparedSlot <= p.ExpectedAbortSlot || handoff.PreparedHash == "" {
		return fmt.Errorf("managed ADD-first prepared source changed")
	}
	oldFound, replacementFound := false, false
	for _, member := range handoff.PreparedConfig.Members {
		if member.ID == p.OldNodeID {
			oldFound = member.WALIdentity == "" || member.WALIdentity == p.OldWALIdentity
		}
		if member.ID == p.Replacement.NodeID {
			replacementFound = true
		}
	}
	if !oldFound || replacementFound {
		return fmt.Errorf("ADD-first source roster changed")
	}
	expected := *handoff.PreparedConfig
	expected.ConfigID++
	expected.Members = append(append([]quepaxa.Member(nil), expected.Members...), *request.Addition)
	if current.ConfigID == handoff.PreparedConfig.ConfigID {
		if uint64(abortSlot) != p.ExpectedAbortSlot || !reflect.DeepEqual(current, *handoff.PreparedConfig) {
			return fmt.Errorf("ADD-first source configuration/revision changed")
		}
	} else if current.ConfigID != expected.ConfigID || !sameRepairClusterMembers(current, expected) {
		return fmt.Errorf("ADD-first terminal configuration changed")
	}
	return nil
}

func sameRepairClusterMembers(actual, expected quepaxa.Cluster) bool {
	if actual.ConfigID != expected.ConfigID || len(actual.Members) != len(expected.Members) {
		return false
	}
	want := make(map[quepaxa.NodeID]quepaxa.Member, len(expected.Members))
	for _, member := range expected.Members {
		if _, exists := want[member.ID]; exists {
			return false
		}
		want[member.ID] = member
	}
	for _, member := range actual.Members {
		if expectedMember, exists := want[member.ID]; !exists || expectedMember != member {
			return false
		}
		delete(want, member.ID)
	}
	return len(want) == 0
}

func (h *Host) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	if err := network.ValidateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, &SQLNotSubmittedError{Stage: SQLStageValidation, Cause: err}
	}
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	if err := h.waitForMembershipTransition(operationCtx); err != nil {
		return ExecuteResponse{}, &SQLNotSubmittedError{Stage: SQLStageAdmission, Cause: err}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ExecuteResponse{}, &SQLNotSubmittedError{Stage: SQLStageAuthority, Cause: fmt.Errorf("SQL host closed")}
	}
	if err := h.ensureInitialRequestAdmission(operationCtx); err != nil {
		return ExecuteResponse{}, &SQLNotSubmittedError{Stage: SQLStageAdmission, Cause: err}
	}
	response, err := h.node.ExecuteManagedSQL(operationCtx, req)
	p := applicationOriginal{kind: "sql", sql: req}
	err = internalRequestOutcome(p, response.MutationReceipt, err)
	h.rememberRequestOutcome(p, err, response.MutationReceipt)
	return response, err
}

// RecoverPending never re-executes an unknown request or treats absence as no
// effect. It resolves only the durable original request and configured barrier.
func (h *Host) RecoverPending(ctx context.Context) error {
	_, err := h.RecoverPendingDetailed(ctx)
	return err
}

func (h *Host) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	operationCtx, n, release, err := h.localReadNode(ctx)
	if err != nil {
		return QueryResponse{}, h.logQueryFailure("host_initial_admission", err)
	}
	defer release()
	if req.Consistency == "linearizable" {
		if err := n.WaitForManagedMembershipTransition(operationCtx); err != nil {
			return QueryResponse{}, err
		}
	}
	return n.QueryManagedSQL(operationCtx, req)
}

func managedHostQueryDiagnostic(stage string) string {
	switch stage {
	case "host_closed", "host_initial_admission":
		return "MANAGED_SQL_QUERY_FAILURE stage=" + stage
	default:
		return ""
	}
}

func (h *Host) logQueryFailure(stage string, err error) error {
	if err != nil && h.internalRequests() {
		if line := managedHostQueryDiagnostic(stage); line != "" {
			log.Print(line)
		}
	}
	return err
}

func (h *Host) Close() error {
	if h.stopAdmission != nil {
		h.stopAdmission()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	return h.node.Shutdown()
}
