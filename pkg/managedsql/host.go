// Package managedsql owns a private SQL-only managed HA runtime. It exposes no
// Node, raw network server, graph, KV, notification, migration, or HTTP surface.
package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

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
	mu        sync.Mutex
	gateMu    sync.RWMutex
	active    *operator.V1Alpha2WriteRequest
	node      *node.Node
	cfg       Config
	identity  node.ManagedStartupIdentity
	authority operator.V1Alpha2AuthorityView
	closed    bool
	uncertain atomic.Bool
}

// Open returns an owned paused host after initialization, without waiting for
// quorum. Evidence/CAS failures after runtime creation return the host as well:
// callers must retain it for diagnosis/recovery or call Close.
func Open(ctx context.Context, cfg Config) (*Host, error) {
	cfg.Runtime = node.CloneManagedRuntimeConfig(cfg.Runtime)
	cfg.RepairOrigin = bytes.Clone(cfg.RepairOrigin)
	if cfg.Runtime.ObjStoreProvider == "" {
		cfg.Runtime.ObjStoreProvider = "s3"
	}
	if cfg.Plan.EvidenceFormat != EvidenceFormat || cfg.Store == nil || cfg.PodUID == "" {
		return nil, fmt.Errorf("exact SQL initialization plan, store, and Pod UID required")
	}
	if cfg.Runtime.ObjStoreDurability == "" {
		cfg.Runtime.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	}
	if cfg.Runtime.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return nil, fmt.Errorf("managed SQL requires configured before-ack durability")
	}
	namespace, err := node.ManagedNamespaceFor(cfg.StorageRoot, cfg.Ref.UID, cfg.Plan.Generation, cfg.Plan.AssignmentID, string(cfg.Runtime.ClusterID))
	if err != nil || cfg.Runtime.ObjStorePrefix != namespace.ConfigPrefix {
		return nil, errors.Join(fmt.Errorf("installed runtime final namespace differs from exact assignment"), err)
	}
	cluster, err := types.ManagedClusterID(cfg.Ref.UID, cfg.Plan.Generation)
	memberID, memberErr := types.ManagedNodeID(cfg.PodUID)
	if err != nil || memberErr != nil || cfg.Runtime.ClusterID != cluster || cfg.Runtime.NodeID != memberID {
		return nil, errors.Join(fmt.Errorf("runtime member identity differs from CR generation or actual Pod UID"), err, memberErr)
	}
	h := &Host{cfg: cfg, node: node.New(&cfg.Runtime)}
	source, member, err := h.node.PreparedStartupBindingDigests()
	if err != nil {
		return nil, err
	}
	common, err := h.node.ManagedBootstrapPlanDigest(source, EvidenceFormat)
	if err != nil || cfg.Plan.PhysicalBinding != source || cfg.Plan.BootstrapPlanDigest != common || cfg.Plan.ClusterUID != cfg.Ref.UID {
		return nil, errors.Join(fmt.Errorf("installed runtime differs from initialization plan"), err)
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
	if err != nil || initial.Assignment().Plan != cfg.Plan {
		return nil, errors.Join(fmt.Errorf("live initialization assignment mismatch"), err)
	}
	status, err := cfg.Store.Load(ctx, cfg.Ref)
	if err != nil {
		return nil, err
	}
	bootstrap, err := status.BootstrapPlan()
	if err != nil {
		return nil, err
	}
	if bootstrap.ClusterID != cfg.Runtime.ClusterID || bootstrap.PhysicalBinding != source || bootstrap.EvidenceFormat != EvidenceFormat || bootstrap.Durability != cfg.Runtime.ObjStoreDurability || bootstrap.Reconfiguration != cfg.Runtime.EnableReconfiguration {
		return nil, fmt.Errorf("installed runtime differs from full bootstrap plan")
	}
	initialMember := false
	for _, m := range bootstrap.Members {
		if m.PodUID == cfg.PodUID && m.Member.ID == cfg.Runtime.NodeID && m.Member.PublicKey == quepaxa.PublicKey(network.PeerPublicKey(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, cfg.Runtime.PeerToken)) {
			initialMember = true
		}
	}
	if cfg.Runtime.Learner == nil && !initialMember {
		return nil, fmt.Errorf("exact initial Pod/member/key is not in bootstrap plan")
	}
	if cfg.Prepared != nil && cfg.Runtime.Learner != nil {
		admission, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
		if err != nil {
			return nil, err
		}
		if repair, ok := admission.Repair(); ok && repairPlanMatchesConfiguredLearner(repair.Plan, cfg) {
			if repair.State != "Closed" {
				return nil, fmt.Errorf("prepared repair learner requires the exact current closed repair")
			}
			if len(cfg.RepairOrigin) == 0 {
				return nil, fmt.Errorf("closed repair learner requires immutable repair origin")
			}
		}
	}
	if initial.Assignment().State == "Unused" && !initialMember {
		return nil, fmt.Errorf("only an exact initial member may consume E0")
	}
	h.authority = initial.Authority()
	ever := initial.EverApplicationReserved()
	if h.authority.Generation != cfg.Plan.Generation || h.authority.Binding.DeploymentUID != cfg.Plan.DeploymentUID || h.authority.Binding.StorageID != cfg.Plan.StorageID {
		return nil, fmt.Errorf("current authority differs from installed generation and storage binding")
	}
	h.identity = node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: source, ConfigDigest: member}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: source, BootstrapPlanDigest: common, EvidenceFormat: EvidenceFormat}
	if cfg.Prepared != nil {
		if cfg.Prepared.Identity() != h.identity.Member {
			return nil, fmt.Errorf("prepared handoff differs from exact configured execution identity")
		}
		h.identity.Fresh = false
	}
	if err := h.ValidateManagedStartup(ctx, h.identity); err != nil {
		return nil, err
	}
	birthAuthority, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
	if err != nil || birthAuthority.Assignment().Plan != cfg.Plan || birthAuthority.Authority() != h.authority {
		return nil, errors.Join(fmt.Errorf("birth assignment changed"), err)
	}
	emptySource, err := h.node.PrepareManagedSQLSource(ctx, h.identity, birthAuthority.Assignment().State == "Unused" && !birthAuthority.EverApplicationReserved() && cfg.Prepared == nil)
	if err != nil {
		return nil, err
	}
	h.identity.Fresh = emptySource && !ever && cfg.Prepared == nil && initialMember
	if h.identity.Fresh && initial.Assignment().State == "Consumed" {
		a, err := h.live(ctx)
		if err != nil {
			return nil, err
		}
		reserved := a.EverApplicationReserved()
		if _, pending := a.Pending(); reserved || pending {
			h.identity.Fresh = false
		}
	}
	if initial.Assignment().State == "Unused" {
		if ever {
			return nil, fmt.Errorf("reserved application history prevents E0 consumption")
		}
		if err := h.node.VerifyManagedSQLFreshSource(ctx, h.identity); err != nil {
			return nil, err
		}
	}
	if err := h.node.OpenManaged(ctx, node.ManagedStartupConfig{StorageRoot: cfg.StorageRoot, PeerRecovery: true, Identity: h.identity, Checker: h, Prepared: cfg.Prepared, RepairOrigin: cfg.RepairOrigin, BootstrapMember: cfg.Runtime.Learner == nil}); err != nil {
		return nil, err
	}
	if initial.Assignment().State == "Unused" {
		state, err := h.node.InspectManagedSQL(ctx, true)
		if err != nil {
			h.uncertain.Store(true)
			return h, err
		}
		evidence := encodeEvidence(state)
		liveInit, err := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
		if err != nil {
			h.uncertain.Store(true)
			return h, err
		}
		if liveInit.EverApplicationReserved() {
			h.uncertain.Store(true)
			return h, fmt.Errorf("reservation raced initial E0 inspection")
		}
		if err := h.node.VerifyManagedSQLFreshSource(ctx, h.identity); err != nil {
			h.uncertain.Store(true)
			return h, err
		}
		_, _, err = operator.CommitV1Alpha2ApplicationInit(ctx, cfg.Store, initial, cfg.Plan, evidence)
		if err != nil {
			live, e := operator.LoadV1Alpha2ApplicationInit(ctx, cfg.Store, cfg.Ref)
			if e != nil || live.Assignment().Plan != cfg.Plan || live.Assignment().State != "Consumed" || live.Assignment().InitialEvidenceDigest != evidenceHash(evidence) {
				h.uncertain.Store(true)
				return h, errors.Join(err, e)
			}
			// Matching consumption establishes initialization only. Admission is
			// always loaded again and local state checked by Ready/Execute.
		}
	}
	return h, nil
}

func repairPlanMatchesConfiguredLearner(plan operator.V1Alpha2RepairPlan, cfg Config) bool {
	return cfg.Runtime.Learner != nil && plan.ClusterUID == cfg.Ref.UID && plan.Generation == cfg.Plan.Generation && plan.DeploymentUID == cfg.Plan.DeploymentUID && plan.StorageID == cfg.Plan.StorageID && plan.BootstrapPlanDigest == cfg.Plan.BootstrapPlanDigest && plan.Replacement.PodUID == cfg.PodUID && plan.Replacement.NodeID == cfg.Runtime.NodeID && plan.Replacement.PublicKey == cfg.Runtime.Learner.PublicKey && plan.Replacement.PeerURL == cfg.Runtime.Learner.PeerURL
}

func encodeEvidence(s node.ManagedSQLSnapshot) json.RawMessage {
	b, _ := json.Marshal(sqlEvidence{EvidenceFormat, fmt.Sprintf("%x", s.Application.Digest)})
	return b
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
		return h.node.VerifyManagedSQLFreshSource(ctx, i)
	}
	if h.cfg.Runtime.Learner != nil && h.cfg.Prepared != nil {
		a, err := h.live(ctx)
		if err != nil {
			return err
		}
		r, ok := a.Repair()
		if !ok || r.PreparedHandoff == nil || r.Plan.Replacement.PodUID != h.cfg.PodUID || r.Plan.Replacement.NodeID != h.cfg.Runtime.NodeID || r.Plan.Replacement.PublicKey != h.cfg.Runtime.Learner.PublicKey || r.Plan.Replacement.PeerURL != h.cfg.Runtime.Learner.PeerURL || r.PreparedHandoff.PhysicalBinding != i.PhysicalBinding || r.PreparedHandoff.SourceRoot != h.cfg.StorageRoot || r.PreparedHandoff.SourcePrefix != path.Join(h.cfg.Runtime.ObjStorePrefix, string(h.cfg.Runtime.ClusterID)) || !bytes.Equal(r.PreparedHandoff.Head, h.cfg.Prepared.View().CapturedHead()) || (r.State != "Closed" && r.State != "Completed") {
			return fmt.Errorf("prepared learner lacks exact live repair handoff")
		}
	}
	return nil
}

func (h *Host) ValidateManagedAdmission(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState) error {
	return h.validateManagedState(ctx, i, runtime, true)
}

func (h *Host) ValidateManagedSQLRead(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState) error {
	return h.validateManagedState(ctx, i, runtime, false)
}

func (h *Host) validateManagedState(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState, allowActiveWrite bool) error {
	if allowActiveWrite && h.uncertain.Load() {
		return fmt.Errorf("SQL outcome unknown; explicit pending recovery required")
	}
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if !allowActiveWrite {
		// live() validates and preserves pending/original SQL and current
		// authority. Neither an unresolved outcome nor newer canonical evidence
		// invalidates this runtime's previously admitted certified snapshot.
		_, err := h.node.InspectManagedSQLRead(ctx)
		return err
	}
	if pending, ok := a.Pending(); ok {
		h.gateMu.RLock()
		active := h.active
		h.gateMu.RUnlock()
		if active == nil || *active != pending {
			return fmt.Errorf("pending SQL blocks new admission")
		}
		return nil
	}
	if a.EverApplicationReserved() && !runtime.ExistingRegisteredWAL && runtime.CertifiedTip == 0 {
		head, err := h.node.ManagedArchiveAvailable(ctx)
		if err != nil {
			return err
		}
		if !head {
			return fmt.Errorf("reserved history cannot admit a new empty-source application baseline")
		}
	}
	s, err := h.node.InspectManagedSQL(ctx, false)
	if err != nil {
		return err
	}
	if evidenceHash(a.Evidence()) != evidenceHash(encodeEvidence(s)) {
		return fmt.Errorf("local SQL state differs from current evidence")
	}
	return nil
}

func requestIdentity(req ExecuteRequest) (operator.V1Alpha2WriteRequest, error) {
	f, err := types.SQLFingerprint(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements})
	return operator.V1Alpha2WriteRequest{Kind: "sql", ID: req.RequestID, Fingerprint: fmt.Sprintf("%x", f)}, err
}

func (h *Host) ValidateManagedSQLWrite(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState, req network.ExecuteRequest) error {
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	w, err := requestIdentity(req)
	if err != nil {
		return err
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	p, ok := a.Pending()
	h.gateMu.RLock()
	active := h.active
	h.gateMu.RUnlock()
	if !ok || p != w || active == nil || *active != w {
		return fmt.Errorf("exact locally owned pending SQL reservation required")
	}
	original, ok, err := a.PendingSQL()
	if err != nil || !ok {
		return errors.Join(fmt.Errorf("original pending SQL missing"), err)
	}
	originalID, err := requestIdentity(original)
	if err != nil || originalID != w {
		return fmt.Errorf("original SQL fingerprint mismatch")
	}
	return nil
}

func (h *Host) Ready(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("SQL host closed")
	}
	return h.node.CheckManagedLocalRead(ctx)
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
	request := node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
	return h.node.ChangeManagedMembership(ctx, request)
}

// CaptureRepairHandoff pins the verified post-REMOVE source in the same closed
// repair. A later live HEAD is never substituted for an already captured one.
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
	request := node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
	source, err := h.node.CaptureManagedRepairSource(ctx, request)
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	namespace, err := node.ManagedNamespaceFor(h.cfg.StorageRoot, h.cfg.Ref.UID, r.Plan.Generation, h.cfg.Plan.AssignmentID, string(h.cfg.Runtime.ClusterID))
	if err != nil {
		return operator.V1Alpha2RepairHandoff{}, err
	}
	capture := operator.V1Alpha2RepairHandoff{PlanDigest: r.Plan.Digest(), SourceRoot: h.cfg.StorageRoot, SourcePrefix: namespace.FinalPrefix, PhysicalBinding: h.identity.PhysicalBinding, Head: source.Head, HeadHash: fmt.Sprintf("%x", source.HeadHash), RemoveTerminalSlot: uint64(source.RemoveTerminal.Slot), RemoveTerminalHash: fmt.Sprintf("%x", source.RemoveTerminal.Hash), Survivors: source.Survivors}
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
// Node independently checks that registration and its QUIC durable-prefix proof.
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
	return h.node.ChangeManagedMembership(ctx, node.ManagedMembershipRequest{RepairOperationID: repairID, Action: node.ManagedMembershipAdd, ExpectedConfigID: r.PreparedHandoff.Survivors.ConfigID, OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity, Addition: &member, HandoffHeadHash: r.PreparedHandoff.HeadHash})
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
	if request.OldNodeID != p.OldNodeID || request.OldWALIdentity != p.OldWALIdentity || i.Member.Generation != p.Generation || i.Member.DeploymentUID != p.DeploymentUID || i.StorageID != p.StorageID {
		return fmt.Errorf("managed membership repair binding changed")
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
			if request.Action != node.ManagedMembershipAdd || request.Addition == nil || member != *request.Addition {
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
		if uint64(request.ExpectedConfigID) != p.ExpectedConfigID || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot || (current.ConfigID != request.ExpectedConfigID && current.ConfigID != request.ExpectedConfigID+1) || (current.ConfigID == request.ExpectedConfigID) != foundOld || current.ConfigID == request.ExpectedConfigID && abortSlot != request.ExpectedAbortSlot {
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

func (h *Host) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ExecuteResponse{}, fmt.Errorf("SQL host closed")
	}
	if err := network.ValidateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	// Retain the authority RV across quorum admission. Refreshing it afterward
	// could carry an old quorum result across repair close/finish/open ABA.
	a, err := h.live(ctx)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if a.SQLReservationsClosed() {
		return ExecuteResponse{}, fmt.Errorf("SQL reservations closed during repair")
	}
	if _, pending := a.Pending(); pending {
		return ExecuteResponse{}, fmt.Errorf("pending SQL must be resolved first")
	}
	if err := h.node.AdmitManaged(ctx); err != nil {
		return ExecuteResponse{}, err
	}
	if !h.node.Ready() {
		return ExecuteResponse{}, fmt.Errorf("runtime control catch-up not ready")
	}
	s, err := h.node.InspectManagedSQL(ctx, false)
	if err != nil {
		return ExecuteResponse{}, err
	}
	reserved, observed, err := operator.ReserveV1Alpha2SQLWrite(ctx, h.cfg.Store, a, h.authority, encodeEvidence(s), req)
	if err != nil || observed != operator.V1Alpha2CASApplied {
		h.uncertain.Store(true)
		return ExecuteResponse{}, errors.Join(fmt.Errorf("SQL reservation not owned"), err)
	}
	w, _ := reserved.Pending()
	original, ok, err := reserved.PendingSQL()
	if err != nil || !ok {
		h.uncertain.Store(true)
		return ExecuteResponse{}, errors.Join(fmt.Errorf("reserved original SQL unavailable"), err)
	}
	h.gateMu.Lock()
	h.active = &w
	h.gateMu.Unlock()
	defer func() { h.gateMu.Lock(); h.active = nil; h.gateMu.Unlock() }()
	response, executeErr := h.node.ExecuteManagedSQL(ctx, original)
	if err := h.finalize(ctx, original, reserved); err != nil {
		h.uncertain.Store(true)
		return response, errors.Join(executeErr, err)
	}
	return response, executeErr
}

func (h *Host) finalize(ctx context.Context, req ExecuteRequest, a operator.V1Alpha2AdmissionSnapshot) error {
	result, err := h.node.ResolveManagedSQL(ctx, req)
	if err != nil || result.State != network.ResolveExecuteRetained || result.Receipt == nil || result.Durability != network.ResolveExecuteDurabilityConfirmed {
		return errors.Join(fmt.Errorf("SQL outcome or configured durability remains unknown"), err)
	}
	s, err := h.node.InspectManagedSQL(ctx, false)
	if err != nil {
		return err
	}
	w, _ := a.Pending()
	_, observed, err := operator.FinalizeV1Alpha2Write(ctx, h.cfg.Store, a, h.authority, w, operator.V1Alpha2WriteCommitted, encodeEvidence(s))
	if err != nil || observed != operator.V1Alpha2CASApplied {
		return errors.Join(fmt.Errorf("SQL finalization not directly confirmed"), err)
	}
	return nil
}

// RecoverPending never re-executes an unknown request or treats absence as no
// effect. It resolves only the durable original request and configured barrier.
func (h *Host) RecoverPending(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("SQL host closed")
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if _, ok := a.Pending(); !ok {
		h.uncertain.Store(false)
		err := h.node.AdmitManaged(ctx)
		if err != nil {
			h.uncertain.Store(true)
		}
		return err
	}
	req, ok, err := a.PendingSQL()
	if err != nil || !ok {
		return errors.Join(fmt.Errorf("original pending SQL required"), err)
	}
	if err := h.finalize(ctx, req, a); err != nil {
		h.uncertain.Store(true)
		return err
	}
	h.uncertain.Store(false)
	return nil
}

func (h *Host) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return QueryResponse{}, fmt.Errorf("SQL host closed")
	}
	return h.node.QueryManagedSQL(ctx, req)
}

func (h *Host) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	return h.node.Shutdown()
}
