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
	"sync"
	"sync/atomic"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const EvidenceFormat = "rhiza-managed-sql/v1"

type RuntimeConfig = types.ExecutionConfig
type ExecuteRequest = network.ExecuteRequest
type ExecuteResponse = network.ExecuteResponse
type QueryRequest = network.QueryRequest
type QueryResponse = network.QueryResponse

type Config struct {
	Runtime  RuntimeConfig
	Store    *operator.V1Alpha2StatusStore
	Ref      operator.V1Alpha2ClusterRef
	Plan     operator.V1Alpha2ApplicationInitPlan
	PodUID   string
	Prepared *recovery.VerifiedPreparedStartup
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
	if cfg.Plan.EvidenceFormat != EvidenceFormat || cfg.Store == nil || cfg.PodUID == "" {
		return nil, fmt.Errorf("exact SQL initialization plan, store, and Pod UID required")
	}
	if cfg.Runtime.ObjStoreDurability == "" {
		cfg.Runtime.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	}
	if cfg.Runtime.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return nil, fmt.Errorf("managed SQL requires configured before-ack durability")
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
	h.authority = initial.Authority()
	if h.authority.Generation != cfg.Plan.Generation || h.authority.Binding.DeploymentUID != cfg.Plan.DeploymentUID || h.authority.Binding.StorageID != cfg.Plan.StorageID {
		return nil, fmt.Errorf("current authority differs from installed generation and storage binding")
	}
	fresh := initial.Assignment().State == "Unused"
	if !fresh && cfg.Prepared == nil {
		admission, e := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
		if e != nil {
			return nil, e
		}
		// Only the explicit immutable initial-consumption record can identify
		// the still-empty bootstrap. Advanced evidence needs existing history.
		fresh = evidenceHash(admission.Evidence()) == initial.Assignment().InitialEvidenceDigest
	}
	h.identity = node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: source, ConfigDigest: member}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: source, BootstrapPlanDigest: common, EvidenceFormat: EvidenceFormat, Fresh: fresh}
	if cfg.Prepared != nil {
		if cfg.Prepared.Identity() != h.identity.Member {
			return nil, fmt.Errorf("prepared handoff differs from exact configured execution identity")
		}
		h.identity.Fresh = false
	}
	if err := h.node.OpenManaged(ctx, node.ManagedStartupConfig{Identity: h.identity, Checker: h, Prepared: cfg.Prepared, BootstrapMember: cfg.Runtime.Learner == nil}); err != nil {
		return nil, err
	}
	if initial.Assignment().State == "Unused" {
		state, err := h.node.InspectManagedSQL(ctx, true)
		if err != nil {
			h.uncertain.Store(true)
			return h, err
		}
		evidence := encodeEvidence(state)
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
		return nil
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if i.Fresh && evidenceHash(a.Evidence()) != s.Assignment().InitialEvidenceDigest {
		return fmt.Errorf("advanced evidence requires existing source history")
	}
	return nil
}

func (h *Host) ValidateManagedAdmission(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState) error {
	if h.uncertain.Load() {
		return fmt.Errorf("SQL outcome unknown; explicit pending recovery required")
	}
	if i != h.identity {
		return fmt.Errorf("execution identity changed")
	}
	a, err := h.live(ctx)
	if err != nil {
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
	if err := h.node.AdmitManaged(ctx); err != nil {
		return err
	}
	if !h.node.Ready() {
		return fmt.Errorf("runtime control catch-up not ready")
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
	if err := h.node.AdmitManaged(ctx); err != nil {
		return ExecuteResponse{}, err
	}
	if !h.node.Ready() {
		return ExecuteResponse{}, fmt.Errorf("runtime control catch-up not ready")
	}
	a, err := h.live(ctx)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if _, pending := a.Pending(); pending {
		return ExecuteResponse{}, fmt.Errorf("pending SQL must be resolved first")
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
	if err := h.node.AdmitManaged(ctx); err != nil {
		return QueryResponse{}, err
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
