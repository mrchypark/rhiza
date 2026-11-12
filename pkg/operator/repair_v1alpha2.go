package operator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type V1Alpha2RepairMember struct {
	PodName   string            `json:"podName"`
	PodUID    string            `json:"podUID"`
	NodeID    types.NodeID      `json:"nodeID"`
	PublicKey quepaxa.PublicKey `json:"publicKey"`
	PeerURL   string            `json:"peerURL"`
}

// V1Alpha2RepairSequence selects the certified membership operation. The empty
// value preserves the historical REMOVE-then-ADD flow.
type V1Alpha2RepairSequence string

const (
	V1Alpha2RepairRemoveThenAdd V1Alpha2RepairSequence = "removeThenAdd"
	V1Alpha2RepairAddThenRemove V1Alpha2RepairSequence = "addThenRemove"
	// Add keeps every current voter and enrolls exactly one additional voter.
	// It is used only for a monotonic managed Pod scale-up.
	V1Alpha2RepairAdd V1Alpha2RepairSequence = "add"
	// Leave removes exactly one named voter after the desired voter count was
	// explicitly reduced. Replacement names that same logical voter.
	V1Alpha2RepairLeave V1Alpha2RepairSequence = "leave"
)

// V1Alpha2RepairPlan is one bounded membership intent, not a certificate or a
// prepared handoff. Legacy REMOVE-first plans do not require a source HEAD;
// ADD-first replacement and add-only enrollment bind a prepared source in
// V1Alpha2RepairHandoff.
// OldWALIdentity is the caller-verified registered nonce, not an external
// fence. For Add, OldNodeID is a current-voter quorum witness and remains in
// FinalVoterIDs. Replacement WAL identity is deliberately absent until
// registration. For Leave, Replacement names the retired logical voter and is
// not part of FinalVoterIDs; PodName/PodUID are not live-workload authority.
type V1Alpha2RepairPlan struct {
	OperationID         string                 `json:"operationID"`
	Sequence            V1Alpha2RepairSequence `json:"sequence,omitempty"`
	ClusterUID          string                 `json:"clusterUID"`
	Generation          int64                  `json:"generation"`
	DeploymentUID       string                 `json:"deploymentUID"`
	StorageID           string                 `json:"storageID"`
	ExpectedConfigID    uint64                 `json:"expectedConfigID"`
	ExpectedAbortSlot   uint64                 `json:"expectedAbortSlot"`
	OldNodeID           types.NodeID           `json:"oldNodeID"`
	OldWALIdentity      string                 `json:"oldWALIdentity"`
	Replacement         V1Alpha2RepairMember   `json:"replacement"`
	BootstrapPlanDigest string                 `json:"bootstrapPlanDigest"`
	FinalVoterIDs       []types.NodeID         `json:"finalVoterIDs"`
}

func (p V1Alpha2RepairPlan) Digest() string {
	b, _ := json.Marshal(p)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

// V1Alpha2RepairCompletion binds the runtime's already-verified exact terminal
// or abort to this handoff. The status helper does not verify consensus proof;
// a trusted host must verify the retained operation/terminal before calling it.
type V1Alpha2RepairCompletion struct {
	OperationID       string         `json:"operationID"`
	PlanDigest        string         `json:"planDigest"`
	ExpectedConfigID  uint64         `json:"expectedConfigID"`
	ExpectedAbortSlot uint64         `json:"expectedAbortSlot"`
	Kind              string         `json:"kind"`
	ConfigID          uint64         `json:"configID"`
	Slot              uint64         `json:"slot"`
	Hash              string         `json:"hash"`
	FinalVoterIDs     []types.NodeID `json:"finalVoterIDs,omitempty"`
}

type V1Alpha2RepairRecord struct {
	Plan            V1Alpha2RepairPlan        `json:"plan"`
	State           string                    `json:"state"`
	Completion      *V1Alpha2RepairCompletion `json:"completion,omitempty"`
	AddCompletion   *V1Alpha2RepairCompletion `json:"addCompletion,omitempty"`
	PreparedHandoff *V1Alpha2RepairHandoff    `json:"preparedHandoff,omitempty"`
}

const V1Alpha2RepairHeadLimit = 64 << 10
const V1Alpha2RepairRecordLimit = 128 << 10

// Status capture is provenance only; the private runtime verifies source and
// REMOVE certificate before publishing the handoff.
type V1Alpha2RepairHandoff struct {
	PlanDigest         string           `json:"planDigest"`
	SourceRoot         string           `json:"sourceRoot"`
	SourcePrefix       string           `json:"sourcePrefix"`
	PhysicalBinding    string           `json:"physicalBinding"`
	Head               []byte           `json:"head"`
	HeadHash           string           `json:"headHash"`
	RemoveTerminalSlot uint64           `json:"removeTerminalSlot,omitempty"`
	RemoveTerminalHash string           `json:"removeTerminalHash,omitempty"`
	Survivors          quepaxa.Cluster  `json:"survivors,omitzero"`
	PreparedConfig     *quepaxa.Cluster `json:"preparedConfig,omitempty"`
	PreparedSlot       uint64           `json:"preparedSlot,omitempty"`
	PreparedHash       string           `json:"preparedHash,omitempty"`
}

func repairSequence(p V1Alpha2RepairPlan) V1Alpha2RepairSequence {
	if p.Sequence == "" {
		return V1Alpha2RepairRemoveThenAdd
	}
	return p.Sequence
}

func validateRepairHandoff(s V1Alpha2StatusSnapshot, p V1Alpha2RepairPlan, h V1Alpha2RepairHandoff) error {
	var assignment V1Alpha2ApplicationInitAssignment
	status, _ := jsonObject(s.object["status"])
	if decodeV1Alpha2(status["applicationInit"], &assignment) != nil {
		return fmt.Errorf("handoff assignment missing")
	}
	cluster, err := types.ManagedClusterID(p.ClusterUID, p.Generation)
	_, prefix, namespaceErr := objectstore.ManagedNamespacePrefixes(h.SourceRoot, p.ClusterUID, p.Generation, assignment.Plan.AssignmentID, string(cluster))
	if err != nil || namespaceErr != nil || h.SourcePrefix != prefix || path.Clean(h.SourcePrefix) != h.SourcePrefix || h.PhysicalBinding != assignment.Plan.PhysicalBinding || h.PlanDigest != p.Digest() || len(h.Head) == 0 || len(h.Head) > V1Alpha2RepairHeadLimit || h.HeadHash != fmt.Sprintf("%x", sha256.Sum256(h.Head)) {
		return fmt.Errorf("invalid repair handoff binding or budget")
	}
	if repairSequence(p) == V1Alpha2RepairAddThenRemove || repairSequence(p) == V1Alpha2RepairAdd {
		if h.PreparedConfig == nil || uint64(h.PreparedConfig.ConfigID) != p.ExpectedConfigID || h.PreparedSlot <= p.ExpectedAbortSlot || h.PreparedSlot > math.MaxInt64 || !validInitDigest(h.PreparedHash) || h.RemoveTerminalSlot != 0 || h.RemoveTerminalHash != "" || h.Survivors.ConfigID != 0 || len(h.Survivors.Members) != 0 {
			return fmt.Errorf("invalid ADD-first prepared source")
		}
		return validateAddFirstPreparedConfig(p, *h.PreparedConfig)
	}
	if h.PreparedConfig != nil || h.PreparedSlot != 0 || h.PreparedHash != "" || h.RemoveTerminalSlot <= p.ExpectedAbortSlot || h.RemoveTerminalSlot > math.MaxInt64 || !validInitDigest(h.RemoveTerminalHash) || uint64(h.Survivors.ConfigID) != p.ExpectedConfigID+1 {
		return fmt.Errorf("invalid REMOVE-first handoff")
	}
	ids := make([]types.NodeID, 0, len(h.Survivors.Members)+1)
	seen := map[types.NodeID]bool{}
	for _, m := range h.Survivors.Members {
		if m.ID == p.OldNodeID || m.ID == p.Replacement.NodeID || seen[m.ID] || !validRepairText(string(m.ID), 256) || m.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("invalid repair survivors")
		}
		seen[m.ID] = true
		ids = append(ids, m.ID)
	}
	if repairSequence(p) != V1Alpha2RepairLeave {
		ids = append(ids, p.Replacement.NodeID)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, p.FinalVoterIDs) {
		return fmt.Errorf("repair survivors differ from final target")
	}
	return nil
}

func validateAddFirstPreparedConfig(p V1Alpha2RepairPlan, source quepaxa.Cluster) error {
	if len(source.Members) < 2 || len(source.Members) >= 64 {
		return fmt.Errorf("ADD-first source roster exceeds bounded intermediate target")
	}
	ids := make([]types.NodeID, 0, len(source.Members))
	seen := make(map[types.NodeID]bool, len(source.Members))
	oldFound := false
	for _, member := range source.Members {
		id := types.NodeID(member.ID)
		if id == "" || seen[id] || id == p.Replacement.NodeID || !validRepairText(string(id), 256) || member.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("invalid ADD-first source membership")
		}
		seen[id] = true
		if id == p.OldNodeID {
			if member.WALIdentity != "" && member.WALIdentity != p.OldWALIdentity {
				return fmt.Errorf("ADD-first source old WAL identity changed")
			}
			oldFound = true
		}
		ids = append(ids, id)
	}
	if !oldFound {
		return fmt.Errorf("ADD-first source does not contain the old voter")
	}
	ids = append(ids, p.Replacement.NodeID)
	// Replacement removes the bound old identity from its final target. Add-only
	// enrollment keeps every current voter and only appends N.
	if repairSequence(p) != V1Alpha2RepairAdd {
		ids = slices.DeleteFunc(ids, func(id types.NodeID) bool { return id == p.OldNodeID })
	}
	slices.Sort(ids)
	if !slices.Equal(ids, p.FinalVoterIDs) {
		return fmt.Errorf("ADD-first source and final target do not match")
	}
	return nil
}

func CaptureV1Alpha2RepairHandoff(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, h V1Alpha2RepairHandoff) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if err := validateV1Alpha2RepairExecutionProfile(old); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	r, ok := old.Repair()
	if !ok || old.status.store != store || old.Authority() != expected || !old.SQLReservationsClosed() || r.PreparedHandoff != nil || !repairMatchesAuthority(r.Plan, expected) || hasV1Alpha2Transition(old.transition) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair handoff requires exact uncaptured closed intent")
	}
	if err := validateRepairHandoff(old.status, r.Plan, h); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if r.Plan.Sequence != V1Alpha2RepairLeave {
		if err := validateV1Alpha2RepairPod(ctx, store, old.status.ref, r.Plan); err != nil {
			return V1Alpha2AdmissionSnapshot{}, "", err
		}
	}
	r.PreparedHandoff = &h
	encoded, err := json.Marshal(r)
	if err != nil || len(encoded) > V1Alpha2RepairRecordLimit {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair record exceeds bounded budget")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, auth map[string]json.RawMessage) { status["repair"] = encoded })
}

// RecordV1Alpha2RepairAddition durably records the exact ADD terminal before
// the same repair operation may proceed to REMOVE. The runtime caller must
// verify the actual consensus terminal and target before invoking this method.
func RecordV1Alpha2RepairAddition(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, completion V1Alpha2RepairCompletion) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if err := validateV1Alpha2RepairExecutionProfile(old); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	r, ok := old.Repair()
	if old.status.store != store || old.Authority() != expected || !ok || !old.SQLReservationsClosed() || !repairMatchesAuthority(r.Plan, expected) || hasV1Alpha2Transition(old.transition) || repairSequence(r.Plan) != V1Alpha2RepairAddThenRemove || r.PreparedHandoff == nil || !validateV1Alpha2RepairAddition(r.Plan, *r.PreparedHandoff, completion) || r.Completion != nil {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair ADD completion preconditions do not match")
	}
	if r.AddCompletion != nil {
		if reflect.DeepEqual(*r.AddCompletion, completion) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair ADD terminal is already recorded; resume from the retained proof")
		}
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair ADD terminal changed")
	}
	r.AddCompletion = &completion
	encoded, err := json.Marshal(r)
	if err != nil || len(encoded) > V1Alpha2RepairRecordLimit {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair record exceeds bounded budget")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, auth map[string]json.RawMessage) { status["repair"] = encoded })
}

func (s V1Alpha2AdmissionSnapshot) ResourceVersion() string { return s.status.ResourceVersion() }
func (s V1Alpha2AdmissionSnapshot) SQLReservationsClosed() bool {
	return s.status.repair != nil && s.status.repair.State == "Closed"
}
func (s V1Alpha2AdmissionSnapshot) Repair() (V1Alpha2RepairRecord, bool) {
	if s.status.repair == nil {
		return V1Alpha2RepairRecord{}, false
	}
	r := *s.status.repair
	r.Plan.FinalVoterIDs = slices.Clone(r.Plan.FinalVoterIDs)
	if r.PreparedHandoff != nil {
		h := *r.PreparedHandoff
		h.Head = slices.Clone(h.Head)
		h.Survivors.Members = slices.Clone(h.Survivors.Members)
		r.PreparedHandoff = &h
	}
	if r.Completion != nil {
		c := *r.Completion
		c.FinalVoterIDs = slices.Clone(c.FinalVoterIDs)
		r.Completion = &c
	}
	return r, true
}

func validRepairText(s string, max int) bool {
	if len(s) == 0 || len(s) > max || !utf8.ValidString(s) || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validateV1Alpha2RepairPlan(s V1Alpha2StatusSnapshot, p V1Alpha2RepairPlan, requireReducedTarget bool) error {
	if p.Sequence != "" && p.Sequence != V1Alpha2RepairRemoveThenAdd && p.Sequence != V1Alpha2RepairAddThenRemove && p.Sequence != V1Alpha2RepairAdd && p.Sequence != V1Alpha2RepairLeave {
		return fmt.Errorf("unsupported repair sequence")
	}
	if repairSequence(p) == V1Alpha2RepairLeave {
		u, urlErr := url.Parse(p.Replacement.PeerURL)
		if p.Replacement.NodeID != p.OldNodeID || p.Replacement.PodName != string(p.OldNodeID) || p.Replacement.PodUID != string(p.OldNodeID) || p.Replacement.PublicKey == (quepaxa.PublicKey{}) || urlErr != nil || u.Scheme != "quic" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || net.ParseIP(u.Hostname()) == nil || net.ParseIP(u.Hostname()).IsUnspecified() {
			return fmt.Errorf("invalid leave member identity")
		}
		if port, err := strconv.Atoi(u.Port()); err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("invalid leave member endpoint")
		}
	} else {
		id, err := types.ManagedNodeID(p.Replacement.PodUID)
		u, urlErr := url.Parse(p.Replacement.PeerURL)
		if err != nil || urlErr != nil || u.Scheme != "quic" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || net.ParseIP(u.Hostname()) == nil || net.ParseIP(u.Hostname()).IsUnspecified() {
			return fmt.Errorf("invalid repair member endpoint")
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 || !validRepairText(p.Replacement.PodName, 253) || !v1alpha2Name.MatchString(p.Replacement.PodName) || p.Replacement.NodeID != id || p.Replacement.PublicKey == (quepaxa.PublicKey{}) || p.OldNodeID == id {
			return fmt.Errorf("invalid repair member identity")
		}
	}
	if !validRepairText(p.OperationID, 256) || p.ClusterUID != s.ref.UID || p.Generation < 0 || !validRepairText(p.DeploymentUID, 256) || !validRepairText(p.StorageID, 256) || p.ExpectedConfigID == 0 || p.ExpectedConfigID > math.MaxInt64-2 || p.ExpectedAbortSlot > math.MaxInt64 {
		return fmt.Errorf("invalid repair operation binding")
	}
	if !validRepairText(string(p.OldNodeID), 256) || !validRepairText(p.OldWALIdentity, 256) || !validInitDigest(p.BootstrapPlanDigest) || len(p.FinalVoterIDs) < 2 || len(p.FinalVoterIDs) > 64 {
		return fmt.Errorf("invalid repair final target")
	}
	switch repairSequence(p) {
	case V1Alpha2RepairAdd:
		if !slices.Contains(p.FinalVoterIDs, p.Replacement.NodeID) || !slices.Contains(p.FinalVoterIDs, p.OldNodeID) {
			return fmt.Errorf("invalid repair final target")
		}
	case V1Alpha2RepairLeave:
		if slices.Contains(p.FinalVoterIDs, p.OldNodeID) || len(p.FinalVoterIDs) < 3 {
			return fmt.Errorf("invalid leave final target")
		}
		if requireReducedTarget {
			target, err := s.TargetVoterCount()
			if err != nil || target != len(p.FinalVoterIDs) {
				return fmt.Errorf("leave final target differs from desired voter count")
			}
		}
	default:
		if !slices.Contains(p.FinalVoterIDs, p.Replacement.NodeID) || slices.Contains(p.FinalVoterIDs, p.OldNodeID) {
			return fmt.Errorf("invalid repair final target")
		}
	}
	for i, node := range p.FinalVoterIDs {
		if !validRepairText(string(node), 256) || i > 0 && p.FinalVoterIDs[i-1] >= node {
			return fmt.Errorf("repair final voters must be unique and sorted")
		}
	}
	if encoded, _ := json.Marshal(p); len(encoded) > 7<<10 {
		return fmt.Errorf("repair intent exceeds bounded record budget")
	}
	status, _ := jsonObject(s.object["status"])
	var assignment V1Alpha2ApplicationInitAssignment
	if decodeV1Alpha2(status["applicationInit"], &assignment) != nil || assignment.State != "Consumed" || p.BootstrapPlanDigest != assignment.Plan.BootstrapPlanDigest {
		return fmt.Errorf("repair requires consumed managed bootstrap history")
	}
	return nil
}

func repairMatchesAuthority(p V1Alpha2RepairPlan, a V1Alpha2AuthorityView) bool {
	return p.ClusterUID == a.Owner.UID && p.Generation == a.Generation && p.DeploymentUID == a.Binding.DeploymentUID && p.StorageID == a.Binding.StorageID
}

func validateV1Alpha2RepairCompletion(p V1Alpha2RepairPlan, c V1Alpha2RepairCompletion) bool {
	if c.OperationID != p.OperationID || c.PlanDigest != p.Digest() || c.ExpectedConfigID != p.ExpectedConfigID || c.ExpectedAbortSlot != p.ExpectedAbortSlot || c.ConfigID == 0 || c.ConfigID > math.MaxInt64 || c.Slot <= p.ExpectedAbortSlot || c.Slot > math.MaxInt64 || !validInitDigest(c.Hash) {
		return false
	}
	if repairSequence(p) == V1Alpha2RepairAdd {
		return c.Kind == "Terminal" && c.ConfigID == p.ExpectedConfigID+1 && slices.Equal(c.FinalVoterIDs, p.FinalVoterIDs) || c.Kind == "Abort" && c.ConfigID >= p.ExpectedConfigID && len(c.FinalVoterIDs) == 0
	}
	if repairSequence(p) == V1Alpha2RepairLeave {
		return c.Kind == "Terminal" && c.ConfigID == p.ExpectedConfigID+1 && slices.Equal(c.FinalVoterIDs, p.FinalVoterIDs) || c.Kind == "Abort" && c.ConfigID >= p.ExpectedConfigID && len(c.FinalVoterIDs) == 0
	}
	// ConfigID alone cannot demonstrate target completion. In particular the
	// REMOVE-only intermediate target cannot reopen this logical repair.
	return c.Kind == "Terminal" && c.ConfigID == p.ExpectedConfigID+2 && slices.Equal(c.FinalVoterIDs, p.FinalVoterIDs) || c.Kind == "Abort" && c.ConfigID >= p.ExpectedConfigID && len(c.FinalVoterIDs) == 0
}

func validateV1Alpha2RepairAddition(p V1Alpha2RepairPlan, h V1Alpha2RepairHandoff, c V1Alpha2RepairCompletion) bool {
	if repairSequence(p) != V1Alpha2RepairAddThenRemove || h.PreparedConfig == nil || c.OperationID != p.OperationID || c.PlanDigest != p.Digest() || c.ExpectedConfigID != p.ExpectedConfigID || c.ExpectedAbortSlot != p.ExpectedAbortSlot || c.Kind != "Terminal" || c.ConfigID != p.ExpectedConfigID+1 || c.ConfigID > math.MaxInt64 || c.Slot <= h.PreparedSlot || c.Slot > math.MaxInt64 || !validInitDigest(c.Hash) {
		return false
	}
	ids := make([]types.NodeID, 0, len(h.PreparedConfig.Members)+1)
	for _, member := range h.PreparedConfig.Members {
		ids = append(ids, types.NodeID(member.ID))
	}
	ids = append(ids, p.Replacement.NodeID)
	if len(ids) > 64 {
		return false
	}
	slices.Sort(ids)
	return c.FinalVoterIDs != nil && slices.Equal(c.FinalVoterIDs, ids)
}

func validV1Alpha2RepairTerminalOrder(p V1Alpha2RepairPlan, completion V1Alpha2RepairCompletion, addition *V1Alpha2RepairCompletion) bool {
	return repairSequence(p) != V1Alpha2RepairAddThenRemove || completion.Kind != "Terminal" || addition != nil && completion.Slot > addition.Slot
}

func parseV1Alpha2Repair(s V1Alpha2StatusSnapshot) (*V1Alpha2RepairRecord, error) {
	status, _ := jsonObject(s.object["status"])
	raw, ok := status["repair"]
	if !ok {
		return nil, nil
	}
	var r V1Alpha2RepairRecord
	if len(raw) > V1Alpha2RepairRecordLimit || decodeV1Alpha2(raw, &r) != nil || validateV1Alpha2RepairPlan(s, r.Plan, false) != nil {
		return nil, fmt.Errorf("invalid repair record")
	}
	if r.PreparedHandoff != nil {
		if err := validateRepairHandoff(s, r.Plan, *r.PreparedHandoff); err != nil {
			return nil, err
		}
	}
	if r.AddCompletion != nil && (repairSequence(r.Plan) != V1Alpha2RepairAddThenRemove || r.PreparedHandoff == nil || !validateV1Alpha2RepairAddition(r.Plan, *r.PreparedHandoff, *r.AddCompletion)) {
		return nil, fmt.Errorf("invalid repair ADD terminal")
	}
	if r.Plan.Generation > s.authority.Generation {
		return nil, fmt.Errorf("repair belongs to a future authority")
	}
	if r.State == "Closed" {
		auth, _ := jsonObject(status["authority"])
		if r.Completion != nil || !repairMatchesAuthority(r.Plan, s.authority) || hasV1Alpha2Transition(auth["transition"]) {
			return nil, fmt.Errorf("repair overlaps changed authority or transition")
		}
	} else if (r.State != "Completed" && r.State != "Aborted") || r.Completion == nil || !validateV1Alpha2RepairCompletion(r.Plan, *r.Completion) || (r.State == "Completed") != (r.Completion.Kind == "Terminal") || (repairSequence(r.Plan) == V1Alpha2RepairAddThenRemove && r.Completion.Kind == "Terminal" && r.AddCompletion == nil) || (repairSequence(r.Plan) == V1Alpha2RepairAddThenRemove && r.Completion.Kind == "Abort" && r.AddCompletion != nil) || (repairSequence(r.Plan) == V1Alpha2RepairAdd && (r.PreparedHandoff == nil || r.AddCompletion != nil)) {
		return nil, fmt.Errorf("invalid repair terminal")
	}
	if r.Completion != nil && !validV1Alpha2RepairTerminalOrder(r.Plan, *r.Completion, r.AddCompletion) {
		return nil, fmt.Errorf("final REMOVE terminal must follow ADD terminal")
	}
	return &r, nil
}

// BeginV1Alpha2Repair closes new reservations before a runtime may freeze its
// membership. Existing pending/evidence is untouched. No membership is executed.
// The trusted caller must verify current consensus configuration and the old
// registered WAL identity; these cannot be proved by Kubernetes status.
func BeginV1Alpha2Repair(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, plan V1Alpha2RepairPlan) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if err := validateV1Alpha2RepairExecutionProfile(old); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if plan.Sequence != V1Alpha2RepairAddThenRemove && plan.Sequence != V1Alpha2RepairAdd && plan.Sequence != V1Alpha2RepairLeave {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("new append-only repair requires ADD-first or add sequence")
	}
	if old.status.store != store || old.Authority() != expected || !repairMatchesAuthority(plan, expected) || hasV1Alpha2Transition(old.transition) || old.SQLReservationsClosed() {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair close preconditions do not match")
	}
	if err := validateV1Alpha2RepairPlan(old.status, plan, true); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if err := validateV1Alpha2ReplicaRepairStep(old, plan); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if previous, ok := old.Repair(); ok {
		p := previous.Plan
		if p.OperationID == plan.OperationID || !(plan.Generation > p.Generation || plan.Generation == p.Generation && (plan.ExpectedConfigID > p.ExpectedConfigID || plan.ExpectedConfigID == p.ExpectedConfigID && plan.ExpectedAbortSlot > p.ExpectedAbortSlot)) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair operation position did not advance")
		}
		if plan.Generation == p.Generation && (plan.ExpectedConfigID < previous.Completion.ConfigID || previous.State == "Aborted" && plan.ExpectedConfigID == previous.Completion.ConfigID && plan.ExpectedAbortSlot < previous.Completion.Slot) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair operation regressed behind terminal")
		}
	}
	if plan.Sequence != V1Alpha2RepairLeave {
		if err := validateV1Alpha2RepairPod(ctx, store, old.status.ref, plan); err != nil {
			return V1Alpha2AdmissionSnapshot{}, "", err
		}
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, auth map[string]json.RawMessage) {
		status["repair"], _ = json.Marshal(V1Alpha2RepairRecord{Plan: plan, State: "Closed"})
	})
}

// FinishV1Alpha2Repair reopens only for this exact verified terminal/abort.
// A matching readback is diagnostic, never permission to execute membership.
// The caller must verify the actual final configuration and exact retained
// consensus terminal/abort, including completing required terminal ACKs.
func FinishV1Alpha2Repair(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, completion V1Alpha2RepairCompletion) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if err := validateV1Alpha2RepairExecutionProfile(old); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	r, ok := old.Repair()
	if old.status.store != store || old.Authority() != expected || !ok || !old.SQLReservationsClosed() || !repairMatchesAuthority(r.Plan, expected) || hasV1Alpha2Transition(old.transition) || !validateV1Alpha2RepairCompletion(r.Plan, completion) || repairSequence(r.Plan) == V1Alpha2RepairAddThenRemove && (completion.Kind == "Terminal" && r.AddCompletion == nil || completion.Kind == "Abort" && r.AddCompletion != nil) || repairSequence(r.Plan) == V1Alpha2RepairAdd && (r.PreparedHandoff == nil || r.AddCompletion != nil) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair completion preconditions do not match")
	}
	if !validV1Alpha2RepairTerminalOrder(r.Plan, completion, r.AddCompletion) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("verified terminal does not satisfy the repair sequence")
	}
	r.State = "Completed"
	if completion.Kind == "Abort" {
		r.State = "Aborted"
	}
	r.Completion = &completion
	if encoded, _ := json.Marshal(r); len(encoded) > V1Alpha2RepairRecordLimit {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair record exceeds bounded budget")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, auth map[string]json.RawMessage) { status["repair"], _ = json.Marshal(r) })
}

func validateV1Alpha2RepairPod(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, p V1Alpha2RepairPlan) error {
	if err := validateV1Alpha2RepairPodIdentity(ctx, store, ref, p); err != nil {
		return err
	}
	var pod struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Status           struct {
			PodIP             string
			ContainerStatuses []struct {
				RestartCount int
				State        struct {
					Running    *struct{}
					Terminated *struct{}
				}
			}
		}
	}
	if err := store.Kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/pods/"+url.PathEscape(p.Replacement.PodName), &pod); err != nil {
		return err
	}
	u, _ := url.Parse(p.Replacement.PeerURL)
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.Name != p.Replacement.PodName || pod.Metadata.UID != p.Replacement.PodUID || pod.Metadata.ResourceVersion == "" || pod.Metadata.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || !net.ParseIP(pod.Status.PodIP).Equal(net.ParseIP(u.Hostname())) {
		return fmt.Errorf("repair candidate identity changed before authorization")
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.RestartCount != 0 || container.State.Running != nil || container.State.Terminated != nil {
			return fmt.Errorf("repair candidate main container has already started")
		}
	}
	if _, err := ValidateV1Alpha2RepairPodBinding(ctx, store, ref, p); err != nil {
		return err
	}
	return nil
}

// Historical control may be inspected but never used to execute membership.
func validateV1Alpha2RepairExecutionProfile(old V1Alpha2AdmissionSnapshot) error {
	protocol, err := mixedApplicationProtocol(old.status.object)
	if err != nil {
		return err
	}
	if err := ValidateV1Alpha2ApplicationProfile(protocol, protocol); err != nil {
		return err
	}
	assignment, err := parseV1Alpha2ApplicationInit(old.status)
	if err != nil {
		return err
	}
	return ValidateV1Alpha2ApplicationProfile(assignment.Plan.ApplicationProtocol, assignment.Plan.EvidenceFormat)
}
