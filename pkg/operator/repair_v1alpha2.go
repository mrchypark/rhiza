package operator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

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

// V1Alpha2RepairPlan is one bounded two-membership-operation intent, not a
// membership certificate or prepared handoff. A source HEAD is not required:
// REMOVE may be the operation that first publishes that control history.
// OldWALIdentity must be the registered old voter nonce verified by the caller;
// it is not an external fence. Replacement WAL identity is deliberately absent.
type V1Alpha2RepairPlan struct {
	OperationID         string               `json:"operationID"`
	ClusterUID          string               `json:"clusterUID"`
	Generation          int64                `json:"generation"`
	DeploymentUID       string               `json:"deploymentUID"`
	StorageID           string               `json:"storageID"`
	ExpectedConfigID    uint64               `json:"expectedConfigID"`
	ExpectedAbortSlot   uint64               `json:"expectedAbortSlot"`
	OldNodeID           types.NodeID         `json:"oldNodeID"`
	OldWALIdentity      string               `json:"oldWALIdentity"`
	Replacement         V1Alpha2RepairMember `json:"replacement"`
	BootstrapPlanDigest string               `json:"bootstrapPlanDigest"`
	FinalVoterIDs       []types.NodeID       `json:"finalVoterIDs"`
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
	Plan       V1Alpha2RepairPlan        `json:"plan"`
	State      string                    `json:"state"`
	Completion *V1Alpha2RepairCompletion `json:"completion,omitempty"`
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

func validateV1Alpha2RepairPlan(s V1Alpha2StatusSnapshot, p V1Alpha2RepairPlan) error {
	id, err := types.ManagedNodeID(p.Replacement.PodUID)
	u, urlErr := url.Parse(p.Replacement.PeerURL)
	if err != nil || urlErr != nil || u.Scheme != "quic" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || net.ParseIP(u.Hostname()) == nil || net.ParseIP(u.Hostname()).IsUnspecified() {
		return fmt.Errorf("invalid repair member endpoint")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || !validRepairText(p.Replacement.PodName, 253) || !v1alpha2Name.MatchString(p.Replacement.PodName) || p.Replacement.NodeID != id || p.Replacement.PublicKey == (quepaxa.PublicKey{}) || !validRepairText(string(p.OldNodeID), 256) || !validRepairText(p.OldWALIdentity, 256) || p.OldNodeID == id {
		return fmt.Errorf("invalid repair member identity")
	}
	if !validRepairText(p.OperationID, 256) || p.ClusterUID != s.ref.UID || p.Generation < 0 || !validRepairText(p.DeploymentUID, 256) || !validRepairText(p.StorageID, 256) || p.ExpectedConfigID == 0 || p.ExpectedConfigID > math.MaxInt64-2 || p.ExpectedAbortSlot > math.MaxInt64 {
		return fmt.Errorf("invalid repair operation binding")
	}
	if !validInitDigest(p.BootstrapPlanDigest) || len(p.FinalVoterIDs) < 2 || len(p.FinalVoterIDs) > 64 || !slices.Contains(p.FinalVoterIDs, id) || slices.Contains(p.FinalVoterIDs, p.OldNodeID) {
		return fmt.Errorf("invalid repair final target")
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
	// ConfigID alone cannot demonstrate target completion. In particular the
	// REMOVE-only intermediate target cannot reopen this logical repair.
	return c.Kind == "Terminal" && c.ConfigID == p.ExpectedConfigID+2 && slices.Equal(c.FinalVoterIDs, p.FinalVoterIDs) || c.Kind == "Abort" && c.ConfigID >= p.ExpectedConfigID && len(c.FinalVoterIDs) == 0
}

func parseV1Alpha2Repair(s V1Alpha2StatusSnapshot) (*V1Alpha2RepairRecord, error) {
	status, _ := jsonObject(s.object["status"])
	raw, ok := status["repair"]
	if !ok {
		return nil, nil
	}
	var r V1Alpha2RepairRecord
	if len(raw) > 16<<10 || decodeV1Alpha2(raw, &r) != nil || validateV1Alpha2RepairPlan(s, r.Plan) != nil {
		return nil, fmt.Errorf("invalid repair record")
	}
	if r.Plan.Generation > s.authority.Generation {
		return nil, fmt.Errorf("repair belongs to a future authority")
	}
	if r.State == "Closed" {
		auth, _ := jsonObject(status["authority"])
		if r.Completion != nil || !repairMatchesAuthority(r.Plan, s.authority) || hasV1Alpha2Transition(auth["transition"]) {
			return nil, fmt.Errorf("repair overlaps changed authority or transition")
		}
	} else if (r.State != "Completed" && r.State != "Aborted") || r.Completion == nil || !validateV1Alpha2RepairCompletion(r.Plan, *r.Completion) || (r.State == "Completed") != (r.Completion.Kind == "Terminal") {
		return nil, fmt.Errorf("invalid repair terminal")
	}
	return &r, nil
}

// BeginV1Alpha2Repair closes new reservations before a runtime may freeze its
// membership. Existing pending/evidence is untouched. No membership is executed.
// The trusted caller must verify current consensus configuration and the old
// registered WAL identity; these cannot be proved by Kubernetes status.
func BeginV1Alpha2Repair(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, plan V1Alpha2RepairPlan) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.status.store != store || old.Authority() != expected || !repairMatchesAuthority(plan, expected) || hasV1Alpha2Transition(old.transition) || old.SQLReservationsClosed() {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair close preconditions do not match")
	}
	if err := validateV1Alpha2RepairPlan(old.status, plan); err != nil {
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
	if err := validateV1Alpha2RepairPod(ctx, store, old.status.ref, plan); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
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
	r, ok := old.Repair()
	if old.status.store != store || old.Authority() != expected || !ok || !old.SQLReservationsClosed() || !repairMatchesAuthority(r.Plan, expected) || hasV1Alpha2Transition(old.transition) || !validateV1Alpha2RepairCompletion(r.Plan, completion) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("repair completion preconditions do not match")
	}
	r.State = "Completed"
	if completion.Kind == "Abort" {
		r.State = "Aborted"
	}
	r.Completion = &completion
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, auth map[string]json.RawMessage) { status["repair"], _ = json.Marshal(r) })
}

func validateV1Alpha2RepairPod(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, p V1Alpha2RepairPlan) error {
	var pod struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Status           struct {
			PodIP                 string
			InitContainerStatuses []struct {
				Name  string
				State struct {
					Terminated *struct {
						ExitCode int
						Message  string
					}
				}
			}
		}
	}
	if err := store.Kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/pods/"+url.PathEscape(p.Replacement.PodName), &pod); err != nil {
		return err
	}
	u, _ := url.Parse(p.Replacement.PeerURL)
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.Name != p.Replacement.PodName || pod.Metadata.UID != p.Replacement.PodUID || pod.Metadata.ResourceVersion == "" || pod.Metadata.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || !net.ParseIP(pod.Status.PodIP).Equal(net.ParseIP(u.Hostname())) {
		return fmt.Errorf("repair Pod identity or endpoint mismatch")
	}
	if len(pod.Status.InitContainerStatuses) == 0 || pod.Status.InitContainerStatuses[0].Name != V1Alpha2PrepareIdentityContainer || pod.Status.InitContainerStatuses[0].State.Terminated == nil || pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 {
		return fmt.Errorf("repair Pod identity is not prepared")
	}
	key, err := DecodeV1Alpha2BootstrapReport(pod.Status.InitContainerStatuses[0].State.Terminated.Message)
	if err != nil || key != p.Replacement.PublicKey {
		return fmt.Errorf("repair Pod public key mismatch")
	}
	var owner *v1alpha2BootstrapOwner
	for i := range pod.Metadata.OwnerReferences {
		o := &pod.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if owner != nil {
				return fmt.Errorf("ambiguous repair Pod owner")
			}
			owner = o
		}
	}
	if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.Name == "" || owner.UID == "" {
		return fmt.Errorf("invalid repair Pod owner")
	}
	var rs struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace
	if err := store.Kube.Get(ctx, base+"/replicasets/"+url.PathEscape(owner.Name), &rs); err != nil {
		return err
	}
	if rs.APIVersion != "apps/v1" || rs.Kind != "ReplicaSet" || rs.Metadata.Name != owner.Name || rs.Metadata.ResourceVersion == "" || rs.Metadata.UID != owner.UID || rs.Metadata.Namespace != ref.Namespace || rs.Metadata.DeletionTimestamp != nil {
		return fmt.Errorf("invalid repair ReplicaSet")
	}
	var deploymentOwner *v1alpha2BootstrapOwner
	for i := range rs.Metadata.OwnerReferences {
		o := &rs.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if deploymentOwner != nil {
				return fmt.Errorf("ambiguous repair Deployment owner")
			}
			deploymentOwner = o
		}
	}
	if deploymentOwner == nil || deploymentOwner.APIVersion != "apps/v1" || deploymentOwner.Kind != "Deployment" || deploymentOwner.UID != p.DeploymentUID || deploymentOwner.Name == "" {
		return fmt.Errorf("invalid repair Deployment owner")
	}
	var dep struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := store.Kube.Get(ctx, base+"/deployments/"+url.PathEscape(deploymentOwner.Name), &dep); err != nil {
		return err
	}
	if dep.APIVersion != "apps/v1" || dep.Kind != "Deployment" || dep.Metadata.Name != deploymentOwner.Name || dep.Metadata.ResourceVersion == "" || dep.Metadata.UID != p.DeploymentUID || dep.Metadata.Namespace != ref.Namespace || dep.Metadata.DeletionTimestamp != nil || !bootstrapOwner(dep.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) {
		return fmt.Errorf("invalid repair Deployment binding")
	}
	return nil
}
