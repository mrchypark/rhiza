package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// V1Alpha2ApplicationInitPlan identifies shared genesis inputs, not a member's
// Pod, Node, public key, or WAL identity. PhysicalBinding is a trusted host input.
type V1Alpha2ApplicationInitPlan struct {
	AssignmentID        string `json:"assignmentID"`
	ClusterUID          string `json:"clusterUID"`
	IntentHash          string `json:"intentHash"`
	Generation          int64  `json:"generation"`
	DeploymentUID       string `json:"deploymentUID"`
	StorageID           string `json:"storageID"`
	PhysicalBinding     string `json:"physicalBinding"`
	BootstrapPlanDigest string `json:"bootstrapPlanDigest"`
	EvidenceFormat      string `json:"evidenceFormat"`
}

type V1Alpha2ApplicationInitAssignment struct {
	Plan                  V1Alpha2ApplicationInitPlan `json:"plan"`
	State                 string                      `json:"state"`
	InitialEvidenceDigest string                      `json:"initialEvidenceDigest,omitempty"`
}

type V1Alpha2ApplicationInitSnapshot struct {
	status     V1Alpha2StatusSnapshot
	assignment V1Alpha2ApplicationInitAssignment
}

func (s V1Alpha2ApplicationInitSnapshot) Assignment() V1Alpha2ApplicationInitAssignment {
	return s.assignment
}
func (s V1Alpha2ApplicationInitSnapshot) Authority() V1Alpha2AuthorityView {
	return s.status.Authority()
}
func (s V1Alpha2ApplicationInitSnapshot) EverApplicationReserved() bool {
	return s.status.EverApplicationReserved()
}

// NewV1Alpha2ApplicationInitPlan binds caller-supplied physical and bootstrap
// digests to the canonical genesis recipe. It does not verify physical storage.
func NewV1Alpha2ApplicationInitPlan(ref V1Alpha2ClusterRef, logicalID, deploymentUID, assignmentID, physicalBinding, bootstrapPlanDigest, evidenceFormat string) (V1Alpha2ApplicationInitPlan, error) {
	storageID := genesisStorageID(ref.UID)
	g := v1alpha2Genesis{OwnerUID: ref.UID, LogicalID: logicalID, DeploymentUID: deploymentUID, StorageID: storageID, IntentHash: genesisHash(ref, logicalID, deploymentUID, storageID)}
	p := V1Alpha2ApplicationInitPlan{AssignmentID: assignmentID, ClusterUID: ref.UID, IntentHash: g.IntentHash, Generation: 0, DeploymentUID: deploymentUID, StorageID: storageID, PhysicalBinding: physicalBinding, BootstrapPlanDigest: bootstrapPlanDigest, EvidenceFormat: evidenceFormat}
	if !validInitString(logicalID) || !validInitString(deploymentUID) || !validInitString(ref.UID) {
		return p, fmt.Errorf("invalid genesis identity")
	}
	if err := validateV1Alpha2ApplicationInitPlan(p, ref, g); err != nil {
		return p, err
	}
	return p, nil
}

func validInitString(s string) bool { return len(s) <= 256 && v1alpha2NonBlank.MatchString(s) }
func validInitDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}
func validateV1Alpha2ApplicationInitPlan(p V1Alpha2ApplicationInitPlan, ref V1Alpha2ClusterRef, g v1alpha2Genesis) error {
	if !validInitString(p.AssignmentID) || p.ClusterUID != ref.UID || p.IntentHash != g.IntentHash || p.Generation != 0 || p.DeploymentUID != g.DeploymentUID || p.StorageID != g.StorageID || !validInitDigest(p.PhysicalBinding) || !validInitDigest(p.BootstrapPlanDigest) || !validInitString(p.EvidenceFormat) {
		return fmt.Errorf("invalid application initialization plan")
	}
	return nil
}

func parseV1Alpha2ApplicationInit(s V1Alpha2StatusSnapshot) (V1Alpha2ApplicationInitAssignment, error) {
	status, err := jsonObject(s.object["status"])
	if err != nil {
		return V1Alpha2ApplicationInitAssignment{}, err
	}
	var receipt v1alpha2Genesis
	if decodeV1Alpha2(status["genesis"], &receipt) != nil {
		return V1Alpha2ApplicationInitAssignment{}, fmt.Errorf("missing genesis receipt")
	}
	var assignment V1Alpha2ApplicationInitAssignment
	if decodeV1Alpha2(status["applicationInit"], &assignment) != nil || validateV1Alpha2ApplicationInitPlan(assignment.Plan, s.ref, receipt) != nil {
		return assignment, fmt.Errorf("invalid application initialization assignment")
	}
	if assignment.State == "Unused" {
		if assignment.InitialEvidenceDigest != "" || s.authority.Generation != 0 || s.authority.Binding.DeploymentUID != assignment.Plan.DeploymentUID || s.authority.Binding.StorageID != assignment.Plan.StorageID {
			return assignment, fmt.Errorf("invalid unused application assignment")
		}
	} else if assignment.State != "Consumed" || !validInitDigest(assignment.InitialEvidenceDigest) {
		return assignment, fmt.Errorf("invalid application assignment state")
	}
	return assignment, nil
}

// LoadV1Alpha2ApplicationInit reads an existing assignment; it never creates one.
func LoadV1Alpha2ApplicationInit(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef) (V1Alpha2ApplicationInitSnapshot, error) {
	if store == nil {
		return V1Alpha2ApplicationInitSnapshot{}, fmt.Errorf("missing status store")
	}
	s, err := store.Load(ctx, ref)
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, err
	}
	a, err := parseV1Alpha2ApplicationInit(s)
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, err
	}
	return V1Alpha2ApplicationInitSnapshot{status: s, assignment: a}, nil
}

// CommitV1Alpha2ApplicationInit atomically consumes an unused assignment and
// stores host-verified initial evidence. Matching readback is not ownership.
func CommitV1Alpha2ApplicationInit(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2ApplicationInitSnapshot, expected V1Alpha2ApplicationInitPlan, evidence json.RawMessage) (V1Alpha2ApplicationInitSnapshot, V1Alpha2CASObservation, error) {
	if store == nil || old.status.store != store || old.status.object == nil || old.assignment.State != "Unused" || old.assignment.Plan != expected || len(evidence) == 0 || len(evidence) > 64<<10 || bytes.Equal(bytes.TrimSpace(evidence), []byte("null")) || !json.Valid(evidence) {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("invalid initialization preconditions")
	}
	status, err := jsonObject(old.status.object["status"])
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, "", err
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, "", err
	}
	if _, ok := auth["evidence"]; ok {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("application evidence already exists")
	}
	if old.status.everApplicationReserved || !bytes.Equal(bytes.TrimSpace(auth["pendingWrite"]), []byte("false")) {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("pendingWrite must be false")
	}
	if _, ok := auth["pendingRequest"]; ok {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("pending request prevents initialization")
	}
	if _, ok := auth["transition"]; ok {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("transition prevents initialization")
	}
	for _, key := range []string{"pendingWrite", "pendingRequest", "transition", "evidence"} {
		if _, ok := status[key]; ok {
			return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("ambiguous top-level %s", key)
		}
	}
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(evidence))
	decoder.UseNumber()
	if err := decoder.Decode(&canonical); err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, "", err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, "", err
	}
	digest := sha256.Sum256(encoded)
	consumed := V1Alpha2ApplicationInitAssignment{Plan: expected, State: "Consumed", InitialEvidenceDigest: hex.EncodeToString(digest[:])}
	auth["evidence"] = bytes.Clone(encoded)
	status["authority"], _ = json.Marshal(auth)
	status["applicationInit"], _ = json.Marshal(consumed)
	statusRaw, _ := json.Marshal(status)
	obj := make(map[string]json.RawMessage, len(old.status.object))
	for k, v := range old.status.object {
		obj[k] = v
	}
	obj["status"] = statusRaw
	p, err := store.path(old.status.ref)
	if err != nil {
		return V1Alpha2ApplicationInitSnapshot{}, "", err
	}
	var response map[string]json.RawMessage
	err = store.Kube.Put(ctx, p+"/status", obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return V1Alpha2ApplicationInitSnapshot{}, "", fmt.Errorf("%w: %v", ErrV1Alpha2StatusConflict, err)
	}
	if err == nil {
		snap, e := store.snapshot(old.status.ref, response)
		if e == nil && snap.resourceVersion != old.status.resourceVersion && sameJSON(snap.object["status"], statusRaw) {
			a, e := parseV1Alpha2ApplicationInit(snap)
			if e == nil && a == consumed {
				return V1Alpha2ApplicationInitSnapshot{status: snap, assignment: a}, V1Alpha2CASApplied, nil
			}
		}
	}
	live, e := store.Load(ctx, old.status.ref)
	if e == nil && live.resourceVersion != old.status.resourceVersion {
		a, e := parseV1Alpha2ApplicationInit(live)
		if e == nil && a == consumed {
			return V1Alpha2ApplicationInitSnapshot{status: live, assignment: a}, V1Alpha2CASMatchingReadback, fmt.Errorf("%w: matching consumed assignment does not establish ownership", ErrV1Alpha2StatusOutcomeUnknown)
		}
	}
	return V1Alpha2ApplicationInitSnapshot{}, V1Alpha2CASUnknown, fmt.Errorf("%w: initialization outcome unknown", ErrV1Alpha2StatusOutcomeUnknown)
}
