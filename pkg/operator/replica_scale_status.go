package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// V1Alpha2ReplicaStep is the Host-approved membership transition target.
// It is durable authority state, not a scale-subresource observation.
type V1Alpha2ReplicaStep struct {
	Generation                int64     `json:"generation"`
	DeploymentUID             string    `json:"deploymentUID"`
	DeploymentResourceVersion string    `json:"deploymentResourceVersion"`
	ConfigID                  uint64    `json:"configID"`
	From                      int32     `json:"from"`
	To                        int32     `json:"to"`
	CheckID                   string    `json:"checkID,omitempty"`
	ExpiresAt                 time.Time `json:"expiresAt"`
}

// V1Alpha2ReplicaCheck is a one-use downscale proof challenge. Its nonce is
// not authorization until the Host records matching proof on the active step.
type V1Alpha2ReplicaCheck struct {
	ID                        string    `json:"id"`
	DeploymentResourceVersion string    `json:"deploymentResourceVersion"`
	ExpiresAt                 time.Time `json:"expiresAt"`
}

func validateV1Alpha2ReplicaGenesis(c V1Alpha2Cluster) error {
	if c.Spec.Replicas != nil && *c.Spec.Replicas != c.Spec.VoterCount {
		return fmt.Errorf("initial replicas must equal voterCount")
	}
	return nil
}

func (s V1Alpha2AdmissionSnapshot) ReplicaProposal() (int32, bool, error) {
	return replicaProposal(s.status)
}

func (s V1Alpha2AdmissionSnapshot) ReplicaStep() (V1Alpha2ReplicaStep, bool, error) {
	return replicaStep(s.status)
}

func (s V1Alpha2AdmissionSnapshot) ReplicaCheck() (V1Alpha2ReplicaCheck, bool, error) {
	return replicaCheck(s.status)
}

func replicaProposal(s V1Alpha2StatusSnapshot) (int32, bool, error) {
	spec, err := jsonObject(s.object["spec"])
	if err != nil {
		return 0, false, fmt.Errorf("invalid cluster spec")
	}
	raw, ok := spec["replicas"]
	if !ok {
		return 0, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false, fmt.Errorf("replicas cannot be null")
	}
	var proposal int32
	if json.Unmarshal(raw, &proposal) != nil || proposal < 3 || proposal > 64 {
		return 0, false, fmt.Errorf("replicas must be between 3 and 64")
	}
	return proposal, true, nil
}

func parseV1Alpha2ReplicaStep(s V1Alpha2StatusSnapshot) (*V1Alpha2ReplicaStep, *int32, error) {
	status, err := jsonObject(s.object["status"])
	if err != nil {
		return nil, nil, err
	}
	_, external, err := replicaProposal(s)
	if err != nil {
		return nil, nil, err
	}
	var applied *int32
	if raw, ok := status["appliedVoterCount"]; ok {
		if !external {
			return nil, nil, fmt.Errorf("manual cluster cannot record appliedVoterCount")
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, nil, fmt.Errorf("appliedVoterCount cannot be null")
		}
		var count int32
		if json.Unmarshal(raw, &count) != nil || count < 3 || count > 64 {
			return nil, nil, fmt.Errorf("invalid appliedVoterCount")
		}
		applied = &count
	}
	raw, ok := status["replicaStep"]
	if !ok {
		return nil, applied, nil
	}
	if !external {
		return nil, nil, fmt.Errorf("manual cluster cannot record replicaStep")
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, applied, fmt.Errorf("replicaStep cannot be null")
	}
	var step V1Alpha2ReplicaStep
	if decodeV1Alpha2(raw, &step) != nil {
		return nil, nil, fmt.Errorf("invalid replicaStep")
	}
	if err := validateV1Alpha2ReplicaStepBinding(s, step); err != nil {
		return nil, nil, err
	}
	return &step, applied, nil
}

func validateV1Alpha2ReplicaStepBinding(s V1Alpha2StatusSnapshot, step V1Alpha2ReplicaStep) error {
	if step.Generation != s.authority.Generation || step.DeploymentUID != s.authority.Binding.DeploymentUID || !v1alpha2NonBlank.MatchString(step.DeploymentUID) || len(step.DeploymentUID) > 256 || !v1alpha2NonBlank.MatchString(step.DeploymentResourceVersion) || len(step.DeploymentResourceVersion) > 256 || step.ConfigID == 0 || step.ConfigID > uint64(math.MaxInt64-2) || step.From < 3 || step.From > 64 || step.To < 3 || step.To > 64 || step.From == step.To || step.To != step.From+1 && step.To != step.From-1 || step.CheckID != "" && !validInitString(step.CheckID) || step.ExpiresAt.IsZero() {
		return fmt.Errorf("invalid replica step binding")
	}
	return nil
}

func replicaStep(s V1Alpha2StatusSnapshot) (V1Alpha2ReplicaStep, bool, error) {
	step, _, err := parseV1Alpha2ReplicaStep(s)
	if err != nil || step == nil {
		return V1Alpha2ReplicaStep{}, false, err
	}
	return *step, true, nil
}

func parseV1Alpha2ReplicaCheck(s V1Alpha2StatusSnapshot) (*V1Alpha2ReplicaCheck, error) {
	status, err := jsonObject(s.object["status"])
	if err != nil {
		return nil, err
	}
	raw, ok := status["replicaCheck"]
	if !ok {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("replicaCheck cannot be null")
	}
	var check V1Alpha2ReplicaCheck
	if decodeV1Alpha2(raw, &check) != nil || validateV1Alpha2ReplicaCheck(check) != nil {
		return nil, fmt.Errorf("invalid replicaCheck")
	}
	step, _, err := parseV1Alpha2ReplicaStep(s)
	if err != nil || step == nil || step.To >= step.From {
		return nil, fmt.Errorf("replicaCheck requires an active down step")
	}
	if step.CheckID != "" && (step.CheckID != check.ID || step.DeploymentResourceVersion != check.DeploymentResourceVersion) {
		return nil, fmt.Errorf("replicaCheck proof mismatch")
	}
	return &check, nil
}

func validateV1Alpha2ReplicaCheck(check V1Alpha2ReplicaCheck) error {
	if !validInitString(check.ID) || !v1alpha2NonBlank.MatchString(check.DeploymentResourceVersion) || len(check.DeploymentResourceVersion) > 256 || check.ExpiresAt.IsZero() {
		return fmt.Errorf("invalid replica check")
	}
	return nil
}

func replicaCheck(s V1Alpha2StatusSnapshot) (V1Alpha2ReplicaCheck, bool, error) {
	check, err := parseV1Alpha2ReplicaCheck(s)
	if err != nil || check == nil {
		return V1Alpha2ReplicaCheck{}, false, err
	}
	return *check, true, nil
}

func effectiveVoterTarget(s V1Alpha2StatusSnapshot) (int, error) {
	spec, err := jsonObject(s.object["spec"])
	if err != nil {
		return 0, err
	}
	var voterCount int
	if json.Unmarshal(spec["voterCount"], &voterCount) != nil || voterCount < 3 || voterCount > 64 {
		return 0, fmt.Errorf("invalid target voter count")
	}
	_, external, err := replicaProposal(s)
	if err != nil {
		return 0, err
	}
	if !external {
		return voterCount, nil
	}
	if step, _, err := parseV1Alpha2ReplicaStep(s); err != nil {
		return 0, err
	} else if step != nil {
		return int(step.To), nil
	}
	if s.appliedVoterCount != nil {
		return int(*s.appliedVoterCount), nil
	}
	// The initial external target is the existing voterCount. A later
	// completed step persists its certified To in appliedVoterCount.
	return voterCount, nil
}

func replicaMovesToward(from, to, proposal int32) bool {
	if proposal > from {
		return to == from+1 && to <= proposal
	}
	if proposal < from {
		return to == from-1 && to >= proposal
	}
	return false
}

func validateV1Alpha2ReplicaRepairStep(s V1Alpha2AdmissionSnapshot, plan V1Alpha2RepairPlan) error {
	_, external, err := s.ReplicaProposal()
	if err != nil || !external {
		return err
	}
	step, active, err := s.ReplicaStep()
	if err != nil {
		return err
	}
	if !active {
		if plan.Sequence == V1Alpha2RepairAdd || plan.Sequence == V1Alpha2RepairLeave {
			return fmt.Errorf("external replica control requires an active step for add or leave")
		}
		return nil
	}
	if previous, ok := s.Repair(); ok && previous.Completion != nil && previous.Completion.ConfigID != step.ConfigID {
		return fmt.Errorf("completed replica step requires Host completion before another repair")
	}
	switch {
	case step.To < step.From:
		if plan.Sequence != V1Alpha2RepairLeave || plan.ExpectedConfigID != step.ConfigID || len(plan.FinalVoterIDs) != int(step.To) {
			return fmt.Errorf("down step permits only the exact leave plan")
		}
	case step.To > step.From:
		if plan.Sequence != V1Alpha2RepairAdd || plan.ExpectedConfigID != step.ConfigID || len(plan.FinalVoterIDs) != int(step.To) {
			return fmt.Errorf("up step permits only the exact add plan")
		}
	default:
		return fmt.Errorf("invalid replica step direction")
	}
	return nil
}

func replicaSelector(view V1Alpha2AuthorityView) string {
	return "rhiza.mrchypark.dev/cluster-uid=" + view.Owner.UID + ",rhiza.mrchypark.dev/generation=" + strconv.FormatInt(view.Generation, 10)
}

func replicaStepPending(s V1Alpha2AdmissionSnapshot) bool {
	_, pending := s.Pending()
	return pending || hasV1Alpha2Transition(s.Transition())
}

func replicaStepDirectionMatches(step V1Alpha2ReplicaStep, sequence V1Alpha2RepairSequence, finalCount int) bool {
	return step.ConfigID <= uint64(math.MaxInt64-2) && finalCount == int(step.To) && ((step.To < step.From && sequence == V1Alpha2RepairLeave) || (step.To > step.From && sequence == V1Alpha2RepairAdd))
}

func replicaStepClosedRepairMatches(step V1Alpha2ReplicaStep, record V1Alpha2RepairRecord, view V1Alpha2AuthorityView) bool {
	plan := record.Plan
	return record.State == "Closed" && plan.ClusterUID == view.Owner.UID && plan.StorageID == view.Binding.StorageID && plan.Generation == step.Generation && plan.DeploymentUID == step.DeploymentUID && plan.ExpectedConfigID == step.ConfigID && replicaStepDirectionMatches(step, plan.Sequence, len(plan.FinalVoterIDs))
}

func replicaStepTerminalMatches(step V1Alpha2ReplicaStep, record V1Alpha2RepairRecord, view V1Alpha2AuthorityView) bool {
	plan := record.Plan
	return record.State == "Completed" && record.Completion != nil && record.Completion.Kind == "Terminal" && record.Completion.ConfigID == step.ConfigID+1 && plan.ClusterUID == view.Owner.UID && plan.StorageID == view.Binding.StorageID && plan.Generation == step.Generation && plan.DeploymentUID == step.DeploymentUID && plan.ExpectedConfigID == step.ConfigID && replicaStepDirectionMatches(step, plan.Sequence, len(plan.FinalVoterIDs))
}

// BeginV1Alpha2ReplicaCheck installs a fresh one-use downscale nonce. It clears
// any old step check reference in the same CAS and ignores old expiry/RV.
func BeginV1Alpha2ReplicaCheck(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, check V1Alpha2ReplicaCheck) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.Authority() != expected {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica check authority mismatch")
	}
	if _, external, err := old.ReplicaProposal(); err != nil || !external {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica check requires external replica control")
	}
	step, active, err := old.ReplicaStep()
	if err != nil || !active || step.From <= step.To {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica check requires an active down step")
	}
	if existing, ok, err := old.ReplicaCheck(); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	} else if ok && existing.ExpiresAt.After(time.Now()) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("unexpired replica check already owns the attempt")
	}
	if replicaStepPending(old) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("transition or pending write blocks replica check")
	}
	if repair, ok := old.Repair(); ok && repair.State == "Closed" {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("closed repair blocks replica check")
	}
	if validateV1Alpha2ReplicaCheck(check) != nil || !check.ExpiresAt.After(time.Now()) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("invalid or expired replica check")
	}
	step.CheckID = ""
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, _ map[string]json.RawMessage) {
		status["replicaStep"], _ = json.Marshal(step)
		status["replicaCheck"], _ = json.Marshal(check)
	})
}

// AbandonV1Alpha2ReplicaCheck clears only the nonce owned by this invocation.
func AbandonV1Alpha2ReplicaCheck(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, checkID string) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.Authority() != expected {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica check authority mismatch")
	}
	check, ok, err := old.ReplicaCheck()
	if err != nil || !ok || check.ID != checkID {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica check changed")
	}
	step, active, err := old.ReplicaStep()
	if err != nil || !active {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step changed")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, _ map[string]json.RawMessage) {
		delete(status, "replicaCheck")
		if step.CheckID != "" {
			step.CheckID = ""
			status["replicaStep"], _ = json.Marshal(step)
		}
	})
}

func CommitV1Alpha2ReplicaStep(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, next *V1Alpha2ReplicaStep) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.Authority() != expected {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step authority mismatch")
	}
	proposal, external, err := old.ReplicaProposal()
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if !external {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step requires external replica control")
	}
	current, hasCurrent, err := old.ReplicaStep()
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	check, hasCheck, err := old.ReplicaCheck()
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if next == nil {
		if !hasCurrent {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("no replica step to complete")
		}
		repair, ok := old.Repair()
		if !ok || !replicaStepTerminalMatches(current, repair, expected) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step completion requires its exact certified terminal")
		}
		return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, _ map[string]json.RawMessage) {
			delete(status, "replicaStep")
			delete(status, "replicaCheck")
			status["appliedVoterCount"], _ = json.Marshal(current.To)
		})
	}
	if replicaStepPending(old) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("transition or pending write blocks replica step")
	}
	if err := validateV1Alpha2ReplicaStepBinding(old.status, *next); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	now := time.Now()
	if hasCheck {
		if next.From <= next.To || next.CheckID != check.ID || next.DeploymentResourceVersion != check.DeploymentResourceVersion || !check.ExpiresAt.After(now) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step requires matching fresh downscale proof")
		}
	} else if next.CheckID != "" {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step has no matching downscale check")
	}
	if repair, ok := old.Repair(); ok && repair.State == "Closed" && !replicaStepClosedRepairMatches(*next, repair, expected) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("closed repair conflicts with replica step")
	}
	if hasCurrent {
		if next.From != current.From || next.To != current.To || next.Generation != current.Generation || next.DeploymentUID != current.DeploymentUID || next.ConfigID != current.ConfigID {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step pair and binding are immutable")
		}
		if next.DeploymentResourceVersion == current.DeploymentResourceVersion && next.ExpiresAt.Equal(current.ExpiresAt) && next.CheckID == current.CheckID {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step reapproval changed neither deployment revision nor expiry")
		}
	} else {
		if !replicaMovesToward(next.From, next.To, proposal) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step must advance one voter toward the proposal")
		}
		target, err := old.TargetVoterCount()
		if err != nil || next.From != int32(target) {
			return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step must start at the certified target")
		}
	}
	if !next.ExpiresAt.After(now) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica step approval is expired")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, _ map[string]json.RawMessage) {
		status["replicaStep"], _ = json.Marshal(next)
	})
}

// ReportV1Alpha2ReplicaScale records scale-subresource observations only.
func ReportV1Alpha2ReplicaScale(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, replicas int32, selector string) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.Authority() != expected {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica scale authority mismatch")
	}
	if replicas < 0 || replicas > 64 {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("observed replicas must be between 0 and 64")
	}
	if selector != replicaSelector(expected) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("replica selector differs from active authority")
	}
	return updateV1Alpha2AdmissionStatus(ctx, store, old, func(status, _ map[string]json.RawMessage) {
		status["replicas"], _ = json.Marshal(replicas)
		status["selector"], _ = json.Marshal(selector)
	})
}
