package managedsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	replicaStepTTL     = 15 * time.Second
	replicaStepRenewal = 10 * time.Second
)

type replicaLeaveProof struct {
	Step      operator.V1Alpha2ReplicaStep
	Cluster   quepaxa.Cluster
	Missing   quepaxa.NodeID
	Survivors []quepaxa.NodeID
	Repair    *operator.V1Alpha2RepairRecord
}

// ReconcileExternalReplicas advances at most one Host-authorized replica step.
// The Deployment is changed only by the operator; this method changes
// membership only after the exact approved Pod topology appears.
func (h *Host) ReconcileExternalReplicas(ctx context.Context) (string, error) {
	h.replicaMu.Lock()
	defer h.replicaMu.Unlock()

	a, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if _, external, err := a.ReplicaProposal(); err != nil {
		return "proposal", err
	} else if !external {
		return "disabled", nil
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}

	if record, ok := a.Repair(); ok && record.State == "Closed" {
		if record.Plan.Sequence == operator.V1Alpha2RepairAdd || record.Plan.Sequence == operator.V1Alpha2RepairLeave {
			return h.resumeExternalReplicaRepair(ctx, record)
		}
		// An ordinary ADD-first replacement remains owned by the existing path.
		return "ordinary-repair", nil
	}

	step, present, err := a.ReplicaStep()
	if err != nil {
		return "step", err
	}
	if present {
		return h.reconcileActiveReplicaStep(ctx, a, step)
	}
	if h.replicaDecisionBlocked(a) {
		return "frozen", fmt.Errorf("pending application write or transition blocks replica scale")
	}

	proposal, enabled, err := a.ReplicaProposal()
	if err != nil || !enabled {
		return "proposal", err
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return "membership", err
	}
	current := len(snapshot.Current.Members)
	if snapshot.PendingOK || current < 3 || int(proposal) == current {
		return "idle", nil
	}
	to := current + 1
	if proposal < int32(current) {
		to = current - 1
	}
	if to < 3 || to > 64 {
		return "blocked", fmt.Errorf("external target cannot produce a bounded one-voter step")
	}
	if err := h.proveFullReplicaRoster(ctx, snapshot); err != nil {
		return h.replicaOrdinaryFallback(ctx, err)
	}
	observation, err := h.observeReplicaDeployment(ctx)
	if err != nil {
		return h.replicaOrdinaryFallback(ctx, err)
	}
	if observation.DesiredReplicas != int32(current) {
		return h.replicaOrdinaryFallback(ctx, fmt.Errorf("Deployment differs from the certified current replica count"))
	}
	if !replicaDeploymentStable(observation, int32(current)) {
		return h.replicaOrdinaryFallback(ctx, fmt.Errorf("Deployment status is not stable at the current count"))
	}
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	if h.replicaDecisionBlocked(latest) {
		return "frozen", fmt.Errorf("authority changed before replica step proposal")
	}
	latestTarget, enabled, err := latest.ReplicaProposal()
	if err != nil || !enabled || latestTarget != proposal {
		return h.replicaOrdinaryFallback(ctx, errors.Join(fmt.Errorf("external target changed during replica proof"), err))
	}
	if step, present, err := latest.ReplicaStep(); err != nil || present {
		return "step", errors.Join(fmt.Errorf("another replica step appeared during proof"), err, stepError(present, step))
	}
	next := &operator.V1Alpha2ReplicaStep{
		Generation: h.authority.Generation, DeploymentUID: h.authority.Binding.DeploymentUID,
		DeploymentResourceVersion: observation.DeploymentResourceVersion, ConfigID: uint64(snapshot.Current.ConfigID),
		From: int32(current), To: int32(to), ExpiresAt: time.Now().Add(replicaStepTTL),
	}
	_, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, h.cfg.Store, latest, h.authority, next)
	if err != nil {
		return "propose", err
	}
	if outcome != operator.V1Alpha2CASApplied {
		return "propose", fmt.Errorf("replica step proposal CAS outcome %s", outcome)
	}
	return "proposed", nil
}

func stepError(present bool, step operator.V1Alpha2ReplicaStep) error {
	if present {
		return fmt.Errorf("replica step from %d to %d is already active", step.From, step.To)
	}
	return nil
}

func (h *Host) replicaOrdinaryFallback(ctx context.Context, cause error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "ordinary-repair", err
	}
	a, err := h.live(ctx)
	if err != nil {
		return "frozen", errors.Join(cause, err)
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", errors.Join(cause, err)
	}
	if _, present, err := a.ReplicaStep(); err != nil {
		return "frozen", errors.Join(cause, err)
	} else if present {
		return "frozen", errors.Join(cause, fmt.Errorf("a replica step appeared during preflight"))
	}
	if h.replicaDecisionBlocked(a) {
		return "frozen", errors.Join(cause, fmt.Errorf("pending application write or transition blocks replica scale"))
	}
	// Preserve same-size append-only repair when external scale preflight is
	// unhealthy or changing. The ordinary path re-observes exact Pods itself.
	return "ordinary-repair", nil
}

func (h *Host) reconcileActiveReplicaStep(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep) (string, error) {
	_, external, err := a.ReplicaProposal()
	if err != nil || !external {
		return "proposal", errors.Join(fmt.Errorf("active replica step requires external control"), err)
	}
	if h.replicaDecisionBlocked(a) {
		return "frozen", fmt.Errorf("pending application write or transition blocks replica step")
	}
	observation, err := h.observeReplicaDeployment(ctx)
	if err != nil {
		return "deployment", err
	}
	if observation.DesiredReplicas != step.From && observation.DesiredReplicas != step.To {
		return "blocked", fmt.Errorf("Deployment desired count is neither side of the approved step")
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return "membership", err
	}
	if snapshot.PendingOK || snapshot.Current.ConfigID != uint(step.ConfigID) && snapshot.Current.ConfigID != uint(step.ConfigID+1) {
		return "blocked", fmt.Errorf("certified membership no longer matches the approved replica step")
	}

	if snapshot.Current.ConfigID == uint(step.ConfigID+1) {
		return h.finishReplicaStep(ctx, a, step, observation, snapshot)
	}
	if len(snapshot.Current.Members) != int(step.From) {
		return "blocked", fmt.Errorf("certified voter count differs from the approved step source")
	}
	if observation.DesiredReplicas == step.From {
		if step.To < step.From {
			return h.reconcileReplicaDownUnapplied(ctx, a, step, observation, snapshot)
		}
		if err := h.proveFullReplicaRoster(ctx, snapshot); err != nil {
			return "health", err
		}
		observation, err = h.observeReplicaDeployment(ctx)
		if err != nil {
			return "deployment", err
		}
		if observation.DesiredReplicas != step.From {
			return "blocked", fmt.Errorf("Deployment changed during replica step revalidation")
		}
		if !replicaDeploymentStable(observation, step.From) {
			return "waiting-deployment", nil
		}
		return h.renewReplicaStepIfNeeded(ctx, a, step, observation)
	}

	pods, err := h.observePods(ctx)
	if err != nil {
		return "pods", err
	}
	if step.To > step.From {
		return h.reconcileReplicaUp(ctx, a, step, observation, snapshot, pods)
	}
	return h.reconcileReplicaDown(ctx, a, step, observation, snapshot, pods)
}

func (h *Host) reconcileReplicaDownUnapplied(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep, observation operator.V1Alpha2ReplicaObservation, snapshot node.ManagedRepairSnapshot) (string, error) {
	check, hasCheck, err := a.ReplicaCheck()
	if err != nil {
		return "check", err
	}
	now := time.Now()
	if !hasCheck {
		if step.CheckID != "" {
			return "frozen", fmt.Errorf("down step retains a check ID without its live challenge")
		}
		if step.DeploymentResourceVersion == observation.DeploymentResourceVersion {
			return "waiting-check", nil
		}
		if !replicaDeploymentStable(observation, step.From) {
			return "waiting-deployment", nil
		}
		latest, err := h.live(ctx)
		if err != nil {
			return "authority", err
		}
		_, present, err := latest.ReplicaCheck()
		current, active, stepErr := latest.ReplicaStep()
		if err != nil || stepErr != nil || present || !active || !sameReplicaStep(current, step) || h.replicaDecisionBlocked(latest) {
			return "renew", errors.Join(fmt.Errorf("unproven down step changed before revision refresh"), err, stepErr)
		}
		if err := h.replicaDRBlocked(ctx); err != nil {
			return "frozen", err
		}
		next := current
		next.DeploymentResourceVersion = observation.DeploymentResourceVersion
		next.ExpiresAt = now.Add(replicaStepTTL)
		_, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, h.cfg.Store, latest, h.authority, &next)
		if err != nil {
			return "renew", err
		}
		if outcome != operator.V1Alpha2CASApplied {
			return "renew", fmt.Errorf("unproven down step refresh CAS outcome %s", outcome)
		}
		return "refreshed-unproven", nil
	}
	if err := validateReplicaChallenge(check, observation, step.From, now); err != nil {
		return "waiting-check", nil
	}
	if step.CheckID == check.ID && step.DeploymentResourceVersion == check.DeploymentResourceVersion && step.ExpiresAt.After(now) {
		return "waiting-apply", nil
	}
	if err := h.proveFullReplicaRoster(ctx, snapshot); err != nil {
		return "health", err
	}
	observation, err = h.observeReplicaDeployment(ctx)
	if err != nil {
		return "deployment", err
	}
	if err := validateReplicaChallenge(check, observation, step.From, time.Now()); err != nil {
		return "blocked", fmt.Errorf("Deployment changed during fresh downscale proof")
	}
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	latestCheck, present, err := latest.ReplicaCheck()
	current, active, stepErr := latest.ReplicaStep()
	if err != nil || stepErr != nil || !present || !sameReplicaCheck(latestCheck, check) || !active || !sameReplicaStepIntent(current, step) || h.replicaDecisionBlocked(latest) {
		return "blocked", errors.Join(fmt.Errorf("live downscale challenge changed during fresh proof"), err, stepErr)
	}
	commitNow := time.Now()
	if err := validateReplicaChallenge(latestCheck, observation, step.From, commitNow); err != nil {
		return "waiting-check", nil
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	if current.CheckID == check.ID && current.DeploymentResourceVersion == check.DeploymentResourceVersion && current.ExpiresAt.After(commitNow) {
		return "waiting-apply", nil
	}
	if current.CheckID != "" {
		return "blocked", fmt.Errorf("down step already names another proof challenge")
	}
	deadline := replicaProofDeadline(check, commitNow)
	if !deadline.After(commitNow) {
		return "waiting-check", nil
	}
	next := current
	next.DeploymentResourceVersion = check.DeploymentResourceVersion
	next.CheckID = check.ID
	next.ExpiresAt = deadline
	_, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, h.cfg.Store, latest, h.authority, &next)
	if err != nil {
		return "prove-down", err
	}
	if outcome != operator.V1Alpha2CASApplied {
		return "prove-down", fmt.Errorf("downscale proof CAS outcome %s", outcome)
	}
	return "proved-down", nil
}

func (h *Host) reconcileReplicaUp(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep, observation operator.V1Alpha2ReplicaObservation, snapshot node.ManagedRepairSnapshot, pods []operator.V1Alpha2ManagedPodObservation) (string, error) {
	if observation.ObservedReplicas != step.To || observation.UpdatedReplicas != step.To {
		return "waiting-deployment", nil
	}
	replacement, err := replicaUpReplacement(snapshot.Current, pods)
	if err != nil {
		if len(pods) == len(snapshot.Current.Members) {
			return "waiting-pod", nil
		}
		return "pods", err
	}
	if err := h.proveReplicaUpRoster(ctx, snapshot, replacement); err != nil {
		return "health", err
	}
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	currentStep, present, err := latest.ReplicaStep()
	if err != nil || !present || !sameReplicaStep(currentStep, step) || h.replicaDecisionBlocked(latest) {
		return "frozen", errors.Join(fmt.Errorf("approved up step changed before ADD"), err)
	}
	plan, err := h.planVoterEnrollment(ctx, latest, snapshot, int(step.To), replacement)
	if err != nil {
		return "plan-add", err
	}
	if plan == nil || plan.Sequence != operator.V1Alpha2RepairAdd || plan.ExpectedConfigID != step.ConfigID || len(plan.FinalVoterIDs) != int(step.To) {
		return "plan-add", fmt.Errorf("external replica ADD does not match the approved step")
	}
	if err := h.replaceVoterLocked(ctx, *plan); err != nil {
		return "membership-add", err
	}
	return h.finishReplicaStepAfterRepair(ctx, step)
}

func (h *Host) reconcileReplicaDown(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep, observation operator.V1Alpha2ReplicaObservation, snapshot node.ManagedRepairSnapshot, pods []operator.V1Alpha2ManagedPodObservation) (string, error) {
	if observation.ObservedReplicas != step.To || observation.UpdatedReplicas != step.To {
		return "waiting-deployment", nil
	}
	missing, survivors, err := replicaDownRemoval(snapshot.Current, pods)
	if err != nil {
		if len(pods) == len(snapshot.Current.Members) {
			return "waiting-pod", nil
		}
		return "pods", err
	}
	if len(survivors) != int(step.To) || missing.ID == h.cfg.Runtime.NodeID {
		// The victim runtime cannot remove itself; a surviving Host resumes it.
		return "waiting-survivor", nil
	}
	if err := h.proveReplicaRosterExcept(ctx, snapshot, missing.ID, nil); err != nil {
		return "health", err
	}
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	currentStep, present, err := latest.ReplicaStep()
	if err != nil || !present || !sameReplicaStep(currentStep, step) || h.replicaDecisionBlocked(latest) {
		return "frozen", errors.Join(fmt.Errorf("approved down step changed before REMOVE"), err)
	}
	proof := replicaLeaveProof{Step: step, Cluster: snapshot.Current, Missing: missing.ID}
	for _, pod := range survivors {
		proof.Survivors = append(proof.Survivors, pod.Member.NodeID)
	}
	slices.Sort(proof.Survivors)
	if err := h.leaveVoterLocked(ctx, LeaveVoterRequest{NodeID: missing.ID}, &proof); err != nil {
		return "membership-remove", err
	}
	return h.finishReplicaStepAfterRepair(ctx, step)
}

func (h *Host) resumeExternalReplicaRepair(ctx context.Context, record operator.V1Alpha2RepairRecord) (string, error) {
	a, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	step, present, err := a.ReplicaStep()
	if err != nil || !present || !replicaStepPlanMatches(step, record.Plan) {
		return "frozen", errors.Join(fmt.Errorf("closed repair does not match the active replica step"), err)
	}
	switch record.Plan.Sequence {
	case operator.V1Alpha2RepairAdd:
		if err := h.replaceVoterLocked(ctx, record.Plan); err != nil {
			return "resume-add", err
		}
		return h.finishReplicaStepAfterRepair(ctx, step)
	case operator.V1Alpha2RepairLeave:
		snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil {
			return "resume-leave", err
		}
		if snapshot.Current.ConfigID == uint(step.ConfigID+1) {
			if err := h.resumeLeave(ctx, record); err != nil {
				return "resume-leave-terminal", err
			}
			return h.finishReplicaStepAfterRepair(ctx, step)
		}
		if snapshot.Current.ConfigID != uint(step.ConfigID) {
			return "resume-leave", fmt.Errorf("closed leave membership is neither its source nor target")
		}
		observation, err := h.observeReplicaDeployment(ctx)
		if err != nil {
			return "resume-leave", err
		}
		if observation.DesiredReplicas != step.To || observation.ObservedReplicas != step.To || observation.UpdatedReplicas != step.To {
			return "waiting-deployment", nil
		}
		pods, err := h.observePods(ctx)
		if err != nil {
			return "resume-leave", err
		}
		missing, survivors, err := replicaDownRemoval(snapshot.Current, pods)
		if err != nil || missing.ID != record.Plan.OldNodeID {
			return "resume-leave", errors.Join(fmt.Errorf("closed leave victim is not the single actual removed voter"), err)
		}
		proof := replicaLeaveProof{Step: step, Cluster: snapshot.Current, Missing: missing.ID, Repair: &record}
		for _, pod := range survivors {
			proof.Survivors = append(proof.Survivors, pod.Member.NodeID)
		}
		slices.Sort(proof.Survivors)
		if err := h.proveReplicaRosterExcept(ctx, snapshot, missing.ID, &record.Plan); err != nil {
			return "resume-leave-health", err
		}
		latest, err := h.live(ctx)
		if err != nil {
			return "authority", err
		}
		current, ok := latest.Repair()
		if !ok || !reflect.DeepEqual(current.Plan, record.Plan) || current.State != "Closed" || replicaDecisionBlockedWith(latest, &record.Plan) {
			return "frozen", fmt.Errorf("closed leave changed before survivor-certified mutation")
		}
		if err := h.leaveVoterLocked(ctx, LeaveVoterRequest{NodeID: missing.ID}, &proof); err != nil {
			return "resume-leave", err
		}
		return h.finishReplicaStepAfterRepair(ctx, step)
	default:
		return "ordinary-repair", nil
	}
}

func (h *Host) verifyReplicaLeaveBeforeRemove(ctx context.Context, record operator.V1Alpha2RepairRecord, proof replicaLeaveProof) error {
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	current, ok := a.Repair()
	step, present, stepErr := a.ReplicaStep()
	if !ok || current.State != "Closed" || !reflect.DeepEqual(current.Plan, record.Plan) || stepErr != nil || !present || !sameReplicaStep(step, proof.Step) || replicaDecisionBlockedWith(a, &record.Plan) {
		return errors.Join(fmt.Errorf("closed leave changed before final survivor proof"), stepErr)
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if snapshot.PendingOK || snapshot.Current.ConfigID != uint(step.ConfigID) || len(snapshot.Current.Members) != int(step.From) {
		return fmt.Errorf("leave source membership changed before final survivor proof")
	}
	observation, err := h.observeReplicaDeployment(ctx)
	if err != nil {
		return err
	}
	if observation.DesiredReplicas != step.To || observation.ObservedReplicas != step.To || observation.UpdatedReplicas != step.To {
		return fmt.Errorf("Deployment is not at the approved actual-removal count")
	}
	pods, err := h.observePods(ctx)
	if err != nil {
		return err
	}
	missing, survivors, err := replicaDownRemoval(snapshot.Current, pods)
	if err != nil || missing.ID != record.Plan.OldNodeID {
		return errors.Join(fmt.Errorf("actual removed voter changed before final leave proof"), err)
	}
	proof.Cluster = snapshot.Current
	proof.Missing = missing.ID
	proof.Survivors = proof.Survivors[:0]
	proof.Repair = &record
	for _, pod := range survivors {
		proof.Survivors = append(proof.Survivors, pod.Member.NodeID)
	}
	slices.Sort(proof.Survivors)
	if err := validateReplicaLeaveProof(a, LeaveVoterRequest{NodeID: missing.ID}, proof); err != nil {
		return err
	}
	if err := h.proveReplicaRosterExcept(ctx, snapshot, missing.ID, &record.Plan); err != nil {
		return err
	}
	latest, err := h.live(ctx)
	if err != nil {
		return err
	}
	latestRepair, ok := latest.Repair()
	latestStep, present, err := latest.ReplicaStep()
	if !ok || latestRepair.State != "Closed" || !reflect.DeepEqual(latestRepair.Plan, record.Plan) || err != nil || !present || !sameReplicaStep(latestStep, step) || replicaDecisionBlockedWith(latest, &record.Plan) {
		return errors.Join(fmt.Errorf("closed leave changed after final survivor proof"), err)
	}
	return h.replicaDRBlocked(ctx)
}

func (h *Host) finishReplicaStepAfterRepair(ctx context.Context, step operator.V1Alpha2ReplicaStep) (string, error) {
	a, err := h.live(ctx)
	if err != nil {
		return "terminal-proof", err
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return "terminal-proof", err
	}
	return h.finishReplicaStep(ctx, a, step, operator.V1Alpha2ReplicaObservation{}, snapshot)
}

func (h *Host) finishReplicaStep(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep, supplied operator.V1Alpha2ReplicaObservation, snapshot node.ManagedRepairSnapshot) (string, error) {
	if snapshot.Current.ConfigID == 0 {
		var err error
		snapshot, err = h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil {
			return "terminal-proof", err
		}
	}
	if snapshot.PendingOK || snapshot.Current.ConfigID != uint(step.ConfigID+1) || len(snapshot.Current.Members) != int(step.To) {
		return "waiting-terminal", nil
	}
	record, ok := a.Repair()
	if !ok || record.State != "Completed" || record.Completion == nil || record.Plan.ExpectedConfigID != step.ConfigID || record.Completion.ConfigID != step.ConfigID+1 || record.Completion.Kind != "Terminal" {
		return "blocked", fmt.Errorf("step has no exact completed repair terminal")
	}
	switch record.Plan.Sequence {
	case operator.V1Alpha2RepairAdd:
		if step.To != step.From+1 {
			return "blocked", fmt.Errorf("completed ADD does not match the approved up step")
		}
		if err := h.verifyCompletedEnrollment(ctx, record); err != nil {
			return "terminal-proof", err
		}
	case operator.V1Alpha2RepairLeave:
		if step.To != step.From-1 {
			return "blocked", fmt.Errorf("completed leave does not match the approved down step")
		}
		if err := h.verifyCompletedLeave(ctx, record); err != nil {
			return "terminal-proof", err
		}
	default:
		return "blocked", fmt.Errorf("unexpected repair sequence completed a replica step")
	}
	ids := make([]string, len(snapshot.Current.Members))
	for i, member := range snapshot.Current.Members {
		ids[i] = string(member.ID)
	}
	final := make([]string, len(record.Plan.FinalVoterIDs))
	for i, member := range record.Plan.FinalVoterIDs {
		final[i] = string(member)
	}
	slices.Sort(ids)
	slices.Sort(final)
	if !slices.Equal(ids, final) {
		return "blocked", fmt.Errorf("completed repair voter IDs differ from the current certified roster")
	}
	if err := h.proveFullReplicaRoster(ctx, snapshot); err != nil {
		return "health", err
	}
	observation := supplied
	if observation.DeploymentResourceVersion == "" {
		var err error
		observation, err = h.observeReplicaDeployment(ctx)
		if err != nil {
			return "deployment", err
		}
	}
	if observation.DesiredReplicas != step.To {
		return "blocked", fmt.Errorf("Deployment did not retain the approved terminal replica count")
	}
	if !replicaDeploymentStable(observation, step.To) {
		return "waiting-deployment", nil
	}
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	if h.replicaDecisionBlocked(latest) {
		return "frozen", fmt.Errorf("authority changed before terminal replica step clear")
	}
	if _, external, err := latest.ReplicaProposal(); err != nil || !external {
		return "blocked", errors.Join(fmt.Errorf("terminal replica step lost external control"), err)
	}
	currentStep, present, err := latest.ReplicaStep()
	if err != nil || !present || !sameReplicaStep(currentStep, step) {
		return "blocked", errors.Join(fmt.Errorf("replica step changed before terminal clear"), err)
	}
	_, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, h.cfg.Store, latest, h.authority, nil)
	if err != nil {
		return "clear", err
	}
	if outcome != operator.V1Alpha2CASApplied {
		return "clear", fmt.Errorf("replica step clear CAS outcome %s", outcome)
	}
	return "completed", nil
}

func (h *Host) renewReplicaStepIfNeeded(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, step operator.V1Alpha2ReplicaStep, observation operator.V1Alpha2ReplicaObservation) (string, error) {
	if step.ExpiresAt.After(time.Now().Add(replicaStepRenewal)) && step.DeploymentResourceVersion == observation.DeploymentResourceVersion {
		return "waiting-deployment", nil
	}
	next := step
	next.DeploymentResourceVersion = observation.DeploymentResourceVersion
	next.ExpiresAt = time.Now().Add(replicaStepTTL)
	latest, err := h.live(ctx)
	if err != nil {
		return "authority", err
	}
	if err := h.replicaDRBlocked(ctx); err != nil {
		return "frozen", err
	}
	if h.replicaDecisionBlocked(latest) {
		return "frozen", fmt.Errorf("authority changed before replica step renewal")
	}
	current, present, err := latest.ReplicaStep()
	if err != nil || !present || !sameReplicaStep(current, step) {
		return "renew", errors.Join(fmt.Errorf("replica step changed before renewal"), err)
	}
	_, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, h.cfg.Store, latest, h.authority, &next)
	if err != nil {
		return "renew", err
	}
	if outcome != operator.V1Alpha2CASApplied {
		return "renew", fmt.Errorf("replica step renewal CAS outcome %s", outcome)
	}
	return "renewed", nil
}

func (h *Host) proveFullReplicaRoster(ctx context.Context, expected node.ManagedRepairSnapshot) error {
	return h.proveReplicaRosterExcept(ctx, expected, "", nil)
}

func (h *Host) proveReplicaUpRoster(ctx context.Context, expected node.ManagedRepairSnapshot, replacement operator.V1Alpha2RepairPodRequest) error {
	before, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !sameReplicaSnapshot(before, expected) {
		return fmt.Errorf("certified roster changed before replica ADD proof")
	}
	pods, err := h.observePods(ctx)
	if err != nil {
		return err
	}
	if actual, err := replicaUpReplacement(expected.Current, pods); err != nil || actual != replacement {
		return errors.Join(fmt.Errorf("live ADD candidate changed before voter probes"), err)
	}
	if err := h.node.ProbeManagedReplicaVoters(ctx, expected.Current, expected.AbortSlot); err != nil {
		return err
	}
	after, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !sameReplicaSnapshot(after, expected) {
		return fmt.Errorf("certified roster changed during replica ADD proof")
	}
	finalPods, err := h.observePods(ctx)
	if err != nil {
		return err
	}
	if actual, err := replicaUpReplacement(expected.Current, finalPods); err != nil || actual != replacement {
		return errors.Join(fmt.Errorf("live ADD candidate changed during voter probes"), err)
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if h.replicaDecisionBlocked(a) {
		return fmt.Errorf("authority changed during replica ADD proof")
	}
	return h.replicaDRBlocked(ctx)
}

func (h *Host) proveReplicaRosterExcept(ctx context.Context, expected node.ManagedRepairSnapshot, excluded quepaxa.NodeID, allowedClosed *operator.V1Alpha2RepairPlan) error {
	before, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !sameReplicaSnapshot(before, expected) {
		return fmt.Errorf("certified roster changed before replica Pod proof")
	}
	pods, err := h.observePods(ctx)
	if err != nil {
		return err
	}
	if excluded == "" {
		if err := replicaFullPods(expected.Current, pods); err != nil {
			return err
		}
		if err := h.node.ProbeManagedReplicaVoters(ctx, expected.Current, expected.AbortSlot); err != nil {
			return err
		}
	} else {
		if _, survivors, err := replicaDownRemoval(expected.Current, pods); err != nil || len(survivors) == 0 {
			return errors.Join(fmt.Errorf("replica Pod set differs from the proposed removal"), err)
		} else if err := h.node.ProbeManagedReplicaVotersExcept(ctx, expected.Current, expected.AbortSlot, excluded); err != nil {
			return err
		}
	}
	after, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !sameReplicaSnapshot(after, expected) {
		return fmt.Errorf("certified roster changed during replica Pod proof")
	}
	finalPods, err := h.observePods(ctx)
	if err != nil {
		return err
	}
	if excluded == "" {
		if err := replicaFullPods(expected.Current, finalPods); err != nil {
			return fmt.Errorf("replica Pod identity changed during health proof: %w", err)
		}
	} else {
		finalMissing, _, err := replicaDownRemoval(expected.Current, finalPods)
		if err != nil || finalMissing.ID != excluded {
			return errors.Join(fmt.Errorf("actual removed voter changed during survivor health proof"), err)
		}
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if replicaDecisionBlockedWith(a, allowedClosed) {
		return fmt.Errorf("authority changed during replica Pod proof")
	}
	return h.replicaDRBlocked(ctx)
}

func sameReplicaSnapshot(a, b node.ManagedRepairSnapshot) bool {
	return a.AbortSlot == b.AbortSlot && a.PendingOK == b.PendingOK && slices.Equal(a.Current.Members, b.Current.Members) && a.Current.ConfigID == b.Current.ConfigID
}

func (h *Host) observePods(ctx context.Context) ([]operator.V1Alpha2ManagedPodObservation, error) {
	_, portText, err := net.SplitHostPort(h.cfg.Runtime.PeerAddr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, err
	}
	return operator.ObserveV1Alpha2ManagedPods(ctx, h.cfg.Store, h.cfg.Ref, port)
}

func (h *Host) observeReplicaDeployment(ctx context.Context) (operator.V1Alpha2ReplicaObservation, error) {
	return operator.ObserveV1Alpha2ReplicaDeployment(ctx, h.cfg.Store, h.cfg.Ref, h.authority)
}

func (h *Host) replicaDecisionBlocked(a operator.V1Alpha2AdmissionSnapshot) bool {
	return replicaDecisionBlockedWith(a, nil)
}

func replicaDecisionBlockedWith(a operator.V1Alpha2AdmissionSnapshot, allowedClosed *operator.V1Alpha2RepairPlan) bool {
	if _, pending := a.Pending(); pending {
		return true
	}
	if transition := bytes.TrimSpace(a.Transition()); len(transition) > 0 && !bytes.Equal(transition, []byte("null")) {
		return true
	}
	record, ok := a.Repair()
	if !ok || record.State != "Closed" {
		return false
	}
	return allowedClosed == nil || !reflect.DeepEqual(record.Plan, *allowedClosed)
}

func (h *Host) replicaDRBlocked(ctx context.Context) error {
	status, err := h.cfg.Store.Load(ctx, h.cfg.Ref)
	if err != nil {
		return err
	}
	queue := bytes.TrimSpace(status.Queue())
	if len(queue) > 0 && !bytes.Equal(queue, []byte("null")) && !bytes.Equal(queue, []byte("{}")) {
		return fmt.Errorf("recovery queue work blocks external replica scale")
	}
	return nil
}

func validateReplicaLeaveProof(a operator.V1Alpha2AdmissionSnapshot, request LeaveVoterRequest, proof replicaLeaveProof) error {
	step, present, err := a.ReplicaStep()
	if err != nil || !present || !sameReplicaStep(step, proof.Step) || step.To != step.From-1 || step.To < 3 {
		return errors.Join(fmt.Errorf("external leave requires the exact active down step"), err)
	}
	if proof.Cluster.ConfigID != uint(step.ConfigID) || len(proof.Cluster.Members) != int(step.From) {
		return fmt.Errorf("external leave proof differs from the step source roster")
	}
	found := false
	var certifiedSurvivors []quepaxa.NodeID
	for _, member := range proof.Cluster.Members {
		if member.ID == proof.Missing {
			found = true
			continue
		}
		certifiedSurvivors = append(certifiedSurvivors, member.ID)
	}
	survivors := slices.Clone(proof.Survivors)
	slices.Sort(certifiedSurvivors)
	slices.Sort(survivors)
	if !found || request.NodeID != proof.Missing || len(survivors) != int(step.To) || !slices.Equal(survivors, certifiedSurvivors) {
		return fmt.Errorf("external leave does not name the actual removed voter")
	}
	if proof.Repair != nil {
		record, ok := a.Repair()
		if !ok || record.State != "Closed" || !reflect.DeepEqual(record.Plan, proof.Repair.Plan) || record.Plan.Sequence != operator.V1Alpha2RepairLeave || quepaxa.NodeID(record.Plan.OldNodeID) != proof.Missing {
			return fmt.Errorf("external leave resume differs from the exact closed repair")
		}
	}
	return nil
}

func validateReplicaRepairEntry(a operator.V1Alpha2AdmissionSnapshot, sequence operator.V1Alpha2RepairSequence) error {
	step, present, err := a.ReplicaStep()
	if err != nil || !present {
		return err
	}
	if step.To < step.From {
		return fmt.Errorf("ordinary replacement is fenced during an external down step")
	}
	if sequence != operator.V1Alpha2RepairAdd {
		return fmt.Errorf("external up step permits ADD only")
	}
	return nil
}

func rejectReplicaDownStep(a operator.V1Alpha2AdmissionSnapshot) error {
	step, present, err := a.ReplicaStep()
	if err != nil {
		return err
	}
	if present && step.To < step.From {
		return fmt.Errorf("replica replacement is fenced during an external down step")
	}
	return nil
}

func replicaStepPlanMatches(step operator.V1Alpha2ReplicaStep, plan operator.V1Alpha2RepairPlan) bool {
	if plan.Generation != step.Generation || plan.DeploymentUID != step.DeploymentUID || plan.ExpectedConfigID != step.ConfigID || len(plan.FinalVoterIDs) != int(step.To) {
		return false
	}
	return step.To < step.From && plan.Sequence == operator.V1Alpha2RepairLeave || step.To > step.From && plan.Sequence == operator.V1Alpha2RepairAdd
}

func sameReplicaStep(a, b operator.V1Alpha2ReplicaStep) bool {
	return sameReplicaStepIntent(a, b) && a.DeploymentResourceVersion == b.DeploymentResourceVersion && a.CheckID == b.CheckID && a.ExpiresAt.Equal(b.ExpiresAt)
}

func sameReplicaStepIntent(a, b operator.V1Alpha2ReplicaStep) bool {
	return a.Generation == b.Generation && a.DeploymentUID == b.DeploymentUID && a.ConfigID == b.ConfigID && a.From == b.From && a.To == b.To
}

func sameReplicaCheck(a, b operator.V1Alpha2ReplicaCheck) bool {
	return a.ID == b.ID && a.DeploymentResourceVersion == b.DeploymentResourceVersion && a.ExpiresAt.Equal(b.ExpiresAt)
}

func validateReplicaChallenge(check operator.V1Alpha2ReplicaCheck, observation operator.V1Alpha2ReplicaObservation, current int32, now time.Time) error {
	if !check.ExpiresAt.After(now) || check.DeploymentResourceVersion != observation.DeploymentResourceVersion || !replicaDeploymentStable(observation, current) {
		return fmt.Errorf("downscale challenge does not match the current stable Deployment revision")
	}
	return nil
}

func replicaProofDeadline(check operator.V1Alpha2ReplicaCheck, now time.Time) time.Time {
	deadline := now.Add(replicaStepTTL)
	if check.ExpiresAt.Before(deadline) {
		deadline = check.ExpiresAt
	}
	return deadline
}

func replicaFullPods(cluster quepaxa.Cluster, pods []operator.V1Alpha2ManagedPodObservation) error {
	if len(pods) != len(cluster.Members) {
		return fmt.Errorf("live Pod count %d differs from certified voters %d", len(pods), len(cluster.Members))
	}
	byID := make(map[quepaxa.NodeID]operator.V1Alpha2ManagedPodObservation, len(pods))
	for _, pod := range pods {
		if pod.Candidate || pod.Member.NodeID == "" {
			return fmt.Errorf("current voter Pod is not running")
		}
		if _, duplicate := byID[pod.Member.NodeID]; duplicate {
			return fmt.Errorf("duplicate live Pod voter identity")
		}
		byID[pod.Member.NodeID] = pod
	}
	for _, member := range cluster.Members {
		pod, ok := byID[member.ID]
		if !ok || !replicaMemberPodEqual(member, pod.Member) {
			return fmt.Errorf("live Pod identity differs from certified voter %q", member.ID)
		}
	}
	return nil
}

func replicaUpReplacement(cluster quepaxa.Cluster, pods []operator.V1Alpha2ManagedPodObservation) (operator.V1Alpha2RepairPodRequest, error) {
	if len(pods) != len(cluster.Members)+1 {
		return operator.V1Alpha2RepairPodRequest{}, fmt.Errorf("up step requires exactly one new live Pod")
	}
	members := cluster.MemberSet()
	var extra *operator.V1Alpha2ManagedPodObservation
	seen := make(map[quepaxa.NodeID]bool, len(cluster.Members))
	for i := range pods {
		pod := &pods[i]
		if member, current := members[pod.Member.NodeID]; current {
			if pod.Candidate || !replicaMemberPodEqual(member, pod.Member) || seen[pod.Member.NodeID] {
				return operator.V1Alpha2RepairPodRequest{}, fmt.Errorf("current voter Pod identity changed during up step")
			}
			seen[pod.Member.NodeID] = true
			continue
		}
		if extra != nil || pod.Member.NodeID == "" {
			return operator.V1Alpha2RepairPodRequest{}, fmt.Errorf("up step has ambiguous extra Pod identities")
		}
		extra = pod
	}
	if extra == nil || len(seen) != len(cluster.Members) {
		return operator.V1Alpha2RepairPodRequest{}, fmt.Errorf("up step is missing a current voter or new Pod")
	}
	return extra.Member, nil
}

func replicaDownRemoval(cluster quepaxa.Cluster, pods []operator.V1Alpha2ManagedPodObservation) (quepaxa.Member, []operator.V1Alpha2ManagedPodObservation, error) {
	if len(pods) != len(cluster.Members)-1 {
		return quepaxa.Member{}, nil, fmt.Errorf("down step requires exactly one removed live Pod")
	}
	members := cluster.MemberSet()
	present := make(map[quepaxa.NodeID]bool, len(pods))
	for _, pod := range pods {
		member, current := members[pod.Member.NodeID]
		if !current || pod.Candidate || !replicaMemberPodEqual(member, pod.Member) || present[pod.Member.NodeID] {
			return quepaxa.Member{}, nil, fmt.Errorf("down step survivor differs from certified identity")
		}
		present[pod.Member.NodeID] = true
	}
	var missing quepaxa.Member
	count := 0
	for _, member := range cluster.Members {
		if !present[member.ID] {
			missing = member
			count++
		}
	}
	if count != 1 {
		return quepaxa.Member{}, nil, fmt.Errorf("down step does not have exactly one actual removed voter")
	}
	return missing, pods, nil
}

func replicaMemberPodEqual(member quepaxa.Member, pod operator.V1Alpha2RepairPodRequest) bool {
	return member.ID == pod.NodeID && member.PublicKey == pod.PublicKey && member.PeerURL == pod.PeerURL
}

func replicaDeploymentStable(observation operator.V1Alpha2ReplicaObservation, replicas int32) bool {
	return observation.DesiredReplicas == replicas && observation.ObservedReplicas == replicas && observation.UpdatedReplicas == replicas && observation.ReadyReplicas == replicas && observation.ObservedDeploymentGeneration == observation.DeploymentGeneration
}
