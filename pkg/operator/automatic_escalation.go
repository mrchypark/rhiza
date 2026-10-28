package operator

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Losing the surviving quorum during an online incident never reuses the
// single-voter proof for DR. Reserve a new generation-wide fence first and
// revoke the old child's execution authority in the same CAS.
func (c *Controller) maybeEscalateAutomatic(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object) (bool, error) {
	disposition, err := c.assessAutomaticEscalation(ctx, r, state, version, sts, nil)
	return disposition != automaticEscalationContinue, err
}

type automaticEscalationDisposition uint8

const (
	automaticEscalationContinue automaticEscalationDisposition = iota
	automaticEscalationWait
	automaticEscalationStop
)

func (c *Controller) assessAutomaticEscalation(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object, child *Resource) (automaticEscalationDisposition, error) {
	intent := state.Intent
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return automaticEscalationStop, err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return automaticEscalationStop, err
	}
	fencePods := map[quepaxa.NodeID]string{}
	for _, ref := range state.Voters {
		fencePods[ref.NodeID] = ref.Pod
	}
	voters := append([]VoterPod(nil), intent.Voters...)
	learnerID := intent.LearnerName
	if intent.LearnerUID != "" {
		learnerID, err = learnerNodeID(state, intent.LearnerName, intent.LearnerUID)
		if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1" {
			learnerID, err = capturedLearnerNodeID(state, intent.LearnerName, intent.LearnerUID)
		}
		if err != nil {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "learner incarnation identity is unavailable")
		}
	}
	for _, ref := range voters {
		if fencePods[ref.NodeID] != ref.Pod {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "intent voter is outside the generation fencing scope")
		}
	}
	// A prospective learner is not a voter probe target until both its exact
	// Pod UID and WAL identity have been captured. Its history still remains in
	// the later generation-fence inventory through includeLearnerHistory.
	if intent.LearnerName != "" && intent.LearnerUID != "" && intent.LearnerWAL != "" {
		voters = append(voters, VoterPod{NodeID: quepaxa.NodeID(learnerID), Pod: intent.LearnerName})
		fencePods[quepaxa.NodeID(learnerID)] = intent.LearnerName
	}
	allowedMemberships := automaticIntentMemberships(intent, learnerID)
	var quorum bool
	probes := 0
	for _, ref := range voters {
		if ref.NodeID == intent.Failed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return automaticEscalationWait, err
		}
		expected, expectedOK := state.Identities[ref.NodeID]
		if ref.NodeID == quepaxa.NodeID(learnerID) {
			expected = FenceTarget{NodeID: learnerID, Pod: ref.Pod, PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}
			expectedOK = intent.LearnerUID != "" && intent.LearnerWAL != ""
		}
		if !expectedOK || expected.NodeID != string(ref.NodeID) || expected.Pod != ref.Pod || expected.PodUID == "" || expected.WALIdentity == "" {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter identity is missing from the generation fencing scope")
		}
		probes++
		var pod object
		err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod)
		if err != nil {
			if apiNotFound(err) {
				// A captured but currently absent Pod cannot provide a quorum
				// observation. Keep its durable target in the eventual closure
				// inventory; absence is no response, never termination proof.
				continue
			}
			return automaticEscalationStop, err
		}
		if str(nested(pod, "metadata", "uid")) != expected.PodUID || str(nested(pod, "status", "podIP")) == "" {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter Pod differs from the generation fencing scope")
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/probe")
		if err != nil {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter probe endpoint is invalid")
		}
		status, result, err := c.automaticQuorumProbe(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"], state.ActiveClusterID, string(ref.NodeID), string(intent.Failed))
		if err != nil {
			if ctx.Err() != nil {
				return automaticEscalationWait, ctx.Err()
			}
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter recovery probe proof is invalid")
		}
		var current object
		if err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &current); err != nil {
			if apiNotFound(err) {
				// The signed response cannot be tied to a Pod still present at
				// verification time, so discard it as no response. Identity
				// changes for a present Pod remain a hard block below.
				continue
			}
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter Pod disappeared during recovery probe")
		}
		if err := validateProbePodIncarnation(pod, current, expected); err != nil {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter Pod changed during recovery probe")
		}
		if result == quorumProbeNoResponse {
			continue
		}
		if status.NodeID != string(ref.NodeID) || status.ClusterID != state.ActiveClusterID || status.ConfigID == 0 || status.WALIdentity == "" {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter recovery probe identity differs from the generation fencing scope")
		}
		if status.WALIdentity != expected.WALIdentity {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter WAL identity differs from the generation fencing scope")
		}
		if ref.NodeID != quepaxa.NodeID(learnerID) && !status.Voting {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter recovery probe is outside the generation fencing scope")
		}
		if string(ref.NodeID) == learnerID && status.WALIdentity != intent.LearnerWAL && intent.LearnerWAL != "" {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "learner WAL differs from the generation fencing scope")
		}
		if !uniqueIDs(nodeIDs(status.Voters)) {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "observed membership is invalid")
		}
		for _, voter := range status.Voters {
			if _, known := fencePods[quepaxa.NodeID(voter)]; !known {
				return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "observed membership is outside the generation fencing scope")
			}
		}
		statusVoters := nodeIDs(status.Voters)
		if !automaticMembershipAllowed(statusVoters, allowedMemberships) {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "observed membership differs from the active intent")
		}
		switch result {
		case quorumProbeAvailable:
			quorum = true
		case quorumProbeUnavailable, quorumProbeUnknown:
			// A completed versioned observation without a fresh positive is
			// policy input only; it never supplies death or fencing proof.
		default:
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "voter recovery probe state is invalid")
		}
	}
	if quorum {
		if intent.NoQuorumSince.IsZero() {
			return automaticEscalationContinue, nil
		}
		intent.NoQuorumSince = time.Time{}
		return automaticEscalationStop, c.saveAuto(ctx, r, state, version)
	}
	if err := ctx.Err(); err != nil {
		return automaticEscalationWait, err
	}
	if probes == 0 {
		return automaticEscalationWait, c.autoStatus(ctx, r, state, "Blocked", "no surviving voter probe was attempted")
	}
	// A frozen Add can itself disable ReadIndex. Give its exact retirement round
	// one finite incident-wide window, without treating snapshots as live quorum.
	if intent.Aborting && !intent.RetirementSince.IsZero() && time.Since(intent.RetirementSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return automaticEscalationWait, nil
	}
	if intent.NoQuorumSince.IsZero() {
		intent.NoQuorumSince = time.Now().UTC()
		return automaticEscalationStop, c.saveAuto(ctx, r, state, version)
	}
	if time.Since(intent.NoQuorumSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return automaticEscalationWait, c.autoStatus(ctx, r, state, "Recovering", "surviving quorum unavailable; waiting before generation-wide fencing")
	}
	// The learner handler runs before these potentially slow voter probes. If
	// its exact captured missing-incarnation grace expires while this assessment
	// is in flight, revalidate that same learner and reserve its abort before a
	// generation intent can supersede the original voter repair.
	if child != nil && !intent.Aborting && intent.LearnerUID != "" && !intent.LearnerMissingSince.IsZero() && time.Since(intent.LearnerMissingSince) >= time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		if intent.RecoveryUID == "" || str(child.Metadata["uid"]) != intent.RecoveryUID || str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "recovery child identity changed before learner retirement recheck")
		}
		before := hashJSON(state)
		handled, err := c.handleAutomaticLearner(ctx, r, state, version, sts, child)
		if err != nil || handled || hashJSON(state) != before {
			return automaticEscalationStop, err
		}
	}
	if intent.Durability == "async" && !intent.Policy.AllowDataLoss {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "async generation escalation requires allowDataLoss")
	}
	archive := recovery.NewManager(c.Bucket, path.Join(c.Prefix, state.ActiveClusterID), 1)
	err = archive.Load(ctx)
	tip := archive.Tip()
	archive.Close()
	if err != nil || tip == 0 {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "certified archive unavailable for escalation")
	}
	state.Sequence++
	if err := validateLearnerHistory(state.Learners); err != nil {
		return automaticEscalationStop, err
	}
	name := "auto-" + shortID(state.BindingUID+":"+fmt.Sprint(state.Sequence))
	request := intent.Request
	request.OperationID = name
	request.Scope = ScopeGeneration
	request.Targets = nil
	for _, ref := range state.Voters {
		target := state.Identities[ref.NodeID]
		target.NodeID = string(ref.NodeID)
		target.Pod = ref.Pod
		request.Targets = append(request.Targets, target)
	}
	if intent.LearnerUID != "" {
		target, err := learnerFenceTarget(state, intent.LearnerName, intent.LearnerUID, intent.LearnerWAL)
		if err != nil {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "learner incarnation identity is unavailable")
		}
		request.Targets = append(request.Targets, target)
	}
	if err := includeLearnerHistory(state, &request); err != nil {
		return automaticEscalationStop, err
	}
	// Reserve nothing the fencing backend cannot execute during the outage.
	if err := fenceCapacityError(len(request.Targets)); err != nil {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", err.Error())
	}
	superseded := intent.RecoveryName + "-" + hashJSON(intent)
	if err := c.immutable(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "superseded", superseded+".json"), jsonBytes(intent)); err != nil {
		return automaticEscalationStop, err
	}
	state.Intent = &autoIntent{Supersedes: superseded, Request: request, Policy: intent.Policy, Durability: intent.Durability, Voters: intent.Voters, RecoveryName: name}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return automaticEscalationStop, err
	}
	return automaticEscalationStop, c.autoStatus(ctx, r, state, "Fencing", "online quorum lost; generation-wide fencing reserved")
}

func automaticIntentMemberships(intent *autoIntent, learnerID string) [][]quepaxa.NodeID {
	base := voterPodIDs(intent.Voters)
	allowed := [][]quepaxa.NodeID{base}
	if intent.Failed != "" {
		withoutFailed := make([]quepaxa.NodeID, 0, len(base))
		for _, id := range base {
			if id != intent.Failed {
				withoutFailed = append(withoutFailed, id)
			}
		}
		allowed = append(allowed, withoutFailed)
		if intent.LearnerName != "" && intent.LearnerUID != "" && intent.LearnerWAL != "" {
			learner := quepaxa.NodeID(learnerID)
			withLearner := append(append([]quepaxa.NodeID(nil), base...), learner)
			allowed = append(allowed, withLearner)
			withoutFailed = append(withoutFailed, learner)
			allowed = append(allowed, withoutFailed)
		}
	} else if intent.LearnerName != "" && intent.LearnerUID != "" && intent.LearnerWAL != "" {
		allowed = append(allowed, append(append([]quepaxa.NodeID(nil), base...), quepaxa.NodeID(learnerID)))
	}
	return allowed
}

func automaticMembershipAllowed(observed []quepaxa.NodeID, allowed [][]quepaxa.NodeID) bool {
	for _, candidate := range allowed {
		if sameIDs(observed, candidate) {
			return true
		}
	}
	return false
}
