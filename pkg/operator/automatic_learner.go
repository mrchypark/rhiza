package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/thanos-io/objstore"
)

func apiNotFound(err error) bool {
	var e *APIError
	return errors.As(err, &e) && e.StatusCode == http.StatusNotFound
}

// handleAutomaticLearner never recreates a journaled Pod under the same voter
// identity. A failed incarnation is fenced and its addition explicitly aborted
// before a new identity is provisioned; retries are bounded per incident.
func (c *Controller) handleAutomaticLearner(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object, child *Resource) (bool, error) {
	intent := state.Intent
	save := func(message string) (bool, error) {
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, err
		}
		return true, c.autoStatus(ctx, r, state, "Recovering", message)
	}
	block := func(message string) (bool, error) { return true, c.autoStatus(ctx, r, state, "Blocked", message) }
	remember := func(entry learnerIncarnation) (bool, error) {
		if err := rememberLearner(state, entry); err != nil {
			return block(err.Error())
		}
		return false, nil
	}
	if intent.Aborting {
		if intent.RecoveryUID == "" {
			return false, nil
		}
		if resourceName(child) != intent.RecoveryName || str(child.Metadata["uid"]) != intent.RecoveryUID ||
			str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID ||
			str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
			return block("recovery resource identity changed before learner retirement acknowledgment")
		}
		desired := automaticRecoveryChildSpec(r, state, intent)
		generation := number(child.Metadata["generation"])
		if !intent.Aborting || hashJSON(desired) != intent.SpecHash || hashJSON(child.Spec) != intent.SpecHash ||
			child.Spec.Membership == nil || !child.Spec.Membership.AbortAddition ||
			child.Spec.Membership.ReplacementPod != learnerPhysicalPod(intent) || generation <= 0 ||
			child.Status.ObservedGeneration != generation || child.Status.Phase != "Aborted" ||
			child.Status.Membership == nil || child.Status.Membership.Phase != "Aborted" {
			return false, nil
		}
		if child.Status.Membership.Add != nil {
			currentAdd, err := c.membershipAddMatchesIntent(ctx, child, state, child.Status.Membership.Add, intent)
			if err != nil {
				return block("aborted learner ADD is not bound to the current immutable credential and request")
			}
			if currentAdd {
				if intent.LearnerWAL == "" {
					if err := checkpointCurrentLearnerWAL(state, intent, child.Status.Membership.Add.WALIdentity); err != nil {
						return block(err.Error())
					}
					intent.LearnerWAL = child.Status.Membership.Add.WALIdentity
					return save("authenticated aborted learner WAL checkpointed before fencing")
				}
				if intent.LearnerWAL != child.Status.Membership.Add.WALIdentity {
					return block("aborted learner ADD WAL differs from the canonical identity")
				}
			} else {
				if _, err := c.historicalRetiringMembershipRequest(ctx, child, state); err != nil {
					return block("aborted learner carries an unverified historical ADD")
				}
			}
		}
		request := intent.Request
		request.Scope = ScopeVoter
		request.OperationID = fmt.Sprintf("%s-learner-fence-%d", intent.RecoveryName, intent.LearnerAttempt)
		request.Targets = []FenceTarget{learnerFenceTarget(state, intent.LearnerName, intent.LearnerUID, intent.LearnerWAL)}
		proof, err := c.fence(ctx, request)
		if err != nil {
			return block("waiting for failed learner fencing proof")
		}
		entry := learnerIncarnation{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, Target: request.Targets[0]}
		entry.Retirement = &learnerRetirement{Request: request, Proof: *proof}
		for _, old := range state.Learners {
			if old.SourceClusterID == entry.SourceClusterID && old.Target.NodeID == entry.Target.NodeID && old.Target.PodUID == entry.Target.PodUID {
				entry.CredentialName = old.CredentialName
				entry.CredentialUID = old.CredentialUID
				break
			}
		}
		if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1" && entry.CredentialUID == "" {
			return block("supervised learner retirement lacks durable creation history")
		}
		if handled, err := remember(entry); handled || err != nil {
			return handled, err
		}
		intent.Aborting = false
		intent.LearnerAttempt++
		intent.LearnerUID = ""
		intent.LearnerWAL = ""
		intent.LearnerPodName = ""
		intent.LearnerSecretName = ""
		intent.LearnerSecretUID = ""
		intent.LearnerSecretRV = ""
		intent.LearnerSecretHash = ""
		intent.LearnerMissingSince = time.Time{}
		intent.LearnerName = fmt.Sprintf("%s-learner-%d", intent.RecoveryName, intent.LearnerAttempt)
		return save("aborted learner replaced with a fresh incarnation")
	}
	if intent.LearnerAttempt >= 3 {
		return block("automatic learner retry limit reached; existing journal retained")
	}
	if intent.LearnerUID == "" {
		assigned, err := c.prepareSameOrdinalLearner(ctx, state, sts, r.Spec.Container)
		if err != nil {
			if errors.Is(err, errSameOrdinalLearnerCapacity) {
				changed, compactErr := c.compactLearnerHistory(ctx, r, state)
				if compactErr != nil {
					return block(compactErr.Error())
				}
				if changed {
					if err := c.saveAuto(ctx, r, state, version); err != nil {
						return true, err
					}
					return true, c.autoStatus(ctx, r, state, "Recovering", "learner history compacted before replacement provisioning")
				}
				return block("same-ordinal learner cannot reserve its fallback generation fence: " + err.Error())
			}
			return block("same-ordinal replacement identity conflicts with the fenced incarnation")
		}
		if assigned {
			var pod object
			if err := c.Kube.Get(ctx, c.corePath("pods", learnerPhysicalPod(intent)), &pod); err != nil {
				return true, err
			}
			entry, err := c.learnerInventoryEntry(ctx, state, sts, pod)
			if err != nil {
				return block(err.Error())
			}
			if err := rememberLearner(state, entry); err != nil {
				return block(err.Error())
			}
			return save("same-ordinal learner identity reserved before startup authorization")
		}
		if handled, err := c.prepareLearnerProvisioning(ctx, r, state, version); handled || err != nil {
			return handled, err
		}
		if _, err := c.ensureAutomaticLearner(ctx, sts, r.Spec.Container, intent.Request.OperationID, state.ActiveClusterID, intent.LearnerName, state.BindingUID); err != nil {
			return block("replacement provisioning is unavailable or conflicts with ownership")
		}
		var pod object
		if err := c.Kube.Get(ctx, c.corePath("pods", learnerPhysicalPod(intent)), &pod); err != nil {
			return true, err
		}
		intent.LearnerUID = str(nested(pod, "metadata", "uid"))
		if intent.LearnerUID == "" {
			return block("replacement Pod UID is unavailable")
		}
		entry, err := c.learnerInventoryEntry(ctx, state, sts, pod)
		if err != nil {
			return block(err.Error())
		}
		if handled, err := remember(entry); handled || err != nil {
			return handled, err
		}
		return save("replacement Pod identity persisted before membership dispatch")
	}
	var pod object
	err := c.Kube.Get(ctx, c.corePath("pods", learnerPhysicalPod(intent)), &pod)
	if err != nil && !apiNotFound(err) {
		return true, err
	}
	available := false
	if err == nil && str(nested(pod, "metadata", "uid")) == intent.LearnerUID && nested(pod, "metadata", "deletionTimestamp") == nil {
		ctr, e := container(sts, r.Spec.Container)
		if e != nil {
			return true, e
		}
		env, e := c.environment(ctx, ctr)
		if e != nil {
			return true, e
		}
		endpoint, e := podEndpoint(pod, r.Spec.Container, "/membership/status")
		if e == nil {
			status, e := c.membershipStatus(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"])
			if errors.Is(e, ErrMembershipUnauthorized) {
				return block("replacement authentication failed")
			}
			if e == nil && string(status.NodeID) == intent.LearnerName && status.ClusterID == state.ActiveClusterID && status.WALIdentity != "" {
				if intent.LearnerWAL != "" && intent.LearnerWAL != status.WALIdentity {
					return block("replacement WAL identity changed")
				}
				if intent.LearnerWAL == "" {
					intent.LearnerWAL = status.WALIdentity
					entry, err := c.learnerInventoryEntry(ctx, state, sts, pod)
					if err != nil {
						return block(err.Error())
					}
					if handled, err := remember(entry); handled || err != nil {
						return handled, err
					}
					return save("replacement WAL identity authenticated")
				}
				available = true
			}
		}
	}
	if available {
		if !intent.LearnerMissingSince.IsZero() {
			intent.LearnerMissingSince = time.Time{}
			return save("replacement communication restored")
		}
		return false, nil
	}
	if intent.LearnerMissingSince.IsZero() {
		intent.LearnerMissingSince = time.Now().UTC()
		return save("replacement unavailable; waiting through grace period")
	}
	if time.Since(intent.LearnerMissingSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return block("replacement remains within retry grace period")
	}
	// The child snapshot can precede an in-flight journal reservation. Revoke
	// its round before physical fencing, which must still precede identity rotation.
	intent.Aborting = true
	if intent.RetirementSince.IsZero() {
		intent.RetirementSince = time.Now().UTC()
	}
	return save("failed learner addition retirement reserved before fencing")
}

func checkpointCurrentLearnerWAL(state *autoControl, intent *autoIntent, wal string) error {
	if state == nil || intent == nil || wal == "" || intent.LearnerName == "" || intent.LearnerUID == "" {
		return fmt.Errorf("current learner WAL checkpoint is incomplete")
	}
	var current *learnerIncarnation
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.SourceClusterID != state.ActiveClusterID || entry.OperationID != intent.Request.OperationID ||
			entry.Target.NodeID != intent.LearnerName || entry.Target.PodUID != intent.LearnerUID {
			continue
		}
		if current != nil {
			return fmt.Errorf("current learner WAL checkpoint has ambiguous identity history")
		}
		current = entry
	}
	if current == nil {
		if intent.LearnerPodName == "" || intent.LearnerSecretUID == "" || intent.LearnerSecretRV == "" || intent.LearnerSecretHash == "" {
			return fmt.Errorf("current learner WAL checkpoint differs from retained identity history")
		}
		state.Learners = append(state.Learners, learnerIncarnation{
			SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
			CredentialName: learnerCredentialName(intent), CredentialUID: intent.LearnerSecretUID,
			Target: FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(intent), PodUID: intent.LearnerUID, WALIdentity: wal},
		})
		return nil
	}
	if current.Target.Pod != learnerPhysicalPod(intent) || current.CredentialName != learnerCredentialName(intent) || current.CredentialUID == "" ||
		(intent.LearnerSecretUID != "" && current.CredentialUID != intent.LearnerSecretUID) || current.Retirement != nil ||
		(current.Target.WALIdentity != "" && current.Target.WALIdentity != wal) {
		return fmt.Errorf("current learner WAL checkpoint differs from retained identity history")
	}
	current.Target.WALIdentity = wal
	return nil
}
