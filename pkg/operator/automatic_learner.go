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
		if child.Spec.Membership == nil || !child.Spec.Membership.AbortAddition || child.Spec.Membership.ReplacementPod != intent.LearnerName || hashJSON(child.Spec) != intent.SpecHash || child.Status.Membership == nil || child.Status.Membership.Phase != "Aborted" {
			return false, nil
		}
		request := intent.Request
		request.Scope = ScopeVoter
		request.OperationID = fmt.Sprintf("%s-learner-fence-%d", intent.RecoveryName, intent.LearnerAttempt)
		request.Targets = []FenceTarget{{NodeID: intent.LearnerName, Pod: intent.LearnerName, PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}}
		if _, err := c.fence(ctx, request); err != nil {
			return block("waiting for failed learner fencing proof")
		}
		entry := learnerIncarnation{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, Target: request.Targets[0]}
		for _, old := range state.Learners {
			if old.SourceClusterID == entry.SourceClusterID && old.Target.Pod == entry.Target.Pod {
				entry.CredentialUID = old.CredentialUID
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
		intent.LearnerMissingSince = time.Time{}
		intent.LearnerName = fmt.Sprintf("%s-learner-%d", intent.RecoveryName, intent.LearnerAttempt)
		return save("aborted learner replaced with a fresh incarnation")
	}
	if intent.LearnerAttempt >= 3 {
		return block("automatic learner retry limit reached; existing journal retained")
	}
	if intent.LearnerUID == "" {
		if _, err := c.ensureAutomaticLearner(ctx, sts, r.Spec.Container, intent.Request.OperationID, state.ActiveClusterID, intent.LearnerName, state.BindingUID); err != nil {
			return block("replacement provisioning is unavailable or conflicts with ownership")
		}
		var pod object
		if err := c.Kube.Get(ctx, c.corePath("pods", intent.LearnerName), &pod); err != nil {
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
	err := c.Kube.Get(ctx, c.corePath("pods", intent.LearnerName), &pod)
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
