package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// reconcileMembership deliberately has no StatefulSet write. A healthy set
// stays untouched; the replacement learner is provisioned out of band.
func (c *Controller) reconcileMembership(ctx context.Context, r *Resource) error {
	m := r.Spec.Membership
	blocked := func(message string) error { return c.recordMembership(ctx, r, "Blocked", message) }
	if r.Status.Membership != nil && r.Status.Membership.Phase == "Complete" {
		return nil
	}
	if !identifier.MatchString(resourceName(r)) || !identifier.MatchString(r.Spec.StatefulSet) || !identifier.MatchString(r.Spec.SourceClusterID) || m.OperationID == "" || !identifier.MatchString(m.OperationID) || !identifier.MatchString(string(m.Remove)) || !identifier.MatchString(m.ReplacementPod) || !identifier.MatchString(m.ReplacementSecret) || str(r.Metadata["uid"]) == "" || len(m.VoterPods) == 0 {
		return blocked("membership resource, StatefulSet, cluster, operation, voter pods, replacement pod, secret, and resource UID are required")
	}
	if m.RemovedPod != "" && !identifier.MatchString(m.RemovedPod) {
		return blocked("removed voter Pod name is invalid")
	}
	if m.Fence.NodeID != m.Remove || !m.Fence.Confirmed || strings.TrimSpace(m.Fence.Evidence) == "" || m.Fence.WALIdentity == "" || m.Fence.WorkloadUID == "" {
		return blocked("confirmed membership fence must bind the removed voter, WAL identity, workload UID, and evidence")
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	automaticState, err := c.validateAutomaticMembershipIncarnation(ctx, r, sts)
	if err != nil {
		return blocked("automatic membership child or StatefulSet identity differs from canonical intent")
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return blocked(err.Error())
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return blocked(err.Error())
	}
	if err = c.storeConfigMatches(env); err != nil {
		return blocked(err.Error())
	}
	if env["RHIZA_CLUSTER_ID"] != r.Spec.SourceClusterID || env["RHIZA_ADMIN_TOKEN"] == "" {
		return blocked("StatefulSet membership cluster or admin token is unavailable")
	}
	pods, err := c.membershipPods(ctx, m.VoterPods)
	if err != nil {
		return blocked(err.Error())
	}
	for _, ref := range m.VoterPods {
		if ref.Pod == m.ReplacementPod {
			return blocked("replacement learner must not be listed as an active voter")
		}
	}
	removedPod := m.RemovedPod
	if removedPod == "" {
		removedPod = string(m.Remove)
	}
	var failed object
	err = c.Kube.Get(ctx, c.corePath("pods", removedPod), &failed)
	var apiErr *APIError
	if err != nil && (!errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound) {
		return blocked("removed voter workload lookup failed")
	}
	if err == nil && str(nested(failed, "metadata", "uid")) != m.Fence.WorkloadUID {
		intent := (*autoIntent)(nil)
		if automaticState != nil {
			intent = automaticState.Intent
		}
		currentStatefulSetOwner := false
		refs := list(nested(failed, "metadata", "ownerReferences"))
		if len(refs) == 1 {
			owner := asObject(refs[0])
			currentStatefulSetOwner = owner["apiVersion"] == "apps/v1" && owner["kind"] == "StatefulSet" &&
				owner["name"] == r.Spec.StatefulSet && owner["uid"] == str(nested(sts, "metadata", "uid")) && owner["controller"] == true
		}
		retiringExactAutomaticAbort := m.AbortAddition && automaticState != nil && intent != nil && intent.Aborting &&
			intent.LearnerPodName != "" && intent.LearnerPodName == removedPod && removedPod == m.ReplacementPod && currentStatefulSetOwner
		historicalJournalReset := false
		if automaticState != nil && intent != nil && !intent.Aborting && !m.AbortAddition && r.Status.Membership != nil && r.Status.Membership.Add != nil {
			currentAdd, matchErr := c.membershipAddMatchesIntent(ctx, r, automaticState, r.Status.Membership.Add, intent)
			if matchErr == nil && !currentAdd {
				_, historyErr := c.historicalRetiringMembershipRequest(ctx, r, automaticState)
				historicalJournalReset = historyErr == nil
			}
		}
		failedUID := str(nested(failed, "metadata", "uid"))
		standaloneWithRecreatedOrdinal := automaticState != nil && intent != nil &&
			intent.LearnerPodName == "" && intent.LearnerName != "" && intent.LearnerUID != "" && intent.LearnerUID != failedUID && removedPod != m.ReplacementPod && currentStatefulSetOwner
		if intent == nil || (!retiringExactAutomaticAbort && !standaloneWithRecreatedOrdinal &&
			!historicalJournalReset && (intent.LearnerPodName == "" || intent.LearnerPodName != removedPod || removedPod != m.ReplacementPod || intent.LearnerUID != failedUID)) {
			return blocked("membership fence workload UID does not match the removed voter workload")
		}
		if !retiringExactAutomaticAbort && !standaloneWithRecreatedOrdinal && !historicalJournalReset {
			if err := c.validateMembershipReplacement(ctx, r, sts, failed); err != nil {
				return blocked("same-ordinal replacement differs from its canonical assignment")
			}
		}
	}
	admin := env["RHIZA_ADMIN_TOKEN"]
	statuses := c.membershipStatuses(ctx, pods, r.Spec.Container, admin)
	quorum, expected, err := membershipQuorum(statuses, r.Spec.SourceClusterID, m.Remove)
	if err != nil {
		return blocked(err.Error())
	}
	if !quorum {
		return blocked("no surviving membership quorum; online replacement will not dispatch whole-generation recovery")
	}
	if r.Status.Membership == nil {
		r.Status.Membership = &MembershipStatus{}
	}
	journal := r.Status.Membership
	if m.AbortAddition {
		if journal.Add == nil {
			return c.recordMembership(ctx, r, "Aborted", "retirement acknowledged before any addition was journaled")
		}
		if automaticState != nil && automaticState.Intent != nil && automaticState.Intent.Aborting {
			currentAdd, matchErr := c.membershipAddMatchesIntent(ctx, r, automaticState, journal.Add, automaticState.Intent)
			if matchErr != nil {
				return blocked("current learner addition differs from its immutable canonical binding: " + matchErr.Error())
			}
			if currentAdd {
				if _, err := c.retiringMembershipRequest(ctx, r); err != nil {
					return blocked(err.Error())
				}
				return c.abortMembership(ctx, r, pods, statuses, admin, expected)
			}
			if err := c.confirmHistoricalMembershipAbort(ctx, r, automaticState, pods, statuses, admin, expected); err != nil {
				return blocked(err.Error())
			}
			return c.recordMembership(ctx, r, "Aborted", "historical addition abort re-confirmed; current learner addition remains absent")
		}
		return c.abortMembership(ctx, r, pods, statuses, admin, expected)
	}
	if journal.Add != nil && automaticState != nil && automaticState.Intent != nil {
		currentAdd, matchErr := c.membershipAddMatchesIntent(ctx, r, automaticState, journal.Add, automaticState.Intent)
		if matchErr != nil {
			return blocked("current learner addition differs from its immutable canonical binding: " + matchErr.Error())
		}
		if !currentAdd {
			if err := c.confirmHistoricalMembershipAbort(ctx, r, automaticState, pods, statuses, admin, expected); err != nil {
				return blocked(err.Error())
			}
			journal.Add = nil
			journal.Phase = "Adding"
			return c.recordMembership(ctx, r, "Adding", "settled historical addition reset before replacement dispatch")
		}
	}
	if journal.Phase == "Aborted" {
		if journal.Add != nil && (expected.ConfigID != journal.Add.ExpectedConfigID || expected.AbortSlot <= journal.Add.ExpectedAbortSlot) {
			return c.recordMembership(ctx, r, "Aborted", "waiting for the journaled addition abort revision to reach surviving voters")
		}
		if journal.Add != nil && automaticState != nil {
			return blocked("aborted addition is not bound to a verifiable historical learner")
		}
		journal.Add, journal.Phase = nil, "Adding"
		return c.recordMembership(ctx, r, "Adding", "aborted addition reset before replacement dispatch")
	}
	if journal.RemoveRequest == nil {
		if expected.Pending {
			return blocked("surviving quorum has a pending membership transition")
		}
		if !containsID(expected.Voters, m.Remove) {
			return blocked("live membership does not contain the removed voter")
		}
		request := network.MembershipChange{OperationID: membershipOperationID(r, "remove"), ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: expected.ConfigID, ExpectedAbortSlot: expected.AbortSlot, Remove: m.Remove, Fence: &network.MembershipFence{NodeID: m.Fence.NodeID, WALIdentity: m.Fence.WALIdentity, WorkloadUID: m.Fence.WorkloadUID, Confirmed: m.Fence.Confirmed, Evidence: m.Fence.Evidence}}
		journal.RemoveRequest, journal.Phase = &request, "Removing"
		return c.recordMembership(ctx, r, "Removing", "remove request journaled before dispatch")
	}
	if journal.RemoveRequest.OperationID != membershipOperationID(r, "remove") || journal.RemoveRequest.ClusterID != r.Spec.SourceClusterID || journal.RemoveRequest.Remove != m.Remove || !sameFence(journal.RemoveRequest.Fence, m.Fence) {
		return blocked("persisted remove request differs from immutable membership specification")
	}
	if !containsID(expected.Voters, m.Remove) {
		if journal.Add != nil && expected.ConfigID == journal.Add.ExpectedConfigID+1 && expected.AbortSlot == 0 && !expected.Pending && containsID(expected.Voters, journal.Add.MemberID) {
			// The add may have committed after the removal was acknowledged. In
			// that case the node correctly rejects replay of the older removal
			// request; validate and durably replay the exact add instead.
			return c.reconcileMembershipAdd(ctx, r, sts, pods, statuses, admin, expected)
		}
		if expected.ConfigID != journal.RemoveRequest.ExpectedConfigID+1 {
			return blocked("live membership advanced beyond the journaled removal without matching the journaled addition")
		}
		return c.reconcileMembershipAdd(ctx, r, sts, pods, statuses, admin, expected)
	}
	if journal.RemoveRequest.ExpectedConfigID != expected.ConfigID || journal.RemoveRequest.ExpectedAbortSlot != expected.AbortSlot {
		return blocked("live membership configuration differs from the persisted remove request")
	}
	if err := c.changeMembership(ctx, survivorPod(pods, statuses, m.Remove, expected), r.Spec.Container, admin, *journal.RemoveRequest); err != nil {
		return c.recordMembership(ctx, r, "Removing", "remove request remains pending: "+err.Error())
	}
	return c.recordMembership(ctx, r, "Removing", "remove request accepted; waiting for configuration confirmation")
}

// validateAutomaticMembershipIncarnation binds every automatic membership
// reconcile to the frozen root intent and the live StatefulSet incarnation.
// Manual children have no automatic owner annotation and retain their existing
// validation path; automatic children are independently authorized before
// reaching reconcileMembership, but this check protects every resume/abort
// dispatch boundary against a same-name replacement workload.
func (c *Controller) validateAutomaticMembershipIncarnation(ctx context.Context, r *Resource, sts object) (*autoControl, error) {
	owner := str(nested(r.Metadata, "annotations", automaticOwner))
	if owner == "" {
		return nil, nil
	}
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	if !identifier.MatchString(logical) {
		return nil, fmt.Errorf("automatic membership logical identity is invalid")
	}
	state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return nil, err
	}
	intent := state.Intent
	if intent == nil {
		return nil, fmt.Errorf("automatic membership canonical intent is unavailable")
	}
	if intent.Failed == "" || intent.Proof == nil || state.BindingUID != owner || intent.Request.BindingUID != owner ||
		intent.Request.LogicalID != logical || intent.Request.Namespace != c.Kube.Namespace ||
		intent.Request.StatefulSet != r.Spec.StatefulSet || intent.Request.StatefulSetUID == "" ||
		intent.Request.StatefulSetUID != state.StatefulSetUID || intent.Request.SourceClusterID != state.ActiveClusterID ||
		intent.Request.SourceClusterID != r.Spec.SourceClusterID || intent.RecoveryName != resourceName(r) ||
		intent.RecoveryUID != str(r.Metadata["uid"]) || intent.SpecHash == "" || intent.SpecHash != hashJSON(r.Spec) ||
		intent.Failed != r.Spec.Membership.Remove || intent.Aborting != r.Spec.Membership.AbortAddition {
		return nil, fmt.Errorf("automatic membership child differs from frozen intent")
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil {
		return nil, fmt.Errorf("automatic membership fence proof is invalid")
	}
	if intent.Request.Scope != ScopeVoter || len(intent.Request.Targets) != 1 {
		return nil, fmt.Errorf("automatic membership fence scope is invalid")
	}
	target := intent.Request.Targets[0]
	if target.NodeID != string(intent.Failed) ||
		r.Spec.Membership.OperationID != intent.RecoveryName || r.Spec.Membership.RemovedPod != target.Pod ||
		r.Spec.Membership.ReplacementPod != learnerPhysicalPod(intent) ||
		r.Spec.Membership.ReplacementSecret != learnerCredentialName(intent) ||
		string(r.Spec.Membership.Fence.NodeID) != target.NodeID || r.Spec.Membership.Fence.WorkloadUID != target.PodUID ||
		r.Spec.Membership.Fence.WALIdentity != target.WALIdentity || !r.Spec.Membership.Fence.Confirmed ||
		r.Spec.Membership.Fence.Evidence != intent.Proof.ProofID {
		return nil, fmt.Errorf("automatic membership fence or child fields differ from frozen intent")
	}
	wantVoters := make([]VoterPod, 0, len(intent.Voters))
	for _, voter := range intent.Voters {
		if voter.NodeID != intent.Failed {
			wantVoters = append(wantVoters, voter)
		}
	}
	wantSpec := automaticRecoveryChildSpec(&ClusterResource{Spec: ClusterSpec{
		StatefulSet: r.Spec.StatefulSet, Container: r.Spec.Container, AnchorID: r.Spec.AnchorID,
	}}, state, intent)
	if hashJSON(wantVoters) != hashJSON(r.Spec.Membership.VoterPods) || hashJSON(wantSpec) != hashJSON(r.Spec) {
		return nil, fmt.Errorf("automatic membership spec differs from production builder")
	}
	if state.StatefulSetUID == "" || str(nested(sts, "metadata", "uid")) != state.StatefulSetUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return nil, fmt.Errorf("live StatefulSet identity differs from canonical binding")
	}
	return state, nil
}

func (c *Controller) reconcileMembershipAdd(ctx context.Context, r *Resource, sts object, pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, admin string, current network.MembershipStatus) error {
	m, journal := r.Spec.Membership, r.Status.Membership
	var replacement object
	if err := c.Kube.Get(ctx, c.corePath("pods", m.ReplacementPod), &replacement); err != nil {
		return c.recordMembership(ctx, r, "Blocked", "replacement learner workload is unavailable")
	}
	var credentialSnapshot object
	if err := c.Kube.Get(ctx, c.corePath("secrets", m.ReplacementSecret), &credentialSnapshot); err != nil {
		return c.recordMembership(ctx, r, "Blocked", "replacement learner credentials are unavailable")
	}
	if err := c.validateMembershipReplacement(ctx, r, sts, replacement, credentialSnapshot); err != nil {
		return c.recordMembership(ctx, r, "Blocked", err.Error())
	}
	endpoint, err := podEndpoint(replacement, r.Spec.Container, "/membership/status")
	if err != nil {
		return c.recordMembership(ctx, r, "Blocked", err.Error())
	}
	member, secretUID, secretResourceVer, err := replacementMemberFromSecret(credentialSnapshot)
	if err != nil || member.ID == "" || member.ID == m.Remove {
		return c.recordMembership(ctx, r, "Blocked", "replacement credentials do not identify a fresh learner")
	}
	learner, err := c.membershipStatus(ctx, endpoint, admin)
	if err != nil || learner.NodeID != member.ID || learner.ClusterID != r.Spec.SourceClusterID || learner.WALIdentity == "" {
		return c.recordMembership(ctx, r, "Blocked", "replacement learner identity does not match the named workload")
	}
	member.WALIdentity = learner.WALIdentity
	expectedConfigID := current.ConfigID
	expectedAbortSlot := current.AbortSlot
	if journal.Add != nil {
		expectedConfigID = journal.Add.ExpectedConfigID
		expectedAbortSlot = journal.Add.ExpectedAbortSlot
	}
	request := network.MembershipChange{OperationID: membershipOperationID(r, "add"), ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: expectedConfigID, ExpectedAbortSlot: expectedAbortSlot, Add: &member}
	// Core clears the abort slot when a membership configuration commits. The
	// journal's abort slot identifies the add round to dispatch, but cannot be
	// required on the resulting configuration.
	if journal.Add != nil && journal.Add.OperationID == request.OperationID && journal.Add.MemberID == member.ID && journal.Add.WALIdentity == member.WALIdentity && journal.Add.RequestHash == hashJSON(request) && journal.Add.SecretUID == secretUID && journal.Add.SecretResourceVer == secretResourceVer && current.AbortSlot == 0 && current.ConfigID == journal.Add.ExpectedConfigID+1 && containsID(current.Voters, journal.Add.MemberID) && learner.Voting && !learner.Pending && learner.WALIdentity == journal.Add.WALIdentity {
		// Membership status proves that the configuration committed, but not
		// that the node's post-commit archive SyncThrough completed. Replaying
		// the exact journaled request is idempotent on the committed node and
		// repeats persistMembership before we publish completion.
		if err := c.changeMembership(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, request); err != nil {
			return c.recordMembership(ctx, r, "Adding", "committed add durability confirmation remains pending: "+err.Error())
		}
		journal.Phase = "Complete"
		return c.recordMembership(ctx, r, "Complete", "replacement learner promoted under surviving quorum")
	}
	if journal.Add == nil {
		if current.Pending || learner.Voting || learner.Pending || containsID(current.Voters, member.ID) {
			return c.recordMembership(ctx, r, "Blocked", "replacement learner or surviving quorum has a pending membership transition")
		}
		// Do not make a membership request until the replacement's complete
		// workload and credential bindings have been validated. The remove may
		// already be visible in status while its archive acknowledgement is still
		// pending, so replay it now, immediately before first add journaling.
		if journal.RemoveRequest == nil {
			return c.recordMembership(ctx, r, "Blocked", "journaled remove request is unavailable before replacement dispatch")
		}
		if err := c.ensureMembershipAddDispatchCurrent(ctx, r); err != nil {
			return err
		}
		if err := c.changeMembership(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, *journal.RemoveRequest); err != nil {
			return c.recordMembership(ctx, r, "Removing", "committed removal durability confirmation remains pending: "+err.Error())
		}
		journal.Add = &MembershipAddJournal{OperationID: request.OperationID, ExpectedConfigID: request.ExpectedConfigID, ExpectedAbortSlot: request.ExpectedAbortSlot, MemberID: member.ID, WALIdentity: member.WALIdentity, RequestHash: hashJSON(request), SecretUID: secretUID, SecretResourceVer: secretResourceVer}
		journal.Phase = "Adding"
		return c.recordMembership(ctx, r, "Adding", "add request journaled before dispatch")
	}
	if journal.Add.OperationID != request.OperationID || journal.Add.MemberID != member.ID || journal.Add.WALIdentity != member.WALIdentity || journal.Add.RequestHash != hashJSON(request) || journal.Add.SecretUID != secretUID || journal.Add.SecretResourceVer != secretResourceVer {
		return c.recordMembership(ctx, r, "Blocked", "persisted add request differs from the immutable replacement credentials")
	}
	if current.ConfigID != journal.Add.ExpectedConfigID || current.AbortSlot != journal.Add.ExpectedAbortSlot {
		return c.recordMembership(ctx, r, "Blocked", "live membership configuration differs from the persisted add request")
	}
	if err := c.changeMembership(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, request); err != nil {
		return c.recordMembership(ctx, r, "Adding", "add request remains pending: "+err.Error())
	}
	return c.recordMembership(ctx, r, "Adding", "add request accepted; waiting for configuration confirmation")
}

// ensureMembershipAddDispatchCurrent prevents a delayed reconciliation from
// acknowledging the removal after another reconciliation has retired the add.
func (c *Controller) ensureMembershipAddDispatchCurrent(ctx context.Context, r *Resource) error {
	var current Resource
	if err := c.Kube.Get(ctx, c.resourcePath(resourceName(r)), &current); err != nil {
		return err
	}
	if str(current.Metadata["uid"]) != str(r.Metadata["uid"]) ||
		str(current.Metadata["resourceVersion"]) != str(r.Metadata["resourceVersion"]) ||
		current.Spec.Membership == nil || current.Spec.Membership.AbortAddition ||
		hashJSON(current.Spec.Membership) != hashJSON(r.Spec.Membership) ||
		hashJSON(current.Status.Membership) != hashJSON(r.Status.Membership) {
		return &APIError{StatusCode: http.StatusConflict}
	}
	return nil
}

func (c *Controller) abortMembership(ctx context.Context, r *Resource, pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, admin string, current network.MembershipStatus) error {
	add := r.Status.Membership.Add
	if current.ConfigID == add.ExpectedConfigID+1 {
		if err := c.confirmRetiredMembershipCommit(ctx, r, statuses, current); err != nil {
			return c.recordMembership(ctx, r, "Blocked", err.Error())
		}
		request, err := c.retiringMembershipRequest(ctx, r)
		if err != nil {
			return c.recordMembership(ctx, r, "Blocked", err.Error())
		}
		if err := c.changeMembership(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, request); err != nil {
			return c.recordMembership(ctx, r, "Aborting", "committed addition durability confirmation remains pending: "+err.Error())
		}
		return c.recordMembership(ctx, r, "Complete", "journaled addition already committed; failed voter identity retained for the next recovery")
	}
	if current.ConfigID != add.ExpectedConfigID || current.AbortSlot < add.ExpectedAbortSlot {
		return c.recordMembership(ctx, r, "Blocked", "live membership does not match the journaled addition for abort")
	}
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot}
	if err := c.membershipMutation(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, "/membership/abort", request); err != nil {
		// A journal can survive a crash before dispatch. Replaying its exact
		// request establishes the round that the next abort must resolve.
		if !current.Pending && current.AbortSlot == add.ExpectedAbortSlot {
			frozen, bindErr := c.retiringMembershipRequest(ctx, r)
			if bindErr != nil {
				return c.recordMembership(ctx, r, "Blocked", bindErr.Error())
			}
			if dispatchErr := c.changeMembership(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, frozen); dispatchErr != nil {
				return c.recordMembership(ctx, r, "Aborting", "retiring addition remains unresolved: "+dispatchErr.Error())
			}
			return c.recordMembership(ctx, r, "Aborting", "journaled addition replayed; waiting to resolve commit or abort")
		}
		return c.recordMembership(ctx, r, "Aborting", "abort request remains pending: "+err.Error())
	}
	return c.recordMembership(ctx, r, "Aborted", "journaled addition aborted by surviving quorum")
}

func (c *Controller) membershipAddMatchesIntent(ctx context.Context, r *Resource, state *autoControl, add *MembershipAddJournal, intent *autoIntent) (bool, error) {
	if add == nil || intent == nil || intent.LearnerName == "" || string(add.MemberID) != intent.LearnerName {
		return false, nil
	}
	if state == nil || add.OperationID != membershipOperationID(r, "add") || add.WALIdentity == "" || add.SecretUID == "" ||
		add.SecretResourceVer == "" || r.Spec.Membership.ReplacementSecret != learnerCredentialName(intent) || intent.LearnerUID == "" {
		return false, fmt.Errorf("current member or immutable Secret identity differs")
	}
	var history *learnerIncarnation
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.SourceClusterID != state.ActiveClusterID || entry.OperationID != intent.Request.OperationID ||
			entry.Target.NodeID != intent.LearnerName || entry.Target.Pod != learnerPhysicalPod(intent) || entry.Target.PodUID != intent.LearnerUID {
			continue
		}
		if history != nil {
			return false, fmt.Errorf("current learner credential history is ambiguous")
		}
		history = entry
	}
	if intent.LearnerPodName == "" {
		if history == nil || history.CredentialName != learnerCredentialName(intent) || history.CredentialUID == "" ||
			history.CredentialUID != add.SecretUID || history.Retirement != nil {
			return false, fmt.Errorf("standalone current learner does not match retained credential history")
		}
	} else if intent.LearnerSecretUID == "" || intent.LearnerSecretRV == "" || intent.LearnerSecretHash == "" ||
		intent.LearnerSecretUID != add.SecretUID || intent.LearnerSecretRV != add.SecretResourceVer {
		return false, fmt.Errorf("current learner Secret differs from canonical assignment")
	}
	if history != nil && (history.CredentialName != learnerCredentialName(intent) || history.CredentialUID != add.SecretUID || history.Retirement != nil) {
		return false, fmt.Errorf("current learner conflicts with retained credential history")
	}
	if history != nil && history.Target.WALIdentity != "" && history.Target.WALIdentity != add.WALIdentity {
		return false, fmt.Errorf("current learner WAL differs from retained identity history")
	}
	credentialUID := intent.LearnerSecretUID
	if history != nil {
		credentialUID = history.CredentialUID
	}
	var secret object
	if err := c.Kube.Get(ctx, c.corePath("secrets", learnerCredentialName(intent)), &secret); err != nil {
		return false, fmt.Errorf("current immutable learner Secret is unavailable")
	}
	member, uid, resourceVersion, err := replacementMemberFromSecret(secret)
	if err != nil || secret["immutable"] != true || str(nested(secret, "metadata", "name")) != learnerCredentialName(intent) ||
		uid != credentialUID || resourceVersion != add.SecretResourceVer ||
		str(nested(secret, "metadata", "annotations", ownerAnnotation)) != intent.Request.OperationID ||
		string(member.ID) != intent.LearnerName || intent.LearnerSecretHash != "" && hashJSON(secret["data"]) != intent.LearnerSecretHash {
		return false, fmt.Errorf("current immutable learner Secret differs from canonical intent")
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{
		OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID,
		ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member,
	}
	if intent.LearnerWAL != "" && intent.LearnerWAL != add.WALIdentity || add.RequestHash == "" || add.RequestHash != hashJSON(request) {
		return false, fmt.Errorf("current learner WAL or complete ADD request differs from canonical intent")
	}
	return true, nil
}

// confirmHistoricalMembershipAbort only repeats the exact abort idempotently.
// It deliberately has no /membership/change fallback: a historical journal
// must never redispatch an old ADD while proving it settled.
func (c *Controller) confirmHistoricalMembershipAbort(ctx context.Context, r *Resource, state *autoControl, pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, admin string, current network.MembershipStatus) error {
	request, err := c.historicalRetiringMembershipRequest(ctx, r, state)
	if err != nil {
		return err
	}
	add := r.Status.Membership.Add
	if current.Pending || current.ConfigID != add.ExpectedConfigID || current.AbortSlot <= add.ExpectedAbortSlot || containsID(current.Voters, add.MemberID) {
		return fmt.Errorf("historical addition is not a settled abort")
	}
	request.Add = nil
	if err := c.membershipMutation(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, "/membership/abort", request); err != nil {
		return fmt.Errorf("historical addition abort confirmation remains unavailable")
	}
	return nil
}

func (c *Controller) membershipPods(ctx context.Context, refs []VoterPod) (map[quepaxa.NodeID]object, error) {
	pods := make(map[quepaxa.NodeID]object, len(refs))
	seenNames := map[string]bool{}
	for _, ref := range refs {
		if !identifier.MatchString(string(ref.NodeID)) || !identifier.MatchString(ref.Pod) || pods[ref.NodeID] != nil || seenNames[ref.Pod] {
			return nil, fmt.Errorf("voter pod mappings must have unique valid node IDs and Pod names")
		}
		seenNames[ref.Pod] = true
		var pod object
		if err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod); err != nil {
			return nil, fmt.Errorf("voter Pod %s is unavailable", ref.Pod)
		}
		if str(nested(pod, "metadata", "uid")) == "" || nested(pod, "metadata", "deletionTimestamp") != nil {
			return nil, fmt.Errorf("voter Pod %s is unavailable", ref.Pod)
		}
		pods[ref.NodeID] = pod
	}
	return pods, nil
}
func (c *Controller) validateMembershipReplacement(ctx context.Context, r *Resource, sts, pod object, credentialSnapshots ...object) error {
	if len(credentialSnapshots) > 1 {
		return fmt.Errorf("replacement learner credential snapshot is ambiguous")
	}
	var credentialSnapshot object
	if len(credentialSnapshots) == 1 {
		credentialSnapshot = credentialSnapshots[0]
	}
	readCredential := func() (object, error) {
		if credentialSnapshot != nil {
			return credentialSnapshot, nil
		}
		var secret object
		if err := c.Kube.Get(ctx, c.corePath("secrets", r.Spec.Membership.ReplacementSecret), &secret); err != nil {
			return nil, err
		}
		credentialSnapshot = secret
		return credentialSnapshot, nil
	}
	if str(nested(pod, "metadata", "uid")) == "" || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fmt.Errorf("replacement learner workload is unavailable")
	}
	if binding := str(nested(r.Metadata, "annotations", automaticOwner)); binding != "" {
		logical := str(nested(r.Metadata, "annotations", automaticLogical))
		if !identifier.MatchString(logical) {
			return fmt.Errorf("automatic same-ordinal replacement owner is invalid")
		}
		state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
		if err != nil {
			return err
		}
		i := state.Intent
		if i != nil && i.LearnerPodName != "" && i.LearnerPodName == r.Spec.Membership.ReplacementPod {
			if state.BindingUID != binding || i.RecoveryName != resourceName(r) || i.RecoveryUID != str(r.Metadata["uid"]) || i.SpecHash != hashJSON(r.Spec) || i.LearnerUID != str(nested(pod, "metadata", "uid")) || i.LearnerPodName != str(nested(pod, "metadata", "name")) || i.LearnerSecretName != r.Spec.Membership.ReplacementSecret || i.LearnerSecretUID == "" || i.LearnerSecretRV == "" || i.LearnerSecretHash == "" || i.LearnerAssignmentID == "" || i.LearnerName == "" || i.Failed != r.Spec.Membership.Remove || i.Aborting != r.Spec.Membership.AbortAddition {
				return fmt.Errorf("same-ordinal replacement differs from its canonical assignment")
			}
			assignment, err := sameOrdinalAssignmentDigest(state, i)
			if err != nil || assignment != i.LearnerAssignmentID {
				return fmt.Errorf("same-ordinal replacement assignment digest differs")
			}
			if str(nested(sts, "metadata", "uid")) != state.StatefulSetUID || state.StatefulSetUID != i.Request.StatefulSetUID || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != state.BindingUID {
				return fmt.Errorf("same-ordinal replacement workload binding differs")
			}
			controllerOwned := false
			for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
				owner := asObject(raw)
				if owner["controller"] == true && owner["kind"] == "StatefulSet" && owner["name"] == r.Spec.StatefulSet && owner["uid"] == state.StatefulSetUID {
					if controllerOwned {
						return fmt.Errorf("same-ordinal replacement has ambiguous workload ownership")
					}
					controllerOwned = true
				}
			}
			if !controllerOwned {
				return fmt.Errorf("same-ordinal replacement is not owned by the source StatefulSet")
			}
			if err := validateSupervisedPod(pod); err != nil {
				return err
			}
			secret, err := readCredential()
			member, uid, rv, memberErr := replacementMemberFromSecret(secret)
			if err != nil || memberErr != nil || string(member.ID) != i.LearnerName || uid != i.LearnerSecretUID || rv != i.LearnerSecretRV {
				return fmt.Errorf("same-ordinal replacement credential differs from canonical assignment")
			}
			if secret["immutable"] != true || str(nested(secret, "metadata", "name")) != i.LearnerSecretName ||
				str(nested(secret, "metadata", "annotations", ownerAnnotation)) != i.Request.OperationID || hashJSON(secret["data"]) != i.LearnerSecretHash {
				return fmt.Errorf("same-ordinal replacement Secret changed")
			}
			return nil
		}
		if i == nil {
			return fmt.Errorf("automatic standalone learner intent is unavailable")
		}
		if i.LearnerPodName == "" {
			if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
				return fmt.Errorf("standalone learner incarnation StatefulSet template protocol differs")
			}
			if state.BindingUID != binding || i.RecoveryName != resourceName(r) || i.RecoveryUID != str(r.Metadata["uid"]) || i.SpecHash != hashJSON(r.Spec) ||
				i.LearnerUID == "" || i.LearnerName == "" || i.Failed != r.Spec.Membership.Remove || i.Aborting != r.Spec.Membership.AbortAddition ||
				i.Request.BindingUID != binding || i.Request.LogicalID != logical || i.Request.Namespace != c.Kube.Namespace ||
				i.Request.StatefulSet != r.Spec.StatefulSet || i.Request.StatefulSetUID == "" || i.Request.StatefulSetUID != state.StatefulSetUID ||
				str(nested(sts, "metadata", "uid")) != state.StatefulSetUID || str(nested(pod, "metadata", "name")) != i.LearnerName ||
				str(nested(pod, "metadata", "uid")) != i.LearnerUID || r.Spec.Membership.ReplacementPod != i.LearnerName ||
				r.Spec.Membership.ReplacementSecret != learnerCredentialName(i) || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != binding ||
				str(nested(pod, "metadata", "annotations", ownerAnnotation)) != i.Request.OperationID {
				return fmt.Errorf("standalone learner incarnation differs from canonical intent")
			}
			if err := validateLearnerHistory(state.Learners); err != nil {
				return fmt.Errorf("standalone learner incarnation history is invalid")
			}
			var history *learnerIncarnation
			for idx := range state.Learners {
				entry := &state.Learners[idx]
				if entry.SourceClusterID == state.ActiveClusterID && entry.OperationID == i.Request.OperationID &&
					entry.Target.NodeID == i.LearnerName && entry.Target.Pod == i.LearnerName {
					if history != nil {
						return fmt.Errorf("standalone learner incarnation history is ambiguous")
					}
					history = entry
				}
			}
			if history == nil || history.Target.PodUID != i.LearnerUID || history.CredentialName != learnerCredentialName(i) || history.CredentialUID == "" ||
				str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation)) != history.CredentialUID {
				return fmt.Errorf("standalone learner incarnation differs from canonical history")
			}
			refs := list(nested(pod, "metadata", "ownerReferences"))
			if len(refs) != 1 {
				return fmt.Errorf("standalone learner incarnation owner differs")
			}
			owner := asObject(refs[0])
			if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["name"] != r.Spec.StatefulSet || owner["uid"] != state.StatefulSetUID || owner["controller"] != false {
				return fmt.Errorf("standalone learner incarnation owner differs")
			}
			if err := validateSupervisedPod(pod); err != nil {
				return err
			}
			secret, err := readCredential()
			member, credentialUID, _, memberErr := replacementMemberFromSecret(secret)
			if err != nil || memberErr != nil || str(nested(secret, "metadata", "name")) != history.CredentialName ||
				string(member.ID) != i.LearnerName || credentialUID != history.CredentialUID {
				return fmt.Errorf("standalone learner incarnation credential differs from canonical history")
			}
			if err := c.validateMembershipLearnerBinding(ctx, pod, r.Spec.StatefulSet, secret); err != nil {
				return fmt.Errorf("standalone learner incarnation binding differs: %w", err)
			}
			return nil
		}
	}
	uid := str(nested(sts, "metadata", "uid"))
	if !ownedBy(pod, uid) && str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) == "" {
		return nil
	}
	// Inventory ownership is not controller ownership, but only a bound supervised learner may use it.
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return fmt.Errorf("replacement learner inventory owner differs")
	}
	owner := asObject(refs[0])
	root := str(nested(r.Metadata, "annotations", automaticOwner))
	if uid == "" || owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["name"] != r.Spec.StatefulSet || owner["uid"] != uid || owner["controller"] != false || str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" || root == "" || nested(pod, "metadata", "annotations", fencingRootAnnotation) != root || nested(pod, "metadata", "annotations", ownerAnnotation) != r.Spec.Membership.OperationID || r.Spec.Membership.ReplacementSecret != r.Spec.Membership.ReplacementPod+"-credentials" {
		return fmt.Errorf("replacement learner inventory binding differs")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return err
	}
	secret, err := readCredential()
	if err != nil {
		return err
	}
	return c.validateMembershipLearnerBinding(ctx, pod, r.Spec.StatefulSet, secret)
}

func ownedBy(pod object, uid string) bool {
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		if str(asObject(ref)["uid"]) == uid {
			return true
		}
	}
	return false
}

func (c *Controller) replacementMember(ctx context.Context, name string) (quepaxa.Member, string, string, error) {
	var secret object
	if err := c.Kube.Get(ctx, c.corePath("secrets", name), &secret); err != nil || secret["immutable"] != true {
		return quepaxa.Member{}, "", "", fmt.Errorf("designated replacement credential secret is unavailable or mutable")
	}
	return replacementMemberFromSecret(secret)
}

func replacementMemberFromSecret(secret object) (quepaxa.Member, string, string, error) {
	if secret["immutable"] != true {
		return quepaxa.Member{}, "", "", fmt.Errorf("designated replacement credential secret is unavailable or mutable")
	}
	data, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	var member quepaxa.Member
	uid, resourceVer := str(nested(secret, "metadata", "uid")), str(nested(secret, "metadata", "resourceVersion"))
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err != nil || decoder.Decode(&member) != nil || decoder.Decode(new(any)) != io.EOF || uid == "" || resourceVer == "" {
		return quepaxa.Member{}, "", "", fmt.Errorf("invalid replacement member credential")
	}
	return member, uid, resourceVer, nil
}

func (c *Controller) validateMembershipLearnerBinding(ctx context.Context, pod object, stsName string, secret object) error {
	name := str(nested(pod, "metadata", "name"))
	operation := str(nested(pod, "metadata", "annotations", ownerAnnotation))
	root := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if operation == "" || root == "" || str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) != name {
		return fmt.Errorf("fencing: learner binding missing")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	rootPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(c.Kube.Namespace) + "/rhizaclusters"
	if err := c.Kube.Get(ctx, rootPath, &roots); err != nil {
		return err
	}
	found := false
	for _, candidate := range roots.Items {
		if str(candidate.Metadata["uid"]) == root && candidate.Spec.StatefulSet == stsName && candidate.Metadata["deletionTimestamp"] == nil {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("fencing: learner root binding differs")
	}
	if str(nested(secret, "metadata", "name")) != name+"-credentials" || secret["immutable"] != true ||
		str(nested(secret, "metadata", "uid")) != str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation)) ||
		str(nested(secret, "metadata", "annotations", ownerAnnotation)) != operation {
		return fmt.Errorf("fencing: learner credentials changed")
	}
	member, _, _, err := replacementMemberFromSecret(secret)
	if err != nil || string(member.ID) != name || member.PublicKey == (quepaxa.PublicKey{}) {
		return fmt.Errorf("fencing: learner member differs")
	}
	return nil
}

func (c *Controller) membershipStatuses(ctx context.Context, pods map[quepaxa.NodeID]object, container, admin string) map[quepaxa.NodeID]network.MembershipStatus {
	result := map[quepaxa.NodeID]network.MembershipStatus{}
	for id, pod := range pods {
		if nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		endpoint, err := podEndpoint(pod, container, "/membership/status")
		if err != nil {
			continue
		}
		if status, err := c.membershipStatus(ctx, endpoint, admin); err == nil && status.NodeID == id {
			result[id] = status
		}
	}
	return result
}

var ErrMembershipUnauthorized = errors.New("membership management authentication failed")

func (c *Controller) membershipStatus(ctx context.Context, endpoint, admin string) (network.MembershipStatus, error) {
	call, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(call, http.MethodGet, endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+admin)
	response, err := c.client().Do(req)
	if err != nil {
		return network.MembershipStatus{}, err
	}
	defer response.Body.Close()
	var status network.MembershipStatus
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return status, ErrMembershipUnauthorized
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&status) != nil {
		return status, fmt.Errorf("membership status unavailable")
	}
	return status, nil
}

func (c *Controller) changeMembership(ctx context.Context, pod object, container, admin string, change network.MembershipChange) error {
	return c.membershipMutation(ctx, pod, container, admin, "/membership/change", change)
}
func (c *Controller) membershipMutation(ctx context.Context, pod object, container, admin, endpointPath string, change network.MembershipChange) error {
	if pod == nil {
		return fmt.Errorf("no surviving voter endpoint")
	}
	endpoint, err := podEndpoint(pod, container, endpointPath)
	if err != nil {
		return err
	}
	data := jsonBytes(change)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(call, http.MethodPost, endpoint, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+admin)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
		var detail network.ErrorResponse
		if readErr == nil && len(data) <= 16<<10 && json.Unmarshal(data, &detail) == nil && detail.Code != "" && detail.Error != "" {
			message := detail.Code + ": " + detail.Error
			// Do not echo known credentials even if a server includes them in its error.
			for _, secret := range []string{admin} {
				if secret != "" {
					message = strings.ReplaceAll(message, secret, "[redacted]")
				}
			}
			return fmt.Errorf("membership change returned HTTP %d: %s", response.StatusCode, message)
		}
		return fmt.Errorf("membership change returned HTTP %d", response.StatusCode)
	}
	return nil
}

func membershipQuorum(statuses map[quepaxa.NodeID]network.MembershipStatus, cluster string, removed quepaxa.NodeID) (bool, network.MembershipStatus, error) {
	var current network.MembershipStatus
	for id, status := range statuses {
		if id == removed || status.NodeID != id || status.ClusterID != cluster || !status.Voting || !containsID(status.Voters, status.NodeID) || !uniqueIDs(status.Voters) {
			continue
		}
		if current.ConfigID == 0 {
			current = status
		} else if current.ConfigID != status.ConfigID || !sameIDs(current.Voters, status.Voters) {
			return false, current, fmt.Errorf("surviving voters disagree on membership status")
		}
	}
	if current.ConfigID == 0 {
		return false, current, nil
	}
	count := 0
	for id, status := range statuses {
		if id != removed && status.NodeID == id && status.ClusterID == cluster && status.Voting && containsID(status.Voters, status.NodeID) && status.ConfigID == current.ConfigID && sameIDs(status.Voters, current.Voters) {
			count++
		}
	}
	return count >= len(current.Voters)/2+1, current, nil
}

func survivorPod(pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, removed quepaxa.NodeID, current network.MembershipStatus) object {
	for id, status := range statuses {
		if id != removed && status.NodeID == id && status.ClusterID == current.ClusterID && status.Voting && containsID(status.Voters, status.NodeID) && status.ConfigID == current.ConfigID && sameIDs(status.Voters, current.Voters) {
			return pods[id]
		}
	}
	return nil
}

func containsID(ids []quepaxa.NodeID, id quepaxa.NodeID) bool {
	for _, value := range ids {
		if value == id {
			return true
		}
	}
	return false
}
func sameIDs(a, b []quepaxa.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, id := range a {
		if seen[id] {
			return false
		}
		seen[id] = true
	}
	for _, id := range b {
		if !seen[id] {
			return false
		}
		delete(seen, id)
	}
	return len(seen) == 0
}
func uniqueIDs(ids []quepaxa.NodeID) bool { return sameIDs(ids, ids) }
func sameFence(f *network.MembershipFence, want MembershipFence) bool {
	return f != nil && f.NodeID == want.NodeID && f.WALIdentity == want.WALIdentity && f.WorkloadUID == want.WorkloadUID && f.Confirmed == want.Confirmed && f.Evidence == want.Evidence
}
func membershipOperationID(r *Resource, phase string) string {
	return str(r.Metadata["uid"]) + ":" + r.Spec.Membership.OperationID + ":" + phase
}
func (c *Controller) recordMembership(ctx context.Context, r *Resource, phase, message string) error {
	r.Status.Phase, r.Status.Message = phase, message
	r.Status.ObservedGeneration = number(r.Metadata["generation"])
	if r.Status.Membership != nil {
		r.Status.Membership.Phase = phase
	}
	return c.Kube.Put(ctx, c.resourcePath(resourceName(r))+"/status", r, r)
}
