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
		return blocked("membership fence workload UID does not match the removed voter workload")
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
	if journal.Phase == "Aborted" {
		if m.AbortAddition {
			return nil
		}
		if journal.Add == nil || expected.ConfigID != journal.Add.ExpectedConfigID || expected.AbortSlot <= journal.Add.ExpectedAbortSlot {
			return blocked("journaled addition abort is not confirmed by the current membership status")
		}
		journal.Add, journal.Phase = nil, "Adding"
		return c.recordMembership(ctx, r, "Adding", "aborted addition reset before replacement dispatch")
	}
	if m.AbortAddition {
		if journal.Add == nil {
			return blocked("addition is not journaled; abort is unavailable")
		}
		return c.abortMembership(ctx, r, pods, statuses, admin, expected)
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

func (c *Controller) reconcileMembershipAdd(ctx context.Context, r *Resource, sts object, pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, admin string, current network.MembershipStatus) error {
	m, journal := r.Spec.Membership, r.Status.Membership
	var replacement object
	if err := c.Kube.Get(ctx, c.corePath("pods", m.ReplacementPod), &replacement); err != nil {
		return c.recordMembership(ctx, r, "Blocked", "replacement learner workload is unavailable")
	}
	if ownedBy(replacement, str(nested(sts, "metadata", "uid"))) || str(nested(replacement, "metadata", "uid")) == "" || nested(replacement, "metadata", "deletionTimestamp") != nil {
		return c.recordMembership(ctx, r, "Blocked", "replacement learner workload is unavailable")
	}
	endpoint, err := podEndpoint(replacement, r.Spec.Container, "/membership/status")
	if err != nil {
		return c.recordMembership(ctx, r, "Blocked", err.Error())
	}
	member, secretUID, secretResourceVer, err := c.replacementMember(ctx, m.ReplacementSecret)
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
		journal.Phase = "Complete"
		return c.recordMembership(ctx, r, "Complete", "replacement learner promoted under surviving quorum")
	}
	if journal.Add == nil {
		if current.Pending || learner.Voting || learner.Pending || containsID(current.Voters, member.ID) {
			return c.recordMembership(ctx, r, "Blocked", "replacement learner or surviving quorum has a pending membership transition")
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

func (c *Controller) abortMembership(ctx context.Context, r *Resource, pods map[quepaxa.NodeID]object, statuses map[quepaxa.NodeID]network.MembershipStatus, admin string, current network.MembershipStatus) error {
	add := r.Status.Membership.Add
	if current.ConfigID != add.ExpectedConfigID || current.AbortSlot < add.ExpectedAbortSlot {
		return c.recordMembership(ctx, r, "Blocked", "live membership does not match the journaled addition for abort")
	}
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot}
	if err := c.membershipMutation(ctx, survivorPod(pods, statuses, "", current), r.Spec.Container, admin, "/membership/abort", request); err != nil {
		return c.recordMembership(ctx, r, "Aborting", "abort request remains pending: "+err.Error())
	}
	return c.recordMembership(ctx, r, "Aborted", "journaled addition aborted by surviving quorum")
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
	if r.Status.Membership != nil {
		r.Status.Membership.Phase = phase
	}
	return c.Kube.Put(ctx, c.resourcePath(resourceName(r))+"/status", r, r)
}
