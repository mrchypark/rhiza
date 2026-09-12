package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// ClusterResource opts an existing workload into unattended recovery. The
// fencing backend is a trusted authority, configured by the operator
// administrator rather than an arbitrary URL supplied by this resource.
type ClusterResource struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   map[string]any `json:"metadata"`
	Spec       ClusterSpec    `json:"spec"`
	Status     ClusterStatus  `json:"status,omitempty"`
}
type ClusterSpec struct {
	LogicalID      string     `json:"logicalID"`
	StatefulSet    string     `json:"statefulSet"`
	Container      string     `json:"container"`
	AdoptClusterID string     `json:"adoptClusterID"`
	VoterPods      []VoterPod `json:"voterPods"`
	Automatic      bool       `json:"automatic"`
	Policy         AutoPolicy `json:"policy"`
}
type ClusterStatus struct {
	Phase              string `json:"phase"`
	Message            string `json:"message"`
	ActiveClusterID    string `json:"activeClusterID,omitempty"`
	ActiveRecovery     string `json:"activeRecovery,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration"`
}
type autoControl struct {
	Version         int                            `json:"version"`
	BindingUID      string                         `json:"bindingUID"`
	BindingHash     string                         `json:"bindingHash"`
	StatefulSetUID  string                         `json:"statefulSetUID"`
	ActiveClusterID string                         `json:"activeClusterID"`
	Voters          []VoterPod                     `json:"voters"`
	Identities      map[quepaxa.NodeID]FenceTarget `json:"identities"`
	SuspectedSince  time.Time                      `json:"suspectedSince"`
	Completions     []time.Time                    `json:"completions,omitempty"`
	Sequence        uint64                         `json:"sequence"`
	Intent          *autoIntent                    `json:"intent,omitempty"`
}
type autoIntent struct {
	Supersedes          string         `json:"supersedes,omitempty"`
	LearnerUID          string         `json:"learnerUID,omitempty"`
	LearnerWAL          string         `json:"learnerWAL,omitempty"`
	LearnerMissingSince time.Time      `json:"learnerMissingSince"`
	LearnerAttempt      int            `json:"learnerAttempt"`
	Aborting            bool           `json:"aborting"`
	PreviousSpecHash    string         `json:"previousSpecHash,omitempty"`
	NoQuorumSince       time.Time      `json:"noQuorumSince"`
	Request             FenceRequest   `json:"request"`
	Policy              AutoPolicy     `json:"policy"`
	Durability          string         `json:"durability"`
	Voters              []VoterPod     `json:"voters"`
	Failed              quepaxa.NodeID `json:"failed,omitempty"`
	Proof               *FenceProof    `json:"proof,omitempty"`
	RecoveryName        string         `json:"recoveryName"`
	RecoveryUID         string         `json:"recoveryUID,omitempty"`
	SpecHash            string         `json:"specHash,omitempty"`
	LearnerName         string         `json:"learnerName,omitempty"`
}

func (c *Controller) clusterPath(name string) string {
	p := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(c.Kube.Namespace) + "/rhizaclusters"
	if name != "" {
		p += "/" + url.PathEscape(name)
	}
	return p
}
func (c *Controller) autoKey(r *ClusterResource) string {
	return path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "control.json")
}
func autoBinding(r *ClusterResource) string {
	return hashJSON(struct {
		LogicalID, StatefulSet, Container, AdoptClusterID string
		Voters                                            []VoterPod
	}{r.Spec.LogicalID, r.Spec.StatefulSet, r.Spec.Container, r.Spec.AdoptClusterID, r.Spec.VoterPods})
}
func (c *Controller) autoStatus(ctx context.Context, r *ClusterResource, state *autoControl, phase, message string) error {
	r.Status = ClusterStatus{Phase: phase, Message: message, ObservedGeneration: number(r.Metadata["generation"])}
	if state != nil {
		r.Status.ActiveClusterID = state.ActiveClusterID
		if state.Intent != nil {
			r.Status.ActiveRecovery = state.Intent.RecoveryName
		}
	}
	return c.Kube.Put(ctx, c.clusterPath(str(r.Metadata["name"]))+"/status", r, r)
}
func (c *Controller) loadAuto(ctx context.Context, key string) (*autoControl, *objstore.ObjectVersion, error) {
	// The body must belong to the version used by the next CAS.
	for range 4 {
		before, err := c.Bucket.Attributes(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		reader, err := c.Bucket.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(data) >= 1<<20 {
			return nil, nil, fmt.Errorf("automatic control record unavailable or oversized")
		}
		after, err := c.Bucket.Attributes(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		if before.Version == nil || after.Version == nil {
			return nil, nil, fmt.Errorf("automatic recovery requires versioned object storage")
		}
		if *before.Version != *after.Version {
			continue
		}
		var state autoControl
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if dec.Decode(&state) != nil || dec.Decode(new(any)) != io.EOF || state.Version != 1 || state.BindingUID == "" || state.ActiveClusterID == "" || state.Identities == nil {
			return nil, nil, fmt.Errorf("invalid automatic control record")
		}
		return &state, after.Version, nil
	}
	return nil, nil, fmt.Errorf("automatic control record changed during read")
}
func (c *Controller) saveAuto(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion) error {
	option := objstore.WithIfNotExists()
	if version != nil {
		option = objstore.WithIfMatch(version)
	}
	return c.Bucket.Upload(ctx, c.autoKey(r), bytes.NewReader(jsonBytes(state)), option)
}
func (c *Controller) reconcileClusters(ctx context.Context) error {
	var clusters struct {
		Items []ClusterResource `json:"items"`
	}
	if err := c.Kube.Get(ctx, c.clusterPath(""), &clusters); err != nil {
		return err
	}
	var failures []error
	for i := range clusters.Items {
		if err := c.reconcileCluster(ctx, &clusters.Items[i]); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
func (c *Controller) reconcileCluster(ctx context.Context, r *ClusterResource) error {
	block := func(state *autoControl, message string) error { return c.autoStatus(ctx, r, state, "Blocked", message) }
	for _, id := range []string{str(r.Metadata["name"]), r.Spec.LogicalID, r.Spec.StatefulSet, r.Spec.AdoptClusterID} {
		if !identifier.MatchString(id) {
			return block(nil, "invalid cluster binding")
		}
	}
	if str(r.Metadata["uid"]) == "" || len(r.Spec.VoterPods) != 3 {
		return block(nil, "cluster UID and exactly three bootstrap voter mappings are required")
	}
	if r.Spec.Container == "" {
		r.Spec.Container = "rhiza"
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	uid := str(nested(sts, "metadata", "uid"))
	if uid == "" {
		return block(nil, "workload UID is unavailable")
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil && !c.Bucket.IsObjNotFoundErr(err) {
		return block(nil, "durable automatic control record is unavailable")
	}
	if state == nil {
		// A separate immutable binding prevents a missing control record or another
		// logical name from silently resetting an established workload's authority.
		key := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json")
		exists, err := c.Bucket.Exists(ctx, key)
		if err != nil {
			return err
		}
		if exists {
			return block(nil, "workload already bound; missing control record requires repair")
		}
		ctr, err := container(sts, r.Spec.Container)
		if err != nil {
			return block(nil, err.Error())
		}
		env, err := c.environment(ctx, ctr)
		if err != nil {
			return err
		}
		if env["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
			return block(nil, "automatic recovery requires membership-enabled nodes")
		}
		if env["RHIZA_CLUSTER_ID"] != r.Spec.AdoptClusterID {
			return block(nil, "adoption generation differs from workload")
		}
		if err := noPVC(sts, ctr, env); err != nil {
			return block(nil, err.Error())
		}
		if err := c.storeConfigMatches(env); err != nil {
			return block(nil, err.Error())
		}
		state = &autoControl{Version: 1, BindingUID: str(r.Metadata["uid"]), BindingHash: autoBinding(r), StatefulSetUID: uid, ActiveClusterID: r.Spec.AdoptClusterID, Voters: r.Spec.VoterPods, Identities: map[quepaxa.NodeID]FenceTarget{}}
		if err := c.saveAuto(ctx, r, state, nil); err != nil {
			return err
		}
		// Crash between record creation and this claim resumes through the same
		// immutable binding below; conflicting adopters never perform side effects.
	}
	if state.BindingUID != str(r.Metadata["uid"]) || state.BindingHash != autoBinding(r) || state.StatefulSetUID != uid {
		return block(state, "immutable cluster or workload binding changed")
	}
	binding := object{"logicalID": r.Spec.LogicalID, "bindingUID": state.BindingUID, "bindingHash": state.BindingHash}
	if err := c.immutable(ctx, path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json"), jsonBytes(binding)); err != nil {
		return block(state, "workload is owned by another automatic binding")
	}
	if version == nil {
		return c.autoStatus(ctx, r, state, "Observing", "durable cluster binding created")
	}
	if state.Intent != nil {
		return c.advanceAutomatic(ctx, r, state, version, sts)
	}
	if !r.Spec.Automatic || r.Metadata["deletionTimestamp"] != nil {
		return c.autoStatus(ctx, r, state, "Manual", "new automatic recovery is suspended")
	}
	obs, statuses, err := c.observeAutomatic(ctx, r, state, sts)
	if err != nil {
		return block(state, err.Error())
	}
	now := time.Now().UTC()
	decision, message := DecideAutoRecovery(r.Spec.Policy, obs, now, state.SuspectedSince, state.Completions)
	if decision == AutoHealthy {
		state.SuspectedSince = time.Time{}
	} else if (decision == AutoSuspect || decision == AutoDegraded) && state.SuspectedSince.IsZero() {
		state.SuspectedSince = now
	}
	// Single-voter repair keeps the live quorum. The same grace/rate limits apply,
	// but async ACK-loss permission is needed only for whole-generation DR.
	var failed quepaxa.NodeID
	if decision == AutoDegraded && len(statuses) == len(state.Voters)-1 {
		for _, ref := range state.Voters {
			if _, ok := statuses[ref.NodeID]; !ok {
				failed = ref.NodeID
			}
		}
		single := obs
		single.QuorumVerified = false
		single.Durability = "before-ack"
		decision, message = DecideAutoRecovery(r.Spec.Policy, single, now, state.SuspectedSince, state.Completions)
	}
	if decision == AutoRecover {
		if c.Fencer == nil {
			return block(state, "fencing backend is not configured")
		}
		if failed != "" && (state.Identities[failed].WALIdentity == "" || state.Identities[failed].PodUID == "") {
			return block(state, "failed voter identity has not been authenticated")
		}
		if failed == "" {
			archive := recovery.NewManager(c.Bucket, path.Join(c.Prefix, state.ActiveClusterID), 1)
			err := archive.Load(ctx)
			tip := archive.Tip()
			archive.Close()
			if err != nil || tip == 0 {
				return block(state, "certified archive is unavailable; no destructive recovery will start")
			}
		}
		if failed != "" && !c.automaticSurvivingQuorum(ctx, r, state, sts, failed) {
			return block(state, "quorum excluding the failed voter is not verified; refusing online fencing")
		}
		state.Sequence++
		name := "auto-" + shortID(state.BindingUID+":"+fmt.Sprint(state.Sequence))
		request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: c.Kube.Namespace, StatefulSet: r.Spec.StatefulSet, StatefulSetUID: uid, SourceClusterID: state.ActiveClusterID, OperationID: name, Scope: "Generation"}
		for _, ref := range state.Voters {
			if failed == "" || ref.NodeID == failed {
				target := state.Identities[ref.NodeID]
				target.NodeID = string(ref.NodeID)
				target.Pod = ref.Pod
				request.Targets = append(request.Targets, target)
			}
		}
		if failed != "" {
			request.Scope = "Voter"
		}
		state.Intent = &autoIntent{Request: request, Policy: r.Spec.Policy, Durability: obs.Durability, Voters: append([]VoterPod(nil), state.Voters...), Failed: failed, RecoveryName: name, LearnerName: name + "-learner"}
	}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return err
	}
	return c.autoStatus(ctx, r, state, string(decision), message)
}

func (c *Controller) observeAutomatic(ctx context.Context, r *ClusterResource, state *autoControl, sts object) (AutoObservation, map[quepaxa.NodeID]network.MembershipStatus, error) {
	obs := AutoObservation{Voters: len(state.Voters)}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return obs, nil, err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return obs, nil, err
	}
	if env["RHIZA_CLUSTER_ID"] != state.ActiveClusterID || env["RHIZA_ADMIN_TOKEN"] == "" {
		return obs, nil, fmt.Errorf("active generation or admin credential differs from workload")
	}
	if err := c.storeConfigMatches(env); err != nil {
		return obs, nil, err
	}
	registered, err := c.sourceMembership(ctx, path.Join(c.Prefix, state.ActiveClusterID))
	if err != nil {
		return obs, nil, err
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return obs, nil, err
	}
	if registered != recovery.NewMembershipRecord(state.ActiveClusterID, members, env["RHIZA_OBJSTORE_DURABILITY"]) {
		return obs, nil, fmt.Errorf("immutable source membership differs from workload")
	}
	obs.Known = true
	obs.StoreAvailable = true
	obs.Durability = registered.Durability
	statuses := map[quepaxa.NodeID]network.MembershipStatus{}
	for _, ref := range state.Voters {
		var pod object
		err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod)
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
				continue
			}
			return obs, nil, err
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/membership/status")
		if err != nil {
			continue
		}
		status, err := c.membershipStatus(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"])
		if err != nil {
			if errors.Is(err, ErrMembershipUnauthorized) {
				return obs, nil, err
			}
			continue
		}
		if status.ClusterID != state.ActiveClusterID || status.NodeID != ref.NodeID || !status.Voting || status.WALIdentity == "" {
			return obs, nil, fmt.Errorf("voter identity differs from active inventory")
		}
		if status.Pending {
			return obs, nil, fmt.Errorf("unmanaged membership transition is pending")
		}
		if len(status.Voters) != len(state.Voters) {
			return obs, nil, fmt.Errorf("membership differs from active inventory")
		}
		for _, voter := range state.Voters {
			if !containsID(status.Voters, voter.NodeID) {
				return obs, nil, fmt.Errorf("membership differs from active inventory")
			}
		}
		var current object
		if err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &current); err != nil {
			return obs, nil, err
		}
		uid := str(nested(pod, "metadata", "uid"))
		if uid == "" || uid != str(nested(current, "metadata", "uid")) || nested(pod, "status", "podIP") != nested(current, "status", "podIP") {
			return obs, nil, fmt.Errorf("voter Pod changed during authentication")
		}
		state.Identities[ref.NodeID] = FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: uid, WALIdentity: status.WALIdentity}
		statuses[ref.NodeID] = status
		endpoint, err = podEndpoint(pod, r.Spec.Container, "/recovery/probe")
		if err != nil {
			continue
		}
		if c.automaticQuorumProbe(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"], state.ActiveClusterID, string(ref.NodeID)) {
			obs.QuorumVerified = true
		}
	}
	obs.Reachable = len(statuses)
	return obs, statuses, nil
}
