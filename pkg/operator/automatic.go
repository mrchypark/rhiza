package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
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
	LogicalID      string `json:"logicalID"`
	StatefulSet    string `json:"statefulSet"`
	Container      string `json:"container"`
	AdoptClusterID string `json:"adoptClusterID"`
	VoterCount     int    `json:"voterCount"`
	HTTPPort       int    `json:"httpPort,omitempty"`
	PeerPort       int    `json:"peerPort,omitempty"`
	ClusterDomain  string `json:"clusterDomain,omitempty"`
	Bootstrap      bool   `json:"bootstrap,omitempty"`
	AnchorID       string `json:"anchorID,omitempty"`

	Automatic bool        `json:"automatic,omitempty"`
	Policy    *AutoPolicy `json:"policy,omitempty"`
}
type ClusterStatus struct {
	Phase              string `json:"phase"`
	Message            string `json:"message"`
	ActiveClusterID    string `json:"activeClusterID,omitempty"`
	ActiveRecovery     string `json:"activeRecovery,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration"`
}
type autoControl struct {
	Version           int                            `json:"version"`
	BindingUID        string                         `json:"bindingUID"`
	BindingHash       string                         `json:"bindingHash"`
	BindingVersion    int                            `json:"bindingVersion,omitempty"`
	LegacyBindingHash string                         `json:"legacyBindingHash,omitempty"`
	StatefulSetUID    string                         `json:"statefulSetUID"`
	ActiveClusterID   string                         `json:"activeClusterID"`
	Voters            []VoterPod                     `json:"voters"`
	Identities        map[quepaxa.NodeID]FenceTarget `json:"identities"`
	SuspectedSince    time.Time                      `json:"suspectedSince"`
	Completions       []time.Time                    `json:"completions,omitempty"`
	Sequence          uint64                         `json:"sequence"`

	Intent *autoIntent `json:"intent,omitempty"`
	// BootstrapGenesis is set only when the protected fencer preflight
	// authenticated the exact initial held HEAD mutation and runtime cohort.
	// It is controller-side durable intent, not authority; the fencer remains
	// authoritative for the binding and closed proof.
	BootstrapGenesis *BootstrapGenesisBinding `json:"bootstrapGenesis,omitempty"`
	Completed        *autoCompletion          `json:"completed,omitempty"`
	Learners         []learnerIncarnation     `json:"learners,omitempty"`
}

const autoBindingVersion = 1

var clusterDomainRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Keep retired learners until their source generation is fully replaced.
type learnerIncarnation struct {
	SourceClusterID string             `json:"sourceClusterID"`
	OperationID     string             `json:"operationID"`
	CredentialName  string             `json:"credentialName,omitempty"`
	CredentialUID   string             `json:"credentialUID"`
	Target          FenceTarget        `json:"target"`
	Retirement      *learnerRetirement `json:"retirement,omitempty"`
}

type learnerRetirement struct {
	Request FenceRequest `json:"request"`
	Proof   FenceProof   `json:"proof"`
}

type bootstrapCandidate struct {
	Version, VoterCount, HTTPPort, PeerPort  int
	LogicalID, StatefulSet, StatefulSetUID   string
	AdoptClusterID, Container, ClusterDomain string
	ServiceName                              string
}

// autoCompletion records an installed outcome, independently of later health.
type autoCompletion struct {
	RecoveryName     string      `json:"recoveryName"`
	RecoveryUID      string      `json:"recoveryUID"`
	SpecHash         string      `json:"specHash"`
	FenceProofID     string      `json:"fenceProofID"`
	FenceRequestHash string      `json:"fenceRequestHash"`
	ClusterID        string      `json:"clusterID"`
	Voters           []VoterPod  `json:"voters"`
	LearnerIntent    *autoIntent `json:"learnerIntent,omitempty"`
	At               time.Time   `json:"at"`
}
type autoIntent struct {
	Supersedes          string    `json:"supersedes,omitempty"`
	LearnerUID          string    `json:"learnerUID,omitempty"`
	LearnerWAL          string    `json:"learnerWAL,omitempty"`
	LearnerMissingSince time.Time `json:"learnerMissingSince"`
	LearnerAttempt      int       `json:"learnerAttempt"`
	Aborting            bool      `json:"aborting"`
	RetirementSince     time.Time `json:"retirementSince,omitempty"`

	PreviousSpecHash string         `json:"previousSpecHash,omitempty"`
	NoQuorumSince    time.Time      `json:"noQuorumSince"`
	Request          FenceRequest   `json:"request"`
	Policy           AutoPolicy     `json:"policy"`
	Durability       string         `json:"durability"`
	Voters           []VoterPod     `json:"voters"`
	Failed           quepaxa.NodeID `json:"failed,omitempty"`
	Proof            *FenceProof    `json:"proof,omitempty"`
	RecoveryName     string         `json:"recoveryName"`
	RecoveryUID      string         `json:"recoveryUID,omitempty"`
	SpecHash         string         `json:"specHash,omitempty"`
	LearnerName      string         `json:"learnerName,omitempty"`
	// LearnerPodName may differ from LearnerName only for a supervisor-assigned
	// fresh logical identity on a recreated StatefulSet ordinal.
	LearnerPodName      string `json:"learnerPodName,omitempty"`
	LearnerSecretName   string `json:"learnerSecretName,omitempty"`
	LearnerSecretUID    string `json:"learnerSecretUID,omitempty"`
	LearnerSecretRV     string `json:"learnerSecretResourceVersion,omitempty"`
	LearnerSecretHash   string `json:"learnerSecretHash,omitempty"`
	LearnerAssignmentID string `json:"learnerAssignmentID,omitempty"`
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
	httpPort, peerPort := r.Spec.HTTPPort, r.Spec.PeerPort
	if httpPort == 0 {
		httpPort = 8080
	}
	if peerPort == 0 {
		peerPort = 9090
	}
	clusterDomain := r.Spec.ClusterDomain
	if clusterDomain == "" {
		clusterDomain = "cluster.local"
	}
	return hashJSON(struct {
		LogicalID, StatefulSet, Container, AdoptClusterID string
		AnchorID                                          string `json:"anchorID,omitempty"`
		ClusterDomain                                     string
		VoterCount, HTTPPort, PeerPort                    int
	}{r.Spec.LogicalID, r.Spec.StatefulSet, r.Spec.Container, r.Spec.AdoptClusterID, r.Spec.AnchorID, clusterDomain, r.Spec.VoterCount, httpPort, peerPort})
}

func (c *Controller) migrateLegacyBinding(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, members []quepaxa.Member, adopted bool, sts object) (bool, error) {
	if state.BindingVersion != 0 {
		return false, nil
	}
	if state.BindingHash == "" {
		return false, fmt.Errorf("legacy automatic binding hash is missing")
	}
	if err := c.verifyLegacyBindingRecord(ctx, r, state, str(nested(sts, "metadata", "uid"))); err != nil {
		return false, err
	}
	if state.ActiveClusterID != r.Spec.AdoptClusterID {
		return false, fmt.Errorf("legacy automatic binding adoptClusterID changed: stored=%s current=%s", state.ActiveClusterID, r.Spec.AdoptClusterID)
	}
	replicas := int(number(nested(sts, "spec", "replicas")))
	if r.Spec.VoterCount != replicas {
		return false, fmt.Errorf("legacy automatic binding migration requires voterCount %d to equal StatefulSet desired replicas %d", r.Spec.VoterCount, replicas)
	}
	if !adopted || len(members) != r.Spec.VoterCount {
		return false, fmt.Errorf("legacy automatic binding migration requires voterCount %d to equal exported RHIZA_CLUSTER_MEMBERS count %d", r.Spec.VoterCount, len(members))
	}
	if err := legacyVoterIdentityDrift(state.Voters, members); err != nil {
		return false, err
	}
	// The old hash is covered by the immutable workload binding below. Do not
	// derive it from Voters, which a completed single-voter repair can rewrite.
	oldHash := state.BindingHash
	newHash := autoBinding(r)
	proof := object{
		"version": 1, "logicalID": r.Spec.LogicalID, "bindingUID": state.BindingUID,
		"statefulSetUID": str(nested(sts, "metadata", "uid")), "adoptClusterID": r.Spec.AdoptClusterID,
		"voterCount": r.Spec.VoterCount, "legacyBindingHash": oldHash, "bindingHash": newHash,
		"members": members,
	}
	proofKey := path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "binding-migrations", shortID(oldHash+":"+newHash)+".json")
	if err := c.immutable(ctx, proofKey, jsonBytes(proof)); err != nil {
		return false, fmt.Errorf("legacy binding migration proof is unavailable or inconsistent")
	}
	state.LegacyBindingHash = oldHash
	state.BindingVersion = autoBindingVersion
	state.BindingHash = newHash
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Controller) verifyLegacyBindingRecord(ctx context.Context, r *ClusterResource, state *autoControl, uid string) error {
	key := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json")
	reader, err := c.Bucket.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("legacy automatic binding record is unavailable")
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	var binding object
	if readErr != nil || closeErr != nil || len(data) >= 1<<20 || json.Unmarshal(data, &binding) != nil || str(binding["logicalID"]) != r.Spec.LogicalID || str(binding["bindingUID"]) != state.BindingUID || str(binding["bindingHash"]) != state.BindingHash {
		return fmt.Errorf("legacy automatic binding record differs from the stored binding")
	}
	return nil
}

func legacyVoterIdentityDrift(voters []VoterPod, members []quepaxa.Member) error {
	if len(voters) != len(members) {
		return fmt.Errorf("legacy automatic binding voter inventory drifts from the exported membership: frozen=%d exported=%d", len(voters), len(members))
	}
	exported := make(map[quepaxa.NodeID]bool, len(members))
	for _, member := range members {
		exported[member.ID] = true
	}
	seen := make(map[quepaxa.NodeID]bool, len(voters))
	for _, voter := range voters {
		if string(voter.NodeID) != voter.Pod || !exported[voter.NodeID] || seen[voter.NodeID] {
			return fmt.Errorf("legacy automatic binding voter inventory drifts from the exported membership: frozen entries must match exported node IDs and Pod names one-for-one")
		}
		seen[voter.NodeID] = true
	}
	return nil
}

func (c *Controller) claimAutoBinding(ctx context.Context, r *ClusterResource, state *autoControl, uid string) error {
	key := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json")
	reader, err := c.Bucket.Get(ctx, key)
	if c.Bucket.IsObjNotFoundErr(err) {
		binding := object{"logicalID": r.Spec.LogicalID, "bindingUID": state.BindingUID, "bindingHash": state.BindingHash}
		if err := c.immutable(ctx, key, jsonBytes(binding)); err != nil {
			return fmt.Errorf("workload is owned by another automatic binding")
		}
		return nil
	}
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	var binding object
	if readErr != nil || closeErr != nil || len(data) >= 1<<20 || json.Unmarshal(data, &binding) != nil {
		return fmt.Errorf("workload binding record is unreadable")
	}
	if str(binding["logicalID"]) != r.Spec.LogicalID || str(binding["bindingUID"]) != state.BindingUID {
		return fmt.Errorf("workload is owned by another automatic binding")
	}
	hash := str(binding["bindingHash"])
	if hash != state.BindingHash && (state.LegacyBindingHash == "" || hash != state.LegacyBindingHash) {
		return fmt.Errorf("workload binding hash changed")
	}
	return nil
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
	return c.reconcileClusterList(ctx, false)
}

func (c *Controller) reconcileInitialSupervisedRoots(ctx context.Context) error {
	return c.reconcileClusterList(ctx, true)
}

func (c *Controller) reconcileClusterList(ctx context.Context, initialRootOnly bool) error {
	var clusters struct {
		Items []ClusterResource `json:"items"`
	}
	if err := c.Kube.Get(ctx, c.clusterPath(""), &clusters); err != nil {
		var apiErr *APIError
		if initialRootOnly && errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return err
	}
	var failures []error
	for i := range clusters.Items {
		var err error
		if initialRootOnly {
			err = c.reconcileClusterMode(ctx, &clusters.Items[i], true)
		} else {
			err = c.reconcileCluster(ctx, &clusters.Items[i])
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// voterCountContract runs before any binding is persisted and on every later
// observation. Derived node IDs are checked for a new binding and for an
// established binding whose frozen inventory already follows the derived shape.
func voterCountContract(r *ClusterResource, sts object, members []quepaxa.Member, adopted, checkIDs bool, allowed map[string]bool) error {
	replicas := int(number(nested(sts, "spec", "replicas")))
	return voterInventoryContract(r, members, adopted, checkIDs, allowed, &replicas)
}

// voterInventoryContract validates the declared cohort independently of the
// current replica count. It is used only while pinning the root UID onto an
// empty supervised source template, before the driver scales that workload.
func voterInventoryContract(r *ClusterResource, members []quepaxa.Member, adopted, checkIDs bool, allowed map[string]bool, desiredReplicas *int) error {
	count := r.Spec.VoterCount
	if count < 3 {
		if count == 0 {
			if desiredReplicas != nil {
				return fmt.Errorf("spec.voterCount is required and must equal the StatefulSet desired replicas and exported RHIZA_CLUSTER_MEMBERS count (%d)", *desiredReplicas)
			}
			return fmt.Errorf("spec.voterCount is required and must be at least 3")
		}
		if desiredReplicas != nil {
			return fmt.Errorf("spec.voterCount=%d is below the minimum 3; set it to the StatefulSet desired replicas and exported RHIZA_CLUSTER_MEMBERS count (%d)", count, *desiredReplicas)
		}
		return fmt.Errorf("spec.voterCount=%d is below the minimum 3", count)
	}
	if count > fenceTargetCapacity {
		return fmt.Errorf("spec.voterCount=%d exceeds the maximum %d", count, fenceTargetCapacity)
	}
	if desiredReplicas != nil && *desiredReplicas != count {
		return fmt.Errorf("StatefulSet has %d desired replicas but voterCount is %d", *desiredReplicas, count)
	}
	if adopted && len(members) != count {
		return fmt.Errorf("RHIZA_CLUSTER_MEMBERS lists %d voters but voterCount is %d", len(members), count)
	}
	if adopted && checkIDs {
		for _, member := range members {
			if !allowed[string(member.ID)] {
				return fmt.Errorf("RHIZA_CLUSTER_MEMBERS node ID %q is neither a derived voter nor a journaled learner", member.ID)
			}
		}
	}
	return nil
}

// bindInitialSupervisedRoot pins the canonical RhizaCluster UID before an
// opted-in supervised workload is first scaled. It is deliberately limited to
// an empty, zero-replica StatefulSet; it never mutates a live Pod or repairs a
// conflicting/previously unbound workload.
func (c *Controller) bindInitialSupervisedRoot(ctx context.Context, r *ClusterResource, sts object, env map[string]string, members []quepaxa.Member, adopted, checkIDs bool, allowed map[string]bool, state *autoControl) (bool, error) {
	if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return false, nil
	}
	rootUID := str(r.Metadata["uid"])
	statefulSetUID := str(nested(sts, "metadata", "uid"))
	if rootUID == "" || statefulSetUID == "" || r.Metadata["deletionTimestamp"] != nil ||
		str(r.Metadata["namespace"]) != "" && str(r.Metadata["namespace"]) != c.Kube.Namespace {
		return false, fmt.Errorf("automatic supervised source root or workload identity is unavailable")
	}
	if env["RHIZA_CLUSTER_ID"] != r.Spec.AdoptClusterID || env["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
		return false, fmt.Errorf("automatic supervised source contract differs from RhizaCluster")
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return false, err
	}
	if err := validateClusterRuntime(r, ctr, env); err != nil {
		return false, err
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return false, err
	}
	if err := c.storeConfigMatches(env); err != nil {
		return false, err
	}
	if err := voterInventoryContract(r, members, adopted, checkIDs, allowed, nil); err != nil {
		return false, err
	}
	if state != nil {
		if state.BindingUID != rootUID || state.StatefulSetUID != statefulSetUID || state.ActiveClusterID != r.Spec.AdoptClusterID {
			return false, fmt.Errorf("durable automatic source binding differs from RhizaCluster")
		}
		if state.BindingVersion == 0 {
			if err := c.verifyLegacyBindingRecord(ctx, r, state, statefulSetUID); err != nil {
				return false, err
			}
		} else if state.BindingHash != autoBinding(r) {
			return false, fmt.Errorf("durable automatic source binding hash differs from RhizaCluster")
		}
	} else {
		bindingKey := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, statefulSetUID, "binding.json")
		exists, err := c.Bucket.Exists(ctx, bindingKey)
		if err != nil {
			return false, fmt.Errorf("durable automatic workload binding is unavailable")
		}
		if exists {
			return false, fmt.Errorf("durable automatic workload binding exists without its control record")
		}
	}
	return c.pinInitialSupervisedRoot(ctx, r, sts, rootUID, statefulSetUID, env)
}

func (c *Controller) pinInitialSupervisedRoot(ctx context.Context, r *ClusterResource, observedSTS object, rootUID, statefulSetUID string, observedEnv map[string]string) (bool, error) {
	var currentRoot ClusterResource
	if err := c.Kube.Get(ctx, c.clusterPath(str(r.Metadata["name"])), &currentRoot); err != nil {
		return false, fmt.Errorf("automatic source root identity could not be revalidated")
	}
	normalizeClusterSpec(&currentRoot)
	if str(currentRoot.Metadata["name"]) != str(r.Metadata["name"]) || str(currentRoot.Metadata["uid"]) != rootUID ||
		currentRoot.Metadata["deletionTimestamp"] != nil || hashJSON(currentRoot.Spec) != hashJSON(r.Spec) {
		return false, fmt.Errorf("automatic source root identity or contract changed")
	}
	if str(nested(observedSTS, "metadata", "uid")) != statefulSetUID {
		return false, fmt.Errorf("automatic source StatefulSet identity changed")
	}
	observedVersion := str(nested(observedSTS, "metadata", "resourceVersion"))
	if observedVersion == "" {
		return false, fmt.Errorf("automatic source StatefulSet resource version is unavailable")
	}
	var current object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &current); err != nil {
		return false, fmt.Errorf("automatic source StatefulSet could not be revalidated")
	}
	if str(nested(current, "metadata", "uid")) != statefulSetUID || str(nested(current, "metadata", "resourceVersion")) != observedVersion || nested(current, "metadata", "deletionTimestamp") != nil {
		return false, fmt.Errorf("automatic source StatefulSet identity or resource version changed")
	}
	if str(nested(current, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return false, fmt.Errorf("automatic source no longer has the supervised workload contract")
	}
	template := asObject(nested(current, "spec", "template"))
	if err := validateSupervisedPod(object{"metadata": template["metadata"], "spec": template["spec"]}); err != nil {
		return false, err
	}
	root := str(nested(current, "spec", "template", "metadata", "annotations", fencingRootAnnotation))
	if root != "" && root != rootUID {
		return false, fmt.Errorf("automatic source template is bound to another root UID")
	}
	needsPin := root == ""
	if needsPin {
		if int(number(nested(current, "spec", "replicas"))) != 0 {
			return false, fmt.Errorf("automatic source root can only be pinned before the StatefulSet is scaled")
		}
		if err := c.validateInitialSourcePods(ctx, current, statefulSetUID, rootUID, true); err != nil {
			return false, err
		}
	}
	ctr, err := container(current, r.Spec.Container)
	if err != nil {
		return false, err
	}
	currentEnv, err := c.environment(ctx, ctr)
	if err != nil {
		return false, fmt.Errorf("automatic source environment could not be revalidated")
	}
	if hashJSON(currentEnv) != hashJSON(observedEnv) || currentEnv["RHIZA_CLUSTER_ID"] != r.Spec.AdoptClusterID || currentEnv["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
		return false, fmt.Errorf("automatic source contract changed before root pin")
	}
	if !needsPin {
		if err := c.validateInitialSourcePods(ctx, current, statefulSetUID, rootUID, false); err != nil {
			return false, err
		}
		return false, nil
	}
	metadata := asObject(nested(current, "spec", "template", "metadata"))
	if metadata == nil {
		metadata = object{}
		asObject(nested(current, "spec", "template"))["metadata"] = metadata
	}
	annotations := asObject(metadata["annotations"])
	if annotations == nil {
		annotations = object{}
		metadata["annotations"] = annotations
	}
	annotations[fencingRootAnnotation] = rootUID
	var updated object
	if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), current, &updated); err != nil {
		return false, fmt.Errorf("automatic source root pin conflicted; retry after revalidation")
	}
	if str(nested(updated, "metadata", "uid")) != statefulSetUID || str(nested(updated, "spec", "template", "metadata", "annotations", fencingRootAnnotation)) != rootUID {
		return false, fmt.Errorf("automatic source root pin response did not preserve the verified binding")
	}
	return true, nil
}

func (c *Controller) validateInitialSourcePods(ctx context.Context, sts object, statefulSetUID, rootUID string, requireEmpty bool) error {
	selector := asObject(nested(sts, "spec", "selector", "matchLabels"))
	if len(selector) == 0 || len(list(nested(sts, "spec", "selector", "matchExpressions"))) != 0 {
		return fmt.Errorf("automatic source requires a nonempty, explicit matchLabels selector")
	}
	var pods podList
	if err := c.Kube.Get(ctx, c.corePath("pods", ""), &pods); err != nil {
		return fmt.Errorf("automatic source Pod inventory could not be verified")
	}
	name := str(nested(sts, "metadata", "name"))
	for _, pod := range pods.Items {
		matched := true
		for key, value := range selector {
			if nested(pod, "metadata", "labels", key) != value {
				matched = false
				break
			}
		}
		owned := false
		for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
			owner := asObject(raw)
			if str(owner["kind"]) == "StatefulSet" && str(owner["uid"]) == statefulSetUID {
				owned = true
			}
		}
		if !matched && !owned {
			continue
		}
		if nested(pod, "metadata", "deletionTimestamp") != nil {
			return fmt.Errorf("automatic source has a terminating matching or StatefulSet-owned Pod")
		}
		if requireEmpty {
			return fmt.Errorf("automatic source root pin requires no matching or StatefulSet-owned Pods")
		}
		if !matched || !owned || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != rootUID {
			return fmt.Errorf("automatic source Pod ownership or root binding differs")
		}
		ownerCount := 0
		for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
			owner := asObject(raw)
			if str(owner["apiVersion"]) == "apps/v1" && str(owner["kind"]) == "StatefulSet" && str(owner["name"]) == name && str(owner["uid"]) == statefulSetUID && owner["controller"] == true {
				ownerCount++
			}
		}
		if ownerCount != 1 {
			return fmt.Errorf("automatic source Pod controller ownership is ambiguous")
		}
	}
	return nil
}

func clusterMembers(raw string) ([]quepaxa.Member, bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, false, nil
	}
	members, err := recoveryMembers(raw)
	return members, true, err
}

func normalizeClusterSpec(r *ClusterResource) {
	if r.Spec.Container == "" {
		r.Spec.Container = "rhiza"
	}
	if r.Spec.HTTPPort == 0 {
		r.Spec.HTTPPort = 8080
	}
	if r.Spec.PeerPort == 0 {
		r.Spec.PeerPort = 9090
	}
	if r.Spec.ClusterDomain == "" {
		r.Spec.ClusterDomain = "cluster.local"
	}
}

func (c *Controller) validateClusterService(ctx context.Context, r *ClusterResource, sts object) error {
	service := str(nested(sts, "spec", "serviceName"))
	if service == "" {
		return fmt.Errorf("StatefulSet %s spec.serviceName is required for voter DNS names", r.Spec.StatefulSet)
	}
	var record object
	err := c.Kube.Get(ctx, c.corePath("services", service), &record)
	if apiNotFound(err) {
		return fmt.Errorf("StatefulSet serviceName %q does not name an existing Service", service)
	}
	if err != nil {
		return err
	}
	if clusterIP := str(nested(record, "spec", "clusterIP")); clusterIP != "None" {
		return fmt.Errorf("Service %q must be headless: clusterIP=%q, want None", service, clusterIP)
	}
	if nested(record, "spec", "publishNotReadyAddresses") != true {
		return fmt.Errorf("Service %q must set publishNotReadyAddresses: true for voter DNS", service)
	}
	selector := asObject(nested(record, "spec", "selector"))
	labels := asObject(nested(sts, "spec", "template", "metadata", "labels"))
	if len(selector) == 0 {
		return fmt.Errorf("Service %q must select the StatefulSet Pods", service)
	}
	for key, value := range selector {
		if labels[key] != value {
			return fmt.Errorf("Service %q selector %s=%q does not match StatefulSet Pod labels", service, key, value)
		}
	}
	return nil
}

func validateClusterRuntime(r *ClusterResource, ctr object, env map[string]string) error {
	nodeID := object(nil)
	for _, raw := range list(ctr["env"]) {
		entry := asObject(raw)
		if str(entry["name"]) == "RHIZA_NODE_ID" {
			nodeID = entry
			break
		}
	}
	if nodeID == nil || str(nested(nodeID, "valueFrom", "fieldRef", "fieldPath")) != "metadata.name" {
		observed := "missing"
		if nodeID != nil {
			observed = str(nodeID["value"])
			if observed == "" {
				observed = "not fieldRef metadata.name"
			}
		}
		return fmt.Errorf("RHIZA_NODE_ID must use fieldRef metadata.name; observed %s", observed)
	}
	for _, check := range []struct {
		key      string
		expected int
	}{
		{"RHIZA_BIND_ADDR", r.Spec.HTTPPort},
		{"RHIZA_PEER_ADDR", r.Spec.PeerPort},
	} {
		addr := env[check.key]
		if addr == "" {
			return fmt.Errorf("%s must be set to a reachable host:port", check.key)
		}
		host, portText, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("%s must be host:port; observed %q", check.key, env[check.key])
		}
		if host == "localhost" {
			return fmt.Errorf("%s must not bind a loopback host; observed %q", check.key, host)
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return fmt.Errorf("%s must not bind a loopback host; observed %q", check.key, host)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			return fmt.Errorf("%s port must be numeric; observed %q", check.key, portText)
		}
		if port != check.expected {
			return fmt.Errorf("%s port is %d but expected %d", check.key, port, check.expected)
		}
	}
	return nil
}

func derivedVoters(r *ClusterResource, sts object) ([]VoterPod, error) {
	service := str(nested(sts, "spec", "serviceName"))
	if service == "" {
		return nil, fmt.Errorf("StatefulSet %s spec.serviceName is required for voter DNS names", r.Spec.StatefulSet)
	}
	voters := make([]VoterPod, 0, r.Spec.VoterCount)
	for i := range r.Spec.VoterCount {
		pod := fmt.Sprintf("%s-%d", r.Spec.StatefulSet, i)
		voters = append(voters, VoterPod{NodeID: quepaxa.NodeID(pod), Pod: pod})
	}
	return voters, nil
}

func allowedGenerationVoterIDs(r *ClusterResource, sts object, state *autoControl) (map[string]bool, error) {
	voters, err := derivedVoters(r, sts)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(voters))
	for _, voter := range voters {
		allowed[string(voter.NodeID)] = true
	}
	if state != nil {
		for _, entry := range state.Learners {
			if entry.SourceClusterID == state.ActiveClusterID {
				allowed[string(entry.Target.NodeID)] = true
			}
		}
	}
	return allowed, nil
}

func clusterMemberSources(r *ClusterResource, sts object, namespace string) ([]quepaxa.Member, error) {
	voters, err := derivedVoters(r, sts)
	if err != nil {
		return nil, err
	}
	service := str(nested(sts, "spec", "serviceName"))
	host := func(pod string) string {
		return fmt.Sprintf("%s.%s.%s.svc.%s", pod, service, namespace, r.Spec.ClusterDomain)
	}
	members := make([]quepaxa.Member, 0, len(voters))
	for _, voter := range voters {
		members = append(members, quepaxa.Member{ID: voter.NodeID, URL: "http://" + host(voter.Pod) + ":" + fmt.Sprint(r.Spec.HTTPPort), PeerURL: "quic://" + host(voter.Pod) + ":" + fmt.Sprint(r.Spec.PeerPort)})
	}
	return members, nil
}

func (c *Controller) validateBootstrappedMembers(ctx context.Context, r *ClusterResource, sts object, adopted bool, members []quepaxa.Member, allowed map[string]bool) error {
	if !adopted {
		return nil
	}
	var secret object
	err := c.Kube.Get(ctx, c.corePath("secrets", bootstrapSecretName(r)), &secret)
	if apiNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	service := str(nested(sts, "spec", "serviceName"))
	if service == "" {
		return fmt.Errorf("StatefulSet %s spec.serviceName is required for voter DNS names", r.Spec.StatefulSet)
	}
	for _, member := range members {
		pod := string(member.ID)
		if !allowed[pod] {
			return fmt.Errorf("adopted member node ID %q is neither a derived voter nor a journaled learner", pod)
		}
		for _, endpoint := range []struct {
			raw    string
			scheme string
			port   int
		}{{member.URL, "http", r.Spec.HTTPPort}, {member.PeerURL, "quic", r.Spec.PeerPort}} {
			parsed, err := url.Parse(endpoint.raw)
			if err != nil || parsed.Scheme != endpoint.scheme || parsed.Port() == "" {
				return fmt.Errorf("adopted member %s endpoint %q must use %s with an explicit port", pod, endpoint.raw, endpoint.scheme)
			}
			if fmt.Sprint(endpoint.port) != parsed.Port() {
				return fmt.Errorf("adopted member %s endpoint %q port does not match declared %d", pod, endpoint.raw, endpoint.port)
			}
			host := strings.TrimSuffix(parsed.Hostname(), ".")
			allowed := map[string]bool{
				pod + "." + service:                                                           true,
				pod + "." + service + "." + c.Kube.Namespace:                                  true,
				pod + "." + service + "." + c.Kube.Namespace + ".svc":                         true,
				pod + "." + service + "." + c.Kube.Namespace + ".svc." + r.Spec.ClusterDomain: true,
			}
			if !allowed[host] {
				return fmt.Errorf("adopted member %s endpoint %q host does not name Service %s", pod, endpoint.raw, service)
			}
		}
	}
	return nil
}

func (c *Controller) rejectRegisteredCluster(ctx context.Context, clusterID string) error {
	prefix := path.Join(c.Prefix, clusterID)
	commitment := path.Join(prefix, "voters/membership.json")
	exists, err := c.Bucket.Exists(ctx, commitment)
	if err != nil {
		return fmt.Errorf("object store unavailable while checking cluster registration: %w", err)
	}
	if !exists {
		return nil
	}
	if _, err := c.sourceMembership(ctx, prefix); err != nil {
		return fmt.Errorf("registered cluster membership is unreadable: %w", err)
	}
	return fmt.Errorf("cluster %s is already registered in object storage; refusing bootstrap", clusterID)
}

func (c *Controller) bootstrapCandidateKey(r *ClusterResource) string {
	return path.Join(c.Prefix, "automatic-bootstrap", c.Kube.Namespace, r.Spec.StatefulSet+".json")
}

func expectedBootstrapCandidate(r *ClusterResource, sts object) bootstrapCandidate {
	return bootstrapCandidate{Version: 1, VoterCount: r.Spec.VoterCount, HTTPPort: r.Spec.HTTPPort, PeerPort: r.Spec.PeerPort, LogicalID: r.Spec.LogicalID, StatefulSet: r.Spec.StatefulSet, StatefulSetUID: str(nested(sts, "metadata", "uid")), AdoptClusterID: r.Spec.AdoptClusterID, Container: r.Spec.Container, ClusterDomain: r.Spec.ClusterDomain, ServiceName: str(nested(sts, "spec", "serviceName"))}
}

func (c *Controller) readBootstrapCandidate(ctx context.Context, r *ClusterResource, sts object) (bool, error) {
	var stored bootstrapCandidate
	reader, err := c.Bucket.Get(ctx, c.bootstrapCandidateKey(r))
	if c.Bucket.IsObjNotFoundErr(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("fresh bootstrap reservation is unavailable")
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if readErr != nil || closeErr != nil || len(data) >= 64<<10 || decoder.Decode(&stored) != nil || decoder.Decode(new(any)) != io.EOF {
		return false, fmt.Errorf("fresh bootstrap reservation is invalid")
	}
	if stored != expectedBootstrapCandidate(r, sts) {
		return false, fmt.Errorf("fresh bootstrap reservation differs from the current workload contract")
	}
	return true, nil
}

// archiveMembershipMissing distinguishes the only recoverable bootstrap
// signal (definitive object-not-found) from unreadable or malformed history.
// A present object is left to the ordinary membership validation path.
func (c *Controller) archiveMembershipMissing(ctx context.Context, sourceID string) (bool, error) {
	reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, sourceID, "voters/membership.json"))
	if c.Bucket.IsObjNotFoundErr(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("source membership history is unavailable")
	}
	if err := reader.Close(); err != nil {
		return false, fmt.Errorf("source membership history is unavailable")
	}
	return false, nil
}

func sameBootstrapTargets(targets []FenceTarget, voters []VoterPod, captured map[quepaxa.NodeID]FenceTarget) bool {
	if len(targets) == 0 || len(targets) != len(voters) {
		return false
	}
	byID := make(map[string]FenceTarget, len(targets))
	for _, target := range targets {
		if target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" || target.NodeName == "" || target.NodeUID == "" || target.ContainerID == "" || target.ImageID == "" {
			return false
		}
		if _, exists := byID[target.NodeID]; exists {
			return false
		}
		byID[target.NodeID] = target
	}
	seenPods := make(map[string]bool, len(voters))
	for _, voter := range voters {
		target, ok := byID[string(voter.NodeID)]
		if !ok || target.Pod != voter.Pod || seenPods[target.Pod] {
			return false
		}
		if captured != nil {
			if previous, exists := captured[voter.NodeID]; !exists || previous != target {
				return false
			}
		}
		seenPods[target.Pod] = true
	}
	return true
}

func bootstrapTemplateInstalled(ctr object, r *ClusterResource) bool {
	secretName := bootstrapSecretName(r)
	found := false
	peersCleared := false
	for _, raw := range list(ctr["env"]) {
		entry := asObject(raw)
		switch str(entry["name"]) {
		case "RHIZA_CLUSTER_MEMBERS":
			found = str(nested(entry, "valueFrom", "secretKeyRef", "name")) == secretName
		case "RHIZA_PEER_TOKEN":
			peersCleared = str(entry["value"]) == ""
		}
	}
	return found && peersCleared
}

func (c *Controller) installBootstrapCredentials(ctx context.Context, r *ClusterResource, sts, ctr object) error {
	sources, err := clusterMemberSources(r, sts, c.Kube.Namespace)
	if err != nil {
		return err
	}
	secretName := bootstrapSecretName(r)
	members, err := c.targetCredentials(ctx, &Resource{Metadata: object{"uid": str(r.Metadata["uid"])}, Spec: Spec{RecoveryID: r.Spec.LogicalID}, Status: Status{Target: r.Spec.AdoptClusterID}}, secretName, sources)
	if err != nil {
		return err
	}
	if len(members) != r.Spec.VoterCount {
		return fmt.Errorf("bootstrap generated %d voters but voterCount is %d", len(members), r.Spec.VoterCount)
	}
	applyTargetCredentials(ctr, secretName)
	asObject(sts["spec"])["replicas"] = float64(0)
	return c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), sts, &sts)
}

func (c *Controller) reconcileBootstrap(ctx context.Context, r *ClusterResource, sts, ctr object, adopted bool) (bool, string, error) {
	uid := str(nested(sts, "metadata", "uid"))
	if number(nested(sts, "spec", "ordinals", "start")) != 0 {
		return false, "", fmt.Errorf("fresh bootstrap requires StatefulSet spec.ordinals.start=0")
	}
	if r.Spec.VoterCount < 3 {
		return false, "", fmt.Errorf("spec.voterCount is required and must be at least 3")
	}
	if r.Spec.VoterCount > fenceTargetCapacity {
		return false, "", fmt.Errorf("spec.voterCount=%d exceeds the maximum %d", r.Spec.VoterCount, fenceTargetCapacity)
	}
	pods, err := c.ownedPods(ctx, uid)
	if err != nil {
		return false, "", err
	}
	if len(pods) != 0 {
		return false, "", fmt.Errorf("fresh bootstrap refuses to start while writer Pods already exist")
	}
	templateInstalled := bootstrapTemplateInstalled(ctr, r)
	if !templateInstalled && int(number(nested(sts, "spec", "replicas"))) != 0 {
		return false, "", fmt.Errorf("fresh bootstrap requires replicas=0 until credentials and template are installed; observed %d", number(nested(sts, "spec", "replicas")))
	}
	key := c.bootstrapCandidateKey(r)
	exists, err := c.Bucket.Exists(ctx, key)
	if err != nil {
		return false, "", fmt.Errorf("object store unavailable while checking bootstrap reservation: %w", err)
	}
	expected := expectedBootstrapCandidate(r, sts)
	if !exists {
		if err := c.rejectRegisteredCluster(ctx, r.Spec.AdoptClusterID); err != nil {
			return false, "", err
		}
		if err := c.immutable(ctx, key, jsonBytes(expected)); err != nil {
			return false, "", fmt.Errorf("fresh bootstrap reservation is unavailable or inconsistent")
		}
	} else {
		var stored bootstrapCandidate
		reader, err := c.Bucket.Get(ctx, key)
		if err != nil {
			return false, "", fmt.Errorf("fresh bootstrap reservation is unreadable")
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(data) == 64<<10 || json.Unmarshal(data, &stored) != nil {
			return false, "", fmt.Errorf("fresh bootstrap reservation is invalid")
		}
		if stored != expected {
			return false, "", fmt.Errorf("fresh bootstrap reservation differs from the current workload contract")
		}
	}
	if !templateInstalled {
		current, currentCtr, err := c.writerFreeBootstrapTarget(ctx, r, sts)
		if err != nil {
			return false, "", err
		}
		// Reservation commits before any write. The replica count and Pod list are
		// checked again immediately before the write; the fresh resourceVersion on
		// the StatefulSet PUT makes a concurrent scale conflict. A Pod appearing in
		// the final window cannot start writers while replicas remains zero, and the
		// next reconcile blocks before scale-up.
		if err := c.installBootstrapCredentials(ctx, r, current, currentCtr); err != nil {
			return false, "", err
		}
		return true, "bootstrap credentials installed; waiting for writer-free scale up", nil
	}
	if int(number(nested(sts, "spec", "replicas"))) == 0 {
		current, _, err := c.writerFreeBootstrapTarget(ctx, r, sts)
		if err != nil {
			return false, "", err
		}
		asObject(current["spec"])["replicas"] = float64(r.Spec.VoterCount)
		if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), current, &current); err != nil {
			return false, "", err
		}
		return true, "bootstrap membership installed; starting voters", nil
	}
	if adopted {
		return false, "", nil
	}
	return true, "bootstrap installed credentials but the workload is not exporting its bootstrap membership", nil
}

func (c *Controller) writerFreeBootstrapTarget(ctx context.Context, r *ClusterResource, sts object) (object, object, error) {
	uid := str(nested(sts, "metadata", "uid"))
	before, err := c.ownedPods(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	if len(before) != 0 {
		return nil, nil, fmt.Errorf("fresh bootstrap refuses to write while writer Pods already exist")
	}
	var current object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &current); err != nil {
		return nil, nil, err
	}
	if str(nested(current, "metadata", "uid")) != uid || int(number(nested(current, "spec", "replicas"))) != 0 {
		return nil, nil, fmt.Errorf("fresh bootstrap write precondition changed: StatefulSet replicas=%v", nested(current, "spec", "replicas"))
	}
	ctr, err := container(current, r.Spec.Container)
	if err != nil {
		return nil, nil, err
	}
	after, err := c.ownedPods(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	if len(after) != 0 {
		return nil, nil, fmt.Errorf("fresh bootstrap refuses to write because a writer Pod appeared during validation")
	}
	return current, ctr, nil
}

func bootstrapSecretName(r *ClusterResource) string {
	operation := &Resource{
		Metadata: object{"uid": str(r.Metadata["uid"])},
		Spec:     Spec{RecoveryID: r.Spec.LogicalID},
		Status:   Status{Target: r.Spec.AdoptClusterID},
	}
	return "rhiza-cluster-" + shortID(str(operation.Metadata["uid"])+":"+r.Spec.LogicalID+":"+r.Spec.AdoptClusterID)
}

func (c *Controller) reconcileCluster(ctx context.Context, r *ClusterResource) error {
	return c.reconcileClusterMode(ctx, r, false)
}

func (c *Controller) reconcileClusterMode(ctx context.Context, r *ClusterResource, initialRootOnly bool) error {
	block := func(state *autoControl, message string) error {
		if initialRootOnly {
			return fmt.Errorf("%s", message)
		}
		return c.autoStatus(ctx, r, state, "Blocked", message)
	}
	normalizeClusterSpec(r)
	for _, id := range []string{str(r.Metadata["name"]), r.Spec.LogicalID, r.Spec.StatefulSet, r.Spec.AdoptClusterID} {
		if !identifier.MatchString(id) {
			return block(nil, "invalid cluster binding")
		}
	}
	if str(r.Metadata["uid"]) == "" {
		return block(nil, "cluster UID is required")
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	uid := str(nested(sts, "metadata", "uid"))
	if uid == "" {
		return block(nil, "workload UID is unavailable")
	}
	if initialRootOnly && str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return nil
	}
	if number(nested(sts, "spec", "ordinals", "start")) != 0 {
		return block(nil, "StatefulSet spec.ordinals.start=0 is required; nonzero starting ordinals are not supported")
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return block(nil, err.Error())
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return err
	}
	if err := anchorEnvGate(r.Spec.AnchorID, env, env["RHIZA_ENABLE_RECONFIGURATION"] == "true", c.Anchor); err != nil {
		return block(nil, err.Error())
	}
	if !clusterDomainRE.MatchString(r.Spec.ClusterDomain) {
		return block(nil, fmt.Sprintf("spec.clusterDomain %q is not a DNS name", r.Spec.ClusterDomain))
	}
	if err := c.validateClusterService(ctx, r, sts); err != nil {
		return block(nil, err.Error())
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil && !c.Bucket.IsObjNotFoundErr(err) {
		return block(nil, "durable automatic control record is unavailable")
	}
	if !initialRootOnly {
		if err := validateAutomaticPolicyForNewIncident(state, r.Spec.Automatic, r.Spec.Policy); err != nil {
			return block(state, err.Error())
		}
	}
	members, adopted, err := clusterMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return block(nil, err.Error())
	}
	if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1" {
		allowed, err := allowedGenerationVoterIDs(r, sts, state)
		if err != nil {
			return block(nil, err.Error())
		}
		checkIDs := state == nil
		if !checkIDs {
			checkIDs = len(state.Voters) == r.Spec.VoterCount
			for _, voter := range state.Voters {
				if !allowed[string(voter.NodeID)] {
					checkIDs = false
					break
				}
			}
		}
		if err := voterInventoryContract(r, members, adopted, checkIDs, allowed, nil); err != nil {
			return block(state, err.Error())
		}
		if err := c.validateBootstrappedMembers(ctx, r, sts, adopted, members, allowed); err != nil {
			return block(state, err.Error())
		}
		deferReconcile, err := c.bindInitialSupervisedRoot(ctx, r, sts, env, members, adopted, checkIDs, allowed, state)
		if err != nil {
			return block(state, err.Error())
		}
		if initialRootOnly {
			return nil
		}
		if deferReconcile || (int(number(nested(sts, "spec", "replicas"))) == 0 && !r.Spec.Bootstrap) {
			return c.autoStatus(ctx, r, state, "Observing", "supervised source root is pinned; waiting for the authorized initial bootstrap or scale")
		}
	}
	if initialRootOnly {
		return nil
	}
	if state == nil || state.BootstrapGenesis != nil {
		handled, message, err := c.reconcileAutomaticBootstrapGenesis(ctx, r, sts, ctr, env, state, version, members, adopted)
		if err != nil {
			return block(state, err.Error())
		}
		if handled {
			return c.autoStatus(ctx, r, state, "Observing", message)
		}
	}
	if state == nil {
		candidateExists, err := c.Bucket.Exists(ctx, c.bootstrapCandidateKey(r))
		if err != nil {
			return block(nil, "object store unavailable while checking bootstrap reservation: "+err.Error())
		}
		if candidateExists || (!adopted && r.Spec.Bootstrap) {
			if err := validateClusterRuntime(r, ctr, env); err != nil {
				return block(nil, err.Error())
			}
			handled, message, err := c.reconcileBootstrap(ctx, r, sts, ctr, adopted)
			if err != nil {
				return block(nil, err.Error())
			}
			if handled {
				return c.autoStatus(ctx, r, nil, "Observing", message)
			}
		} else if !adopted {
			return block(nil, "RHIZA_CLUSTER_MEMBERS is empty; set spec.bootstrap: true for a fresh workload or export RHIZA_CLUSTER_MEMBERS")
		}
	}
	allowed, err := allowedGenerationVoterIDs(r, sts, state)
	if err != nil {
		return block(nil, err.Error())
	}
	// A binding created by this schema already has the allowed inventory. Keep
	// enforcing its node IDs, while an older frozen inventory is not invalidated
	// by ID shape changes alone.
	checkIDs := state == nil
	if !checkIDs {
		checkIDs = len(state.Voters) == r.Spec.VoterCount
		for _, voter := range state.Voters {
			if !allowed[string(voter.NodeID)] {
				checkIDs = false
				break
			}
		}
	}
	if err := voterCountContract(r, sts, members, adopted, checkIDs, allowed); err != nil {
		return block(nil, err.Error())
	}
	if err := c.validateBootstrappedMembers(ctx, r, sts, adopted, members, allowed); err != nil {
		return block(nil, err.Error())
	}
	if state == nil {
		// A separate immutable binding prevents a missing control record or another
		// logical name from silently resetting an established workload's authority.
		key := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json")
		exists, err := c.Bucket.Exists(ctx, key)
		if err != nil {
			return block(nil, "object store unavailable while checking workload binding: "+err.Error())
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
		voters, err := derivedVoters(r, sts)
		if err != nil {
			return block(nil, err.Error())
		}
		state = &autoControl{Version: 1, BindingUID: str(r.Metadata["uid"]), BindingHash: autoBinding(r), BindingVersion: autoBindingVersion, StatefulSetUID: uid, ActiveClusterID: r.Spec.AdoptClusterID, Voters: voters, Identities: map[quepaxa.NodeID]FenceTarget{}}
		if err := c.saveAuto(ctx, r, state, nil); err != nil {
			return err
		}
		// Crash between record creation and this claim resumes through the same
		// immutable binding below; conflicting adopters never perform side effects.
	}
	if state.BindingUID != str(r.Metadata["uid"]) || state.StatefulSetUID != uid {
		return block(state, "immutable cluster or workload binding changed")
	}
	if state.BindingVersion == 0 {
		migrated, err := c.migrateLegacyBinding(ctx, r, state, version, members, adopted, sts)
		if err != nil {
			return block(state, err.Error())
		}
		if migrated {
			return c.autoStatus(ctx, r, state, "Observing", "legacy automatic binding migrated; continue with next reconcile")
		}
	} else if state.BindingHash != autoBinding(r) {
		return block(state, "immutable cluster or workload binding changed")
	}
	if err := c.claimAutoBinding(ctx, r, state, uid); err != nil {
		return block(state, err.Error())
	}
	if version == nil {
		return c.autoStatus(ctx, r, state, "Observing", "durable cluster binding created")
	}
	if err := validateLearnerHistory(state.Learners); err != nil {
		return block(state, err.Error())
	}
	if state.Intent != nil {
		return c.advanceAutomatic(ctx, r, state, version, sts)
	}
	if compacted, err := c.compactLearnerHistory(ctx, r, state); err != nil {
		return block(state, err.Error())
	} else if compacted {
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Observing", "learner history compacted; continue with next reconcile")
	}
	if state.BootstrapGenesis != nil {
		handled, message, err := c.reconcileAutomaticBootstrapGenesis(ctx, r, sts, ctr, env, state, version, members, adopted)
		if err != nil {
			return block(state, err.Error())
		}
		if handled {
			return c.autoStatus(ctx, r, state, "Observing", message)
		}
	}
	if !r.Spec.Automatic || r.Metadata["deletionTimestamp"] != nil {
		return c.autoStatus(ctx, r, state, "Manual", "new automatic recovery is suspended")
	}
	policy := *r.Spec.Policy
	// The declared count, the workload environment, and the replica count are one
	// contract; reconciling a mismatch as an outage would fence a live majority.
	obs, _, err := c.observeAutomatic(ctx, r, state, sts)
	if err != nil {
		return block(state, err.Error())
	}
	now := time.Now().UTC()
	decision, message := DecideAutoRecovery(policy, obs, now, state.SuspectedSince, state.Completions)
	if decision == AutoHealthy {
		state.SuspectedSince = time.Time{}
	} else if (decision == AutoSuspect || decision == AutoDegraded) && state.SuspectedSince.IsZero() {
		state.SuspectedSince = now
	}
	// Ordinary failed-Pod replacement belongs to the engine. The operator starts
	// automatic recovery only when the current generation has lost quorum.
	if decision == AutoRecover {
		if c.Fencer == nil {
			return block(state, "fencing backend is not configured")
		}
		if !automaticArchiveAllowsGenerationRecovery(ctx, c.Bucket, path.Join(c.Prefix, state.ActiveClusterID), obs.Durability, policy.AllowDataLoss) {
			return block(state, "certified archive is unavailable; no destructive recovery will start")
		}
		state.Sequence++
		name := "auto-" + shortID(state.BindingUID+":"+fmt.Sprint(state.Sequence))
		request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: c.Kube.Namespace, StatefulSet: r.Spec.StatefulSet, StatefulSetUID: uid, SourceClusterID: state.ActiveClusterID, OperationID: name, Scope: "Generation"}
		for _, ref := range state.Voters {
			target := state.Identities[ref.NodeID]
			target.NodeID = string(ref.NodeID)
			target.Pod = ref.Pod
			request.Targets = append(request.Targets, target)
		}
		if err := includeLearnerHistory(state, &request); err != nil {
			return block(state, err.Error())
		}
		// The scope is final only after the retained learner history is added.
		if err := fenceCapacityError(len(request.Targets)); err != nil {
			return block(state, err.Error())
		}
		state.Intent = &autoIntent{Request: request, Policy: policy, Durability: obs.Durability, Voters: append([]VoterPod(nil), state.Voters...), RecoveryName: name, LearnerName: name + "-learner"}
	}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return err
	}
	return c.autoStatus(ctx, r, state, string(decision), message)
}

// reconcileAutomaticBootstrapGenesis is the only automatic path that may
// bypass archive membership discovery. It requires an immutable bootstrap
// reservation, a definitive missing membership object, and the authenticated
// CONTROL preflight for the exact held initial HEAD/cohort. Any later fence is
// still submitted through the normal durable automatic intent and Fence proof.
func (c *Controller) reconcileAutomaticBootstrapGenesis(ctx context.Context, r *ClusterResource, sts, ctr object, env map[string]string, state *autoControl, version *objstore.ObjectVersion, members []quepaxa.Member, adopted bool) (bool, string, error) {
	if state != nil && state.BootstrapGenesis == nil {
		return false, "", nil
	}
	if state != nil && (state.Intent != nil || state.BootstrapGenesis == nil) {
		return false, "", nil
	}
	if !r.Spec.Automatic || !r.Spec.Bootstrap || r.Metadata["deletionTimestamp"] != nil || r.Spec.Policy == nil || !r.Spec.Policy.AllowDataLoss {
		if state != nil {
			return true, "bootstrap recovery policy changed; no further fencing will start", nil
		}
		return false, "", nil
	}
	if err := validateClusterRuntime(r, ctr, env); err != nil {
		return false, "", err
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return false, "", err
	}
	if env["RHIZA_CLUSTER_ID"] != r.Spec.AdoptClusterID || env["RHIZA_OBJSTORE_DURABILITY"] != "async" || env["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
		return false, "", nil
	}
	exists, err := c.readBootstrapCandidate(ctx, r, sts)
	if err != nil || !exists {
		if state != nil {
			return true, "bootstrap candidate is unavailable or changed", err
		}
		return false, "", err
	}
	missing, err := c.archiveMembershipMissing(ctx, r.Spec.AdoptClusterID)
	if err != nil {
		return false, "", err
	}
	if !missing {
		if state == nil {
			return false, "", nil
		}
		registered, err := c.sourceMembership(ctx, path.Join(c.Prefix, r.Spec.AdoptClusterID))
		if err != nil || !adopted || registered != recovery.NewMembershipRecord(r.Spec.AdoptClusterID, members, env["RHIZA_OBJSTORE_DURABILITY"]) {
			return true, "bootstrap membership appeared but does not match the source contract", err
		}
		state.BootstrapGenesis = nil
		state.SuspectedSince = time.Time{}
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, "", err
		}
		return true, "initial archive membership is now registered; resuming ordinary observation", nil
	}
	if !adopted || len(members) != r.Spec.VoterCount {
		return false, "", nil
	}
	allowed, err := allowedGenerationVoterIDs(r, sts, state)
	if err != nil {
		return false, "", err
	}
	if replicas := int(number(nested(sts, "spec", "replicas"))); replicas == 0 {
		if err := c.validateInitialSourcePods(ctx, sts, str(nested(sts, "metadata", "uid")), str(r.Metadata["uid"]), true); err != nil {
			return false, "", err
		}
		if err := voterInventoryContract(r, members, adopted, true, allowed, nil); err != nil {
			return false, "", err
		}
	} else if err := voterCountContract(r, sts, members, adopted, true, allowed); err != nil {
		return false, "", err
	}
	if err := c.validateBootstrappedMembers(ctx, r, sts, adopted, members, allowed); err != nil {
		return false, "", err
	}
	preflight, ok := c.Fencer.(BootstrapGenesisPreflighter)
	if !ok || preflight == nil {
		if state != nil {
			return true, "authenticated bootstrap preflight is unavailable", nil
		}
		return false, "", nil
	}
	response, err := preflight.BootstrapGenesisPreflight(ctx, BootstrapGenesisPreflightRequest{
		ClusterName: str(r.Metadata["name"]), ClusterUID: str(r.Metadata["uid"]), StatefulSetUID: str(nested(sts, "metadata", "uid")),
	})
	if err != nil {
		if state != nil {
			if errors.Is(err, ErrFencePending) {
				return true, "bootstrap runtime cohort or canonical genesis evidence is pending", nil
			}
			return true, "authenticated bootstrap preflight failed", err
		}
		// A fresh zero-Pod bootstrap remains on the existing path. Once Pods
		// exist, the old path refuses to reinitialize them and this branch stays
		// fail-closed rather than inventing runtime identity.
		pods, podErr := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
		if podErr == nil && len(pods) == 0 && int(number(nested(sts, "spec", "replicas"))) == 0 {
			return false, "", nil
		}
		return true, "bootstrap genesis preflight is pending or unavailable", nil
	}
	if response.Binding.Version != 1 || response.Binding.ClusterName != str(r.Metadata["name"]) || response.Binding.ClusterUID != str(r.Metadata["uid"]) || !validEvidenceDigest(response.Binding.CandidateDigest) {
		return true, "authenticated bootstrap preflight returned an invalid binding", nil
	}
	voters, err := derivedVoters(r, sts)
	if err != nil || !sameBootstrapTargets(response.Targets, voters, nil) {
		return true, "authenticated bootstrap preflight differs from the declared voter inventory", err
	}
	if state == nil {
		bindingKey := path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, str(nested(sts, "metadata", "uid")), "binding.json")
		bound, err := c.Bucket.Exists(ctx, bindingKey)
		if err != nil || bound {
			return true, "automatic bootstrap workload binding is unavailable or already claimed", err
		}
		identities := make(map[quepaxa.NodeID]FenceTarget, len(response.Targets))
		for _, target := range response.Targets {
			identities[quepaxa.NodeID(target.NodeID)] = target
		}
		state = &autoControl{Version: 1, BindingUID: str(r.Metadata["uid"]), BindingHash: autoBinding(r), BindingVersion: autoBindingVersion,
			StatefulSetUID: str(nested(sts, "metadata", "uid")), ActiveClusterID: r.Spec.AdoptClusterID,
			Voters: voters, Identities: identities, BootstrapGenesis: &response.Binding, SuspectedSince: time.Now().UTC()}
		if err := c.saveAuto(ctx, r, state, nil); err != nil {
			return true, "", err
		}
		return true, "protected bootstrap genesis is identified; starting the configured failure grace period", nil
	}
	if state.BootstrapGenesis == nil || *state.BootstrapGenesis != response.Binding || state.BindingUID != str(r.Metadata["uid"]) || state.StatefulSetUID != str(nested(sts, "metadata", "uid")) || state.ActiveClusterID != r.Spec.AdoptClusterID || !sameBootstrapTargets(response.Targets, state.Voters, state.Identities) {
		return true, "bootstrap genesis binding or captured runtime cohort changed", nil
	}
	if state.SuspectedSince.IsZero() {
		state.SuspectedSince = time.Now().UTC()
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, "", err
		}
		return true, "bootstrap failure grace period started", nil
	}
	// The protected preflight is the source of truth for this special
	// observation; no API membership/read failure is translated to quorum loss.
	observation := AutoObservation{Known: true, Voters: len(state.Voters), StoreAvailable: true, Durability: "async"}
	decision, message := DecideAutoRecovery(*r.Spec.Policy, observation, time.Now().UTC(), state.SuspectedSince, state.Completions)
	if decision != AutoRecover {
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, "", err
		}
		return true, message, nil
	}
	if c.Fencer == nil {
		return true, "fencing backend is unavailable", nil
	}
	state.Sequence++
	name := "auto-" + shortID(state.BindingUID+":"+fmt.Sprint(state.Sequence))
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: c.Kube.Namespace,
		StatefulSet: r.Spec.StatefulSet, StatefulSetUID: state.StatefulSetUID, SourceClusterID: state.ActiveClusterID,
		OperationID: name, Scope: ScopeGeneration, Targets: append([]FenceTarget(nil), response.Targets...), BootstrapGenesis: state.BootstrapGenesis}
	if err := validateRequest(&request); err != nil {
		return true, "bootstrap generation fence request is invalid", err
	}
	state.Intent = &autoIntent{Request: request, Policy: *r.Spec.Policy, Durability: "async", Voters: append([]VoterPod(nil), state.Voters...), RecoveryName: name, LearnerName: name + "-learner"}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return true, "", err
	}
	return true, "bootstrap generation fence is durably reserved", nil
}

// An empty async archive can authorize generation recovery only when its
// initialized HEAD is present and the policy explicitly accepts ACK loss.
func automaticArchiveAllowsGenerationRecovery(ctx context.Context, bucket objstore.Bucket, prefix, durability string, allowDataLoss bool) bool {
	archive := recovery.NewManager(bucket, prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return false
	}
	if archive.Tip() != 0 {
		return true
	}
	_, hasHead := archive.HeadVersion()
	return durability == "async" && allowDataLoss && hasHead
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
		state.Identities[ref.NodeID] = capturedFenceIdentity(FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: uid, WALIdentity: status.WALIdentity}, current)
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
