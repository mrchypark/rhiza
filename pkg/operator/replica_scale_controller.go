package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	v1alpha2ReplicaClusterLabel    = "rhiza.mrchypark.dev/cluster-uid"
	v1alpha2ReplicaGenerationLabel = "rhiza.mrchypark.dev/generation"
)

var (
	v1alpha2ReplicaCheckWait = 6 * time.Second
	v1alpha2ReplicaCheckPoll = 200 * time.Millisecond
	v1alpha2ReplicaCheckTTL  = 10 * time.Second
)

// V1Alpha2ReplicaObservation is a validation result for one live Deployment.
// Pod readiness is evidence only; it never contributes membership.
type V1Alpha2ReplicaObservation struct {
	DesiredReplicas              int32
	ObservedReplicas             int32
	ReadyReplicas                int32
	UpdatedReplicas              int32
	DeploymentGeneration         int64
	ObservedDeploymentGeneration int64
	DeploymentResourceVersion    string
	Selector                     string
}

type v1alpha2ReplicaDeployment struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Metadata   v1alpha2ReplicaMetadata `json:"metadata"`
	Spec       struct {
		Replicas *int32 `json:"replicas"`
		Selector struct {
			MatchLabels      map[string]string `json:"matchLabels"`
			MatchExpressions []json.RawMessage `json:"matchExpressions"`
		} `json:"selector"`
		Template struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas           int32 `json:"replicas"`
		ReadyReplicas      int32 `json:"readyReplicas"`
		UpdatedReplicas    int32 `json:"updatedReplicas"`
		ObservedGeneration int64 `json:"observedGeneration"`
	} `json:"status"`
}

type v1alpha2ReplicaScale struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Metadata   v1alpha2ReplicaMetadata `json:"metadata"`
	Spec       struct {
		Replicas *int32 `json:"replicas"`
	} `json:"spec"`
	Status struct {
		Replicas int32 `json:"replicas"`
	} `json:"status"`
}

type v1alpha2ReplicaMetadata struct {
	Name, Namespace, UID, ResourceVersion string
	Generation                            int64
	DeletionTimestamp                     *string
	Labels                                map[string]string
	OwnerReferences                       []v1alpha2BootstrapOwner
}

func (m v1alpha2ReplicaMetadata) bootstrapMetadata() v1alpha2BootstrapMetadata {
	return v1alpha2BootstrapMetadata{Name: m.Name, Namespace: m.Namespace, UID: m.UID, ResourceVersion: m.ResourceVersion, DeletionTimestamp: m.DeletionTimestamp, OwnerReferences: m.OwnerReferences}
}

// ObserveV1Alpha2ReplicaDeployment validates the authority-owned Deployment and
// returns counts suitable for HPA status. It does not establish membership.
func ObserveV1Alpha2ReplicaDeployment(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, expected V1Alpha2AuthorityView) (V1Alpha2ReplicaObservation, error) {
	admission, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return V1Alpha2ReplicaObservation{}, err
	}
	return observeV1Alpha2ReplicaDeployment(ctx, store, admission, expected)
}

func observeV1Alpha2ReplicaDeployment(ctx context.Context, store *V1Alpha2StatusStore, admission V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView) (V1Alpha2ReplicaObservation, error) {
	if store == nil || admission.status.store != store || admission.Authority() != expected {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("replica observation authority mismatch")
	}
	status, err := jsonObject(admission.status.object["status"])
	if err != nil || len(status["genesis"]) == 0 || bytes.Equal(bytes.TrimSpace(status["genesis"]), []byte("null")) {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("replica control requires committed genesis")
	}
	name, err := V1Alpha2GenesisDeploymentName(admission.status.ref.UID)
	if err != nil {
		return V1Alpha2ReplicaObservation{}, err
	}
	base := "/apis/apps/v1/namespaces/" + url.PathEscape(admission.status.ref.Namespace) + "/deployments/" + url.PathEscape(name)
	var deployment v1alpha2ReplicaDeployment
	if err := store.Kube.Get(ctx, base, &deployment); err != nil {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("observe replica Deployment: %w", err)
	}
	generation := strconv.FormatInt(expected.Generation, 10)
	wantLabels := map[string]string{v1alpha2ReplicaClusterLabel: expected.Owner.UID, v1alpha2ReplicaGenerationLabel: generation}
	if deployment.APIVersion != "apps/v1" || deployment.Kind != "Deployment" || deployment.Metadata.Name != name || deployment.Metadata.Namespace != admission.status.ref.Namespace || deployment.Metadata.UID != expected.Binding.DeploymentUID || deployment.Metadata.UID == "" || deployment.Metadata.ResourceVersion == "" || deployment.Metadata.Generation < 1 || deployment.Metadata.DeletionTimestamp != nil || !selectorLabelsMatch(deployment.Metadata.Labels, wantLabels) || !bootstrapOwner(deployment.Metadata.bootstrapMetadata(), V1Alpha2APIVersion, "RhizaCluster", expected.Owner.Name, expected.Owner.UID) {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("replica Deployment identity mismatch")
	}
	selector, err := v1alpha2ReplicaSelector(deployment.Spec.Selector.MatchLabels, deployment.Spec.Selector.MatchExpressions, wantLabels)
	if err != nil || !selectorLabelsMatch(deployment.Spec.Template.Metadata.Labels, wantLabels) {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("replica Deployment selector mismatch")
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 3 || *deployment.Spec.Replicas > 64 || deployment.Status.Replicas < 0 || deployment.Status.ReadyReplicas < 0 || deployment.Status.ReadyReplicas > deployment.Status.Replicas || deployment.Status.UpdatedReplicas < 0 || deployment.Status.UpdatedReplicas > deployment.Status.Replicas || deployment.Status.ObservedGeneration < 0 || deployment.Status.ObservedGeneration > deployment.Metadata.Generation {
		return V1Alpha2ReplicaObservation{}, fmt.Errorf("invalid replica counts")
	}
	return V1Alpha2ReplicaObservation{DesiredReplicas: *deployment.Spec.Replicas, ObservedReplicas: deployment.Status.Replicas, ReadyReplicas: deployment.Status.ReadyReplicas, UpdatedReplicas: deployment.Status.UpdatedReplicas, DeploymentGeneration: deployment.Metadata.Generation, ObservedDeploymentGeneration: deployment.Status.ObservedGeneration, DeploymentResourceVersion: deployment.Metadata.ResourceVersion, Selector: selector}, nil
}

func v1alpha2ReplicaSelector(matchLabels map[string]string, expressions []json.RawMessage, expected map[string]string) (string, error) {
	if len(expressions) != 0 || len(matchLabels) != len(expected) {
		return "", fmt.Errorf("unsupported selector")
	}
	keys := make([]string, 0, len(expected))
	for key, value := range expected {
		if matchLabels[key] != value {
			return "", fmt.Errorf("selector label mismatch")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+matchLabels[key])
	}
	return strings.Join(parts, ","), nil
}

func selectorLabelsMatch(labels, expected map[string]string) bool {
	if len(labels) < len(expected) {
		return false
	}
	for key, value := range expected {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// ReconcileV1Alpha2ReplicaScale applies at most one durable Host-authorized
// replica step. The Deployment /scale resourceVersion is the atomic guard.
func ReconcileV1Alpha2ReplicaScale(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef) error {
	snapshot, err := store.Load(ctx, ref)
	if err != nil {
		return err
	}
	spec, err := jsonObject(snapshot.object["spec"])
	if err != nil {
		return err
	}
	if _, enabled := spec["replicas"]; !enabled {
		return nil
	}
	admission, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return err
	}
	_, enabled, err := admission.ReplicaProposal()
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	step, present, err := admission.ReplicaStep()
	if err != nil {
		return err
	}
	if !present {
		observation, err := observeV1Alpha2ReplicaDeployment(ctx, store, admission, admission.Authority())
		if err != nil {
			return err
		}
		return reportV1Alpha2ReplicaScale(ctx, store, admission, admission.Authority(), observation)
	}
	if step.To < step.From {
		return reconcileV1Alpha2ReplicaDown(ctx, store, ref, admission, step)
	}
	observation, err := observeV1Alpha2ReplicaDeployment(ctx, store, admission, admission.Authority())
	if err != nil {
		return err
	}
	if err := validateV1Alpha2ReplicaStepCounts(step, admission.Authority()); err != nil {
		return err
	}
	if observation.DesiredReplicas == step.To {
		return reportV1Alpha2ReplicaScale(ctx, store, admission, admission.Authority(), observation)
	}
	if observation.DesiredReplicas != step.From || step.DeploymentResourceVersion != observation.DeploymentResourceVersion || !step.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("replica step prior count, revision, or expiry changed")
	}
	if replicaStepBlocked(admission) {
		return fmt.Errorf("replica mutation is blocked by authority work")
	}
	if err := applyV1Alpha2ReplicaScale(ctx, store, ref, step); err != nil {
		return err
	}
	observation, err = observeV1Alpha2ReplicaDeployment(ctx, store, admission, admission.Authority())
	if err != nil {
		return err
	}
	return reportV1Alpha2ReplicaScale(ctx, store, admission, admission.Authority(), observation)
}

func replicaStepBlocked(admission V1Alpha2AdmissionSnapshot) bool {
	_, pending := admission.Pending()
	repair, repairing := admission.Repair()
	return pending || hasV1Alpha2Transition(admission.Transition()) || repairing && repair.State == "Closed"
}

func validateV1Alpha2ReplicaStepCounts(step V1Alpha2ReplicaStep, authority V1Alpha2AuthorityView) error {
	if step.Generation != authority.Generation || step.DeploymentUID != authority.Binding.DeploymentUID {
		return fmt.Errorf("replica step authority changed")
	}
	if step.ConfigID == 0 || step.From < 3 || step.From > 64 || step.To < 3 || step.To > 64 || step.From-step.To != 1 && step.To-step.From != 1 {
		return fmt.Errorf("replica step is not one exact replica")
	}
	return nil
}

func reconcileV1Alpha2ReplicaDown(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, admission V1Alpha2AdmissionSnapshot, pinned V1Alpha2ReplicaStep) error {
	// CR and Scale cannot be CASed atomically; the nonce binds fresh Host proof
	// to exact intent, and the Scale resourceVersion makes the write one-shot.
	if err := validateV1Alpha2ReplicaStepCounts(pinned, admission.Authority()); err != nil {
		return err
	}
	observation, err := observeV1Alpha2ReplicaDeployment(ctx, store, admission, admission.Authority())
	if err != nil {
		return err
	}
	if observation.DesiredReplicas == pinned.To {
		return reportV1Alpha2ReplicaScale(ctx, store, admission, admission.Authority(), observation)
	}
	if observation.DesiredReplicas != pinned.From {
		return fmt.Errorf("replica step prior count changed")
	}
	if !replicaObservationStable(observation, pinned.From) {
		return fmt.Errorf("downscale Deployment is not stable")
	}
	if replicaStepBlocked(admission) {
		return fmt.Errorf("replica mutation is blocked by authority work")
	}
	nonce, err := newV1Alpha2ReplicaCheckID()
	if err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, v1alpha2ReplicaCheckWait)
	defer cancel()
	check := V1Alpha2ReplicaCheck{ID: nonce, DeploymentResourceVersion: observation.DeploymentResourceVersion, ExpiresAt: time.Now().Add(v1alpha2ReplicaCheckTTL)}
	withCheck, _, err := BeginV1Alpha2ReplicaCheck(attemptCtx, store, admission, admission.Authority(), check)
	if err != nil {
		return err
	}
	proven, provenObservation, err := waitV1Alpha2ReplicaCheck(attemptCtx, store, ref, withCheck, pinned, check)
	if err != nil {
		_, _, _ = AbandonV1Alpha2ReplicaCheck(ctx, store, withCheck, admission.Authority(), check.ID)
		return err
	}
	if replicaStepBlocked(proven) {
		_, _, _ = AbandonV1Alpha2ReplicaCheck(ctx, store, proven, admission.Authority(), check.ID)
		return fmt.Errorf("replica mutation is blocked by authority work")
	}
	if !replicaObservationStable(provenObservation, pinned.From) || provenObservation.DeploymentResourceVersion != check.DeploymentResourceVersion {
		return fmt.Errorf("downscale Deployment changed before scale")
	}
	currentStep, ok, err := proven.ReplicaStep()
	if err != nil || !ok || currentStep.CheckID != check.ID {
		return fmt.Errorf("downscale proof changed before scale")
	}
	if err := validateV1Alpha2ReplicaProof(proven, pinned, check, time.Now()); err != nil {
		return err
	}
	if err := replicaAttemptBeforeScale(attemptCtx, check, currentStep); err != nil {
		return err
	}
	if err := applyV1Alpha2ReplicaScale(attemptCtx, store, ref, currentStep); err != nil {
		return err
	}
	observation, err = observeV1Alpha2ReplicaDeployment(ctx, store, proven, admission.Authority())
	if err != nil {
		return err
	}
	return reportV1Alpha2ReplicaScale(ctx, store, proven, admission.Authority(), observation)
}

func waitV1Alpha2ReplicaCheck(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, initial V1Alpha2AdmissionSnapshot, pinned V1Alpha2ReplicaStep, check V1Alpha2ReplicaCheck) (V1Alpha2AdmissionSnapshot, V1Alpha2ReplicaObservation, error) {
	ticker := time.NewTicker(v1alpha2ReplicaCheckPoll)
	defer ticker.Stop()
	for {
		current, err := LoadV1Alpha2Admission(ctx, store, ref)
		if err != nil {
			if ctx.Err() != nil {
				return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("fresh replica proof timeout")
			}
			return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, err
		}
		if current.Authority() != initial.Authority() {
			return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("replica proof authority changed")
		}
		currentCheck, ok, err := current.ReplicaCheck()
		if err != nil || !ok || !sameV1Alpha2ReplicaCheck(currentCheck, check) {
			return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("replica check changed during proof wait")
		}
		step, ok, err := current.ReplicaStep()
		if err != nil || !ok || !sameV1Alpha2ReplicaStepIntent(step, pinned) {
			return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("replica step changed during proof wait")
		}
		if step.CheckID != "" {
			if step.CheckID != check.ID || step.DeploymentResourceVersion != check.DeploymentResourceVersion || !check.ExpiresAt.After(time.Now()) || !step.ExpiresAt.After(time.Now()) {
				return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("replica proof does not match the current check")
			}
			observation, err := observeV1Alpha2ReplicaDeployment(ctx, store, current, current.Authority())
			if err != nil {
				return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, err
			}
			if replicaObservationStable(observation, pinned.From) && observation.DeploymentResourceVersion == check.DeploymentResourceVersion {
				final, err := LoadV1Alpha2Admission(ctx, store, ref)
				if err != nil {
					return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, err
				}
				if err := validateV1Alpha2ReplicaProof(final, pinned, check, time.Now()); err != nil {
					return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, err
				}
				finalObservation, err := observeV1Alpha2ReplicaDeployment(ctx, store, final, final.Authority())
				if err != nil {
					return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, err
				}
				return final, finalObservation, nil
			}
		}
		select {
		case <-ctx.Done():
			return V1Alpha2AdmissionSnapshot{}, V1Alpha2ReplicaObservation{}, fmt.Errorf("fresh replica proof timeout")
		case <-ticker.C:
		}
	}
}

func replicaAttemptBeforeScale(ctx context.Context, check V1Alpha2ReplicaCheck, step V1Alpha2ReplicaStep) error {
	now := time.Now()
	deadline, ok := ctx.Deadline()
	if err := ctx.Err(); err != nil || !ok || !deadline.After(now) || !check.ExpiresAt.After(now) || !step.ExpiresAt.After(now) {
		return fmt.Errorf("replica attempt deadline or proof expiry elapsed")
	}
	return nil
}

func validateV1Alpha2ReplicaProof(admission V1Alpha2AdmissionSnapshot, pinned V1Alpha2ReplicaStep, check V1Alpha2ReplicaCheck, now time.Time) error {
	currentCheck, ok, err := admission.ReplicaCheck()
	if err != nil || !ok || !sameV1Alpha2ReplicaCheck(currentCheck, check) || !check.ExpiresAt.After(now) {
		return fmt.Errorf("fresh replica check is missing or expired")
	}
	step, ok, err := admission.ReplicaStep()
	if err != nil || !ok || !sameV1Alpha2ReplicaStepIntent(step, pinned) || step.CheckID != check.ID || step.DeploymentResourceVersion != check.DeploymentResourceVersion || !step.ExpiresAt.After(now) {
		return fmt.Errorf("fresh replica proof does not match the pinned step")
	}
	return nil
}

func sameV1Alpha2ReplicaStepIntent(a, b V1Alpha2ReplicaStep) bool {
	return a.Generation == b.Generation && a.DeploymentUID == b.DeploymentUID && a.ConfigID == b.ConfigID && a.From == b.From && a.To == b.To
}

func sameV1Alpha2ReplicaCheck(a, b V1Alpha2ReplicaCheck) bool {
	return a.ID == b.ID && a.DeploymentResourceVersion == b.DeploymentResourceVersion && a.ExpiresAt.Equal(b.ExpiresAt)
}

func replicaObservationStable(observation V1Alpha2ReplicaObservation, desired int32) bool {
	return observation.DesiredReplicas == desired && observation.ObservedReplicas == desired && observation.ReadyReplicas == desired && observation.UpdatedReplicas == desired && observation.ObservedDeploymentGeneration == observation.DeploymentGeneration
}

func newV1Alpha2ReplicaCheckID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("create replica check nonce: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func applyV1Alpha2ReplicaScale(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, step V1Alpha2ReplicaStep) error {
	name, _ := V1Alpha2GenesisDeploymentName(ref.UID)
	body := map[string]any{
		"apiVersion": "autoscaling/v1",
		"kind":       "Scale",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       ref.Namespace,
			"uid":             step.DeploymentUID,
			"resourceVersion": step.DeploymentResourceVersion,
		},
		"spec": map[string]any{"replicas": step.To},
	}
	var written v1alpha2ReplicaScale
	path := "/apis/apps/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/deployments/" + url.PathEscape(name) + "/scale"
	if err := store.Kube.Put(ctx, path, body, &written); err != nil {
		return fmt.Errorf("apply replica step: %w", err)
	}
	if written.Metadata.UID != step.DeploymentUID || written.Spec.Replicas == nil || *written.Spec.Replicas != step.To || written.Metadata.ResourceVersion == step.DeploymentResourceVersion {
		return fmt.Errorf("replica step response did not prove the expected mutation")
	}
	return nil
}

func reportV1Alpha2ReplicaScale(ctx context.Context, store *V1Alpha2StatusStore, admission V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, observation V1Alpha2ReplicaObservation) error {
	status, err := jsonObject(admission.status.object["status"])
	if err != nil {
		return err
	}
	var replicas int32
	var selector string
	if json.Unmarshal(status["replicas"], &replicas) == nil && json.Unmarshal(status["selector"], &selector) == nil && replicas == observation.ObservedReplicas && selector == observation.Selector {
		return nil
	}
	_, _, err = ReportV1Alpha2ReplicaScale(ctx, store, admission, expected, observation.ObservedReplicas, observation.Selector)
	return err
}
