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
	"sort"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const frozenSourcePreflightPath = "/v1/archive-genesis/frozen-source/preflight"
const bootstrapGenesisPreflightPath = "/v1/archive-genesis/bootstrap/preflight"
const bootstrapGenesisFenceConfirmPath = "/v1/archive-genesis/bootstrap/fence-confirm"

type BootstrapGenesisPreflightRequest struct {
	ClusterName    string `json:"clusterName"`
	ClusterUID     string `json:"clusterUID"`
	StatefulSetUID string `json:"statefulSetUID"`
}

type BootstrapGenesisPreflightResponse struct {
	Binding BootstrapGenesisBinding `json:"binding"`
	Targets []FenceTarget           `json:"targets"`
}

type BootstrapGenesisPreflighter interface {
	BootstrapGenesisPreflight(context.Context, BootstrapGenesisPreflightRequest) (BootstrapGenesisPreflightResponse, error)
}

func (fc *FencingClient) ConfirmBootstrapGenesisFence(ctx context.Context, request FenceRequest) (BootstrapGenesisFenceConfirmation, error) {
	if err := validateURL(fc.URL); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	if err := validateRequest(&request); err != nil || request.BootstrapGenesis == nil {
		return BootstrapGenesisFenceConfirmation{}, errors.Join(err, fmt.Errorf("fencing: bootstrap fence request is required"))
	}
	body, err := json.Marshal(request)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = bootstrapGenesisFenceConfirmPath
	endpoint.RawPath = ""
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+token)
	response, err := cloneClient(fc.Client).Do(httpRequest)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: bootstrap fence confirmation request failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusServiceUnavailable {
		return BootstrapGenesisFenceConfirmation{}, ErrFencePending
	}
	if response.StatusCode != http.StatusOK {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: bootstrap fence confirmation HTTP %d", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(responseBody) > maxResponseBytes {
		return BootstrapGenesisFenceConfirmation{}, errors.Join(err, fmt.Errorf("fencing: invalid bootstrap fence confirmation response size"))
	}
	var confirmation BootstrapGenesisFenceConfirmation
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&confirmation); err != nil || decoder.Decode(new(any)) != io.EOF {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: invalid bootstrap fence confirmation response")
	}
	if hashJSON(confirmation.Request) != hashJSON(request) || confirmation.Proof.IndependentV2 == nil ||
		!strings.HasPrefix(confirmation.Proof.ProofID, "independent-v2-") {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: inconsistent bootstrap fence confirmation")
	}
	if err := validateFenceProof(confirmation.Request, &confirmation.Proof); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	return confirmation, nil
}

func (e *FencingExecutor) handleBootstrapGenesisFenceConfirmation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != bootstrapGenesisFenceConfirmPath || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}
	var request FenceRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || validateRequest(&request) != nil || request.BootstrapGenesis == nil {
		http.Error(w, "invalid bootstrap fence confirmation request", http.StatusBadRequest)
		return
	}
	confirmation, err := e.confirmBootstrapGenesisFence(r.Context(), request)
	if err != nil {
		if transientFenceAuthority(err) || errors.Is(err, fencing.ErrPending) {
			http.Error(w, "bootstrap fence confirmation pending", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "bootstrap fence confirmation rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(confirmation)
}

func (e *FencingExecutor) confirmBootstrapGenesisFence(ctx context.Context, request FenceRequest) (BootstrapGenesisFenceConfirmation, error) {
	if e == nil || e.Kube == nil || e.AuthorityState == nil || e.ArchiveBucket == nil || !e.RequireMutationJournal || request.Namespace != e.Kube.Namespace || request.BootstrapGenesis == nil {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("fencing: canonical bootstrap fence authority is unavailable")
	}
	if err := validateRequest(&request); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, request.SourceClusterID)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	requestDigest, err := requestHash(&request)
	if err != nil || state.StatefulSetUID != request.StatefulSetUID || state.Ledger.SourceClusterID != request.SourceClusterID ||
		!state.Ledger.RequireMutationJournal || state.Ledger.ClosedBy != request.OperationID || state.Ledger.ClosedHash != requestDigest {
		return BootstrapGenesisFenceConfirmation{}, errors.Join(err, fmt.Errorf("fencing: bootstrap fence does not match the closed canonical CONTROL root"))
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil || bindingUID != request.BindingUID {
		return BootstrapGenesisFenceConfirmation{}, errors.Join(err, fmt.Errorf("fencing: bootstrap fence workload binding differs from CONTROL root"))
	}
	fenceRequest := ArchiveForkGenesisRequest{StatefulSetUID: request.StatefulSetUID, SourceFenceOperationID: request.OperationID}
	fence, err := e.readCanonicalSourceFence(ctx, fenceRequest, request.SourceClusterID, bindingUID, state)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	if hashJSON(fence.Spec) != hashJSON(request) || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return BootstrapGenesisFenceConfirmation{}, fencing.ErrPending
	}
	proof := fence.Status.Proof
	if err := validateFenceProof(request, proof); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	if err := validateIndependentV2(request, proof.IndependentV2); err != nil || proof.IndependentV2.LedgerHash != e.ledgerHash(state) {
		return BootstrapGenesisFenceConfirmation{}, errors.Join(err, fmt.Errorf("fencing: bootstrap proof is not bound to the closed CONTROL root"))
	}
	if err := verifyIndependentProofCoverage(state, request, proof.IndependentV2); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	// This is a historical proof lookup, not a new preflight. The same
	// candidate/cohort was revalidated before close and immediately before the
	// executor persisted this proof. After proof, source Pods and the live
	// StatefulSet environment may legitimately be replaced by the successor.
	return BootstrapGenesisFenceConfirmation{Request: fence.Spec, Proof: *proof}, nil
}

func (fc *FencingClient) BootstrapGenesisPreflight(ctx context.Context, request BootstrapGenesisPreflightRequest) (BootstrapGenesisPreflightResponse, error) {
	if err := validateURL(fc.URL); err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	if request.ClusterName == "" || request.ClusterUID == "" || request.StatefulSetUID == "" {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: automatic bootstrap identity is required")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = bootstrapGenesisPreflightPath
	endpoint.RawPath = ""
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+token)
	response, err := cloneClient(fc.Client).Do(httpRequest)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: automatic bootstrap preflight request failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusServiceUnavailable {
		return BootstrapGenesisPreflightResponse{}, ErrFencePending
	}
	if response.StatusCode != http.StatusOK {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: automatic bootstrap preflight HTTP %d", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(responseBody) > maxResponseBytes {
		return BootstrapGenesisPreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: invalid automatic bootstrap preflight response size"))
	}
	var result BootstrapGenesisPreflightResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || decoder.Decode(new(any)) != io.EOF {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: invalid automatic bootstrap preflight response")
	}
	if result.Binding.Version != 1 || result.Binding.ClusterName != request.ClusterName || result.Binding.ClusterUID != request.ClusterUID || !validEvidenceDigest(result.Binding.CandidateDigest) ||
		len(result.Targets) == 0 || len(result.Targets) > fenceTargetCapacity {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: inconsistent automatic bootstrap preflight response")
	}
	for _, target := range result.Targets {
		if target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" || target.NodeName == "" || target.NodeUID == "" || target.ContainerID == "" || target.ImageID == "" {
			return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: incomplete automatic bootstrap target")
		}
	}
	return result, nil
}

func (e *FencingExecutor) handleBootstrapGenesisPreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != bootstrapGenesisPreflightPath || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}
	var request BootstrapGenesisPreflightRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || request.ClusterName == "" || request.ClusterUID == "" || request.StatefulSetUID == "" {
		http.Error(w, "invalid automatic bootstrap preflight request", http.StatusBadRequest)
		return
	}
	result, err := e.bootstrapGenesisPreflight(r.Context(), request)
	if err != nil {
		if transientFenceAuthority(err) || errors.Is(err, fencing.ErrPending) {
			http.Error(w, "automatic bootstrap preflight pending", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "automatic bootstrap preflight rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (e *FencingExecutor) bootstrapGenesisPreflight(ctx context.Context, request BootstrapGenesisPreflightRequest) (BootstrapGenesisPreflightResponse, error) {
	if e == nil || e.Kube == nil || e.AuthorityState == nil || !e.RequireMutationJournal {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: canonical bootstrap authority is unavailable")
	}
	var cluster ClusterResource
	clusterPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizaclusters/" + url.PathEscape(request.ClusterName)
	if err := e.Kube.Get(ctx, clusterPath, &cluster); err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	if str(cluster.Metadata["name"]) != request.ClusterName || str(cluster.Metadata["namespace"]) != e.Kube.Namespace || str(cluster.Metadata["uid"]) != request.ClusterUID || cluster.Metadata["deletionTimestamp"] != nil {
		return BootstrapGenesisPreflightResponse{}, fmt.Errorf("fencing: automatic bootstrap cluster identity changed")
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, cluster.Spec.AdoptClusterID)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	candidate, targets, err := e.deriveBootstrapGenesisCandidate(ctx, cluster, state, request.StatefulSetUID, false, "", nil)
	if err != nil {
		return BootstrapGenesisPreflightResponse{}, err
	}
	return BootstrapGenesisPreflightResponse{Binding: BootstrapGenesisBinding{Version: 1, CandidateDigest: hashJSON(candidate), ClusterName: request.ClusterName, ClusterUID: request.ClusterUID}, Targets: targets}, nil
}

func (e *FencingExecutor) deriveBootstrapGenesisCandidate(ctx context.Context, cluster ClusterResource, state fenceLedger, statefulSetUID string, closed bool, operationID string, expectedTargets []FenceTarget) (bootstrapGenesisCandidate, []FenceTarget, error) {
	var zero bootstrapGenesisCandidate
	uid, sourceID := str(cluster.Metadata["uid"]), cluster.Spec.AdoptClusterID
	if uid == "" || sourceID == "" || cluster.Spec.StatefulSet == "" || cluster.Spec.LogicalID == "" || !cluster.Spec.Automatic || !cluster.Spec.Bootstrap || cluster.Spec.Policy == nil || cluster.Spec.Policy.AllowDataLoss == false {
		return zero, nil, fmt.Errorf("fencing: bootstrap policy is not explicitly eligible for async data loss")
	}
	if err := cluster.Spec.Policy.validate(); err != nil {
		return zero, nil, err
	}
	if cluster.Status.ActiveClusterID != "" && cluster.Status.ActiveClusterID != sourceID || cluster.Status.ActiveRecovery != "" {
		return zero, nil, fmt.Errorf("fencing: bootstrap cluster has a conflicting active generation or recovery")
	}
	if len(state.Ledger.ClosedNodes) != 0 || closed && (state.Ledger.ClosedBy != operationID || operationID == "") || !closed && state.Ledger.ClosedBy != "" {
		return zero, nil, fmt.Errorf("fencing: bootstrap source has prior closure history or a different close operation")
	}
	if state.StatefulSetUID != statefulSetUID || state.Ledger.SourceClusterID != sourceID || state.Ledger.ClosedBy != "" && (!closed || state.Ledger.ClosedBy != operationID) || !state.Ledger.RequireRuntimeBinding || !state.Ledger.RequireMutationJournal {
		return zero, nil, fmt.Errorf("fencing: bootstrap source root identity or open state differs")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return zero, nil, err
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(cluster.Spec.StatefulSet), &sts); err != nil {
		return zero, nil, err
	}
	if str(nested(sts, "metadata", "uid")) != statefulSetUID || str(nested(sts, "metadata", "namespace")) != e.Kube.Namespace || nested(sts, "metadata", "deletionTimestamp") != nil {
		return zero, nil, fmt.Errorf("fencing: bootstrap StatefulSet identity changed")
	}
	if cluster.Spec.VoterCount < 3 || cluster.Spec.VoterCount > fenceTargetCapacity || int(number(nested(sts, "spec", "replicas"))) != cluster.Spec.VoterCount || number(nested(sts, "spec", "ordinals", "start")) != 0 {
		return zero, nil, fmt.Errorf("fencing: bootstrap StatefulSet voter contract differs")
	}
	ctr, err := container(sts, cluster.Spec.Container)
	if err != nil {
		return zero, nil, err
	}
	if !bootstrapTemplateInstalled(ctr, &cluster) {
		return zero, nil, fmt.Errorf("fencing: bootstrap credential template is not installed")
	}
	secretName := bootstrapSecretName(&cluster)
	for _, raw := range list(ctr["env"]) {
		entry := asObject(raw)
		if str(entry["name"]) != "RHIZA_CLUSTER_MEMBERS" {
			continue
		}
		if str(nested(entry, "valueFrom", "secretKeyRef", "name")) != secretName || str(nested(entry, "valueFrom", "secretKeyRef", "key")) != "members" {
			return zero, nil, fmt.Errorf("fencing: bootstrap membership must reference the immutable members key")
		}
	}
	var credentials object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", secretName), &credentials); err != nil {
		return zero, nil, fmt.Errorf("fencing: bootstrap credential Secret is unavailable: %w", err)
	}
	if credentials["immutable"] != true || str(nested(credentials, "metadata", "name")) != secretName ||
		str(nested(credentials, "metadata", "namespace")) != e.Kube.Namespace ||
		str(nested(credentials, "metadata", "annotations", ownerAnnotation)) != uid+":"+cluster.Spec.LogicalID {
		return zero, nil, fmt.Errorf("fencing: bootstrap credential Secret identity or immutability differs")
	}
	env, err := e.controller().environment(ctx, ctr)
	if err != nil {
		return zero, nil, err
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return zero, nil, err
	}
	if err := validateClusterRuntime(&cluster, ctr, env); err != nil {
		return zero, nil, err
	}
	if err := e.archiveStoreConfigMatches(env); err != nil {
		return zero, nil, err
	}
	if env["RHIZA_CLUSTER_ID"] != sourceID || env["RHIZA_OBJSTORE_DURABILITY"] != "async" || env["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
		return zero, nil, fmt.Errorf("fencing: bootstrap runtime identity, async durability, or reconfiguration policy differs")
	}
	namespace := struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}{}
	if err := e.Kube.Get(ctx, "/api/v1/namespaces/"+url.PathEscape(e.Kube.Namespace), &namespace); err != nil {
		return zero, nil, err
	}
	if namespace.Metadata.UID == "" {
		return zero, nil, fmt.Errorf("fencing: Kubernetes namespace UID is unavailable")
	}
	pods, err := e.controller().ownedPods(ctx, statefulSetUID)
	if err != nil {
		return zero, nil, err
	}
	if len(pods) != cluster.Spec.VoterCount {
		return zero, nil, fencing.ErrPending
	}
	podsByName := make(map[string]object, len(pods))
	for _, pod := range pods {
		name := str(nested(pod, "metadata", "name"))
		if name == "" || podsByName[name] != nil {
			return zero, nil, fmt.Errorf("fencing: bootstrap Pod names are ambiguous")
		}
		podsByName[name] = pod
	}
	targets := make([]FenceTarget, 0, len(pods))
	seenPodUID := map[string]bool{}
	expectedByPodUID := make(map[string]FenceTarget, len(expectedTargets))
	for _, target := range expectedTargets {
		if target.PodUID == "" || expectedByPodUID[target.PodUID].PodUID != "" {
			return zero, nil, fmt.Errorf("fencing: bootstrap expected target inventory is ambiguous")
		}
		expectedByPodUID[target.PodUID] = target
	}
	nodeUIDs := map[string]string{}
	for ordinal := range cluster.Spec.VoterCount {
		podName := fmt.Sprintf("%s-%d", cluster.Spec.StatefulSet, ordinal)
		pod, exists := podsByName[podName]
		if !exists {
			return zero, nil, fencing.ErrPending
		}
		podUID := str(nested(pod, "metadata", "uid"))
		if podUID == "" || seenPodUID[podUID] ||
			str(nested(pod, "metadata", "namespace")) != e.Kube.Namespace || nested(pod, "metadata", "deletionTimestamp") != nil {
			return zero, nil, fencing.ErrPending
		}
		seenPodUID[podUID] = true
		owners := list(nested(pod, "metadata", "ownerReferences"))
		owner := object(nil)
		if len(owners) == 1 {
			owner = asObject(owners[0])
		}
		if owner == nil || str(owner["apiVersion"]) != "apps/v1" || str(owner["kind"]) != "StatefulSet" || str(owner["name"]) != cluster.Spec.StatefulSet || str(owner["uid"]) != statefulSetUID || owner["controller"] != true {
			return zero, nil, fmt.Errorf("fencing: bootstrap Pod ownership differs")
		}
		if err := validateSupervisedPod(pod); err != nil {
			return zero, nil, fencing.ErrPending
		}
		for _, rawCondition := range list(nested(pod, "status", "conditions")) {
			condition := asObject(rawCondition)
			if condition == nil {
				return zero, nil, fencing.ErrPending
			}
			if str(condition["type"]) == "Ready" && str(condition["status"]) == "True" {
				return zero, nil, fmt.Errorf("fencing: bootstrap preflight requires the initial genesis wait to remain unready")
			}
		}
		var matching *fencing.Run
		if closed {
			target, ok := expectedByPodUID[podUID]
			if !ok || target.Pod != podName || target.NodeID != podName || target.WALIdentity == "" || target.NodeName == "" || target.NodeUID == "" || target.ContainerID == "" || target.ImageID == "" {
				return zero, nil, fmt.Errorf("fencing: closed bootstrap target does not match the captured Pod")
			}
			for runID, run := range state.Ledger.Runs {
				if runID != run.Identity.RunID || run.Identity.PodUID != podUID || run.Identity.NodeID != podName || run.Identity.SourceClusterID != sourceID || run.Identity.WALIdentity != target.WALIdentity || run.Runtime == nil {
					continue
				}
				runtime := run.Runtime
				if runtime.NodeName != target.NodeName || runtime.NodeUID != target.NodeUID || runtime.ContainerID != target.ContainerID || runtime.ImageID != target.ImageID {
					continue
				}
				if matching != nil {
					return zero, nil, fmt.Errorf("fencing: closed bootstrap target maps to ambiguous CONTROL runs")
				}
				copy := run
				matching = &copy
			}
		} else {
			statuses := list(nested(pod, "status", "containerStatuses"))
			if len(statuses) != 1 {
				return zero, nil, fencing.ErrPending
			}
			status := asObject(statuses[0])
			if status == nil {
				return zero, nil, fencing.ErrPending
			}
			liveContainerID, liveImageID := str(status["containerID"]), str(status["imageID"])
			for runID, run := range state.Ledger.Runs {
				if runID != run.Identity.RunID || run.Identity.PodUID != podUID || run.Identity.NodeID != podName || run.Identity.SourceClusterID != sourceID || run.Runtime == nil || run.Stop != nil {
					continue
				}
				if run.Runtime.ContainerID != liveContainerID || run.Runtime.ImageID != liveImageID {
					continue
				}
				if matching != nil {
					return zero, nil, fmt.Errorf("fencing: live bootstrap Pod maps to ambiguous CONTROL runs")
				}
				copy := run
				matching = &copy
			}
		}
		if matching == nil || matching.Runtime == nil || !closed && matching.Stop != nil || matching.MutationProducerUID == "" || matching.MutationProfile != fencing.MutationHeldExecutionProfile || !validEvidenceDigest(matching.MutationScopeHash) || matching.Identity.WALIdentity == "" {
			return zero, nil, fencing.ErrPending
		}
		runtime := *matching.Runtime
		if err := runtime.Validate(matching.Identity); err != nil || e.RuntimeAgent == nil || runtime.AuthorityID != e.RuntimeAgent.AuthorityID || runtime.BindingUID != uid || runtime.NamespaceUID != namespace.Metadata.UID || runtime.StatefulSetUID != statefulSetUID || runtime.SourceClusterID != sourceID || runtime.NodeName != str(nested(pod, "spec", "nodeName")) {
			return zero, nil, fmt.Errorf("fencing: bootstrap Pod differs from its CONTROL runtime enrollment")
		}
		nodeUID, exists := nodeUIDs[runtime.NodeName]
		if !exists {
			var node struct {
				Metadata struct {
					UID string `json:"uid"`
				} `json:"metadata"`
			}
			if err := e.Kube.Get(ctx, "/api/v1/nodes/"+url.PathEscape(runtime.NodeName), &node); err != nil {
				return zero, nil, err
			}
			nodeUID = node.Metadata.UID
			nodeUIDs[runtime.NodeName] = nodeUID
		}
		if nodeUID == "" || nodeUID != runtime.NodeUID {
			return zero, nil, fmt.Errorf("fencing: bootstrap node identity differs from CONTROL runtime enrollment")
		}
		if !closed {
			statuses := list(nested(pod, "status", "containerStatuses"))
			if len(statuses) != 1 {
				return zero, nil, fencing.ErrPending
			}
			status := asObject(statuses[0])
			if status == nil || str(status["containerID"]) != runtime.ContainerID || str(status["imageID"]) != runtime.ImageID {
				return zero, nil, fencing.ErrPending
			}
		}
		targets = append(targets, FenceTarget{NodeID: matching.Identity.NodeID, Pod: podName, PodUID: podUID, WALIdentity: matching.Identity.WALIdentity,
			NodeName: runtime.NodeName, NodeUID: runtime.NodeUID, ContainerID: runtime.ContainerID, ImageID: runtime.ImageID})
	}
	if closed && len(expectedByPodUID) != len(pods) {
		return zero, nil, fmt.Errorf("fencing: closed bootstrap target inventory changed")
	}
	if len(state.Ledger.Runs) == 0 || len(state.Ledger.Runs) > fenceTargetCapacity {
		return zero, nil, fmt.Errorf("fencing: bootstrap CONTROL runtime history is empty or exceeds capacity")
	}
	runs := make([]bootstrapGenesisRunBinding, 0, len(state.Ledger.Runs))
	for runID, run := range state.Ledger.Runs {
		if runID != run.Identity.RunID || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil || run.MutationProducerUID == "" || run.MutationProfile != fencing.MutationHeldExecutionProfile || !validEvidenceDigest(run.MutationScopeHash) || run.Identity.WALIdentity == "" || run.Runtime.BindingUID != uid || run.Runtime.NamespaceUID != namespace.Metadata.UID || run.Runtime.StatefulSetUID != statefulSetUID || run.Runtime.SourceClusterID != sourceID {
			return zero, nil, fmt.Errorf("fencing: bootstrap CONTROL runtime history is incomplete")
		}
		runs = append(runs, bootstrapGenesisRunBinding{RunID: runID, Identity: run.Identity, Runtime: *run.Runtime, MutationProducerUID: run.MutationProducerUID,
			MutationScopeHash: run.MutationScopeHash, MutationProfile: run.MutationProfile, MutationStageIncarnation: run.MutationStageIncarnation})
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].RunID < runs[j].RunID })
	sort.Slice(targets, func(i, j int) bool { return targets[i].NodeID < targets[j].NodeID })
	storeScope, err := e.storageScope(ctx, sts, sourceID)
	if err != nil {
		return zero, nil, err
	}
	archiveScope := fencing.ArchiveScope{Provider: storeScope.Provider, Bucket: storeScope.Bucket, Prefix: storeScope.Prefix, ClusterID: sourceID}
	scopeHash, err := archiveScope.Hash()
	if err != nil || storeScope.Provider != "gcs" || state.Ledger.ArchiveScopeHash != scopeHash {
		return zero, nil, errors.Join(err, fmt.Errorf("fencing: bootstrap archive scope differs from CONTROL authority"))
	}
	if err := e.validatePendingFrozenEmptyGenesis(ctx, state, archiveScope, statefulSetUID, sourceID, operationID, ""); err != nil {
		return zero, nil, err
	}
	rootDigest := hashJSON([]any{"bootstrap-genesis-control-v1", state.StatefulSetUID, state.Ledger.SourceClusterID, state.Ledger.RequireRuntimeBinding,
		state.Ledger.RequireMutationJournal, state.Ledger.ArchiveGenesisState, state.Ledger.ArchiveScopeHash, state.Ledger.ArchiveGenesisRef, runs})
	candidate := bootstrapGenesisCandidate{Version: 1, ClusterName: str(cluster.Metadata["name"]), ClusterUID: uid, ClusterSpecDigest: hashJSON(cluster.Spec),
		StatefulSetUID: statefulSetUID, StatefulSetDigest: hashJSON(nested(sts, "spec")), NamespaceUID: namespace.Metadata.UID, SourceClusterID: sourceID,
		ArchiveScope:    fencing.ArchiveScope{Provider: storeScope.Provider, Bucket: storeScope.Bucket, Prefix: storeScope.Prefix, ClusterID: sourceID},
		AuthorityDigest: rootDigest, Targets: targets}
	return candidate, targets, nil
}

func (e *FencingExecutor) archiveStoreConfigMatches(env map[string]string) error {
	if e == nil || e.ArchiveBucket == nil || e.ArchiveStoreIdentity == nil {
		return fmt.Errorf("fencing: configured archive store identity is unavailable")
	}
	actual := StoreIdentity(env)
	for _, key := range []string{"RHIZA_OBJSTORE_PROVIDER", "RHIZA_OBJSTORE_ENDPOINT", "RHIZA_OBJSTORE_BUCKET", "RHIZA_OBJSTORE_PREFIX", "RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT", "azure_connection_string_digest"} {
		configured, ok := e.ArchiveStoreIdentity[key]
		if !ok || configured != actual[key] {
			return fmt.Errorf("fencing: workload object store differs from configured archive store")
		}
	}
	return nil
}

func (e *FencingExecutor) bootstrapGenesisFenceCandidate(ctx context.Context, request FenceRequest, state fenceLedger, closed bool) (bootstrapGenesisCandidate, []FenceTarget, error) {
	if request.BootstrapGenesis == nil {
		return bootstrapGenesisCandidate{}, nil, fmt.Errorf("fencing: bootstrap genesis binding is missing")
	}
	binding := request.BootstrapGenesis
	if binding.ClusterName == "" || binding.ClusterUID == "" || request.Namespace != e.Kube.Namespace {
		return bootstrapGenesisCandidate{}, nil, fmt.Errorf("fencing: bootstrap cluster lookup identity is incomplete")
	}
	var cluster ClusterResource
	clusterPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizaclusters/" + url.PathEscape(binding.ClusterName)
	if err := e.Kube.Get(ctx, clusterPath, &cluster); err != nil {
		return bootstrapGenesisCandidate{}, nil, err
	}
	if str(cluster.Metadata["name"]) != binding.ClusterName || str(cluster.Metadata["namespace"]) != e.Kube.Namespace || str(cluster.Metadata["uid"]) != binding.ClusterUID || cluster.Metadata["deletionTimestamp"] != nil || cluster.Spec.LogicalID != request.LogicalID || cluster.Spec.StatefulSet != request.StatefulSet || cluster.Spec.AdoptClusterID != request.SourceClusterID {
		return bootstrapGenesisCandidate{}, nil, fmt.Errorf("fencing: automatic bootstrap Cluster binding changed")
	}
	if closed {
		expectedHash, err := requestHash(&request)
		if err != nil || state.Ledger.ClosedBy != request.OperationID || state.Ledger.ClosedHash != expectedHash {
			return bootstrapGenesisCandidate{}, nil, errors.Join(err, fmt.Errorf("fencing: bootstrap candidate is not bound to the closed request"))
		}
	}
	return e.deriveBootstrapGenesisCandidate(ctx, cluster, state, request.StatefulSetUID, closed, request.OperationID, func() []FenceTarget {
		if closed {
			return request.Targets
		}
		return nil
	}())
}

type bootstrapGenesisCandidate struct {
	Version           int
	ClusterName       string
	ClusterUID        string
	ClusterSpecDigest string
	StatefulSetUID    string
	StatefulSetDigest string
	NamespaceUID      string
	SourceClusterID   string
	ArchiveScope      fencing.ArchiveScope
	AuthorityDigest   string
	Targets           []FenceTarget
}

type bootstrapGenesisRunBinding struct {
	RunID                    string
	Identity                 fencing.Identity
	Runtime                  fencing.RuntimeBinding
	MutationProducerUID      string
	MutationScopeHash        string
	MutationProfile          string
	MutationStageIncarnation string
}

type FrozenSourcePreflightRequest struct {
	RecoveryName string `json:"recoveryName"`
	RecoveryUID  string `json:"recoveryUID"`
}

type FrozenSourcePreflightResponse struct {
	Identity                      recovery.FrozenSourceIdentity `json:"identity"`
	ClosureReady                  bool                          `json:"closureReady"`
	ProofDigest                   string                        `json:"proofDigest,omitempty"`
	ClosedRootDigest              string                        `json:"closedRootDigest,omitempty"`
	EmptyGenesisEligible          bool                          `json:"emptyGenesisEligible,omitempty"`
	ManualMembershipBindingDigest string                        `json:"manualMembershipBindingDigest,omitempty"`
}

type FrozenSourcePreflighter interface {
	FrozenSourcePreflight(context.Context, FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error)
}

func (fc *FencingClient) FrozenSourcePreflight(ctx context.Context, request FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error) {
	if err := validateURL(fc.URL); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if request.RecoveryName == "" || request.RecoveryUID == "" {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source recovery identity is required")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = frozenSourcePreflightPath
	endpoint.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cloneClient(fc.Client).Do(req)
	if err != nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source preflight request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusServiceUnavailable {
		return FrozenSourcePreflightResponse{}, ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source preflight HTTP %d", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(responseBody) > maxResponseBytes {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: invalid frozen source preflight response size"))
	}
	var result FrozenSourcePreflightResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || decoder.Decode(new(any)) != io.EOF {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: invalid frozen source preflight response")
	}
	if _, err := recovery.FrozenTargetID(result.Identity); err != nil ||
		(result.ClosureReady && (!validEvidenceDigest(result.ProofDigest) || !validEvidenceDigest(result.ClosedRootDigest))) ||
		(!result.ClosureReady && (result.ProofDigest != "" || result.ClosedRootDigest != "")) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: inconsistent frozen source preflight response")
	}
	return result, nil
}

func (e *FencingExecutor) handleFrozenSourcePreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != frozenSourcePreflightPath || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}
	var request FrozenSourcePreflightRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || request.RecoveryName == "" || request.RecoveryUID == "" {
		http.Error(w, "invalid frozen source preflight request", http.StatusBadRequest)
		return
	}
	result, err := e.frozenSourcePreflight(r.Context(), request)
	if err != nil {
		if transientFenceAuthority(err) || errors.Is(err, fencing.ErrPending) {
			http.Error(w, "frozen source preflight pending", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "frozen source preflight rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (e *FencingExecutor) frozenSourcePreflight(ctx context.Context, request FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error) {
	if e == nil || e.Kube == nil || e.AuthorityState == nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: canonical frozen source authority is unavailable")
	}
	var persisted Resource
	recoveryPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizarecoveries/" + url.PathEscape(request.RecoveryName)
	if err := e.Kube.Get(ctx, recoveryPath, &persisted); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if str(persisted.Metadata["name"]) != request.RecoveryName || str(persisted.Metadata["uid"]) != request.RecoveryUID ||
		str(persisted.Metadata["namespace"]) != e.Kube.Namespace || persisted.Metadata["deletionTimestamp"] != nil ||
		persisted.Status.RecoveryMode != RecoveryModeFrozenSource || persisted.Status.StatefulSetUID == "" ||
		persisted.Status.StatefulSetUID != persisted.Spec.Fence.StatefulSetUID || persisted.Spec.Fence.RecoveryID != persisted.Spec.RecoveryID ||
		persisted.Spec.Fence.ClusterID != persisted.Spec.SourceClusterID || persisted.Status.Source != persisted.Spec.SourceClusterID ||
		persisted.Spec.RecoveryID == "" || persisted.Spec.SourceClusterID == "" || persisted.Status.RecoveryID != persisted.Spec.RecoveryID ||
		persisted.Status.SpecHash != immutableSpecHash(persisted.Spec) ||
		(persisted.Status.FrozenEmptyGenesisPending && (persisted.Status.TargetBaseKind != "empty" || !persisted.Spec.AllowDataLoss || persisted.Status.SourceDurability != "async")) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: persisted frozen recovery identity is incomplete or changed")
	}
	uid, sourceID, operationID := persisted.Status.StatefulSetUID, persisted.Spec.SourceClusterID, persisted.Spec.RecoveryID
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, sourceID)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != sourceID || !state.Ledger.RequireMutationJournal {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: canonical source root identity or profile differs")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	sts, err := e.archiveForkStatefulSet(ctx, uid)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	storeScope, err := e.storageScope(ctx, sts, sourceID)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	archiveScope := fencing.ArchiveScope{Provider: storeScope.Provider, Bucket: storeScope.Bucket, Prefix: storeScope.Prefix, ClusterID: sourceID}
	scopeHash, err := archiveScope.Hash()
	if err != nil || storeScope.Provider != "gcs" || state.Ledger.ArchiveScopeHash != scopeHash {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: canonical source archive scope differs"))
	}
	result := FrozenSourcePreflightResponse{Identity: recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: uid}}
	if persisted.Status.TargetBaseKind == "empty" {
		closedDigest := ""
		if state.Ledger.ClosedBy != "" {
			if state.Ledger.ClosedBy != operationID {
				return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source root is closed by another operation")
			}
			closedDigest = e.ledgerHash(state)
		}
		if err := e.validatePendingFrozenEmptyGenesis(ctx, state, archiveScope, uid, sourceID, operationID, closedDigest); err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
		result.EmptyGenesisEligible = true
	}
	if state.Ledger.ClosedBy == "" {
		return result, nil
	}
	if state.Ledger.ClosedBy != operationID {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source root is closed by another operation")
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	fenceRequest := ArchiveForkGenesisRequest{StatefulSetUID: uid, SourceFenceOperationID: operationID}
	fence, err := e.readCanonicalSourceFence(ctx, fenceRequest, sourceID, bindingUID, state)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if fence.Spec.OperationID != operationID || fence.Spec.SourceClusterID != sourceID || fence.Spec.StatefulSetUID != uid || fence.Spec.Scope != ScopeGeneration || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return FrozenSourcePreflightResponse{}, fencing.ErrPending
	}
	requestDigest, err := requestHash(&fence.Spec)
	if err != nil || state.Ledger.ClosedHash != requestDigest {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source fence does not match the closed root")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	proof := fence.Status.Proof.IndependentV2
	if err := validateIndependentV2(fence.Spec, proof); err != nil || proof.LedgerHash != e.ledgerHash(state) || proof.StorageScope != storeScope {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: independent proof is not bound to the source root and scope"))
	}
	if err := verifyIndependentProofCoverage(state, fence.Spec, proof); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	currentUIDs, err := e.currentInventory(ctx, uid, nil)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	sort.Strings(currentUIDs)
	proofUIDs := append([]string(nil), proof.CurrentPodUIDs...)
	sort.Strings(proofUIDs)
	if !sameStrings(currentUIDs, proofUIDs) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source Pod inventory changed since independent proof")
	}
	if persisted.Spec.AllowMissingSourceMembership {
		ctr, err := container(sts, persisted.Spec.Container)
		if err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
		env, err := e.controller().environment(ctx, ctr)
		if err != nil || noPVC(sts, ctr, env) != nil || e.controller().storeConfigMatches(env) != nil {
			return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: manual source workload configuration is unavailable or unsafe"))
		}
		manualDigest, missing, err := e.manualMissingSourceMembershipBinding(ctx, persisted, env, archiveScope, state, fence)
		if err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
		if missing {
			result.ManualMembershipBindingDigest = manualDigest
		}
	}
	result.ClosureReady = true
	result.ProofDigest = hashJSON(fence.Status.Proof)
	result.ClosedRootDigest = e.ledgerHash(state)
	if persisted.Status.TargetBaseKind == "empty" {
		if err := e.validatePendingFrozenEmptyGenesis(ctx, state, archiveScope, uid, sourceID, operationID, result.ClosedRootDigest); err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
	}
	return result, nil
}

// manualMissingSourceMembershipBinding derives the opt-in binding only from a
// definite DATA NotFound and the complete independently verified closed fence.
// The resulting digest is not a source membership receipt; it only authorizes
// the controller/fencer to use the declared cohort as target input.
func (e *FencingExecutor) manualMissingSourceMembershipBinding(ctx context.Context, stored Resource, env map[string]string, scope fencing.ArchiveScope, state fenceLedger, fence fenceResource) (string, bool, error) {
	reconfiguration, reconfigErr := reconfigurationEnabled(env)
	if !stored.Spec.AllowMissingSourceMembership || stored.Spec.RecoveryID == "" ||
		!stored.Spec.AllowDataLoss || stored.Status.SourceDurability != "async" || env["RHIZA_OBJSTORE_DURABILITY"] != "async" || reconfigErr != nil || !reconfiguration ||
		str(nested(stored.Metadata, "annotations", automaticOwner)) != "" || str(nested(stored.Metadata, "annotations", automaticLogical)) != "" ||
		stored.Spec.Membership != nil {
		return "", false, errors.Join(reconfigErr, fmt.Errorf("fencing: manual missing-membership opt-in is not eligible"))
	}
	if e.ArchiveBucket == nil {
		return "", false, fmt.Errorf("fencing: source membership store is unavailable")
	}
	key := path.Join(scope.Prefix, "voters/membership.json")
	reader, err := e.ArchiveBucket.Get(ctx, key)
	if err == nil {
		if closeErr := reader.Close(); closeErr != nil {
			return "", false, closeErr
		}
		return "", false, nil
	}
	if !e.ArchiveBucket.IsObjNotFoundErr(err) {
		return "", false, fmt.Errorf("fencing: source membership cannot be verified")
	}
	if state.Ledger.ClosedBy != stored.Spec.RecoveryID || state.Ledger.ClosedHash == "" ||
		fence.Spec.OperationID != stored.Spec.RecoveryID || fence.Spec.SourceClusterID != stored.Spec.SourceClusterID ||
		fence.Spec.StatefulSetUID != stored.Status.StatefulSetUID || fence.Spec.Scope != ScopeGeneration ||
		fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return "", false, fmt.Errorf("fencing: manual missing-membership cohort is not bound to its closed CONTROL fence")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return "", false, err
	}
	if err := verifyIndependentProofCoverage(state, fence.Spec, fence.Status.Proof.IndependentV2); err != nil {
		return "", false, err
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(members) < 3 || env["RHIZA_CLUSTER_ID"] != stored.Spec.SourceClusterID {
		return "", false, errors.Join(err, fmt.Errorf("fencing: manual source cohort is incomplete"))
	}
	if len(fence.Spec.Targets) != len(members) {
		return "", false, fmt.Errorf("fencing: manual source cohort differs from the closed CONTROL fence")
	}
	targets := make(map[string]FenceTarget, len(fence.Spec.Targets))
	for _, target := range fence.Spec.Targets {
		if target.NodeID == "" || target.Pod == "" || target.NodeID != target.Pod || target.PodUID == "" || target.WALIdentity == "" || targets[target.NodeID].NodeID != "" {
			return "", false, fmt.Errorf("fencing: manual closed source cohort is incomplete or ambiguous")
		}
		targets[target.NodeID] = target
	}
	for _, member := range members {
		if _, ok := targets[string(member.ID)]; !ok {
			return "", false, fmt.Errorf("fencing: manual declared voter is absent from the closed CONTROL cohort")
		}
		delete(targets, string(member.ID))
	}
	if len(targets) != 0 {
		return "", false, fmt.Errorf("fencing: closed CONTROL cohort contains an undeclared voter")
	}
	if stored.Status.SpecHash != immutableSpecHash(stored.Spec) || str(stored.Metadata["uid"]) == "" ||
		stored.Status.RecoveryID != stored.Spec.RecoveryID || stored.Status.StatefulSetUID == "" {
		return "", false, fmt.Errorf("fencing: manual recovery identity or immutable spec binding differs")
	}
	scopeHash, err := scope.Hash()
	if err != nil {
		return "", false, err
	}
	record := recovery.NewMembershipRecord(stored.Spec.SourceClusterID, members, "async")
	digest := hashJSON([]any{"rhiza-manual-missing-source-membership-v1", str(stored.Metadata["uid"]), hashJSON(stored.Spec),
		stored.Status.StatefulSetUID, scopeHash, e.ledgerHash(state), stableSourceFenceEvidenceDigest(fence), hashJSON(record)})
	if stored.Status.ManualMembershipBindingDigest != "" && stored.Status.ManualMembershipBindingDigest != digest {
		return "", false, fmt.Errorf("fencing: persisted manual source-cohort binding changed")
	}
	return digest, true, nil
}

func (e *FencingExecutor) validatePendingFrozenEmptyGenesis(ctx context.Context, state fenceLedger, scope fencing.ArchiveScope, uid, sourceID, recoveryID, closedRootDigest string) error {
	if state.StatefulSetUID != uid || (state.Ledger.ClosedBy != "" && state.Ledger.ClosedBy != recoveryID) || !state.Ledger.RequireMutationJournal ||
		state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || state.Ledger.ArchiveGenesisRef == nil || state.Ledger.ArchiveGenesisRef.Mutation == nil {
		return fmt.Errorf("canonical source root does not bind an admitted genesis reference")
	}
	if closedRootDigest != "" && (state.Ledger.ClosedBy != recoveryID || e.ledgerHash(state) != closedRootDigest) {
		return fmt.Errorf("canonical closed root changed after its independent proof")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return err
	}
	ref := state.Ledger.ArchiveGenesisRef
	if err := ref.ValidateForScope(scope); err != nil {
		return err
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.OwnerStatefulSetUID != uid || claim.OwnerSourceClusterID != sourceID || claim.GenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, ref) {
		return errors.Join(err, fmt.Errorf("source archive claim does not bind the root genesis reference"))
	}
	mutation := ref.Mutation
	runtime, run, err := e.archiveGenesisProducerRun(ctx, state, mutation.ProducerUID)
	if err != nil || run.Runtime == nil || *run.Runtime != runtime || run.MutationProducerUID != mutation.ProducerUID || run.MutationProfile != fencing.MutationHeldExecutionProfile ||
		run.Identity.WALIdentity == "" || run.MutationScopeHash != mutation.Intent.StoreScopeHash || runtime.StatefulSetUID != uid || runtime.SourceClusterID != sourceID {
		return errors.Join(err, fmt.Errorf("genesis candidate is not bound to the held source runtime"))
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	phaseMismatch := (state.Ledger.ClosedBy == "" && (producer.Phase != fencing.MutationOpen || producer.CloseOperationID != "")) ||
		(state.Ledger.ClosedBy != "" && (producer.Phase != fencing.MutationClosed || producer.CloseOperationID != recoveryID || producer.ClosedThrough != producer.LastAdmitted))
	if err != nil || producerUID != mutation.ProducerUID || phaseMismatch || producer.CompactedThrough != 0 || producer.LastAdmitted != uint64(len(producer.Entries)) {
		return errors.Join(err, fmt.Errorf("genesis producer has incomplete or compacted source history"))
	}
	var winnerSeen bool
	for runID, sourceRun := range state.Ledger.Runs {
		if sourceRun.MutationProducerUID == "" {
			continue
		}
		if sourceRun.Runtime == nil || sourceRun.Runtime.StatefulSetUID != uid || sourceRun.Runtime.SourceClusterID != sourceID || sourceRun.MutationProfile != fencing.MutationHeldExecutionProfile ||
			sourceRun.MutationScopeHash != mutation.Intent.StoreScopeHash || sourceRun.Identity.WALIdentity == "" {
			return fmt.Errorf("source mutation producer has no exact source runtime binding")
		}
		other, otherUID, _, err := e.AuthorityState.ReadProducer(ctx, *sourceRun.Runtime)
		otherPhaseMismatch := (state.Ledger.ClosedBy == "" && (other.Phase != fencing.MutationOpen || other.CloseOperationID != "")) ||
			(state.Ledger.ClosedBy != "" && (other.Phase != fencing.MutationClosed || other.CloseOperationID != recoveryID || other.ClosedThrough != other.LastAdmitted))
		if err != nil || otherUID != sourceRun.MutationProducerUID || other.Runtime != *sourceRun.Runtime || other.WALIdentity != sourceRun.Identity.WALIdentity || otherPhaseMismatch ||
			other.CompactedThrough != 0 || other.LastAdmitted != uint64(len(other.Entries)) {
			return errors.Join(err, fmt.Errorf("source contains incomplete or compacted mutation history"))
		}
		for sequence, entry := range other.Entries {
			isWinner := runID == runtime.RunID && otherUID == mutation.ProducerUID && sequence == mutation.Sequence && entry.Intent == mutation.Intent
			if isWinner {
				if winnerSeen {
					return fmt.Errorf("source contains duplicate archive genesis winner entries")
				}
				winnerSeen = true
				if err := validateFrozenEmptyGenesisWinner(other, otherUID, sequence, entry, state.Ledger.ClosedBy == recoveryID, recoveryID); err != nil {
					return err
				}
				continue
			}
			if err := e.validateFrozenEmptyGenesisLoser(ctx, *sourceRun.Runtime, sourceRun, otherUID, sequence, entry, *ref); err != nil {
				return fmt.Errorf("source contains non-winner mutation history: %w", err)
			}
		}
	}
	if !winnerSeen {
		return fmt.Errorf("source root winner is absent from its mutation producer")
	}
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil || mutation.Intent.PayloadSize != int64(len(emptyHead)) || mutation.Intent.PayloadDigest != authorityStateDigest(emptyHead) {
		return errors.Join(err, fmt.Errorf("genesis intent does not bind canonical empty HEAD bytes"))
	}
	return nil
}

func validateFrozenEmptyGenesisWinner(producer fencing.MutationProducer, producerUID string, sequence uint64, entry fencing.MutationEntry, sourceClosed bool, recoveryID string) error {
	if !isArchiveGenesisMutation(entry.Intent) {
		return fmt.Errorf("source root winner is not an archive HEAD create")
	}
	if !sourceClosed || entry.GuardedLogicalResolution == nil {
		if entry.Resolution != nil || entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil || len(entry.Dispatches) != 1 {
			return fmt.Errorf("pending genesis has a terminal or ambiguous provider result")
		}
		for _, dispatch := range entry.Dispatches {
			if !entry.DispatchClosed || entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || dispatch.ReceiptDigest != "" {
				return fmt.Errorf("pending genesis dispatch is not the exact unresolved held create")
			}
		}
		return nil
	}
	guarded := entry.GuardedLogicalResolution
	if producer.CloseOperationID != recoveryID || producer.Phase != fencing.MutationClosed || entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest || entry.CopyCompletion != nil || guarded == nil {
		return fmt.Errorf("closed genesis lacks its nonterminal guarded logical resolution")
	}
	if err := producer.ValidateGuardedLogicalResolution(producerUID, sequence, entry); err != nil {
		return fmt.Errorf("closed genesis guarded resolution is invalid: %w", err)
	}
	if entry.Resolution.ReceiptDigest != guarded.GuardDigest || entry.Resolution.PreparationHash != guarded.PreparationHash {
		return fmt.Errorf("closed genesis resolution differs from its exact guard evidence")
	}
	return nil
}

func (e *FencingExecutor) validateFrozenEmptyGenesisLoser(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry, winner fencing.ArchiveGenesisRef) error {
	if winner.Mutation == nil || entry.Intent != winner.Mutation.Intent || !isArchiveGenesisMutation(entry.Intent) || entry.Preparation != nil || len(entry.Dispatches) != 0 || !entry.DispatchClosed || entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil ||
		entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.Resolution.AllCopiesTerminal || !mutationDigestValidOperator(entry.Resolution.ReceiptDigest) {
		return fmt.Errorf("entry is not an exact unprepared terminal genesis loser")
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return err
	}
	reader, _, err := evidence.Read(ctx, "archive-genesis-cancellations/"+entry.Resolution.ReceiptDigest)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || authorityStateDigest(body) != entry.Resolution.ReceiptDigest {
		return errors.Join(err, fmt.Errorf("genesis loser cancellation evidence digest differs"))
	}
	expected := archiveGenesisCancellationProof{Version: 1, Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent, WinningGenesis: winner}
	var proof archiveGenesisCancellationProof
	if err := json.Unmarshal(body, &proof); err != nil {
		return err
	}
	canonical, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(body, canonical) || proof.Version != expected.Version || proof.Runtime != expected.Runtime || proof.WALIdentity != expected.WALIdentity ||
		proof.ProducerUID != expected.ProducerUID || proof.Sequence != expected.Sequence || proof.Intent != expected.Intent || !sameArchiveGenesisRef(&proof.WinningGenesis, &expected.WinningGenesis) {
		return errors.Join(err, fmt.Errorf("genesis loser cancellation does not bind the exact source winner"))
	}
	return nil
}
