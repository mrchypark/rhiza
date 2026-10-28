package operator

import (
	"context"
	"fmt"
	"net/url"
	"sort"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
)

type peerClosureBinding struct {
	root         ClusterResource
	rootUID      string
	namespace    string
	rootHash     string
	workloadUID  string
	workloadHash string
	autoControl  string
	autoBinding  string
	environment  map[string]string
	callerPod    object
}

func peerClosureOperationID(rootUID, workloadUID, source string, configID uint64, abortSlot uint64) string {
	return "peer-closure-" + hashJSON([]any{rootUID, workloadUID, source, configID, abortSlot})
}

func (e *FencingExecutor) peerClosureBinding(ctx context.Context, caller fencing.Identity, workloadUID string) (peerClosureBinding, error) {
	return e.peerClosureBindingWithPrepareInit(ctx, caller, workloadUID, false)
}

// peerClosureBindingWithPrepareInit keeps the ordinary voter binding strict
// while allowing the exact fixed init profile after its caller has already
// been validated as a StatefulSet-owned mandatory-init learner.
func (e *FencingExecutor) peerClosureBindingWithPrepareInit(ctx context.Context, caller fencing.Identity, workloadUID string, prepareInit bool) (peerClosureBinding, error) {
	var callerPod object
	callerName := fencing.PodNameForIdentity(caller)
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", callerName), &callerPod); err != nil {
		return peerClosureBinding{}, err
	}
	if str(nested(callerPod, "metadata", "uid")) != caller.PodUID || nested(callerPod, "metadata", "deletionTimestamp") != nil {
		return peerClosureBinding{}, fmt.Errorf("fencing: caller Pod incarnation changed")
	}
	ownerName, ownerUID := "", ""
	for _, raw := range list(nested(callerPod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if str(owner["apiVersion"]) == "apps/v1" && str(owner["kind"]) == "StatefulSet" && owner["controller"] == true {
			ownerName, ownerUID = str(owner["name"]), str(owner["uid"])
			break
		}
	}
	if ownerName == "" || ownerUID == "" || ownerUID != workloadUID {
		return peerClosureBinding{}, fmt.Errorf("fencing: caller workload identity differs")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return peerClosureBinding{}, err
	}
	if str(nested(sts, "metadata", "uid")) != workloadUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return peerClosureBinding{}, fmt.Errorf("fencing: StatefulSet binding changed")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return peerClosureBinding{}, err
	}
	var root *ClusterResource
	for i := range roots.Items {
		candidate := &roots.Items[i]
		if candidate.Spec.StatefulSet != ownerName || candidate.Metadata["deletionTimestamp"] != nil || !candidate.Spec.Automatic || candidate.Status.ActiveClusterID != caller.SourceClusterID {
			continue
		}
		if root != nil {
			return peerClosureBinding{}, fmt.Errorf("fencing: ambiguous automatic workload binding")
		}
		root = candidate
	}
	if root == nil {
		return peerClosureBinding{}, fmt.Errorf("fencing: trusted automatic workload binding not found")
	}
	rootUID := str(root.Metadata["uid"])
	if rootUID == "" {
		return peerClosureBinding{}, fmt.Errorf("fencing: trusted root UID unavailable")
	}
	if root.Spec.LogicalID == "" || str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return peerClosureBinding{}, fmt.Errorf("fencing: trusted supervised workload binding unavailable")
	}
	if number(root.Metadata["generation"]) != root.Status.ObservedGeneration {
		return peerClosureBinding{}, fmt.Errorf("fencing: trusted root has not observed its current generation")
	}
	autoControl, autoBinding, err := e.controller().peerClosureAutoBinding(ctx, root, workloadUID, caller.SourceClusterID)
	if err != nil {
		return peerClosureBinding{}, err
	}
	if prepareInit {
		if err := validatePreparePod(callerPod, caller.PodUID); err != nil {
			return peerClosureBinding{}, err
		}
	} else if err := validateSupervisedPod(callerPod); err != nil {
		return peerClosureBinding{}, err
	}
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	if len(containers) != 1 {
		return peerClosureBinding{}, fmt.Errorf("fencing: supervised workload container unavailable")
	}
	ctr := asObject(containers[0])
	selected, err := container(sts, root.Spec.Container)
	if err != nil || selected["name"] != ctr["name"] {
		return peerClosureBinding{}, fmt.Errorf("fencing: automatic root container binding differs")
	}
	env, err := e.controller().environment(ctx, ctr)
	if err != nil {
		return peerClosureBinding{}, err
	}
	if env["RHIZA_CLUSTER_ID"] != caller.SourceClusterID || env["RHIZA_ADMIN_TOKEN"] == "" {
		return peerClosureBinding{}, fmt.Errorf("fencing: source probe credentials unavailable")
	}
	rootHash := hashJSON([]any{rootUID, root.Spec, root.Status.ActiveClusterID, number(root.Metadata["generation"])})
	workloadHash := hashJSON([]any{workloadUID, number(nested(sts, "metadata", "generation")), nested(sts, "spec", "template")})
	return peerClosureBinding{root: *root, rootUID: rootUID, namespace: e.Kube.Namespace, rootHash: rootHash, workloadUID: workloadUID, workloadHash: workloadHash, autoControl: autoControl, autoBinding: autoBinding, environment: env, callerPod: callerPod}, nil
}

func (e *FencingExecutor) peerClosureFenceRequest(binding peerClosureBinding, source string, closure fencing.PeerClosureRequest, operationID string, targets []fencing.PeerClosureTarget) FenceRequest {
	fenceTargets := make([]FenceTarget, 0, len(targets))
	for _, target := range targets {
		fenceTargets = append(fenceTargets, FenceTarget{NodeID: target.NodeID, Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity, NodeName: target.NodeName, NodeUID: target.NodeUID, ContainerID: target.ContainerID, ImageID: target.ImageID})
	}
	return FenceRequest{
		LogicalID: binding.root.Spec.LogicalID, BindingUID: binding.rootUID, Namespace: e.Kube.Namespace,
		StatefulSet: binding.root.Spec.StatefulSet, StatefulSetUID: binding.workloadUID, SourceClusterID: source,
		OperationID: operationID, Scope: ScopeVoter, Targets: fenceTargets,
	}
}

func (e *FencingExecutor) peerClosureOpenCaller(ctx context.Context, binding peerClosureBinding, caller fencing.Identity) (fenceLedger, error) {
	state, err := e.read(ctx, binding.workloadUID, caller.SourceClusterID)
	if err != nil {
		return fenceLedger{}, err
	}
	if state.Ledger.ClosedBy != "" || state.Ledger.ClosedNodes[caller.NodeID].OperationID != "" {
		return fenceLedger{}, fencing.ErrClosed
	}
	run, ok := state.Ledger.Runs[caller.RunID]
	if !ok || run.Identity != caller || run.Stop != nil || run.Exited || run.Drain != nil {
		return fenceLedger{}, fmt.Errorf("fencing: caller ledger incarnation is not live")
	}
	return state, nil
}

func (e *FencingExecutor) requestPeerClosure(ctx context.Context, caller fencing.Identity, closure fencing.PeerClosureRequest, workloadUID string) (fencing.PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	binding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	state, err := e.peerClosureOpenCaller(ctx, binding, caller)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	op := peerClosureOperationID(binding.rootUID, binding.workloadUID, caller.SourceClusterID, closure.ExpectedConfigID, *closure.ExpectedAbortSlot)
	name := "fence-" + hashJSON([]string{binding.rootUID, op})[:40]
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizafences/" + name
	var record fenceResource
	err = e.Kube.Get(ctx, endpoint, &record)
	if err == nil {
		return peerClosureReceiptFromRecord(caller, closure, binding, op, state.Ledger, record, false)
	}
	if !apiNotFound(err) {
		return fencing.PeerClosureReceipt{}, err
	}
	var targetRuns []fencing.Run
	for _, run := range state.Ledger.Runs {
		if run.Identity.NodeID != closure.TargetNodeID {
			continue
		}
		if run.Identity.WALIdentity != "" && run.Identity.WALIdentity != closure.TargetWALIdentity {
			return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: target WAL differs from retained ledger")
		}
		targetRuns = append(targetRuns, run)
	}
	if len(targetRuns) == 0 {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: target is absent from retained ledger")
	}
	endpointProbe, err := podEndpoint(binding.callerPod, binding.root.Spec.Container, "/recovery/probe")
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	status, result, err := e.controller().automaticQuorumProbe(ctx, endpointProbe, binding.environment["RHIZA_ADMIN_TOKEN"], caller.SourceClusterID, caller.NodeID, closure.TargetNodeID)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	if !peerClosureObservationValid(status, result, caller, closure) {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: survivor observation does not authorize peer closure")
	}
	var currentCaller object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", fencing.PodNameForIdentity(caller)), &currentCaller); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	callerTarget := FenceTarget{NodeID: caller.NodeID, Pod: fencing.PodNameForIdentity(caller), PodUID: caller.PodUID, WALIdentity: caller.WALIdentity}
	if err := validateProbePodIncarnation(binding.callerPod, currentCaller, callerTarget); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	currentBinding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	if currentBinding.rootUID != binding.rootUID || currentBinding.workloadUID != binding.workloadUID || currentBinding.rootHash != binding.rootHash || currentBinding.workloadHash != binding.workloadHash || currentBinding.autoControl != binding.autoControl || currentBinding.autoBinding != binding.autoBinding {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: trusted workload binding changed during probe")
	}
	if err := validateProbePodIncarnation(binding.callerPod, currentBinding.callerPod, callerTarget); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	latest, err := e.read(ctx, binding.workloadUID, caller.SourceClusterID)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	if latest.Ledger.ClosedBy != "" || latest.Ledger.ClosedNodes[caller.NodeID].OperationID != "" {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: source ledger changed during probe")
	}
	if stop := latest.Ledger.ClosedNodes[closure.TargetNodeID]; stop.OperationID != "" && stop.OperationID != op {
		return fencing.PeerClosureReceipt{}, fencing.ErrClosed
	}
	latestCaller, ok := latest.Ledger.Runs[caller.RunID]
	if !ok || latestCaller.Identity != caller || latestCaller.Stop != nil || latestCaller.Exited || latestCaller.Drain != nil {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: caller ledger changed during probe")
	}
	latestTargetRuns := make([]fencing.Run, 0, len(targetRuns))
	for _, run := range latest.Ledger.Runs {
		if run.Identity.NodeID != closure.TargetNodeID {
			continue
		}
		if run.Identity.WALIdentity != "" && run.Identity.WALIdentity != closure.TargetWALIdentity {
			return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: target ledger changed during probe")
		}
		latestTargetRuns = append(latestTargetRuns, run)
	}
	if len(latestTargetRuns) != len(targetRuns) {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: target ledger changed during probe")
	}
	targets, err := e.peerClosureTargets(ctx, latestTargetRuns, closure)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	request := e.peerClosureFenceRequest(binding, caller.SourceClusterID, closure, op, targets)
	hash, err := requestHash(&request)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	voters := append([]string(nil), status.Voters...)
	sort.Strings(voters)
	reservationTargets := append([]fencing.PeerClosureTarget(nil), targets...)
	sort.Slice(reservationTargets, func(i, j int) bool {
		if reservationTargets[i].PodUID != reservationTargets[j].PodUID {
			return reservationTargets[i].PodUID < reservationTargets[j].PodUID
		}
		return reservationTargets[i].WALIdentity < reservationTargets[j].WALIdentity
	})
	reservation := fencing.PeerClosureReservation{
		OperationID: op, RequestHash: hash, ConfigID: uint64(status.ConfigID), AbortSlot: *closure.ExpectedAbortSlot,
		TargetNodeID: closure.TargetNodeID, TargetWAL: closure.TargetWALIdentity,
		Voters: voters, Targets: reservationTargets,
	}
	if _, err := e.change(ctx, binding.workloadUID, caller.SourceClusterID, func(l *fencing.Ledger) error { return l.ReservePeerClosure(reservation) }); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	proof, err := (&KubernetesFencer{Kube: e.Kube}).Fence(ctx, request)
	if err != nil {
		if err == ErrFencePending {
			return fencing.PeerClosureReceipt{}, fencing.ErrPending
		}
		return fencing.PeerClosureReceipt{}, err
	}
	return peerClosureReceipt(caller, closure, op, request, proof)
}

func peerClosureObservationValid(status network.VoterRecoveryStatus, result quorumProbeState, caller fencing.Identity, closure fencing.PeerClosureRequest) bool {
	return result == quorumProbeAvailable && closure.ExpectedAbortSlot != nil && status.ConfigID == uint(closure.ExpectedConfigID) && status.AbortSlot == *closure.ExpectedAbortSlot &&
		status.ClusterID == caller.SourceClusterID && status.NodeID == caller.NodeID && status.WALIdentity == caller.WALIdentity &&
		status.Voting && !status.Pending && containsString(status.Voters, caller.NodeID) && containsString(status.Voters, closure.TargetNodeID) &&
		status.QuorumKnown && status.Quorum && !status.QuorumUnavailable && status.QuorumConfigID == status.ConfigID && status.CertifiedTip >= status.ReadIndexSlot
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (e *FencingExecutor) peerClosureTargets(ctx context.Context, runs []fencing.Run, closure fencing.PeerClosureRequest) ([]fencing.PeerClosureTarget, error) {
	byIdentity := map[fencing.PeerClosureTarget]bool{}
	for _, run := range runs {
		id := run.Identity
		if id.WALIdentity != "" && id.WALIdentity != closure.TargetWALIdentity {
			return nil, fmt.Errorf("fencing: target WAL differs from retained ledger")
		}
		podName := fencing.PodNameForIdentity(id)
		target := fencing.PeerClosureTarget{NodeID: id.NodeID, Pod: podName, PodUID: id.PodUID, WALIdentity: id.WALIdentity}
		var pod object
		err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod)
		if err != nil && !apiNotFound(err) {
			return nil, err
		}
		if err == nil && str(nested(pod, "metadata", "uid")) == id.PodUID && nested(pod, "metadata", "deletionTimestamp") == nil {
			captured := capturedFenceIdentity(FenceTarget{NodeID: id.NodeID, Pod: podName, PodUID: id.PodUID, WALIdentity: id.WALIdentity}, pod)
			target.NodeName, target.NodeUID, target.ContainerID, target.ImageID = captured.NodeName, captured.NodeUID, captured.ContainerID, captured.ImageID
		}
		byIdentity[target] = true
	}
	if len(byIdentity) == 0 || len(byIdentity) > fenceTargetCapacity {
		return nil, fmt.Errorf("fencing: target incarnation inventory is empty or exceeds capacity")
	}
	result := make([]fencing.PeerClosureTarget, 0, len(byIdentity))
	requestedWAL := false
	for target := range byIdentity {
		if target.NodeID != closure.TargetNodeID {
			return nil, fmt.Errorf("fencing: target inventory differs from requested slot")
		}
		if target.WALIdentity == closure.TargetWALIdentity {
			requestedWAL = true
		}
		result = append(result, target)
	}
	if !requestedWAL {
		return nil, fmt.Errorf("fencing: target inventory does not contain requested WAL")
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].PodUID != result[j].PodUID {
			return result[i].PodUID < result[j].PodUID
		}
		return result[i].WALIdentity < result[j].WALIdentity
	})
	return result, nil
}

func (e *FencingExecutor) readPeerClosure(ctx context.Context, caller fencing.Identity, closure fencing.PeerClosureRequest, workloadUID string) (fencing.PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	binding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	state, err := e.peerClosureOpenCaller(ctx, binding, caller)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	op := peerClosureOperationID(binding.rootUID, binding.workloadUID, caller.SourceClusterID, closure.ExpectedConfigID, *closure.ExpectedAbortSlot)
	name := "fence-" + hashJSON([]string{binding.rootUID, op})[:40]
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizafences/" + name
	var record fenceResource
	if err := e.Kube.Get(ctx, endpoint, &record); err != nil {
		if apiNotFound(err) {
			return fencing.PeerClosureReceipt{}, fencing.ErrNotFound
		}
		return fencing.PeerClosureReceipt{}, err
	}
	return peerClosureReceiptFromRecord(caller, closure, binding, op, state.Ledger, record, true)
}

func peerClosureReceiptFromRecord(caller fencing.Identity, closure fencing.PeerClosureRequest, binding peerClosureBinding, operationID string, ledger fencing.Ledger, record fenceResource, allowHistorical bool) (fencing.PeerClosureReceipt, error) {
	wantName := "fence-" + hashJSON([]string{binding.rootUID, operationID})[:40]
	if str(record.Metadata["name"]) != wantName || record.Metadata["deletionTimestamp"] != nil || record.Spec.OperationID != operationID || record.Spec.BindingUID != binding.rootUID || record.Spec.Namespace != binding.namespace || record.Spec.StatefulSetUID != binding.workloadUID || record.Spec.SourceClusterID != caller.SourceClusterID || record.Spec.Scope != ScopeVoter || record.Spec.LogicalID != binding.root.Spec.LogicalID || record.Spec.StatefulSet != binding.root.Spec.StatefulSet {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: immutable peer-closure binding differs")
	}
	if err := peerClosureRecordMatchesLedger(ledger, closure, operationID, record.Spec, allowHistorical); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	found := false
	requestedWAL := false
	for _, target := range record.Spec.Targets {
		if target.NodeID != closure.TargetNodeID || fencing.ValidatePodBinding(target.NodeID, target.Pod, target.PodUID) != nil || (target.WALIdentity != "" && target.WALIdentity != closure.TargetWALIdentity) {
			return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: immutable peer-closure target differs")
		}
		if target.WALIdentity == closure.TargetWALIdentity {
			requestedWAL = true
		}
		found = true
	}
	if !found || !requestedWAL {
		return fencing.PeerClosureReceipt{}, fmt.Errorf("fencing: immutable peer-closure target omitted")
	}
	if record.Status.Proof == nil {
		return fencing.PeerClosureReceipt{}, fencing.ErrPending
	}
	if err := validateFenceProof(record.Spec, record.Status.Proof); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	return peerClosureReceipt(caller, closure, operationID, record.Spec, record.Status.Proof)
}

func peerClosureRecordMatchesLedger(ledger fencing.Ledger, closure fencing.PeerClosureRequest, operationID string, request FenceRequest, allowHistorical bool) error {
	if closure.ExpectedAbortSlot == nil {
		return fmt.Errorf("fencing: peer-closure abort slot is missing")
	}
	hash, err := requestHash(&request)
	if err != nil {
		return err
	}
	stop, closed := ledger.ClosedNodes[closure.TargetNodeID]
	reservation := ledger.PeerClosure
	if !closed || stop.OperationID != operationID || stop.RequestHash != hash {
		return fmt.Errorf("fencing: peer-closure record has no exact durable closure")
	}
	if request.OperationID != operationID || request.Scope != ScopeVoter {
		return fmt.Errorf("fencing: peer-closure request differs from reserved operation")
	}
	if reservation == nil || reservation.OperationID != operationID {
		if allowHistorical {
			return nil
		}
		return fmt.Errorf("fencing: peer-closure record has no active durable reservation")
	}
	if reservation.RequestHash != hash || reservation.ConfigID != closure.ExpectedConfigID ||
		reservation.AbortSlot != *closure.ExpectedAbortSlot || reservation.TargetNodeID != closure.TargetNodeID || reservation.TargetWAL != closure.TargetWALIdentity {
		return fmt.Errorf("fencing: peer-closure record has no exact durable reservation")
	}
	if !samePeerClosureTargetInventory(reservation.Targets, request.Targets) {
		return fmt.Errorf("fencing: peer-closure target inventory differs from durable reservation")
	}
	return nil
}

func samePeerClosureTargetInventory(reserved []fencing.PeerClosureTarget, request []FenceTarget) bool {
	if len(reserved) != len(request) {
		return false
	}
	for i := range reserved {
		target := request[i]
		if reserved[i] != (fencing.PeerClosureTarget{
			NodeID: target.NodeID, Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity,
			NodeName: target.NodeName, NodeUID: target.NodeUID, ContainerID: target.ContainerID, ImageID: target.ImageID,
		}) {
			return false
		}
	}
	return true
}

func peerClosureReceipt(caller fencing.Identity, closure fencing.PeerClosureRequest, operationID string, request FenceRequest, proof *FenceProof) (fencing.PeerClosureReceipt, error) {
	if err := validateFenceProof(request, proof); err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	hash, err := requestHash(&request)
	if err != nil {
		return fencing.PeerClosureReceipt{}, err
	}
	targets := make([]fencing.PeerClosureTarget, 0, len(request.Targets))
	for _, target := range request.Targets {
		targets = append(targets, fencing.PeerClosureTarget{NodeID: target.NodeID, Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity, NodeName: target.NodeName, NodeUID: target.NodeUID, ContainerID: target.ContainerID, ImageID: target.ImageID})
	}
	abortSlot := *closure.ExpectedAbortSlot
	return fencing.PeerClosureReceipt{OperationID: operationID, SourceClusterID: caller.SourceClusterID, BindingUID: request.BindingUID, StatefulSetUID: request.StatefulSetUID, RequestHash: hash, ExpectedConfigID: closure.ExpectedConfigID, ExpectedAbortSlot: &abortSlot, TargetNodeID: closure.TargetNodeID, TargetWALIdentity: closure.TargetWALIdentity, Targets: targets, ProofID: proof.ProofID, ProcessesTerminated: proof.ProcessesTerminated, RecreationBlocked: proof.RecreationBlocked, StorageQuiesced: proof.StorageQuiesced}, nil
}
