package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

func membershipRepairOperationID(requestHash string) string {
	return "membership-repair-" + shortID(requestHash)
}

func membershipRepairAssignmentOperationID(operationID, podUID string) string {
	return operationID + "-pod-" + shortID(podUID)
}

func membershipRepairRequestHash(a fencing.MembershipRepairAuthorization) string {
	if a.ExpectedAbortSlot == nil {
		return ""
	}
	return hashJSON(struct {
		RootUID, StatefulSetUID, LedgerUID, SourceClusterID                 string
		ConfigID                                                            uint64
		AbortSlot                                                           uint64
		OldIdentity                                                         fencing.Identity
		Membership                                                          quepaxa.Cluster
		BootstrapMembershipHash, ProfileHash, StoreIdentityHash, Durability string
	}{a.RootUID, a.StatefulSetUID, a.LedgerUID, a.SourceClusterID, uint64(a.Membership.ConfigID), uint64(*a.ExpectedAbortSlot), a.OldIdentity, a.Membership, a.BootstrapMembershipHash, a.ProfileHash, a.StoreIdentityHash, a.Durability})
}

func membershipRepairAuthorizationHash(a fencing.MembershipRepairAuthorization) string {
	return fencing.MembershipRepairAuthorizationHash(a)
}

func (e *FencingExecutor) qualifyMembershipRepair(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairQualificationRequest) (fencing.MembershipRepairQualificationResult, error) {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairQualificationResult{}, err
	}
	view, err := state.Ledger.ReadMembershipRepair(caller)
	if err != nil || view.Authorization.OperationID != request.OperationID || request.CandidatePodUID == "" {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	if view.Preclosure != nil {
		if view.Preclosure.Request.ScopeDigest == "" || view.Preclosure.CandidatePodUID != request.CandidatePodUID {
			return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
		}
		return fencing.MembershipRepairQualificationResult{OperationID: request.OperationID, Preclosure: &view.Preclosure.Response}, nil
	}
	if view.Progress.Stage != fencing.MembershipRepairCandidateBound || e.verifyMembershipRepairArchive(ctx, binding, view) != nil {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	var candidate *fencing.MembershipRepairCandidate
	for i := range view.Progress.Candidates {
		if view.Progress.Candidates[i].Assignment.PodUID == request.CandidatePodUID {
			candidate = &view.Progress.Candidates[i]
			break
		}
	}
	if candidate == nil || candidate.Member == nil || !repairCandidateInitReady(*candidate) {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	var challengeBytes [32]byte
	if _, err := rand.Read(challengeBytes[:]); err != nil {
		return fencing.MembershipRepairQualificationResult{}, fmt.Errorf("create membership-repair probe challenge: %w", err)
	}
	challenge := hex.EncodeToString(challengeBytes[:])
	abortSlot := *view.Authorization.ExpectedAbortSlot
	bindRef := fencing.BootstrapAssignmentDigest(candidate.Assignment)
	scopeDigest := fencing.MembershipRepairProbeScopeDigest(challenge, view.Authorization, candidate.Assignment, *candidate.Member)
	probeRequest := network.MembershipRepairProbeRequest{
		Version: network.RecoveryProbeProtocolVersion, Challenge: challenge, ScopeDigest: scopeDigest,
		SourceConfigID: uint(view.Authorization.Membership.ConfigID), AbortSlot: &abortSlot,
		ExcludedOldMemberID: view.Authorization.OldIdentity.NodeID, CandidateMember: *candidate.Member, CandidateBindRef: bindRef,
	}
	if err := network.ValidateMembershipRepairProbeRequest(probeRequest); err != nil {
		return fencing.MembershipRepairQualificationResult{}, err
	}
	probeResponse, err := e.requestMembershipRepairQualification(ctx, binding, probeRequest)
	if err != nil {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	if err := e.verifyMembershipRepairQualificationArchive(ctx, binding, caller, view.Authorization, probeRequest, probeResponse); err != nil {
		return fencing.MembershipRepairQualificationResult{}, err
	}
	currentBinding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil || currentBinding.rootUID != binding.rootUID || currentBinding.workloadUID != binding.workloadUID || currentBinding.rootHash != binding.rootHash || currentBinding.autoControl != binding.autoControl || currentBinding.autoBinding != binding.autoBinding {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	if err := validateProbePodIncarnation(binding.callerPod, currentBinding.callerPod, FenceTarget{NodeID: caller.NodeID, Pod: caller.PodName, PodUID: caller.PodUID, WALIdentity: caller.WALIdentity}); err != nil {
		return fencing.MembershipRepairQualificationResult{}, fencing.ErrPending
	}
	retained := &fencing.MembershipRepairPreclosure{CandidatePodUID: candidate.Assignment.PodUID, Request: probeRequest, Response: probeResponse}
	_, err = e.changeExisting(ctx, workloadUID, caller.SourceClusterID, view.Authorization.LedgerUID, func(l *fencing.Ledger) error {
		if l.MembershipRepair == nil || !reflect.DeepEqual(l.MembershipRepair.Authorization, view.Authorization) || !reflect.DeepEqual(l.MembershipRepair.Progress, view.Progress) || l.MembershipRepair.Preclosure != nil || l.Runs[caller.RunID].Identity != caller {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		l.MembershipRepair.Preclosure = retained
		return nil
	})
	if err != nil {
		return fencing.MembershipRepairQualificationResult{}, err
	}
	return fencing.MembershipRepairQualificationResult{OperationID: request.OperationID, Preclosure: &retained.Response}, nil
}

func repairCandidateInitReady(candidate fencing.MembershipRepairCandidate) bool {
	if candidate.Assignment.Validate() != nil || candidate.Assignment.Purpose != fencing.BootstrapLearner {
		return false
	}
	for _, attempt := range candidate.Attempts {
		if attempt.Assignment != candidate.Assignment || len(attempt.Outcomes) != 1 || attempt.Terminal == nil ||
			attempt.Terminal.ExitCode != 0 || attempt.Terminal.Reason != "Completed" || attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID ||
			attempt.Outcomes[0].ValidateForAssignment(candidate.Assignment) != nil || attempt.Outcomes[0].State != fencing.InitAttemptMaterialized {
			continue
		}
		return true
	}
	return false
}

func (e *FencingExecutor) requestMembershipRepairQualification(ctx context.Context, binding peerClosureBinding, probeRequest network.MembershipRepairProbeRequest) (network.MembershipRepairProbeResponse, error) {
	endpoint, err := podEndpoint(binding.callerPod, binding.root.Spec.Container, "/recovery/probe")
	if err != nil {
		return network.MembershipRepairProbeResponse{}, err
	}
	body, err := json.Marshal(probeRequest)
	if err != nil {
		return network.MembershipRepairProbeResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return network.MembershipRepairProbeResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+binding.environment["RHIZA_ADMIN_TOKEN"])
	request.Header.Set("Content-Type", "application/json")
	response, err := e.controller().client().Do(request)
	if err != nil {
		return network.MembershipRepairProbeResponse{}, fmt.Errorf("membership-repair runtime qualification unavailable: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || response.StatusCode != http.StatusOK || len(data) > 64<<10 {
		return network.MembershipRepairProbeResponse{}, fmt.Errorf("membership-repair runtime qualification returned an invalid response")
	}
	proof, err := network.StrictDecodeMembershipRepairProbeResponse(data, probeRequest, binding.environment["RHIZA_ADMIN_TOKEN"])
	if err != nil {
		return network.MembershipRepairProbeResponse{}, fmt.Errorf("membership-repair runtime qualification response is invalid: %w", err)
	}
	return proof, nil
}

func (e *FencingExecutor) verifyMembershipRepairQualificationArchive(ctx context.Context, binding peerClosureBinding, caller fencing.Identity, authorization fencing.MembershipRepairAuthorization, request network.MembershipRepairProbeRequest, proof network.MembershipRepairProbeResponse) error {
	current, pending, abort, err := e.membershipRepairArchiveState(ctx, binding, authorization)
	if err != nil || pending.ConfigID != 0 || current.ConfigID != request.SourceConfigID || request.AbortSlot == nil || abort != *request.AbortSlot || !reflect.DeepEqual(current, authorization.Membership) {
		return fencing.ErrPending
	}
	if proof.VerifierNodeID != caller.NodeID || proof.VerifierWALIdentity != caller.WALIdentity || proof.SourceConfigID != request.SourceConfigID ||
		!reflect.DeepEqual(proof.SourceMembers, authorization.Membership.Members) || proof.AbortSlot == nil || *proof.AbortSlot != *request.AbortSlot ||
		proof.ExcludedOldMemberID != authorization.OldIdentity.NodeID || proof.ReadIndexSlot == 0 || proof.BarrierSlot < proof.ReadIndexSlot {
		return fencing.ErrPending
	}
	nonce := network.MembershipRepairBarrierNonce(request.Challenge)
	verified, err := e.verifyMembershipRepairBarrier(ctx, binding, authorization.SourceClusterID, quepaxa.Slot(proof.BarrierSlot), nonce)
	if err != nil {
		return fmt.Errorf("verify membership-repair certified challenge barrier: %w", err)
	}
	prefixHash, prefixErr := hex.DecodeString(proof.PrefixHash)
	if verified.Decision.Slot != quepaxa.Slot(proof.BarrierSlot) || verified.Membership.ConfigID != authorization.Membership.ConfigID ||
		!reflect.DeepEqual(verified.Membership, authorization.Membership) || verified.AbortSlot != *request.AbortSlot || verified.Pending ||
		prefixErr != nil || len(prefixHash) != 32 || !bytes.Equal(prefixHash, verified.PrefixHash[:]) || proof.BarrierNonce != hex.EncodeToString(nonce[:]) ||
		proof.CandidatePrefixThrough != proof.BarrierSlot || proof.CandidatePrefixHash != proof.PrefixHash {
		return fencing.ErrPending
	}
	return nil
}

// verifyMembershipRepairBarrier reuses the exact retained DR anchor approval
// when the active source archive is a generation target. The base-zero path is
// retained only for genuinely unanchored source generations.
func (e *FencingExecutor) verifyMembershipRepairBarrier(ctx context.Context, binding peerClosureBinding, source string, slot quepaxa.Slot, nonce [quepaxa.ReadBarrierNonceSize]byte) (recovery.CertifiedBarrierState, error) {
	initial, err := recoveryMembers(binding.environment["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(initial) == 0 {
		return recovery.CertifiedBarrierState{}, fencing.ErrPending
	}
	archive := recovery.NewManager(e.Bucket, path.Join(e.Prefix, source), 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return recovery.CertifiedBarrierState{}, err
	}
	view, err := archive.CaptureCertifiedView()
	if err != nil {
		return recovery.CertifiedBarrierState{}, err
	}
	anchorHash, anchored := view.GenerationAnchor()
	options, approved, err := e.membershipRepairEvidenceOptions(ctx, binding, source, initial)
	if err != nil {
		return recovery.CertifiedBarrierState{}, err
	}
	if anchored {
		if !approved {
			return recovery.CertifiedBarrierState{}, fmt.Errorf("anchored membership-repair source has no unique retained DR approval")
		}
		if options.ExpectedAnchorHash != anchorHash {
			return recovery.CertifiedBarrierState{}, fmt.Errorf("membership-repair source anchor differs from retained DR approval")
		}
		return archive.VerifyCertifiedBarrierWithEvidenceState(ctx, quepaxa.Cluster{ConfigID: 1, Members: initial}, true, slot, nonce, options)
	}
	if approved {
		return recovery.CertifiedBarrierState{}, fmt.Errorf("retained DR approval points to an unanchored membership-repair source")
	}
	return archive.VerifyCertifiedBarrierState(ctx, quepaxa.Cluster{ConfigID: 1, Members: initial}, true, slot, nonce)
}

func (e *FencingExecutor) membershipRepairEvidenceOptions(ctx context.Context, binding peerClosureBinding, source string, initial []quepaxa.Member) (recovery.EvidenceOptions, bool, error) {
	var options recovery.EvidenceOptions
	state, ledgerUID, _, err := e.readWithUID(ctx, binding.workloadUID, source)
	if err != nil || ledgerUID == "" || state.Ledger.Bootstrap == nil || state.Ledger.Bootstrap.RootUID != binding.rootUID || state.Ledger.Bootstrap.StatefulSetUID != binding.workloadUID {
		return options, false, fencing.ErrPending
	}
	durability := binding.environment["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	var approved *recoveryanchor.Request
	var approvedReceipt *recoveryanchor.Receipt
	for operationID, operation := range state.Ledger.Bootstrap.Operations {
		if operation.Purpose != fencing.BootstrapDRSuccessor || operation.OperationID != operationID || operation.RootUID != binding.rootUID || operation.StatefulSetUID != binding.workloadUID || operation.SourceClusterID != source {
			continue
		}
		for podUID, assignment := range operation.Assignments {
			request, receipt := assignment.DRApproval.Request, assignment.DRApproval.Receipt
			if assignment.Validate() != nil || assignment.Purpose != fencing.BootstrapDRSuccessor || assignment.OperationID != operationID || assignment.PodUID != podUID || assignment.RootUID != binding.rootUID || assignment.StatefulSetUID != binding.workloadUID || assignment.SourceClusterID != source || assignment.AuthorizationHash != operation.AuthorizationHash || fencing.BootstrapDRAuthorizationHash(assignment) != operation.AuthorizationHash || request.TargetClusterID != source || request.StatefulSetUID != binding.workloadUID || path.Clean(request.TargetPrefix) != path.Join(e.Prefix, source) || receipt.Target.ClusterID != source {
				return options, false, fencing.ErrPending
			}
			if approved != nil && (*approved != request || *approvedReceipt != receipt) {
				return options, false, fmt.Errorf("membership-repair source has ambiguous retained DR approvals")
			}
			copyRequest, copyReceipt := request, receipt
			approved, approvedReceipt = &copyRequest, &copyReceipt
		}
	}
	if approved == nil {
		return options, false, nil
	}
	wantBinding, bindingErr := expectedAnchorBinding(e.AnchorBindingConfig, e.AnchorStorageID, source)
	if e.Anchor == nil || bindingErr != nil || approvedReceipt.Target != wantBinding {
		return options, false, fmt.Errorf("membership-repair source DR storage binding is unavailable")
	}
	if err := e.Anchor.VerifyAdmission(ctx, *approved, *approvedReceipt, wantBinding, approvedReceipt.Generation); err != nil {
		return options, false, fmt.Errorf("membership-repair source DR admission is no longer current: %w", err)
	}
	anchorHash, err := hex.DecodeString(approved.TargetAnchorHash)
	if err != nil || len(anchorHash) != len(options.ExpectedAnchorHash) {
		return options, false, fmt.Errorf("retained DR target anchor hash is invalid")
	}
	copy(options.ExpectedAnchorHash[:], anchorHash)
	options.ExpectedMembership = approved.TargetMembership
	options.ExpectedForkResult = approved.Fork
	options.ExpectedSourcePrefix = approved.SourcePrefix
	options.ExpectedOperationID = approved.OperationID
	if recovery.NewMembershipRecord(source, initial, durability) != options.ExpectedMembership {
		return recovery.EvidenceOptions{}, false, fmt.Errorf("configured membership differs from retained DR target membership")
	}
	return options, true, nil
}

func (e *FencingExecutor) initializeMembershipRepairObserver(ctx context.Context, binding peerClosureBinding, source string, initial []quepaxa.Member, archive *recovery.Manager, observer *quepaxa.Core) error {
	view, err := archive.CaptureCertifiedView()
	if err != nil {
		return err
	}
	anchorHash, anchored := view.GenerationAnchor()
	options, approved, err := e.membershipRepairEvidenceOptions(ctx, binding, source, initial)
	if err != nil {
		return err
	}
	if anchored {
		if !approved || options.ExpectedAnchorHash != anchorHash {
			return fmt.Errorf("anchored membership-repair source lacks its exact retained DR approval")
		}
		checkpoints := checkpoint.NewManager(e.Bucket, path.Join(e.Prefix, source), "", 1)
		if _, err := recovery.InitializeCertifiedRecoveryBase(ctx, observer, view, options.ExpectedMembership, options.ExpectedAnchorHash, checkpoints); err != nil {
			return fmt.Errorf("initialize trusted membership-repair source base: %w", err)
		}
		return nil
	}
	if approved {
		return fmt.Errorf("retained DR approval points to a source archive without its generation anchor")
	}
	return nil
}

// reserveMembershipRepair is the only source-side mutation that may install a
// same-generation learner assignment. It never calls the peer-closure path.
func (e *FencingExecutor) reserveMembershipRepair(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairReserveRequest) (fencing.MembershipRepairReserveResult, error) {
	closure := fencing.PeerClosureRequest{ExpectedConfigID: request.ExpectedConfigID, ExpectedAbortSlot: request.ExpectedAbortSlot, TargetNodeID: request.TargetNodeID, TargetWALIdentity: request.TargetWALIdentity}
	if err := closure.Validate(caller); err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	current, pending, abortSlot, err := e.membershipRepairArchiveState(ctx, binding, fencing.MembershipRepairAuthorization{SourceClusterID: caller.SourceClusterID})
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	if state.Ledger.MembershipRepair != nil && state.Ledger.MembershipRepair.Progress.Stage != fencing.MembershipRepairComplete && state.Ledger.MembershipRepair.Progress.Stage != fencing.MembershipRepairCancelled {
		authorization := state.Ledger.MembershipRepair.Authorization
		if authorization.Validate() != nil || authorization.RootUID != binding.rootUID || authorization.StatefulSetUID != workloadUID || authorization.SourceClusterID != caller.SourceClusterID || authorization.ExpectedAbortSlot == nil || uint64(authorization.Membership.ConfigID) != request.ExpectedConfigID || uint64(*authorization.ExpectedAbortSlot) != *request.ExpectedAbortSlot || authorization.OldIdentity.NodeID != request.TargetNodeID || authorization.OldIdentity.WALIdentity != request.TargetWALIdentity || membershipRepairRequestHash(authorization) != authorization.RequestHash || membershipRepairAuthorizationHash(authorization) != authorization.AuthorizationHash {
			return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
		}
		view, readErr := state.Ledger.ReadMembershipRepair(caller)
		if readErr != nil {
			return fencing.MembershipRepairReserveResult{}, readErr
		}
		if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
			return fencing.MembershipRepairReserveResult{}, err
		}
		if err := e.ensureMembershipRepairCandidate(ctx, caller, binding, authorization); err != nil {
			return fencing.MembershipRepairReserveResult{}, err
		}
		latest, err := e.read(ctx, workloadUID, caller.SourceClusterID)
		if err != nil {
			return fencing.MembershipRepairReserveResult{}, err
		}
		view, err = latest.Ledger.ReadMembershipRepair(caller)
		if err != nil {
			return fencing.MembershipRepairReserveResult{}, err
		}
		if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
			return fencing.MembershipRepairReserveResult{}, err
		}
		return fencing.MembershipRepairReserveResult{Repair: &view}, nil
	}
	if pending.ConfigID != 0 || uint64(current.ConfigID) != request.ExpectedConfigID || abortSlot != quepaxa.Slot(*request.ExpectedAbortSlot) {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	if !repairMembershipContains(current, caller.NodeID, caller.WALIdentity) || !repairMembershipContains(current, request.TargetNodeID, request.TargetWALIdentity) {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	endpoint, err := podEndpoint(binding.callerPod, binding.root.Spec.Container, "/recovery/probe")
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	status, observed, err := e.controller().automaticQuorumProbe(ctx, endpoint, binding.environment["RHIZA_ADMIN_TOKEN"], caller.SourceClusterID, caller.NodeID, request.TargetNodeID)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	if !peerClosureObservationValid(status, observed, caller, closure) {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	old, err := retainedRepairTarget(state.Ledger, request.TargetNodeID, request.TargetWALIdentity, caller.PodName)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	pin, err := e.initBootstrapPin(ctx, binding, caller.SourceClusterID)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	durability := binding.environment["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	storeHash := fencing.BootstrapStoreIdentityHash(StoreIdentity(binding.environment))
	expectedAbort := quepaxa.Slot(*request.ExpectedAbortSlot)
	authorization := fencing.MembershipRepairAuthorization{
		RootUID: binding.rootUID, StatefulSetUID: workloadUID, LedgerUID: pin.LedgerUID,
		SourceClusterID: caller.SourceClusterID, MembershipHash: hashJSON(current.Members),
		ExpectedAbortSlot: &expectedAbort, BootstrapMembershipHash: pin.MembershipHash,
		ProfileHash: pin.ProfileHash, StoreIdentityHash: storeHash, Durability: durability,
		OldIdentity: old, Membership: current,
	}
	requestHash := membershipRepairRequestHash(authorization)
	authorization.OperationID = membershipRepairOperationID(requestHash)
	authorization.RequestHash = requestHash
	authorization.AuthorizationHash = membershipRepairAuthorizationHash(authorization)
	currentBinding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil || currentBinding.rootUID != binding.rootUID || currentBinding.workloadUID != binding.workloadUID || currentBinding.rootHash != binding.rootHash || currentBinding.workloadHash != binding.workloadHash || currentBinding.autoControl != binding.autoControl || currentBinding.autoBinding != binding.autoBinding {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	callerTarget := FenceTarget{NodeID: caller.NodeID, Pod: fencing.PodNameForIdentity(caller), PodUID: caller.PodUID, WALIdentity: caller.WALIdentity}
	if err := validateProbePodIncarnation(binding.callerPod, currentBinding.callerPod, callerTarget); err != nil {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	latestCurrent, latestPending, latestAbort, err := e.membershipRepairArchiveState(ctx, currentBinding, authorization)
	if err != nil || latestPending.ConfigID != 0 || !reflect.DeepEqual(latestCurrent, current) || latestAbort != abortSlot {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	latest, latestUID, _, err := e.readWithUID(ctx, workloadUID, caller.SourceClusterID)
	if err != nil || latestUID != pin.LedgerUID || latest.Ledger.Runs[caller.RunID].Identity != caller {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	latestTarget, err := retainedRepairTarget(latest.Ledger, request.TargetNodeID, request.TargetWALIdentity, fencing.PodNameForIdentity(caller))
	if err != nil || latestTarget != old {
		return fencing.MembershipRepairReserveResult{}, fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, workloadUID, caller.SourceClusterID, latestUID, func(ledger *fencing.Ledger) error {
		if ledger.ClosedBy != "" || ledger.ClosedNodes[caller.NodeID].OperationID != "" || ledger.Runs[caller.RunID].Identity != caller {
			return fencing.ErrClosed
		}
		return nil
	}, func(ledger *fencing.Ledger) error { return ledger.InstallMembershipRepair(authorization) })
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	if err := e.ensureMembershipRepairCandidate(ctx, caller, binding, authorization); err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	latest, err = e.read(ctx, workloadUID, caller.SourceClusterID)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	view, err := latest.Ledger.ReadMembershipRepair(caller)
	if err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
		return fencing.MembershipRepairReserveResult{}, err
	}
	return fencing.MembershipRepairReserveResult{Repair: &view}, nil
}

func repairMembershipContains(cluster quepaxa.Cluster, nodeID, wal string) bool {
	for _, member := range cluster.Members {
		if string(member.ID) == nodeID && member.WALIdentity == wal {
			return true
		}
	}
	return false
}

func retainedRepairTarget(ledger fencing.Ledger, nodeID, wal, callerPod string) (fencing.Identity, error) {
	var target *fencing.Identity
	for _, run := range ledger.Runs {
		id := run.Identity
		if id.NodeID != nodeID || id.WALIdentity != wal {
			continue
		}
		if target != nil && *target != id {
			return fencing.Identity{}, fencing.ErrPending
		}
		copy := id
		target = &copy
	}
	if target == nil || fencing.PodNameForIdentity(*target) == callerPod {
		return fencing.Identity{}, fencing.ErrPending
	}
	return *target, nil
}

func (e *FencingExecutor) ensureMembershipRepairCandidate(ctx context.Context, caller fencing.Identity, binding peerClosureBinding, authorization fencing.MembershipRepairAuthorization) error {
	name := fencing.PodNameForIdentity(authorization.OldIdentity)
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", name), &pod); err != nil {
		if apiNotFound(err) {
			return nil
		}
		return err
	}
	uid := str(nested(pod, "metadata", "uid"))
	if uid == authorization.OldIdentity.PodUID || uid == "" || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil
	}
	if err := validatePreparePod(pod, uid); err != nil {
		return err
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return fencing.ErrPending
	}
	owner := asObject(refs[0])
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["uid"] != authorization.StatefulSetUID || owner["controller"] != true || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != authorization.RootUID {
		return fencing.ErrPending
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, authorization.StatefulSetUID, authorization.SourceClusterID)
	if err != nil || ledgerUID != authorization.LedgerUID || state.Ledger.MembershipRepair == nil || !reflect.DeepEqual(state.Ledger.MembershipRepair.Authorization, authorization) {
		return fencing.ErrPending
	}
	for _, prior := range state.Ledger.MembershipRepair.Progress.Candidates {
		if prior.Assignment.PodUID != uid {
			continue
		}
		assignment := prior.Assignment
		if !assignmentMatchesRepair(assignment, authorization, name, uid) {
			return fencing.ErrPending
		}
		identity := fencing.Identity{SourceClusterID: authorization.SourceClusterID, NodeID: assignment.NodeID, PodName: name, PodUID: uid, CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID}
		registration, err := e.incarnationCredentialForInit(ctx, identity, authorization.StatefulSetUID, identity, false)
		if err != nil || !bootstrapPublicMemberMatches(assignment.PublicMember, registration.Learner) {
			return fencing.ErrPending
		}
		return nil
	}
	if state.Ledger.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdmitted {
		return fencing.ErrPending
	}
	current, pending, abort, err := e.membershipRepairArchiveState(ctx, binding, authorization)
	if err != nil || pending.ConfigID != 0 || !reflect.DeepEqual(current, authorization.Membership) || authorization.ExpectedAbortSlot == nil || abort != *authorization.ExpectedAbortSlot {
		return fencing.ErrPending
	}
	identity := fencing.Identity{SourceClusterID: authorization.SourceClusterID, NodeID: "", PodName: name, PodUID: uid}
	identity.NodeID, _ = fencing.ReplacementNodeID(uid)
	registration, err := e.incarnationCredentialForInit(ctx, identity, authorization.StatefulSetUID, identity, true)
	if err != nil {
		return err
	}
	var member quepaxa.Member
	if err := json.Unmarshal(registration.Learner, &member); err != nil || member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) {
		return fencing.ErrPending
	}
	assignment := fencing.BootstrapAssignment{
		OperationID: membershipRepairAssignmentOperationID(authorization.OperationID, uid), RequestHash: authorization.RequestHash,
		AuthorizationHash: authorization.AuthorizationHash, Purpose: fencing.BootstrapLearner,
		RootUID: authorization.RootUID, StatefulSetUID: authorization.StatefulSetUID, SourceClusterID: authorization.SourceClusterID,
		PodName: name, PodUID: uid, NodeID: identity.NodeID, CredentialName: registration.Identity.CredentialName, CredentialUID: registration.Identity.CredentialUID,
		MembershipHash: authorization.BootstrapMembershipHash, ProfileHash: authorization.ProfileHash, LedgerUID: authorization.LedgerUID,
		PublicMember: fencing.BootstrapPublicMember{ID: string(member.ID), URL: member.URL, PeerURL: member.PeerURL, LogURL: member.LogURL, PublicKey: hex.EncodeToString(member.PublicKey[:]), WALIdentity: member.WALIdentity},
		Durability:   authorization.Durability, StoreIdentityHash: authorization.StoreIdentityHash,
	}
	if err := assignment.Validate(); err != nil {
		return err
	}
	_, err = e.changeExisting(ctx, authorization.StatefulSetUID, authorization.SourceClusterID, ledgerUID, nil, func(ledger *fencing.Ledger) error {
		if ledger.MembershipRepair == nil || !reflect.DeepEqual(ledger.MembershipRepair.Authorization, authorization) || ledger.ClosedBy != "" {
			return fencing.ErrPending
		}
		return ledger.AppendMembershipRepairCandidate(fencing.MembershipRepairCandidate{Assignment: assignment})
	})
	return err
}

func assignmentMatchesRepair(assignment fencing.BootstrapAssignment, authorization fencing.MembershipRepairAuthorization, podName, podUID string) bool {
	return assignment.Validate() == nil && assignment.Purpose == fencing.BootstrapLearner && assignment.OperationID == membershipRepairAssignmentOperationID(authorization.OperationID, podUID) &&
		assignment.RequestHash == authorization.RequestHash && assignment.AuthorizationHash == authorization.AuthorizationHash && assignment.RootUID == authorization.RootUID &&
		assignment.StatefulSetUID == authorization.StatefulSetUID && assignment.LedgerUID == authorization.LedgerUID && assignment.SourceClusterID == authorization.SourceClusterID &&
		assignment.PodName == podName && assignment.PodUID == podUID && assignment.MembershipHash == authorization.BootstrapMembershipHash &&
		assignment.ProfileHash == authorization.ProfileHash && assignment.StoreIdentityHash == authorization.StoreIdentityHash && assignment.Durability == authorization.Durability && assignment.PublicMember.ID == assignment.NodeID
}

func (e *FencingExecutor) readMembershipRepair(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairStatusRequest) (fencing.MembershipRepairStatusResult, error) {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairStatusResult{}, err
	}
	view, err := state.Ledger.ReadMembershipRepair(caller)
	if err != nil {
		if errors.Is(err, fencing.ErrNotFound) {
			return e.readMembershipRepairSourceOrEmpty(ctx, caller, binding, request.TargetPodName)
		}
		return fencing.MembershipRepairStatusResult{}, err
	}
	if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
		return fencing.MembershipRepairStatusResult{}, err
	}
	if request.TargetPodName != "" {
		// An exhausted repair is not an active assignment for discovery. Keep
		// the read path side-effect-free while allowing a voter to form the
		// exact first reserve request from retained slot evidence.
		if view.Progress.Stage == fencing.MembershipRepairComplete || view.Progress.Stage == fencing.MembershipRepairCancelled {
			return e.readMembershipRepairSourceOrEmpty(ctx, caller, binding, request.TargetPodName)
		}
	}
	return fencing.MembershipRepairStatusResult{Repair: &view}, nil
}

func (e *FencingExecutor) readMembershipRepairSourceOrEmpty(ctx context.Context, caller fencing.Identity, binding peerClosureBinding, targetPodName string) (fencing.MembershipRepairStatusResult, error) {
	if targetPodName == "" {
		return fencing.MembershipRepairStatusResult{}, nil
	}
	observation, err := e.membershipRepairSourceObservation(ctx, caller, binding, targetPodName)
	if err != nil {
		return fencing.MembershipRepairStatusResult{}, err
	}
	return fencing.MembershipRepairStatusResult{Observation: observation}, nil
}

func (e *FencingExecutor) membershipRepairSourceObservation(ctx context.Context, caller fencing.Identity, binding peerClosureBinding, targetPodName string) (*fencing.MembershipRepairSourceObservation, error) {
	if !identifier.MatchString(targetPodName) || targetPodName == caller.PodName {
		return nil, fencing.ErrPending
	}
	control, controlVersion, err := e.controller().loadAuto(ctx, e.controller().autoKey(&binding.root))
	if err != nil || control.BindingUID != binding.rootUID || control.BindingHash != autoBinding(&binding.root) || control.BindingVersion != autoBindingVersion || control.LegacyBindingHash != "" || control.StatefulSetUID != binding.workloadUID || control.ActiveClusterID != caller.SourceClusterID {
		return nil, fencing.ErrPending
	}
	controlHash := hashJSON([]any{control.Version, control.BindingUID, control.BindingHash, control.BindingVersion, control.StatefulSetUID, control.ActiveClusterID})
	recordHash, err := e.controller().readAutoBinding(ctx, &binding.root, control, binding.workloadUID)
	if err != nil || controlHash != binding.autoControl || recordHash != binding.autoBinding {
		return nil, fencing.ErrPending
	}
	current, pending, abortSlot, err := e.membershipRepairArchiveState(ctx, binding, fencing.MembershipRepairAuthorization{SourceClusterID: caller.SourceClusterID})
	if err != nil || pending.ConfigID != 0 || current.ConfigID == 0 {
		return nil, fencing.ErrPending
	}
	if changed, projectionErr := e.refreshCompletedRepairProjection(ctx, binding, control, controlVersion, caller.SourceClusterID, &current, targetPodName); projectionErr != nil {
		return nil, projectionErr
	} else if changed {
		return nil, fencing.ErrPending
	}
	var targetNode string
	for _, voter := range control.Voters {
		if voter.Pod != targetPodName {
			continue
		}
		if targetNode != "" || voter.NodeID == "" {
			return nil, fencing.ErrPending
		}
		targetNode = string(voter.NodeID)
	}
	old, ok := control.Identities[quepaxa.NodeID(targetNode)]
	if targetNode == "" || !ok || old.NodeID != targetNode || old.Pod != targetPodName || old.PodUID == "" || old.WALIdentity == "" || !repairMembershipContains(current, targetNode, old.WALIdentity) {
		return nil, fencing.ErrPending
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", targetPodName), &pod); err != nil {
		return nil, fencing.ErrPending
	}
	replacementUID := str(nested(pod, "metadata", "uid"))
	if replacementUID == "" || replacementUID == old.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil || str(nested(pod, "metadata", "name")) != targetPodName {
		return nil, fencing.ErrPending
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return nil, fencing.ErrPending
	}
	owner := asObject(refs[0])
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["uid"] != binding.workloadUID || owner["controller"] != true || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != binding.rootUID {
		return nil, fencing.ErrPending
	}
	if err := validatePreparePod(pod, replacementUID); err != nil {
		return nil, fencing.ErrPending
	}
	return &fencing.MembershipRepairSourceObservation{
		ConfigID: uint64(current.ConfigID), AbortSlot: uint64(abortSlot), TargetPodName: targetPodName,
		TargetNodeID: targetNode, TargetPodUID: old.PodUID, TargetWALIdentity: old.WALIdentity,
		ReplacementPodUID: replacementUID, MembershipHash: hashJSON(current.Members),
	}, nil
}

// refreshCompletedRepairProjection reconciles the existing automatic-control
// voter/identity projection from accepted, retained repair completion records.
// It never creates a repair record or infers completion from Pod/status data.
func (e *FencingExecutor) refreshCompletedRepairProjection(ctx context.Context, binding peerClosureBinding, control *autoControl, version *objstore.ObjectVersion, source string, current *quepaxa.Cluster, podName string) (bool, error) {
	ledgerState, ledgerUID, _, err := e.readWithUID(ctx, binding.workloadUID, source)
	if err != nil {
		return false, err
	}
	pin, err := e.controller().readBootstrapLedgerPin(ctx, &binding.root)
	if err != nil || pin.StatefulSetUID != binding.workloadUID || pin.SourceClusterID != source || pin.LedgerUID != ledgerUID {
		return false, fencing.ErrPending
	}
	records := make([]fencing.MembershipRepairRecord, 0, 1+len(ledgerState.Ledger.MembershipRepairHistory))
	if ledgerState.Ledger.MembershipRepair != nil {
		records = append(records, *ledgerState.Ledger.MembershipRepair)
	}
	for _, record := range ledgerState.Ledger.MembershipRepairHistory {
		records = append(records, record)
	}
	var selected *fencing.MembershipRepairCandidate
	var selectedMember quepaxa.Member
	var selectedConfig uint
	for i := range records {
		record := records[i]
		candidate, member, configID, ok := completedRepairProjection(record, binding.rootUID, binding.workloadUID, ledgerUID, source, podName)
		if !ok || configID > uint(current.ConfigID) || !membershipHasExactMember(*current, member) {
			continue
		}
		if selected == nil || configID > selectedConfig {
			copy := candidate
			selected, selectedMember, selectedConfig = &copy, member, configID
		}
	}
	if selected == nil {
		return false, nil
	}
	if len(current.Members) != len(control.Voters) {
		return false, fencing.ErrPending
	}
	for _, voter := range control.Voters {
		if voter.Pod == podName {
			continue
		}
		if !repairMembershipContains(*current, string(voter.NodeID), control.Identities[voter.NodeID].WALIdentity) {
			return false, fencing.ErrPending
		}
	}
	if selected.Assignment.PodUID == "" || selected.Assignment.PodName != podName || selectedMember.ID == "" {
		return false, fencing.ErrPending
	}
	newID := quepaxa.NodeID(selectedMember.ID)
	for _, voter := range control.Voters {
		if voter.Pod != podName && voter.NodeID == newID {
			return false, fencing.ErrPending
		}
	}
	idx := -1
	for i, voter := range control.Voters {
		if voter.Pod == podName {
			if idx != -1 {
				return false, fencing.ErrPending
			}
			idx = i
		}
	}
	if idx == -1 {
		return false, fencing.ErrPending
	}
	oldID := control.Voters[idx].NodeID
	target := FenceTarget{NodeID: string(newID), Pod: podName, PodUID: selected.Assignment.PodUID, WALIdentity: selectedMember.WALIdentity}
	if oldID == newID && control.Identities[newID] == target {
		return false, nil
	}
	delete(control.Identities, oldID)
	control.Voters[idx] = VoterPod{NodeID: newID, Pod: podName}
	control.Identities[newID] = target
	controller := e.controller()
	if err := controller.saveAuto(ctx, &binding.root, control, version); err != nil {
		return false, err
	}
	return true, nil
}

func completedRepairProjection(record fencing.MembershipRepairRecord, rootUID, statefulSetUID, ledgerUID, source, podName string) (fencing.MembershipRepairCandidate, quepaxa.Member, uint, bool) {
	auth := record.Authorization
	if auth.Validate() != nil || auth.RootUID != rootUID || auth.StatefulSetUID != statefulSetUID || auth.LedgerUID != ledgerUID || auth.SourceClusterID != source || record.Progress.Stage != fencing.MembershipRepairComplete {
		return fencing.MembershipRepairCandidate{}, quepaxa.Member{}, 0, false
	}
	copyLedger := fencing.Ledger{SourceClusterID: source, MembershipRepair: &record}
	copyLedger.MembershipRepair.Progress.Stage = fencing.MembershipRepairAdded
	if err := copyLedger.AdvanceMembershipRepair(auth.OperationID, record.Progress.Revision, fencing.MembershipRepairComplete); err != nil {
		return fencing.MembershipRepairCandidate{}, quepaxa.Member{}, 0, false
	}
	completion := record.Progress.Completion
	if completion == nil || completion.Accepted == nil {
		return fencing.MembershipRepairCandidate{}, quepaxa.Member{}, 0, false
	}
	request, response := completion.Request, *completion.Accepted
	digest, err := network.MembershipRepairCompletionRequestDigest(request)
	if err != nil || network.ValidateMembershipRepairCompletionRequest(request) != nil || request.RepairOperationID != auth.OperationID || request.RepairBindingDigest != fencing.MembershipRepairAuthorizationHash(auth) ||
		response.Outcome != "completed" || response.MAC == "" || response.RequestDigest != digest || response.ScopeDigest != request.ScopeDigest || response.TerminalHash != request.TerminalHash || response.TerminalSlot != request.TerminalSlot {
		return fencing.MembershipRepairCandidate{}, quepaxa.Member{}, 0, false
	}
	var candidate *fencing.MembershipRepairCandidate
	var phase *fencing.MembershipRepairPhase
	var result *fencing.MembershipRepairPhaseResult
	for i := range record.Progress.Candidates {
		if record.Progress.Candidates[i].Assignment.PodUID == request.CandidatePodUID {
			candidate = &record.Progress.Candidates[i]
		}
	}
	for i := range record.Progress.Phases {
		if record.Progress.Phases[i].Hash == request.PhaseHash {
			phase = &record.Progress.Phases[i]
		}
	}
	for i := range record.Progress.Results {
		if record.Progress.Results[i].PhaseHash == request.PhaseHash {
			result = &record.Progress.Results[i]
		}
	}
	view := fencing.MembershipRepairView{Authorization: auth, Progress: record.Progress, Preclosure: record.Preclosure}
	if candidate == nil || candidate.Member == nil || phase == nil || result == nil || phase.Kind != fencing.MembershipRepairAdd || !completionMatchesPhase(request, view, *phase, *result) ||
		candidate.Assignment.PodName != podName || candidate.Assignment.PodUID == auth.OldIdentity.PodUID || result.Outcome != "completed" || result.Target.ConfigID != auth.Membership.ConfigID+2 ||
		phase.Change.Add == nil || *phase.Change.Add != *candidate.Member {
		return fencing.MembershipRepairCandidate{}, quepaxa.Member{}, 0, false
	}
	return *candidate, *candidate.Member, result.Target.ConfigID, true
}

func membershipHasExactMember(cluster quepaxa.Cluster, member quepaxa.Member) bool {
	for _, current := range cluster.Members {
		if current.ID == member.ID {
			return current == member
		}
	}
	return false
}

func (e *FencingExecutor) authorizeMembershipRepairPhase(ctx context.Context, caller fencing.Identity, phase fencing.MembershipRepairPhase, workloadUID string) error {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return err
	}
	view, err := state.Ledger.ReadMembershipRepair(caller)
	if err != nil {
		return err
	}
	if !retainedRepairPhase(view.Progress.Phases, phase) {
		if err := e.retainMembershipRepairPhase(ctx, binding, caller, workloadUID, view, phase); err != nil {
			return err
		}
		state, err = e.read(ctx, workloadUID, caller.SourceClusterID)
		if err != nil {
			return err
		}
	}
	if err := state.Ledger.AuthorizeMembershipPhase(caller, phase); err != nil {
		return err
	}
	view, err = state.Ledger.ReadMembershipRepair(caller)
	if err != nil {
		return err
	}
	return e.verifyMembershipRepairArchive(ctx, binding, view)
}

func membershipRepairPhaseOperationID(repairID string, kind fencing.MembershipRepairPhaseKind) string {
	return "membership-repair-phase-" + shortID(hashJSON([]string{repairID, string(kind)}))
}

type certifiedRepairTransition struct {
	Transition quepaxa.ConfigTransition
	Target     quepaxa.Cluster
}

func (e *FencingExecutor) certifiedRepairTransitions(ctx context.Context, binding peerClosureBinding, source string) ([]certifiedRepairTransition, error) {
	initialMembers, err := recoveryMembers(binding.environment["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(initialMembers) == 0 {
		return nil, fencing.ErrPending
	}
	archive := recovery.NewManager(e.Bucket, path.Join(e.Prefix, source), 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return nil, err
	}
	tip := archive.Tip()
	if tip == 0 {
		return nil, fencing.ErrPending
	}
	dir, err := os.MkdirTemp("", ".rhiza-membership-repair-history-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	wal, err := qlog.Open(dir)
	if err != nil {
		return nil, err
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "rhiza-membership-repair-history", Cluster: quepaxa.Cluster{ConfigID: 1, Members: initialMembers}, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		return nil, err
	}
	if err := e.initializeMembershipRepairObserver(ctx, binding, source, initialMembers, archive, core); err != nil {
		return nil, err
	}
	readPage := func(pageCtx context.Context, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		return archive.DecisionsFrom(pageCtx, from, limit)
	}
	if err := recovery.ReplayCertifiedThrough(ctx, core, tip, readPage); err != nil {
		return nil, err
	}
	history, err := core.MembershipHistory()
	if err != nil {
		return nil, err
	}
	if err := archive.Load(ctx); err != nil || archive.Tip() != tip {
		return nil, fencing.ErrPending
	}
	transitions := make([]certifiedRepairTransition, 0, len(history.Transitions))
	for _, transition := range history.Transitions {
		// A terminal is the final slot under the old configuration; the new
		// target becomes active at the following slot.
		transitions = append(transitions, certifiedRepairTransition{Transition: transition, Target: core.ClusterForSlot(transition.Terminal.Slot + 1)})
	}
	return transitions, nil
}

func (e *FencingExecutor) observeMembershipRepairPhase(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairPhaseObserveRequest) (fencing.MembershipRepairPhaseObserveResult, error) {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	view, err := state.Ledger.ReadMembershipRepair(caller)
	if err != nil || view.Authorization.OperationID != request.OperationID {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	var phase *fencing.MembershipRepairPhase
	var candidate *fencing.MembershipRepairCandidate
	for i := range view.Progress.Phases {
		if view.Progress.Phases[i].Hash == request.PhaseHash {
			phase = &view.Progress.Phases[i]
			break
		}
	}
	if phase == nil || phase.Change.OperationID != request.JournalOperationID || phase.Change.ClusterID != caller.SourceClusterID ||
		request.JournalRequestHash != hashJSON(phase.Change) || request.TerminalSlot == 0 {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	for i := range view.Progress.Candidates {
		if view.Progress.Candidates[i].Assignment.PodUID == phase.CandidatePodUID {
			candidate = &view.Progress.Candidates[i]
			break
		}
	}
	if candidate == nil || candidate.Member == nil || phase.Validate(view.Authorization, *candidate) != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	expected, err := expectedRepairPhaseTarget(view.Authorization.Membership, view.Progress.Phases, *phase)
	if err != nil || !reflect.DeepEqual(request.Target, expected) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	for _, retained := range view.Progress.Results {
		if retained.PhaseHash == phase.Hash {
			if retained.JournalOperationID != request.JournalOperationID || retained.JournalRequestHash != request.JournalRequestHash ||
				retained.TerminalSlot != request.TerminalSlot || !reflect.DeepEqual(retained.Target, request.Target) {
				return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
			}
			return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: phase.Hash, Stage: view.Progress.Stage, Complete: view.Progress.Stage == fencing.MembershipRepairComplete}, nil
		}
	}
	transitions, err := e.certifiedRepairTransitions(ctx, binding, caller.SourceClusterID)
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	var certified *certifiedRepairTransition
	for i := range transitions {
		if transitions[i].Transition.Terminal.Slot == quepaxa.Slot(request.TerminalSlot) {
			certified = &transitions[i]
			break
		}
	}
	if certified == nil || certified.Transition.Freeze.Slot == 0 || certified.Transition.Terminal.Slot != certified.Transition.Freeze.Slot+16 ||
		!reflect.DeepEqual(certified.Target, expected) || certified.Transition.Freeze.Hash == ([32]byte{}) || certified.Transition.Terminal.Hash == ([32]byte{}) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	current, pending, abort, err := e.membershipRepairArchiveState(ctx, binding, view.Authorization)
	if err != nil || pending.ConfigID != 0 || abort != 0 {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	if phase.Kind == fencing.MembershipRepairRemove && !reflect.DeepEqual(current, expected) || phase.Kind == fencing.MembershipRepairAdd && !reflect.DeepEqual(current, expected) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	result := fencing.MembershipRepairPhaseResult{PhaseHash: phase.Hash, JournalOperationID: request.JournalOperationID,
		JournalRequestHash: request.JournalRequestHash, Target: expected, FreezeSlot: uint64(certified.Transition.Freeze.Slot),
		FreezeHash: fmt.Sprintf("%x", certified.Transition.Freeze.Hash[:]), TerminalSlot: uint64(certified.Transition.Terminal.Slot),
		TerminalHash: fmt.Sprintf("%x", certified.Transition.Terminal.Hash[:]), Outcome: "completed"}
	_, err = e.changeExisting(ctx, workloadUID, caller.SourceClusterID, view.Authorization.LedgerUID, func(l *fencing.Ledger) error {
		if l.MembershipRepair == nil || !reflect.DeepEqual(l.MembershipRepair.Authorization, view.Authorization) || !reflect.DeepEqual(l.MembershipRepair.Progress, view.Progress) ||
			l.Runs[caller.RunID].Identity != caller || !reflect.DeepEqual(l.MembershipRepair.Preclosure, view.Preclosure) {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		record := l.MembershipRepair
		var next fencing.MembershipRepairStage
		if phase.Kind == fencing.MembershipRepairRemove {
			if record.Progress.Stage != fencing.MembershipRepairOldClosed {
				return fencing.ErrPending
			}
			next = fencing.MembershipRepairRemoved
		} else {
			if record.Progress.Stage != fencing.MembershipRepairRemoved {
				return fencing.ErrPending
			}
			next = fencing.MembershipRepairAdded
		}
		if err := l.AdvanceMembershipRepair(request.OperationID, record.Progress.Revision, next); err != nil {
			return err
		}
		record.Progress.Results = append(record.Progress.Results, result)
		record.Progress.Revision++
		return nil
	})
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	stage := fencing.MembershipRepairRemoved
	if phase.Kind == fencing.MembershipRepairAdd {
		stage = fencing.MembershipRepairAdded
	}
	return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: phase.Hash, Stage: stage}, nil
}

func expectedRepairPhaseTarget(source quepaxa.Cluster, phases []fencing.MembershipRepairPhase, phase fencing.MembershipRepairPhase) (quepaxa.Cluster, error) {
	prior := source
	if phase.Kind == fencing.MembershipRepairAdd {
		if len(phases) != 2 || phases[0].Kind != fencing.MembershipRepairRemove || phases[1].Hash != phase.Hash {
			return quepaxa.Cluster{}, fencing.ErrPending
		}
		var err error
		prior, err = applyRepairMembershipPhase(source, phases[0])
		if err != nil {
			return quepaxa.Cluster{}, err
		}
	}
	return applyRepairMembershipPhase(prior, phase)
}

func (e *FencingExecutor) completeMembershipRepairPhase(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairPhaseObserveRequest) (fencing.MembershipRepairPhaseObserveResult, error) {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	record := state.Ledger.MembershipRepair
	if record == nil || record.Authorization.OperationID != request.OperationID {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	view := fencing.MembershipRepairView{Authorization: record.Authorization, Progress: record.Progress, Preclosure: record.Preclosure}
	var phase *fencing.MembershipRepairPhase
	var candidate *fencing.MembershipRepairCandidate
	for i := range view.Progress.Phases {
		if view.Progress.Phases[i].Hash == request.PhaseHash {
			phase = &view.Progress.Phases[i]
		}
	}
	if phase == nil || phase.Kind != fencing.MembershipRepairAdd || phase.Change.OperationID != request.JournalOperationID || request.JournalRequestHash != hashJSON(phase.Change) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	for i := range view.Progress.Candidates {
		if view.Progress.Candidates[i].Assignment.PodUID == phase.CandidatePodUID {
			candidate = &view.Progress.Candidates[i]
		}
	}
	if candidate == nil || candidate.Member == nil || phase.Change.Add == nil || *phase.Change.Add != *candidate.Member {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	var phaseResult *fencing.MembershipRepairPhaseResult
	for i := range view.Progress.Results {
		if view.Progress.Results[i].PhaseHash == phase.Hash {
			phaseResult = &view.Progress.Results[i]
			break
		}
	}
	if phaseResult == nil || phaseResult.Outcome != "completed" || phaseResult.JournalOperationID != request.JournalOperationID ||
		phaseResult.JournalRequestHash != request.JournalRequestHash || phaseResult.TerminalSlot != request.TerminalSlot ||
		!reflect.DeepEqual(phaseResult.Target, request.Target) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	if view.Progress.Stage == fencing.MembershipRepairComplete {
		if view.Progress.Completion == nil || view.Progress.Completion.Accepted == nil ||
			!completionMatchesPhase(view.Progress.Completion.Request, view, *phase, *phaseResult) {
			return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
		}
		return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: request.PhaseHash, Stage: fencing.MembershipRepairComplete, Complete: true}, nil
	}
	if view.Progress.Stage != fencing.MembershipRepairAdded {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	current, pending, abort, err := e.membershipRepairArchiveState(ctx, binding, view.Authorization)
	if err != nil || pending.ConfigID != 0 || abort != 0 || !reflect.DeepEqual(current, request.Target) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	addSource, err := expectedRepairPhaseSource(view.Authorization.Membership, view.Progress.Phases, *phase)
	if err != nil || len(addSource.Members) < 2 || !sameVoterInBoth(addSource, request.Target, caller) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	var candidateRun *fencing.Run
	for _, run := range state.Ledger.Runs {
		identity := run.Identity
		if identity.PodUID == candidate.Assignment.PodUID {
			if candidateRun != nil && candidateRun.Identity != identity {
				return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
			}
			copy := run
			candidateRun = &copy
		}
	}
	if candidateRun == nil || candidateRun.Exited || candidateRun.Stop != nil || candidateRun.Drain != nil ||
		candidateRun.Identity.NodeID != string(candidate.Member.ID) || candidateRun.Identity.WALIdentity != candidate.Member.WALIdentity ||
		candidateRun.Identity.SourceClusterID != view.Authorization.SourceClusterID || !state.Ledger.BootstrapInitReady(candidateRun.Identity) {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	completion, err := e.retainMembershipRepairCompletion(ctx, workloadUID, caller, view, *phase, *phaseResult, candidate.Assignment, caller)
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	if completion.Accepted != nil {
		return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: request.PhaseHash, Stage: fencing.MembershipRepairComplete, Complete: true}, nil
	}
	executor, executorBinding, err := e.resolveMembershipRepairCompletionExecutor(ctx, caller, workloadUID, binding, view.Authorization, *phase, request.Target, completion.Request)
	if err != nil {
		if !e.membershipRepairCompletionExecutorRetired(ctx, workloadUID, caller.SourceClusterID, view.Authorization, completion.Request) {
			return fencing.MembershipRepairPhaseObserveResult{}, err
		}
		completion, err = e.supersedeMembershipRepairCompletion(ctx, workloadUID, caller, view, *phase, *phaseResult)
		if err != nil {
			return fencing.MembershipRepairPhaseObserveResult{}, err
		}
		executor, executorBinding, err = e.resolveMembershipRepairCompletionExecutor(ctx, caller, workloadUID, binding, view.Authorization, *phase, request.Target, completion.Request)
		if err != nil {
			return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
		}
	}
	proof, err := e.requestMembershipRepairCompletion(ctx, executorBinding, completion.Request)
	if err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, fencing.ErrPending
	}
	if err := e.verifyMembershipRepairCompletion(ctx, executor, workloadUID, executorBinding, view, *phase, *phaseResult, completion.Request, proof); err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	if err := e.validateMembershipRepairCompletionBindings(ctx, workloadUID, view, *candidate, *candidateRun); err != nil {
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	_, err = e.changeExisting(ctx, workloadUID, caller.SourceClusterID, view.Authorization.LedgerUID, func(l *fencing.Ledger) error {
		if l.MembershipRepair == nil || !reflect.DeepEqual(l.MembershipRepair.Authorization, view.Authorization) ||
			l.Runs[caller.RunID].Identity != caller || !retainedRepairPhase(l.MembershipRepair.Progress.Phases, *phase) {
			return fencing.ErrPending
		}
		if l.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdded || l.MembershipRepair.Progress.Completion == nil ||
			!reflect.DeepEqual(l.MembershipRepair.Progress.Completion.Request, completion.Request) || l.MembershipRepair.Progress.Completion.Accepted != nil {
			return fencing.ErrPending
		}
		currentCandidate, ok := l.Runs[candidateRun.Identity.RunID]
		if !ok || currentCandidate.Identity != candidateRun.Identity || currentCandidate.Exited || currentCandidate.Stop != nil || currentCandidate.Drain != nil ||
			currentCandidate.Identity.WALIdentity != candidate.Member.WALIdentity || !l.BootstrapInitReady(currentCandidate.Identity) {
			return fencing.ErrPending
		}
		currentExecutor, ok := l.Runs[executor.RunID]
		if !ok || currentExecutor.Identity != executor || currentExecutor.Exited || currentExecutor.Stop != nil || currentExecutor.Drain != nil {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		record := l.MembershipRepair
		accepted := proof
		record.Progress.Completion.Accepted = &accepted
		return l.AdvanceMembershipRepair(request.OperationID, record.Progress.Revision, fencing.MembershipRepairComplete)
	})
	if err != nil {
		latest, _, _, readErr := e.readWithUID(ctx, workloadUID, caller.SourceClusterID)
		if readErr == nil && latest.Ledger.MembershipRepair != nil && latest.Ledger.MembershipRepair.Progress.Stage == fencing.MembershipRepairComplete &&
			latest.Ledger.MembershipRepair.Progress.Completion != nil && latest.Ledger.MembershipRepair.Progress.Completion.Accepted != nil &&
			reflect.DeepEqual(*latest.Ledger.MembershipRepair.Progress.Completion.Accepted, proof) {
			return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: request.PhaseHash, Stage: fencing.MembershipRepairComplete, Complete: true}, nil
		}
		return fencing.MembershipRepairPhaseObserveResult{}, err
	}
	return fencing.MembershipRepairPhaseObserveResult{OperationID: request.OperationID, PhaseHash: request.PhaseHash, Stage: fencing.MembershipRepairComplete, Complete: true}, nil
}

func expectedRepairPhaseSource(source quepaxa.Cluster, phases []fencing.MembershipRepairPhase, phase fencing.MembershipRepairPhase) (quepaxa.Cluster, error) {
	if phase.Kind != fencing.MembershipRepairAdd || len(phases) != 2 || phases[0].Kind != fencing.MembershipRepairRemove || phases[1].Hash != phase.Hash {
		return quepaxa.Cluster{}, fencing.ErrPending
	}
	return applyRepairMembershipPhase(source, phases[0])
}

func sameVoterInBoth(source, target quepaxa.Cluster, caller fencing.Identity) bool {
	var sourceMember, targetMember *quepaxa.Member
	for i := range source.Members {
		if string(source.Members[i].ID) == caller.NodeID {
			member := source.Members[i]
			sourceMember = &member
		}
	}
	for i := range target.Members {
		if string(target.Members[i].ID) == caller.NodeID {
			member := target.Members[i]
			targetMember = &member
		}
	}
	return sourceMember != nil && targetMember != nil && sourceMember.PublicKey == targetMember.PublicKey &&
		sourceMember.WALIdentity == caller.WALIdentity && targetMember.WALIdentity == caller.WALIdentity && *sourceMember == *targetMember
}

func membershipRepairCompletionScopeDigest(request network.MembershipRepairCompletionRequest) string {
	return fencing.MembershipRepairCompletionScopeDigest(request)
}

func completionMatchesPhase(request network.MembershipRepairCompletionRequest, view fencing.MembershipRepairView, phase fencing.MembershipRepairPhase, result fencing.MembershipRepairPhaseResult) bool {
	if phase.AbortSlot == nil || request.RepairOperationID != view.Authorization.OperationID || request.RepairBindingDigest != membershipRepairAuthorizationHash(view.Authorization) ||
		request.PhaseHash != phase.Hash || request.JournalOperationID != phase.Change.OperationID || request.JournalRequestHash != hashJSON(phase.Change) ||
		request.SourceClusterID != phase.Change.ClusterID || request.SourceConfigID != phase.Change.ExpectedConfigID || request.AbortSlot == nil ||
		*request.AbortSlot != *phase.AbortSlot || request.FreezeSlot != result.FreezeSlot || request.FreezeHash != result.FreezeHash ||
		request.TerminalSlot != result.TerminalSlot || request.TerminalHash != result.TerminalHash || !reflect.DeepEqual(request.Target, result.Target) ||
		request.CandidatePodUID != phase.CandidatePodUID || phase.Change.Add == nil || request.CandidateMember != *phase.Change.Add ||
		request.CandidateBindRef != completionCandidateBindRef(view, phase.CandidatePodUID) || request.ExecutorNodeID == "" || request.ExecutorPodUID == "" ||
		request.ExecutorRunID == "" || request.ExecutorWALIdentity == "" || request.ExecutorNodeID == string(request.CandidateMember.ID) {
		return false
	}
	return request.ScopeDigest == membershipRepairCompletionScopeDigest(request)
}

func completionCandidateBindRef(view fencing.MembershipRepairView, podUID string) string {
	for _, candidate := range view.Progress.Candidates {
		if candidate.Assignment.PodUID == podUID {
			return fencing.BootstrapAssignmentDigest(candidate.Assignment)
		}
	}
	return ""
}

func (e *FencingExecutor) retainMembershipRepairCompletion(ctx context.Context, workloadUID string, caller fencing.Identity, view fencing.MembershipRepairView, phase fencing.MembershipRepairPhase, result fencing.MembershipRepairPhaseResult, assignment fencing.BootstrapAssignment, executor fencing.Identity) (fencing.MembershipRepairCompletion, error) {
	if view.Progress.Completion != nil {
		attempt := *view.Progress.Completion
		if !completionMatchesPhase(attempt.Request, view, phase, result) {
			return fencing.MembershipRepairCompletion{}, fencing.ErrPending
		}
		return attempt, nil
	}
	challengeBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		return fencing.MembershipRepairCompletion{}, err
	}
	challenge := hex.EncodeToString(challengeBytes)
	abort := *phase.AbortSlot
	candidate := *phase.Change.Add
	request := network.MembershipRepairCompletionRequest{
		Version: network.RecoveryProbeProtocolVersion, Action: network.MembershipRepairCompletionAction, Challenge: challenge,
		AttemptRevision: view.Progress.Revision + 1, RepairOperationID: view.Authorization.OperationID,
		RepairBindingDigest: membershipRepairAuthorizationHash(view.Authorization), PhaseHash: phase.Hash,
		JournalOperationID: phase.Change.OperationID, JournalRequestHash: hashJSON(phase.Change), SourceClusterID: phase.Change.ClusterID,
		SourceConfigID: phase.Change.ExpectedConfigID, AbortSlot: &abort, FreezeSlot: result.FreezeSlot, FreezeHash: result.FreezeHash,
		TerminalSlot: result.TerminalSlot, TerminalHash: result.TerminalHash, Target: result.Target, CandidatePodUID: phase.CandidatePodUID,
		CandidateMember: candidate, CandidateBindRef: fencing.BootstrapAssignmentDigest(assignment),
		ExecutorNodeID: executor.NodeID, ExecutorPodUID: executor.PodUID, ExecutorRunID: executor.RunID, ExecutorWALIdentity: executor.WALIdentity,
	}
	request.ScopeDigest = membershipRepairCompletionScopeDigest(request)
	if err := network.ValidateMembershipRepairCompletionRequest(request); err != nil {
		return fencing.MembershipRepairCompletion{}, err
	}
	retained := fencing.MembershipRepairCompletion{Request: request}
	_, err := e.changeExisting(ctx, workloadUID, caller.SourceClusterID, view.Authorization.LedgerUID, func(l *fencing.Ledger) error {
		if l.MembershipRepair == nil || !reflect.DeepEqual(l.MembershipRepair.Authorization, view.Authorization) ||
			l.MembershipRepair.Progress.Revision != view.Progress.Revision || l.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdded ||
			l.Runs[caller.RunID].Identity != caller || l.MembershipRepair.Progress.Completion != nil {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		l.MembershipRepair.Progress.Completion = &retained
		l.MembershipRepair.Progress.Revision++
		return nil
	})
	if err != nil {
		// A competing eligible driver may have won the challenge CAS, or the
		// successful write response may have been lost. Reuse only that exact
		// retained scope; never mint a second concurrent challenge.
		latest, latestUID, _, readErr := e.readWithUID(ctx, workloadUID, caller.SourceClusterID)
		if readErr == nil && latestUID == view.Authorization.LedgerUID && latest.Ledger.MembershipRepair != nil &&
			reflect.DeepEqual(latest.Ledger.MembershipRepair.Authorization, view.Authorization) && latest.Ledger.MembershipRepair.Progress.Stage == fencing.MembershipRepairAdded &&
			latest.Ledger.MembershipRepair.Progress.Completion != nil && latest.Ledger.MembershipRepair.Progress.Completion.Accepted == nil &&
			completionMatchesPhase(latest.Ledger.MembershipRepair.Progress.Completion.Request, view, phase, result) {
			return *latest.Ledger.MembershipRepair.Progress.Completion, nil
		}
		return fencing.MembershipRepairCompletion{}, err
	}
	return retained, nil
}

func (e *FencingExecutor) requestMembershipRepairCompletion(ctx context.Context, binding peerClosureBinding, completion network.MembershipRepairCompletionRequest) (network.MembershipRepairCompletionResponse, error) {
	if err := network.ValidateMembershipRepairCompletionRequest(completion); err != nil {
		return network.MembershipRepairCompletionResponse{}, err
	}
	endpoint, err := podEndpoint(binding.callerPod, binding.root.Spec.Container, "/recovery/probe")
	if err != nil {
		return network.MembershipRepairCompletionResponse{}, err
	}
	body, err := json.Marshal(completion)
	if err != nil {
		return network.MembershipRepairCompletionResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return network.MembershipRepairCompletionResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+binding.environment["RHIZA_ADMIN_TOKEN"])
	request.Header.Set("Content-Type", "application/json")
	response, err := e.controller().client().Do(request)
	if err != nil {
		return network.MembershipRepairCompletionResponse{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || response.StatusCode != http.StatusOK || len(data) > 64<<10 {
		return network.MembershipRepairCompletionResponse{}, fmt.Errorf("membership-repair runtime completion returned an invalid response")
	}
	return network.StrictDecodeMembershipRepairCompletionResponse(data, completion, binding.environment["RHIZA_ADMIN_TOKEN"])
}

// resolveMembershipRepairCompletionExecutor treats the retained executor as a
// shared challenge target, not as a driver lease. Any eligible voter can drive
// the same exact attempt, but only the exact retained live voter can execute it.
func (e *FencingExecutor) resolveMembershipRepairCompletionExecutor(ctx context.Context, driver fencing.Identity, workloadUID string, driverBinding peerClosureBinding,
	authorization fencing.MembershipRepairAuthorization, phase fencing.MembershipRepairPhase, target quepaxa.Cluster, request network.MembershipRepairCompletionRequest) (fencing.Identity, peerClosureBinding, error) {
	state, ledgerUID, _, err := e.readWithUID(ctx, workloadUID, driver.SourceClusterID)
	if err != nil || ledgerUID != authorization.LedgerUID || state.Ledger.MembershipRepair == nil ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Authorization, authorization) {
		return fencing.Identity{}, peerClosureBinding{}, fencing.ErrPending
	}
	run, ok := state.Ledger.Runs[request.ExecutorRunID]
	identity := run.Identity
	if !ok || run.Exited || run.Stop != nil || run.Drain != nil || identity.SourceClusterID != driver.SourceClusterID || identity.NodeID != request.ExecutorNodeID ||
		identity.PodUID != request.ExecutorPodUID || identity.RunID != request.ExecutorRunID || identity.WALIdentity != request.ExecutorWALIdentity {
		return fencing.Identity{}, peerClosureBinding{}, fencing.ErrPending
	}
	addSource, err := expectedRepairPhaseSource(authorization.Membership, state.Ledger.MembershipRepair.Progress.Phases, phase)
	if err != nil || len(addSource.Members) < 2 || !sameVoterInBoth(addSource, target, identity) {
		return fencing.Identity{}, peerClosureBinding{}, fencing.ErrPending
	}
	binding, liveState, err := e.membershipRepairCaller(ctx, identity, workloadUID)
	if err != nil || liveState.Ledger.Runs[identity.RunID].Identity != identity || binding.rootUID != driverBinding.rootUID ||
		binding.workloadUID != driverBinding.workloadUID || binding.rootHash != driverBinding.rootHash || binding.autoControl != driverBinding.autoControl ||
		binding.autoBinding != driverBinding.autoBinding {
		return fencing.Identity{}, peerClosureBinding{}, fencing.ErrPending
	}
	return identity, binding, nil
}

func (e *FencingExecutor) membershipRepairCompletionExecutorRetired(ctx context.Context, workloadUID, source string, authorization fencing.MembershipRepairAuthorization, request network.MembershipRepairCompletionRequest) bool {
	state, ledgerUID, _, err := e.readWithUID(ctx, workloadUID, source)
	if err != nil || ledgerUID != authorization.LedgerUID || state.Ledger.MembershipRepair == nil ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Authorization, authorization) {
		return false
	}
	run, ok := state.Ledger.Runs[request.ExecutorRunID]
	if !ok || run.Identity.NodeID != request.ExecutorNodeID || run.Identity.PodUID != request.ExecutorPodUID ||
		run.Identity.RunID != request.ExecutorRunID || run.Identity.WALIdentity != request.ExecutorWALIdentity {
		return false
	}
	return run.Exited || run.Stop != nil || run.Drain != nil
}

func (e *FencingExecutor) supersedeMembershipRepairCompletion(ctx context.Context, workloadUID string, driver fencing.Identity, view fencing.MembershipRepairView,
	phase fencing.MembershipRepairPhase, result fencing.MembershipRepairPhaseResult) (fencing.MembershipRepairCompletion, error) {
	prior := view.Progress.Completion
	if prior == nil || prior.Accepted != nil || !completionMatchesPhase(prior.Request, view, phase, result) {
		return fencing.MembershipRepairCompletion{}, fencing.ErrPending
	}
	challenge := make([]byte, 32)
	if _, err := rand.Read(challenge); err != nil {
		return fencing.MembershipRepairCompletion{}, err
	}
	abort := *phase.AbortSlot
	request := prior.Request
	request.Challenge = hex.EncodeToString(challenge)
	request.AttemptRevision++
	request.ExecutorNodeID, request.ExecutorPodUID, request.ExecutorRunID, request.ExecutorWALIdentity = driver.NodeID, driver.PodUID, driver.RunID, driver.WALIdentity
	request.AbortSlot = &abort
	request.ScopeDigest = membershipRepairCompletionScopeDigest(request)
	if err := network.ValidateMembershipRepairCompletionRequest(request); err != nil {
		return fencing.MembershipRepairCompletion{}, fencing.ErrPending
	}
	next := fencing.MembershipRepairCompletion{Request: request}
	_, err := e.changeExisting(ctx, workloadUID, driver.SourceClusterID, view.Authorization.LedgerUID, func(l *fencing.Ledger) error {
		if l.MembershipRepair == nil || !reflect.DeepEqual(l.MembershipRepair.Authorization, view.Authorization) ||
			l.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdded || l.MembershipRepair.Progress.Completion == nil ||
			!reflect.DeepEqual(l.MembershipRepair.Progress.Completion.Request, prior.Request) || l.MembershipRepair.Progress.Completion.Accepted != nil ||
			!retainedRepairPhase(l.MembershipRepair.Progress.Phases, phase) || l.Runs[driver.RunID].Identity != driver ||
			l.Runs[driver.RunID].Exited || l.Runs[driver.RunID].Stop != nil || l.Runs[driver.RunID].Drain != nil {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		l.MembershipRepair.Progress.Completion = &next
		l.MembershipRepair.Progress.Revision++
		return nil
	})
	if err != nil {
		latest, latestUID, _, readErr := e.readWithUID(ctx, workloadUID, driver.SourceClusterID)
		if readErr == nil && latestUID == view.Authorization.LedgerUID && latest.Ledger.MembershipRepair != nil &&
			reflect.DeepEqual(latest.Ledger.MembershipRepair.Authorization, view.Authorization) && latest.Ledger.MembershipRepair.Progress.Stage == fencing.MembershipRepairAdded &&
			latest.Ledger.MembershipRepair.Progress.Completion != nil && latest.Ledger.MembershipRepair.Progress.Completion.Accepted == nil &&
			completionMatchesPhase(latest.Ledger.MembershipRepair.Progress.Completion.Request, view, phase, result) {
			return *latest.Ledger.MembershipRepair.Progress.Completion, nil
		}
		return fencing.MembershipRepairCompletion{}, err
	}
	return next, nil
}

func (e *FencingExecutor) verifyMembershipRepairCompletion(ctx context.Context, caller fencing.Identity, workloadUID string, binding peerClosureBinding, view fencing.MembershipRepairView,
	phase fencing.MembershipRepairPhase, result fencing.MembershipRepairPhaseResult, request network.MembershipRepairCompletionRequest, proof network.MembershipRepairCompletionResponse) error {
	if !completionMatchesPhase(request, view, phase, result) || request.ExecutorNodeID != caller.NodeID || request.ExecutorPodUID != caller.PodUID ||
		request.ExecutorRunID != caller.RunID || request.ExecutorWALIdentity != caller.WALIdentity {
		return fencing.ErrPending
	}
	currentBinding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil || currentBinding.rootUID != binding.rootUID || currentBinding.workloadUID != binding.workloadUID || currentBinding.rootHash != binding.rootHash ||
		currentBinding.autoControl != binding.autoControl || currentBinding.autoBinding != binding.autoBinding || state.Ledger.MembershipRepair == nil ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Authorization, view.Authorization) || state.Ledger.MembershipRepair.Progress.Completion == nil ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Progress.Completion.Request, request) {
		return fencing.ErrPending
	}
	transitions, err := e.certifiedRepairTransitions(ctx, currentBinding, caller.SourceClusterID)
	if err != nil {
		return err
	}
	var match *certifiedRepairTransition
	for i := range transitions {
		if uint64(transitions[i].Transition.Terminal.Slot) == request.TerminalSlot {
			match = &transitions[i]
			break
		}
	}
	if match == nil || uint64(match.Transition.Freeze.Slot) != request.FreezeSlot ||
		fmt.Sprintf("%x", match.Transition.Freeze.Hash[:]) != request.FreezeHash || fmt.Sprintf("%x", match.Transition.Terminal.Hash[:]) != request.TerminalHash ||
		!reflect.DeepEqual(match.Target, request.Target) || match.Target.ConfigID != request.SourceConfigID+1 {
		return fencing.ErrPending
	}
	current, pending, abort, err := e.membershipRepairArchiveState(ctx, currentBinding, view.Authorization)
	if err != nil || pending.ConfigID != 0 || abort != 0 || !reflect.DeepEqual(current, request.Target) {
		return fencing.ErrPending
	}
	if proof.Outcome != "completed" || proof.TerminalSlot != request.TerminalSlot || proof.TerminalHash != request.TerminalHash {
		return fencing.ErrPending
	}
	return nil
}

func (e *FencingExecutor) validateMembershipRepairCompletionBindings(ctx context.Context, workloadUID string, view fencing.MembershipRepairView, candidate fencing.MembershipRepairCandidate, run fencing.Run) error {
	identity := run.Identity
	if candidate.Assignment.Validate() != nil || candidate.Assignment.PodUID != identity.PodUID || candidate.Assignment.NodeID != identity.NodeID ||
		candidate.Assignment.PodName != fencing.PodNameForIdentity(identity) || identity.SourceClusterID != view.Authorization.SourceClusterID ||
		identity.CredentialUID != candidate.Assignment.CredentialUID || identity.CredentialName != candidate.Assignment.CredentialName ||
		identity.WALIdentity == "" || run.Exited || run.Stop != nil || run.Drain != nil || !repairCandidateInitReady(candidate) {
		return fencing.ErrPending
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", candidate.Assignment.PodName), &pod); err != nil ||
		str(nested(pod, "metadata", "uid")) != candidate.Assignment.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fencing.ErrPending
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return fencing.ErrPending
	}
	owner := asObject(refs[0])
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["uid"] != view.Authorization.StatefulSetUID ||
		owner["controller"] != true || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != view.Authorization.RootUID {
		return fencing.ErrPending
	}
	registration, err := e.incarnationCredential(ctx, identity, workloadUID, identity, false)
	if err != nil || registration.Identity != identity {
		return fencing.ErrPending
	}
	var member quepaxa.Member
	if err := json.Unmarshal(registration.Learner, &member); err != nil || candidate.Member == nil {
		return fencing.ErrPending
	}
	member.WALIdentity = identity.WALIdentity
	if member != *candidate.Member {
		return fencing.ErrPending
	}
	return nil
}

func (e *FencingExecutor) prepareMembershipRepairPhase(ctx context.Context, caller fencing.Identity, workloadUID string, request fencing.MembershipRepairPhasePrepareRequest) (fencing.MembershipRepairPhasePrepareResult, error) {
	binding, state, err := e.membershipRepairCaller(ctx, caller, workloadUID)
	if err != nil {
		return fencing.MembershipRepairPhasePrepareResult{}, err
	}
	view, err := state.Ledger.ReadMembershipRepair(caller)
	if err != nil || view.Authorization.OperationID != request.OperationID {
		return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
	}
	var candidate *fencing.MembershipRepairCandidate
	for i := range view.Progress.Candidates {
		if view.Preclosure != nil && view.Progress.Candidates[i].Assignment.PodUID == view.Preclosure.CandidatePodUID {
			candidate = &view.Progress.Candidates[i]
		}
	}
	if candidate == nil || candidate.Member == nil {
		return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
	}
	for _, prior := range view.Progress.Phases {
		if prior.Kind == request.Kind {
			if prior.CandidatePodUID != candidate.Assignment.PodUID || prior.Change.OperationID != membershipRepairPhaseOperationID(request.OperationID, request.Kind) {
				return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
			}
			return fencing.MembershipRepairPhasePrepareResult{Phase: prior}, nil
		}
	}
	phase := fencing.MembershipRepairPhase{Kind: request.Kind, CandidatePodUID: candidate.Assignment.PodUID}
	switch request.Kind {
	case fencing.MembershipRepairRemove:
		if view.Progress.Stage != fencing.MembershipRepairCandidateBound && view.Progress.Stage != fencing.MembershipRepairOldClosed || view.Preclosure == nil {
			return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
		}
		if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
			return fencing.MembershipRepairPhasePrepareResult{}, err
		}
		abort := uint64(*view.Authorization.ExpectedAbortSlot)
		closure := fencing.PeerClosureRequest{ExpectedConfigID: uint64(view.Authorization.Membership.ConfigID), ExpectedAbortSlot: &abort, TargetNodeID: view.Authorization.OldIdentity.NodeID, TargetWALIdentity: view.Authorization.OldIdentity.WALIdentity}
		receipt, err := e.readPeerClosure(ctx, caller, closure, workloadUID)
		if err != nil {
			return fencing.MembershipRepairPhasePrepareResult{}, err
		}
		abortValue := quepaxa.Slot(abort)
		phase.AbortSlot = &abortValue
		phase.Change = network.MembershipChange{
			OperationID: membershipRepairPhaseOperationID(request.OperationID, request.Kind), ClusterID: view.Authorization.SourceClusterID,
			ExpectedConfigID: view.Authorization.Membership.ConfigID, ExpectedAbortSlot: abortValue,
			Remove: quepaxa.NodeID(view.Authorization.OldIdentity.NodeID),
			Fence:  &network.MembershipFence{NodeID: quepaxa.NodeID(view.Authorization.OldIdentity.NodeID), WALIdentity: view.Authorization.OldIdentity.WALIdentity, WorkloadUID: workloadUID, Confirmed: true, Evidence: receipt.ProofID},
		}
	case fencing.MembershipRepairAdd:
		if view.Progress.Stage != fencing.MembershipRepairRemoved || len(view.Progress.Phases) != 1 || view.Progress.Phases[0].Kind != fencing.MembershipRepairRemove {
			return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
		}
		current, pending, abort, err := e.membershipRepairArchiveState(ctx, binding, view.Authorization)
		if err != nil || pending.ConfigID != 0 || uint64(current.ConfigID) != uint64(view.Authorization.Membership.ConfigID)+1 || abort != 0 {
			return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
		}
		priorTarget, err := applyRepairMembershipPhase(view.Authorization.Membership, view.Progress.Phases[0])
		if err != nil || !reflect.DeepEqual(current, priorTarget) {
			return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
		}
		abortValue := quepaxa.Slot(0)
		phase.AbortSlot = &abortValue
		member := *candidate.Member
		phase.Change = network.MembershipChange{OperationID: membershipRepairPhaseOperationID(request.OperationID, request.Kind), ClusterID: view.Authorization.SourceClusterID,
			ExpectedConfigID: current.ConfigID, ExpectedAbortSlot: abortValue, Add: &member}
	default:
		return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
	}
	phase.Hash, err = fencing.MembershipRepairPhaseHash(phase)
	if err != nil || phase.Validate(view.Authorization, *candidate) != nil {
		return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
	}
	if err := e.retainMembershipRepairPhase(ctx, binding, caller, workloadUID, view, phase); err != nil {
		return fencing.MembershipRepairPhasePrepareResult{}, err
	}
	latest, err := e.read(ctx, workloadUID, caller.SourceClusterID)
	if err != nil || latest.Ledger.MembershipRepair == nil || !retainedRepairPhase(latest.Ledger.MembershipRepair.Progress.Phases, phase) {
		return fencing.MembershipRepairPhasePrepareResult{}, fencing.ErrPending
	}
	return fencing.MembershipRepairPhasePrepareResult{Phase: phase}, nil
}

func retainedRepairPhase(phases []fencing.MembershipRepairPhase, phase fencing.MembershipRepairPhase) bool {
	for _, retained := range phases {
		if reflect.DeepEqual(retained, phase) {
			return true
		}
	}
	return false
}

func (e *FencingExecutor) retainMembershipRepairPhase(ctx context.Context, binding peerClosureBinding, caller fencing.Identity, workloadUID string, view fencing.MembershipRepairView, phase fencing.MembershipRepairPhase) error {
	var candidate *fencing.MembershipRepairCandidate
	for i := range view.Progress.Candidates {
		if view.Progress.Candidates[i].Assignment.PodUID == phase.CandidatePodUID {
			candidate = &view.Progress.Candidates[i]
			break
		}
	}
	if candidate == nil || phase.Validate(view.Authorization, *candidate) != nil {
		return fencing.ErrPending
	}
	current, pending, abortSlot, err := e.membershipRepairArchiveState(ctx, binding, view.Authorization)
	if err != nil {
		return err
	}
	if err := validateRepairArchiveProgress(view, current, pending, abortSlot); err != nil {
		return err
	}
	switch phase.Kind {
	case fencing.MembershipRepairRemove:
		if pending.ConfigID != 0 || !reflect.DeepEqual(current, view.Authorization.Membership) || phase.Change.ExpectedAbortSlot != abortSlot {
			return fencing.ErrPending
		}
		if view.Progress.Stage != fencing.MembershipRepairCandidateBound && view.Progress.Stage != fencing.MembershipRepairOldClosed {
			return fencing.ErrPending
		}
		closureAbort := uint64(*phase.AbortSlot)
		closure := fencing.PeerClosureRequest{ExpectedConfigID: uint64(view.Authorization.Membership.ConfigID), ExpectedAbortSlot: &closureAbort, TargetNodeID: string(phase.Change.Remove), TargetWALIdentity: view.Authorization.OldIdentity.WALIdentity}
		receipt, err := e.readPeerClosure(ctx, caller, closure, workloadUID)
		if err != nil {
			return err
		}
		fence := phase.Change.Fence
		if fence == nil || !fence.Confirmed || string(fence.NodeID) != view.Authorization.OldIdentity.NodeID || fence.WALIdentity != view.Authorization.OldIdentity.WALIdentity || fence.WorkloadUID != workloadUID || fence.Evidence != receipt.ProofID {
			return fmt.Errorf("fencing: REMOVE phase does not carry the exact retained closure receipt")
		}
	case fencing.MembershipRepairAdd:
		if phase.Change.ExpectedAbortSlot != abortSlot || pending.ConfigID != 0 {
			return fencing.ErrPending
		}
		if view.Progress.Stage == fencing.MembershipRepairOldClosed {
			if len(view.Progress.Phases) != 1 || view.Progress.Phases[0].Kind != fencing.MembershipRepairRemove {
				return fencing.ErrPending
			}
			removed, applyErr := applyRepairMembershipPhase(view.Authorization.Membership, view.Progress.Phases[0])
			if applyErr != nil || !reflect.DeepEqual(current, removed) {
				return fencing.ErrPending
			}
		} else if view.Progress.Stage != fencing.MembershipRepairRemoved {
			return fencing.ErrPending
		}
	default:
		return fencing.ErrPending
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, workloadUID, caller.SourceClusterID)
	if err != nil {
		return err
	}
	if state.Ledger.MembershipRepair == nil {
		return fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, workloadUID, caller.SourceClusterID, ledgerUID, nil, func(ledger *fencing.Ledger) error {
		record := ledger.MembershipRepair
		if record == nil || !reflect.DeepEqual(record.Authorization, view.Authorization) || !reflect.DeepEqual(record.Progress, view.Progress) {
			return fencing.ErrPending
		}
		if phase.Kind == fencing.MembershipRepairRemove && record.Progress.Stage == fencing.MembershipRepairCandidateBound {
			if err := ledger.AdvanceMembershipRepair(view.Authorization.OperationID, record.Progress.Revision, fencing.MembershipRepairOldClosed); err != nil {
				return err
			}
		}
		if phase.Kind == fencing.MembershipRepairAdd && record.Progress.Stage == fencing.MembershipRepairOldClosed {
			if err := ledger.AdvanceMembershipRepair(view.Authorization.OperationID, record.Progress.Revision, fencing.MembershipRepairRemoved); err != nil {
				return err
			}
		}
		return ledger.AppendMembershipRepairPhase(phase)
	})
	return err
}

func (e *FencingExecutor) membershipRepairCaller(ctx context.Context, caller fencing.Identity, workloadUID string) (peerClosureBinding, fenceLedger, error) {
	if e.Bucket == nil {
		return peerClosureBinding{}, fenceLedger{}, fmt.Errorf("fencing: membership-repair archive is unavailable")
	}
	binding, err := e.peerClosureBinding(ctx, caller, workloadUID)
	if err != nil {
		return peerClosureBinding{}, fenceLedger{}, err
	}
	state, err := e.peerClosureOpenCaller(ctx, binding, caller)
	if err != nil {
		return peerClosureBinding{}, fenceLedger{}, err
	}
	return binding, state, nil
}

func (e *FencingExecutor) recordMembershipRepairBind(ctx context.Context, workloadUID string, identity fencing.Identity) error {
	state, ledgerUID, _, err := e.readWithUID(ctx, workloadUID, identity.SourceClusterID)
	if err != nil {
		return err
	}
	if state.Ledger.MembershipRepair == nil {
		return nil
	}
	record := state.Ledger.MembershipRepair
	if record.Progress.Stage != fencing.MembershipRepairAdmitted {
		return nil
	}
	var assignment *fencing.BootstrapAssignment
	for i := range record.Progress.Candidates {
		candidate := &record.Progress.Candidates[i].Assignment
		if candidate.PodUID == identity.PodUID {
			assignment = candidate
			break
		}
	}
	if assignment == nil {
		return nil
	}
	if assignment.NodeID != identity.NodeID || assignment.PodName != fencing.PodNameForIdentity(identity) || assignment.CredentialName != identity.CredentialName || assignment.CredentialUID != identity.CredentialUID || identity.WALIdentity == "" {
		return fmt.Errorf("fencing: bound runtime differs from retained repair assignment")
	}
	binding, err := e.peerClosureBinding(ctx, identity, workloadUID)
	if err != nil {
		return err
	}
	registration, err := e.incarnationCredential(ctx, identity, workloadUID, identity, false)
	if err != nil {
		return err
	}
	if registration.Identity != identity {
		return fmt.Errorf("fencing: bound repair credential identity changed")
	}
	var member quepaxa.Member
	if err := json.Unmarshal(registration.Learner, &member); err != nil {
		return fmt.Errorf("fencing: bound repair member is invalid")
	}
	member.WALIdentity = identity.WALIdentity
	view := fencing.MembershipRepairView{Authorization: record.Authorization, Progress: record.Progress}
	if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
		return err
	}
	_, err = e.changeExisting(ctx, workloadUID, identity.SourceClusterID, ledgerUID, nil, func(ledger *fencing.Ledger) error {
		if ledger.MembershipRepair == nil || ledger.MembershipRepair.Authorization.OperationID != record.Authorization.OperationID {
			return fencing.ErrPending
		}
		if ledger.MembershipRepair.Progress.Stage == fencing.MembershipRepairCandidateBound {
			return ledger.BindMembershipRepairCandidate(identity.PodUID, member)
		}
		if ledger.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdmitted {
			return fencing.ErrPending
		}
		if err := ledger.BindMembershipRepairCandidate(identity.PodUID, member); err != nil {
			return err
		}
		return ledger.AdvanceMembershipRepair(record.Authorization.OperationID, ledger.MembershipRepair.Progress.Revision, fencing.MembershipRepairCandidateBound)
	})
	return err
}

func (e *FencingExecutor) verifyMembershipRepairArchive(ctx context.Context, binding peerClosureBinding, view fencing.MembershipRepairView) (resultErr error) {
	authorization := view.Authorization
	state, ledgerUID, _, err := e.readWithUID(ctx, binding.workloadUID, authorization.SourceClusterID)
	if err != nil {
		return err
	}
	if ledgerUID != authorization.LedgerUID || state.Ledger.MembershipRepair == nil ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Authorization, authorization) ||
		!reflect.DeepEqual(state.Ledger.MembershipRepair.Progress, view.Progress) ||
		authorization.RootUID != binding.rootUID || authorization.StatefulSetUID != binding.workloadUID || authorization.SourceClusterID != binding.root.Spec.AdoptClusterID || state.Ledger.SourceClusterID != authorization.SourceClusterID {
		return fmt.Errorf("fencing: membership-repair immutable binding changed")
	}
	current, pending, abortSlot, err := e.membershipRepairArchiveState(ctx, binding, authorization)
	if err != nil {
		return err
	}
	return validateRepairArchiveProgress(view, current, pending, abortSlot)
}

func (e *FencingExecutor) membershipRepairArchiveState(ctx context.Context, binding peerClosureBinding, authorization fencing.MembershipRepairAuthorization) (quepaxa.Cluster, quepaxa.Cluster, quepaxa.Slot, error) {
	initial, err := recoveryMembers(binding.environment["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(initial) == 0 {
		return quepaxa.Cluster{}, quepaxa.Cluster{}, 0, fmt.Errorf("fencing: source membership is unavailable")
	}
	durability := binding.environment["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	if archived, err := e.controller().sourceMembership(ctx, path.Join(e.Prefix, authorization.SourceClusterID)); err != nil || archived != recovery.NewMembershipRecord(authorization.SourceClusterID, initial, durability) {
		return quepaxa.Cluster{}, quepaxa.Cluster{}, 0, fmt.Errorf("fencing: immutable source membership binding changed")
	}
	return e.certifiedRepairMembership(ctx, binding, authorization.SourceClusterID, initial)
}

func (e *FencingExecutor) certifiedRepairMembership(ctx context.Context, binding peerClosureBinding, source string, initial []quepaxa.Member) (cluster quepaxa.Cluster, pending quepaxa.Cluster, abortSlot quepaxa.Slot, resultErr error) {
	if e.Bucket == nil {
		return cluster, pending, 0, fmt.Errorf("fencing: membership-repair archive is unavailable")
	}
	archive := recovery.NewManager(e.Bucket, path.Join(e.Prefix, source), 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return cluster, pending, 0, fmt.Errorf("fencing: source archive is unavailable: %w", err)
	}
	tip := archive.Tip()
	if tip == 0 {
		return cluster, pending, 0, fencing.ErrPending
	}
	dir, err := os.MkdirTemp("", ".rhiza-membership-repair-")
	if err != nil {
		return cluster, pending, 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(dir)) }()
	wal, err := qlog.Open(dir)
	if err != nil {
		return cluster, pending, 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "rhiza-membership-repair-observer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: initial}, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		return cluster, pending, 0, err
	}
	if err := e.initializeMembershipRepairObserver(ctx, binding, source, initial, archive, core); err != nil {
		return cluster, pending, 0, err
	}
	readPage := func(pageCtx context.Context, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		values, observedTip, err := archive.DecisionsFrom(pageCtx, from, limit)
		if err == nil && observedTip != tip {
			err = fmt.Errorf("fencing: source archive changed during membership replay")
		}
		return values, observedTip, err
	}
	if err := recovery.ReplayCertifiedThrough(ctx, core, tip, readPage); err != nil {
		return cluster, pending, 0, fmt.Errorf("fencing: source membership replay failed: %w", err)
	}
	cluster = core.CurrentCluster()
	if cluster.ConfigID == 0 || len(cluster.Members) == 0 {
		return quepaxa.Cluster{}, quepaxa.Cluster{}, 0, fencing.ErrPending
	}
	pending, _ = core.PendingReconfiguration()
	abortSlot, _, hasAbort := core.LastReconfigurationAbort()
	if !hasAbort {
		abortSlot = 0
	}
	if err := ctx.Err(); err != nil {
		return quepaxa.Cluster{}, quepaxa.Cluster{}, 0, err
	}
	if err := archive.Load(ctx); err != nil || archive.Tip() != tip {
		return quepaxa.Cluster{}, quepaxa.Cluster{}, 0, fmt.Errorf("fencing: source archive changed during membership verification")
	}
	return cluster, pending, abortSlot, nil
}

func validateRepairArchiveProgress(view fencing.MembershipRepairView, current, pending quepaxa.Cluster, lastAbort quepaxa.Slot) error {
	states := []quepaxa.Cluster{view.Authorization.Membership}
	for _, phase := range view.Progress.Phases {
		prior := states[len(states)-1]
		if phase.Change.ExpectedConfigID != prior.ConfigID {
			return fmt.Errorf("fencing: retained repair phase does not follow certified membership")
		}
		next, err := applyRepairMembershipPhase(prior, phase)
		if err != nil {
			return err
		}
		states = append(states, next)
	}
	currentIndex := -1
	for i, state := range states {
		if reflect.DeepEqual(current, state) {
			currentIndex = i
			break
		}
	}
	if currentIndex < 0 {
		return fmt.Errorf("fencing: certified membership differs from retained repair history")
	}
	if currentIndex == 0 {
		if view.Authorization.ExpectedAbortSlot != nil && *view.Authorization.ExpectedAbortSlot != lastAbort {
			return fmt.Errorf("fencing: membership-repair reservation differs from certified abort revision")
		}
		if len(view.Progress.Phases) > 0 && view.Progress.Phases[0].Change.ExpectedAbortSlot != lastAbort {
			return fmt.Errorf("fencing: first repair phase differs from certified abort revision")
		}
	} else if lastAbort != 0 {
		// A committed membership configuration resets the reconfiguration abort
		// revision. A later nonzero value means an unrecorded consensus round.
		return fmt.Errorf("fencing: certified abort revision advanced outside retained repair history")
	}
	if pending.ConfigID != 0 {
		if currentIndex+1 >= len(states) || !reflect.DeepEqual(pending, states[currentIndex+1]) {
			return fmt.Errorf("fencing: pending membership transition differs from retained repair phase")
		}
	}
	switch view.Progress.Stage {
	case fencing.MembershipRepairAdmitted, fencing.MembershipRepairCandidateBound:
		if currentIndex != 0 || pending.ConfigID != 0 {
			return fencing.ErrPending
		}
	case fencing.MembershipRepairOldClosed:
		if currentIndex > 1 || (pending.ConfigID != 0 && currentIndex != 0) {
			return fencing.ErrPending
		}
	case fencing.MembershipRepairRemoved:
		if currentIndex == 1 {
			if pending.ConfigID != 0 && (len(states) < 3 || !reflect.DeepEqual(pending, states[2])) {
				return fencing.ErrPending
			}
			break
		}
		// The ADD may commit before the authority's progress CAS. Accept only
		// the exact retained second transition so a retry can advance the stage.
		if currentIndex != 2 || pending.ConfigID != 0 || len(view.Progress.Phases) != 2 || view.Progress.Phases[1].Kind != fencing.MembershipRepairAdd {
			return fencing.ErrPending
		}
	case fencing.MembershipRepairAdded, fencing.MembershipRepairComplete:
		if currentIndex != 2 {
			return fencing.ErrPending
		}
	case fencing.MembershipRepairCancelled:
		if currentIndex != 0 || pending.ConfigID != 0 {
			return fencing.ErrPending
		}
	default:
		return fmt.Errorf("fencing: unknown membership-repair stage")
	}
	return nil
}

func applyRepairMembershipPhase(prior quepaxa.Cluster, phase fencing.MembershipRepairPhase) (quepaxa.Cluster, error) {
	next := quepaxa.Cluster{ConfigID: prior.ConfigID + 1, Members: append([]quepaxa.Member(nil), prior.Members...)}
	switch phase.Kind {
	case fencing.MembershipRepairRemove:
		found := false
		members := next.Members[:0]
		for _, member := range next.Members {
			if member.ID == phase.Change.Remove {
				found = true
				continue
			}
			members = append(members, member)
		}
		if !found || len(members) < 1 {
			return quepaxa.Cluster{}, fmt.Errorf("fencing: repair removal target is not in certified membership")
		}
		next.Members = members
	case fencing.MembershipRepairAdd:
		if phase.Change.Add == nil {
			return quepaxa.Cluster{}, fmt.Errorf("fencing: repair addition member is unavailable")
		}
		for _, member := range next.Members {
			if member.ID == phase.Change.Add.ID {
				return quepaxa.Cluster{}, fmt.Errorf("fencing: repair addition duplicates certified membership")
			}
		}
		next.Members = append(next.Members, *phase.Change.Add)
	default:
		return quepaxa.Cluster{}, fmt.Errorf("fencing: unknown membership-repair phase")
	}
	return next, nil
}
