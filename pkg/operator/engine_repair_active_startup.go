package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// resolveEngineRepairActiveRestartRegistration advances an already-promoted
// candidate's active Run before returning its ordinary voter grant. Logical
// identity and WAL come from the canonical repair record, not the physical
// supervisor request.
func (e *FencingExecutor) resolveEngineRepairActiveRestartRegistration(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, bool, error) {
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	request.PodName = physical
	if e == nil || e.AuthorityState == nil || ctx == nil || statefulSetUID == "" || physical == "" ||
		request.SourceClusterID == "" || request.PodUID == "" || request.RunID == "" || request.WALIdentity != "" || request.AssignmentID != "" {
		return fencing.StartupGrant{}, false, nil
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, request.SourceClusterID)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return fencing.StartupGrant{}, false, nil
		}
		return fencing.StartupGrant{}, false, err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != request.SourceClusterID {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: active candidate restart root identity differs")
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, request.SourceClusterID); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	if root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
		return fencing.StartupGrant{}, false, fencing.ErrClosed
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fencing.StartupGrant{}, false, err
	}
	var selected *fencing.EngineRepairRecord
	for _, reference := range root.Ledger.EngineRepairs {
		if reference.Binding.SourceClusterID != request.SourceClusterID || reference.CandidateIdentity.PodName != physical {
			continue
		}
		record, readErr := store.ReadEngineRepair(ctx, request.SourceClusterID, reference.OperationID)
		if readErr != nil {
			return fencing.StartupGrant{}, true, readErr
		}
		if record.Phase == fencing.RepairGenerationClosing && record.ClosingFrom == fencing.RepairActive {
			return fencing.StartupGrant{}, true, fencing.ErrClosed
		}
		if record.Phase != fencing.RepairActive {
			continue
		}
		if selected != nil {
			return fencing.StartupGrant{}, true, fmt.Errorf("fencing: active candidate restart is ambiguous")
		}
		copy := record
		selected = &copy
	}
	if selected == nil {
		return fencing.StartupGrant{}, false, nil
	}
	if err := e.verifyEngineRepairPhysicalStartupConfig(ctx, statefulSetUID, request, physical); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	record, currentIdentity, currentRuntime, err := store.ReadEngineRepairCurrentCandidate(ctx, request.SourceClusterID, selected.Binding.OperationID)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	_, ordinalErr := engineRepairPodOrdinal(physical)
	retryingCurrent := request.RunID == currentIdentity.RunID
	if record.Digest() != selected.Digest() || record.Phase != fencing.RepairActive || ordinalErr != nil ||
		currentIdentity.PodName != physical || currentIdentity.PodUID != request.PodUID ||
		(!retryingCurrent && candidateRunIDReserved(record, request.RunID)) ||
		currentRuntime.StatefulSetUID != statefulSetUID || currentRuntime.Validate(currentIdentity) != nil {
		return fencing.StartupGrant{}, true, fencing.ErrClosed
	}
	// This history is pinned to the immutable pre-restart candidate runtime.
	// The root's current runtime may already be a later active Run on retry.
	historyRuntime := record.CurrentCandidateRuntime()
	if historyRuntime == nil {
		return fencing.StartupGrant{}, true, fencing.ErrClosed
	}
	if err := e.verifyEngineRepairCandidateActiveHistory(ctx, root.Ledger, *historyRuntime, record); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	// Reject stale and otherwise non-current requests from canonical state even
	// when runtime enrollment is unavailable. Only a valid current assignment
	// needs the runtime agent to proceed to a new enrollment.
	if e.RuntimeAgent == nil {
		return fencing.StartupGrant{}, true, fencing.ErrUnavailable
	}
	nextIdentity := currentIdentity
	nextIdentity.RunID = request.RunID
	base, err := e.RuntimeAgent.enrollmentBase(ctx, e.Kube.Namespace, statefulSetUID, nextIdentity)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	runtime, err := e.RuntimeAgent.Enroll(ctx, base)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if retryingCurrent {
		// The first request may have committed the root CAS and lost only its
		// grant response. Reissue only when the currently attested runtime is the
		// exact Run already named by the canonical root reference.
		if runtime != currentRuntime {
			return fencing.StartupGrant{}, true, fencing.ErrClosed
		}
		if err := e.verifyEngineRepairActiveRestartCurrent(ctx, statefulSetUID, record, currentIdentity, currentRuntime); err != nil {
			return fencing.StartupGrant{}, true, err
		}
		grant, err := e.engineRepairActiveRestartGrant(ctx, request, record, currentIdentity)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
		if err := grant.Validate(request); err != nil {
			return fencing.StartupGrant{}, true, err
		}
		return grant, true, nil
	}
	if root.Ledger.RequireMutationJournal {
		scopeHash, stageIncarnation, bindingErr := e.mutationJournalBindingInputs(ctx, statefulSetUID, nextIdentity, runtime)
		prior, priorOK := root.Ledger.Runs[currentIdentity.RunID]
		if bindingErr != nil || !priorOK || prior.MutationProducerUID != stableMutationProducerUID(currentRuntime) ||
			prior.MutationScopeHash != scopeHash || prior.MutationProfile != e.sourceMutationProfile() ||
			prior.MutationStageIncarnation != stageIncarnation {
			return fencing.StartupGrant{}, true, errors.Join(bindingErr, fmt.Errorf("fencing: active restart storage binding differs from the canonical prior Run"))
		}
	}
	updated, err := store.RestartActiveCandidateRuntime(ctx, request.SourceClusterID, record.Binding.OperationID,
		record.Digest(), currentIdentity.RunID, nextIdentity, runtime)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if err := e.pinMutationJournalBeforeGrant(ctx, statefulSetUID, nextIdentity, runtime); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if err := e.verifyEngineRepairActiveRestartCurrent(ctx, statefulSetUID, updated, nextIdentity, runtime); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	grant, err := e.engineRepairActiveRestartGrant(ctx, request, updated, nextIdentity)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if err := grant.Validate(request); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	return grant, true, nil
}

func (e *FencingExecutor) verifyEngineRepairPhysicalStartupConfig(ctx context.Context, statefulSetUID string, request fencing.Identity, physical string) error {
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", physical), &pod); err != nil {
		return err
	}
	if str(nested(pod, "metadata", "uid")) != request.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fencing.ErrClosed
	}
	ownerName := ""
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if owner["controller"] == true && owner["kind"] == "StatefulSet" && str(owner["uid"]) == statefulSetUID {
			if ownerName != "" {
				return fmt.Errorf("fencing: active restart Pod StatefulSet owner is ambiguous")
			}
			ownerName = str(owner["name"])
		}
	}
	if ownerName == "" {
		return fmt.Errorf("fencing: active restart Pod is not owned by the authenticated StatefulSet")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != statefulSetUID {
		return fencing.ErrClosed
	}
	find := func(containers []any) (map[string]string, error) {
		var found map[string]string
		for _, raw := range containers {
			container := asObject(raw)
			env, err := e.controller().environment(ctx, container)
			if err != nil {
				return nil, err
			}
			if env["RHIZA_CLUSTER_ID"] != request.SourceClusterID {
				continue
			}
			if found != nil || env["RHIZA_NODE_ID"] != request.NodeID {
				return nil, fmt.Errorf("fencing: active restart physical NodeID differs from its Pod configuration")
			}
			found = env
		}
		if found == nil {
			return nil, fmt.Errorf("fencing: active restart application container configuration is missing")
		}
		return found, nil
	}
	podEnv, err := find(list(nested(pod, "spec", "containers")))
	if err != nil {
		return err
	}
	templateEnv, err := find(list(nested(sts, "spec", "template", "spec", "containers")))
	if err != nil {
		return err
	}
	if podEnv["RHIZA_CLUSTER_MEMBERS"] == "" || podEnv["RHIZA_CLUSTER_MEMBERS"] != templateEnv["RHIZA_CLUSTER_MEMBERS"] ||
		podEnv["RHIZA_CLUSTER_ID"] != templateEnv["RHIZA_CLUSTER_ID"] {
		return fmt.Errorf("fencing: active restart Pod differs from its StatefulSet source configuration")
	}
	return nil
}

func (e *FencingExecutor) verifyEngineRepairActiveRestartCurrent(ctx context.Context, statefulSetUID string, record fencing.EngineRepairRecord, identity fencing.Identity, runtime fencing.RuntimeBinding) error {
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != identity.SourceClusterID ||
		root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
		return fencing.ErrClosed
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, identity.SourceClusterID); err != nil {
		return err
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return err
	}
	latest, current, currentRuntime, err := store.ReadEngineRepairCurrentCandidate(ctx, identity.SourceClusterID, record.Binding.OperationID)
	if err != nil || latest.Digest() != record.Digest() || latest.Phase != fencing.RepairActive || current != identity || currentRuntime != runtime {
		return errors.Join(err, fmt.Errorf("fencing: active candidate restart changed before startup grant"))
	}
	if err := latest.ValidateAgainstLedger(root.Ledger); err != nil {
		return err
	}
	priorRuntime := latest.CurrentCandidateRuntime()
	if priorRuntime == nil {
		return fmt.Errorf("fencing: active candidate history lost its trusted runtime")
	}
	if err := e.verifyEngineRepairCandidateActiveHistory(ctx, root.Ledger, *priorRuntime, latest); err != nil {
		return err
	}
	run, ok := root.Ledger.Runs[identity.RunID]
	prior, priorOK := root.Ledger.Runs[latest.CurrentCandidateIdentity().RunID]
	wantProvenance := engineRepairRunProvenance(latest)
	wantProvenance.CandidateIdentity = identity
	wantProvenance.RuntimeBindingDigest = fencing.RuntimeBindingDigest(runtime)
	if !ok || !priorOK || run.Identity != identity || run.Runtime == nil || *run.Runtime != runtime ||
		fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Stop != nil || run.Drain != nil || run.Exited || run.NeverBoundExit != nil ||
		run.EngineRepairProvenance == nil || *run.EngineRepairProvenance != wantProvenance || run.Promotion == nil || prior.Promotion == nil || *run.Promotion != *prior.Promotion {
		return fmt.Errorf("fencing: active candidate restart Run lacks inherited authority provenance and promotion")
	}
	if root.Ledger.RequireMutationJournal {
		scopeHash, stageIncarnation, err := e.mutationJournalBindingInputs(ctx, statefulSetUID, identity, runtime)
		if err != nil {
			return err
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
		if err != nil {
			return err
		}
		if run.MutationProducerUID != stableMutationProducerUID(runtime) || run.MutationScopeHash != scopeHash ||
			run.MutationProfile != e.sourceMutationProfile() || run.MutationStageIncarnation != stageIncarnation ||
			producerUID != run.MutationProducerUID || producer.Runtime != runtime || producer.WALIdentity != identity.WALIdentity ||
			producer.Phase != fencing.MutationReserved || producer.EvidenceFault != "" || producer.Validate() != nil {
			return fmt.Errorf("fencing: active candidate restart lacks exact reserved mutation producer binding")
		}
	}
	return nil
}

func (e *FencingExecutor) engineRepairActiveRestartGrant(ctx context.Context, request fencing.Identity, record fencing.EngineRepairRecord, identity fencing.Identity) (fencing.StartupGrant, error) {
	if record.RemovalOperation == nil || record.Phase != fencing.RepairActive || identity.PodName != request.PodName ||
		identity.PodUID != request.PodUID || identity.RunID != request.RunID || identity.WALIdentity == "" {
		return fencing.StartupGrant{}, fmt.Errorf("fencing: active candidate startup assignment is incomplete")
	}
	_, target, err := engineRepairCanonicalAdd(record)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	peerToken, err := e.engineRepairPinnedPeerToken(ctx, record)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	members, err := json.Marshal(target.Members)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	grantIdentity := identity
	grantIdentity.WALIdentity = ""
	return fencing.StartupGrant{Identity: grantIdentity, Role: fencing.StartupRoleVoterActive,
		Environment: map[string]string{"RHIZA_NODE_ID": identity.NodeID, "RHIZA_PEER_TOKEN": peerToken, "RHIZA_CLUSTER_MEMBERS": string(members)}}, nil
}

func (e *FencingExecutor) engineRepairPinnedPeerToken(ctx context.Context, record fencing.EngineRepairRecord) (string, error) {
	credential := record.CandidateCredential
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", credential.Name), &secret); err != nil {
		return "", err
	}
	if str(nested(secret, "metadata", "name")) != credential.Name || str(nested(secret, "metadata", "uid")) != credential.UID ||
		str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace || secret["type"] != "Opaque" {
		return "", fmt.Errorf("fencing: pinned active candidate credential reference changed")
	}
	data := asObject(secret["data"])
	var tokens []string
	for _, key := range []string{"RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKENS"} {
		encoded := str(data[key])
		if encoded == "" {
			continue
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil || len(decoded) == 0 {
			return "", fmt.Errorf("fencing: pinned active candidate credential data is invalid")
		}
		if key == "RHIZA_PEER_TOKEN" {
			tokens = append(tokens, string(decoded))
			continue
		}
		var mapped map[string]string
		if json.Unmarshal(decoded, &mapped) != nil || len(mapped) == 0 {
			return "", fmt.Errorf("fencing: pinned active candidate peer-token map is invalid")
		}
		for _, token := range mapped {
			tokens = append(tokens, token)
		}
	}
	matched := ""
	for _, token := range tokens {
		if token == "" || fencing.EngineRepairPeerTokenDigest([]byte(token)) != credential.PeerTokenDigest {
			continue
		}
		if matched != "" && matched != token {
			return "", fmt.Errorf("fencing: pinned active candidate peer credential is ambiguous")
		}
		matched = token
	}
	if matched == "" || quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(record.Binding.SourceClusterID), record.CandidateMember.ID, matched)) != record.CandidateMember.PublicKey {
		return "", fmt.Errorf("fencing: pinned active candidate peer credential differs from its public member")
	}
	return matched, nil
}
