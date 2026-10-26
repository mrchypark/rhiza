package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type registeredVoterIdentity struct {
	Version          int    `json:"version"`
	Cluster          string `json:"cluster"`
	Node             string `json:"node"`
	Membership       string `json:"membership"`
	Store            string `json:"store"`
	Nonce            string `json:"nonce"`
	Registered       bool   `json:"registered"`
	LearnerPublicKey string `json:"learner_public_key,omitempty"`
}

// createPendingEngineRepairPreflight enriches only authenticated, public
// physical evidence. Secret references and old identity are derived from the
// owned Pod/StatefulSet and immutable archive, never selected by the caller.
func (e *FencingExecutor) createPendingEngineRepairPreflight(ctx context.Context, authenticatedStatefulSetUID string, identity fencing.Identity, proposal fencing.PendingEngineRepairProposal) error {
	if e == nil || e.AuthorityState == nil || e.ArchiveBucket == nil || e.Kube == nil || ctx == nil ||
		identity.WALIdentity != "" || identity.AssignmentID != "" || proposal.BootstrapIdentity != identity ||
		proposal.ValidatePreflight() != nil || proposal.SourceClusterID != identity.SourceClusterID || proposal.OldMemberID != identity.NodeID {
		return fmt.Errorf("fencing: physical engine repair preflight is invalid")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", identity.PodName), &pod); err != nil {
		return err
	}
	if str(nested(pod, "metadata", "uid")) != identity.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fmt.Errorf("fencing: preflight Pod incarnation differs")
	}
	var ownerName string
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if owner["controller"] == true && owner["kind"] == "StatefulSet" && str(owner["uid"]) == authenticatedStatefulSetUID {
			ownerName = str(owner["name"])
		}
	}
	if ownerName == "" || authenticatedStatefulSetUID == "" {
		return fmt.Errorf("fencing: preflight Pod is not owned by its authenticated StatefulSet")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != authenticatedStatefulSetUID {
		return fencing.ErrClosed
	}
	ordinal, err := engineRepairPodOrdinal(identity.PodName)
	if err != nil || ordinal != proposal.StableOrdinal {
		return fmt.Errorf("fencing: preflight ordinal differs from Pod name")
	}
	var podContainer object
	for _, raw := range list(nested(pod, "spec", "containers")) {
		container := asObject(raw)
		env, err := e.controller().environment(ctx, container)
		if err != nil {
			return err
		}
		if env["RHIZA_CLUSTER_ID"] == identity.SourceClusterID {
			if podContainer != nil {
				return fmt.Errorf("fencing: preflight application container is ambiguous")
			}
			podContainer = container
		}
	}
	if podContainer == nil {
		return fmt.Errorf("fencing: preflight application container is missing")
	}
	appEnv, err := e.controller().environment(ctx, podContainer)
	if err != nil {
		return err
	}
	if appEnv["RHIZA_CLUSTER_ID"] != identity.SourceClusterID || appEnv["RHIZA_NODE_ID"] != identity.NodeID {
		return fencing.ErrClosed
	}
	var templateContainer object
	for _, raw := range list(nested(sts, "spec", "template", "spec", "containers")) {
		container := asObject(raw)
		env, err := e.controller().environment(ctx, container)
		if err != nil {
			return err
		}
		if env["RHIZA_CLUSTER_ID"] == identity.SourceClusterID {
			if templateContainer != nil {
				return fmt.Errorf("fencing: preflight template container is ambiguous")
			}
			templateContainer = container
		}
	}
	if templateContainer == nil {
		return fmt.Errorf("fencing: preflight StatefulSet template container is missing")
	}
	templateEnv, err := e.controller().environment(ctx, templateContainer)
	if err != nil {
		return err
	}
	if templateEnv["RHIZA_CLUSTER_MEMBERS"] != appEnv["RHIZA_CLUSTER_MEMBERS"] || templateEnv["RHIZA_CLUSTER_ID"] != appEnv["RHIZA_CLUSTER_ID"] {
		return fmt.Errorf("fencing: preflight Pod configuration differs from StatefulSet template")
	}
	members, err := recoveryMembers(appEnv["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return err
	}
	sourceConfig := quepaxa.Cluster{Members: members}
	oldMember, ok := sourceConfig.MemberSet()[quepaxa.NodeID(identity.NodeID)]
	if !ok || proposal.CandidateMember.ID == oldMember.ID || proposal.CandidateMember.URL != oldMember.URL ||
		proposal.CandidateMember.LogURL != oldMember.LogURL || proposal.CandidateMember.PeerURL != oldMember.PeerURL {
		return fmt.Errorf("fencing: candidate endpoint is not the old member's stable slot")
	}
	prefix := path.Join(appEnv["RHIZA_OBJSTORE_PREFIX"], identity.SourceClusterID)
	identityKey := path.Join(prefix, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(identity.NodeID))))
	identityData, err := readAuthorityObject(ctx, e.ArchiveBucket, identityKey)
	if err != nil {
		return err
	}
	var registered registeredVoterIdentity
	decoder := json.NewDecoder(bytes.NewReader(identityData))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&registered) != nil || decoder.Decode(new(any)) != io.EOF || registered.Cluster != identity.SourceClusterID ||
		registered.Node != identity.NodeID || !registered.Registered || registered.Nonce == "" || registered.Membership == "" || registered.Store == "" {
		return fmt.Errorf("fencing: immutable old voter registration is invalid")
	}
	if registered.Nonce != proposal.OldWALIdentity {
		return fmt.Errorf("fencing: preflight old WAL identity differs from archive registration")
	}
	peerSecretName := engineRepairSecretSourceName(podContainer, "RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKEN")
	peerKey := "RHIZA_PEER_TOKEN"
	if peerSecretName == "" {
		peerSecretName = engineRepairSecretSourceName(podContainer, "RHIZA_PEER_TOKENS", "RHIZA_PEER_TOKENS")
		peerKey = "RHIZA_PEER_TOKENS"
	}
	adminSecretName := engineRepairSecretSourceName(podContainer, "RHIZA_ADMIN_TOKEN", "RHIZA_ADMIN_TOKEN")
	if peerSecretName == "" || adminSecretName == "" || peerSecretName != engineRepairSecretSourceName(templateContainer, peerKey, peerKey) ||
		adminSecretName != engineRepairSecretSourceName(templateContainer, "RHIZA_ADMIN_TOKEN", "RHIZA_ADMIN_TOKEN") {
		return fmt.Errorf("fencing: preflight peer/admin credentials are not fixed Secret references")
	}
	peerToken, peerUID, err := e.preflightSecretValue(ctx, peerSecretName, peerKey)
	if err != nil {
		return err
	}
	if peerKey == "RHIZA_PEER_TOKENS" {
		var tokens map[string]string
		if json.Unmarshal([]byte(peerToken), &tokens) != nil || tokens[identity.NodeID] == "" {
			return fmt.Errorf("fencing: fixed peer token map is invalid")
		}
		peerToken = tokens[identity.NodeID]
	}
	adminToken, adminUID, err := e.preflightSecretValue(ctx, adminSecretName, "RHIZA_ADMIN_TOKEN")
	if err != nil {
		return err
	}
	if quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(identity.SourceClusterID), proposal.CandidateMember.ID, peerToken)) != proposal.CandidateMember.PublicKey {
		return fmt.Errorf("fencing: candidate public key does not match fixed per-Pod peer credential")
	}
	incumbent, incumbentWAL, incumbentReferenceDigest, err := e.resolveEngineRepairIncumbent(ctx, authenticatedStatefulSetUID,
		identity.SourceClusterID, proposal.StableOrdinal, identity.NodeID, identity.PodName, registered)
	if err != nil {
		return err
	}
	proposal.StatefulSetUID = authenticatedStatefulSetUID
	proposal.OldMemberID = incumbent
	proposal.SourceOldWALIdentity = incumbentWAL
	proposal.IncumbentReferenceDigest = incumbentReferenceDigest
	proposal.CandidateCredential = fencing.EngineRepairCredential{Name: peerSecretName, UID: peerUID, PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte(peerToken))}
	proposal.SourceAdminCredential = fencing.EngineRepairAdminCredential{Name: adminSecretName, UID: adminUID, AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte(adminToken))}
	if err := proposal.Validate(); err != nil {
		return err
	}
	store, err := NewEngineRepairStore(e.AuthorityState, authenticatedStatefulSetUID)
	if err != nil {
		return err
	}
	_, err = store.CreateOrReadPendingEngineRepair(ctx, proposal)
	return err
}

// resolveEngineRepairIncumbent translates the physical Pod's stable ordinal
// into the current canonical voter. A completed engine-repair chain is
// followed only through exact Active records whose candidate activation is
// independently reverified; the returned digest pins that chain tip before
// the pending request digest is calculated.
func (e *FencingExecutor) resolveEngineRepairIncumbent(ctx context.Context, statefulSetUID, sourceClusterID string, ordinal int,
	physicalNodeID, podName string, genesis registeredVoterIdentity) (string, string, string, error) {
	fail := func(err error) (string, string, string, error) { return "", "", "", err }
	if e == nil || e.AuthorityState == nil || e.ArchiveBucket == nil || ctx == nil || statefulSetUID == "" || sourceClusterID == "" ||
		ordinal < 0 || physicalNodeID == "" || podName == "" || genesis.Cluster != sourceClusterID || genesis.Node != physicalNodeID ||
		!genesis.Registered || genesis.Nonce == "" || genesis.Membership == "" {
		return fail(fmt.Errorf("fencing: engine repair incumbent context is incomplete"))
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, sourceClusterID)
	if err != nil {
		return fail(err)
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != sourceClusterID || root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
		return fail(fencing.ErrClosed)
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, sourceClusterID); err != nil {
		return fail(err)
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fail(err)
	}
	incumbent, incumbentWAL := physicalNodeID, genesis.Nonce
	genesisDigest := hashJSON([]any{"engine-repair-incumbent-genesis-v1", statefulSetUID, sourceClusterID, ordinal,
		physicalNodeID, genesis.Membership, genesis.Nonce})
	chainDigest := genesisDigest
	consumed := make(map[string]struct{}, len(root.Ledger.EngineRepairs))
	for {
		var next *fencing.EngineRepairRecord
		for _, reference := range root.Ledger.EngineRepairs {
			if reference.Binding.SourceClusterID != sourceClusterID || reference.Binding.OldMemberID != incumbent || reference.Binding.OldWALIdentity != incumbentWAL {
				continue
			}
			if _, seen := consumed[reference.OperationID]; seen {
				return fail(fmt.Errorf("fencing: engine repair incumbent chain contains a cycle"))
			}
			record, readErr := store.ReadEngineRepair(ctx, sourceClusterID, reference.OperationID)
			if readErr != nil {
				return fail(readErr)
			}
			if err := record.ValidateAgainstLedger(root.Ledger); err != nil {
				return fail(err)
			}
			if record.Phase != fencing.RepairActive || record.CurrentCandidateIdentity().PodName != podName {
				return fail(fmt.Errorf("fencing: ordinal incumbent has an incomplete or mismatched repair reference"))
			}
			if next != nil {
				return fail(fmt.Errorf("fencing: ordinal incumbent has multiple active repair successors"))
			}
			copy := record
			next = &copy
		}
		if next == nil {
			break
		}
		candidate := next.CurrentCandidateIdentity()
		runtime := next.CurrentCandidateRuntime()
		if runtime == nil || candidate.NodeID == incumbent || candidate.WALIdentity == "" || next.CandidateMember.WALIdentity != candidate.WALIdentity ||
			runtime.Validate(candidate) != nil {
			return fail(fmt.Errorf("fencing: ordinal incumbent repair has no exact active runtime"))
		}
		candidateOrdinal, ordinalErr := engineRepairPodOrdinal(candidate.PodName)
		if ordinalErr != nil || candidateOrdinal != ordinal {
			return fail(fmt.Errorf("fencing: completed repair candidate changed the stable ordinal"))
		}
		if err := e.verifyEngineRepairCandidateActiveHistory(ctx, root.Ledger, *runtime, *next); err != nil {
			return fail(fmt.Errorf("fencing: ordinal incumbent repair lacks canonical active history: %w", err))
		}
		consumed[next.Binding.OperationID] = struct{}{}
		chainDigest = hashJSON([]any{"engine-repair-incumbent-transition-v1", chainDigest,
			engineRepairRootReference(*next), next.Digest()})
		incumbent, incumbentWAL = candidate.NodeID, candidate.WALIdentity
	}
	var activeRun *fencing.Run
	for _, run := range root.Ledger.Runs {
		if run.Identity.SourceClusterID != sourceClusterID || run.Identity.NodeID != incumbent || run.Identity.WALIdentity != incumbentWAL ||
			run.Stop != nil || run.Drain != nil || run.Exited || run.NeverBoundExit != nil || run.IndependentTerminal != nil ||
			fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil ||
			run.Identity.PodName != podName {
			continue
		}
		if activeRun != nil {
			return fail(fmt.Errorf("fencing: ordinal incumbent has multiple active voter runs"))
		}
		copy := run
		activeRun = &copy
	}
	if activeRun == nil {
		return fail(fmt.Errorf("fencing: ordinal has no canonical active incumbent run"))
	}
	if err := engineRepairCallerIsActiveSurvivor(fenceLedger{StatefulSetUID: statefulSetUID, Ledger: root.Ledger}, activeRun.Identity); err != nil {
		return fail(fmt.Errorf("fencing: ordinal incumbent is not an active canonical voter: %w", err))
	}
	if len(consumed) == 0 {
		chainDigest = hashJSON([]any{"engine-repair-incumbent-genesis-root-v1", chainDigest, activeRun.Identity, activeRun.Runtime})
	} else {
		chainDigest = hashJSON([]any{"engine-repair-incumbent-active-tip-v1", chainDigest, activeRun.Identity, activeRun.Runtime})
	}
	return incumbent, incumbentWAL, chainDigest, nil
}

// verifyEngineRepairCandidateActiveHistory verifies the authority-persisted
// activation receipt when a later Pod incarnation has replaced the original
// candidate Pod. Live endpoint checks are performed before the record can
// enter RepairActive; replaying them against a new Pod UID would incorrectly
// make a completed logical incumbent disappear from its stable ordinal.
func (e *FencingExecutor) verifyEngineRepairCandidateActiveHistory(ctx context.Context, ledger fencing.Ledger, runtime fencing.RuntimeBinding, record fencing.EngineRepairRecord) error {
	candidate := record.CurrentCandidateIdentity()
	if e == nil || ctx == nil || record.Validate() != nil || record.Phase != fencing.RepairActive ||
		runtime.Validate(candidate) != nil || !engineRepairRuntimeMatches(record, runtime) ||
		record.ValidateAgainstLedger(ledger) != nil {
		return fmt.Errorf("fencing: active engine repair history is not canonical")
	}
	run, exists := ledger.Runs[candidate.RunID]
	provenance := run.EngineRepairProvenance
	if !exists || run.Identity != candidate || run.Runtime == nil || *run.Runtime != runtime ||
		fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Promotion == nil ||
		run.Promotion.OperationID != engineRepairAddOperationID(record.Binding.OperationID) ||
		!validEvidenceDigest(run.Promotion.EvidenceHash) || run.MutationProducerUID != stableMutationProducerUID(runtime) ||
		!validEvidenceDigest(run.MutationScopeHash) || run.MutationProfile == "" || run.MutationStageIncarnation == "" ||
		provenance == nil || provenance.OperationID != record.Binding.OperationID ||
		provenance.IdentityDigest != record.IdentityDigest() || provenance.CandidateIdentity != candidate {
		return fmt.Errorf("fencing: active engine repair run lacks exact operation provenance")
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil {
		return fmt.Errorf("fencing: read historical candidate producer enrollment: %w", err)
	}
	if producerUID != run.MutationProducerUID || producer.Runtime != runtime || producer.WALIdentity != candidate.WALIdentity ||
		producer.Phase != fencing.MutationOpen || producer.EvidenceFault != "" || producer.Validate() != nil {
		return fmt.Errorf("fencing: historical candidate producer enrollment is not canonical")
	}
	if err := e.validateMutationArchiveHead(ctx, runtime, producerUID, producer); err != nil {
		return fmt.Errorf("fencing: historical candidate mutation archive differs: %w", err)
	}
	return nil
}

func (e *FencingExecutor) preflightSecretValue(ctx context.Context, name, key string) (string, string, error) {
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", name), &secret); err != nil {
		return "", "", err
	}
	uid := str(nested(secret, "metadata", "uid"))
	if str(nested(secret, "metadata", "name")) != name || str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace || secret["type"] != "Opaque" || uid == "" {
		return "", "", fmt.Errorf("fencing: fixed preflight Secret identity is invalid")
	}
	value, err := base64.StdEncoding.DecodeString(str(asObject(secret["data"])[key]))
	if err != nil || len(value) == 0 {
		return "", "", fmt.Errorf("fencing: fixed preflight Secret value is invalid")
	}
	return string(value), uid, nil
}

func engineRepairSecretSourceName(container object, name, key string) string {
	for _, raw := range list(container["env"]) {
		entry, ok := raw.(map[string]any)
		if !ok {
			return ""
		}
		if entry["name"] != name {
			continue
		}
		valueFrom, ok := entry["valueFrom"].(map[string]any)
		if !ok {
			return ""
		}
		ref, ok := valueFrom["secretKeyRef"].(map[string]any)
		if !ok || ref["key"] != key || ref["optional"] == true {
			return ""
		}
		if ref["name"] != "" {
			return str(ref["name"])
		}
		return ""
	}
	for _, raw := range list(container["envFrom"]) {
		entry, ok := raw.(map[string]any)
		if !ok {
			return ""
		}
		if str(nested(entry, "secretRef", "name")) != "" && nested(entry, "secretRef", "optional") != true {
			return str(nested(entry, "secretRef", "name"))
		}
	}
	return ""
}

func engineRepairPodOrdinal(name string) (int, error) {
	i := strings.LastIndexByte(name, '-')
	if i < 1 {
		return 0, fmt.Errorf("fencing: Pod has no StatefulSet ordinal")
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil || n < 0 {
		return 0, fmt.Errorf("fencing: invalid StatefulSet ordinal")
	}
	return n, nil
}

func writeEngineRepairStartupError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, fencing.ErrPending), errors.Is(err, ErrFencePending):
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, fencing.ErrClosed):
		http.Error(w, "engine repair bootstrap closed", http.StatusGone)
	case transientFenceAuthority(err):
		http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "engine repair bootstrap rejected", http.StatusConflict)
	}
}

// resolveEngineRepairBootstrapRegistration returns a learner grant only after
// the candidate's exact trusted runtime is enrolled in the canonical ledger.
func (e *FencingExecutor) resolveEngineRepairBootstrapRegistration(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, bool, error) {
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	record, found, err := e.engineRepairRecordForCandidatePod(ctx, statefulSetUID, request.SourceClusterID, request.NodeID, physical, request.PodUID, request.RunID)
	if err != nil {
		return fencing.StartupGrant{}, found, err
	}
	if !found {
		store, storeErr := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if storeErr != nil {
			return fencing.StartupGrant{}, false, storeErr
		}
		pending, listErr := store.ListPendingEngineRepairs(ctx, request.SourceClusterID)
		if errors.Is(listErr, errAuthorityStateNotFound) {
			// A missing root has no canonical pending references. Let the
			// registration path reach initializeRoot, which performs the legacy
			// ledger check before creating any canonical state. Other read errors
			// remain fail-closed.
			return fencing.StartupGrant{}, false, nil
		}
		if listErr != nil {
			return fencing.StartupGrant{}, false, listErr
		}
		for _, item := range pending {
			if item.BootstrapIdentity.PodName == physical && item.BootstrapIdentity.PodUID == request.PodUID &&
				item.BootstrapIdentity.RunID == request.RunID && item.OldMemberID == request.NodeID {
				return fencing.StartupGrant{}, true, fencing.ErrPending
			}
		}
		return fencing.StartupGrant{}, false, nil
	}
	if request.WALIdentity != "" || request.AssignmentID != "" || request.NodeID != record.Binding.OldMemberID ||
		record.CurrentCandidateIdentity().PodName != physical || record.CurrentCandidateIdentity().PodUID != request.PodUID {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: engine repair bootstrap runtime differs from its reservation")
	}
	currentIdentity := record.CurrentCandidateIdentity()
	if currentIdentity.RunID != request.RunID {
		if candidateRunIDReserved(record, request.RunID) {
			return fencing.StartupGrant{}, true, fmt.Errorf("fencing: engine repair supervisor RunID was already used")
		}
		if e.RuntimeAgent == nil {
			return fencing.StartupGrant{}, true, fencing.ErrUnavailable
		}
		currentRuntime := record.CurrentCandidateRuntime()
		if currentRuntime == nil {
			return fencing.StartupGrant{}, true, fencing.ErrPending
		}
		nextIdentity := currentIdentity
		nextIdentity.RunID = request.RunID
		base, enrollErr := e.RuntimeAgent.enrollmentBase(ctx, e.Kube.Namespace, statefulSetUID, nextIdentity)
		if enrollErr != nil {
			return fencing.StartupGrant{}, true, enrollErr
		}
		runtime, enrollErr := e.RuntimeAgent.Enroll(ctx, base)
		if enrollErr != nil {
			return fencing.StartupGrant{}, true, enrollErr
		}
		store, storeErr := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if storeErr != nil {
			return fencing.StartupGrant{}, true, storeErr
		}
		record, err = store.ContinueCandidateRuntime(ctx, request.SourceClusterID, record.Binding.OperationID,
			record.Digest(), currentIdentity.RunID, nextIdentity, runtime)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
		currentIdentity = record.CurrentCandidateIdentity()
	} else if record.CurrentCandidateRuntime() == nil {
		if e.RuntimeAgent == nil {
			return fencing.StartupGrant{}, true, fencing.ErrUnavailable
		}
		base, err := e.RuntimeAgent.enrollmentBase(ctx, e.Kube.Namespace, statefulSetUID, currentIdentity)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
		runtime, err := e.RuntimeAgent.Enroll(ctx, base)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
		store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
		record, err = store.PinCandidateRuntime(ctx, request.SourceClusterID, record.Binding.OperationID, record.Digest(), runtime)
		if err != nil {
			return fencing.StartupGrant{}, true, err
		}
	}
	if err := e.registerEngineRepairCandidateRun(ctx, statefulSetUID, record, currentIdentity); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	memberJSON, token, err := e.readEngineRepairCredential(ctx, record)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	bootstrapMembers, err := e.engineRepairBootstrapMembership(ctx, request, record)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	identity := currentIdentity
	identity.WALIdentity = ""
	grant := fencing.StartupGrant{Identity: identity, Role: fencing.StartupRoleLearnerPendingRemove,
		ExpectedWALIdentity: currentIdentity.WALIdentity, ReplacedNodeID: record.Binding.OldMemberID,
		ReplacedWALIdentity: record.Binding.OldWALIdentity,
		Environment: map[string]string{"RHIZA_NODE_ID": record.CandidateIdentity.NodeID, "RHIZA_LEARNER": string(memberJSON),
			"RHIZA_PEER_TOKEN": token, "RHIZA_PEER_TOKENS": "", "RHIZA_CLUSTER_MEMBERS": bootstrapMembers}}
	if err := grant.Validate(request); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if err := e.verifyEngineRepairCandidateGrantCurrent(ctx, statefulSetUID, record, currentIdentity); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	return grant, true, nil
}

// verifyEngineRepairCandidateGrantCurrent is the final authority read before
// returning startup configuration. Enrollment, Pod environment, and archive
// checks above may take time; a concurrently closed root or replaced candidate
// must not receive the learner grant from that stale snapshot.
func (e *FencingExecutor) verifyEngineRepairCandidateGrantCurrent(ctx context.Context, statefulSetUID string, record fencing.EngineRepairRecord, identity fencing.Identity) error {
	if e == nil || e.AuthorityState == nil || ctx == nil || record.Validate() != nil || record.Binding.StatefulSetUID != statefulSetUID ||
		record.CurrentCandidateIdentity() != identity || record.CurrentCandidateRuntime() == nil {
		return fencing.ErrClosed
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != identity.SourceClusterID ||
		root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
		return fencing.ErrClosed
	}
	if _, closed := root.Ledger.ClosedNodes[identity.NodeID]; closed {
		return fencing.ErrClosed
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, identity.SourceClusterID); err != nil {
		return err
	}
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil || root.Ledger.EngineRepairs[key] != engineRepairRootReference(record) {
		return fencing.ErrClosed
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return err
	}
	canonical, err := store.ReadEngineRepair(ctx, identity.SourceClusterID, record.Binding.OperationID)
	if err != nil {
		return err
	}
	if canonical.Digest() != record.Digest() || canonical.ValidateAgainstLedger(root.Ledger) != nil {
		return fencing.ErrClosed
	}
	run, exists := root.Ledger.Runs[identity.RunID]
	provenance := run.EngineRepairProvenance
	if !exists || run.Runtime == nil || *run.Runtime != *canonical.CurrentCandidateRuntime() ||
		fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.Stop != nil || run.Exited || run.NeverBoundExit != nil ||
		provenance == nil || provenance.OperationID != canonical.Binding.OperationID ||
		provenance.IdentityDigest != canonical.IdentityDigest() || provenance.CandidateIdentity != identity ||
		provenance.ValidateForRun(run) != nil {
		return fencing.ErrClosed
	}
	return nil
}

// engineRepairBootstrapMembership restores the immutable genesis member list
// removed by the supervisor's environment scrub. The Pod's actual resolved
// configuration is accepted only when its public membership fingerprint
// matches the immutable voter registration; dynamic SourceConfig is not used
// as a substitute for genesis configuration.
func (e *FencingExecutor) engineRepairBootstrapMembership(ctx context.Context, request fencing.Identity, record fencing.EngineRepairRecord) (string, error) {
	if e == nil || e.Kube == nil || e.ArchiveBucket == nil || request.PodName == "" || request.PodUID == "" || request.NodeID == "" ||
		request.SourceClusterID != record.Binding.SourceClusterID {
		return "", fmt.Errorf("fencing: engine repair bootstrap membership context is incomplete")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", request.PodName), &pod); err != nil {
		return "", err
	}
	if str(nested(pod, "metadata", "name")) != request.PodName || str(nested(pod, "metadata", "uid")) != request.PodUID ||
		nested(pod, "metadata", "deletionTimestamp") != nil {
		return "", fmt.Errorf("fencing: engine repair bootstrap Pod incarnation changed")
	}
	var members []quepaxa.Member
	var runtimeEnv map[string]string
	for _, raw := range list(nested(pod, "spec", "containers")) {
		env, err := e.controller().environment(ctx, asObject(raw))
		if err != nil {
			return "", err
		}
		if env["RHIZA_CLUSTER_ID"] != request.SourceClusterID {
			continue
		}
		if runtimeEnv != nil || env["RHIZA_NODE_ID"] != request.NodeID {
			return "", fmt.Errorf("fencing: engine repair bootstrap application configuration is ambiguous")
		}
		runtimeEnv = env
		members, err = recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
		if err != nil {
			return "", fmt.Errorf("fencing: engine repair bootstrap membership is invalid: %w", err)
		}
	}
	if runtimeEnv == nil || len(members) < 2 {
		return "", fmt.Errorf("fencing: engine repair bootstrap application configuration is missing")
	}
	sourceMembership := quepaxa.Cluster{Members: members}
	oldMember, ok := sourceMembership.MemberSet()[quepaxa.NodeID(request.NodeID)]
	if !ok || oldMember.URL != record.CandidateMember.URL || oldMember.LogURL != record.CandidateMember.LogURL || oldMember.PeerURL != record.CandidateMember.PeerURL {
		return "", fmt.Errorf("fencing: engine repair bootstrap membership does not preserve the ordinal endpoint")
	}
	// The resolved Pod environment supplies the archive prefix, but never a
	// credential reference or a peer/admin token for this grant.
	if runtimeEnv["RHIZA_OBJSTORE_PREFIX"] == "" {
		return "", fmt.Errorf("fencing: engine repair bootstrap archive prefix is unavailable")
	}
	identityKey := path.Join(runtimeEnv["RHIZA_OBJSTORE_PREFIX"], request.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(request.NodeID))))
	identityData, err := readAuthorityObject(ctx, e.ArchiveBucket, identityKey)
	if err != nil {
		return "", err
	}
	var registered registeredVoterIdentity
	decoder := json.NewDecoder(bytes.NewReader(identityData))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&registered) != nil || decoder.Decode(new(any)) != io.EOF || registered.Version != 2 || registered.Cluster != request.SourceClusterID ||
		registered.Node != request.NodeID || !registered.Registered || registered.Membership == "" {
		return "", fmt.Errorf("fencing: immutable bootstrap membership registration is invalid")
	}
	canonicalRegistration, err := json.Marshal(registered)
	if err != nil || !bytes.Equal(canonicalRegistration, identityData) {
		return "", fmt.Errorf("fencing: immutable bootstrap membership registration is not canonical")
	}
	durability := runtimeEnv["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	if recovery.NewMembershipRecord(request.SourceClusterID, members, durability).Membership != registered.Membership {
		return "", fmt.Errorf("fencing: bootstrap membership differs from immutable genesis registration")
	}
	encoded, err := json.Marshal(members)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (e *FencingExecutor) engineRepairRecordForCandidatePod(ctx context.Context, statefulSetUID, sourceClusterID, oldMemberID, podName, podUID, runID string) (fencing.EngineRepairRecord, bool, error) {
	if e == nil || e.AuthorityState == nil || statefulSetUID == "" || sourceClusterID == "" || oldMemberID == "" || podName == "" || podUID == "" || runID == "" {
		return fencing.EngineRepairRecord{}, false, nil
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, sourceClusterID)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return fencing.EngineRepairRecord{}, false, nil
		}
		return fencing.EngineRepairRecord{}, false, err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != sourceClusterID {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: canonical repair root identity differs")
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, sourceClusterID); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	var match *fencing.EngineRepairRecord
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	for _, reference := range root.Ledger.EngineRepairs {
		if reference.Binding.SourceClusterID != sourceClusterID {
			continue
		}
		record, err := store.ReadEngineRepair(ctx, sourceClusterID, reference.OperationID)
		if err != nil {
			return fencing.EngineRepairRecord{}, false, err
		}
		current := record.CurrentCandidateIdentity()
		if current.PodName != podName {
			continue
		}
		if record.Phase == fencing.RepairGenerationClosing && record.ClosingFrom == fencing.RepairActive {
			return fencing.EngineRepairRecord{}, true, fencing.ErrClosed
		}
		if reference.Binding.OldMemberID != oldMemberID || current.PodUID != podUID {
			continue
		}
		if match != nil {
			return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: canonical engine repair candidate is ambiguous")
		}
		if record.Binding.StatefulSetUID != statefulSetUID || record.Binding.OldMemberID != oldMemberID ||
			(record.CurrentCandidateIdentity().RunID != runID && candidateRunIDReserved(record, runID)) ||
			!engineRepairAllowsCandidateStartup(record.Phase) || root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
			return fencing.EngineRepairRecord{}, true, fencing.ErrClosed
		}
		copy := record
		match = &copy
	}
	if match == nil {
		return fencing.EngineRepairRecord{}, false, nil
	}
	return *match, true, nil
}

func candidateRunIDReserved(record fencing.EngineRepairRecord, runID string) bool {
	if runID == "" || record.CandidateIdentity.RunID == runID {
		return true
	}
	for _, continuation := range record.CandidateContinuations {
		if continuation.Identity.RunID == runID {
			return true
		}
	}
	return false
}

func (e *FencingExecutor) engineRepairRecordForRuntime(ctx context.Context, statefulSetUID string, identity fencing.Identity) (fencing.EngineRepairRecord, bool, error) {
	if e == nil || e.AuthorityState == nil || statefulSetUID == "" || identity.SourceClusterID == "" || identity.NodeID == "" || identity.PodName == "" || identity.PodUID == "" || identity.RunID == "" {
		return fencing.EngineRepairRecord{}, false, nil
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return fencing.EngineRepairRecord{}, false, nil
		}
		return fencing.EngineRepairRecord{}, false, err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != identity.SourceClusterID {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: canonical repair root identity differs")
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, identity.SourceClusterID); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	var match *fencing.EngineRepairRecord
	for _, reference := range root.Ledger.EngineRepairs {
		if reference.Binding.TargetMemberID != identity.NodeID || reference.Binding.SourceClusterID != identity.SourceClusterID {
			continue
		}
		record, err := store.ReadEngineRepair(ctx, identity.SourceClusterID, reference.OperationID)
		if err != nil {
			return fencing.EngineRepairRecord{}, false, err
		}
		_, currentIdentity, _, currentErr := store.ReadEngineRepairCurrentCandidate(ctx, identity.SourceClusterID, reference.OperationID)
		if currentErr != nil {
			return fencing.EngineRepairRecord{}, false, currentErr
		}
		if currentIdentity.PodName != identity.PodName {
			continue
		}
		if match != nil {
			return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: canonical engine repair runtime is ambiguous")
		}
		if record.Binding.StatefulSetUID != statefulSetUID || currentIdentity != identity ||
			!engineRepairAllowsCandidateStartup(record.Phase) || root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
			return fencing.EngineRepairRecord{}, true, fencing.ErrClosed
		}
		copy := record
		match = &copy
	}
	if match == nil {
		return fencing.EngineRepairRecord{}, false, nil
	}
	return *match, true, nil
}

// currentEngineRepairForVoter selects the highest canonical generation and
// verifies that the authenticated caller is still an exact active voter in
// that repair's original configuration. Historical records are never adopted
// as permission to coordinate a newer membership.
func (e *FencingExecutor) currentEngineRepairForVoter(ctx context.Context, statefulSetUID string, identity fencing.Identity) (fencing.EngineRepairRecord, bool, error) {
	if e == nil || e.AuthorityState == nil || statefulSetUID == "" || identity.SourceClusterID == "" || identity.PodUID == "" || identity.RunID == "" || identity.WALIdentity == "" {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: engine repair voter identity is incomplete")
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return fencing.EngineRepairRecord{}, false, nil
		}
		return fencing.EngineRepairRecord{}, false, err
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != identity.SourceClusterID {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: canonical repair root identity differs")
	}
	if err := validateEngineRepairRootReferences(root, statefulSetUID, identity.SourceClusterID); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	var selected *fencing.EngineRepairRecord
	for _, reference := range root.Ledger.EngineRepairs {
		record, err := store.ReadEngineRepair(ctx, identity.SourceClusterID, reference.OperationID)
		if err != nil {
			return fencing.EngineRepairRecord{}, false, err
		}
		if selected == nil || record.Binding.OldConfigID > selected.Binding.OldConfigID {
			copy := record
			selected = &copy
			continue
		}
		if record.Binding.OldConfigID == selected.Binding.OldConfigID {
			return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: multiple engine repairs share the current generation")
		}
	}
	if selected == nil {
		return fencing.EngineRepairRecord{}, false, nil
	}
	if err := selected.ValidateAgainstLedger(root.Ledger); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	if err := engineRepairCallerIsActiveSurvivor(fenceLedger{StatefulSetUID: root.StatefulSetUID, Ledger: root.Ledger}, identity); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	member, ok := selected.SourceConfig.MemberSet()[quepaxa.NodeID(identity.NodeID)]
	if !ok || identity.NodeID == selected.Binding.OldMemberID || member.WALIdentity != identity.WALIdentity {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("fencing: engine repair caller is not an original surviving voter")
	}
	return *selected, true, nil
}

func engineRepairCallerIsActiveSurvivor(ledger fenceLedger, identity fencing.Identity) error {
	run, ok := ledger.Ledger.Runs[identity.RunID]
	if !ok || run.Identity != identity || run.Runtime == nil || run.Stop != nil || run.Drain != nil || run.Exited ||
		run.NeverBoundExit != nil || run.IndependentTerminal != nil || fencing.RunRole(run) != fencing.StartupRoleVoterActive ||
		ledger.Ledger.ClosedBy != "" || ledger.Ledger.ClosedHash != "" {
		return fmt.Errorf("fencing: engine repair caller is not an active surviving voter run")
	}
	if run.Runtime.Validate(identity) != nil || run.Runtime.StatefulSetUID != ledger.StatefulSetUID || run.Runtime.SourceClusterID != ledger.Ledger.SourceClusterID {
		return fmt.Errorf("fencing: engine repair caller runtime binding differs")
	}
	return nil
}

func (e *FencingExecutor) engineRepairRunnerRequest(ctx context.Context, authorization, statefulSetUID string, identity fencing.Identity, request fencing.EngineRepairRequest, action string) (fencing.EngineRepairResponse, error) {
	var response fencing.EngineRepairResponse
	if request.Identity != identity || identity.WALIdentity == "" {
		return response, fmt.Errorf("fencing: engine repair identity differs from authenticated runtime")
	}
	if action != "engine-repair-storage-effects-verify" && request.RemovalOperation != nil {
		return response, fmt.Errorf("fencing: unexpected engine removal operation payload")
	}
	state, err := e.read(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return response, err
	}
	if err := engineRepairCallerIsActiveSurvivor(state, identity); err != nil {
		return response, err
	}
	run := state.Ledger.Runs[identity.RunID]
	if run.Runtime == nil || run.Runtime.StatefulSetUID != statefulSetUID || run.Runtime.Validate(identity) != nil {
		return response, fmt.Errorf("fencing: engine repair caller runtime binding differs")
	}
	switch action {
	case "engine-repair-current":
		if request.OperationID != "" || request.ExpectedDigest != "" || request.Record != nil {
			return response, fmt.Errorf("fencing: malformed current engine repair request")
		}
		record, found, err := e.currentEngineRepairForVoter(ctx, statefulSetUID, identity)
		if err != nil || !found {
			return response, err
		}
		response.Found = true
		response.Record = &record
		return response, nil
	case "engine-repair-cas":
		if request.OperationID == "" || request.ExpectedDigest == "" || request.Record == nil ||
			request.Record.Binding.OperationID != request.OperationID || request.Record.Binding.SourceClusterID != identity.SourceClusterID {
			return response, fmt.Errorf("fencing: malformed engine repair CAS request")
		}
		current, found, err := e.currentEngineRepairForVoter(ctx, statefulSetUID, identity)
		if err != nil || !found {
			return response, errors.Join(err, fmt.Errorf("fencing: no canonical engine repair is available for CAS"))
		}
		if current.Binding.OperationID != request.OperationID || current.Digest() != request.ExpectedDigest {
			return response, fmt.Errorf("fencing: canonical engine repair changed before CAS")
		}
		member, ok := request.Record.SourceConfig.MemberSet()[quepaxa.NodeID(identity.NodeID)]
		if !ok || identity.NodeID == request.Record.Binding.OldMemberID || member.WALIdentity != identity.WALIdentity {
			return response, fmt.Errorf("fencing: engine repair CAS caller is not a surviving source voter")
		}
		store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if err != nil {
			return response, err
		}
		updated, err := store.CompareAndSwapEngineRepair(ctx, identity.SourceClusterID, request.ExpectedDigest, *request.Record)
		if err != nil {
			return response, err
		}
		response.Found, response.Record = true, &updated
		return response, nil
	case "engine-repair-storage-effects":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil {
			return response, fmt.Errorf("fencing: malformed engine repair storage-closure request")
		}
		proof, err := e.executeEngineRepairStorageEffects(ctx, authorization, identity, request.OperationID)
		if err != nil {
			return response, err
		}
		response.Proof = &proof
		return response, nil
	case "engine-repair-storage-effects-verify":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil || request.PendingID != "" || request.PendingDigest != "" ||
			request.RemovalOperation == nil || request.RemovalOperation.OperationID != request.OperationID || request.RemovalOperation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
			return response, fmt.Errorf("fencing: malformed engine removal storage-effects verification request")
		}
		if err := e.verifyEngineRemovalStorageEffects(ctx, statefulSetUID, identity, request.OperationID, *request.RemovalOperation); err != nil {
			return response, err
		}
		return response, nil
	case "engine-repair-confirm":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil {
			return response, fmt.Errorf("fencing: malformed engine repair confirmation request")
		}
		record, err := e.confirmEngineRepairActivation(ctx, statefulSetUID, identity, request.OperationID)
		if err != nil {
			return response, err
		}
		response.Found = true
		response.Record = &record
		return response, nil
	case "engine-repair-pending":
		if request.OperationID != "" || request.ExpectedDigest != "" || request.Record != nil || request.PendingID != "" || request.PendingDigest != "" {
			return response, fmt.Errorf("fencing: malformed pending engine repair lookup")
		}
		store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if err != nil {
			return response, err
		}
		requests, err := store.ListPendingEngineRepairs(ctx, identity.SourceClusterID)
		if err != nil {
			return response, err
		}
		response.PendingRequests = requests
		return response, nil
	case "engine-repair-adopt":
		if request.PendingID == "" || !validEvidenceDigest(request.PendingDigest) || request.OperationID != "" || request.ExpectedDigest != "" || request.Record == nil {
			return response, fmt.Errorf("fencing: malformed pending engine repair adoption")
		}
		record := *request.Record
		if err := record.ValidateAdoptionProposal(); err != nil {
			return response, err
		}
		member, ok := record.SourceConfig.MemberSet()[quepaxa.NodeID(identity.NodeID)]
		if record.Binding.StatefulSetUID != statefulSetUID || record.Binding.SourceClusterID != identity.SourceClusterID ||
			record.Binding.OldMemberID == identity.NodeID || !ok || member.WALIdentity != identity.WALIdentity {
			return response, fmt.Errorf("fencing: pending repair adopter is not an original surviving voter")
		}
		if err := engineRepairCallerIsActiveSurvivor(state, identity); err != nil {
			return response, err
		}
		store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
		if err != nil {
			return response, err
		}
		adopted, err := store.AdoptPendingEngineRepair(ctx, request.PendingID, request.PendingDigest, record)
		if err != nil {
			return response, err
		}
		response.Found = true
		response.Record = &adopted
		return response, nil
	default:
		return response, fmt.Errorf("fencing: unknown engine repair action")
	}
}

func (e *FencingExecutor) bindEngineRepairCandidate(ctx context.Context, statefulSetUID string, identity fencing.Identity) (fencing.BindReceipt, bool, error) {
	record, found, err := e.engineRepairRecordForRuntime(ctx, statefulSetUID, identity)
	if err != nil || !found {
		return fencing.BindReceipt{}, found, err
	}
	if record.Phase == fencing.RepairActive && sameEngineRepairCandidateRun(record.CurrentCandidateIdentity(), identity) {
		// An already-active candidate restart resumes the ordinary active-voter
		// bind path. Its current Run/runtime was advanced atomically in the root;
		// do not route it through the learner-only WAL binding transition.
		return fencing.BindReceipt{}, false, nil
	}
	if _, _, err := e.readEngineRepairCredential(ctx, record); err != nil {
		return fencing.BindReceipt{}, true, err
	}
	if record.Binding.StatefulSetUID != statefulSetUID || record.Phase == fencing.RepairActive ||
		record.CurrentCandidateIdentity() != identity || record.CurrentCandidateRuntime() == nil ||
		record.CurrentCandidateRuntime().Validate(identity) != nil {
		return fencing.BindReceipt{}, true, fencing.ErrClosed
	}
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return fencing.BindReceipt{}, true, err
	}
	expected := engineRepairRootReference(record)
	state, err := e.change(ctx, statefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		if ledger.EngineRepairs[key] != expected {
			return fencing.ErrClosed
		}
		run, exists := ledger.Runs[identity.RunID]
		unbound := identity
		unbound.WALIdentity = ""
		if !exists || (run.Identity != unbound && run.Identity != identity) || run.Runtime == nil ||
			*run.Runtime != *record.CurrentCandidateRuntime() || fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove ||
			run.Stop != nil || run.Exited || run.NeverBoundExit != nil || run.Promotion != nil ||
			run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" {
			return fencing.ErrClosed
		}
		return ledger.Bind(identity)
	})
	if err != nil {
		return fencing.BindReceipt{}, true, err
	}
	run, exists := state.Ledger.Runs[identity.RunID]
	if !exists || run.Identity != identity || run.Runtime == nil || *run.Runtime != *record.CurrentCandidateRuntime() ||
		state.Ledger.EngineRepairs[key] != expected {
		return fencing.BindReceipt{}, true, fmt.Errorf("fencing: bound engine repair candidate readback differs")
	}
	return fencing.BindReceipt{Identity: identity, Role: fencing.StartupRoleLearnerPendingRemove}, true, nil
}

func (e *FencingExecutor) registerEngineRepairLearnerIdentity(ctx context.Context, statefulSetUID string, identity fencing.Identity, registration fencing.LearnerIdentityRegistration) (bool, error) {
	record, found, err := e.engineRepairRecordForRuntime(ctx, statefulSetUID, identity)
	if err != nil || !found {
		return found, err
	}
	if e.ArchiveBucket == nil || !slices.Contains(e.ArchiveBucket.SupportedObjectUploadOptions(), objstore.IfNotExists) || len(registration.Data) == 0 || len(registration.Data) > 64<<10 {
		return true, fmt.Errorf("fencing: engine repair identity registration is unavailable")
	}
	if _, _, err := e.readEngineRepairCredential(ctx, record); err != nil {
		return true, err
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", identity.PodName), &pod); err != nil {
		return true, err
	}
	if str(nested(pod, "metadata", "uid")) != identity.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return true, fmt.Errorf("fencing: engine repair identity Pod changed")
	}
	var runtimeEnv map[string]string
	for _, raw := range list(nested(pod, "spec", "containers")) {
		env, err := e.controller().environment(ctx, asObject(raw))
		if err != nil {
			return true, err
		}
		if env["RHIZA_CLUSTER_ID"] == identity.SourceClusterID {
			if runtimeEnv != nil {
				return true, fmt.Errorf("fencing: engine repair application container is ambiguous")
			}
			runtimeEnv = env
		}
	}
	if runtimeEnv == nil {
		return true, fmt.Errorf("fencing: engine repair application configuration is unavailable")
	}
	members, err := recoveryMembers(runtimeEnv["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(members) < 2 {
		return true, fmt.Errorf("fencing: engine repair source membership is unavailable")
	}
	durability := runtimeEnv["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	membership := recovery.NewMembershipRecord(identity.SourceClusterID, members, durability)
	provider := runtimeEnv["RHIZA_OBJSTORE_PROVIDER"]
	if provider == "" {
		provider = "filesystem"
	}
	storeData, _ := json.Marshal([]string{provider, runtimeEnv["RHIZA_OBJSTORE_ENDPOINT"], runtimeEnv["RHIZA_OBJSTORE_BUCKET"], path.Clean(runtimeEnv["RHIZA_OBJSTORE_PREFIX"]), runtimeEnv["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"]})
	storeDigest := sha256.Sum256(storeData)
	want := registeredVoterIdentity{Version: 2, Cluster: identity.SourceClusterID, Node: identity.NodeID,
		Membership: membership.Membership, Store: hex.EncodeToString(storeDigest[:]), Nonce: identity.WALIdentity,
		Registered: true, LearnerPublicKey: fmt.Sprintf("%x", record.CandidateMember.PublicKey[:])}
	wantData, err := json.Marshal(want)
	if err != nil || !bytes.Equal(wantData, registration.Data) {
		return true, fmt.Errorf("fencing: engine repair voter identity differs from its reserved candidate")
	}
	key := path.Join(runtimeEnv["RHIZA_OBJSTORE_PREFIX"], identity.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(identity.NodeID))))
	if registration.Key != key {
		return true, fmt.Errorf("fencing: engine repair voter identity key differs")
	}
	current, readErr := readAuthorityObject(ctx, e.ArchiveBucket, key)
	if readErr == nil {
		if !bytes.Equal(current, registration.Data) {
			return true, fmt.Errorf("fencing: immutable engine repair voter identity already differs")
		}
	} else if !e.ArchiveBucket.IsObjNotFoundErr(readErr) {
		return true, readErr
	}
	if err := e.registerEngineRepairCandidateRun(ctx, statefulSetUID, record, identity); err != nil {
		return true, err
	}
	if readErr == nil {
		return true, nil
	}
	writeErr := e.ArchiveBucket.Upload(ctx, key, bytes.NewReader(registration.Data), objstore.WithIfNotExists())
	if writeErr != nil && !e.ArchiveBucket.IsConditionNotMetErr(writeErr) {
		return true, writeErr
	}
	readback, readErr := readAuthorityObject(ctx, e.ArchiveBucket, key)
	if readErr != nil || !bytes.Equal(readback, registration.Data) {
		return true, errors.Join(readErr, fmt.Errorf("fencing: engine repair voter identity conditional-write readback differs"))
	}
	return true, nil
}

// registerEngineRepairCandidateRun verifies the trusted learner registration
// created with the canonical candidate pin. Its WAL stays empty until Bind.
// The pending role carries no producer authority.
func (e *FencingExecutor) registerEngineRepairCandidateRun(ctx context.Context, statefulSetUID string, record fencing.EngineRepairRecord, identity fencing.Identity) error {
	currentIdentity := record.CurrentCandidateIdentity()
	currentRuntime := record.CurrentCandidateRuntime()
	if e == nil || ctx == nil || record.Validate() != nil ||
		record.Binding.StatefulSetUID != statefulSetUID || currentIdentity != identity || currentRuntime == nil ||
		!engineRepairAllowsCandidateStartup(record.Phase) {
		return fmt.Errorf("fencing: canonical engine repair runtime enrollment is unavailable")
	}
	generationKey, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return err
	}
	state, err := e.read(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return err
	}
	_, voterClosed := state.Ledger.ClosedNodes[identity.NodeID]
	if state.StatefulSetUID != statefulSetUID || state.Ledger.SourceClusterID != identity.SourceClusterID ||
		state.Ledger.ClosedBy != "" || state.Ledger.ClosedHash != "" || voterClosed ||
		state.Ledger.EngineRepairs[generationKey] != engineRepairRootReference(record) {
		return fencing.ErrClosed
	}
	run, exists := state.Ledger.Runs[identity.RunID]
	unboundIdentity := identity
	unboundIdentity.WALIdentity = ""
	provenance := run.EngineRepairProvenance
	if !exists || (run.Identity != unboundIdentity && run.Identity != identity) || run.Runtime == nil || *run.Runtime != *currentRuntime ||
		fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.Stop != nil || run.Exited || run.NeverBoundExit != nil ||
		run.Promotion != nil || run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" ||
		provenance == nil || provenance.OperationID != record.Binding.OperationID || provenance.IdentityDigest != record.IdentityDigest() ||
		provenance.CandidateIdentity != identity || provenance.ValidateForRun(run) != nil {
		return fmt.Errorf("fencing: engine repair runtime run differs from its canonical unbound learner candidate")
	}
	return nil
}

func engineRepairAllowsCandidateStartup(phase fencing.EngineRepairPhase) bool {
	switch phase {
	case fencing.RepairCandidatePrepared, fencing.RepairRemoveFrozen, fencing.RepairStorageClosing, fencing.RepairRemoved, fencing.RepairAdding, fencing.RepairActive:
		return true
	default:
		return false
	}
}

func (e *FencingExecutor) readEngineRepairCredential(ctx context.Context, record fencing.EngineRepairRecord) ([]byte, string, error) {
	credential := record.CandidateCredential
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", record.CandidateIdentity.PodName), &pod); err != nil {
		return nil, "", err
	}
	if str(nested(pod, "metadata", "uid")) != record.CandidateIdentity.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, "", fmt.Errorf("fencing: engine repair credential Pod incarnation differs")
	}
	statefulSetName := ""
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if owner["controller"] != true || owner["kind"] != "StatefulSet" || str(owner["uid"]) != record.Binding.StatefulSetUID {
			continue
		}
		if statefulSetName != "" {
			return nil, "", fmt.Errorf("fencing: engine repair Pod has ambiguous StatefulSet ownership")
		}
		statefulSetName = str(owner["name"])
	}
	if statefulSetName == "" {
		return nil, "", fmt.Errorf("fencing: engine repair Pod lacks its pinned StatefulSet owner")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(statefulSetName), &sts); err != nil {
		return nil, "", err
	}
	if str(nested(sts, "metadata", "uid")) != record.Binding.StatefulSetUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return nil, "", fmt.Errorf("fencing: engine repair StatefulSet incarnation differs")
	}
	var templateContainer, podContainer object
	for _, raw := range list(nested(sts, "spec", "template", "spec", "containers")) {
		candidate := asObject(raw)
		env, err := e.controller().environment(ctx, candidate)
		if err != nil {
			return nil, "", err
		}
		if env["RHIZA_CLUSTER_ID"] != record.Binding.SourceClusterID {
			continue
		}
		if templateContainer != nil {
			return nil, "", fmt.Errorf("fencing: engine repair StatefulSet application container is ambiguous")
		}
		templateContainer = candidate
	}
	if templateContainer == nil {
		return nil, "", fmt.Errorf("fencing: engine repair StatefulSet application container is unavailable")
	}
	var err error
	podContainer, err = namedPodContainer(pod, str(templateContainer["name"]))
	if err != nil {
		return nil, "", err
	}
	templateEnv, err := e.controller().environment(ctx, templateContainer)
	if err != nil {
		return nil, "", err
	}
	podEnv, err := e.controller().environment(ctx, podContainer)
	if err != nil {
		return nil, "", err
	}
	if templateEnv["RHIZA_PEER_TOKEN"] != podEnv["RHIZA_PEER_TOKEN"] || templateEnv["RHIZA_PEER_TOKENS"] != podEnv["RHIZA_PEER_TOKENS"] {
		return nil, "", fmt.Errorf("fencing: engine repair Pod credential source differs from its StatefulSet")
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", credential.Name), &secret); err != nil {
		return nil, "", err
	}
	if secret["type"] != "Opaque" ||
		str(nested(secret, "metadata", "name")) != credential.Name || str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace ||
		str(nested(secret, "metadata", "uid")) != credential.UID {
		return nil, "", fmt.Errorf("fencing: engine repair peer credential identity differs")
	}
	data := asObject(secret["data"])
	var token string
	if templateEnv["RHIZA_PEER_TOKEN"] != "" && templateEnv["RHIZA_PEER_TOKENS"] == "" &&
		engineRepairSecretEnvSource(templateContainer, "RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKEN", credential.Name) &&
		engineRepairSecretEnvSource(podContainer, "RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKEN", credential.Name) {
		encoded := str(data["RHIZA_PEER_TOKEN"])
		tokenBytes, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil {
			return nil, "", fmt.Errorf("fencing: fixed peer token encoding is invalid")
		}
		token = string(tokenBytes)
	} else if templateEnv["RHIZA_PEER_TOKENS"] != "" && templateEnv["RHIZA_PEER_TOKEN"] == "" &&
		engineRepairSecretEnvSource(templateContainer, "RHIZA_PEER_TOKENS", "RHIZA_PEER_TOKENS", credential.Name) &&
		engineRepairSecretEnvSource(podContainer, "RHIZA_PEER_TOKENS", "RHIZA_PEER_TOKENS", credential.Name) {
		encoded := str(data["RHIZA_PEER_TOKENS"])
		tokensBytes, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil {
			return nil, "", fmt.Errorf("fencing: fixed peer token map encoding is invalid")
		}
		var tokens map[string]string
		if json.Unmarshal(tokensBytes, &tokens) != nil {
			return nil, "", fmt.Errorf("fencing: fixed peer token map is invalid")
		}
		token = tokens[record.Binding.OldMemberID]
	}
	tokenBytes := []byte(token)
	if len(tokenBytes) == 0 || fencing.EngineRepairPeerTokenDigest(tokenBytes) != credential.PeerTokenDigest ||
		quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(record.Binding.SourceClusterID), record.CandidateMember.ID, token)) != record.CandidateMember.PublicKey {
		return nil, "", fmt.Errorf("fencing: engine repair peer credential does not match its public identity")
	}
	memberBytes, err := json.Marshal(record.CandidateMember)
	if err != nil {
		return nil, "", err
	}
	return memberBytes, token, nil
}

func engineRepairSecretEnvSource(container object, name, key, secretName string) bool {
	explicit := false
	for _, raw := range list(container["env"]) {
		entry := asObject(raw)
		if str(entry["name"]) != name {
			continue
		}
		explicit = true
		return str(nested(entry, "valueFrom", "secretKeyRef", "name")) == secretName && str(nested(entry, "valueFrom", "secretKeyRef", "key")) == key
	}
	if explicit {
		return false
	}
	for _, raw := range list(container["envFrom"]) {
		if str(nested(asObject(raw), "secretRef", "name")) == secretName {
			return true
		}
	}
	return false
}

func (e *FencingExecutor) verifyRegisteredLostVoterIdentity(ctx context.Context, identity fencing.Identity, record fencing.EngineRepairRecord, localHash string) error {
	if !validEvidenceDigest(localHash) || record.Binding.OldMemberID == "" || record.Binding.OldWALIdentity == "" {
		return fmt.Errorf("fencing: typed lost-WAL identity is incomplete")
	}
	podName := identity.PodName
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return err
	}
	if str(nested(pod, "metadata", "uid")) != identity.PodUID || str(nested(pod, "metadata", "name")) != podName || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fmt.Errorf("fencing: recovery bootstrap Pod incarnation changed")
	}
	var runtimeEnv map[string]string
	for _, raw := range list(nested(pod, "spec", "containers")) {
		env, err := e.controller().environment(ctx, asObject(raw))
		if err != nil {
			return err
		}
		if env["RHIZA_NODE_ID"] == record.Binding.OldMemberID && env["RHIZA_CLUSTER_ID"] == record.Binding.SourceClusterID {
			if runtimeEnv != nil {
				return fmt.Errorf("fencing: recovery bootstrap application container is ambiguous")
			}
			runtimeEnv = env
		}
	}
	if runtimeEnv == nil || runtimeEnv["RHIZA_OBJSTORE_PREFIX"] == "" {
		return fmt.Errorf("fencing: recovery bootstrap source archive binding is unavailable")
	}
	identityKey := path.Join(runtimeEnv["RHIZA_OBJSTORE_PREFIX"], record.Binding.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(record.Binding.OldMemberID))))
	registered, err := readAuthorityObject(ctx, e.ArchiveBucket, identityKey)
	if err != nil {
		return err
	}
	var saved registeredVoterIdentity
	decoder := json.NewDecoder(bytes.NewReader(registered))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&saved) != nil || decoder.Decode(new(any)) != io.EOF || saved.Version != 2 || !saved.Registered ||
		saved.Cluster != record.Binding.SourceClusterID || saved.Node != record.Binding.OldMemberID || saved.Nonce != record.Binding.OldWALIdentity {
		return fmt.Errorf("fencing: immutable registered old-voter identity differs")
	}
	canonical, err := json.Marshal(saved)
	if err != nil || !bytes.Equal(canonical, registered) {
		return fmt.Errorf("fencing: immutable registered old-voter identity is not canonical")
	}
	digest := sha256.Sum256(registered)
	if hex.EncodeToString(digest[:]) != localHash {
		return fmt.Errorf("fencing: local typed lost-WAL identity differs from immutable registration")
	}
	return nil
}

func (e *FencingExecutor) verifyRegisteredCandidateIdentity(ctx context.Context, identity fencing.Identity, record fencing.EngineRepairRecord, localHash string) error {
	if !validEvidenceDigest(localHash) || identity.PodName == "" || record.CandidateIdentity.PodName != identity.PodName ||
		record.CandidateIdentity.PodUID != identity.PodUID || record.CandidateIdentity.RunID != identity.RunID {
		return fmt.Errorf("fencing: reserved candidate identity is incomplete")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", identity.PodName), &pod); err != nil {
		return err
	}
	if str(nested(pod, "metadata", "uid")) != identity.PodUID || str(nested(pod, "metadata", "name")) != identity.PodName || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fmt.Errorf("fencing: recovery bootstrap Pod incarnation changed")
	}
	var runtimeEnv map[string]string
	for _, raw := range list(nested(pod, "spec", "containers")) {
		env, err := e.controller().environment(ctx, asObject(raw))
		if err != nil {
			return err
		}
		if env["RHIZA_NODE_ID"] == record.Binding.OldMemberID && env["RHIZA_CLUSTER_ID"] == record.Binding.SourceClusterID {
			if runtimeEnv != nil {
				return fmt.Errorf("fencing: recovery bootstrap application container is ambiguous")
			}
			runtimeEnv = env
		}
	}
	if runtimeEnv == nil || runtimeEnv["RHIZA_OBJSTORE_PREFIX"] == "" {
		return fmt.Errorf("fencing: recovery bootstrap source archive binding is unavailable")
	}
	members, err := recoveryMembers(runtimeEnv["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(members) < 2 {
		return fmt.Errorf("fencing: recovery bootstrap source membership is unavailable")
	}
	durability := runtimeEnv["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	membership := recovery.NewMembershipRecord(record.Binding.SourceClusterID, members, durability)
	provider := runtimeEnv["RHIZA_OBJSTORE_PROVIDER"]
	if provider == "" {
		provider = "filesystem"
	}
	storeData, _ := json.Marshal([]string{provider, runtimeEnv["RHIZA_OBJSTORE_ENDPOINT"], runtimeEnv["RHIZA_OBJSTORE_BUCKET"], path.Clean(runtimeEnv["RHIZA_OBJSTORE_PREFIX"]), runtimeEnv["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"]})
	storeDigest := sha256.Sum256(storeData)
	base := registeredVoterIdentity{Version: 2, Cluster: record.Binding.SourceClusterID, Node: record.CandidateIdentity.NodeID,
		Membership: membership.Membership, Store: hex.EncodeToString(storeDigest[:]), Nonce: record.CandidateIdentity.WALIdentity,
		LearnerPublicKey: fmt.Sprintf("%x", record.CandidateMember.PublicKey[:])}
	base.Registered = false
	unregistered, err := json.Marshal(base)
	if err != nil {
		return err
	}
	base.Registered = true
	registered, err := json.Marshal(base)
	if err != nil {
		return err
	}
	wantHash := sha256.Sum256(unregistered)
	localRegistered := false
	if hex.EncodeToString(wantHash[:]) != localHash {
		wantHash = sha256.Sum256(registered)
		if hex.EncodeToString(wantHash[:]) != localHash {
			return fmt.Errorf("fencing: local candidate identity differs from its canonical assignment")
		}
		localRegistered = true
	}
	key := path.Join(runtimeEnv["RHIZA_OBJSTORE_PREFIX"], identity.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(record.CandidateIdentity.NodeID))))
	remote, readErr := readAuthorityObject(ctx, e.ArchiveBucket, key)
	if readErr == nil {
		if !bytes.Equal(remote, registered) {
			return fmt.Errorf("fencing: immutable candidate voter identity differs")
		}
		return nil
	}
	if !e.ArchiveBucket.IsObjNotFoundErr(readErr) {
		return readErr
	}
	if localRegistered {
		return fmt.Errorf("fencing: registered candidate identity is absent from the archive")
	}
	return nil
}
