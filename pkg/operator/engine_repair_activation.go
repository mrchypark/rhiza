package operator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"slices"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type engineRepairMembershipOperation struct {
	Request network.MembershipChange `json:"request"`
	Target  quepaxa.Cluster          `json:"target"`
}

// engineRepairCandidateActivationEvidence proves the assigned candidate has
// applied and durably archived the exact canonical ADD. Readiness is reported
// for diagnostics only; quorum, applied tip, archive tip, and exact membership
// readbacks are all independently required.
func (e *FencingExecutor) engineRepairCandidateActivationEvidence(ctx context.Context, runtime fencing.RuntimeBinding, record fencing.EngineRepairRecord) (learnerPromotionHash, error) {
	candidate := record.CurrentCandidateIdentity()
	if e == nil || ctx == nil || e.Kube == nil || e.ArchiveBucket == nil || e.AuthorityState == nil ||
		record.Validate() != nil || record.RemovalOperation == nil || record.AddRequestHash == "" ||
		(record.Phase != fencing.RepairAdding && record.Phase != fencing.RepairActive) ||
		runtime.Validate(candidate) != nil || !engineRepairRuntimeMatches(record, runtime) {
		return learnerPromotionHash{}, fmt.Errorf("fencing: canonical engine repair ADD evidence is incomplete")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, record.Binding.StatefulSetUID)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	canonical, err := store.ReadEngineRepair(ctx, record.Binding.SourceClusterID, record.Binding.OperationID)
	if err != nil {
		return learnerPromotionHash{}, fmt.Errorf("fencing: read canonical engine repair ADD record: %w", err)
	}
	if canonical.Digest() != record.Digest() {
		return learnerPromotionHash{}, fmt.Errorf("fencing: engine repair ADD record is not the exact canonical revision")
	}
	state, err := e.read(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	if err := validateEngineRepairLedgerForActivation(canonical, state.Ledger); err != nil {
		return learnerPromotionHash{}, err
	}
	request, target, err := engineRepairCanonicalAdd(canonical)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	if hashJSON(request) != canonical.AddRequestHash {
		return learnerPromotionHash{}, fmt.Errorf("fencing: canonical engine repair ADD request digest differs")
	}
	appEnv, err := e.engineRepairCandidateEnvironment(ctx, canonical)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	base := path.Join(appEnv["RHIZA_OBJSTORE_PREFIX"], canonical.Binding.SourceClusterID)
	operationName := fmt.Sprintf("%d.json", request.ExpectedConfigID)
	if request.ExpectedAbortSlot != 0 {
		operationName = fmt.Sprintf("%d-abort-%d.json", request.ExpectedConfigID, request.ExpectedAbortSlot)
	}
	key := path.Join(base, "membership", operationName)
	data, err := readBoundedArchiveObject(ctx, e.ArchiveBucket, key, 64<<10)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	var operation engineRepairMembershipOperation
	if err := decodeCanonicalArchiveJSON(data, &operation); err != nil || !sameEngineRepairMembershipChange(operation.Request, request) || !sameRepairSourceConfig(operation.Target, target) {
		return learnerPromotionHash{}, fmt.Errorf("fencing: durable engine repair ADD readback differs from its exact request")
	}

	adminToken, err := e.readEngineRepairAdminToken(ctx, canonical)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	membership, err := readEngineRepairMembershipStatus(ctx, canonical.CandidateMember.URL, adminToken)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	candidate = canonical.CurrentCandidateIdentity()
	if membership.ClusterID != canonical.Binding.SourceClusterID || membership.NodeID != canonical.CandidateMember.ID ||
		membership.ConfigID != target.ConfigID || membership.WALIdentity != candidate.WALIdentity ||
		!membership.Voting || membership.Pending || !sameNodeIDs(membership.Voters, target.Members) {
		return learnerPromotionHash{}, fmt.Errorf("fencing: candidate membership status differs from the canonical ADD target")
	}
	recoveryStatus, err := readEngineRepairRecoveryStatus(ctx, canonical.CandidateMember.URL)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	minimumTip := canonical.Binding.FreezeSlot + 33
	if minimumTip < canonical.Binding.FreezeSlot || recoveryStatus.NodeID != string(canonical.CandidateMember.ID) ||
		recoveryStatus.ClusterID != canonical.Binding.SourceClusterID || !recoveryStatus.Quorum ||
		recoveryStatus.CertifiedTip < minimumTip || recoveryStatus.AppliedTip < minimumTip ||
		recoveryStatus.ArchiveTip < recoveryStatus.AppliedTip {
		return learnerPromotionHash{}, fmt.Errorf("fencing: candidate has not applied and archived the durable engine repair ADD prefix")
	}
	operationDigest := hashJSON(operation)
	evidence := hashJSON([]any{"engine-repair-add-active-v1", canonical.IdentityDigest(), canonical.AddRequestHash,
		operationDigest, membership, minimumTip})
	return learnerPromotionHash{operationID: engineRepairAddOperationID(canonical.Binding.OperationID), digest: evidence}, nil
}

func sameEngineRepairMembershipChange(left, right network.MembershipChange) bool {
	return reflect.DeepEqual(left, right)
}

func engineRepairCanonicalAdd(record fencing.EngineRepairRecord) (network.MembershipChange, quepaxa.Cluster, error) {
	if record.RemovalOperation == nil || record.StorageProofDigest == "" || record.RemoveTerminalDigest == "" || record.AddRequestHash == "" {
		return network.MembershipChange{}, quepaxa.Cluster{}, fmt.Errorf("fencing: engine repair ADD precedes durable REMOVE closure")
	}
	removeTarget := record.RemovalOperation.Target
	target := quepaxa.Cluster{ConfigID: removeTarget.ConfigID + 1, Members: append([]quepaxa.Member(nil), removeTarget.Members...)}
	if _, exists := target.MemberSet()[record.CandidateMember.ID]; exists {
		return network.MembershipChange{}, quepaxa.Cluster{}, fmt.Errorf("fencing: candidate already exists in REMOVE target")
	}
	target.Members = append(target.Members, record.CandidateMember)
	request := network.MembershipChange{OperationID: engineRepairAddOperationID(record.Binding.OperationID),
		ClusterID: record.Binding.SourceClusterID, ExpectedConfigID: removeTarget.ConfigID,
		ExpectedAbortSlot: 0, Add: &record.CandidateMember}
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) == 0 || hashJSON(request) != record.AddRequestHash {
		return network.MembershipChange{}, quepaxa.Cluster{}, fmt.Errorf("fencing: engine repair ADD request cannot be reconstructed exactly")
	}
	return request, target, nil
}

func engineRepairAddOperationID(operationID string) string {
	return fmt.Sprintf("engine-repair-add-%x", sha256.Sum256([]byte(operationID)))
}

func (e *FencingExecutor) engineRepairCandidateEnvironment(ctx context.Context, record fencing.EngineRepairRecord) (map[string]string, error) {
	candidate := record.CurrentCandidateIdentity()
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", candidate.PodName), &pod); err != nil {
		return nil, err
	}
	if str(nested(pod, "metadata", "uid")) != candidate.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, fmt.Errorf("fencing: candidate Pod incarnation changed during ADD verification")
	}
	for _, raw := range list(nested(pod, "spec", "containers")) {
		env, err := e.controller().environment(ctx, asObject(raw))
		if err != nil {
			return nil, err
		}
		if env["RHIZA_CLUSTER_ID"] == record.Binding.SourceClusterID && env["RHIZA_OBJSTORE_PREFIX"] != "" {
			return env, nil
		}
	}
	return nil, fmt.Errorf("fencing: candidate source archive configuration is unavailable")
}

func readEngineRepairMembershipStatus(ctx context.Context, endpoint, token string) (network.MembershipStatus, error) {
	var status network.MembershipStatus
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || token == "" {
		return status, fmt.Errorf("fencing: candidate membership endpoint is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/membership/status"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return status, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return status, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return status, fmt.Errorf("fencing: candidate membership readback returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil || decoder.Decode(new(any)) != io.EOF {
		return network.MembershipStatus{}, fmt.Errorf("fencing: candidate membership readback is malformed")
	}
	return status, nil
}

func readEngineRepairRecoveryStatus(ctx context.Context, endpoint string) (network.VoterRecoveryStatus, error) {
	var status network.VoterRecoveryStatus
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return status, fmt.Errorf("fencing: candidate recovery endpoint is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/recovery/status"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return status, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return status, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return status, fmt.Errorf("fencing: candidate recovery readback returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil || decoder.Decode(new(any)) != io.EOF {
		return network.VoterRecoveryStatus{}, fmt.Errorf("fencing: candidate recovery readback is malformed")
	}
	return status, nil
}

func sameNodeIDs(ids []quepaxa.NodeID, members []quepaxa.Member) bool {
	if len(ids) != len(members) {
		return false
	}
	want := make([]quepaxa.NodeID, len(members))
	for i, member := range members {
		want[i] = member.ID
	}
	slices.Sort(ids)
	slices.Sort(want)
	return reflect.DeepEqual(ids, want)
}

func validateEngineRepairLedgerForActivation(record fencing.EngineRepairRecord, ledger fencing.Ledger) error {
	candidate := record.CurrentCandidateIdentity()
	if record.Phase == fencing.RepairAdding {
		run, exists := ledger.Runs[candidate.RunID]
		if exists && fencing.RunRole(run) == fencing.StartupRoleVoterActive {
			if run.Promotion == nil || run.Promotion.OperationID != engineRepairAddOperationID(record.Binding.OperationID) {
				return fmt.Errorf("fencing: active candidate lacks the exact engine repair ADD promotion")
			}
			// Validate the inventory as it existed at learner enrollment. The
			// active role and evidence digest are checked after external readback.
			runs := make(map[string]fencing.Run, len(ledger.Runs))
			for runID, current := range ledger.Runs {
				runs[runID] = current
			}
			ledger.Runs = runs
			run.Role = fencing.StartupRoleLearnerPendingRemove
			run.Promotion = nil
			run.MutationProducerUID = ""
			run.MutationScopeHash = ""
			run.MutationProfile = ""
			run.MutationStageIncarnation = ""
			ledger.Runs[candidate.RunID] = run
		}
	}
	return record.ValidateAgainstLedger(ledger)
}

func (e *FencingExecutor) verifyEngineRepairCandidateActive(ctx context.Context, runtime fencing.RuntimeBinding, record fencing.EngineRepairRecord) error {
	proof, err := e.engineRepairCandidateActivationEvidence(ctx, runtime, record)
	if err != nil {
		return err
	}
	state, err := e.read(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		return err
	}
	canonical, err := NewEngineRepairStore(e.AuthorityState, record.Binding.StatefulSetUID)
	if err != nil {
		return err
	}
	latest, err := canonical.ReadEngineRepair(ctx, record.Binding.SourceClusterID, record.Binding.OperationID)
	if err != nil {
		return fmt.Errorf("fencing: reread canonical candidate activation record: %w", err)
	}
	if latest.IdentityDigest() != record.IdentityDigest() || latest.Phase != fencing.RepairAdding && latest.Phase != fencing.RepairActive {
		return fmt.Errorf("fencing: canonical candidate activation record changed")
	}
	if err := validateEngineRepairLedgerForActivation(latest, state.Ledger); err != nil {
		return err
	}
	candidate := latest.CurrentCandidateIdentity()
	run, ok := state.Ledger.Runs[candidate.RunID]
	if !ok || run.Identity != candidate || run.Runtime == nil || *run.Runtime != runtime ||
		fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Promotion == nil ||
		run.Promotion.OperationID != proof.operationID || run.Promotion.EvidenceHash != proof.digest ||
		run.MutationProducerUID != stableMutationProducerUID(runtime) || run.MutationScopeHash == "" || run.MutationProfile == "" || run.MutationStageIncarnation == "" {
		return fmt.Errorf("fencing: candidate lacks canonical ADD promotion and producer enrollment")
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil {
		return fmt.Errorf("fencing: read candidate producer enrollment: %w", err)
	}
	if producerUID != run.MutationProducerUID || producer.Runtime != runtime || producer.WALIdentity != candidate.WALIdentity ||
		producer.Phase != fencing.MutationOpen || producer.EvidenceFault != "" || producer.Validate() != nil {
		return fmt.Errorf("fencing: candidate producer enrollment is not canonical")
	}
	if err := e.validateMutationArchiveHead(ctx, runtime, producerUID, producer); err != nil {
		return fmt.Errorf("fencing: candidate mutation archive evidence differs: %w", err)
	}
	return nil
}

func engineRepairRuntimeMatches(record fencing.EngineRepairRecord, runtime fencing.RuntimeBinding) bool {
	candidate := record.CurrentCandidateIdentity()
	currentRuntime := record.CurrentCandidateRuntime()
	return currentRuntime != nil && *currentRuntime == runtime &&
		record.CurrentCandidateRuntimeHash() == fencing.RuntimeBindingDigest(runtime) &&
		reflect.DeepEqual(candidate, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID,
			PodName: runtime.PodName, PodUID: runtime.PodUID, RunID: runtime.RunID, AssignmentID: runtime.AssignmentID, WALIdentity: candidate.WALIdentity})
}
