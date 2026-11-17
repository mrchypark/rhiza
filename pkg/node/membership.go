package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"reflect"
	"slices"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type membershipOperation struct {
	Request             network.MembershipChange `json:"request"`
	Target              quepaxa.Cluster          `json:"target"`
	ManagedExcludedRead bool                     `json:"managedExcludedRead,omitempty"`
	Reservation         *membershipReservation   `json:"reservation,omitempty"`
}

type membershipReservation struct {
	OperationID       string                  `json:"operationID"`
	Action            ManagedMembershipAction `json:"action"`
	ExpectedAbortSlot quepaxa.Slot            `json:"expectedAbortSlot"`
	Source            quepaxa.Cluster         `json:"source"`
	IntentHash        string                  `json:"intentHash"`
}

func managedMembershipIntentHash(r ManagedMembershipRequest) string {
	member := quepaxa.Member{}
	if r.Addition != nil {
		member = *r.Addition
	}
	payload := fmt.Sprintf("%s\x00%s\x00%t\x00%d\x00%d\x00%s\x00%s\x00%s\x00%x\x00%s", r.RepairOperationID, r.Action, r.AddOnly, r.ExpectedConfigID, r.ExpectedAbortSlot, r.OldNodeID, r.OldWALIdentity, member.ID, member.PublicKey, member.PeerURL)
	sum := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("%x", sum)
}

func hydrateMembershipReservation(operation *membershipOperation, request network.MembershipChange, intentHash string) error {
	if operation == nil || operation.Reservation == nil {
		return nil
	}
	r := operation.Reservation
	if r.OperationID != request.OperationID || r.Action != ManagedMembershipAdd || r.Source.ConfigID != request.ExpectedConfigID || r.ExpectedAbortSlot != request.ExpectedAbortSlot || request.Add == nil || r.IntentHash == "" || r.IntentHash != intentHash {
		return network.ErrRequestConflict
	}
	target := quepaxa.Cluster{ConfigID: r.Source.ConfigID + 1}
	for _, member := range r.Source.Members {
		if request.Add != nil && member.ID == request.Add.ID {
			return network.ErrInvalidRequest
		}
		target.Members = append(target.Members, member)
	}
	target.Members = append(target.Members, *request.Add)
	operation.Request = request
	operation.Target = target
	return nil
}

type managedMembershipStage string

const (
	managedMembershipStageContext                      managedMembershipStage = "context"
	managedMembershipStageLiveGuard                    managedMembershipStage = "live_guard"
	managedMembershipStageRegistration                 managedMembershipStage = "registration"
	managedMembershipStageRegistrationRead             managedMembershipStage = "registration_read"
	managedMembershipStageRegistrationRecordValidation managedMembershipStage = "registration_record_validation"
	managedMembershipStageRegistrationProfileCompute   managedMembershipStage = "registration_profile_compute"
	managedMembershipStageRegistrationProfileMatch     managedMembershipStage = "registration_profile_match"
	managedMembershipStageTargetValidation             managedMembershipStage = "target_validation"
	managedMembershipStageStorageCAS                   managedMembershipStage = "storage_cas"
	managedMembershipStageReconfiguration              managedMembershipStage = "reconfiguration"
	managedMembershipStageCompletion                   managedMembershipStage = "completion"
	managedMembershipStagePersist                      managedMembershipStage = "persist"
)

func logManagedMembershipStageFailure(ctx context.Context, request network.MembershipChange, stage managedMembershipStage, err error) {
	if err == nil || request.Add == nil {
		return
	}
	permit, ok := ctx.Value(managedMembershipPermitKey{}).(managedMembershipPermit)
	if !ok || permit.request.Action != ManagedMembershipAdd || !reflect.DeepEqual(permit.change, request) {
		return
	}
	log.Printf("MANAGED_MEMBERSHIP_STAGE_FAILURE action=add stage=%s cause=%s", stage, managedMembershipCauseClass(err))
}

func (n *Node) changeMembership(ctx context.Context, request network.MembershipChange) (err error) {
	stage := managedMembershipStageContext
	defer func() { logManagedMembershipStageFailure(ctx, request, stage, err) }()

	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	stage = managedMembershipStageLiveGuard
	if err := n.validateManagedMembership(ctx, request, false); err != nil {
		return err
	}
	if !n.config.EnableReconfiguration || n.bucket == nil || !n.core.IsVoter() {
		return network.ErrNotReady
	}
	if request.OperationID == "" || len(request.OperationID) > 256 || request.ClusterID != string(n.config.ClusterID) || request.ExpectedConfigID == 0 || request.ExpectedConfigID == ^uint(0) || (request.Remove == "") == (request.Add == nil) {
		return network.ErrInvalidRequest
	}
	key := n.membershipOperationKey(request)
	stage = managedMembershipStageStorageCAS
	stored, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return err
	}
	operation := membershipOperation{Request: request}
	current := n.core.CurrentCluster()
	var reservation *membershipReservation
	if stored != nil {
		if err := json.Unmarshal(stored, &operation); err != nil {
			return err
		}
		if operation.Reservation != nil {
			reservation = operation.Reservation
			intentHash := ""
			if permit, ok := ctx.Value(managedMembershipPermitKey{}).(managedMembershipPermit); ok {
				intentHash = managedMembershipIntentHash(permit.request)
			}
			if err := hydrateMembershipReservation(&operation, request, intentHash); err != nil || !reflect.DeepEqual(reservation.Source, current) {
				return network.ErrRequestConflict
			}
			current = reservation.Source
		} else if !reflect.DeepEqual(operation.Request, request) {
			return network.ErrRequestConflict
		}
		if n.managedStartup != nil && request.Remove != "" && !operation.ManagedExcludedRead {
			return network.ErrRequestConflict
		}
	}
	if stored == nil || reservation != nil {
		if current.ConfigID != request.ExpectedConfigID {
			return network.ErrRequestConflict
		}
		if revision, _, _ := n.core.LastReconfigurationAbort(); revision != request.ExpectedAbortSlot {
			return network.ErrRequestConflict
		}
		operation.Target = quepaxa.Cluster{ConfigID: current.ConfigID + 1}
		found := false
		for _, member := range current.Members {
			if member.ID == request.Remove {
				found = true
				continue
			}
			if request.Add != nil && member.ID == request.Add.ID {
				return network.ErrInvalidRequest
			}
			operation.Target.Members = append(operation.Target.Members, member)
		}
		if request.Add != nil {
			if request.Fence != nil || request.Add.ID == "" || request.Add.PublicKey == (quepaxa.PublicKey{}) || request.Add.WALIdentity == "" || request.Add.PeerURL == "" {
				return network.ErrInvalidRequest
			}
			operation.Target.Members = append(operation.Target.Members, *request.Add)
			stage = managedMembershipStageRegistrationRead
			registrationKey := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(request.Add.ID))))
			data, err := readVoterRegistration(ctx, n.bucket, registrationKey)
			if err != nil {
				return err
			}
			stage = managedMembershipStageRegistrationRecordValidation
			var registration voterIdentity
			if len(data) == 0 || json.Unmarshal(data, &registration) != nil || registration.Version != voterIdentityVersion || registration.Node != string(request.Add.ID) || registration.Cluster != request.ClusterID || registration.Nonce != request.Add.WALIdentity || registration.LearnerPublicKey != fmt.Sprintf("%x", request.Add.PublicKey[:]) {
				return network.ErrInvalidRequest
			}
			candidateConfig := *n.config
			candidateConfig.NodeID = request.Add.ID
			candidateConfig.Learner = request.Add
			stage = managedMembershipStageRegistrationProfileCompute
			profile, profileErr := n.managedVoterProfileFor(&candidateConfig)
			if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
				profile, profileErr = n.managedLearnerIntentProfileFor(&candidateConfig, managedLearnerProfileForRepairAdd)
			}
			if profileErr != nil {
				return profileErr
			}
			stage = managedMembershipStageRegistrationProfileMatch
			if profile != nil && (registration.ApplicationProtocol != profile.ApplicationProtocol || registration.BootstrapPlanDigest != profile.BootstrapPlanDigest || registration.MemberConfigDigest != profile.MemberConfigDigest || registration.Generation == nil || *registration.Generation != profile.Generation) {
				return fmt.Errorf("candidate voter append-only profile differs from the trusted group")
			}
		} else {
			if !found || len(operation.Target.Members) == 0 || request.Remove == n.config.NodeID {
				return network.ErrInvalidRequest
			}
			if err := n.validateRemovalAuthority(ctx, request); err != nil {
				return err
			}
		}
		stage = managedMembershipStageTargetValidation
		if err := n.core.ValidateReconfigurationTarget(operation.Target); err != nil {
			return fmt.Errorf("%w: %v", network.ErrInvalidRequest, err)
		}
		if pending, ok := n.core.PendingReconfiguration(); ok && !reflect.DeepEqual(pending, operation.Target) {
			return network.ErrRequestConflict
		}
		if n.managedStartup != nil && request.Remove != "" {
			if _, _, err := n.core.ReadIndexExcluding(ctx, request.Remove); err != nil {
				return fmt.Errorf("managed membership excluded-old read-index config=%d local_tip=%d: %w", request.ExpectedConfigID, n.core.Tip(), err)
			}
			if err := n.validateManagedMembership(ctx, request, false); err != nil {
				return err
			}
			// Retain the initial exclusion proof in this exact CAS journal: a
			// frozen configuration cannot obtain another ordinary ReadIndex.
			operation.ManagedExcludedRead = true
		}
		stage = managedMembershipStageStorageCAS
		encoded, err := json.Marshal(operation)
		if err != nil {
			return err
		}
		if len(encoded) >= 64<<10 {
			return network.ErrInvalidRequest
		}
		if reservation == nil {
			if err := matchVoterRegistration(ctx, n.bucket, key, encoded, nil); err != nil {
				if errors.Is(err, ErrVoterStateLost) {
					return network.ErrRequestConflict
				}
				return err
			}
		}
	}
	stage = managedMembershipStageLiveGuard
	if err := n.validateManagedMembership(ctx, request, false); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, operation.Target) {
		if current.ConfigID != request.ExpectedConfigID {
			return network.ErrRequestConflict
		}
		if revision, _, _ := n.core.LastReconfigurationAbort(); revision != request.ExpectedAbortSlot {
			return network.ErrRequestConflict
		}
		if pending, ok := n.core.PendingReconfiguration(); ok {
			if !reflect.DeepEqual(pending, operation.Target) {
				return network.ErrRequestConflict
			}
		} else {
			stage = managedMembershipStageReconfiguration
			if _, err := n.core.BeginReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
				return fmt.Errorf("membership begin config=%d local_tip=%d: %w", request.ExpectedConfigID, n.core.Tip(), err)
			}
		}
		stage = managedMembershipStageLiveGuard
		if err := n.validateManagedMembership(ctx, request, false); err != nil {
			return err
		}
		stage = managedMembershipStageReconfiguration
		if err := n.core.FinishReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
			return fmt.Errorf("membership finish config=%d local_tip=%d: %w", request.ExpectedConfigID, n.core.Tip(), err)
		}
		if !reflect.DeepEqual(n.core.CurrentCluster(), operation.Target) {
			return network.ErrRequestConflict
		}
	} else {
		stage = managedMembershipStageCompletion
		// A locally durable terminal does not establish delivery/ACK completion.
		terminal, err := n.retainedMembershipTerminal(operation)
		if err != nil {
			return err
		}
		completed, err := n.core.CompleteDecision(ctx, terminal.Slot)
		if err != nil {
			return fmt.Errorf("membership terminal completion: %w", err)
		}
		if completed.Hash != terminal.Hash || !bytes.Equal(completed.Value, terminal.Value) || !bytes.Equal(completed.Certificate, terminal.Certificate) {
			return fmt.Errorf("membership terminal changed")
		}
	}
	stage = managedMembershipStagePersist
	if err := n.persistMembership(ctx); err != nil {
		return err
	}
	if permit, ok := ctx.Value(managedMembershipPermitKey{}).(managedMembershipPermit); ok && permit.capture != nil {
		stage = managedMembershipStageCompletion
		result, err := n.captureManagedRepairSource(ctx, permit.request)
		if err != nil {
			return err
		}
		*permit.capture = result
	}
	return nil
}

// Selects the retained terminal for a journal's exact target and abort round.
// OperationID provenance is checked against the immutable journal by callers.
func (n *Node) retainedMembershipTerminal(operation membershipOperation) (quepaxa.DecidedValue, error) {
	terminal, found, err := n.findRetainedMembershipTerminal(operation)
	if err != nil {
		return quepaxa.DecidedValue{}, err
	}
	if !found {
		return quepaxa.DecidedValue{}, fmt.Errorf("membership terminal unavailable")
	}
	return terminal, nil
}

func (n *Node) findRetainedMembershipTerminal(operation membershipOperation) (quepaxa.DecidedValue, bool, error) {
	history, err := n.core.MembershipHistory()
	if err != nil {
		return quepaxa.DecidedValue{}, false, fmt.Errorf("membership completion history: %w", err)
	}
	var terminal *quepaxa.DecidedValue
	var previous quepaxa.Slot
	for _, transition := range history.Transitions {
		actualTarget := n.core.ClusterForSlot(transition.Terminal.Slot + 1)
		targetMatches := reflect.DeepEqual(actualTarget, operation.Target)
		if operation.Reservation != nil {
			targetMatches = reservationTerminalTargetMatches(operation.Reservation.Source, actualTarget, operation.Request.Add)
		}
		if n.core.ClusterForSlot(transition.Freeze.Slot).ConfigID != operation.Request.ExpectedConfigID || !targetMatches {
			previous = transition.Terminal.Slot
			continue
		}
		if terminal != nil {
			return quepaxa.DecidedValue{}, false, fmt.Errorf("ambiguous membership terminal")
		}
		if previous < n.core.CompactionFloor() {
			return quepaxa.DecidedValue{}, false, fmt.Errorf("membership revision history compacted")
		}
		var revision quepaxa.Slot
		for slot := previous + 1; slot < transition.Freeze.Slot; slot++ {
			value, ok := n.core.CertifiedValue(slot)
			if !ok {
				return quepaxa.DecidedValue{}, false, fmt.Errorf("membership revision history unavailable")
			}
			control, err := quepaxa.DecodeReconfiguration(value.Value)
			if err != nil {
				return quepaxa.DecidedValue{}, false, fmt.Errorf("invalid membership revision history: %w", err)
			}
			if control {
				revision = slot
			}
		}
		if revision != operation.Request.ExpectedAbortSlot {
			return quepaxa.DecidedValue{}, false, network.ErrRequestConflict
		}
		decision := transition.Terminal
		terminal = &decision
	}
	if terminal == nil {
		return quepaxa.DecidedValue{}, false, nil
	}
	return *terminal, true, nil
}

func reservationTerminalTargetMatches(source, actual quepaxa.Cluster, addition *quepaxa.Member) bool {
	// Consensus certificates do not contain the journal OperationID. This match
	// is deliberately target-based: any matching target blocks no-effect abort.
	// That false-positive direction is safe; history ambiguity/compaction errors
	// also block. It is not proof that this operation ID produced the terminal.
	if addition == nil || actual.ConfigID != source.ConfigID+1 || len(actual.Members) != len(source.Members)+1 {
		return false
	}
	actualByID := make(map[quepaxa.NodeID]quepaxa.Member, len(actual.Members))
	for _, member := range actual.Members {
		actualByID[member.ID] = member
	}
	for _, member := range source.Members {
		got, ok := actualByID[member.ID]
		if !ok || !reflect.DeepEqual(got, member) {
			return false
		}
	}
	got, ok := actualByID[addition.ID]
	return ok && got.PublicKey == addition.PublicKey && got.PeerURL == addition.PeerURL && got.WALIdentity != ""
}

func (n *Node) membershipOperationKey(request network.MembershipChange) string {
	name := fmt.Sprintf("%d.json", request.ExpectedConfigID)
	if request.ExpectedAbortSlot != 0 {
		name = fmt.Sprintf("%d-abort-%d.json", request.ExpectedConfigID, request.ExpectedAbortSlot)
	}
	return path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "membership", name)
}

// ReserveManagedRepairAdd durably claims the single membership journal slot
// for this config/abort revision without beginning consensus reconfiguration.
// Learner registration and handoff are validated by changeMembership later.
func (n *Node) ReserveManagedRepairAdd(ctx context.Context, r ManagedMembershipRequest) error {
	if n == nil || r.Action != ManagedMembershipAdd || r.Addition == nil || r.Addition.ID == "" || r.Addition.PublicKey == (quepaxa.PublicKey{}) || r.Addition.PeerURL == "" || r.HandoffHeadHash != "" {
		return network.ErrInvalidRequest
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.config == nil || n.core == nil || n.bucket == nil || !n.opened.Load() || !n.config.EnableReconfiguration || !n.core.IsVoter() {
		return network.ErrNotReady
	}
	current := n.core.CurrentCluster()
	current.Members = append([]quepaxa.Member(nil), current.Members...)
	if _, pending := n.core.PendingReconfiguration(); pending || current.ConfigID != r.ExpectedConfigID {
		return fmt.Errorf("membership reservation source changed: %w", network.ErrRequestConflict)
	}
	if revision, _, _ := n.core.LastReconfigurationAbort(); revision != r.ExpectedAbortSlot {
		return fmt.Errorf("membership reservation abort revision changed: %w", network.ErrRequestConflict)
	}
	change := r.change(string(n.config.ClusterID))
	key := n.membershipOperationKey(change)
	stored, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return err
	}
	if stored != nil {
		var operation membershipOperation
		if json.Unmarshal(stored, &operation) != nil {
			return network.ErrRequestConflict
		}
		if operation.Reservation != nil {
			if operation.Reservation.OperationID == change.OperationID && operation.Reservation.Action == ManagedMembershipAdd && reflect.DeepEqual(operation.Reservation.Source, current) && operation.Reservation.ExpectedAbortSlot == r.ExpectedAbortSlot && operation.Reservation.IntentHash == managedMembershipIntentHash(r) {
				return nil
			}
		}
		return network.ErrRequestConflict
	}
	reservation := membershipOperation{Reservation: &membershipReservation{OperationID: change.OperationID, Action: ManagedMembershipAdd, ExpectedAbortSlot: r.ExpectedAbortSlot, Source: current, IntentHash: managedMembershipIntentHash(r)}}
	encoded, err := json.Marshal(reservation)
	if err != nil || len(encoded) >= 64<<10 {
		return fmt.Errorf("membership reservation exceeds bounded budget")
	}
	if err := matchVoterRegistration(ctx, n.bucket, key, encoded, nil); err != nil {
		if errors.Is(err, ErrVoterStateLost) {
			return network.ErrRequestConflict
		}
		return err
	}
	return nil
}

func (n *Node) abortMembership(ctx context.Context, request network.MembershipChange) error {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.validateManagedMembership(ctx, request, true); err != nil {
		return err
	}
	if !n.config.EnableReconfiguration || n.bucket == nil || !n.core.IsVoter() {
		return network.ErrNotReady
	}
	if request.OperationID == "" || request.Remove != "" || request.ClusterID != string(n.config.ClusterID) || request.ExpectedConfigID != n.core.ConfigID() {
		return network.ErrInvalidRequest
	}
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(request))
	if err != nil {
		return err
	}
	var operation membershipOperation
	if data == nil || json.Unmarshal(data, &operation) != nil || operation.Request.Add == nil || operation.Request.OperationID != request.OperationID || operation.Request.ClusterID != request.ClusterID || operation.Request.ExpectedConfigID != request.ExpectedConfigID || operation.Request.ExpectedAbortSlot != request.ExpectedAbortSlot {
		return network.ErrRequestConflict
	}
	if request.Add != nil && !reflect.DeepEqual(operation.Request, request) {
		return network.ErrRequestConflict
	}
	revision, target, aborted := n.core.LastReconfigurationAbort()
	if aborted && revision > request.ExpectedAbortSlot && reflect.DeepEqual(target, operation.Target) {
		return n.persistMembership(ctx)
	}
	if revision != request.ExpectedAbortSlot {
		return network.ErrRequestConflict
	}
	pending, ok := n.core.PendingReconfiguration()
	if !ok || !reflect.DeepEqual(pending, operation.Target) {
		return network.ErrRequestConflict
	}
	if err := n.validateManagedMembership(ctx, request, true); err != nil {
		return err
	}
	if err := n.core.AbortReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
		return err
	}
	revision, target, aborted = n.core.LastReconfigurationAbort()
	if !aborted || revision <= request.ExpectedAbortSlot || !reflect.DeepEqual(target, operation.Target) {
		return network.ErrRequestConflict
	}
	return n.persistMembership(ctx)
}

func (n *Node) persistMembership(ctx context.Context) error {
	through := n.core.Tip()
	if err := n.core.EnsureDurableThrough(ctx, through); err != nil {
		return err
	}
	if n.archive != nil {
		return n.archive.SyncThrough(ctx, n.core, through)
	}
	return nil
}

func (n *Node) validateMembershipFence(ctx context.Context, request network.MembershipChange) error {
	fence := request.Fence
	if fence == nil || !fence.Confirmed || fence.NodeID != request.Remove || fence.WorkloadUID == "" || fence.WALIdentity == "" || fence.Evidence == "" {
		return network.ErrInvalidRequest
	}
	return n.validateRegisteredVoterWAL(ctx, request.Remove, fence.WALIdentity)
}

func (n *Node) validateRegisteredVoterWAL(ctx context.Context, id quepaxa.NodeID, identity string) error {
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(id))))
	data, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return err
	}
	var registration voterIdentity
	if len(data) == 0 || json.Unmarshal(data, &registration) != nil || registration.Version != voterIdentityVersion || registration.Node != string(id) || registration.Cluster != string(n.config.ClusterID) || registration.Nonce != identity {
		return network.ErrInvalidRequest
	}
	profile, err := n.managedVoterProfileFor(n.config)
	if err != nil {
		return err
	}
	if profile != nil && (registration.ApplicationProtocol != profile.ApplicationProtocol || registration.BootstrapPlanDigest != profile.BootstrapPlanDigest || registration.Generation == nil || *registration.Generation != profile.Generation || registration.MemberConfigDigest == "") {
		return network.ErrInvalidRequest
	}
	return nil
}

// Admission must use the configuration that owns the drain, even if another
// goroutine has already learned its terminal while this recorder was verifying.
func (n *Node) verifyMembershipAdmission(ctx context.Context, target quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
	previous := n.core.ClusterForSlot(through)
	for _, candidate := range target.Members {
		if !slices.ContainsFunc(previous.Members, func(member quepaxa.Member) bool { return member.ID == candidate.ID }) {
			if err := n.transport.VerifyLearner(ctx, candidate, through, prefix); err != nil {
				return err
			}
		}
	}
	return nil
}
