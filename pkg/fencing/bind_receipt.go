package fencing

import "fmt"

const (
	MutationExecutionProfile     = "gcs-json-v1"
	MutationHeldExecutionProfile = "gcs-json-held-v2"
)

// MutationProfileSupported is used by readers that must preserve old bindings
// while admitting the explicitly versioned held-create protocol.
func MutationProfileSupported(profile string) bool {
	return profile == MutationExecutionProfile || profile == MutationHeldExecutionProfile
}

// ProducerBinding names the durable journal authority, not a mutation ACK or
// proof that staging, provider execution or runtime termination has completed.
type ProducerBinding struct {
	ContractVersion  int            `json:"contractVersion"`
	Runtime          RuntimeBinding `json:"runtime"`
	ProducerUID      string         `json:"producerUID"`
	StoreScopeHash   string         `json:"storeScopeHash"`
	ExecutionProfile string         `json:"executionProfile"`
	StageIncarnation string         `json:"stageIncarnation,omitempty"`
}

// BindReceipt keeps cooperative identity binding separate from mutation support.
// A nil Producer must never enable the source mutation client.
type BindReceipt struct {
	Identity Identity         `json:"identity"`
	Role     StartupRole      `json:"role,omitempty"`
	Producer *ProducerBinding `json:"producer,omitempty"`
}

func (r BindReceipt) Validate(expected Identity) error {
	if r.Identity != expected || expected.SourceClusterID == "" || expected.NodeID == "" || expected.PodUID == "" || expected.RunID == "" || expected.WALIdentity == "" {
		return fmt.Errorf("fencing: bind receipt identity differs")
	}
	role, ok := r.normalizedRole()
	if !ok {
		return fmt.Errorf("fencing: bind receipt role is invalid")
	}
	if role == StartupRoleLearnerPendingRemove && r.Producer != nil {
		return fmt.Errorf("fencing: pending learner cannot receive a mutation producer")
	}
	if p := r.Producer; p != nil {
		if p.ContractVersion != MutationIntentVersion || p.ProducerUID == "" || len(p.ProducerUID) > 128 || !mutationDigestValid(p.StoreScopeHash) || !MutationProfileSupported(p.ExecutionProfile) || (p.StageIncarnation != "" && !mutationDigestValid(p.StageIncarnation)) {
			return fmt.Errorf("fencing: incomplete mutation producer binding")
		}
		if err := p.Runtime.Validate(expected); err != nil {
			return err
		}
	}
	return nil
}

// BindMutationStage pins an explicitly provisioned source spool before WAL
// activation. A canonically promoted learner is the sole exception: its WAL
// was bound while it had no producer, and stage pinning still precedes producer
// activation. The value is immutable for the producer's lifetime; an absent
// stage store is not a bootstrap signal and cannot clear a prior pin.
func (l *Ledger) BindMutationStage(id Identity, incarnation string) error {
	run, ok := l.Runs[id.RunID]
	before := id
	if ok {
		before.WALIdentity = run.Identity.WALIdentity
	}
	if !ok || runRole(run) != StartupRoleVoterActive || !l.RequireMutationJournal || run.Runtime == nil || run.MutationProducerUID == "" || !mutationDigestValid(incarnation) || before != run.Identity || id.WALIdentity == "" || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != id.WALIdentity) || run.MutationScopeHash == "" || !MutationProfileSupported(run.MutationProfile) {
		return fmt.Errorf("fencing: invalid mutation stage enrollment")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[id.NodeID]; closed {
		return ErrClosed
	}
	if err := run.Runtime.Validate(id); err != nil {
		return err
	}
	if run.MutationStageIncarnation != "" {
		if run.MutationStageIncarnation != incarnation {
			return fmt.Errorf("fencing: mutation stage incarnation changed")
		}
		return nil
	}
	if run.Identity.WALIdentity != "" && run.Promotion == nil {
		return fmt.Errorf("fencing: mutation stage pin must precede WAL activation")
	}
	if run.Promotion != nil && run.Identity.WALIdentity != id.WALIdentity {
		return fmt.Errorf("fencing: promoted mutation stage identity differs")
	}
	run.MutationStageIncarnation = incarnation
	l.Runs[id.RunID] = run
	return nil
}

func (r BindReceipt) SameBinding(other BindReceipt) bool {
	rRole, rOK := r.normalizedRole()
	otherRole, otherOK := other.normalizedRole()
	return rOK && otherOK && r.Identity == other.Identity && rRole == otherRole && ((r.Producer == nil && other.Producer == nil) ||
		(r.Producer != nil && other.Producer != nil && *r.Producer == *other.Producer))
}

func (r BindReceipt) normalizedRole() (StartupRole, bool) {
	if r.Role == "" {
		// Empty role is the historical static-voter wire format only. A
		// supervisor-assigned logical identity must carry its authority role.
		if r.Identity.AssignmentID != "" || (r.Identity.PodName != "" && r.Identity.PodName != r.Identity.NodeID) {
			return "", false
		}
		return StartupRoleVoterActive, true
	}
	return r.Role, r.Role.Valid()
}

func (r BindReceipt) clone() BindReceipt {
	if r.Producer != nil {
		copy := *r.Producer
		r.Producer = &copy
	}
	return r
}

// BindMutationScope must be persisted in the root before returning a producer
// binding. Ordinary voters bind before WAL activation. A canonically promoted
// learner may bind its first scope after WAL binding because it had no producer
// while pending; its producer is not activated until this scope is persisted.
func (l *Ledger) BindMutationScope(id Identity, scope, profile string) error {
	run, ok := l.Runs[id.RunID]
	before := id
	before.WALIdentity = run.Identity.WALIdentity
	if !ok || runRole(run) != StartupRoleVoterActive || !l.RequireMutationJournal || run.Runtime == nil || run.MutationProducerUID == "" || before != run.Identity || id.WALIdentity == "" || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != id.WALIdentity) || !mutationDigestValid(scope) || !MutationProfileSupported(profile) {
		return fmt.Errorf("fencing: invalid mutation scope enrollment")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[id.NodeID]; closed {
		return ErrClosed
	}
	if err := run.Runtime.Validate(id); err != nil {
		return err
	}
	if run.MutationScopeHash != "" {
		if run.MutationScopeHash != scope || run.MutationProfile != profile {
			return fmt.Errorf("fencing: mutation scope/profile changed")
		}
		return nil
	}
	if run.MutationProfile != "" {
		return fmt.Errorf("fencing: mutation scope must precede WAL activation")
	}
	if run.Identity.WALIdentity != "" && (run.Promotion == nil || run.Identity.WALIdentity != id.WALIdentity) {
		return fmt.Errorf("fencing: mutation scope must precede WAL activation")
	}
	run.MutationScopeHash, run.MutationProfile = scope, profile
	l.Runs[id.RunID] = run
	return nil
}
