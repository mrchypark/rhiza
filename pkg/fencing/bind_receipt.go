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
	if r.Role == "" {
		r.Role = StartupRoleVoterActive // historical receipts predate role binding
	}
	if !r.Role.Valid() {
		return fmt.Errorf("fencing: bind receipt role is invalid")
	}
	if r.Role == StartupRoleLearnerPendingRemove && r.Producer != nil {
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
	if run.Identity.WALIdentity != "" {
		return fmt.Errorf("fencing: mutation stage pin must precede WAL activation")
	}
	run.MutationStageIncarnation = incarnation
	l.Runs[id.RunID] = run
	return nil
}

func (r BindReceipt) SameBinding(other BindReceipt) bool {
	if r.Role == "" {
		r.Role = StartupRoleVoterActive
	}
	if other.Role == "" {
		other.Role = StartupRoleVoterActive
	}
	return r.Identity == other.Identity && r.Role == other.Role && ((r.Producer == nil && other.Producer == nil) ||
		(r.Producer != nil && other.Producer != nil && *r.Producer == *other.Producer))
}

func (r BindReceipt) clone() BindReceipt {
	if r.Producer != nil {
		copy := *r.Producer
		r.Producer = &copy
	}
	return r
}

// BindMutationScope must be persisted in the root before returning a producer
// binding. It cannot retrofit a new scope onto an already-bound old WAL.
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
	if run.Identity.WALIdentity != "" || run.MutationProfile != "" {
		return fmt.Errorf("fencing: mutation scope must precede WAL activation")
	}
	run.MutationScopeHash, run.MutationProfile = scope, profile
	l.Runs[id.RunID] = run
	return nil
}
