package fencing

import "fmt"

const MutationExecutionProfile = "gcs-json-v1"

// ProducerBinding names the durable journal authority, not a mutation ACK or
// proof that staging, provider execution or runtime termination has completed.
type ProducerBinding struct {
	ContractVersion  int            `json:"contractVersion"`
	Runtime          RuntimeBinding `json:"runtime"`
	ProducerUID      string         `json:"producerUID"`
	StoreScopeHash   string         `json:"storeScopeHash"`
	ExecutionProfile string         `json:"executionProfile"`
}

// BindReceipt keeps cooperative identity binding separate from mutation support.
// A nil Producer must never enable the source mutation client.
type BindReceipt struct {
	Identity Identity         `json:"identity"`
	Producer *ProducerBinding `json:"producer,omitempty"`
}

func (r BindReceipt) Validate(expected Identity) error {
	if r.Identity != expected || expected.SourceClusterID == "" || expected.NodeID == "" || expected.PodUID == "" || expected.RunID == "" || expected.WALIdentity == "" {
		return fmt.Errorf("fencing: bind receipt identity differs")
	}
	if p := r.Producer; p != nil {
		if p.ContractVersion != MutationIntentVersion || p.ProducerUID == "" || len(p.ProducerUID) > 128 || !mutationDigestValid(p.StoreScopeHash) || p.ExecutionProfile != MutationExecutionProfile {
			return fmt.Errorf("fencing: incomplete mutation producer binding")
		}
		if err := p.Runtime.Validate(expected); err != nil {
			return err
		}
	}
	return nil
}

func (r BindReceipt) SameBinding(other BindReceipt) bool {
	return r.Identity == other.Identity && ((r.Producer == nil && other.Producer == nil) ||
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
	if !ok || !l.RequireMutationJournal || run.Runtime == nil || run.MutationProducerUID == "" || before != run.Identity || id.WALIdentity == "" || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != id.WALIdentity) || !mutationDigestValid(scope) || profile != MutationExecutionProfile {
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
