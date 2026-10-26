package fencing

import "fmt"

// ContinueUnboundRegistration transfers only Bind authority, not proof of process exit.
// The caller persists revocation and the new WAL expectation in one root CAS.
func (l *Ledger) ContinueUnboundRegistration(id Identity, runtime RuntimeBinding, wal string) error {
	if !mutationDigestValid(wal) || id.WALIdentity != "" || id.PodName != id.NodeID || id.AssignmentID != "" || runtime.Validate(id) != nil {
		return fmt.Errorf("fencing: invalid registration continuation")
	}
	if current, ok := l.Runs[id.RunID]; ok {
		if current.RegistrationExpectedWAL != wal || current.RegistrationSupersededBy != "" {
			return ErrClosed
		}
		return l.RegisterEnrolledWithRole(id, runtime, StartupRoleVoterActive)
	}
	priorID := ""
	for runID, prior := range l.Runs {
		if prior.Identity.NodeID != id.NodeID {
			continue
		}
		if prior.Identity.PodUID != id.PodUID || prior.Identity.PodName != id.PodName ||
			prior.Identity.WALIdentity != "" || prior.Stop != nil || prior.Exited || prior.Runtime == nil ||
			prior.Runtime.Validate(prior.Identity) != nil || prior.Runtime.StatefulSetUID != runtime.StatefulSetUID ||
			prior.EngineRepairProvenance != nil || runRole(prior) != StartupRoleVoterActive ||
			(prior.RegistrationExpectedWAL != "" && prior.RegistrationExpectedWAL != wal) {
			return ErrClosed
		}
		if prior.RegistrationSupersededBy != "" {
			continue
		}
		if priorID != "" {
			return fmt.Errorf("fencing: ambiguous unbound registration")
		}
		priorID = runID
	}
	if priorID == "" {
		return ErrClosed
	}
	if err := l.RegisterEnrolledWithRole(id, runtime, StartupRoleVoterActive); err != nil {
		return err
	}
	prior := l.Runs[priorID]
	prior.RegistrationSupersededBy = id.RunID
	l.Runs[priorID] = prior
	current := l.Runs[id.RunID]
	current.RegistrationExpectedWAL = wal
	l.Runs[id.RunID] = current
	return nil
}
