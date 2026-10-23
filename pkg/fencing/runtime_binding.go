package fencing

import "fmt"

// RuntimeBinding is captured by a trusted runtime agent before writer admission.
// It belongs to one run, even when several runs share a Pod UID. The eventual
// WAL binding stays in Run.Identity and cannot change this runtime incarnation.
type RuntimeBinding struct {
	AuthorityID         string `json:"authorityID"`
	NamespaceUID        string `json:"namespaceUID"`
	BindingUID          string `json:"bindingUID"`
	StatefulSetUID      string `json:"statefulSetUID"`
	SourceClusterID     string `json:"sourceClusterID"`
	NodeID              string `json:"nodeID"`
	PodName             string `json:"podName,omitempty"`
	PodUID              string `json:"podUID"`
	RunID               string `json:"runID"`
	AssignmentID        string `json:"assignmentID,omitempty"`
	NodeName            string `json:"nodeName"`
	NodeUID             string `json:"nodeUID"`
	MachineID           string `json:"machineID"`
	BootID              string `json:"bootID"`
	RuntimeID           string `json:"runtimeID"`
	SandboxID           string `json:"sandboxID"`
	ContainerID         string `json:"containerID"`
	ImageID             string `json:"imageID"`
	CapturedScopeDigest string `json:"capturedScopeDigest,omitempty"`
}

func (b RuntimeBinding) Validate(id Identity) error {
	if b.SourceClusterID != id.SourceClusterID || b.NodeID != id.NodeID || b.PodUID != id.PodUID || b.RunID != id.RunID || b.AssignmentID != id.AssignmentID || (id.PodName != "" && b.PodName != id.PodName) {
		return fmt.Errorf("fencing: runtime enrollment identity differs")
	}
	for _, value := range []string{
		b.AuthorityID, b.NamespaceUID, b.BindingUID, b.StatefulSetUID,
		b.SourceClusterID, b.NodeID, b.PodUID, b.RunID, b.NodeName, b.NodeUID,
		b.MachineID, b.BootID, b.RuntimeID, b.SandboxID, b.ContainerID, b.ImageID,
	} {
		if value == "" {
			return fmt.Errorf("fencing: runtime enrollment is incomplete")
		}
	}
	if b.CapturedScopeDigest != "" && !mutationDigestValid(b.CapturedScopeDigest) {
		return fmt.Errorf("fencing: runtime cgroup scope digest is invalid")
	}
	return nil
}

// RegisterEnrolled must receive authenticated runtime evidence from the authority,
// never application-supplied metadata. The caller persists this pure transition
// with the same CAS used for closure and WAL binding.
func (l *Ledger) RegisterEnrolled(id Identity, runtime RuntimeBinding) error {
	return l.RegisterEnrolledWithRole(id, runtime, StartupRoleVoterActive)
}

// RegisterEnrolledWithRole persists the authority-resolved startup role with
// the runtime enrollment in the same ledger CAS.
func (l *Ledger) RegisterEnrolledWithRole(id Identity, runtime RuntimeBinding, role StartupRole) error {
	if err := runtime.Validate(id); err != nil {
		return err
	}
	if old, exists := l.Runs[id.RunID]; exists {
		if old.Runtime != nil && *old.Runtime != runtime {
			return fmt.Errorf("fencing: runtime enrollment is immutable")
		}
		if runRole(old) != role {
			return fmt.Errorf("fencing: startup role is immutable")
		}
	}
	if err := l.RegisterWithRole(id, role); err != nil {
		return err
	}
	run := l.Runs[id.RunID]
	run.Runtime = &runtime
	l.Runs[id.RunID] = run
	return nil
}

// BindMutationProducer pins the Kubernetes object's immutable UID before writer
// admission. A trusted authority calls this only after creating/verifying the
// deterministic Reserved producer. Finishing that initialization after closure
// is allowed for an unbound run: it cannot reopen WAL or mutation admission.
func (l *Ledger) BindMutationProducer(id Identity, runtime RuntimeBinding, producerUID string) error {
	run, exists := l.Runs[id.RunID]
	if !exists || runRole(run) != StartupRoleVoterActive || producerUID == "" || run.Runtime == nil || *run.Runtime != runtime {
		return fmt.Errorf("fencing: mutation producer has no matching runtime enrollment")
	}
	if err := runtime.Validate(id); err != nil {
		return err
	}
	registered := id
	registered.WALIdentity = run.Identity.WALIdentity
	if registered != run.Identity || (id.WALIdentity != "" && id.WALIdentity != run.Identity.WALIdentity) {
		return fmt.Errorf("fencing: mutation producer identity differs")
	}
	if run.MutationProducerUID != "" {
		if run.MutationProducerUID == producerUID {
			return nil
		}
		return fmt.Errorf("fencing: mutation producer UID is immutable")
	}
	if run.Identity.WALIdentity != "" && run.Promotion == nil {
		return fmt.Errorf("fencing: cannot enroll a mutation producer after WAL admission")
	}
	run.MutationProducerUID = producerUID
	l.Runs[id.RunID] = run
	return nil
}
