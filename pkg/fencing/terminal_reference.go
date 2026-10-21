package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// TerminalReference retains one independently verified run termination. It is
// not a cooperative DrainReceipt: a dead process cannot manufacture a clean
// engine shutdown. The authority records this only after it verifies evidence
// v2 and persists the full evidence in the named immutable object. Readers must
// revalidate that object's UID, content digest and evidence before composition;
// merely finding this reference is not a recovery authorization.
type TerminalReference struct {
	EvidenceVersion       int         `json:"evidenceVersion"`
	Request               StopRequest `json:"request"`
	RuntimeHash           string      `json:"runtimeHash"`
	ProducerUID           string      `json:"producerUID"`
	ProofName             string      `json:"proofName"`
	ProofUID              string      `json:"proofUID"`
	ProofDigest           string      `json:"proofDigest"`
	RuntimeReceiptDigest  string      `json:"runtimeReceiptDigest"`
	MutationReceiptDigest string      `json:"mutationReceiptDigest"`
	StorageScopeDigest    string      `json:"storageScopeDigest"`
}

// RuntimeBindingDigest binds terminal evidence to the complete immutable run
// enrollment, including node boot, sandbox/container and control-plane identity.
func RuntimeBindingDigest(runtime RuntimeBinding) string {
	data, _ := json.Marshal(runtime)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateTerminalReference(run Run, reference TerminalReference) error {
	if run.Stop == nil || *run.Stop != reference.Request || reference.Request.Identity != run.Identity {
		return fmt.Errorf("fencing: independent terminal reference has another closed run")
	}
	if run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil || reference.RuntimeHash != RuntimeBindingDigest(*run.Runtime) {
		return fmt.Errorf("fencing: independent terminal runtime enrollment differs")
	}
	if run.MutationProducerUID == "" || reference.ProducerUID != run.MutationProducerUID {
		return fmt.Errorf("fencing: independent terminal producer enrollment differs")
	}
	if reference.EvidenceVersion != 2 || reference.ProofName == "" || len(reference.ProofName) > 63 || reference.ProofUID == "" || len(reference.ProofUID) > 128 {
		return fmt.Errorf("fencing: independent terminal evidence identity is incomplete or unsupported")
	}
	for _, value := range []string{reference.Request.RequestHash, reference.RuntimeHash, reference.ProofDigest, reference.RuntimeReceiptDigest, reference.MutationReceiptDigest, reference.StorageScopeDigest} {
		if !mutationDigestValid(value) {
			return fmt.Errorf("fencing: independent terminal evidence digest is incomplete")
		}
	}
	if reference.Request.OperationID == "" {
		return fmt.Errorf("fencing: independent terminal operation is missing")
	}
	return nil
}

// RecordIndependentTerminal is a pure CAS transition for the trusted evidence
// verifier, never an application registration endpoint. The original stop request
// remains immutable when a later generation-wide closure includes this run.
func (l *Ledger) RecordIndependentTerminal(reference TerminalReference) error {
	run, exists := l.Runs[reference.Request.Identity.RunID]
	if !exists || run.Identity.SourceClusterID != l.SourceClusterID {
		return fmt.Errorf("fencing: independent terminal run is not enrolled")
	}
	if err := validateTerminalReference(run, reference); err != nil {
		return err
	}
	if run.IndependentTerminal != nil && *run.IndependentTerminal != reference {
		return fmt.Errorf("fencing: independent terminal evidence is immutable")
	}
	run.IndependentTerminal = &reference
	l.Runs[run.Identity.RunID] = run
	return nil
}

// IndependentTerminal returns the exact retained reference after checking its
// current runtime/producer/closure and storage scope. The caller must then load
// and verify ProofName/ProofUID/ProofDigest before relying on its contents.
func (l *Ledger) IndependentTerminal(runID, storageScopeDigest string) (TerminalReference, error) {
	run, exists := l.Runs[runID]
	if !exists || run.IndependentTerminal == nil || run.Identity.SourceClusterID != l.SourceClusterID {
		return TerminalReference{}, fmt.Errorf("fencing: no retained independent terminal evidence")
	}
	reference := *run.IndependentTerminal
	if err := validateTerminalReference(run, reference); err != nil {
		return TerminalReference{}, err
	}
	if reference.StorageScopeDigest != storageScopeDigest {
		return TerminalReference{}, fmt.Errorf("fencing: retained terminal evidence names another storage scope")
	}
	return reference, nil
}
