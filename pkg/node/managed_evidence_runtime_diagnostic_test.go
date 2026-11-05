package node

import (
	"context"
	"errors"
	"testing"
)

func TestManagedEvidenceRuntimeDiagnosticNumbersAndIdentity(t *testing.T) {
	err := errors.Join(context.Canceled, errors.New("private payload"))
	before := ManagedRuntimeState{RecorderTip: 13, CertifiedTip: 12, AppliedTip: 12, WALIdentity: "private-wal"}
	after := before
	after.RecorderTip = ^uint64(0)
	for reason, want := range map[string]string{
		"unresolved_history": "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=unresolved_history recorded=13 certified=12 applied=12",
		"snapshot_changed":   "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=snapshot_changed before_recorded=13 before_certified=12 before_applied=12 after_recorded=18446744073709551615 after_certified=12 after_applied=12",
	} {
		if managedEvidenceRuntimeDiagnostic(reason, before, after, err) != want || logManagedEvidenceRuntimeFailure(reason, before, after, err) != err || managedEvidenceRuntimeDiagnostic(reason, before, after, nil) != "" {
			t.Fatal("diagnostic changed numbers, identity, redaction, or success")
		}
	}
	for _, reason := range []string{"", "private", "snapshot_changed payload=private", "snapshot_changed\nprivate"} {
		if managedEvidenceRuntimeDiagnostic(reason, before, after, err) != "" {
			t.Fatal("unsafe reason accepted")
		}
	}
}
