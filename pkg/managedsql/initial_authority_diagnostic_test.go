package managedsql

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

func TestManagedInitialAuthorityDiagnosticScopeAndIdentity(t *testing.T) {
	err := errors.Join(context.Canceled, errors.New("private payload"))
	h := &Host{}
	h.cfg.Plan.ApplicationProtocol = sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	for _, leaf := range []string{"identity", "live", "pending_closed", "evidence"} {
		if managedInitialAuthorityDiagnostic(leaf, err) != "MANAGED_INITIAL_AUTHORITY_FAILURE leaf="+leaf || h.logInitialAuthorityFailure(leaf, err) != err || h.logInitialAuthorityFailure(leaf, nil) != nil {
			t.Fatal("diagnostic changed error identity, redaction, or success")
		}
	}
	for _, leaf := range []string{"", "private", "live payload=private", "live\nprivate"} {
		if managedInitialAuthorityDiagnostic(leaf, err) != "" {
			t.Fatal("unsafe leaf accepted")
		}
	}
}
