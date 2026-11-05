package node

import (
	"context"
	"errors"
	"testing"
)

func TestManagedSQLQueryDiagnosticScopeStagesAndErrorIdentity(t *testing.T) {
	plain := context.Background()
	ctx := context.WithValue(plain, managedSQLQueryDiagnosticKey{}, true)
	for _, stage := range []string{"node_borrow", "node_local_read", "node_admission", "node_server", "admission_runtime_pre", "admission_pre_control", "admission_owned_wait", "admission_read_index", "admission_wait_tip", "admission_replay", "admission_runtime_post", "admission_fixed_replay", "admission_unresolved_history"} {
		if managedSQLQueryDiagnostic(plain, stage) != "" {
			t.Fatal("non-query diagnostic enabled")
		}
		if got := managedSQLQueryDiagnostic(ctx, stage); got != "MANAGED_SQL_QUERY_FAILURE stage="+stage {
			t.Fatalf("stage %q not preserved", stage)
		}
	}
	for _, stage := range []string{"", "unknown", "node_borrow extra=private", "https://private.invalid", "node_borrow\npayload"} {
		if managedSQLQueryDiagnostic(ctx, stage) != "" {
			t.Fatal("unsafe stage accepted")
		}
	}
	err := errors.Join(context.Canceled, errors.New("private error must not be formatted"))
	if logManagedSQLQueryFailure(ctx, "node_borrow", err) != err || logManagedSQLQueryFailure(ctx, "node_borrow", nil) != nil || logManagedSQLQueryFailure(plain, "node_borrow", err) != err {
		t.Fatal("query diagnostic changed error identity")
	}
}
