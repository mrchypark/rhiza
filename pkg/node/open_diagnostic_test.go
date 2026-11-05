package node

import (
	"context"
	"errors"
	"testing"
)

func TestManagedNodeOpenDiagnosticStagesAndErrorIdentity(t *testing.T) {
	for _, stage := range []string{"prepared_config", "prepared_authority", "prepared_identity", "prepared_lock", "prepared_origin", "prepared_adoption", "prepared_wal", "prepared_materializer", "prepared_restore", "prepared_runtime_open", "adoption_identity", "adoption_wal_history", "adoption_sql_reconstruct", "adoption_sql_digest", "runtime_config", "runtime_optional_supervisor", "runtime_policy", "runtime_wal_lock", "runtime_wal_open", "runtime_source_open", "runtime_registration", "runtime_checkpoint_load", "runtime_archive_load", "runtime_materializer", "runtime_core_open", "runtime_archive_recover", "runtime_pair_reconstruct", "runtime_peer_open", "runtime_post_checkpoint", "prepared_runtime_adoption"} {
		if got := managedNodeOpenDiagnostic(stage); got != "MANAGED_HOST_OPEN_FAILURE stage="+stage {
			t.Fatalf("stage %q not preserved", stage)
		}
	}
	for _, stage := range []string{"", "unknown", "host_config extra=private", "https://private.invalid", "host_config\npayload"} {
		if managedNodeOpenDiagnostic(stage) != "" {
			t.Fatal("unsafe open stage accepted")
		}
	}
	original := errors.Join(context.Canceled, errors.New("private error must not be formatted"))
	if logManagedNodeOpenFailure(true, "adoption_identity", original) != original || logManagedNodeOpenFailure(true, "adoption_identity", nil) != nil || logManagedNodeOpenFailure(false, "adoption_identity", original) != original {
		t.Fatal("open diagnostic changed error identity")
	}
}

func TestManagedSQLQueryHistoryDiagnosticScopeAndNumbers(t *testing.T) {
	ctx := context.WithValue(context.Background(), managedSQLQueryDiagnosticKey{}, true)
	if managedSQLQueryHistoryDiagnostic(context.Background(), 1, 2, 1, 1) != "" {
		t.Fatal("non-query history diagnostic enabled")
	}
	if got := managedSQLQueryHistoryDiagnostic(ctx, 0, ^uint64(0), 3, 2); got != "MANAGED_SQL_QUERY_HISTORY_REFUSED target=0 recorded=18446744073709551615 certified=3 applied=2" {
		t.Fatal("query history numeric diagnostic changed")
	}
}
