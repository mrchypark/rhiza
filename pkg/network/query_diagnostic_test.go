package network

import (
	"context"
	"errors"
	"testing"
)

func TestManagedQueryDiagnosticScopeStagesAndErrorIdentity(t *testing.T) {
	plain := context.Background()
	ctx := context.WithValue(plain, managedQueryDiagnosticKey{}, true)
	for _, stage := range []string{"server_read_acquire", "server_read_index", "server_catchup", "server_apply", "server_materialize"} {
		if managedQueryDiagnostic(plain, stage) != "" {
			t.Fatal("non-query diagnostic enabled")
		}
		if got := managedQueryDiagnostic(ctx, stage); got != "MANAGED_SQL_QUERY_FAILURE stage="+stage {
			t.Fatalf("stage %q not preserved", stage)
		}
	}
	for _, stage := range []string{"", "unknown", "server_apply extra=private", "https://private.invalid", "server_apply\npayload"} {
		if managedQueryDiagnostic(ctx, stage) != "" {
			t.Fatal("unsafe stage accepted")
		}
	}
	err := errors.Join(context.Canceled, errors.New("private error must not be formatted"))
	if logManagedQueryFailure(ctx, "server_apply", err) != err || logManagedQueryFailure(ctx, "server_apply", nil) != nil || logManagedQueryFailure(plain, "server_apply", err) != err {
		t.Fatal("query diagnostic changed error identity")
	}
}
