package managedsql

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

func TestManagedHostQueryDiagnosticStagesAndErrorIdentity(t *testing.T) {
	for _, stage := range []string{"host_closed", "host_initial_admission"} {
		if got := managedHostQueryDiagnostic(stage); got != "MANAGED_SQL_QUERY_FAILURE stage="+stage {
			t.Fatalf("stage %q not preserved", stage)
		}
	}
	for _, stage := range []string{"", "unknown", "host_closed extra=private", "https://private.invalid", "host_closed\npayload"} {
		if managedHostQueryDiagnostic(stage) != "" {
			t.Fatal("unsafe stage accepted")
		}
	}
	err := errors.Join(context.Canceled, errors.New("private error must not be formatted"))
	h := &Host{}
	h.cfg.Plan.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	if h.logQueryFailure("host_closed", err) != err || h.logQueryFailure("host_closed", nil) != nil {
		t.Fatal("query diagnostic changed error identity")
	}
}
