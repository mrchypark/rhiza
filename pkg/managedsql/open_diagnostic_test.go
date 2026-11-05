package managedsql

import (
	"context"
	"errors"
	"testing"
)

func TestManagedHostOpenDiagnosticStagesAndErrorIdentity(t *testing.T) {
	for _, stage := range []string{"host_config", "host_binding", "host_plan", "host_bootstrap", "host_authority", "host_prepared_identity", "host_source", "host_node_open", "host_initialization"} {
		if got := managedHostOpenDiagnostic(stage); got != "MANAGED_HOST_OPEN_FAILURE stage="+stage {
			t.Fatalf("stage %q not preserved", stage)
		}
	}
	for _, stage := range []string{"", "unknown", "host_config extra=private", "https://private.invalid", "host_config\npayload"} {
		if managedHostOpenDiagnostic(stage) != "" {
			t.Fatal("unsafe open stage accepted")
		}
	}
	original := errors.Join(context.Canceled, errors.New("private error must not be formatted"))
	if logManagedHostOpenFailure("host_config", original) != original || logManagedHostOpenFailure("host_config", nil) != nil {
		t.Fatal("open diagnostic changed error identity")
	}
}
