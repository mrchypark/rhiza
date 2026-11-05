package node

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestManagedAdoptionReconstructionDiagnosticLeaves(t *testing.T) {
	for _, leaf := range []string{"adoption_sql_restore_state", "adoption_sql_actual_inspect", "adoption_sql_applied_below_floor", "adoption_sql_applied_above_tip", "adoption_sql_base_binding", "adoption_sql_base_missing", "adoption_sql_adopted_prefix", "adoption_sql_bucket_open", "adoption_sql_temp_create", "adoption_sql_cache_create", "adoption_sql_reader_open", "adoption_sql_root_open", "adoption_sql_root_state_hash", "adoption_sql_checkpoint_dir_create", "adoption_sql_checkpoint_download", "adoption_sql_certified_suffix", "adoption_sql_expected_open", "adoption_sql_checkpoint_restore", "adoption_sql_suffix_apply", "adoption_sql_tip_mismatch", "adoption_sql_expected_inspect", "adoption_sql_bucket_close", "adoption_sql_temp_remove", "adoption_sql_expected_close"} {
		line := managedNodeOpenDiagnostic(leaf)
		if line != "MANAGED_HOST_OPEN_FAILURE stage="+leaf {
			t.Fatalf("fixed leaf not preserved: %q", leaf)
		}
		original := errors.Join(context.Canceled, errors.New("private-payload"))
		if got := logManagedNodeOpenFailure(false, leaf, original); got != original || !errors.Is(got, context.Canceled) {
			t.Fatal("diagnostic changed error identity")
		}
		if logManagedNodeOpenFailure(true, leaf, original) != original {
			t.Fatal("enabled diagnostic changed error identity")
		}
		if logManagedNodeOpenFailure(true, leaf, nil) != nil || strings.Contains(line, "private-payload") {
			t.Fatal("success emitted an error or private error entered formatter")
		}
	}
	for _, unsafe := range []string{"adoption_sql_private", "adoption_sql_root_open payload=private", "adoption_sql_root_open\nprivate", "https://private.invalid"} {
		if managedNodeOpenDiagnostic(unsafe) != "" {
			t.Fatal("unsafe diagnostic accepted")
		}
	}
}
