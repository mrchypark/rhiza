package node

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestManagedPostCheckpointDiagnosticLeaves(t *testing.T) {
	for _, leaf := range []string{"runtime_post_prepared_view", "runtime_post_prepared_base", "runtime_post_prepared_validate_base", "runtime_post_prepared_root", "runtime_post_prepared_prefix", "runtime_post_local_resume", "runtime_post_recorder", "runtime_post_adopted_tip", "runtime_post_recover", "runtime_post_restore", "runtime_post_material_tip", "runtime_post_material_decision", "runtime_post_material_validate_tip", "runtime_post_replay_graph", "runtime_post_replay_adopted", "runtime_post_replay_local", "runtime_post_recovery_evidence", "runtime_post_selected_seal", "runtime_post_checkpoint_promote", "runtime_post_local_backup"} {
		line := managedNodeOpenDiagnostic(leaf)
		if line != "MANAGED_HOST_OPEN_FAILURE stage="+leaf {
			t.Fatalf("fixed post-checkpoint leaf not preserved: %q", leaf)
		}
		original := errors.Join(context.Canceled, errors.New("private-payload"))
		for _, enabled := range []bool{false, true} {
			if got := logManagedNodeOpenFailure(enabled, leaf, original); got != original || !errors.Is(got, context.Canceled) {
				t.Fatal("diagnostic changed error identity")
			}
		}
		if logManagedNodeOpenFailure(true, leaf, nil) != nil || strings.Contains(line, "private-payload") {
			t.Fatal("success or private error entered diagnostic")
		}
	}
	for _, unsafe := range []string{"runtime_post_unknown", "runtime_post_recover payload=private", "runtime_post_restore\nprivate", "/private/path", "SELECT private"} {
		if managedNodeOpenDiagnostic(unsafe) != "" {
			t.Fatal("unsafe post-checkpoint diagnostic accepted")
		}
	}
}
