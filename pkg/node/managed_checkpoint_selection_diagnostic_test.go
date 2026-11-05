package node

import (
	"errors"
	"fmt"
	"testing"
)

func TestManagedCheckpointSelectionDiagnosticFixedFormat(t *testing.T) {
	reasons := []string{
		"selection_unavailable", "local_seal_read", "current_read", "refreshed_local_seal_read",
		"current_without_certified_qckp", "current_ahead", "current_rank_conflict",
		"archive_load", "archive_sealed", "archive_tip_behind", "archive_base_ahead",
		"archive_base_invalid", "archive_decision_unavailable", "core_certificate_mismatch",
		"archive_certificate_invalid", "candidate_validation", "root_open",
		"root_state_policy_mismatch", "current_certificate_hash_mismatch",
		"prefix_refresh_unavailable", "prefix_archive_load", "prefix_unexpected_base",
		"prefix_page_incomplete", "prefix_decision_identity", "prefix_decision_policy",
		"prefix_certificate_accept",
	}
	for _, reason := range reasons {
		for _, prepared := range []bool{false, true} {
			for _, voter := range []bool{false, true} {
				want := fmt.Sprintf("MANAGED_CHECKPOINT_SELECT_FAILURE reason=%s prepared_readonly_learner=%t local_voter=%t local_config=7", reason, prepared, voter)
				if got := managedCheckpointSelectionDiagnostic(reason, prepared, voter, 7); got != want {
					t.Fatalf("diagnostic differs: %q", got)
				}
			}
		}
	}
}

func TestManagedCheckpointSelectionFailurePreservesErrorAndMissingResources(t *testing.T) {
	cause := errors.New("private-error-path-payload")
	want := fmt.Errorf("original wrapping: %w", cause)
	for _, n := range []*Node{nil, {}, {config: nil}} {
		got := n.managedCheckpointSelectionFailure("selection_unavailable", want)
		if got != want || !errors.Is(got, cause) {
			t.Fatal("diagnostic changed original error identity or wrapping")
		}
	}
}
