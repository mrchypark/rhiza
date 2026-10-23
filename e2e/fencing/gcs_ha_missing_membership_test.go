package main

import (
	"os"
	"strings"
	"testing"
)

func TestMissingMembershipLauncherUsesDistinctDefaultOffCases(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(launcher)
	for _, required := range []string{
		"missing_membership=${RHIZA_GCS_MISSING_MEMBERSHIP:-no}",
		"missing_membership_case=${RHIZA_GCS_MISSING_MEMBERSHIP_CASE:-}",
		"negative|positive",
		"\"$initial_head_ack_loss\" \"$async_nonempty\" \"$manual_terminal\" \"$missing_membership\"",
		"run-gcs-ha-missing-membership",
		"FENCING_E2E_QUALIFICATION=\"gcs-missing-membership-$missing_membership_case\"",
		"strict missing-membership artifact projection failed",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("missing-membership launcher contract is missing %q", required)
		}
	}
	start := strings.Index(source, "if [ \"$missing_membership\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?missing-membership qualification requires the fencing-e2e driver}\"")
	dispatch := strings.Index(source, "step 30-missing-membership-collector env")
	call := strings.Index(source, `"$FENCING_E2E_BIN" run-gcs-ha-missing-membership`)
	branchExit := strings.Index(source, "echo \"GCS missing-membership $missing_membership_case qualification collector completed\"\n  exit 0\nfi")
	legacy := strings.Index(source, "step 30-driver python3 -u")
	if start < 0 || dispatch < start || call < dispatch || branchExit < call || legacy < branchExit {
		t.Fatal("missing-membership scenario must use the native driver and exit before legacy Python")
	}
}
