package operator

import (
	"os/exec"
	"testing"
)

func TestPhase1AdmissionOracle(t *testing.T) {
	cmd := exec.Command("sh", "../../scripts/test-ha-v1alpha2-contract.sh", "--self-test-oracle")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("admission oracle self-test: %v\n%s", err, output)
	}
}
