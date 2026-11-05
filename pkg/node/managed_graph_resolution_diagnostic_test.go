package node

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphResolutionDiagnosticFixedLeavesAndRedaction(t *testing.T) {
	state := managedGraphResolutionDiagnosticState{originalSlot: 77, cut: 77, floor: 1, recorded: 81, certified: 80, applied: 77, certificatePresent: true, certificateMatches: true, bindingCurrent: true, voter: true, cutCovered: true}
	err := fmt.Errorf("PRIVATE_ID/PATH/PAYLOAD: %w", network.ErrDurabilityUnavailable)
	for _, leaf := range []string{"original_certificate", "durability_result", "runtime_post", "original_recheck"} {
		want := "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=" + leaf + " original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=durability_unavailable"
		if got := managedGraphResolutionFailureDiagnostic(leaf, state, err); got != want || strings.Contains(got, "PRIVATE") {
			t.Fatalf("unsafe or changed diagnostic: %q", got)
		}
	}
	for _, leaf := range []string{"", "private", "runtime_post extra=PRIVATE", "runtime_post\nPRIVATE"} {
		if got := managedGraphResolutionFailureDiagnostic(leaf, state, err); got != "" {
			t.Fatalf("unknown leaf emitted: %q", got)
		}
	}
	if managedGraphResolutionFailureDiagnostic("runtime_post", state, nil) != "" {
		t.Fatal("successful resolution emitted failure diagnostic")
	}
	if got := logManagedGraphResolutionFailure("runtime_post", state, err); got != err || !errors.Is(got, network.ErrDurabilityUnavailable) {
		t.Fatal("diagnostic changed original error identity")
	}
}
