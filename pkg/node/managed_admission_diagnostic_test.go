package node

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedAdmissionDiagnosticsAreFixedAndRedacted(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	var output bytes.Buffer
	log.SetOutput(&output)
	logManagedGraphHistoryRefused("initial", 17, ManagedRuntimeState{
		RecorderTip:  15,
		CertifiedTip: 14,
		AppliedTip:   13,
	})
	secret := fmt.Errorf("private peer payload and object path: %w", network.ErrNotReady)
	got := managedRequestResolutionStageFailure("resolve_admit", secret)
	if got != secret || !errors.Is(got, network.ErrNotReady) {
		t.Fatalf("ResolveRequest admission diagnostic changed original error: got=%v", got)
	}
	logManagedRequestOriginalLookupError(secret)
	logManagedRequestOriginalNotFound()
	logManagedRequestOriginalFound(materializer.RequestOriginal{
		Kind: types.MutationGraph, RequestID: "private-request-id", Slot: 7,
		Receipt: types.MutationReceipt{Slot: 7},
	}, types.MutationGraph, "private-request-id")
	logManagedRequestOriginalFound(materializer.RequestOriginal{
		Kind: types.MutationGraph, RequestID: "private-request-id",
	}, types.MutationGraph, "private-request-id")

	line := output.String()
	for _, fixed := range []string{
		"MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=initial read_target_known=true target=17 recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true",
		"MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=not_ready",
		"MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=not_ready",
		"MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found",
		"MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=7",
		"MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false",
	} {
		if !strings.Contains(line, fixed) {
			t.Fatalf("diagnostic omitted %q: %q", fixed, line)
		}
	}
	for _, secretText := range []string{"private peer payload", "object path", secret.Error(), "private-request-id"} {
		if strings.Contains(line, secretText) {
			t.Fatalf("diagnostic leaked %q: %q", secretText, line)
		}
	}
}
