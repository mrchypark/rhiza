package managedsql

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

func captureManagedHostDiagnostic(t *testing.T) *bytes.Buffer {
	t.Helper()
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	output := new(bytes.Buffer)
	log.SetOutput(output)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})
	return output
}

func TestManagedHostRequestRecoveryPrecheckDiagnosticIsFixed(t *testing.T) {
	output := captureManagedHostDiagnostic(t)
	var outage atomic.Bool
	var attempts atomic.Int64
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	selectors := []network.RequestStatusRequest{
		{Kind: "sql", RequestID: "secret-canceled-lifetime"},
		{Kind: "sql", RequestID: "secret-live-lifetime"},
	}
	if hosts[0].closed || hosts[1].closed {
		t.Fatal("precheck fixtures unexpectedly closed")
	}
	// This is the real Close cancellation-before-mutex state: admission lifetime
	// has ended, but RecoverRequest still owns the Host entry point.
	hosts[0].stopAdmission()
	result, err := hosts[0].RecoverRequest(context.Background(), selectors[0])
	if !errors.Is(err, network.ErrCommitUnknown) || !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("canceled-lifetime recovery precheck must preserve UNKNOWN and not-ready: result=%+v err=%v", result, err)
	}
	assertRecoveryPrecheckResult(t, result, err, selectors[0])

	// With Host lifetime still live, a canceled caller exercises the actual Node
	// EnsureInitialManagedAdmission refusal. If recovery incorrectly continues,
	// the live managed Node emits its resolve_borrow marker.
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()
	result, err = hosts[1].RecoverRequest(callerCtx, selectors[1])
	if !errors.Is(err, network.ErrCommitUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("live-lifetime Node refusal must preserve UNKNOWN and cancellation: result=%+v err=%v", result, err)
	}
	assertRecoveryPrecheckResult(t, result, err, selectors[1])
	line := output.String()
	if strings.Count(line, "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready") != 1 || strings.Count(line, "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=canceled") != 1 {
		t.Fatalf("actual recovery prechecks did not emit one fixed marker per refusal: %q", line)
	}
	if strings.Contains(line, selectors[0].RequestID) || strings.Contains(line, selectors[1].RequestID) || strings.Contains(line, "secret") {
		t.Fatalf("Host recovery diagnostic leaked private data: %q", line)
	}
	if strings.Contains(line, "MANAGED_REQUEST_RESOLUTION_FAILURE") {
		t.Fatalf("resolver was entered after recovery precheck refusal: %q", line)
	}
}

func assertRecoveryPrecheckResult(t *testing.T, result RecoveryResult, err error, selector network.RequestStatusRequest) {
	t.Helper()
	if result.Kind != selector.Kind || string(result.RequestID) != selector.RequestID || result.SQL != nil || result.KV != nil || result.Graph != nil {
		t.Fatalf("recovery precheck changed result identity or fabricated a receipt: %+v", result)
	}
	metadata, ok := MutationErrorContext(err)
	if !ok || metadata.Kind != selector.Kind || string(metadata.RequestID) != selector.RequestID || metadata.Slot != 0 {
		t.Fatalf("UNKNOWN mutation context changed: %+v present=%t", metadata, ok)
	}
}

func TestManagedHostReadyDuringCanceledAdmissionLifetimeDoesNotEmitRecoveryDiagnostic(t *testing.T) {
	output := captureManagedHostDiagnostic(t)
	var outage atomic.Bool
	var attempts atomic.Int64
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	host := hosts[0]
	host.stopAdmission()
	if host.closed {
		t.Fatal("fixture must represent Close's cancel-before-mutex interval, not a completed Close")
	}
	if err := host.Ready(context.Background()); err == nil {
		t.Fatal("Ready succeeded during canceled admission lifetime")
	}
	if strings.Contains(output.String(), "MANAGED_HOST_REQUEST_RECOVERY_FAILURE") {
		t.Fatalf("non-recovery Ready/Close path emitted recovery diagnostic: %q", output.String())
	}
}
