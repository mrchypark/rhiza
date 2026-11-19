package managedsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphExecuteFailureIsRedactedAndPreservesError(t *testing.T) {
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
	secret := fmt.Errorf("object path and payload: %w", network.ErrNotReady)
	got := managedGraphExecuteFailure("host_initial_admission", secret)
	if got != secret || got.Error() != secret.Error() || !errors.Is(got, network.ErrNotReady) {
		t.Fatalf("diagnostic changed public error: got=%v", got)
	}
	line := output.String()
	if !strings.Contains(line, "stage=host_initial_admission cause=not_ready") || strings.Contains(line, secret.Error()) {
		t.Fatalf("diagnostic was not fixed-label/redacted: %q", line)
	}
}

func TestManagedGraphNotSubmittedPreservesCauseAndBoundary(t *testing.T) {
	err := &GraphNotSubmittedError{Stage: SQLStageAdmission, Cause: network.ErrNotReady}
	if !errors.Is(err, ErrGraphNotSubmitted) || !errors.Is(err, network.ErrNotReady) || errors.Is(err, ErrSQLNotSubmitted) || errors.Is(err, ErrKVNotSubmitted) {
		t.Fatalf("Graph submission boundary lost: %v", err)
	}
	h := &Host{}
	if _, got := h.GraphExecute(context.Background(), types.GraphCommand{RequestID: "not-graph", Cypher: "RETURN 1"}); !errors.Is(got, ErrGraphNotSubmitted) || !errors.Is(got, network.ErrNotReady) {
		t.Fatalf("closed Graph host did not report pre-submission: %v", got)
	}
}
