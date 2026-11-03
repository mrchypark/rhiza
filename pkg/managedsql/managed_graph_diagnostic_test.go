package managedsql

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

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
