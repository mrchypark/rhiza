package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
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
	secret := fmt.Errorf("private value: %w", context.DeadlineExceeded)
	got := managedGraphExecuteFailure("network_submit", secret)
	if got != secret || got.Error() != secret.Error() || !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("diagnostic changed public error: got=%v", got)
	}
	line := output.String()
	if !strings.Contains(line, "stage=network_submit cause=deadline") || strings.Contains(line, secret.Error()) {
		t.Fatalf("diagnostic was not fixed-label/redacted: %q", line)
	}
}
