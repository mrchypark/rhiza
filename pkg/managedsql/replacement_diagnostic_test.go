package managedsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

func TestRepairResumeFailureIsBoundedAndUnwraps(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	secret := errors.New("public error code: not-ready")
	for _, stage := range []repairResumeStage{
		repairResumeRegistration,
		repairResumeMembership,
		repairResumeVerification,
		repairResumeCompletion,
	} {
		t.Run(string(stage), func(t *testing.T) {
			var output bytes.Buffer
			log.SetOutput(&output)
			err := repairResumeFailure(stage, secret)
			if err != secret {
				t.Fatal("diagnostic changed the public error value")
			}
			if !errors.Is(err, secret) {
				t.Fatal("diagnostic did not preserve the wrapped cause")
			}
			if got := err.Error(); got != secret.Error() {
				t.Fatalf("public error message changed: %q", got)
			}
			line := output.String()
			if !strings.Contains(line, "stage="+string(stage)+" cause=other") || strings.Contains(line, secret.Error()) {
				t.Fatalf("diagnostic was not bounded/redacted: %q", line)
			}
		})
	}

	wrapped := fmt.Errorf("add registered learner: %w", network.ErrNotReady)
	if got := repairResumeFailure(repairResumeMembership, wrapped); got != wrapped || !errors.Is(got, network.ErrNotReady) || got.Error() != wrapped.Error() {
		t.Fatalf("diagnostic changed the wrapped membership error: %v", got)
	}
}

func TestManagedRepairCauseClassAllowlist(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "deadline"},
		{name: "not-ready", err: network.ErrNotReady, want: "not_ready"},
		{name: "invalid", err: network.ErrInvalidRequest, want: "invalid_request"},
		{name: "conflict", err: network.ErrRequestConflict, want: "request_conflict"},
		{name: "unknown-secret", err: errors.New("private object path and payload"), want: "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := managedRepairCauseClass(test.err); got != test.want {
				t.Fatalf("cause class=%q, want %q", got, test.want)
			}
		})
	}
}
