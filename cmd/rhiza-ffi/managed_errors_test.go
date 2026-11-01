//go:build cgo

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedSQLSubmissionErrorCodes(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{"known-contention", &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLStageReservation}, "sql_not_submitted"},
		{"pending-original", &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLStagePending, RecoveryRequired: true}, "sql_recovery_required"},
		{"pre-deadline", &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLStageAdmission, Cause: context.DeadlineExceeded}, "sql_not_submitted"},
		{"submitted", &network.CommitUnknownError{RequestID: "original", Cause: context.Canceled}, "commit_unknown"},
		{"unknown-priority", errors.Join(network.ErrCommitUnknown, &managedsql.SQLNotSubmittedError{}), "commit_unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := errorCode(test.err); got != test.code {
				t.Fatalf("code=%s want=%s", got, test.code)
			}
		})
	}
}
