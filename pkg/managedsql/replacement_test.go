package managedsql

import (
	"context"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestReplaceVoterAddFirstRejectsNonAppendOnlyBeforeRuntimeAccess(t *testing.T) {
	for _, protocol := range []string{
		"", // legacy protocol 7
		types.ManagedApplicationProtocolSQLKV,
		sqlpolicy.ManagedRequestApplicationProtocol,
		sqlpolicy.ManagedGraphRequestApplicationProtocol,
	} {
		t.Run(protocol, func(t *testing.T) {
			h := &Host{cfg: Config{Plan: operator.V1Alpha2ApplicationInitPlan{ApplicationProtocol: protocol}}}
			plan := operator.V1Alpha2RepairPlan{Sequence: operator.V1Alpha2RepairAddThenRemove}
			if err := h.ReplaceVoter(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "managed append-only application protocol") {
				t.Fatalf("ADD-first must fail before runtime/status access for protocol %q: %v", protocol, err)
			}
		})
	}
	for _, protocol := range []string{
		sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol,
		sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol,
	} {
		if err := validateRepairSequenceProfile(operator.V1Alpha2RepairAddThenRemove, protocol); err != nil {
			t.Fatalf("supported protocol %q rejected: %v", protocol, err)
		}
	}
}

func TestReplaceVoterAppendOnlyRejectsRemoveFirstBeforeRuntimeAccess(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		for _, sequence := range []operator.V1Alpha2RepairSequence{"", operator.V1Alpha2RepairRemoveThenAdd} {
			t.Run(protocol+"/"+string(sequence), func(t *testing.T) {
				h := &Host{cfg: Config{Plan: operator.V1Alpha2ApplicationInitPlan{ApplicationProtocol: protocol}}}
				if err := h.ReplaceVoter(context.Background(), operator.V1Alpha2RepairPlan{Sequence: sequence}); err == nil || !strings.Contains(err.Error(), "append-only replacement requires ADD-first") {
					t.Fatalf("REMOVE-first must fail before runtime/status access: %v", err)
				}
			})
		}
	}
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		for _, sequence := range []operator.V1Alpha2RepairSequence{"", operator.V1Alpha2RepairRemoveThenAdd} {
			if err := validateRepairSequenceProfile(sequence, protocol); err == nil {
				t.Fatalf("retired legacy replacement %q/%q accepted", protocol, sequence)
			}
		}
	}
}
