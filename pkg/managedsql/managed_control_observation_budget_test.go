package managedsql

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedGraphHostOperationContextSharesNodeObservationBudget(t *testing.T) {
	h := &Host{
		cfg:  Config{Plan: operator.V1Alpha2ApplicationInitPlan{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol}},
		node: node.New(&types.ExecutionConfig{}), admissionLifetime: context.Background(),
	}
	operationCtx, release := h.internalRequestContext(context.Background())
	defer release()
	if shared := node.WithManagedControlObservationBudget(operationCtx); shared != operationCtx {
		t.Fatal("Host did not install the context-private operation budget before Node admission")
	}
	initialCtx, releaseInitial := h.internalRequestContext(operationCtx)
	defer releaseInitial()
	if shared := node.WithManagedControlObservationBudget(initialCtx); shared != initialCtx {
		t.Fatal("nested Host initial-admission context replaced the operation budget")
	}
	executionCtx := node.WithManagedControlObservationBudget(initialCtx)
	if executionCtx != initialCtx {
		t.Fatal("Node execution context replaced the budget shared with initial admission")
	}
}
