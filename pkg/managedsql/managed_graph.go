package managedsql

import (
	"context"
	"fmt"
	"log"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

// Return the owned Node, never its raw Server. No Host mutex is held across a
// blocking Graph read: the Node method borrows and drains its actual runtime.
func (h *Host) graphReadNode(ctx context.Context, consistency string) (context.Context, *node.Node, func(), error) {
	if !sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		return ctx, nil, nil, fmt.Errorf("managed Graph Host required")
	}
	ctx, n, release, err := h.localReadNode(ctx)
	if err == nil && consistency == "linearizable" {
		if err := n.WaitForManagedMembershipTransition(ctx); err != nil {
			release()
			return ctx, nil, nil, err
		}
	}
	return ctx, n, release, err
}

func (h *Host) GraphExecute(ctx context.Context, request types.GraphCommand) (network.GraphExecuteResponse, error) {
	if !sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		return network.GraphExecuteResponse{}, network.ErrNotReady
	}
	frame, err := network.PrepareManagedGraphExecution(request)
	if err != nil {
		return network.GraphExecuteResponse{}, err
	}
	owned, recognized, err := types.DecodeManagedGraphExecution(frame)
	if err != nil || !recognized {
		return network.GraphExecuteResponse{}, network.ErrInvalidRequest
	}
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	for attempt := 0; ; attempt++ {
		if err := h.waitForMembershipTransition(operationCtx); err != nil {
			return network.GraphExecuteResponse{}, managedGraphExecuteFailure("host_transition", err)
		}
		result, err, admissionFail := h.graphExecuteAttempt(operationCtx, owned)
		if shouldRetryMembershipTransition(err, attempt) {
			continue
		}
		err = exhaustedMembershipTransitionError(err)
		if admissionFail && err != nil {
			return network.GraphExecuteResponse{}, managedGraphExecuteFailure("host_initial_admission", err)
		}
		return result, err
	}
}

func (h *Host) graphExecuteAttempt(ctx context.Context, owned types.GraphCommand) (network.GraphExecuteResponse, error, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.node == nil {
		return network.GraphExecuteResponse{}, network.ErrNotReady, false
	}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return network.GraphExecuteResponse{}, err, true
	}
	result, err := h.node.ExecuteManagedGraph(ctx, owned)
	p := applicationOriginal{kind: "graph", graph: owned}
	err = internalRequestOutcome(p, result.MutationReceipt, err)
	h.rememberRequestOutcome(p, err, result.MutationReceipt)
	return result, err, false
}

func managedGraphExecuteFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	log.Printf("MANAGED_GRAPH_EXECUTE_FAILURE stage=%s cause=%s", stage, managedRepairCauseClass(err))
	return err
}

func (h *Host) SetGraphStreamOffset(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphExecuteResponse, error) {
	return h.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamOffset: &types.GraphStreamOffsetMutation{Stream: request.Stream, Consumer: request.Consumer, Sequence: request.Sequence}})
}

func (h *Host) TrimGraphStream(ctx context.Context, request network.GraphStreamTrimRequest) (network.GraphExecuteResponse, error) {
	return h.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamTrim: &types.GraphStreamTrimMutation{Stream: request.Stream, ThroughSequence: request.ThroughSequence}})
}

func callGraphRead[T any](h *Host, ctx context.Context, consistency string, fn func(context.Context, *node.Node) (T, error)) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		opCtx, n, release, err := h.graphReadNode(ctx, consistency)
		if err != nil {
			if consistency == "linearizable" && shouldRetryMembershipTransition(err, attempt) {
				if waitErr := h.waitForLinearizableTransition(ctx); waitErr != nil {
					return zero, waitErr
				}
				continue
			}
			return zero, err
		}
		resp, err := fn(opCtx, n)
		release()
		if consistency == "linearizable" && shouldRetryMembershipTransition(err, attempt) {
			continue
		}
		return resp, exhaustedMembershipTransitionError(err)
	}
}

func (h *Host) GraphQuery(ctx context.Context, request network.GraphQueryRequest) (types.GraphCommandResult, error) {
	return callGraphRead(h, ctx, request.Consistency, func(opCtx context.Context, n *node.Node) (types.GraphCommandResult, error) {
		return n.GraphQueryManaged(opCtx, request)
	})
}

func (h *Host) GraphReachable(ctx context.Context, request types.GraphReachableRequest) (types.GraphReachableResult, error) {
	return callGraphRead(h, ctx, request.Consistency, func(opCtx context.Context, n *node.Node) (types.GraphReachableResult, error) {
		return n.GraphReachableManaged(opCtx, request)
	})
}

func (h *Host) GraphChanges(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	return callGraphRead(h, ctx, request.Consistency, func(opCtx context.Context, n *node.Node) (network.GraphStreamReadResponse, error) {
		return n.GraphChangesManaged(opCtx, request)
	})
}

func (h *Host) GraphStreamRead(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	return callGraphRead(h, ctx, request.Consistency, func(opCtx context.Context, n *node.Node) (network.GraphStreamReadResponse, error) {
		return n.GraphStreamReadManaged(opCtx, request)
	})
}

func (h *Host) GraphStreamOffset(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphStreamOffsetResponse, error) {
	return callGraphRead(h, ctx, request.Consistency, func(opCtx context.Context, n *node.Node) (network.GraphStreamOffsetResponse, error) {
		return n.GraphStreamOffsetManaged(opCtx, request)
	})
}
