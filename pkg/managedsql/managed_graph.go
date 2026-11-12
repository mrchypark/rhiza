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
	if err := h.waitForMembershipTransition(operationCtx); err != nil {
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("host_transition", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.node == nil {
		return network.GraphExecuteResponse{}, network.ErrNotReady
	}
	if err := h.ensureInitialRequestAdmission(operationCtx); err != nil {
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("host_initial_admission", err)
	}
	result, err := h.node.ExecuteManagedGraph(operationCtx, owned)
	p := applicationOriginal{kind: "graph", graph: owned}
	err = internalRequestOutcome(p, result.MutationReceipt, err)
	h.rememberRequestOutcome(p, err, result.MutationReceipt)
	return result, err
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

func (h *Host) GraphQuery(ctx context.Context, request network.GraphQueryRequest) (types.GraphCommandResult, error) {
	ctx, n, release, err := h.graphReadNode(ctx, request.Consistency)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	defer release()
	return n.GraphQueryManaged(ctx, request)
}

func (h *Host) GraphReachable(ctx context.Context, request types.GraphReachableRequest) (types.GraphReachableResult, error) {
	ctx, n, release, err := h.graphReadNode(ctx, request.Consistency)
	if err != nil {
		return types.GraphReachableResult{}, err
	}
	defer release()
	return n.GraphReachableManaged(ctx, request)
}

func (h *Host) GraphChanges(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	ctx, n, release, err := h.graphReadNode(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	defer release()
	return n.GraphChangesManaged(ctx, request)
}

func (h *Host) GraphStreamRead(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	ctx, n, release, err := h.graphReadNode(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	defer release()
	return n.GraphStreamReadManaged(ctx, request)
}

func (h *Host) GraphStreamOffset(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphStreamOffsetResponse, error) {
	ctx, n, release, err := h.graphReadNode(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamOffsetResponse{}, err
	}
	defer release()
	return n.GraphStreamOffsetManaged(ctx, request)
}
