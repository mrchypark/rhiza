package managedsql

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

// Return the owned Node, never its raw Server. No Host mutex is held across a
// blocking Graph read: the Node method borrows and drains its actual runtime.
func (h *Host) graphReadNode(ctx context.Context) (*node.Node, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.cfg.Plan.ApplicationProtocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || h.node == nil {
		return nil, fmt.Errorf("managed Graph Host required")
	}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return nil, err
	}
	return h.node, nil
}

func (h *Host) GraphExecute(ctx context.Context, request types.GraphCommand) (network.GraphExecuteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.cfg.Plan.ApplicationProtocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || h.node == nil {
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
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return network.GraphExecuteResponse{}, err
	}
	result, err := h.node.ExecuteManagedGraph(ctx, owned)
	p := applicationOriginal{kind: "graph", graph: owned}
	err = internalRequestOutcome(p, result.MutationReceipt, err)
	h.rememberRequestOutcome(p, err, result.MutationReceipt)
	return result, err
}

func (h *Host) SetGraphStreamOffset(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphExecuteResponse, error) {
	return h.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamOffset: &types.GraphStreamOffsetMutation{Stream: request.Stream, Consumer: request.Consumer, Sequence: request.Sequence}})
}

func (h *Host) TrimGraphStream(ctx context.Context, request network.GraphStreamTrimRequest) (network.GraphExecuteResponse, error) {
	return h.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamTrim: &types.GraphStreamTrimMutation{Stream: request.Stream, ThroughSequence: request.ThroughSequence}})
}

func (h *Host) GraphQuery(ctx context.Context, request network.GraphQueryRequest) (types.GraphCommandResult, error) {
	n, err := h.graphReadNode(ctx)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	return n.GraphQueryManaged(ctx, request)
}

func (h *Host) GraphReachable(ctx context.Context, request types.GraphReachableRequest) (types.GraphReachableResult, error) {
	n, err := h.graphReadNode(ctx)
	if err != nil {
		return types.GraphReachableResult{}, err
	}
	return n.GraphReachableManaged(ctx, request)
}

func (h *Host) GraphChanges(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	n, err := h.graphReadNode(ctx)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	return n.GraphChangesManaged(ctx, request)
}

func (h *Host) GraphStreamRead(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	n, err := h.graphReadNode(ctx)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	return n.GraphStreamReadManaged(ctx, request)
}

func (h *Host) GraphStreamOffset(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphStreamOffsetResponse, error) {
	n, err := h.graphReadNode(ctx)
	if err != nil {
		return network.GraphStreamOffsetResponse{}, err
	}
	return n.GraphStreamOffsetManaged(ctx, request)
}
