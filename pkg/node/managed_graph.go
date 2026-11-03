package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

type ManagedGraphSnapshot struct {
	Application materializer.ManagedApplicationState
	History     materializer.GraphHistory
	Runtime     ManagedRuntimeState
}

// Startup/evidence only: a certified-prefix inspection plus clean paired
// lineage, not a full native-content digest or per-request admission scan.
func (n *Node) InspectManagedGraph(ctx context.Context, fresh, localRead bool) (ManagedGraphSnapshot, error) {
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return ManagedGraphSnapshot{}, err
	}
	defer release()
	if n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return ManagedGraphSnapshot{}, network.ErrInvalidRequest
	}
	digest, empty, runtime, err := n.inspectManagedApplication(ctx, fresh, localRead)
	if err != nil {
		return ManagedGraphSnapshot{}, err
	}
	history, err := n.material.ManagedGraphHistory(ctx)
	if err != nil || runtime != n.managedRuntimeState() || fresh && history.LastGraphSlot != 0 {
		return ManagedGraphSnapshot{}, errors.Join(fmt.Errorf("Graph lineage changed or is not fresh"), err)
	}
	return ManagedGraphSnapshot{Application: materializer.ManagedApplicationState{Digest: digest, FreshEmpty: empty && history.LastGraphSlot == 0}, History: history, Runtime: runtime}, nil
}

// The outer borrow owns every pointer access, the actual long poll and its
// response checks. Nested admission checks cannot shorten this lifetime.
func (n *Node) beginManagedGraphRead(ctx context.Context, consistency string) (context.Context, func(), error) {
	_, scoped, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if n.managedStartup.Identity.ApplicationProtocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || n.server == nil {
		release()
		return ctx, nil, network.ErrNotReady
	}
	if consistency != "" && consistency != "local" && consistency != "linearizable" {
		release()
		return ctx, nil, network.ErrInvalidRequest
	}
	if consistency == "linearizable" {
		// Certified replay must be able to finish a known native/SQLite gap.
		// Check runtime/fatal health and membership here, not write admission's
		// unresolved-recorder predicate. The Server owns the actual ReadIndex,
		// certified catch-up/replay and clean backend snapshot boundary.
		if _, err := n.checkInternalApplicationRuntime(scoped); err != nil {
			release()
			return ctx, nil, err
		}
		if !n.managedInitiallyAdmitted.Load() || !n.core.IsVoter() {
			release()
			return ctx, nil, network.ErrNotReady
		}
	} else if err := n.CheckManagedLocalRead(scoped); err != nil {
		release()
		return ctx, nil, fmt.Errorf("managed Graph read local-projection: %w", err)
	}
	return scoped, release, nil
}

func (n *Node) finishManagedGraphRead(ctx context.Context, err error) error {
	_, runtimeErr := n.checkInternalApplicationRuntime(ctx)
	if err != nil {
		err = fmt.Errorf("managed Graph read server-operation: %w", err)
	}
	projectionErr := n.material.ProjectionReadable(ctx)
	if projectionErr != nil {
		projectionErr = fmt.Errorf("managed Graph read tail-projection: %w", projectionErr)
	}
	return errors.Join(err, ctx.Err(), runtimeErr, projectionErr)
}

func (n *Node) ExecuteManagedGraph(ctx context.Context, original types.GraphCommand) (network.GraphExecuteResponse, error) {
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return network.GraphExecuteResponse{}, err
	}
	defer release()
	if n.managedStartup.Identity.ApplicationProtocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || n.server == nil {
		return network.GraphExecuteResponse{}, network.ErrNotReady
	}
	if err := n.admitInternalApplication(ctx); err != nil {
		return network.GraphExecuteResponse{}, err
	}
	if err := n.recordManagedApplicationStart(ctx); err != nil {
		return network.GraphExecuteResponse{}, err
	}
	return n.server.GraphExecute(ctx, original)
}

func (n *Node) GraphQueryManaged(ctx context.Context, request network.GraphQueryRequest) (types.GraphCommandResult, error) {
	ctx, release, err := n.beginManagedGraphRead(ctx, request.Consistency)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	defer release()
	result, err := n.server.GraphQuery(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}

func (n *Node) GraphReachableManaged(ctx context.Context, request types.GraphReachableRequest) (types.GraphReachableResult, error) {
	ctx, release, err := n.beginManagedGraphRead(ctx, request.Consistency)
	if err != nil {
		return types.GraphReachableResult{}, err
	}
	defer release()
	result, err := n.server.GraphReachable(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}

func (n *Node) GraphChangesManaged(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	ctx, release, err := n.beginManagedGraphRead(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	defer release()
	result, err := n.server.GraphChanges(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}

func (n *Node) GraphStreamReadManaged(ctx context.Context, request network.GraphStreamReadRequest) (network.GraphStreamReadResponse, error) {
	ctx, release, err := n.beginManagedGraphRead(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamReadResponse{}, err
	}
	defer release()
	if localtesthooks.Enabled && request.WaitMS != 0 {
		localtesthooks.Hit("node:managed-graph:before-stream-longpoll")
	}
	result, err := n.server.GraphStreamRead(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}

func (n *Node) GraphStreamOffsetManaged(ctx context.Context, request network.GraphStreamOffsetRequest) (network.GraphStreamOffsetResponse, error) {
	ctx, release, err := n.beginManagedGraphRead(ctx, request.Consistency)
	if err != nil {
		return network.GraphStreamOffsetResponse{}, err
	}
	defer release()
	result, err := n.server.GraphStreamOffset(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}

func (n *Node) GraphRequestStatusManaged(ctx context.Context, request network.RequestStatusRequest) (network.RequestStatusResponse, error) {
	if request.Kind != "graph" {
		return network.RequestStatusResponse{}, network.ErrInvalidRequest
	}
	ctx, release, err := n.beginManagedGraphRead(ctx, "local")
	if err != nil {
		return network.RequestStatusResponse{}, err
	}
	defer release()
	result, err := n.server.RequestStatus(ctx, request)
	return result, n.finishManagedGraphRead(ctx, err)
}
