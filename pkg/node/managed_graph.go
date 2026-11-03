package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

type managedControlObservationBudgetKey struct{}

type managedControlObservationBudget struct{ used atomic.Bool }

func (b *managedControlObservationBudget) consume() bool {
	return b != nil && b.used.CompareAndSwap(false, true)
}

// WithManagedControlObservationBudget shares one fallback allowance across
// nested Host admission and execution calls.
func WithManagedControlObservationBudget(ctx context.Context) context.Context {
	if ctx.Value(managedControlObservationBudgetKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, managedControlObservationBudgetKey{}, &managedControlObservationBudget{})
}

func managedControlObservationBudgetFromContext(ctx context.Context) *managedControlObservationBudget {
	budget, _ := ctx.Value(managedControlObservationBudgetKey{}).(*managedControlObservationBudget)
	return budget
}

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
	if err != nil {
		return ManagedGraphSnapshot{}, errors.Join(fmt.Errorf("Graph lineage changed or is not fresh"), err)
	}
	current, err := n.managedRuntimeStateWithContext(ctx)
	if err != nil {
		return ManagedGraphSnapshot{}, errors.Join(fmt.Errorf("Graph lineage changed or is not fresh"), err)
	}
	if runtime != current || fresh && history.LastGraphSlot != 0 {
		return ManagedGraphSnapshot{}, fmt.Errorf("Graph lineage changed or is not fresh")
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
	if !isManagedGraphApplicationProtocol(n.managedStartup.Identity.ApplicationProtocol) || n.server == nil {
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
		appendOnlyNotReady := isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) && errors.Is(err, network.ErrNotReady)
		if appendOnlyNotReady {
			localtesthooks.Hit("node:managed-graph:local-read-not-ready")
		}
		// A local read must not perform ReadIndex or wait for quorum, but an
		// append-only runtime also cannot expose its projection until the first
		// certified root is durable. Once normal local-read admission checks
		// establish a live voter, wait on the publisher's completion signal and
		// revalidate the projection. The publisher does not call this public read
		// path, so this wait cannot wait on its own completion.
		if appendOnlyNotReady && !n.managedGraphInitialRootComplete() {
			if _, runtimeErr := n.checkInternalApplicationRuntime(scoped); runtimeErr != nil {
				release()
				return ctx, nil, runtimeErr
			}
			if !n.managedInitiallyAdmitted.Load() || !n.core.IsVoter() {
				release()
				return ctx, nil, network.ErrNotReady
			}
			if waitErr := n.ensureManagedGraphInitialRoot(scoped); waitErr != nil {
				release()
				return ctx, nil, waitErr
			}
		}
		if appendOnlyNotReady {
			// Always take a fresh projection check after the initial-root gate,
			// including when the publisher completed between the first failed
			// check and the wait decision above.
			err = n.CheckManagedLocalRead(scoped)
		}
		if err != nil {
			release()
			return ctx, nil, fmt.Errorf("managed Graph read local-projection: %w", err)
		}
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
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("node_binding", err)
	}
	defer release()
	if !isManagedGraphApplicationProtocol(n.managedStartup.Identity.ApplicationProtocol) || n.server == nil {
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("node_protocol", network.ErrNotReady)
	}
	if err := n.admitInternalApplication(ctx); err != nil {
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("node_admission", err)
	}
	if err := n.recordManagedApplicationStart(ctx); err != nil {
		return network.GraphExecuteResponse{}, managedGraphExecuteFailure("node_application_start", err)
	}
	return n.server.GraphExecute(ctx, original)
}

func managedGraphExecuteFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	log.Printf("MANAGED_GRAPH_EXECUTE_FAILURE stage=%s cause=%s", stage, managedMembershipCauseClass(err))
	return err
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
