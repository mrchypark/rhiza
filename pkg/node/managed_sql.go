package node

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedSQLWriteChecker must check the exact live pending request, not just
// general readiness. Only the private SQL host implements this boundary.
type ManagedSQLWriteChecker interface {
	ValidateManagedSQLWrite(context.Context, ManagedStartupIdentity, ManagedRuntimeState, network.ExecuteRequest) error
}

type ManagedSQLSnapshot struct {
	Application materializer.SQLApplicationState
	Runtime     ManagedRuntimeState
}

// InspectManagedSQL rejects histories outside this bounded SQL-only host. An
// unresolved recorder is never empty-state proof, even when SQLite is empty.
func (n *Node) InspectManagedSQL(ctx context.Context, fresh bool) (ManagedSQLSnapshot, error) {
	if _, err := n.ManagedState(); err != nil {
		return ManagedSQLSnapshot{}, err
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return ManagedSQLSnapshot{}, err
	}
	n.replayMu.Lock()
	defer n.replayMu.Unlock()
	before := n.managedRuntimeState()
	if before.WALIdentity == "" || before.RecorderTip > before.CertifiedTip || before.AppliedTip != before.CertifiedTip || n.core.CompactionFloor() != 0 {
		return ManagedSQLSnapshot{}, fmt.Errorf("SQL host requires registered, resolved, uncompacted history")
	}
	for from := quepaxa.Slot(1); uint64(from) <= before.CertifiedTip; {
		values, _, err := n.core.DecisionsFrom(from, 256)
		if err != nil || len(values) == 0 {
			return ManagedSQLSnapshot{}, fmt.Errorf("SQL history unavailable: %w", err)
		}
		for _, v := range values {
			if len(v.Value) != 0 {
				_, sql, err := types.DecodeSQLBatch(v.Value)
				if err != nil || sql && fresh {
					return ManagedSQLSnapshot{}, fmt.Errorf("application history prevents fresh SQL initialization")
				}
				if !sql {
					_, schedule, e1 := types.DecodeLeaderSchedule(v.Value)
					barrier, e2 := types.DecodeReadBarrier(v.Value)
					_, seal, e3 := types.DecodeCheckpointSeal(v.Value)
					reconfig, e4 := types.DecodeReconfiguration(v.Value)
					if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !(schedule || barrier || seal || reconfig) {
						return ManagedSQLSnapshot{}, fmt.Errorf("non-SQL application history")
					}
				}
			}
			from = v.Slot + 1
		}
	}
	app, err := n.material.InspectSQLApplicationState(ctx)
	if err != nil {
		return ManagedSQLSnapshot{}, err
	}
	if fresh && !app.FreshEmpty {
		return ManagedSQLSnapshot{}, fmt.Errorf("application state is not fresh empty")
	}
	if before != n.managedRuntimeState() {
		return ManagedSQLSnapshot{}, fmt.Errorf("history changed during SQL inspection")
	}
	return ManagedSQLSnapshot{Application: app, Runtime: before}, nil
}

func (n *Node) ExecuteManagedSQL(ctx context.Context, req network.ExecuteRequest) (network.ExecuteResponse, error) {
	state, err := n.ManagedState()
	if err != nil {
		return network.ExecuteResponse{}, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedSQLWriteChecker)
	if !ok {
		return network.ExecuteResponse{}, fmt.Errorf("exact managed SQL reservation checker unavailable")
	}
	if err := network.ValidateExecuteRequest(req); err != nil {
		return network.ExecuteResponse{}, err
	}
	if err := checker.ValidateManagedSQLWrite(ctx, n.managedStartup.Identity, state, req); err != nil {
		return network.ExecuteResponse{}, err
	}
	return n.server.Execute(ctx, req)
}

func (n *Node) ResolveManagedSQL(ctx context.Context, req network.ExecuteRequest) (network.ResolveExecuteResult, error) {
	if _, err := n.ManagedState(); err != nil {
		return network.ResolveExecuteResult{}, err
	}
	return n.server.ResolveExecute(ctx, req)
}

func (n *Node) QueryManagedSQL(ctx context.Context, req network.QueryRequest) (network.QueryResponse, error) {
	if n.managedStartup == nil || !n.Ready() {
		return network.QueryResponse{}, network.ErrNotReady
	}
	return n.server.Query(ctx, req)
}
