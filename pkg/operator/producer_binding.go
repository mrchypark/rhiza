package operator

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func (e *FencingExecutor) bindSupervisorWAL(ctx context.Context, uid string, id fencing.Identity) (fencing.BindReceipt, error) {
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	scopeHash := ""
	if state.Ledger.RequireMutationJournal {
		var pod object
		if err := e.Kube.Get(ctx, e.controller().corePath("pods", id.NodeID), &pod); err != nil {
			return fencing.BindReceipt{}, err
		}
		if str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: bind Pod incarnation differs")
		}
		ownerName := ""
		for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
			owner := asObject(ref)
			if str(owner["kind"]) == "StatefulSet" && str(owner["uid"]) == uid {
				ownerName = str(owner["name"])
			}
		}
		if ownerName == "" {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: bind workload owner differs")
		}
		var sts object
		if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
			return fencing.BindReceipt{}, err
		}
		if str(nested(sts, "metadata", "uid")) != uid || nested(sts, "metadata", "deletionTimestamp") != nil {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: bind workload incarnation differs")
		}
		scope, err := e.storageScope(ctx, sts, id.SourceClusterID)
		if err != nil {
			return fencing.BindReceipt{}, err
		}
		// Compare actual Pod configuration too; a changed template cannot silently
		// redirect a still-running source incarnation to a different store.
		podView := object{"spec": object{"template": object{"spec": pod["spec"]}}}
		podScope, err := e.storageScope(ctx, podView, id.SourceClusterID)
		if err != nil || podScope != scope || scope.Provider != "gcs" {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: source/profile storage scope differs")
		}
		scopeHash = hashJSON(scope)
	}
	return e.bindWALAtScope(ctx, uid, id, scopeHash)
}

// scopeHash is derived only by the authenticated server from the owned workload,
// never accepted from the supervisor's request body. Tests use synthetic scopes.
func (e *FencingExecutor) bindWALAtScope(ctx context.Context, uid string, id fencing.Identity, scopeHash string) (fencing.BindReceipt, error) {
	state, err := e.change(ctx, uid, id.SourceClusterID, func(l *fencing.Ledger) error {
		if l.RequireMutationJournal {
			if err := l.BindMutationScope(id, scopeHash, fencing.MutationExecutionProfile); err != nil {
				return err
			}
		} else if scopeHash != "" {
			return fmt.Errorf("fencing: mutation binding profile changed")
		}
		return l.Bind(id)
	})
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	run := state.Ledger.Runs[id.RunID]
	if state.Ledger.RequireMutationJournal {
		if run.Runtime == nil {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: bound runtime is missing")
		}
		producer, err := e.changeMutationProducer(ctx, *run.Runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) })
		if err != nil {
			return fencing.BindReceipt{}, err
		}
		if producer.Phase != fencing.MutationOpen || producer.WALIdentity != id.WALIdentity {
			return fencing.BindReceipt{}, fencing.ErrClosed
		}
	}
	confirmed, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	current, ok := confirmed.Ledger.Runs[id.RunID]
	_, voterClosed := confirmed.Ledger.ClosedNodes[id.NodeID]
	if !ok || current.Identity != id || current.Stop != nil || confirmed.Ledger.ClosedBy != "" || voterClosed {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	if confirmed.Ledger.RequireMutationJournal != state.Ledger.RequireMutationJournal {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: binding profile changed during activation")
	}
	receipt := fencing.BindReceipt{Identity: id}
	if confirmed.Ledger.RequireMutationJournal {
		if current.Runtime == nil || run.Runtime == nil || *current.Runtime != *run.Runtime || current.MutationProducerUID != run.MutationProducerUID || current.MutationScopeHash != scopeHash || current.MutationProfile != fencing.MutationExecutionProfile {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: producer binding changed during activation")
		}
		receipt.Producer = &fencing.ProducerBinding{ContractVersion: fencing.MutationIntentVersion,
			Runtime: *current.Runtime, ProducerUID: current.MutationProducerUID,
			StoreScopeHash: current.MutationScopeHash, ExecutionProfile: current.MutationProfile}
	}
	return receipt, receipt.Validate(id)
}
