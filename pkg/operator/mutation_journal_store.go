package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// This backend is deliberately not an application-facing HTTP endpoint. Runtime
// enrollment and receipt updates must come from trusted runtime/provider workers.
// API persistence alone does not grant dispatch or validate provider evidence.
// The root enrollment ledger freezes the cohort before producer closure begins.
func (e *FencingExecutor) mutationProducerPath(runtime fencing.RuntimeBinding) string {
	return e.controller().corePath("configmaps", "rhiza-mutations-"+hashJSON(runtime)[:40])
}

func (e *FencingExecutor) enrolledMutationRun(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.Run, error) {
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return fencing.Run{}, err
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || (!state.Ledger.RequireRuntimeBinding && !state.Ledger.RequireMutationJournal) || run.Runtime == nil || *run.Runtime != runtime {
		return fencing.Run{}, fmt.Errorf("fencing: mutation producer lacks trusted root enrollment")
	}
	if err := runtime.Validate(run.Identity); err != nil {
		return fencing.Run{}, err
	}
	_, voterClosed := state.Ledger.ClosedNodes[run.Identity.NodeID]
	if (state.Ledger.ClosedBy != "" || voterClosed) && run.Stop == nil {
		return fencing.Run{}, fmt.Errorf("fencing: closed producer cohort lacks its stop binding")
	}
	return run, nil
}

// reserveMutationProducer may create only Reserved state. It never creates Open
// state from absence. If root closure already happened, create the deterministic
// missing producer directly Closed so a delayed initializer cannot reopen it.
func (e *FencingExecutor) reserveMutationProducer(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.MutationProducer, error) {
	return e.mutateProducer(ctx, runtime, true, func(*fencing.MutationProducer) error { return nil })
}

// changeMutationProducer never recreates missing state. The transition is pure;
// storage/execution side effects cannot be performed inside this CAS callback.
// On uncertain response loss, callers retry the same operation identity.
func (e *FencingExecutor) changeMutationProducer(ctx context.Context, runtime fencing.RuntimeBinding, transition func(*fencing.MutationProducer) error) (fencing.MutationProducer, error) {
	return e.mutateProducer(ctx, runtime, false, transition)
}

func (e *FencingExecutor) mutateProducer(ctx context.Context, runtime fencing.RuntimeBinding, allowCreate bool, transition func(*fencing.MutationProducer) error) (fencing.MutationProducer, error) {
	endpoint := e.mutationProducerPath(runtime)
	for range 8 {
		run, err := e.enrolledMutationRun(ctx, runtime)
		if err != nil {
			return fencing.MutationProducer{}, err
		}
		var resource object
		err = e.Kube.Get(ctx, endpoint, &resource)
		creating := apiNotFound(err)
		if err != nil && (!creating || !allowCreate) {
			return fencing.MutationProducer{}, err
		}
		var producer fencing.MutationProducer
		if creating {
			if run.Identity.WALIdentity != "" || run.MutationProducerUID != "" {
				return producer, fmt.Errorf("fencing: missing bound producer cannot be recreated")
			}
			initial, err := fencing.NewMutationProducer(run.Identity, runtime)
			if err != nil {
				return producer, err
			}
			producer = *initial
		} else {
			if nested(resource, "metadata", "deletionTimestamp") != nil || str(nested(resource, "metadata", "uid")) == "" || str(nested(resource, "metadata", "resourceVersion")) == "" {
				return producer, fmt.Errorf("fencing: mutation producer deleting or lacks storage identity")
			}
			decoder := json.NewDecoder(strings.NewReader(str(nested(resource, "data", "producer.json"))))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&producer); err != nil {
				return producer, err
			}
			if err := decoder.Decode(new(any)); err != io.EOF {
				return producer, fmt.Errorf("fencing: trailing producer state")
			}
			if producer.Runtime != runtime || producer.Validate() != nil {
				return producer, fmt.Errorf("fencing: stored mutation producer identity/state differs")
			}
			uid := str(nested(resource, "metadata", "uid"))
			if run.MutationProducerUID == "" {
				if !allowCreate || run.Identity.WALIdentity != "" || producer.LastAdmitted != 0 || (producer.Phase != fencing.MutationReserved && producer.Phase != fencing.MutationClosed) {
					return producer, fmt.Errorf("fencing: producer UID must be pinned before activation")
				}
				_, err := e.change(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error { return l.BindMutationProducer(run.Identity, runtime, uid) })
				if err != nil {
					return producer, err
				}
				// Reload both objects: closure may have raced this initialization.
				continue
			}
			if run.MutationProducerUID != uid {
				return producer, fmt.Errorf("fencing: producer object was replaced")
			}
		}
		// Every bound WAL is immutable in the root; producer transitions cannot
		// silently point at a different incarnation's admission authority.
		if producer.Runtime != runtime || (producer.WALIdentity != "" && producer.WALIdentity != run.Identity.WALIdentity) {
			return producer, fmt.Errorf("fencing: stored mutation producer WAL differs from root")
		}
		if run.Stop != nil && producer.Phase != fencing.MutationClosed {
			if err := producer.Close(run.Stop.OperationID, run.Stop.RequestHash); err != nil {
				return producer, err
			}
		}
		if err := transition(&producer); err != nil {
			return producer, err
		}
		if producer.Runtime != runtime || (producer.WALIdentity != "" && producer.WALIdentity != run.Identity.WALIdentity) {
			return producer, fmt.Errorf("fencing: transition changed producer WAL identity")
		}
		if err := producer.Validate(); err != nil {
			return producer, err
		}
		data, err := json.Marshal(producer)
		if err != nil {
			return producer, err
		}
		if len(data) > 700000 {
			return producer, fmt.Errorf("fencing: mutation journal capacity exhausted")
		}
		if creating {
			name := strings.TrimPrefix(endpoint, e.controller().corePath("configmaps", "")+"/")
			resource = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": name, "namespace": e.Kube.Namespace}, "data": object{"producer.json": string(data)}}
			err = e.Kube.Post(ctx, e.controller().corePath("configmaps", ""), resource, nil)
		} else {
			resource["data"] = object{"producer.json": string(data)}
			err = e.Kube.Put(ctx, endpoint, resource, nil)
		}
		if err == nil {
			if creating {
				// Creation is not enrollment. Bind the server-assigned UID in
				// the root before returning any authorization.
				continue
			}
			return producer, nil
		}
		var api *APIError
		if !errors.As(err, &api) || api.StatusCode != http.StatusConflict {
			return producer, err
		}
	}
	return fencing.MutationProducer{}, fencing.ErrUnavailable
}

// closeMutationProducers includes every enrolled run in the closed scope, not
// merely ConfigMaps returned by a list. Incomplete creation is still an obligation.
// The result is a mutation barrier only, never runtime-termination evidence.
func (e *FencingExecutor) closeMutationProducers(ctx context.Context, state fenceLedger, nodes []string) (bool, error) {
	if !state.Ledger.RequireRuntimeBinding || (len(nodes) == 0 && state.Ledger.ClosedBy == "") {
		return false, fmt.Errorf("fencing: producer cohort is not frozen")
	}
	for _, nodeID := range nodes {
		if _, ok := state.Ledger.ClosedNodes[nodeID]; !ok {
			return false, fmt.Errorf("fencing: voter producer cohort is not frozen")
		}
	}
	complete := true
	for _, run := range state.Ledger.Runs {
		if len(nodes) != 0 && !slices.Contains(nodes, run.Identity.NodeID) {
			continue
		}
		if run.Runtime == nil || run.Stop == nil {
			return false, fmt.Errorf("fencing: producer cohort lacks runtime enrollment or closure")
		}
		producer, err := e.reserveMutationProducer(ctx, *run.Runtime)
		if err != nil {
			return false, err
		}
		if producer.Phase != fencing.MutationClosed || !producer.BarrierComplete() {
			complete = false
		}
	}
	return complete, nil
}
