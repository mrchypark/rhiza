package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type sourceMutationRequest struct {
	ProducerUID string                 `json:"producerUID"`
	Sequence    uint64                 `json:"sequence"`
	Intent      fencing.MutationIntent `json:"intent"`
}

func (e *FencingExecutor) handleSourceMutation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || (r.URL.Path != "/v1/source-mutations/admit" && r.URL.Path != "/v1/source-mutations/execute") || r.URL.RawQuery != "" {
		w.Header().Set("Allow", "POST")
		http.NotFound(w, r)
		return
	}
	if e.SourceMutationStorage == nil {
		http.Error(w, "source mutation execution is not configured", http.StatusServiceUnavailable)
		return
	}
	var request struct {
		Identity    fencing.Identity       `json:"identity"`
		ProducerUID string                 `json:"producerUID"`
		Sequence    uint64                 `json:"sequence"`
		Intent      fencing.MutationIntent `json:"intent"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid source mutation request", http.StatusBadRequest)
		return
	}
	if err := e.executeSourceMutationRequest(r.Context(), r.Header.Get("Authorization"), request.Identity, sourceMutationRequest{ProducerUID: request.ProducerUID, Sequence: request.Sequence, Intent: request.Intent}, r.URL.Path == "/v1/source-mutations/admit"); err != nil {
		e.writeSourceStageError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte("{}\n"))
}

func (e *FencingExecutor) executeSourceMutationRequest(ctx context.Context, authorization string, id fencing.Identity, request sourceMutationRequest, admit bool) error {
	if request.ProducerUID == "" || request.Sequence == 0 || request.Intent.OperationID != fencing.MutationRequestID(request.ProducerUID, request.Sequence) {
		return fmt.Errorf("fencing: mutation request tuple is incomplete")
	}
	uid, err := e.authenticate(ctx, authorization, id)
	if err != nil {
		return sourceStageAuthenticationError{err: err}
	}
	runtime, scopeHash, err := e.sourceMutationProducer(ctx, uid, id, admit)
	if err != nil {
		return err
	}
	scope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil || hashJSON(scope) != scopeHash || request.Intent.StoreScopeHash != scopeHash {
		return fmt.Errorf("fencing: source mutation storage scope differs")
	}
	if admit {
		if request.Intent.Kind == "upload" {
			stages, err := e.sourceStagesFor(ctx, runtime)
			if err != nil {
				return err
			}
			payload, err := stages.openMutationPayloadContext(ctx, request.ProducerUID, request.Intent)
			if err != nil {
				return err
			}
			if err := payload.Close(); err != nil {
				return err
			}
		}
		if err := e.compactSourceMutationJournal(ctx, runtime, request.Sequence); err != nil {
			return err
		}
		if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
			if p.WALIdentity != id.WALIdentity {
				return fmt.Errorf("fencing: mutation producer WAL differs")
			}
			return p.Admit(request.Sequence, request.Intent)
		}); err != nil {
			return err
		}
	}
	provider, err := newGCSMutationProvider(e.SourceMutationStorage, scope)
	if err != nil {
		return err
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return err
	}
	err = e.ExecuteSourceMutation(ctx, runtime, request.ProducerUID, request.Sequence, request.Intent, scope, provider, receipts)
	if err != nil {
		return err
	}
	return nil
}

// sourceMutationProducer permits replay of an exact admitted tuple after root
// closure, but new admission still requires an open producer and run.
func (e *FencingExecutor) sourceMutationProducer(ctx context.Context, uid string, id fencing.Identity, requireOpen bool) (fencing.RuntimeBinding, string, error) {
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.RuntimeBinding{}, "", err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Runtime == nil || run.Runtime.StatefulSetUID != uid || run.MutationProducerUID == "" ||
		run.MutationScopeHash == "" || run.MutationProfile != fencing.MutationExecutionProfile || run.MutationStageIncarnation == "" {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: source mutation is outside enrolled producer")
	}
	if err := run.Runtime.Validate(id); err != nil {
		return fencing.RuntimeBinding{}, "", err
	}
	stages, err := e.sourceStagesFor(ctx, *run.Runtime)
	if err != nil {
		return fencing.RuntimeBinding{}, "", err
	}
	if run.MutationStageIncarnation != stages.incarnation {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: source mutation stage identity differs")
	}
	if e.AuthorityState == nil {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: canonical mutation producer state is not configured")
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
	if err != nil {
		return fencing.RuntimeBinding{}, "", err
	}
	if producerUID != run.MutationProducerUID || producer.Runtime != *run.Runtime || producer.WALIdentity != id.WALIdentity || producer.EvidenceFault != "" || producer.Validate() != nil {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("fencing: canonical mutation producer identity or state differs")
	}
	_, closedVoter := state.Ledger.ClosedNodes[id.NodeID]
	if requireOpen && (run.Stop != nil || closedVoter || state.Ledger.ClosedBy != "" || producer.Phase != fencing.MutationOpen) {
		return fencing.RuntimeBinding{}, "", fencing.ErrClosed
	}
	return *run.Runtime, run.MutationScopeHash, nil
}

func (e *FencingExecutor) sourceMutationStorageScope(ctx context.Context, runtime fencing.RuntimeBinding) (AttestationStoreScope, error) {
	var workloads struct {
		Items []object `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+e.Kube.Namespace+"/statefulsets", &workloads); err != nil {
		return AttestationStoreScope{}, err
	}
	for _, workload := range workloads.Items {
		if str(nested(workload, "metadata", "uid")) == runtime.StatefulSetUID {
			return e.storageScope(ctx, workload, runtime.SourceClusterID)
		}
	}
	return AttestationStoreScope{}, fmt.Errorf("fencing: enrolled source StatefulSet is unavailable")
}

var _ mutationExecutionProvider = (*gcsMutationProvider)(nil)
