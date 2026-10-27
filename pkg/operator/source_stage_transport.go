package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

const sourceStageIdentityHeader = "X-Rhiza-Fencing-Identity"

type sourceStageAuthenticationError struct{ err error }

func (e sourceStageAuthenticationError) Error() string { return e.err.Error() }
func (e sourceStageAuthenticationError) Unwrap() error { return e.err }

type sourceStageReserveRequest struct {
	Identity fencing.Identity       `json:"identity"`
	Ticket   string                 `json:"ticket"`
	Draft    fencing.MutationIntent `json:"draft"`
}

// handleSourceStage exposes only bounded staging operations. Authentication
// and producer scope are checked before either ticket metadata or payload bytes
// can be changed.
func (e *FencingExecutor) handleSourceStage(w http.ResponseWriter, r *http.Request) {
	if e == nil || e.AuthorityState == nil && e.SourceStages == nil {
		http.Error(w, "source staging is not configured", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/source-stages/reserve" {
		if r.URL.RawQuery != "" {
			http.Error(w, "unexpected source stage reservation query", http.StatusBadRequest)
			return
		}
		e.handleSourceStageReserve(w, r)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/source-stages/") {
		http.NotFound(w, r)
		return
	}
	ticket := strings.TrimPrefix(r.URL.Path, "/v1/source-stages/")
	if !validStageTicket(ticket) || strings.Contains(ticket, "/") {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPost:
		e.handleSourceStageSeal(w, r, ticket)
	case http.MethodDelete:
		if r.URL.RawQuery != "" {
			http.Error(w, "unexpected source stage cancellation query", http.StatusBadRequest)
			return
		}
		e.handleSourceStageCancel(w, r, ticket)
	default:
		w.Header().Set("Allow", "POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (e *FencingExecutor) handleSourceStageReserve(w http.ResponseWriter, r *http.Request) {
	var request sourceStageReserveRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	d.DisallowUnknownFields()
	if err := d.Decode(&request); err != nil || d.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid source stage reservation", http.StatusBadRequest)
		return
	}
	producerUID, scope, stages, runtime, logical, err := e.sourceStageProducer(r, request.Identity)
	if err == nil && request.Draft.StoreScopeHash != scope {
		err = fmt.Errorf("fencing: source stage scope differs from enrolled workload")
	}
	if err == nil {
		if logical {
			err = stages.ReserveContext(r.Context(), producerUID, request.Ticket, request.Draft)
		} else {
			err = e.reserveSourceStage(r.Context(), runtime, stages, producerUID, request.Ticket, request.Draft)
		}
	}
	if err != nil {
		e.writeSourceStageError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte("{}\n"))
}

func (e *FencingExecutor) handleSourceStageSeal(w http.ResponseWriter, r *http.Request, ticket string) {
	if r.Header.Get("Content-Type") != "application/octet-stream" || r.Header.Get("Content-Encoding") != "" {
		http.Error(w, "source stage requires an unencoded byte stream", http.StatusUnsupportedMediaType)
		return
	}
	id, err := sourceStageIdentity(r)
	if err != nil {
		http.Error(w, "invalid source stage identity", http.StatusBadRequest)
		return
	}
	producerUID, _, stages, _, _, err := e.sourceStageProducer(r, id)
	if err != nil {
		e.writeSourceStageError(w, err)
		return
	}
	query := r.URL.Query()
	if len(query) != 2 || len(query["digest"]) != 1 || len(query["size"]) != 1 {
		http.Error(w, "invalid source stage descriptor", http.StatusBadRequest)
		return
	}
	size, err := strconv.ParseInt(query.Get("size"), 10, 64)
	if err != nil || size < 0 || size > maxSourceStageBytes {
		http.Error(w, "invalid source stage size", http.StatusBadRequest)
		return
	}
	input := http.MaxBytesReader(w, r.Body, size+1)
	descriptor, err := stages.SealContext(r.Context(), producerUID, ticket, query.Get("digest"), size, input)
	if err != nil {
		e.writeSourceStageError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(descriptor); err != nil {
		return
	}
}

func (e *FencingExecutor) handleSourceStageCancel(w http.ResponseWriter, r *http.Request, ticket string) {
	id, err := sourceStageIdentity(r)
	if err != nil {
		http.Error(w, "invalid source stage identity", http.StatusBadRequest)
		return
	}
	producerUID, _, stages, _, _, err := e.sourceStageProducer(r, id)
	if err == nil {
		err = stages.CancelContext(r.Context(), producerUID, ticket)
	}
	if err != nil {
		e.writeSourceStageError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func sourceStageIdentity(r *http.Request) (fencing.Identity, error) {
	encoded := r.Header.Get(sourceStageIdentityHeader)
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || encoded == "" {
		return fencing.Identity{}, fmt.Errorf("fencing: source stage identity header missing")
	}
	var id fencing.Identity
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&id); err != nil || d.Decode(new(any)) != io.EOF {
		return fencing.Identity{}, fmt.Errorf("fencing: source stage identity malformed")
	}
	return id, nil
}

func (e *FencingExecutor) sourceStageProducer(r *http.Request, id fencing.Identity) (string, string, *SourceStageStore, fencing.RuntimeBinding, bool, error) {
	uid, err := e.authenticate(r.Context(), r.Header.Get("Authorization"), id)
	if err != nil {
		return "", "", nil, fencing.RuntimeBinding{}, false, sourceStageAuthenticationError{err: err}
	}
	state, err := e.read(r.Context(), uid, id.SourceClusterID)
	if err != nil {
		return "", "", nil, fencing.RuntimeBinding{}, false, err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	_, closed := state.Ledger.ClosedNodes[id.NodeID]
	if state.Ledger.Profile == fencing.LogicalWriterProfileV1 {
		if err := e.validateMutationJournalProfile(state); err != nil {
			return "", "", nil, fencing.RuntimeBinding{}, true, err
		}
		if !ok || fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Identity != id || run.Stop != nil || closed || state.Ledger.ClosedBy != "" ||
			run.Runtime != nil || run.LogicalBinding == nil || run.LogicalBinding.ValidateBound() != nil || !run.LogicalBinding.MatchesIdentity(id) ||
			run.MutationProducerUID == "" || run.MutationScopeHash == "" || !fencing.MutationProfileSupported(run.MutationProfile) {
			return "", "", nil, fencing.RuntimeBinding{}, true, fmt.Errorf("fencing: source stage is outside the active logical producer")
		}
		stages, err := e.sourceStagesForRoot(r.Context(), uid, id.SourceClusterID)
		if err != nil || (run.MutationStageIncarnation != "" && run.MutationStageIncarnation != stages.incarnation) {
			return "", "", nil, fencing.RuntimeBinding{}, true, errors.Join(err, fmt.Errorf("fencing: bound logical source stage store is unavailable or differs"))
		}
		producer, producerUID, _, err := e.AuthorityState.ReadLogicalProducer(r.Context(), uid, *run.LogicalBinding)
		if err != nil {
			return "", "", nil, fencing.RuntimeBinding{}, true, err
		}
		if producerUID != run.MutationProducerUID || producer.Profile != fencing.LogicalWriterProfileV1 || producer.WALIdentity != id.WALIdentity || producer.Phase != fencing.MutationOpen || producer.Validate() != nil {
			return "", "", nil, fencing.RuntimeBinding{}, true, fmt.Errorf("fencing: logical source stage producer is not open and bound")
		}
		return producerUID, run.MutationScopeHash, stages, fencing.RuntimeBinding{}, true, nil
	}
	if !ok || fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Identity != id || run.Stop != nil || closed || state.Ledger.ClosedBy != "" || !state.Ledger.RequireMutationJournal || run.Runtime == nil || run.Runtime.StatefulSetUID != uid || run.MutationProducerUID == "" || run.MutationScopeHash == "" || !fencing.MutationProfileSupported(run.MutationProfile) {
		return "", "", nil, fencing.RuntimeBinding{}, false, fmt.Errorf("fencing: source stage is outside the active enrolled producer")
	}
	if err := run.Runtime.Validate(id); err != nil {
		return "", "", nil, fencing.RuntimeBinding{}, false, err
	}
	stages, err := e.sourceStagesFor(r.Context(), *run.Runtime)
	if err != nil || run.MutationStageIncarnation == "" || run.MutationStageIncarnation != stages.incarnation {
		return "", "", nil, fencing.RuntimeBinding{}, false, errors.Join(err, fmt.Errorf("fencing: bound source stage store is unavailable or differs"))
	}
	var producer fencing.MutationProducer
	var producerUID string
	if e.AuthorityState != nil {
		producer, producerUID, _, err = e.AuthorityState.ReadProducer(r.Context(), *run.Runtime)
		if err != nil {
			return "", "", nil, fencing.RuntimeBinding{}, false, err
		}
	} else {
		var resource object
		if err := e.Kube.Get(r.Context(), e.mutationProducerPath(*run.Runtime), &resource); err != nil {
			return "", "", nil, fencing.RuntimeBinding{}, false, err
		}
		if nested(resource, "metadata", "deletionTimestamp") != nil || str(nested(resource, "metadata", "resourceVersion")) == "" {
			return "", "", nil, fencing.RuntimeBinding{}, false, fmt.Errorf("fencing: source stage producer storage identity differs")
		}
		producerUID = str(nested(resource, "metadata", "uid"))
		d := json.NewDecoder(strings.NewReader(str(nested(resource, "data", "producer.json"))))
		d.DisallowUnknownFields()
		if err := d.Decode(&producer); err != nil || d.Decode(new(any)) != io.EOF {
			return "", "", nil, fencing.RuntimeBinding{}, false, fmt.Errorf("fencing: source stage producer record is invalid")
		}
	}
	if producerUID != run.MutationProducerUID || producer.Runtime != *run.Runtime || producer.WALIdentity != id.WALIdentity || producer.Phase != fencing.MutationOpen || producer.Validate() != nil {
		return "", "", nil, fencing.RuntimeBinding{}, false, fmt.Errorf("fencing: source stage producer is not open and bound")
	}
	return run.MutationProducerUID, run.MutationScopeHash, stages, *run.Runtime, false, nil
}

func (e *FencingExecutor) reserveSourceStage(ctx context.Context, runtime fencing.RuntimeBinding, stages *SourceStageStore, producerUID, ticket string, draft fencing.MutationIntent) error {
	if e != nil && e.AuthorityState != nil {
		if err := e.ReconcileArchivedStages(ctx, runtime); err != nil {
			return err
		}
	}
	return stages.ReserveContext(ctx, producerUID, ticket, draft)
}

func (e *FencingExecutor) writeSourceStageError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	if fencing.IsMutationPreDispatchContention(err) {
		w.Header().Set(fencing.MutationPendingReasonHeader, fencing.MutationPreDispatchContentionReason)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if errors.Is(err, fencing.ErrPending) || errors.Is(err, ErrFencePending) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if errors.Is(err, fencing.ErrClosed) {
		w.WriteHeader(http.StatusGone)
		return
	}
	if errors.Is(err, fencing.ErrMutationConditionFailed) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if errors.Is(err, errLogicalMutationExecutionUnavailable) {
		http.Error(w, "logical source mutation execution is not configured", http.StatusServiceUnavailable)
		return
	}
	if transientFenceAuthority(err) {
		http.Error(w, "source staging authority unavailable", http.StatusServiceUnavailable)
		return
	}
	var authErr sourceStageAuthenticationError
	if errors.As(err, &authErr) {
		http.Error(w, "supervisor identity rejected", http.StatusForbidden)
		return
	}
	http.Error(w, "source stage rejected", http.StatusConflict)
}
