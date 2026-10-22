package operator

import (
	"bytes"
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
	if e.SourceStages == nil {
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
	producerUID, scope, err := e.sourceStageProducer(r, request.Identity)
	if err == nil && request.Draft.StoreScopeHash != scope {
		err = fmt.Errorf("fencing: source stage scope differs from enrolled workload")
	}
	if err == nil {
		err = e.SourceStages.Reserve(producerUID, request.Ticket, request.Draft)
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
	producerUID, _, err := e.sourceStageProducer(r, id)
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
	descriptor, err := e.SourceStages.Seal(producerUID, ticket, query.Get("digest"), size, input)
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
	producerUID, _, err := e.sourceStageProducer(r, id)
	if err == nil {
		err = e.SourceStages.Cancel(producerUID, ticket)
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

func (e *FencingExecutor) sourceStageProducer(r *http.Request, id fencing.Identity) (string, string, error) {
	uid, err := e.authenticate(r.Context(), r.Header.Get("Authorization"), id)
	if err != nil {
		return "", "", sourceStageAuthenticationError{err: err}
	}
	state, err := e.read(r.Context(), uid, id.SourceClusterID)
	if err != nil {
		return "", "", err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	_, closed := state.Ledger.ClosedNodes[id.NodeID]
	if !ok || run.Identity != id || run.Stop != nil || closed || state.Ledger.ClosedBy != "" || !state.Ledger.RequireMutationJournal || run.Runtime == nil || run.Runtime.StatefulSetUID != uid || run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile != fencing.MutationExecutionProfile {
		return "", "", fmt.Errorf("fencing: source stage is outside the active enrolled producer")
	}
	if err := run.Runtime.Validate(id); err != nil {
		return "", "", err
	}
	var resource object
	if err := e.Kube.Get(r.Context(), e.mutationProducerPath(*run.Runtime), &resource); err != nil {
		return "", "", err
	}
	if nested(resource, "metadata", "deletionTimestamp") != nil || str(nested(resource, "metadata", "uid")) != run.MutationProducerUID || str(nested(resource, "metadata", "resourceVersion")) == "" {
		return "", "", fmt.Errorf("fencing: source stage producer storage identity differs")
	}
	var producer fencing.MutationProducer
	d := json.NewDecoder(strings.NewReader(str(nested(resource, "data", "producer.json"))))
	d.DisallowUnknownFields()
	if err := d.Decode(&producer); err != nil || d.Decode(new(any)) != io.EOF || producer.Runtime != *run.Runtime || producer.WALIdentity != id.WALIdentity || producer.Phase != fencing.MutationOpen || producer.Validate() != nil {
		return "", "", fmt.Errorf("fencing: source stage producer is not open and bound")
	}
	return run.MutationProducerUID, run.MutationScopeHash, nil
}

func (e *FencingExecutor) writeSourceStageError(w http.ResponseWriter, err error) {
	if err == nil {
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
