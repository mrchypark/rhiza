package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

// V1Alpha2WriteRequest is the stable identity of one protected mutation.
type V1Alpha2WriteRequest struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

type v1alpha2PendingRequest struct {
	V1Alpha2WriteRequest
	PriorEvidence json.RawMessage `json:"priorEvidence"`
	OriginalSQL   json.RawMessage `json:"originalSQL,omitempty"`
}

// V1Alpha2AdmissionSnapshot contains one observed authority, not future permission.
type V1Alpha2AdmissionSnapshot struct {
	status     V1Alpha2StatusSnapshot
	evidence   json.RawMessage
	pending    *v1alpha2PendingRequest
	transition json.RawMessage
}

func (s V1Alpha2AdmissionSnapshot) Authority() V1Alpha2AuthorityView { return s.status.Authority() }
func (s V1Alpha2AdmissionSnapshot) Evidence() json.RawMessage        { return bytes.Clone(s.evidence) }
func (s V1Alpha2AdmissionSnapshot) Pending() (V1Alpha2WriteRequest, bool) {
	if s.pending == nil {
		return V1Alpha2WriteRequest{}, false
	}
	return s.pending.V1Alpha2WriteRequest, true
}
func (s V1Alpha2AdmissionSnapshot) Transition() json.RawMessage { return bytes.Clone(s.transition) }

// PendingSQL returns the retained original SQL request; old pending requests may
// have no payload and need the original client to resubmit it.
func (s V1Alpha2AdmissionSnapshot) PendingSQL() (network.ExecuteRequest, bool, error) {
	if s.pending == nil || len(s.pending.OriginalSQL) == 0 {
		return network.ExecuteRequest{}, false, nil
	}
	r, err := decodePendingSQL(s.pending.OriginalSQL, s.pending.V1Alpha2WriteRequest)
	return r, err == nil, err
}

func sqlRequestFingerprint(req network.ExecuteRequest) ([32]byte, error) {
	return types.SQLFingerprint(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements})
}

func decodePendingSQL(raw json.RawMessage, request V1Alpha2WriteRequest) (network.ExecuteRequest, error) {
	var r network.ExecuteRequest
	if len(raw) == 0 || len(raw) > 32<<10 {
		return r, fmt.Errorf("invalid pending SQL size")
	}
	commands, ok, err := types.DecodeSQLBatch(types.AssembleSQLBatch([][]byte{raw}))
	if err != nil || !ok || len(commands) != 1 || commands[0].Migration != nil {
		return r, fmt.Errorf("invalid pending SQL command")
	}
	command := commands[0]
	canonical, err := types.EncodeSQLBatchItem(command)
	if err != nil || !sameJSON(canonical, raw) {
		return r, fmt.Errorf("noncanonical pending SQL command")
	}
	r = network.ExecuteRequest{RequestID: command.RequestID, SQL: command.SQL, Args: command.Args, WantRows: command.WantRows, RequireOne: command.RequireOne, Statements: command.Statements}
	if err := network.ValidateExecuteRequest(r); err != nil {
		return r, err
	}
	f, err := sqlRequestFingerprint(r)
	if err != nil || request.Kind != "sql" || request.ID != r.RequestID || request.Fingerprint != fmt.Sprintf("%x", f) {
		return r, fmt.Errorf("pending SQL fingerprint mismatch")
	}
	return r, nil
}

// LoadV1Alpha2Admission allows inspection of pending state for recovery. It
// never turns missing evidence into a fresh authority.
func LoadV1Alpha2Admission(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef) (V1Alpha2AdmissionSnapshot, error) {
	if store == nil {
		return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("missing authority store")
	}
	s, err := store.Load(ctx, ref)
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, err
	}
	return parseV1Alpha2Admission(s)
}

func validV1Alpha2WriteRequest(r V1Alpha2WriteRequest) bool {
	return len(r.Kind) <= 128 && len(r.ID) <= 256 && len(r.Fingerprint) <= 256 && v1alpha2NonBlank.MatchString(r.Kind) && v1alpha2NonBlank.MatchString(r.ID) && v1alpha2NonBlank.MatchString(r.Fingerprint)
}

func parseV1Alpha2Admission(s V1Alpha2StatusSnapshot) (V1Alpha2AdmissionSnapshot, error) {
	status, err := jsonObject(s.object["status"])
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, err
	}
	for _, key := range []string{"pendingWrite", "pendingRequest", "transition", "evidence"} {
		if _, exists := status[key]; exists {
			return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("ambiguous top-level %s", key)
		}
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, err
	}
	evidence := auth["evidence"]
	if len(evidence) == 0 || bytes.Equal(bytes.TrimSpace(evidence), []byte("null")) || !json.Valid(evidence) {
		return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("missing application evidence")
	}
	var pending bool
	if len(auth["pendingWrite"]) == 0 || bytes.Equal(bytes.TrimSpace(auth["pendingWrite"]), []byte("null")) || json.Unmarshal(auth["pendingWrite"], &pending) != nil {
		return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("invalid pending write")
	}
	var request *v1alpha2PendingRequest
	if raw, ok := auth["pendingRequest"]; ok {
		var r v1alpha2PendingRequest
		if !pending || decodeV1Alpha2(raw, &r) != nil || !validV1Alpha2WriteRequest(r.V1Alpha2WriteRequest) || !sameJSON(r.PriorEvidence, evidence) {
			return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("invalid pending request")
		}
		if len(r.OriginalSQL) > 0 {
			if _, err := decodePendingSQL(r.OriginalSQL, r.V1Alpha2WriteRequest); err != nil {
				return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("invalid pending SQL: %w", err)
			}
		}
		request = &r
	} else if pending {
		return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("pending request identity missing")
	}
	transition := auth["transition"]
	if len(transition) > 0 && !bytes.Equal(bytes.TrimSpace(transition), []byte("null")) {
		if _, err := jsonObject(transition); err != nil {
			return V1Alpha2AdmissionSnapshot{}, fmt.Errorf("invalid transition")
		}
	}
	return V1Alpha2AdmissionSnapshot{status: s, evidence: bytes.Clone(evidence), pending: request, transition: bytes.Clone(transition)}, nil
}

// ReserveV1Alpha2SQLWrite retains the bounded original SQL request so a host
// can resolve the exact pending operation after restart.
func ReserveV1Alpha2SQLWrite(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, evidence json.RawMessage, req network.ExecuteRequest) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if err := network.ValidateExecuteRequest(req); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	fingerprint, err := sqlRequestFingerprint(req)
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	request := V1Alpha2WriteRequest{Kind: "sql", ID: req.RequestID, Fingerprint: fmt.Sprintf("%x", fingerprint)}
	raw, err := types.EncodeSQLBatchItem(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements})
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if _, err := decodePendingSQL(raw, request); err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	if old.status.store != store || old.Authority() != expected || old.pending != nil || hasV1Alpha2Transition(old.transition) || !sameJSON(old.evidence, evidence) || !validV1Alpha2WriteRequest(request) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("write reservation preconditions do not match")
	}
	return updateV1Alpha2Admission(ctx, store, old, func(auth map[string]json.RawMessage) {
		auth["pendingWrite"] = json.RawMessage("true")
		auth["pendingRequest"], _ = json.Marshal(v1alpha2PendingRequest{V1Alpha2WriteRequest: request, PriorEvidence: bytes.Clone(old.evidence), OriginalSQL: raw})
	})
}

type V1Alpha2WriteOutcome string

const (
	V1Alpha2WriteCommitted V1Alpha2WriteOutcome = "Committed"
	V1Alpha2WriteNoEffect  V1Alpha2WriteOutcome = "ProvenNoEffect"
)

// V1Alpha2CASObservation distinguishes a direct response from a diagnostic
// matching readback; matching readback does not establish writer ownership.
type V1Alpha2CASObservation string

const (
	V1Alpha2CASApplied          V1Alpha2CASObservation = "Applied"
	V1Alpha2CASMatchingReadback V1Alpha2CASObservation = "MatchingReadbackUnknown"
	V1Alpha2CASUnknown          V1Alpha2CASObservation = "Unknown"
)

func ReserveV1Alpha2Write(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, evidence json.RawMessage, request V1Alpha2WriteRequest) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.status.store != store || old.Authority() != expected || old.pending != nil || hasV1Alpha2Transition(old.transition) || !sameJSON(old.evidence, evidence) || !validV1Alpha2WriteRequest(request) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("write reservation preconditions do not match")
	}
	return updateV1Alpha2Admission(ctx, store, old, func(auth map[string]json.RawMessage) {
		auth["pendingWrite"] = json.RawMessage("true")
		auth["pendingRequest"], _ = json.Marshal(v1alpha2PendingRequest{V1Alpha2WriteRequest: request, PriorEvidence: bytes.Clone(old.evidence)})
	})
}

// FinalizeV1Alpha2Write requires the host to establish the actual outcome and
// evidence. Unknown or not-found outcomes cannot be passed as no-effect proof.
func FinalizeV1Alpha2Write(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, expected V1Alpha2AuthorityView, request V1Alpha2WriteRequest, outcome V1Alpha2WriteOutcome, evidence json.RawMessage) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if old.status.store != store || old.Authority() != expected || old.pending == nil || old.pending.V1Alpha2WriteRequest != request || hasV1Alpha2Transition(old.transition) || (outcome != V1Alpha2WriteCommitted && outcome != V1Alpha2WriteNoEffect) || len(evidence) == 0 || bytes.Equal(bytes.TrimSpace(evidence), []byte("null")) || !json.Valid(evidence) || (outcome == V1Alpha2WriteNoEffect && !sameJSON(evidence, old.evidence)) {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("write finalization preconditions do not match")
	}
	return updateV1Alpha2Admission(ctx, store, old, func(auth map[string]json.RawMessage) {
		auth["pendingWrite"] = json.RawMessage("false")
		auth["evidence"] = bytes.Clone(evidence)
		delete(auth, "pendingRequest")
	})
}

func hasV1Alpha2Transition(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func updateV1Alpha2Admission(ctx context.Context, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, change func(map[string]json.RawMessage)) (V1Alpha2AdmissionSnapshot, V1Alpha2CASObservation, error) {
	if store == nil || old.status.store != store || old.status.object == nil || old.status.resourceVersion == "" {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("invalid admission snapshot")
	}
	p, err := store.path(old.status.ref)
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	status, err := jsonObject(old.status.object["status"])
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	change(auth)
	status["authority"], err = json.Marshal(auth)
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	statusRaw, err := json.Marshal(status)
	if err != nil {
		return V1Alpha2AdmissionSnapshot{}, "", err
	}
	obj := make(map[string]json.RawMessage, len(old.status.object))
	for k, v := range old.status.object {
		obj[k] = v
	}
	obj["status"] = statusRaw
	var response map[string]json.RawMessage
	err = store.Kube.Put(ctx, p+"/status", obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return V1Alpha2AdmissionSnapshot{}, "", fmt.Errorf("%w: %v", ErrV1Alpha2StatusConflict, err)
	}
	if err == nil {
		snap, e := store.snapshot(old.status.ref, response)
		if e == nil && snap.resourceVersion != old.status.resourceVersion && sameJSON(snap.object["status"], statusRaw) {
			result, e := parseV1Alpha2Admission(snap)
			if e == nil {
				return result, V1Alpha2CASApplied, nil
			}
		}
	}
	live, readErr := store.Load(ctx, old.status.ref)
	if readErr == nil && live.resourceVersion != old.status.resourceVersion && sameJSON(live.object["status"], statusRaw) {
		result, e := parseV1Alpha2Admission(live)
		if e == nil {
			return result, V1Alpha2CASMatchingReadback, fmt.Errorf("%w: matching readback does not establish ownership", ErrV1Alpha2StatusOutcomeUnknown)
		}
	}
	return V1Alpha2AdmissionSnapshot{}, V1Alpha2CASUnknown, fmt.Errorf("%w: admission update outcome unknown", ErrV1Alpha2StatusOutcomeUnknown)
}
