package fencing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// sourceRelayAuthority is the bounded source-mutation surface needed by the
// local supervisor. The remote AuthorityClient is already bound by Supervisor
// before the child can bind its WAL.
type sourceRelayAuthority interface {
	ReserveUpload(context.Context, string, MutationIntent) error
	SealUpload(context.Context, string, []byte) (sourceSealedPayload, error)
	Admit(context.Context, uint64, MutationIntent) error
	LookupMutation(context.Context, Identity, string, uint64, MutationIntent) (MutationLookupResult, error)
	Execute(context.Context, uint64, MutationIntent) error
	ArchiveGenesisStatus(context.Context, string, uint) (ArchiveGenesisStatus, error)
}

type sourceRelayRequest struct {
	Identity       Identity                    `json:"identity"`
	Ticket         string                      `json:"ticket,omitempty"`
	ProducerUID    string                      `json:"producerUID,omitempty"`
	Sequence       uint64                      `json:"sequence,omitempty"`
	Intent         MutationIntent              `json:"intent,omitempty"`
	ArchiveGenesis ArchiveGenesisStatusRequest `json:"archiveGenesis,omitempty"`
}

const sourceStageIdentityHeader = "X-Rhiza-Fencing-Identity"

func validStageTicket(value string) bool {
	return len(value) == sha256.Size*2 && mutationDigestValid(value) && strings.ToLower(value) == value
}

func sourceRelayPath(path string) bool {
	return strings.HasPrefix(path, "/v1/source-stages/") || strings.HasPrefix(path, "/v1/source-mutations/")
}

// sourceRelay handles the local Unix-socket hop. The supervisor ticket is
// never forwarded: the remote authority client authenticates with its own
// projected token and only the bound identity is relayed.
func (s *Supervisor) sourceRelay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || (r.URL.RawQuery != "" && !strings.HasPrefix(r.URL.Path, "/v1/source-stages/")) {
		http.Error(w, "invalid source mutation request", http.StatusBadRequest)
		return
	}
	authority, ok := s.Authority.(sourceRelayAuthority)
	if !ok {
		http.Error(w, "source mutation relay is unavailable", http.StatusServiceUnavailable)
		return
	}
	s.mu.Lock()
	identity, bound := s.bound, s.binding != nil
	s.mu.Unlock()
	if !bound || identity.WALIdentity == "" {
		http.Error(w, "source mutation requires a bound producer", http.StatusConflict)
		return
	}
	var err error
	var result any
	switch {
	case r.URL.Path == "/v1/source-stages/reserve":
		var request sourceRelayRequest
		if err = decodeSourceRelayJSON(w, r, &request, 65536); err == nil && request.Identity != identity {
			err = fmt.Errorf("fencing: source identity differs from bound session")
		}
		if err == nil {
			err = authority.ReserveUpload(r.Context(), request.Ticket, request.Intent)
		}
	case strings.HasPrefix(r.URL.Path, "/v1/source-stages/") && r.URL.Path != "/v1/source-stages/reserve":
		ticket := strings.TrimPrefix(r.URL.Path, "/v1/source-stages/")
		query := r.URL.Query()
		if !validStageTicket(ticket) || strings.Contains(ticket, "/") || len(query) != 2 || len(query["digest"]) != 1 || len(query["size"]) != 1 || r.Header.Get("Content-Type") != "application/octet-stream" || r.Header.Get("Content-Encoding") != "" {
			err = fmt.Errorf("fencing: invalid source stage seal request")
			break
		}
		size, parseErr := strconv.ParseInt(query.Get("size"), 10, 64)
		if parseErr != nil || size < 0 || size > maxStagePayloadBytes {
			err = fmt.Errorf("fencing: invalid source stage size")
			break
		}
		payload, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, size+1))
		if readErr != nil || int64(len(payload)) != size {
			err = fmt.Errorf("fencing: source stage payload size differs")
			break
		}
		var sealed sourceSealedPayload
		sealed, err = authority.SealUpload(r.Context(), ticket, payload)
		result = sealed
	case strings.HasPrefix(r.URL.Path, "/v1/source-mutations/"):
		var request sourceRelayRequest
		if err = decodeSourceRelayJSON(w, r, &request, 65536); err == nil && request.Identity != identity {
			err = fmt.Errorf("fencing: source identity differs from bound session")
		}
		if err != nil {
			break
		}
		switch r.URL.Path {
		case "/v1/source-mutations/admit":
			err = authority.Admit(r.Context(), request.Sequence, request.Intent)
		case "/v1/source-mutations/execute":
			err = authority.Execute(r.Context(), request.Sequence, request.Intent)
		case "/v1/source-mutations/lookup":
			if request.ProducerUID == "" {
				err = fmt.Errorf("fencing: source mutation producer is missing")
				break
			}
			var lookup MutationLookupResult
			lookup, err = authority.LookupMutation(r.Context(), identity, request.ProducerUID, request.Sequence, request.Intent)
			result = lookup
		case "/v1/source-mutations/genesis-status":
			if request.ArchiveGenesis.ArchiveKey != "archive/head.bin" || request.ArchiveGenesis.ConfigID == 0 || request.ProducerUID != "" || request.Sequence != 0 {
				err = fmt.Errorf("fencing: invalid archive genesis status request")
				break
			}
			var status ArchiveGenesisStatus
			status, err = authority.ArchiveGenesisStatus(r.Context(), request.ArchiveGenesis.ArchiveKey, request.ArchiveGenesis.ConfigID)
			result = status
		default:
			http.NotFound(w, r)
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeSourceRelayError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if result == nil {
		result = struct{}{}
	}
	_ = json.NewEncoder(w).Encode(result)
}

func decodeSourceRelayJSON(w http.ResponseWriter, r *http.Request, out any, limit int64) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing source relay request")
	}
	return nil
}

func writeSourceRelayError(w http.ResponseWriter, err error) {
	switch {
	case IsMutationPreDispatchContention(err):
		w.Header().Set(MutationPendingReasonHeader, MutationPreDispatchContentionReason)
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, ErrPending):
		w.WriteHeader(http.StatusAccepted)
	case err == ErrClosed:
		w.WriteHeader(http.StatusGone)
	case errors.Is(err, ErrMutationConditionFailed):
		w.WriteHeader(http.StatusPreconditionFailed)
	case errorsIsUnavailable(err):
		http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "source mutation rejected", http.StatusConflict)
	}
}

func errorsIsUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// Session methods implement sourceMutationAuthority over the authenticated
// local relay. A call failure is never converted into an affirmative outcome.
func (s *Session) ReserveUpload(ctx context.Context, ticket string, intent MutationIntent) error {
	id, _, err := s.sourceBinding()
	if err != nil {
		return err
	}
	return s.sourceJSON(ctx, "/v1/source-stages/reserve", sourceRelayRequest{Identity: id, Ticket: ticket, Intent: intent}, nil)
}

func (s *Session) SealUpload(ctx context.Context, ticket string, payload []byte) (sourceSealedPayload, error) {
	if !validStageTicket(ticket) || strings.Contains(ticket, "/") || int64(len(payload)) > maxStagePayloadBytes {
		return sourceSealedPayload{}, fmt.Errorf("fencing: invalid source stage payload")
	}
	_, _, err := s.sourceBinding()
	if err != nil {
		return sourceSealedPayload{}, err
	}
	digest := sha256.Sum256(payload)
	path := "/v1/source-stages/" + ticket + "?digest=" + hex.EncodeToString(digest[:]) + "&size=" + strconv.Itoa(len(payload))
	var sealed sourceSealedPayload
	if err := s.sourceBytes(ctx, path, payload, &sealed); err != nil {
		return sourceSealedPayload{}, err
	}
	if sealed.Ref != "payload-"+ticket || sealed.Size != int64(len(payload)) || sealed.Digest != hex.EncodeToString(digest[:]) {
		return sourceSealedPayload{}, fmt.Errorf("fencing: supervisor sealed different source bytes")
	}
	return sealed, nil
}

func (s *Session) Admit(ctx context.Context, sequence uint64, intent MutationIntent) error {
	id, producer, err := s.sourceBinding()
	if err != nil {
		return err
	}
	return s.sourceJSON(ctx, "/v1/source-mutations/admit", sourceRelayRequest{Identity: id, ProducerUID: producer, Sequence: sequence, Intent: intent}, nil)
}

func (s *Session) Lookup(ctx context.Context, sequence uint64, intent MutationIntent) (MutationLookupResult, error) {
	id, producer, err := s.sourceBinding()
	if err != nil {
		return MutationLookupResult{}, err
	}
	var result MutationLookupResult
	err = s.sourceJSON(ctx, "/v1/source-mutations/lookup", sourceRelayRequest{Identity: id, ProducerUID: producer, Sequence: sequence, Intent: intent}, &result)
	return result, err
}

func (s *Session) Execute(ctx context.Context, sequence uint64, intent MutationIntent) error {
	id, producer, err := s.sourceBinding()
	if err != nil {
		return err
	}
	return s.sourceJSON(ctx, "/v1/source-mutations/execute", sourceRelayRequest{Identity: id, ProducerUID: producer, Sequence: sequence, Intent: intent}, nil)
}

// ArchiveGenesisStatus uses the same authenticated local source relay and
// protected authority-client route as Upload/Execute; it is not a separate
// supervisor-management channel.
func (s *Session) ArchiveGenesisStatus(ctx context.Context, archiveKey string, configID uint) (ArchiveGenesisStatus, error) {
	id, _, err := s.sourceBinding()
	if err != nil {
		return ArchiveGenesisStatus{}, err
	}
	if archiveKey != "archive/head.bin" || configID == 0 {
		return ArchiveGenesisStatus{}, fmt.Errorf("fencing: invalid archive genesis key")
	}
	var result ArchiveGenesisStatus
	err = s.sourceJSON(ctx, "/v1/source-mutations/genesis-status", sourceRelayRequest{Identity: id, ArchiveGenesis: ArchiveGenesisStatusRequest{ArchiveKey: archiveKey, ConfigID: configID}}, &result)
	if err != nil {
		return ArchiveGenesisStatus{}, err
	}
	s.mu.Lock()
	var scopeHash string
	if s.binding != nil && s.binding.Producer != nil {
		scopeHash = s.binding.Producer.StoreScopeHash
	}
	s.mu.Unlock()
	if err := result.Validate(scopeHash, configID); err != nil {
		return ArchiveGenesisStatus{}, err
	}
	return result, nil
}

func (s *Session) sourceBinding() (Identity, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding == nil || s.binding.Producer == nil || s.identity.WALIdentity == "" {
		return Identity{}, "", fmt.Errorf("fencing: source mutation requires a bound producer")
	}
	return s.identity, s.binding.Producer.ProducerUID, nil
}

func (s *Session) sourceJSON(ctx context.Context, path string, value, out any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.sourceRequest(ctx, path, "application/json", bytes.NewReader(body), out)
}

func (s *Session) sourceBytes(ctx context.Context, path string, body []byte, out any) error {
	return s.sourceRequest(ctx, path, "application/octet-stream", bytes.NewReader(body), out)
}

func (s *Session) sourceRequest(ctx context.Context, path, contentType string, body io.Reader, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://supervisor"+path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.ticket)
	request.Header.Set("Content-Type", contentType)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusAccepted:
		return sourceMutationPendingResponse(response, path)
	case http.StatusGone:
		return ErrClosed
	case http.StatusPreconditionFailed:
		return ErrMutationConditionFailed
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: supervisor source HTTP %d", response.StatusCode)
	}
	if out == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(data) > 65536 {
		return fmt.Errorf("%w: supervisor source response unreadable", ErrUnavailable)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing supervisor source response")
	}
	return nil
}

var _ sourceMutationAuthority = (*Session)(nil)
