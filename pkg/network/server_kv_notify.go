package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "put")
}

type KVGetRequest struct{ Key, Consistency string }
type KVGetResponse struct {
	Found        bool   `json:"found"`
	Value        []byte `json:"value,omitempty"`
	AppliedSlot  uint64 `json:"applied_slot"`
	ConsensusTip uint64 `json:"consensus_tip"`
}
type KVMutationRequest struct {
	RequestID      string `json:"request_id"`
	Key            string `json:"key"`
	Value          []byte `json:"value,omitempty"`
	Expected       []byte `json:"expected,omitempty"`
	ExpectedExists bool   `json:"expected_exists,omitempty"`
	TTLMS          int64  `json:"ttl_ms,omitempty"`
}
type KVMutationResponse struct {
	types.MutationReceipt
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Key         string `json:"key"`
		Consistency string `json:"consistency,omitempty"`
	}
	if err := decodeJSON(w, r, &req); err != nil || req.Key == "" {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "key is required and the request must be valid JSON")
		return
	}
	release, err := s.acquireRead(r.Context(), false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	response, err := s.kvGet(r.Context(), KVGetRequest{Key: req.Key, Consistency: req.Consistency})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) KVGet(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		return KVGetResponse{}, err
	}
	defer release()
	return s.kvGet(ctx, req)
}

func (s *Server) kvGet(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	if req.Key == "" {
		return KVGetResponse{}, fmt.Errorf("%w: key is required", ErrInvalidRequest)
	}
	if err := s.readBarrier(ctx, req.Consistency); err != nil {
		return KVGetResponse{}, err
	}
	value, found, appliedSlot, err := s.material.KVGetAt(ctx, req.Key, time.Now())
	return KVGetResponse{Found: found, Value: value, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, err
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "delete")
}

func (s *Server) handleKVCAS(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "cas")
}

func (s *Server) handleKVMutation(w http.ResponseWriter, r *http.Request, operation string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req KVMutationRequest
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "invalid KV mutation request")
		return
	}
	response, err := s.KVMutate(r.Context(), operation, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) KVPut(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "put", req)
}
func (s *Server) KVDelete(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "delete", req)
}
func (s *Server) KVCAS(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "cas", req)
}

func (s *Server) KVMutate(ctx context.Context, operation string, req KVMutationRequest) (KVMutationResponse, error) {
	if !s.writable || !s.Ready() {
		return KVMutationResponse{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return KVMutationResponse{}, err
	}
	intent := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS}
	charge, err := mutationCharge(intent)
	if err != nil {
		return KVMutationResponse{}, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return KVMutationResponse{}, err
	}
	defer lease.releaseCaller()
	intent.RequestID, intent.Operation, intent.Key = cloneMutationString(intent.RequestID), cloneMutationString(intent.Operation), cloneMutationString(intent.Key)
	if managedKVExecutionPolicy(s.expectedPolicy) {
		intent.Value, intent.Expected = bytes.Clone(intent.Value), bytes.Clone(intent.Expected)
	} else {
		intent.Value, intent.Expected = append([]byte(nil), intent.Value...), append([]byte(nil), intent.Expected...)
	}
	req.RequestID, req.Key, req.Value, req.Expected = intent.RequestID, intent.Key, intent.Value, intent.Expected
	if operation != "put" && operation != "delete" && operation != "cas" {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	if req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	unlock, err := s.lockRequest(ctx, req.RequestID)
	if err != nil {
		return KVMutationResponse{}, err
	}
	defer unlock()
	var mixedFingerprint [32]byte
	if managedKVExecutionPolicy(s.expectedPolicy) {
		mixedFingerprint, err = types.ManagedKVFingerprint(intent)
		if err != nil {
			return KVMutationResponse{}, err
		}
		receipt, found, matches, err := s.material.KVRequestStatusFingerprint(ctx, req.RequestID, mixedFingerprint)
		if err != nil {
			return KVMutationResponse{}, err
		}
		if !matches {
			return KVMutationResponse{}, ErrRequestConflict
		}
		if found {
			if err := s.waitReceiptDurable(ctx, receipt, req.RequestID); err != nil {
				return KVMutationResponse{}, err
			}
			return KVMutationResponse{MutationReceipt: receipt}, nil
		}
	} else {
		receipt, found, matches, err := s.material.KVRequestStatus(ctx, intent)
		if err != nil {
			return KVMutationResponse{}, err
		}
		if !matches {
			return KVMutationResponse{}, ErrRequestConflict
		}
		if found {
			if err := s.waitReceiptDurable(ctx, receipt, req.RequestID); err != nil {
				return KVMutationResponse{}, err
			}
			return KVMutationResponse{MutationReceipt: receipt}, nil
		}
	}
	command := intent
	now := time.Now().UnixMilli()
	if req.TTLMS > math.MaxInt64-now {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	command.ObservedAtUnixMS = now
	if req.TTLMS > 0 {
		command.ExpiresAtUnixMS = now + req.TTLMS
	}
	submitSlot, err := s.kvBatcher.submit(ctx, command, lease)
	if err != nil {
		return KVMutationResponse{}, err
	}
	if managedKVExecutionPolicy(s.expectedPolicy) {
		receipt, found, matches, err := s.material.KVRequestStatusFingerprint(ctx, req.RequestID, mixedFingerprint)
		if err != nil {
			return KVMutationResponse{}, commitUnknown(submitSlot, req.RequestID, err)
		}
		if !found {
			return KVMutationResponse{}, commitUnknown(submitSlot, req.RequestID, fmt.Errorf("KV mutation receipt is unavailable"))
		}
		if !matches {
			return KVMutationResponse{}, ErrRequestConflict
		}
		return KVMutationResponse{MutationReceipt: receipt}, nil
	}
	if localtesthooks.Enabled {
		localtesthooks.Hit("network:kv-postsubmit:before-receipt")
	}
	receipt, found, matches, err := s.material.KVRequestStatus(ctx, intent)
	if err != nil {
		return KVMutationResponse{}, commitUnknown(submitSlot, req.RequestID, err)
	}
	if !matches {
		return KVMutationResponse{}, ErrRequestConflict
	}
	if !found {
		return KVMutationResponse{}, commitUnknown(submitSlot, req.RequestID, fmt.Errorf("KV mutation receipt is unavailable"))
	}
	return KVMutationResponse{MutationReceipt: receipt}, nil
}

var errConsistency = errors.New("consistency must be local or linearizable")

func (s *Server) readBarrier(ctx context.Context, consistency string) error {
	switch consistency {
	case "", "local":
		return nil
	case "linearizable":
		index, source, err := s.core.ReadIndex(ctx)
		if err != nil {
			return logManagedQueryFailure(ctx, "server_read_index", err)
		}
		if index > s.core.Tip() {
			if s.transport == nil || source == s.core.NodeID() {
				return logManagedQueryFailure(ctx, "server_catchup", fmt.Errorf("read-index source cannot supply slot %d", index))
			}
			if err := s.catchUpFrom(ctx, source, index, true); err != nil {
				return logManagedQueryFailure(ctx, "server_catchup", err)
			}
		}
		if s.localMode {
			if uint64(index) <= s.material.Tip() {
				return nil
			}
			if err := s.beginLocalLifecycle(); err != nil {
				return err
			}
			done := make(chan error, 1)
			go func() {
				applyCtx, cancel := context.WithTimeout(s.proposalCtx, 30*time.Second)
				err := s.applyDecisions(applyCtx, index)
				cancel()
				s.finishLocalLifecycle(err)
				done <- err
			}()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-done:
				return err
			}
		}
		return logManagedQueryFailure(ctx, "server_apply", s.applyDecisions(ctx, index))
	default:
		return errConsistency
	}
}

func (s *Server) handleNotifyPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req types.NotifyCommand
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "invalid notification request")
		return
	}
	receipt, err := s.NotifyPublish(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(receipt)
}

func (s *Server) NotifyPublish(ctx context.Context, req types.NotifyCommand) (types.MutationReceipt, error) {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return types.MutationReceipt{}, ErrInvalidRequest
	}
	if !s.writable || !s.Ready() {
		return types.MutationReceipt{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return types.MutationReceipt{}, err
	}
	charge, err := mutationCharge(req)
	if err != nil {
		return types.MutationReceipt{}, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return types.MutationReceipt{}, err
	}
	defer lease.releaseCaller()
	req.RequestID, req.Topic = cloneMutationString(req.RequestID), cloneMutationString(req.Topic)
	req.Payload = append([]byte(nil), req.Payload...)
	if req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		return types.MutationReceipt{}, ErrInvalidRequest
	}
	unlock, err := s.lockRequest(ctx, req.RequestID)
	if err != nil {
		return types.MutationReceipt{}, err
	}
	defer unlock()
	if matches, err := s.material.NotifyRequestMatches(ctx, req); err != nil {
		return types.MutationReceipt{}, err
	} else if !matches {
		return types.MutationReceipt{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.MutationReceipt(ctx, types.MutationNotify, req.RequestID); err != nil {
		return types.MutationReceipt{}, err
	} else if found {
		if err := s.waitReceiptDurable(ctx, receipt, req.RequestID); err != nil {
			return types.MutationReceipt{}, err
		}
		return receipt, nil
	}
	value, err := types.EncodeNotifyCommand(req)
	if err != nil {
		return types.MutationReceipt{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if len(value) > quepaxa.MaxReplicatedValueBytes {
		return types.MutationReceipt{}, fmt.Errorf("%w: encoded command exceeds %d bytes", ErrInvalidRequest, quepaxa.MaxReplicatedValueBytes)
	}
	slot, err := s.proposeWithLease(ctx, value, lease)
	if err == nil {
		if matches, matchErr := s.material.NotifyRequestMatches(ctx, req); matchErr != nil {
			err = matchErr
		} else if !matches {
			err = ErrRequestConflict
		}
	}
	if err != nil {
		return types.MutationReceipt{}, commitUnknown(slot, req.RequestID, err)
	}
	receipt, found, err := s.material.MutationReceipt(ctx, types.MutationNotify, req.RequestID)
	if err != nil {
		return types.MutationReceipt{}, commitUnknown(slot, req.RequestID, err)
	}
	if !found {
		return types.MutationReceipt{}, commitUnknown(slot, req.RequestID, fmt.Errorf("notification receipt is unavailable"))
	}
	return receipt, nil
}

func (s *Server) NotifySubscribe(topic string) (<-chan []byte, func(), error) {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return nil, nil, ErrInvalidRequest
	}
	if s.localFailure() != nil {
		return nil, nil, ErrNotReady
	}
	if topic == "" || len(topic) > 256 {
		return nil, nil, ErrInvalidRequest
	}
	ch, cancel, err := s.material.Subscribe(topic)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrOverloaded, err)
	}
	return ch, cancel, nil
}

func (s *Server) NotificationDrops() uint64 {
	return s.material.NotificationDrops()
}

func (s *Server) handleNotifySubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" || len(topic) > 256 {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "topic is required")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeHTTPError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming unsupported")
		return
	}
	ch, cancel, err := s.NotifySubscribe(topic)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer cancel()
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case payload, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(payload)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}
