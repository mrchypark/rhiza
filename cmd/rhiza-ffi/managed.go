//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
)

type managedOpenRequest struct {
	RuntimeProfile    string `json:"runtime_profile"`
	IdentityFile      string `json:"identity_file"`
	HandoffFile       string `json:"handoff_file"`
	RepairHandoffFile string `json:"repair_handoff_file"`
}

func goOpenManaged(input []byte) []byte {
	var request managedOpenRequest
	if validInput(input) != nil || bytes.Equal(bytes.TrimSpace(input), []byte("null")) || decodeStrict(input, &request) != nil {
		return marshalError("invalid_request", "invalid managed open request")
	}
	if request.RuntimeProfile == "" {
		request.RuntimeProfile = operator.V1Alpha2ManagedRuntimeProfilePath
	}
	if request.IdentityFile == "" {
		request.IdentityFile = operator.V1Alpha2BootstrapIdentityPath
	}
	if request.HandoffFile == "" {
		request.HandoffFile = operator.V1Alpha2BootstrapHandoffPath
	}
	if request.RepairHandoffFile == "" {
		request.RepairHandoffFile = managedbootstrap.RepairHandoffPath
	}
	ref := operator.V1Alpha2ClusterRef{Name: os.Getenv("RHIZA_CLUSTER_NAME"), Namespace: os.Getenv("POD_NAMESPACE"), UID: os.Getenv("RHIZA_CLUSTER_UID")}
	pod, uid := os.Getenv("POD_NAME"), os.Getenv("POD_UID")
	if ref.Name == "" || ref.Namespace == "" || ref.UID == "" || pod == "" || uid == "" {
		return marshalError("invalid_request", "managed execution identity required")
	}
	f, err := os.Open(request.RuntimeProfile)
	if err != nil {
		return marshalError("invalid_request", "managed profile unavailable")
	}
	profile, err := operator.DecodeV1Alpha2RuntimeProfile(f)
	err = errors.Join(err, f.Close())
	if err != nil {
		return marshalError("invalid_request", "invalid managed profile")
	}
	kube, err := operator.NewInCluster(ref.Namespace)
	if err != nil {
		return marshalError("not_ready", "managed Kubernetes client unavailable")
	}
	localtesthooks.HitManagedKubeClient(kube.Client)
	ctx, cancel := context.WithCancel(context.Background())
	opening, finishOpening := managedStartupContext(ctx, defaultTimeout)
	cfg, cleanup, err := managedbootstrap.LoadConfig(opening, managedbootstrap.ConfigRequest{Kube: kube, Ref: ref, PodName: pod, PodUID: uid, IdentityPath: request.IdentityFile, BootstrapPath: request.HandoffFile, RepairPath: request.RepairHandoffFile, Profile: profile})
	if err != nil {
		err = errors.Join(err, finishOpening())
		cancel()
		_ = cleanup()
		return marshalError(errorCode(err), "managed startup inputs rejected")
	}
	host, err := managedsql.Open(opening, cfg)
	err = errors.Join(err, finishOpening())
	if err != nil {
		cancel()
		if host != nil {
			_ = host.Close()
		}
		_ = cleanup()
		return marshalError(errorCode(err), "managed startup rejected")
	}
	return registerEntry(&ffiEntry{managed: host, cleanup: cleanup, ctx: ctx, cancel: cancel, drained: make(chan struct{})})
}

// Node derives its background lifetime from Open's context. Retire only the
// startup timer on success; handle cancellation must still reach that context.
func managedStartupContext(parent context.Context, budget time.Duration) (context.Context, func() error) {
	ctx, cancel := context.WithCancelCause(parent)
	timer := time.AfterFunc(budget, func() { cancel(context.DeadlineExceeded) })
	var once sync.Once
	var result error
	finish := func() error {
		once.Do(func() {
			if !timer.Stop() {
				cancel(context.DeadlineExceeded)
			}
			result = context.Cause(ctx)
		})
		return result
	}
	return ctx, finish
}

func registerEntry(entry *ffiEntry) []byte {
	ffiRegistry.Lock()
	handle := ffiRegistry.next
	if handle == 0 || handle == math.MaxUint64 {
		ffiRegistry.Unlock()
		entry.cancel()
		_ = entry.closeOwned()
		return marshalError("handle_exhausted", "Rhiza handle space exhausted")
	}
	ffiRegistry.next++
	ffiRegistry.m[handle] = entry
	ffiRegistry.Unlock()
	return dataJSON(map[string]uint64{"handle": handle})
}

func (entry *ffiEntry) closeOwned() error {
	var err error
	if entry.managed != nil {
		err = entry.managed.Close()
	}
	if entry.db != nil {
		err = errors.Join(err, entry.db.Close())
	}
	if entry.cleanup != nil {
		err = errors.Join(err, entry.cleanup())
	}
	return err
}

func dispatchManaged(ctx context.Context, entry *ffiEntry, call callEnvelope) (any, error) {
	decode := func(target any) error {
		if decodeStrict(call.Request, target) != nil {
			return &ffiError{Code: "invalid_request", Message: "invalid managed request"}
		}
		return nil
	}
	switch call.Operation {
	case "execute":
		var r managedsql.ExecuteRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return entry.managed.Execute(ctx, r)
	case "query":
		var r managedsql.QueryRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return entry.managed.Query(ctx, r)
	case "ready":
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		return entry.managed.Ready(ctx) == nil, nil
	case "recover_pending":
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		return nil, entry.managed.RecoverPending(ctx)
	case "recover_pending_detailed":
		selector, err := decodeManagedRequestSelector(call.Request, true)
		if err != nil {
			return nil, err
		}
		var result managedsql.RecoveryResult
		if selector == nil {
			result, err = entry.managed.RecoverPendingDetailed(ctx)
		} else {
			result, err = entry.managed.RecoverRequest(ctx, *selector)
		}
		if err != nil {
			return nil, err
		}
		return managedRecoveryWire(result)
	case "kv_put", "kv_delete", "kv_cas":
		operation, request, err := decodeManagedKVMutation(call.Operation, call.Request)
		if err != nil {
			return nil, err
		}
		result, err := entry.managed.KVMutate(ctx, operation, request)
		if err != nil {
			return nil, err
		}
		return managedKVReceiptWire(result.MutationReceipt), nil
	case "kv_get":
		var wire struct {
			Key         *string `json:"key_b64"`
			Consistency *string `json:"consistency"`
		}
		if err := decode(&wire); err != nil {
			return nil, err
		}
		key, err := managedRawBytes(wire.Key, 1024)
		if err != nil || wire.Consistency == nil || (*wire.Consistency != "local" && *wire.Consistency != "linearizable") {
			return nil, invalidManagedKVRequest()
		}
		result, err := entry.managed.KVGet(ctx, network.KVGetRequest{Key: string(key), Consistency: *wire.Consistency})
		if err != nil {
			return nil, err
		}
		return managedKVGetWire(result), nil
	case "request_status":
		selector, err := decodeManagedRequestSelector(call.Request, false)
		if err != nil {
			return nil, err
		}
		result, err := entry.managed.RequestStatus(ctx, *selector)
		if err != nil {
			return nil, err
		}
		return managedStatusWire(selector.Kind, result), nil
	case "graph_execute":
		frame, err := decodeManagedGraphFrame(call.Request)
		if err != nil {
			return nil, err
		}
		original, recognized, err := types.DecodeManagedGraphExecution(frame)
		if err != nil || !recognized {
			return nil, invalidManagedKVRequest()
		}
		result, err := entry.managed.GraphExecute(ctx, original)
		if err != nil {
			return nil, err
		}
		return managedKVReceiptWire(result.MutationReceipt), nil
	case "graph_query":
		var request network.GraphQueryRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		return entry.managed.GraphQuery(ctx, request)
	case "graph_reachable":
		var request types.GraphReachableRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		return entry.managed.GraphReachable(ctx, request)
	case "graph_changes", "graph_stream_read":
		var request network.GraphStreamReadRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if call.Operation == "graph_changes" {
			return entry.managed.GraphChanges(ctx, request)
		}
		return entry.managed.GraphStreamRead(ctx, request)
	case "graph_stream_offset":
		var request network.GraphStreamOffsetRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		return entry.managed.GraphStreamOffset(ctx, request)
	default:
		return nil, &ffiError{Code: "invalid_request", Message: "operation unavailable on managed SQL handle"}
	}
}

// A selector is either the exact pair or, only for the compatibility recovery
// facade, an empty object. Explicit null is not an omitted selector.
func decodeManagedRequestSelector(raw json.RawMessage, allowEmpty bool) (*network.RequestStatusRequest, error) {
	var wire struct {
		Kind      *string `json:"kind"`
		RequestID *string `json:"request_id_b64"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, invalidManagedKVRequest()
	}
	var seen uint8
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, invalidManagedKVRequest()
		}
		var bit uint8
		var target **string
		switch key {
		case "kind":
			bit, target = 1, &wire.Kind
		case "request_id_b64":
			bit, target = 2, &wire.RequestID
		default:
			return nil, invalidManagedKVRequest()
		}
		if seen&bit != 0 || decoder.Decode(target) != nil {
			return nil, invalidManagedKVRequest()
		}
		seen |= bit
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return nil, invalidManagedKVRequest()
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, invalidManagedKVRequest()
	}
	if seen == 0 && allowEmpty {
		return nil, nil
	}
	if seen != 3 || wire.Kind == nil || (*wire.Kind != "sql" && *wire.Kind != "kv" && *wire.Kind != "graph") {
		return nil, invalidManagedKVRequest()
	}
	id, err := managedRawBytes(wire.RequestID, 64)
	if err != nil {
		return nil, invalidManagedKVRequest()
	}
	return &network.RequestStatusRequest{Kind: *wire.Kind, RequestID: string(id)}, nil
}

func invalidManagedKVRequest() error {
	return &ffiError{Code: "invalid_request", Message: "invalid managed request"}
}

func managedRawBytes(value *string, limit int) ([]byte, error) {
	if value == nil || len(*value) > base64.StdEncoding.EncodedLen(limit) {
		return nil, invalidManagedKVRequest()
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(*value)
	if err != nil || len(decoded) == 0 || len(decoded) > limit || base64.StdEncoding.EncodeToString(decoded) != *value {
		return nil, invalidManagedKVRequest()
	}
	return decoded, nil
}

func decodeManagedKVMutation(outer string, raw json.RawMessage) (string, network.KVMutationRequest, error) {
	// This must consume original bytes before a generic decoder can discard
	// duplicate/missing fields or normalize a raw-byte identity.
	command, err := types.DecodeManagedKVOriginal(raw)
	if err != nil || outer != "kv_"+command.Operation {
		return "", network.KVMutationRequest{}, invalidManagedKVRequest()
	}
	return command.Operation, network.KVMutationRequest{RequestID: command.RequestID, Key: command.Key, Value: command.Value, Expected: command.Expected, ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}, nil
}

type managedKVReceipt struct {
	Slot             uint64               `json:"slot"`
	Status           types.MutationStatus `json:"status"`
	Applied          bool                 `json:"applied"`
	ErrorCode        string               `json:"error_code,omitempty"`
	RetryThroughSlot uint64               `json:"retry_through_slot"`
}

func managedKVReceiptWire(receipt types.MutationReceipt) managedKVReceipt {
	return managedKVReceipt{receipt.Slot, receipt.Status, receipt.Applied, receipt.ErrorCode, receipt.RetryThroughSlot}
}

type managedKVRead struct {
	Found        bool   `json:"found"`
	Value        []byte `json:"value"`
	AppliedSlot  uint64 `json:"applied_slot"`
	ConsensusTip uint64 `json:"consensus_tip"`
}

func managedKVGetWire(result network.KVGetResponse) managedKVRead {
	// Found is authoritative: present nil/empty storage is logical empty bytes,
	// not absence. Original mutation intent still retains nil versus empty.
	var value []byte
	if result.Found {
		value = append([]byte{}, result.Value...)
	}
	return managedKVRead{result.Found, value, result.AppliedSlot, result.ConsensusTip}
}

func managedStatusWire(kind string, result network.RequestStatusResponse) any {
	var receipt any
	if result.Receipt != nil {
		if kind == "kv" || kind == "graph" {
			receipt = managedKVReceiptWire(*result.Receipt)
		} else {
			receipt = result.Receipt
		}
	}
	return struct {
		State   string `json:"state"`
		Tip     uint64 `json:"tip"`
		Receipt any    `json:"receipt,omitempty"`
	}{result.State, result.Tip, receipt}
}

func managedRecoveryWire(result managedsql.RecoveryResult) (any, error) {
	if result.Kind == "" && result.SQL == nil && result.KV == nil && result.Graph == nil && len(result.RequestID) == 0 {
		return struct {
			Kind string `json:"kind"`
		}{"none"}, nil
	}
	var sql *managedsql.ExecuteResponse
	var kv *managedKVReceipt
	var graph *managedKVReceipt
	switch {
	case result.Kind == "sql" && result.SQL != nil && result.KV == nil && result.Graph == nil && len(result.RequestID) > 0:
		sql = result.SQL
	case result.Kind == "kv" && result.KV != nil && result.SQL == nil && result.Graph == nil && len(result.RequestID) > 0:
		receipt := managedKVReceiptWire(result.KV.MutationReceipt)
		kv = &receipt
	case result.Kind == "graph" && result.Graph != nil && result.SQL == nil && result.KV == nil && len(result.RequestID) > 0:
		receipt := managedKVReceiptWire(result.Graph.MutationReceipt)
		graph = &receipt
	default:
		return nil, &ffiError{Code: "internal", Message: "invalid managed recovery result"}
	}
	return struct {
		Kind      string                      `json:"kind"`
		RequestID []byte                      `json:"request_id_b64"`
		SQL       *managedsql.ExecuteResponse `json:"sql,omitempty"`
		KV        *managedKVReceipt           `json:"kv,omitempty"`
		Graph     *managedKVReceipt           `json:"graph,omitempty"`
	}{result.Kind, result.RequestID, sql, kv, graph}, nil
}
