//go:build cgo

package main

/*
#include <stdlib.h>
#include <stdint.h>
typedef struct { void *data; size_t len; } RhizaBuffer;
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/mrchypark/rhiza"
)

const (
	maxInputBytes  = 16 << 20
	defaultTimeout = 30 * time.Second
)

type ffiEntry struct {
	db      *rhiza.DB
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	active  int
	closed  bool
	drained chan struct{}
}

var ffiRegistry = struct {
	sync.Mutex
	next uint64
	m    map[uint64]*ffiEntry
}{next: 1, m: make(map[uint64]*ffiEntry)}

type ffiEnvelope struct {
	Data  any       `json:"data,omitempty"`
	Error *ffiError `json:"error,omitempty"`
}

type ffiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ffiError) Error() string { return e.Message }

type callEnvelope struct {
	Operation string          `json:"operation"`
	Request   json.RawMessage `json:"request"`
}

func main() {}

// RhizaOpen opens an embedded DB from a JSON-encoded rhiza.Config.
//
//export RhizaOpen
func RhizaOpen(data unsafe.Pointer, length C.size_t) C.RhizaBuffer {
	return cBuffer(goOpen(copiedInput(data, length)))
}

// RhizaCall invokes one public embedded DB operation.
//
//export RhizaCall
func RhizaCall(handle C.uint64_t, data unsafe.Pointer, length C.size_t, timeoutMS C.uint64_t) C.RhizaBuffer {
	return cBuffer(goCall(uint64(handle), copiedInput(data, length), uint64(timeoutMS)))
}

// RhizaClose cancels active calls, waits for them, and closes the DB.
//
//export RhizaClose
func RhizaClose(handle C.uint64_t) C.RhizaBuffer { return cBuffer(goClose(uint64(handle))) }

// RhizaFree frees a buffer returned by RhizaOpen, RhizaCall, or RhizaClose.
//
//export RhizaFree
func RhizaFree(buffer C.RhizaBuffer) { C.free(buffer.data) }

func copiedInput(data unsafe.Pointer, length C.size_t) []byte {
	if uint64(length) > maxInputBytes {
		return nil
	}
	if data == nil && length != 0 {
		return nil
	}
	if length == 0 {
		return []byte{}
	}
	return C.GoBytes(data, C.int(length))
}

func cBuffer(data []byte) C.RhizaBuffer {
	if len(data) == 0 {
		return C.RhizaBuffer{}
	}
	return C.RhizaBuffer{data: C.CBytes(data), len: C.size_t(len(data))}
}

func goOpen(input []byte) []byte {
	if err := validInput(input); err != nil {
		return errorJSON(err)
	}
	if bytes.Equal(bytes.TrimSpace(input), []byte("null")) {
		return errorJSON(&ffiError{Code: "invalid_request", Message: "open config must be an object"})
	}
	var config rhiza.Config
	if err := decodeStrict(input, &config); err != nil {
		return errorJSON(&ffiError{Code: "invalid_request", Message: "invalid open request: " + err.Error()})
	}
	db, err := rhiza.Open(context.Background(), config)
	if err != nil {
		return errorJSON(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry := &ffiEntry{db: db, ctx: ctx, cancel: cancel, drained: make(chan struct{})}
	ffiRegistry.Lock()
	handle := ffiRegistry.next
	if handle == 0 || handle == math.MaxUint64 {
		ffiRegistry.Unlock()
		cancel()
		_ = db.Close()
		return errorJSON(&ffiError{Code: "handle_exhausted", Message: "Rhiza handle space exhausted"})
	}
	ffiRegistry.next++
	ffiRegistry.m[handle] = entry
	ffiRegistry.Unlock()
	return dataJSON(map[string]uint64{"handle": handle})
}

func goCall(handle uint64, input []byte, timeoutMS uint64) []byte {
	if err := validInput(input); err != nil {
		return errorJSON(err)
	}
	var err error
	entry, release, err := acquire(handle)
	if err != nil {
		return errorJSON(err)
	}
	defer release()
	timeout, err := timeoutDuration(timeoutMS)
	if err != nil {
		return errorJSON(&ffiError{Code: "invalid_request", Message: err.Error()})
	}
	ctx, cancel := context.WithTimeout(entry.ctx, timeout)
	defer cancel()
	var call callEnvelope
	if err := decodeStrict(input, &call); err != nil {
		return errorJSON(&ffiError{Code: "invalid_request", Message: "invalid call request: " + err.Error()})
	}
	if call.Operation == "" || len(call.Request) == 0 || bytes.Equal(bytes.TrimSpace(call.Request), []byte("null")) {
		return errorJSON(&ffiError{Code: "invalid_request", Message: "operation and request are required"})
	}
	result, err := dispatch(ctx, entry.db, call)
	if err != nil {
		var syntax *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &syntax) || errors.As(err, &typeError) {
			return errorJSON(&ffiError{Code: "invalid_request", Message: err.Error()})
		}
		return errorJSON(err)
	}
	return dataJSON(result)
}

func goClose(handle uint64) []byte {
	ffiRegistry.Lock()
	entry, ok := ffiRegistry.m[handle]
	if ok {
		delete(ffiRegistry.m, handle)
	}
	ffiRegistry.Unlock()
	if !ok {
		return errorJSON(&ffiError{Code: "invalid_handle", Message: "unknown or closed Rhiza handle"})
	}
	entry.mu.Lock()
	entry.closed = true
	entry.cancel()
	if entry.active == 0 {
		close(entry.drained)
	}
	entry.mu.Unlock()
	<-entry.drained
	if err := entry.db.Close(); err != nil {
		return errorJSON(err)
	}
	return dataJSON(nil)
}

func acquire(handle uint64) (*ffiEntry, func(), error) {
	ffiRegistry.Lock()
	entry := ffiRegistry.m[handle]
	ffiRegistry.Unlock()
	if entry == nil {
		return nil, nil, &ffiError{Code: "invalid_handle", Message: "unknown or closed Rhiza handle"}
	}
	entry.mu.Lock()
	if entry.closed {
		entry.mu.Unlock()
		return nil, nil, &ffiError{Code: "closed", Message: "Rhiza handle is closing"}
	}
	entry.active++
	entry.mu.Unlock()
	return entry, func() {
		entry.mu.Lock()
		entry.active--
		if entry.closed && entry.active == 0 {
			close(entry.drained)
		}
		entry.mu.Unlock()
	}, nil
}

func timeoutDuration(ms uint64) (time.Duration, error) {
	if ms == 0 {
		return defaultTimeout, nil
	}
	if ms > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return 0, fmt.Errorf("timeout_ms overflows duration")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func validInput(input []byte) error {
	if input == nil {
		return &ffiError{Code: "invalid_request", Message: "input is nil or exceeds 16 MiB"}
	}
	if len(input) > maxInputBytes {
		return &ffiError{Code: "invalid_request", Message: "input exceeds 16 MiB"}
	}
	return nil
}

func decodeStrict(input []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func dispatch(ctx context.Context, db *rhiza.DB, call callEnvelope) (any, error) {
	decode := func(target any) error {
		if err := decodeStrict(call.Request, target); err != nil {
			return &ffiError{Code: "invalid_request", Message: err.Error()}
		}
		return nil
	}
	switch call.Operation {
	case "execute", "execute_returning":
		var r rhiza.ExecuteRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		if call.Operation == "execute" {
			return db.Execute(ctx, r)
		}
		return db.ExecuteReturning(ctx, r)
	case "query":
		var r rhiza.QueryRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.Query(ctx, r)
	case "kv_get":
		var r rhiza.KVGetRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.KVGet(ctx, r)
	case "kv_put":
		var r rhiza.KVMutationRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.KVPut(ctx, r)
	case "kv_delete":
		var r rhiza.KVMutationRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.KVDelete(ctx, r)
	case "kv_cas":
		var r rhiza.KVMutationRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.KVCAS(ctx, r)
	case "graph_execute":
		var r rhiza.GraphCommand
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.GraphExecute(ctx, r)
	case "graph_query":
		var r rhiza.GraphQueryRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.GraphQuery(ctx, r)
	case "graph_reachable":
		var r rhiza.GraphReachableRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.GraphReachable(ctx, r)
	case "graph_stream_read":
		var r rhiza.GraphStreamReadRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.GraphStreamRead(ctx, r)
	case "graph_stream_offset":
		var r rhiza.GraphStreamOffsetRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.GraphStreamOffset(ctx, r)
	case "set_graph_stream_offset":
		var r rhiza.GraphStreamOffsetRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return nil, db.SetGraphStreamOffset(ctx, r)
	case "trim_graph_stream":
		var r rhiza.GraphStreamTrimRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return nil, db.TrimGraphStream(ctx, r)
	case "request_status":
		var r rhiza.RequestStatusRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return db.RequestStatus(ctx, r)
	case "ready":
		return db.Ready(), nil
	case "object_store_stats":
		stats, ok := db.ObjectStoreStats()
		return map[string]any{"stats": stats, "available": ok}, nil
	default:
		return nil, &ffiError{Code: "invalid_request", Message: "unknown operation: " + call.Operation}
	}
}

func dataJSON(value any) []byte {
	b, err := json.Marshal(map[string]any{"data": value})
	if err != nil {
		return marshalError("internal", "encode response: "+err.Error())
	}
	return b
}
func errorJSON(err error) []byte {
	var typed *ffiError
	if errors.As(err, &typed) {
		return marshalError(typed.Code, typed.Message)
	}
	return marshalError(errorCode(err), err.Error())
}
func marshalError(code, message string) []byte {
	b, err := json.Marshal(ffiEnvelope{Error: &ffiError{Code: code, Message: message}})
	if err != nil {
		return []byte(`{"error":{"code":"internal","message":"encode error"}}`)
	}
	return b
}
func errorCode(err error) string {
	switch {
	case errors.Is(err, rhiza.ErrCommitUnknown):
		return "commit_unknown"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, rhiza.ErrInvalidRequest):
		return "invalid_request"
	case errors.Is(err, rhiza.ErrNotReady):
		return "not_ready"
	case errors.Is(err, rhiza.ErrQuorumUnavailable):
		return "quorum_unavailable"
	case errors.Is(err, rhiza.ErrRequestConflict):
		return "request_conflict"
	case errors.Is(err, rhiza.ErrDurabilityUnavailable):
		return "durability_unavailable"
	default:
		return "internal"
	}
}
