//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"google.golang.org/api/googleapi"
)

type logicalMutationTraceKey struct{}

// LogicalMutationStage is a fixed execution boundary for local test traces.
type LogicalMutationStage string

const (
	LogicalMutationAttemptStarted   LogicalMutationStage = "attempt-started"
	LogicalMutationDispatchGranted  LogicalMutationStage = "dispatch-granted"
	LogicalMutationPayloadOpened    LogicalMutationStage = "payload-open"
	LogicalMutationProviderStarted  LogicalMutationStage = "provider-started"
	LogicalMutationProviderReturned LogicalMutationStage = "provider-returned"
	LogicalMutationReceiptPersisted LogicalMutationStage = "receipt-persisted"
	LogicalMutationJournalResolved  LogicalMutationStage = "journal-resolved"
	LogicalMutationKeyReleased      LogicalMutationStage = "key-released"
	LogicalMutationAttemptReturned  LogicalMutationStage = "attempt-returned"
)

// LogicalMutationOutcome is a coarse result category; it never contains an error message.
type LogicalMutationOutcome string

const (
	LogicalMutationStarted   LogicalMutationOutcome = "started"
	LogicalMutationSucceeded LogicalMutationOutcome = "succeeded"
	LogicalMutationFailed    LogicalMutationOutcome = "failed"
	LogicalMutationPending   LogicalMutationOutcome = "pending"
)

const defaultLogicalMutationTraceLimit = 256

// LogicalMutationExecutionEvent contains only bounded identifiers and fixed categories.
type LogicalMutationExecutionEvent struct {
	ProducerHash string                 `json:"producer_hash"`
	Sequence     uint64                 `json:"sequence"`
	Stage        LogicalMutationStage   `json:"stage"`
	Outcome      LogicalMutationOutcome `json:"outcome"`
	ErrorClass   string                 `json:"error_class"`
	HTTPStatus   int                    `json:"http_status,omitempty"`
	ContextState string                 `json:"context_state"`
}

// LogicalMutationExecutionTrace is a bounded, concurrency-safe test observer.
type LogicalMutationExecutionTrace struct {
	mu        sync.Mutex
	limit     int
	events    []LogicalMutationExecutionEvent
	next      int
	truncated bool
}

// NewLogicalMutationExecutionTrace creates a bounded trace. Nonpositive limits use a conservative default.
func NewLogicalMutationExecutionTrace(limit int) *LogicalMutationExecutionTrace {
	if limit <= 0 {
		limit = defaultLogicalMutationTraceLimit
	}
	return &LogicalMutationExecutionTrace{limit: limit}
}

// Context attaches this trace to an in-process request context.
func (t *LogicalMutationExecutionTrace) Context(ctx context.Context) context.Context {
	if t == nil || ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, logicalMutationTraceKey{}, t)
}

// Snapshot returns a copy of retained events and whether the configured bound was reached.
func (t *LogicalMutationExecutionTrace) Snapshot() ([]LogicalMutationExecutionEvent, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.truncated {
		return append([]LogicalMutationExecutionEvent(nil), t.events...), false
	}
	ordered := make([]LogicalMutationExecutionEvent, 0, len(t.events))
	ordered = append(ordered, t.events[t.next:]...)
	ordered = append(ordered, t.events[:t.next]...)
	return ordered, true
}

// HitLogicalMutationExecution records one fixed execution boundary when a test trace is attached.
func HitLogicalMutationExecution(ctx context.Context, producerUID string, sequence uint64, stage LogicalMutationStage, outcome LogicalMutationOutcome) {
	class, status, state := classifyLogicalMutationError(ctx, nil)
	hitLogicalMutationExecution(ctx, producerUID, sequence, stage, outcome, class, status, state)
}

// HitLogicalMutationExecutionError records one completion boundary with only
// an allowlisted error class, recognized HTTP status, and context state.
func HitLogicalMutationExecutionError(ctx context.Context, producerUID string, sequence uint64, stage LogicalMutationStage, err error) {
	class, status, state := classifyLogicalMutationError(ctx, err)
	outcome := LogicalMutationSucceeded
	if err != nil {
		outcome = LogicalMutationPending
	}
	hitLogicalMutationExecution(ctx, producerUID, sequence, stage, outcome, class, status, state)
}

func hitLogicalMutationExecution(ctx context.Context, producerUID string, sequence uint64, stage LogicalMutationStage,
	outcome LogicalMutationOutcome, errorClass string, status int, contextState string) {
	if ctx == nil || producerUID == "" || sequence == 0 || !validLogicalMutationStage(stage) || !validLogicalMutationOutcome(outcome) {
		return
	}
	trace, ok := ctx.Value(logicalMutationTraceKey{}).(*LogicalMutationExecutionTrace)
	if !ok || trace == nil {
		return
	}
	digest := sha256.Sum256([]byte(producerUID))
	event := LogicalMutationExecutionEvent{ProducerHash: hex.EncodeToString(digest[:6]), Sequence: sequence,
		Stage: stage, Outcome: outcome, ErrorClass: errorClass, HTTPStatus: status, ContextState: contextState}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.events) < trace.limit {
		trace.events = append(trace.events, event)
		return
	}
	trace.events[trace.next] = event
	trace.next = (trace.next + 1) % trace.limit
	trace.truncated = true
}

func classifyLogicalMutationError(ctx context.Context, err error) (class string, status int, contextState string) {
	contextState = "active"
	if ctx == nil {
		contextState = "unavailable"
	} else if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
		contextState = "canceled"
	} else if errors.Is(ctxErr, context.DeadlineExceeded) {
		contextState = "deadline"
	} else if ctxErr != nil {
		contextState = "other"
	}
	switch {
	case err == nil:
		return "none", 0, contextState
	case errors.Is(err, context.Canceled):
		return "canceled", 0, contextState
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline", 0, contextState
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code >= 400 && apiErr.Code <= 599 {
		return "googleapi-http", apiErr.Code, contextState
	}
	return "other", 0, contextState
}

func validLogicalMutationStage(stage LogicalMutationStage) bool {
	switch stage {
	case LogicalMutationAttemptStarted, LogicalMutationDispatchGranted, LogicalMutationPayloadOpened, LogicalMutationProviderStarted, LogicalMutationProviderReturned,
		LogicalMutationReceiptPersisted, LogicalMutationJournalResolved, LogicalMutationKeyReleased, LogicalMutationAttemptReturned:
		return true
	default:
		return false
	}
}

func validLogicalMutationOutcome(outcome LogicalMutationOutcome) bool {
	switch outcome {
	case LogicalMutationStarted, LogicalMutationSucceeded, LogicalMutationFailed, LogicalMutationPending:
		return true
	default:
		return false
	}
}
