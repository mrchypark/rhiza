//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

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

type LogicalMutationOutcome string

const (
	LogicalMutationStarted   LogicalMutationOutcome = "started"
	LogicalMutationSucceeded LogicalMutationOutcome = "succeeded"
	LogicalMutationFailed    LogicalMutationOutcome = "failed"
	LogicalMutationPending   LogicalMutationOutcome = "pending"
)

type LogicalMutationExecutionEvent struct {
	ProducerHash string                 `json:"producer_hash"`
	Sequence     uint64                 `json:"sequence"`
	Stage        LogicalMutationStage   `json:"stage"`
	Outcome      LogicalMutationOutcome `json:"outcome"`
	ErrorClass   string                 `json:"error_class"`
	HTTPStatus   int                    `json:"http_status,omitempty"`
	ContextState string                 `json:"context_state"`
}

type LogicalMutationExecutionTrace struct{}

func NewLogicalMutationExecutionTrace(int) *LogicalMutationExecutionTrace {
	return &LogicalMutationExecutionTrace{}
}
func (*LogicalMutationExecutionTrace) Context(ctx context.Context) context.Context { return ctx }
func (*LogicalMutationExecutionTrace) Snapshot() ([]LogicalMutationExecutionEvent, bool) {
	return nil, false
}
func HitLogicalMutationExecution(context.Context, string, uint64, LogicalMutationStage, LogicalMutationOutcome) {
}
func HitLogicalMutationExecutionError(context.Context, string, uint64, LogicalMutationStage, error) {}
