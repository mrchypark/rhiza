package managedsql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLNotSubmitted means this Execute invocation did not enter the private
	// SQL execution boundary. It is not no-effect proof for another invocation
	// or permission to replace a pending request with a new one.
	ErrSQLNotSubmitted     = errors.New("managed SQL not submitted")
	ErrSQLPending          = errors.New("pending SQL must be resolved first")
	ErrSQLRecoveryRequired = errors.New("SQL outcome unknown; explicit pending recovery required")
)

type SQLSubmissionStage string

const (
	SQLStageValidation  SQLSubmissionStage = "validation"
	SQLStageAuthority   SQLSubmissionStage = "authority"
	SQLStagePending     SQLSubmissionStage = "pending"
	SQLStageRepair      SQLSubmissionStage = "repair"
	SQLStageAdmission   SQLSubmissionStage = "admission"
	SQLStageReservation SQLSubmissionStage = "reservation"
)

// SQLNotSubmittedError reports only the current call's boundary. The caller
// must resolve the retained original request when RecoveryRequired is true.
// No stage grants automatic retry across authority changes or an unknown CAS.
type SQLNotSubmittedError struct {
	Stage            SQLSubmissionStage
	RecoveryRequired bool
	Cause            error
}

func (e *SQLNotSubmittedError) Error() string {
	return fmt.Sprintf("%v: stage=%s recovery_required=%t: %v", ErrSQLNotSubmitted, e.Stage, e.RecoveryRequired, e.Cause)
}
func (e *SQLNotSubmittedError) Unwrap() []error { return []error{ErrSQLNotSubmitted, e.Cause} }

// ErrKVNotSubmitted describes this call only, never an older pending outcome.
var ErrKVNotSubmitted = errors.New("managed KV not submitted")

type KVNotSubmittedError struct {
	Stage            SQLSubmissionStage
	RecoveryRequired bool
	Cause            error
}

func (e *KVNotSubmittedError) Error() string {
	return fmt.Sprintf("%v: stage=%s recovery_required=%t: %v", ErrKVNotSubmitted, e.Stage, e.RecoveryRequired, e.Cause)
}
func (e *KVNotSubmittedError) Unwrap() []error { return []error{ErrKVNotSubmitted, e.Cause} }
