package cri

import "errors"

// StopDiagnosticError carries only an allowlisted stop stage and code across
// the runtime-agent boundary. Cause remains available to trusted callers but
// is never serialized by the runtime-agent API.
type StopDiagnosticError struct {
	Stage string
	Code  string
	Cause error
}

func (e *StopDiagnosticError) Error() string { return "fencing: runtime stop incomplete" }
func (e *StopDiagnosticError) Unwrap() error { return e.Cause }

func stopDiagnostic(stage, code string, cause error) error {
	return &StopDiagnosticError{Stage: stage, Code: code, Cause: cause}
}

// NewStopDiagnosticError constructs a diagnostic only for a fixed stage/code
// pair. Invalid pairs are deliberately reduced to a generic backend category.
func NewStopDiagnosticError(stage, code string, cause error) error {
	if !ValidStopDiagnostic(stage, code) {
		stage, code = "backend_stop", "backend_unclassified"
	}
	if _, ok := stopDiagnosticFrom(cause); ok {
		return cause
	}
	return stopDiagnostic(stage, code, cause)
}

func ValidStopDiagnostic(stage, code string) bool {
	switch stage {
	case "stop_request":
		return code == "invalid_request"
	case "enrollment_state":
		return code == "missing_or_unreadable" || code == "binding_mismatch"
	case "enrollment_capture":
		return code == "capture_unverified"
	case "stop_intent_persist":
		return code == "intent_write_failed"
	case "passive_observation_persist":
		return code == "observation_write_failed"
	case "pre_container_identity":
		return code == "runtime_identity_unverified"
	case "pre_container_state":
		return code == "container_state_unverified"
	case "pre_container_scope":
		return code == "process_scope_unverified"
	case "cri_stop_container":
		return code == "container_stop_unconfirmed"
	case "post_container_state":
		return code == "container_state_unverified"
	case "pre_sandbox_scope":
		switch code {
		case "cgroup_scope_unverified", "cgroup_identity_changed", "cgroup_observation_unavailable", "target_process_live", "target_process_state_unavailable", "cgroup_populated":
			return true
		default:
			return false
		}
	case "pre_sandbox_identity":
		return code == "runtime_identity_unverified"
	case "cri_stop_sandbox":
		return code == "sandbox_stop_unconfirmed"
	case "post_sandbox_state":
		return code == "sandbox_state_unverified"
	case "post_stop_identity":
		return code == "runtime_identity_unverified"
	case "post_stop_cgroup":
		return code == "cgroup_empty_unverified"
	case "receipt_validation":
		return code == "receipt_invalid"
	case "receipt_persist":
		return code == "receipt_write_failed"
	case "backend_stop":
		return code == "backend_unclassified"
	default:
		return false
	}
}

func stopDiagnosticFrom(err error) (*StopDiagnosticError, bool) {
	var diagnostic *StopDiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic == nil || !ValidStopDiagnostic(diagnostic.Stage, diagnostic.Code) {
		return nil, false
	}
	return diagnostic, true
}

// StopDiagnosticFrom returns only a validated, bounded diagnostic tuple.
func StopDiagnosticFrom(err error) (*StopDiagnosticError, bool) {
	return stopDiagnosticFrom(err)
}
