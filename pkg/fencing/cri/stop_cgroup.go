package cri

import (
	"errors"
	"fmt"
)

type stopCgroupCheckFailure struct {
	code  string
	cause error
}

func (e *stopCgroupCheckFailure) Error() string { return "fencing: cgroup stop verification failed" }
func (e *stopCgroupCheckFailure) Unwrap() error { return e.cause }

func stopCgroupFailure(code string, cause error) error {
	return &stopCgroupCheckFailure{code: code, cause: cause}
}

func stopCgroupFailureCode(err error) string {
	var failure *stopCgroupCheckFailure
	if errors.As(err, &failure) && failure != nil {
		return failure.code
	}
	return "cgroup_scope_unverified"
}

func preSandboxScopeDiagnostic(err error) error {
	code := stopCgroupFailureCode(err)
	if code == "cgroup_scope_unverified" {
		return stopDiagnostic("pre_sandbox_scope", code, err)
	}
	return stopDiagnostic("pre_sandbox_scope", code, fmt.Errorf("%w: stop scope verification failed", ErrUnverified))
}

type stopCgroupPin interface {
	verifyEmpty() error
	close()
}
