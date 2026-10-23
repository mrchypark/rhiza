package cri

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// PassiveScope is enrollment-time identity for the trusted sandbox process.
// Kernel handles are deliberately not serialized; they remain in the provider
// process until a durable observation has been recorded by the runtime agent.
type PassiveScope struct {
	Version                 int      `json:"version"`
	SandboxPID              uint32   `json:"sandboxPID"`
	SandboxStart            uint64   `json:"sandboxStartTime"`
	SandboxPath             string   `json:"sandboxCgroupPath"`
	SandboxDev              uint64   `json:"sandboxCgroupDev"`
	SandboxIno              uint64   `json:"sandboxCgroupIno"`
	SandboxPIDNSDev         uint64   `json:"sandboxPIDNamespaceDev"`
	SandboxPIDNSIno         uint64   `json:"sandboxPIDNamespaceIno"`
	TargetPIDNSDev          uint64   `json:"targetPIDNamespaceDev"`
	TargetPIDNSIno          uint64   `json:"targetPIDNamespaceIno"`
	SandboxImage            string   `json:"sandboxImage"`
	SandboxExecutableSHA256 string   `json:"sandboxExecutableSHA256"`
	SandboxArgs             []string `json:"sandboxArgs"`
	RuntimeHash             string   `json:"runtimeHash"`
	ProfileID               string   `json:"profileID"`
}

func validPassiveSandboxProfile(approvedExecutableSHA256 string, approvedArgs, runtimeArgs []string) bool {
	return validOCIDigest("sha256:"+approvedExecutableSHA256) && len(approvedArgs) > 0 && passiveStringsEqual(approvedArgs, runtimeArgs)
}

func passiveStringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// PassiveObservation records a pre-intent observation only. It is not a stop
// receipt and cannot authorize fencing until bound to a fresh StopRequest.
type PassiveObservation struct {
	Version           int    `json:"version"`
	RuntimeHash       string `json:"runtimeHash"`
	ScopeDigest       string `json:"scopeDigest"`
	TargetPIDExited   bool   `json:"targetPIDExited"`
	TargetCgroupGone  bool   `json:"targetCgroupGone"`
	SandboxPIDExited  bool   `json:"sandboxPIDExited"`
	SandboxCgroupGone bool   `json:"sandboxCgroupGone"`
	ObservedAt        string `json:"observedAt"`
}

// PassiveReceipt is distinct from a CRI stop receipt: it makes no claim that
// StopContainer or StopPodSandbox was acknowledged or that CRI reports NotReady.
type PassiveReceipt struct {
	Mode        string                 `json:"mode"`
	Runtime     fencing.RuntimeBinding `json:"runtime"`
	Request     fencing.StopRequest    `json:"request"`
	Observation PassiveObservation     `json:"observation"`
}

func passiveScopeDigest(scope CgroupScope) (string, error) {
	data, err := json.Marshal(scope)
	if err != nil {
		return "", err
	}
	return rawScopeDigest(data), nil
}

func ValidatePassiveScope(scope CgroupScope) error {
	p := scope.Passive
	if p == nil || p.Version != 1 || p.SandboxPID == 0 || p.SandboxStart == 0 || p.SandboxPath == "" ||
		p.SandboxDev == 0 || p.SandboxIno == 0 || p.SandboxPIDNSDev == 0 || p.SandboxPIDNSIno == 0 ||
		p.TargetPIDNSDev == 0 || p.TargetPIDNSIno == 0 || p.SandboxImage == "" ||
		!validOCIDigest("sha256:"+p.SandboxExecutableSHA256) || len(p.SandboxArgs) == 0 ||
		p.RuntimeHash != scope.RuntimeHash || p.ProfileID == "" || p.ProfileID != scope.ProfileID {
		return fmt.Errorf("%w: passive terminal scope is incomplete or mismatched", ErrUnverified)
	}
	for _, arg := range p.SandboxArgs {
		if arg == "" || strings.ContainsRune(arg, 0) {
			return fmt.Errorf("%w: passive sandbox arguments are invalid", ErrUnverified)
		}
	}
	return nil
}

func ValidatePassiveObservation(binding fencing.RuntimeBinding, scope CgroupScope, observation PassiveObservation) error {
	digest, err := passiveScopeDigest(scope)
	if err != nil || ValidatePassiveScope(scope) != nil || observation.Version != 1 ||
		observation.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || observation.ScopeDigest != digest ||
		!observation.TargetPIDExited || !observation.TargetCgroupGone || !observation.SandboxPIDExited || !observation.SandboxCgroupGone ||
		observation.ObservedAt == "" {
		return fmt.Errorf("%w: passive terminal observation is incomplete or mismatched", ErrUnverified)
	}
	if _, err := time.Parse(time.RFC3339Nano, observation.ObservedAt); err != nil {
		return fmt.Errorf("%w: passive terminal observation time is invalid", ErrUnverified)
	}
	return nil
}

func bindPassiveReceipt(binding fencing.RuntimeBinding, request fencing.StopRequest, scope CgroupScope, observation PassiveObservation) (json.RawMessage, error) {
	if binding.Validate(request.Identity) != nil || request.OperationID == "" || !validOCIDigest("sha256:"+request.RequestHash) || ValidatePassiveObservation(binding, scope, observation) != nil {
		return nil, fmt.Errorf("%w: passive terminal receipt binding differs", ErrUnverified)
	}
	receipt := PassiveReceipt{Mode: "passive-kernel-v1", Runtime: binding, Request: request, Observation: observation}
	data, err := json.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("%w: encode passive terminal receipt: %v", ErrUnverified, err)
	}
	return data, nil
}

// PassiveTerminalBackend is an optional backend extension. Implementations
// must observe only; they must never issue a CRI stop from these methods.
type PassiveTerminalBackend interface {
	ObservePassiveTerminal(context.Context, fencing.RuntimeBinding, json.RawMessage) (PassiveObservation, bool, error)
	BindPassiveTerminal(fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage, PassiveObservation) (json.RawMessage, error)
	CompletePassiveTerminal(fencing.RuntimeBinding, json.RawMessage)
}

func rawScopeDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
