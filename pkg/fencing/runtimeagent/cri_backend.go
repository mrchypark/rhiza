package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
)

type CRIBackend struct {
	Provider   *cri.Provider
	ProcRoot   string
	CgroupRoot string
}

func (b *CRIBackend) Enroll(ctx context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error) {
	if b == nil || b.Provider == nil {
		return fencing.RuntimeBinding{}, nil, fmt.Errorf("fencing: CRI provider missing")
	}
	binding, scope, err := b.Provider.EnrollBinding(ctx, base, b.ProcRoot, b.CgroupRoot)
	if err != nil {
		return fencing.RuntimeBinding{}, nil, err
	}
	data, err := json.Marshal(scope)
	return binding, data, err
}

func (b *CRIBackend) Stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, scopeData json.RawMessage) (json.RawMessage, error) {
	scope, err := decodeScope(scopeData)
	if err != nil {
		return nil, err
	}
	if b == nil || b.Provider == nil {
		return nil, fmt.Errorf("fencing: CRI provider missing")
	}
	observation, err := b.Provider.StopInCgroup(ctx, binding, request, b.ProcRoot, b.CgroupRoot, scope)
	if err != nil {
		return nil, err
	}
	return encodeObservation(observation, binding, request, scope)
}

func (b *CRIBackend) ResumeStop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, scopeData json.RawMessage) (json.RawMessage, error) {
	scope, err := decodeScope(scopeData)
	if err != nil {
		return nil, err
	}
	if b == nil || b.Provider == nil {
		return nil, fmt.Errorf("fencing: CRI provider missing")
	}
	observation, err := b.Provider.ResumeStopInCgroup(ctx, binding, request, b.ProcRoot, b.CgroupRoot, scope)
	if err != nil {
		return nil, err
	}
	return encodeObservation(observation, binding, request, scope)
}

func (b *CRIBackend) ObservePassiveTerminal(ctx context.Context, binding fencing.RuntimeBinding, scopeData json.RawMessage) (cri.PassiveObservation, bool, error) {
	scope, err := decodeScope(scopeData)
	if err != nil {
		return cri.PassiveObservation{}, false, err
	}
	if b == nil || b.Provider == nil {
		return cri.PassiveObservation{}, false, fmt.Errorf("fencing: CRI provider missing")
	}
	return b.Provider.ObservePassiveTerminal(ctx, binding, scope)
}

func (b *CRIBackend) BindPassiveTerminal(binding fencing.RuntimeBinding, request fencing.StopRequest, scopeData json.RawMessage, observation cri.PassiveObservation) (json.RawMessage, error) {
	scope, err := decodeScope(scopeData)
	if err != nil {
		return nil, err
	}
	if b == nil || b.Provider == nil {
		return nil, fmt.Errorf("fencing: CRI provider missing")
	}
	return b.Provider.BindPassiveTerminal(binding, request, scope, observation)
}

func (b *CRIBackend) CompletePassiveTerminal(binding fencing.RuntimeBinding, scopeData json.RawMessage) {
	scope, err := decodeScope(scopeData)
	if err == nil && b != nil && b.Provider != nil {
		b.Provider.CompletePassiveTerminal(binding, scope)
	}
}

func decodeScope(scopeData json.RawMessage) (cri.CgroupScope, error) {
	var scope cri.CgroupScope
	d := json.NewDecoder(bytes.NewReader(scopeData))
	d.DisallowUnknownFields()
	if err := d.Decode(&scope); err != nil {
		return cri.CgroupScope{}, fmt.Errorf("fencing: captured cgroup scope is invalid: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return cri.CgroupScope{}, fmt.Errorf("fencing: trailing cgroup scope")
	}
	return scope, nil
}

func encodeObservation(observation cri.StopObservation, binding fencing.RuntimeBinding, request fencing.StopRequest, scope cri.CgroupScope) (json.RawMessage, error) {
	completeStop := observation.ContainerStopAcknowledged && observation.SandboxStopAcknowledged
	recovered := observation.RecoveredTerminalState && !observation.ContainerStopAcknowledged && !observation.SandboxStopAcknowledged
	if observation.Runtime != binding || observation.Request != request || observation.CgroupScope != scope || !observation.CgroupEmpty ||
		!observation.ContainerExited || !observation.SandboxNotReady || observation.ObservedAt == "" || (!completeStop && !recovered) {
		return nil, fmt.Errorf("fencing: runtime did not produce a complete terminal observation")
	}
	return json.Marshal(observation)
}
