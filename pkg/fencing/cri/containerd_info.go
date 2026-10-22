package cri

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// containerdVerbosePID parses the pinned containerd CRI verbose-info shape.
// CRI itself leaves Info runtime-specific, so callers must first require the
// exact configured containerd runtime version and runtime type.
func containerdVerbosePID(info map[string]string, sandboxID, runtimeType string) (uint32, error) {
	if len(info) != 1 || info["info"] == "" || sandboxID == "" || runtimeType == "" {
		return 0, fmt.Errorf("%w: containerd verbose info is missing or ambiguous", ErrUnverified)
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewBufferString(info["info"]))
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("%w: invalid containerd verbose info: %w", ErrUnverified, err)
	}
	if token != json.Delim('{') {
		return 0, fmt.Errorf("%w: containerd verbose info is not an object", ErrUnverified)
	}
	fields = make(map[string]json.RawMessage, 9)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return 0, fmt.Errorf("%w: invalid containerd verbose info key", ErrUnverified)
		}
		if _, exists := fields[key]; exists {
			return 0, fmt.Errorf("%w: duplicate containerd verbose info field", ErrUnverified)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return 0, fmt.Errorf("%w: invalid containerd verbose info value", ErrUnverified)
		}
		fields[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return 0, fmt.Errorf("%w: incomplete containerd verbose info", ErrUnverified)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0, fmt.Errorf("%w: trailing containerd verbose info", ErrUnverified)
	}
	want := []string{"sandboxID", "pid", "removing", "snapshotKey", "snapshotter", "runtimeType", "runtimeOptions", "config", "runtimeSpec"}
	if len(fields) != len(want) {
		return 0, fmt.Errorf("%w: containerd verbose info schema differs", ErrUnverified)
	}
	for _, key := range want {
		if len(fields[key]) == 0 {
			return 0, fmt.Errorf("%w: containerd verbose info field %q is missing", ErrUnverified, key)
		}
	}
	var got struct {
		SandboxID   string `json:"sandboxID"`
		PID         uint32 `json:"pid"`
		Removing    bool   `json:"removing"`
		RuntimeType string `json:"runtimeType"`
	}
	if err := json.Unmarshal([]byte(info["info"]), &got); err != nil || got.SandboxID != sandboxID || got.PID == 0 || got.Removing || got.RuntimeType != runtimeType {
		return 0, fmt.Errorf("%w: containerd verbose identity differs", ErrUnverified)
	}
	return got.PID, nil
}

func containerdPIDFromBinding(binding fencing.RuntimeBinding, responseInfo map[string]string, expectedRuntimeType string) (uint32, error) {
	return containerdVerbosePID(responseInfo, binding.SandboxID, expectedRuntimeType)
}
