package cri

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// containerdVerbosePID parses the pinned containerd CRI verbose-info shape.
// CRI itself leaves Info runtime-specific, so callers must first require the
// exact configured containerd runtime version and runtime type.
func containerdVerbosePID(info map[string]string, sandboxID, runtimeType string) (uint32, error) {
	_, pid, err := containerdVerboseIdentity(info, sandboxID, runtimeType)
	return pid, err
}

func containerdVerboseIdentity(info map[string]string, sandboxID, runtimeType string) (string, uint32, error) {
	identity, err := containerdVerboseRuntimeIdentity(info, sandboxID, runtimeType)
	return identity.SandboxID, identity.PID, err
}

type containerdRuntimeIdentity struct {
	SandboxID   string
	PID         uint32
	CgroupsPath string
}

func containerdVerboseRuntimeIdentity(info map[string]string, sandboxID, runtimeType string) (containerdRuntimeIdentity, error) {
	if len(info) != 1 || info["info"] == "" || sandboxID == "" || runtimeType == "" {
		if sandboxID == "" && len(info) == 1 && info["info"] != "" && runtimeType != "" {
			// Enrollment discovers the sandbox only from this strict, exact schema.
		} else {
			return containerdRuntimeIdentity{}, fmt.Errorf("%w: containerd verbose info is missing or ambiguous", ErrUnverified)
		}
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewBufferString(info["info"]))
	token, err := decoder.Token()
	if err != nil {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: invalid containerd verbose info: %w", ErrUnverified, err)
	}
	if token != json.Delim('{') {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: containerd verbose info is not an object", ErrUnverified)
	}
	fields = make(map[string]json.RawMessage, 9)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return containerdRuntimeIdentity{}, fmt.Errorf("%w: invalid containerd verbose info key", ErrUnverified)
		}
		if _, exists := fields[key]; exists {
			return containerdRuntimeIdentity{}, fmt.Errorf("%w: duplicate containerd verbose info field", ErrUnverified)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return containerdRuntimeIdentity{}, fmt.Errorf("%w: invalid containerd verbose info value", ErrUnverified)
		}
		fields[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: incomplete containerd verbose info", ErrUnverified)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: trailing containerd verbose info", ErrUnverified)
	}
	want := []string{"sandboxID", "pid", "removing", "snapshotKey", "snapshotter", "runtimeType", "runtimeOptions", "config", "runtimeSpec"}
	if len(fields) != len(want) {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: containerd verbose info schema differs", ErrUnverified)
	}
	for _, key := range want {
		if len(fields[key]) == 0 {
			return containerdRuntimeIdentity{}, fmt.Errorf("%w: containerd verbose info field %q is missing", ErrUnverified, key)
		}
	}
	var got struct {
		SandboxID   string          `json:"sandboxID"`
		PID         uint32          `json:"pid"`
		Removing    bool            `json:"removing"`
		RuntimeType string          `json:"runtimeType"`
		RuntimeSpec json.RawMessage `json:"runtimeSpec"`
	}
	if err := json.Unmarshal([]byte(info["info"]), &got); err != nil || got.SandboxID == "" || got.PID == 0 || got.Removing || got.RuntimeType != runtimeType || (sandboxID != "" && got.SandboxID != sandboxID) {
		return containerdRuntimeIdentity{}, fmt.Errorf("%w: containerd verbose identity differs", ErrUnverified)
	}
	cgroupsPath, err := runtimeSpecCgroupsPath(got.RuntimeSpec)
	if err != nil {
		return containerdRuntimeIdentity{}, err
	}
	return containerdRuntimeIdentity{SandboxID: got.SandboxID, PID: got.PID, CgroupsPath: cgroupsPath}, nil
}

func containerdPIDFromBinding(binding fencing.RuntimeBinding, responseInfo map[string]string, expectedRuntimeType string) (uint32, error) {
	return containerdVerbosePID(responseInfo, binding.SandboxID, expectedRuntimeType)
}

func runtimeSpecCgroupsPath(runtimeSpec json.RawMessage) (string, error) {
	runtimeFields, err := uniqueJSONFields(runtimeSpec)
	if err != nil {
		return "", fmt.Errorf("%w: invalid containerd runtime spec", ErrUnverified)
	}
	linux, ok := runtimeFields["linux"]
	if !ok {
		return "", fmt.Errorf("%w: containerd runtime spec linux section is missing", ErrUnverified)
	}
	linuxFields, err := uniqueJSONFields(linux)
	if err != nil {
		return "", fmt.Errorf("%w: invalid containerd runtime spec linux section", ErrUnverified)
	}
	pathValue, ok := linuxFields["cgroupsPath"]
	if !ok {
		return "", fmt.Errorf("%w: containerd runtime spec cgroups path is missing", ErrUnverified)
	}
	var cgroupsPath string
	if err := json.Unmarshal(pathValue, &cgroupsPath); err != nil || cgroupsPath == "" {
		return "", fmt.Errorf("%w: containerd runtime spec cgroups path is invalid", ErrUnverified)
	}
	return cgroupsPath, nil
}

func uniqueJSONFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("expected object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, fmt.Errorf("invalid object key")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate object key")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("incomplete object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing object data")
	}
	return fields, nil
}

// containerdCgroupsPath maps only explicit OCI hierarchy-root absolute paths
// and the pinned containerd systemd slice:prefix:name form. Relative paths
// depend on runtime-specific working-directory semantics and are rejected.
func containerdCgroupsPath(value, containerID string) (string, error) {
	if containerID == "" || !safeCgroupComponent(containerID) {
		return "", fmt.Errorf("%w: containerd cgroups identity is invalid", ErrUnverified)
	}
	if strings.Contains(value, ":") {
		parts := strings.Split(value, ":")
		if len(parts) != 3 || parts[1] != "cri-containerd" || parts[2] != containerID {
			return "", fmt.Errorf("%w: containerd systemd cgroups identity differs", ErrUnverified)
		}
		slicePath, err := expandSystemdSlice(parts[0])
		if err != nil {
			return "", fmt.Errorf("%w: containerd systemd slice is invalid", ErrUnverified)
		}
		return strings.TrimPrefix(path.Join(slicePath, parts[1]+"-"+parts[2]+".scope"), "/"), nil
	}
	if !strings.HasPrefix(value, "/") || value == "/" || path.Clean(value) != value || strings.HasPrefix(value, "/sys/fs/cgroup/") {
		return "", fmt.Errorf("%w: containerd cgroups path is not a canonical hierarchy-root path", ErrUnverified)
	}
	for _, component := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if !safeCgroupComponent(component) {
			return "", fmt.Errorf("%w: containerd cgroups path component is invalid", ErrUnverified)
		}
	}
	base := path.Base(value)
	if base == "cri-containerd-"+containerID+".scope" {
		parent := strings.TrimPrefix(path.Dir(value), "/")
		if parent != "" {
			for _, component := range strings.Split(parent, "/") {
				if !strings.HasSuffix(component, ".slice") || !safeCgroupComponent(component) {
					return "", fmt.Errorf("%w: containerd systemd cgroups hierarchy is invalid", ErrUnverified)
				}
			}
		}
		return strings.TrimPrefix(value, "/"), nil
	}
	if base != containerID {
		return "", fmt.Errorf("%w: containerd cgroups path container identity differs", ErrUnverified)
	}
	return strings.TrimPrefix(value, "/"), nil
}

func expandSystemdSlice(slice string) (string, error) {
	if !strings.HasSuffix(slice, ".slice") || strings.ContainsAny(slice, "/\\:") {
		return "", fmt.Errorf("invalid systemd slice")
	}
	base := strings.TrimSuffix(slice, ".slice")
	if base == "-" {
		return "", nil
	}
	if base == "" {
		return "", fmt.Errorf("invalid systemd slice")
	}
	var components []string
	prefix := ""
	for _, part := range strings.Split(base, "-") {
		if !safeCgroupComponent(part) {
			return "", fmt.Errorf("invalid systemd slice")
		}
		unit := prefix + part + ".slice"
		components = append(components, unit)
		prefix += part + "-"
	}
	return path.Join(components...), nil
}

func safeCgroupComponent(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}
