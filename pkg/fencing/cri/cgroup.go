package cri

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// CgroupScope pins a host cgroup-v2 directory captured from the target
// container's init process. Path is only meaningful beneath the configured
// host cgroup root; callers must persist this value with the enrolled binding.
type CgroupScope struct {
	Path        string        `json:"path"`
	Dev         uint64        `json:"dev"`
	Ino         uint64        `json:"ino"`
	PID         uint32        `json:"pid"`
	StartTime   uint64        `json:"startTime"`
	RuntimeHash string        `json:"runtimeHash"`
	ProfileID   string        `json:"profileID"`
	Passive     *PassiveScope `json:"passive,omitempty"`
}

// captureCgroupScope resolves the pinned containerd runtimeSpec path beneath
// the fixed cgroup root and requires the CRI PID to be a direct member of that
// populated cgroup. procRoot/cgroupRoot come from fixed agent config.
func captureCgroupScope(procRoot, cgroupRoot string, pid uint32, cgroupsPath string, binding fencing.RuntimeBinding, profileID string) (CgroupScope, error) {
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if pid == 0 || profileID == "" || !filepath.IsAbs(procRoot) || !filepath.IsAbs(cgroupRoot) || binding.Validate(identity) != nil {
		return CgroupScope{}, fmt.Errorf("%w: invalid cgroup capture configuration", ErrUnverified)
	}
	startTime, err := processStartTime(procRoot, pid)
	if err != nil {
		return CgroupScope{}, fmt.Errorf("%w: read enrolled process identity: %w", ErrUnverified, err)
	}
	relative, err := containerdCgroupsPath(cgroupsPath, strings.TrimPrefix(binding.ContainerID, "containerd://"))
	if err != nil {
		return CgroupScope{}, err
	}
	root, err := filepath.EvalSymlinks(cgroupRoot)
	if err != nil {
		return CgroupScope{}, fmt.Errorf("%w: resolve cgroup root: %w", ErrUnverified, err)
	}
	path := filepath.Join(root, relative)
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !under(root, resolved) {
		return CgroupScope{}, fmt.Errorf("%w: resolve cgroup scope", ErrUnverified)
	}
	if err := cgroupContainsPID(resolved, pid); err != nil {
		return CgroupScope{}, fmt.Errorf("%w: CRI process is not a direct cgroup member", ErrUnverified)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return CgroupScope{}, fmt.Errorf("%w: cgroup directory unavailable", ErrUnverified)
	}
	dev, ino, ok := fileIdentity(info)
	if !ok {
		return CgroupScope{}, fmt.Errorf("%w: cgroup filesystem identity unavailable", ErrUnverified)
	}
	populated, err := cgroupPopulated(resolved)
	if err != nil || !populated {
		return CgroupScope{}, fmt.Errorf("%w: enrolled cgroup is not populated", ErrUnverified)
	}
	if current, err := processStartTime(procRoot, pid); err != nil || current != startTime {
		return CgroupScope{}, fmt.Errorf("%w: process changed during cgroup capture", ErrUnverified)
	}
	scope := CgroupScope{Path: relative, Dev: dev, Ino: ino, PID: pid, StartTime: startTime,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: profileID}
	if err := scope.VerifyProcess(procRoot, cgroupRoot); err != nil {
		return CgroupScope{}, err
	}
	return scope, nil
}

// VerifyEmpty confirms the same cgroup inode still exists and cgroup.events
// reports populated=0, which includes descendants under cgroup v2.
func (s CgroupScope) VerifyEmpty(cgroupRoot string) error {
	return s.verify(cgroupRoot, false)
}

// VerifyPopulated confirms the enrolled execution scope is still populated
// immediately before an exact CRI stop is attempted.
func (s CgroupScope) VerifyPopulated(cgroupRoot string) error {
	return s.verify(cgroupRoot, true)
}

// VerifyProcess requires the captured PID still to refer to the same kernel
// process incarnation. It is used before issuing a stop, never after exit.
func (s CgroupScope) VerifyProcess(procRoot, cgroupRoot string) error {
	if s.PID == 0 || s.StartTime == 0 || !filepath.IsAbs(procRoot) {
		return fmt.Errorf("%w: invalid enrolled process identity", ErrUnverified)
	}
	startTime, err := processStartTime(procRoot, s.PID)
	if err != nil || startTime != s.StartTime {
		return fmt.Errorf("%w: enrolled process identity changed", ErrUnverified)
	}
	if err := s.VerifyPopulated(cgroupRoot); err != nil {
		return err
	}
	if current, err := processStartTime(procRoot, s.PID); err != nil || current != s.StartTime {
		return fmt.Errorf("%w: enrolled process identity changed during cgroup verification", ErrUnverified)
	}
	return nil
}

func (s CgroupScope) verify(cgroupRoot string, wantPopulated bool) error {
	if s.Path == "" || !filepath.IsAbs(cgroupRoot) || filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path || s.Path == "." || strings.HasPrefix(s.Path, ".."+string(filepath.Separator)) || s.Path == ".." {
		return fmt.Errorf("%w: invalid enrolled cgroup scope", ErrUnverified)
	}
	root, err := filepath.EvalSymlinks(cgroupRoot)
	if err != nil {
		return fmt.Errorf("%w: resolve cgroup root: %w", ErrUnverified, err)
	}
	path, err := filepath.EvalSymlinks(filepath.Join(root, s.Path))
	if err != nil || !under(root, path) {
		return fmt.Errorf("%w: enrolled cgroup scope unavailable", ErrUnverified)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: enrolled cgroup scope unavailable: %w", ErrUnverified, err)
	}
	dev, ino, ok := fileIdentity(info)
	if !ok || dev != s.Dev || ino != s.Ino {
		return fmt.Errorf("%w: cgroup identity changed", ErrUnverified)
	}
	populated, err := cgroupPopulated(path)
	if err != nil || populated != wantPopulated {
		if wantPopulated {
			return fmt.Errorf("%w: enrolled cgroup is not populated", ErrUnverified)
		}
		return fmt.Errorf("%w: cgroup or descendant remains populated", ErrUnverified)
	}
	if wantPopulated {
		if s.PID == 0 {
			return fmt.Errorf("%w: enrolled cgroup process identity is missing", ErrUnverified)
		}
		if err := cgroupContainsPID(path, s.PID); err != nil {
			return fmt.Errorf("%w: enrolled process is not a direct cgroup member", ErrUnverified)
		}
	}
	return nil
}

func cgroupContainsPID(path string, pid uint32) error {
	if pid == 0 {
		return fmt.Errorf("zero PID")
	}
	data, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return err
	}
	count := 0
	for _, field := range strings.Fields(string(data)) {
		listed, err := strconv.ParseUint(field, 10, 32)
		if err != nil || listed == 0 {
			return fmt.Errorf("invalid cgroup process list")
		}
		if uint32(listed) == pid {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("direct cgroup membership is not unique")
	}
	return nil
}

func under(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func cgroupPopulated(path string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if ok && key == "populated" && (value == "0" || value == "1") {
			return value == "1", nil
		}
	}
	return false, fmt.Errorf("cgroup.events populated field missing")
}

func processStartTime(procRoot string, pid uint32) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "stat"))
	if err != nil {
		return 0, err
	}
	// comm may contain spaces and parentheses; fields after its final ')' start
	// at proc stat field 3, making starttime (field 22) index 19.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 || end+1 >= len(data) {
		return 0, fmt.Errorf("malformed proc stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		return 0, fmt.Errorf("truncated proc stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func processCgroupPath(procRoot string, pid uint32) (string, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "cgroup"))
	if err != nil {
		return "", fmt.Errorf("%w: read process cgroup: %w", ErrUnverified, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "0::/") {
			continue
		}
		relative := strings.TrimPrefix(line, "0::/")
		if reason := cgroupPathRejection(relative); reason != "" {
			if reason == cgroupPathTraversal {
				return "", &cgroupPathTraversalError{namespaceRelation: cgroupNamespaceRelation(procRoot, pid)}
			}
			return "", fmt.Errorf("%w: unified cgroup path is unsafe (%s)", ErrUnverified, reason)
		}
		return relative, nil
	}
	return "", fmt.Errorf("%w: unified cgroup path missing", ErrUnverified)
}

// cgroupPathTraversalError preserves the fail-closed path rejection while
// exposing only a bounded namespace-relationship category to diagnostics.
type cgroupPathTraversalError struct {
	namespaceRelation string
}

func (e *cgroupPathTraversalError) Error() string {
	return "fencing: runtime state unverified: unified cgroup path is unsafe (traversal)"
}

func (e *cgroupPathTraversalError) Unwrap() error { return ErrUnverified }

func (e *cgroupPathTraversalError) CgroupNamespaceDiagnosticCode() string {
	return e.namespaceRelation
}

func cgroupNamespaceRelation(procRoot string, pid uint32) string {
	agent, _ := os.Stat("/proc/self/ns/cgroup")
	host, _ := os.Stat(filepath.Join(procRoot, "1", "ns", "cgroup"))
	target, _ := os.Stat(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "ns", "cgroup"))
	return classifyCgroupNamespaceRelation(agent, host, target)
}

func classifyCgroupNamespaceRelation(agent, host, target os.FileInfo) string {
	missing := 0
	if agent == nil {
		missing++
	}
	if host == nil {
		missing++
	}
	if target == nil {
		missing++
	}
	if missing == 1 {
		if agent == nil {
			return "cgroup_namespace_agent_unavailable"
		}
		if host == nil {
			return "cgroup_namespace_host_unavailable"
		}
		return "cgroup_namespace_target_unavailable"
	}
	if missing > 1 {
		return "cgroup_namespace_multiple_unavailable"
	}
	agentHost := os.SameFile(agent, host)
	agentTarget := os.SameFile(agent, target)
	hostTarget := os.SameFile(host, target)
	switch {
	case agentHost && agentTarget:
		return "cgroup_namespace_all_same"
	case agentHost:
		return "cgroup_namespace_agent_host_same_target_other"
	case agentTarget:
		return "cgroup_namespace_agent_target_same_host_other"
	case hostTarget:
		return "cgroup_namespace_host_target_same_agent_other"
	default:
		return "cgroup_namespace_all_different"
	}
}

// cgroupPathRejection returns a non-sensitive category for a rejected unified
// path. Keep these checks fail-closed: callers use the accepted value beneath
// the configured host cgroup root.
type cgroupPathRejectionReason string

const (
	cgroupPathRoot         cgroupPathRejectionReason = "root"
	cgroupPathAbsolute     cgroupPathRejectionReason = "absolute"
	cgroupPathDot          cgroupPathRejectionReason = "dot"
	cgroupPathNoncanonical cgroupPathRejectionReason = "noncanonical"
	cgroupPathTraversal    cgroupPathRejectionReason = "traversal"
)

func cgroupPathRejection(path string) cgroupPathRejectionReason {
	if path == "" {
		return cgroupPathRoot
	}
	if filepath.IsAbs(path) {
		return cgroupPathAbsolute
	}
	for _, component := range strings.Split(path, string(filepath.Separator)) {
		switch component {
		case "..":
			return cgroupPathTraversal
		case ".":
			return cgroupPathDot
		}
	}
	if filepath.Clean(path) != path {
		return cgroupPathNoncanonical
	}
	return ""
}

func fileIdentity(info os.FileInfo) (uint64, uint64, bool) {
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	devField, inoField := value.FieldByName("Dev"), value.FieldByName("Ino")
	if !devField.IsValid() || !inoField.IsValid() || !devField.CanUint() || !inoField.CanUint() {
		return 0, 0, false
	}
	return devField.Uint(), inoField.Uint(), true
}
