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
	Path        string `json:"path"`
	Dev         uint64 `json:"dev"`
	Ino         uint64 `json:"ino"`
	PID         uint32 `json:"pid"`
	StartTime   uint64 `json:"startTime"`
	RuntimeHash string `json:"runtimeHash"`
	ProfileID   string `json:"profileID"`
}

// captureCgroupScope resolves a CRI-selected process's unified cgroup and
// requires it to be populated. procRoot/cgroupRoot come from fixed agent config.
func captureCgroupScope(procRoot, cgroupRoot string, pid uint32, binding fencing.RuntimeBinding, profileID string) (CgroupScope, error) {
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if pid == 0 || profileID == "" || !filepath.IsAbs(procRoot) || !filepath.IsAbs(cgroupRoot) || binding.Validate(identity) != nil {
		return CgroupScope{}, fmt.Errorf("%w: invalid cgroup capture configuration", ErrUnverified)
	}
	startTime, err := processStartTime(procRoot, pid)
	if err != nil {
		return CgroupScope{}, fmt.Errorf("%w: read enrolled process identity: %w", ErrUnverified, err)
	}
	relative, err := processCgroupPath(procRoot, pid)
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
	return CgroupScope{Path: relative, Dev: dev, Ino: ino, PID: pid, StartTime: startTime,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: profileID}, nil
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
	relative, err := processCgroupPath(procRoot, s.PID)
	if err != nil || relative != s.Path {
		return fmt.Errorf("%w: enrolled process moved to another cgroup", ErrUnverified)
	}
	if current, err := processStartTime(procRoot, s.PID); err != nil || current != s.StartTime {
		return fmt.Errorf("%w: enrolled process identity changed during cgroup verification", ErrUnverified)
	}
	return s.VerifyPopulated(cgroupRoot)
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
			return "", fmt.Errorf("%w: unified cgroup path is unsafe (%s)", ErrUnverified, reason)
		}
		return relative, nil
	}
	return "", fmt.Errorf("%w: unified cgroup path missing", ErrUnverified)
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
