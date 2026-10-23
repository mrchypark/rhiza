//go:build linux

package cri

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"golang.org/x/sys/unix"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type passiveSession struct {
	targetPIDFD, sandboxPIDFD   int
	targetCgroup, sandboxCgroup int
	cgroupDev                   uint64
	targetIno, sandboxIno       uint64
	runtimeHash                 string
	scopeDigest                 string
	closed                      bool
	mu                          sync.Mutex
}

type sandboxRuntimeIdentity struct {
	PID         uint32
	RuntimeType string
	CgroupsPath string
	Image       string
	Args        []string
}

func (p *Provider) capturePassiveScope(ctx context.Context, binding fencing.RuntimeBinding, procRoot, cgroupRoot string, targetPID uint32, targetPath string, scope *CgroupScope) error {
	targetID, ok := strings.CutPrefix(binding.ContainerID, "containerd://")
	if !ok || targetID == "" {
		return fmt.Errorf("%w: target container identity is not containerd-qualified", ErrUnverified)
	}
	expectedTargetPath, err := containerdCgroupsPath(targetPath, targetID)
	if err != nil || expectedTargetPath != scope.Path {
		return fmt.Errorf("%w: target cgroup differs from verbose runtime identity", ErrUnverified)
	}
	response, err := p.client.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: binding.SandboxID, Verbose: true})
	if err != nil || response == nil || response.Status == nil || response.Status.Id != binding.SandboxID || len(response.Info) != 1 || response.Info["info"] == "" {
		return fmt.Errorf("%w: verbose sandbox identity unavailable", ErrUnverified)
	}
	sandbox, err := parseContainerdSandboxInfo(response.Info["info"])
	if err != nil || sandbox.PID == 0 || sandbox.RuntimeType != p.profile.ContainerdRuntimeType || sandbox.Image == "" ||
		!validPassiveSandboxProfile(p.profile.SandboxExecutableSHA256, p.profile.SandboxArgs, sandbox.Args) {
		return fmt.Errorf("%w: verbose sandbox identity differs", ErrUnverified)
	}
	sandboxPath, err := containerdCgroupsPath(sandbox.CgroupsPath, binding.SandboxID)
	if err != nil {
		return err
	}
	args := sandbox.Args
	actualArgs, err := processArguments(procRoot, sandbox.PID)
	if err != nil || !passiveStringsEqual(args, actualArgs) {
		return fmt.Errorf("%w: sandbox process arguments differ from runtime spec", ErrUnverified)
	}
	targetNS, err := processNamespaceIdentity(procRoot, targetPID, "pid")
	if err != nil {
		return err
	}
	sandboxNS, err := processNamespaceIdentity(procRoot, sandbox.PID, "pid")
	if err != nil {
		return err
	}
	if targetNS == sandboxNS {
		return fmt.Errorf("%w: target and sandbox PID namespaces are not isolated", ErrUnverified)
	}
	if err := verifyPassiveProcessProfile(procRoot, targetPID, true); err != nil {
		return err
	}
	if err := verifyPassiveProcessProfile(procRoot, sandbox.PID, false); err != nil {
		return err
	}
	targetFD, targetDev, targetIno, err := openPinnedCgroup(cgroupRoot, scope.Path, scope.Dev, scope.Ino)
	if err != nil {
		return err
	}
	sandboxFD, sandboxDev, sandboxIno, err := openPinnedCgroup(cgroupRoot, sandboxPath, 0, 0)
	if err != nil {
		_ = unix.Close(targetFD)
		return err
	}
	if targetDev != sandboxDev {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		return fmt.Errorf("%w: target and sandbox cgroup filesystems differ", ErrUnverified)
	}
	sandboxFDPath := fmt.Sprintf("/proc/self/fd/%d", sandboxFD)
	if err := cgroupContainsPID(sandboxFDPath, sandbox.PID); err != nil {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		return fmt.Errorf("%w: sandbox process is outside captured cgroup", ErrUnverified)
	}
	if populated, err := cgroupPopulated(sandboxFDPath); err != nil || !populated {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		return fmt.Errorf("%w: sandbox cgroup is not populated", ErrUnverified)
	}
	targetPIDFD, err := pinPID(procRoot, targetPID, scope.StartTime)
	if err != nil {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		return err
	}
	sandboxStart, err := processStartTime(procRoot, sandbox.PID)
	if err != nil {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		_ = unix.Close(targetPIDFD)
		return err
	}
	sandboxPIDFD, err := pinPID(procRoot, sandbox.PID, sandboxStart)
	if err != nil {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		_ = unix.Close(targetPIDFD)
		return err
	}
	sandboxExecutableSHA256, err := sandboxExecutableDigest(procRoot, sandbox.PID, sandboxStart)
	if err != nil || sandboxExecutableSHA256 != p.profile.SandboxExecutableSHA256 {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox executable is not administrator-qualified", ErrUnverified)
	}
	second, err := p.client.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: binding.SandboxID, Verbose: true})
	if err != nil || second == nil || second.Status == nil || second.Status.Id != binding.SandboxID ||
		second.Status.State != runtimeapi.PodSandboxState_SANDBOX_READY || len(second.Info) != 1 || second.Info["info"] == "" {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox identity changed during kernel pin", ErrUnverified)
	}
	secondSandbox, err := parseContainerdSandboxInfo(second.Info["info"])
	secondStart, startErr := processStartTime(procRoot, sandbox.PID)
	if err != nil || startErr != nil || secondSandbox.PID != sandbox.PID || secondSandbox.RuntimeType != sandbox.RuntimeType ||
		secondSandbox.CgroupsPath != sandbox.CgroupsPath || secondSandbox.Image != sandbox.Image ||
		!passiveStringsEqual(secondSandbox.Args, sandbox.Args) || secondStart != sandboxStart {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox process changed during kernel pin", ErrUnverified)
	}
	if secondSandbox.Image != sandbox.Image || !validPassiveSandboxProfile(p.profile.SandboxExecutableSHA256, p.profile.SandboxArgs, secondSandbox.Args) {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox identity changed during kernel pin", ErrUnverified)
	}
	secondExecutableSHA256, err := sandboxExecutableDigest(procRoot, sandbox.PID, sandboxStart)
	if err != nil || secondExecutableSHA256 != p.profile.SandboxExecutableSHA256 || secondExecutableSHA256 != sandboxExecutableSHA256 {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox executable changed during kernel pin", ErrUnverified)
	}
	secondArgs, err := processArguments(procRoot, sandbox.PID)
	if err != nil || !passiveStringsEqual(secondArgs, sandbox.Args) {
		closePassiveFDs(targetFD, sandboxFD, targetPIDFD, sandboxPIDFD)
		return fmt.Errorf("%w: sandbox arguments changed during kernel pin", ErrUnverified)
	}
	passive := &PassiveScope{Version: 1, SandboxPID: sandbox.PID, SandboxStart: sandboxStart, SandboxPath: sandboxPath,
		SandboxDev: sandboxDev, SandboxIno: sandboxIno, SandboxPIDNSDev: sandboxNS[0], SandboxPIDNSIno: sandboxNS[1],
		TargetPIDNSDev: targetNS[0], TargetPIDNSIno: targetNS[1], SandboxImage: sandbox.Image, SandboxExecutableSHA256: sandboxExecutableSHA256,
		SandboxArgs: append([]string(nil), args...), RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: scope.ProfileID}
	scope.Passive = passive
	data, err := json.Marshal(*scope)
	if err != nil {
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		_ = unix.Close(targetPIDFD)
		_ = unix.Close(sandboxPIDFD)
		return err
	}
	key := rawScopeDigest(data)
	p.passiveMu.Lock()
	if p.passive == nil {
		p.passive = make(map[string]*passiveSession)
	}
	if len(p.passive) >= 128 && p.passive[key] == nil {
		p.passiveMu.Unlock()
		_ = unix.Close(targetFD)
		_ = unix.Close(sandboxFD)
		_ = unix.Close(targetPIDFD)
		_ = unix.Close(sandboxPIDFD)
		return fmt.Errorf("%w: passive kernel handle capacity reached", ErrUnverified)
	}
	if old := p.passive[key]; old != nil {
		old.close()
	}
	p.passive[key] = &passiveSession{targetPIDFD: targetPIDFD, sandboxPIDFD: sandboxPIDFD, targetCgroup: targetFD, sandboxCgroup: sandboxFD,
		cgroupDev: targetDev, targetIno: targetIno, sandboxIno: sandboxIno, runtimeHash: passive.RuntimeHash, scopeDigest: key}
	p.passiveMu.Unlock()
	return nil
}

func closePassiveFDs(fds ...int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
}

func parseContainerdSandboxInfo(data string) (sandboxRuntimeIdentity, error) {
	fields, err := uniqueJSONFields([]byte(data))
	if err != nil {
		return sandboxRuntimeIdentity{}, fmt.Errorf("%w: invalid sandbox info", ErrUnverified)
	}
	allowed := map[string]bool{"pid": true, "processStatus": true, "netNamespaceClosed": true, "image": true, "snapshotKey": true, "snapshotter": true, "runtimeType": true, "runtimeOptions": true, "config": true, "runtimeSpec": true, "cniResult": true, "sandboxMetadata": true, "overhead": true, "resources": true}
	for key := range fields {
		if !allowed[key] {
			return sandboxRuntimeIdentity{}, fmt.Errorf("%w: unknown sandbox info field", ErrUnverified)
		}
	}
	for _, key := range []string{"pid", "runtimeType", "runtimeSpec", "config"} {
		if len(fields[key]) == 0 {
			return sandboxRuntimeIdentity{}, fmt.Errorf("%w: incomplete sandbox info", ErrUnverified)
		}
	}
	var result sandboxRuntimeIdentity
	if json.Unmarshal(fields["pid"], &result.PID) != nil || json.Unmarshal(fields["runtimeType"], &result.RuntimeType) != nil {
		return result, fmt.Errorf("%w: invalid sandbox identity", ErrUnverified)
	}
	result.CgroupsPath, err = runtimeSpecCgroupsPath(fields["runtimeSpec"])
	if err != nil {
		return result, err
	}
	if _, err := uniqueJSONFields(fields["config"]); err != nil {
		return result, fmt.Errorf("%w: invalid sandbox config", ErrUnverified)
	}
	if len(fields["image"]) == 0 || json.Unmarshal(fields["image"], &result.Image) != nil || result.Image == "" {
		return result, fmt.Errorf("%w: invalid runtime sandbox image reference", ErrUnverified)
	}
	spec, err := uniqueJSONFields(fields["runtimeSpec"])
	if err != nil {
		return result, fmt.Errorf("%w: invalid sandbox runtime spec", ErrUnverified)
	}
	process, err := uniqueJSONFields(spec["process"])
	if err != nil {
		return result, fmt.Errorf("%w: invalid sandbox process spec", ErrUnverified)
	}
	if json.Unmarshal(process["args"], &result.Args) != nil || len(result.Args) == 0 {
		return result, fmt.Errorf("%w: invalid sandbox arguments", ErrUnverified)
	}
	return result, nil
}

func processArguments(procRoot string, pid uint32) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "cmdline"))
	if err != nil || len(data) == 0 || data[len(data)-1] != 0 {
		return nil, fmt.Errorf("%w: process arguments unavailable", ErrUnverified)
	}
	parts := bytes.Split(data[:len(data)-1], []byte{0})
	result := make([]string, len(parts))
	for i, part := range parts {
		if len(part) == 0 {
			return nil, fmt.Errorf("%w: process arguments malformed", ErrUnverified)
		}
		result[i] = string(part)
	}
	return result, nil
}

func sandboxExecutableDigest(procRoot string, pid uint32, expectedStart uint64) (string, error) {
	start, err := processStartTime(procRoot, pid)
	if err != nil || start != expectedStart {
		return "", fmt.Errorf("%w: sandbox process identity changed", ErrUnverified)
	}
	path := filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "exe")
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("%w: sandbox executable unavailable", ErrUnverified)
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > 128<<20 {
		return "", fmt.Errorf("%w: sandbox executable identity unavailable", ErrUnverified)
	}
	if err := verifyStaticSandboxELF(f); err != nil {
		return "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("%w: sandbox executable rewind failed", ErrUnverified)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("%w: sandbox executable read failed", ErrUnverified)
	}
	after, err := os.Stat(path)
	startAfter, startErr := processStartTime(procRoot, pid)
	if err != nil || startErr != nil || startAfter != expectedStart || !os.SameFile(before, after) {
		return "", fmt.Errorf("%w: sandbox executable changed during verification", ErrUnverified)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyStaticSandboxELF(reader io.ReaderAt) error {
	file, err := elf.NewFile(reader)
	if err != nil || (file.Type != elf.ET_EXEC && file.Type != elf.ET_DYN) || file.Entry == 0 {
		return fmt.Errorf("%w: sandbox executable is not a static ELF executable", ErrUnverified)
	}
	for _, program := range file.Progs {
		switch program.Type {
		case elf.PT_INTERP:
			return fmt.Errorf("%w: sandbox executable requires an ELF interpreter", ErrUnverified)
		case elf.PT_DYNAMIC:
			if err := verifyStaticELFDynamic(program, file.Class, file.ByteOrder); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyStaticELFDynamic(program *elf.Prog, class elf.Class, order binary.ByteOrder) error {
	entrySize := int64(8)
	if class == elf.ELFCLASS64 {
		entrySize = 16
	} else if class != elf.ELFCLASS32 {
		return fmt.Errorf("%w: sandbox ELF dynamic format is unsupported", ErrUnverified)
	}
	if program.Filesz == 0 || program.Filesz%uint64(entrySize) != 0 {
		return fmt.Errorf("%w: sandbox ELF dynamic table is malformed", ErrUnverified)
	}
	entries := program.Open()
	var raw [16]byte
	terminated := false
	for remaining := program.Filesz; remaining >= uint64(entrySize); remaining -= uint64(entrySize) {
		if _, err := io.ReadFull(entries, raw[:entrySize]); err != nil {
			return fmt.Errorf("%w: sandbox ELF dynamic table is unreadable", ErrUnverified)
		}
		var tag elf.DynTag
		if class == elf.ELFCLASS32 {
			tag = elf.DynTag(int32(order.Uint32(raw[:4])))
		} else {
			tag = elf.DynTag(int64(order.Uint64(raw[:8])))
		}
		if tag == elf.DT_NULL {
			terminated = true
			break
		}
		switch tag {
		case elf.DT_NEEDED, elf.DT_CONFIG, elf.DT_RPATH, elf.DT_RUNPATH, elf.DT_AUDIT, elf.DT_DEPAUDIT,
			elf.DT_VERNEED, elf.DT_VERNEEDNUM, elf.DT_AUXILIARY, elf.DT_FILTER:
			return fmt.Errorf("%w: sandbox executable has an external ELF dependency", ErrUnverified)
		}
	}
	if !terminated {
		return fmt.Errorf("%w: sandbox ELF dynamic table is unterminated", ErrUnverified)
	}
	return nil
}

func processNamespaceIdentity(procRoot string, pid uint32, ns string) ([2]uint64, error) {
	var identity [2]uint64
	info, err := os.Stat(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "ns", ns))
	if err != nil {
		return identity, fmt.Errorf("%w: process namespace unavailable", ErrUnverified)
	}
	dev, ino, ok := fileIdentity(info)
	if !ok {
		return identity, fmt.Errorf("%w: process namespace identity unavailable", ErrUnverified)
	}
	identity = [2]uint64{dev, ino}
	return identity, nil
}

func verifyPassiveProcessProfile(procRoot string, pid uint32, target bool) error {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "status"))
	if err != nil {
		return fmt.Errorf("%w: process security status unavailable", ErrUnverified)
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[key] = strings.TrimSpace(value)
		}
	}
	if values["NoNewPrivs"] != "1" {
		return fmt.Errorf("%w: process no-new-privileges profile differs", ErrUnverified)
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		value, ok := values[key]
		n, e := strconv.ParseUint(value, 16, 64)
		if !ok || e != nil {
			return fmt.Errorf("%w: process capability profile unavailable", ErrUnverified)
		}
		if target && n != 0 {
			return fmt.Errorf("%w: target process has capabilities", ErrUnverified)
		}
		if !target && n&((uint64(1)<<21)|(uint64(1)<<19)) != 0 {
			return fmt.Errorf("%w: sandbox process has unsafe capabilities", ErrUnverified)
		}
	}
	mounts, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "mountinfo"))
	if err != nil {
		return fmt.Errorf("%w: process mount profile unavailable", ErrUnverified)
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			continue
		}
		right := strings.Fields(parts[1])
		left := strings.Fields(parts[0])
		if len(right) < 3 || len(left) < 6 || right[0] != "cgroup2" {
			continue
		}
		if !strings.Contains(","+left[5]+",", ",ro,") {
			return fmt.Errorf("%w: writable cgroup mount alias", ErrUnverified)
		}
	}
	return nil
}

func openPinnedCgroup(root, relative string, wantDev, wantIno uint64) (int, uint64, uint64, error) {
	if !filepath.IsAbs(root) || relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.HasPrefix(relative, "../") || relative == ".." {
		return -1, 0, 0, fmt.Errorf("%w: invalid cgroup pin path", ErrUnverified)
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, 0, 0, fmt.Errorf("%w: cgroup root unavailable", ErrUnverified)
	}
	defer unix.Close(rootFD)
	fd, err := unix.Openat2(rootFD, relative, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS})
	if err != nil {
		return -1, 0, 0, fmt.Errorf("%w: cgroup handle unavailable", ErrUnverified)
	}
	var stat unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &stat) != nil || unix.Fstatfs(fd, &fs) != nil || uint64(fs.Type) != uint64(unix.CGROUP2_SUPER_MAGIC) {
		unix.Close(fd)
		return -1, 0, 0, fmt.Errorf("%w: cgroup handle is not cgroup v2", ErrUnverified)
	}
	dev, ino := pinnedCgroupIdentity(&stat)
	if wantDev != 0 && (dev != wantDev || ino != wantIno) {
		unix.Close(fd)
		return -1, 0, 0, fmt.Errorf("%w: cgroup handle identity differs", ErrUnverified)
	}
	return fd, dev, ino, nil
}

func pinnedCgroupIdentity(stat *unix.Stat_t) (dev, ino uint64) {
	return uint64(stat.Dev), stat.Ino
}

func pinPID(procRoot string, pid uint32, start uint64) (int, error) {
	before, err := processStartTime(procRoot, pid)
	if err != nil || before != start {
		return -1, fmt.Errorf("%w: process incarnation changed", ErrUnverified)
	}
	fd, err := unix.PidfdOpen(int(pid), 0)
	if err != nil {
		return -1, fmt.Errorf("%w: process pin unavailable", ErrUnverified)
	}
	after, err := processStartTime(procRoot, pid)
	if err != nil || after != start {
		unix.Close(fd)
		return -1, fmt.Errorf("%w: process incarnation changed during pin", ErrUnverified)
	}
	return fd, nil
}

func (s *passiveSession) observe(binding fencing.RuntimeBinding, scope CgroupScope) (PassiveObservation, bool, error) {
	if s == nil {
		return PassiveObservation{}, false, fmt.Errorf("%w: passive kernel scope is missing", ErrUnverified)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || scope.Passive == nil || s.runtimeHash != fencing.RuntimeScopeBindingDigest(binding) {
		return PassiveObservation{}, false, fmt.Errorf("%w: passive kernel scope differs", ErrUnverified)
	}
	ready := func(fd int) (bool, error) {
		p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(p, 0)
		return n > 0 && p[0].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) != 0, err
	}
	targetExit, err := ready(s.targetPIDFD)
	if err != nil {
		return PassiveObservation{}, false, err
	}
	sandboxExit, err := ready(s.sandboxPIDFD)
	if err != nil {
		return PassiveObservation{}, false, err
	}
	targetGone, err := detachedCgroup(s.targetCgroup, s.cgroupDev, s.targetIno)
	if err != nil {
		return PassiveObservation{}, false, err
	}
	sandboxGone, err := detachedCgroup(s.sandboxCgroup, s.cgroupDev, s.sandboxIno)
	if err != nil {
		return PassiveObservation{}, false, err
	}
	if !targetExit || !sandboxExit || !targetGone || !sandboxGone {
		return PassiveObservation{}, false, nil
	}
	digest, err := passiveScopeDigest(scope)
	if err != nil {
		return PassiveObservation{}, false, err
	}
	return PassiveObservation{Version: 1, RuntimeHash: s.runtimeHash, ScopeDigest: digest, TargetPIDExited: true, TargetCgroupGone: true, SandboxPIDExited: true, SandboxCgroupGone: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}, true, nil
}

func detachedCgroup(fd int, dev, ino uint64) (bool, error) {
	var stat unix.Stat_t
	var fs unix.Statfs_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return false, err
	}
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return false, err
	}
	if uint64(fs.Type) != uint64(unix.CGROUP2_SUPER_MAGIC) || uint64(stat.Dev) != dev || stat.Ino != ino {
		return false, fmt.Errorf("%w: pinned cgroup identity changed", ErrUnverified)
	}
	return stat.Nlink == 0, nil
}

func (s *passiveSession) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, fd := range []int{s.targetPIDFD, s.sandboxPIDFD, s.targetCgroup, s.sandboxCgroup} {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
}
