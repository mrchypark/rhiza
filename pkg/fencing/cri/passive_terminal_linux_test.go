//go:build linux

package cri

import (
	"bytes"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPinnedCgroupIdentityUsesActualLinuxDeviceAndInode(t *testing.T) {
	fd, err := unix.Open(t.TempDir(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		t.Fatal(err)
	}
	dev, ino := pinnedCgroupIdentity(&stat)
	if dev != uint64(stat.Dev) || ino != stat.Ino {
		t.Fatalf("pinned identity swapped or changed actual Linux stat values: got dev=%d ino=%d, stat dev=%d ino=%d", dev, ino, stat.Dev, stat.Ino)
	}
}

func TestContainerdSandboxInfoUsesActualTopLevelImageAndRuntimeArgs(t *testing.T) {
	data := `{"pid":1,"processStatus":"running","image":"registry.example/pause@sha256:` + strings.Repeat("a", 64) + `","runtimeType":"io.containerd.runc.v2","config":{"image":{"image":"stale-config-tag"}},"runtimeSpec":{"process":{"args":["/pause"]},"linux":{"cgroupsPath":"kubepods/pause"}}}`
	got, err := parseContainerdSandboxInfo(data)
	if err != nil {
		t.Fatalf("parse pinned containerd SandboxInfo: %v", err)
	}
	if got.Image != "registry.example/pause@sha256:"+strings.Repeat("a", 64) || !passiveStringsEqual(got.Args, []string{"/pause"}) {
		t.Fatalf("parsed wrong containerd runtime identity: %+v", got)
	}
	if _, err := parseContainerdSandboxInfo(`{"pid":1,"runtimeType":"io.containerd.runc.v2","config":{},"runtimeSpec":{"process":{"args":["/pause"]},"linux":{"cgroupsPath":"kubepods/pause"}}}`); err == nil {
		t.Fatal("missing actual container.Info image was accepted")
	}
}

func TestSandboxExecutableDigestMatchesLiveProcExecutable(t *testing.T) {
	pid := uint32(os.Getpid())
	start, err := processStartTime("/proc", pid)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	staticErr := verifyStaticSandboxELF(file)
	digest, digestErr := sandboxExecutableDigest("/proc", pid, start)
	if staticErr != nil {
		if digestErr == nil {
			t.Fatal("dynamic live executable unexpectedly qualified as a passive sandbox")
		}
		return
	}
	if digestErr != nil {
		t.Fatalf("hash static current process executable through procfs: %v", digestErr)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	if digest != hex.EncodeToString(want[:]) {
		t.Fatalf("live executable digest mismatch: got %s want %s", digest, hex.EncodeToString(want[:]))
	}
	if _, err := sandboxExecutableDigest("/proc", pid, start+1); err == nil {
		t.Fatal("changed PID start time was accepted")
	}
}

func TestVerifyStaticSandboxELFRequiresNoInterpreterOrExternalDependencies(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{name: "static executable", data: sandboxELF32(elf.ET_EXEC, nil), want: true},
		{name: "static pie with relocation table", data: sandboxELF32(elf.ET_DYN, []elf.DynTag{elf.DT_RELA, elf.DT_NULL}, elf.PT_DYNAMIC), want: true},
		{name: "interpreter", data: sandboxELF32(elf.ET_EXEC, nil, elf.PT_INTERP), want: false},
		{name: "needed library without interpreter", data: sandboxELF32(elf.ET_DYN, []elf.DynTag{elf.DT_NEEDED, elf.DT_NULL}, elf.PT_DYNAMIC), want: false},
		{name: "external runpath without interpreter", data: sandboxELF32(elf.ET_DYN, []elf.DynTag{elf.DT_RUNPATH, elf.DT_NULL}, elf.PT_DYNAMIC), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := verifyStaticSandboxELF(bytes.NewReader(tt.data)); (err == nil) != tt.want {
				t.Fatalf("verifyStaticSandboxELF() error = %v, want success=%t", err, tt.want)
			}
		})
	}
}

func sandboxELF32(fileType elf.Type, dynamicTags []elf.DynTag, programType ...elf.ProgType) []byte {
	const headerSize, programHeaderSize = 52, 32
	data := make([]byte, headerSize)
	copy(data[:4], []byte{0x7f, 'E', 'L', 'F'})
	data[4], data[5], data[6] = byte(elf.ELFCLASS32), byte(elf.ELFDATA2LSB), byte(elf.EV_CURRENT)
	order := binary.LittleEndian
	order.PutUint16(data[16:18], uint16(fileType))
	order.PutUint16(data[18:20], uint16(elf.EM_386))
	order.PutUint32(data[20:24], uint32(elf.EV_CURRENT))
	order.PutUint32(data[24:28], 0x8048000)
	order.PutUint32(data[28:32], headerSize)
	order.PutUint16(data[40:42], headerSize)
	order.PutUint16(data[42:44], programHeaderSize)
	order.PutUint16(data[44:46], uint16(len(programType)))
	if len(programType) == 0 {
		return data
	}
	ph := make([]byte, programHeaderSize)
	order.PutUint32(ph[0:4], uint32(programType[0]))
	order.PutUint32(ph[4:8], headerSize+programHeaderSize)
	switch programType[0] {
	case elf.PT_INTERP:
		payload := []byte("/lib/ld.so\x00")
		order.PutUint32(ph[16:20], uint32(len(payload)))
		order.PutUint32(ph[20:24], uint32(len(payload)))
		data = append(data, ph...)
		return append(data, payload...)
	case elf.PT_DYNAMIC:
		payload := make([]byte, len(dynamicTags)*8)
		for i, tag := range dynamicTags {
			order.PutUint32(payload[i*8:i*8+4], uint32(int32(tag)))
		}
		order.PutUint32(ph[16:20], uint32(len(payload)))
		order.PutUint32(ph[20:24], uint32(len(payload)))
		data = append(data, ph...)
		return append(data, payload...)
	default:
		panic("unsupported fixture program type")
	}
}
