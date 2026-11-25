//go:build darwin

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

var cloneLocalSnapshot = unix.Fclonefileat
var sanitizeLocalSnapshotMetadata = sanitizeLocalCloneMetadata
var removeLocalCloneMetadataProbe = os.Remove
var errLocalCloneMetadataProbe = errors.New("Local checkpoint clone metadata probe failed")

// cloneAndHashLocalSnapshot clones into the private staging directory without
// moving or modifying the materializer-owned source snapshot.
func cloneAndHashLocalSnapshot(ctx context.Context, sourcePath, targetPath string, maxBytes uint64) (uint64, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, "", true, err
	}
	sourceFD, err := unix.Open(sourcePath, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, "", true, err
	}
	defer unix.Close(sourceFD)
	var sourceInfo unix.Stat_t
	if err := unix.Fstat(sourceFD, &sourceInfo); err != nil {
		return 0, "", true, err
	}
	if sourceInfo.Mode&unix.S_IFMT != unix.S_IFREG || sourceInfo.Size < 0 || uint64(sourceInfo.Size) > maxBytes {
		return 0, "", true, fmt.Errorf("invalid or oversized Local checkpoint source")
	}
	dirFD, err := unix.Open(filepath.Dir(targetPath), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, "", true, err
	}
	defer unix.Close(dirFD)
	name := filepath.Base(targetPath)
	err = cloneLocalSnapshot(sourceFD, dirFD, name, unix.CLONE_NOOWNERCOPY)
	if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENOSYS) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", true, err
	}
	cloneFD, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, "", true, err
	}
	clone := os.NewFile(uintptr(cloneFD), targetPath)
	defer clone.Close()
	var cloneInfo unix.Stat_t
	if err := unix.Fstat(cloneFD, &cloneInfo); err != nil {
		return 0, "", true, err
	}
	if cloneInfo.Mode&unix.S_IFMT != unix.S_IFREG || cloneInfo.Nlink != 1 || (cloneInfo.Dev == sourceInfo.Dev && cloneInfo.Ino == sourceInfo.Ino) || cloneInfo.Size != sourceInfo.Size {
		return 0, "", true, fmt.Errorf("invalid Local checkpoint clone identity or size")
	}
	if err := sanitizeLocalSnapshotMetadata(cloneFD, filepath.Dir(targetPath)); err != nil {
		if errors.Is(err, unix.EIO) || errors.Is(err, unix.ENOSPC) || errors.Is(err, errLocalCloneMetadataProbe) {
			return 0, "", true, err
		}
		if closeErr := clone.Close(); closeErr != nil {
			return 0, "", true, closeErr
		}
		if unlinkErr := unix.Unlinkat(dirFD, name, 0); unlinkErr != nil {
			return 0, "", true, unlinkErr
		}
		return 0, "", false, nil
	}
	hash := sha256.New()
	count, err := io.Copy(hash, contextReader{ctx: ctx, reader: io.LimitReader(clone, int64(maxBytes)+1)})
	if err != nil {
		return 0, "", true, err
	}
	if uint64(count) > maxBytes || count != sourceInfo.Size {
		return 0, "", true, fmt.Errorf("Local checkpoint clone size changed")
	}
	if err := localCheckpointSyncFile(clone); err != nil {
		return 0, "", true, err
	}
	if err := clone.Close(); err != nil {
		return 0, "", true, err
	}
	return uint64(count), hex.EncodeToString(hash.Sum(nil)), true, nil
}

func sanitizeLocalCloneMetadata(fd int, dir string) error {
	if err := unix.Fchflags(fd, 0); err != nil {
		return err
	}
	if err := unix.Fchmod(fd, 0600); err != nil {
		return err
	}
	probe, err := os.CreateTemp(dir, ".clone-metadata-")
	if err != nil {
		return fmt.Errorf("%w: create: %w", errLocalCloneMetadataProbe, err)
	}
	probePath := probe.Name()
	baseline, metadataErr := localFDXattrs(int(probe.Fd()))
	closeErr := probe.Close()
	removeErr := removeLocalCloneMetadataProbe(probePath)
	if closeErr != nil {
		return fmt.Errorf("%w: close: %w", errLocalCloneMetadataProbe, closeErr)
	}
	if removeErr != nil {
		return fmt.Errorf("%w: remove: %w", errLocalCloneMetadataProbe, removeErr)
	}
	if metadataErr != nil {
		return metadataErr
	}
	actual, err := localFDXattrs(fd)
	if err != nil {
		return err
	}
	for name := range actual {
		if _, ok := baseline[name]; !ok {
			if err := unix.Fremovexattr(fd, name); err != nil {
				return err
			}
		}
	}
	actual, err = localFDXattrs(fd)
	if err != nil {
		return err
	}
	if len(actual) != len(baseline) {
		return fmt.Errorf("Local checkpoint clone extended attributes differ from a private file")
	}
	for name, value := range baseline {
		got, ok := actual[name]
		if !ok || !bytes.Equal(value, got) {
			return fmt.Errorf("Local checkpoint clone extended attributes differ from a private file")
		}
	}
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		return err
	}
	if info.Flags != 0 || info.Mode&07777 != 0600 || info.Uid != uint32(unix.Geteuid()) {
		return fmt.Errorf("Local checkpoint clone metadata differs from a private file")
	}
	return nil
}

func localFDXattrs(fd int) (map[string][]byte, error) {
	size, err := unix.Flistxattr(fd, nil)
	if err != nil {
		return nil, err
	}
	if size > 1<<20 {
		return nil, fmt.Errorf("Local checkpoint clone metadata exceeds bound")
	}
	result := make(map[string][]byte)
	if size == 0 {
		return result, nil
	}
	names := make([]byte, size)
	n, err := unix.Flistxattr(fd, names)
	if err != nil {
		return nil, err
	}
	var total int
	for _, name := range bytes.Split(names[:n], []byte{0}) {
		if len(name) == 0 {
			continue
		}
		size, err := unix.Fgetxattr(fd, string(name), nil)
		if err != nil {
			return nil, err
		}
		if size > (1<<20)-total {
			return nil, fmt.Errorf("Local checkpoint clone metadata exceeds bound")
		}
		total += size
		value := make([]byte, size)
		n, err := unix.Fgetxattr(fd, string(name), value)
		if err != nil {
			return nil, err
		}
		result[string(name)] = value[:n]
	}
	return result, nil
}
