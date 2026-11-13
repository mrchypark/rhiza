// Package atomicdir provides descriptor-relative, no-replace directory moves.
package atomicdir

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// ErrUnsupported reports that no native no-replace rename is available.
var ErrUnsupported = errors.New("atomic directory rename unsupported")

// RenameNoReplace atomically moves oldPath to newPath without replacing an
// existing destination. Both paths must be clean absolute siblings. The parent
// path is opened component-by-component without following symlinks and remains
// held across validation, rename, inode verification, and parent fsync.
func RenameNoReplace(oldPath, newPath string) error {
	return renameNoReplaceWithHook(oldPath, newPath, nil)
}

func renameNoReplaceWithHook(oldPath, newPath string, hook func()) (resultErr error) {
	oldPath, err := cleanAbsolute(oldPath)
	if err != nil {
		return err
	}
	newPath, err = cleanAbsolute(newPath)
	if err != nil {
		return err
	}
	if filepath.Dir(oldPath) != filepath.Dir(newPath) {
		return errors.New("atomic directory rename requires sibling paths")
	}
	oldBase, newBase := filepath.Base(oldPath), filepath.Base(newPath)
	if !validBase(oldBase) || !validBase(newBase) || oldBase == newBase {
		return errors.New("invalid atomic directory basename")
	}
	parentFD, err := openDirectoryNoFollow(filepath.Dir(oldPath))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, unix.Close(parentFD)) }()

	before, err := statAt(parentFD, oldBase)
	if err != nil {
		return &os.LinkError{Op: "renameat", Old: oldPath, New: newPath, Err: err}
	}
	if before.mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("atomic directory rename source is not a directory")
	}
	if _, err := statAt(parentFD, newBase); err == nil {
		return &os.LinkError{Op: "renameat", Old: oldPath, New: newPath, Err: unix.EEXIST}
	} else if !errors.Is(err, unix.ENOENT) {
		return &os.LinkError{Op: "renameat", Old: oldPath, New: newPath, Err: err}
	}
	if hook != nil {
		hook()
	}
	if err := renameNoReplace(parentFD, oldBase, newBase); err != nil {
		return &os.LinkError{Op: "renameat", Old: oldPath, New: newPath, Err: err}
	}
	after, err := statAt(parentFD, newBase)
	if err != nil {
		return &os.LinkError{Op: "renameat", Old: oldPath, New: newPath, Err: errors.Join(errors.New("rename outcome is ambiguous"), err)}
	}
	if after.dev != before.dev || after.ino != before.ino || after.mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("atomic directory rename changed identity")
	}
	if _, err := statAt(parentFD, oldBase); !errors.Is(err, unix.ENOENT) {
		return errors.New("atomic directory rename source still present")
	}
	if err := unix.Fsync(parentFD); err != nil {
		return &os.LinkError{Op: "fsync", Old: oldPath, New: newPath, Err: err}
	}
	return nil
}

type fileIdentity struct {
	dev  uint64
	ino  uint64
	mode uint32
}

func statAt(parentFD int, base string) (fileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(parentFD, base, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fileIdentity{}, err
	}
	return fileIdentity{dev: uint64(st.Dev), ino: st.Ino, mode: uint32(st.Mode)}, nil
}

func cleanAbsolute(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return "", errors.New("atomic directory path is not clean and absolute")
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return "", errors.New("atomic directory path contains an invalid component")
		}
	}
	return path, nil
}

func validBase(base string) bool {
	return base != "" && base != "." && base != ".." && filepath.Base(base) == base
}

func openDirectoryNoFollow(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return -1, errors.New("directory path is not clean and absolute")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if path == "/" {
		components = nil
	}
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(fd)
			return -1, errors.New("directory path contains an invalid component")
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if openErr != nil || closeErr != nil {
			if next >= 0 {
				_ = unix.Close(next)
			}
			return -1, errors.Join(openErr, closeErr)
		}
		fd = next
	}
	return fd, nil
}
