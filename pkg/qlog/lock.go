package qlog

import (
	"fmt"
	"os"
	"syscall"
)

// LockFile prevents multiple processes from using the same WAL directory.
type LockFile struct {
	file *os.File
}

// Acquire acquires an exclusive lock on the WAL directory.
// Returns true if clean start, false if crash recovery needed.
func Acquire(dir string) (bool, *LockFile, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return false, nil, fmt.Errorf("create lock directory: %w", err)
	}
	path := dir + "/lock.qlog"

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return false, nil, fmt.Errorf("open lock file: %w", err)
	}

	// Try exclusive lock (non-blocking)
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return false, nil, fmt.Errorf("another process is using this WAL")
		}
		return false, nil, fmt.Errorf("acquire lock: %w", err)
	}

	// Check if file had content (indicates crash)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return false, nil, fmt.Errorf("stat lock file: %w", err)
	}

	cleanStart := info.Size() == 0

	// Write PID to lock file
	if err := f.Truncate(0); err != nil {
		f.Close()
		return false, nil, fmt.Errorf("truncate lock file: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		return false, nil, fmt.Errorf("seek lock file: %w", err)
	}
	fmt.Fprintf(f, "%d", os.Getpid())

	return cleanStart, &LockFile{file: f}, nil
}

// Release marks a clean shutdown and releases the stable lock inode.
func (lf *LockFile) Release() error {
	if lf.file == nil {
		return nil
	}
	if err := lf.file.Truncate(0); err != nil {
		return err
	}
	if err := lf.file.Sync(); err != nil {
		return err
	}
	if err := syscall.Flock(int(lf.file.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}
	err := lf.file.Close()
	lf.file = nil
	return err
}
