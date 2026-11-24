package qlog

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func lockWAL(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
}

func isWALLocked(err error) bool { return errors.Is(err, windows.ERROR_LOCK_VIOLATION) }

func unlockWAL(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &windows.Overlapped{})
}
