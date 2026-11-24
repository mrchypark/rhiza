//go:build !windows

package qlog

import (
	"errors"
	"os"
	"syscall"
)

func lockWAL(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func isWALLocked(err error) bool { return errors.Is(err, syscall.EWOULDBLOCK) }

func unlockWAL(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
