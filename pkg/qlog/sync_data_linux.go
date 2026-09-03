//go:build linux

package qlog

import (
	"os"
	"syscall"
)

func syncData(file *os.File) error { return syscall.Fdatasync(int(file.Fd())) }
