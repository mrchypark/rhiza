//go:build !linux

package qlog

import "os"

func syncData(file *os.File) error { return file.Sync() }
