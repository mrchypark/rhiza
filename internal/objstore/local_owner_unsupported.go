//go:build !darwin && !linux

package objstore

import (
	"fmt"
	"os"
)

func acquireLocalFilesystemOwner(string) (*LocalFilesystemOwner, error) {
	return nil, fmt.Errorf("filesystem-backed Local recovery is unsupported on this platform")
}

// Unsupported owners never acquire descriptors; close defensively if supplied.
func unlockClose(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}
