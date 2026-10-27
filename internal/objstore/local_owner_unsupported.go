//go:build !darwin && !linux

package objstore

import "fmt"

func acquireLocalFilesystemOwner(string) (*LocalFilesystemOwner, error) {
	return nil, fmt.Errorf("filesystem-backed Local recovery is unsupported on this platform")
}
