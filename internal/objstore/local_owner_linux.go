//go:build linux

package objstore

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func supportedLocalFilesystem(root string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		return err
	}
	switch uint64(stat.Type) {
	case 0xef53, 0x58465342, 0x9123683e, 0x794c7630, 0x01021994:
		return nil // ext, XFS, Btrfs, overlayfs, tmpfs
	default:
		return fmt.Errorf("unsupported Local archive filesystem type %#x", stat.Type)
	}
}
