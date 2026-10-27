//go:build darwin

package objstore

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func supportedLocalFilesystem(root string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		return err
	}
	name := strings.TrimRight(string(stat.Fstypename[:]), "\x00")
	if name != "apfs" && name != "hfs" {
		return fmt.Errorf("unsupported Local archive filesystem %q", name)
	}
	return nil
}
