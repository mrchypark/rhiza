//go:build linux

package atomicdir

import "golang.org/x/sys/unix"

func renameNoReplace(parentFD int, oldBase, newBase string) error {
	return unix.Renameat2(parentFD, oldBase, parentFD, newBase, unix.RENAME_NOREPLACE)
}
