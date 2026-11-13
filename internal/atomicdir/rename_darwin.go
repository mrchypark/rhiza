//go:build darwin

package atomicdir

import "golang.org/x/sys/unix"

func renameNoReplace(parentFD int, oldBase, newBase string) error {
	return unix.RenameatxNp(parentFD, oldBase, parentFD, newBase, unix.RENAME_EXCL)
}
