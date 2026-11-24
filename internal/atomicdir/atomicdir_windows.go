//go:build windows

// Package atomicdir provides descriptor-relative, no-replace directory moves.
package atomicdir

import "errors"

// ErrUnsupported reports that no safe descriptor-relative directory rename is available.
var ErrUnsupported = errors.New("atomic directory rename unsupported")

// RenameNoReplace fails closed because Windows has no verified implementation of
// the parent identity and durability guarantees required by this package.
func RenameNoReplace(oldPath, newPath string) error {
	return ErrUnsupported
}
