//go:build !linux && !darwin && !windows

package atomicdir

func renameNoReplace(parentFD int, oldBase, newBase string) error {
	return ErrUnsupported
}
