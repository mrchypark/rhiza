//go:build !linux && !darwin

package atomicdir

func renameNoReplace(parentFD int, oldBase, newBase string) error {
	return ErrUnsupported
}
