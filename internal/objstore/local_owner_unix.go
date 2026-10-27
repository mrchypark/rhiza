//go:build darwin || linux

package objstore

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func acquireLocalFilesystemOwner(root string) (*LocalFilesystemOwner, error) {
	if err := supportedLocalFilesystem(root); err != nil {
		return nil, err
	}
	anchorPath := ownerAnchorPath(root)
	anchorFD, err := unix.Open(anchorPath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("open Local owner anchor: %w", err)
	}
	anchor := os.NewFile(uintptr(anchorFD), anchorPath)
	var anchorStat unix.Stat_t
	if err := unix.Fstat(anchorFD, &anchorStat); err != nil || anchorStat.Mode&unix.S_IFMT != unix.S_IFREG || anchorStat.Mode&0077 != 0 {
		_ = anchor.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unsafe Local owner anchor")
	}
	if err := unix.Flock(anchorFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = anchor.Close()
		return nil, fmt.Errorf("Local filesystem archive is already owned: %w", err)
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unlockClose(anchor)
		return nil, err
	}
	rootFile := os.NewFile(uintptr(rootFD), root)
	if err := unix.Flock(rootFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = rootFile.Close()
		_ = unlockClose(anchor)
		return nil, fmt.Errorf("Local filesystem archive alias is already owned: %w", err)
	}
	return &LocalFilesystemOwner{root: rootFile, anchor: anchor}, nil
}

func unlockClose(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return errorsJoin(unlockErr, file.Close())
}
