//go:build !linux

package cri

import "fmt"

func pinStopCgroup(string, string, CgroupScope) (stopCgroupPin, error) {
	return nil, fmt.Errorf("%w: pinned CRI cgroup stop requires Linux", ErrUnverified)
}
