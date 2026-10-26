//go:build !linux

package cri

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type passiveSession struct{}

func (s *passiveSession) observe(fencing.RuntimeBinding, CgroupScope) (PassiveObservation, bool, error) {
	return PassiveObservation{}, false, fmt.Errorf("%w: passive kernel proof requires Linux", ErrUnverified)
}

func (s *passiveSession) close() {}

func (p *Provider) capturePassiveScope(context.Context, fencing.RuntimeBinding, string, string, uint32, string, *CgroupScope) error {
	return fmt.Errorf("%w: passive kernel proof requires Linux", ErrUnverified)
}
