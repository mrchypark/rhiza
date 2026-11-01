package operator

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

// V1Alpha2PreparedStartupChecker binds a trusted host mapping to live status.
// Physical source and configuration digests must be established by the host.
type V1Alpha2PreparedStartupChecker struct {
	Store     *V1Alpha2StatusStore
	Ref       V1Alpha2ClusterRef
	Authority V1Alpha2AuthorityView
	Identity  recovery.PreparedStartupIdentity
}

func (c *V1Alpha2PreparedStartupChecker) ValidatePreparedStartup(ctx context.Context, identity recovery.PreparedStartupIdentity) error {
	if c == nil || identity != c.Identity || identity.ClusterUID != c.Ref.UID || identity.DeploymentUID != c.Authority.Binding.DeploymentUID || identity.Generation != c.Authority.Generation || identity.SourceBinding == "" || identity.ConfigDigest == "" {
		return fmt.Errorf("prepared identity does not match trusted mapping")
	}
	state, err := LoadV1Alpha2Admission(ctx, c.Store, c.Ref)
	if err != nil {
		return err
	}
	if state.Authority() != c.Authority || hasV1Alpha2Transition(state.Transition()) {
		return fmt.Errorf("current authority does not match prepared startup")
	}
	return nil
}
