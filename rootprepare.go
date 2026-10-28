package rhiza

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/node"
)

// PrepareRestore materializes the current certified source into DataDir for a
// fresh Pod. It does not open a voter WAL or grant membership authority.
func PrepareRestore(ctx context.Context, config Config) error {
	if config.ClusterID == "" {
		return fmt.Errorf("cluster ID is required for prepared restore")
	}
	internalConfig, err := executionConfig(config)
	if err != nil {
		return err
	}
	return node.PrepareRestore(ctx, internalConfig)
}

// PrepareManagedRestore performs the offline preparation selected by an
// authority-issued bootstrap assignment. It reports local cleanup facts only;
// the caller must still establish private-namespace cleanup before success.
func PrepareManagedRestore(ctx context.Context, config Config, assignment fencing.BootstrapAssignment) (fencing.InitLiveCleanup, string, error) {
	return prepareManagedRestore(ctx, config, assignment, fencing.InitAdmission{})
}

// PrepareManagedRestoreWithAdmission binds preparation to the authority's
// exact response from BeginInit. The plain PrepareManagedRestore entry point
// intentionally cannot authorize the self-member DR restore path.
func PrepareManagedRestoreWithAdmission(ctx context.Context, config Config, admission fencing.InitAdmission) (fencing.InitLiveCleanup, string, error) {
	return prepareManagedRestore(ctx, config, admission.Assignment(), admission)
}

func prepareManagedRestore(ctx context.Context, config Config, assignment fencing.BootstrapAssignment, admission fencing.InitAdmission) (fencing.InitLiveCleanup, string, error) {
	if config.AdminToken != "" || config.PeerToken != "" {
		return fencing.InitLiveCleanup{}, "", fmt.Errorf("managed init must not receive runtime credentials")
	}
	execution, err := executionConfig(config)
	if err != nil {
		return fencing.InitLiveCleanup{}, "", err
	}
	if admission.Assignment() != (fencing.BootstrapAssignment{}) {
		return node.PrepareManagedInitWithAdmission(ctx, execution, admission)
	}
	return node.PrepareManagedInit(ctx, execution, assignment)
}
