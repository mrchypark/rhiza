package rhiza

import (
	"context"
	"fmt"

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
