package rhiza

import (
	"context"

	"github.com/mrchypark/rhiza/pkg/node"
)

// PrepareRestore materializes the current certified source into DataDir for a
// fresh Pod. It does not open a voter WAL or grant membership authority.
func PrepareRestore(ctx context.Context, config Config) error {
	internalConfig, err := executionConfig(config)
	if err != nil {
		return err
	}
	return node.PrepareRestore(ctx, internalConfig)
}
