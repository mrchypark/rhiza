//go:build !darwin

package node

import "context"

func cloneAndHashLocalSnapshot(context.Context, string, string, uint64) (uint64, string, bool, error) {
	return 0, "", false, nil
}
