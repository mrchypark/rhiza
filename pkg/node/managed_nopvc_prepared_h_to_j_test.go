//go:build rhiza_local_testhooks

package node

import "testing"

func TestManagedNoPVCPreparedLearnerAdoptsHBeforeArchivedJCatchUp(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, true, false, false, false, false)
}
