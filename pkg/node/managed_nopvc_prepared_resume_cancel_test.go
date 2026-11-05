//go:build rhiza_local_testhooks

package node

import "testing"

func TestManagedNoPVCPreparedLearnerResumesSameHAfterCancelDuringJ(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, true, true, false, false, false)
}

func TestManagedNoPVCPreparedLearnerResumesPartialReplayAfterCancelDuringJ(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, true, true, true, false, false)
}
