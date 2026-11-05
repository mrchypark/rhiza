package quepaxa

import (
	"crypto/sha256"
	"testing"
	"time"
)

func TestRuntimeProgressSnapshotIncludesActualPendingRecorder(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	proposal := newProposal(highestPriority, "local", []byte("actual pending recorder value"))
	if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		progress := core.RuntimeProgressSnapshot()
		if progress.CertifiedTip == 0 && progress.RecorderTip == 1 {
			if !progress.RecorderHashPresent || progress.RecorderHashIsCertified || progress.RecorderHash != sha256.Sum256(proposal.Value) {
				t.Fatalf("pending recorder identity=%+v, want exact candidate hash without certificate", progress)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	progress := core.RuntimeProgressSnapshot()
	t.Fatalf("actual pending recorder snapshot=%+v, want certified=0 recorder=1", progress)
}
