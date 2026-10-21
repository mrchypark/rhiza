package fencing

import (
	"errors"
	"testing"
)

func TestMutationArchiveScanRejectsInventedCoverageAndFaultBlocksAdmission(t *testing.T) {
	p := newJournalProducer(t, "cursor")
	settledMutation(t, p, 1)
	plan, err := p.PlanMutationCompaction(compactionProducerUID, 1)
	mustJournal(t, err)
	ref := archiveReference(plan)
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, plan, ref))
	p.ArchiveScan = &MutationArchiveScan{VerificationID: journalTestDigest, Head: ref}
	if p.Validate() == nil {
		t.Fatal("zero verified links claimed an entire archived history")
	}
	p.ArchiveScan.Links = 1
	mustJournal(t, p.Validate())
	p.ArchiveScan.Sequence = 2
	if p.Validate() == nil {
		t.Fatal("cursor claims a result beyond its archived watermark")
	}
	p.ArchiveScan = nil
	p.EvidenceFault = journalTestDigest
	for name, transition := range map[string]func() error{
		"activate": func() error {
			return p.Activate(Identity{SourceClusterID: p.Runtime.SourceClusterID, NodeID: p.Runtime.NodeID, PodUID: p.Runtime.PodUID, RunID: p.Runtime.RunID, WALIdentity: p.WALIdentity})
		},
		"admit":      func() error { return p.Admit(2, journalUpload("late")) },
		"prepare":    func() error { return p.Prepare(1, journalPreparation(1)) },
		"dispatch":   func() error { return p.GrantDispatch(1, "late") },
		"compaction": func() error { _, err := p.PlanMutationCompaction(compactionProducerUID, 2); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := transition(); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("evidence fault did not prevent new authority: %v", err)
			}
		})
	}
	mustJournal(t, p.Close("fence", journalTestDigest))
	if p.BarrierComplete() {
		t.Fatal("closed journal ignored latched evidence loss")
	}
}
