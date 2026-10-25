package main

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestGCSOrdinaryRecoveryStatusOracleRequiresFixedArchiveAndAppliedTargets(t *testing.T) {
	target := gcsOrdinaryRecoveryTarget{CertifiedTip: 10, AppliedTip: 10, ArchiveTip: 8}
	valid := network.VoterRecoveryStatus{
		ClusterID: "cluster", NodeID: "voter", Durability: "before-ack", Ready: true, Quorum: true,
		CertifiedTip: 11, AppliedTip: 11, ArchiveTip: 8,
	}
	tests := []struct {
		name   string
		mutate func(*network.VoterRecoveryStatus)
	}{
		{name: "not ready", mutate: func(status *network.VoterRecoveryStatus) { status.Ready = false }},
		{name: "no quorum", mutate: func(status *network.VoterRecoveryStatus) { status.Quorum = false }},
		{name: "wrong cluster", mutate: func(status *network.VoterRecoveryStatus) { status.ClusterID = "other" }},
		{name: "wrong node", mutate: func(status *network.VoterRecoveryStatus) { status.NodeID = "other" }},
		{name: "wrong durability", mutate: func(status *network.VoterRecoveryStatus) { status.Durability = "async" }},
		{name: "certified tip behind", mutate: func(status *network.VoterRecoveryStatus) { status.CertifiedTip = 9 }},
		{name: "applied tip behind", mutate: func(status *network.VoterRecoveryStatus) { status.AppliedTip = 9 }},
		{name: "archive tip behind fixed target", mutate: func(status *network.VoterRecoveryStatus) { status.ArchiveTip = 7 }},
	}
	if !gcsOrdinaryRecoveryStatusMatches(valid, "cluster", "voter", target) {
		t.Fatal("accepted recovery status at the fixed certified/applied/archive target was rejected")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := valid
			test.mutate(&status)
			if gcsOrdinaryRecoveryStatusMatches(status, "cluster", "voter", target) {
				t.Fatalf("accepted invalid recovery status: %+v", status)
			}
		})
	}
}

func TestGCSOrdinaryRecoveryRequiresBoundApplicationSupervisedLaunch(t *testing.T) {
	source := gcsRecoverySource{StatefulSet: "rhiza", StatefulSetUID: "sts-uid", Container: "rhiza"}
	statefulSet := func(appCommand []any, appArgs []any) obj {
		return obj{
			"metadata": obj{"name": "rhiza", "uid": "sts-uid"},
			"spec": obj{"template": obj{"spec": obj{"containers": []any{
				obj{"name": "rhiza", "command": appCommand, "args": appArgs},
				obj{"name": "sidecar", "command": []any{"/usr/local/bin/rhiza-fencer"}, "args": []any{"supervise", "--", "/usr/local/bin/rhiza"}},
			}}}},
		}
	}

	t.Run("accepts production supervised command on bound application container", func(t *testing.T) {
		sts := statefulSet([]any{"/usr/local/bin/rhiza-fencer"}, []any{"supervise", "--", "/usr/local/bin/rhiza"})
		if err := requireGCSOrdinarySupervisedLaunch(sts, source); err != nil {
			t.Fatalf("valid supervised application launch rejected: %v", err)
		}
	})
	t.Run("rejects direct unsupervised application even when sidecar is supervised", func(t *testing.T) {
		sts := statefulSet([]any{"/usr/local/bin/rhiza"}, nil)
		if err := requireGCSOrdinarySupervisedLaunch(sts, source); err == nil {
			t.Fatal("accepted direct application launch because a sidecar had the supervisor command")
		}
	})
	t.Run("rejects StatefulSet replacement after source capture", func(t *testing.T) {
		sts := statefulSet([]any{"/usr/local/bin/rhiza-fencer"}, []any{"supervise", "--", "/usr/local/bin/rhiza"})
		sts["metadata"].(obj)["uid"] = "replacement-uid"
		if err := requireGCSOrdinarySupervisedLaunch(sts, source); err == nil {
			t.Fatal("accepted supervised launch from a different StatefulSet UID")
		}
	})
}

func TestGCSOrdinaryRecoveryTargetRequiresStableSurvivorStatuses(t *testing.T) {
	first := network.VoterRecoveryStatus{
		ClusterID: "cluster", NodeID: "v1", Durability: "before-ack", Ready: true, Quorum: true,
		CertifiedTip: 10, AppliedTip: 10, ArchiveTip: 8,
	}
	second := first
	second.NodeID = "v2"
	if target, ok := gcsOrdinaryRecoveryTargetFromSurvivors("cluster", "v1", first, "v2", second); !ok ||
		target != (gcsOrdinaryRecoveryTarget{CertifiedTip: 10, AppliedTip: 10, ArchiveTip: 8}) {
		t.Fatalf("stable survivor target=%+v ok=%v", target, ok)
	}
	second.ArchiveTip = 9
	if target, ok := gcsOrdinaryRecoveryTargetFromSurvivors("cluster", "v1", first, "v2", second); !ok ||
		target != (gcsOrdinaryRecoveryTarget{CertifiedTip: 10, AppliedTip: 10, ArchiveTip: 9}) {
		t.Fatalf("target did not pin the highest pre-delete survivor archive tip: %+v ok=%v", target, ok)
	}
	second.CertifiedTip++
	if _, ok := gcsOrdinaryRecoveryTargetFromSurvivors("cluster", "v1", first, "v2", second); ok {
		t.Fatal("accepted a moving certified target")
	}
}

func TestGCSOrdinaryRecoveryCurrentStatusRequiresAllVotersAtExactTip(t *testing.T) {
	statuses := []network.VoterRecoveryStatus{
		{Ready: true, Quorum: true, CertifiedTip: 12, AppliedTip: 12},
		{Ready: true, Quorum: true, CertifiedTip: 12, AppliedTip: 12},
		{Ready: true, Quorum: true, CertifiedTip: 12, AppliedTip: 12},
	}
	if tip, ok := gcsOrdinaryRecoveryStatusesAtCurrentTip(statuses...); !ok || tip != 12 {
		t.Fatalf("current tip=%d ok=%v", tip, ok)
	}
	statuses[2].AppliedTip = 11
	if _, ok := gcsOrdinaryRecoveryStatusesAtCurrentTip(statuses...); ok {
		t.Fatal("accepted a replacement that had not applied the exact current tip")
	}
}

func TestGCSOrdinaryRecoveryRejectsAnyPriorIncarnationReuse(t *testing.T) {
	prior := []operator.FenceTarget{
		{NodeID: "old-0", WALIdentity: "old-wal"},
		{NodeID: "fresh-0-a", WALIdentity: "fresh-wal-a"},
	}
	tests := []struct {
		name   string
		status network.MembershipStatus
		want   bool
	}{
		{name: "new logical identity and WAL", status: network.MembershipStatus{NodeID: "fresh-0-b", WALIdentity: "fresh-wal-b"}, want: true},
		{name: "original logical identity", status: network.MembershipStatus{NodeID: "old-0", WALIdentity: "fresh-wal-b"}},
		{name: "first replacement logical identity", status: network.MembershipStatus{NodeID: "fresh-0-a", WALIdentity: "fresh-wal-b"}},
		{name: "original WAL", status: network.MembershipStatus{NodeID: "fresh-0-b", WALIdentity: "old-wal"}},
		{name: "first replacement WAL", status: network.MembershipStatus{NodeID: "fresh-0-b", WALIdentity: "fresh-wal-a"}},
		{name: "missing identity", status: network.MembershipStatus{WALIdentity: "fresh-wal-b"}},
		{name: "missing WAL", status: network.MembershipStatus{NodeID: "fresh-0-b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := gcsOrdinaryIncarnationIsFresh(test.status, prior); got != test.want {
				t.Fatalf("freshness = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGCSOrdinaryRecoveryRequiresExactGraphAndKVRetryReceipts(t *testing.T) {
	tests := []struct {
		name     string
		response obj
		wantSlot int64
		wantErr  bool
	}{
		{name: "committed graph receipt", response: obj{"status": "committed", "slot": int64(12)}, wantSlot: 12},
		{name: "committed kv receipt", response: obj{"status": "committed", "slot": int64(19)}, wantSlot: 19},
		{name: "missing slot", response: obj{"status": "committed"}, wantErr: true},
		{name: "noncommitted mutation", response: obj{"status": "rejected", "slot": int64(12)}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			slot, err := gcsOrdinaryMutationReceiptSlot(test.response)
			if test.wantErr {
				if err == nil {
					t.Fatal("accepted a mutation response without an exact committed receipt")
				}
				return
			}
			if err != nil || slot != test.wantSlot {
				t.Fatalf("receipt slot=%d err=%v, want %d", slot, err, test.wantSlot)
			}
		})
	}

	if !gcsOrdinaryMutationRetryMatches(obj{"state": "committed", "receipt": obj{"status": "committed", "slot": int64(12)}}, 12) {
		t.Fatal("exact committed retry receipt was rejected")
	}
	for _, response := range []obj{
		{"state": "unknown_or_expired", "receipt": obj{"status": "committed", "slot": int64(12)}},
		{"state": "committed", "receipt": obj{"status": "committed", "slot": int64(13)}},
		{"state": "committed", "receipt": obj{"status": "rejected", "slot": int64(12)}},
	} {
		if gcsOrdinaryMutationRetryMatches(response, 12) {
			t.Fatalf("accepted a retry with a changed or noncommitted receipt: %+v", response)
		}
	}
}

func TestGCSOrdinaryCheckpointOracleRequiresPersistedIdentityAndIndexAdvance(t *testing.T) {
	before := gcsOrdinaryCheckpointEvidence{
		ConfigID: 1, SealConfigID: 3, SealDecisionSlot: 21, Generation: 4, Index: 20,
		Hash: "state-hash-a", RootHash: "root-a", PrefixHash: "prefix-a", MembershipVersions: 2,
	}
	tests := []struct {
		name    string
		after   gcsOrdinaryCheckpointEvidence
		minimum uint64
		want    bool
	}{
		{name: "advances past second recovery writes", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30, want: true},
		{name: "same index is not advancement", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 21, Generation: 5, Index: 20,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 20},
		{name: "checkpoint does not cover second recovery", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 26, Generation: 5, Index: 25,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30},
		{name: "same persisted root is not new identity", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-a", RootHash: "root-a", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30},
		{name: "storage namespace remains fixed while seal config advances", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 2, SealConfigID: 5, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30},
		{name: "dynamic membership seal must advance", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 3, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30},
		{name: "seal decision must advance", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 21, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 4,
		}, minimum: 30},
		{name: "membership history must advance with seal", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-b", MembershipVersions: 2,
		}, minimum: 30},
		{name: "prefix must advance", after: gcsOrdinaryCheckpointEvidence{
			ConfigID: 1, SealConfigID: 5, SealDecisionSlot: 31, Generation: 5, Index: 30,
			Hash: "state-hash-b", RootHash: "root-b", PrefixHash: "prefix-a", MembershipVersions: 4,
		}, minimum: 30},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := gcsOrdinaryCheckpointAdvanced(before, test.after, test.minimum); got != test.want {
				t.Fatalf("checkpoint advancement = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGCSOrdinaryCheckpointSealBindsCertifiedStateAndDynamicMembership(t *testing.T) {
	root := &checkpoint.Checkpoint{ConfigID: 1, Index: 40}
	root.Hash[0], root.RootHash[0] = 1, 2
	prefix := [32]byte{3}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 3, Index: 40, StateHash: root.Hash, RootHash: root.RootHash, PrefixHash: prefix,
		NextLeaderOrder: []quepaxa.NodeID{"v1"},
		Membership: &quepaxa.MembershipRecord{
			Genesis:     quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "v0"}, {ID: "v1"}, {ID: "v2"}}},
			Transitions: []quepaxa.ConfigTransition{{}, {}},
		},
	}
	if !gcsOrdinaryCheckpointSealMatches(root, seal, 41, 3, prefix) {
		t.Fatal("reconfigured certified seal with the fixed checkpoint namespace was rejected")
	}

	tests := []struct {
		name       string
		mutateRoot func(*checkpoint.Checkpoint)
		mutateSeal func(*quepaxa.CheckpointSeal)
		slot       quepaxa.Slot
		configID   uint
		prefix     [32]byte
	}{
		{name: "root namespace changed", mutateRoot: func(root *checkpoint.Checkpoint) { root.ConfigID = 3 }},
		{name: "checkpoint index differs", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.Index++ }},
		{name: "checkpoint root differs", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.RootHash[0]++ }},
		{name: "checkpoint state differs", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.StateHash[0]++ }},
		{name: "seal prefix differs", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.PrefixHash[0]++ }},
		{name: "computed archive prefix differs", prefix: [32]byte{4}},
		{name: "seal membership version differs", configID: 4},
		{name: "seal decision is not after checkpoint index", slot: 40},
		{name: "membership record missing", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.Membership = nil }},
		{name: "membership history incomplete", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.Membership.Transitions = seal.Membership.Transitions[:1] }},
		{name: "membership genesis differs", mutateSeal: func(seal *quepaxa.CheckpointSeal) { seal.Membership.Genesis.ConfigID = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changedRoot, changedSeal := *root, seal
			changedSeal.Membership = &*seal.Membership
			changedSeal.Membership.Transitions = append([]quepaxa.ConfigTransition(nil), seal.Membership.Transitions...)
			if test.mutateRoot != nil {
				test.mutateRoot(&changedRoot)
			}
			if test.mutateSeal != nil {
				test.mutateSeal(&changedSeal)
			}
			slot, configID, actualPrefix := quepaxa.Slot(41), uint(3), prefix
			if test.slot != 0 {
				slot = test.slot
			}
			if test.configID != 0 {
				configID = test.configID
			}
			if test.prefix != ([32]byte{}) {
				actualPrefix = test.prefix
			}
			if gcsOrdinaryCheckpointSealMatches(&changedRoot, changedSeal, slot, configID, actualPrefix) {
				t.Fatal("accepted a root/seal pair with mismatched certified identity")
			}
		})
	}
}
