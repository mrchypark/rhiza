package operator

import (
	"crypto/sha256"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestRepairArchiveProgressRequiresExactCertifiedMembershipSequence(t *testing.T) {
	old := quepaxa.Member{ID: "old", URL: "http://old", PeerURL: "quic://old", LogURL: "http://old-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old"))), WALIdentity: "old-wal"}
	other := quepaxa.Member{ID: "other", URL: "http://other", PeerURL: "quic://other", LogURL: "http://other-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other"))), WALIdentity: "other-wal"}
	added := quepaxa.Member{ID: "pod-new-uid", URL: "http://new", PeerURL: "quic://new", LogURL: "http://new-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("new"))), WALIdentity: "new-wal"}
	base := quepaxa.Cluster{ConfigID: 8, Members: []quepaxa.Member{old, other}}
	auth := fencing.MembershipRepairAuthorization{Membership: base}
	zero := quepaxa.Slot(0)
	remove := fencing.MembershipRepairPhase{Kind: fencing.MembershipRepairRemove, AbortSlot: &zero, Change: network.MembershipChange{
		ExpectedConfigID: 8, ExpectedAbortSlot: 0, Remove: "old",
	}}
	add := fencing.MembershipRepairPhase{Kind: fencing.MembershipRepairAdd, AbortSlot: &zero, Change: network.MembershipChange{
		ExpectedConfigID: 9, ExpectedAbortSlot: 0, Add: &added,
	}}
	withoutOld, err := applyRepairMembershipPhase(base, remove)
	if err != nil {
		t.Fatal(err)
	}
	withNew, err := applyRepairMembershipPhase(withoutOld, add)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		stage   fencing.MembershipRepairStage
		phases  []fencing.MembershipRepairPhase
		current quepaxa.Cluster
		pending quepaxa.Cluster
		wantErr bool
	}{
		{name: "pre-remove", stage: fencing.MembershipRepairOldClosed, phases: []fencing.MembershipRepairPhase{remove}, current: base},
		{name: "remove committed", stage: fencing.MembershipRepairRemoved, phases: []fencing.MembershipRepairPhase{remove}, current: withoutOld},
		{name: "remove pending", stage: fencing.MembershipRepairOldClosed, phases: []fencing.MembershipRepairPhase{remove}, current: base, pending: withoutOld},
		{name: "add pending", stage: fencing.MembershipRepairRemoved, phases: []fencing.MembershipRepairPhase{remove, add}, current: withoutOld, pending: withNew},
		{name: "add committed before stage CAS", stage: fencing.MembershipRepairRemoved, phases: []fencing.MembershipRepairPhase{remove, add}, current: withNew},
		{name: "add committed", stage: fencing.MembershipRepairAdded, phases: []fencing.MembershipRepairPhase{remove, add}, current: withNew},
		{name: "wrong voter set", stage: fencing.MembershipRepairRemoved, phases: []fencing.MembershipRepairPhase{remove}, current: base, wantErr: true},
		{name: "wrong pending target", stage: fencing.MembershipRepairOldClosed, phases: []fencing.MembershipRepairPhase{remove}, current: base, pending: withNew, wantErr: true},
		{name: "stage ahead of archive", stage: fencing.MembershipRepairAdded, phases: []fencing.MembershipRepairPhase{remove, add}, current: withoutOld, wantErr: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			view := fencing.MembershipRepairView{Authorization: auth, Progress: fencing.MembershipRepairProgress{Revision: 1, Stage: test.stage, Phases: test.phases}}
			err := validateRepairArchiveProgress(view, test.current, test.pending, 0)
			if (err != nil) != test.wantErr {
				t.Fatalf("archive progress error=%v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}
