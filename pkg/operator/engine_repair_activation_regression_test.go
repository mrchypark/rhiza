package operator

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestEngineRepairCanonicalAddResetsAbortRevisionAndValueComparesMember(t *testing.T) {
	_, _, record := engineRepairStoreFixture(t)
	removeTarget := quepaxa.Cluster{ConfigID: uint(record.SourceConfig.ConfigID + 1)}
	for _, member := range record.SourceConfig.Members {
		if string(member.ID) != record.Binding.OldMemberID {
			removeTarget.Members = append(removeTarget.Members, member)
		}
	}
	record.RemovalOperation = &quepaxa.EngineRemoval{Target: removeTarget}
	record.StorageProofDigest = hashBytes([]byte("storage-proof"))
	record.RemoveTerminalDigest = hashBytes([]byte("remove-terminal"))
	want := network.MembershipChange{OperationID: engineRepairAddOperationID(record.Binding.OperationID),
		ClusterID: record.Binding.SourceClusterID, ExpectedConfigID: removeTarget.ConfigID, ExpectedAbortSlot: 0,
		Add: &record.CandidateMember}
	record.AddRequestHash = hashJSON(want)

	got, target, err := engineRepairCanonicalAdd(record)
	if err != nil {
		t.Fatalf("reconstruct canonical ADD: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ADD request=%+v, want %+v", got, want)
	}
	encoded, err := json.Marshal(engineRepairMembershipOperation{Request: got, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	var readback engineRepairMembershipOperation
	if err := json.Unmarshal(encoded, &readback); err != nil {
		t.Fatal(err)
	}
	if !sameEngineRepairMembershipChange(readback.Request, got) {
		t.Fatal("JSON readback with an independently allocated ADD member pointer was rejected")
	}
	if target.ConfigID != removeTarget.ConfigID+1 {
		t.Fatalf("ADD target config=%d, want %d", target.ConfigID, removeTarget.ConfigID+1)
	}
}
