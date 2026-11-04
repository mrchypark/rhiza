package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedPreparedStartupRefusesUncertifiedRecorderTail(t *testing.T) {
	ctx := context.Background()
	ids := []quepaxa.NodeID{"a", "b", "c"}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: make([]quepaxa.Member, len(ids))}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	for i, id := range ids {
		member := peerMember("prepared-recorder", id, string(id)+"-peer")
		cluster.Members[i] = member
	}
	for _, id := range ids {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := wal.Close(); err != nil {
				t.Errorf("close %s WAL: %v", id, err)
			}
		})
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: id, Cluster: cluster, WAL: wal, Transport: transport,
			EnableReconfiguration: true, ValidateValue: types.ValidateManagedKVExecutionPolicy,
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[id] = core
	}
	proposer := transport.cores["a"]
	certified, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "prepared-prefix", Operation: "put", Key: "prefix", Value: []byte("certified")})
	if err != nil {
		t.Fatal(err)
	}
	if slot, receipts, err := proposer.Propose(ctx, certified); err != nil || slot != 1 || len(receipts) == 0 {
		t.Fatalf("establish real quorum-certified prefix: slot=%d receipts=%d err=%v", slot, len(receipts), err)
	}
	transport.disable("b", "c")
	ordinary, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "prepared-pending", Operation: "put", Key: "pending", Value: []byte("uncommitted")})
	if err != nil {
		t.Fatal(err)
	}
	if slot, receipts, err := proposer.Propose(ctx, ordinary); !errors.Is(err, quepaxa.ErrQuorumUnavailable) || slot != 2 || len(receipts) != 0 {
		t.Fatalf("create real unquorate recorder tail: slot=%d receipts=%d err=%v", slot, len(receipts), err)
	}
	if proposer.Tip() != 1 || proposer.RecorderTip() != 2 {
		t.Fatalf("fixture lacks exact certified/recorded gap: C=%d R=%d", proposer.Tip(), proposer.RecorderTip())
	}
	if _, ok := proposer.CertifiedValue(2); ok {
		t.Fatal("unquorate recorder tail unexpectedly has a certificate")
	}
	beforeCertified, beforeRecorded := proposer.Tip(), proposer.RecorderTip()
	if err := refusePreparedUncertifiedRecorder(proposer); err == nil {
		t.Fatal("prepared startup accepted recorder progress beyond the certified prefix")
	}
	if proposer.Tip() != beforeCertified || proposer.RecorderTip() != beforeRecorded {
		t.Fatalf("prepared recovery guard changed Core progress or acknowledged the pending proposal: before=(%d,%d) after=(%d,%d)", beforeCertified, beforeRecorded, proposer.Tip(), proposer.RecorderTip())
	}
}
