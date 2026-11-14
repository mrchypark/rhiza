package node

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Exercise both real Node verification routes against independently valid,
// differently ordered certificates for the same membership decisions.
func TestManagedAppendOnlyNodeAlternativeMembershipProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	tr := &managedAppendOnlyTestTransport{peers: map[quepaxa.NodeID]*quepaxa.Core{}, down: map[quepaxa.NodeID]bool{}}
	for _, m := range members {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: cluster, WAL: wal, Transport: tr, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		tr.peers[m.ID] = core
	}
	source, lagger := tr.peers["n1"], tr.peers["n2"]
	tr.down["n2"] = true
	target := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	if _, err := source.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err := source.DecisionsFrom(1, 256)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.Slot == 1 {
			var cert map[string]json.RawMessage
			if err := json.Unmarshal(value.Certificate, &cert); err != nil {
				t.Fatal(err)
			}
			var summaries []json.RawMessage
			if err := json.Unmarshal(cert["summaries"], &summaries); err != nil {
				t.Fatal(err)
			}
			if len(summaries) < 2 {
				t.Fatal("freeze has no quorum")
			}
			slices.Reverse(summaries)
			cert["summaries"], err = json.Marshal(summaries)
			if err != nil {
				t.Fatal(err)
			}
			value.Certificate, err = json.Marshal(cert)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := lagger.AcceptCertifiedValue(value); err != nil {
			t.Fatalf("lagger slot %d: %v", value.Slot, err)
		}
	}
	tr.down["n2"] = false
	if _, _, err := source.Propose(ctx, quepaxa.EncodeReadBarrier([quepaxa.ReadBarrierNonceSize]byte{7})); err != nil {
		t.Fatal(err)
	}
	if err := lagger.WaitTip(ctx, source.Tip()); err != nil {
		t.Fatal(err)
	}
	cut := source.Tip()
	prefix, ok := source.PrefixHash(cut)
	if !ok {
		t.Fatal("missing prefix")
	}
	next, following, err := source.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	history, err := source.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	local, err := lagger.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(history, local) {
		t.Fatal("proofs unexpectedly identical")
	}
	seal := quepaxa.CheckpointSeal{Index: cut, ConfigID: 2, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &history}
	n := &Node{core: lagger, config: &types.ExecutionConfig{NodeID: "n2", Members: members, EnableReconfiguration: true}}
	if err := n.verifyManagedAppendOnlyLocalHistory(ctx, seal); err != nil {
		t.Fatalf("local alternative proof: %v", err)
	}
	bucket := objstore.NewInMemBucket()
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "proof", 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	archive.SetGroupDelay(0)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, lagger, cut); err != nil {
		t.Fatalf("archive through %d: %v", cut, err)
	}
	n.archive = archive
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol}}
	if err := n.verifyManagedAppendOnlyArchiveHistory(ctx, seal); err != nil {
		t.Fatalf("archive alternative proof: %v", err)
	}
	encoded, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	var invalid quepaxa.MembershipRecord
	if err := json.Unmarshal(encoded, &invalid); err != nil {
		t.Fatal(err)
	}
	var malformed map[string]json.RawMessage
	if err := json.Unmarshal(invalid.Transitions[0].Freeze.Certificate, &malformed); err != nil {
		t.Fatal(err)
	}
	malformed["summaries"] = json.RawMessage("[]")
	invalid.Transitions[0].Freeze.Certificate, err = json.Marshal(malformed)
	if err != nil {
		t.Fatal(err)
	}
	badSeal := seal
	badSeal.Membership = &invalid
	if err := n.verifyManagedAppendOnlyLocalHistory(ctx, badSeal); err == nil {
		t.Fatal("local path accepted quorumless history")
	}
	if err := n.verifyManagedAppendOnlyArchiveHistory(ctx, badSeal); err == nil {
		t.Fatal("archive path accepted quorumless history")
	}
	verifyArchiveWhileLaterTerminalPending(t, ctx, source, cluster, tr, archive, seal, members)
	t.Log(fmt.Sprintf("verified local and archive cut %d with distinct witness bytes", cut))
}
