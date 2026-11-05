//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestReplaceVoterPromotedPreparedRestartRecoversRecordedControl(t *testing.T) {
	runReplaceVoterCoordinatesActualPreparedLearner(t, func(ctx context.Context, learner *Host, cfg Config, source *Host) func(*Host) {
		snapshot, err := learner.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil {
			t.Fatal("inspect actual promoted membership", err)
		}
		promoted := false
		for _, member := range snapshot.Current.Members {
			if member.ID == cfg.Runtime.NodeID && member.WALIdentity == snapshot.LocalWALIdentity && member.PublicKey == cfg.Runtime.Learner.PublicKey {
				promoted = true
			}
		}
		if !promoted || snapshot.Current.ConfigID != 3 {
			t.Fatal("fixture did not complete actual ADD/REMOVE with exact learner WAL identity")
		}
		profile := network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, Generation: int64(cfg.Plan.Generation)}
		client, err := network.NewTransportForManagedProfile(source.cfg.Runtime.ClusterID, source.cfg.Runtime.NodeID, &snapshot.Current, source.cfg.Runtime.PeerToken, profile)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		before, err := learner.node.ManagedState()
		if err != nil || before.RecorderTip != before.CertifiedTip {
			t.Fatal("promoted fixture must start without a recorder gap", before, err)
		}
		sourceBefore, err := source.node.ManagedState()
		if err != nil || sourceBefore.CertifiedTip != before.CertifiedTip {
			t.Fatal("actual voters must share the captured certified cut", sourceBefore, before, err)
		}
		original, found, err := source.node.RetainedRequest(ctx, "sql", "repair-pending")
		if err != nil || !found {
			t.Fatal("capture actual retained application original", err)
		}
		var nonce [types.ReadBarrierNonceSize]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			t.Fatal(err)
		}
		value := types.EncodeReadBarrier(nonce)
		hash := sha256.Sum256(value)
		slot := quepaxa.Slot(before.CertifiedTip + 1)
		var priority quepaxa.Priority
		for i := range priority {
			priority[i] = 0xff
		}
		request := quepaxa.RecordRequest{Slot: slot, Step: 5, ConfigID: snapshot.Current.ConfigID, Proposal: quepaxa.Proposal{Priority: priority, ProposerID: source.cfg.Runtime.NodeID, Hash: hash, Value: value}}
		// Both acknowledgements come from actual authenticated QUIC Record RPCs.
		// No decision/certificate is fabricated or delivered for this CONTROL.
		for _, id := range []quepaxa.NodeID{cfg.Runtime.NodeID, source.cfg.Runtime.NodeID} {
			summary, err := client.SendRecord(ctx, id, request)
			if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash {
				t.Fatal("record exact CONTROL on actual promoted voter quorum", id, err)
			}
		}
		pending, err := learner.node.ManagedState()
		if err != nil || pending.RecorderTip != uint64(slot) || pending.CertifiedTip != before.CertifiedTip || pending.AppliedTip != before.AppliedTip {
			t.Fatal("actual recorder gap changed certified/applied cut", pending, err)
		}
		return func(reopened *Host) {
			page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, slot, 1)
			if err != nil || len(page.Decisions) != 1 || page.Decisions[0].Slot != slot || page.Decisions[0].Hash != hash || !reflect.DeepEqual(page.Decisions[0].Value, value) || len(page.Decisions[0].Certificate) == 0 {
				t.Fatal("restart did not certify the exact retained CONTROL at its original slot", err)
			}
			after, found, err := reopened.node.RetainedRequest(ctx, "sql", "repair-pending")
			if err != nil || !found || !reflect.DeepEqual(after, original) {
				t.Fatal("voter recovery changed retained application original/receipt", err)
			}
		}
	})
}
