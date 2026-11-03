package quepaxa

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func managedNoPVCBinding(identity []byte, protocol string) *ManagedAppendOnlyConfig {
	var walIdentity [32]byte
	copy(walIdentity[:], identity)
	return &ManagedAppendOnlyConfig{
		Binding: ManagedAppendOnlyBinding{
			ApplicationProtocol: protocol,
			BootstrapPlanDigest: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
			MemberConfigDigest:  hex.EncodeToString(bytes.Repeat([]byte{0x22}, 32)),
			Generation:          7,
			WALIdentity:         walIdentity,
		},
		Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil },
	}
}

func TestManagedNoPVCLearnerRequiresBoundIdentityWithoutPreaddingVoter(t *testing.T) {
	genesis := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	identity := bytes.Repeat([]byte{0x6d}, 32)
	openWAL := func() *qlog.WAL {
		t.Helper()
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		return wal
	}

	t.Run("registered absent learner accepted", func(t *testing.T) {
		wal := openWAL()
		config := Config{
			NodeID: "d", Cluster: genesis, WAL: wal, Transport: &clusterTransport{},
			ManagedAppendOnly: managedNoPVCBinding(identity, managedRequestAppendOnlyApplicationProtocol),
		}
		core, err := NewLearner(config)
		if err != nil {
			t.Fatalf("managed learner with its registered startup identity was rejected: %v", err)
		}
		if core.IsVoter() || core.WALIdentity() != hex.EncodeToString(identity) {
			t.Fatalf("new learner has wrong role or WAL identity: voter=%t identity=%q", core.IsVoter(), core.WALIdentity())
		}
		if len(core.CurrentCluster().Members) != len(genesis.Members) {
			t.Fatal("learner was pre-added to the trusted voter roster")
		}
	})

	t.Run("same configuration cannot bypass learner role", func(t *testing.T) {
		config := Config{
			NodeID: "d", Cluster: genesis, WAL: openWAL(), Transport: &clusterTransport{},
			ManagedAppendOnly: managedNoPVCBinding(identity, managedRequestAppendOnlyApplicationProtocol),
		}
		if _, err := New(config); err == nil {
			t.Fatal("regular voter constructor admitted an unlisted local member")
		}
	})

	t.Run("wrong registered nonce remains rejected", func(t *testing.T) {
		wal := openWAL()
		wrong := bytes.Repeat([]byte{0x7e}, 32)
		if _, err := NewLearner(Config{
			NodeID: "d", Cluster: genesis, WAL: wal, Transport: &clusterTransport{},
			ManagedAppendOnly: managedNoPVCBinding(wrong, managedRequestAppendOnlyApplicationProtocol),
		}); err == nil {
			t.Fatal("learner accepted a managed binding for a different registered nonce")
		}
	})

	t.Run("wrong profile remains rejected", func(t *testing.T) {
		wal := openWAL()
		if _, err := NewLearner(Config{
			NodeID: "d", Cluster: genesis, WAL: wal, Transport: &clusterTransport{},
			ManagedAppendOnly: managedNoPVCBinding(identity, "rhiza-managed-request-prototype/v0"),
		}); err == nil {
			t.Fatal("learner accepted an unsupported managed profile")
		}
	})
}
