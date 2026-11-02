package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedKVProfileConstructorRefusal(t *testing.T) {
	for _, bad := range []sqlpolicy.MaterializationPolicy{0, -1, 9} {
		if s, err := NewServerForPolicy(nil, nil, "cluster", false, nil, bad); s != nil || !errors.Is(err, sqlpolicy.ErrIncompatible) {
			t.Fatal("unknown profile", s, err)
		}
	}
	_, mixed, _ := managedKVRealServer(t, 8)
	_, legacy, _ := managedKVRealServer(t, 7)
	if NewServer(nil, mixed, "cluster", false, nil) != nil {
		t.Fatal("legacy facade accepted mixed dependency")
	}
	for _, pair := range []struct {
		mPolicy sqlpolicy.MaterializationPolicy
		mixed   bool
	}{{7, true}, {8, false}} {
		m := legacy
		if pair.mixed {
			m = mixed
		}
		if s, err := NewServerForPolicy(nil, m, "cluster", false, nil, pair.mPolicy); s != nil || !errors.Is(err, sqlpolicy.ErrIncompatible) {
			t.Fatal("materializer mismatch", s, err)
		}
	}
	transport := NewTransport("cluster", "n1", &quepaxa.Cluster{}, "")
	defer transport.Close()
	if s, err := NewServerForPolicy(nil, nil, "cluster", false, transport, 8); s != nil || !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatal("transport mismatch", s, err)
	}
}

func TestManagedKVProfileProductionPeer(t *testing.T) {
	for _, policy := range []sqlpolicy.MaterializationPolicy{7, 8} {
		member := testMember("cluster", "n1", "token")
		core := mustCore(t, "n1", []quepaxa.Member{member}, nil, nil)
		s, err := NewServerForPolicy(core, nil, "cluster", true, nil, policy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", s, []quepaxa.Member{member}, "token", "admin")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
		member.PeerURL = "quic://" + peer.Addr()
		config := quepaxa.Cluster{Members: []quepaxa.Member{member}}
		tr, err := NewTransportForPolicy("cluster", "n1", &config, "token", policy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := tr.ReadTip(ctx, "n1"); err != nil {
			cancel()
			t.Fatal("matching production peer failed", err)
		}
		cancel()
		if policy == 8 {
			legacy, _ := types.EncodeKVCommand(types.KVCommand{RequestID: "i", Operation: "put", Key: "k"})
			legacyItem, _ := types.EncodeKVBatchItem(types.KVCommand{RequestID: "i", Operation: "put", Key: "k"})
			graph, _ := types.EncodeGraphBatch([]types.GraphCommand{{RequestID: "g", Cypher: "RETURN 1"}})
			notify, _ := types.EncodeNotifyCommand(types.NotifyCommand{RequestID: "n", Topic: "t"})
			migration, _ := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "m", Migration: &types.SQLMigration{Version: 1, Name: "m", Checksum: "bad"}}})
			badTime, _ := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "i", Operation: "put", Key: "k", TTLMS: 1, ObservedAtUnixMS: 10, ExpiresAtUnixMS: 99})
			for _, value := range [][]byte{legacy, types.AssembleKVBatch([][]byte{legacyItem}), graph, notify, migration, badTime, []byte("QKVC\x01{}"), []byte("unknown")} {
				for _, operation := range []peerfb.Operation{peerfb.OperationStageValue, peerfb.OperationRecord, peerfb.OperationLearned} {
					req := tr.request(operation)
					hash := sha256.Sum256(value)
					req.Hash = hash[:]
					req.Value = value
					proposal := quepaxa.Proposal{ProposerID: "n1", Value: value, Hash: hash}
					req.Record = &peerfb.RecordRequestT{Slot: 1, Step: 1, Proposal: proposalToWire(proposal)}
					req.Decision = decisionToWire(quepaxa.Decision{Slot: 1, Step: 1, Proposal: proposal})
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, err := tr.call(ctx, "n1", req)
					cancel()
					expected := s.validateExecutionPolicy(value)
					if expected == nil || !errors.Is(err, errPeerRejected) || !strings.Contains(err.Error(), expected.Error()) {
						t.Fatalf("ingress %v accepted/rejected for wrong reason: %v", operation, err)
					}
					if core.Tip() != 0 || core.RecorderTip() != 0 {
						t.Fatal("rejected ingress changed history")
					}
				}
			}
			valid, _ := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "valid", Operation: "put", Key: "key"})
			hash := sha256.Sum256(valid)
			req := tr.request(peerfb.OperationStageValue)
			req.Value = valid
			req.Hash = hash[:]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := tr.call(ctx, "n1", req)
			cancel()
			if err != nil || core.Tip() != 0 || core.RecorderTip() != 0 {
				t.Fatal("valid mixed staged value rejected or advanced history", err)
			}
		}
		other := sqlpolicy.MaterializationPolicy(15 - int(policy))
		wrong, err := NewTransportForPolicy("cluster", "n1", &config, "token", other)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		_, err = wrong.ReadTip(ctx, "n1")
		cancel()
		_ = wrong.Close()
		if err == nil {
			t.Fatal("cross-profile production listener accepted")
		}
	}
}
