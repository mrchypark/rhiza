package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnNextFreezeWhilePublicReadySnapshotIsPreviousConfiguration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	current := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{
		peerMember("next-freeze", "a", "a"),
		peerMember("next-freeze", "b", "b"),
		peerMember("next-freeze", "c", "c"),
	}}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: map[quepaxa.NodeID]bool{"b": true}}
	for _, member := range current.Members {
		transport.ids = append(transport.ids, member.ID)
		transport.cores[member.ID] = newPendingLearnTestCore(t, member.ID, current, transport)
	}
	n := &Node{config: &types.ExecutionConfig{EnableReconfiguration: true}, core: transport.cores["b"]}
	n.opened.Store(true)
	n.ready.Store(true)
	previous := current
	previous.ConfigID = 1
	n.rememberReadyCluster(&previous)
	if n.Ready() || n.peerLearnReady() {
		t.Fatal("previous-config snapshot unexpectedly opened public readiness")
	}
	target := quepaxa.Cluster{ConfigID: 3, Members: current.Members[:2]}
	freeze, err := transport.cores["a"].BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := transport.cores["a"].CertifiedValue(freeze)
	if !ok {
		t.Fatal("source did not certify the next freeze")
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(value.Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = value.Value
	if _, pending := n.core.PendingReconfiguration(); pending {
		t.Fatal("receiver already has the freeze")
	}
	if !n.peerLearnDecisionAllowed(decision) {
		t.Fatal("certified next freeze rejected solely while public ready snapshot is stale")
	}
	wrongConfig := decision
	wrongConfig.ConfigID++
	wrongSlot := decision
	wrongSlot.Slot++
	wrongHash := decision
	wrongHash.Proposal.Hash[0] ^= 1
	var control map[string]json.RawMessage
	if err := json.Unmarshal(value.Value[len("QRCF\x00"):], &control); err != nil {
		t.Fatal(err)
	}
	var prefix [32]byte
	if err := json.Unmarshal(control["prefix_hash"], &prefix); err != nil {
		t.Fatal(err)
	}
	prefix[0] ^= 1
	control["prefix_hash"], err = json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	wrongPrefix := decision
	wrongPrefix.Proposal.Value = append([]byte("QRCF\x00"), payload...)
	wrongPrefix.Proposal.Hash = quepaxa.ValueHash(sha256.Sum256(wrongPrefix.Proposal.Value))
	for _, invalid := range []quepaxa.Decision{wrongConfig, wrongSlot, wrongHash, wrongPrefix} {
		if n.peerLearnDecisionAllowed(invalid) {
			t.Fatal("next-freeze admission accepted mismatched config, slot, value hash, or prefix")
		}
	}
	n.terminalRequested.Store(true)
	if n.peerLearnDecisionAllowed(decision) {
		t.Fatal("terminated node accepted next freeze")
	}
	n.terminalRequested.Store(false)
	n.ready.Store(false)
	if n.peerLearnDecisionAllowed(decision) {
		t.Fatal("node without completed initial readiness accepted next freeze")
	}
	n.ready.Store(true)
	invalidCertificate := decision
	invalidCertificate.Summaries = nil
	if err := n.core.AcceptDecision(invalidCertificate); err == nil {
		t.Fatal("next freeze bypassed certificate validation")
	}
	if err := n.core.AcceptDecision(decision); err != nil {
		t.Fatal(err)
	}
	if n.Ready() {
		t.Fatal("learning a freeze opened public readiness")
	}
}
