package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipRepairProbeRuntimeVerifiesExactCandidatePrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, identities, _, _ := newMembershipOperationNode(t)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "probe.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	n.material = material
	n.server = network.NewServer(n.core, material, "cluster", true, nil)
	t.Cleanup(func() { n.server.Close() })
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "probe-seed", SQL: "CREATE TABLE probe_seed (id INTEGER)"}); err != nil {
		t.Fatalf("seed source prefix: %v", err)
	}
	n.ready.Store(true)
	sourceCluster := n.core.CurrentCluster()
	sourceMembers := append([]quepaxa.Member(nil), sourceCluster.Members...)
	for i := range sourceMembers {
		sourceMembers[i].PeerURL = "quic://127.0.0.1:1"
	}
	sourcePeer, err := network.StartPeerServer(ctx, "127.0.0.1:0", n.server, sourceMembers, "a-token", "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourcePeer.Close() })
	for i := range sourceMembers {
		if sourceMembers[i].ID == "a" {
			sourceMembers[i].PeerURL = "quic://" + sourcePeer.Addr()
		}
	}
	var peerIDs []network.PeerIdentity
	for _, member := range sourceMembers {
		peerID, err := network.NewPeerIdentity("cluster", member)
		if err != nil {
			t.Fatal(err)
		}
		peerIDs = append(peerIDs, peerID)
	}
	learnerID := quepaxa.NodeID("candidate")
	learnerWAL, err := qlog.Open(filepath.Join(t.TempDir(), "candidate-qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = learnerWAL.Close() })
	learnerWALBytes := sha256.Sum256([]byte("membership-repair-candidate-wal"))
	if err := learnerWAL.BindIdentity(learnerWALBytes[:]); err != nil {
		t.Fatal(err)
	}
	learnerTransport := network.NewLearnerTransport("cluster", learnerID, sourceCluster.ConfigID, peerIDs, "admin-token")
	t.Cleanup(func() { _ = learnerTransport.Close() })
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: learnerID, Cluster: sourceCluster, WAL: learnerWAL,
		Transport: learnerTransport, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	learnerServer := network.NewServer(learnerCore, nil, "cluster", false, learnerTransport)
	t.Cleanup(func() { learnerServer.Close() })
	learner := quepaxa.Member{
		ID: learnerID, URL: "http://candidate",
		PublicKey:   peerMember("cluster", learnerID, "candidate-token").PublicKey,
		WALIdentity: hex.EncodeToString(learnerWALBytes[:]),
	}
	learnerPeer, err := network.StartLearnerPeerServer(ctx, "127.0.0.1:0", learnerServer, sourceMembers, "candidate-token", "admin-token", learner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = learnerPeer.Close() })
	learner.PeerURL = "quic://" + learnerPeer.Addr()
	if learner.LogURL != "" {
		t.Fatalf("test learner unexpectedly configured the unrelated log endpoint: %q", learner.LogURL)
	}
	n.transport = network.NewTransport("cluster", "a", &sourceCluster, "a-token")
	t.Cleanup(func() { _ = n.transport.Close() })
	identity := fencing.Identity{SourceClusterID: "cluster", NodeID: "a", PodUID: "pod-a", RunID: "run-a", WALIdentity: identities["a"]}
	session, _ := newMembershipRepairSession(t, identity, nil, 0)
	n.fenceSession = session
	n.server.SetRecoveryArchive("admin-token", func(context.Context) error { return nil })
	n.server.SetMembershipRepairProbe(n.verifyMembershipRepairProbe)

	abort := quepaxa.Slot(0)
	request := network.MembershipRepairProbeRequest{
		Version: 1, Challenge: stringsOf('a', 64), ScopeDigest: stringsOf('b', 64),
		SourceConfigID: sourceCluster.ConfigID, AbortSlot: &abort, ExcludedOldMemberID: "c",
		CandidateMember: learner, CandidateBindRef: "candidate-bind-ref",
	}
	invoke := func(candidate quepaxa.Member) *httptest.ResponseRecorder {
		req := request
		req.CandidateMember = candidate
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		httpRequest := httptest.NewRequest(http.MethodPost, "/recovery/probe", bytes.NewReader(body))
		httpRequest.Header.Set("Authorization", "Bearer admin-token")
		httpRequest.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		n.server.ServeHTTP(response, httpRequest)
		return response
	}

	wrongWAL := learner
	wrongWAL.WALIdentity = stringsOf('c', 64)
	if response := invoke(wrongWAL); response.Code == http.StatusOK {
		t.Fatalf("candidate with a different WAL binding received proof: %s", response.Body.String())
	}
	response := invoke(learner)
	if response.Code != http.StatusOK {
		t.Fatalf("exact candidate proof status=%d body=%s", response.Code, response.Body.String())
	}
	proof, err := network.StrictDecodeMembershipRepairProbeResponse(response.Body.Bytes(), request, "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	if proof.CandidateMember != learner || proof.CandidateBindRef != request.CandidateBindRef || proof.CandidatePrefixThrough < proof.ReadIndexSlot || proof.CandidatePrefixHash != proof.PrefixHash {
		t.Fatalf("runtime proof does not bind the candidate and observed prefix: %+v", proof)
	}

	wrongAbort := request
	wrongAbort.AbortSlot = new(quepaxa.Slot)
	*wrongAbort.AbortSlot = 1
	beforeTip := n.core.Tip()
	if _, err := n.verifyMembershipRepairProbe(ctx, wrongAbort); err == nil {
		t.Fatal("runtime accepted a changed abort revision")
	}
	if n.core.Tip() != beforeTip {
		t.Fatalf("invalid abort scope mutated consensus tip from %d to %d", beforeTip, n.core.Tip())
	}
}
