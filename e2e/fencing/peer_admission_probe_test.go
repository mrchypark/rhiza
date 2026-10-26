package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

type peerProbeUnavailableTransport struct{}

func (peerProbeUnavailableTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, fmt.Errorf("unavailable")
}
func (peerProbeUnavailableTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return fmt.Errorf("unavailable")
}
func (peerProbeUnavailableTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 0, fmt.Errorf("unavailable")
}
func (peerProbeUnavailableTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return fmt.Errorf("unavailable")
}
func (peerProbeUnavailableTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, fmt.Errorf("unavailable")
}

func TestPeerAdmissionProbeUsesAuthenticatedQUICAndRejectsPriorCredential(t *testing.T) {
	clusterID := "peer-probe-test"
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := []string{"token-a-current", "token-b-current", "token-c-current"}
	packets := make([]net.PacketConn, len(ids))
	transports := make([]*quic.Transport, len(ids))
	addresses := make([]string, len(ids))
	for i := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		packets[i] = packet
		transports[i] = &quic.Transport{Conn: packet}
		addresses[i] = "quic://" + packet.LocalAddr().String()
	}
	members := make([]quepaxa.Member, len(ids))
	current := make([]peerAdmissionCredential, len(ids))
	for i, id := range ids {
		members[i] = quepaxa.Member{ID: id, URL: addresses[i], PeerURL: addresses[i],
			PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), id, tokens[i]))}
		current[i] = peerAdmissionCredential{Member: members[i], Token: tokens[i], PodUID: "pod-" + string(id),
			SecretName: "secret-" + string(id), SecretUID: "secret-uid-" + string(id), ContainerID: "container-" + string(id),
			CredentialUID: "secret-uid-" + string(id)}
	}
	peers := make([]*network.PeerServer, len(ids))
	servers := make([]*network.Server, len(ids))
	wals := make([]*qlog.WAL, len(ids))
	serveCtx, stop := context.WithCancel(context.Background())
	defer func() {
		stop()
		for i := range peers {
			if peers[i] != nil {
				_ = peers[i].Close()
			}
			if servers[i] != nil {
				servers[i].Close()
			}
			if transports[i] != nil {
				_ = transports[i].Close()
			}
			if packets[i] != nil {
				_ = packets[i].Close()
			}
			if wals[i] != nil {
				_ = wals[i].Close()
			}
		}
	}()
	for i, id := range ids {
		wal, err := qlog.Open(t.TempDir() + "/" + string(id))
		if err != nil {
			t.Fatal(err)
		}
		wals[i] = wal
		identity := bytes.Repeat([]byte{byte(i + 1)}, 32)
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		members[i].WALIdentity = hex.EncodeToString(identity)
		current[i].Member = members[i]
	}
	for i, id := range ids {
		wal := wals[i]
		core, err := quepaxa.New(quepaxa.Config{NodeID: id, Cluster: quepaxa.Cluster{ConfigID: 7, Members: members}, WAL: wal,
			Transport: peerProbeUnavailableTransport{}})
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = network.NewServer(core, nil, types.ClusterID(clusterID), true, nil)
		peer, err := network.StartPeerServerOnTransport(serveCtx, transports[i], servers[i], members, tokens[i], "")
		if err != nil {
			t.Fatal(err)
		}
		peers[i] = peer
	}
	for i := range members {
		members[i].PeerURL = "quic://" + peers[i].Addr()
		current[i].Member = members[i]
	}
	// The server cores were created with the pre-listen addresses; they are
	// numerically identical to PeerServer.Addr because the packet sockets were
	// bound before constructing the membership.
	previousMember := quepaxa.Member{ID: "a-retired", URL: members[0].URL, PeerURL: members[0].PeerURL,
		WALIdentity: "wal-a-retired", PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), "a-retired", "token-a-old"))}
	previous := []peerAdmissionCredential{{Member: previousMember, Token: "token-a-old", PodUID: "pod-a-old", SecretName: "secret-a-old",
		SecretUID: "secret-a-old-uid", ContainerID: "container-a-old", CredentialUID: "secret-a-old-uid"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := runPeerAdmissionProbe(ctx, peerAdmissionProbeRequest{ClusterID: clusterID, ConfigID: 7, Current: current, Previous: previous})
	if err != nil {
		t.Fatalf("authenticated peer probe failed: %v", err)
	}
	if got.Schema != "rhiza-peer-admission-v1" || got.ConfigID != 7 || got.CurrentSuccesses != 9 || got.PreviousRejections != 3 {
		t.Fatalf("unexpected fixed probe result: %+v", got)
	}
}

func TestPeerAdmissionProbeRejectsTokenAliasAndMalformedInput(t *testing.T) {
	request := peerAdmissionProbeRequest{ClusterID: "cluster", ConfigID: 3}
	request.Current = make([]peerAdmissionCredential, 3)
	for i, id := range []quepaxa.NodeID{"a", "b", "c"} {
		token := "current-" + string(id)
		member := quepaxa.Member{ID: id, URL: "quic://127.0.0.1:1234", PeerURL: "quic://127.0.0.1:1234", WALIdentity: "wal-" + string(id),
			PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(request.ClusterID), id, token))}
		request.Current[i] = peerAdmissionCredential{Member: member, Token: token, PodUID: "pod", SecretName: "secret", SecretUID: "secret-uid", ContainerID: "container", CredentialUID: "secret-uid"}
	}
	request.Previous = []peerAdmissionCredential{peerAdmissionPreviousFixture(request.ClusterID, "retired", request.Current[0].Token)}
	if _, err := runPeerAdmissionProbe(context.Background(), request); err == nil || !strings.Contains(err.Error(), "aliases") {
		t.Fatalf("credential alias error=%v, want fail-closed alias rejection", err)
	}
	for _, raw := range []string{
		`{"clusterID":"x","configID":1,"extra":true}`,
		`{} {}`,
		`{"clusterID":"x","clusterID":"y"}`,
		`{"current":[{"member":{"id":"a","id":"b"}}]}`,
		strings.Repeat("x", peerAdmissionProbeMaxInput+1),
	} {
		if _, err := decodePeerAdmissionProbeRequest(strings.NewReader(raw)); err == nil {
			t.Fatalf("malformed request accepted (length %d)", len(raw))
		}
	}
}

func TestPeerAdmissionProbeRejectsOverlappingAndDuplicatePreviousIDs(t *testing.T) {
	request := peerAdmissionValidationFixture()
	overlap := request
	overlap.Previous = []peerAdmissionCredential{request.Current[0]}
	if _, err := runPeerAdmissionProbe(context.Background(), overlap); err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("overlapping previous identity error=%v", err)
	}
	duplicate := request
	duplicate.Previous = []peerAdmissionCredential{request.Previous[0], request.Previous[0]}
	if _, err := runPeerAdmissionProbe(context.Background(), duplicate); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate previous identity error=%v", err)
	}
}

func TestPeerAdmissionProbeRequiresExactWrappedAuthenticationDenial(t *testing.T) {
	response := errors.New("peer rejected request: peer authentication failed")
	phase := fmt.Errorf("peer RPC phase=response-decode elapsed=12ms cause_type=*errors.errorString: %w", response)
	valid := fmt.Errorf("%w: %v", errors.New("peer rejected request"), phase)
	if !isExactPeerAuthenticationDenial(valid) {
		t.Fatal("canonical response-decode authentication rejection was not recognized")
	}

	for name, candidate := range map[string]error{
		"lookalike without sentinel":     fmt.Errorf("%w: %v", errors.New("not the peer-rejection sentinel"), phase),
		"timeout with misleading suffix": fmt.Errorf("%w: peer rejected request: peer RPC phase=response-read elapsed=12ms cause_type=*net.OpError: peer authentication failed", errors.New("peer rejected request")),
		"different response denial":      fmt.Errorf("%w: peer RPC phase=response-decode elapsed=12ms cause_type=*errors.errorString: peer rejected request: cluster identity mismatch", errors.New("peer rejected request")),
		"malformed elapsed field":        fmt.Errorf("%w: peer RPC phase=response-decode elapsed=not-a-duration cause_type=*errors.errorString: peer rejected request: peer authentication failed", errors.New("peer rejected request")),
	} {
		t.Run(name, func(t *testing.T) {
			if isExactPeerAuthenticationDenial(candidate) {
				t.Fatal("non-authentication error was classified as the exact peer authentication denial")
			}
		})
	}
}

func peerAdmissionValidationFixture() peerAdmissionProbeRequest {
	clusterID := "peer-probe-validation"
	request := peerAdmissionProbeRequest{ClusterID: clusterID, ConfigID: 7}
	for _, id := range []quepaxa.NodeID{"a", "b", "c"} {
		token := "current-" + string(id)
		member := quepaxa.Member{ID: id, URL: "quic://127.0.0.1:1234", PeerURL: "quic://127.0.0.1:1234", WALIdentity: "wal-" + string(id),
			PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), id, token))}
		request.Current = append(request.Current, peerAdmissionCredential{Member: member, Token: token, PodUID: "pod-" + string(id),
			SecretName: "secret-" + string(id), SecretUID: "uid-" + string(id), ContainerID: "container-" + string(id), CredentialUID: "uid-" + string(id)})
	}
	request.Previous = []peerAdmissionCredential{peerAdmissionPreviousFixture(clusterID, "a-retired", "old-a")}
	return request
}

func peerAdmissionPreviousFixture(clusterID, id, token string) peerAdmissionCredential {
	memberID := quepaxa.NodeID(id)
	member := quepaxa.Member{ID: memberID, URL: "quic://127.0.0.1:1234", PeerURL: "quic://127.0.0.1:1234", WALIdentity: "wal-" + id,
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), memberID, token))}
	return peerAdmissionCredential{Member: member, Token: token, PodUID: "old-pod-" + id, SecretName: "old-secret-" + id,
		SecretUID: "old-uid-" + id, ContainerID: "old-container-" + id, CredentialUID: "old-uid-" + id}
}

func TestPeerAdmissionProbePinsPodReplicaSetDeploymentChain(t *testing.T) {
	deploymentName, deploymentUID := "rhiza-fencer", "deployment-uid"
	pod := obj{"metadata": obj{"ownerReferences": []any{obj{"kind": "ReplicaSet", "name": "fencer-abc", "uid": "rs-uid", "controller": true}}}}
	validRS := obj{"metadata": obj{"name": "fencer-abc", "uid": "rs-uid", "ownerReferences": []any{obj{"kind": "Deployment", "name": deploymentName, "uid": deploymentUID, "controller": true}}}}
	if name, uid, err := peerProbePodReplicaSet(pod, []any{validRS}, deploymentName, deploymentUID); err != nil || name != "fencer-abc" || uid != "rs-uid" {
		t.Fatalf("valid fencer ownership chain=(%q,%q,%v)", name, uid, err)
	}
	wrongDeployment := obj{"metadata": obj{"name": "fencer-abc", "uid": "rs-uid", "ownerReferences": []any{obj{"kind": "Deployment", "name": deploymentName, "uid": "replacement-deployment-uid", "controller": true}}}}
	wrongRSUID := obj{"metadata": obj{"ownerReferences": []any{obj{"kind": "ReplicaSet", "name": "fencer-abc", "uid": "replacement-rs-uid", "controller": true}}}}
	multipleRSOwners := obj{"metadata": obj{"ownerReferences": []any{
		obj{"kind": "ReplicaSet", "name": "fencer-abc", "uid": "rs-uid", "controller": true},
		obj{"kind": "ReplicaSet", "name": "fencer-def", "uid": "other-rs", "controller": true},
	}}}
	for name, candidate := range map[string]struct {
		pod any
		rs  []any
	}{
		"wrong deployment UID":     {pod: pod, rs: []any{wrongDeployment}},
		"wrong ReplicaSet UID":     {pod: wrongRSUID, rs: []any{validRS}},
		"multiple Pod controllers": {pod: multipleRSOwners, rs: []any{validRS}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := peerProbePodReplicaSet(candidate.pod, candidate.rs, deploymentName, deploymentUID); err == nil {
				t.Fatal("invalid fencer owner chain was accepted")
			}
		})
	}
}
