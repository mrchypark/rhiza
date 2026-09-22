package network

import (
	"context"
	"errors"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"strings"
	"testing"
	"time"
)

func TestTempAdmissionNeverCertifies(t *testing.T) {
	ctx := context.Background()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	m, err := materializer.Open(t.TempDir()+"/db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s := NewServer(core, m, "temp-policy", true, nil)
	defer s.Close()
	calls := []func() (ExecuteResponse, error){
		func() (ExecuteResponse, error) {
			return s.Execute(ctx, ExecuteRequest{RequestID: "temp", SQL: "CREATE TEMP TABLE scratch(id)"})
		},
		func() (ExecuteResponse, error) {
			return s.Execute(ctx, ExecuteRequest{RequestID: "temp", Statements: []types.SQLStatement{{SQL: "CREATE TABLE durable(id)"}, {SQL: "CREATE TEMP VIEW scratch AS SELECT 1"}}})
		},
		func() (ExecuteResponse, error) {
			return s.ExecuteReturning(ctx, ExecuteRequest{RequestID: "temp", SQL: "SELECT * FROM 'temp'.scratch"})
		},
		func() (ExecuteResponse, error) {
			return s.Migrate(ctx, MigrationRequest{RequestID: "temp", Version: 1, Name: "temp", Checksum: strings.Repeat("0", 64), Statements: []types.SQLStatement{{SQL: "CREATE TABLE durable(id)"}, {SQL: "CREATE TEMP TRIGGER scratch AFTER INSERT ON durable BEGIN SELECT 1; END"}}})
		},
	}
	for _, query := range []string{"CREATE \vTEMP TABLE scratch(id)", "CREATE \ufeffTEMP TABLE scratch(id)", "CREATE TABLE \ufefftemp.scratch(id)"} {
		calls = append(calls, func() (ExecuteResponse, error) { return s.Execute(ctx, ExecuteRequest{RequestID: "temp", SQL: query}) })
	}
	for _, call := range calls {
		if _, err := call(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("admission=%v", err)
		}
		if core.Tip() != 0 || m.Tip() != 0 {
			t.Fatal("certified rejected SQL")
		}
		if _, found, err := m.MutationReceipt(ctx, types.MutationSQL, "temp"); err != nil || found {
			t.Fatal("receipt created", err)
		}
	}
}

func TestOldPolicyPeersCannotNegotiate(t *testing.T) {
	member := testMember("policy-peer", "n1", "voter")
	cluster := quepaxa.Cluster{Members: []quepaxa.Member{member}}
	core := mustCore(t, "n1", cluster.Members, nil, nil)
	server := NewServer(core, nil, "policy-peer", true, nil)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, cluster.Members, "voter", "admin")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	cluster.Members[0].PeerURL = "quic://" + peer.Addr()
	for _, protocol := range []string{"rhiza-peer-v3", "rhiza-peer-v4", "rhiza-peer-v5"} {
		client := NewTransport("policy-peer", "n1", &cluster, "voter")
		client.tls.NextProtos = []string{protocol}
		_, err = client.ReadTip(ctx, "n1")
		client.Close()
		if err == nil || !strings.Contains(err.Error(), "application protocol") {
			t.Fatalf("%s handshake=%v", protocol, err)
		}
	}
}
