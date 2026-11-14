//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestAuthenticatedPeerReadIndexPersistsRetainedTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token"), testMember("cluster", "c", "c-token")}
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	tr := &removalCoreTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}}
	for _, m := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: initial, WAL: wal, Transport: tr, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		tr.cores[m.ID] = core
	}
	freeze, err := tr.cores["a"].BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: members[:2]})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	core := tr.cores["a"]
	core.TestDropTerminalDurability(freeze + 16)
	if got := core.ReadIndexParticipationReason(); got != "terminal_not_durable" {
		t.Fatalf("fixture reason=%s", got)
	}
	server := NewServer(core, nil, "cluster", true, nil, func() bool { return true })
	t.Cleanup(server.Close)
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, members, "a-token", "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	active := append([]quepaxa.Member(nil), members[:2]...)
	active[0].PeerURL = "quic://" + peer.Addr()
	client := NewTransport("cluster", "b", &quepaxa.Cluster{ConfigID: 2, Members: active}, "b-token")
	t.Cleanup(func() { _ = client.Close() })
	badClient := NewTransport("cluster", "b", &quepaxa.Cluster{ConfigID: 2, Members: active}, "wrong-token")
	t.Cleanup(func() { _ = badClient.Close() })
	if _, err := badClient.ReadTip(ctx, "a"); err == nil {
		t.Fatal("invalid identity was accepted")
	}
	if core.CanParticipateReadIndex() {
		t.Fatal("unauthenticated read activated voter")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := client.ReadTip(canceled, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
	if core.CanParticipateReadIndex() {
		t.Fatal("canceled read activated voter")
	}
	if _, err := client.ReadTip(ctx, "a"); err != nil {
		t.Fatalf("authenticated read-index: %v", err)
	}
	if !core.CanParticipateReadIndex() {
		t.Fatal("terminal remained undurable")
	}
}
