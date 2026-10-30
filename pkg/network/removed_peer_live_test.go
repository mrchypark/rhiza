package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type removalCoreTransport struct {
	cores map[quepaxa.NodeID]*quepaxa.Core
}

func (t *removalCoreTransport) SendRecord(ctx context.Context, id quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return t.cores[id].Record(ctx, r)
}
func (t *removalCoreTransport) SendDecision(ctx context.Context, d quepaxa.Decision) error {
	for _, c := range t.cores {
		if err := c.AcceptDecision(d); err != nil {
			return err
		}
	}
	return nil
}
func (t *removalCoreTransport) ReadTip(_ context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	return t.cores[id].Tip(), nil
}
func (t *removalCoreTransport) StageValue(_ context.Context, id quepaxa.NodeID, h quepaxa.ValueHash, v []byte) error {
	return t.cores[id].StageValue(h, v)
}
func (t *removalCoreTransport) FetchValue(_ context.Context, id quepaxa.NodeID, h quepaxa.ValueHash) ([]byte, error) {
	v, ok := t.cores[id].Value(h)
	if !ok {
		return nil, errors.New("missing value")
	}
	return v, nil
}

func TestRemovedPeerCannotReuseLiveQUICOrReturnAfterReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token"), testMember("cluster", "c", "c-token")}
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	tr := &removalCoreTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}}
	var aWAL *qlog.WAL
	var aDir string
	for _, m := range members {
		dir := filepath.Join(t.TempDir(), "wal")
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if m.ID == "a" {
			aWAL = wal
			aDir = dir
		} else {
			t.Cleanup(func() { _ = wal.Close() })
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: initial, WAL: wal, Transport: tr, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		tr.cores[m.ID] = core
	}
	aOpen := true
	t.Cleanup(func() {
		if aOpen {
			_ = aWAL.Close()
		}
	})
	server := NewServer(tr.cores["a"], nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, members, "a-token", "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	clientMembers := append([]quepaxa.Member(nil), members...)
	clientMembers[0].PeerURL = "quic://" + peer.Addr()
	client := NewTransport("cluster", "c", &quepaxa.Cluster{ConfigID: 1, Members: clientMembers}, "c-token")
	t.Cleanup(func() { _ = client.Close() })
	value := policySQL(t, "SELECT 1")
	hash := sha256.Sum256(value)
	if err := client.StageValue(ctx, "a", hash, value); err != nil {
		t.Fatalf("pre-removal RPC: %v", err)
	}
	conn, err := client.connection(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	target := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	if _, err := tr.cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := tr.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if conn.Context().Err() != nil {
		t.Fatal("old QUIC connection closed before post-removal check")
	}
	if err := client.StageValue(ctx, "a", hash, value); err == nil || !strings.Contains(err.Error(), "cluster identity mismatch") {
		t.Fatalf("removed voter RPC: %v", err)
	}
	request := client.request(peerfb.OperationRecord)
	request.Record = &peerfb.RecordRequestT{Slot: 1}
	if _, err := client.call(ctx, "a", request); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("historical envelope accepted removed voter: %v", err)
	}
	current := client.request(peerfb.OperationStageValue)
	current.ConfigId = 2
	current.Hash = hash[:]
	current.Value = value
	if _, err := client.call(ctx, "a", current); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("current envelope accepted removed voter: %v", err)
	}
	again, err := client.connection(ctx, "a")
	if err != nil || again != conn {
		t.Fatal("post-removal request did not reuse same QUIC connection")
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err := aWAL.Close(); err != nil {
		t.Fatal(err)
	}
	aOpen = false
	wal, err := qlog.Open(aDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	restarted, err := quepaxa.New(quepaxa.Config{NodeID: "a", Cluster: initial, WAL: wal, Transport: tr, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.CurrentCluster().ConfigID != 2 {
		t.Fatal("terminal not recovered")
	}
	if err := restarted.ValidateReconfigurationTarget(quepaxa.Cluster{ConfigID: 3, Members: members}); err == nil {
		t.Fatal("retired identity accepted after WAL replay")
	}
}
