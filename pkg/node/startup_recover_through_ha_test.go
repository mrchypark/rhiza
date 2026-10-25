package node

import (
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNodeOpenRecoversClusteredDurableRecorderWithLiveQuorum(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := newStartupArchiveS3(t, ctx, binary)
	clusterID := "startup-ha-recorder"
	members, tokens, _, held := startupArchiveMembers(t, clusterID, "b", []string{"a", "b", "c"})
	closeStartupPeerSockets(t, held)
	t.Cleanup(cancel)

	nodeConfig := func(id quepaxa.NodeID, dataDir string) *types.ExecutionConfig {
		for _, member := range members {
			if member.ID == id {
				config := store.executionConfig(t, clusterID, id, tokens[id], strings.TrimPrefix(member.PeerURL, "quic://"), members)
				config.DataDir = dataDir
				config.EnableReconfiguration = false
				return config
			}
		}
		t.Fatalf("member %q is missing", id)
		return nil
	}

	openNode := func(id quepaxa.NodeID, dataDir string) (*Node, func()) {
		t.Helper()
		node := New(nodeConfig(id, dataDir))
		if err := node.Open(ctx); err != nil {
			t.Fatalf("open voter %s: %v", id, err)
		}
		active := true
		shutdown := func() {
			if !active {
				return
			}
			active = false
			if err := node.Shutdown(); err != nil {
				t.Errorf("shutdown voter %s: %v", id, err)
			}
		}
		t.Cleanup(shutdown)
		return node, shutdown
	}

	a, _ := openNode("a", t.TempDir())
	bDataDir := t.TempDir()
	b, shutdownB := openNode("b", bDataDir)

	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "startup-ha-record", SQL: "CREATE TABLE startup_ha_recorder(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "a", Hash: sha256.Sum256(value), Value: value}
	if summary, err := a.transport.SendRecord(ctx, "b", quepaxa.RecordRequest{
		Slot: 1, Step: 4, ConfigID: 1, Proposal: proposal,
	}); err != nil {
		t.Fatalf("authenticated peer record at b: %v", err)
	} else if summary.RecorderID != "b" {
		t.Fatalf("record reply recorder=%q, want b", summary.RecorderID)
	}
	if b.core.Tip() != 0 || b.core.RecorderTip() != 1 || b.core.IsDecided(1) {
		t.Fatalf("pre-restart B state tip=%d recorder=%d decided=%t, want 0/1/false", b.core.Tip(), b.core.RecorderTip(), b.core.IsDecided(1))
	}
	shutdownB()

	_, _ = openNode("c", t.TempDir())
	recovered, _ := openNode("b", bDataDir)
	if gotTip, gotRecorderTip := recovered.core.Tip(), recovered.core.RecorderTip(); gotTip != 1 || gotRecorderTip != 1 || !recovered.core.IsDecided(1) {
		t.Fatalf("recovered B state tip=%d recorder=%d decided=%t, want 1/1/true", gotTip, gotRecorderTip, recovered.core.IsDecided(1))
	}
	if _, err := recovered.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM startup_ha_recorder", Consistency: "linearizable"}); err != nil {
		t.Fatalf("query value recovered from durable recorder: %v", err)
	}
	assertStartupNodeReadyAndWritable(t, ctx, recovered, 1, members)
}
