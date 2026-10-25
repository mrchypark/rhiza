package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestNodeOpenLiveQuorumServesUserWrites(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	for _, tc := range []struct {
		name          string
		pendingFreeze bool
	}{
		{name: "intact_membership"},
		{name: "pending_freeze_archive_then_current_quorum", pendingFreeze: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runStartupOpenWithLiveQuorum(t, binary, tc.pendingFreeze)
		})
	}
}

func runStartupOpenWithLiveQuorum(t *testing.T, binary string, pendingFreeze bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	store := newStartupArchiveS3(t, ctx, binary)
	clusterID := "startup-live-quorum-" + strings.ReplaceAll(t.Name(), "/", "-")
	members, tokens, peerAddr, held := startupArchiveMembers(t, clusterID, "b", []string{"a", "b", "c"})
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	source := newStartupArchiveCores(t, cluster, true)
	bucket := store.newBucket(t)
	seedStartupArchiveVoterRegistrations(t, ctx, store, source, bucket, clusterID, members, tokens, peerAddr)
	if _, _, err := source.cores["a"].Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{7, 3, 1})); err != nil {
		t.Fatalf("commit initial certified barrier: %v", err)
	}

	archive := recovery.NewManager(bucket, filepath.Join("startup", clusterID), recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatalf("load startup archive: %v", err)
	}
	archiveMembers := members
	archiveThrough := source.cores["a"].Tip()
	wantConfigID := uint(1)
	if pendingFreeze {
		target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)}
		if _, err := source.cores["a"].BeginReconfiguration(ctx, target); err != nil {
			t.Fatalf("commit removal freeze: %v", err)
		}
		freeze := source.cores["a"].Tip()
		if source.cores["b"].Tip() != freeze {
			t.Fatalf("retained source voter tip=%d, want freeze %d", source.cores["b"].Tip(), freeze)
		}
		archiveThrough = freeze
		if err := source.wals["b"].Sync(); err != nil {
			t.Fatalf("sync retained voter WAL at freeze: %v", err)
		}
		if err := archive.SyncThrough(ctx, source.cores["a"], freeze); err != nil {
			t.Fatalf("publish freeze prefix: %v", err)
		}
		dataDir := cloneStartupArchiveNodeData(t, source.dataDirs["b"])
		if err := source.cores["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatalf("commit removal terminal with the original quorum: %v", err)
		}
		archiveMembers = target.Members
		wantConfigID = target.ConfigID
		config := store.executionConfig(t, clusterID, "b", tokens["b"], peerAddr, members)
		config.DataDir = dataDir
		node := New(config)
		if err := node.Open(ctx); err != nil {
			t.Fatalf("Node.Open from retained freeze WAL: %v", err)
		}
		defer func() {
			if err := node.Shutdown(); err != nil {
				t.Errorf("shutdown Node: %v", err)
			}
		}()
		if node.Ready() {
			t.Fatal("retained freeze WAL became serving-ready without a current quorum")
		}
		terminal := source.cores["a"].Tip()
		if terminal <= archiveThrough {
			t.Fatalf("terminal slot=%d did not advance beyond freeze=%d", terminal, archiveThrough)
		}
		if err := archive.SyncThrough(ctx, source.cores["a"], terminal); err != nil {
			t.Fatalf("publish certified removal terminal: %v", err)
		}
		closeStartupPeerSockets(t, held)
		startStartupArchivePeerServers(t, ctx, clusterID, "b", source, archiveMembers, tokens)
		assertStartupNodeReadyAndWritable(t, ctx, node, wantConfigID, archiveMembers)
		return
	}

	if err := source.wals["b"].Sync(); err != nil {
		t.Fatalf("sync intact voter WAL: %v", err)
	}
	if err := archive.SyncThrough(ctx, source.cores["a"], quepaxa.Slot(archiveThrough)); err != nil {
		t.Fatalf("publish intact membership prefix: %v", err)
	}
	dataDir := cloneStartupArchiveNodeData(t, source.dataDirs["b"])
	closeStartupPeerSockets(t, held)
	config := store.executionConfig(t, clusterID, "b", tokens["b"], peerAddr, members)
	config.DataDir = dataDir
	node := New(config)
	if err := node.Open(ctx); err != nil {
		t.Fatalf("Node.Open with intact voter identity/WAL: %v", err)
	}
	defer func() {
		if err := node.Shutdown(); err != nil {
			t.Errorf("shutdown Node: %v", err)
		}
	}()
	startStartupArchivePeerServers(t, ctx, clusterID, "b", source, members, tokens)
	assertStartupNodeReadyAndWritable(t, ctx, node, wantConfigID, members)
}

func startStartupArchivePeerServers(t *testing.T, ctx context.Context, clusterID string, localID quepaxa.NodeID, source *startupArchiveCores, members []quepaxa.Member, tokens map[quepaxa.NodeID]string) {
	t.Helper()
	const adminToken = "startup-test-admin-token"
	for _, member := range members {
		if member.ID == localID {
			continue
		}
		material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 4)
		if err != nil {
			t.Fatalf("open current voter %s materializer: %v", member.ID, err)
		}
		t.Cleanup(func() {
			if err := material.Close(); err != nil {
				t.Errorf("close current voter %s materializer: %v", member.ID, err)
			}
		})
		server := network.NewServer(source.cores[member.ID], material, types.ClusterID(clusterID), false, nil)
		peer, err := network.StartPeerServer(ctx, strings.TrimPrefix(member.PeerURL, "quic://"), server, members, tokens[member.ID], adminToken)
		if err != nil {
			server.Close()
			t.Fatalf("start current voter %s peer server: %v", member.ID, err)
		}
		t.Cleanup(func() {
			if err := peer.Close(); err != nil {
				t.Errorf("close current voter %s peer server: %v", member.ID, err)
			}
			server.Close()
		})
	}
}

func assertStartupNodeReadyAndWritable(t *testing.T, ctx context.Context, node *Node, configID uint, members []quepaxa.Member) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current := node.core.CurrentCluster()
		if node.Ready() && current.ConfigID == configID && sameStartupMembers(current.Members, members) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Node did not become Ready for current membership: ready=%t current=%+v: %v", node.Ready(), current, ctx.Err())
		case <-ticker.C:
		}
	}
	if !node.Ready() {
		t.Fatal("Node lost Ready before user write")
	}
	if _, err := node.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "startup-live-quorum-create",
		SQL:       "CREATE TABLE startup_live_quorum (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
	}); err != nil {
		t.Fatalf("execute schema mutation through current quorum: %v", err)
	}
	if _, err := node.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "startup-live-quorum-insert",
		SQL:       "INSERT INTO startup_live_quorum VALUES (1, 'served')",
	}); err != nil {
		t.Fatalf("execute user write through current quorum: %v", err)
	}
	result, err := node.server.Query(ctx, network.QueryRequest{
		SQL: "SELECT value FROM startup_live_quorum WHERE id=1", Consistency: "linearizable",
	})
	if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "served" {
		t.Fatalf("linearizable readback rows=%v err=%v", result.Rows, err)
	}
	if result.ConsensusTip < result.AppliedSlot {
		t.Fatalf("linearizable readback consensus tip=%d is behind applied slot=%d", result.ConsensusTip, result.AppliedSlot)
	}
}

func sameStartupMembers(left, right []quepaxa.Member) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ID != right[i].ID || left[i].PublicKey != right[i].PublicKey || left[i].WALIdentity != right[i].WALIdentity || left[i].PeerURL != right[i].PeerURL {
			return false
		}
	}
	return true
}
