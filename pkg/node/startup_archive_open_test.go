package node

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestNodeOpenRefreshesFixedArchiveAfterInitialQuorumFailure(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := newStartupArchiveS3(t, ctx, binary)
	const clusterID = "startup-fixed-head"
	members, tokens, peerAddr, held := startupArchiveMembers(t, clusterID, "a", []string{"a", "b", "c"})
	defer closeStartupPeerSockets(t, held)
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	source := newStartupArchiveCores(t, cluster, true)
	if _, _, err := source.cores["a"].Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})); err != nil {
		t.Fatal(err)
	}

	publisher := store.newBucket(t)
	archive := recovery.NewManager(publisher, filepath.Join("startup", clusterID), recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	node := New(store.executionConfig(t, clusterID, "a", tokens["a"], peerAddr, members))
	if err := node.Open(ctx); err != nil {
		t.Fatalf("Node.Open: %v", err)
	}
	defer func() {
		if err := node.Shutdown(); err != nil {
			t.Errorf("shutdown Node: %v", err)
		}
	}()

	if err := archive.SyncThrough(ctx, source.cores["a"], source.cores["a"].Tip()); err != nil {
		t.Fatalf("publish fixed startup prefix: %v", err)
	}
	waitNodeMaterialTip(t, ctx, node, 1)
	node.archiveHintMu.Lock()
	attempted := node.archiveStartupCheck.attempted
	node.archiveHintMu.Unlock()
	if !attempted {
		t.Fatal("initial quorum failure did not consume the Open-scoped archive check")
	}
	want, ok := source.cores["a"].PrefixHash(1)
	got, gotOK := node.core.PrefixHash(1)
	if !ok || !gotOK || got != want {
		t.Fatalf("recovered prefix=%x present=%t, want=%x present=%t", got, gotOK, want, ok)
	}
	if node.Ready() {
		t.Fatal("archive-only recovery marked a node ready without current quorum")
	}
}

func TestNodeOpenConsumesPendingTerminalArchiveGap(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	store := newStartupArchiveS3(t, ctx, binary)
	const clusterID = "startup-pending-terminal"
	members, tokens, peerAddr, held := startupArchiveMembers(t, clusterID, "b", []string{"a", "b", "c"})
	defer closeStartupPeerSockets(t, held)
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	source := newStartupArchiveCores(t, initial, true)
	publisher := store.newBucket(t)
	seedStartupArchiveVoterRegistrations(t, ctx, store, source, publisher, clusterID, members, tokens, peerAddr)
	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{members[0], members[1]}}
	if _, err := source.cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("commit removal freeze: %v", err)
	}
	freeze := source.cores["a"].Tip()
	if source.cores["b"].Tip() != freeze {
		t.Fatalf("retained voter WAL tip=%d, want freeze %d", source.cores["b"].Tip(), freeze)
	}

	archive := recovery.NewManager(publisher, filepath.Join("startup", clusterID), recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, source.cores["a"], freeze); err != nil {
		t.Fatalf("publish freeze prefix: %v", err)
	}
	if err := source.wals["b"].Sync(); err != nil {
		t.Fatalf("sync retained source WAL at freeze: %v", err)
	}
	dataDir := cloneStartupArchiveNodeData(t, source.dataDirs["b"])
	if err := source.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("commit removal terminal with the original quorum: %v", err)
	}
	terminal := source.cores["a"].Tip()
	config := store.executionConfig(t, clusterID, "b", tokens["b"], peerAddr, members)
	config.DataDir = dataDir
	node := New(config)
	if err := node.Open(ctx); err != nil {
		t.Fatalf("Node.Open with intact freeze WAL: %v", err)
	}
	defer func() {
		if err := node.Shutdown(); err != nil {
			t.Errorf("shutdown Node: %v", err)
		}
	}()
	key, ok := node.core.StartupTerminalGapSnapshot()
	if !ok {
		t.Fatal("Node.Open did not preserve the locally certified pending terminal gap")
	}

	if err := archive.SyncThrough(ctx, source.cores["a"], terminal); err != nil {
		t.Fatalf("publish terminal prefix: %v", err)
	}
	waitNodeMaterialTip(t, ctx, node, uint64(terminal))
	if got := node.core.CurrentCluster(); got.ConfigID != target.ConfigID || !containsMember(got.Members, "b") || containsMember(got.Members, "c") {
		t.Fatalf("terminal archive did not install the exact pending target: %+v", got)
	}
	node.archiveHintMu.Lock()
	attempted := node.archiveHintLastAttempted == key
	node.archiveHintMu.Unlock()
	if !attempted {
		t.Fatal("startup terminal gap was not consumed under its captured key")
	}
	if node.Ready() {
		t.Fatal("archive terminal recovery marked the node ready without current quorum")
	}
}

func TestNodeOpenRemovedSingletonNeverBecomesReady(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := newStartupArchiveS3(t, ctx, binary)
	const clusterID = "startup-removed-singleton"
	members, tokens, peerAddr, held := startupArchiveMembers(t, clusterID, "b", []string{"a", "b"})
	defer closeStartupPeerSockets(t, held)
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	source := newStartupArchiveCores(t, initial, true)
	publisher := store.newBucket(t)
	seedStartupArchiveVoterRegistrations(t, ctx, store, source, publisher, clusterID, members, tokens, peerAddr)
	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{members[0]}}
	if _, err := source.cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("commit singleton-removal freeze: %v", err)
	}
	freeze := source.cores["a"].Tip()
	archive := recovery.NewManager(publisher, filepath.Join("startup", clusterID), recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, source.cores["a"], freeze); err != nil {
		t.Fatalf("publish singleton-removal freeze: %v", err)
	}
	if err := source.wals["b"].Sync(); err != nil {
		t.Fatalf("sync removed source WAL at freeze: %v", err)
	}
	dataDir := cloneStartupArchiveNodeData(t, source.dataDirs["b"])
	if err := source.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("commit singleton-removal terminal: %v", err)
	}
	terminal := source.cores["a"].Tip()
	if err := archive.SyncThrough(ctx, source.cores["a"], terminal); err != nil {
		t.Fatalf("publish singleton-removal terminal: %v", err)
	}
	config := store.executionConfig(t, clusterID, "b", tokens["b"], peerAddr, members)
	config.DataDir = dataDir
	node := New(config)
	if err := node.Open(ctx); err != nil {
		t.Fatalf("Node.Open with removed voter WAL: %v", err)
	}
	defer func() {
		if err := node.Shutdown(); err != nil {
			t.Errorf("shutdown Node: %v", err)
		}
	}()
	if got := node.core.CurrentCluster(); got.ConfigID != target.ConfigID || len(got.Members) != 1 || got.Members[0].ID != "a" {
		t.Fatalf("removed WAL recovered membership=%+v, want singleton %+v", got, target)
	}
	if node.core.IsVoter() || node.Ready() {
		t.Fatalf("removed singleton became a voter/ready: voter=%t ready=%t", node.core.IsVoter(), node.Ready())
	}
}

func cloneStartupArchiveNodeData(t *testing.T, source string) string {
	t.Helper()
	destination := t.TempDir()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected non-regular startup fixture file %q (%s)", relative, info.Mode())
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, contents, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("clone startup fixture data from %s: %v", source, err)
	}
	return destination
}

type startupArchiveTestStore struct {
	endpoint string
	bucket   string
	prefix   string
	access   string
	secret   string
}

func newStartupArchiveS3(t *testing.T, ctx context.Context, binary string) startupArchiveTestStore {
	t.Helper()
	gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel()
		if err := gateway.Close(closeCtx); err != nil {
			t.Errorf("reap Versity Gateway: %v", err)
		}
	})
	client, _, err := gateway.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	store := startupArchiveTestStore{
		endpoint: strings.TrimPrefix(gateway.Endpoint, "http://"),
		bucket:   fmt.Sprintf("rhiza-startup-%d", time.Now().UnixNano()),
		prefix:   "startup",
		access:   gateway.AccessKey,
		secret:   gateway.SecretKey,
	}
	if err := client.MakeBucket(ctx, store.bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
	return store
}

func (s startupArchiveTestStore) newBucket(t *testing.T) *objmetrics.MeteredBucket {
	t.Helper()
	bucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: s.endpoint, Bucket: s.bucket,
		Region: "us-east-1", Insecure: true, AccessKey: s.access, SecretKey: s.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("close test S3 bucket: %v", err)
		}
	})
	return bucket
}

func (s startupArchiveTestStore) executionConfig(t *testing.T, clusterID string, nodeID quepaxa.NodeID, peerToken, peerAddr string, members []quepaxa.Member) *types.ExecutionConfig {
	t.Helper()
	return &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: types.ClusterID(clusterID), NodeID: types.NodeID(nodeID),
		PeerAddr: peerAddr, PeerToken: peerToken, AdminToken: "startup-test-admin-token",
		Members: append([]quepaxa.Member(nil), members...), EnableReconfiguration: true,
		ObjStoreProvider: "s3", ObjStoreEndpoint: s.endpoint, ObjStoreBucket: s.bucket,
		ObjStorePrefix: s.prefix, ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
		ObjStoreAccessKey: s.access, ObjStoreSecretKey: s.secret,
		ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
	}
}

func seedStartupArchiveVoterRegistrations(t *testing.T, ctx context.Context, store startupArchiveTestStore, source *startupArchiveCores, bucket *objmetrics.MeteredBucket, clusterID string, members []quepaxa.Member, tokens map[quepaxa.NodeID]string, peerAddr string) {
	t.Helper()
	for _, member := range members {
		config := store.executionConfig(t, clusterID, member.ID, tokens[member.ID], peerAddr, members)
		config.DataDir = source.dataDirs[member.ID]
		local, err := loadVoterIdentity(config)
		if err != nil {
			t.Fatalf("load original %s voter identity before offline registration: %v", member.ID, err)
		}
		if err := ensureVoterIdentity(ctx, config, bucket, source.wals[member.ID], local, true); err != nil {
			t.Fatalf("offline-register original %s voter identity: %v", member.ID, err)
		}
		registered, err := loadVoterIdentity(config)
		if err != nil || registered.identity == nil || !registered.identity.Registered {
			t.Fatalf("verify original %s voter registration: registered=%t err=%v", member.ID, registered.identity != nil && registered.identity.Registered, err)
		}
	}
}

type startupArchiveCores struct {
	transport *membershipOperationTransport
	cores     map[quepaxa.NodeID]*quepaxa.Core
	wals      map[quepaxa.NodeID]*qlog.WAL
	dataDirs  map[quepaxa.NodeID]string
}

func newStartupArchiveCores(t *testing.T, cluster quepaxa.Cluster, reconfiguration bool) *startupArchiveCores {
	t.Helper()
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	source := &startupArchiveCores{
		transport: transport,
		cores:     make(map[quepaxa.NodeID]*quepaxa.Core),
		wals:      make(map[quepaxa.NodeID]*qlog.WAL),
		dataDirs:  make(map[quepaxa.NodeID]string),
	}
	for _, member := range cluster.Members {
		dataDir := t.TempDir()
		wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport,
			EnableReconfiguration: reconfiguration,
		})
		if err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		source.cores[member.ID], source.wals[member.ID], source.dataDirs[member.ID] = core, wal, dataDir
		transport.ids = append(transport.ids, member.ID)
	}
	transport.cores = source.cores
	t.Cleanup(func() {
		for id, wal := range source.wals {
			if wal != nil {
				if err := wal.Close(); err != nil {
					t.Errorf("close %s source WAL: %v", id, err)
				}
			}
		}
	})
	return source
}

func startupArchiveMembers(t *testing.T, clusterID string, localID quepaxa.NodeID, ids []string) ([]quepaxa.Member, map[quepaxa.NodeID]string, string, []net.PacketConn) {
	t.Helper()
	members := make([]quepaxa.Member, 0, len(ids))
	tokens := make(map[quepaxa.NodeID]string, len(ids))
	var localPeerAddr string
	var held []net.PacketConn
	for _, rawID := range ids {
		id := quepaxa.NodeID(rawID)
		token := "startup-peer-" + rawID
		member := peerMember(clusterID, id, token)
		var peerAddr string
		if id == localID {
			peerAddr = archiveTerminalPeerAddress(t)
			localPeerAddr = peerAddr
		} else if len(held) < 2 {
			conn, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, conn)
			peerAddr = conn.LocalAddr().String()
		} else {
			peerAddr = archiveTerminalPeerAddress(t)
		}
		member.PeerURL = "quic://" + peerAddr
		members = append(members, member)
		tokens[id] = token
	}
	return members, tokens, localPeerAddr, held
}

func closeStartupPeerSockets(t *testing.T, sockets []net.PacketConn) {
	t.Helper()
	for _, socket := range sockets {
		if err := socket.Close(); err != nil {
			t.Errorf("close silent peer socket: %v", err)
		}
	}
}

func waitNodeMaterialTip(t *testing.T, ctx context.Context, node *Node, want uint64) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for node.material.Tip() < want {
		select {
		case <-ctx.Done():
			t.Fatalf("materialized tip=%d, want at least %d: %v", node.material.Tip(), want, ctx.Err())
		case <-ticker.C:
		}
	}
}
