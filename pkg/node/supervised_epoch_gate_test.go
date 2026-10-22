package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type unreadableArchiveHeadBucket struct{ objstore.Bucket }

func (b unreadableArchiveHeadBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == "cluster/archive/head.bin" {
		return nil, errors.New("injected archive HEAD read failure")
	}
	return b.Bucket.Get(ctx, name)
}

func TestSupervisedArchiveStartupRequiresReadableUnsealedArchive(t *testing.T) {
	ctx := context.Background()
	if err := loadNodeArchive(ctx, nil, types.ObjectStoreDurabilityAsync, true); err == nil {
		t.Fatal("supervised async startup accepted a missing archive")
	}
	if err := loadNodeArchive(ctx, nil, types.ObjectStoreDurabilityAsync, false); err != nil {
		t.Fatalf("standalone async startup without an archive: %v", err)
	}

	base := objstore.NewInMemBucket()
	if err := base.Upload(ctx, "cluster/archive/head.bin", strings.NewReader("head")); err != nil {
		t.Fatal(err)
	}
	unreadable := recovery.NewManager(unreadableArchiveHeadBucket{Bucket: base}, "cluster", 1)
	defer unreadable.Close()
	if err := loadNodeArchive(ctx, unreadable, types.ObjectStoreDurabilityAsync, true); err == nil {
		t.Fatal("supervised async startup swallowed an unreadable archive HEAD")
	}
	if err := loadNodeArchive(ctx, unreadable, types.ObjectStoreDurabilityAsync, false); err != nil {
		t.Fatalf("standalone async startup changed its load-error policy: %v", err)
	}

	sealedBucket := sealedArchiveBucket(t)
	sealed := recovery.NewManager(sealedBucket, "cluster", 1)
	defer sealed.Close()
	if err := loadNodeArchive(ctx, sealed, types.ObjectStoreDurabilityAsync, true); !errors.Is(err, recovery.ErrArchiveSealed) {
		t.Fatalf("supervised async startup seal error=%v, want %v", err, recovery.ErrArchiveSealed)
	}
}

func TestSupervisedAsyncArchiveGateAppliesToNonVoter(t *testing.T) {
	ctx := context.Background()
	archive := recovery.NewManager(sealedArchiveBucket(t), "cluster", 1)
	defer archive.Close()

	member := peerMember("cluster", "source", "source-token")
	wal, err := qlog.Open(t.TempDir() + "/learner-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: "learner", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}, WAL: wal, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(t.TempDir()+"/learner.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	if core.IsVoter() {
		t.Fatal("fixture learner unexpectedly started as a voter")
	}
	if err := installArchiveDurabilityBarriers(server, core, archive, types.ObjectStoreDurabilityAsync, true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Query(ctx, network.QueryRequest{SQL: "SELECT 1", Consistency: "local"}); !errors.Is(err, recovery.ErrArchiveSealed) {
		t.Fatalf("non-voter query error=%v, want sealed-archive rejection", err)
	}
}

func TestStandaloneAsyncDoesNotInstallArchiveGate(t *testing.T) {
	ctx := context.Background()
	archive := recovery.NewManager(sealedArchiveBucket(t), "cluster", 1)
	defer archive.Close()

	wal, err := qlog.Open(t.TempDir() + "/source-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "source", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}, WAL: wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(t.TempDir()+"/source.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	if err := installArchiveDurabilityBarriers(server, core, archive, types.ObjectStoreDurabilityAsync, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Query(ctx, network.QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatalf("standalone async read unexpectedly used the shared-archive gate: %v", err)
	}
}

func TestInvalidPreflightDoesNotEnrollWithSupervisor(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "supervisor.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var startups atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/startup" {
			startups.Add(1)
		}
		http.Error(w, "unexpected supervisor request", http.StatusForbidden)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")

	config := &types.ExecutionConfig{NodeID: "local", DataDir: t.TempDir(), Local: true, ObjStoreDurability: "invalid"}
	n := New(config)
	err = n.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid object-store durability") {
		t.Fatalf("Open error=%v, want invalid durability preflight error", err)
	}
	if got := startups.Load(); got != 0 {
		t.Fatalf("invalid configuration contacted supervisor %d times", got)
	}
}

func sealedArchiveBucket(t *testing.T) objstore.Bucket {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive := recovery.NewManager(bucket, "cluster", 1)
	defer archive.Close()
	wal, err := qlog.Open(t.TempDir() + "/archive-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "archive-writer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "archive-writer"}}}, WAL: wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("archive-test-decision")); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Seal(ctx, bucket, "cluster", "supervised-epoch-test"); err != nil {
		t.Fatal(err)
	}
	return bucket
}
