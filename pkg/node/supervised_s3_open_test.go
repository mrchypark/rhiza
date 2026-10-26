package node

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// TestNodeOpenCooperativeS3WithBeforeAckProfile exercises startup in the same
// three-member S3/before-ack configuration as the Kind workload. This covers
// Node.Open and cooperative binding; it does not claim a client ACK was issued.
func TestNodeOpenCooperativeS3WithBeforeAckProfile(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	if os.Getenv("RHIZA_VERSITY_EVIDENCE_DIR") == "" {
		t.Skip("set RHIZA_VERSITY_EVIDENCE_DIR to a persistent directory outside Go temp")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := gateway.Close(closeCtx); err != nil {
			t.Errorf("reap local gateway: %v", err)
			return
		}
		completion, err := os.ReadFile(gateway.CompletionPath)
		if err != nil {
			t.Errorf("read gateway cleanup receipt: %v", err)
			return
		}
		if !strings.Contains(string(completion), "gateway_reaped=true\n") || !strings.Contains(string(completion), "forced=0\n") {
			t.Errorf("gateway cleanup was not verified: %q", completion)
		}
	})

	client, _, err := gateway.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	const bucketName = "rhiza"
	if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}

	const (
		clusterID = "chaos-source"
		nodeID    = quepaxa.NodeID("rhiza-0")
		peerToken = "fencing-peer-rhiza-0"
	)
	members := []quepaxa.Member{
		kindS3TestMember(clusterID, "rhiza-0", "fencing-peer-rhiza-0"),
		kindS3TestMember(clusterID, "rhiza-1", "fencing-peer-rhiza-1"),
		kindS3TestMember(clusterID, "rhiza-2", "fencing-peer-rhiza-2"),
	}
	config := &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: clusterID, NodeID: nodeID,
		BindAddr: "127.0.0.1:0", PeerAddr: "127.0.0.1:0",
		AdminToken: "fencing-test-admin", PeerToken: peerToken, Members: members,
		EnableReconfiguration: true,
		ObjStoreProvider:      "s3",
		ObjStoreEndpoint:      strings.TrimPrefix(gateway.Endpoint, "http://"),
		ObjStoreBucket:        bucketName,
		ObjStorePrefix:        "rhiza",
		ObjStoreRegion:        "us-east-1",
		ObjStoreInsecure:      true,
		ObjStoreAccessKey:     gateway.AccessKey,
		ObjStoreSecretKey:     gateway.SecretKey,
		ObjStoreDurability:    types.ObjectStoreDurabilityBeforeAck,
		ObjStoreSyncInterval:  time.Second,
	}

	identity := fencing.Identity{
		SourceClusterID: clusterID,
		NodeID:          string(nodeID),
		PodName:         string(nodeID),
		PodUID:          "kind-rhiza-0-pod-uid",
		RunID:           "kind-rhiza-0-run",
	}
	socketDir, err := os.MkdirTemp("", "rhiza-s3-open-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(socketDir); err != nil {
			t.Errorf("remove startup socket directory: %v", err)
		}
	})
	socket := filepath.Join(socketDir, "fence.sock")
	var startupCalls, bindCalls atomic.Int32
	startCooperativeOpenSupervisor(t, socket, identity, &startupCalls, &bindCalls)
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "kind-test-ticket")
	t.Setenv("RHIZA_POD_NAME", string(nodeID))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	n := New(config)
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Errorf("shutdown opened node: %v", err)
		}
	})
	if err := n.Open(ctx); err != nil {
		t.Fatalf("open supervised three-member S3 node: %v", err)
	}

	if startupCalls.Load() != 1 || bindCalls.Load() != 1 {
		t.Fatalf("supervisor calls startup=%d bind=%d, want one each", startupCalls.Load(), bindCalls.Load())
	}
	if n.core == nil || n.wal == nil {
		t.Fatal("Node.Open returned before WAL and consensus initialization completed")
	}
	if n.fenceSession == nil || n.fenceSession.StartupRole() != fencing.StartupRoleVoterActive {
		t.Fatalf("startup role=%v, want active voter", n.fenceSession)
	}
	if bound := n.fenceSession.Identity(); bound.WALIdentity == "" || bound.WALIdentity != n.core.WALIdentity() {
		t.Fatalf("WAL binding was not retained by the opened core: session=%+v core=%q", bound, n.core.WALIdentity())
	}
	if _, producer := n.fenceSession.MutationBinding(); producer {
		t.Fatal("nil-producer cooperative bind receipt unexpectedly created a mutation producer")
	}
	if n.protectedBucket == nil || !n.protectedBucket.cooperativeDirect || !n.fenceWriterActive.Load() {
		t.Fatal("successful active nil-producer bind did not enable cooperative S3 writes")
	}
	if n.archive == nil || n.config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		t.Fatalf("before-ack archive path not retained: archive=%t durability=%q", n.archive != nil, n.config.ObjStoreDurability)
	}

	if err := n.Shutdown(); err != nil {
		t.Fatalf("shutdown node: %v", err)
	}
}

func kindS3TestMember(clusterID string, id quepaxa.NodeID, token string) quepaxa.Member {
	member := peerMember(clusterID, id, token)
	member.URL = "http://" + string(id) + ".rhiza-headless:8080"
	member.PeerURL = "quic://" + string(id) + ".rhiza-headless:9090"
	return member
}

func startCooperativeOpenSupervisor(t *testing.T, socket string, identity fencing.Identity, startupCalls, bindCalls *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/startup":
			startupCalls.Add(1)
			_ = json.NewEncoder(w).Encode(fencing.StartupGrant{Identity: identity, Role: fencing.StartupRoleVoterActive})
		case "/v1/bind":
			var bound fencing.Identity
			if err := json.NewDecoder(r.Body).Decode(&bound); err != nil {
				http.Error(w, "invalid bind identity", http.StatusBadRequest)
				return
			}
			want := identity
			if bound.WALIdentity == "" || bound.SourceClusterID != want.SourceClusterID || bound.NodeID != want.NodeID || bound.PodName != want.PodName || bound.PodUID != want.PodUID || bound.RunID != want.RunID || bound.AssignmentID != "" {
				http.Error(w, "bind identity differs", http.StatusForbidden)
				return
			}
			bindCalls.Add(1)
			_ = json.NewEncoder(w).Encode(fencing.BindReceipt{Identity: bound, Role: fencing.StartupRoleVoterActive})
		case "/v1/stop":
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("shutdown startup supervisor: %v", err)
		}
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("serve startup supervisor: %v", err)
			}
		case <-ctx.Done():
			t.Errorf("startup supervisor did not stop: %v", ctx.Err())
		}
	})
}
