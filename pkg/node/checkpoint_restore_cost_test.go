package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// TestCheckpointRestoreCostExercisesNodeRecoveryPathS3 measures a real
// Node.Open recovery against an explicitly configured S3-compatible endpoint.
// It is opt-in so ordinary CI never writes to a provider.
func TestCheckpointRestoreCostExercisesNodeRecoveryPathS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	server, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := server.Close(closeCtx); err != nil {
			t.Errorf("stop local Versity Gateway: %v", err)
		}
		records, err := server.AccessRecords()
		if err != nil {
			t.Errorf("read final Versity access records: %v", err)
			return
		}
		t.Logf("versity_version=%q versity_binary_sha256=%s server_access_requests=%d", server.Version, server.BinarySHA256, versityfixture.RequestCount(records))
		for _, line := range records {
			t.Logf("VERSITY_ACCESS %s", line)
		}
	}()

	endpoint := strings.TrimPrefix(server.Endpoint, "http://")
	region := "us-east-1"
	bucketName := fmt.Sprintf("rhiza-node-restore-%d", time.Now().UnixNano())
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(server.AccessKey, server.SecretKey, ""), Secure: false, Region: region,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region}); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("rhiza-node-restore-cost/%d", time.Now().UnixNano())
	requestCount := func() uint64 {
		t.Helper()
		records, err := server.AccessRecords()
		if err != nil {
			t.Fatalf("read Versity access records: %v", err)
		}
		return versityfixture.RequestCount(records)
	}
	requestsBeforeNode := requestCount()
	config := &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: "node-restore-cost", NodeID: "n1",
		BindAddr: "127.0.0.1:0", PeerAddr: "127.0.0.1:0", PeerToken: "node-restore-cost-token",
		ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucketName,
		ObjStorePrefix: prefix, ObjStoreRegion: region, ObjStoreInsecure: true,
		ObjStoreAccessKey: server.AccessKey, ObjStoreSecretKey: server.SecretKey,
		ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
		Members:            []quepaxa.Member{peerMember("node-restore-cost", "n1", "node-restore-cost-token")},
	}

	cleanupBucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: endpoint, Bucket: bucketName,
		Region: region, Insecure: true, AccessKey: server.AccessKey, SecretKey: server.SecretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupBucket.Close()
	var requestsAfterRestore uint64
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		cleanupStart := requestCount()
		var names []string
		if err := cleanupBucket.Iter(cleanupCtx, prefix+"/", func(name string) error {
			names = append(names, name)
			return nil
		}, objstore.WithRecursiveIter()); err != nil {
			t.Errorf("list test prefix for cleanup: %v", err)
			return
		}
		for _, name := range names {
			if err := cleanupBucket.Delete(cleanupCtx, name); err != nil {
				t.Errorf("delete test object %q: %v", name, err)
			}
		}
		t.Logf("node_restore_cleanup_server_requests=%d", requestCount()-cleanupStart)
	}()

	first := New(config)
	defer first.Shutdown()
	firstOpenStarted := time.Now()
	if err := first.Open(ctx); err != nil {
		t.Fatalf("open initial node: %v", err)
	}
	firstOpen := time.Since(firstOpenStarted)
	if _, err := first.server.KVPut(ctx, network.KVMutationRequest{
		RequestID: "node-restore-cost-seed", Key: "restore-key", Value: []byte("restored-value"),
	}); err != nil {
		t.Fatalf("write state before checkpoint publication: %v", err)
	}
	firstBucket := first.bucket
	if err := first.Shutdown(); err != nil {
		t.Fatalf("shutdown and publish certified checkpoint: %v", err)
	}
	firstStats := firstBucket.Stats()
	requestsAfterInitialPublication := requestCount()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(filepath.Join(config.DataDir, "sqlite.db"+suffix)); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove materialized state %q before recovery: %v", suffix, err)
		}
	}

	second := New(config)
	defer second.Shutdown()
	secondOpenStarted := time.Now()
	if err := second.Open(ctx); err != nil {
		t.Fatalf("reopen node from S3 checkpoint: %v; restore phases=%+v", err, second.recoveryPhases)
	}
	secondOpen := time.Since(secondOpenStarted)
	phases := second.recoveryPhases
	if !phases.selectedCheckpoint || phases.checkpointIndex == 0 || phases.checkpointRootHash == ([32]byte{}) {
		t.Fatalf("Node.Open did not select a certified checkpoint for restoration: %+v", phases)
	}
	if !phases.checkpointDownloadVerifyStarted || !phases.checkpointDownloadVerifyComplete || phases.checkpointDownloadVerify <= 0 {
		t.Fatalf("Node.Open did not complete checkpoint download and verification: %+v", phases)
	}
	if !phases.materializerRestoreStarted || !phases.materializerRestoreComplete || phases.materializerRestore <= 0 {
		t.Fatalf("Node.Open did not complete materializer restore: %+v", phases)
	}
	if !second.Ready() || second.material.Tip() == 0 {
		t.Fatalf("recovered node ready=%t materialized tip=%d", second.Ready(), second.material.Tip())
	}
	if second.material.Tip() < phases.checkpointIndex {
		t.Fatalf("materialized tip=%d is behind selected checkpoint index=%d", second.material.Tip(), phases.checkpointIndex)
	}
	result, err := second.server.KVGet(ctx, network.KVGetRequest{Key: "restore-key", Consistency: "linearizable"})
	if err != nil || string(result.Value) != "restored-value" {
		t.Fatalf("restored value=%q error=%v", result.Value, err)
	}
	secondStats, ok := second.ObjectStoreStats()
	if !ok {
		t.Fatal("reopened node did not expose object-store counters")
	}
	requestsAfterRestore = requestCount()
	record, err := json.Marshal(map[string]any{
		"profile": "actual-node-open-s3-restore", "provider_qualification": false,
		"prefix": prefix, "seed_value_bytes": len("restored-value"),
		"initial_node_open_ms":                                             float64(firstOpen) / float64(time.Millisecond),
		"initial_node_stats_including_shutdown_publication":                firstStats,
		"initial_node_server_requests_including_open_seed_and_publication": requestsAfterInitialPublication - requestsBeforeNode,
		"reopened_node_open_ms":                                            float64(secondOpen) / float64(time.Millisecond),
		"reopened_node_stats_through_ready_and_readback":                   secondStats,
		"reopened_node_server_requests_through_ready_and_readback":         requestsAfterRestore - requestsAfterInitialPublication,
		"selected_checkpoint_index":                                        phases.checkpointIndex,
		"selected_checkpoint_root_hash":                                    hex.EncodeToString(phases.checkpointRootHash[:]),
		"checkpoint_download_verify_ms":                                    float64(phases.checkpointDownloadVerify) / float64(time.Millisecond),
		"checkpoint_download_verify_complete":                              phases.checkpointDownloadVerifyComplete,
		"materializer_restore_ms":                                          float64(phases.materializerRestore) / float64(time.Millisecond),
		"materializer_restore_complete":                                    phases.materializerRestoreComplete,
		"bytes_uploaded_semantics":                                         "client bytes consumed for upload attempts",
		"bytes_published_semantics":                                        "per-upload acknowledgement, not unique HEAD-reachable checkpoint bytes",
		"retry_attribution":                                                "use counters as reported; unknown metadata is not zero",
		"server_request_counts":                                            "reconcile from the local Versity access log; not inferred from client counters",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NODE_CHECKPOINT_RESTORE_COST %s", record)
}
