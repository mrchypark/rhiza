//go:build rhiza_local_testhooks

package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	objstore "github.com/thanos-io/objstore"
)

// Adapted from the preserved prepared/unprepared comparison blob
// b7f1bd2eb4991ac0c3c85a58b1facd405d55768a. Ordinary Shutdown must now
// succeed without arranging its old private checkpoint-cache precondition.
func TestNodeOrdinaryParallelShutdownRetainsCertifiedArchive(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	for _, withBase := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint_base=%t", withBase), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
				defer stop()
				if err := gateway.Close(closeCtx); err != nil {
					t.Errorf("stop local gateway: %v", err)
				}
			}()
			endpoint := strings.TrimPrefix(gateway.Endpoint, "http://")
			const region = "us-east-1"
			bucketName := fmt.Sprintf("rhiza-shutdown-%d", time.Now().UnixNano())
			client, err := minio.New(endpoint, &minio.Options{
				Creds: credentials.NewStaticV4(gateway.AccessKey, gateway.SecretKey, ""), Secure: false, Region: region,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region}); err != nil {
				t.Fatal(err)
			}
			const clusterID = "ordinary-parallel-shutdown"
			ids := []quepaxa.NodeID{"n1", "n2", "n3"}
			tokens := []string{"shutdown-peer-n1", "shutdown-peer-n2", "shutdown-peer-n3"}
			members := make([]quepaxa.Member, len(ids))
			addresses := make([]string, len(ids))
			for i, id := range ids {
				conn, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				addresses[i] = conn.LocalAddr().String()
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				members[i] = peerMember(clusterID, id, tokens[i])
				members[i].PeerURL = "quic://" + addresses[i]
			}
			nodes := make([]*Node, 0, 3)
			defer func() {
				for _, node := range nodes {
					if err := node.Shutdown(); err != nil {
						t.Errorf("failure-path shutdown: %v", err)
					}
				}
			}()
			prefix := fmt.Sprintf("rhiza-parallel-shutdown/%d", time.Now().UnixNano())
			for i, id := range ids {
				node := New(&types.ExecutionConfig{
					DataDir: t.TempDir(), ClusterID: clusterID, NodeID: types.NodeID(id),
					PeerAddr: addresses[i], PeerToken: tokens[i], Members: members,
					ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucketName,
					ObjStorePrefix: prefix, ObjStoreRegion: region, ObjStoreInsecure: true,
					ObjStoreAccessKey: gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
					ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
					ObjStoreDurability: types.ObjectStoreDurabilityAsync,
				})
				nodes = append(nodes, node)
				if err := node.Open(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := waitForNodeReadiness(ctx, nodes); err != nil {
				t.Fatal(err)
			}
			put := func(request, value string) {
				t.Helper()
				if _, err := nodes[0].server.KVPut(ctx, network.KVMutationRequest{RequestID: request, Key: "key", Value: []byte(value)}); err != nil {
					t.Fatal(err)
				}
				if err := waitForNodeTip(ctx, nodes, nodes[0].core.Tip()); err != nil {
					t.Fatal(err)
				}
			}
			put("base-value", "base")
			if withBase {
				// Establish an older certified base while peers are alive. The
				// subsequent mutation must still be recovered from the suffix;
				// this is not pre-shutdown preparation of the final state.
				if err := nodes[0].checkpointer.CheckpointOnShutdown(ctx, nodes[0].material.StateTip()); err != nil {
					t.Fatal(err)
				}
			}
			put("suffix-value", "suffix")
			through := nodes[0].core.Tip()
			start := make(chan struct{})
			results := make(chan error, len(nodes))
			for _, node := range nodes {
				go func() { <-start; results <- node.Shutdown() }()
			}
			close(start)
			// Join every Close before testing its result, including failure.
			var closeErr error
			for range nodes {
				closeErr = errors.Join(closeErr, <-results)
			}
			if closeErr != nil {
				t.Fatalf("ordinary parallel shutdown: %v", closeErr)
			}
			bucket, err := objmetrics.NewBucket(objmetrics.Config{
				Provider: objmetrics.ProviderS3, Endpoint: endpoint, Bucket: bucketName,
				Region: region, Insecure: true, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			archive := recovery.NewManager(bucket, path.Join(prefix, clusterID), 1)
			defer archive.Close()
			if err := archive.Load(ctx); err != nil {
				t.Fatal(err)
			}
			seal, _, hasBase := archive.RecoveryBase()
			if hasBase != withBase || archive.Tip() < through || hasBase && archive.Tip() <= seal.Index {
				t.Fatalf("archive base=%t tip=%d seal=%d required=%d", hasBase, archive.Tip(), seal.Index, through)
			}
			if withBase {
				manager := checkpoint.NewManager(bucket, path.Join(prefix, clusterID), t.TempDir(), 1)
				if err := manager.Load(ctx); err != nil {
					t.Fatal(err)
				}
				current := manager.Latest()
				if current == nil || current.Index != uint64(seal.Index) || current.RootHash != seal.RootHash || current.Hash != seal.StateHash {
					t.Fatal("fresh CURRENT does not match the retained certified archive base")
				}
				if err := manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash); err != nil {
					t.Fatalf("retained certified root: %v", err)
				}
			}
			for _, node := range nodes {
				if node.peer != nil || node.archive != nil || node.wal != nil || node.material != nil || node.cancel != nil || node.Ready() {
					t.Fatal("shutdown left owned resources or readiness active")
				}
			}
		})
	}
}

type shutdownArchiveFailureBucket struct {
	objstore.Bucket
	headOnly bool
	failure  error
}

func (b shutdownArchiveFailureBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/archive/") && (!b.headOnly || strings.HasSuffix(name, "/archive/head.bin")) {
		return b.failure
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func TestNodeShutdownRetainsArchivePublicationFailure(t *testing.T) {
	for _, headOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("HEAD=%t", headOnly), func(t *testing.T) {
			ctx := context.Background()
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "shutdown-failure", SQL: "CREATE TABLE durable (id INTEGER)"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := core.Propose(ctx, value); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected archive publication failure")
			archive := recovery.NewManager(shutdownArchiveFailureBucket{Bucket: objstore.NewInMemBucket(), headOnly: headOnly, failure: failure}, "shutdown", 1)
			node := &Node{config: &types.ExecutionConfig{}, core: core, wal: wal, archive: archive}
			closeErr := node.Shutdown()
			if headOnly {
				// The existing CAS manager maps failed HEAD writes to its own
				// retry-exhaustion error. Close must retain that actual error,
				// not pretend it still exposes the underlying upload sentinel.
				if closeErr == nil || closeErr.Error() != "shared archive publication conflicted too many times" {
					t.Fatalf("shutdown HEAD failure=%v", closeErr)
				}
			} else if !errors.Is(closeErr, failure) {
				t.Fatalf("shutdown error=%v, want original extent publication failure", closeErr)
			}
			if node.archive != nil || node.wal != nil || node.Ready() {
				t.Fatal("failure bypassed resource teardown")
			}
			if err := archive.SyncThrough(ctx, core, core.Tip()); !errors.Is(err, recovery.ErrArchiveClosed) {
				t.Fatalf("archive worker not closed/joined: %v", err)
			}
		})
	}
}

func TestNodeOpenCatchUpRepairsHeldLearnGapForConcurrentKVCalls(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := gateway.Close(closeCtx); err != nil {
			t.Errorf("stop local Versity Gateway: %v", err)
		}
	})

	endpoint := strings.TrimPrefix(gateway.Endpoint, "http://")
	const region = "us-east-1"
	bucket := fmt.Sprintf("rhiza-gap-%d", time.Now().UnixNano())
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(gateway.AccessKey, gateway.SecretKey, ""),
		Secure: false, Region: region,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		t.Fatal(err)
	}

	const clusterID = "learn-gap-lifecycle"
	ids := []quepaxa.NodeID{"n1", "n2", "n3"}
	tokens := map[quepaxa.NodeID]string{"n1": "gap-peer-n1", "n2": "gap-peer-n2", "n3": "gap-peer-n3"}
	addresses := make(map[quepaxa.NodeID]string, len(ids))
	for _, id := range ids {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[id] = conn.LocalAddr().String()
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	members := make([]quepaxa.Member, 0, len(ids))
	for _, id := range ids {
		member := peerMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + addresses[id]
		members = append(members, member)
	}

	nodes := make([]*Node, 0, len(ids))
	var releaseOnce sync.Once
	var releaseLower chan struct{}
	var restoreHook func()
	releaseHeldLower := func() {
		if releaseLower != nil {
			releaseOnce.Do(func() { close(releaseLower) })
		}
	}
	t.Cleanup(func() {
		releaseHeldLower()
		if restoreHook != nil {
			restoreHook()
		}
		// Observe the certified state while all peers remain available, then
		// exercise the same existing durability/checkpoint preparation path that
		// Node.Shutdown uses before shutting nodes down sequentially.
		for i, node := range nodes {
			logNodeCheckpointObservation(t, ids[i], node, ctx)
		}
		prepareCtx, stopPrepare := context.WithTimeout(ctx, 10*time.Second)
		for _, node := range nodes {
			if node.checkpointer != nil {
				node.checkpointer.Stop()
			}
		}
		for i, node := range nodes {
			if err := prepareNodeCheckpointBeforeShutdown(prepareCtx, node, t.TempDir()); err != nil {
				t.Errorf("prepare node %s durability before sequential shutdown: %v", ids[i], err)
			}
		}
		stopPrepare()
		for i := len(nodes) - 1; i >= 0; i-- {
			logNodeCheckpointObservation(t, ids[i], nodes[i], ctx)
			if err := nodes[i].Shutdown(); err != nil {
				t.Errorf("shutdown node %s: %v", ids[i], err)
			}
		}
	})

	prefix := fmt.Sprintf("rhiza-learn-gap/%d", time.Now().UnixNano())
	for _, id := range ids {
		config := &types.ExecutionConfig{
			DataDir: t.TempDir(), ClusterID: clusterID, NodeID: types.NodeID(id),
			PeerAddr: addresses[id], PeerToken: tokens[id], Members: members,
			ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucket,
			ObjStorePrefix: prefix, ObjStoreRegion: region, ObjStoreInsecure: true,
			ObjStoreAccessKey: gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
			ObjStoreSyncInterval: time.Hour, ObjStoreDurability: types.ObjectStoreDurabilityAsync,
		}
		node := New(config)
		nodes = append(nodes, node)
		if err := node.Open(ctx); err != nil {
			t.Fatalf("open node %s: %v", id, err)
		}
	}
	if err := waitForNodeReadiness(ctx, nodes); err != nil {
		t.Fatal(err)
	}

	apis := make([]*network.Server, len(nodes))
	for i, node := range nodes {
		api, err := node.API()
		if err != nil {
			t.Fatalf("get API for %s: %v", ids[i], err)
		}
		apis[i] = api
	}

	const lowerTimeout = 300 * time.Millisecond
	lowerSlot := nodes[0].core.Tip() + 1
	releaseLower = make(chan struct{})
	lowerReceived := make(chan string, 2)
	var eventMu sync.Mutex
	applyComplete := make(map[string]bool)
	responseBeforeApply := make([]string, 0, 1)
	var lowerHoldMu sync.Mutex
	lowerHeld := make(map[string]bool, 2)
	restoreHook = localtesthooks.Set(func(event string) {
		key := foregroundLearnEventKey(event)
		if strings.HasPrefix(event, "network:learned:phase=received:") {
			if (key == fmt.Sprintf("n2:%d", lowerSlot) || key == fmt.Sprintf("n3:%d", lowerSlot)) && strings.Contains(event, ":sender=n1:") {
				lowerHoldMu.Lock()
				first := !lowerHeld[key]
				lowerHeld[key] = true
				lowerHoldMu.Unlock()
				if first {
					lowerReceived <- event
				}
				<-releaseLower
			}
		}
		if strings.HasPrefix(event, "network:learned:phase=apply-complete:") || strings.HasPrefix(event, "network:learned:phase=response-written:") {
			key := foregroundLearnEventKey(event)
			eventMu.Lock()
			defer eventMu.Unlock()
			if strings.HasPrefix(event, "network:learned:phase=apply-complete:") {
				applyComplete[key] = true
			} else if !applyComplete[key] {
				responseBeforeApply = append(responseBeforeApply, event)
			}
		}
	})

	lowerCtx, cancelLower := context.WithTimeout(ctx, lowerTimeout)
	lowerResult := make(chan error, 1)
	go func() {
		_, err := apis[0].KVPut(lowerCtx, network.KVMutationRequest{
			RequestID: "learn-gap-lower", Key: "learn-gap-lower", Value: []byte("lower-prefix"),
		})
		lowerResult <- err
	}()
	for range 2 {
		select {
		case <-lowerReceived:
		case <-ctx.Done():
			cancelLower()
			t.Fatalf("did not hold both lower learned handlers: %v", ctx.Err())
		}
	}
	var lowerErr error
	select {
	case lowerErr = <-lowerResult:
	case <-ctx.Done():
		cancelLower()
		t.Fatalf("lower API call did not return: %v", ctx.Err())
	}
	if !errors.Is(lowerErr, network.ErrCommitUnknown) || !errors.Is(lowerErr, context.DeadlineExceeded) {
		t.Fatalf("lower API error=%v, want retained CommitUnknown caused by the bounded deadline", lowerErr)
	}
	if !nodes[0].core.IsDecided(lowerSlot) {
		t.Fatalf("leader did not certify lower slot %d after failed call: %v", lowerSlot, lowerErr)
	}
	cancelLower()
	t.Logf("retained lower-call failure at slot %d: %v", lowerSlot, lowerErr)
	if err := waitForNodeTip(ctx, nodes, lowerSlot); err != nil {
		t.Fatalf("automatic Node catch-up did not install lower slot while learned handlers remained held: %v", err)
	}
	for i, api := range apis {
		got, err := api.KVGet(ctx, network.KVGetRequest{Key: "learn-gap-lower", Consistency: "local"})
		if err != nil || !got.Found || string(got.Value) != "lower-prefix" {
			t.Fatalf("lower-prefix readback on %s: found=%t value=%q error=%v", ids[i], got.Found, got.Value, err)
		}
	}
	// Releasing the held requests only after the catch-up and lower readback
	// checks proves the background worker repaired the gap first. The canceled
	// lower RPCs need not emit an apply or response event.
	releaseHeldLower()

	const concurrentCalls = 16
	start := make(chan struct{})
	type callResult struct {
		index int
		err   error
	}
	results := make(chan callResult, concurrentCalls)
	for i := 0; i < concurrentCalls; i++ {
		go func(index int) {
			<-start
			callCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			defer stop()
			key := fmt.Sprintf("learn-gap-high-%02d", index)
			_, err := apis[0].KVPut(callCtx, network.KVMutationRequest{
				RequestID: key, Key: key, Value: []byte(fmt.Sprintf("payload-%02d", index)),
			})
			results <- callResult{index: index, err: err}
		}(i)
	}
	close(start)
	for range concurrentCalls {
		select {
		case result := <-results:
			if result.err != nil {
				t.Errorf("concurrent API call %d: %v", result.index, result.err)
			}
		case <-ctx.Done():
			t.Fatalf("only collected concurrent API results before timeout: %v", ctx.Err())
		}
	}

	if err := waitForNodeTip(ctx, nodes, nodes[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	for call := 0; call < concurrentCalls; call++ {
		key := fmt.Sprintf("learn-gap-high-%02d", call)
		want := fmt.Sprintf("payload-%02d", call)
		for i, api := range apis {
			got, err := api.KVGet(ctx, network.KVGetRequest{Key: key, Consistency: "local"})
			if err != nil || !got.Found || string(got.Value) != want {
				t.Fatalf("read back %s on %s: found=%t value=%q want=%q error=%v", key, ids[i], got.Found, got.Value, want, err)
			}
		}
	}
	eventMu.Lock()
	violations := append([]string(nil), responseBeforeApply...)
	eventMu.Unlock()
	if len(violations) != 0 {
		t.Fatalf("peer response was written before apply completed: %s", violations[0])
	}
	t.Logf("all %d concurrent API calls completed; all peers read back every value; held lower handlers released after catch-up", concurrentCalls)
}

// prepareNodeCheckpointBeforeShutdown invokes the existing archive and
// checkpointer paths while every peer is still live. It does not change the
// production shutdown sequence or relax its strict error handling.
func prepareNodeCheckpointBeforeShutdown(ctx context.Context, node *Node, localDir string) error {
	if node == nil || node.archive == nil || node.core == nil || node.material == nil || node.checkpointer == nil {
		return fmt.Errorf("node durability components unavailable")
	}
	if err := node.archive.SyncThrough(ctx, node.core, node.core.Tip()); err != nil {
		return fmt.Errorf("archive sync through core tip: %w", err)
	}
	stateTip := node.material.StateTip()
	if err := node.checkpointer.CheckpointOnShutdown(ctx, stateTip); err != nil {
		return fmt.Errorf("checkpoint current materializer state: %w", err)
	}
	latest := node.checkpoints.Latest()
	if latest == nil || latest.Index < stateTip {
		return fmt.Errorf("local certified checkpoint does not cover materializer state: checkpoint=%v state_tip=%d", latest, stateTip)
	}
	seal, ok, err := node.core.LatestCheckpointSeal()
	if err != nil {
		return fmt.Errorf("read local checkpoint seal: %w", err)
	}
	if !ok || uint64(seal.Index) != latest.Index || seal.RootHash != latest.RootHash {
		return fmt.Errorf("local checkpoint cache is not matched by a certified seal: checkpoint_index=%d sealed=%t seal_index=%d", latest.Index, ok, seal.Index)
	}
	sharedCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	shared, err := readFreshSharedCurrent(sharedCtx, node, localDir)
	if err != nil {
		return fmt.Errorf("read shared CURRENT after checkpoint preparation: %w", err)
	}
	if shared == nil || shared.Index != latest.Index || shared.RootHash != latest.RootHash || shared.Hash != seal.StateHash || stateTip > shared.Index {
		return fmt.Errorf("shared CURRENT is not the prepared certified checkpoint: present=%t shared_index=%d local_index=%d state_tip=%d root_matches=%t state_hash_matches=%t", shared != nil, checkpointIndex(shared), latest.Index, stateTip, shared != nil && shared.RootHash == seal.RootHash, shared != nil && shared.Hash == seal.StateHash)
	}
	return nil
}

// logNodeCheckpointObservation records a bounded, read-only view immediately
// before shutdown. The fresh manager observes shared CURRENT without refreshing
// or otherwise changing the node's cached checkpoint manager.
func logNodeCheckpointObservation(t *testing.T, id quepaxa.NodeID, node *Node, parent context.Context) {
	t.Helper()
	if node == nil || node.core == nil || node.material == nil {
		t.Logf("NODE_CHECKPOINT_OBSERVATION at=%s node=%s status=unavailable reason=partially-open-node", time.Now().UTC().Format(time.RFC3339Nano), id)
		return
	}
	observedAt := time.Now().UTC()
	localIndex, localRoot := "none", "none"
	if node.checkpoints != nil {
		if latest := node.checkpoints.Latest(); latest != nil {
			localIndex = fmt.Sprint(latest.Index)
			localRoot = hex.EncodeToString(latest.RootHash[:])
		}
	}
	sealIndex, sealRoot, sealSlot := "none", "none", "none"
	if seal, ok, err := node.core.LatestCheckpointSeal(); err != nil {
		sealIndex = "error:" + fmt.Sprintf("%T", err)
	} else if ok {
		sealIndex = fmt.Sprint(seal.Index)
		sealRoot = hex.EncodeToString(seal.RootHash[:])
		sealSlot = fmt.Sprint(seal.DecisionSlot)
	}
	sharedIndex, sharedRoot, sharedStateHash := "none", "none", "none"
	sharedStatus := "unavailable"
	sharedCtx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	shared, err := readFreshSharedCurrent(sharedCtx, node, t.TempDir())
	if err != nil {
		sharedStatus = "error:" + fmt.Sprintf("%T", err)
	} else if shared == nil {
		sharedStatus = "absent"
	} else {
		sharedStatus = "present"
		sharedIndex = fmt.Sprint(shared.Index)
		sharedRoot = hex.EncodeToString(shared.RootHash[:])
		sharedStateHash = hex.EncodeToString(shared.Hash[:])
	}
	archiveTip := "unavailable"
	if node.archive != nil {
		archiveTip = fmt.Sprint(node.archive.Tip())
	}
	t.Logf("NODE_CHECKPOINT_OBSERVATION at=%s node=%s core_tip=%d material_state_tip=%d local_current_index=%s local_current_root=%s core_seal_index=%s core_seal_root=%s core_seal_slot=%s archive_tip=%s shared_current_status=%s shared_current_index=%s shared_current_root=%s shared_current_state_hash=%s shared_observation=read-only-fresh-manager capture_consistency=non-atomic-independent-reads", observedAt.Format(time.RFC3339Nano), id, node.core.Tip(), node.material.StateTip(), localIndex, localRoot, sealIndex, sealRoot, sealSlot, archiveTip, sharedStatus, sharedIndex, sharedRoot, sharedStateHash)
}

func readFreshSharedCurrent(ctx context.Context, node *Node, localDir string) (*checkpoint.Checkpoint, error) {
	if node == nil || node.bucket == nil || node.config == nil || node.core == nil {
		return nil, fmt.Errorf("node shared checkpoint components unavailable")
	}
	shared := checkpoint.NewManager(node.bucket, path.Join(node.config.ObjStorePrefix, string(node.config.ClusterID)), localDir, node.core.ConfigID())
	if err := shared.Load(ctx); err != nil {
		return nil, err
	}
	return shared.Latest(), nil
}

func checkpointIndex(checkpoint *checkpoint.Checkpoint) uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.Index
}

func waitForNodeReadiness(ctx context.Context, nodes []*Node) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, node := range nodes {
			ready = ready && node.Ready()
		}
		if ready {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("nodes did not become ready: %w", ctx.Err())
		}
	}
}

func waitForNodeTip(ctx context.Context, nodes []*Node, through quepaxa.Slot) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		caughtUp := true
		for _, node := range nodes {
			caughtUp = caughtUp && node.core.Tip() >= through && quepaxa.Slot(node.material.Tip()) >= through
		}
		if caughtUp {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("nodes did not apply through slot %d: %w", through, ctx.Err())
		}
	}
}

func foregroundLearnEventKey(event string) string {
	var node, slot string
	for _, field := range strings.Split(event, ":") {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "node":
			node = value
		case "slot":
			slot = value
		}
	}
	return node + ":" + slot
}
