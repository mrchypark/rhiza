//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza/internal/foregroundcosttest"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	nodeForegroundWorkers      = 16
	nodeForegroundKeys         = 4096
	nodeForegroundValueBytes   = 1024
	nodeForegroundWarmup       = 30 * time.Second
	nodeForegroundMeasure      = 120 * time.Second
	nodeForegroundCallTimeout  = 30 * time.Second
	nodeForegroundDrainReserve = 2 * nodeForegroundCallTimeout
	nodeForegroundCleanup      = 65 * time.Second
)

func nodeForegroundBudgetAvailable(deadline, now time.Time, required time.Duration) bool {
	return required >= 0 && deadline.Sub(now) >= required
}

func requireNodeForegroundBudget(t testing.TB, deadline time.Time, required time.Duration, phase string) {
	t.Helper()
	now := time.Now()
	if !nodeForegroundBudgetAvailable(deadline, now, required) {
		t.Fatalf("insufficient five-minute Node foreground budget at %s: remaining=%s required=%s", phase, deadline.Sub(now), required)
	}
}

// TestNodeForegroundAPICostS3 exercises the real Node.Open catch-up lifecycle,
// API path, and local S3-compatible object store. It is opt-in and is not AWS
// or provider-wide qualification.
func TestNodeForegroundAPICostS3(t *testing.T) {
	if os.Getenv("RHIZA_NODE_FOREGROUND_API_COST") != "1" {
		t.Skip("set RHIZA_NODE_FOREGROUND_API_COST=1 to run the fixed 150-second Node API measurement")
	}
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	testDeadline, ok := t.Deadline()
	if !ok {
		t.Fatal("test runner must supply the fixed five-minute deadline")
	}
	ctx, cancel := context.WithDeadline(context.Background(), testDeadline)
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
	bucketName := fmt.Sprintf("rhiza-node-cost-%d", time.Now().UnixNano())
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(gateway.AccessKey, gateway.SecretKey, ""), Secure: false, Region: region,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region}); err != nil {
		t.Fatal(err)
	}
	setupStartRequests, err := nodeVersityRequestCount(gateway)
	if err != nil {
		t.Fatalf("read Versity setup baseline: %v", err)
	}
	transportObserver, unregisterTransportObserver, err := objmetrics.RegisterS3TransportObserver(endpoint, []string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("register tagged S3 transport observer: %v", err)
	}
	t.Cleanup(unregisterTransportObserver)

	const clusterID = "foreground-node-cost"
	ids := []quepaxa.NodeID{"n1", "n2", "n3"}
	tokens := map[quepaxa.NodeID]string{"n1": "node-cost-n1", "n2": "node-cost-n2", "n3": "node-cost-n3"}
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
	t.Cleanup(func() {
		transportObserver.SetPhase("cleanup_preparation")
		cleanupStart, cleanupStartErr := nodeVersityRequestCount(gateway)
		storeBeforeCleanup := snapshotNodeStoreStats(nodes)
		storeBuckets := make([]*objmetrics.MeteredBucket, len(nodes))
		for i, node := range nodes {
			storeBuckets[i] = node.bucket
		}
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
			transportObserver.SetPhase("pre_shutdown_" + string(ids[i]) + "_observation")
			logNodeCheckpointObservation(t, ids[i], nodes[i], ctx)
			transportObserver.SetPhase("shutdown_" + string(ids[i]))
			if err := nodes[i].Shutdown(); err != nil {
				t.Errorf("shutdown node %s: %v", ids[i], err)
			}
		}
		storeAfterCleanup := snapshotNodeStoreBuckets(storeBuckets)
		logNodeStoreDelta(t, "cleanup", ids, storeBeforeCleanup, storeAfterCleanup)
		cleanupEnd, cleanupEndErr := nodeVersityRequestCount(gateway)
		logVersityRequestDelta(t, "cleanup", cleanupStart, cleanupStartErr, cleanupEnd, cleanupEndErr)
		logNodeTransportObserver(t, transportObserver)
	})
	prefix := fmt.Sprintf("rhiza-node-foreground-cost/%d", time.Now().UnixNano())
	for _, id := range ids {
		config := &types.ExecutionConfig{
			DataDir: t.TempDir(), ClusterID: clusterID, NodeID: types.NodeID(id),
			PeerAddr: addresses[id], PeerToken: tokens[id], Members: members,
			ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucketName,
			ObjStorePrefix: prefix, ObjStoreRegion: region, ObjStoreInsecure: true,
			ObjStoreAccessKey: gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
			ObjStoreSyncInterval: time.Hour, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
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
	setupEndRequests, err := nodeVersityRequestCount(gateway)
	if err != nil {
		t.Fatalf("read Versity post-setup count: %v", err)
	}
	logVersityRequestDelta(t, "node_open_setup", setupStartRequests, nil, setupEndRequests, nil)
	setupStore := snapshotNodeStoreStats(nodes)
	logNodeStoreCumulative(t, "setup_through_node_open", ids, setupStore)

	const reserve = nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup
	seedDeadline := testDeadline.Add(-reserve)
	if !time.Now().Before(seedDeadline) {
		t.Fatalf("Node startup left no bounded seed budget before fixed windows: now=%s seed_deadline=%s reserve=%s", time.Now().UTC().Format(time.RFC3339Nano), seedDeadline.UTC().Format(time.RFC3339Nano), reserve)
	}
	value := make([]byte, nodeForegroundValueBytes)
	for i := range value {
		value[i] = byte((i*31 + 7) % 251)
	}
	var sequence atomic.Uint64
	clientID := time.Now().UnixNano()
	put := func(callCtx context.Context, requestID, key string, payload []byte) error {
		bounded, stop := context.WithTimeout(callCtx, nodeForegroundCallTimeout)
		defer stop()
		_, err := apis[0].KVPut(bounded, network.KVMutationRequest{RequestID: requestID, Key: key, Value: payload})
		return err
	}
	get := func(callCtx context.Context, key string) error {
		bounded, stop := context.WithTimeout(callCtx, nodeForegroundCallTimeout)
		defer stop()
		got, err := apis[0].KVGet(bounded, network.KVGetRequest{Key: key, Consistency: "linearizable"})
		if err != nil {
			return err
		}
		if !got.Found || string(got.Value) != string(value) {
			return fmt.Errorf("linearizable readback mismatch: found=%t value_bytes=%d", got.Found, len(got.Value))
		}
		return nil
	}
	seedCtx, seedCancel := context.WithDeadline(ctx, seedDeadline)
	transportObserver.SetPhase("seed")
	seedStoreStart := snapshotNodeStoreStats(nodes)
	seedServerStart, seedServerStartErr := nodeVersityRequestCount(gateway)
	seedErr := foregroundcosttest.Seed(seedCtx, foregroundcosttest.SeedOptions{
		Workers: nodeForegroundWorkers, Keys: nodeForegroundKeys, ProgressEvery: 512,
		Value: value, ClientID: clientID,
	}, put, func(done int) { t.Logf("node_foreground_seed_completed=%d/%d", done, nodeForegroundKeys) })
	seedCancel()
	if seedErr != nil {
		t.Fatalf("seed the fixed %d-key Node API workload: %v", nodeForegroundKeys, seedErr)
	}
	seedStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "seed", ids, seedStoreStart, seedStoreEnd)
	seedEndRequests, seedServerEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "seed", seedServerStart, seedServerStartErr, seedEndRequests, seedServerEndErr)
	t.Logf("node_foreground_seed_logical_puts=%d node_foreground_seed_payload_bytes_api_acknowledged=%d", nodeForegroundKeys, nodeForegroundKeys*nodeForegroundValueBytes)

	options := func(duration time.Duration) foregroundcosttest.Options {
		return foregroundcosttest.Options{
			Workers: nodeForegroundWorkers, Keys: nodeForegroundKeys, Value: value,
			ClientID: clientID, Sequence: &sequence, Duration: duration,
			CallTimeout: nodeForegroundCallTimeout, Validate: true,
		}
	}
	warmMetrics := &nodeForegroundCostMetrics{}
	warmStoreStart := snapshotNodeStoreStats(nodes)
	warmStartRequests, warmStartErr := nodeVersityRequestCount(gateway)
	requireNodeForegroundBudget(t, testDeadline, nodeForegroundWarmup+nodeForegroundMeasure+nodeForegroundDrainReserve+nodeForegroundCleanup, "after seed accounting, before warmup")
	transportObserver.SetPhase("warmup")
	warmResult, err := foregroundcosttest.RunWindow(ctx, options(nodeForegroundWarmup), put, get, warmMetrics.observe, nil)
	warmMetrics.logAndFail(t, "warmup", warmResult, err)
	warmStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "warmup", ids, warmStoreStart, warmStoreEnd)
	warmEndRequests, warmEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "warmup", warmStartRequests, warmStartErr, warmEndRequests, warmEndErr)
	metrics := &nodeForegroundCostMetrics{}
	measureStoreStart := snapshotNodeStoreStats(nodes)
	measureStartRequests, measureStartErr := nodeVersityRequestCount(gateway)
	requireNodeForegroundBudget(t, testDeadline, nodeForegroundMeasure+nodeForegroundCallTimeout+nodeForegroundCleanup, "after warmup accounting, before measurement")
	transportObserver.SetPhase("measurement")
	measuredResult, err := foregroundcosttest.RunWindow(ctx, options(nodeForegroundMeasure), put, get, metrics.observe, nil)
	metrics.logAndFail(t, "measurement", measuredResult, err)
	measureStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "measurement", ids, measureStoreStart, measureStoreEnd)
	measureEndRequests, measureEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "measurement", measureStartRequests, measureStartErr, measureEndRequests, measureEndErr)
	t.Logf("versity_fixture_version=%q versity_binary_sha256=%s endpoint_scope=local-loopback; phase server request counts come from the gateway access log", gateway.Version, gateway.BinarySHA256)
}

type nodeForegroundCostMetrics struct {
	mu                   sync.Mutex
	puts, gets           int
	putErrors, getErrors int
	unknownCommits       int
	inWindow, drained    int
	putBytesAttempted    uint64
	putBytesAcknowledged uint64
	latencies            []time.Duration
	firstError           string
}

type nodeStoreSnapshot struct {
	stats objmetrics.Stats
	ok    bool
}

type nodeStoreDelta struct {
	Uploads, Gets, Lists, Heads, Deletes, Failures                          uint64
	BytesUploadedAttempts, BytesPublished, BytesDownloaded                  uint64
	HTTPRequests, HTTPRequestBodyBytes, HTTPResponseBodyBytes, HTTPFailures uint64
	HTTPGet, HTTPPut, HTTPHead, HTTPDelete, HTTPOther, S3HTTPFailures       uint64
	SDKRetries, RetryMetadataRequests, RetryMetadataUnknownRequests         uint64
	TransportFailures, Unexpected4xx, HTTP5xx, ConditionConflicts           uint64
}

func snapshotNodeStoreStats(nodes []*Node) []nodeStoreSnapshot {
	snapshots := make([]nodeStoreSnapshot, len(nodes))
	for i, node := range nodes {
		if node != nil {
			snapshots[i].stats, snapshots[i].ok = node.ObjectStoreStats()
		}
	}
	return snapshots
}

func snapshotNodeStoreBuckets(buckets []*objmetrics.MeteredBucket) []nodeStoreSnapshot {
	snapshots := make([]nodeStoreSnapshot, len(buckets))
	for i, bucket := range buckets {
		if bucket != nil {
			snapshots[i] = nodeStoreSnapshot{stats: bucket.Stats(), ok: true}
		}
	}
	return snapshots
}

func nodeStoreStatsDelta(before, after objmetrics.Stats) (nodeStoreDelta, bool) {
	if after.Uploads < before.Uploads || after.Gets < before.Gets || after.Lists < before.Lists || after.Heads < before.Heads || after.Deletes < before.Deletes || after.Failures < before.Failures ||
		after.BytesUploaded < before.BytesUploaded || after.BytesPublished < before.BytesPublished || after.BytesDownloaded < before.BytesDownloaded ||
		after.HTTPRequests < before.HTTPRequests || after.HTTPRequestBodyBytes < before.HTTPRequestBodyBytes || after.HTTPResponseBodyBytes < before.HTTPResponseBodyBytes || after.HTTPFailures < before.HTTPFailures ||
		after.HTTPGetRequests < before.HTTPGetRequests || after.HTTPPutRequests < before.HTTPPutRequests || after.HTTPHeadRequests < before.HTTPHeadRequests || after.HTTPDeleteRequests < before.HTTPDeleteRequests || after.HTTPOtherRequests < before.HTTPOtherRequests || after.S3HTTPFailures < before.S3HTTPFailures ||
		after.SDKRetries < before.SDKRetries || after.RetryMetadataRequests < before.RetryMetadataRequests || after.RetryMetadataUnknownRequests < before.RetryMetadataUnknownRequests || after.TransportFailures < before.TransportFailures || after.Unexpected4xx < before.Unexpected4xx || after.HTTP5xx < before.HTTP5xx || after.ConditionConflicts < before.ConditionConflicts {
		return nodeStoreDelta{}, false
	}
	return nodeStoreDelta{
		Uploads: after.Uploads - before.Uploads, Gets: after.Gets - before.Gets,
		Lists: after.Lists - before.Lists, Heads: after.Heads - before.Heads,
		Deletes: after.Deletes - before.Deletes, Failures: after.Failures - before.Failures,
		BytesUploadedAttempts:        after.BytesUploaded - before.BytesUploaded,
		BytesPublished:               after.BytesPublished - before.BytesPublished,
		BytesDownloaded:              after.BytesDownloaded - before.BytesDownloaded,
		HTTPRequests:                 after.HTTPRequests - before.HTTPRequests,
		HTTPRequestBodyBytes:         after.HTTPRequestBodyBytes - before.HTTPRequestBodyBytes,
		HTTPResponseBodyBytes:        after.HTTPResponseBodyBytes - before.HTTPResponseBodyBytes,
		HTTPFailures:                 after.HTTPFailures - before.HTTPFailures,
		HTTPGet:                      after.HTTPGetRequests - before.HTTPGetRequests,
		HTTPPut:                      after.HTTPPutRequests - before.HTTPPutRequests,
		HTTPHead:                     after.HTTPHeadRequests - before.HTTPHeadRequests,
		HTTPDelete:                   after.HTTPDeleteRequests - before.HTTPDeleteRequests,
		HTTPOther:                    after.HTTPOtherRequests - before.HTTPOtherRequests,
		S3HTTPFailures:               after.S3HTTPFailures - before.S3HTTPFailures,
		SDKRetries:                   after.SDKRetries - before.SDKRetries,
		RetryMetadataRequests:        after.RetryMetadataRequests - before.RetryMetadataRequests,
		RetryMetadataUnknownRequests: after.RetryMetadataUnknownRequests - before.RetryMetadataUnknownRequests,
		TransportFailures:            after.TransportFailures - before.TransportFailures,
		Unexpected4xx:                after.Unexpected4xx - before.Unexpected4xx,
		HTTP5xx:                      after.HTTP5xx - before.HTTP5xx,
		ConditionConflicts:           after.ConditionConflicts - before.ConditionConflicts,
	}, true
}

func logNodeStoreCumulative(t *testing.T, phase string, ids []quepaxa.NodeID, snapshots []nodeStoreSnapshot) {
	t.Helper()
	for i, id := range ids {
		if i >= len(snapshots) || !snapshots[i].ok {
			t.Logf("node_objectstore_phase=%s node=%s status=unavailable cumulative=true", phase, id)
			continue
		}
		t.Logf("node_objectstore_phase=%s node=%s status=measured cumulative=true stats=%+v", phase, id, snapshots[i].stats)
	}
}

func logNodeStoreDelta(t *testing.T, phase string, ids []quepaxa.NodeID, before, after []nodeStoreSnapshot) {
	t.Helper()
	for i, id := range ids {
		if i >= len(before) || i >= len(after) || !before[i].ok || !after[i].ok {
			t.Logf("node_objectstore_phase=%s node=%s status=unavailable delta=not-measured", phase, id)
			continue
		}
		delta, ok := nodeStoreStatsDelta(before[i].stats, after[i].stats)
		if !ok {
			t.Errorf("node object-store counters decreased during %s for %s; phase delta unavailable", phase, id)
			continue
		}
		t.Logf("node_objectstore_phase=%s node=%s status=measured cumulative=false delta=%+v", phase, id, delta)
	}
}

func logVersityRequestDelta(t *testing.T, phase string, before uint64, beforeErr error, after uint64, afterErr error) {
	t.Helper()
	if beforeErr != nil || afterErr != nil {
		t.Errorf("Versity server request delta unavailable for %s: before_error=%v after_error=%v", phase, beforeErr, afterErr)
		t.Logf("versity_server_phase=%s status=unavailable request_delta=not-measured", phase)
		return
	}
	if after < before {
		t.Errorf("Versity server request counter decreased during %s: before=%d after=%d", phase, before, after)
		t.Logf("versity_server_phase=%s status=unavailable request_delta=not-measured", phase)
		return
	}
	t.Logf("versity_server_phase=%s status=measured request_count_delta=%d", phase, after-before)
}

func TestNodeStoreStatsDeltaSeparatesAttemptsAcknowledgmentsAndUnknownRetries(t *testing.T) {
	before := objmetrics.Stats{
		Uploads: 2, Gets: 3, BytesUploaded: 700, BytesPublished: 600,
		SDKRetries: 4, RetryMetadataRequests: 5, RetryMetadataUnknownRequests: 6,
	}
	after := objmetrics.Stats{
		Uploads: 5, Gets: 8, BytesUploaded: 1700, BytesPublished: 1500,
		SDKRetries: 7, RetryMetadataRequests: 9, RetryMetadataUnknownRequests: 11,
	}
	delta, ok := nodeStoreStatsDelta(before, after)
	if !ok || delta.Uploads != 3 || delta.Gets != 5 || delta.BytesUploadedAttempts != 1000 || delta.BytesPublished != 900 || delta.SDKRetries != 3 || delta.RetryMetadataRequests != 4 || delta.RetryMetadataUnknownRequests != 5 {
		t.Fatalf("node store delta=%+v valid=%t", delta, ok)
	}
	after.BytesUploaded = before.BytesUploaded - 1
	if _, ok := nodeStoreStatsDelta(before, after); ok {
		t.Fatal("counter decrease was reported as a zero-valued phase delta")
	}
}

func TestNodeForegroundBudgetBoundariesPreserveWindowsAndCleanupReserve(t *testing.T) {
	deadline := time.Date(2026, time.October, 3, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		required  time.Duration
		remaining time.Duration
		want      bool
	}{
		{name: "post-seed exact reserve", required: nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, remaining: nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, want: true},
		{name: "post-seed short by one nanosecond", required: nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, remaining: nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup - time.Nanosecond, want: false},
		{name: "post-warmup one remaining drain", required: nodeForegroundMeasure + nodeForegroundCallTimeout + nodeForegroundCleanup, remaining: nodeForegroundMeasure + nodeForegroundCallTimeout + nodeForegroundCleanup, want: true},
		{name: "post-warmup retains cleanup and drain", required: nodeForegroundMeasure + nodeForegroundCallTimeout + nodeForegroundCleanup, remaining: nodeForegroundMeasure + nodeForegroundCallTimeout + nodeForegroundCleanup - time.Nanosecond, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := deadline.Add(-test.remaining)
			if got := nodeForegroundBudgetAvailable(deadline, now, test.required); got != test.want {
				t.Fatalf("budget available=%t, want %t (remaining=%s required=%s)", got, test.want, test.remaining, test.required)
			}
		})
	}
}

func (m *nodeForegroundCostMetrics) observe(observation foregroundcosttest.Observation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if observation.Operation == foregroundcosttest.Put {
		m.puts++
		m.putBytesAttempted += nodeForegroundValueBytes
		if observation.Err == nil {
			m.putBytesAcknowledged += nodeForegroundValueBytes
		} else {
			m.putErrors++
			if errors.Is(observation.Err, network.ErrCommitUnknown) {
				m.unknownCommits++
			}
		}
	} else {
		m.gets++
		if observation.Err != nil {
			m.getErrors++
		}
	}
	if !observation.InWindow {
		m.drained++
	} else {
		m.inWindow++
	}
	if !observation.Started.IsZero() && !observation.Completed.IsZero() {
		m.latencies = append(m.latencies, observation.Completed.Sub(observation.Started))
	}
	if observation.Err != nil && m.firstError == "" {
		m.firstError = fmt.Sprintf("operation=%s request_id=%s key=%s error=%v", observation.Operation, observation.RequestID, observation.Key, observation.Err)
	}
}

func (m *nodeForegroundCostMetrics) logAndFail(t *testing.T, phase string, result foregroundcosttest.WindowResult, runErr error) {
	t.Helper()
	m.mu.Lock()
	puts, gets := m.puts, m.gets
	putErrors, getErrors, unknownCommits := m.putErrors, m.getErrors, m.unknownCommits
	inWindow, drained := m.inWindow, m.drained
	attempted, acknowledged := m.putBytesAttempted, m.putBytesAcknowledged
	firstError := m.firstError
	latencies := append([]time.Duration(nil), m.latencies...)
	m.mu.Unlock()
	p99 := time.Duration(0)
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p99 = latencies[(len(latencies)-1)*99/100]
	}
	t.Logf("node_foreground_phase=%s started=%d completed=%d pending_at_cutoff=%d outstanding=%d puts=%d gets=%d api_errors=%d put_errors=%d get_errors=%d commit_unknown=%d completed_in_window=%d completed_during_drain=%d put_payload_bytes_attempted=%d put_payload_bytes_api_acknowledged=%d api_completion_p99=%s first_error=%q", phase, result.Started, result.Completed, result.PendingAtCutoff, result.Outstanding, puts, gets, putErrors+getErrors, putErrors, getErrors, unknownCommits, inWindow, drained, attempted, acknowledged, p99, firstError)
	if runErr != nil {
		t.Fatalf("Node foreground %s runner: %v", phase, runErr)
	}
	if putErrors+getErrors != 0 {
		t.Fatalf("Node foreground %s had %d API errors (including %d unknown commit outcomes)", phase, putErrors+getErrors, unknownCommits)
	}
}

func nodeVersityRequestCount(server *versityfixture.Server) (uint64, error) {
	records, err := server.AccessRecords()
	if err != nil {
		return 0, err
	}
	return versityfixture.RequestCount(records), nil
}

func logNodeTransportObserver(t *testing.T, observer *objmetrics.S3TransportObserver) {
	t.Helper()
	total, requests, aggregates, first := observer.Snapshot()
	t.Logf("node_s3_transport_observer attempts=%d request_groups=%d error_classes=%d", total, len(requests), len(aggregates))
	for _, request := range requests {
		t.Logf("node_s3_transport_request_count owner=%s phase=%s method=%s count=%d", request.Owner, request.Phase, request.Method, request.Count)
	}
	for _, aggregate := range aggregates {
		t.Logf("node_s3_transport_error_count owner=%s phase=%s method=%s status_family=%s outcome=%s error_class=%s count=%d", aggregate.Owner, aggregate.Phase, aggregate.Method, aggregate.StatusFamily, aggregate.Outcome, aggregate.ErrorClass, aggregate.Count)
	}
	if first != nil {
		t.Logf("node_s3_transport_first_failure sequence=%d owner=%s phase=%s method=%s status=%d outcome=%s error_class=%s elapsed=%s", first.Sequence, first.Owner, first.Phase, first.Method, first.StatusCode, first.Outcome, first.ErrorClass, first.Elapsed)
	}
}
