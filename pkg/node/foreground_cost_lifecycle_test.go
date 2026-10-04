//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza/internal/foregroundcosttest"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	nodeForegroundWorkers       = 16
	nodeForegroundKeys          = 4096
	nodeForegroundValueBytes    = 1024
	nodeForegroundWarmup        = 30 * time.Second
	nodeForegroundMeasure       = 120 * time.Second
	nodeForegroundCallTimeout   = 30 * time.Second
	nodeForegroundDrainReserve  = 2 * nodeForegroundCallTimeout
	nodeForegroundPrepareLimit  = 10 * time.Second
	nodeForegroundShutdownLimit = 10 * time.Second // mirrors Node.Shutdown's archive/checkpoint context
	nodeForegroundGatewayClose  = 7 * time.Second
	nodeForegroundCleanupSlack  = 5 * time.Second
	nodeForegroundGCGracePeriod = 24 * time.Hour
	nodeForegroundP99MinSamples = 10000
	// Planning reserve for three per-node preparations, their bounded archive/
	// checkpoint shutdown operations, gateway close, and small accounting slack.
	// It is not a hard upper bound for Checkpointer/Node worker joins; the outer
	// test deadline remains the final cap.
	nodeForegroundCleanup = 3*(nodeForegroundPrepareLimit+nodeForegroundShutdownLimit) + nodeForegroundGatewayClose + nodeForegroundCleanupSlack
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

func withNodeForegroundPrepareContext(parent context.Context, prepare func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, nodeForegroundPrepareLimit)
	defer cancel()
	return prepare(ctx)
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
	scenario := os.Getenv("RHIZA_NODE_FOREGROUND_API_COST_SCENARIO")
	if scenario == "" {
		scenario = "baseline"
	}
	if scenario != "baseline" && scenario != "checkpoint-active" && scenario != "archive-cleanup-active" {
		t.Fatalf("unsupported Node foreground cost scenario %q", scenario)
	}
	t.Logf("node_foreground_scenario=%s", scenario)
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
	ctx = objmetrics.WithReplayObservation(ctx)
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
		for _, node := range nodes {
			if node.checkpointer != nil {
				node.checkpointer.Stop()
			}
		}
		for i, node := range nodes {
			if err := withNodeForegroundPrepareContext(ctx, func(prepareCtx context.Context) error {
				return prepareNodeCheckpointBeforeShutdown(prepareCtx, node, t.TempDir())
			}); err != nil {
				t.Errorf("prepare node %s durability before sequential shutdown: %v", ids[i], err)
			}
		}
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
			ObjStoreGCGracePeriod: nodeForegroundGCGracePeriod,
			ObjStoreAccessKey:     gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
			ObjStoreSyncInterval: time.Hour, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
		}
		node := New(config)
		nodes = append(nodes, node)
		if err := node.Open(ctx); err != nil {
			t.Fatalf("open node %s: %v", id, err)
		}
	}
	for i, node := range nodes {
		stats, ok := node.ObjectStoreStats()
		if !ok || !stats.ReplayGroupingEnabled {
			t.Fatalf("Node %s object-store replay grouping is not enabled: stats=%+v available=%t", ids[i], stats, ok)
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
	seedStart := time.Now()
	t.Logf("node_foreground_seed_budget test_deadline=%s seed_deadline=%s reserve=%s", testDeadline.UTC().Format(time.RFC3339Nano), seedDeadline.UTC().Format(time.RFC3339Nano), reserve)
	var seedAPIAcknowledged atomic.Uint64
	seedPut := func(callCtx context.Context, requestID, key string, payload []byte) error {
		err := put(callCtx, requestID, key, payload)
		if err == nil {
			seedAPIAcknowledged.Add(1)
		}
		return err
	}
	seedErr := foregroundcosttest.Seed(seedCtx, foregroundcosttest.SeedOptions{
		Workers: nodeForegroundWorkers, Keys: nodeForegroundKeys, ProgressEvery: 512,
		Value: value, ClientID: clientID,
	}, seedPut, func(done int) { t.Logf("node_foreground_seed_completed=%d/%d", done, nodeForegroundKeys) })
	seedEnd := time.Now()
	t.Logf("node_foreground_seed_result duration=%s api_acknowledged=%d/%d error=%t", seedEnd.Sub(seedStart), seedAPIAcknowledged.Load(), nodeForegroundKeys, seedErr != nil)
	seedCancel()
	if seedErr != nil {
		t.Fatalf("seed the fixed %d-key Node API workload: %v", nodeForegroundKeys, seedErr)
	}
	seedStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "seed", ids, seedStoreStart, seedStoreEnd)
	seedEndRequests, seedServerEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "seed", seedServerStart, seedServerStartErr, seedEndRequests, seedServerEndErr)
	t.Logf("node_foreground_seed_logical_puts=%d node_foreground_seed_payload_bytes_api_acknowledged=%d", nodeForegroundKeys, nodeForegroundKeys*nodeForegroundValueBytes)

	options := func(duration time.Duration, maintenance *nodeForegroundMaintenance) foregroundcosttest.Options {
		var maintenanceSnapshot func() (bool, uint64)
		if maintenance != nil {
			maintenanceSnapshot = maintenance.snapshot
		}
		return foregroundcosttest.Options{
			Workers: nodeForegroundWorkers, Keys: nodeForegroundKeys, Value: value,
			ClientID: clientID, Sequence: &sequence, Duration: duration,
			CallTimeout: nodeForegroundCallTimeout, Validate: true, Maintenance: maintenanceSnapshot,
		}
	}
	warmMetrics := &nodeForegroundCostMetrics{}
	warmStoreStart := snapshotNodeStoreStats(nodes)
	warmStartRequests, warmStartErr := nodeVersityRequestCount(gateway)
	requireNodeForegroundBudget(t, testDeadline, nodeForegroundWarmup+nodeForegroundMeasure+nodeForegroundDrainReserve+nodeForegroundCleanup, "after seed accounting, before warmup")
	transportObserver.SetPhase("warmup")
	warmResult, err := foregroundcosttest.RunWindow(ctx, options(nodeForegroundWarmup, nil), put, get, warmMetrics.observe, nil)
	warmMetrics.logAndFail(t, "warmup", warmResult, err)
	warmStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "warmup", ids, warmStoreStart, warmStoreEnd)
	warmEndRequests, warmEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "warmup", warmStartRequests, warmStartErr, warmEndRequests, warmEndErr)
	metrics := &nodeForegroundCostMetrics{}
	measureStoreStart := snapshotNodeStoreStats(nodes)
	measureStartRequests, measureStartErr := nodeVersityRequestCount(gateway)
	maintenanceStateDir := t.TempDir()
	requireNodeForegroundBudget(t, testDeadline, nodeForegroundMeasure+nodeForegroundCallTimeout+nodeForegroundCleanup, "after warmup accounting, before measurement")
	var maintenance *nodeForegroundMaintenance
	var archivePhaseTrace *nodeArchiveGCPhaseTrace
	if scenario != "baseline" {
		if scenario == "archive-cleanup-active" {
			archivePhaseTrace = newNodeArchiveGCPhaseTrace()
		}
		operation := func(operationCtx context.Context) (nodeForegroundMaintenanceResult, error) {
			switch scenario {
			case "checkpoint-active":
				return runNodeForegroundCheckpoint(operationCtx, nodes[0], maintenanceStateDir)
			case "archive-cleanup-active":
				return runNodeForegroundArchiveCleanup(t, operationCtx, nodes[0], maintenanceStateDir, archivePhaseTrace)
			default:
				return nodeForegroundMaintenanceResult{}, fmt.Errorf("unsupported maintenance scenario %q", scenario)
			}
		}
		maintenance = newNodeForegroundMaintenance(ctx, scenario, transportObserver, operation)
		t.Cleanup(maintenance.stopAndWait)
	}
	transportObserver.SetPhase("measurement")
	observeMeasured := func(observation foregroundcosttest.Observation) {
		metrics.observe(observation)
		if maintenance != nil {
			maintenance.start(observation.Cutoff)
		}
	}
	measuredResult, err := foregroundcosttest.RunWindow(ctx, options(nodeForegroundMeasure, maintenance), put, get, observeMeasured, nil)
	if maintenance != nil {
		maintenance.stopAndWait()
		transportObserver.SetPhase("measurement")
		maintenance.log(t, nodeForegroundMeasure)
		if archivePhaseTrace != nil {
			archivePhaseTrace.log(t)
		}
		if firstErr := maintenance.firstError(); firstErr != nil {
			types, status := nodeForegroundMaintenanceErrorTypeChain(firstErr)
			t.Logf("node_foreground_maintenance_error_type_chain status=%s types=%v", status, types)
			t.Errorf("Node foreground %s maintenance failed: %v", scenario, firstErr)
		}
		if maintenance.completedWork() == 0 {
			t.Errorf("Node foreground %s recorded no completed maintenance work; see operation/no-op counts", scenario)
		}
	}
	metrics.logAndFail(t, "measurement", measuredResult, err)
	measureStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "measurement", ids, measureStoreStart, measureStoreEnd)
	measureEndRequests, measureEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "measurement", measureStartRequests, measureStartErr, measureEndRequests, measureEndErr)
	t.Logf("versity_fixture_version=%q versity_binary_sha256=%s endpoint_scope=local-loopback; phase server request counts come from the gateway access log", gateway.Version, gateway.BinarySHA256)
}

func TestNodeForegroundPreparationUsesFreshCanceledContexts(t *testing.T) {
	parent := context.Background()
	var previous context.Context
	var seen []context.Context
	for i := 0; i < 3; i++ {
		if err := withNodeForegroundPrepareContext(parent, func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("preparation %d started with canceled context: %w", i, err)
			}
			if previous != nil {
				if previous == ctx {
					return fmt.Errorf("preparation %d reused the previous context", i)
				}
				if !errors.Is(previous.Err(), context.Canceled) {
					return fmt.Errorf("preparation %d began before the previous context was canceled: %v", i, previous.Err())
				}
			}
			previous = ctx
			seen = append(seen, ctx)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("observed %d preparation contexts, want 3", len(seen))
	}
	for i, ctx := range seen {
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Errorf("preparation context %d after callback = %v, want canceled", i, ctx.Err())
		}
	}
}

type nodeForegroundCyclicDiagnosticError struct{}

func (nodeForegroundCyclicDiagnosticError) Error() string { return "cycle" }
func (nodeForegroundCyclicDiagnosticError) Unwrap() error {
	return nodeForegroundCyclicDiagnosticError{}
}

func TestNodeForegroundMaintenanceErrorTypeChain(t *testing.T) {
	const secret = "private-key-and-url-must-not-appear"
	tests := []struct {
		name       string
		err        error
		wantTypes  []string
		wantStatus string
	}{
		{"nil", nil, nil, "none"},
		{"wrapped overload", fmt.Errorf("%s: %w", secret, fmt.Errorf("inner %s: %w", secret, network.ErrOverloaded)), []string{"*fmt.wrapError", "*fmt.wrapError", "*errors.errorString"}, "complete"},
		{"bare overload remains an error", network.ErrOverloaded, []string{"*errors.errorString"}, "complete"},
		{"canceled remains an error", fmt.Errorf("canceled %s: %w", secret, context.Canceled), []string{"*fmt.wrapError", "*errors.errorString"}, "complete"},
		{"joined branches are not a single chain", errors.Join(fmt.Errorf("%s: %w", secret, network.ErrOverloaded), context.Canceled), []string{"*errors.joinError"}, "unknown_branch"},
		{"cycle is explicitly unknown", nodeForegroundCyclicDiagnosticError{}, []string{"node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError", "node.nodeForegroundCyclicDiagnosticError"}, "unknown_bounded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTypes, gotStatus := nodeForegroundMaintenanceErrorTypeChain(tt.err)
			if gotStatus != tt.wantStatus || !reflect.DeepEqual(gotTypes, tt.wantTypes) {
				t.Fatalf("type chain=%v status=%s, want %v %s", gotTypes, gotStatus, tt.wantTypes, tt.wantStatus)
			}
			if len(gotTypes) > 8 || strings.Contains(strings.Join(gotTypes, " "), secret) {
				t.Fatalf("type-only diagnostic exceeded bound or leaked payload: %v", gotTypes)
			}
			if tt.err != nil {
				maintenance := &nodeForegroundMaintenance{}
				maintenance.record(nodeForegroundMaintenanceResult{}, 0, tt.err)
				if maintenance.firstError() != tt.err || maintenance.failures != 1 || maintenance.completedWork() != 0 {
					t.Fatalf("diagnostic changed maintenance failure accounting")
				}
			}
		})
	}
}

func TestNodeForegroundMaintenanceStopsSchedulingAndJoinsInflightWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	operation := func(ctx context.Context) (nodeForegroundMaintenanceResult, error) {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return nodeForegroundMaintenanceResult{}, fmt.Errorf("in-flight operation context was canceled during join: %w", err)
		}
		return nodeForegroundMaintenanceResult{workEvidence: "controlled-work"}, nil
	}
	maintenance := newNodeForegroundMaintenance(context.Background(), "test", nil, operation)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		maintenance.stopAndWait()
	})
	maintenance.start(time.Now().Add(time.Minute))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance operation did not start")
	}
	joined := make(chan struct{})
	maintenance.stopOnce.Do(func() { close(maintenance.stopCh) })
	go func() {
		maintenance.stopAndWait()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("stop returned before the in-flight maintenance operation completed")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("stop did not join the completed maintenance operation")
	}
	if got := maintenance.completedWork(); got != 1 {
		t.Fatalf("completed maintenance work=%d, want 1", got)
	}
	maintenance.record(nodeForegroundMaintenanceResult{noOp: true, workEvidence: "must-not-count-as-work"}, time.Millisecond, nil)
	if got := maintenance.completedWork(); got != 1 {
		t.Fatalf("completed maintenance work after no-op=%d, want 1", got)
	}

	var unexpected atomic.Int32
	afterCutoff := newNodeForegroundMaintenance(context.Background(), "test-cutoff", nil, func(context.Context) (nodeForegroundMaintenanceResult, error) {
		unexpected.Add(1)
		return nodeForegroundMaintenanceResult{}, nil
	})
	afterCutoff.start(time.Now().Add(-time.Second))
	afterCutoff.stopAndWait()
	if got := unexpected.Load(); got != 0 {
		t.Fatalf("maintenance operations started after cutoff=%d, want 0", got)
	}
}

func TestNodeForegroundArchiveCleanupCompletionClassification(t *testing.T) {
	tests := []struct {
		name, want string
		tip        uint64
		deletes    uint64
	}{
		{"retention scan without deletions or tip movement", "archive_cleanup_completed_retention_scan", 10, 0},
		{"concurrent tip advancement is not deletion evidence", "archive_cleanup_completed_retention_scan", 11, 0},
		{"interval deletions are observed but not attributed", "archive_cleanup_completed_with_concurrent_deletes", 10, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nodeForegroundMaintenanceResult{archiveTip: tt.tip, stats: nodeStoreDelta{Deletes: tt.deletes}}
			classifyNodeForegroundArchiveCleanup(&result)
			if result.workEvidence != tt.want || result.noOp {
				t.Fatalf("cleanup classification=%q no_op=%t, want %q and work", result.workEvidence, result.noOp, tt.want)
			}
			maintenance := &nodeForegroundMaintenance{}
			maintenance.record(result, 0, nil)
			if got := maintenance.completedWork(); got != 1 {
				t.Fatalf("completed cleanup work=%d, want 1", got)
			}
		})
	}
	result := nodeForegroundMaintenanceResult{archiveTip: 10, stats: nodeStoreDelta{Deletes: 1}}
	classifyNodeForegroundArchiveCleanup(&result)
	failed := &nodeForegroundMaintenance{}
	failed.record(result, 0, errors.New("cleanup failed"))
	if got := failed.completedWork(); got != 0 {
		t.Fatalf("failed cleanup completed work=%d, want 0", got)
	}
	// Candidate/tip observation alone does not count as checkpoint work;
	// runNodeForegroundCheckpoint must first verify the certified seal and CURRENT.
	checkpoint := &nodeForegroundMaintenance{}
	checkpoint.record(nodeForegroundMaintenanceResult{candidate: 10, archiveTip: 11}, 0, nil)
	if got := checkpoint.completedWork(); got != 0 {
		t.Fatalf("uncertified checkpoint observation completed work=%d, want 0", got)
	}
}

func TestNodeForegroundMetricsSeparateMaintenanceCohortsAndDrain(t *testing.T) {

	metrics := &nodeForegroundCostMetrics{}
	start := time.Now()
	metrics.observe(foregroundcosttest.Observation{
		Operation: foregroundcosttest.Put, Overlap: true, InWindow: true,
		Started: start, Completed: start.Add(2 * time.Millisecond),
	})
	metrics.observe(foregroundcosttest.Observation{
		Operation: foregroundcosttest.Get, Overlap: true, InWindow: true,
		Started: start, Completed: start.Add(4 * time.Millisecond), Err: context.DeadlineExceeded,
	})
	metrics.observe(foregroundcosttest.Observation{
		Operation: foregroundcosttest.Put, Overlap: true, InWindow: false,
		Started: start, Completed: start.Add(35 * time.Second), Err: context.DeadlineExceeded,
	})
	metrics.observe(foregroundcosttest.Observation{
		Operation: foregroundcosttest.Get, Overlap: false, InWindow: true,
		Started: start, Completed: start.Add(time.Millisecond),
	})

	if metrics.overlap.puts != 2 || metrics.overlap.putDrained != 1 || metrics.overlap.putErrorsInWindow != 0 || metrics.overlap.putErrorsDrained != 1 {
		t.Fatalf("overlap PUT accounting=%+v", metrics.overlap)
	}
	if metrics.overlap.gets != 1 || metrics.overlap.getErrorsInWindow != 1 || metrics.overlap.getErrorsDrained != 0 {
		t.Fatalf("overlap GET accounting=%+v", metrics.overlap)
	}
	if metrics.nonOverlap.gets != 1 || metrics.nonOverlap.getErrors != 0 {
		t.Fatalf("non-overlap GET accounting=%+v", metrics.nonOverlap)
	}
	if got, ok := nodeForegroundP99(metrics.overlap.putAll); ok || got != 0 {
		t.Fatalf("small overlap cohort p99=%s measured=%t, want insufficient samples", got, ok)
	}
}

type nodeForegroundMetricCohort struct {
	puts, gets                           int
	putErrors, getErrors                 int
	putErrorsInWindow, getErrorsInWindow int
	putErrorsDrained, getErrorsDrained   int
	putDrained, getDrained               int
	putAll, putSuccess                   []time.Duration
	getAll, getSuccess                   []time.Duration
}

type nodeForegroundMaintenanceSample struct {
	active bool
	epoch  uint64
}

type nodeForegroundMaintenanceResult struct {
	workEvidence string
	noOp         bool
	candidate    uint64
	winner       uint64
	archiveTip   uint64
	stats        nodeStoreDelta
}

type nodeForegroundMaintenanceOperation func(context.Context) (nodeForegroundMaintenanceResult, error)

type nodeForegroundMaintenance struct {
	mode           string
	parent         context.Context
	observer       *objmetrics.S3TransportObserver
	operation      nodeForegroundMaintenanceOperation
	startCh        chan time.Time
	stopCh         chan struct{}
	doneCh         chan struct{}
	startOnce      sync.Once
	stopOnce       sync.Once
	state          atomic.Pointer[nodeForegroundMaintenanceSample]
	mu             sync.Mutex
	attempts       int
	completed      int
	work           int
	noops          int
	failures       int
	durations      []time.Duration
	firstErr       error
	candidateFirst uint64
	candidateLast  uint64
	winnerFirst    uint64
	winnerLast     uint64
	stats          nodeForegroundMaintenanceTotals
}

type nodeForegroundMaintenanceTotals struct {
	HTTPRequests, HTTPFailures, Lists, Deletes, Uploads, Gets, Heads uint64
	AttemptedBytes, AcknowledgedBytes, DownloadedBytes               uint64
	SDKRetries, RetryMetadataUnknown, TransportFailures              uint64
	Unexpected4xx, HTTP5xx, ConditionConflicts                       uint64
}

func newNodeForegroundMaintenance(parent context.Context, mode string, observer *objmetrics.S3TransportObserver, operation nodeForegroundMaintenanceOperation) *nodeForegroundMaintenance {
	maintenance := &nodeForegroundMaintenance{
		mode: mode, parent: parent, observer: observer, operation: operation,
		startCh: make(chan time.Time, 1), stopCh: make(chan struct{}), doneCh: make(chan struct{}),
	}
	maintenance.state.Store(&nodeForegroundMaintenanceSample{})
	go maintenance.run()
	return maintenance
}

func (m *nodeForegroundMaintenance) snapshot() (bool, uint64) {
	if m == nil {
		return false, 0
	}
	sample := m.state.Load()
	if sample == nil {
		return false, 0
	}
	return sample.active, sample.epoch
}

func (m *nodeForegroundMaintenance) start(cutoff time.Time) {
	if m == nil || cutoff.IsZero() {
		return
	}
	m.startOnce.Do(func() {
		select {
		case m.startCh <- cutoff:
		case <-m.stopCh:
		}
	})
}

func (m *nodeForegroundMaintenance) stopAndWait() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() { close(m.stopCh) })
	<-m.doneCh
}

func (m *nodeForegroundMaintenance) run() {
	defer close(m.doneCh)
	var cutoff time.Time
	select {
	case cutoff = <-m.startCh:
	case <-m.stopCh:
		return
	case <-m.parent.Done():
		return
	}
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}
		if !time.Now().Before(cutoff) {
			return
		}
		m.state.Store(&nodeForegroundMaintenanceSample{active: true, epoch: m.snapshotEpoch() + 1})
		if m.observer != nil {
			m.observer.SetPhase(m.mode + "_overlap")
		}
		operationCtx, cancel := context.WithTimeout(m.parent, nodeForegroundPrepareLimit)
		started := time.Now()
		result, err := m.operation(operationCtx)
		duration := time.Since(started)
		cancel()
		m.state.Store(&nodeForegroundMaintenanceSample{epoch: m.snapshotEpoch() + 1})
		if m.observer != nil {
			m.observer.SetPhase("measurement")
		}
		m.record(result, duration, err)
		if err != nil || m.parent.Err() != nil {
			return
		}
	}
}

func (m *nodeForegroundMaintenance) snapshotEpoch() uint64 {
	sample := m.state.Load()
	if sample == nil {
		return 0
	}
	return sample.epoch
}

func (m *nodeForegroundMaintenance) record(result nodeForegroundMaintenanceResult, duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts++
	m.durations = append(m.durations, duration)
	if err != nil {
		m.failures++
		if m.firstErr == nil {
			m.firstErr = err
		}
		return
	}
	m.completed++
	if result.noOp {
		m.noops++
	} else if result.workEvidence != "" {
		m.work++
	}
	if m.candidateFirst == 0 {
		m.candidateFirst = result.candidate
	}
	m.candidateLast = result.candidate
	if m.winnerFirst == 0 {
		m.winnerFirst = result.winner
	}
	m.winnerLast = result.winner
	m.stats.HTTPRequests += result.stats.HTTPRequests
	m.stats.HTTPFailures += result.stats.HTTPFailures
	m.stats.Lists += result.stats.Lists
	m.stats.Deletes += result.stats.Deletes
	m.stats.Uploads += result.stats.Uploads
	m.stats.Gets += result.stats.Gets
	m.stats.Heads += result.stats.Heads
	m.stats.AttemptedBytes += result.stats.BytesUploadedAttempts
	m.stats.AcknowledgedBytes += result.stats.BytesPublished
	m.stats.DownloadedBytes += result.stats.BytesDownloaded
	m.stats.SDKRetries += result.stats.SDKRetries
	m.stats.RetryMetadataUnknown += result.stats.RetryMetadataUnknownRequests
	m.stats.TransportFailures += result.stats.TransportFailures
	m.stats.Unexpected4xx += result.stats.Unexpected4xx
	m.stats.HTTP5xx += result.stats.HTTP5xx
	m.stats.ConditionConflicts += result.stats.ConditionConflicts
}

func (m *nodeForegroundMaintenance) firstError() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.firstErr
}

// nodeForegroundMaintenanceErrorTypeChain emits only concrete Go type names.
// It follows one-cause Unwrap chains, not joined branches; the bound ends cycles.
func nodeForegroundMaintenanceErrorTypeChain(err error) ([]string, string) {
	if err == nil {
		return nil, "none"
	}
	const limit = 8
	types := make([]string, 0, limit)
	for err != nil && len(types) < limit {
		types = append(types, reflect.TypeOf(err).String())
		if _, branched := err.(interface{ Unwrap() []error }); branched {
			return types, "unknown_branch"
		}
		err = errors.Unwrap(err)
	}
	if err != nil {
		return types, "unknown_bounded"
	}
	return types, "complete"
}

func (m *nodeForegroundMaintenance) completedWork() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.work
}

func (m *nodeForegroundMaintenance) log(t *testing.T, measurementWindow time.Duration) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	var total, maximum time.Duration
	for _, duration := range m.durations {
		total += duration
		if duration > maximum {
			maximum = duration
		}
	}
	average := time.Duration(0)
	if len(m.durations) != 0 {
		average = total / time.Duration(len(m.durations))
	}
	t.Logf("node_foreground_maintenance mode=%s attempts=%d completed=%d work=%d noops=%d failures=%d total_operation_time=%s max_operation_time=%s average_operation_time=%s measured_window=%s candidate_first=%d candidate_last=%d winning_current_first=%d winning_current_last=%d interval_store_totals=%+v interval_store_attribution=concurrent_with_api_traffic first_error=%v", m.mode, m.attempts, m.completed, m.work, m.noops, m.failures, total, maximum, average, measurementWindow, m.candidateFirst, m.candidateLast, m.winnerFirst, m.winnerLast, m.stats, m.firstErr)
}

func runNodeForegroundCheckpoint(ctx context.Context, node *Node, sharedReadDir string) (nodeForegroundMaintenanceResult, error) {
	if node == nil || node.archive == nil || node.core == nil || node.material == nil || node.checkpointer == nil || node.checkpoints == nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint maintenance components unavailable")
	}
	beforeCandidate := node.checkpoints.Latest()
	beforeIndex := uint64(0)
	if beforeCandidate != nil {
		beforeIndex = beforeCandidate.Index
	}
	beforeArchiveTip := uint64(node.archive.Tip())
	beforeStore := snapshotNodeStoreStats([]*Node{node})
	if err := node.archive.SyncThrough(ctx, node.core, node.core.Tip()); err != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("sync archive through live core tip: %w", err)
	}
	stateTip := node.material.StateTip()
	if err := node.checkpointer.CheckpointOnShutdown(ctx, stateTip); err != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint live materialized state: %w", err)
	}
	candidate := node.checkpoints.Latest()
	if candidate == nil || candidate.Index < stateTip {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint candidate does not cover materialized state: candidate=%v state_tip=%d", candidate, stateTip)
	}
	seal, sealed, err := node.core.LatestCheckpointSeal()
	if err != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("read current core checkpoint seal: %w", err)
	}
	if !sealed {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint maintenance produced no certified core seal")
	}
	shared, readErr := readNodeForegroundSharedCurrent(ctx, node, sharedReadDir)
	if readErr != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("read shared CURRENT after checkpoint: %w", readErr)
	}
	if shared == nil || shared.Index < candidate.Index || shared.Index < stateTip ||
		shared.Index != uint64(seal.Index) || shared.RootHash != seal.RootHash || shared.Hash != seal.StateHash {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("winning shared CURRENT is not the current certified seal or does not cover candidate/state: present=%t current_index=%d candidate_index=%d state_tip=%d seal_index=%d", shared != nil, checkpointIndex(shared), candidate.Index, stateTip, seal.Index)
	}
	if shared.Index == candidate.Index && (shared.RootHash != candidate.RootHash || shared.Hash != candidate.Hash) {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("same-index winning CURRENT differs from generated checkpoint candidate: current_index=%d", candidate.Index)
	}
	archiveTip := uint64(node.archive.Tip())
	if archiveTip < uint64(seal.DecisionSlot) {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive tip does not cover winning certified checkpoint: archive_tip=%d seal_decision_slot=%d", archiveTip, seal.DecisionSlot)
	}
	afterStore := snapshotNodeStoreStats([]*Node{node})
	if len(beforeStore) != 1 || len(afterStore) != 1 || !beforeStore[0].ok || !afterStore[0].ok {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint maintenance store counters unavailable")
	}
	stats, ok := nodeStoreStatsDelta(beforeStore[0].stats, afterStore[0].stats)
	if !ok {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("checkpoint maintenance store counters decreased")
	}
	// This interval includes concurrent API calls and is reported as such; it is
	// not represented as maintenance-only object-store work.
	result := nodeForegroundMaintenanceResult{candidate: candidate.Index, winner: shared.Index, archiveTip: archiveTip, stats: stats}
	if candidate.Index > beforeIndex || archiveTip > beforeArchiveTip {
		result.workEvidence = "certified_checkpoint_or_archive_advanced"
	} else {
		result.noOp = true
	}
	return result, nil
}

type nodeArchiveGCPhase struct {
	name      string
	started   time.Time
	elapsed   time.Duration
	remaining time.Duration
	count     int
	errors    int
	active    bool
}

type nodeArchiveGCPhaseTrace struct {
	mu                    sync.Mutex
	deadline              time.Time
	operations            int
	unknown               bool
	phases                [14]nodeArchiveGCPhase
	compactionChoices     [2]int // full, reuse
	publicationResults    [4]int // success, typed condition, context done, other
	compactionClassified  bool
	publicationClassified bool
}

func newNodeArchiveGCPhaseTrace() *nodeArchiveGCPhaseTrace {
	trace := &nodeArchiveGCPhaseTrace{}
	for i, name := range [...]string{"manager-lock", "remote-lock", "load", "compaction", "extent-upload", "publication", "readback", "pins", "readers", "candidate-scan", "object-scan", "delete-wait", "marker-cleanup", "lock-release"} {
		trace.phases[i].name = name
	}
	return trace
}

func (trace *nodeArchiveGCPhaseTrace) setDeadline(ctx context.Context) {
	trace.mu.Lock()
	trace.deadline, _ = ctx.Deadline()
	trace.operations++
	trace.mu.Unlock()
}

func (trace *nodeArchiveGCPhaseTrace) hit(event string) {
	if !strings.HasPrefix(event, "archive-gc:") {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	switch event {
	case "archive-gc:compaction-choice:full", "archive-gc:compaction-choice:reuse":
		if !trace.phases[3].active || trace.compactionClassified {
			trace.unknown = true
		}
		trace.compactionClassified = true
		if event == "archive-gc:compaction-choice:full" {
			trace.compactionChoices[0]++
		} else {
			trace.compactionChoices[1]++
		}
		return
	case "archive-gc:publication-result:success", "archive-gc:publication-result:typed_condition", "archive-gc:publication-result:context_done", "archive-gc:publication-result:other":
		if !trace.phases[5].active || trace.publicationClassified {
			trace.unknown = true
		}
		trace.publicationClassified = true
		switch event {
		case "archive-gc:publication-result:success":
			trace.publicationResults[0]++
		case "archive-gc:publication-result:typed_condition":
			trace.publicationResults[1]++
		case "archive-gc:publication-result:context_done":
			trace.publicationResults[2]++
		case "archive-gc:publication-result:other":
			trace.publicationResults[3]++
		}
		return
	}
	for i := range trace.phases {
		phase := &trace.phases[i]
		prefix := "archive-gc:" + phase.name + ":"
		if !strings.HasPrefix(event, prefix) {
			continue
		}
		switch strings.TrimPrefix(event, prefix) {
		case "begin":
			for j := range trace.phases {
				if trace.phases[j].active {
					trace.unknown = true
				}
			}
			if phase.active {
				trace.unknown = true
			}
			phase.started, phase.active = time.Now(), true
			if i == 3 {
				trace.compactionClassified = false
			} else if i == 5 {
				trace.publicationClassified = false
			}
		case "success", "error":
			if !phase.active {
				trace.unknown = true
				return
			}
			if i == 3 && !trace.compactionClassified || i == 5 && !trace.publicationClassified {
				trace.unknown = true
			}
			phase.elapsed += time.Since(phase.started)
			phase.count++
			if strings.HasSuffix(event, ":error") {
				phase.errors++
			}
			if !trace.deadline.IsZero() {
				remaining := time.Until(trace.deadline)
				if phase.count == 1 || remaining < phase.remaining {
					phase.remaining = remaining
				}
			}
			phase.active = false
		default:
			trace.unknown = true
		}
		return
	}
	trace.unknown = true
}

func (trace *nodeArchiveGCPhaseTrace) log(t *testing.T) {
	t.Helper()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.operations == 0 || trace.phases[0].count != trace.operations {
		trace.unknown = true
	}
	if trace.compactionChoices[0]+trace.compactionChoices[1] != trace.phases[3].count ||
		trace.publicationResults[0]+trace.publicationResults[1]+trace.publicationResults[2]+trace.publicationResults[3] != trace.phases[5].count ||
		trace.publicationResults[0] != trace.phases[5].count-trace.phases[5].errors {
		trace.unknown = true
	}
	for _, phase := range trace.phases {
		if phase.active {
			trace.unknown = true
		}
		if phase.count != 0 || phase.active {
			t.Logf("node_archive_cleanup_phase name=%s count=%d errors=%d elapsed=%s minimum_deadline_remaining_at_terminal=%s active=%t", phase.name, phase.count, phase.errors, phase.elapsed, phase.remaining, phase.active)
		}
	}
	t.Logf("node_archive_cleanup_choices full=%d reuse=%d publication_success=%d publication_typed_condition=%d publication_context_done=%d publication_other=%d", trace.compactionChoices[0], trace.compactionChoices[1], trace.publicationResults[0], trace.publicationResults[1], trace.publicationResults[2], trace.publicationResults[3])
	t.Logf("node_archive_cleanup_phase_coverage operations=%d incomplete_or_ambiguous=%t", trace.operations, trace.unknown)
	if trace.unknown {
		t.Error("archive cleanup phase and decision counts are incomplete or ambiguous")
	}
}

func TestNodeArchiveGCPhaseTraceFiniteTerminals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	trace := newNodeArchiveGCPhaseTrace()
	trace.setDeadline(ctx)
	trace.hit("archive-gc:manager-lock:begin")
	trace.hit("archive-gc:manager-lock:success")
	trace.hit("archive-gc:load:begin")
	trace.hit("archive-gc:load:error")
	trace.hit("archive-gc:compaction:begin")
	trace.hit("archive-gc:compaction-choice:full")
	trace.hit("archive-gc:compaction:success")
	trace.hit("archive-gc:publication:begin")
	trace.hit("archive-gc:publication-result:typed_condition")
	trace.hit("archive-gc:publication:error")
	trace.hit("archive-gc:object-scan:begin")
	trace.hit("archive-gc:object-scan:success")
	if trace.unknown || trace.phases[2].count != 1 || trace.phases[2].errors != 1 || trace.phases[2].active || trace.phases[2].remaining <= 0 || trace.phases[10].count != 1 ||
		trace.compactionChoices != [2]int{1, 0} || trace.publicationResults != [4]int{0, 1, 0, 0} {
		t.Fatalf("finite phase trace lost known terminal: unknown=%t load=%+v object_scan=%+v", trace.unknown, trace.phases[2], trace.phases[10])
	}
	trace.log(t)
	trace.hit("archive-gc:load:begin")
	trace.hit("archive-gc:compaction:begin")
	if !trace.unknown {
		t.Fatal("overlapping phase callbacks must mark attribution ambiguous")
	}
	missing := newNodeArchiveGCPhaseTrace()
	missing.hit("archive-gc:compaction:begin")
	missing.hit("archive-gc:compaction:success")
	if !missing.unknown {
		t.Fatal("compaction without a choice must be incomplete")
	}
	duplicate := newNodeArchiveGCPhaseTrace()
	duplicate.hit("archive-gc:publication:begin")
	duplicate.hit("archive-gc:publication-result:typed_condition")
	duplicate.hit("archive-gc:publication-result:other")
	if !duplicate.unknown {
		t.Fatal("two classifications for one publication must be ambiguous")
	}
}

func runNodeForegroundArchiveCleanup(t *testing.T, ctx context.Context, node *Node, sharedReadDir string, phaseTrace *nodeArchiveGCPhaseTrace) (nodeForegroundMaintenanceResult, error) {
	if node == nil || node.archive == nil || node.core == nil || node.bucket == nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup components unavailable")
	}
	beforeStore := snapshotNodeStoreStats([]*Node{node})
	beforeTip := uint64(node.archive.Tip())
	phaseTrace.setDeadline(ctx)
	cleanupCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, phaseTrace.hit)
	cleanupErr := node.archive.Cleanup(cleanupCtx, node.config.ObjStoreGCGracePeriod)
	if cleanupErr != nil {
		afterStore := snapshotNodeStoreStats([]*Node{node})
		if len(beforeStore) == 1 && len(afterStore) == 1 && beforeStore[0].ok && afterStore[0].ok {
			if delta, ok := nodeStoreStatsDelta(beforeStore[0].stats, afterStore[0].stats); ok {
				t.Logf("node_archive_cleanup_failed_interval status=measured attribution=concurrent_with_api_traffic completed_work=false delta=%+v", delta)
			} else {
				t.Log("node_archive_cleanup_failed_interval status=counter_decrease attribution=unknown completed_work=false")
			}
		} else {
			t.Log("node_archive_cleanup_failed_interval status=unavailable attribution=unknown completed_work=false")
		}
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup with configured %s grace: %w", node.config.ObjStoreGCGracePeriod, cleanupErr)
	}
	afterStore := snapshotNodeStoreStats([]*Node{node})
	if len(beforeStore) != 1 || len(afterStore) != 1 || !beforeStore[0].ok || !afterStore[0].ok {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup store counters unavailable")
	}
	stats, ok := nodeStoreStatsDelta(beforeStore[0].stats, afterStore[0].stats)
	if !ok {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup store counters decreased")
	}
	afterTip := uint64(node.archive.Tip())
	seal, sealed, err := node.core.LatestCheckpointSeal()
	if err != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("read certified seal after archive cleanup: %w", err)
	}
	if !sealed || afterTip < uint64(seal.DecisionSlot) || afterTip < beforeTip {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup lost certified coverage: sealed=%t before_tip=%d after_tip=%d seal_slot=%d", sealed, beforeTip, afterTip, seal.DecisionSlot)
	}
	shared, readErr := readNodeForegroundSharedCurrent(ctx, node, sharedReadDir)
	if readErr != nil {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("read shared CURRENT after archive cleanup: %w", readErr)
	}
	if shared == nil || shared.Index != uint64(seal.Index) || shared.RootHash != seal.RootHash || shared.Hash != seal.StateHash || afterTip < uint64(seal.DecisionSlot) {
		return nodeForegroundMaintenanceResult{}, fmt.Errorf("archive cleanup left CURRENT outside the certified archive: current_present=%t current_index=%d seal_index=%d archive_tip=%d", shared != nil, checkpointIndex(shared), seal.Index, afterTip)
	}
	result := nodeForegroundMaintenanceResult{winner: uint64(seal.Index), archiveTip: afterTip, stats: stats}
	// A successful Cleanup invocation is direct work evidence: it performed
	// the retention scan (Load, compact, publish, verify, iterate candidates,
	// delete expired). stats.Deletes is a concurrent interval total that
	// includes foreground API traffic and is NOT attributed to cleanup.
	// afterTip may advance from concurrent foreground Archive publication,
	// not from GC cleanup, so it is NOT used as cleanup-work evidence.
	// Tip before/after is retained solely as a concurrent safety diagnostic.
	classifyNodeForegroundArchiveCleanup(&result)
	return result, nil
}

func classifyNodeForegroundArchiveCleanup(result *nodeForegroundMaintenanceResult) {
	if result.stats.Deletes > 0 {
		result.workEvidence = "archive_cleanup_completed_with_concurrent_deletes"
	} else {
		result.workEvidence = "archive_cleanup_completed_retention_scan"
	}
}

func readNodeForegroundSharedCurrent(ctx context.Context, node *Node, rootDir string) (*checkpoint.Checkpoint, error) {
	localDir, err := os.MkdirTemp(rootDir, "shared-current-")
	if err != nil {
		return nil, fmt.Errorf("create isolated shared CURRENT reader directory: %w", err)
	}
	defer os.RemoveAll(localDir)
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return readFreshSharedCurrent(readCtx, node, localDir)
}

type nodeForegroundCostMetrics struct {
	mu                   sync.Mutex
	puts, gets           int
	putErrors, getErrors int
	unknownCommits       int
	inWindow, drained    int
	putBytesAttempted    uint64
	putBytesAcknowledged uint64
	inWindowAll          []time.Duration
	inWindowSuccess      []time.Duration
	overlap              nodeForegroundMetricCohort
	nonOverlap           nodeForegroundMetricCohort
	firstError           string
}

type nodeStoreSnapshot struct {
	stats objmetrics.Stats
	ok    bool
}

type nodeStoreDelta struct {
	Uploads, Gets, Lists, Heads, Deletes, Failures                                        uint64
	BytesUploadedAttempts, BytesPublished, BytesDownloaded                                uint64
	HTTPRequests, HTTPRequestBodyBytes, HTTPResponseBodyBytes, HTTPFailures               uint64
	HTTPGet, HTTPPut, HTTPHead, HTTPDelete, HTTPOther, S3HTTPFailures                     uint64
	SDKRetries, RetryMetadataRequests, RetryMetadataUnknownRequests                       uint64
	TransportFailures, Unexpected4xx, HTTP5xx, ConditionConflicts                         uint64
	ObservedRequestIdentities, ObservedRequestRepeats, RequestGroupingUnknown             uint64
	ReplayTrackerCapacityMisses, ReplayIdentityCapacityMisses, ReplayIncompleteOperations uint64
	ReplayTrackedOperationsStart, ReplayTrackedOperationsEnd                              uint64
	ReplayOpenReadersStart, ReplayOpenReadersEnd                                          uint64
	ReplayGroupingEnabled                                                                 bool
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
	if before.ReplayGroupingEnabled != after.ReplayGroupingEnabled ||
		after.Uploads < before.Uploads || after.Gets < before.Gets || after.Lists < before.Lists || after.Heads < before.Heads || after.Deletes < before.Deletes || after.Failures < before.Failures ||
		after.BytesUploaded < before.BytesUploaded || after.BytesPublished < before.BytesPublished || after.BytesDownloaded < before.BytesDownloaded ||
		after.HTTPRequests < before.HTTPRequests || after.HTTPRequestBodyBytes < before.HTTPRequestBodyBytes || after.HTTPResponseBodyBytes < before.HTTPResponseBodyBytes || after.HTTPFailures < before.HTTPFailures ||
		after.HTTPGetRequests < before.HTTPGetRequests || after.HTTPPutRequests < before.HTTPPutRequests || after.HTTPHeadRequests < before.HTTPHeadRequests || after.HTTPDeleteRequests < before.HTTPDeleteRequests || after.HTTPOtherRequests < before.HTTPOtherRequests || after.S3HTTPFailures < before.S3HTTPFailures ||
		after.SDKRetries < before.SDKRetries || after.RetryMetadataRequests < before.RetryMetadataRequests || after.RetryMetadataUnknownRequests < before.RetryMetadataUnknownRequests ||
		after.ObservedRequestIdentities < before.ObservedRequestIdentities || after.ObservedRequestRepeats < before.ObservedRequestRepeats || after.RequestGroupingUnknown < before.RequestGroupingUnknown ||
		after.ReplayTrackerCapacityMisses < before.ReplayTrackerCapacityMisses || after.ReplayIdentityCapacityMisses < before.ReplayIdentityCapacityMisses || after.ReplayIncompleteOperations < before.ReplayIncompleteOperations ||
		after.TransportFailures < before.TransportFailures || after.Unexpected4xx < before.Unexpected4xx || after.HTTP5xx < before.HTTP5xx || after.ConditionConflicts < before.ConditionConflicts {
		return nodeStoreDelta{}, false
	}
	return nodeStoreDelta{
		ReplayGroupingEnabled: after.ReplayGroupingEnabled,
		Uploads:               after.Uploads - before.Uploads, Gets: after.Gets - before.Gets,
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
		ObservedRequestIdentities:    after.ObservedRequestIdentities - before.ObservedRequestIdentities,
		ObservedRequestRepeats:       after.ObservedRequestRepeats - before.ObservedRequestRepeats,
		RequestGroupingUnknown:       after.RequestGroupingUnknown - before.RequestGroupingUnknown,
		ReplayTrackerCapacityMisses:  after.ReplayTrackerCapacityMisses - before.ReplayTrackerCapacityMisses,
		ReplayIdentityCapacityMisses: after.ReplayIdentityCapacityMisses - before.ReplayIdentityCapacityMisses,
		ReplayIncompleteOperations:   after.ReplayIncompleteOperations - before.ReplayIncompleteOperations,
		ReplayTrackedOperationsStart: before.ReplayTrackedOperationsActive,
		ReplayTrackedOperationsEnd:   after.ReplayTrackedOperationsActive,
		ReplayOpenReadersStart:       before.ReplayOpenReaders,
		ReplayOpenReadersEnd:         after.ReplayOpenReaders,
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
		ReplayGroupingEnabled: true,
		Uploads:               2, Gets: 3, BytesUploaded: 700, BytesPublished: 600,
		SDKRetries: 4, RetryMetadataRequests: 5, RetryMetadataUnknownRequests: 6,
		ObservedRequestIdentities: 10, ObservedRequestRepeats: 3, RequestGroupingUnknown: 2,
		ReplayTrackerCapacityMisses: 1, ReplayIdentityCapacityMisses: 1, ReplayIncompleteOperations: 2,
		ReplayTrackedOperationsActive: 4, ReplayOpenReaders: 2,
	}
	after := objmetrics.Stats{
		ReplayGroupingEnabled: true,
		Uploads:               5, Gets: 8, BytesUploaded: 1700, BytesPublished: 1500,
		SDKRetries: 7, RetryMetadataRequests: 9, RetryMetadataUnknownRequests: 11,
		ObservedRequestIdentities: 16, ObservedRequestRepeats: 8, RequestGroupingUnknown: 5,
		ReplayTrackerCapacityMisses: 2, ReplayIdentityCapacityMisses: 3, ReplayIncompleteOperations: 5,
		ReplayTrackedOperationsActive: 1, ReplayOpenReaders: 0,
	}
	delta, ok := nodeStoreStatsDelta(before, after)
	if !ok || !delta.ReplayGroupingEnabled || delta.Uploads != 3 || delta.Gets != 5 || delta.BytesUploadedAttempts != 1000 || delta.BytesPublished != 900 || delta.SDKRetries != 3 || delta.RetryMetadataRequests != 4 || delta.RetryMetadataUnknownRequests != 5 ||
		delta.ObservedRequestIdentities != 6 || delta.ObservedRequestRepeats != 5 || delta.RequestGroupingUnknown != 3 || delta.ReplayTrackerCapacityMisses != 1 || delta.ReplayIdentityCapacityMisses != 2 || delta.ReplayIncompleteOperations != 3 || delta.ReplayTrackedOperationsStart != 4 || delta.ReplayTrackedOperationsEnd != 1 || delta.ReplayOpenReadersStart != 2 || delta.ReplayOpenReadersEnd != 0 {
		t.Fatalf("node store delta=%+v valid=%t", delta, ok)
	}
	after.ReplayGroupingEnabled = false
	if _, ok := nodeStoreStatsDelta(before, after); ok {
		t.Fatal("observer mode change was reported as a valid phase delta")
	}
	after.ReplayGroupingEnabled = true
	after.BytesUploaded = before.BytesUploaded - 1
	if _, ok := nodeStoreStatsDelta(before, after); ok {
		t.Fatal("counter decrease was reported as a zero-valued phase delta")
	}
}

func TestNodeForegroundBudgetBoundariesPreserveWindowsAndCleanupReserve(t *testing.T) {
	if nodeForegroundCleanup != 72*time.Second {
		t.Fatalf("cleanup planning reserve=%s, want 72s (3x 10s preparation + 3x 10s shutdown operation + 7s gateway close + 5s slack)", nodeForegroundCleanup)
	}
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
	cohort := &m.nonOverlap
	if observation.Overlap {
		cohort = &m.overlap
	}
	if observation.Operation == foregroundcosttest.Put {
		m.puts++
		cohort.puts++
		m.putBytesAttempted += nodeForegroundValueBytes
		if observation.Err == nil {
			m.putBytesAcknowledged += nodeForegroundValueBytes
		} else {
			m.putErrors++
			cohort.putErrors++
			if errors.Is(observation.Err, network.ErrCommitUnknown) {
				m.unknownCommits++
			}
		}
	} else {
		m.gets++
		cohort.gets++
		if observation.Err != nil {
			m.getErrors++
			cohort.getErrors++
		}
	}
	if !observation.InWindow {
		m.drained++
		if observation.Operation == foregroundcosttest.Put {
			cohort.putDrained++
			if observation.Err != nil {
				cohort.putErrorsDrained++
			}
		} else {
			cohort.getDrained++
			if observation.Err != nil {
				cohort.getErrorsDrained++
			}
		}
	} else {
		m.inWindow++
		if observation.Operation == foregroundcosttest.Put && observation.Err != nil {
			cohort.putErrorsInWindow++
		} else if observation.Operation == foregroundcosttest.Get && observation.Err != nil {
			cohort.getErrorsInWindow++
		}
		if !observation.Started.IsZero() && !observation.Completed.IsZero() {
			latency := observation.Completed.Sub(observation.Started)
			m.inWindowAll = append(m.inWindowAll, latency)
			if observation.Err == nil {
				m.inWindowSuccess = append(m.inWindowSuccess, latency)
			}
			if observation.Operation == foregroundcosttest.Put {
				cohort.putAll = append(cohort.putAll, latency)
				if observation.Err == nil {
					cohort.putSuccess = append(cohort.putSuccess, latency)
				}
			} else {
				cohort.getAll = append(cohort.getAll, latency)
				if observation.Err == nil {
					cohort.getSuccess = append(cohort.getSuccess, latency)
				}
			}
		}
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
	inWindowAll := append([]time.Duration(nil), m.inWindowAll...)
	inWindowSuccess := append([]time.Duration(nil), m.inWindowSuccess...)
	overlap := cloneNodeForegroundMetricCohort(m.overlap)
	nonOverlap := cloneNodeForegroundMetricCohort(m.nonOverlap)
	m.mu.Unlock()
	t.Logf("node_foreground_phase=%s started=%d completed=%d pending_at_cutoff=%d outstanding=%d puts=%d gets=%d api_errors=%d put_errors=%d get_errors=%d commit_unknown=%d completed_in_window=%d completed_during_drain=%d put_payload_bytes_attempted=%d put_payload_bytes_api_acknowledged=%d first_error=%q", phase, result.Started, result.Completed, result.PendingAtCutoff, result.Outstanding, puts, gets, putErrors+getErrors, putErrors, getErrors, unknownCommits, inWindow, drained, attempted, acknowledged, firstError)
	logNodeForegroundPercentiles(t, phase, "all", inWindowAll, inWindowSuccess)
	logNodeForegroundCohort(t, phase, "maintenance_overlap", overlap)
	logNodeForegroundCohort(t, phase, "maintenance_non_overlap", nonOverlap)
	if runErr != nil {
		t.Fatalf("Node foreground %s runner: %v", phase, runErr)
	}
	if putErrors+getErrors != 0 {
		t.Fatalf("Node foreground %s had %d API errors (including %d unknown commit outcomes)", phase, putErrors+getErrors, unknownCommits)
	}
}

func cloneNodeForegroundMetricCohort(cohort nodeForegroundMetricCohort) nodeForegroundMetricCohort {
	return nodeForegroundMetricCohort{
		puts: cohort.puts, gets: cohort.gets, putErrors: cohort.putErrors, getErrors: cohort.getErrors,
		putErrorsInWindow: cohort.putErrorsInWindow, getErrorsInWindow: cohort.getErrorsInWindow,
		putErrorsDrained: cohort.putErrorsDrained, getErrorsDrained: cohort.getErrorsDrained,
		putDrained: cohort.putDrained, getDrained: cohort.getDrained,
		putAll: append([]time.Duration(nil), cohort.putAll...), putSuccess: append([]time.Duration(nil), cohort.putSuccess...),
		getAll: append([]time.Duration(nil), cohort.getAll...), getSuccess: append([]time.Duration(nil), cohort.getSuccess...),
	}
}

func nodeForegroundP99(samples []time.Duration) (time.Duration, bool) {
	if len(samples) < nodeForegroundP99MinSamples {
		return 0, false
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[(len(ordered)-1)*99/100], true
}

func logNodeForegroundPercentiles(t *testing.T, phase, cohort string, all, successful []time.Duration) {
	t.Helper()
	allP99, allReady := nodeForegroundP99(all)
	successP99, successReady := nodeForegroundP99(successful)
	allStatus, successStatus := "measured", "measured"
	allText, successText := allP99.String(), successP99.String()
	if !allReady {
		allStatus, allText = "insufficient_samples", "not-reported"
	}
	if !successReady {
		successStatus, successText = "insufficient_samples", "not-reported"
	}
	t.Logf("node_foreground_p99 phase=%s cohort=%s all_completed_samples=%d all_completed_status=%s all_completed_p99=%s successful_samples=%d successful_status=%s successful_p99=%s minimum_samples=%d", phase, cohort, len(all), allStatus, allText, len(successful), successStatus, successText, nodeForegroundP99MinSamples)
}

func logNodeForegroundCohort(t *testing.T, phase, name string, cohort nodeForegroundMetricCohort) {
	t.Helper()
	logNodeForegroundPercentiles(t, phase, name+"_put", cohort.putAll, cohort.putSuccess)
	logNodeForegroundPercentiles(t, phase, name+"_get", cohort.getAll, cohort.getSuccess)
	t.Logf("node_foreground_cohort phase=%s cohort=%s puts_completed=%d puts_in_window=%d put_errors_in_window=%d put_errors_during_drain=%d gets_completed=%d gets_in_window=%d get_errors_in_window=%d get_errors_during_drain=%d", phase, name, cohort.puts, cohort.puts-cohort.putDrained, cohort.putErrorsInWindow, cohort.putErrorsDrained, cohort.gets, cohort.gets-cohort.getDrained, cohort.getErrorsInWindow, cohort.getErrorsDrained)
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
	var attributed, physical uint64
	for _, attribution := range observer.ExtentAttributionSnapshot() {
		attributed += attribution.PhysicalErrorAttempts
		t.Logf("node_archive_extent_attribution owner=%s phase=%s physical_class=%s final_sdk_class=%s guard_outcome=%s operations=%d physical_error_attempts=%d observed_412=%d", attribution.Owner, attribution.Phase, attribution.PhysicalClass, attribution.FinalSDKClass, attribution.GuardOutcome, attribution.Operations, attribution.PhysicalErrorAttempts, attribution.Observed412)
		if !nodeArchiveExtentAttributionAllowed(attribution) {
			t.Errorf("archive extent attribution unresolved: owner=%s phase=%s physical_class=%s final_sdk_class=%s guard_outcome=%s operations=%d physical_error_attempts=%d observed_412=%d", attribution.Owner, attribution.Phase, attribution.PhysicalClass, attribution.FinalSDKClass, attribution.GuardOutcome, attribution.Operations, attribution.PhysicalErrorAttempts, attribution.Observed412)
		}
	}
	for _, aggregate := range aggregates {
		if aggregate.Method == "PUT" && aggregate.ResourceClass == "archive_block" && aggregate.ConditionalPresence == "if_none_match" && aggregate.Outcome == "round_trip_error" {
			physical += aggregate.Count
		}
	}
	t.Logf("node_archive_extent_attribution_coverage physical_error_attempts=%d attributed_error_attempts=%d", physical, attributed)
	if physical != attributed {
		t.Errorf("archive extent attribution incomplete: physical_error_attempts=%d attributed_error_attempts=%d", physical, attributed)
	}
	t.Logf("node_s3_transport_observer attempts=%d request_groups=%d error_classes=%d", total, len(requests), len(aggregates))
	for _, request := range requests {
		t.Logf("node_s3_transport_request_count owner=%s phase=%s method=%s count=%d", request.Owner, request.Phase, request.Method, request.Count)
	}
	for _, aggregate := range aggregates {
		t.Logf("node_s3_transport_error_count owner=%s phase=%s method=%s status_family=%s outcome=%s error_class=%s resource_class=%s conditional_presence=%s count=%d", aggregate.Owner, aggregate.Phase, aggregate.Method, aggregate.StatusFamily, aggregate.Outcome, aggregate.ErrorClass, aggregate.ResourceClass, aggregate.ConditionalPresence, aggregate.Count)
	}
	if first != nil {
		t.Logf("node_s3_transport_first_failure sequence=%d owner=%s phase=%s method=%s status=%d outcome=%s error_class=%s resource_class=%s conditional_presence=%s declared_content_length=%d elapsed=%s", first.Sequence, first.Owner, first.Phase, first.Method, first.StatusCode, first.Outcome, first.ErrorClass, first.ResourceClass, first.ConditionalPresence, first.DeclaredContentLength, first.Elapsed)
	}
}

func nodeArchiveExtentAttributionAllowed(row objmetrics.ExtentUploadAttributionAggregate) bool {
	if row.Operations == 0 || row.PhysicalErrorAttempts == 0 {
		return false
	}
	switch row.Owner {
	case "n1", "n2", "n3":
	default:
		return false
	}
	switch row.Phase {
	case "setup", "seed", "warmup", "measurement", "cleanup_preparation",
		"checkpoint-active_overlap", "archive-cleanup-active_overlap",
		"pre_shutdown_n1_observation", "pre_shutdown_n2_observation", "pre_shutdown_n3_observation",
		"shutdown_n1", "shutdown_n2", "shutdown_n3":
	default:
		return false
	}
	switch row.PhysicalClass {
	case "broken_pipe", "connection_reset", "other", "mixed":
	default:
		return false
	}
	switch row.FinalSDKClass {
	case "success":
		return row.GuardOutcome == "not_attempted"
	case "typed_condition", "broken_pipe":
		return row.GuardOutcome == "verified"
	default:
		return false
	}
}

func TestNodeArchiveExtentAttributionRowGate(t *testing.T) {
	base := objmetrics.ExtentUploadAttributionAggregate{
		Owner: "n3", Phase: "cleanup_preparation", PhysicalClass: "broken_pipe",
		FinalSDKClass: "typed_condition", GuardOutcome: "verified", Operations: 1,
		PhysicalErrorAttempts: 1, Observed412: 1,
	}
	for _, tc := range []struct {
		name string
		edit func(*objmetrics.ExtentUploadAttributionAggregate)
		want bool
	}{
		{name: "typed_condition_verified", want: true},
		{name: "epipe_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.FinalSDKClass = "broken_pipe" }, want: true},
		{name: "success", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) {
			r.FinalSDKClass, r.GuardOutcome = "success", "not_attempted"
		}, want: true},
		{name: "other_physical_but_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.PhysicalClass = "other" }, want: true},
		{name: "mixed_physical_but_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.PhysicalClass = "mixed" }, want: true},
		{name: "unknown_class", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.PhysicalClass = "unknown" }},
		{name: "missing_owner", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Owner = "" }},
		{name: "unrecognized_owner", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Owner = "n4" }},
		{name: "missing_phase", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Phase = "" }},
		{name: "unrecognized_phase", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Phase = "other_phase" }},
		{name: "zero_operations", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Operations = 0 }},
		{name: "zero_physical_attempts", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.PhysicalErrorAttempts = 0 }},
		{name: "typed_unverified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.GuardOutcome = "unverified" }},
		{name: "typed_context_done", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.GuardOutcome = "context_done" }},
		{name: "typed_no_guard", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.GuardOutcome = "not_attempted" }},
		{name: "reset_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.FinalSDKClass = "connection_reset" }},
		{name: "other_final_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.FinalSDKClass = "other_error" }},
		{name: "success_unverified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) {
			r.FinalSDKClass, r.GuardOutcome = "success", "unverified"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			if tc.edit != nil {
				tc.edit(&row)
			}
			if got := nodeArchiveExtentAttributionAllowed(row); got != tc.want {
				t.Fatalf("row=%+v allowed=%t, want %t", row, got, tc.want)
			}
		})
	}
}
