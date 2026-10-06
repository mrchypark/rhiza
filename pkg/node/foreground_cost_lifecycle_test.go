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
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const (
	nodeForegroundWorkers              = 16
	nodeForegroundKeys                 = 4096
	nodeForegroundValueBytes           = 1024
	nodeForegroundWarmup               = 30 * time.Second
	nodeForegroundMeasure              = 120 * time.Second
	nodeForegroundCallTimeout          = 30 * time.Second
	nodeForegroundDrainReserve         = 2 * nodeForegroundCallTimeout
	nodeForegroundStartupSeedAllowance = 2 * time.Minute
	nodeForegroundPrepareLimit         = 10 * time.Second
	nodeForegroundShutdownLimit        = 10 * time.Second // mirrors Node.Shutdown's archive/checkpoint context
	nodeForegroundGatewayClose         = 7 * time.Second
	nodeForegroundCleanupSlack         = 5 * time.Second
	nodeForegroundGCGracePeriod        = 24 * time.Hour
	// Bounded recovery-pin lifecycle table. Eight keys, eight ordinary rows per
	// key, and two reserved close rows per key are fixed; nothing grows.
	nodeForegroundRecoveryPinKeyLimit      = 8
	nodeForegroundRecoveryPinEventLimit    = 8
	nodeForegroundRecoveryPinReservedClose = 2
	nodeForegroundRecoveryPinTerminalLimit = 16
	// nodeForegroundRecoveryPinGuardLimit bounds the distinct positive active-pin
	// guard reads retained for one maintenance operation. It is a fixed capacity,
	// not a history size: repeats of an already-retained hash are deduplicated and
	// a further distinct hash is recorded as a drop rather than displacing an
	// already-retained binding.
	nodeForegroundRecoveryPinGuardLimit = 4
	nodeForegroundClaimLifecycleLimit   = 64
	nodeForegroundClaimReleaseLimit     = 4
	nodeForegroundP99MinSamples         = 10000
	// Planning reserve for three per-node preparations, their bounded archive/
	// checkpoint shutdown operations, gateway close, and small accounting slack.
	// It is not a hard upper bound for Checkpointer/Node worker joins; the outer
	// test deadline remains the final cap.
	nodeForegroundCleanup = 3*(nodeForegroundPrepareLimit+nodeForegroundShutdownLimit) + nodeForegroundGatewayClose + nodeForegroundCleanupSlack
)

func nodeForegroundBudgetAvailable(deadline, now time.Time, required time.Duration) bool {
	return required >= 0 && deadline.Sub(now) >= required
}

func nodeForegroundPreparationBridge(preparationCtx context.Context, outerCancel context.CancelFunc) func() bool {
	return context.AfterFunc(preparationCtx, outerCancel)
}

func nodeForegroundPreparationFinished(preparationCtx context.Context, deadline, now time.Time, stop func() bool) bool {
	stopped := stop()
	return stopped && preparationCtx.Err() == nil && !now.After(deadline)
}

func requireNodeForegroundBudget(t testing.TB, deadline time.Time, required time.Duration, phase string) {
	t.Helper()
	now := time.Now()
	if !nodeForegroundBudgetAvailable(deadline, now, required) {
		t.Fatalf("insufficient Node foreground budget at %s: remaining=%s required=%s", phase, deadline.Sub(now), required)
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
		t.Fatal("test runner must supply the fixed seven-minute deadline")
	}
	const reserve = nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup
	preparationStart := time.Now()
	preparationDeadline := preparationStart.Add(nodeForegroundStartupSeedAllowance)
	if !nodeForegroundBudgetAvailable(testDeadline, preparationStart, nodeForegroundStartupSeedAllowance+reserve) || preparationDeadline.After(testDeadline.Add(-reserve)) {
		t.Fatalf("insufficient Node foreground preparation and fixed-window budget before startup: remaining=%s preparation=%s reserve=%s", testDeadline.Sub(preparationStart), nodeForegroundStartupSeedAllowance, reserve)
	}
	ctx, cancel := context.WithDeadline(context.Background(), testDeadline)
	t.Cleanup(cancel)
	preparationCtx, preparationCancel := context.WithDeadline(ctx, preparationDeadline)
	stopPreparationBridge := nodeForegroundPreparationBridge(preparationCtx, cancel)
	defer func() { _ = stopPreparationBridge(); preparationCancel() }()
	var seedAPIAcknowledged atomic.Uint64
	var preparationEnd time.Time
	var preparationCompleted bool
	t.Logf("node_foreground_preparation_budget outer_deadline=%s start=%s deadline=%s allowance=%s fixed_post_seed_reserve=%s entry_remaining=%s", testDeadline.UTC().Format(time.RFC3339Nano), preparationStart.UTC().Format(time.RFC3339Nano), preparationDeadline.UTC().Format(time.RFC3339Nano), nodeForegroundStartupSeedAllowance, reserve, testDeadline.Sub(preparationStart))
	defer func() {
		end := preparationEnd
		if end.IsZero() {
			end = time.Now()
		}
		t.Logf("node_foreground_preparation_result start=%s end=%s elapsed=%s deadline=%s completed=%t deadline_exceeded=%t context_state_at_exit=%v outer_error=%v api_acknowledged=%d/%d", preparationStart.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano), end.Sub(preparationStart), preparationDeadline.UTC().Format(time.RFC3339Nano), preparationCompleted, end.After(preparationDeadline), preparationCtx.Err(), ctx.Err(), seedAPIAcknowledged.Load(), nodeForegroundKeys)
	}()

	gateway, err := versityfixture.Start(preparationCtx, binary, t.TempDir())
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
	if err := client.MakeBucket(preparationCtx, bucketName, minio.MakeBucketOptions{Region: region}); err != nil {
		t.Fatal(err)
	}
	setupStartRequests, err := nodeVersityRequestCount(gateway)
	if err != nil {
		t.Fatalf("read Versity setup baseline: %v", err)
	}
	ctx = objmetrics.WithReplayObservation(ctx)
	// The recovery pin observer is installed on the outer context before any
	// Node.Open, so startup and catch-up pin lifecycles are observable and not
	// only the maintenance path. It is a fixed-size nonblocking collector.
	recoveryPinCollector := &nodeForegroundRecoveryPinCollector{}
	ctx = localtesthooks.WithRecoveryPinTrace(ctx, recoveryPinCollector.observe)
	claimCollector := &nodeForegroundClaimLifecycleCollector{}
	ctx = checkpoint.WithPublisherBusyObserver(ctx, claimCollector.observe)
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
		// Emit one more bounded pin snapshot after the reverse shutdown loop so
		// shutdown-time catch-up close ACKs (which carry the observer onto a
		// fresh context) remain visible for live-vs-leak diagnosis. The
		// immutable first-Busy copy and the body-level final_lifecycle snapshot
		// are unchanged; absence of a close row in any earlier scope stays
		// unknown rather than evidence of a leak. No I/O, capacity, deadline, or
		// shutdown behavior changes.
		postShutdown := recoveryPinCollector.snapshot()
		postShutdown.scope = "post_shutdown"
		postShutdown.log(t)
		claimCollector.log(t, "post_shutdown")
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
	if err := waitForNodeReadiness(preparationCtx, nodes); err != nil {
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

	if !time.Now().Before(preparationDeadline) {
		t.Fatalf("Node startup exhausted named preparation allowance before seed: now=%s preparation_deadline=%s reserve=%s", time.Now().UTC().Format(time.RFC3339Nano), preparationDeadline.UTC().Format(time.RFC3339Nano), reserve)
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
	seedCtx, seedCancel := context.WithDeadline(ctx, preparationDeadline)
	transportObserver.SetPhase("seed")
	seedStoreStart := snapshotNodeStoreStats(nodes)
	seedServerStart, seedServerStartErr := nodeVersityRequestCount(gateway)
	seedStart := time.Now()
	t.Logf("node_foreground_seed_budget test_deadline=%s seed_deadline=%s reserve=%s", testDeadline.UTC().Format(time.RFC3339Nano), preparationDeadline.UTC().Format(time.RFC3339Nano), reserve)
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
	preparationEnd = seedEnd
	t.Logf("node_foreground_seed_result duration=%s api_acknowledged=%d/%d error=%t", seedEnd.Sub(seedStart), seedAPIAcknowledged.Load(), nodeForegroundKeys, seedErr != nil)
	seedCancel()
	if seedErr != nil {
		t.Fatalf("seed the fixed %d-key Node API workload: %v", nodeForegroundKeys, seedErr)
	}
	if acknowledged := seedAPIAcknowledged.Load(); acknowledged != nodeForegroundKeys {
		t.Fatalf("seed API acknowledgments=%d, want %d", acknowledged, nodeForegroundKeys)
	}
	seedStoreEnd := snapshotNodeStoreStats(nodes)
	logNodeStoreDelta(t, "seed", ids, seedStoreStart, seedStoreEnd)
	seedEndRequests, seedServerEndErr := nodeVersityRequestCount(gateway)
	logVersityRequestDelta(t, "seed", seedServerStart, seedServerStartErr, seedEndRequests, seedServerEndErr)
	t.Logf("node_foreground_seed_logical_puts=%d node_foreground_seed_payload_bytes_api_acknowledged=%d", nodeForegroundKeys, nodeForegroundKeys*nodeForegroundValueBytes)
	if scenario == "archive-cleanup-active" {
		transportObserver.SetPhase("certified_gc_preparation")
		certifiedStart := time.Now()
		certifiedErr := prepareNodeForegroundCertifiedGC(preparationCtx, nodes[0], t.TempDir())
		preparationEnd = time.Now()
		logNodeStoreDelta(t, "certified_gc_preparation", ids, seedStoreEnd, snapshotNodeStoreStats(nodes))
		preparationServerEnd, preparationServerEndErr := nodeVersityRequestCount(gateway)
		logVersityRequestDelta(t, "certified_gc_preparation", seedEndRequests, seedServerEndErr, preparationServerEnd, preparationServerEndErr)
		t.Logf("node_foreground_certified_gc_preparation elapsed=%s error=%t", preparationEnd.Sub(certifiedStart), certifiedErr != nil)
		if certifiedErr != nil {
			t.Fatalf("prepare certified archive coverage before GC measurement: %v", certifiedErr)
		}
	}
	preparationEnd = time.Now()
	if !nodeForegroundPreparationFinished(preparationCtx, preparationDeadline, preparationEnd, stopPreparationBridge) {
		cancel()
		t.Fatalf("Node foreground preparation did not finish within its named allowance: end=%s deadline=%s preparation_error=%v", preparationEnd.UTC().Format(time.RFC3339Nano), preparationDeadline.UTC().Format(time.RFC3339Nano), preparationCtx.Err())
	}
	preparationCompleted = true
	preparationCancel()

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
	var publisherBusyOperationID atomic.Uint64
	var publisherBusyFailures nodeForegroundPublisherBusyFailures
	var archiveBusyFailures nodeForegroundArchiveBusyFailures
	if scenario != "baseline" {
		if scenario == "archive-cleanup-active" {
			archivePhaseTrace = newNodeArchiveGCPhaseTrace()
		}
		operation := func(operationCtx context.Context) (nodeForegroundMaintenanceResult, error) {
			operationID := publisherBusyOperationID.Add(1)
			collector := &nodeForegroundPublisherBusyCollector{}
			observePublisher := func(event checkpoint.PublisherBusyObservation) {
				claimCollector.observe(event)
				if event.Source == "publisher_claim_active" || event.Source == "publisher_claim_conditional_exhausted" || event.Source == "generation_claim_active" || event.Source == "generation_claim_conditional_exhausted" {
					collector.observe(event)
				}
			}
			operationCtx = checkpoint.WithPublisherBusyObserver(operationCtx, observePublisher)
			archiveCollector := &nodeForegroundArchiveBusyCollector{}
			guardCollector := &nodeForegroundRecoveryPinGuardCollector{operationID: operationID}
			if scenario == "checkpoint-active" {
				operationCtx = localtesthooks.WithArchiveBusyTrace(operationCtx, archiveCollector.observe)
				// The positive active-pin guard reads are emitted on the same
				// per-operation context as the archive Busy attribution, so binding
				// them needs no production field. The fan-out delivers every row to
				// the existing global lifecycle collector first, so no current
				// global row, close row, or terminal row is lost or changed.
				operationCtx = localtesthooks.WithRecoveryPinTrace(operationCtx,
					func(event localtesthooks.RecoveryPinEvent) {
						recoveryPinCollector.observe(event)
						guardCollector.observe(event)
					})
			}
			var result nodeForegroundMaintenanceResult
			var err error
			switch scenario {
			case "checkpoint-active":
				result, err = runNodeForegroundCheckpoint(t, operationCtx, nodes[0], maintenanceStateDir, operationID, observePublisher)
			case "archive-cleanup-active":
				result, err = runNodeForegroundArchiveCleanup(t, operationCtx, nodes[0], maintenanceStateDir, archivePhaseTrace)
			default:
				err = fmt.Errorf("unsupported maintenance scenario %q", scenario)
			}
			if errors.Is(err, checkpoint.ErrPublisherBusy) {
				events, overflow := collector.snapshot()
				publisherBusyFailures.record(operationID, events, overflow)
				claimCollector.latchFailure(operationID, events)
			}
			if errors.Is(err, recovery.ErrArchiveBusy) {
				// latchFailure captures the pin table at the first Busy before the
				// pinned guard key is released, so the first-Busy evidence is
				// immutable while later close/expiry rows stay observable in the
				// final lifecycle snapshot.
				snapshot := archiveCollector.snapshot()
				// Capture both snapshots before latching, so the per-operation
				// guard binding and the archive Busy rows are read while the pinned
				// guard key is still held, and latchFailure stores plain copies
				// instead of re-reading live collector state.
				guard := guardCollector.snapshot()
				recoveryPinCollector.latchFailure(guard)
				archiveBusyFailures.record(operationID, snapshot)
			}
			return result, err
		}
		maintenance = newNodeForegroundMaintenance(ctx, scenario, transportObserver, operation)
		t.Cleanup(maintenance.stopAndWait)
	}
	transportObserver.SetPhase("measurement")
	overloadSites := &nodeForegroundOverloadSiteCollector{}
	restoreOverloadSites := localtesthooks.Set(overloadSites.observe)
	t.Cleanup(restoreOverloadSites)
	batchAdmission := &nodeForegroundBatchAdmissionCollector{}
	restoreBatchAdmission := localtesthooks.SetBatchAdmission(batchAdmission.observe)
	var restoreBatchAdmissionOnce sync.Once
	restoreBatchAdmissionSafely := func() { restoreBatchAdmissionOnce.Do(restoreBatchAdmission) }
	t.Cleanup(restoreBatchAdmissionSafely)
	observeMeasured := func(observation foregroundcosttest.Observation) {
		metrics.observe(observation)
		if maintenance != nil {
			maintenance.start(observation.Cutoff)
		}
	}
	measuredResult, err := foregroundcosttest.RunWindow(ctx, options(nodeForegroundMeasure, maintenance), put, get, observeMeasured, nil)
	// RunWindow closes its gate and joins every admitted worker before returning,
	// so this restore excludes preparation and warmup while retaining completion
	// events through the measurement drain.
	restoreBatchAdmissionSafely()
	restoreOverloadSites()
	overloadSites.log(t)
	batchAdmission.log(t, measuredResult, metrics)
	if maintenance != nil {
		maintenance.stopAndWait()
		transportObserver.SetPhase("measurement")
		maintenance.log(t, nodeForegroundMeasure)
		claimCollector.log(t, "measurement_terminal")
		if archivePhaseTrace != nil {
			archivePhaseTrace.log(t)
		}
		// The pin lifecycle table covers the whole run, including pins created
		// before Node.Open, so it is logged next to the terminal archive Busy
		// attribution rather than only on failure.
		recoveryPinCollector.snapshot().log(t)
		if firstBusy := recoveryPinCollector.firstBusySnapshot(); firstBusy != nil {
			t.Log("node_foreground_recovery_pin scope=first_busy (immutable copy captured before guard unpin)")
			firstBusy.log(t)
		} else {
			t.Log("node_foreground_recovery_pin scope=first_busy snapshot=absent")
		}
		if firstErr := maintenance.firstError(); firstErr != nil {
			types, status := nodeForegroundMaintenanceErrorTypeChain(firstErr)
			t.Logf("node_foreground_maintenance_error_type_chain status=%s types=%v", status, types)
			if errors.Is(firstErr, checkpoint.ErrPublisherBusy) {
				publisherBusyFailures.log(t)
			}
			if errors.Is(firstErr, recovery.ErrArchiveBusy) {
				archiveBusyFailures.log(t)
			}
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

func TestNodeForegroundPublisherBusyCollectorBoundsConcurrentEvents(t *testing.T) {
	collector := &nodeForegroundPublisherBusyCollector{}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			collector.observe(checkpoint.PublisherBusyObservation{Source: "test", Attempt: i + 1})
		}(i)
	}
	group.Wait()
	events, overflow := collector.snapshot()
	if len(events) != nodeForegroundPublisherBusyEventLimit || overflow != 4 {
		t.Fatalf("events=%d overflow=%d", len(events), overflow)
	}

	failures := &nodeForegroundPublisherBusyFailures{}
	failures.record(2, events, overflow)
	failures.record(3, []checkpoint.PublisherBusyObservation{{Source: "later"}}, 0)
	if failures.failure == nil || failures.failure.operationID != 2 || len(failures.failure.events) != nodeForegroundPublisherBusyEventLimit || failures.failure.overflow != 4 {
		t.Fatalf("failure=%+v", failures.failure)
	}
}

func TestNodeForegroundClaimLifecycleCollectorJoinsOnlyExactConfirmedVersion(t *testing.T) {
	collector := &nodeForegroundClaimLifecycleCollector{}
	var namespace, version [32]byte
	namespace[0], version[0] = 1, 2
	holder := checkpoint.PublisherBusyObservation{Source: "acquire_confirmed", Purpose: "maintenance", HolderCategory: "node_catchup", Generation: 7, NamespaceDigest: namespace, VersionDigest: version, VersionPresent: true}
	collector.observe(holder)
	guard := checkpoint.PublisherBusyObservation{Source: "publisher_claim_active", ActiveClaim: true, Purpose: "maintenance", Generation: 7, NamespaceDigest: namespace, VersionDigest: version, VersionPresent: true}
	collector.observe(guard)
	collector.latchFailure(9, []checkpoint.PublisherBusyObservation{guard})
	if collector.guard == nil || collector.holder == nil || collector.holder.HolderCategory != "node_catchup" || collector.operationID != 9 {
		t.Fatalf("confirmed holder was not joined: %+v", collector)
	}
	collector.observe(checkpoint.PublisherBusyObservation{Source: "release_upload_ack", Purpose: "maintenance", Generation: 7, NamespaceDigest: namespace})
	if len(collector.releases) != 1 {
		t.Fatalf("release rows=%d, want 1", len(collector.releases))
	}
	for i := 0; i < nodeForegroundClaimLifecycleLimit+3; i++ {
		collector.observe(checkpoint.PublisherBusyObservation{Source: "acquire_upload_ack", Generation: uint64(i + 20)})
	}
	if collector.dropped != 6 || collector.guard == nil || collector.holder == nil {
		t.Fatalf("bounded ring lost immutable first failure: dropped=%d guard=%v holder=%v", collector.dropped, collector.guard, collector.holder)
	}
	unknown := &nodeForegroundClaimLifecycleCollector{}
	wrong := holder
	wrong.VersionDigest[0]++
	unknown.observe(wrong)
	unknown.latchFailure(1, []checkpoint.PublisherBusyObservation{guard})
	if unknown.holder != nil || unknown.guard == nil {
		t.Fatalf("mismatched version was falsely joined: %+v", unknown)
	}
}

// The recovery pin table is fixed and bounded: ordinary rows stop at the
// per-key limit, close rows keep their own reserved budget, and a key beyond
// the table is counted as unknown instead of growing.
func TestNodeForegroundRecoveryPinCollectorBoundsRowsAndKeys(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	for i := 0; i < nodeForegroundRecoveryPinEventLimit+4; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "key-a", Phase: localtesthooks.RecoveryPinCreateAttempt})
	}
	snapshot := collector.snapshot()
	if len(snapshot.hashes) != 1 || snapshot.counts[0] != nodeForegroundRecoveryPinEventLimit {
		t.Fatalf("ordinary rows: keys=%d count=%d, want 1/%d", len(snapshot.hashes), snapshot.counts[0], nodeForegroundRecoveryPinEventLimit)
	}
	if snapshot.eventOverflow != 4 {
		t.Fatalf("ordinary overflow=%d, want 4", snapshot.eventOverflow)
	}

	// Close rows use their own reserved budget and are never displaced by the
	// full ordinary budget.
	for i := 0; i < nodeForegroundRecoveryPinReservedClose+3; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "key-a", Phase: localtesthooks.RecoveryPinCloseAttempt})
	}
	snapshot = collector.snapshot()
	if snapshot.closeCounts[0] != nodeForegroundRecoveryPinReservedClose || snapshot.closeOverflow != 3 {
		t.Fatalf("close rows=%d overflow=%d, want %d/3", snapshot.closeCounts[0], snapshot.closeOverflow, nodeForegroundRecoveryPinReservedClose)
	}

	// A key beyond the fixed table replaces the oldest slot; the table never
	// grows and the displaced join is reported as unknown.
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
		// Ordinary rows only: a dropped decision row is covered separately so the
		// terminal_missing contract is not conflated with key-table overflow.
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("key-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	snapshot = collector.snapshot()
	if len(snapshot.hashes) > nodeForegroundRecoveryPinKeyLimit {
		t.Fatalf("tracked keys=%d exceed the fixed limit %d", len(snapshot.hashes), nodeForegroundRecoveryPinKeyLimit)
	}
	if snapshot.keyEvicted == 0 || !snapshot.joinUnknown {
		t.Fatalf("a displaced key must be counted and joined as unknown: %+v", snapshot)
	}
	if snapshot.keyMiss != 0 {
		t.Fatalf("a replaceable key must not be counted as a miss: %d", snapshot.keyMiss)
	}
	if snapshot.termMiss {
		t.Fatalf("terminal missing must not be reported before the fixed terminal budget is exceeded")
	}
}

// The fixed table replaces the oldest key rather than freezing the first eight
// forever: the ninth key displaces the oldest slot, the pinned guard key is
// retained until a failure is latched, and a displaced join stays explicitly
// unknown.
func TestNodeForegroundRecoveryPinCollectorReplacesOldestKey(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("fill-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "ninth", Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	snapshot := collector.snapshot()
	if len(snapshot.hashes) != nodeForegroundRecoveryPinKeyLimit {
		t.Fatalf("tracked keys=%d, want %d", len(snapshot.hashes), nodeForegroundRecoveryPinKeyLimit)
	}
	if containsRecoveryPinHash(snapshot.hashes, "fill-00") {
		t.Fatalf("the oldest key must be replaced: %v", snapshot.hashes)
	}
	if !containsRecoveryPinHash(snapshot.hashes, "ninth") {
		t.Fatalf("the newest key must be retained: %v", snapshot.hashes)
	}
	if snapshot.keyEvicted != 1 || !snapshot.joinUnknown {
		t.Fatalf("replacement must count an eviction and an unknown join: %+v", snapshot)
	}
	if snapshot.termMiss {
		t.Fatalf("a replaced ordinary key must not fabricate a missing terminal")
	}
}

func containsRecoveryPinHash(hashes []string, want string) bool {
	for _, hash := range hashes {
		if hash == want {
			return true
		}
	}
	return false
}

// The guard key seen by the first guard read with a live lease is pinned: later
// keys replace other slots, and the guard identity is never overwritten by a
// replacement.
func TestNodeForegroundRecoveryPinCollectorRetainsGuardKey(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 5000})
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit*2; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("other-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	snapshot := collector.snapshot()
	if !containsRecoveryPinHash(snapshot.hashes, "guard-key") {
		t.Fatalf("the pinned guard key must survive replacement: %v", snapshot.hashes)
	}
	if snapshot.termMiss {
		t.Fatalf("the retained guard row must stay attributable: %+v", snapshot)
	}
	if snapshot.keyMiss != 0 {
		t.Fatalf("replacement must keep admitting keys: %d", snapshot.keyMiss)
	}
}

// An expired (or zero-delta) guard row describes a healthy released pin, so it
// must not pin its key; the later guard read on the live lease is the key that
// gets retained.
func TestNodeForegroundRecoveryPinCollectorExpiredGuardDoesNotPinKey(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "expired-startup", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseExpired, LeaseDeltaMS: -1000})
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "zero-lease", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseZero, LeaseDeltaMS: 0})
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "active-guard", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 1000})
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("churn-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	snapshot := collector.snapshot()
	if !containsRecoveryPinHash(snapshot.hashes, "active-guard") {
		t.Fatalf("the live-lease guard key must be retained: %v", snapshot.hashes)
	}
	if containsRecoveryPinHash(snapshot.hashes, "expired-startup") {
		t.Fatalf("an expired guard key must not be pinned: %v", snapshot.hashes)
	}
}

// After a failure is latched the guard key is no longer pinned, so ordinary
// replacement ordering resumes.
func TestNodeForegroundRecoveryPinCollectorGuardPinReleasedAfterFailure(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 5000})
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{})
	if collector.latchCount != 1 {
		t.Fatalf("latchCount=%d, want 1 after the first latch", collector.latchCount)
	}
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit*2; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("post-failure-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	if containsRecoveryPinHash(collector.snapshot().hashes, "guard-key") {
		t.Fatalf("the guard pin must be released once a failure is latched")
	}
}

// The first-Busy copy is immutable: post-latch churn beyond the fixed capacity,
// shutdown rows, and a repeated latch must never mutate or replace it, while the
// final lifecycle snapshot still shows the later close timeline.
func TestNodeForegroundRecoveryPinCollectorFirstBusySnapshotImmutable(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "cause-key", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 5000})
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "cause-key", Phase: localtesthooks.RecoveryPinCreateConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 4000})
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{})
	first := collector.firstBusySnapshot()
	if first == nil {
		t.Fatalf("the first Busy must capture a pin snapshot")
	}
	if first.scope != "first_busy" {
		t.Fatalf("scope=%q, want first_busy", first.scope)
	}
	if !containsRecoveryPinHash(first.hashes, "cause-key") {
		t.Fatalf("the first-Busy copy must retain the cause key: %v", first.hashes)
	}
	// Post-latch churn plus a shutdown close row for the cause key.
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit*3; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("post-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "cause-key", Phase: localtesthooks.RecoveryPinCloseConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	// A repeated latch must not overwrite the first capture.
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{})

	again := collector.firstBusySnapshot()
	if len(again.hashes) != len(first.hashes) || len(again.hashes) > nodeForegroundRecoveryPinKeyLimit {
		t.Fatalf("first-Busy copy mutated: %v vs %v", first.hashes, again.hashes)
	}
	if !containsRecoveryPinHash(again.hashes, "cause-key") {
		t.Fatalf("the first-Busy cause key must survive post-latch eviction: %v", again.hashes)
	}
	for i := range again.counts {
		if again.counts[i] != first.counts[i] || again.closeCounts[i] != first.closeCounts[i] {
			t.Fatalf("first-Busy row counts mutated: before=%v/%v after=%v/%v", first.counts, first.closeCounts, again.counts, again.closeCounts)
		}
	}
	if first.keyEvicted != 0 || first.joinUnknown {
		t.Fatalf("the first-Busy copy must record no later drops: evicted=%d unknown=%t", first.keyEvicted, first.joinUnknown)
	}
	// The final lifecycle snapshot is separate and still shows later activity.
	final := collector.snapshot()
	if final.keyEvicted == 0 {
		t.Fatalf("the final snapshot must record post-latch evictions")
	}
}

// Cleanup ordering: a body-level snapshot taken before shutdown cannot contain
// the shutdown-time close ACK, while the post-shutdown snapshot can. This is the
// deterministic form of the fixture's cleanup ordering (reverse Shutdown loop
// inside t.Cleanup runs after the body-level log), with no provider involved.
func TestNodeForegroundRecoveryPinPostShutdownSnapshotSeesShutdownClose(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "catchup-key", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 5000})
	// The Busy happens before shutdown, so the first-Busy copy is latched here.
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{})
	bodyScope := collector.snapshot()
	// Simulate the shutdown-time catch-up close that carries the observer onto a
	// fresh context inside Node.Shutdown.
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "catchup-key", Phase: localtesthooks.RecoveryPinCloseConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerNodeCatchup})
	postScope := collector.snapshot()
	if postScope.closeCounts[0] != 1 {
		t.Fatalf("post-shutdown snapshot must contain the shutdown close row: %+v", postScope.closeCounts)
	}
	if bodyScope.closeCounts[0] != 0 {
		t.Fatalf("the earlier body-level snapshot must not retroactively contain the shutdown close row")
	}
	if first := collector.firstBusySnapshot(); first == nil || first.closeCounts[0] != 0 {
		t.Fatalf("first-Busy copy must stay independent of later shutdown rows")
	}
}

// An empty table still produces one finite summary marker per scope. The
// summary stays observational: zero keys/rows means unknown, never "no pin",
// "leak", or "close acknowledged".
func TestNodeForegroundRecoveryPinEmptySnapshotLogsFiniteSummary(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	for _, scope := range []string{"final_lifecycle", "post_shutdown"} {
		snapshot := collector.snapshot()
		if len(snapshot.hashes) != 0 {
			t.Fatalf("scope=%s: an unobserved table must stay empty: %v", scope, snapshot.hashes)
		}
		if snapshot.keyMiss != 0 || snapshot.keyEvicted != 0 || snapshot.joinUnknown || snapshot.termMiss {
			t.Fatalf("scope=%s: an unobserved table must not fabricate drops: %+v", scope, snapshot)
		}
		snapshot.scope = scope
		// The unconditional summary line is emitted before any row loop, so an
		// empty snapshot still logs exactly one marker for this scope.
		snapshot.log(t)
	}
}

// A full table no longer rejects a key: the decision row for the replacing key
// is still retained in both orderings, so a reader never mistakes the displaced
// ordinary rows for the cause, while the displaced join stays unknown.
func TestNodeForegroundRecoveryPinCollectorKeyFullRetainsNewDecision(t *testing.T) {
	for _, ordering := range []struct {
		name  string
		order func(*nodeForegroundRecoveryPinCollector)
	}{
		{
			name: "ordinary_before_decision",
			order: func(c *nodeForegroundRecoveryPinCollector) {
				c.observe(localtesthooks.RecoveryPinEvent{KeyHash: "replacing-key", Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
				c.observe(localtesthooks.RecoveryPinEvent{KeyHash: "replacing-key", Phase: localtesthooks.RecoveryPinCloseConflict, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
			},
		},
		{
			name: "decision_before_ordinary",
			order: func(c *nodeForegroundRecoveryPinCollector) {
				c.observe(localtesthooks.RecoveryPinEvent{KeyHash: "replacing-key", Phase: localtesthooks.RecoveryPinCloseConflict, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
				c.observe(localtesthooks.RecoveryPinEvent{KeyHash: "replacing-key", Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
			},
		},
	} {
		t.Run(ordering.name, func(t *testing.T) {
			collector := &nodeForegroundRecoveryPinCollector{}
			for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
				collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: fmt.Sprintf("fill-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
			}
			ordering.order(collector)
			snapshot := collector.snapshot()
			if len(snapshot.hashes) != nodeForegroundRecoveryPinKeyLimit {
				t.Fatalf("tracked keys=%d, want the fixed limit %d", len(snapshot.hashes), nodeForegroundRecoveryPinKeyLimit)
			}
			if !containsRecoveryPinHash(snapshot.hashes, "replacing-key") {
				t.Fatalf("the replacing key must be admitted: %v", snapshot.hashes)
			}
			if snapshot.keyMiss != 0 {
				t.Fatalf("replacement must not reject a key: key_miss=%d", snapshot.keyMiss)
			}
			if snapshot.termMiss {
				t.Fatalf("a retained decision row must not report terminal_missing: %+v", snapshot)
			}
			if snapshot.keyEvicted == 0 || !snapshot.joinUnknown {
				t.Fatalf("the displaced join must stay explicitly unknown: %+v", snapshot)
			}
		})
	}
	// A key that stays within the table must not be reported as a missing
	// terminal, so the flag never becomes a permanent false positive.
	control := &nodeForegroundRecoveryPinCollector{}
	control.observe(localtesthooks.RecoveryPinEvent{KeyHash: "single", Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup})
	control.observe(localtesthooks.RecoveryPinEvent{KeyHash: "single", Phase: localtesthooks.RecoveryPinCloseConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
	if control.snapshot().termMiss {
		t.Fatalf("a retained decision row must not report terminal_missing")
	}
}

// The fixed decision budget is exhausted in order, and the drop is reported
// explicitly instead of silently losing the terminal cause.
func TestNodeForegroundRecoveryPinCollectorDecisionBudgetExhaustion(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	for i := 0; i < nodeForegroundRecoveryPinTerminalLimit; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "key-a", Phase: localtesthooks.RecoveryPinCloseConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
	}
	if collector.snapshot().termMiss {
		t.Fatalf("the decision budget must not report a miss before it is exhausted")
	}
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: "key-a", Phase: localtesthooks.RecoveryPinCloseError, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
	snapshot := collector.snapshot()
	if !snapshot.termMiss {
		t.Fatalf("a decision row beyond the fixed budget must set terminal_missing")
	}
	if snapshot.closeOverflow == 0 {
		t.Fatalf("a dropped close row must be counted as an overflow")
	}
	if len(snapshot.terminals) > nodeForegroundRecoveryPinTerminalLimit {
		t.Fatalf("retained decision rows=%d exceed the fixed budget %d", len(snapshot.terminals), nodeForegroundRecoveryPinTerminalLimit)
	}
}

func TestNodeForegroundRecoveryPinCollectorConcurrentObservation(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			collector.observe(localtesthooks.RecoveryPinEvent{
				KeyHash: fmt.Sprintf("key-%d", i%nodeForegroundRecoveryPinKeyLimit),
				Phase:   localtesthooks.RecoveryPinCloseConfirmed,
			})
			_ = collector.snapshot()
		}(i)
	}
	group.Wait()
	snapshot := collector.snapshot()
	if len(snapshot.hashes) > nodeForegroundRecoveryPinKeyLimit {
		t.Fatalf("tracked keys=%d exceed the fixed limit %d", len(snapshot.hashes), nodeForegroundRecoveryPinKeyLimit)
	}
	var closes int
	for _, count := range snapshot.closeCounts {
		if count > nodeForegroundRecoveryPinReservedClose {
			t.Fatalf("close rows=%d exceed the reserved budget %d", count, nodeForegroundRecoveryPinReservedClose)
		}
		closes += count
	}
	if closes == 0 {
		t.Fatalf("concurrent close events were all dropped")
	}
	if snapshot.eventOverflow > 0 && closes == 0 {
		t.Fatalf("overflow recorded without any retained row")
	}
}

// The collector keeps only bounded classifications: never the full object key,
// owner string, or token.
// The per-operation guard collector retains at most the fixed capacity of
// DISTINCT positive hashes: a duplicate consumes no slot and is not a drop, and
// a further distinct hash is dropped without displacing a retained binding.
func TestNodeForegroundRecoveryPinGuardCollectorBoundsDistinctHashes(t *testing.T) {
	collector := &nodeForegroundRecoveryPinGuardCollector{}
	positive := func(hash string, observedAt int64) localtesthooks.RecoveryPinEvent {
		return localtesthooks.RecoveryPinEvent{
			KeyHash: hash, Phase: localtesthooks.RecoveryPinGuardRead,
			OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown,
			LeaseState:    localtesthooks.RecoveryPinLeaseActive,
			LeaseDeltaMS:  5000, ObservedAtMS: observedAt,
		}
	}
	collector.observe(positive("guard-a", 100))
	collector.observe(positive("guard-b", 200))
	collector.observe(positive("guard-c", 300))
	collector.observe(positive("guard-d", 350))
	// A duplicate of an already-retained hash must not consume a slot and must
	// not overwrite the first-retained values for that hash.
	collector.observe(positive("guard-a", 999))
	snap := collector.snapshot()
	if len(snap.records) != nodeForegroundRecoveryPinGuardLimit {
		t.Fatalf("retained=%d, want %d distinct hashes", len(snap.records), nodeForegroundRecoveryPinGuardLimit)
	}
	// A fifth distinct hash is dropped; it never displaces a retained binding.
	collector.observe(positive("guard-e", 400))
	snap = collector.snapshot()
	if len(snap.records) != nodeForegroundRecoveryPinGuardLimit {
		t.Fatalf("a rejected distinct hash must not evict a retained binding: retained=%d", len(snap.records))
	}
	if snap.records[0].keyHash != "guard-a" || snap.records[0].observedAtMS != 100 {
		t.Fatalf("the first-retained values must win for a deduplicated hash: %+v", snap.records[0])
	}
	if snap.records[1].keyHash != "guard-b" {
		t.Fatalf("second retained hash=%q, want guard-b", snap.records[1].keyHash)
	}
	if snap.dropped != 1 || !snap.joinUnknown {
		t.Fatalf("dropped=%d joinUnknown=%t, want 1/true", snap.dropped, snap.joinUnknown)
	}
}

// Only a live-lease guard read is a positive binding; an expired-healthy or
// zero-delta guard row is not evidence of an active pin.
func TestNodeForegroundRecoveryPinGuardCollectorOnlyLiveLeaseIsPositive(t *testing.T) {
	collector := &nodeForegroundRecoveryPinGuardCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "expired-key", Phase: localtesthooks.RecoveryPinGuardRead,
		LeaseState: localtesthooks.RecoveryPinLeaseExpired, LeaseDeltaMS: -1000,
	})
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "zero-key", Phase: localtesthooks.RecoveryPinGuardRead,
		LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 0,
	})
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "active-key", Phase: localtesthooks.RecoveryPinGuardRead,
		LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 5000,
	})
	snap := collector.snapshot()
	if len(snap.records) != 1 || snap.records[0].keyHash != "active-key" {
		t.Fatalf("only the live-lease guard read may be a positive binding: %+v", snap.records)
	}
	if snap.dropped != 0 || snap.joinUnknown {
		t.Fatalf("non-positive rows must not count as drops: dropped=%d unknown=%t", snap.dropped, snap.joinUnknown)
	}
}

// The first-Busy copy keeps the per-operation guard binding field-for-field even
// when the global table later overflows and later rows arrive. Guard drop state
// stays independent of the global counters.
func TestNodeForegroundRecoveryPinGuardBindingSurvivesGlobalOverflow(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	guard := &nodeForegroundRecoveryPinGuardCollector{}
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinGuardRead,
		OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown,
		LeaseState:    localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 7000, ObservedAtMS: 111,
	})
	guard.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinGuardRead,
		OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown,
		LeaseState:    localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 7000, ObservedAtMS: 111,
	})
	guardSnapshot := guard.snapshot()
	collector.latchFailure(guardSnapshot)
	// Drive the global table well past its key capacity after the latch.
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit*2; i++ {
		collector.observe(localtesthooks.RecoveryPinEvent{
			KeyHash: fmt.Sprintf("unrelated-%02d", i), Phase: localtesthooks.RecoveryPinCreateAttempt,
			OwnerCategory: localtesthooks.RecoveryPinOwnerStartup,
		})
	}
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinCloseConfirmed,
		OwnerCategory: localtesthooks.RecoveryPinOwnerNodeCatchup,
	})
	first := collector.firstBusySnapshot()
	if first == nil || len(first.guard.records) != 1 {
		t.Fatalf("first-Busy copy must retain its guard binding: %+v", first)
	}
	want := nodeForegroundRecoveryPinGuardRecord{
		keyHash: "guard-key", leaseState: localtesthooks.RecoveryPinLeaseActive,
		leaseDeltaMS: 7000, observedAtMS: 111,
	}
	if first.guard.records[0] != want {
		t.Fatalf("latched guard record=%+v, want %+v", first.guard.records[0], want)
	}
	if first.guard.dropped != 0 || first.guard.joinUnknown {
		t.Fatalf("latched guard drop state must stay independent: %+v", first.guard)
	}
	final := collector.snapshot()
	if final.keyMiss == 0 && final.keyEvicted == 0 && !final.joinUnknown {
		t.Fatalf("the global table must actually record overflow after the latch")
	}
	if final.guard.records != nil {
		t.Fatalf("a non-latched global snapshot must not fabricate a guard binding")
	}
	// A repeated latch must not rewrite the first-Busy evidence.
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{
		records: []nodeForegroundRecoveryPinGuardRecord{{keyHash: "other", leaseState: localtesthooks.RecoveryPinLeaseActive, leaseDeltaMS: 1}},
		dropped: 3, joinUnknown: true,
	})
	again := collector.firstBusySnapshot()
	if len(again.guard.records) != 1 || again.guard.records[0] != want || again.guard.dropped != 0 {
		t.Fatalf("a repeated latch overwrote the first-Busy guard binding: %+v", again.guard)
	}
}

// The guard binding carries the test-side maintenance operation identity that
// created it: a nonzero operation ID is retained in the immutable first-Busy
// snapshot and emitted with every guard line, and a repeated latch can neither
// change it nor replace it.
func TestNodeForegroundRecoveryPinGuardBindingCarriesOperationID(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	guard := &nodeForegroundRecoveryPinGuardCollector{operationID: 42}
	guard.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: "guard-key", Phase: localtesthooks.RecoveryPinGuardRead,
		OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown,
		LeaseState:    localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 4000, ObservedAtMS: 77,
	})
	first := guard.snapshot()
	if first.operationID != 42 {
		t.Fatalf("guard snapshot operation_id=%d, want 42", first.operationID)
	}
	collector.latchFailure(first)
	latched := collector.firstBusySnapshot()
	if latched == nil || latched.guard.operationID != 42 {
		t.Fatalf("the first-Busy binding must retain operation_id=42: %+v", latched)
	}
	// A repeated latch carrying a different identity must not rewrite the
	// first cause or its operation identity.
	collector.latchFailure(nodeForegroundRecoveryPinGuardSnapshot{
		operationID: 99,
		records:     []nodeForegroundRecoveryPinGuardRecord{{keyHash: "other-key", leaseState: localtesthooks.RecoveryPinLeaseActive, leaseDeltaMS: 1}},
		dropped:     2, joinUnknown: true,
	})
	again := collector.firstBusySnapshot()
	if again.guard.operationID != 42 || len(again.guard.records) != 1 || again.guard.records[0].keyHash != "guard-key" || again.guard.dropped != 0 {
		t.Fatalf("a repeated latch rewrote the first-Busy binding: %+v", again.guard)
	}
	// A per-operation collector created without an operation identity stays
	// explicitly zero rather than borrowing another operation's identity.
	if got := (&nodeForegroundRecoveryPinGuardCollector{}).snapshot().operationID; got != 0 {
		t.Fatalf("operation_id=%d, want 0 for a collector without an operation", got)
	}
	// Emit the immutable copy through the real logging path. The identity is
	// asserted above; the emitted operation_id=42 line is verified from the
	// recorded verbose test output rather than through a log-capture shim.
	again.log(t)
}

// The explicit fan-out delivers every row to the existing global collector, so
// installing the per-operation guard observer loses no global evidence.
func TestNodeForegroundRecoveryPinFanOutPreservesGlobalCollector(t *testing.T) {
	global := &nodeForegroundRecoveryPinCollector{}
	guard := &nodeForegroundRecoveryPinGuardCollector{}
	for _, event := range []localtesthooks.RecoveryPinEvent{
		{KeyHash: "shared", Phase: localtesthooks.RecoveryPinCreateAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerStartup},
		{KeyHash: "shared", Phase: localtesthooks.RecoveryPinGuardRead, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown, LeaseState: localtesthooks.RecoveryPinLeaseActive, LeaseDeltaMS: 3000},
		{KeyHash: "shared", Phase: localtesthooks.RecoveryPinCloseConfirmed, OwnerCategory: localtesthooks.RecoveryPinOwnerNodeCatchup},
	} {
		global.observe(event)
		guard.observe(event)
	}
	snap := global.snapshot()
	if len(snap.hashes) != 1 || snap.hashes[0] != "shared" || snap.counts[0] != 2 || snap.closeCounts[0] != 1 {
		t.Fatalf("the fan-out must preserve global rows: hashes=%v counts=%v closes=%v", snap.hashes, snap.counts, snap.closeCounts)
	}
	if len(snap.terminals) != 2 {
		t.Fatalf("terminal rows must remain separate: %+v", snap.terminals)
	}
	if got := guard.snapshot(); len(got.records) != 1 || got.records[0].keyHash != "shared" {
		t.Fatalf("the per-operation guard collector must retain the positive row: %+v", got.records)
	}
}

func TestNodeForegroundRecoveryPinCollectorRetainsOnlyBoundedFields(t *testing.T) {
	collector := &nodeForegroundRecoveryPinCollector{}
	const secretKey = "archive/recovery-pins/deadbeefdeadbeef-deadbeefdeadbeef"
	const secretOwner = "sensitive-owner-identity"
	const secretToken = "sensitive-pin-token"
	// The production side emits only the bounded hash suffix; the collector
	// must never be handed or retain the full object key.
	const boundedHash = "deadbeefdeadbeef"
	collector.observe(localtesthooks.RecoveryPinEvent{
		KeyHash: boundedHash, Phase: localtesthooks.RecoveryPinCreateConfirmed,
		OwnerCategory: localtesthooks.RecoveryPinOwnerStartup, LeaseState: localtesthooks.RecoveryPinLeaseActive,
		LeaseDeltaMS: 1000, ReadStatus: localtesthooks.RecoveryPinReadOK, WriteStatus: localtesthooks.RecoveryPinWriteOK,
		ReadbackStatus: localtesthooks.RecoveryPinReadbackDone,
	})
	// Production always stamps at least guard_unknown, so a captured row never
	// carries an empty category.
	collector.observe(localtesthooks.RecoveryPinEvent{KeyHash: boundedHash, Phase: localtesthooks.RecoveryPinCloseAttempt, OwnerCategory: localtesthooks.RecoveryPinOwnerGuardUnknown})
	snapshot := collector.snapshot()
	if len(snapshot.hashes) != 1 {
		t.Fatalf("keys=%d, want 1", len(snapshot.hashes))
	}
	if strings.Contains(snapshot.hashes[0], "recovery-pins") || strings.Contains(snapshot.hashes[0], secretKey) {
		t.Fatalf("retained key is not bounded: %q", snapshot.hashes[0])
	}
	rows := append([]localtesthooks.RecoveryPinEvent(nil), snapshot.events[0][:snapshot.counts[0]]...)
	rows = append(rows, snapshot.closes[0][:snapshot.closeCounts[0]]...)
	for _, row := range rows {
		if row.OwnerCategory != localtesthooks.RecoveryPinOwnerStartup && row.OwnerCategory != localtesthooks.RecoveryPinOwnerGuardUnknown {
			t.Fatalf("owner category is not a finite classification: %q", row.OwnerCategory)
		}
		if strings.Contains(row.OwnerCategory, secretOwner) || strings.Contains(row.KeyHash, secretToken) {
			t.Fatalf("retained row leaked owner or token material: %+v", row)
		}
	}
}

func TestNodeForegroundArchiveBusyCollectorBoundsConcurrentEvents(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	// Deterministic fill-and-stop: sequential over-capacity events overflow by
	// count only and never overwrite a retained branch.
	for i := 0; i < nodeForegroundArchiveBusyOrdinaryLimit+4; i++ {
		collector.observe(localtesthooks.ArchiveBusyEvent{Operation: "archive-trim", Resource: "gc_lock", Branch: "active_live_lease", Stage: "site"})
	}
	sequential := collector.snapshot()
	if len(sequential.events) != nodeForegroundArchiveBusyOrdinaryLimit || sequential.overflow != 4 {
		t.Fatalf("sequential events=%d overflow=%d, want %d/4", len(sequential.events), sequential.overflow, nodeForegroundArchiveBusyOrdinaryLimit)
	}

	// Concurrent observations must stay within the fixed capacity and safe to
	// snapshot while running.
	concurrent := &nodeForegroundArchiveBusyCollector{}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			concurrent.observe(localtesthooks.ArchiveBusyEvent{Operation: "archive-trim", Resource: "gc_lock", Branch: "active_live_lease", Stage: "site"})
		}()
	}
	group.Wait()
	snapshot := concurrent.snapshot()
	if len(snapshot.events) > nodeForegroundArchiveBusyEventLimit {
		t.Fatalf("concurrent events=%d exceed the fixed capacity %d", len(snapshot.events), nodeForegroundArchiveBusyEventLimit)
	}
	// Fill-and-stop keeps the earliest exact branches; a later event must never
	// overwrite an earlier one.
	for i, event := range snapshot.events {
		if event.Branch != "active_live_lease" || event.Stage != "site" {
			t.Fatalf("event[%d]=%+v, want the retained first branch", i, event)
		}
	}

	failures := &nodeForegroundArchiveBusyFailures{}
	failures.record(2, sequential)
	failures.record(3, nodeForegroundArchiveBusySnapshot{events: []localtesthooks.ArchiveBusyEvent{{Branch: "later"}}})
	if !failures.recorded || failures.operationID != 2 || len(failures.snapshot.events) != nodeForegroundArchiveBusyOrdinaryLimit || failures.snapshot.overflow != 4 {
		t.Fatalf("failure=%+v", failures)
	}
	// The first failing operation wins; a later observation cannot rewrite it.
	if failures.snapshot.events[0].Branch != "active_live_lease" {
		t.Fatalf("first event overwritten: %+v", failures.snapshot.events[0])
	}
}

// The exact post-entry return-path terminal branch must survive a full ordinary
// budget, so the reported cause is never an early pre-entry retry.
func TestNodeForegroundArchiveBusyTerminalSurvivesPreEntryRetries(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	for i := 0; i < nodeForegroundArchiveBusyEventLimit*2; i++ {
		collector.observe(localtesthooks.ArchiveBusyEvent{
			Operation: "archive-trim", Resource: "publication_lock", Branch: "active_live_lease", Stage: "site", Entered: false,
		})
	}
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-trim", Resource: "publication_lock", Branch: "returned", Stage: "terminal", Entered: true,
	})
	snapshot := collector.snapshot()
	if !snapshot.hasTerm || snapshot.termMiss {
		t.Fatalf("terminal not captured: %+v", snapshot)
	}
	if snapshot.terminal.Branch != "returned" || snapshot.terminal.Stage != "terminal" || !snapshot.terminal.Entered {
		t.Fatalf("terminal=%+v, want the exact returned/terminal/entered branch", snapshot.terminal)
	}
	if len(snapshot.events) != nodeForegroundArchiveBusyOrdinaryLimit || snapshot.overflow == 0 {
		t.Fatalf("events=%d overflow=%d, want %d ordinary and a non-zero overflow", len(snapshot.events), snapshot.overflow, nodeForegroundArchiveBusyOrdinaryLimit)
	}
	// Every retained ordinary row is a pre-entry retry and must not claim the cause.
	for i, event := range snapshot.events {
		if event.Entered || event.Stage == "terminal" {
			t.Fatalf("retained ordinary row[%d]=%+v is not a pre-entry retry", i, event)
		}
	}
}

// A suppressed GC release branch is not an operation return cause and must not
// claim the reserved terminal slot.

// The specific return-path branch must survive both orderings against the
// generic outer "returned" marker.
func TestNodeForegroundArchiveBusyTerminalPrefersSpecificOverGeneric(t *testing.T) {
	specific := localtesthooks.ArchiveBusyEvent{
		Operation: "archive-trim", Resource: "publication_lock", Branch: "release_mismatch_or_conflict", Stage: "terminal", Entered: true,
	}
	generic := localtesthooks.ArchiveBusyEvent{
		Operation: "archive-trim", Resource: "publication_lock", Branch: "returned", Stage: "terminal", Entered: false,
	}

	// Ordering A: specific first, generic later. The generic event must not erase
	// the already-known specific cause.
	specificFirst := &nodeForegroundArchiveBusyCollector{}
	specificFirst.observe(specific)
	specificFirst.observe(generic)
	firstSnapshot := specificFirst.snapshot()
	if !firstSnapshot.hasTerm || firstSnapshot.terminal.Branch != "release_mismatch_or_conflict" {
		t.Fatalf("specific-first terminal=%+v, want the specific branch retained", firstSnapshot.terminal)
	}

	// Ordering B: generic first, specific later. The specific event must upgrade
	// the earlier generic capture.
	genericFirst := &nodeForegroundArchiveBusyCollector{}
	genericFirst.observe(generic)
	genericFirst.observe(specific)
	secondSnapshot := genericFirst.snapshot()
	if !secondSnapshot.hasTerm || secondSnapshot.terminal.Branch != "release_mismatch_or_conflict" {
		t.Fatalf("generic-first terminal=%+v, want the specific branch to upgrade", secondSnapshot.terminal)
	}

	// Repeated generic events never displace a specific cause in either direction.
	genericFirst.observe(generic)
	if repeat := genericFirst.snapshot(); repeat.terminal.Branch != "release_mismatch_or_conflict" {
		t.Fatalf("repeated generic erased the specific cause: %+v", repeat.terminal)
	}

	// With only generic events the generic branch is still reported, so the
	// reserved slot is never left blank when a cause was actually seen.
	genericOnly := &nodeForegroundArchiveBusyCollector{}
	genericOnly.observe(generic)
	if genericOnly.snapshot().terminal.Branch != "returned" {
		t.Fatalf("generic-only terminal=%+v, want the generic branch retained", genericOnly.snapshot().terminal)
	}
}

// A specific terminal capture does not consume or disturb the ordinary budget.
func TestNodeForegroundArchiveBusySpecificTerminalKeepsOrdinaryBudget(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	for i := 0; i < nodeForegroundArchiveBusyOrdinaryLimit; i++ {
		collector.observe(localtesthooks.ArchiveBusyEvent{
			Operation: "archive-trim", Resource: "gc_lock", Branch: "active_live_lease", Stage: "site", Entered: false,
		})
	}
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-trim", Resource: "publication_lock", Branch: "release_mismatch_or_conflict", Stage: "terminal", Entered: true,
	})
	snapshot := collector.snapshot()
	if len(snapshot.events) != nodeForegroundArchiveBusyOrdinaryLimit || snapshot.overflow != 0 {
		t.Fatalf("events=%d overflow=%d, want %d/0", len(snapshot.events), snapshot.overflow, nodeForegroundArchiveBusyOrdinaryLimit)
	}
	if snapshot.terminal.Branch != "release_mismatch_or_conflict" {
		t.Fatalf("terminal=%+v, want the specific branch", snapshot.terminal)
	}
}
func TestNodeForegroundArchiveBusySuppressedDoesNotClaimTerminal(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-gc", Resource: "gc_lock", Branch: "release_mismatch_or_conflict", Stage: "suppressed", Entered: true,
	})
	snapshot := collector.snapshot()
	if snapshot.hasTerm {
		t.Fatalf("suppressed release claimed the terminal slot: %+v", snapshot.terminal)
	}
	if len(snapshot.events) != 1 || snapshot.events[0].Stage != "suppressed" {
		t.Fatalf("events=%+v, want the suppressed row kept as an ordinary event", snapshot.events)
	}

	// A suppressed release branch arriving after a real terminal cause must not
	// downgrade or displace it.
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-gc", Resource: "gc_lock", Branch: "returned", Stage: "terminal", Entered: true,
	})
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-gc", Resource: "gc_lock", Branch: "release_mismatch_or_conflict", Stage: "suppressed", Entered: true,
	})
	afterSuppressed := collector.snapshot()
	if !afterSuppressed.hasTerm || afterSuppressed.terminal.Stage != "terminal" {
		t.Fatalf("suppressed branch displaced the terminal cause: %+v", afterSuppressed)
	}
	if afterSuppressed.terminal.Stage != "terminal" || afterSuppressed.terminal.Branch == "release_mismatch_or_conflict" {
		t.Fatalf("suppressed branch claimed the terminal slot: %+v", afterSuppressed.terminal)
	}
}

// A terminal event dropped under TryLock contention is reported as unknown,
// never inferred from the retained pre-entry rows.
func TestNodeForegroundArchiveBusyTerminalMissIsUnknown(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	for i := 0; i < nodeForegroundArchiveBusyOrdinaryLimit; i++ {
		collector.observe(localtesthooks.ArchiveBusyEvent{
			Operation: "archive-trim", Resource: "publication_lock", Branch: "active_live_lease", Stage: "site", Entered: false,
		})
	}

	// Hold the collector lock so the next observation must miss its TryLock.
	if !collector.mu.TryLock() {
		t.Fatal("could not hold the collector lock")
	}
	observed := make(chan struct{})
	go func() {
		collector.observe(localtesthooks.ArchiveBusyEvent{
			Operation: "archive-trim", Resource: "publication_lock", Branch: "release_mismatch_or_conflict", Stage: "terminal", Entered: true,
		})
		close(observed)
	}()
	<-observed
	collector.mu.Unlock()

	snapshot := collector.snapshot()
	if !snapshot.termMiss {
		t.Fatalf("dropped terminal was not reported unknown: %+v", snapshot)
	}
	if snapshot.hasTerm {
		t.Fatalf("a missed terminal must not be fabricated: %+v", snapshot.terminal)
	}
	if snapshot.overflow == 0 {
		t.Fatal("a dropped terminal must still count as overflow")
	}
	// The retained rows are pre-entry retries only and must not be read as a cause.
	for i, event := range snapshot.events {
		if event.Entered || event.Stage == "terminal" {
			t.Fatalf("retained row[%d]=%+v is not a pre-entry retry", i, event)
		}
	}
}

// The emitted row carries only the fixed allowlisted vocabulary. No lock owner,
// object key, URL, hash, token, or raw error has any field reaching the log.
func TestNodeForegroundArchiveBusyRowIsAllowlisted(t *testing.T) {
	collector := &nodeForegroundArchiveBusyCollector{}
	collector.observe(localtesthooks.ArchiveBusyEvent{
		Operation: "archive-trim", Resource: "gc_lock", Branch: "active_live_lease", Stage: "site",
	})
	snapshot := collector.snapshot()
	operations := map[string]bool{"archive-sync": true, "archive-trim": true, "other": true}
	resources := map[string]bool{"publication_lock": true, "gc_lock": true, "recovery_pin": true, "other": true}
	var line strings.Builder
	for _, event := range snapshot.events {
		fmt.Fprintf(&line, "node_foreground_archive_busy operation_id=1 operation=%q resource=%q branch=%q stage=%q entered=%t overflow=%d\n", event.Operation, event.Resource, event.Branch, event.Stage, event.Entered, snapshot.overflow)
		if !operations[event.Operation] || !resources[event.Resource] {
			t.Fatalf("row outside the allowlist: %+v", event)
		}
	}
	for _, forbidden := range []string{"owner", "token", "key=", "url", "hash", "error="} {
		if strings.Contains(line.String(), forbidden) {
			t.Fatalf("log line leaked %q: %s", forbidden, line.String())
		}
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

const nodeForegroundPublisherBusyEventLimit = 4

const nodeForegroundArchiveBusyEventLimit = 8

// nodeForegroundArchiveBusyOrdinaryLimit is the fixed total capacity minus the
// one reserved terminal slot. Ordinary events never grow it.
const nodeForegroundArchiveBusyOrdinaryLimit = nodeForegroundArchiveBusyEventLimit - 1

// The callback never waits for a lock or grows a slice. The first
// nodeForegroundArchiveBusyOrdinaryLimit ordinary branches are kept in order;
// further ordinary events only increment overflow. An earlier exact branch is
// never overwritten by a later one.
//
// The last slot is reserved for the exact operation return-path terminal event,
// because that is the branch that actually caused the returned ErrArchiveBusy.
// Ordinary pre-entry retries can therefore never push it out. A
// Stage=="terminal" event is reserved exclusively for an operation return
// path; the GC suppressed release branch is not a return cause and never claims
// this slot.
type nodeForegroundArchiveBusyCollector struct {
	mu       sync.Mutex
	events   [nodeForegroundArchiveBusyEventLimit]localtesthooks.ArchiveBusyEvent
	count    int
	terminal localtesthooks.ArchiveBusyEvent
	hasTerm  bool
	termMiss atomic.Bool
	overflow atomic.Uint64
}

// isSpecificArchiveBusyTerminal reports whether an event names the exact
// return-path cause rather than the generic outer "returned" marker that
// withPublicationLock emits for the same error. A specific branch is retained
// in preference to the generic one regardless of arrival order.
func isSpecificArchiveBusyTerminal(event localtesthooks.ArchiveBusyEvent) bool {
	return event.Stage == "terminal" && event.Branch != "returned"
}

func (c *nodeForegroundArchiveBusyCollector) observe(event localtesthooks.ArchiveBusyEvent) {
	if !c.mu.TryLock() {
		c.overflow.Add(1)
		// A terminal event we could not capture is explicitly unknown, so a reader
		// never infers the cause from the retained pre-entry events instead.
		if event.Stage == "terminal" {
			c.termMiss.Store(true)
		}
		return
	}
	if event.Stage == "terminal" {
		// The return-path terminal branch is the actual cause, so it is never
		// dropped by a full ordinary budget. Within the reserved slot a specific
		// terminal branch always beats the generic Branch=="returned" outer marker:
		// a later generic event cannot erase an earlier specific cause, and a later
		// specific event may upgrade an earlier generic one. This never overwrites
		// an ordinary event.
		if !c.hasTerm || !isSpecificArchiveBusyTerminal(c.terminal) || isSpecificArchiveBusyTerminal(event) {
			c.terminal = event
			c.hasTerm = true
		}
	} else if c.count < nodeForegroundArchiveBusyOrdinaryLimit {
		c.events[c.count] = event
		c.count++
	} else {
		c.overflow.Add(1)
	}
	c.mu.Unlock()
}

type nodeForegroundArchiveBusySnapshot struct {
	events   []localtesthooks.ArchiveBusyEvent
	terminal localtesthooks.ArchiveBusyEvent
	hasTerm  bool
	termMiss bool
	overflow uint64
}

func (c *nodeForegroundArchiveBusyCollector) snapshot() nodeForegroundArchiveBusySnapshot {
	c.mu.Lock()
	events := append([]localtesthooks.ArchiveBusyEvent(nil), c.events[:c.count]...)
	terminal, hasTerm := c.terminal, c.hasTerm
	c.mu.Unlock()
	return nodeForegroundArchiveBusySnapshot{events: events, terminal: terminal, hasTerm: hasTerm, termMiss: c.termMiss.Load(), overflow: c.overflow.Load()}
}

type nodeForegroundArchiveBusyFailures struct {
	mu          sync.Mutex
	operationID uint64
	snapshot    nodeForegroundArchiveBusySnapshot
	recorded    bool
}

func (f *nodeForegroundArchiveBusyFailures) record(operationID uint64, snapshot nodeForegroundArchiveBusySnapshot) {
	f.mu.Lock()
	if !f.recorded {
		f.operationID, f.snapshot, f.recorded = operationID, snapshot, true
	}
	f.mu.Unlock()
}

func (f *nodeForegroundArchiveBusyFailures) log(t *testing.T) {
	f.mu.Lock()
	operationID, snapshot := f.operationID, f.snapshot
	f.mu.Unlock()
	if snapshot.hasTerm {
		// The reserved return-path terminal row carries the exact branch that
		// produced the returned ErrArchiveBusy. It is printed after the ordinary
		// rows so a reader cannot mistake an early pre-entry retry for the cause.
		for _, event := range snapshot.events {
			t.Logf("node_foreground_archive_busy operation_id=%d operation=%q resource=%q branch=%q stage=%q entered=%t overflow=%d", operationID, event.Operation, event.Resource, event.Branch, event.Stage, event.Entered, snapshot.overflow)
		}
		t.Logf("node_foreground_archive_busy operation_id=%d operation=%q resource=%q branch=%q stage=%q entered=%t overflow=%d attribution=terminal", operationID, snapshot.terminal.Operation, snapshot.terminal.Resource, snapshot.terminal.Branch, snapshot.terminal.Stage, snapshot.terminal.Entered, snapshot.overflow)
		return
	}
	if len(snapshot.events) == 0 {
		t.Logf("node_foreground_archive_busy operation_id=%d events=0 overflow=%d attribution=unknown", operationID, snapshot.overflow)
		return
	}
	if snapshot.termMiss {
		// A terminal event was dropped under contention or overflow, so the cause
		// is explicitly undecided; the retained rows are pre-entry retries only.
		for _, event := range snapshot.events {
			t.Logf("node_foreground_archive_busy operation_id=%d operation=%q resource=%q branch=%q stage=%q entered=%t overflow=%d", operationID, event.Operation, event.Resource, event.Branch, event.Stage, event.Entered, snapshot.overflow)
		}
		t.Logf("node_foreground_archive_busy operation_id=%d events=%d overflow=%d attribution=terminal_missing", operationID, len(snapshot.events), snapshot.overflow)
		return
	}
	for _, event := range snapshot.events {
		t.Logf("node_foreground_archive_busy operation_id=%d operation=%q resource=%q branch=%q stage=%q entered=%t overflow=%d", operationID, event.Operation, event.Resource, event.Branch, event.Stage, event.Entered, snapshot.overflow)
	}
}

// nodeForegroundRecoveryPinCollector is a fixed-size table: at most
// nodeForegroundRecoveryPinKeyLimit hashed pin keys, each with
// nodeForegroundRecoveryPinEventLimit lifecycle rows plus
// nodeForegroundRecoveryPinReservedClose close rows that are never displaced by
// ordinary rows. The capacity never grows and there is no blocking. When every
// slot is taken, the slot with the oldest last-observed sequence is replaced by
// the new key, which is the approved fixed-array policy rather than an LRU:
// the replacement is counted and the displaced rows are reported as an
// explicitly unknown join. The first guard_read key is pinned so a later
// replacement can never discard the guard identity; it is released only once a
// failure is latched. No full object key, owner string, token, or credential is
// retained.
type nodeForegroundRecoveryPinCollector struct {
	mu            sync.Mutex
	hashes        [nodeForegroundRecoveryPinKeyLimit]string
	seqs          [nodeForegroundRecoveryPinKeyLimit]uint64
	nextSeq       uint64
	events        [nodeForegroundRecoveryPinKeyLimit][nodeForegroundRecoveryPinEventLimit]localtesthooks.RecoveryPinEvent
	counts        [nodeForegroundRecoveryPinKeyLimit]int
	closes        [nodeForegroundRecoveryPinKeyLimit][nodeForegroundRecoveryPinReservedClose]localtesthooks.RecoveryPinEvent
	closeCounts   [nodeForegroundRecoveryPinKeyLimit]int
	guardKey      string
	failureLatch  bool
	latchCount    int
	firstBusy     *nodeForegroundRecoveryPinSnapshot
	keyMiss       atomic.Uint64
	keyEvicted    atomic.Uint64
	joinUnknown   atomic.Bool
	eventOverflow atomic.Uint64
	closeOverflow atomic.Uint64
	termMiss      atomic.Bool
	// terminalEvents retains the bounded rows that describe the terminal
	// post-entry outcome, so a later ordinary row can never be mistaken for the
	// actual cause of a returned Busy.
	terminals   [nodeForegroundRecoveryPinTerminalLimit]localtesthooks.RecoveryPinEvent
	terminalSet [nodeForegroundRecoveryPinTerminalLimit]bool
	terminalN   int
}

func isRecoveryPinClosePhase(phase string) bool {
	return phase == localtesthooks.RecoveryPinCloseAttempt || phase == localtesthooks.RecoveryPinCloseConfirmed ||
		phase == localtesthooks.RecoveryPinCloseConflict || phase == localtesthooks.RecoveryPinCloseError
}

// isRecoveryPinDecisionPhase reports the rows that can explain a later Busy:
// the terminal close outcome and the guard's own read. Ordinary create and
// renew progress must not consume the fixed terminal budget.
func isRecoveryPinDecisionPhase(phase string) bool {
	return isRecoveryPinClosePhase(phase) || phase == localtesthooks.RecoveryPinGuardRead
}

// nodeForegroundRecoveryPinGuardCollector binds the positive active-pin guard
// reads performed by one maintenance operation to that operation. It retains at
// most nodeForegroundRecoveryPinGuardLimit DISTINCT key hashes: a repeat of an
// already-retained hash is deduplicated and consumes no slot, and a further
// distinct hash is never allowed to displace a retained binding, so the missing
// binding stays an explicit unknown instead of silently becoming a different
// key. Each retained record keeps exactly the fields the guard already read, so
// no extra provider I/O and no new production field is involved.
type nodeForegroundRecoveryPinGuardCollector struct {
	mu sync.Mutex
	// operationID is the already-created test-side maintenance operation
	// identity. It is carried by value only so the emitted binding names the
	// operation whose guard reads are recorded; it adds no production or hook
	// field and is never inferred from a later snapshot.
	operationID uint64
	records     [nodeForegroundRecoveryPinGuardLimit]nodeForegroundRecoveryPinGuardRecord
	count       int
	dropped     atomic.Uint64
	joinUnknown atomic.Bool
}

// nodeForegroundRecoveryPinGuardRecord is a plain copy of the observed guard
// read. It retains no owner string, full object key, token, or credential.
type nodeForegroundRecoveryPinGuardRecord struct {
	keyHash      string
	leaseState   string
	leaseDeltaMS int64
	observedAtMS int64
}

type nodeForegroundRecoveryPinGuardSnapshot struct {
	operationID uint64
	records     []nodeForegroundRecoveryPinGuardRecord
	dropped     uint64
	joinUnknown bool
}

// observe retains a positive guard read. It is nonblocking: a contended lock
// records an explicit drop and sets the unknown flag rather than stalling the
// maintenance path.
func (c *nodeForegroundRecoveryPinGuardCollector) observe(event localtesthooks.RecoveryPinEvent) {
	// Only a guard read that saw a live lease is a positive binding: an expired or
	// zero-delta guard row describes a healthy released pin, not an active pin.
	if event.Phase != localtesthooks.RecoveryPinGuardRead ||
		event.LeaseState != localtesthooks.RecoveryPinLeaseActive || event.LeaseDeltaMS <= 0 {
		return
	}
	if !c.mu.TryLock() {
		c.dropped.Add(1)
		c.joinUnknown.Store(true)
		return
	}
	defer c.mu.Unlock()
	for i := 0; i < c.count; i++ {
		if c.records[i].keyHash == event.KeyHash {
			// A duplicate read of a retained hash is deduplicated: it consumes no
			// slot, displaces nothing, and is not a drop. The first retained values
			// stay authoritative for that hash.
			return
		}
	}
	if c.count >= nodeForegroundRecoveryPinGuardLimit {
		c.dropped.Add(1)
		c.joinUnknown.Store(true)
		return
	}
	c.records[c.count] = nodeForegroundRecoveryPinGuardRecord{
		keyHash:      event.KeyHash,
		leaseState:   event.LeaseState,
		leaseDeltaMS: event.LeaseDeltaMS,
		observedAtMS: event.ObservedAtMS,
	}
	c.count++
}

// snapshot copies the retained records by value under the collector lock, so
// the returned snapshot never aliases the live arrays.
func (c *nodeForegroundRecoveryPinGuardCollector) snapshot() nodeForegroundRecoveryPinGuardSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nodeForegroundRecoveryPinGuardSnapshot{
		operationID: c.operationID,
		records:     append([]nodeForegroundRecoveryPinGuardRecord(nil), c.records[:c.count]...),
		dropped:     c.dropped.Load(),
		joinUnknown: c.joinUnknown.Load(),
	}
}

// observe is nonblocking. A contended lock drops the row and records the
// unknown explicitly rather than stalling a production hot path.
func (c *nodeForegroundRecoveryPinCollector) observe(event localtesthooks.RecoveryPinEvent) {
	if !c.mu.TryLock() {
		c.eventOverflow.Add(1)
		if isRecoveryPinClosePhase(event.Phase) {
			c.closeOverflow.Add(1)
		}
		// A dropped close or guard decision row is explicitly unknown, so a
		// reader never treats a retained ordinary row as the terminal cause.
		if isRecoveryPinDecisionPhase(event.Phase) {
			c.termMiss.Store(true)
		}
		return
	}
	defer c.mu.Unlock()
	index := -1
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
		if c.hashes[i] == event.KeyHash {
			index = i
			break
		}
	}
	if index < 0 {
		// The key table is full: replace the oldest slot by sequence. The pinned
		// guard key is never a candidate, so guard identity survives later keys.
		victim := -1
		for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
			if c.hashes[i] == "" {
				victim = i
				break
			}
			if !c.failureLatch && c.guardKey != "" && c.hashes[i] == c.guardKey {
				continue
			}
			if victim < 0 || c.hashes[victim] == "" || c.seqs[i] < c.seqs[victim] {
				victim = i
			}
		}
		if victim < 0 {
			// No slot can be replaced; the row stays explicitly unknown.
			c.keyMiss.Add(1)
			c.eventOverflow.Add(1)
			c.joinUnknown.Store(true)
			if isRecoveryPinDecisionPhase(event.Phase) {
				c.termMiss.Store(true)
			}
			return
		}
		if c.hashes[victim] != "" {
			// The displaced key's retained rows are gone, so its lifecycle join
			// can no longer be proven end to end.
			c.keyEvicted.Add(1)
			c.joinUnknown.Store(true)
		}
		c.hashes[victim] = event.KeyHash
		c.counts[victim] = 0
		c.closeCounts[victim] = 0
		index = victim
	}
	c.nextSeq++
	c.seqs[index] = c.nextSeq
	if event.Phase == localtesthooks.RecoveryPinGuardRead && c.guardKey == "" &&
		event.LeaseState == localtesthooks.RecoveryPinLeaseActive && event.LeaseDeltaMS > 0 {
		// Only a guard read that saw a live lease pins its key: an expired or
		// zero-delta guard row describes a healthy released pin, not the cause
		// of a later Busy.
		c.guardKey = event.KeyHash
	}
	if isRecoveryPinClosePhase(event.Phase) {
		if c.closeCounts[index] < nodeForegroundRecoveryPinReservedClose {
			c.closes[index][c.closeCounts[index]] = event
			c.closeCounts[index]++
		} else {
			c.closeOverflow.Add(1)
			c.eventOverflow.Add(1)
		}
	} else if c.counts[index] < nodeForegroundRecoveryPinEventLimit {
		c.events[index][c.counts[index]] = event
		c.counts[index]++
	} else {
		c.eventOverflow.Add(1)
	}
	if isRecoveryPinDecisionPhase(event.Phase) && c.terminalN < nodeForegroundRecoveryPinTerminalLimit {
		c.terminals[c.terminalN] = event
		c.terminalSet[c.terminalN] = true
		c.terminalN++
	} else if isRecoveryPinDecisionPhase(event.Phase) {
		// The terminal budget is fixed. A dropped terminal row is explicitly
		// unknown; a retained ordinary row never claims the cause.
		c.termMiss.Store(true)
		c.eventOverflow.Add(1)
	}
}

type nodeForegroundRecoveryPinSnapshot struct {
	scope         string
	hashes        []string
	events        [][nodeForegroundRecoveryPinEventLimit]localtesthooks.RecoveryPinEvent
	counts        []int
	closes        [][nodeForegroundRecoveryPinReservedClose]localtesthooks.RecoveryPinEvent
	closeCounts   []int
	terminals     []localtesthooks.RecoveryPinEvent
	terminalSet   []bool
	keyMiss       uint64
	keyEvicted    uint64
	joinUnknown   bool
	eventOverflow uint64
	closeOverflow uint64
	termMiss      bool
	// guard binds the positive active-pin guard reads this same maintenance
	// operation performed. It is captured by value before the pinned guard key is
	// released, and it never replaces or is derived from the terminal rows above.
	guard nodeForegroundRecoveryPinGuardSnapshot
}

func (c *nodeForegroundRecoveryPinCollector) snapshot() nodeForegroundRecoveryPinSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

func (c *nodeForegroundRecoveryPinCollector) snapshotLocked() nodeForegroundRecoveryPinSnapshot {
	snap := nodeForegroundRecoveryPinSnapshot{
		terminals:   append([]localtesthooks.RecoveryPinEvent(nil), c.terminals[:c.terminalN]...),
		terminalSet: append([]bool(nil), c.terminalSet[:c.terminalN]...),
		keyMiss:     c.keyMiss.Load(), eventOverflow: c.eventOverflow.Load(),
		closeOverflow: c.closeOverflow.Load(), termMiss: c.termMiss.Load(),
		keyEvicted: c.keyEvicted.Load(), joinUnknown: c.joinUnknown.Load(),
	}
	for i := 0; i < nodeForegroundRecoveryPinKeyLimit; i++ {
		if c.hashes[i] == "" {
			continue
		}
		snap.hashes = append(snap.hashes, c.hashes[i])
		snap.counts = append(snap.counts, c.counts[i])
		snap.closeCounts = append(snap.closeCounts, c.closeCounts[i])
		snap.events = append(snap.events, c.events[i])
		snap.closes = append(snap.closes, c.closes[i])
	}
	return snap
}

// latchFailure captures an immutable copy of the pin table as it exists at the
// first Busy and only then releases the pinned guard key, so post-latch
// replacement and shutdown rows can never mutate the first-Busy evidence. The
// first capture wins: a repeated latch never overwrites it.
func (c *nodeForegroundRecoveryPinCollector) latchFailure(guard nodeForegroundRecoveryPinGuardSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.firstBusy == nil {
		captured := c.snapshotLocked()
		captured.scope = "first_busy"
		captured.guard = guard
		c.firstBusy = &captured
	}
	c.failureLatch = true
	c.latchCount++
}

// firstBusySnapshot returns the immutable first-Busy copy, or nil if no Busy was
// latched.
func (c *nodeForegroundRecoveryPinCollector) firstBusySnapshot() *nodeForegroundRecoveryPinSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.firstBusy == nil {
		return nil
	}
	captured := *c.firstBusy
	return &captured
}

// log prints the bounded lifecycle rows next to the existing terminal archive
// Busy attribution. Missing or evicted rows are reported as unknown; the
// historical Busy cause itself is never asserted here.
func (snap nodeForegroundRecoveryPinSnapshot) log(t *testing.T) {
	scope := snap.scope
	if scope == "" {
		scope = "final_lifecycle"
	}
	// Always emit one bounded summary line for this scope, so a run whose table
	// is empty still produces a finite marker for the recorder. The summary
	// reports only what was observed: an empty table is unknown, never evidence
	// that no pin existed, that a pin leaked, or that a close was acknowledged.
	t.Logf("node_foreground_recovery_pin scope=%s summary keys_retained=%d/%d slots_unused=%d join_unknown=%t key_evicted=%d key_miss=%d event_overflow=%d close_overflow=%d terminal_rows=%d terminal_missing=%t interpretation=observed_only_empty_is_unknown_not_cause",
		scope, len(snap.hashes), nodeForegroundRecoveryPinKeyLimit, nodeForegroundRecoveryPinKeyLimit-len(snap.hashes),
		snap.joinUnknown, snap.keyEvicted, snap.keyMiss, snap.eventOverflow, snap.closeOverflow, len(snap.terminals), snap.termMiss)
	for i, hash := range snap.hashes {
		for j := 0; j < snap.counts[i]; j++ {
			event := snap.events[i][j]
			t.Logf("node_foreground_recovery_pin scope=%s key_hash=%s row=%d/%d phase=%q owner_category=%q lease_state=%q lease_delta_ms=%d read=%q write=%q readback=%q version_present=%t observed_at_ms=%d key_miss=%d event_overflow=%d close_overflow=%d",
				scope, hash, j, nodeForegroundRecoveryPinEventLimit, event.Phase, event.OwnerCategory, event.LeaseState, event.LeaseDeltaMS, event.ReadStatus, event.WriteStatus, event.ReadbackStatus, event.VersionPresent, event.ObservedAtMS, snap.keyMiss, snap.eventOverflow, snap.closeOverflow)
		}
		for j := 0; j < snap.closeCounts[i]; j++ {
			event := snap.closes[i][j]
			t.Logf("node_foreground_recovery_pin scope=%s key_hash=%s close_row=%d/%d phase=%q owner_category=%q read=%q write=%q readback=%q observed_at_ms=%d key_miss=%d event_overflow=%d close_overflow=%d",
				scope, hash, j, nodeForegroundRecoveryPinReservedClose, event.Phase, event.OwnerCategory, event.ReadStatus, event.WriteStatus, event.ReadbackStatus, event.ObservedAtMS, snap.keyMiss, snap.eventOverflow, snap.closeOverflow)
		}
	}
	if snap.keyMiss > 0 || snap.keyEvicted > 0 || snap.joinUnknown || snap.eventOverflow > 0 || snap.closeOverflow > 0 || snap.termMiss {
		t.Logf("node_foreground_recovery_pin scope=%s rows=unknown key_miss=%d key_evicted=%d join_unknown=%t event_overflow=%d close_overflow=%d terminal_missing=%t", scope, snap.keyMiss, snap.keyEvicted, snap.joinUnknown, snap.eventOverflow, snap.closeOverflow, snap.termMiss)
	}
	// The guard binding is a separate, operation-scoped observation. Its own
	// drop and unknown flags are reported independently of the global table
	// counters above, and an absent or dropped binding is never evidence that no
	// guard read happened.
	t.Logf("node_foreground_recovery_pin_guard operation_id=%d scope=%s guard_keys=%d/%d guard_dropped=%d guard_join_unknown=%t interpretation=operation_binding_only_not_cause",
		snap.guard.operationID, scope, len(snap.guard.records), nodeForegroundRecoveryPinGuardLimit, snap.guard.dropped, snap.guard.joinUnknown)
	for i, record := range snap.guard.records {
		t.Logf("node_foreground_recovery_pin_guard operation_id=%d scope=%s row=%d/%d key_hash=%s lease_state=%q lease_delta_ms=%d observed_at_ms=%d",
			snap.guard.operationID, scope, i, nodeForegroundRecoveryPinGuardLimit, record.keyHash, record.leaseState, record.leaseDeltaMS, record.observedAtMS)
	}
	if len(snap.guard.records) == 0 {
		attribution := "guard_unknown"
		if snap.guard.dropped > 0 || snap.guard.joinUnknown {
			attribution = "guard_join_unknown"
		}
		t.Logf("node_foreground_recovery_pin_guard operation_id=%d scope=%s attribution=%s guard_dropped=%d", snap.guard.operationID, scope, attribution, snap.guard.dropped)
	}
}

// This collector is diagnostic only. The ring is bounded, while the first
// failed operation's guard and any later matching release rows are reserved.
type nodeForegroundClaimLifecycleCollector struct {
	mu             sync.Mutex
	events         []checkpoint.PublisherBusyObservation
	dropped        uint64
	operationID    uint64
	guard          *checkpoint.PublisherBusyObservation
	holder         *checkpoint.PublisherBusyObservation
	releases       []checkpoint.PublisherBusyObservation
	releaseDropped uint64
}

func (c *nodeForegroundClaimLifecycleCollector) observe(event checkpoint.PublisherBusyObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) == nodeForegroundClaimLifecycleLimit {
		copy(c.events, c.events[1:])
		c.events[len(c.events)-1] = event
		c.dropped++
	} else {
		c.events = append(c.events, event)
	}
	if c.guard != nil && event.Generation == c.guard.Generation && event.NamespaceDigest == c.guard.NamespaceDigest &&
		(event.Source == "release_upload_ack" || event.Source == "release_fenced" || event.Source == "release_error") {
		if len(c.releases) < nodeForegroundClaimReleaseLimit {
			c.releases = append(c.releases, event)
		} else {
			c.releaseDropped++
		}
	}
}

func (c *nodeForegroundClaimLifecycleCollector) latchFailure(operationID uint64, events []checkpoint.PublisherBusyObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.guard != nil {
		return
	}
	for _, event := range events {
		if event.Source != "publisher_claim_active" || !event.ActiveClaim || !event.VersionPresent {
			continue
		}
		guard := event
		c.guard = &guard
		c.operationID = operationID
		for i := len(c.events) - 1; i >= 0; i-- {
			candidate := c.events[i]
			if candidate.Source != "acquire_confirmed" && candidate.Source != "renew_confirmed" {
				continue
			}
			if candidate.Generation == guard.Generation && candidate.NamespaceDigest == guard.NamespaceDigest &&
				candidate.VersionPresent && candidate.VersionDigest == guard.VersionDigest {
				holder := candidate
				c.holder = &holder
				break
			}
		}
		return
	}
}

func (c *nodeForegroundClaimLifecycleCollector) log(t *testing.T, scope string) {
	c.mu.Lock()
	guard, holder, operationID, dropped := c.guard, c.holder, c.operationID, c.dropped
	releases := append([]checkpoint.PublisherBusyObservation(nil), c.releases...)
	releaseDropped := c.releaseDropped
	c.mu.Unlock()
	if guard == nil {
		t.Logf("node_foreground_checkpoint_claim scope=%s guard=absent holder=unknown lifecycle_dropped=%d", scope, dropped)
		return
	}
	category := "unknown"
	if holder != nil {
		category = holder.HolderCategory
	}
	t.Logf("node_foreground_checkpoint_claim scope=%s operation_id=%d guard=stable_read generation=%d namespace_digest=%x version_digest=%x version_present=%t holder_category=%s holder_join=%t lifecycle_dropped=%d release_rows=%d release_dropped=%d", scope, operationID, guard.Generation, guard.NamespaceDigest, guard.VersionDigest, guard.VersionPresent, category, holder != nil, dropped, len(releases), releaseDropped)
	for _, event := range releases {
		t.Logf("node_foreground_checkpoint_claim scope=%s operation_id=%d phase=%s generation=%d namespace_digest=%x version_digest=%x version_present=%t holder_category=%s", scope, operationID, event.Source, event.Generation, event.NamespaceDigest, event.VersionDigest, event.VersionPresent, event.HolderCategory)
	}
}

type nodeForegroundPublisherBusyCollector struct {
	mu       sync.Mutex
	events   []checkpoint.PublisherBusyObservation
	overflow uint64
}

func (c *nodeForegroundPublisherBusyCollector) observe(event checkpoint.PublisherBusyObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) < nodeForegroundPublisherBusyEventLimit {
		c.events = append(c.events, event)
		return
	}
	c.overflow++
}

func (c *nodeForegroundPublisherBusyCollector) snapshot() ([]checkpoint.PublisherBusyObservation, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]checkpoint.PublisherBusyObservation(nil), c.events...), c.overflow
}

type nodeForegroundPublisherBusyFailure struct {
	operationID uint64
	events      []checkpoint.PublisherBusyObservation
	overflow    uint64
}

type nodeForegroundPublisherBusyFailures struct {
	mu      sync.Mutex
	failure *nodeForegroundPublisherBusyFailure
}

func (f *nodeForegroundPublisherBusyFailures) record(operationID uint64, events []checkpoint.PublisherBusyObservation, overflow uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure == nil {
		f.failure = &nodeForegroundPublisherBusyFailure{operationID: operationID, events: events, overflow: overflow}
	}
}

func (f *nodeForegroundPublisherBusyFailures) log(t *testing.T) {
	f.mu.Lock()
	failure := f.failure
	f.mu.Unlock()
	if failure == nil || len(failure.events) == 0 {
		var operationID uint64
		var overflow uint64
		if failure != nil {
			operationID, overflow = failure.operationID, failure.overflow
		}
		t.Logf("node_foreground_publisher_busy operation_id=%d source=%q events=0 overflow=%d attribution=unattributed", operationID, "unattributed_propagated", overflow)
		return
	}
	for _, event := range failure.events {
		t.Logf("node_foreground_publisher_busy operation_id=%d source=%q requested_purpose=%q attempt=%d active_claim=%t purpose=%q purpose_truncated=%t owner_id=%q owner_id_truncated=%t generation=%d reserved_index=%d bound_index=%d lease_remaining_ms=%d overflow=%d", failure.operationID, event.Source, event.RequestedPurpose, event.Attempt, event.ActiveClaim, event.Purpose, event.PurposeTruncated, event.OwnerID, event.OwnerIDTruncated, event.Generation, event.ReservedIndex, event.BoundIndex, event.LeaseRemainingMillis, failure.overflow)
	}
}

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

type nodeForegroundClaimRetryCounts struct {
	attempts, refusals, waits, archiveSyncs int
}

func nodeForegroundLiveClaimRefusal(event checkpoint.PublisherBusyObservation) bool {
	return event.Source == "publisher_claim_active" && event.RequestedPurpose == "publisher" && event.ActiveClaim && event.VersionPresent && event.LeaseRemainingMillis > 0
}

// A live claim refusal occurs before this attempt uploads a publisher claim.
// Recheck archive coverage because the materialized tip can advance while waiting.
func retryNodeForegroundCheckpointClaim(ctx context.Context, syncArchive, checkpointAttempt func(context.Context) error, forward func(checkpoint.PublisherBusyObservation)) (nodeForegroundClaimRetryCounts, error) {
	const maxAttempts = 50
	const retryDelay = 20 * time.Millisecond
	var counts nodeForegroundClaimRetryCounts
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return counts, err
		}
		counts.archiveSyncs++
		if err := syncArchive(ctx); err != nil {
			return counts, fmt.Errorf("sync archive through live core tip: %w", err)
		}
		var seen atomic.Int32
		var liveClaim atomic.Bool
		attemptCtx := checkpoint.WithPublisherBusyObserver(ctx, func(event checkpoint.PublisherBusyObservation) {
			if forward != nil {
				forward(event)
			}
			seen.Add(1)
			if nodeForegroundLiveClaimRefusal(event) {
				liveClaim.Store(true)
			}
		})
		counts.attempts++
		err := checkpointAttempt(attemptCtx)
		if err == nil {
			return counts, nil
		}
		// A wrapped or joined Busy, or any positive acquisition/publication
		// event in this attempt, has an ambiguous commit outcome and is terminal.
		if err != checkpoint.ErrPublisherBusy || seen.Load() != 1 || !liveClaim.Load() {
			return counts, fmt.Errorf("checkpoint live materialized state: %w", err)
		}
		counts.refusals++
		if attempt+1 == maxAttempts {
			return counts, fmt.Errorf("checkpoint live materialized state: %w", err)
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-timer.C:
			counts.waits++
		case <-ctx.Done():
			timer.Stop()
			return counts, ctx.Err()
		}
	}
	panic("unreachable checkpoint claim retry state")
}

func runNodeForegroundCheckpoint(t *testing.T, ctx context.Context, node *Node, sharedReadDir string, operationID uint64, forward func(checkpoint.PublisherBusyObservation)) (nodeForegroundMaintenanceResult, error) {
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
	var stateTip uint64
	counts, err := retryNodeForegroundCheckpointClaim(ctx, func(attemptCtx context.Context) error {
		return node.archive.SyncThrough(attemptCtx, node.core, node.core.Tip())
	}, func(attemptCtx context.Context) error {
		stateTip = node.material.StateTip()
		return node.checkpointer.CheckpointOnShutdown(attemptCtx, stateTip)
	}, forward)
	if counts.refusals > 0 || err != nil {
		t.Logf("node_foreground_checkpoint_claim_retry operation_id=%d attempts=%d pre_admission_refusals=%d waits=%d archive_sync_checks=%d final_success=%t", operationID, counts.attempts, counts.refusals, counts.waits, counts.archiveSyncs, err == nil)
	}
	if err != nil {
		return nodeForegroundMaintenanceResult{}, err
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

func prepareNodeForegroundCertifiedGC(ctx context.Context, node *Node, sharedReadDir string) error {
	if err := prepareNodeCheckpointBeforeShutdown(ctx, node, sharedReadDir); err != nil {
		return err
	}
	seal, sealed, err := node.core.LatestCheckpointSeal()
	if err != nil {
		return fmt.Errorf("read prepared GC checkpoint seal: %w", err)
	}
	if !sealed || uint64(node.archive.Tip()) < uint64(seal.DecisionSlot) {
		return fmt.Errorf("prepared GC checkpoint lacks certified archive coverage: sealed=%t archive_tip=%d seal_slot=%d", sealed, node.archive.Tip(), seal.DecisionSlot)
	}
	return nil
}

func TestNodeForegroundCertifiedGCPreparationRequiresRealSealAndCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := New(&types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: "certified-gc-preparation", NodeID: "n1",
		Members: []quepaxa.Member{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(),
		ObjStoreSyncInterval: time.Hour, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	})
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Shutdown(); err != nil {
			t.Errorf("shutdown prepared node: %v", err)
		}
	}()
	if seal, sealed, err := n.core.LatestCheckpointSeal(); err != nil || sealed {
		t.Fatalf("initial checkpoint seal=%+v sealed=%t error=%v, want absent", seal, sealed, err)
	}
	initial, err := readNodeForegroundSharedCurrent(ctx, n, t.TempDir())
	if err != nil || initial != nil {
		t.Fatalf("initial fresh shared CURRENT=%v error=%v, want absent", initial, err)
	}
	api, err := n.API()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.KVPut(ctx, network.KVMutationRequest{RequestID: "certified-gc-preparation", Key: "seed", Value: []byte("value")}); err != nil {
		t.Fatalf("seed live node: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := prepareNodeForegroundCertifiedGC(canceled, n, t.TempDir()); err == nil {
		t.Fatal("canceled certified preparation was accepted before measurement")
	}
	if seal, sealed, err := n.core.LatestCheckpointSeal(); err != nil || sealed {
		t.Fatalf("canceled preparation published a seal: seal=%+v sealed=%t error=%v", seal, sealed, err)
	}
	if err := prepareNodeForegroundCertifiedGC(ctx, n, t.TempDir()); err != nil {
		t.Fatalf("prepare real certified checkpoint while node is live: %v", err)
	}
	seal, sealed, err := n.core.LatestCheckpointSeal()
	if err != nil || !sealed {
		t.Fatalf("prepared checkpoint seal=%+v sealed=%t error=%v", seal, sealed, err)
	}
	root := n.checkpoints.Latest()
	shared, err := readNodeForegroundSharedCurrent(ctx, n, t.TempDir())
	if err != nil || root == nil || shared == nil {
		t.Fatalf("prepared root=%v fresh CURRENT=%v error=%v", root, shared, err)
	}
	if root.Index != uint64(seal.Index) || root.RootHash != seal.RootHash || root.Hash != seal.StateHash ||
		shared.Index != root.Index || shared.RootHash != root.RootHash || shared.Hash != root.Hash ||
		uint64(n.archive.Tip()) < uint64(seal.DecisionSlot) {
		t.Fatalf("prepared seal/root/CURRENT/archive coverage mismatch: root=%d shared=%d seal=%d archive_tip=%d seal_slot=%d", root.Index, shared.Index, seal.Index, n.archive.Tip(), seal.DecisionSlot)
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

// These names are fixed hook literals, not request identities. A batch-level
// rejection may fail several API calls, so their counts are never attributed
// one-for-one to the separately logged foreground API errors.
var nodeForegroundOverloadSiteNames = [...]string{
	"network:mutation-admission:rejected",
	"network:mutation-queue:reservation-rejected",
	"network:mutation-queue:input-channel-full",
	"network:mutation-queue:worker-inflight-bytes-rejected",
	"network:proposal-admission:byte-budget-rejected",
	"network:proposal-admission:local-cap-rejected",
	"network:proposal-admission:operation-cap-rejected",
}

// Atomic fixed-size counters keep the global test-hook callback nonblocking.
// Unrelated hook names are ignored; an unrecognized overload name is counted
// as unknown rather than retained as a potentially unbounded string.
type nodeForegroundOverloadSiteCollector struct {
	counts  [len(nodeForegroundOverloadSiteNames)]atomic.Uint64
	unknown atomic.Uint64
	first   atomic.Uint32 // 1-based site index, or len(names)+1 for unknown.
}

func (c *nodeForegroundOverloadSiteCollector) observe(name string) {
	if !strings.HasPrefix(name, "network:mutation-") && !strings.HasPrefix(name, "network:proposal-admission:") {
		return
	}
	for i, site := range nodeForegroundOverloadSiteNames {
		if name == site {
			c.counts[i].Add(1)
			c.first.CompareAndSwap(0, uint32(i+1))
			return
		}
	}
	c.unknown.Add(1)
	c.first.CompareAndSwap(0, uint32(len(nodeForegroundOverloadSiteNames)+1))
}

func (c *nodeForegroundOverloadSiteCollector) log(t *testing.T) {
	t.Helper()
	first := "not_observed"
	if index := c.first.Load(); index > 0 {
		first = "site_unknown"
		if index <= uint32(len(nodeForegroundOverloadSiteNames)) {
			first = nodeForegroundOverloadSiteNames[index-1]
		}
	}
	unknown := c.unknown.Load()
	total := unknown
	for i, site := range nodeForegroundOverloadSiteNames {
		count := c.counts[i].Load()
		total += count
		t.Logf("node_foreground_overload_site phase=measurement site=%s events=%d interpretation=aggregate_site_events_not_per_request_attribution", site, count)
	}
	t.Logf("node_foreground_overload_site phase=measurement total_events=%d site_unknown=%d first_site=%s interpretation=aggregate_site_events_not_per_request_attribution", total, unknown, first)
}

func TestNodeForegroundOverloadSiteCollectorCountsFixedNames(t *testing.T) {
	collector := &nodeForegroundOverloadSiteCollector{}
	restore := localtesthooks.Set(collector.observe)
	t.Cleanup(restore)
	localtesthooks.Hit("network:learned:unrelated")
	localtesthooks.Hit(nodeForegroundOverloadSiteNames[2])
	localtesthooks.Hit("network:mutation-queue:unrecognized")
	const workers = 16
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for _, site := range nodeForegroundOverloadSiteNames {
				localtesthooks.Hit(site)
			}
		}()
	}
	group.Wait()
	if got := collector.first.Load(); got != 3 {
		t.Fatalf("first site index=%d, want input-channel-full index 3", got)
	}
	if got := collector.unknown.Load(); got != 1 {
		t.Fatalf("unknown overload site events=%d, want 1", got)
	}
	for i, site := range nodeForegroundOverloadSiteNames {
		want := uint64(workers)
		if i == 2 {
			want++
		}
		if got := collector.counts[i].Load(); got != want {
			t.Fatalf("site %s events=%d, want %d", site, got, want)
		}
	}
}

const nodeForegroundBatchAdmissionReasonCount = 6

type nodeForegroundBatchAdmissionCollector struct {
	reasons            [nodeForegroundBatchAdmissionReasonCount]atomic.Uint64
	waitedCount        atomic.Uint64
	waitedNanos        atomic.Uint64
	waitedMaxNanos     atomic.Uint64
	durableCount       atomic.Uint64
	durableNanos       atomic.Uint64
	durableMaxNanos    atomic.Uint64
	acceptedNotDurable atomic.Uint64
	invalid            atomic.Uint64
}

func nodeForegroundBatchAdmissionReasonIndex(reason string) (int, bool) {
	switch reason {
	case localtesthooks.BatchAdmissionAccepted:
		return 0, true
	case localtesthooks.BatchAdmissionWaitBudgetExhausted:
		return 1, true
	case localtesthooks.BatchAdmissionContextCanceled:
		return 2, true
	case localtesthooks.BatchAdmissionServerNotReady:
		return 3, true
	case localtesthooks.BatchAdmissionByteRejected:
		return 4, true
	case localtesthooks.BatchAdmissionJoinedExisting:
		return 5, true
	default:
		return 0, false
	}
}

func nodeForegroundAtomicMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); current < value; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (c *nodeForegroundBatchAdmissionCollector) observe(event localtesthooks.BatchAdmissionEvent) {
	index, known := nodeForegroundBatchAdmissionReasonIndex(event.Reason)
	if !known || event.WaitNanos < 0 || event.AcceptedToDurableNanos < 0 ||
		(!event.Waited && event.WaitNanos != 0) ||
		(event.Reason != localtesthooks.BatchAdmissionAccepted && event.AcceptedToDurableNanos != 0) ||
		(event.Reason != localtesthooks.BatchAdmissionAccepted && event.Durable) ||
		(!event.Durable && event.AcceptedToDurableNanos != 0) {
		c.invalid.Add(1)
		return
	}
	c.reasons[index].Add(1)
	if event.Waited {
		waited := uint64(event.WaitNanos)
		c.waitedCount.Add(1)
		c.waitedNanos.Add(waited)
		nodeForegroundAtomicMax(&c.waitedMaxNanos, waited)
	}
	if event.Reason != localtesthooks.BatchAdmissionAccepted {
		return
	}
	if !event.Durable {
		c.acceptedNotDurable.Add(1)
		return
	}
	durable := uint64(event.AcceptedToDurableNanos)
	c.durableCount.Add(1)
	c.durableNanos.Add(durable)
	nodeForegroundAtomicMax(&c.durableMaxNanos, durable)
}

func (c *nodeForegroundBatchAdmissionCollector) log(t *testing.T, result foregroundcosttest.WindowResult, metrics *nodeForegroundCostMetrics) {
	t.Helper()
	metrics.mu.Lock()
	inWindow, drained := metrics.inWindow, metrics.drained
	metrics.mu.Unlock()
	t.Logf("node_foreground_batch_admission phase=measurement completion_cohort=measurement_window_plus_drain event_in_window_attribution=unavailable api_completed_in_window=%d api_completed_during_drain=%d runner_started=%d runner_completed=%d pending_at_cutoff=%d outstanding=%d accepted=%d wait_budget_exhausted=%d context_canceled=%d server_not_ready=%d byte_rejected=%d joined_existing=%d waited_count=%d waited_nanos_sum=%d waited_nanos_max=%d durable_count=%d durable_nanos_sum=%d durable_nanos_max=%d accepted_not_durable=%d invalid_events=%d", inWindow, drained, result.Started, result.Completed, result.PendingAtCutoff, result.Outstanding, c.reasons[0].Load(), c.reasons[1].Load(), c.reasons[2].Load(), c.reasons[3].Load(), c.reasons[4].Load(), c.reasons[5].Load(), c.waitedCount.Load(), c.waitedNanos.Load(), c.waitedMaxNanos.Load(), c.durableCount.Load(), c.durableNanos.Load(), c.durableMaxNanos.Load(), c.acceptedNotDurable.Load(), c.invalid.Load())
}

func TestNodeForegroundBatchAdmissionCollectorAggregatesConcurrentEvents(t *testing.T) {
	collector := &nodeForegroundBatchAdmissionCollector{}
	restore := localtesthooks.SetBatchAdmission(collector.observe)
	t.Cleanup(restore)
	const workers = 16
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: localtesthooks.BatchAdmissionAccepted, Waited: true, WaitNanos: 11, AcceptedToDurableNanos: 17, Durable: true})
			localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: localtesthooks.BatchAdmissionAccepted})
			localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: localtesthooks.BatchAdmissionWaitBudgetExhausted, Waited: true, WaitNanos: 23})
			localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: localtesthooks.BatchAdmissionJoinedExisting})
		}()
	}
	group.Wait()
	localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: "unknown"})
	localtesthooks.HitBatchAdmission(localtesthooks.BatchAdmissionEvent{Reason: localtesthooks.BatchAdmissionAccepted, WaitNanos: -1})
	if got := collector.reasons[0].Load(); got != 2*workers {
		t.Fatalf("accepted=%d, want %d", got, 2*workers)
	}
	if got := collector.reasons[1].Load(); got != workers {
		t.Fatalf("wait budget exhausted=%d, want %d", got, workers)
	}
	if got := collector.reasons[5].Load(); got != workers {
		t.Fatalf("joined existing=%d, want %d", got, workers)
	}
	if got := collector.waitedCount.Load(); got != 2*workers {
		t.Fatalf("waited count=%d, want %d", got, 2*workers)
	}
	if got := collector.waitedNanos.Load(); got != uint64(workers*(11+23)) || collector.waitedMaxNanos.Load() != 23 {
		t.Fatalf("waited aggregate sum=%d max=%d", got, collector.waitedMaxNanos.Load())
	}
	if got := collector.durableCount.Load(); got != workers || collector.durableNanos.Load() != uint64(workers*17) || collector.durableMaxNanos.Load() != 17 {
		t.Fatalf("durable aggregate count=%d sum=%d max=%d", got, collector.durableNanos.Load(), collector.durableMaxNanos.Load())
	}
	if got := collector.acceptedNotDurable.Load(); got != workers {
		t.Fatalf("accepted not durable=%d, want %d", got, workers)
	}
	if got := collector.invalid.Load(); got != 2 {
		t.Fatalf("invalid=%d, want 2", got)
	}
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
		{name: "preparation exact allowance and reserve", required: nodeForegroundStartupSeedAllowance + nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, remaining: nodeForegroundStartupSeedAllowance + nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, want: true},
		{name: "preparation short by one nanosecond", required: nodeForegroundStartupSeedAllowance + nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup, remaining: nodeForegroundStartupSeedAllowance + nodeForegroundWarmup + nodeForegroundMeasure + nodeForegroundDrainReserve + nodeForegroundCleanup - time.Nanosecond, want: false},
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

func TestNodeForegroundPreparationDeadlineBridge(t *testing.T) {
	t.Run("expired preparation interrupts blocking startup", func(t *testing.T) {
		outerCtx, outerCancel := context.WithCancel(context.Background())
		defer outerCancel()
		preparationCtx, preparationCancel := context.WithCancel(outerCtx)
		stop := nodeForegroundPreparationBridge(preparationCtx, outerCancel)
		startupDone := make(chan struct{})
		go func() {
			<-outerCtx.Done()
			close(startupDone)
		}()
		preparationCancel()
		select {
		case <-startupDone:
		case <-time.After(time.Second):
			t.Fatal("preparation cancellation did not interrupt blocking startup")
		}
		if nodeForegroundPreparationFinished(preparationCtx, time.Now(), time.Now(), stop) {
			t.Fatal("expired preparation was accepted")
		}
	})
	t.Run("successful preparation preserves real node lifetime", func(t *testing.T) {
		outerCtx, outerCancel := context.WithCancel(context.Background())
		defer outerCancel()
		preparationCtx, preparationCancel := context.WithCancel(outerCtx)
		stop := nodeForegroundPreparationBridge(preparationCtx, outerCancel)
		n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()})
		if err := n.Open(outerCtx); err != nil {
			preparationCancel()
			t.Fatal(err)
		}
		defer n.Shutdown()
		if !nodeForegroundPreparationFinished(preparationCtx, time.Now().Add(time.Minute), time.Now(), stop) {
			preparationCancel()
			t.Fatal("healthy preparation failed completion gate")
		}
		preparationCancel()
		if outerCtx.Err() != nil || !n.ready.Load() {
			t.Fatal("preparation cancellation ended the opened node lifetime")
		}
		if _, err := n.server.Query(outerCtx, network.QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); err != nil {
			t.Fatalf("local Node API after preparation cancellation: %v", err)
		}
	})
	t.Run("completion after deadline is rejected", func(t *testing.T) {
		outerCtx, outerCancel := context.WithCancel(context.Background())
		defer outerCancel()
		preparationCtx, preparationCancel := context.WithCancel(outerCtx)
		defer preparationCancel()
		deadline := time.Now()
		if !nodeForegroundPreparationFinished(preparationCtx, deadline, deadline, nodeForegroundPreparationBridge(preparationCtx, outerCancel)) {
			t.Fatal("completion at exact deadline was rejected")
		}
		if nodeForegroundPreparationFinished(preparationCtx, deadline, deadline.Add(time.Nanosecond), func() bool { return true }) {
			t.Fatal("late completion was accepted")
		}
	})
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
	case "setup", "seed", "certified_gc_preparation", "warmup", "measurement", "cleanup_preparation",
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
		{name: "certified_gc_preparation_verified", edit: func(r *objmetrics.ExtentUploadAttributionAggregate) { r.Phase = "certified_gc_preparation" }, want: true},
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
