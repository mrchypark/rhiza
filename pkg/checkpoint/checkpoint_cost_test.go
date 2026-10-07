package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/thanos-io/objstore"
)

// TestCheckpointCost is an opt-in S3-compatible measurement, not cloud
// qualification. Its 128 MiB graph file exercises multiple SectionReader uploads.
func TestCheckpointCost(t *testing.T) {
	bin := os.Getenv("RHIZA_VERSITYGW_BIN")
	if bin == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN for a local Versity S3 measurement")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := versityfixture.Start(ctx, bin, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := server.Close(stopCtx); err != nil {
			t.Errorf("stop local Versity child: %v", err)
		}
		records, err := server.AccessRecords()
		if err != nil {
			t.Errorf("read Versity access records: %v", err)
			return
		}
		for _, record := range records {
			t.Logf("versity_access_record=%s", record)
		}
		t.Logf("versity_access_log_records=%d versity_access_log_requests=%d", len(records), versityfixture.RequestCount(records))
	}()
	setupClient, setupCounts, err := server.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setupClient.ListBuckets(ctx); err != nil {
		t.Fatalf("authenticated Versity readiness: %v", err)
	}
	bucketName := "rhiza-checkpoint-" + server.RunID[:16]
	if err := setupClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create task-owned bucket: %v", err)
	}
	setupAttempts, setupResponses := setupCounts.Snapshot()
	setupServerRequests := accessRequestCount(t, server)
	if setupServerRequests != setupResponses {
		t.Fatalf("setup server requests=%d, client responses=%d (attempts=%d)", setupServerRequests, setupResponses, setupAttempts)
	}
	bucket, err := objmetrics.NewBucketWithContext(objmetrics.WithReplayObservation(ctx), objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: strings.TrimPrefix(server.Endpoint, "http://"),
		Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: server.AccessKey, SecretKey: server.SecretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bucket.Stats().ReplayGroupingEnabled {
		t.Fatal("checkpoint cost fixture did not enable request replay grouping")
	}
	defer bucket.Close()
	prefix := fmt.Sprintf("rhiza-checkpoint-cost/%d", time.Now().UnixNano())
	manager := NewManager(bucket, prefix, t.TempDir(), 1)
	graph := checkpointCostGraphSourceVariant(t, 2*blockSize, "checkpoint-cost-graph")
	fixtures := map[string][]Source{
		"first":     {source(t, RoleSQLite, "checkpoint-cost-first"), graph},
		"unchanged": {source(t, RoleSQLite, "checkpoint-cost-first"), graph},
		"sparse":    {source(t, RoleSQLite, "checkpoint-cost-sparse"), graph},
		"dense":     {source(t, RoleSQLite, "checkpoint-cost-dense"), checkpointCostGraphSourceVariant(t, 2*blockSize, "checkpoint-cost-dense-graph")},
	}

	cleanup := func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		before, serverBefore := bucket.Stats(), accessRequestCount(t, server)
		var names []string
		if err := bucket.Iter(cleanupCtx, prefix+"/", func(name string) error { names = append(names, name); return nil }, objstore.WithRecursiveIter()); err != nil {
			t.Error(err)
			return
		}
		for _, name := range names {
			if err := bucket.Delete(cleanupCtx, name); err != nil {
				t.Error(err)
			}
		}
		cleanupStats := checkpointCostDelta(before, bucket.Stats())
		cleanupServerRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_cleanup", cleanupServerRequests, cleanupStats.HTTPRequests)
		t.Logf("CHECKPOINT_COST_CLEANUP server_requests=%d stats=%+v", cleanupServerRequests, cleanupStats)
	}
	defer cleanup()

	var previousObjects map[string]int64
	var sparseNewBlockBytes int64
	for index, scenario := range []string{"first", "unchanged", "sparse", "dense"} {
		claimBefore, claimServerBefore := bucket.Stats(), accessRequestCount(t, server)
		claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", uint64(index), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		claimStats := checkpointCostDelta(claimBefore, bucket.Stats())
		claimRequests := accessRequestCount(t, server) - claimServerBefore
		assertServerRequestDelta(t, "checkpoint_claim", claimRequests, claimStats.HTTPRequests)

		before, serverBefore := bucket.Stats(), accessRequestCount(t, server)
		started := time.Now()
		scanHashDurations := make(map[string]time.Duration, len(fixtures[scenario]))
		scanHashSources := 0
		root, err := manager.createFilesWithScanObserver(ctx, claim, fixtures[scenario], uint64(index+1), func(role string, elapsed time.Duration) {
			scanHashDurations[role] += elapsed
			scanHashSources++
		})
		createTime := time.Since(started)
		createStats := checkpointCostDelta(before, bucket.Stats())
		createRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_create_"+scenario, createRequests, createStats.HTTPRequests)
		if err != nil {
			t.Fatal(err)
		}
		if scanHashSources != len(fixtures[scenario]) {
			t.Fatalf("scenario=%s scan/hash timing callbacks=%d, want one per source (%d)", scenario, scanHashSources, len(fixtures[scenario]))
		}

		before, serverBefore = bucket.Stats(), accessRequestCount(t, server)
		started = time.Now()
		promoteErr := manager.PromoteCertifiedCurrent(ctx, root)
		publishTime := time.Since(started)
		publishStats := checkpointCostDelta(before, bucket.Stats())
		publishRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_publish_"+scenario, publishRequests, publishStats.HTTPRequests)

		before, serverBefore = bucket.Stats(), accessRequestCount(t, server)
		readback := NewManager(bucket, prefix, t.TempDir(), 1)
		if err := readback.Load(ctx); err != nil {
			t.Fatalf("publication outcome unknown for %s: promote=%v readback=%v", scenario, promoteErr, err)
		}
		readbackRequests := accessRequestCount(t, server) - serverBefore
		readbackStats := checkpointCostDelta(before, bucket.Stats())
		assertServerRequestDelta(t, "checkpoint_current_readback_"+scenario, readbackRequests, readbackStats.HTTPRequests)
		winner := readback.Latest()
		if winner == nil || winner.RootHash != root.RootHash {
			t.Fatalf("scenario=%s winner=%+v candidate=%x promote_error=%v", scenario, winner, root.RootHash, promoteErr)
		}

		before, serverBefore = bucket.Stats(), accessRequestCount(t, server)
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		releaseStats := checkpointCostDelta(before, bucket.Stats())
		releaseRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_claim_release_"+scenario, releaseRequests, releaseStats.HTTPRequests)

		before, serverBefore = bucket.Stats(), accessRequestCount(t, server)
		reachable, err := checkpointReachableObjects(ctx, readback, winner)
		if err != nil {
			t.Fatal(err)
		}
		reachableStats := checkpointCostDelta(before, bucket.Stats())
		reachableRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_reachability_"+scenario, reachableRequests, reachableStats.HTTPRequests)
		var newlyReachable, newlyReachableBlocks int64
		for key, size := range reachable {
			if _, found := previousObjects[key]; !found {
				newlyReachable += size
				if strings.Contains(key, "/checkpoint/blocks/") {
					newlyReachableBlocks += size
				}
			}
		}
		switch scenario {
		case "first":
			if newlyReachableBlocks == 0 {
				t.Fatal("first checkpoint has no newly reachable block bytes")
			}
		case "unchanged":
			if newlyReachableBlocks != 0 {
				t.Fatalf("unchanged checkpoint newly reachable block bytes=%d", newlyReachableBlocks)
			}
		case "sparse":
			if newlyReachableBlocks == 0 {
				t.Fatal("sparse checkpoint has no newly reachable block bytes")
			}
			sparseNewBlockBytes = newlyReachableBlocks
		case "dense":
			if newlyReachableBlocks <= sparseNewBlockBytes {
				t.Fatalf("dense new block bytes=%d, sparse=%d", newlyReachableBlocks, sparseNewBlockBytes)
			}
		}

		before, serverBefore = bucket.Stats(), accessRequestCount(t, server)
		started = time.Now()
		if _, err := readback.DownloadAndVerifyRootFiles(ctx, winner, t.TempDir()); err != nil {
			t.Fatal(err)
		}
		restoreTime := time.Since(started)
		restoreStats := checkpointCostDelta(before, bucket.Stats())
		restoreRequests := accessRequestCount(t, server) - serverBefore
		assertServerRequestDelta(t, "checkpoint_restore_"+scenario, restoreRequests, restoreStats.HTTPRequests)
		promotionError := ""
		if promoteErr != nil {
			promotionError = promoteErr.Error()
		}
		result, err := json.Marshal(map[string]any{
			"provider": "local-versity-s3", "qualification": "local-measurement-not-provider-qualification",
			"versity_version": server.Version, "versity_binary_sha256": server.BinarySHA256,
			"scenario": scenario, "publication_outcome": "candidate_won_verified_current_readback",
			"promotion_error": promotionError, "source_bytes": root.Size, "block_count": len(root.Files[1].Blocks),
			"reachable_bytes": sumReachable(reachable), "newly_reachable_bytes": newlyReachable,
			"newly_reachable_block_bytes": newlyReachableBlocks, "create_ms": createTime.Milliseconds(),
			"snapshot_scan_hash_ms_by_role": durationMapMilliseconds(scanHashDurations),
			"snapshot_scan_hash_sources":    scanHashSources,
			"publish_ms":                    publishTime.Milliseconds(), "restore_ms": restoreTime.Milliseconds(),
			"claim_stats": claimStats, "create_stats": createStats, "publish_stats": publishStats,
			"current_readback_stats": readbackStats, "claim_release_stats": releaseStats,
			"reachability_stats": reachableStats, "restore_stats": restoreStats,
			"server_requests": map[string]uint64{"claim": claimRequests, "create": createRequests,
				"publish": publishRequests, "current_readback": readbackRequests, "claim_release": releaseRequests,
				"reachability": reachableRequests, "restore": restoreRequests},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("CHECKPOINT_COST %s", result)
		previousObjects = reachable
		manager = readback
	}
	measureCheckpointCostCASLoser(t, ctx, bucket, server, prefix, manager, previousObjects)
}

func durationMapMilliseconds(durations map[string]time.Duration) map[string]float64 {
	result := make(map[string]float64, len(durations))
	for role, duration := range durations {
		result[role] = float64(duration) / float64(time.Millisecond)
	}
	return result
}

type checkpointScanTimingBucket struct {
	objstore.Bucket
	uploadStarted chan struct{}
	releaseUpload chan struct{}
}

func (b *checkpointScanTimingBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "checkpoint/blocks/") {
		select {
		case b.uploadStarted <- struct{}{}:
		default:
		}
		select {
		case <-b.releaseUpload:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestCheckpointScanHashTimingEndsBeforeBlockUpload(t *testing.T) {
	base := objstore.NewInMemBucket()
	bucket := &checkpointScanTimingBucket{
		Bucket: base, uploadStarted: make(chan struct{}, 1), releaseUpload: make(chan struct{}),
	}
	manager := NewManager(bucket, "checkpoint-scan-timing", "", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	claim, err := manager.AcquirePublisherClaim(ctx, "scan-timing-test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources := []Source{
		source(t, RoleSQLite, "scan-timing-sqlite"),
		checkpointCostGraphSourceVariant(t, 1024, "scan-timing-test"),
	}
	scanEvents := make(chan struct {
		role    string
		elapsed time.Duration
	}, 1)
	createDone := make(chan error, 1)
	go func() {
		_, createErr := manager.createFilesWithScanObserver(ctx, claim, sources, 1, func(role string, elapsed time.Duration) {
			scanEvents <- struct {
				role    string
				elapsed time.Duration
			}{role: role, elapsed: elapsed}
		})
		createDone <- createErr
	}()
	select {
	case event := <-scanEvents:
		if event.role != RoleSQLite || event.elapsed < 0 {
			t.Fatalf("scan event=%+v, want SQLite role and nonnegative elapsed time", event)
		}
	case <-ctx.Done():
		t.Fatalf("hash scan did not complete before upload gate: %v", ctx.Err())
	case err := <-createDone:
		t.Fatalf("checkpoint creation exited before scan callback: %v", err)
	}
	select {
	case <-bucket.uploadStarted:
		// The upload is now blocked, while the scan event above has already arrived.
	case <-ctx.Done():
		t.Fatalf("block upload did not reach gate: %v", ctx.Err())
	case err := <-createDone:
		t.Fatalf("checkpoint creation exited before reaching block upload: %v", err)
	}
	close(bucket.releaseUpload)
	select {
	case err := <-createDone:
		if err != nil {
			t.Fatalf("complete checkpoint creation after releasing upload gate: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("checkpoint creation did not complete after releasing upload gate: %v", ctx.Err())
	}
}

type checkpointCostRaceRoleKey struct{}

type checkpointCostCASRaceBucket struct {
	objstore.Bucket
	loserCurrentRead    chan struct{}
	winnerCurrentSaved  chan struct{}
	releaseLoserCurrent chan struct{}
	loserConflicts      atomic.Uint64
}

func (b *checkpointCostCASRaceBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.Bucket.Get(ctx, name)
	if err != nil || !strings.HasSuffix(name, "/checkpoint/CURRENT") || ctx.Value(checkpointCostRaceRoleKey{}) != "loser" {
		return reader, err
	}
	select {
	case b.loserCurrentRead <- struct{}{}:
	default:
	}
	select {
	case <-b.winnerCurrentSaved:
		select {
		case <-b.releaseLoserCurrent:
			return reader, nil
		case <-ctx.Done():
			_ = reader.Close()
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		_ = reader.Close()
		return nil, ctx.Err()
	}
}

func (b *checkpointCostCASRaceBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, reader, options...)
	if err != nil && strings.HasSuffix(name, "/checkpoint/CURRENT") && ctx.Value(checkpointCostRaceRoleKey{}) == "loser" && b.Bucket.IsConditionNotMetErr(err) {
		b.loserConflicts.Add(1)
	}
	if err == nil && strings.HasSuffix(name, "/checkpoint/CURRENT") && ctx.Value(checkpointCostRaceRoleKey{}) == "winner" {
		close(b.winnerCurrentSaved)
	}
	return err
}

func TestCheckpointCostCASRaceKeepsHigherIndexWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := objstore.NewInMemBucket()
	prefix := "checkpoint-cost-cas-race"
	manager := NewManager(store, prefix, t.TempDir(), 1)
	baseRoot := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "base")}, 1)
	if err := manager.PromoteCertifiedCurrent(ctx, baseRoot); err != nil {
		t.Fatal(err)
	}
	claim, err := manager.AcquirePublisherClaim(ctx, "cas-race", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.ReleasePublisherClaim(context.Background(), claim) }()
	loser, err := manager.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, "loser")}, 2)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := manager.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, "winner")}, 3)
	if err != nil {
		t.Fatal(err)
	}
	race := &checkpointCostCASRaceBucket{Bucket: store, loserCurrentRead: make(chan struct{}, 1), winnerCurrentSaved: make(chan struct{}), releaseLoserCurrent: make(chan struct{})}
	loserManager := NewManager(race, prefix, t.TempDir(), manager.configID)
	winnerManager := NewManager(race, prefix, t.TempDir(), manager.configID)
	loserCtx := context.WithValue(ctx, checkpointCostRaceRoleKey{}, "loser")
	winnerCtx := context.WithValue(ctx, checkpointCostRaceRoleKey{}, "winner")
	loserDone := make(chan error, 1)
	go func() { loserDone <- loserManager.PromoteCertifiedCurrent(loserCtx, loser) }()
	select {
	case <-race.loserCurrentRead:
	case <-ctx.Done():
		t.Fatalf("loser did not read CURRENT: %v", ctx.Err())
	}
	if err := winnerManager.PromoteCertifiedCurrent(winnerCtx, winner); err != nil {
		t.Fatal(err)
	}
	close(race.releaseLoserCurrent)
	if err := <-loserDone; !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("loser result=%v, want ErrStaleCheckpoint after conditional write loss", err)
	}
	if got := race.loserConflicts.Load(); got != 1 {
		t.Fatalf("loser conditional conflicts=%d, want 1", got)
	}
	fresh := NewManager(store, prefix, t.TempDir(), manager.configID)
	if err := fresh.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fresh.Latest(); got == nil || got.Index != winner.Index || got.RootHash != winner.RootHash {
		t.Fatalf("fresh CURRENT=%+v, want winner index=%d hash=%x", got, winner.Index, winner.RootHash)
	}
}

func measureCheckpointCostCASLoser(t *testing.T, ctx context.Context, bucket *objmetrics.MeteredBucket, server *versityfixture.Server, prefix string, manager *Manager, previousObjects map[string]int64) {
	t.Helper()
	current := manager.Latest()
	if current == nil {
		t.Fatal("checkpoint CAS-loser measurement requires an existing CURRENT winner")
	}
	claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost-cas-race", current.Index, time.Minute)
	if err != nil {
		t.Fatalf("acquire single active claim for CAS-race candidates: %v", err)
	}
	claimReleased := false
	defer func() {
		if !claimReleased {
			_ = manager.ReleasePublisherClaim(context.Background(), claim)
		}
	}()
	loserSources := []Source{source(t, RoleSQLite, "checkpoint-cost-cas-loser"), checkpointCostGraphSourceVariant(t, 1<<20, "checkpoint-cost-cas-loser")}
	winnerSources := []Source{source(t, RoleSQLite, "checkpoint-cost-cas-winner"), checkpointCostGraphSourceVariant(t, 1<<20, "checkpoint-cost-cas-winner")}
	before := bucket.Stats()
	loserRoot, err := manager.CreateFiles(ctx, claim, loserSources, current.Index+1)
	loserCreateStats := checkpointCostDelta(before, bucket.Stats())
	if err != nil {
		t.Fatalf("create distinct loser candidate: %v", err)
	}
	before = bucket.Stats()
	winnerRoot, err := manager.CreateFiles(ctx, claim, winnerSources, current.Index+2)
	winnerCreateStats := checkpointCostDelta(before, bucket.Stats())
	if err != nil {
		t.Fatalf("create distinct winner candidate: %v", err)
	}
	if loserCreateStats.AttemptedBytes == 0 || loserCreateStats.AttemptedBytes != loserCreateStats.PublishedBytes {
		t.Fatalf("loser candidate creation attempted=%d acknowledged=%d, want nonzero successful uploads", loserCreateStats.AttemptedBytes, loserCreateStats.PublishedBytes)
	}
	if winnerCreateStats.AttemptedBytes == 0 || winnerCreateStats.AttemptedBytes != winnerCreateStats.PublishedBytes {
		t.Fatalf("winner candidate creation attempted=%d acknowledged=%d, want nonzero successful uploads", winnerCreateStats.AttemptedBytes, winnerCreateStats.PublishedBytes)
	}

	tracer := &checkpointCostCASRaceBucket{Bucket: bucket, loserCurrentRead: make(chan struct{}, 1), winnerCurrentSaved: make(chan struct{}), releaseLoserCurrent: make(chan struct{})}
	loserManager := NewManager(tracer, prefix, t.TempDir(), manager.configID)
	winnerManager := NewManager(tracer, prefix, t.TempDir(), manager.configID)
	raceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	loserCtx := context.WithValue(raceCtx, checkpointCostRaceRoleKey{}, "loser")
	winnerCtx := context.WithValue(raceCtx, checkpointCostRaceRoleKey{}, "winner")
	loserDone := make(chan error, 1)
	loserStartStats, loserStartCalls := bucket.Stats(), accessRequestCount(t, server)
	go func() { loserDone <- loserManager.PromoteCertifiedCurrent(loserCtx, loserRoot) }()
	select {
	case <-tracer.loserCurrentRead:
	case <-raceCtx.Done():
		cancel()
		<-loserDone
		t.Fatalf("loser did not read old CURRENT before deadline: %v", raceCtx.Err())
	}
	loserBeforeRaceStats, loserBeforeRaceCalls := bucket.Stats(), accessRequestCount(t, server)
	loserPreparation := checkpointCostDelta(loserStartStats, loserBeforeRaceStats)
	assertServerRequestDelta(t, "checkpoint_cas_loser_preparation", loserBeforeRaceCalls-loserStartCalls, loserPreparation.HTTPRequests)

	winnerStartCalls := loserBeforeRaceCalls
	winnerErr := winnerManager.PromoteCertifiedCurrent(winnerCtx, winnerRoot)
	if winnerErr != nil {
		cancel()
		<-loserDone
		t.Fatalf("promote intended CURRENT winner: %v", winnerErr)
	}
	winnerAfterStats := bucket.Stats()
	winnerStats := checkpointCostDelta(loserBeforeRaceStats, winnerAfterStats)
	winnerEndCalls := accessRequestCount(t, server)
	assertServerRequestDelta(t, "checkpoint_cas_winner_publish", winnerEndCalls-winnerStartCalls, winnerStats.HTTPRequests)
	close(tracer.releaseLoserCurrent)

	loserErr := <-loserDone
	// The winner snapshot is the boundary: the losing GET remained held until
	// after the winner's successful conditional CURRENT update.
	loserStats := checkpointCostDelta(winnerAfterStats, bucket.Stats())
	loserEndCalls := accessRequestCount(t, server)
	assertServerRequestDelta(t, "checkpoint_cas_loser_publish", loserEndCalls-winnerEndCalls, loserStats.HTTPRequests)
	if !errors.Is(loserErr, ErrStaleCheckpoint) {
		t.Fatalf("losing CURRENT candidate error=%v, want stale after conditional conflict", loserErr)
	}
	if loserStats.ConditionConflicts != 1 || loserStats.HTTPFailures != 0 || loserStats.HTTP5xx != 0 || loserStats.TransportFailures != 0 {
		t.Fatalf("losing CURRENT candidate was not one expected conditional conflict: %+v", loserStats)
	}
	if got := tracer.loserConflicts.Load(); got != 1 {
		t.Fatalf("losing CURRENT conditional writes classified by bucket=%d, want 1", got)
	}
	if loserStats.AttemptedBytes == 0 || loserStats.PublishedBytes != 0 {
		t.Fatalf("losing CURRENT pointer attempted=%d acknowledged=%d, want attempted and not published", loserStats.AttemptedBytes, loserStats.PublishedBytes)
	}

	readback := NewManager(bucket, prefix, t.TempDir(), manager.configID)
	readbackBefore, readbackCalls := bucket.Stats(), accessRequestCount(t, server)
	if err := readback.Load(ctx); err != nil {
		t.Fatalf("fresh CURRENT readback after CAS race: %v", err)
	}
	readbackStats := checkpointCostDelta(readbackBefore, bucket.Stats())
	readbackEndCalls := accessRequestCount(t, server)
	assertServerRequestDelta(t, "checkpoint_cas_winner_readback", readbackEndCalls-readbackCalls, readbackStats.HTTPRequests)
	winner := readback.Latest()
	if winner == nil || winner.RootHash != winnerRoot.RootHash || winner.Index != winnerRoot.Index {
		t.Fatalf("fresh CURRENT winner=%+v, want index=%d hash=%x", winner, winnerRoot.Index, winnerRoot.RootHash)
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatalf("release single race claim: %v", err)
	}
	claimReleased = true

	reachableBefore, reachableCalls := bucket.Stats(), accessRequestCount(t, server)
	winnerReachable, err := checkpointReachableObjects(ctx, readback, winner)
	if err != nil {
		t.Fatalf("measure verified winner reachability: %v", err)
	}
	loserReachable, err := checkpointReachableObjects(ctx, readback, loserRoot)
	if err != nil {
		t.Fatalf("measure losing candidate reachability: %v", err)
	}
	reachabilityStats := checkpointCostDelta(reachableBefore, bucket.Stats())
	reachabilityEndCalls := accessRequestCount(t, server)
	assertServerRequestDelta(t, "checkpoint_cas_reachability", reachabilityEndCalls-reachableCalls, reachabilityStats.HTTPRequests)
	var winnerReachableBytes, winnerNewlyReachableBytes, winnerNewBlocksBytes, loserCandidateOnlyBlockBytes int64
	for key, size := range winnerReachable {
		winnerReachableBytes += size
		if _, existed := previousObjects[key]; !existed {
			winnerNewlyReachableBytes += size
			if strings.Contains(key, "/checkpoint/blocks/") {
				winnerNewBlocksBytes += size
			}
		}
	}
	for key, size := range loserReachable {
		if strings.Contains(key, "/checkpoint/blocks/") {
			if _, winnerReferences := winnerReachable[key]; !winnerReferences {
				loserCandidateOnlyBlockBytes += size
			}
		}
	}
	if loserCandidateOnlyBlockBytes == 0 || winnerNewBlocksBytes == 0 {
		t.Fatalf("candidate distinction missing: loser_only_blocks=%d winner_new_blocks=%d", loserCandidateOnlyBlockBytes, winnerNewBlocksBytes)
	}
	result, err := json.Marshal(map[string]any{
		"scenario": "checkpoint-current-cas-loser", "winner_outcome": "verified_by_fresh_current_readback",
		"winner_index": winner.Index, "winner_root_hash": fmt.Sprintf("%x", winner.RootHash),
		"loser_index": loserRoot.Index, "loser_error": loserErr.Error(),
		"loser_candidate_attempted_upload_bytes":     loserCreateStats.AttemptedBytes,
		"loser_candidate_acknowledged_upload_bytes":  loserCreateStats.PublishedBytes,
		"loser_current_pointer_attempted_bytes":      loserStats.AttemptedBytes,
		"loser_current_pointer_acknowledged_bytes":   loserStats.PublishedBytes,
		"loser_condition_conflicts":                  loserStats.ConditionConflicts,
		"winner_candidate_attempted_upload_bytes":    winnerCreateStats.AttemptedBytes,
		"winner_candidate_acknowledged_upload_bytes": winnerCreateStats.PublishedBytes,
		"winner_reachable_bytes":                     winnerReachableBytes,
		"winner_newly_reachable_bytes":               winnerNewlyReachableBytes,
		"winner_newly_reachable_block_bytes":         winnerNewBlocksBytes,
		"loser_candidate_only_block_bytes":           loserCandidateOnlyBlockBytes,
		"loser_preparation_stats":                    loserPreparation, "winner_publish_stats": winnerStats,
		"loser_publish_stats": loserStats, "winner_readback_stats": readbackStats,
		"reachability_stats": reachabilityStats,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CHECKPOINT_COST %s", result)
}

func TestCheckpointCostPublisherClaimLifetime(t *testing.T) {
	ctx := context.Background()
	sources := []Source{source(t, RoleSQLite, "checkpoint-cost"), source(t, RoleGraphData, "graph")}

	t.Run("release before promotion is fenced", func(t *testing.T) {
		manager := NewManager(objstore.NewInMemBucket(), "checkpoint-cost-early-release", t.TempDir(), 1)
		claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.CreateFiles(ctx, claim, sources, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); !errors.Is(err, ErrPublisherFenced) {
			t.Fatalf("promotion after releasing claim=%v, want ErrPublisherFenced", err)
		}
	})

	t.Run("promote before release succeeds", func(t *testing.T) {
		manager := NewManager(objstore.NewInMemBucket(), "checkpoint-cost-valid-lifetime", t.TempDir(), 1)
		claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.CreateFiles(ctx, claim, sources, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
			t.Fatalf("promote with active claim: %v", err)
		}
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatalf("release after promotion: %v", err)
		}
	})
}

func checkpointCostGraphSource(t *testing.T, size int64) Source {
	return checkpointCostGraphSourceVariant(t, size, "rhiza-checkpoint-cost")
}

func checkpointCostGraphSourceVariant(t *testing.T, size int64, variant string) Source {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "graph-data-*.db")
	if err != nil {
		t.Fatal(err)
	}
	pattern := []byte(variant + "\n")
	chunk := bytes.Repeat(pattern, max(1, 65536/len(pattern)))
	for written := int64(0); written < size; {
		part := chunk
		if int64(len(part)) > size-written {
			part = part[:size-written]
		}
		n, err := file.Write(part)
		written += int64(n)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return Source{Role: RoleGraphData, Path: file.Name()}
}

func checkpointReachableObjects(ctx context.Context, manager *Manager, root *Checkpoint) (map[string]int64, error) {
	objects := make(map[string]int64)
	if root == nil {
		return objects, nil
	}
	rootKey := manager.key(rootName(root.Index, root.RootHash))
	rootAttributes, err := manager.bucket.Attributes(ctx, rootKey)
	if err != nil {
		return nil, err
	}
	objects[rootKey] = rootAttributes.Size
	for _, file := range root.Files {
		for _, block := range file.Blocks {
			key := manager.key(blockObjectKey(block))
			if _, exists := objects[key]; exists {
				continue
			}
			attributes, err := manager.bucket.Attributes(ctx, key)
			if err != nil {
				return nil, err
			}
			objects[key] = attributes.Size
		}
	}
	return objects, nil
}

func sumReachable(objects map[string]int64) int64 {
	var total int64
	for _, size := range objects {
		total += size
	}
	return total
}

type checkpointCostStats struct {
	ReplayGroupingEnabled        bool   `json:"replay_grouping_enabled"`
	Uploads                      uint64 `json:"uploads"`
	Gets                         uint64 `json:"gets"`
	Lists                        uint64 `json:"lists"`
	Heads                        uint64 `json:"heads"`
	Deletes                      uint64 `json:"deletes"`
	Failures                     uint64 `json:"failures"`
	HTTPRequests                 uint64 `json:"http_requests"`
	HTTPFailures                 uint64 `json:"http_failures"`
	ConditionConflicts           uint64 `json:"condition_conflicts"`
	TransportFailures            uint64 `json:"transport_failures"`
	HTTP5xx                      uint64 `json:"http_5xx"`
	HTTPGet                      uint64 `json:"http_get_requests"`
	HTTPPut                      uint64 `json:"http_put_requests"`
	HTTPHead                     uint64 `json:"http_head_requests"`
	HTTPDelete                   uint64 `json:"http_delete_requests"`
	AttemptedBytes               uint64 `json:"attempted_bytes"`
	PublishedBytes               uint64 `json:"published_bytes"`
	DownloadedBytes              uint64 `json:"downloaded_bytes"`
	Retries                      uint64 `json:"retries"`
	RetryMetadataKnown           uint64 `json:"retry_metadata_known_requests"`
	RetryMetadataUnknown         uint64 `json:"retry_metadata_unknown_requests"`
	RequestBodyBytes             uint64 `json:"request_body_bytes_consumed"`
	ResponseBodyBytes            uint64 `json:"response_body_bytes_consumed"`
	ObservedRequestIdentities    uint64 `json:"observed_request_identities"`
	ObservedRequestRepeats       uint64 `json:"observed_request_repeats"`
	RequestGroupingUnknown       uint64 `json:"request_grouping_unknown"`
	ReplayTrackerCapacityMisses  uint64 `json:"replay_tracker_capacity_misses"`
	ReplayIdentityCapacityMisses uint64 `json:"replay_identity_capacity_misses"`
	ReplayIncompleteOperations   uint64 `json:"replay_incomplete_operations"`
	ReplayTrackedOperationsStart uint64 `json:"replay_tracked_operations_start"`
	ReplayTrackedOperationsEnd   uint64 `json:"replay_tracked_operations_end"`
	ReplayOpenReadersStart       uint64 `json:"replay_open_readers_start"`
	ReplayOpenReadersEnd         uint64 `json:"replay_open_readers_end"`
}

func checkpointCostDelta(before, after objmetrics.Stats) checkpointCostStats {
	return checkpointCostStats{
		ReplayGroupingEnabled: after.ReplayGroupingEnabled,
		Uploads:               after.Uploads - before.Uploads, Gets: after.Gets - before.Gets,
		Lists: after.Lists - before.Lists, Heads: after.Heads - before.Heads,
		Deletes: after.Deletes - before.Deletes, Failures: after.Failures - before.Failures,
		HTTPRequests: after.HTTPRequests - before.HTTPRequests, HTTPFailures: after.HTTPFailures - before.HTTPFailures,
		ConditionConflicts: after.ConditionConflicts - before.ConditionConflicts,
		TransportFailures:  after.TransportFailures - before.TransportFailures,
		HTTP5xx:            after.HTTP5xx - before.HTTP5xx,
		HTTPGet:            after.HTTPGetRequests - before.HTTPGetRequests, HTTPPut: after.HTTPPutRequests - before.HTTPPutRequests,
		HTTPHead: after.HTTPHeadRequests - before.HTTPHeadRequests, HTTPDelete: after.HTTPDeleteRequests - before.HTTPDeleteRequests,
		AttemptedBytes: after.BytesUploaded - before.BytesUploaded, PublishedBytes: after.BytesPublished - before.BytesPublished,
		DownloadedBytes: after.BytesDownloaded - before.BytesDownloaded, Retries: after.SDKRetries - before.SDKRetries,
		RetryMetadataKnown:           after.RetryMetadataRequests - before.RetryMetadataRequests,
		RetryMetadataUnknown:         after.RetryMetadataUnknownRequests - before.RetryMetadataUnknownRequests,
		RequestBodyBytes:             after.HTTPRequestBodyBytes - before.HTTPRequestBodyBytes,
		ResponseBodyBytes:            after.HTTPResponseBodyBytes - before.HTTPResponseBodyBytes,
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
	}
}

func accessRequestCount(t *testing.T, server *versityfixture.Server) uint64 {
	t.Helper()
	records, err := server.AccessRecords()
	if err != nil {
		t.Fatalf("read Versity access records: %v", err)
	}
	return versityfixture.RequestCount(records)
}

func assertServerRequestDelta(t *testing.T, phase string, serverCalls, clientAttempts uint64) {
	t.Helper()
	if serverCalls != clientAttempts {
		t.Fatalf("%s server requests=%d, client transport attempts=%d", phase, serverCalls, clientAttempts)
	}
}
