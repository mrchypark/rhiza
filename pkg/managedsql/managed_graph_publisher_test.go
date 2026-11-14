package managedsql

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Public Open/admission starts the product publisher. No test calls the
// publisher directly or creates a claim/candidate/seal on its behalf.
func TestManagedGraphPublicPublisherCreatesCertifiedPairAndSuffix(t *testing.T) {
	testManagedGraphPublicPublisherWithProtocol(t, false, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
}

func TestManagedGraphPublicRestartInstallsCertifiedPairAndSuffix(t *testing.T) {
	testManagedGraphPublicPublisherWithProtocol(t, true, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
}

func waitGraphHostPublicReady(t *testing.T, ctx context.Context, h *Host) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := h.Ready(ctx); err == nil {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("public restarted readiness did not complete", ctx.Err())
		}
	}
}

func testManagedGraphPublicPublisherWithProtocol(t *testing.T, restart bool, protocol string) {
	configs, fixture := sqlClusterForProtocol(t, protocol)
	var holdNewRoots atomic.Bool
	rootRelease := make(chan struct{})
	var releaseRoots sync.Once
	if restart {
		// The existing S3 wire gate is armed only after a real product root.
		// Subsequent root JSON uploads cannot certify a root containing B;
		// archive extents/HEAD and before-ACK durability remain unrestricted.
		fixture.source.putReady = make(chan string, 16)
		fixture.source.putRelease = rootRelease
		fixture.source.putSuffix = ".json"
		fixture.source.putPrefix = "/checkpoint/roots/"
		fixture.source.putEnabled = &holdNewRoots
		t.Cleanup(func() { holdNewRoots.Store(false); releaseRoots.Do(func() { close(rootRelease) }) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	hosts := make([]*Host, len(configs))
	for i := range configs {
		// Existing product controls, shortened only to observe the real worker.
		configs[i].Runtime.CheckpointInterval = 200 * time.Millisecond
		h, err := fixture.open(ctx, configs[i])
		if h != nil {
			t.Cleanup(func() { _ = fixture.closeHost(h) })
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	for _, h := range hosts {
		ready := time.NewTicker(30 * time.Millisecond)
		for {
			if err := h.Ready(ctx); err == nil {
				break
			}
			select {
			case <-ready.C: // Public readiness observation only; no private Admit.
			case <-ctx.Done():
				ready.Stop()
				t.Fatal("public initial readiness did not complete", ctx.Err())
			}
		}
		ready.Stop()
	}
	first, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "publisher-first", Cypher: "CREATE (:PublishedGraph {id: 'first'})"})
	if err != nil || first.Slot == 0 {
		t.Fatal("actual first Graph ACK", first, err)
	}
	cfg := configs[0].Runtime
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: cfg.ObjStoreEndpoint, Bucket: cfg.ObjStoreBucket, Region: cfg.ObjStoreRegion, Insecure: cfg.ObjStoreInsecure, AccessKey: cfg.ObjStoreAccessKey, SecretKey: cfg.ObjStoreSecretKey}, fixture.source.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, fixture.source.finalPrefix, t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	var locator checkpoint.ManagedGraphLocator
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		candidate, found, err := cp.LoadManagedGraphLocator(ctx)
		if err != nil {
			t.Fatal("actual product locator read failed", err)
		}
		if found && candidate.Index >= first.Slot {
			locator = candidate
			break
		}
		select {
		case <-tick.C: // Read-only observation, not a publication retry.
		case <-ctx.Done():
			t.Fatal("actual product publisher did not create a root", ctx.Err())
		}
	}
	root, err := cp.OpenRoot(ctx, locator.Index, locator.RootHash)
	if err != nil || len(root.Files) != 2 || root.Files[0].Role != checkpoint.RoleSQLite || root.Files[1].Role != checkpoint.RoleGraphData {
		t.Fatal("actual product paired root unavailable", err)
	}
	if restart {
		holdNewRoots.Store(true)
	}
	later, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "publisher-suffix", Cypher: "CREATE (:PublishedGraph {id: 'suffix'})"})
	if err != nil || later.Slot <= locator.SealDecisionSlot {
		t.Fatal("actual nonempty certified suffix absent", later, err)
	}
	for _, h := range hosts {
		stats, available := h.node.ObjectStoreStats()
		if !available || stats.Deletes != 0 {
			t.Fatalf("publisher used provider Delete: available=%t count=%d", available, stats.Deletes)
		}
	}
	// Close/drain one real publisher and keep the other two voters available.
	if err := fixture.closeHost(hosts[0]); err != nil {
		t.Fatal(err)
	}
	if rows, err := hosts[1].GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:PublishedGraph) RETURN n.id", Consistency: "linearizable"}); err != nil || len(rows.Rows) != 2 {
		t.Fatal("remaining quorum did not retain both actual Graph effects", err)
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(cfg.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	verifier, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "publisher-readonly-verifier", Cluster: quepaxa.Cluster{ConfigID: 1, Members: cfg.Members}, WAL: wal, EnableReconfiguration: cfg.EnableReconfiguration, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, fixture.source.finalPrefix, 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, base := archive.RecoveryBase(); base || verifier.CompactionFloor() != 0 {
		t.Fatal("product publisher trimmed archive/WAL")
	}
	decision, tip, err := archive.ObserveManagedGraphDecision(ctx, quepaxa.Slot(locator.SealDecisionSlot))
	seal, decoded, decodeErr := quepaxa.DecodeCheckpointSeal(decision.Value)
	if err != nil || decodeErr != nil || !decoded || tip < quepaxa.Slot(later.Slot) || decision.Hash != locator.SealDecisionHash || sha256.Sum256(decision.Value) != decision.Hash || seal.RootHash != locator.RootHash || uint64(seal.Index) != locator.Index {
		t.Fatal("actual archive seal/suffix did not bind locator", err, decodeErr)
	}
	verifier.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := verifier.ValidateCheckpointBase(ctx, seal, decision); err != nil {
		t.Fatal("actual producer root lacks trusted certified authority", err)
	}
	if err := bucket.Iter(ctx, fixture.source.finalPrefix+"/checkpoint/", func(key string) error {
		if strings.HasSuffix(key, "/PUBLISHER") || strings.Contains(key, "/recovery-pins/") {
			t.Errorf("product publisher created a claim/GC pin: %s", key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual public product pair cut=%d seal=%d suffix=%d archiveBase0/WALfloor0", locator.Index, locator.SealDecisionSlot, later.Slot)
	if restart {
		selected, exists, err := cp.LoadManagedGraphLocator(ctx)
		if err != nil || !exists || selected.Index >= later.Slot {
			t.Fatal("actual selected locator does not leave Graph B in certified suffix", selected, err)
		}
		selectedDecision, selectedTip, err := archive.ObserveManagedGraphDecision(ctx, quepaxa.Slot(selected.SealDecisionSlot))
		selectedSeal, decoded, decodeErr := quepaxa.DecodeCheckpointSeal(selectedDecision.Value)
		if err != nil || decodeErr != nil || !decoded || selectedDecision.Hash != selected.SealDecisionHash || sha256.Sum256(selectedDecision.Value) != selectedDecision.Hash || uint64(selectedSeal.Index) != selected.Index || selectedSeal.RootHash != selected.RootHash || selectedTip < quepaxa.Slot(later.Slot) {
			t.Fatal("selected locator does not bind exact certified slot/hash/root", err, decodeErr)
		}
		if err := verifier.ValidateCheckpointBase(ctx, selectedSeal, selectedDecision); err != nil {
			t.Fatal("selected locator lacks real certificate and paired root authority", err)
		}
		// The root-upload gate prevents any candidate containing Graph B from
		// being certified. Check every seal in the fixed loaded archive as well;
		// startup must not silently select a different root that absorbed B.
		for from := quepaxa.Slot(1); from <= archive.Tip(); {
			page, _, err := archive.DecisionsFrom(ctx, from, 128)
			if err != nil || len(page) == 0 {
				t.Fatal("captured certified suffix is incomplete", err)
			}
			for _, value := range page {
				candidate, isSeal, err := quepaxa.DecodeCheckpointSeal(value.Value)
				if err != nil || isSeal && uint64(candidate.Index) >= later.Slot {
					t.Fatal("a certified root absorbed the required Graph suffix", err)
				}
				from = value.Slot + 1
			}
		}
		// Release the read-only inspection handle before the public opener
		// acquires this same registered WAL. Never run two WAL owners together.
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		dbPath := filepath.Join(cfg.DataDir, "sqlite.db")
		oldMain, err := os.Stat(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		reopened, err := fixture.open(ctx, configs[0])
		if reopened != nil {
			t.Cleanup(func() { _ = fixture.closeHost(reopened) })
		}
		if err != nil {
			t.Fatal("actual public paired reconstruction startup", err)
		}
		waitGraphHostPublicReady(t, ctx, reopened)
		newMain, err := os.Stat(dbPath)
		if err != nil || os.SameFile(oldMain, newMain) {
			t.Fatal("public startup did not install a fresh pair at the actual dbPath", err)
		}
		for _, expected := range []struct {
			id     string
			result network.GraphExecuteResponse
		}{{"publisher-first", first}, {"publisher-suffix", later}} {
			status, err := reopened.RequestStatus(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: expected.id})
			if err != nil || status.Receipt == nil || *status.Receipt != expected.result.MutationReceipt {
				t.Fatalf("public installed pair lost exact original receipt: got=%+v error=%v", status, err)
			}
		}
		rows, err := reopened.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:PublishedGraph) RETURN n.id", Consistency: "local"})
		if err != nil || len(rows.Rows) != 2 || rows.AppliedSlot < later.Slot {
			t.Fatal("public installed pair omitted nonempty certified suffix", rows, err)
		}
		t.Logf("actual public restart installed fresh sqlite inode at original dbPath; paired C=%d certified suffix ACK=%d applied=%d", locator.Index, later.Slot, rows.AppliedSlot)
	}
}
