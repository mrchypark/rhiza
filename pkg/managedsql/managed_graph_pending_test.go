//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func managedGraphCertifiedCounts(t *testing.T, ctx context.Context, cfg Config, id string) (int, int) {
	t.Helper()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	identity := node.New(&cfg.Runtime)
	physicalBinding, _, err := identity.PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal("derive exact trusted profile binding for peer observer", err)
	}
	bootstrapDigest, err := identity.ManagedBootstrapPlanDigestForProtocol(physicalBinding, cfg.Plan.EvidenceFormat, cfg.Plan.ApplicationProtocol)
	if err != nil || bootstrapDigest != cfg.Plan.BootstrapPlanDigest {
		t.Fatalf("peer observer profile differs from exact public plan: digest=%q plan=%q err=%v", bootstrapDigest, cfg.Plan.BootstrapPlanDigest, err)
	}
	profile := network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: bootstrapDigest, Generation: int64(cfg.Plan.Generation)}
	peer, err := network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	tip, err := peer.ReadTip(ctx, cfg.Runtime.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	total, original := 0, 0
	for from := quepaxa.Slot(1); from <= tip; {
		page, err := peer.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("actual Graph certified count page unavailable", err)
		}
		for _, value := range page.Decisions {
			if value.Slot > tip {
				break
			}
			command, graph, err := types.DecodeManagedGraphExecution(value.Value)
			if err != nil {
				t.Fatal(err)
			}
			if graph {
				total++
				if command.RequestID == id {
					original++
				}
			}
			from = value.Slot + 1
		}
	}
	return total, original
}

func TestManagedGraphPublicRecoveryHealsPendingWithoutProposal(t *testing.T) {
	testManagedGraphPublicPending(t, "recover")
}

func TestManagedGraphPublicLinearizableReadHealsPendingWithoutProposal(t *testing.T) {
	testManagedGraphPublicPending(t, "read")
}

func TestManagedGraphPublicNewWriteHealsPendingWithoutResubmission(t *testing.T) {
	testManagedGraphPublicPending(t, "write")
}

func TestManagedGraphPublicRestartHealsCertifiedNativePending(t *testing.T) {
	testManagedGraphPublicPending(t, "restart")
}

func testManagedGraphPublicPending(t *testing.T, mode string) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	hosts := make([]*Host, len(configs))
	stopRuntime := make([]context.CancelFunc, len(configs))
	for i, cfg := range configs {
		if mode == "restart" {
			cfg.Runtime.CheckpointInterval = 200 * time.Millisecond
			configs[i] = cfg
		}
		openCtx, stop := context.WithCancel(ctx)
		stopRuntime[i] = stop
		t.Cleanup(stop)
		h, err := fixture.open(openCtx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	for _, h := range hosts {
		tick := time.NewTicker(20 * time.Millisecond)
		for {
			if err := h.Ready(ctx); err == nil {
				break
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				tick.Stop()
				t.Fatal("actual public initial readiness", ctx.Err())
			}
		}
		tick.Stop()
	}
	if mode == "restart" {
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
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			_, found, err := cp.LoadManagedGraphLocator(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				break
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				t.Fatal("actual product root required before native-pending restart", ctx.Err())
			}
		}
	}
	target := configs[1].Runtime.NodeID
	push := make(chan uint64, 64)
	background, native, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var backgroundOnce, nativeOnce, releaseOnce sync.Once
	var held, cancelAtNative atomic.Bool
	observation, cancelObservation := context.WithCancel(ctx)
	defer cancelObservation()
	restore := localtesthooks.Set(func(event string) {
		if slot, matched := managedGraphLearnedSlot(event, string(target)); held.Load() && matched {
			select {
			case push <- slot:
			case <-ctx.Done():
				return
			}
			<-release
		}
		if held.Load() && event == "node:catchup-peer:phase=before-local-replay:node="+string(target) {
			backgroundOnce.Do(func() { close(background) })
			<-release
		}
		if cancelAtNative.Load() && event == "materializer:graph:after-native-before-sqlite" {
			nativeOnce.Do(func() { close(native); cancelObservation() })
		}
	})
	t.Cleanup(func() { held.Store(false); releaseOnce.Do(func() { close(release) }); restore() })
	held.Store(true)
	select {
	case <-background:
	case <-ctx.Done():
		t.Fatal("actual target periodic replay did not reach its barrier")
	}
	original := types.GraphCommand{RequestID: "\xff-pending", Cypher: "CREATE (:PendingGraph {value: 'original'})"}
	ack, err := hosts[0].GraphExecute(ctx, original)
	if err != nil || ack.Slot == 0 || ack.Status != types.MutationCommitted {
		t.Fatal("actual source Graph ACK", ack, err)
	}
	query := network.GraphQueryRequest{Cypher: "MATCH (n:PendingGraph) RETURN n.value", Consistency: "linearizable"}
	if rows, err := hosts[2].GraphQuery(ctx, query); err != nil || len(rows.Rows) != 1 {
		t.Fatal("other actual voter has not applied the original", err)
	}
	before, err := hosts[1].node.ManagedState()
	originalDelivered := false
	for {
		if err != nil || before.AppliedTip >= ack.Slot {
			t.Fatal("actual target certified/apply gap escaped its barrier", before, err)
		}
		if originalDelivered && before.CertifiedTip >= ack.Slot {
			break
		}
		select {
		case slot := <-push:
			originalDelivered = originalDelivered || slot == ack.Slot
			before, err = hosts[1].node.ManagedState()
		case <-ctx.Done():
			t.Fatal("exact original Graph certificate/prefix was not observed", ctx.Err())
		}
	}
	// Both other replicas have applied this command. No new application command
	// is submitted after this point, so the armed native boundary is target1.
	cancelAtNative.Store(true)
	_, err = hosts[1].GraphQuery(observation, query)
	cancelAtNative.Store(false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("actual native commit/SQLite cancellation missing: %v", err)
	}
	select {
	case <-native:
	default:
		t.Fatal("native commit boundary was never reached")
	}
	lag, err := hosts[1].node.ManagedState()
	if err != nil || lag.AppliedTip != before.AppliedTip || lag.CertifiedTip < ack.Slot {
		t.Fatal("actual SQLite rollback/runtime replay eligibility changed", lag, err)
	}
	pendingObservation := func(err error) bool {
		// A complete native commit clears its native per-command pending flag;
		// the following SQLite rollback leaves an explicit cross-store tip gap.
		// Both are precise pending observations, not arbitrary startup errors.
		return errors.Is(err, materializer.ErrGraphApplyPending) || err != nil && strings.HasSuffix(err.Error(), fmt.Sprintf("graph materializer tip %d does not match SQLite tip %d", ack.Slot, lag.AppliedTip))
	}
	if _, err := hosts[1].GraphQuery(ctx, network.GraphQueryRequest{Cypher: query.Cypher, Consistency: "local"}); !pendingObservation(err) {
		t.Fatalf("public local observation did not reject actual pending projection: %v", err)
	}
	selector := network.RequestStatusRequest{Kind: "graph", RequestID: original.RequestID}
	if _, err := hosts[1].RequestStatus(ctx, selector); !pendingObservation(err) {
		t.Fatalf("public status did not reject actual pending projection: %v", err)
	}
	count, own := managedGraphCertifiedCounts(t, ctx, configs[0], original.RequestID)
	if mode == "read" {
		rows, err := hosts[1].GraphQuery(ctx, query)
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "original" {
			t.Fatalf("actual public linearizable read did not heal certified pending: rows=%+v error=%v", rows, err)
		}
		status, err := hosts[1].RequestStatus(ctx, selector)
		if err != nil || status.Receipt == nil || *status.Receipt != ack.MutationReceipt {
			t.Fatalf("linearizable read changed original receipt: status=%+v error=%v", status, err)
		}
	} else if mode == "write" {
		next := types.GraphCommand{RequestID: "\xfe-after-pending", Cypher: "CREATE (:AfterPendingGraph {value: 'new'})"}
		result, err := hosts[1].GraphExecute(ctx, next)
		if err != nil || result.Status != types.MutationCommitted || result.Slot <= ack.Slot {
			t.Fatalf("new public write did not heal prior certified pending: receipt=%+v error=%v", result, err)
		}
		rows, err := hosts[1].GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:AfterPendingGraph) RETURN n.value", Consistency: "local"})
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "new" {
			t.Fatalf("new public write effect missing or duplicated: rows=%+v error=%v", rows, err)
		}
		_, nextCount := managedGraphCertifiedCounts(t, ctx, configs[1], next.RequestID)
		if nextCount != 1 {
			t.Fatalf("new public write certified %d times", nextCount)
		}
	} else if mode == "restart" {
		// Stop the actual target runtime before releasing its blocked replay.
		// The cancelled background/peer calls cannot silently heal the fixture.
		stopRuntime[1]()
		held.Store(false)
		releaseOnce.Do(func() { close(release) })
		if err := hosts[1].Close(); err != nil {
			t.Fatal(err)
		}
		observed, err := materializer.InspectManagedGraphInstallTarget(ctx, filepath.Join(configs[1].Runtime.DataDir, "sqlite.db"), types.DefaultIdempotencyWindowSlots, configs[1].Plan.ApplicationProtocol)
		if err != nil || len(observed.Replay) == 0 {
			t.Fatalf("restart fixture did not preserve actual native pending: state=%+v error=%v", observed, err)
		}
		reopened, err := fixture.open(ctx, configs[1])
		if reopened != nil {
			t.Cleanup(func() { _ = reopened.Close() })
		}
		if err != nil {
			t.Fatal("public startup failed to authenticate/reconcile native pending", err)
		}
		hosts[1] = reopened
		waitGraphHostPublicReady(t, ctx, reopened)
		status, err := reopened.RequestStatus(ctx, selector)
		if err != nil || status.Receipt == nil || *status.Receipt != ack.MutationReceipt {
			t.Fatalf("public restarted pending lost original receipt: status=%+v error=%v", status, err)
		}
	} else {
		resolved, err := hosts[1].RecoverRequest(ctx, selector)
		if err != nil || resolved.Graph == nil || resolved.Graph.MutationReceipt != ack.MutationReceipt || string(resolved.RequestID) != original.RequestID {
			t.Fatalf("actual public pending recovery failed: result=%+v error=%v", resolved, err)
		}
	}
	afterCount, afterOwn := managedGraphCertifiedCounts(t, ctx, configs[0], original.RequestID)
	wantCount := count
	if mode == "write" {
		wantCount++
	}
	if wantCount != afterCount || own != 1 || afterOwn != 1 {
		t.Fatalf("recovery proposed Graph application: total %d->%d original %d->%d", count, afterCount, own, afterOwn)
	}
	rows, err := hosts[1].GraphQuery(ctx, network.GraphQueryRequest{Cypher: query.Cypher, Consistency: "local"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "original" {
		t.Fatalf("recovery duplicated/lost original native effect: rows=%+v error=%v", rows, err)
	}
	t.Logf("actual pending slot=%d healed with Graph application count %d->%d", ack.Slot, count, afterCount)
}

func managedGraphLearnedSlot(event, target string) (uint64, bool) {
	if !strings.HasPrefix(event, "network:learned:phase=apply-start:node="+target+":sender=") {
		return 0, false
	}
	index := strings.LastIndex(event, ":slot=")
	if index < 0 {
		return 0, false
	}
	slot, err := strconv.ParseUint(event[index+6:], 10, 64)
	return slot, err == nil && slot != 0
}

func TestManagedGraphPendingLearnedSlotMatchesExactTarget(t *testing.T) {
	for _, tc := range []struct {
		event string
		want  bool
	}{
		{"network:learned:phase=apply-start:node=target:sender=peer:slot=8", true},
		{"network:learned:phase=apply-start:node=target:sender=peer:slot=7", false},
		{"network:learned:phase=apply-start:node=other:sender=peer:slot=8", false},
		{"network:learned:phase=accept-start:node=target:sender=peer:slot=8", false},
		{"network:learned:phase=apply-start:node=target:sender=peer:slot=0", false},
		{"network:learned:phase=apply-start:node=target:sender=peer:slot=8junk", false},
	} {
		slot, matched := managedGraphLearnedSlot(tc.event, "target")
		if (matched && slot == 8) != tc.want {
			t.Fatalf("Learned event matched wrong actual target/slot: %q", tc.event)
		}
	}
}
