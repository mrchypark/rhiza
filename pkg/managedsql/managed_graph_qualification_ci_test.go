//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
)

// Actual three-Host/Core/WAL/native/S3-SDK regression. The HTTP server is the
// existing synthetic wire fixture, not Floci/provider parity or a Rust test.
func TestManagedGraphActualUnknownRetainsNativeOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	var armed, outage atomic.Bool
	var kubeRequests atomic.Int64
	entered, release := make(chan string, 8), make(chan struct{})
	var releaseOnce sync.Once
	fixture.source.putReady, fixture.source.putRelease = entered, release
	fixture.source.putSuffix, fixture.source.putEnabled = "archive/head.bin", &armed
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		// Observe the real product publisher within this bounded fixture; the
		// default production interval is not a 25-second test precondition.
		cfg.Runtime.CheckpointInterval = 200 * time.Millisecond
		kube := *cfg.Store.Kube
		client := *kube.Client
		base := client.Transport
		client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
			kubeRequests.Add(1)
			if outage.Load() {
				return nil, errors.New("test: Kubernetes unavailable in normal observation window")
			}
			return base.RoundTrip(r)
		})
		kube.Client = &client
		cfg.Store = &operator.V1Alpha2StatusStore{Kube: &kube}
		configs[i] = cfg
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	for _, h := range hosts {
		awaitPublicInternalReady(t, ctx, h)
	}
	rootTick := time.NewTicker(20 * time.Millisecond)
	defer rootTick.Stop()
	for {
		_, err := hosts[0].ObserveManagedGraphQualification(ctx, "", nil)
		if err == nil {
			break
		}
		if !errors.Is(err, network.ErrNotReady) {
			t.Fatal("initial product root validation failed before measurement", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial product root unavailable before archive gate", ctx.Err())
		case <-rootTick.C:
		}
	}
	xctx, cancelX := context.WithCancel(ctx)
	var callers sync.WaitGroup
	t.Cleanup(func() {
		armed.Store(false)
		releaseOnce.Do(func() { close(release) })
		cancelX()
		callers.Wait()
		outage.Store(false)
	})
	id := "\xff-actual-unknown"
	request := types.GraphCommand{RequestID: id, Cypher: "CREATE (:UnknownCI {id:'X', value:7})", Events: []types.GraphStreamEvent{{Stream: "unknown-ci", Kind: "X", Payload: map[string]any{"n": int64(7)}}}}
	canonical, err := types.EncodeManagedGraphOriginal(request)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		response network.GraphExecuteResponse
		err      error
	}
	done := make(chan outcome, 1)
	kubeRequests.Store(0)
	outage.Store(true)
	armed.Store(true)
	callers.Add(1)
	go func() {
		defer callers.Done()
		result, err := hosts[0].GraphExecute(xctx, request)
		done <- outcome{result, err}
	}()
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("Graph call completed before the real archive gate: %+v %v", result.response, result.err)
	case <-ctx.Done():
		t.Fatal("archive gate not reached", ctx.Err())
	}
	var slot uint64
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for slot == 0 {
		original, found, err := hosts[0].node.RetainedRequest(ctx, "graph", id)
		if err == nil && found {
			if original.Graph == nil || !bytes.Equal(original.CanonicalOriginal, canonical) || original.Receipt.Status != types.MutationCommitted || !original.Receipt.Applied {
				t.Fatal("actual native original differs before caller cancellation")
			}
			slot = original.Slot
			break
		}
		select {
		case result := <-done:
			t.Fatalf("Graph returned before applied original: %+v %v", result.response, result.err)
		case <-ctx.Done():
			t.Fatal("actual native original not applied before cancellation", err)
		case <-tick.C:
		}
	}
	cancelX()
	var result outcome
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal("canceled Graph caller did not complete", ctx.Err())
	}
	var unknown *network.CommitUnknownError
	if !errors.As(result.err, &unknown) || !errors.Is(result.err, context.Canceled) || unknown.RequestID != id || unknown.Slot != 0 && uint64(unknown.Slot) != slot {
		t.Fatalf("actual Graph call did not retain UNKNOWN: %+v %v", result.response, result.err)
	}
	context, ok := MutationErrorContext(result.err)
	if !ok || context.Kind != "graph" || string(context.RequestID) != id || context.Slot != uint64(unknown.Slot) {
		t.Fatal("actual UNKNOWN lost its raw selector/context")
	}
	armed.Store(false)
	releaseOnce.Do(func() { close(release) })
	y, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "Y", Cypher: "CREATE (:UnknownCI {id:'Y', value:8})"})
	if err != nil || y.Status != types.MutationCommitted || y.Slot <= slot {
		t.Fatalf("UNKNOWN globally blocked healthy neighbor: %+v %v", y, err)
	}
	before, own := managedGraphCertifiedCounts(t, ctx, configs[0], id)
	var recoveryAttempts atomic.Int64
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		// Classify synchronously using the actual decoder; do not retain or log
		// the borrowed application bytes. Control proposals are not counted.
		if QualificationApplicationKind(value) != "" {
			recoveryAttempts.Add(1)
		}
	})
	defer restoreProposal()
	recovered, err := hosts[0].RecoverRequest(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: id})
	if err != nil || recovered.Graph == nil || recovered.Graph.Slot != slot || !recovered.Graph.Applied || string(recovered.RequestID) != id {
		t.Fatalf("exact native original recovery differs: %+v %v", recovered, err)
	}
	after, afterOwn := managedGraphCertifiedCounts(t, ctx, configs[0], id)
	if before != after || own != 1 || afterOwn != 1 {
		t.Fatalf("Graph certified application count changed during recovery: %d->%d own=%d->%d", before, after, own, afterOwn)
	}
	if recoveryAttempts.Load() != 0 {
		t.Fatalf("exact recovery entered Graph proposal %d times", recoveryAttempts.Load())
	}
	actual, found, err := hosts[0].node.RetainedRequest(ctx, "graph", id)
	if err != nil || !found || actual.Slot != slot || !bytes.Equal(actual.CanonicalOriginal, canonical) {
		t.Fatal("canonical original/first slot changed after recovery", err)
	}
	rows, err := hosts[0].GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:UnknownCI) RETURN n.id,n.value ORDER BY n.id", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][0] != "X" || rows.Rows[1][0] != "Y" {
		t.Fatal("actual Graph X/Y effects differ", err)
	}
	if kubeRequests.Load() != 0 {
		t.Fatalf("normal operations requested Kubernetes %d times", kubeRequests.Load())
	}
	var httpRequests uint64
	for _, h := range hosts {
		stats, ok := h.node.ObjectStoreStats()
		if !ok || stats.Deletes != 0 || stats.HTTPDeleteRequests != 0 {
			t.Fatalf("actual SDK Delete observation invalid: available=%v logical=%d HTTP=%d", ok, stats.Deletes, stats.HTTPDeleteRequests)
		}
		httpRequests += stats.HTTPRequests
	}
	if httpRequests == 0 {
		t.Fatal("actual SDK HTTP observation was never exercised")
	}
	// Reopen the same owned DataDir with no in-memory hint. This is not an
	// empty-disk/Pod replacement claim. Startup control may use Kubernetes;
	// normal observations after actual fresh admission must not use it.
	outage.Store(false)
	if err := hosts[0].Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixture.open(ctx, configs[0])
	if reopened != nil {
		t.Cleanup(func() { _ = reopened.Close() })
	}
	if err != nil {
		t.Fatal("fresh Host same-DataDir reopen failed", err)
	}
	awaitPublicInternalReady(t, ctx, reopened)
	kubeRequests.Store(0)
	outage.Store(true)
	if _, err := reopened.RecoverPendingDetailed(ctx); !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatal("fresh Host without a hint chose an arbitrary retained request", err)
	}
	recovered, err = reopened.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: id})
	if err != nil || recovered.Graph == nil || recovered.Graph.Slot != slot || string(recovered.RequestID) != id {
		t.Fatal("fresh Host selector lost the actual original", err)
	}
	if current, own := managedGraphCertifiedCounts(t, ctx, configs[0], id); current != after || own != 1 {
		t.Fatal("fresh Host recovery changed certified application history")
	}
	if recoveryAttempts.Load() != 0 {
		t.Fatal("fresh Host recovery entered a Graph application proposal")
	}
	if kubeRequests.Load() != 0 {
		t.Fatal("fresh Host normal recovery requested Kubernetes")
	}
	recoveryEntries := recoveryAttempts.Load()
	// A real new mutation is the positive control for the same installed
	// observer. A disconnected/no-op observer must not certify a zero window.
	if _, err := reopened.GraphExecute(ctx, types.GraphCommand{RequestID: "observer-positive", Cypher: "CREATE (:ObserverCI {n:1})"}); err != nil {
		t.Fatal("proposal observer positive control failed", err)
	}
	if recoveryAttempts.Load() == 0 {
		t.Fatal("proposal observer never saw an actual new Graph proposal")
	}
	graphPositive := recoveryAttempts.Load()
	if _, err := reopened.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "observer-positive-kv", Key: "observer-positive-kv", Value: []byte("positive")}); err != nil {
		t.Fatal("KV observer positive control", err)
	}
	kvPositive := recoveryAttempts.Load() - graphPositive
	if kvPositive <= 0 {
		t.Fatal("observer missed actual KV entry")
	}
	if _, err := reopened.Execute(ctx, ExecuteRequest{RequestID: "observer-positive-sql", SQL: "CREATE TABLE observer_positive_sql(n INTEGER)"}); err != nil {
		t.Fatal("SQL observer positive control", err)
	}
	sqlPositive := recoveryAttempts.Load() - graphPositive - kvPositive
	if sqlPositive <= 0 {
		t.Fatal("observer missed actual SQL entry")
	}
	t.Logf("actual UNKNOWN native slot=%d Graph certified application count=%d->%d recovery application proposal entries=%d Kube requests=0 provider logical/HTTP Deletes=0 positive entries Graph=%d KV=%d SQL=%d", slot, before, after, recoveryEntries, graphPositive, kvPositive, sqlPositive)
}
