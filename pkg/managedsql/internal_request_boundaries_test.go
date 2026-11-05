package managedsql

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

// This tests Host mutex serialization, not Node.Shutdown cancelling an active
// borrowed operation. The latter has a separate Node lifetime regression.
func TestInternalRequestHostSerializesCloseBehindExecute(t *testing.T) {
	var outage, armed atomic.Bool
	var attempts atomic.Int64
	entered := make(chan string, 8)
	readErrors := make(chan error, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var gatedPath string
	var canceledObserved bool
	// Register before the fixture: inspect every captured error only after its
	// Hosts and HTTP server have joined. Other requests/errors remain failures.
	t.Cleanup(func() {
		for {
			select {
			case err := <-readErrors:
				if !canceledObserved || gatedPath == "" || !strings.HasPrefix(err.Error(), gatedPath+": ") || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("unexpected source request read failure: %v", err)
				}
			default:
				return
			}
		}
	})
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts, func(source *sqlWireControl) {
		source.putReady, source.putRelease = entered, release
		source.putSuffix = "archive/head.bin"
		source.putEnabled = &armed
		source.putReadErrors = readErrors
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	host := hosts[0]
	type execution struct {
		response ExecuteResponse
		err      error
	}
	operation := make(chan execution, 1)
	closeStarted := make(chan struct{})
	closed := make(chan error, 1)
	var callers sync.WaitGroup
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		callers.Wait() // release all SDK work before fixture Host/server cleanup
	})
	armed.Store(true)
	callers.Add(1)
	go func() {
		defer callers.Done()
		response, err := host.Execute(ctx, ExecuteRequest{RequestID: "close-execute", SQL: "CREATE TABLE close_execute(id INTEGER)"})
		operation <- execution{response, err}
	}()
	select {
	case gatedPath = <-entered:
		if !strings.HasSuffix(gatedPath, "/archive/head.bin") {
			t.Fatal("Close fixture gated a non-archive request")
		}
	case result := <-operation:
		t.Fatalf("Execute completed before archive gate: %+v %v", result.response, result.err)
	case <-ctx.Done():
		t.Fatal("archive gate not reached", ctx.Err())
	}
	// Inspect the real applied receipt without trying to acquire the Host mutex.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := host.node.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "close-execute"})
		if err == nil && status.Receipt != nil && status.Receipt.Status == types.MutationCommitted {
			break
		}
		select {
		case result := <-operation:
			t.Fatalf("Execute returned before applied receipt: %+v %v", result.response, result.err)
		case <-ctx.Done():
			t.Fatal("applied receipt not observed", ctx.Err())
		case <-tick.C:
		}
	}
	original, found, err := host.node.RetainedRequest(ctx, "sql", "close-execute")
	if err != nil || !found || original.Slot == 0 || original.SQL == nil || original.SQL.RequestID != "close-execute" || original.SQL.SQL != "CREATE TABLE close_execute(id INTEGER)" {
		t.Fatalf("applied exact original missing before Close cancellation: found=%t original=%+v error=%v", found, original, err)
	}
	callers.Add(1)
	go func() {
		defer callers.Done()
		close(closeStarted) // attempted public Close, not a Node shutdown marker
		closed <- host.Close()
	}()
	select {
	case <-closeStarted:
	case <-ctx.Done():
		t.Fatal("Close caller did not start", ctx.Err())
	}
	select {
	case err := <-closed:
		t.Fatalf("Close finished while real Execute held SDK gate: %v", err)
	default:
	}
	// closeStarted is only a caller-start signal. Observe the actual canceled
	// SDK operation before releasing its request body; scheduler timing must not
	// decide between a normal ACK and the committed-but-unknown outcome.
	select {
	case result := <-operation:
		var unknown *network.CommitUnknownError
		if !errors.As(result.err, &unknown) || !errors.Is(result.err, context.Canceled) || unknown.RequestID != "close-execute" || result.response.Status == types.MutationCommitted {
			t.Fatalf("Close cancellation did not preserve exact committed-but-unknown outcome: response=%+v error=%v", result.response, result.err)
		}
		canceledObserved = true
	case <-ctx.Done():
		t.Fatal("Execute did not drain", ctx.Err())
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("Close after Execute", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish after Execute", ctx.Err())
	}
	if err := host.Ready(ctx); err == nil {
		t.Fatal("closed Host became Ready")
	}
	if _, err := host.Execute(ctx, ExecuteRequest{RequestID: "after-close", SQL: "SELECT 1"}); err == nil {
		t.Fatal("closed Host accepted new Execute")
	}
	if _, err := host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "close-execute"}); err == nil {
		t.Fatal("closed Host accepted new recovery")
	}
	if err := host.node.CheckManagedLocalRead(ctx); err == nil {
		t.Fatal("closed Node retained an accessible runtime")
	}
}

func TestInternalRequestFirstKVWithoutPrivateAdmissionOrReady(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	before := attempts.Load()
	// No Ready or private AdmitManaged: this public write owns first admission.
	result, err := hosts[0].KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "first-kv", Key: "first-key", Value: []byte("first-value")})
	if err != nil || result.Status != types.MutationCommitted || !result.Applied || result.Slot == 0 {
		t.Fatalf("first public KV did not admit and commit: %+v %v", result, err)
	}
	if attempts.Load() <= before {
		t.Fatal("first KV skipped live initial admission")
	}
	attempts.Store(0)
	outage.Store(true)
	t.Cleanup(func() { outage.Store(false) })
	value, err := hosts[0].KVGet(ctx, network.KVGetRequest{Key: "first-key", Consistency: "local"})
	if err != nil || !value.Found || string(value.Value) != "first-value" || value.AppliedSlot < result.Slot {
		t.Fatalf("first KV readback: %+v %v", value, err)
	}
	if attempts.Load() != 0 {
		t.Fatal("admitted local read accessed Kubernetes")
	}
}

func TestInternalRequestAdditionalOperationsWithoutKube(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	configs, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	host := hosts[0]
	sql, err := host.Execute(ctx, ExecuteRequest{RequestID: "published-sql", SQL: "CREATE TABLE outage_boundary(id INTEGER)"})
	if err != nil || sql.Status != types.MutationCommitted || sql.Slot == 0 {
		t.Fatalf("actual SQL ACK: %+v %v", sql, err)
	}
	kv, err := host.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "published-kv", Key: "published-key", Value: []byte("published-value")})
	if err != nil || kv.Status != types.MutationCommitted || !kv.Applied || kv.Slot == 0 {
		t.Fatalf("actual KV ACK: %+v %v", kv, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy)
	attempts.Store(0)
	outage.Store(true)
	t.Cleanup(func() { outage.Store(false) })
	status, err := host.RequestStatus(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "published-sql"})
	if err != nil || status.Receipt == nil || status.Receipt.Status != types.MutationCommitted || status.Receipt.Slot != sql.Slot {
		t.Fatalf("SQL status without Kubernetes: %+v %v", status, err)
	}
	value, err := host.KVGet(ctx, network.KVGetRequest{Key: "published-key", Consistency: "linearizable"})
	if err != nil || !value.Found || string(value.Value) != "published-value" || value.AppliedSlot < kv.Slot {
		t.Fatalf("linearizable KV without Kubernetes: %+v %v", value, err)
	}
	// These selectors refer to actual public ACKs, not inserted RAM hints.
	sqlRecovery, err := host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "published-sql"})
	if err != nil || sqlRecovery.Kind != "sql" || string(sqlRecovery.RequestID) != "published-sql" || sqlRecovery.SQL == nil || sqlRecovery.SQL.Slot != sql.Slot || sqlRecovery.SQL.Status != types.MutationCommitted {
		t.Fatalf("explicit SQL recovery without Kubernetes: %+v %v", sqlRecovery, err)
	}
	kvRecovery, err := host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: "published-kv"})
	if err != nil || kvRecovery.Kind != "kv" || string(kvRecovery.RequestID) != "published-kv" || kvRecovery.KV == nil || kvRecovery.KV.Slot != kv.Slot || !kvRecovery.KV.Applied || kvRecovery.KV.Status != types.MutationCommitted {
		t.Fatalf("explicit KV recovery without Kubernetes: %+v %v", kvRecovery, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy); after != before {
		t.Fatalf("recovery submitted application commands: before=%d after=%d", before, after)
	}
	if attempts.Load() != 0 {
		t.Fatalf("normal operations accessed Kubernetes %d times", attempts.Load())
	}
}
