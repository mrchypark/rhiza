package managedsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestInternalRequestHintsDoNotSelectLatestOrPoisonRuntime(t *testing.T) {
	h := &Host{}
	x := network.RequestStatusRequest{Kind: "sql", RequestID: "X"}
	y := network.RequestStatusRequest{Kind: "kv", RequestID: "\xff"}
	h.rememberRequestOutcome(x.Kind, x.RequestID, &network.CommitUnknownError{RequestID: x.RequestID, Cause: context.DeadlineExceeded})
	h.rememberRequestOutcome(y.Kind, y.RequestID, nil)
	if _, ok := h.recoveryTargets[x]; !ok || len(h.recoveryTargets) != 1 || h.uncertain.Load() {
		t.Fatal("independent successful Y must not erase X or poison runtime")
	}
	h.rememberRequestOutcome(y.Kind, y.RequestID, &network.CommitUnknownError{RequestID: y.RequestID, Cause: context.Canceled})
	if len(h.recoveryTargets) != 2 {
		t.Fatal("ambiguous targets collapsed to latest")
	}
	h.rememberRequestOutcome(x.Kind, x.RequestID, errors.New("lookup failed"))
	if len(h.recoveryTargets) != 2 {
		t.Fatal("nonterminal error erased unresolved target")
	}
	h.rememberRequestOutcome(x.Kind, x.RequestID, nil)
	if len(h.recoveryTargets) != 1 {
		t.Fatal("exact successful result did not clear only its own hint")
	}
}

func TestInternalRequestNoArgUnknownOrAmbiguous(t *testing.T) {
	for _, targets := range []int{0, 2, 3} {
		h := &Host{cfg: Config{Plan: operator.V1Alpha2ApplicationInitPlan{ApplicationProtocol: sqlpolicy.ManagedRequestApplicationProtocol}}}
		for i := 0; i < targets; i++ {
			h.rememberRequestOutcome("sql", string(rune('X'+i)), &network.CommitUnknownError{Cause: context.Canceled})
		}
		result, err := h.RecoverPendingDetailed(context.Background())
		if !errors.Is(err, network.ErrCommitUnknown) || result.Kind != "" {
			t.Fatalf("targets=%d selected an arbitrary original or returned clean none: %v", targets, err)
		}
		if _, ok := MutationErrorContext(err); ok {
			t.Fatal("ambiguity acquired an arbitrary mutation identity")
		}
		if len(h.recoveryTargets) > 2 {
			t.Fatal("unbounded RAM target history")
		}
	}
}

// This uses the real three-Host/Core/WAL/SQLite/S3-wire fixture. The transport
// fault is Kubernetes-only, not a real-cloud or process-termination experiment.
func openInternalRequestHosts(t *testing.T, outage *atomic.Bool, attempts *atomic.Int64, setup ...func(*sqlWireControl)) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedRequestApplicationProtocol)
	if len(setup) > 1 {
		t.Fatal("one optional source fixture setup expected")
	}
	if len(setup) == 1 {
		setup[0](fixture.source)
	}
	hosts := make([]*Host, len(configs))
	for i, config := range configs {
		kube := *config.Store.Kube
		client := *kube.Client
		base := client.Transport
		client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
			attempts.Add(1)
			if outage.Load() {
				return nil, errors.New("test Kube unavailable after startup")
			}
			return base.RoundTrip(r)
		})
		kube.Client = &client
		config.Store = &operator.V1Alpha2StatusStore{Kube: &kube}
		host, err := fixture.open(context.Background(), config)
		if host != nil {
			t.Cleanup(func() {
				if err := host.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = host
	}
	return configs, hosts, fixture
}

func awaitPublicInternalReady(t *testing.T, ctx context.Context, host *Host) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := host.Ready(ctx); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("public initial Ready did not admit runtime", ctx.Err())
		case <-tick.C:
		}
	}
}

func TestInternalRequestHostOperationsWithoutKube(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	configs, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	beforeAdmission := attempts.Load()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	if attempts.Load() <= beforeAdmission {
		t.Fatal("public Ready skipped initial live startup checker")
	}
	host := hosts[0]
	first, err := host.Execute(ctx, ExecuteRequest{RequestID: "X", SQL: "CREATE TABLE internal_request_proof (id INTEGER PRIMARY KEY)"})
	if err != nil || first.Status != types.MutationCommitted || first.Slot == 0 {
		t.Fatalf("SQL preflight: %+v %v", first, err)
	}
	// Model loss at the caller response-delivery boundary after actual commit.
	// This tests hint selection, not an injected storage/network timeout.
	host.mu.Lock()
	host.rememberRequestOutcome("sql", "X", &network.CommitUnknownError{RequestID: "X", Cause: context.DeadlineExceeded})
	host.mu.Unlock()
	attempts.Store(0)
	outage.Store(true)
	t.Cleanup(func() { outage.Store(false) })
	y, err := host.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "\xff", Key: "\xfe", Value: []byte("Y")})
	if err != nil || y.Status != types.MutationCommitted || !y.Applied || y.Slot <= first.Slot {
		t.Fatalf("independent Y: %+v %v", y, err)
	}
	z, err := host.Execute(ctx, ExecuteRequest{RequestID: "Z", SQL: "INSERT INTO internal_request_proof VALUES (1)"})
	if err != nil || z.Status != types.MutationCommitted || z.Slot <= y.Slot {
		t.Fatalf("SQL write without Kube: %+v %v", z, err)
	}
	for _, consistency := range []string{"local", "linearizable"} {
		rows, err := host.Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM internal_request_proof", Consistency: consistency})
		if err != nil || len(rows.Rows) != 1 || len(rows.Columns) != 1 || len(rows.Rows[0]) != 1 || fmt.Sprint(rows.Rows[0][0]) != "1" {
			t.Fatalf("%s SQL query: %+v %v", consistency, rows, err)
		}
	}
	value, err := host.KVGet(ctx, network.KVGetRequest{Key: "\xfe", Consistency: "local"})
	if err != nil || !value.Found || string(value.Value) != "Y" {
		t.Fatalf("KV local read: %+v %v", value, err)
	}
	if err := host.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := host.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: "\xff"})
	if err != nil || status.Receipt == nil || status.Receipt.Slot != y.Slot {
		t.Fatalf("KV status: %+v %v", status, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy)
	recovered, err := host.RecoverPendingDetailed(ctx)
	if err != nil || recovered.Kind != "sql" || string(recovered.RequestID) != "X" || recovered.SQL == nil || recovered.SQL.Slot != first.Slot {
		t.Fatalf("no-arg selected Y instead of original X: %+v %v", recovered, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy); after != before {
		t.Fatal("recovery submitted another application command")
	}
	_, err = host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "missing"})
	if !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatalf("missing original was treated as no effect: %v", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("normal operation attempted Kubernetes %d times", got)
	}
}

func TestInternalRequestFirstExecuteWithoutPrivateAdmissionOrReady(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	before := attempts.Load()
	// Deliberately no Ready and no private Node.AdmitManaged before Execute.
	result, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "first-public-write", SQL: "CREATE TABLE public_first_write(id INTEGER)"})
	if err != nil || result.Status != types.MutationCommitted || result.Slot == 0 {
		t.Fatalf("first public Execute did not admit: %+v %v", result, err)
	}
	if attempts.Load() <= before {
		t.Fatal("first Execute skipped live initial admission")
	}
	attempts.Store(0)
	outage.Store(true)
	t.Cleanup(func() { outage.Store(false) })
	if err := hosts[0].Ready(ctx); err != nil {
		t.Fatal("already-admitted Ready repeated initial admission", err)
	}
	if _, err := hosts[0].Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM public_first_write", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("post-admission local calls accessed Kubernetes")
	}
}

// No manual error hint is installed here: the actual KVMutate call must return
// CommitUnknown after Apply while the real SDK archive request is held.
func TestInternalRequestActualPostApplyCancellationRecoversExactOriginal(t *testing.T) {
	var outage, armed atomic.Bool
	var attempts atomic.Int64
	entered := make(chan string, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	configs, hosts, fixture := openInternalRequestHosts(t, &outage, &attempts, func(source *sqlWireControl) {
		source.putReady = entered
		source.putRelease = release
		source.putSuffix = "archive/head.bin"
		source.putEnabled = &armed
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	xctx, cancelX := context.WithCancel(ctx)
	defer cancelX()
	var caller sync.WaitGroup
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancelX()
		caller.Wait() // release SDK work before any Host/server cleanup waits
	})
	id, key := "\xff", "\xfe"
	request := network.KVMutationRequest{RequestID: id, Key: key, Value: []byte("original-X"), Expected: []byte{}, TTLMS: 60000}
	type outcome struct {
		response network.KVMutationResponse
		err      error
	}
	done := make(chan outcome, 1)
	armed.Store(true)
	caller.Add(1)
	go func() {
		defer caller.Done()
		response, err := hosts[0].KVMutate(xctx, "put", request)
		done <- outcome{response, err}
	}()
	select {
	case <-entered:
	case observed := <-done:
		t.Fatalf("caller completed before controlled archive HEAD: %+v %v", observed.response, observed.err)
	case <-ctx.Done():
		t.Fatal("archive HEAD gate not reached", ctx.Err())
	}
	// A HEAD can also be publishing an earlier control slot. Require actual X
	// receipt/Apply before cancellation rather than inferring it from the PUT.
	var receipt types.MutationReceipt
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for receipt.Slot == 0 {
		status, err := hosts[0].node.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id})
		if err == nil && status.Receipt != nil && status.State == string(types.MutationCommitted) {
			receipt = *status.Receipt
			break
		}
		select {
		case observed := <-done:
			t.Fatalf("caller completed before actual applied X: %+v %v", observed.response, observed.err)
		case <-ctx.Done():
			t.Fatalf("actual X receipt not applied before cancellation: %v (last status error %v)", ctx.Err(), err)
		case <-tick.C:
		}
	}
	beforeOriginal, found, err := hosts[0].node.RetainedRequest(ctx, "kv", id)
	if err != nil || !found || beforeOriginal.Slot != receipt.Slot || beforeOriginal.KV == nil {
		t.Fatalf("applied canonical X missing: found=%t error=%v", found, err)
	}
	if beforeOriginal.KV.RequestID != id || beforeOriginal.KV.Key != key || string(beforeOriginal.KV.Value) != "original-X" || beforeOriginal.KV.Expected == nil || beforeOriginal.KV.TTLMS != request.TTLMS || beforeOriginal.KV.ExpiresAtUnixMS-beforeOriginal.KV.ObservedAtUnixMS != request.TTLMS {
		t.Fatal("actual applied X did not preserve raw original/TTL")
	}
	cancelX()
	var observed outcome
	select {
	case observed = <-done:
	case <-ctx.Done():
		t.Fatal("canceled caller did not return", ctx.Err())
	}
	var unknown *network.CommitUnknownError
	if !errors.As(observed.err, &unknown) || !errors.Is(observed.err, context.Canceled) || unknown.RequestID != id || errors.Is(observed.err, ErrKVNotSubmitted) || (unknown.Slot != 0 && uint64(unknown.Slot) != receipt.Slot) {
		t.Fatalf("actual post-Apply cancellation did not retain UNKNOWN: response=%+v error=%v", observed.response, observed.err)
	}
	releaseOnce.Do(func() { close(release) })
	y, err := hosts[0].KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "Y", Key: "neighbor", Value: []byte("healthy")})
	if err != nil || y.Status != types.MutationCommitted || !y.Applied || y.Slot <= receipt.Slot {
		t.Fatalf("X UNKNOWN globally blocked healthy Y: %+v %v", y, err)
	}
	neighbor, err := hosts[0].KVGet(ctx, network.KVGetRequest{Key: "neighbor", Consistency: "local"})
	if err != nil || !neighbor.Found || string(neighbor.Value) != "healthy" {
		t.Fatalf("Y readback: %+v %v", neighbor, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy)
	recovered, err := hosts[0].RecoverRequest(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id})
	if err != nil || recovered.KV == nil || recovered.KV.Slot != receipt.Slot || !recovered.KV.Applied || string(recovered.RequestID) != id {
		t.Fatalf("exact X recovery: %+v %v", recovered, err)
	}
	xvalue, err := hosts[0].KVGet(ctx, network.KVGetRequest{Key: key, Consistency: "local"})
	if err != nil || !xvalue.Found || string(xvalue.Value) != "original-X" {
		t.Fatal("X readback differs after exact recovery", err)
	}
	afterOriginal, found, err := hosts[0].node.RetainedRequest(ctx, "kv", id)
	if err != nil || !found || afterOriginal.Slot != beforeOriginal.Slot || string(afterOriginal.ExecutionItem) != string(beforeOriginal.ExecutionItem) {
		t.Fatal("recovery changed original execution/TTL", err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy); after != before {
		t.Fatal("exact recovery submitted another application command")
	}
	if err := hosts[0].Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixture.open(context.Background(), configs[0])
	if reopened != nil {
		t.Cleanup(func() {
			if err := reopened.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal("same-DataDir public Host reopen", err)
	}
	if _, err := reopened.RecoverPendingDetailed(ctx); !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatal("new handle invented a no-arg target", err)
	}
	recovered, err = reopened.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id})
	if err != nil || recovered.KV == nil || recovered.KV.Slot != receipt.Slot || !recovered.KV.Applied {
		t.Fatalf("new handle exact original recovery: %+v %v", recovered, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0], sqlpolicy.ManagedRequestMaterializationPolicy); after != before {
		t.Fatal("reopen recovery submitted another application command")
	}
}
