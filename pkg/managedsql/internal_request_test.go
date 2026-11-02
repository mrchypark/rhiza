package managedsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
func openInternalRequestHosts(t *testing.T, outage *atomic.Bool, attempts *atomic.Int64) ([]Config, []*Host) {
	t.Helper()
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedRequestApplicationProtocol)
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
	return configs, hosts
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
	configs, hosts := openInternalRequestHosts(t, &outage, &attempts)
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
	_, hosts := openInternalRequestHosts(t, &outage, &attempts)
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
