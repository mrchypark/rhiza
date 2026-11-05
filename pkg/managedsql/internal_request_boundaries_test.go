package managedsql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

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
