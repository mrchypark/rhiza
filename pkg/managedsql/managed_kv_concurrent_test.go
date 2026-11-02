package managedsql

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func mixedHostQuorum(t *testing.T, wrappers ...func(int, http.RoundTripper) http.RoundTripper) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, fixture := sqlClusterForProtocol(t, types.ManagedApplicationProtocolSQLKV)
	hosts := make([]*Host, 3)
	for i := range hosts {
		kube := *configs[i].Store.Kube
		client := *kube.Client
		if len(wrappers) != 0 {
			client.Transport = wrappers[0](i, client.Transport)
		}
		kube.Client = &client
		configs[i].Store = &operator.V1Alpha2StatusStore{Kube: &kube}
		h, err := fixture.open(context.Background(), configs[i])
		if h != nil {
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal("actual mixed Host Open", err)
		}
		hosts[i] = h
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	return configs, hosts, fixture
}

// Count actual certified application decisions over the real private QUIC
// protocol. A deduplicated second proposal would still increase this count.
func hostApplicationDecisionCount(t *testing.T, ctx context.Context, cfg Config) int {
	t.Helper()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	client, err := network.NewTransportForPolicy(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, sqlpolicy.ManagedSQLKVMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tip, err := client.ReadTip(ctx, cfg.Runtime.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for from := quepaxa.Slot(1); from <= tip; {
		page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("actual certified decision page", err)
		}
		for _, decision := range page.Decisions {
			if decision.Slot > tip {
				break
			}
			_, sql, err := types.DecodeSQLBatch(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			_, kv, err := types.DecodeManagedKVBatch(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			if sql || kv {
				count++
			}
			from = decision.Slot + 1
		}
	}
	return count
}

func TestManagedKVHostMixedRoundTripAndDetailedRecovery(t *testing.T) {
	configs, hosts, fixture := mixedHostQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h := hosts[0]
	beforeReservation, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "too-large-new", Key: "k", Value: bytes.Repeat([]byte{1}, 100000)}); !errors.Is(err, ErrKVNotSubmitted) {
		t.Fatal("new oversized reservation admitted", err)
	}
	afterReservation, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || beforeReservation.ResourceVersion() != afterReservation.ResourceVersion() {
		t.Fatal("preflight wrote authority", err)
	}
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE mixed_host(n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	id, key := string([]byte{0xff}), string([]byte{0xfe})
	req := network.KVMutationRequest{RequestID: id, Key: key, Value: []byte("original"), Expected: []byte{}, TTLMS: 60000}
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	ack, err := h.KVMutate(ctx, "put", req)
	if !errors.Is(err, network.ErrCommitUnknown) || ack.Status != types.MutationCommitted {
		t.Fatalf("submitted KV outcome: %+v %v", ack, err)
	}
	metadata, ok := MutationErrorContext(err)
	if !ok || metadata.Kind != "kv" || !bytes.Equal(metadata.RequestID, []byte(id)) || metadata.Slot != ack.Slot {
		t.Fatalf("lost KV context: %+v %v", metadata, err)
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	original, ok, err := a.PendingKV()
	if err != nil || !ok || original.Expected == nil {
		t.Fatal("original nullable fields lost", err)
	}
	// Same fingerprint is insufficient to replace the reserved original.
	changed := req
	changed.Expected = nil
	if err := h.ValidateManagedKVRecovery(ctx, h.identity, "put", changed); err == nil {
		t.Fatal("nil/empty original substitution accepted")
	}
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "blocked-sql", SQL: "INSERT INTO mixed_host VALUES (9)"}); !errors.Is(err, ErrSQLNotSubmitted) {
		t.Fatalf("KV pending did not block SQL: %v", err)
	}
	if _, err := h.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id}); err != nil {
		t.Fatal("uncertain pending blocked local status", err)
	}
	if value, err := h.KVGet(ctx, network.KVGetRequest{Key: key, Consistency: "local"}); err != nil || !value.Found || string(value.Value) != "original" {
		t.Fatal("uncertain pending blocked admitted local data", value, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0])
	fixture.mu.Lock()
	fixture.failFinalize = false
	fixture.mu.Unlock()
	result, err := h.RecoverPendingDetailed(ctx)
	if err != nil || result.Kind != "kv" || !bytes.Equal(result.RequestID, []byte(id)) || result.KV == nil || result.SQL != nil || result.KV.MutationReceipt != ack.MutationReceipt {
		t.Fatalf("exact KV recovery: %+v %v", result, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0]); after != before {
		t.Fatalf("recovery re-proposed application: %d -> %d", before, after)
	}
	for _, other := range hosts {
		waitSQLCertifiedApplication(t, ctx, other, ack.Slot)
	}
	falseReq := network.KVMutationRequest{RequestID: "false-cas", Key: "absent", ExpectedExists: true, Expected: []byte{}, Value: nil}
	falseACK, err := h.KVMutate(ctx, "cas", falseReq)
	if err != nil || falseACK.Applied || falseACK.Status != types.MutationCommitted {
		t.Fatal("false CAS not committed", falseACK, err)
	}
	duplicate, err := h.KVMutate(ctx, "cas", falseReq)
	if err != nil || duplicate != falseACK {
		t.Fatal("original false receipt changed", duplicate, err)
	}
	// SQL RETURNING shares the same pending lane and detailed recovery spine.
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	sqlReq := ExecuteRequest{RequestID: "sql-returning", SQL: "INSERT INTO mixed_host VALUES (1) RETURNING n", WantRows: true}
	sqlACK, err := h.Execute(ctx, sqlReq)
	if !errors.Is(err, network.ErrCommitUnknown) || len(sqlACK.Statements) != 1 {
		t.Fatal("SQL original outcome missing", sqlACK, err)
	}
	if _, err := h.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "blocked-kv", Key: "k"}); !errors.Is(err, ErrKVNotSubmitted) {
		t.Fatal("SQL pending did not block KV", err)
	}
	before = hostApplicationDecisionCount(t, ctx, configs[0])
	fixture.mu.Lock()
	fixture.failFinalize = false
	fixture.mu.Unlock()
	sqlResult, err := h.RecoverPendingDetailed(ctx)
	if err != nil || sqlResult.Kind != "sql" || string(sqlResult.RequestID) != sqlReq.RequestID || sqlResult.SQL == nil || sqlResult.KV != nil || !reflect.DeepEqual(*sqlResult.SQL, sqlACK) {
		t.Fatalf("SQL RETURNING recovery: %+v %v", sqlResult, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0]); after != before {
		t.Fatal("SQL detailed recovery resubmitted")
	}
	rows, err := h.Query(ctx, QueryRequest{SQL: "SELECT n FROM mixed_host"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatal("application executed twice", rows, err)
	}
}

func TestManagedKVHostLegacyRefusesKVBeforeReservation(t *testing.T) {
	configs, hosts, _ := openSQLQuorum(t)
	before, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts[0].KVMutate(context.Background(), "put", network.KVMutationRequest{RequestID: "no", Key: "key"}); !errors.Is(err, ErrKVNotSubmitted) {
		t.Fatal(err)
	}
	after, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil || before.ResourceVersion() != after.ResourceVersion() {
		t.Fatal("legacy rejection changed authority", err)
	}
}

func TestManagedKVHostRemoteFinalizationBoundProof(t *testing.T) {
	req := network.KVMutationRequest{RequestID: string([]byte{0xff}), Key: string([]byte{0xfe}), Value: []byte("once"), Expected: []byte{}}
	var origin atomic.Pointer[Host]
	var visited, leaked atomic.Bool
	var saved atomic.Value
	configs, hosts, fixture := mixedHostQuorum(t, func(index int, base http.RoundTripper) http.RoundTripper {
		if index != 0 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if h := origin.Load(); h != nil {
				inProof := r.Context().Value(remoteRecoveryContextKey{}) != nil
				if inProof && visited.CompareAndSwap(false, true) {
					saved.Store(r.Context())
					if err := h.ValidateManagedKVRecovery(context.Background(), h.identity, "put", req); err == nil {
						leaked.Store(true)
					}
				}
			}
			return base.RoundTrip(r)
		})
	})
	origin.Store(hosts[0])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	ack, err := hosts[0].KVMutate(ctx, "put", req)
	if !errors.Is(err, network.ErrCommitUnknown) || ack.Slot == 0 {
		t.Fatal(ack, err)
	}
	reserved, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	original, err := pendingOriginal(reserved)
	if err != nil {
		t.Fatal(err)
	}
	waitSQLCertifiedApplication(t, ctx, hosts[1], ack.Slot)
	before := hostApplicationDecisionCount(t, ctx, configs[0])
	fixture.mu.Lock()
	fixture.failFinalize = false
	fixture.mu.Unlock()
	result, err := hosts[1].RecoverPendingDetailed(ctx)
	if err != nil || result.KV == nil || result.KV.MutationReceipt != ack.MutationReceipt {
		t.Fatal("other Host did not finalize exact receipt", result, err)
	}
	if _, err := hosts[0].verifyRemoteApplication(ctx, original, reserved); !errors.Is(err, ErrSQLRecoveryRequired) {
		t.Fatal("uncertain Host bypassed admission", err)
	}
	if err := hosts[0].RecoverPending(ctx); err != nil {
		t.Fatal("explicit no-pending re-admission", err)
	}
	proofBefore, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	proven, err := hosts[0].verifyRemoteApplication(ctx, original, reserved)
	if err != nil || proven.KV == nil || proven.KV.MutationReceipt != ack.MutationReceipt {
		t.Fatal("exact remote proof", proven, err)
	}
	if !visited.Load() || leaked.Load() {
		t.Fatalf("resolver scope escaped to plain context: visited=%v leaked=%v", visited.Load(), leaked.Load())
	}
	savedContext := saved.Load().(context.Context)
	if err := hosts[0].ValidateManagedKVRecovery(context.WithoutCancel(savedContext), hosts[0].identity, "put", req); err == nil {
		t.Fatal("expired proof survived WithoutCancel")
	}
	if err := hosts[0].ValidateManagedKVRecovery(ctx, hosts[0].identity, "put", req); err == nil {
		t.Fatal("missing pending authorized outside proof")
	}
	// Package-private controls exercise each proof binding independently. The
	// actual helper's captured context above has already expired.
	proof := savedContext.Value(remoteRecoveryContextKey{}).(remoteRecoveryProof)
	scope, endScope := context.WithCancel(ctx)
	defer endScope()
	proof.scopeDone = scope.Done()
	valid := context.WithValue(scope, remoteRecoveryContextKey{}, proof)
	if err := hosts[0].ValidateManagedKVRecovery(context.WithValue(valid, concurrentSQLContextKey{}, "derived"), hosts[0].identity, "put", req); err != nil {
		t.Fatal("live derived scope rejected", err)
	}
	for _, fault := range []string{"issuer", "kind", "raw-id", "empty-rv", "wrong-rv", "nil-original", "scope"} {
		t.Run(fault, func(t *testing.T) {
			bad := proof
			switch fault {
			case "issuer":
				bad.issuer = hosts[1]
			case "kind":
				bad.request.Kind = "sql"
			case "raw-id":
				bad.request.ID = "other"
			case "empty-rv":
				bad.resourceVersion = ""
			case "wrong-rv":
				bad.resourceVersion = "changed"
			case "nil-original":
				changed := original.kv
				changed.Expected = nil
				raw, _, err := types.EncodeManagedKVOriginal(changed)
				if err != nil {
					t.Fatal(err)
				}
				bad.original = string(raw)
			case "scope":
				bad.scopeDone = nil
			}
			if err := hosts[0].ValidateManagedKVRecovery(context.WithValue(scope, remoteRecoveryContextKey{}, bad), hosts[0].identity, "put", req); err == nil {
				t.Fatal("invalid scoped proof accepted")
			}
		})
	}
	runtime, err := hosts[0].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].ValidateManagedKVWrite(valid, hosts[0].identity, runtime, "put", req); err == nil {
		t.Fatal("proof authorized write without pending")
	}
	hosts[0].uncertain.Store(true)
	admitErr := hosts[0].ValidateManagedAdmission(valid, hosts[0].identity, runtime)
	stillUncertain := hosts[0].uncertain.Load()
	hosts[0].uncertain.Store(false)
	if !errors.Is(admitErr, ErrSQLRecoveryRequired) || !stillUncertain {
		t.Fatal("proof changed uncertain admission", admitErr)
	}
	proofAfter, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || proofAfter.ResourceVersion() != proofBefore.ResourceVersion() {
		t.Fatal("remote observation wrote status", err)
	}
	changed := original
	changed.kv.Expected = nil
	if _, err := hosts[0].verifyRemoteApplication(ctx, changed, reserved); err == nil {
		t.Fatal("same fingerprint changed original authorized")
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0]); after != before {
		t.Fatalf("remote proof re-proposed: %d -> %d", before, after)
	}
	// An invalid proof must not fall through even when a matching ordinary
	// pending has subsequently been created through the real reservation API.
	newPending, observed, err := operator.ReserveV1Alpha2KVWrite(ctx, configs[0].Store, proofAfter, proofAfter.Authority(), proofAfter.Evidence(), original.kv)
	if err != nil || observed != operator.V1Alpha2CASApplied {
		t.Fatal("new actual pending control", observed, err)
	}
	if err := hosts[0].ValidateManagedKVRecovery(ctx, hosts[0].identity, "put", req); err != nil {
		t.Fatal("ordinary exact pending control failed", err)
	}
	if err := hosts[0].ValidateManagedKVRecovery(valid, hosts[0].identity, "put", req); err == nil {
		t.Fatal("stale proof fell through to matching new pending")
	}
	endScope()
	if err := hosts[0].ValidateManagedKVRecovery(context.WithoutCancel(valid), hosts[0].identity, "put", req); err == nil {
		t.Fatal("expired proof fell through to new pending")
	}
	current, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || current.ResourceVersion() != newPending.ResourceVersion() {
		t.Fatal("proof checker changed pending", err)
	}
}

func TestManagedKVHostOldPendingDoesNotGetNewAdmissionCap(t *testing.T) {
	configs, hosts, _ := mixedHostQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := hosts[0]
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	command := types.KVCommand{RequestID: string([]byte{0xff}), Operation: "put", Key: "old-pending", Value: bytes.Repeat([]byte{1}, 100000)}
	if _, err := types.ManagedKVExecutionSizeBound(command); err == nil {
		t.Fatal("fixture does not exceed new-reservation execution cap")
	}
	reserved, observed, err := operator.ReserveV1Alpha2KVWrite(ctx, configs[0].Store, a, a.Authority(), a.Evidence(), command)
	if err != nil || observed != operator.V1Alpha2CASApplied {
		t.Fatal("actual old reservation", observed, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0])
	_, err = h.RecoverPendingDetailed(ctx)
	if !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrKVNotSubmitted) {
		t.Fatalf("old pending got new admission rejection: %v", err)
	}
	metadata, ok := MutationErrorContext(err)
	if !ok || metadata.Kind != "kv" || !bytes.Equal(metadata.RequestID, []byte(command.RequestID)) || metadata.Slot != 0 {
		t.Fatal("original missing context lost", metadata)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.ResourceVersion() != reserved.ResourceVersion() || !bytes.Equal(after.Evidence(), reserved.Evidence()) {
		t.Fatal("unresolved old pending modified", err)
	}
	stored, ok, err := after.PendingKV()
	if err != nil || !ok || !bytes.Equal(stored.Value, command.Value) {
		t.Fatal("old original was discarded", err)
	}
	if hostApplicationDecisionCount(t, ctx, configs[0]) != before {
		t.Fatal("missing receipt caused resubmission")
	}
}

func TestManagedKVHostLocalObservationSurvivesQuorumLoss(t *testing.T) {
	_, hosts, _ := mixedHostQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := hosts[0]
	req := network.KVMutationRequest{RequestID: "ack", Key: "key", Value: []byte("acknowledged")}
	ack, err := h.KVMutate(ctx, "put", req)
	if err != nil {
		t.Fatal(err)
	}
	for _, other := range hosts[1:] {
		waitSQLCertifiedApplication(t, ctx, other, ack.Slot)
		if err := other.Close(); err != nil {
			t.Fatal(err)
		}
	}
	closed, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	err = h.Ready(closed)
	stop()
	if err != nil {
		t.Fatal("local readiness was removed by quorum loss", err)
	}
	value, err := h.KVGet(ctx, network.KVGetRequest{Key: req.Key, Consistency: "local"})
	if err != nil || !value.Found || string(value.Value) != "acknowledged" {
		t.Fatal("local acknowledged read failed", value, err)
	}
	status, err := h.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID})
	if err != nil || status.Receipt == nil || status.Receipt.Slot != ack.Slot {
		t.Fatal("local observational status failed", status, err)
	}
	linear, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = h.KVGet(linear, network.KVGetRequest{Key: req.Key, Consistency: "linearizable"})
	stop()
	if err == nil {
		t.Fatal("linearizable read bypassed quorum")
	}
	write, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = h.KVMutate(write, "put", network.KVMutationRequest{RequestID: "must-not-reserve", Key: "new"})
	stop()
	if !errors.Is(err, ErrKVNotSubmitted) {
		t.Fatal("new write bypassed quorum admission", err)
	}
}

func TestManagedKVHostPostBarrierPendingRemovalUsesStrictRemoteProof(t *testing.T) {
	type recoveryCallKey struct{}
	var reads, proofWrites atomic.Int32
	var removed atomic.Bool
	var complete func() error
	configs, hosts, fixture := mixedHostQuorum(t, func(index int, base http.RoundTripper) http.RoundTripper {
		if index != 1 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if r.Context().Value(remoteRecoveryContextKey{}) != nil && r.Method != http.MethodGet && r.Method != http.MethodHead {
				proofWrites.Add(1)
			}
			if r.Method == http.MethodGet && r.Context().Value(recoveryCallKey{}) == true {
				// Detailed recovery live(2), outer validator(2), Node pre-barrier
				// validator(2), then Node post-barrier validator admission GET.
				if reads.Add(1) == 8 {
					if complete == nil {
						return nil, errors.New("missing completion calibration")
					}
					if err := complete(); err != nil {
						return nil, err
					}
					removed.Store(true)
				}
			}
			return base.RoundTrip(r)
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req := network.KVMutationRequest{RequestID: string([]byte{0xff}), Key: "false", ExpectedExists: true, Expected: []byte{}, Value: nil}
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	ack, err := hosts[0].KVMutate(ctx, "cas", req)
	if !errors.Is(err, network.ErrCommitUnknown) || ack.Status != types.MutationCommitted || ack.Applied {
		t.Fatal("actual false CAS receipt missing", ack, err)
	}
	for _, h := range hosts[1:] {
		waitSQLCertifiedApplication(t, ctx, h, ack.Slot)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0])
	fixture.mu.Lock()
	fixture.failFinalize = false
	fixture.mu.Unlock()
	complete = func() error {
		result, err := hosts[2].RecoverPendingDetailed(ctx)
		if err != nil {
			return err
		}
		if result.KV == nil || result.KV.MutationReceipt != ack.MutationReceipt {
			return errors.New("other Host changed original false receipt")
		}
		return nil
	}
	result, err := hosts[1].RecoverPendingDetailed(context.WithValue(ctx, recoveryCallKey{}, true))
	if !removed.Load() || reads.Load() < 8 {
		t.Fatal("actual post-barrier removal boundary not reached", reads.Load(), err)
	}
	if err != nil || result.KV == nil || result.KV.MutationReceipt != ack.MutationReceipt || result.Kind != "kv" || !bytes.Equal(result.RequestID, []byte(req.RequestID)) {
		t.Fatalf("post-barrier removed pending not strictly resolved: result=%+v err=%v", result, err)
	}
	if proofWrites.Load() != 0 {
		t.Fatal("remote observation made status writes", proofWrites.Load())
	}
	if hostApplicationDecisionCount(t, ctx, configs[0]) != before {
		t.Fatal("post-barrier recovery submitted another application")
	}
}
