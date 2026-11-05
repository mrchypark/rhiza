package managedsql

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func mixedHostQuorum(t *testing.T, wrappers ...func(int, http.RoundTripper) http.RoundTripper) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
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
func hostApplicationDecisionCount(t *testing.T, ctx context.Context, cfg Config, policies ...sqlpolicy.MaterializationPolicy) int {
	t.Helper()
	return len(hostApplicationDecisionSnapshot(t, ctx, cfg, policies...))
}

type managedApplicationDecision struct {
	slot      quepaxa.Slot
	hash      string
	kind      string
	requestID string
}

// Snapshot actual certified SQL/KV application decisions over the real
// private QUIC protocol. Including slot, kind, request ID, and proposal hash
// distinguishes a new intent from a previously decided value becoming visible
// on a lagging observer.
func hostApplicationDecisionSnapshot(t *testing.T, ctx context.Context, cfg Config, policies ...sqlpolicy.MaterializationPolicy) []managedApplicationDecision {
	t.Helper()
	policy, _, policyErr := types.ManagedApplicationPolicy(cfg.Plan.ApplicationProtocol)
	if policyErr != nil {
		t.Fatal(policyErr)
	}
	if len(policies) > 1 {
		t.Fatal("one explicit peer policy expected")
	}
	if len(policies) == 1 && policies[0] != policy {
		t.Fatal("explicit peer policy differs from configured application protocol")
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	var client *network.Transport
	var err error
	identity := node.New(&cfg.Runtime)
	physicalBinding, _, bindingErr := identity.PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if bindingErr != nil {
		t.Fatal("derive exact trusted profile binding for peer observer", bindingErr)
	}
	bootstrapDigest, digestErr := identity.ManagedBootstrapPlanDigestForProtocol(physicalBinding, cfg.Plan.EvidenceFormat, cfg.Plan.ApplicationProtocol)
	if digestErr != nil || bootstrapDigest != cfg.Plan.BootstrapPlanDigest {
		t.Fatalf("peer observer profile differs from exact public plan: digest=%q plan=%q err=%v", bootstrapDigest, cfg.Plan.BootstrapPlanDigest, digestErr)
	}
	profile := network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: bootstrapDigest, Generation: int64(cfg.Plan.Generation)}
	client, err = network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tip, err := client.ReadTip(ctx, cfg.Runtime.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	decisions := make([]managedApplicationDecision, 0)
	for from := quepaxa.Slot(1); from <= tip; {
		page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("actual certified decision page", err)
		}
		for _, decision := range page.Decisions {
			if decision.Slot > tip {
				break
			}
			sqlCommands, sql, err := types.DecodeSQLBatch(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			kvCommands, kv, err := types.DecodeManagedKVBatch(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			if sql {
				for _, command := range sqlCommands {
					decisions = append(decisions, managedApplicationDecision{slot: decision.Slot, hash: hex.EncodeToString(decision.Hash[:]), kind: "sql", requestID: command.RequestID})
				}
			}
			if kv {
				for _, command := range kvCommands {
					decisions = append(decisions, managedApplicationDecision{slot: decision.Slot, hash: hex.EncodeToString(decision.Hash[:]), kind: "kv", requestID: command.RequestID})
				}
			}
			from = decision.Slot + 1
		}
	}
	return decisions
}

func TestManagedKVHostMixedRoundTripAndDetailedRecovery(t *testing.T) {
	configs, hosts, _ := mixedHostQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h := hosts[0]
	beforeAuthority, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "too-large-new", Key: "k", Value: bytes.Repeat([]byte{1}, 100000)}); !errors.Is(err, ErrKVNotSubmitted) {
		t.Fatal("oversized new execution admitted", err)
	}
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE mixed_host(n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	id, key := string([]byte{0xff}), string([]byte{0xfe})
	req := network.KVMutationRequest{RequestID: id, Key: key, Value: []byte("original"), Expected: []byte{}, TTLMS: 60000}
	ack, err := h.KVMutate(ctx, "put", req)
	if err != nil || ack.Status != types.MutationCommitted {
		t.Fatal("KV ACK", ack, err)
	}
	before := hostApplicationDecisionCount(t, ctx, configs[0])
	result, err := h.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id})
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
	sqlReq := ExecuteRequest{RequestID: "sql-returning", SQL: "INSERT INTO mixed_host VALUES (1) RETURNING n", WantRows: true}
	sqlACK, err := h.Execute(ctx, sqlReq)
	if err != nil || len(sqlACK.Statements) != 1 {
		t.Fatal("SQL original result missing", sqlACK, err)
	}
	before = hostApplicationDecisionCount(t, ctx, configs[0])
	sqlResult, err := h.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: sqlReq.RequestID})
	if err != nil || sqlResult.Kind != "sql" || string(sqlResult.RequestID) != sqlReq.RequestID || sqlResult.SQL == nil || sqlResult.KV != nil || !reflect.DeepEqual(*sqlResult.SQL, sqlACK) {
		t.Fatalf("SQL RETURNING recovery: %+v %v", sqlResult, err)
	}
	if after := hostApplicationDecisionCount(t, ctx, configs[0]); after != before {
		t.Fatal("SQL recovery resubmitted")
	}
	rows, err := h.Query(ctx, QueryRequest{SQL: "SELECT n FROM mixed_host"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatal("application executed twice", rows, err)
	}
	afterAuthority, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || beforeAuthority.ResourceVersion() != afterAuthority.ResourceVersion() || !bytes.Equal(beforeAuthority.Evidence(), afterAuthority.Evidence()) {
		t.Fatal("ordinary SQL/KV execution or recovery mutated CR authority", err)
	}
	if _, pending := afterAuthority.Pending(); pending {
		t.Fatal("ordinary execution created a CR pending request")
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
