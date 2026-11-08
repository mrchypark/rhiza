package managedsql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/quepaxa/walfb"
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
	configs, hosts, _ := mixedHostQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := hosts[0]
	req := network.KVMutationRequest{RequestID: "ack", Key: "key", Value: []byte("acknowledged")}
	ack, err := h.KVMutate(ctx, "put", req)
	if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 {
		t.Fatal("initial KV ACK", ack, err)
	}
	readAcknowledged := func(phase string) network.RequestStatusResponse {
		t.Helper()
		status, err := h.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID})
		if err != nil || status.State != string(ack.Status) || status.Receipt == nil || *status.Receipt != ack.MutationReceipt {
			t.Fatalf("%s: acknowledged state or receipt differs from ACK: state=%q receipt=%+v ACK=%+v err=%v", phase, status.State, status.Receipt, ack.MutationReceipt, err)
		}
		return status
	}
	// A real independent commit advances the observation tip, not the
	// request's retained outcome. Exercise that distinction before quorum loss.
	initial := readAcknowledged("before independent commit")
	progress, err := h.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "advance-status-tip", Key: "independent", Value: []byte("committed")})
	if err != nil || progress.Status != types.MutationCommitted || progress.Slot <= initial.Tip {
		t.Fatal("independent KV commit did not advance history", progress, err)
	}
	advanced := readAcknowledged("after independent commit")
	if advanced.Tip < progress.Slot {
		t.Fatalf("status tip did not include the independent applied commit: %d -> %d, commit slot=%d", initial.Tip, advanced.Tip, progress.Slot)
	}
	t.Logf("independent commit advanced status tip %d -> %d with identical ACK receipt at slot %d", initial.Tip, advanced.Tip, ack.Slot)
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
	status := readAcknowledged("after quorum loss")
	if status.Tip < advanced.Tip {
		t.Fatalf("local status tip regressed after quorum loss: %d -> %d", advanced.Tip, status.Tip)
	}
	linear, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = h.KVGet(linear, network.KVGetRequest{Key: req.Key, Consistency: "linearizable"})
	stop()
	if err == nil {
		t.Fatal("linearizable read bypassed quorum")
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: configs[0].Runtime.Members}
	profile := network.ManagedPeerProfile{ApplicationProtocol: configs[0].Plan.ApplicationProtocol, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, Generation: int64(configs[0].Plan.Generation)}
	client, err := network.NewTransportForManagedProfile(configs[0].Runtime.ClusterID, configs[0].Runtime.NodeID, &cluster, configs[0].Runtime.PeerToken, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	beforeSeed, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	certified := hostQuorumLossCertifiedPrefix(t, ctx, client, configs[0].Runtime.NodeID, beforeSeed.CertifiedTip)
	// Real authenticated recorder traffic creates an uncertified control ISR.
	// No certificate or private Core state is fabricated; the other voters are closed.
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	seed := types.EncodeReadBarrier(nonce)
	seedHash := sha256.Sum256(seed)
	seedSlot := quepaxa.Slot(beforeSeed.RecorderTip + 1)
	summary, err := client.SendRecord(ctx, configs[0].Runtime.NodeID, quepaxa.RecordRequest{Slot: seedSlot, Step: 4, ConfigID: 1, Proposal: quepaxa.Proposal{Priority: quepaxa.Priority{31: 1}, ProposerID: configs[0].Runtime.NodeID, Hash: seedHash, Value: seed}})
	if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != seedHash {
		t.Fatal("actual pending read-barrier ISR was not recorded", err)
	}
	beforeWrite, err := h.node.ManagedState()
	if err != nil || beforeWrite.RecorderTip < uint64(seedSlot) || beforeWrite.RecorderTip <= beforeWrite.CertifiedTip || beforeWrite.CertifiedTip != beforeSeed.CertifiedTip || beforeWrite.AppliedTip != beforeSeed.AppliedTip {
		t.Fatalf("fixture lacks an uncertified recorder gap: before=%+v pending=%+v err=%v", beforeSeed, beforeWrite, err)
	}
	write, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	refused, err := h.KVMutate(write, "put", network.KVMutationRequest{RequestID: "must-not-reserve", Key: "new"})
	stop()
	// A live caller joining a failed earlier barrier gets typed availability,
	// not the previous owner's deadline. Generic unresolved-history text alone
	// is not an accepted classification.
	notSubmitted := errors.Is(err, ErrKVNotSubmitted)
	callerDeadline := errors.Is(err, context.DeadlineExceeded)
	unavailable := errors.Is(err, network.ErrNotReady)
	if !notSubmitted && !callerDeadline && !unavailable {
		t.Fatalf("quorum refusal lacks a known typed availability classification: not_submitted=%t deadline=%t not_ready=%t err=%v", notSubmitted, callerDeadline, unavailable, err)
	}
	if !reflect.DeepEqual(refused, network.KVMutationResponse{}) {
		t.Fatalf("quorum-refused write returned an ACK: %+v", refused)
	}
	// An admitted 9/10 runtime waits for fresh quorum inside the engine, so
	// deadline expiry need not use the Host's initial-admission error wrapper.
	// A real own-nonce control ISR may be recorded, even in a reused vacant
	// slot, but cannot certify/apply intent or replace runtime identity.
	afterWrite, err := h.node.ManagedState()
	if err != nil || afterWrite.WALIdentity != beforeWrite.WALIdentity || afterWrite.ExistingRegisteredWAL != beforeWrite.ExistingRegisteredWAL || afterWrite.CertifiedTip != beforeWrite.CertifiedTip || afterWrite.AppliedTip != beforeWrite.AppliedTip || afterWrite.RecorderTip < beforeWrite.RecorderTip {
		t.Fatalf("quorum-refused write changed runtime: before=%+v after=%+v err=%v", beforeWrite, afterWrite, err)
	}
	if after := hostQuorumLossCertifiedPrefix(t, ctx, client, configs[0].Runtime.NodeID, afterWrite.CertifiedTip); !reflect.DeepEqual(certified, after) {
		t.Fatal("quorum-refused write changed certified decision bytes")
	}
	missing, err := h.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: "must-not-reserve"})
	if err != nil || missing.Receipt != nil {
		t.Fatalf("quorum-refused write produced a receipt: %+v %v", missing, err)
	}
	value, err = h.KVGet(ctx, network.KVGetRequest{Key: "new", Consistency: "local"})
	if err != nil || value.Found {
		t.Fatalf("quorum-refused write changed the value: %+v %v", value, err)
	}
	retained := readAcknowledged("after quorum-refused write")
	if retained.Tip < status.Tip {
		t.Fatalf("local status tip regressed after quorum-refused write: %d -> %d", status.Tip, retained.Tip)
	}
	value, err = h.KVGet(ctx, network.KVGetRequest{Key: req.Key, Consistency: "local"})
	if err != nil || !value.Found || string(value.Value) != "acknowledged" {
		t.Fatal("quorum-refused write changed prior acknowledged data", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	assertHostQuorumLossControlWAL(t, configs[0], beforeWrite, seedHash)
}

func hostQuorumLossCertifiedPrefix(t *testing.T, ctx context.Context, client *network.Transport, id quepaxa.NodeID, tip uint64) []quepaxa.DecidedValue {
	t.Helper()
	var values []quepaxa.DecidedValue
	for from := quepaxa.Slot(1); uint64(from) <= tip; {
		page, err := client.FetchDecisions(ctx, id, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("read actual certified prefix", err)
		}
		for _, d := range page.Decisions {
			if uint64(d.Slot) > tip {
				break
			}
			if d.Slot != from || len(d.Certificate) == 0 || sha256.Sum256(d.Value) != d.Hash {
				t.Fatal("invalid certified prefix identity")
			}
			values = append(values, d)
			from++
		}
	}
	return values
}

func assertHostQuorumLossControlWAL(t *testing.T, cfg Config, before node.ManagedRuntimeState, seedHash [32]byte) {
	t.Helper()
	wal, err := qlog.OpenReadOnly(filepath.Join(cfg.Runtime.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Error(err)
		}
	}()
	nonce, registered := wal.Identity()
	if !registered || hex.EncodeToString(nonce) != before.WALIdentity {
		t.Fatal("registered WAL nonce changed")
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[[32]byte][]byte)
	seedRecorded := false
	control := func(value []byte) bool {
		barrier, err := types.DecodeReadBarrier(value)
		if err != nil {
			t.Fatal(err)
		}
		if barrier {
			return true
		}
		_, checkpoint, err := quepaxa.DecodeCheckpointSeal(value)
		if err != nil {
			t.Fatal(err)
		}
		if checkpoint {
			return true
		}
		_, schedule, err := quepaxa.DecodeLeaderSchedule(value)
		if err != nil {
			t.Fatal(err)
		}
		return schedule
	}
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal {
			if sha256.Sum256(entry.Payload) != entry.Hash {
				t.Fatal("WAL proposal identity mismatch")
			}
			values[entry.Hash] = entry.Payload
			commands, kv, err := types.DecodeManagedKVBatch(entry.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if kv {
				for _, command := range commands {
					if command.RequestID == "must-not-reserve" {
						t.Fatal("refused KV original reached WAL")
					}
				}
			}
			if seedRecorded && !control(entry.Payload) {
				t.Fatal("post-injection proposal is not control-only")
			}
		}
		if entry.Type == qlog.EntryDecide && entry.Slot > before.CertifiedTip {
			t.Fatal("new certified decision without quorum")
		}
		if entry.Type != qlog.EntryReceipt || entry.Slot == 0 {
			continue
		}
		if entry.Hash == seedHash {
			seedRecorded = true
			continue
		}
		if !seedRecorded {
			continue
		}
		payload := bytes.TrimPrefix(entry.Payload, []byte("QISR2\x00"))
		if len(payload) == len(entry.Payload) {
			payload = bytes.TrimPrefix(entry.Payload, []byte("QISR\x00"))
		}
		if len(payload) == len(entry.Payload) || len(payload) < 8 || !walfb.RecorderStateBufferHasIdentifier(payload) {
			t.Fatal("invalid actual recorder WAL message")
		}
		state := walfb.GetRootAsRecorderState(payload, 0).UnPack()
		if state.Slot != entry.Slot || state.Step < 4 {
			t.Fatal("recorder WAL slot/step mismatch")
		}
		for _, p := range []*walfb.ProposalT{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
			if p == nil {
				continue
			}
			var hash [32]byte
			if len(p.Hash) != len(hash) {
				t.Fatal("invalid recorder hash")
			}
			copy(hash[:], p.Hash)
			if !control(values[hash]) {
				t.Fatal("post-injection ISR references application data")
			}
		}
		if !control(values[entry.Hash]) {
			t.Fatal("post-injection recorder input is not control-only")
		}
	}
	// A refused call may join an existing failed owner without recording a new
	// barrier. Node's direct tests verify own-nonce certificate proofs; this Host
	// test verifies a real pending ISR and no application effects on refusal.
	if !seedRecorded {
		t.Fatal("missing actual injected pending barrier WAL record")
	}
}
