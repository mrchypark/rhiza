//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Isolate peer delivery/apply faults from independent archive/checkpoint
// publication. Foreground BeforeAck sync is not intercepted by these hooks.
func pauseCertifiedRecoveryBackground(t *testing.T) func(context.Context, int) {
	t.Helper()
	var checkpoint, archive atomic.Int32
	restore := localtesthooks.SetBackgroundPublication(func(ctx context.Context, phase string) {
		switch phase {
		case "checkpoint":
			checkpoint.Add(1)
		case "archive":
			archive.Add(1)
		default:
			t.Errorf("unexpected background publication phase %q", phase)
		}
		<-ctx.Done()
	})
	// Installed before fixture creation: its Hosts cancel/join these workers
	// before this registry entry is restored.
	t.Cleanup(restore)
	return func(ctx context.Context, count int) {
		t.Helper()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for checkpoint.Load() < int32(count) || archive.Load() < int32(count) {
			select {
			case <-ctx.Done():
				t.Fatal("recovery fixture background workers did not pause", ctx.Err())
			case <-tick.C:
			}
		}
	}
}

// Hold actual Learned delivery before certificate acceptance and the actual
// periodic certificate pull. No Kubernetes dependency, SQL, or result is mocked.
type certifiedRecoveryRecordGate struct {
	slot    atomic.Uint64
	seen    chan struct{}
	release func()
}

func heldCertifiedRecoveryHosts(t *testing.T, ctx context.Context, originalHash [32]byte) ([]Config, []*Host, *sqlStatusFixture, *atomic.Bool, <-chan struct{}, <-chan struct{}, func(), *certifiedRecoveryRecordGate) {
	t.Helper()
	held := &atomic.Bool{}
	backgroundPaused := pauseCertifiedRecoveryBackground(t)
	configs, hosts, fixture := concurrentSQLHosts(t, nil)
	backgroundPaused(ctx, len(hosts))
	pushSeen, pullSeen, releasePull := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pushOnce, pullOnce, releaseOnce sync.Once
	recordRelease := make(chan struct{})
	var recordSeenOnce, recordReleaseOnce sync.Once
	recordGate := &certifiedRecoveryRecordGate{seen: make(chan struct{})}
	recordGate.release = func() { recordReleaseOnce.Do(func() { close(recordRelease) }) }
	target := configs[1].Runtime.NodeID
	healthyPeer := configs[2].Runtime.NodeID
	restore := localtesthooks.Set(func(event string) {
		if !held.Load() {
			return
		}
		for _, nodeID := range []quepaxa.NodeID{target, healthyPeer} {
			prefix := fmt.Sprintf("network:record:phase=before:node=%s:sender=%s:slot=", nodeID, configs[0].Runtime.NodeID)
			if !strings.HasPrefix(event, prefix) {
				continue
			}
			var slot uint64
			var hash string
			if _, err := fmt.Sscanf(strings.TrimPrefix(event, prefix), "%d:hash=%s", &slot, &hash); err != nil || hash != fmt.Sprintf("%x", originalHash) {
				return
			}
			recordGate.slot.CompareAndSwap(0, slot)
			if slot == 0 || slot != recordGate.slot.Load() {
				t.Error("original Record changed slot while gated")
				return
			}
			if nodeID == healthyPeer {
				recordSeenOnce.Do(func() { close(recordGate.seen) })
				select {
				case <-recordRelease:
				case <-ctx.Done():
				}
			} else {
				// Make the third recorder's exclusion observed before the
				// required target can complete the recorder quorum.
				select {
				case <-recordGate.seen:
				case <-recordRelease:
				case <-ctx.Done():
				}
			}
			return
		}
		if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=received:node=%s:", target)) {
			pushOnce.Do(func() { close(pushSeen) })
			select {
			case <-releasePull:
			case <-ctx.Done():
			}
		}
		if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=apply-start:node=%s:", healthyPeer)) {
			// SendDecision cancels outstanding peers after its first remote
			// ACK. Let that healthy ACK return only after the target's real
			// Learned RPC has reached its acceptance hold.
			select {
			case <-pushSeen:
			case <-releasePull:
			case <-ctx.Done():
			}
		}
		if strings.HasPrefix(event, fmt.Sprintf("node:catchup-peer:phase=before-accept:node=%s:", target)) {
			pullOnce.Do(func() { close(pullSeen) })
			select {
			case <-releasePull:
			case <-ctx.Done():
			}
		}
	})
	release := func() {
		recordGate.release()
		held.Store(false)
		releaseOnce.Do(func() { close(releasePull) })
	}
	// Release the periodic worker before the Host cleanup waits for shutdown.
	t.Cleanup(func() { release(); restore() })
	return configs, hosts, fixture, held, pushSeen, pullSeen, release, recordGate
}

func TestManagedSQLCertifiedRecoveryGapHoldsPushAndPeriodicPull(t *testing.T) {
	t.Run("quorum-recovers-original-slot", testCertifiedRecoveryOriginalSlot)
	t.Run("no-quorum-remains-unknown", testCertifiedRecoveryWithoutQuorum)
}

func testCertifiedRecoveryOriginalSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	req := ExecuteRequest{RequestID: "certified-gap-original", SQL: "CREATE TABLE certified_gap_original(id INTEGER PRIMARY KEY)"}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: req.RequestID, SQL: req.SQL}})
	if err != nil {
		t.Fatal(err)
	}
	originalHash := sha256.Sum256(value)
	configs, hosts, _, held, pushSeen, pullSeen, release, recordGate := heldCertifiedRecoveryHosts(t, ctx, originalHash)
	held.Store(true)
	ack, err := hosts[0].Execute(ctx, req)
	if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 {
		t.Fatal("original durable ACK", ack, err)
	}
	select {
	case <-recordGate.seen:
	default:
		t.Fatal("third recorder was not actually gated before ACK")
	}
	if recordGate.slot.Load() != ack.Slot {
		t.Fatal("gated original Record differs from ACK slot")
	}
	recordGate.release() // do not constrain subsequent normal recovery
	for _, observation := range []struct {
		name string
		done <-chan struct{}
	}{{"target Learned before acceptance", pushSeen}, {"target periodic certificate pull", pullSeen}} {
		select {
		case <-observation.done:
		case <-ctx.Done():
			t.Fatal("actual delivery gap not observed: "+observation.name, ctx.Err())
		}
	}
	state, err := hosts[1].node.ManagedState()
	if err != nil || state.RecorderTip < ack.Slot || state.CertifiedTip >= ack.Slot || state.AppliedTip >= ack.Slot {
		t.Fatalf("missing actual certified gap: state=%+v slot=%d error=%v", state, ack.Slot, err)
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	values, prefix := certifiedRecoveryPrefix(t, ctx, configs[0])
	if ack.Slot > uint64(len(values)) || values[ack.Slot-1].Hash != originalHash {
		t.Fatal("ACK certificate does not cover the gated original bytes")
	}
	var certificate struct {
		Summaries []struct {
			RecorderID quepaxa.NodeID `json:"recorder_id"`
		} `json:"summaries"`
	}
	if err := json.Unmarshal(values[ack.Slot-1].Certificate, &certificate); err != nil {
		t.Fatal(err)
	}
	recorders := make(map[quepaxa.NodeID]bool)
	for _, summary := range certificate.Summaries {
		recorders[summary.RecorderID] = true
	}
	if len(recorders) != 2 || !recorders[configs[0].Runtime.NodeID] || !recorders[configs[1].Runtime.NodeID] || recorders[configs[2].Runtime.NodeID] {
		t.Fatal("actual certificate did not require origin and target recorders", recorders)
	}
	t.Logf("actual original slot=%d recorder/certified/applied=%d/%d/%d third_record_gated=true target_in_certificate=true", ack.Slot, state.RecorderTip, state.CertifiedTip, state.AppliedTip)
	beforeApplications := hostApplicationDecisionSnapshot(t, ctx, configs[0])
	originalCount := 0
	for _, application := range beforeApplications {
		if application.requestID == req.RequestID {
			originalCount++
			if uint64(application.slot) != ack.Slot {
				t.Fatal("original was already submitted at another position")
			}
		}
	}
	if originalCount != 1 {
		t.Fatal("source ACK does not have exactly one actual original command")
	}
	selector := network.RequestStatusRequest{Kind: "sql", RequestID: req.RequestID}
	// Holding Learned/pull does not disable normal QuePaxa recovery. A fresh
	// owned barrier may recover this exact original at its existing slot.
	result, err := hosts[1].RecoverRequest(ctx, selector)
	if err != nil || result.SQL == nil {
		t.Fatal("normal quorum failed to recover the original position", result, err)
	}
	want, _ := json.Marshal(ack)
	got, _ := json.Marshal(result.SQL)
	if !bytes.Equal(want, got) {
		t.Fatalf("recovered ACK/result differs: want=%s got=%s", want, got)
	}
	afterValues, _ := certifiedRecoveryPrefix(t, ctx, configs[1])
	var recoveredPrefix [32]byte
	for i, value := range values {
		if i >= len(afterValues) || value.Slot != afterValues[i].Slot || value.Hash != afterValues[i].Hash || !bytes.Equal(value.Value, afterValues[i].Value) {
			t.Fatal("quorum recovery changed the original certified value prefix")
		}
		recoveredPrefix = quepaxa.AdvancePrefixHash(recoveredPrefix, value.Slot, value.Hash)
	}
	if recoveredPrefix != prefix {
		t.Fatal("quorum recovery changed the certified prefix hash")
	}
	afterApplications := hostApplicationDecisionSnapshot(t, ctx, configs[1])
	if len(beforeApplications) != len(afterApplications) {
		t.Fatal("recovery proposed another application command")
	}
	for i := range beforeApplications {
		if beforeApplications[i] != afterApplications[i] {
			t.Fatal("recovery changed an application slot/hash/identity")
		}
	}
	original, found, err := hosts[1].node.RetainedRequest(ctx, "sql", req.RequestID)
	if err != nil || !found || original.Slot != ack.Slot || original.SQL == nil || original.SQL.RequestID != req.RequestID || original.SQL.SQL != req.SQL || original.Receipt != ack.MutationReceipt {
		t.Fatal("recovery did not retain the exact original and receipt", original, err)
	}
	if ack.Slot == 0 || ack.Slot > uint64(len(afterValues)) || len(afterValues[ack.Slot-1].Certificate) == 0 {
		t.Fatal("original ACK slot lacks its actual certificate")
	}
	// SQL originals retain the canonical batch item and ordinal, not the
	// Graph-only DecisionHash. Bind that item to the actual certified batch.
	commands, ok, err := types.DecodeSQLBatch(afterValues[ack.Slot-1].Value)
	if err != nil || !ok || uint64(original.BatchIndex) >= uint64(len(commands)) {
		t.Fatal("original ACK slot lacks its SQL batch ordinal", err)
	}
	command := commands[original.BatchIndex]
	item, err := types.EncodeSQLBatchItem(command)
	if err != nil || !bytes.Equal(item, original.ExecutionItem) || !bytes.Equal(item, original.CanonicalOriginal) {
		t.Fatal("certified SQL batch item differs from retained original", err)
	}
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil || fingerprint != original.Fingerprint || command.RequestID != req.RequestID || command.SQL != req.SQL {
		t.Fatal("certified SQL batch item has a different request binding", err)
	}
	resolved, err := hosts[1].node.ResolveManagedSQL(ctx, req)
	if err != nil || resolved.Durability != network.ResolveExecuteDurabilityConfirmed || resolved.Receipt == nil || *resolved.Receipt != ack.MutationReceipt {
		t.Fatal("original durability was not confirmed", resolved, err)
	}
	rows, err := hosts[1].Query(ctx, QueryRequest{SQL: "SELECT name FROM sqlite_master WHERE type='table' AND name='certified_gap_original'", Consistency: "local"})
	if err != nil || rows.AppliedSlot < ack.Slot || len(rows.Rows) != 1 || rows.Rows[0][0] != "certified_gap_original" {
		t.Fatal("original schema effect is not visible at its ACK slot", rows, err)
	}
	if final := hostApplicationDecisionSnapshot(t, ctx, configs[1]); len(final) != len(afterApplications) {
		t.Fatal("durability/readback resubmitted an application")
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.ResourceVersion() != before.ResourceVersion() || !bytes.Equal(after.Evidence(), before.Evidence()) {
		t.Fatal("recovery mutated CR authority", err)
	}
	release()
	waitSQLCertifiedApplication(t, ctx, hosts[1], ack.Slot)
}

func certifiedRecoveryPrefix(t *testing.T, ctx context.Context, cfg Config) ([]quepaxa.DecidedValue, [32]byte) {
	t.Helper()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	profile := network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, Generation: int64(cfg.Plan.Generation)}
	client, err := network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tip, err := client.ReadTip(ctx, cfg.Runtime.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	var values []quepaxa.DecidedValue
	var prefix [32]byte
	for from := quepaxa.Slot(1); from <= tip; {
		page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("read actual certified recovery prefix", err)
		}
		for _, decision := range page.Decisions {
			if decision.Slot > tip {
				break
			}
			if decision.Slot != from || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
				t.Fatal("invalid actual certified prefix identity")
			}
			values = append(values, decision)
			prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
			from++
		}
	}
	return values, prefix
}

func testCertifiedRecoveryWithoutQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	paused := pauseCertifiedRecoveryBackground(t)
	configs, hosts, _ := concurrentSQLHosts(t, nil)
	paused(ctx, len(hosts))
	target := hosts[1]
	before, err := target.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	values, prefix := certifiedRecoveryPrefix(t, ctx, configs[1])
	for _, i := range []int{0, 2} {
		if err := hosts[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	cfg := configs[1]
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	client, err := network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, Generation: int64(cfg.Plan.Generation)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	req := types.SQLCommand{RequestID: "uncertified-gap-original", SQL: "CREATE TABLE uncertified_gap_original(id INTEGER PRIMARY KEY)"}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{req})
	if err != nil {
		t.Fatal(err)
	}
	slot, hash := quepaxa.Slot(before.CertifiedTip+1), sha256.Sum256(value)
	summary, err := client.SendRecord(ctx, cfg.Runtime.NodeID, quepaxa.RecordRequest{Slot: slot, Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: cfg.Runtime.NodeID, Hash: hash, Value: value}})
	if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash {
		t.Fatal("actual uncertified SQL original was not recorded", err)
	}
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	result, recoverErr := target.RecoverRequest(short, network.RequestStatusRequest{Kind: "sql", RequestID: req.RequestID})
	stop()
	if !errors.Is(recoverErr, network.ErrCommitUnknown) || result.SQL != nil || errors.Is(recoverErr, ErrSQLNotSubmitted) {
		t.Fatal("unconfirmed/no-quorum original did not remain UNKNOWN", result, recoverErr)
	}
	after, err := target.node.ManagedState()
	if err != nil || after.CertifiedTip != before.CertifiedTip || after.AppliedTip != before.AppliedTip || after.WALIdentity != before.WALIdentity {
		t.Fatal("uncertified recovery changed certified/applied identity", after, err)
	}
	afterValues, afterPrefix := certifiedRecoveryPrefix(t, ctx, cfg)
	if len(afterValues) != len(values) || afterPrefix != prefix {
		t.Fatal("no-quorum recovery changed the certified prefix")
	}
	for i := range values {
		if values[i].Slot != afterValues[i].Slot || values[i].Hash != afterValues[i].Hash || !bytes.Equal(values[i].Value, afterValues[i].Value) || !bytes.Equal(values[i].Certificate, afterValues[i].Certificate) {
			t.Fatal("no-quorum recovery changed existing certificate/value bytes")
		}
	}
	status, err := target.node.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: req.RequestID})
	if err != nil || status.Receipt != nil {
		t.Fatal("uncertified original acquired a receipt", status, err)
	}
	rows, err := target.Query(ctx, QueryRequest{SQL: "SELECT name FROM sqlite_master WHERE type='table' AND name='uncertified_gap_original'", Consistency: "local"})
	if err != nil || len(rows.Rows) != 0 {
		t.Fatal("uncertified original was applied", rows, err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(cfg.Runtime.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Error(err)
		}
	}()
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	foundOriginal := false
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal && entry.Hash == hash && bytes.Equal(entry.Payload, value) {
			foundOriginal = true
		}
		if entry.Type == qlog.EntryDecide && entry.Slot >= uint64(slot) {
			t.Fatal("no-quorum WAL contains an invented certified decision")
		}
	}
	if !foundOriginal {
		t.Fatal("no-quorum recovery lost the original WAL bytes")
	}
}
