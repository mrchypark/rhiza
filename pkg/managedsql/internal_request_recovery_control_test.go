//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestInternalRequestActualPostApplyCancellationRecoversExactOriginal(t *testing.T) {
	pauseCertifiedRecoveryBackground(t)
	testInternalRequestActualPostApplyCancellationRecoversExactOriginal(t, nil)
}

func TestInternalRequestReopenedKVInitialAdmissionAcrossCertifiedReadBarrierDuringInspection(t *testing.T) {
	pauseCertifiedRecoveryBackground(t)
	testInternalRequestActualPostApplyCancellationRecoversExactOriginal(t, func(ctx context.Context, h *Host, cfg, readyPeer Config, original materializer.RequestOriginal, _ uint64, _ func(string)) (context.Context, func(RecoveryResult, error)) {
		cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
		client, err := network.NewTransportForManagedProfile(readyPeer.Runtime.ClusterID, readyPeer.Runtime.NodeID, &cluster, readyPeer.Runtime.PeerToken, network.ManagedPeerProfile{ApplicationProtocol: readyPeer.Plan.ApplicationProtocol, BootstrapPlanDigest: readyPeer.Plan.BootstrapPlanDigest, Generation: int64(readyPeer.Plan.Generation)})
		if err != nil {
			t.Fatal(err)
		}
		workerCtx, cancelWorker := context.WithCancel(ctx)
		start, done, applied, stop := make(chan internalRecoveryEvidence, 1), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var stopOnce, appliedOnce sync.Once
		var slot atomic.Uint64
		var workerErr error
		var oldCut, newCut internalRecoveryEvidence
		var fresh, originalDecision quepaxa.DecidedValue
		marker := &struct{}{}
		var armed atomic.Bool
		var samples atomic.Int32
		var beforeSamples [2][3]uint64
		var injected atomic.Bool
		healthy := quepaxa.NodeID("")
		for _, member := range cfg.Runtime.Members {
			if member.ID != cfg.Runtime.NodeID && member.ID != readyPeer.Runtime.NodeID {
				healthy = member.ID
			}
		}
		if healthy == "" {
			t.Fatal("three actual fixture voters required")
		}
		restoreEvent := localtesthooks.Set(func(event string) {
			if event == "managed-host:initial-authority:before-evidence" {
				samples.Store(0)
				armed.Store(true)
			}
			current := slot.Load()
			if current == 0 {
				return
			}
			if event == fmt.Sprintf("network:learned:phase=apply-complete:node=%s:sender=%s:slot=%d", cfg.Runtime.NodeID, readyPeer.Runtime.NodeID, current) {
				appliedOnce.Do(func() { close(applied) })
			}
			if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=apply-start:node=%s:", healthy)) && strings.HasSuffix(event, fmt.Sprintf(":slot=%d", current)) {
				select {
				case <-applied:
				case <-stop:
				case <-workerCtx.Done():
				}
			}
		})
		restoreState := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
			if observed.Value(managedGraphCallerMarkerKey{}) != marker || !armed.Load() || observed.Err() != nil {
				return
			}
			// Inside the armed inspector: runtime binding sample, captured-before
			// sample, then final sample after application inspection releases its
			// materializer read lock. Do not inject into the initial/before sample.
			index := samples.Add(1)
			if index <= 2 {
				state, err := h.node.ManagedState()
				if err != nil {
					t.Fatal("capture actual pre-inspection sample", err)
				}
				beforeSamples[index-1] = [3]uint64{state.RecorderTip, state.CertifiedTip, state.AppliedTip}
				return
			}
			if index != 3 {
				return
			}
			oldCut = captureInternalRecoveryEvidence(ctx, h, cfg)
			if oldCut.err != nil || oldCut.recorded != oldCut.certified || oldCut.applied != oldCut.certified || oldCut.floor != 0 || oldCut.certified >= 127 {
				t.Fatal("post-inspection cut is not the bounded resolved fixture", oldCut, oldCut.err)
			}
			if beforeSamples[0] != beforeSamples[1] || beforeSamples[1] != [3]uint64{oldCut.recorded, oldCut.certified, oldCut.applied} {
				t.Fatal("inspection samples changed before controlled injection", beforeSamples, oldCut)
			}
			start <- oldCut
			select {
			case <-done:
			case <-observed.Done():
				t.Fatal("controlled certification did not finish", observed.Err())
			}
			if workerErr != nil {
				t.Fatal("controlled certification failed", workerErr)
			}
			injected.Store(true)
		})
		t.Cleanup(func() {
			stopOnce.Do(func() { close(stop) })
			cancelWorker()
			<-done
			restoreState()
			restoreEvent()
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		go func() {
			defer close(done)
			workerErr = func() error {
				select {
				case oldCut = <-start:
				case <-workerCtx.Done():
					return workerCtx.Err()
				}
				before, err := client.FetchDecisions(workerCtx, cfg.Runtime.NodeID, 1, 128)
				if err != nil || uint64(len(before.Decisions)) != oldCut.certified {
					return fmt.Errorf("actual captured prefix unavailable: %w", err)
				}
				if original.Slot == 0 || original.Slot > uint64(len(before.Decisions)) {
					return fmt.Errorf("original slot not covered")
				}
				for i, decision := range before.Decisions {
					if uint64(decision.Slot) != uint64(i+1) || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
						return fmt.Errorf("captured certificate identity invalid")
					}
				}
				originalDecision = before.Decisions[original.Slot-1]
				commands, batch, err := types.DecodeManagedKVBatch(originalDecision.Value)
				if err != nil {
					return err
				}
				if !batch {
					command, single, err := types.DecodeManagedKVCommand(originalDecision.Value)
					if err != nil || !single || original.BatchIndex != 0 {
						return fmt.Errorf("actual original frame invalid: %w", err)
					}
					commands = []types.KVCommand{command}
				}
				if int(original.BatchIndex) >= len(commands) {
					return fmt.Errorf("actual original batch ordinal invalid")
				}
				command := commands[original.BatchIndex]
				item, err := types.EncodeManagedKVBatchItem(command)
				if err != nil || command.RequestID != original.RequestID || !bytes.Equal(item, original.ExecutionItem) {
					return fmt.Errorf("actual certified original item differs: %w", err)
				}
				canonical, _, err := types.EncodeManagedKVOriginal(command)
				if err != nil || !bytes.Equal(canonical, original.CanonicalOriginal) {
					return fmt.Errorf("actual certified canonical original differs: %w", err)
				}
				fingerprint, err := types.ManagedKVFingerprint(command)
				if err != nil || fingerprint != original.Fingerprint {
					return fmt.Errorf("actual certified fingerprint differs: %w", err)
				}
				var nonce [types.ReadBarrierNonceSize]byte
				nonce[0] = 93
				value := types.EncodeReadBarrier(nonce)
				hash := sha256.Sum256(value)
				next := quepaxa.Slot(oldCut.certified + 1)
				slot.Store(uint64(next))
				proposal := quepaxa.Proposal{ProposerID: readyPeer.Runtime.NodeID, Hash: hash, Value: value}
				var summaries []quepaxa.Summary
				// Both rounds use actual authenticated Record RPCs and durable WAL
				// receipts. Phase 6 certifies their actual phase-5 AggregatePrior;
				// no recorder, certificate, Core snapshot or public ACK is fabricated.
				for _, step := range []quepaxa.Step{5, 6} {
					summaries = nil
					for _, member := range cfg.Runtime.Members {
						summary, err := client.SendRecord(workerCtx, member.ID, quepaxa.RecordRequest{Slot: next, Step: step, ConfigID: 1, Proposal: proposal})
						if err != nil || summary.RecorderID != member.ID || summary.Step != step || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash || (step == 6 && (summary.AggregatePrior == nil || summary.AggregatePrior.Hash != hash)) {
							return fmt.Errorf("actual recorder proof invalid at step %d: %w", step, err)
						}
						summaries = append(summaries, summary)
					}
				}
				if err := client.SendDecision(workerCtx, quepaxa.Decision{ConfigID: 1, Slot: next, Step: 6, Proposal: proposal, Summaries: summaries}); err != nil {
					return err
				}
				select {
				case <-applied:
				case <-workerCtx.Done():
					return workerCtx.Err()
				}
				after, err := client.FetchDecisions(workerCtx, cfg.Runtime.NodeID, 1, 128)
				if err != nil || len(after.Decisions) != len(before.Decisions)+1 || !reflect.DeepEqual(before.Decisions, after.Decisions[:len(before.Decisions)]) {
					return fmt.Errorf("actual certified prefix/original certificate changed: %w", err)
				}
				fresh = after.Decisions[len(before.Decisions)]
				barrier, err := types.DecodeReadBarrier(fresh.Value)
				if err != nil || !barrier || fresh.Slot != next || fresh.Hash != hash || !bytes.Equal(fresh.Value, value) || len(fresh.Certificate) == 0 {
					return fmt.Errorf("actual new certificate is not the exact read barrier")
				}
				newCut = captureInternalRecoveryEvidence(ctx, h, cfg)
				if newCut.err != nil || newCut.certified != uint64(next) || newCut.applied != uint64(next) || newCut.recorded != uint64(next) || newCut.stateTip != oldCut.stateTip || newCut.digest != oldCut.digest || newCut.floor != oldCut.floor || newCut.root != oldCut.root || newCut.wal != oldCut.wal {
					return fmt.Errorf("pure read barrier changed application/receipts/authority or did not apply: %v", newCut.err)
				}
				return nil
			}()
		}()
		verify := func(first RecoveryResult, firstErr error) {
			restoreState()
			restoreEvent()
			if !injected.Load() {
				t.Fatal("post-inspection certified advance was not injected")
			}
			if !errors.Is(firstErr, network.ErrCommitUnknown) || first.KV != nil || !strings.Contains(firstErr.Error(), "history changed during application inspection") {
				t.Fatal("controlled inspection advance did not preserve UNKNOWN without ACK at the intended guard", first, firstErr)
			}
			t.Logf("forced read-barrier certificate slot=%d kind=read_barrier hash=%x certificate_bytes=%d before_R/C/A=%d/%d/%d after_R/C/A=%d/%d/%d state_tip_same=%t application_receipts_digest_same=%t root_floor_same=%t wal_same=%t", fresh.Slot, fresh.Hash, len(fresh.Certificate), oldCut.recorded, oldCut.certified, oldCut.applied, newCut.recorded, newCut.certified, newCut.applied, oldCut.stateTip == newCut.stateTip, oldCut.digest == newCut.digest, oldCut.floor == newCut.floor && oldCut.root == newCut.root, oldCut.wal == newCut.wal)
			t.Logf("preserved original slot=%d value_hash=%x certificate_sha256=%x new_certificate_sha256=%x", originalDecision.Slot, originalDecision.Hash, sha256.Sum256(originalDecision.Certificate), sha256.Sum256(fresh.Certificate))
			cancelWorker()
			<-done
			if err := client.Close(); err != nil {
				t.Fatal("controlled peer cleanup", err)
			}
			stable := captureInternalRecoveryEvidence(ctx, h, cfg)
			if stable.err != nil || stable != newCut {
				t.Fatal("controlled completion did not retain its stable cut", stable, stable.err)
			}
			// A concurrent maintenance cut may refuse admission as UNKNOWN.
			// Once that work has completed, make exactly one explicit recovery
			// of the same original; never loop/replay the mutation itself.
			if original.Kind != types.MutationKV {
				t.Fatal("stable recovery fixture requires the actual KV original")
			}
			// MutationKind is an internal uint8 enum, not the public wire kind.
			recovered, err := h.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: original.RequestID})
			if err != nil || recovered.KV == nil || !bytes.Equal(recovered.RequestID, []byte(original.RequestID)) || !reflect.DeepEqual(recovered.KV.MutationReceipt, original.Receipt) {
				t.Fatal("one stable-cut recovery did not recover the exact original receipt", recovered, err)
			}
		}
		return managedGraphCallerMarkedContext{Context: ctx, marker: marker}, verify
	}, true)
}

func TestInternalRequestActualPostApplyCancellationRecoversExactOriginalWithControlISR(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, false)
}

func TestInternalRequestReopenedKVInitialAdmissionRetainsOwnedCutAcrossControlISR(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, true)
}

func TestInternalRequestReopenedKVInitialAdmissionRejectsApplicationISRAfterOwnedCut(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, true, true)
}

func internalRecoveryPrefixThrough(t *testing.T, ctx context.Context, cfg Config, through uint64, observe func(string)) ([]quepaxa.DecidedValue, [32]byte) {
	t.Helper()
	// The caller supplies an actual certified slot, not a latest-tip guess.
	// These small fixtures fit in one Sync response; never retry or ReadIndex.
	if through == 0 || through > 128 {
		t.Fatal("known recovery prefix is outside the one-RPC fixture bound", through)
	}
	if observe != nil {
		observe("before-prefix-rpc")
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	profile := network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, Generation: int64(cfg.Plan.Generation)}
	client, err := network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error("owned prefix client cleanup", err)
		}
	}()
	page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, 1, int(through))
	if observe != nil {
		observe("after-prefix-rpc")
	}
	if err != nil {
		t.Fatal("single authenticated known-cut prefix RPC", err)
	}
	if page.ClusterID != cfg.Runtime.ClusterID || page.ProposerID != cfg.Runtime.NodeID || page.ConfigID != 1 || uint64(page.Tip) < through || uint64(len(page.Decisions)) != through {
		t.Fatal("actual response does not cover the exact known prefix", through, len(page.Decisions), page.Tip)
	}
	var prefix [32]byte
	for i, decision := range page.Decisions {
		if uint64(decision.Slot) != uint64(i+1) || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
			t.Fatal("invalid actual certified prefix identity")
		}
		prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
	}
	return page.Decisions, prefix
}

func internalRecoveryApplications(t *testing.T, values []quepaxa.DecidedValue) []managedApplicationDecision {
	t.Helper()
	var applications []managedApplicationDecision
	for _, decision := range values {
		if len(decision.Value) == 0 {
			continue
		} // certified recovery no-op, not an application original
		sqlCommands, sql, err := types.DecodeSQLBatch(decision.Value)
		if err != nil {
			t.Fatal(err)
		}
		kvCommands, kv, err := types.DecodeManagedKVBatch(decision.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !kv {
			command, single, err := types.DecodeManagedKVCommand(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			if single {
				kv = true
				kvCommands = []types.KVCommand{command}
			}
		}
		for _, command := range sqlCommands {
			applications = append(applications, managedApplicationDecision{slot: decision.Slot, hash: fmt.Sprintf("%x", decision.Hash), kind: "sql", requestID: command.RequestID})
		}
		for _, command := range kvCommands {
			applications = append(applications, managedApplicationDecision{slot: decision.Slot, hash: fmt.Sprintf("%x", decision.Hash), kind: "kv", requestID: command.RequestID})
		}
		if !sql && !kv {
			barrier, e1 := types.DecodeReadBarrier(decision.Value)
			_, seal, e2 := types.DecodeCheckpointSeal(decision.Value)
			_, schedule, e3 := types.DecodeLeaderSchedule(decision.Value)
			reconfiguration, e4 := types.DecodeReconfiguration(decision.Value)
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !(barrier || seal || schedule || reconfiguration) {
				t.Fatal("certified recovery prefix contains an unclassified value")
			}
		}
	}
	return applications
}

func runInternalRequestRecoveryWithControlISR(t *testing.T, beforeFinalRuntime bool, applicationISR ...bool) {
	lateApplication := len(applicationISR) == 1 && applicationISR[0]
	// Retain the actual SDK-held post-Apply cancellation, UNKNOWN metadata,
	// canonical raw original/TTL, healthy neighbor, and exact receipt oracles.
	pauseCertifiedRecoveryBackground(t)
	testInternalRequestActualPostApplyCancellationRecoversExactOriginal(t, func(ctx context.Context, h *Host, cfg, readyPeer Config, original materializer.RequestOriginal, knownY uint64, observeReadyPeer func(string)) (context.Context, func(RecoveryResult, error)) {
		before, err := h.node.InspectManagedKVRead(ctx)
		if err != nil || before.Runtime.RecorderTip != before.Runtime.CertifiedTip || before.Runtime.AppliedTip != before.Runtime.CertifiedTip {
			t.Fatal("capture resolved actual prefix before hook", before.Runtime, err)
		}
		values, prefix := internalRecoveryPrefixThrough(t, ctx, readyPeer, knownY, observeReadyPeer)
		beforeApplications := internalRecoveryApplications(t, values)
		if uint64(len(values)) != before.Runtime.CertifiedTip {
			t.Fatal("ready peer does not cover the exact captured target prefix")
		}
		if original.Slot == 0 || original.Slot > uint64(len(values)) {
			t.Fatal("original slot not covered")
		}
		decision := values[original.Slot-1]
		commands, recognized, err := types.DecodeManagedKVBatch(decision.Value)
		if err != nil {
			t.Fatal("invalid original batch", err)
		}
		if !recognized {
			command, single, decodeErr := types.DecodeManagedKVCommand(decision.Value)
			if decodeErr != nil || !single || original.BatchIndex != 0 {
				t.Fatal("invalid original single frame", decodeErr)
			}
			commands = []types.KVCommand{command}
		}
		if int(original.BatchIndex) >= len(commands) || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
			t.Fatal("original certificate/batch missing", err)
		}
		command := commands[original.BatchIndex]
		item, err := types.EncodeManagedKVBatchItem(command)
		if err != nil || !bytes.Equal(item, original.ExecutionItem) {
			t.Fatal("ready peer original payload differs", err)
		}
		canonical, _, err := types.EncodeManagedKVOriginal(command)
		if err != nil || !bytes.Equal(canonical, original.CanonicalOriginal) {
			t.Fatal("original canonical intent differs", err)
		}
		fingerprint, err := types.ManagedKVFingerprint(command)
		if err != nil || fingerprint != original.Fingerprint || command.RequestID != original.RequestID {
			t.Fatal("original fingerprint/request binding differs", err)
		}
		cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
		client, err := network.NewTransportForManagedProfile(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken, network.ManagedPeerProfile{ApplicationProtocol: cfg.Plan.ApplicationProtocol, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, Generation: int64(cfg.Plan.Generation)})
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		injected := false
		marker := &struct{}{}
		var armed atomic.Bool
		var observations atomic.Int32
		var ownMu sync.Mutex
		var ownValue []byte
		restoreProposal := func() {}
		if beforeFinalRuntime {
			var seedNonce [types.ReadBarrierNonceSize]byte
			seedNonce[0] = 78
			seed := types.EncodeReadBarrier(seedNonce)
			seedHash := sha256.Sum256(seed)
			summary, err := client.SendRecord(ctx, cfg.Runtime.NodeID, quepaxa.RecordRequest{Slot: quepaxa.Slot(before.Runtime.CertifiedTip + 1), Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: cfg.Runtime.NodeID, Hash: seedHash, Value: seed}})
			if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != seedHash {
				t.Fatal("seed actual unresolved CONTROL requiring an owned barrier", err)
			}
			restoreProposal = localtesthooks.SetProposalEntry(func(value []byte) {
				barrier, err := types.DecodeReadBarrier(value)
				if err == nil && barrier {
					ownMu.Lock()
					if ownValue == nil {
						ownValue = bytes.Clone(value)
					}
					ownMu.Unlock()
				}
			})
		}
		restoreContext := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
			if observed.Value(managedGraphCallerMarkerKey{}) == marker && observed.Err() == nil {
				if beforeFinalRuntime {
					if observations.Add(1) != 2 {
						return
					}
					once.Do(func() {
						cut, err := h.node.ManagedState()
						if err != nil || cut.RecorderTip != cut.CertifiedTip || cut.AppliedTip != cut.CertifiedTip || cut.CertifiedTip <= before.Runtime.CertifiedTip {
							t.Fatal("final initial-admission sample lacks a resolved owned cut", cut, err)
						}
						ownMu.Lock()
						owned := bytes.Clone(ownValue)
						ownMu.Unlock()
						if len(owned) == 0 {
							t.Fatal("actual owned CONTROL proposal was not observed")
						}
						var ownSlot quepaxa.Slot
						var ownDecision quepaxa.DecidedValue
						for from := quepaxa.Slot(before.Runtime.CertifiedTip + 1); uint64(from) <= cut.CertifiedTip; {
							page, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
							if err != nil || len(page.Decisions) == 0 {
								t.Fatal("read actual owned certified suffix before injection", err)
							}
							for _, decision := range page.Decisions {
								if uint64(decision.Slot) > cut.CertifiedTip {
									break
								}
								if decision.Slot != from || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
									t.Fatal("actual owned cut has invalid certificate/prefix continuity")
								}
								if bytes.Equal(decision.Value, owned) && decision.Hash == sha256.Sum256(owned) {
									ownSlot = decision.Slot
									ownDecision = decision
								}
								from++
							}
							if uint64(from) > cut.CertifiedTip {
								break
							}
						}
						if ownSlot == 0 {
							t.Fatal("owned nonce/value has no actual original-slot certificate")
						}
						var nonce [types.ReadBarrierNonceSize]byte
						nonce[0] = 80
						value := types.EncodeReadBarrier(nonce)
						if lateApplication {
							value, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: "untrusted-after-owned-cut", Operation: "put", Key: "must-not-apply", Value: []byte("unconfirmed")})
							if err != nil {
								t.Fatal(err)
							}
						}
						hash := sha256.Sum256(value)
						slot := quepaxa.Slot(cut.CertifiedTip + 1)
						summary, err := client.SendRecord(ctx, cfg.Runtime.NodeID, quepaxa.RecordRequest{Slot: slot, Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: cfg.Runtime.NodeID, Hash: hash, Value: value}})
						if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash {
							t.Fatal("record later actual CONTROL after owned cut", err)
						}
						after, err := h.node.ManagedState()
						if err != nil || after.WALIdentity != cut.WALIdentity || after.RecorderTip != uint64(slot) || after.CertifiedTip != cut.CertifiedTip || after.AppliedTip != cut.AppliedTip {
							t.Fatal("fresh CONTROL changed captured certified/applied cut", cut, after, err)
						}
						ownedAfter, err := client.FetchDecisions(ctx, cfg.Runtime.NodeID, ownSlot, 1)
						if err != nil || len(ownedAfter.Decisions) != 1 || !reflect.DeepEqual(ownedAfter.Decisions[0], ownDecision) {
							t.Fatal("fresh ISR changed the actual owned original-slot certificate", err)
						}
						injected = true
						t.Logf("initial admission final sample owned_slot=%d before_R/C/A=%d/%d/%d fresh_R/C/A=%d/%d/%d", ownSlot, cut.RecorderTip, cut.CertifiedTip, cut.AppliedTip, after.RecorderTip, after.CertifiedTip, after.AppliedTip)
					})
					return
				}
				armed.Store(true)
			}
		})
		restore := localtesthooks.Set(func(event string) {
			if event != "managed-host:initial-authority:before-evidence" || !armed.Load() {
				return
			}
			once.Do(func() {
				captured, err := h.node.ManagedState()
				if err != nil || captured.CertifiedTip != before.Runtime.CertifiedTip || captured.AppliedTip != before.Runtime.AppliedTip || captured.RecorderTip != captured.CertifiedTip {
					t.Fatal("admission cut changed before injection", captured, err)
				}
				var nonce [types.ReadBarrierNonceSize]byte
				nonce[0] = 79
				value := types.EncodeReadBarrier(nonce)
				hash := sha256.Sum256(value)
				summary, err := client.SendRecord(ctx, cfg.Runtime.NodeID, quepaxa.RecordRequest{Slot: quepaxa.Slot(before.Runtime.CertifiedTip + 1), Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: cfg.Runtime.NodeID, Hash: hash, Value: value}})
				if err != nil || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != hash {
					t.Fatal("actual CONTROL ISR not recorded", err)
				}
				injected = true
			})
		})
		cleanup := func() {
			restore()
			restoreContext()
			restoreProposal()
			client.Close()
		}
		t.Cleanup(cleanup)
		verify := func(_ RecoveryResult, _ error) {
			restore()
			restoreContext()
			restoreProposal()
			defer client.Close()
			if !injected {
				t.Error("new Host recovery did not reach runtime observer")
				return
			}
			after, err := h.node.InspectManagedKVRead(ctx)
			if err != nil || before.Runtime.WALIdentity != after.Runtime.WALIdentity || after.Runtime.CertifiedTip < before.Runtime.CertifiedTip || after.Runtime.AppliedTip < before.Runtime.AppliedTip || after.Runtime.AppliedTip > after.Runtime.CertifiedTip || after.Runtime.RecorderTip < before.Runtime.RecorderTip+1 || !reflect.DeepEqual(before.Application, after.Application) {
				t.Error("recovery regressed its cut or changed application/receipts", err)
			}
			// The operation under test has now initialized this Host; only now
			// may its certified tip be observed through the peer protocol.
			afterValues, _ := internalRecoveryPrefixThrough(t, ctx, cfg, after.Runtime.CertifiedTip, nil)
			if len(afterValues) < len(values) || !reflect.DeepEqual(values, afterValues[:len(values)]) {
				t.Fatal("recovery forked certified original bytes/certificates")
			}
			var capturedPrefix [32]byte
			for _, decision := range afterValues[:len(values)] {
				capturedPrefix = quepaxa.AdvancePrefixHash(capturedPrefix, decision.Slot, decision.Hash)
			}
			if capturedPrefix != prefix {
				t.Fatal("captured certified prefix changed")
			}
			for _, decision := range afterValues[len(values):] {
				barrier, e1 := types.DecodeReadBarrier(decision.Value)
				_, seal, e2 := types.DecodeCheckpointSeal(decision.Value)
				_, schedule, e3 := types.DecodeLeaderSchedule(decision.Value)
				reconfiguration, e4 := types.DecodeReconfiguration(decision.Value)
				if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !(barrier || seal || schedule || reconfiguration) {
					t.Fatal("recovery certified a non-control suffix")
				}
			}
			afterApplications := internalRecoveryApplications(t, afterValues)
			if !reflect.DeepEqual(beforeApplications, afterApplications) {
				t.Fatal("recovery submitted an additional application original")
			}
			originalCount := 0
			for _, application := range afterApplications {
				if application.kind == "kv" && application.requestID == original.RequestID {
					originalCount++
				}
			}
			if originalCount != 1 {
				t.Fatal("original application count is not exactly one", originalCount)
			}
			t.Logf("recovery cut before R/C/A=%d/%d/%d after=%d/%d/%d applications=%d/%d original_count=%d", before.Runtime.RecorderTip, before.Runtime.CertifiedTip, before.Runtime.AppliedTip, after.Runtime.RecorderTip, after.Runtime.CertifiedTip, after.Runtime.AppliedTip, len(beforeApplications), len(afterApplications), originalCount)
		}
		return managedGraphCallerMarkedContext{Context: ctx, marker: marker}, verify
	}, beforeFinalRuntime && lateApplication)
}
