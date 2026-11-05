//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestInternalRequestActualPostApplyCancellationRecoversExactOriginalWithControlISR(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, false)
}

func TestInternalRequestReopenedKVInitialAdmissionRetainsOwnedCutAcrossControlISR(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, true)
}

func TestInternalRequestReopenedKVInitialAdmissionRejectsApplicationISRAfterOwnedCut(t *testing.T) {
	runInternalRequestRecoveryWithControlISR(t, true, true)
}

func runInternalRequestRecoveryWithControlISR(t *testing.T, beforeFinalRuntime bool, applicationISR ...bool) {
	lateApplication := len(applicationISR) == 1 && applicationISR[0]
	// Retain the actual SDK-held post-Apply cancellation, UNKNOWN metadata,
	// canonical raw original/TTL, healthy neighbor, and exact receipt oracles.
	pauseCertifiedRecoveryBackground(t)
	testInternalRequestActualPostApplyCancellationRecoversExactOriginal(t, func(ctx context.Context, h *Host, cfg, readyPeer Config, original materializer.RequestOriginal) (context.Context, func()) {
		before, err := h.node.InspectManagedKVRead(ctx)
		if err != nil || before.Runtime.RecorderTip != before.Runtime.CertifiedTip || before.Runtime.AppliedTip != before.Runtime.CertifiedTip {
			t.Fatal("capture resolved actual prefix before hook", before.Runtime, err)
		}
		values, prefix := certifiedRecoveryPrefix(t, ctx, readyPeer)
		beforeApplications := hostApplicationDecisionSnapshot(t, ctx, readyPeer)
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
		verify := func() {
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
			afterValues, _ := certifiedRecoveryPrefix(t, ctx, cfg)
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
			afterApplications := hostApplicationDecisionSnapshot(t, ctx, cfg)
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
