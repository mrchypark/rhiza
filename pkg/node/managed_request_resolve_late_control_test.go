//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedRequestResolveCommittedSQLKVWithLateControl(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		for _, certify := range []bool{false, true} {
			name := "minority-isr"
			if certify {
				name = "certified-unapplied-suffix"
			}
			t.Run(kind+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				n, _, fixture := newKVWrapperFixture(t)
				id := "resolve-known-" + kind
				var receipt types.MutationReceipt
				if kind == "sql" {
					ack, err := n.ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: id, SQL: "CREATE TABLE resolve_late_sql(id INTEGER PRIMARY KEY)"})
					if err != nil {
						t.Fatal("actual SQL ACK", err)
					}
					receipt = ack.MutationReceipt
				} else {
					ack, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: id, Key: "resolve-late", Value: []byte("retained")})
					if err != nil {
						t.Fatal("actual KV ACK", err)
					}
					receipt = ack.MutationReceipt
				}
				if receipt.Status != types.MutationCommitted || receipt.Slot == 0 {
					t.Fatal("missing committed original")
				}
				original, found, err := n.RetainedRequest(ctx, kind, id)
				if err != nil || !found || original.Receipt != receipt {
					t.Fatal("exact retained original", err)
				}
				values, tip, err := n.core.DecisionsFrom(1, 256)
				if err != nil {
					t.Fatal(err)
				}
				prefix, ok := n.core.PrefixHash(tip)
				if !ok {
					t.Fatal("actual prefix unavailable")
				}
				var nonce [types.ReadBarrierNonceSize]byte
				for i := range nonce {
					nonce[i] = byte(193 + i)
				}
				control := types.EncodeReadBarrier(nonce)
				injected := false
				restore := localtesthooks.Set(func(event string) {
					if event != "node:managed-request:resolved-before-runtime-recheck" {
						return
					}
					if injected {
						t.Fatal("final boundary repeated")
					}
					injected = true
					if n.core.Tip() != tip || n.material.Tip() != uint64(tip) {
						t.Fatal("fault preceded durable applied result")
					}
					if certify {
						slot, _, err := n.core.Propose(ctx, control)
						if err != nil || slot != tip+1 {
							t.Fatal("actual control certification", slot, err)
						}
					} else {
						recordFreshAdmissionControl(t, ctx, fixture, tip+1, control, false)
					}
				})
				defer restore()
				resolved, err := n.ResolveRequest(ctx, kind, id)
				if !injected {
					t.Fatal("final boundary not reached", err)
				}
				if err != nil {
					if !errors.Is(err, network.ErrCommitUnknown) || !strings.Contains(err.Error(), "request runtime changed during resolution") {
						t.Fatal("wrong RED boundary", err)
					}
					t.Errorf("known committed original rejected only by unrelated later control: %v", err)
				} else if resolved.Receipt != receipt || !reflect.DeepEqual(resolved.Original, original) {
					t.Error("resolution changed original/receipt")
				}
				after, found, err := n.RetainedRequest(ctx, kind, id)
				if err != nil || !found || !reflect.DeepEqual(after, original) {
					t.Fatal("original changed", err)
				}
				afterValues, afterTip, err := n.core.DecisionsFrom(1, 256)
				want := values
				wantTip := tip
				if certify {
					d, ok := n.core.CertifiedValue(tip + 1)
					if !ok || len(d.Certificate) == 0 || !bytes.Equal(d.Value, control) {
						t.Fatal("actual control certificate missing")
					}
					want = append(append([]quepaxa.DecidedValue(nil), values...), d)
					wantTip++
				} else if _, ok := n.core.CertifiedValue(tip + 1); ok {
					t.Fatal("minority control received fake certificate")
				}
				afterPrefix, ok := n.core.PrefixHash(tip)
				if err != nil || afterTip != wantTip || !reflect.DeepEqual(want, afterValues) || !ok || afterPrefix != prefix || n.material.Tip() != uint64(tip) {
					t.Fatal("resolution altered original prefix or applied new suffix")
				}
			})
		}
	}
}

func TestManagedRequestResolveSQLKVMissingAndDurabilityRemainUnknown(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			n, _, _ := newKVWrapperFixture(t)
			id := "resolve-negative-" + kind
			if kind == "sql" {
				if _, err := n.ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: id, SQL: "CREATE TABLE resolve_negative(id INTEGER)"}); err != nil {
					t.Fatal(err)
				}
			} else if _, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: id, Key: id, Value: []byte("original")}); err != nil {
				t.Fatal(err)
			}
			before, found, err := n.RetainedRequest(ctx, kind, id)
			if err != nil || !found {
				t.Fatal(err)
			}
			tip := n.core.Tip()
			n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return network.ErrDurabilityUnavailable })
			if _, err := n.ResolveRequest(ctx, kind, id); !errors.Is(err, network.ErrCommitUnknown) {
				t.Fatal("unconfirmed result returned success", err)
			}
			if _, err := n.ResolveRequest(ctx, kind, "missing-original"); !errors.Is(err, network.ErrCommitUnknown) {
				t.Fatal("missing original returned success", err)
			}
			after, found, err := n.RetainedRequest(ctx, kind, id)
			if err != nil || !found || !reflect.DeepEqual(before, after) || n.core.Tip() != tip || n.material.Tip() != uint64(tip) {
				t.Fatal("UNKNOWN changed original or history", err)
			}
		})
	}
}

func TestManagedRequestResolutionSQLKVCertificateItemBoundaries(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			n, _, _ := newKVWrapperFixture(t)
			id := "cut-boundary-" + kind
			if kind == "sql" {
				if _, err := n.ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: id, SQL: "CREATE TABLE cut_boundary(id INTEGER)"}); err != nil {
					t.Fatal(err)
				}
			} else if _, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: id, Key: id, Value: []byte("retained")}); err != nil {
				t.Fatal(err)
			}
			original, found, err := n.RetainedRequest(ctx, kind, id)
			if err != nil || !found {
				t.Fatal(err)
			}
			if original.DecisionHash != ([32]byte{}) {
				t.Fatal("fixture incorrectly relies on Graph decision hash")
			}
			decision, found := n.core.CertifiedValue(quepaxa.Slot(original.Slot))
			if !found || n.core.ValidateCertifiedValue(ctx, decision) != nil || !managedRequestCertifiedOriginalMatches(decision, original) {
				t.Fatal("actual batch certificate does not bind original")
			}
			if _, err := n.captureManagedRequestResolutionCut(ctx, original); err != nil {
				t.Fatal("valid original cut", err)
			}
			for _, name := range []string{"ordinal", "execution", "canonical", "fingerprint", "request-id", "uncertified-slot"} {
				t.Run(name, func(t *testing.T) {
					bad := original
					switch name {
					case "ordinal":
						bad.BatchIndex = ^uint32(0)
					case "execution":
						bad.ExecutionItem = append(bytes.Clone(bad.ExecutionItem), 0)
					case "canonical":
						bad.CanonicalOriginal = append(bytes.Clone(bad.CanonicalOriginal), 0)
					case "fingerprint":
						bad.Fingerprint[0] ^= 1
					case "request-id":
						bad.RequestID += "different"
					case "uncertified-slot":
						bad.Slot = uint64(n.core.Tip() + 1)
						bad.Receipt.Slot = bad.Slot
					}
					if _, err := n.captureManagedRequestResolutionCut(ctx, bad); err == nil {
						t.Fatal("mismatched/uncertified original established cut")
					}
				})
			}
		})
	}
}
