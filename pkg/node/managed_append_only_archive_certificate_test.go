package node

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type managedAppendOnlyCertificateSource struct {
	*quepaxa.Core
	decision quepaxa.DecidedValue
}

func (s managedAppendOnlyCertificateSource) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	decisions, tip, err := s.Core.DecisionsFromBounded(from, itemLimit, payloadByteLimit)
	if err != nil {
		return nil, tip, err
	}
	for i := range decisions {
		if decisions[i].Slot == s.decision.Slot {
			decisions[i].Certificate = append([]byte(nil), s.decision.Certificate...)
			break
		}
	}
	return decisions, tip, nil
}

func TestManagedAppendOnlyRootSelectorArchiveCertificateMustMatchCertifiedDecision(t *testing.T) {
	tests := []struct {
		name        string
		certificate func(*testing.T, *quepaxa.Core, quepaxa.DecidedValue) []byte
		wantError   bool
		cacheModes  []string
	}{
		{
			name: "malformed certificate",
			certificate: func(t *testing.T, _ *quepaxa.Core, _ quepaxa.DecidedValue) []byte {
				t.Helper()
				return []byte("not a QuePaxa certificate")
			},
			wantError:  true,
			cacheModes: []string{"hit", "miss"},
		},
		{
			name: "wrong slot certificate",
			certificate: func(t *testing.T, core *quepaxa.Core, _ quepaxa.DecidedValue) []byte {
				t.Helper()
				previous, ok := core.CertifiedValue(1)
				if !ok {
					t.Fatal("actual slot-1 certificate unavailable")
				}
				return previous.Certificate
			},
			wantError:  true,
			cacheModes: []string{"hit", "miss"},
		},
		{
			name: "certificate without quorum",
			certificate: func(t *testing.T, _ *quepaxa.Core, actual quepaxa.DecidedValue) []byte {
				t.Helper()
				var certificate map[string]json.RawMessage
				if err := json.Unmarshal(actual.Certificate, &certificate); err != nil {
					t.Fatalf("decode actual certificate for quorum-negative case: %v", err)
				}
				certificate["summaries"] = json.RawMessage("[]")
				encoded, err := json.Marshal(certificate)
				if err != nil {
					t.Fatalf("encode quorum-negative certificate: %v", err)
				}
				return encoded
			},
			wantError:  true,
			cacheModes: []string{"hit", "miss"},
		},
		{
			name: "valid certificate with JSON whitespace",
			certificate: func(_ *testing.T, _ *quepaxa.Core, actual quepaxa.DecidedValue) []byte {
				return append(append([]byte(nil), actual.Certificate...), []byte(" \n\t")...)
			},
			cacheModes: []string{"hit", "miss"},
		},
	}

	for _, test := range tests {
		for _, cacheMode := range test.cacheModes {
			t.Run(test.name+"/cache-"+cacheMode, func(t *testing.T) {
				ctx := context.Background()
				n, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
				actual, ok := core.CertifiedValue(2)
				if !ok || actual.Slot != 2 || len(actual.Certificate) == 0 {
					t.Fatalf("actual slot-2 QCKP certificate unavailable: %+v ok=%t", actual, ok)
				}
				altered := actual
				altered.Certificate = test.certificate(t, core, actual)
				if len(altered.Certificate) == 0 {
					t.Fatal("test certificate substitution is empty")
				}
				if actual.Hash != altered.Hash || !bytes.Equal(actual.Value, altered.Value) || actual.Slot != altered.Slot {
					t.Fatal("test substitution changed the actual certified decision identity")
				}
				publisher := archive
				if cacheMode == "miss" {
					var err error
					publisher, err = recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
					if err != nil {
						t.Fatal("create independent archive publisher:", err)
					}
					t.Cleanup(func() {
						publisher.Close()
					})
					publisher.SetGroupDelay(0)
					if err := publisher.Load(ctx); err != nil {
						t.Fatal("load independent archive publisher:", err)
					}
				}
				if err := publisher.SyncThrough(ctx, managedAppendOnlyCertificateSource{Core: core, decision: altered}, actual.Slot); err != nil {
					t.Fatalf("publish actual decision with substituted archive certificate: %v", err)
				}
				before := bucket.mutationSnapshot()
				root, seal, err := n.selectManagedAppendOnlyRoot(ctx)
				after := bucket.mutationSnapshot()
				if len(after) != len(before) {
					t.Fatalf("read-only selection mutated provider: new mutations=%+v", after[len(before):])
				}
				localAfter, stillCertified := core.CertifiedValue(actual.Slot)
				if core.Tip() != actual.Slot || !stillCertified || localAfter.Hash != actual.Hash || !bytes.Equal(localAfter.Value, actual.Value) || !bytes.Equal(localAfter.Certificate, actual.Certificate) {
					t.Fatalf("selector imported or changed local certification: tip=%d stillCertified=%t local=%+v", core.Tip(), stillCertified, localAfter)
				}
				if test.wantError {
					if err == nil || root != nil || seal != nil {
						t.Fatalf("selection accepted substituted archive certificate: root=%+v seal=%+v err=%v", root, seal, err)
					}
					sealed, found, sealErr := core.LatestManagedCheckpointSeal()
					if sealErr != nil || !found {
						t.Fatalf("actual local QCKP seal unavailable for promotion check: found=%t err=%v", found, sealErr)
					}
					if promoteErr := n.promoteManagedAppendOnlySeal(ctx, sealed); promoteErr == nil {
						t.Fatal("promotion accepted substituted archive certificate")
					}
					if got := bucket.mutationSnapshot(); len(got) != len(before) {
						t.Fatalf("certificate refusal mutated provider during promotion: new mutations=%+v", got[len(before):])
					}
					return
				}
				if err != nil || root == nil || seal == nil || seal.DecisionSlot != actual.Slot {
					t.Fatalf("selection rejected valid certificate encoding variant: root=%+v seal=%+v err=%v", root, seal, err)
				}
			})
		}
	}
}
