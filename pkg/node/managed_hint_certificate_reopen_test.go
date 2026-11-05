//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A certified hint is allowed to be memory-only in Core. Once Node applies
// it to durable application state, that application must not outlive the local
// certificate needed by retained-startup verification. No certificate is
// fabricated here: slot 1 was proposed by the fixture's real three-voter quorum.
func TestManagedGraphAppliedHintCertificateSurvivesWALReopen(t *testing.T) {
	for _, mode := range []string{"success", "capacity", "canceled"} {
		t.Run(mode, func(t *testing.T) { testManagedGraphAppliedHintReopen(t, mode) })
	}
}

func testManagedGraphAppliedHintReopen(t *testing.T, mode string) {
	f := newPreparedLearnerFixture(t)
	value, found := f.sourceCore.CertifiedValue(1)
	if !found || value.Slot != 1 || len(value.Certificate) == 0 {
		t.Fatal("source lacks its actual quorum-certified Graph original")
	}
	cluster := f.sourceCore.ClusterForSlot(value.Slot)
	dir := t.TempDir()
	walPath := filepath.Join(dir, "qlog")
	wal, err := qlog.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "hint-reopen-observer", Cluster: cluster, WAL: wal, EnableReconfiguration: true, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.ValidateCertifiedValue(f.ctx, value); err != nil {
		t.Fatal("source certificate is invalid", err)
	}
	if err := core.AcceptCertifiedHints([]quepaxa.DecidedValue{value}); err != nil {
		t.Fatal("accept actual certified hint", err)
	}
	prefix, ok := core.PrefixHash(value.Slot)
	if !ok || prefix == ([32]byte{}) {
		t.Fatal("hint lacks its exact certified prefix")
	}
	matPath := filepath.Join(dir, "sqlite.db")
	mat, err := f.node.openMaterializer(matPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	target := &Node{core: core, material: mat}
	applyCtx := f.ctx
	if mode == "capacity" {
		if err := wal.SetMaxBytes(wal.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "canceled" {
		ctx, cancel := context.WithCancel(f.ctx)
		cancel()
		applyCtx = ctx
	}
	applyErr := target.replayLocalDecisions(applyCtx)
	if mode != "success" {
		want := qlog.ErrCapacity
		if mode == "canceled" {
			want = context.Canceled
		}
		if !errors.Is(applyErr, want) || mat.Tip() != 0 {
			t.Fatalf("failed certificate persistence advanced application: tip=%d err=%v", mat.Tip(), applyErr)
		}
		if _, exists, err := mat.RetainedRequestOriginal(f.ctx, types.MutationGraph, "graph-alternate-source"); err != nil || exists {
			t.Fatal("failed certificate persistence minted a receipt", err)
		}
		got, exists := core.CertifiedValue(value.Slot)
		gotPrefix, ok := core.PrefixHash(value.Slot)
		if !exists || !reflect.DeepEqual(got, value) || !ok || gotPrefix != prefix {
			t.Fatal("failed apply changed the actual certified hint")
		}
		return
	}
	if applyErr != nil {
		t.Fatal("apply the actual certified hint through Node replay", applyErr)
	}
	original, exists, err := mat.RetainedRequestOriginal(f.ctx, types.MutationGraph, "graph-alternate-source")
	if err != nil || !exists || original.Slot != uint64(value.Slot) || original.DecisionHash != value.Hash || !bytes.Equal(original.ExecutionItem, value.Value) || mat.Tip() != uint64(value.Slot) {
		t.Fatal("applied original lost exact certified slot/value binding", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mat.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedMaterial, err := f.node.openMaterializer(matPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedMaterial.Close()
	after, exists, err := reopenedMaterial.RetainedRequestOriginal(f.ctx, types.MutationGraph, original.RequestID)
	if err != nil || !exists || !reflect.DeepEqual(after, original) || reopenedMaterial.Tip() != uint64(value.Slot) {
		t.Fatal("application reopen changed original bytes/slot/receipt", err)
	}
	inspection, err := qlog.OpenReadOnly(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	recovered, err := newManagedReadOnlyCore("hint-reopen-inspector", cluster, inspection, true, f.node.materializationPolicy())
	if err != nil {
		t.Fatal(err)
	}
	got, hasCertificate := recovered.CertifiedValue(value.Slot)
	gotPrefix, hasPrefix := recovered.PrefixHash(value.Slot)
	if recovered.Tip() < value.Slot || !hasCertificate || !reflect.DeepEqual(got, value) || !hasPrefix || gotPrefix != prefix {
		t.Fatalf("applied certified original outlived its WAL certificate: applied=%d recovered=%d certificate=%t prefix=%t", reopenedMaterial.Tip(), recovered.Tip(), hasCertificate, hasPrefix)
	}
}
