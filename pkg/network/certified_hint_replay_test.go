package network

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestServerCertifiedHintReplayPersistsBeforeApply(t *testing.T) {
	for _, mode := range []string{"success", "capacity", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("hint-replay", "a", "a"), testMember("hint-replay", "b", "b"), testMember("hint-replay", "c", "c")}}
			transport := &removalCoreTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core)}
			for _, member := range cluster.Members {
				wal, err := qlog.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = wal.Close() })
				core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport})
				if err != nil {
					t.Fatal(err)
				}
				transport.cores[member.ID] = core
			}
			command := types.SQLCommand{RequestID: "hint-replay-original", SQL: "CREATE TABLE hint_replay_original(id INTEGER PRIMARY KEY)"}
			item, err := types.EncodeSQLBatchItem(command)
			if err != nil {
				t.Fatal(err)
			}
			slot, _, err := transport.cores["a"].Propose(ctx, types.AssembleSQLBatch([][]byte{item}))
			if err != nil {
				t.Fatal("actual three-voter proposal", err)
			}
			value, found := transport.cores["a"].CertifiedValue(slot)
			if !found || slot != 1 || len(value.Certificate) == 0 {
				t.Fatal("actual quorum certificate unavailable")
			}
			dir := t.TempDir()
			walPath := filepath.Join(dir, "qlog")
			wal, err := qlog.Open(walPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wal.Close() })
			core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "replay-inspector", Cluster: cluster, WAL: wal})
			if err != nil {
				t.Fatal(err)
			}
			if err := core.ValidateCertifiedValue(ctx, value); err != nil {
				t.Fatal(err)
			}
			if err := core.AcceptCertifiedHints([]quepaxa.DecidedValue{value}); err != nil {
				t.Fatal(err)
			}
			prefix, ok := core.PrefixHash(slot)
			if !ok {
				t.Fatal("actual certified prefix missing")
			}
			matPath := filepath.Join(dir, "sqlite.db")
			mat, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(matPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = mat.Close() })
			server := &Server{core: core, material: mat}
			applyCtx := ctx
			if mode == "capacity" {
				// A fresh observer WAL can contain zero bytes. Admit a positive
				// ceiling smaller than this real certified decision frame.
				if err := wal.SetMaxBytes(max(int64(1), wal.Bytes())); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "canceled" {
				canceled, stop := context.WithCancel(ctx)
				stop()
				applyCtx = canceled
			}
			applyErr := server.applyDecisions(applyCtx, slot)
			original, exists, err := mat.RetainedRequestOriginal(ctx, types.MutationSQL, command.RequestID)
			if mode != "success" {
				want := qlog.ErrCapacity
				if mode == "canceled" {
					want = context.Canceled
				}
				if !errors.Is(applyErr, want) || err != nil || exists || mat.Tip() != 0 {
					t.Fatalf("failed certificate persistence changed application/receipt: tip=%d exists=%t err=%v lookup=%v", mat.Tip(), exists, applyErr, err)
				}
				return
			}
			if applyErr != nil || err != nil || !exists || original.Slot != uint64(slot) || original.SQL == nil || original.SQL.RequestID != command.RequestID || original.SQL.SQL != command.SQL || original.Receipt.Status != types.MutationCommitted {
				t.Fatal("actual certified SQL original not applied", applyErr, err)
			}
			if err := errors.Join(wal.Sync(), wal.Close(), mat.Close()); err != nil {
				t.Fatal(err)
			}
			inspection, err := qlog.OpenReadOnly(walPath)
			if err != nil {
				t.Fatal(err)
			}
			defer inspection.Close()
			recovered, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "reopened-inspector", Cluster: cluster, WAL: inspection})
			if err != nil {
				t.Fatal(err)
			}
			got, present := recovered.CertifiedValue(slot)
			gotPrefix, prefixOK := recovered.PrefixHash(slot)
			if recovered.Tip() != slot || !present || !reflect.DeepEqual(got, value) || !prefixOK || gotPrefix != prefix {
				t.Fatal("application outlived its exact WAL certificate/prefix")
			}
			reopened, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(matPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			after, exists, err := reopened.RetainedRequestOriginal(ctx, types.MutationSQL, command.RequestID)
			if err != nil || !exists || !reflect.DeepEqual(original, after) || reopened.Tip() != uint64(slot) {
				t.Fatal("SQL reopen changed exact original/receipt", err)
			}
		})
	}
}
