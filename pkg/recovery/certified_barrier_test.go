package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestVerifyCertifiedBarrier(t *testing.T) {
	ctx := context.Background()
	member := quepaxa.Member{ID: "source"}
	initial := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	source, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}

	var nonce [quepaxa.ReadBarrierNonceSize]byte
	nonce[0] = 0x5a
	barrierSlot, _, err := source.Propose(ctx, quepaxa.EncodeReadBarrier(nonce))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(ctx, []byte("after-barrier")); err != nil {
		t.Fatal(err)
	}

	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	if err := writer.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	verifier := NewManager(bucket, "cluster", 1)
	defer verifier.Close()

	decision, membership, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, nonce)
	if err != nil {
		t.Fatalf("verify valid barrier: %v", err)
	}
	if decision.Slot != barrierSlot || membership.ConfigID != initial.ConfigID || len(membership.Members) != 1 || membership.Members[0].ID != member.ID {
		t.Fatalf("verified wrong barrier/membership: slot=%d membership=%+v", decision.Slot, membership)
	}

	wrongNonce := nonce
	wrongNonce[1] = 0x01
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, wrongNonce); err == nil {
		t.Fatal("verification accepted a different nonce")
	}
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot+1, nonce); err == nil {
		t.Fatal("verification accepted a non-barrier value at another slot")
	}

	// A verifier must not trust a long-lived caller Manager's validated extent
	// cache after the backing object disappears.
	if err := verifier.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifier.DecisionsFrom(ctx, 1, 256); err != nil {
		t.Fatal(err)
	}
	verifier.mu.Lock()
	ref := verifier.extents[0]
	verifier.mu.Unlock()
	if err := bucket.Delete(ctx, verifier.key(extentObjectKey(ref.hash, ref.object))); err != nil {
		t.Fatal(err)
	}
	// The old receiver's cache is specifically unsafe here: with an unchanged
	// HEAD it still reports the formerly validated extent as readable.
	if err := verifier.Load(ctx); err != nil {
		t.Fatalf("cached receiver reload unexpectedly noticed immutable extent loss: %v", err)
	}
	if values, _, err := verifier.DecisionsFrom(ctx, 1, 256); err != nil || len(values) == 0 {
		t.Fatalf("cached receiver did not reproduce stale extent evidence: values=%d err=%v", len(values), err)
	}
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, nonce); err == nil {
		t.Fatal("verification trusted cached extent after its backing object disappeared")
	}
}

func TestArchiveHeadRejectsGenesisPrefix(t *testing.T) {
	_, err := encodeHead(archiveHead{
		ConfigID: 1, Generation: 1, BasePrefix: [32]byte{1}, Tip: 1,
		TailHash: [32]byte{2}, TailObject: 1,
	})
	if err == nil {
		t.Fatal("archive codec accepted a nonzero genesis prefix")
	}
}

func TestReplayCertifiedThroughRejectsMissingAndCanceledPages(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "observer"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: "observer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}, WAL: wal,
	})
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("page unavailable")
	err = ReplayCertifiedThrough(ctx, core, 1, func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		return nil, 1, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("missing page error=%v, want %v", err, wantErr)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = ReplayCertifiedThrough(canceled, core, 1, func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		t.Fatal("readPage called for canceled context")
		return nil, 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replay error=%v, want context.Canceled", err)
	}
}
