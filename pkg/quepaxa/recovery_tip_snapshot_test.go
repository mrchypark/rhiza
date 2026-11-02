package quepaxa

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// Pause the first loop's cancellation check, after its tip condition has been
// evaluated. This controls a real concurrent certified-prefix installation
// without a production hook or a fabricated decision.
type recoveryTipContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func TestDecisionsFromReturnsBoundedPageAndGlobalTip(t *testing.T) {
	cores, _ := newTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	core := cores["n2"]
	values := []string{"first", "second", "third"}
	for i, value := range values {
		slot, receipts, err := core.Propose(ctx, []byte(value))
		if err != nil || slot != Slot(i+1) || !core.IsQuorum(receipts) {
			t.Fatalf("proposal slot=%d receipts=%+v err=%v", slot, receipts, err)
		}
	}
	for _, tc := range []struct {
		from Slot
		want int
	}{{1, 2}, {3, 1}, {4, 0}} {
		page, tip, err := core.DecisionsFrom(tc.from, 2)
		if err != nil || tip != 3 || len(page) != tc.want {
			t.Fatalf("from=%d page=%+v globalTip=%d err=%v", tc.from, page, tip, err)
		}
		for i, decision := range page {
			slot := tc.from + Slot(i)
			value := values[int(slot)-1]
			if decision.Slot != slot || string(decision.Value) != value || decision.Hash != sha256.Sum256([]byte(value)) || len(decision.Certificate) == 0 {
				t.Fatalf("from=%d index=%d decision=%+v globalTip=%d", tc.from, i, decision, tip)
			}
		}
	}
}

func (c *recoveryTipContext) Err() error {
	c.once.Do(func() {
		close(c.entered)
		select {
		case <-c.release:
		case <-c.Context.Done():
		}
	})
	return c.Context.Err()
}

func TestRecoveryThroughUsesSingleTipSnapshot(t *testing.T) {
	cores, transport := newTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := cores["n2"]
	for _, value := range [][]byte{[]byte("first"), []byte("second")} {
		if _, _, err := source.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	values, tip, err := source.DecisionsFrom(1, 2)
	if err != nil || tip != 2 || len(values) != 2 {
		t.Fatalf("source prefix tip=%d count=%d err=%v", tip, len(values), err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	target := newCore("n2", source.config, wal, transport)
	paused := &recoveryTipContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(paused.release) }) }
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- target.RecoverThrough(paused, 1)
	}()
	t.Cleanup(func() {
		release()
		cancel()
		<-done // Drain owned recovery before its WAL is closed.
	})
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal("recovery did not reach cancellation check")
	}
	if err := target.AcceptCertifiedValues(values); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != 2 {
		t.Fatalf("installed prefix tip=%d, want 2", target.Tip())
	}
	release()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("recovery did not finish")
	}
	if extra, ok := target.CertifiedValue(3); ok {
		barrier, decodeErr := DecodeReadBarrier(extra.Value)
		t.Fatalf("recovery through 1 created slot 3: barrier=%v decodeErr=%v tip=%d", barrier, decodeErr, target.Tip())
	}
	if target.Tip() != 2 || target.RecorderTip() != 2 {
		t.Fatalf("recovery changed installed prefix: tip=%d recorder=%d", target.Tip(), target.RecorderTip())
	}
}
