package recovery

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// TestArchivePublicationLockSerializesIndependentManagers characterizes two
// independent Manager instances sharing one bucket. Both read sequential
// certified windows from the same single-node Core; this is not a multi-voter
// or multi-proposer reproduction. Their distinct targets wait at PUBLISH_LOCK
// and are acknowledged only after their slots are durable. It makes no claim
// about the segmented-extent design proposed in issue #315.
func TestArchivePublicationLockSerializesIndependentManagers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{
		NodeID:  "n1",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first certified target", "second certified target"} {
		if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if core.Tip() != 2 {
		t.Fatalf("certified tip=%d, want 2", core.Tip())
	}

	probe := &archivePublicationLockProbe{
		Bucket:  objstore.NewInMemBucket(),
		held:    make(chan struct{}),
		release: make(chan struct{}),
		busy:    make(chan struct{}),
	}
	probe.holdNext.Store(true)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(probe.release) }) }

	firstManager := NewManager(probe, "cluster", 1)
	secondManager := NewManager(probe, "cluster", 1)
	t.Cleanup(firstManager.Close)
	t.Cleanup(secondManager.Close)
	t.Cleanup(release)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- firstManager.SyncThrough(ctx, archiveCertifiedWindow{source: core, tip: 1}, 1)
	}()
	select {
	case <-probe.held:
	case <-ctx.Done():
		t.Fatalf("first manager did not acquire PUBLISH_LOCK: %v", ctx.Err())
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- secondManager.SyncThrough(ctx, archiveCertifiedWindow{source: core, tip: 2}, 2)
	}()
	select {
	case <-probe.busy:
	case <-ctx.Done():
		t.Fatalf("second manager did not observe the live publication lock: %v", ctx.Err())
	}
	select {
	case err := <-secondDone:
		t.Fatalf("second manager finished while publication was still held: %v", err)
	default:
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatalf("first manager: %v", err)
	}
	firstReader := NewManager(probe.Bucket, "cluster", 1)
	t.Cleanup(firstReader.Close)
	if err := firstReader.Load(ctx); err != nil {
		t.Fatalf("load after first acknowledgement: %v", err)
	}
	if firstReader.Tip() < 1 {
		t.Fatalf("first acknowledgement did not cover slot 1: tip=%d", firstReader.Tip())
	}
	firstValue, ok := core.CertifiedValue(1)
	if !ok {
		t.Fatal("slot 1 is not certified")
	}
	values, _, err := firstReader.DecisionsFrom(ctx, 1, 1)
	if err != nil || len(values) != 1 || !archiveDecisionsEqual(&values[0], &firstValue) {
		t.Fatalf("slot 1 coverage after first acknowledgement: values=%d err=%v", len(values), err)
	}

	if err := <-secondDone; err != nil {
		t.Fatalf("second manager: %v", err)
	}
	if probe.busyReads.Load() == 0 {
		t.Fatal("second manager never observed a busy publication lock")
	}
	reader := NewManager(probe.Bucket, "cluster", 1)
	t.Cleanup(reader.Close)
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load after both acknowledgements: %v", err)
	}
	values, tip, err := reader.DecisionsFrom(ctx, 1, 2)
	if err != nil || tip != 2 || len(values) != 2 {
		t.Fatalf("final coverage: tip=%d values=%d err=%v, want 2/2/nil", tip, len(values), err)
	}
	for slot := quepaxa.Slot(1); slot <= 2; slot++ {
		want, ok := core.CertifiedValue(slot)
		if !ok || !archiveDecisionsEqual(&values[slot-1], &want) {
			t.Fatalf("slot %d does not match its certified decision", slot)
		}
	}
}

// archiveCertifiedWindow exposes a sequential certified prefix of the shared
// Core's log while retaining that Core's decisions and prefix hashes.
type archiveCertifiedWindow struct {
	source source
	tip    quepaxa.Slot
}

func (s archiveCertifiedWindow) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	decisions, tip, err := s.source.DecisionsFromBounded(from, itemLimit, payloadByteLimit)
	if err != nil {
		return decisions, tip, err
	}
	if tip > s.tip {
		tip = s.tip
	}
	return trimArchiveCertifiedWindow(decisions, from, s.tip, itemLimit, payloadByteLimit), tip, nil
}

func trimArchiveCertifiedWindow(decisions []quepaxa.DecidedValue, from, windowTip quepaxa.Slot, itemLimit, payloadByteLimit int) []quepaxa.DecidedValue {
	if from > windowTip || itemLimit <= 0 || payloadByteLimit <= 0 {
		return nil
	}
	limit := min(len(decisions), itemLimit)
	if remaining := uint64(windowTip - from); remaining < uint64(limit) {
		limit = int(remaining) + 1
	}
	decisions = decisions[:limit]
	used := 0
	for i, decision := range decisions {
		remaining := payloadByteLimit - used
		if len(decision.Value) > remaining || len(decision.Certificate) > remaining-len(decision.Value) {
			return decisions[:i]
		}
		used += len(decision.Value) + len(decision.Certificate)
	}
	return decisions
}

func (s archiveCertifiedWindow) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	return s.source.PrefixHash(slot)
}

func (s archiveCertifiedWindow) Tip() quepaxa.Slot { return s.tip }

func TestTrimArchiveCertifiedWindowBounds(t *testing.T) {
	decisions := []quepaxa.DecidedValue{
		{Slot: 1, Value: []byte("a"), Certificate: []byte("1")},
		{Slot: 2, Value: []byte("bb"), Certificate: []byte("22")},
		{Slot: 3, Value: []byte("ccc"), Certificate: []byte("333")},
	}
	for _, test := range []struct {
		name             string
		from, tip        quepaxa.Slot
		itemLimit        int
		payloadByteLimit int
		want             int
	}{
		{name: "from beyond window", from: 3, tip: 2, itemLimit: 3, payloadByteLimit: 100},
		{name: "window before item limit", from: 1, tip: 2, itemLimit: 3, payloadByteLimit: 100, want: 2},
		{name: "item limit before window", from: 1, tip: 3, itemLimit: 2, payloadByteLimit: 100, want: 2},
		{name: "byte limit before item limit", from: 1, tip: 3, itemLimit: 3, payloadByteLimit: 5, want: 1},
		{name: "exact byte limit", from: 1, tip: 3, itemLimit: 3, payloadByteLimit: 6, want: 2},
		{name: "window beyond decisions", from: 1, tip: 9, itemLimit: 9, payloadByteLimit: 100, want: 3},
		{name: "zero item limit", from: 1, tip: 3, payloadByteLimit: 100},
		{name: "zero byte limit", from: 1, tip: 3, itemLimit: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := trimArchiveCertifiedWindow(decisions, test.from, test.tip, test.itemLimit, test.payloadByteLimit)
			if len(got) != test.want {
				t.Fatalf("decisions=%d, want %d", len(got), test.want)
			}
			if len(got) > 0 && got[0].Slot != test.from {
				t.Fatalf("first slot=%d, want %d", got[0].Slot, test.from)
			}
		})
	}
}

// archivePublicationLockProbe pauses the first acquired lock after its write is
// durable so another Manager observes a genuinely live lease, not a fabricated
// busy result or a request-count proxy.
type archivePublicationLockProbe struct {
	objstore.Bucket
	holdNext  atomic.Bool
	holding   atomic.Bool
	held      chan struct{}
	release   chan struct{}
	busyReads atomic.Uint64
	busy      chan struct{}
	busyOnce  sync.Once
}

func (b *archivePublicationLockProbe) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, opts...); err != nil {
		return err
	}
	if !strings.HasSuffix(name, "/archive/PUBLISH_LOCK") || !b.holdNext.CompareAndSwap(true, false) {
		return nil
	}
	b.holding.Store(true)
	defer b.holding.Store(false)
	close(b.held)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return nil
	}
}

func (b *archivePublicationLockProbe) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.HasSuffix(name, "/archive/PUBLISH_LOCK") && b.holding.Load() {
		b.busyReads.Add(1)
		b.busyOnce.Do(func() { close(b.busy) })
	}
	return b.Bucket.Get(ctx, name)
}
