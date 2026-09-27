package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type archiveMemoryProposal struct {
	Priority   quepaxa.Priority  `json:"priority"`
	ProposerID quepaxa.NodeID    `json:"proposer_id"`
	Hash       quepaxa.ValueHash `json:"hash"`
}

type archiveMemorySummary struct {
	RecorderID   quepaxa.NodeID         `json:"recorder_id"`
	Step         quepaxa.Step           `json:"step"`
	FirstCurrent *archiveMemoryProposal `json:"first_current,omitempty"`
}

type archiveMemoryCertificate struct {
	ConfigID  uint                   `json:"config_id"`
	Slot      quepaxa.Slot           `json:"slot"`
	Step      quepaxa.Step           `json:"step"`
	Proposal  archiveMemoryProposal  `json:"proposal"`
	Summaries []archiveMemorySummary `json:"summaries"`
}

type archiveMemorySource struct {
	count        int
	valueSize    int
	prefixes     [][32]byte
	payloadBytes uint64
}

func newArchiveMemorySource(count, valueSize int) (*archiveMemorySource, error) {
	source := &archiveMemorySource{count: count, valueSize: valueSize, prefixes: make([][32]byte, count+1)}
	value := make([]byte, valueSize)
	for i := 1; i <= count; i++ {
		slot := quepaxa.Slot(i)
		fillArchiveMemoryValue(value, slot)
		hash := sha256.Sum256(value)
		certificate, err := archiveMemoryCertificateBytes(slot, hash)
		if err != nil {
			return nil, err
		}
		source.payloadBytes += uint64(len(value) + len(certificate))
		source.prefixes[i] = quepaxa.AdvancePrefixHash(source.prefixes[i-1], slot, hash)
	}
	return source, nil
}

func fillArchiveMemoryValue(value []byte, slot quepaxa.Slot) {
	for i := range value {
		value[i] = byte(slot*31 + quepaxa.Slot(i%251))
	}
	if len(value) >= 8 {
		binary.BigEndian.PutUint64(value[:8], uint64(slot))
	}
}

func archiveMemoryCertificateBytes(slot quepaxa.Slot, hash quepaxa.ValueHash) ([]byte, error) {
	proposal := archiveMemoryProposal{Priority: quepaxa.Priority{0: 1}, ProposerID: "memory-test", Hash: hash}
	certificate := archiveMemoryCertificate{
		ConfigID: 1, Slot: slot, Step: 4, Proposal: proposal,
		Summaries: []archiveMemorySummary{{RecorderID: "memory-test", Step: 4, FirstCurrent: &proposal}},
	}
	return json.Marshal(certificate)
}

func (s *archiveMemorySource) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	if from == 0 {
		from = 1
	}
	if itemLimit <= 0 || payloadByteLimit <= 0 {
		return nil, quepaxa.Slot(s.count), fmt.Errorf("decision limits must be positive")
	}
	capacity := max(0, min(itemLimit, s.count-int(from)+1))
	decisions := make([]quepaxa.DecidedValue, 0, capacity)
	used := 0
	for slot := from; slot <= quepaxa.Slot(s.count) && len(decisions) < itemLimit; slot++ {
		value := make([]byte, s.valueSize)
		fillArchiveMemoryValue(value, slot)
		hash := sha256.Sum256(value)
		certificate, err := archiveMemoryCertificateBytes(slot, hash)
		if err != nil {
			return nil, quepaxa.Slot(s.count), err
		}
		payloadSize := len(value) + len(certificate)
		if payloadSize > payloadByteLimit-used {
			if len(decisions) == 0 {
				return nil, quepaxa.Slot(s.count), fmt.Errorf("decision %d exceeds source payload limit", slot)
			}
			break
		}
		decisions = append(decisions, quepaxa.DecidedValue{Slot: slot, Hash: hash, Value: value, Certificate: certificate})
		used += payloadSize
	}
	return decisions, quepaxa.Slot(s.count), nil
}

func (s *archiveMemorySource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	if slot > quepaxa.Slot(s.count) {
		return [32]byte{}, false
	}
	return s.prefixes[slot], true
}

func (s *archiveMemorySource) Tip() quepaxa.Slot { return quepaxa.Slot(s.count) }

func newArchiveMemoryBucket(t testing.TB, dir string) *objmetrics.MeteredBucket {
	t.Helper()
	bucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return bucket
}

type archiveMemoryCASConflictBucket struct {
	objstore.Bucket
	armed atomic.Bool
	run   func() error
}

type archiveMemoryMetricsBucket struct {
	objstore.Bucket
	puts          atomic.Uint64
	gets          atomic.Uint64
	attributes    atomic.Uint64
	uploadedBytes atomic.Uint64
}

type archiveMemoryCountingReader struct {
	reader io.Reader
	bytes  *atomic.Uint64
}

func (r archiveMemoryCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.bytes.Add(uint64(n))
	}
	return n, err
}

func (b *archiveMemoryMetricsBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.puts.Add(1)
	return b.Bucket.Upload(ctx, name, archiveMemoryCountingReader{reader: reader, bytes: &b.uploadedBytes}, options...)
}

func (b *archiveMemoryMetricsBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, name)
}

func (b *archiveMemoryMetricsBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.attributes.Add(1)
	return b.Bucket.Attributes(ctx, name)
}

func (b *archiveMemoryCASConflictBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "archive/head.bin") && b.armed.CompareAndSwap(true, false) {
		if err := b.run(); err != nil {
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestArchivePublicationRetainsOnlyBoundedDecisionBodies(t *testing.T) {
	const (
		valueSize = 4 << 10
		shortN    = maxExtentItems
		longN     = 6 * maxExtentItems
	)
	shortBefore, shortAfter, shortExtents := measureArchiveMemoryAtHead(t, shortN, valueSize, "short")
	longBefore, longAfter, longExtents := measureArchiveMemoryAtHead(t, longN, valueSize, "long")
	t.Logf("retained HeapAlloc delta before HEAD publish: short=%d B (%d extents), long=%d B (%d extents); after stable publish: short=%d B, long=%d B", shortBefore, shortExtents, longBefore, longExtents, shortAfter, longAfter)
	if shortExtents < 1 || longExtents < 6 {
		t.Fatalf("archive extents short=%d long=%d; wanted one and at least six", shortExtents, longExtents)
	}
	const retainedSlack = 12 << 20
	if longBefore > shortBefore+retainedSlack {
		t.Fatalf("pre-publish retained heap grew with backlog: short=%d B long=%d B (allowed growth %d B)", shortBefore, longBefore, retainedSlack)
	}
	if longAfter > shortAfter+retainedSlack {
		t.Fatalf("post-publish retained heap grew with backlog: short=%d B long=%d B (allowed growth %d B)", shortAfter, longAfter, retainedSlack)
	}
}

func measureArchiveMemoryAtHead(t *testing.T, count, valueSize int, label string) (beforePublish, afterPublish uint64, extents int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	source, err := newArchiveMemorySource(count, valueSize)
	if err != nil {
		t.Fatal(err)
	}
	base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
	blocked := &blockingHeadUploadBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{})}
	blocked.armed.Store(true)
	manager := NewManager(blocked, "memory-"+label, 1)
	t.Cleanup(manager.Close)
	if !manager.CASSupported() {
		t.Fatal("filesystem bucket must support conditional HEAD publication for this test")
	}
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	done := make(chan error, 1)
	go func() { done <- manager.syncNow(ctx, source, source.Tip()) }()
	released := false
	doneRead := false
	defer func() {
		if !released {
			close(blocked.release)
			if !doneRead {
				<-done
			}
		}
	}()
	select {
	case <-blocked.started:
	case err := <-done:
		doneRead = true
		t.Fatalf("sync finished before reaching HEAD upload barrier: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for HEAD upload barrier: %v", ctx.Err())
	}
	runtime.GC()
	var atHead runtime.MemStats
	runtime.ReadMemStats(&atHead)
	if atHead.HeapAlloc >= baseline.HeapAlloc {
		beforePublish = atHead.HeapAlloc - baseline.HeapAlloc
	}
	runtime.KeepAlive(source)
	runtime.KeepAlive(manager)
	runtime.KeepAlive(blocked)
	close(blocked.release)
	released = true
	if err := <-done; err != nil {
		t.Fatalf("sync through %d: %v", count, err)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc >= baseline.HeapAlloc {
		afterPublish = after.HeapAlloc - baseline.HeapAlloc
	}
	manager.mu.Lock()
	extents = len(manager.extents)
	manager.mu.Unlock()
	runtime.KeepAlive(source)
	runtime.KeepAlive(manager)
	return beforePublish, afterPublish, extents
}

func BenchmarkArchiveMemoryPublication(b *testing.B) {
	for _, tc := range []struct {
		name  string
		count int
	}{
		{name: "short-one-extent", count: maxExtentItems},
		{name: "long-six-extents", count: 6 * maxExtentItems},
		{name: "cas-conflict-one-extent", count: maxExtentItems},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const valueSize = 512
			source, err := newArchiveMemorySource(tc.count, valueSize)
			if err != nil {
				b.Fatal(err)
			}
			base, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(b.TempDir(), "objects")})
			if err != nil {
				b.Fatal(err)
			}
			defer base.Close()
			counter := &archiveMemoryMetricsBucket{Bucket: base}
			var puts, gets, headAttrs, uploadedBytes uint64
			var extentCount int
			ctx := context.Background()
			b.ReportAllocs()
			b.SetBytes(int64(source.payloadBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				counter.puts.Store(0)
				counter.gets.Store(0)
				counter.attributes.Store(0)
				counter.uploadedBytes.Store(0)
				prefix := fmt.Sprintf("archive-memory-%s-%d", tc.name, i)
				var bucket objstore.Bucket = counter
				var competitor *Manager
				if tc.name == "cas-conflict-one-extent" {
					competitor = NewManager(counter, prefix, 1)
					conflict := &archiveMemoryCASConflictBucket{Bucket: counter, run: func() error {
						return competitor.syncNow(ctx, source, source.Tip())
					}}
					conflict.armed.Store(true)
					bucket = conflict
				}
				manager := NewManager(bucket, prefix, 1)
				if !manager.CASSupported() {
					b.Fatal("filesystem bucket does not support CAS")
				}
				if err := manager.syncNow(ctx, source, source.Tip()); err != nil {
					b.Fatal(err)
				}
				manager.mu.Lock()
				extentCount = len(manager.extents)
				manager.mu.Unlock()
				puts, gets, headAttrs, uploadedBytes = counter.puts.Load(), counter.gets.Load(), counter.attributes.Load(), counter.uploadedBytes.Load()
				manager.Close()
				if competitor != nil {
					competitor.Close()
				}
				b.StopTimer()
				removeArchiveMemoryPrefix(b, base, prefix)
				b.StartTimer()
			}
			if b.N != 0 {
				b.ReportMetric(float64(source.payloadBytes), "decision-payload-B/op")
				b.ReportMetric(float64(uploadedBytes), "uploaded-B/op")
				b.ReportMetric(float64(extentCount), "extents/op")
				b.ReportMetric(float64(puts), "PUT/op")
				b.ReportMetric(float64(gets), "GET/op")
				b.ReportMetric(float64(headAttrs), "ATTR/op")
			}
			runtime.KeepAlive(source)
		})
	}
}

func removeArchiveMemoryPrefix(tb testing.TB, bucket objstore.Bucket, prefix string) {
	tb.Helper()
	ctx := context.Background()
	var names []string
	if err := bucket.Iter(ctx, prefix+"/", func(name string) error {
		names = append(names, name)
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		tb.Fatal(err)
	}
	for _, name := range names {
		if err := bucket.Delete(ctx, name); err != nil {
			tb.Fatal(err)
		}
	}
}
