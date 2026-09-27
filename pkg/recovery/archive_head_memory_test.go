package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// archiveHeadEvidenceBucket counts logical object requests and bytes actually
// consumed/produced, while the underlying filesystem bucket avoids retaining
// uploaded extent bodies in this test process.
type archiveHeadEvidenceBucket struct {
	objstore.Bucket
	gets, puts, attrs, iters atomic.Uint64
	readBytes, writeBytes    atomic.Uint64
}

type archiveHeadEvidenceReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (r archiveHeadEvidenceReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.n.Add(uint64(n))
	}
	return n, err
}

func (b *archiveHeadEvidenceBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.puts.Add(1)
	return b.Bucket.Upload(ctx, name, archiveHeadEvidenceReader{r: r, n: &b.writeBytes}, opts...)
}

func (b *archiveHeadEvidenceBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	r, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return &archiveHeadEvidenceReadCloser{ReadCloser: r, count: &b.readBytes}, nil
}

type archiveHeadEvidenceReadCloser struct {
	io.ReadCloser
	count *atomic.Uint64
}

func (r *archiveHeadEvidenceReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.count.Add(uint64(n))
	}
	return n, err
}

func (b *archiveHeadEvidenceBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.attrs.Add(1)
	return b.Bucket.Attributes(ctx, name)
}

func (b *archiveHeadEvidenceBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	b.iters.Add(1)
	return b.Bucket.Iter(ctx, dir, f, opts...)
}

func (b *archiveHeadEvidenceBucket) reset() {
	b.gets.Store(0)
	b.puts.Store(0)
	b.attrs.Store(0)
	b.iters.Store(0)
	b.readBytes.Store(0)
	b.writeBytes.Store(0)
}

func (b *archiveHeadEvidenceBucket) metrics() (gets, puts, attrs, iters, readBytes, writeBytes uint64) {
	return b.gets.Load(), b.puts.Load(), b.attrs.Load(), b.iters.Load(), b.readBytes.Load(), b.writeBytes.Load()
}

type archiveHeadEvidenceFixture struct {
	bucket     *archiveHeadEvidenceBucket
	prefix     string
	manager    *Manager
	core       *quepaxa.Core
	head       archiveHead
	encoded    []byte
	seal       quepaxa.CheckpointSeal
	decision   quepaxa.DecidedValue
	formatOnly bool
}

func newArchiveHeadEvidenceFixture(t testing.TB, name string, urlBytes int, transitions, maxPayload bool) *archiveHeadEvidenceFixture {
	t.Helper()
	if maxPayload {
		return newArchiveHeadFormatOnlyMaximumFixture(t, name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
	bucket := &archiveHeadEvidenceBucket{Bucket: base}
	prefix := "head-evidence-" + name
	url := "https://node.invalid/" + strings.Repeat("a", urlBytes-len("https://node.invalid/"))
	members := []quepaxa.Member{{ID: "n1", URL: url}}
	if transitions {
		members = append(members, quepaxa.Member{ID: "n2"}, quepaxa.Member{ID: "n3"})
	}
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: members}
	transport := newLargeProofCluster(t, bootstrap.Members)
	core := transport.cores["n1"]
	if transitions {
		for _, target := range []quepaxa.Cluster{
			{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)},
			{ConfigID: 3, Members: append([]quepaxa.Member(nil), members[:1]...)},
		} {
			if _, err := core.BeginReconfiguration(ctx, target); err != nil {
				t.Fatalf("begin evidence membership transition: %v", err)
			}
			if err := core.FinishReconfiguration(ctx); err != nil {
				t.Fatalf("finish evidence membership transition: %v", err)
			}
		}
	}
	command, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "head-evidence-" + name, Events: []types.GraphStreamEvent{{Stream: "evidence", Kind: "created", Payload: "head"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatalf("propose evidence command: %v", err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "materializer.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	for slot := quepaxa.Slot(1); slot <= core.Tip(); slot++ {
		value, ok := core.CertifiedValue(slot)
		if !ok {
			t.Fatalf("missing evidence decision at slot %d", slot)
		}
		if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{value}); err != nil {
			t.Fatalf("apply evidence decision: %v", err)
		}
	}
	cp := checkpoint.NewManager(bucket, prefix, "", 1)
	claim, err := cp.AcquirePublisherClaim(ctx, prefix, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimReleased := false
	t.Cleanup(func() {
		if !claimReleased {
			_ = cp.ReleasePublisherClaim(ctx, claim)
		}
	})
	files, index, closeFiles, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFiles)
	sources := make([]checkpoint.Source, 0, len(files))
	for _, f := range files {
		sources = append(sources, checkpoint.Source{Role: string(f.Role), Path: f.Path})
	}
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefixHash, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("missing evidence prefix hash")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: core.ConfigID(), Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefixHash, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("prepare valid evidence checkpoint: %v", err)
	}
	sealBytes, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decisionSlot, _, err := core.Propose(ctx, sealBytes)
	if err != nil {
		t.Fatalf("certify evidence checkpoint: %v", err)
	}
	decision, ok := core.CertifiedValue(decisionSlot)
	if !ok {
		t.Fatal("missing evidence checkpoint certificate")
	}
	if err := core.ValidateCheckpointBase(ctx, seal, decision); err != nil {
		t.Fatalf("authenticate evidence checkpoint membership: %v", err)
	}
	manager := NewManager(bucket, prefix, 1)
	t.Cleanup(manager.Close)
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("publish evidence decisions: %v", err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("latest evidence seal: ok=%v err=%v", ok, err)
	}
	if err := manager.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatalf("trim evidence archive: %v", err)
	}
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatalf("release checkpoint publisher: %v", err)
	}
	claimReleased = true
	manager.mu.Lock()
	head := manager.head
	manager.mu.Unlock()
	encoded, err := encodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	return &archiveHeadEvidenceFixture{bucket: bucket, prefix: prefix, manager: manager, core: core, head: head, encoded: encoded, seal: seal, decision: decision}
}

// The maximum combined-payload case is a format-limit fixture, not an
// authenticated consensus proof. Keep it independent of Core's live worker
// goroutines so process-wide TotalAlloc measures the bounded HEAD read path.
func newArchiveHeadFormatOnlyMaximumFixture(t testing.TB, name string) *archiveHeadEvidenceFixture {
	t.Helper()
	ctx := context.Background()
	base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
	bucket := &archiveHeadEvidenceBucket{Bucket: base}
	prefix := "head-evidence-" + name
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: 1, NextLeaderOrder: []quepaxa.NodeID{"format-only"},
		RootHash: sha256.Sum256([]byte("format-only root")), StateHash: sha256.Sum256([]byte("format-only state")),
		PrefixHash: sha256.Sum256([]byte("format-only prefix")),
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decision := quepaxa.DecidedValue{
		Slot: seal.Index, Hash: sha256.Sum256(value), Value: value,
		Certificate: make([]byte, maxExtentPayload-len(value)),
	}
	for i := range decision.Certificate {
		decision.Certificate[i] = byte(i*31 + 7)
	}
	head := archiveHead{
		ConfigID: 1, Generation: 1, Base: seal.Index, BasePrefix: seal.PrefixHash,
		BaseSeal: &seal, BaseDecision: &decision, Tip: seal.Index,
	}
	encoded, err := encodeHead(head)
	if err != nil {
		t.Fatalf("encode format-only maximum payload HEAD: %v", err)
	}
	if len(decision.Value)+len(decision.Certificate) != maxExtentPayload {
		t.Fatalf("format-only payload=%d, want %d", len(decision.Value)+len(decision.Certificate), maxExtentPayload)
	}
	if err := bucket.Upload(ctx, prefix+"/archive/head.bin", bytes.NewReader(encoded)); err != nil {
		t.Fatalf("write format-only maximum payload HEAD: %v", err)
	}
	manager := NewManager(bucket, prefix, 1)
	t.Cleanup(manager.Close)
	if err := manager.Load(ctx); err != nil {
		t.Fatalf("load format-only maximum payload HEAD: %v", err)
	}
	return &archiveHeadEvidenceFixture{bucket: bucket, prefix: prefix, manager: manager, head: head, encoded: encoded, seal: seal, decision: decision, formatOnly: true}
}

type archiveHeadEvidenceStage struct {
	allocated uint64
	retained  uint64
}

func sampleArchiveHeadEvidence(t testing.TB, keep func() any, operation func() error) archiveHeadEvidenceStage {
	t.Helper()
	runtime.GC()
	var before, after, collected runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := operation(); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(keep())
	runtime.GC()
	runtime.ReadMemStats(&collected)
	runtime.KeepAlive(keep())
	retained := uint64(0)
	if collected.HeapAlloc > before.HeapAlloc {
		retained = collected.HeapAlloc - before.HeapAlloc
	}
	return archiveHeadEvidenceStage{allocated: after.TotalAlloc - before.TotalAlloc, retained: retained}
}

func TestArchiveHeadMemoryEvidenceStages(t *testing.T) {
	for _, tc := range []struct {
		name        string
		urlBytes    int
		transitions bool
		maxPayload  bool
	}{
		{name: "small-authenticated", urlBytes: 512},
		{name: "large-authenticated-history", urlBytes: 13 << 10, transitions: true},
		{name: "maximum-combined-format-only-fake-certificate", urlBytes: 512, maxPayload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveHeadEvidenceFixture(t, tc.name, tc.urlBytes, tc.transitions, tc.maxPayload)
			if !tc.maxPayload {
				if err := f.manager.Load(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := f.manager.Load(context.Background()); err != nil {
					t.Fatalf("stable HEAD readback: %v", err)
				}
			}
			label := "authenticated"
			if f.formatOnly {
				label = "format-only; fake certificate; no authentication claim"
			}
			t.Logf("case=%s (%s) HEAD=%d B payload=%d B", tc.name, label, len(f.encoded), len(f.decision.Value)+len(f.decision.Certificate))
			f.bucket.reset()
			encodedStage := sampleArchiveHeadEvidence(t, func() any { return f.encoded }, func() error {
				var err error
				f.encoded, err = encodeHead(f.head)
				return err
			})
			t.Logf("stage=encode allocated=%d B retained-after-GC=%d B", encodedStage.allocated, encodedStage.retained)
			if encodedStage.allocated > uint64(2*len(f.encoded))+2<<20 {
				t.Fatalf("encode allocation %d exceeds bounded allowance for %d-byte HEAD", encodedStage.allocated, len(f.encoded))
			}
			if encodedStage.retained > uint64(len(f.encoded))+2<<20 {
				t.Fatalf("encode retained heap %d exceeds bounded allowance for %d-byte HEAD", encodedStage.retained, len(f.encoded))
			}

			var decoded archiveHead
			decodeStage := sampleArchiveHeadEvidence(t, func() any { return decoded }, func() error {
				var err error
				decoded, err = decodeHead(f.encoded)
				return err
			})
			t.Logf("stage=decode allocated=%d B retained-after-GC=%d B", decodeStage.allocated, decodeStage.retained)
			if decodeStage.allocated > uint64(len(f.encoded))+2<<20 || decodeStage.retained > uint64(len(f.encoded))+2<<20 {
				t.Fatalf("decode stage exceeds bound: %+v for %d-byte HEAD", decodeStage, len(f.encoded))
			}

			f.bucket.reset()
			reader := NewManager(f.bucket, f.prefix, 1)
			readStage := sampleArchiveHeadEvidence(t, func() any { return reader }, func() error { return reader.Load(context.Background()) })
			gets, _, attrs, _, readBytes, _ := f.bucket.metrics()
			t.Logf("stage=stable-readback allocated=%d B retained-after-GC=%d B GET=%d ATTR=%d read=%d B", readStage.allocated, readStage.retained, gets, attrs, readBytes)
			if gets == 0 || readBytes < uint64(len(f.encoded)) {
				t.Fatalf("stable readback metrics GET=%d bytes=%d HEAD=%d", gets, readBytes, len(f.encoded))
			}
			if !f.formatOnly {
				seal, decision, ok := reader.RecoveryBase()
				if !ok || seal.RootHash != f.seal.RootHash || decision.Slot != f.decision.Slot || decision.Hash != f.decision.Hash || !bytes.Equal(decision.Value, f.sealBytes()) || !bytes.Equal(decision.Certificate, f.decision.Certificate) {
					t.Fatal("stable readback did not preserve the authenticated checkpoint proof")
				}
				if err := f.core.ValidateCheckpointBase(context.Background(), seal, decision); err != nil {
					t.Fatalf("authenticate stable-readback checkpoint: %v", err)
				}
			} else if _, decision, ok := reader.RecoveryBase(); !ok || len(decision.Value)+len(decision.Certificate) != maxExtentPayload {
				t.Fatal("format-only stable readback did not preserve maximum payload")
			}
			readbackAllocBound := uint64(3*len(f.encoded) + maxExtentSize + 4<<20)
			readbackRetainedBound := readbackAllocBound
			if f.formatOnly {
				// Go 1.27 io.ReadAll stages exponentially grown chunks and then
				// allocates/copies the right-sized result. The measured two growth
				// allocations per chunk account for up to about 4x this full-sized
				// input; decodeBaseDecision copies the body once more. This fixture
				// has no tail extents, so 1 MiB covers fixed object/runtime overhead
				// while another whole-HEAD copy still exceeds the bound.
				readbackAllocBound = uint64(5*len(f.encoded) + 1<<20)
				readbackRetainedBound = uint64(len(f.encoded) + 2<<20)
			}
			if readStage.allocated > readbackAllocBound || readStage.retained > readbackRetainedBound {
				t.Fatalf("stable readback exceeds algorithm-derived bounds alloc=%d B retained=%d B: %+v", readbackAllocBound, readbackRetainedBound, readStage)
			}

			f.bucket.reset()
			var snapshot *RecoverySnapshot
			pinStage := sampleArchiveHeadEvidence(t, func() any { return snapshot }, func() error {
				var err error
				snapshot, err = reader.BeginRecoverySnapshot(context.Background(), "head-memory-evidence", time.Minute)
				return err
			})
			if !f.formatOnly {
				seal, decision, ok := snapshot.RecoveryBase()
				if !ok || seal.RootHash != f.seal.RootHash || decision.Slot != f.decision.Slot || decision.Hash != f.decision.Hash || !bytes.Equal(decision.Value, f.sealBytes()) || !bytes.Equal(decision.Certificate, f.decision.Certificate) {
					t.Fatal("pinned snapshot did not retain authenticated checkpoint proof")
				}
				if err := f.core.ValidateCheckpointBase(context.Background(), seal, decision); err != nil {
					t.Fatalf("authenticate pinned snapshot checkpoint: %v", err)
				}
			} else if _, decision, ok := snapshot.RecoveryBase(); !ok || len(decision.Value)+len(decision.Certificate) != maxExtentPayload {
				t.Fatal("format-only pinned snapshot did not retain maximum payload")
			}
			gets, puts, attrs, iters, readBytes, writeBytes := f.bucket.metrics()
			t.Logf("stage=pinned-snapshot allocated=%d B retained-after-GC=%d B GET=%d PUT=%d ATTR=%d ITER=%d read=%d B write=%d B", pinStage.allocated, pinStage.retained, gets, puts, attrs, iters, readBytes, writeBytes)
			if puts == 0 || writeBytes == 0 {
				t.Fatal("pinned snapshot metrics did not observe pin publication")
			}
			pinBound := uint64(3*len(f.encoded) + maxExtentSize + 4<<20)
			if pinStage.allocated > pinBound || pinStage.retained > pinBound {
				t.Fatalf("pinned snapshot exceeds generous bounded allowance %d B: %+v", pinBound, pinStage)
			}
			if err := snapshot.Close(context.Background()); err != nil {
				t.Fatalf("close pinned snapshot: %v", err)
			}
			runtime.KeepAlive(decoded)
			runtime.KeepAlive(reader)
		})
	}
}

func (f *archiveHeadEvidenceFixture) sealBytes() []byte {
	if f.head.BaseDecision == nil {
		return nil
	}
	return f.head.BaseDecision.Value
}

func BenchmarkArchiveHeadEvidenceStages(b *testing.B) {
	for _, tc := range []struct {
		name        string
		urlBytes    int
		transitions bool
		maxPayload  bool
	}{
		{name: "small-authenticated", urlBytes: 512},
		{name: "large-authenticated-history", urlBytes: 13 << 10, transitions: true},
		{name: "maximum-combined-format-only-fake-certificate", urlBytes: 512, maxPayload: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := newArchiveHeadEvidenceFixture(b, tc.name, tc.urlBytes, tc.transitions, tc.maxPayload)
			ctx := context.Background()
			b.SetBytes(int64(len(f.encoded)))
			for _, stageName := range []string{"encode", "decode", "stable-readback", "pinned-snapshot"} {
				b.Run(stageName, func(b *testing.B) {
					f.bucket.reset()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch stageName {
						case "encode":
							if _, err := encodeHead(f.head); err != nil {
								b.Fatal(err)
							}
						case "decode":
							if _, err := decodeHead(f.encoded); err != nil {
								b.Fatal(err)
							}
						case "stable-readback":
							reader := NewManager(f.bucket, f.prefix, 1)
							if err := reader.Load(ctx); err != nil {
								b.Fatal(err)
							}
							reader.Close()
						case "pinned-snapshot":
							reader := NewManager(f.bucket, f.prefix, 1)
							if err := reader.Load(ctx); err != nil {
								b.Fatal(err)
							}
							snapshot, err := reader.BeginRecoverySnapshot(ctx, fmt.Sprintf("bench-%d", i), time.Minute)
							if err != nil {
								b.Fatal(err)
							}
							if err := snapshot.Close(ctx); err != nil {
								b.Fatal(err)
							}
							reader.Close()
						}
					}
					b.StopTimer()
					gets, puts, attrs, iters, readBytes, writeBytes := f.bucket.metrics()
					if b.N > 0 {
						b.ReportMetric(float64(len(f.encoded)), "HEAD-B/op")
						b.ReportMetric(float64(gets)/float64(b.N), "GET/op")
						b.ReportMetric(float64(puts)/float64(b.N), "PUT/op")
						b.ReportMetric(float64(attrs)/float64(b.N), "ATTR/op")
						b.ReportMetric(float64(iters)/float64(b.N), "ITER/op")
						b.ReportMetric(float64(readBytes)/float64(b.N), "read-B/op")
						b.ReportMetric(float64(writeBytes)/float64(b.N), "write-B/op")
					}
					runtime.KeepAlive(f)
				})
			}
		})
	}
}
