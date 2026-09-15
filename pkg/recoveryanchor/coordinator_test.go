package recoveryanchor

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

// --- test backend ---

type inMemoryBackend struct {
	mu       sync.Mutex
	records  map[string]recordEntry
	versionN int
}

type recordEntry struct {
	record  Record
	version string
}

func newInMemoryBackend() *inMemoryBackend {
	return &inMemoryBackend{records: make(map[string]recordEntry)}
}

func (b *inMemoryBackend) Read(_ context.Context, id string) (Record, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.records[id]
	if !ok {
		return Record{}, "", fmt.Errorf("anchor %q: %w", id, errTestNotFound)
	}
	// Deep copy.
	rec := e.record
	if rec.Evidence != nil {
		ev := make([]byte, len(rec.Evidence))
		copy(ev, rec.Evidence)
		rec.Evidence = ev
	}
	if rec.Transition != nil {
		tr := *rec.Transition
		if tr.Evidence != nil {
			tev := make([]byte, len(tr.Evidence))
			copy(tev, tr.Evidence)
			tr.Evidence = tev
		}
		rec.Transition = &tr
	}
	if rec.LastTransition != nil {
		lt := *rec.LastTransition
		rec.LastTransition = &lt
	}
	return rec, e.version, nil
}

var errTestNotFound = fmt.Errorf("not found")

func (b *inMemoryBackend) CAS(_ context.Context, id string, version string, record Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.records[id]
	if ok && e.version != version {
		return fmt.Errorf("CAS conflict: stored %q != expected %q", e.version, version)
	}
	b.versionN++
	next := fmt.Sprintf("%d", b.versionN)
	// Deep copy before storing.
	rec := record
	if rec.Evidence != nil {
		ev := make([]byte, len(rec.Evidence))
		copy(ev, rec.Evidence)
		rec.Evidence = ev
	}
	if rec.Transition != nil {
		tr := *rec.Transition
		if tr.Evidence != nil {
			tev := make([]byte, len(tr.Evidence))
			copy(tev, tr.Evidence)
			tr.Evidence = tev
		}
		rec.Transition = &tr
	}
	if rec.LastTransition != nil {
		lt := *rec.LastTransition
		rec.LastTransition = &lt
	}
	b.records[id] = recordEntry{record: rec, version: next}
	return nil
}

// seedRecord inserts a pre-existing record into the backend.
func seedRecord(be *inMemoryBackend, id string, rec Record) {
	be.mu.Lock()
	defer be.mu.Unlock()
	be.versionN++
	be.records[id] = recordEntry{record: rec, version: fmt.Sprintf("%d", be.versionN)}
}

// --- helpers ---

const testHex64 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func hex64(ch byte) string {
	return string([]byte{ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch,
		ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch,
		ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch,
		ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch, ch})
}

func makeRequest(anchorID, opID string) Request {
	return Request{
		AnchorID:         anchorID,
		OperationID:      opID,
		StatefulSetUID:   "sts-0",
		SourceClusterID:  "src",
		TargetClusterID:  "tgt",
		SourcePrefix:     "src/pfx",
		TargetPrefix:     "tgt/pfx",
		SourceMembership: "mem1",
		FenceHash:        hex64('a'),
		TargetAnchorHash: hex64('b'),
		Fork: recovery.ForkResult{
			Tip:          10,
			PrefixHash:   hex64('c'),
			ManifestHash: hex64('d'),
		},
		TargetMembership: recovery.MembershipRecord{Version: 1, Cluster: "tgt", Membership: "mem", Durability: "async"},
	}
}

func seededRecord(binding Binding, gen uint64, evidence []byte) Record {
	return Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        binding,
		Evidence:       evidence,
		Generation:     gen,
	}
}

func goodVerifier(_ context.Context, _ Request, rec Record) (Binding, []byte, error) {
	return Binding{ClusterID: "tgt", StorageID: "tgt/pfx"}, rec.Transition.Evidence, nil
}

func TestActivateSuccess(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("base-ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if receipt.RequestHash == "" {
		t.Fatal("empty request hash")
	}
	if receipt.Target.ClusterID != "tgt" || receipt.Target.StorageID != "tgt/pfx" {
		t.Fatal("target binding mismatch")
	}
	if receipt.Generation != 1 {
		t.Fatal("generation mismatch")
	}
	if receipt.AnchorID != "a1" || receipt.OperationID != "op1" {
		t.Fatal("anchor/op mismatch")
	}
	if err := co.Verify(ctx, req, receipt); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	rec, _, _ := be.Read(ctx, "a1")
	if rec.PendingWrite {
		t.Fatal("PendingWrite should be false after commit")
	}
	if rec.Transition != nil {
		t.Fatal("Transition should be nil after commit")
	}
	if string(rec.Evidence) != "base-ev" {
		t.Fatalf("evidence should be preserved from frozen, got %q", rec.Evidence)
	}
}

func TestActivateIdempotent(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	r1, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatal("idempotent re-activate should return same receipt")
	}
}

func TestMissingRecordRejects(t *testing.T) {
	be := newInMemoryBackend()
	var verifierCalls int
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: func(_ context.Context, _ Request, _ Record) (Binding, []byte, error) {
		verifierCalls++
		return Binding{}, nil, fmt.Errorf("should not be called")
	}}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	_, err := co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing record")
	}
	if verifierCalls != 0 {
		t.Fatal("verifier should not be called for missing record")
	}
	be.mu.Lock()
	if len(be.records) != 0 {
		t.Fatal("no CAS should occur for missing record")
	}
	be.mu.Unlock()
}

func TestCASConflictResume(t *testing.T) {
	be := newInMemoryBackend()
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// Seed with pending transition matching request.
	seedRecord(be, "a1", Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        Binding{ClusterID: "src", StorageID: "src/pfx"},
		Evidence:       []byte("frozen-ev"),
		Generation:     0,
		Transition: &Transition{
			Request:    req,
			Source:     Binding{ClusterID: "src", StorageID: "src/pfx"},
			Generation: 0,
			Evidence:   []byte("frozen-ev"),
		},
	})

	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatalf("resume Activate: %v", err)
	}
	if receipt.Generation != 1 {
		t.Fatal("generation mismatch after resume")
	}
}

func TestConcurrentConflict(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	ctx := context.Background()
	req1 := makeRequest("a1", "op1")
	req2 := makeRequest("a1", "op2")

	var start sync.WaitGroup
	start.Add(2)
	err1c := make(chan error, 1)
	err2c := make(chan error, 1)
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}

	go func() {
		start.Done()
		start.Wait()
		_, err := co.Activate(ctx, req1)
		err1c <- err
	}()
	go func() {
		start.Done()
		start.Wait()
		_, err := co.Activate(ctx, req2)
		err2c <- err
	}()

	err1 := <-err1c
	err2 := <-err2c
	if err1 != nil && err2 != nil {
		t.Fatal("at least one concurrent activation should succeed")
	}
	if err1 == nil && err2 == nil {
		t.Fatal("concurrent activations should not both succeed")
	}
}

func TestStaleSource(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 1, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()

	req := makeRequest("a1", "op2")
	req.SourceClusterID = "other-src"
	_, err := co.Activate(ctx, req)
	if err == nil || err.Error() != "stale source binding" {
		t.Fatalf("expected stale source binding, got: %v", err)
	}
}

func TestPendingWriteReject(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        Binding{ClusterID: "src", StorageID: "src/pfx"},
		PendingWrite:   true,
		Generation:     0,
	})
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	_, err := co.Activate(ctx, req)
	if err == nil || err.Error() != "pending application write" {
		t.Fatalf("expected pending application write, got: %v", err)
	}
}

func TestBadProof(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	badVerifier := func(_ context.Context, _ Request, _ Record) (Binding, []byte, error) {
		return Binding{ClusterID: "tgt", StorageID: "tgt/pfx"}, []byte("bad-proof"), nil
	}
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: badVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	_, err := co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected proof mismatch error")
	}
}

func TestMismatchedPending(t *testing.T) {
	be := newInMemoryBackend()
	ctx := context.Background()
	req1 := makeRequest("a1", "op1")
	seedRecord(be, "a1", Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        Binding{ClusterID: "src", StorageID: "src/pfx"},
		Generation:     0,
		Transition: &Transition{
			Request:    req1,
			Source:     Binding{ClusterID: "src", StorageID: "src/pfx"},
			Generation: 0,
			Evidence:   []byte("ev"),
		},
	})

	req2 := makeRequest("a1", "op2")
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	_, err := co.Activate(ctx, req2)
	if err == nil || err.Error() != "frozen request hash mismatch" {
		t.Fatalf("expected frozen request hash mismatch, got: %v", err)
	}
}

func TestChangedFrozenEvidence(t *testing.T) {
	be := newInMemoryBackend()
	verifier := func(_ context.Context, _ Request, rec Record) (Binding, []byte, error) {
		return Binding{ClusterID: "tgt", StorageID: "tgt/pfx"}, []byte("proof-v2"), nil
	}
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: verifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// Seed with pending transition frozen to proof-v1.
	seedRecord(be, "a1", Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        Binding{ClusterID: "src", StorageID: "src/pfx"},
		Evidence:       []byte("proof-v1"),
		Generation:     0,
		Transition: &Transition{
			Request:    req,
			Source:     Binding{ClusterID: "src", StorageID: "src/pfx"},
			Generation: 0,
			Evidence:   []byte("proof-v1"),
		},
	})

	// Verifier returns proof-v2 but frozen evidence is proof-v1.
	_, err := co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected proof mismatch with changed frozen evidence")
	}
}

func TestVerifyTamperAnchorID(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with AnchorID.
	bad := receipt
	bad.AnchorID = "tampered"
	err = co.Verify(ctx, req, bad)
	if err == nil {
		t.Fatal("expected error for tampered anchor ID")
	}
}

func TestVerifyTamperOperationID(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with OperationID.
	bad := receipt
	bad.OperationID = "tampered"
	err = co.Verify(ctx, req, bad)
	if err == nil {
		t.Fatal("expected error for tampered operation ID")
	}
}

func TestVerifySchemaMismatch(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Wrong EvidenceFormat.
	co2 := &Coordinator{Backend: be, EvidenceFormat: "wrong/v2", Verifier: goodVerifier}
	err = co2.Verify(ctx, req, receipt)
	if err == nil {
		t.Fatal("expected schema mismatch error")
	}
}

func TestCheckBinding(t *testing.T) {
	rec := Record{Version: Version1, Binding: Binding{ClusterID: "a", StorageID: "b"}, Generation: 5}
	if err := CheckBinding(rec, Binding{ClusterID: "a", StorageID: "b"}, 5); err != nil {
		t.Fatal(err)
	}
	if err := CheckBinding(rec, Binding{ClusterID: "a", StorageID: "x"}, 5); err == nil {
		t.Fatal("expected binding mismatch")
	}
	if err := CheckBinding(rec, Binding{ClusterID: "a", StorageID: "b"}, 3); err == nil {
		t.Fatal("expected generation mismatch")
	}
	rec2 := Record{Version: Version1, PendingWrite: true}
	if err := CheckBinding(rec2, Binding{}, 0); err == nil {
		t.Fatal("expected pending write error")
	}
	rec3 := Record{Version: Version1, Transition: &Transition{}}
	if err := CheckBinding(rec3, Binding{}, 0); err == nil {
		t.Fatal("expected pending transition error")
	}
}

func TestUpdateApplication(t *testing.T) {
	be := newInMemoryBackend()
	ctx := context.Background()
	bind := Binding{ClusterID: "c", StorageID: "s"}
	seedRecord(be, "a1", Record{Version: Version1, EvidenceFormat: "test/v1", Binding: bind, Generation: 3})

	_, v, _ := be.Read(ctx, "a1")
	if err := UpdateApplication(ctx, be, "a1", v, bind, 3, []byte("ev1"), true); err != nil {
		t.Fatal(err)
	}
	rec, _, _ := be.Read(ctx, "a1")
	if string(rec.Evidence) != "ev1" || !rec.PendingWrite {
		t.Fatal("evidence/pending not updated")
	}
	if rec.Generation != 3 || rec.Binding != bind {
		t.Fatal("recovery fields changed")
	}

	// Wrong version should fail.
	if err := UpdateApplication(ctx, be, "a1", "wrong", bind, 3, []byte("ev2"), false); err == nil {
		t.Fatal("expected version mismatch")
	}

	// Clear PendingWrite.
	_, v2, _ := be.Read(ctx, "a1")
	if err := UpdateApplication(ctx, be, "a1", v2, bind, 3, []byte("ev2"), false); err != nil {
		t.Fatal(err)
	}
	rec2, _, _ := be.Read(ctx, "a1")
	if rec2.PendingWrite {
		t.Fatal("PendingWrite should be false")
	}
}

func TestExactRetryWithAppEvidenceAdvance(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("base")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// App advances evidence.
	bind := Binding{ClusterID: "tgt", StorageID: "tgt/pfx"}
	_, v, _ := be.Read(ctx, "a1")
	UpdateApplication(ctx, be, "a1", v, bind, receipt.Generation, []byte("app-evidence"), false)

	// Same request again: should return unchanged receipt.
	r2, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != r2 {
		t.Fatal("exact retry should return same receipt")
	}

	// App evidence preserved.
	rec, _, _ := be.Read(ctx, "a1")
	if string(rec.Evidence) != "app-evidence" {
		t.Fatalf("app evidence lost: %s", rec.Evidence)
	}
}

func TestValidateRequestRejectsNilBackend(t *testing.T) {
	co := &Coordinator{EvidenceFormat: "test/v1", Verifier: goodVerifier}
	_, err := co.Activate(context.Background(), makeRequest("a1", "op1"))
	if err == nil {
		t.Fatal("expected error for nil backend")
	}
}

func TestValidateRequestRejectsBadHash(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()

	req := makeRequest("a1", "op1")
	req.FenceHash = "not-a-valid-hex-hash"
	_, err := co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected error for bad fence hash")
	}

	req2 := makeRequest("a1", "op1")
	req2.Fork.PrefixHash = "tooshort"
	_, err = co.Activate(ctx, req2)
	if err == nil {
		t.Fatal("expected error for bad fork prefix hash")
	}
}

func TestValidateRequestRejectsOverlappingPrefix(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()

	req := makeRequest("a1", "op1")
	req.SourcePrefix = "shared/path"
	req.TargetPrefix = "shared/path"
	_, err := co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected error for overlapping prefixes")
	}
}

func TestBackendReadError(t *testing.T) {
	be := &failingReadBackend{}
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	_, err := co.Activate(context.Background(), makeRequest("a1", "op1"))
	if err == nil {
		t.Fatal("expected read error")
	}
}

type failingReadBackend struct{}

func (b *failingReadBackend) Read(_ context.Context, _ string) (Record, string, error) {
	return Record{}, "", fmt.Errorf("io error")
}
func (b *failingReadBackend) CAS(_ context.Context, _ string, _ string, _ Record) error {
	return fmt.Errorf("should not be called")
}

func TestValidateRequestRejectsTraversal(t *testing.T) {
	be := newInMemoryBackend()
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()

	for _, bad := range []string{".", "..", "../foo"} {
		req := makeRequest("a1", "op1")
		req.SourcePrefix = bad
		_, err := co.Activate(ctx, req)
		if err == nil || err.Error() != "source_prefix contains traversal" {
			t.Fatalf("expected traversal error for source %q, got: %v", bad, err)
		}
	}
}

func TestCheckBindingRejectsVersion(t *testing.T) {
	rec := Record{Version: 99, Binding: Binding{ClusterID: "a", StorageID: "b"}, Generation: 1}
	err := CheckBinding(rec, Binding{ClusterID: "a", StorageID: "b"}, 1)
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestUpdateApplicationRejectsEmpty(t *testing.T) {
	be := newInMemoryBackend()
	ctx := context.Background()
	bind := Binding{ClusterID: "c", StorageID: "s"}
	seedRecord(be, "a1", Record{Version: Version1, EvidenceFormat: "test/v1", Binding: bind, Generation: 3})

	err := UpdateApplication(ctx, be, "a1", "", bind, 3, []byte("ev"), false)
	if err == nil {
		t.Fatal("expected error for empty version")
	}

	err = UpdateApplication(ctx, be, "a1", "v1", Binding{}, 3, []byte("ev"), false)
	if err == nil {
		t.Fatal("expected error for empty binding")
	}
}

type casFailBackend struct {
	inner     *inMemoryBackend
	mu        *sync.Mutex
	failCount int
}

func (b *casFailBackend) Read(ctx context.Context, id string) (Record, string, error) {
	return b.inner.Read(ctx, id)
}

func (b *casFailBackend) CAS(ctx context.Context, id string, version string, record Record) error {
	b.mu.Lock()
	if b.failCount > 0 {
		b.failCount--
		b.mu.Unlock()
		return fmt.Errorf("injected CAS failure")
	}
	b.mu.Unlock()
	return b.inner.CAS(ctx, id, version, record)
}

func TestCASReserveThenCommitErrorConverges(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// Fail the reserve (first CAS), succeed on retry.
	wb := &casFailBackend{inner: be, mu: &sync.Mutex{}, failCount: 1}
	co := &Coordinator{Backend: wb, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	_, err := co.Activate(ctx, req)
	// Should converge: first CAS fails, loop retries, second attempt succeeds.
	if err != nil {
		t.Fatalf("expected convergence after reserve failure, got: %v", err)
	}

	// Verify committed state.
	co2 := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	r2, err := co2.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Generation != 1 {
		t.Fatal("generation should be 1")
	}
}

func TestCASCommitThenErrorConverges(t *testing.T) {
	be := newInMemoryBackend()
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// Seed with pending transition matching request.
	seedRecord(be, "a1", Record{
		Version:        Version1,
		EvidenceFormat: "test/v1",
		Binding:        Binding{ClusterID: "src", StorageID: "src/pfx"},
		Evidence:       []byte("frozen"),
		Generation:     0,
		Transition: &Transition{
			Request:    req,
			Source:     Binding{ClusterID: "src", StorageID: "src/pfx"},
			Generation: 0,
			Evidence:   []byte("frozen"),
		},
	})

	// Fail commit (1 CAS failure), succeed on retry.
	wb := &casFailBackend{inner: be, mu: &sync.Mutex{}, failCount: 1}
	co := &Coordinator{Backend: wb, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatalf("expected convergence after commit failure, got: %v", err)
	}
	if receipt.Generation != 1 {
		t.Fatal("generation should be 1")
	}
}

func TestSameOpConcurrentIdenticalReceipt(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}

	var wg sync.WaitGroup
	wg.Add(2)
	r1c := make(chan Receipt, 1)
	r2c := make(chan Receipt, 1)
	err1c := make(chan error, 1)
	err2c := make(chan error, 1)

	go func() {
		defer wg.Done()
		r, err := co.Activate(ctx, req)
		r1c <- r
		err1c <- err
	}()
	go func() {
		defer wg.Done()
		r, err := co.Activate(ctx, req)
		r2c <- r
		err2c <- err
	}()

	wg.Wait()
	e1, e2 := <-err1c, <-err2c
	r1, r2 := <-r1c, <-r2c

	if e1 != nil || e2 != nil {
		t.Fatalf("both should succeed: e1=%v e2=%v", e1, e2)
	}
	if r1 != r2 {
		t.Fatal("same-op concurrent calls should return identical receipt")
	}
	if r1.Generation != 1 {
		t.Fatal("generation should be 1")
	}
}

// --- Tamper tests ---

func TestFenceHashTamperAfterCommit(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper FenceHash: same request fields but different FenceHash.
	tampered := req
	tampered.FenceHash = hex64('z')
	if tampered.FenceHash == req.FenceHash {
		t.Fatal("tamper should change FenceHash")
	}
	err = co.Verify(ctx, tampered, receipt)
	if err == nil {
		t.Fatal("expected error for FenceHash tamper")
	}
}

func TestBindingTamperAfterCommit(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper stored binding.
	rec, v, _ := be.Read(ctx, "a1")
	rec.Binding = Binding{ClusterID: "tampered", StorageID: "tampered"}
	be.CAS(ctx, "a1", v, rec)

	err = co.Verify(ctx, req, receipt)
	if err == nil {
		t.Fatal("expected error for binding tamper")
	}
}

func TestGenerationTamperAfterCommit(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("ev")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")
	receipt, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper stored generation.
	rec, v, _ := be.Read(ctx, "a1")
	rec.Generation = 99
	be.CAS(ctx, "a1", v, rec)

	err = co.Verify(ctx, req, receipt)
	if err == nil {
		t.Fatal("expected error for generation tamper")
	}
}

// --- Evidence mutation under frozen transition ---

func TestCurrentEvidenceChangedUnderFrozenRejects(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("original")))
	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: goodVerifier}
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// First activation to get past schema check.
	_, err := co.Activate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// Set pending transition with frozen evidence.
	rec, v, _ := be.Read(ctx, "a1")
	frozenEv := []byte("frozen-evidence")
	rec.Transition = &Transition{
		Request:    req,
		Source:     rec.Binding,
		Generation: rec.Generation,
		Evidence:   frozenEv,
	}
	rec.Evidence = frozenEv
	be.CAS(ctx, "a1", v, rec)

	// App changes current evidence under frozen transition.
	rec2, v2, _ := be.Read(ctx, "a1")
	rec2.Evidence = []byte("app-changed-evidence")
	be.CAS(ctx, "a1", v2, rec2)

	// Activate should reject: current evidence != frozen evidence.
	_, err = co.Activate(ctx, req)
	if err == nil {
		t.Fatal("expected rejection when current evidence differs from frozen")
	}
}

// --- Verifier callback mutation aliasing guard ---

func TestVerifierMutatesEvidenceRejected(t *testing.T) {
	be := newInMemoryBackend()
	seedRecord(be, "a1", seededRecord(Binding{ClusterID: "src", StorageID: "src/pfx"}, 0, []byte("frozen")))
	ctx := context.Background()
	req := makeRequest("a1", "op1")

	// Verifier that mutates rec.Transition.Evidence.
	mutatingVerifier := func(_ context.Context, _ Request, rec Record) (Binding, []byte, error) {
		// Mutate the slice in place.
		if rec.Transition != nil && len(rec.Transition.Evidence) > 0 {
			rec.Transition.Evidence[0] = 'M'
		}
		// Return mutated bytes.
		return Binding{ClusterID: "tgt", StorageID: "tgt/pfx"}, rec.Transition.Evidence, nil
	}

	co := &Coordinator{Backend: be, EvidenceFormat: "test/v1", Verifier: mutatingVerifier}
	_, err := co.Activate(ctx, req)
	// Should succeed because frozenEvidenceCopy was taken before callback,
	// and proofBytes == frozenEvidenceCopy (both point to same underlying mutated data
	// BUT frozenEvidenceCopy was cloned from original, so if callback mutates
	// the original AFTER clone, clone is unaffected).
	// Actually: clone happens before callback, callback mutates original.
	// proofBytes points to mutated original, frozenEvidenceCopy is clean.
	// So bytes.Equal(proofBytes, frozenEvidenceCopy) should FAIL.
	if err == nil {
		t.Fatal("expected proof mismatch when verifier mutates evidence")
	}
}
