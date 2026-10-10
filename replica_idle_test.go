package rhiza

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	thanosobjstore "github.com/thanos-io/objstore"
)

func checkpointReplicaFixture(t testing.TB) (*ReadReplica, *learnerCheckpointSource, ReplicaConfig) {
	t.Helper()
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY)")
	_, sealed := source.publishCheckpoint(t, true)
	config := learnerReplicaConfig(t.TempDir(), bucketDir, "r1")
	replica, err := OpenReadReplica(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	if sealed.Index == 0 || replica.core.CompactionFloor() != sealed.Index {
		t.Fatalf("initial certified checkpoint floor=%d required=%d", replica.core.CompactionFloor(), sealed.Index)
	}
	return replica, source, config
}

func TestReadReplicaIdleSyncAndChangedCheckpoint(t *testing.T) {
	r, source, config := checkpointReplicaFixture(t)
	ctx := context.Background()
	owner := r.pinOwner
	if owner == "" || r.syncedHead == nil {
		t.Fatal("initial recovery did not record its verified head and owner")
	}
	before := r.ObjectStoreStats()
	for range 5 {
		if err := r.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	after := r.ObjectStoreStats()
	if after.Heads-before.Heads != 5 || after.Gets != before.Gets || after.Uploads != before.Uploads || after.Lists != before.Lists || after.Deletes != before.Deletes {
		t.Fatalf("idle sync should only HEAD: before=%+v after=%+v", before, after)
	}
	// An unchanged cached head must never hide an unavailable remote store.
	bucket := r.bucket.Bucket
	r.bucket.Bucket = failingReplicaAttributes{Bucket: bucket}
	if err := r.Sync(ctx); !errors.Is(err, errReplicaStoreUnavailable) {
		t.Fatalf("remote error was hidden: %v", err)
	}
	r.bucket.Bucket = bucket

	source.execute(t, "INSERT INTO items VALUES (1)")
	if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
		t.Fatal(err)
	}
	// Publish only the certified suffix here, leaving the base unchanged.
	floor := r.core.CompactionFloor()
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := r.Query(ctx, QueryRequest{SQL: "SELECT id FROM items"})
	if err != nil || len(rows.Rows) != 1 || r.core.CompactionFloor() != floor {
		t.Fatalf("suffix not applied: rows=%v err=%v floor=%d", rows.Rows, err, r.core.CompactionFloor())
	}
	suffixStats := r.ObjectStoreStats()
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	idleStats := r.ObjectStoreStats()
	if idleStats.Heads-suffixStats.Heads != 1 || idleStats.Gets != suffixStats.Gets || idleStats.Uploads != suffixStats.Uploads {
		t.Fatalf("caught-up suffix should use one HEAD: before=%+v after=%+v", suffixStats, idleStats)
	}
	_, changed := source.publishCheckpoint(t, true)
	if changed.Index <= floor {
		t.Fatalf("changed checkpoint index=%d must exceed old base=%d", changed.Index, floor)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if r.core.CompactionFloor() != changed.Index {
		t.Fatalf("changed checkpoint not adopted: floor=%d required=%d", r.core.CompactionFloor(), changed.Index)
	}
	if r.pinOwner != owner {
		t.Fatal("successful recovery should reuse the process pin owner")
	}
	rows, err = r.Query(ctx, QueryRequest{SQL: "SELECT id FROM items"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("new checkpoint not applied: rows=%v err=%v", rows.Rows, err)
	}
	if r.ObjectStoreStats().Uploads <= after.Uploads {
		t.Fatal("changed checkpoint did not use recovery pins")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReadReplica(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.pinOwner == owner || reopened.ObjectStoreStats().Uploads == 0 {
		t.Fatal("restart should use a new owner and verify recovery with pins")
	}
}

func TestReadReplicaPinCloseFailureInvalidatesIdleState(t *testing.T) {
	for _, prefix := range []string{"archive/recovery-pins/", "checkpoint/recovery-pins/"} {
		t.Run(prefix, func(t *testing.T) {
			r, source, _ := checkpointReplicaFixture(t)
			ctx := context.Background()
			owner := r.pinOwner
			floor := r.core.CompactionFloor()
			source.execute(t, "INSERT INTO items VALUES (1)")
			_, changed := source.publishCheckpoint(t, true)
			if changed.Index <= floor || changed.Index <= r.core.Tip() {
				t.Fatalf("pin-failure fixture needs a newer base: index=%d old_base=%d reader_tip=%d", changed.Index, floor, r.core.Tip())
			}
			bucket := r.bucket.Bucket
			fault := &failingReplicaPinClose{Bucket: bucket, prefix: prefix}
			r.bucket.Bucket = fault
			if err := r.Sync(ctx); !errors.Is(err, errReplicaStoreUnavailable) {
				t.Fatalf("pin close error was hidden: %v", err)
			}
			r.bucket.Bucket = bucket
			if !fault.failed.Load() || r.syncedHead != nil || r.pinOwner != "" {
				t.Fatal("failed release must invalidate both cached head and owner")
			}
			if err := r.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if r.pinOwner == owner || r.pinOwner == "" || r.syncedHead == nil {
				t.Fatal("retry must verify recovery with a new owner")
			}
		})
	}
}

type failingReplicaPinClose struct {
	thanosobjstore.Bucket
	prefix string
	failed atomic.Bool
}

func (b *failingReplicaPinClose) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	if strings.Contains(name, b.prefix) {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		var pin struct {
			LeaseUntilMS int64 `json:"lease_until_unix_ms"`
		}
		if err := json.Unmarshal(data, &pin); err != nil {
			return err
		}
		if pin.LeaseUntilMS <= time.Now().UnixMilli() && b.failed.CompareAndSwap(false, true) {
			return errReplicaStoreUnavailable
		}
		reader = bytes.NewReader(data)
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

var errReplicaStoreUnavailable = errors.New("test object store unavailable")

type failingReplicaAttributes struct{ thanosobjstore.Bucket }

func (b failingReplicaAttributes) Attributes(context.Context, string) (thanosobjstore.ObjectAttributes, error) {
	return thanosobjstore.ObjectAttributes{}, errReplicaStoreUnavailable
}

func BenchmarkReadReplicaIdleSync(b *testing.B) {
	r, _, _ := checkpointReplicaFixture(b)
	ctx := context.Background()
	before := r.ObjectStoreStats()
	b.ResetTimer()
	for range b.N {
		if err := r.Sync(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after := r.ObjectStoreStats()
	b.ReportMetric(float64(after.Heads-before.Heads)/float64(b.N), "HEAD/op")
	b.ReportMetric(float64(after.Gets-before.Gets)/float64(b.N), "GET/op")
	b.ReportMetric(float64(after.Uploads-before.Uploads)/float64(b.N), "PUT/op")
}
