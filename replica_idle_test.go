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

func checkpointReplicaFixture(t testing.TB) (*ReadReplica, Config, ReplicaConfig) {
	t.Helper()
	ctx := context.Background()
	voterConfig := Config{
		ClusterID: "idle-replica", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(),
		ObjStoreDurability: ObjectStoreDurabilityBeforeAck,
	}
	voter, err := Open(ctx, voterConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY)"}); err != nil {
		_ = voter.Close()
		t.Fatal(err)
	}
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
	config := ReplicaConfig{
		ClusterID: voterConfig.ClusterID, ReplicaID: "r1", DataDir: t.TempDir(),
		Members: []ReplicaMember{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: voterConfig.ObjStoreDir,
		SyncInterval: time.Hour,
	}
	replica, err := OpenReadReplica(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	return replica, voterConfig, config
}

func TestReadReplicaIdleSyncAndChangedCheckpoint(t *testing.T) {
	r, voterConfig, config := checkpointReplicaFixture(t)
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

	voter, err := Open(ctx, voterConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
		_ = voter.Close()
		t.Fatal(err)
	}
	// Before the voter closes, only the certified suffix changed; the base did not.
	floor := r.core.CompactionFloor()
	if err := r.Sync(ctx); err != nil {
		_ = voter.Close()
		t.Fatal(err)
	}
	rows, err := r.Query(ctx, QueryRequest{SQL: "SELECT id FROM items"})
	if err != nil || len(rows.Rows) != 1 || r.core.CompactionFloor() != floor {
		_ = voter.Close()
		t.Fatalf("suffix not applied: rows=%v err=%v floor=%d", rows.Rows, err, r.core.CompactionFloor())
	}
	suffixStats := r.ObjectStoreStats()
	if err := r.Sync(ctx); err != nil {
		_ = voter.Close()
		t.Fatal(err)
	}
	idleStats := r.ObjectStoreStats()
	if idleStats.Heads-suffixStats.Heads != 1 || idleStats.Gets != suffixStats.Gets || idleStats.Uploads != suffixStats.Uploads {
		_ = voter.Close()
		t.Fatalf("caught-up suffix should use one HEAD: before=%+v after=%+v", suffixStats, idleStats)
	}
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
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
			r, voterConfig, _ := checkpointReplicaFixture(t)
			ctx := context.Background()
			owner := r.pinOwner
			voter, err := Open(ctx, voterConfig)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := voter.Execute(ctx, ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
				_ = voter.Close()
				t.Fatal(err)
			}
			if err := voter.Close(); err != nil {
				t.Fatal(err)
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
