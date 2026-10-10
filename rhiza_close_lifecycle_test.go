package rhiza

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

type closeLifecycleFixture struct {
	configs []Config
	fail    atomic.Bool
	denied  atomic.Int64
}

func newCloseLifecycleFixture(t *testing.T, ctx context.Context, durability ObjectStoreDurability, headOnly bool) *closeLifecycleFixture {
	t.Helper()
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := gateway.Close(closeCtx); err != nil {
			t.Errorf("stop local gateway: %v", err)
		}
	})
	target, err := url.Parse(gateway.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &closeLifecycleFixture{}
	forward := httputil.NewSingleHostReverseProxy(target)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fixture.fail.Load() && r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/archive/") &&
			(!headOnly || strings.HasSuffix(r.URL.Path, "/head.bin")) {
			fixture.denied.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>injected archive publication failure</Message></Error>"))
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	client, err := minio.New(strings.TrimPrefix(gateway.Endpoint, "http://"), &minio.Options{
		Creds: credentials.NewStaticV4(gateway.AccessKey, gateway.SecretKey, ""), Secure: false, Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("rhiza-public-close-%d", time.Now().UnixNano())
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
	const clusterID = "public-close"
	members := make([]Member, 3)
	for i := range members {
		id := NodeID(fmt.Sprintf("n%d", i+1))
		token := fmt.Sprintf("close-peer-%d", i+1)
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		members[i] = Member{ID: id, PeerURL: "quic://" + conn.LocalAddr().String(), PublicKey: PeerPublicKey(clusterID, string(id), token)}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i, member := range members {
		fixture.configs = append(fixture.configs, Config{
			DataDir: t.TempDir(), ClusterID: clusterID, NodeID: string(member.ID),
			PeerAddr: strings.TrimPrefix(member.PeerURL, "quic://"), PeerToken: fmt.Sprintf("close-peer-%d", i+1), Members: members,
			ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(proxy.URL, "http://"), ObjStoreBucket: bucket,
			ObjStorePrefix: "close", ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
			ObjStoreAccessKey: gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
			ObjStoreDurability: durability, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
		})
	}
	return fixture
}

func openCloseLifecycleVoters(t *testing.T, ctx context.Context, configs []Config) []*DB {
	t.Helper()
	var dbs []*DB
	for _, config := range configs {
		db, err := Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		dbs = append(dbs, db)
		t.Cleanup(func() { _ = db.Close() })
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, db := range dbs {
			ready = ready && db.Ready()
		}
		if ready {
			return dbs
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func closeLifecycleVoters(dbs []*DB, parallel bool) []error {
	errs := make([]error, len(dbs))
	if !parallel {
		for i, db := range dbs {
			errs[i] = db.Close()
		}
		return errs
	}
	type result struct {
		index int
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, len(dbs))
	for i, db := range dbs {
		go func() { <-start; results <- result{i, db.Close()} }()
	}
	close(start)
	for range dbs {
		got := <-results
		errs[got.index] = got.err
	}
	return errs
}

func assertCloseLifecycleSQL(t *testing.T, rows QueryResponse, err error) {
	t.Helper()
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "retained" {
		t.Fatalf("cold SQL readback=%#v error=%v", rows.Rows, err)
	}
}

func closeLifecycleReplicaConfig(t *testing.T, config Config) ReplicaConfig {
	t.Helper()
	var members []ReplicaMember
	for _, member := range config.Members {
		identity, err := NewReplicaMember(config.ClusterID, member)
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, identity)
	}
	return ReplicaConfig{
		ClusterID: config.ClusterID, ReplicaID: "cold-reader", DataDir: t.TempDir(), Members: members, SyncInterval: time.Hour,
		ObjStoreProvider: config.ObjStoreProvider, ObjStoreEndpoint: config.ObjStoreEndpoint, ObjStoreBucket: config.ObjStoreBucket,
		ObjStorePrefix: config.ObjStorePrefix, ObjStoreRegion: config.ObjStoreRegion, ObjStoreInsecure: true,
		ObjStoreAccessKey: config.ObjStoreAccessKey, ObjStoreSecretKey: config.ObjStoreSecretKey,
	}
}

func TestDBCloseParallelAndLastVoterColdRecovery(t *testing.T) {
	for _, durability := range []ObjectStoreDurability{ObjectStoreDurabilityAsync, ObjectStoreDurabilityBeforeAck} {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%t", durability, parallel), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				fixture := newCloseLifecycleFixture(t, ctx, durability, false)
				dbs := openCloseLifecycleVoters(t, ctx, fixture.configs)
				for i, sql := range []string{"CREATE TABLE retained (value TEXT)", "INSERT INTO retained VALUES ('retained')"} {
					if _, err := dbs[0].Execute(ctx, ExecuteRequest{RequestID: fmt.Sprintf("close-sql-%d", i), SQL: sql}); err != nil {
						t.Fatal(err)
					}
				}
				rows, err := dbs[0].Query(ctx, QueryRequest{SQL: "SELECT value FROM retained", Consistency: "linearizable"})
				assertCloseLifecycleSQL(t, rows, err)
				through := rows.AppliedSlot
				for i, err := range closeLifecycleVoters(dbs, parallel) {
					if err != nil {
						t.Fatalf("ordinary Close n%d: %v", i+1, err)
					}
				}
				// All voters are offline. This fresh observer has neither their
				// WALs nor their materializers and must use certified archive replay.
				reader, err := OpenReadReplica(ctx, closeLifecycleReplicaConfig(t, fixture.configs[0]))
				if err != nil {
					t.Fatal(err)
				}
				rows, err = reader.Query(ctx, QueryRequest{SQL: "SELECT value FROM retained"})
				assertCloseLifecycleSQL(t, rows, err)
				if status := reader.Status(); status.Source != "object-store" || status.AppliedSlot < through || status.SourceTip < through || reader.core.CompactionFloor() != 0 || reader.checkpoints.Latest() != nil {
					t.Fatalf("fresh untrimmed archive recovery: %+v floor=%d", status, reader.core.CompactionFloor())
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				// Retain every original registered WAL; rebuild materializers only.
				for _, config := range fixture.configs {
					for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "latticedb"} {
						if err := os.RemoveAll(filepath.Join(config.DataDir, name)); err != nil {
							t.Fatal(err)
						}
					}
				}
				reopened := openCloseLifecycleVoters(t, ctx, fixture.configs)
				for _, db := range reopened {
					rows, err := db.Query(ctx, QueryRequest{SQL: "SELECT value FROM retained", Consistency: "linearizable"})
					assertCloseLifecycleSQL(t, rows, err)
					if rows.AppliedSlot < through {
						t.Fatalf("original WAL replay regressed: got=%d required=%d", rows.AppliedSlot, through)
					}
				}
				for _, err := range closeLifecycleVoters(reopened, true) {
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestDBCloseRetainsNativeArchiveFailureAndJoins(t *testing.T) {
	for _, durability := range []ObjectStoreDurability{ObjectStoreDurabilityAsync, ObjectStoreDurabilityBeforeAck} {
		for _, headOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/HEAD=%t", durability, headOnly), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				fixture := newCloseLifecycleFixture(t, ctx, durability, headOnly)
				dbs := openCloseLifecycleVoters(t, ctx, fixture.configs)
				if _, err := dbs[0].Execute(ctx, ExecuteRequest{RequestID: "failure-seed", SQL: "CREATE TABLE retained (value TEXT)"}); err != nil {
					t.Fatal(err)
				}
				var unpublishedSlot uint64
				if durability == ObjectStoreDurabilityBeforeAck {
					// Reject publication before the proposal, not after its ACK
					// barrier has already made the certified suffix durable.
					fixture.fail.Store(true)
					const requestID = "failure-suffix"
					_, err := dbs[0].Execute(ctx, ExecuteRequest{RequestID: requestID, SQL: "INSERT INTO retained VALUES ('retained')"})
					var unknown *network.CommitUnknownError
					if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, ErrDurabilityUnavailable) ||
						!errors.As(err, &unknown) || unknown.Slot == 0 || unknown.RequestID != requestID {
						t.Fatalf("failed before-ack publication error=%v detail=%+v", err, unknown)
					}
					unpublishedSlot = uint64(unknown.Slot)
					status, statusErr := dbs[0].RequestStatus(ctx, RequestStatusRequest{Kind: "sql", RequestID: requestID})
					if statusErr != nil || status.State != "committed" || status.Receipt == nil || status.Receipt.Slot != unpublishedSlot {
						t.Fatalf("certified local receipt=%+v error=%v required_slot=%d", status, statusErr, unpublishedSlot)
					}
					t.Logf("before-ack returned no successful ACK: %v; committed local receipt slot=%d", err, unpublishedSlot)
				} else if _, err := dbs[0].api.ProposeControl(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})); err != nil {
					t.Fatal(err)
				}
				for i, db := range dbs {
					if unpublishedSlot == 0 {
						if _, err := db.Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM retained", Consistency: "linearizable"}); err != nil {
							t.Fatal(err)
						}
						continue
					}
					rows, err := db.Query(ctx, QueryRequest{SQL: "SELECT value FROM retained", Consistency: "linearizable"})
					assertCloseLifecycleSQL(t, rows, err)
					response := httptest.NewRecorder()
					db.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/recovery/status", nil).WithContext(ctx))
					if response.Code != http.StatusOK {
						t.Fatalf("voter n%d recovery status HTTP=%d body=%s", i+1, response.Code, response.Body.String())
					}
					var status network.VoterRecoveryStatus
					if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
						t.Fatal(err)
					}
					config := fixture.configs[i]
					if status.NodeID != config.NodeID || status.ClusterID != config.ClusterID || status.Durability != string(durability) ||
						!status.Ready || !status.Quorum || status.CertifiedTip < unpublishedSlot || status.AppliedTip < unpublishedSlot || status.ArchiveTip >= unpublishedSlot {
						t.Fatalf("voter n%d lacks certified unpublished suffix: %+v required_slot=%d", i+1, status, unpublishedSlot)
					}
					t.Logf("certified unpublished suffix: %+v", status)
				}
				fixture.fail.Store(true)
				deniedBeforeClose := fixture.denied.Load()
				errs := closeLifecycleVoters(dbs, true)
				deniedAfterClose := fixture.denied.Load()
				if errors.Join(errs...) == nil || deniedAfterClose == 0 || unpublishedSlot != 0 && deniedAfterClose <= deniedBeforeClose {
					t.Fatalf("native archive failure was not surfaced: errors=%v denied_before=%d denied_after=%d", errs, deniedBeforeClose, deniedAfterClose)
				}
				t.Logf("joined Close errors=%v denied_before=%d denied_after=%d", errs, deniedBeforeClose, deniedAfterClose)
				nativeFailure := false
				for i, db := range dbs {
					wantError := "injected archive publication failure"
					if headOnly {
						// Preserve the existing CAS manager's actual failure,
						// which does not retain the underlying HEAD error.
						wantError = "shared archive publication conflicted too many times"
					}
					if errs[i] == nil {
						t.Fatalf("voter n%d reported success despite an unpublished suffix", i+1)
					}
					// Concurrent archive writers can also report lock contention;
					// at least one must retain the actual failed publication.
					nativeFailure = nativeFailure || strings.Contains(errs[i].Error(), wantError)
					if db.Ready() || db.Close() != errs[i] {
						t.Fatal("Close did not retain its result/readiness")
					}
					_, lock, err := qlog.Acquire(filepath.Join(fixture.configs[i].DataDir, "qlog"))
					if err != nil {
						t.Fatalf("owned WAL lock still held after failed Close: %v", err)
					}
					if err := lock.Release(); err != nil {
						t.Fatal(err)
					}
					conn, err := net.ListenPacket("udp", fixture.configs[i].PeerAddr)
					if err != nil {
						t.Fatalf("owned peer socket still held after failed Close: %v", err)
					}
					if err := conn.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if !nativeFailure {
					t.Fatalf("no native publication failure retained: %v", errs)
				}
				// Corrupted certified evidence must not become a successful
				// empty bootstrap of a fresh observer. No Close retry occurs.
				fixture.fail.Store(false)
				config := fixture.configs[0]
				client, err := minio.New(config.ObjStoreEndpoint, &minio.Options{
					Creds: credentials.NewStaticV4(config.ObjStoreAccessKey, config.ObjStoreSecretKey, ""), Secure: false, Region: config.ObjStoreRegion,
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.PutObject(ctx, config.ObjStoreBucket, "close/public-close/archive/head.bin", bytes.NewReader([]byte("corrupt")), 7, minio.PutObjectOptions{}); err != nil {
					t.Fatal(err)
				}
				reader, err := OpenReadReplica(ctx, closeLifecycleReplicaConfig(t, config))
				if err == nil {
					_ = reader.Close()
					t.Fatal("fresh recovery accepted corrupted archive evidence")
				}
			})
		}
	}
}

func TestColdReadReplicaReplaysCertifiedCheckpointAndSuffix(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	source.execute(t, "CREATE TABLE retained (value TEXT)")
	source.execute(t, "INSERT INTO retained VALUES ('base')")
	_, sealed := source.publishCheckpoint(t, true)
	source.execute(t, "UPDATE retained SET value='retained'")
	if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
		t.Fatal(err)
	}
	through := source.core.Tip()
	source.archive.Close()
	if err := source.material.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadReplica(ctx, learnerReplicaConfig(t.TempDir(), bucketDir, "cold-base-reader"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close cold read replica: %v", err)
		}
	}()
	rows, err := reader.Query(ctx, QueryRequest{SQL: "SELECT value FROM retained"})
	assertCloseLifecycleSQL(t, rows, err)
	if status := reader.Status(); status.Source != "object-store" || status.AppliedSlot < uint64(through) || reader.core.CompactionFloor() != sealed.Index {
		t.Fatalf("cold checkpoint/suffix recovery: %+v floor=%d required=%d", status, reader.core.CompactionFloor(), through)
	}
}
