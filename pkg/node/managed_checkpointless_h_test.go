package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Only HEAD is pinned to bytes captured from the real source. CURRENT and all
// immutable objects still come from that source; no certificates are invented.
type checkpointlessHHeadBucket struct {
	objstore.Bucket
	key  string
	head []byte
}

func (b checkpointlessHHeadBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == b.key {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(b.head)), nil
	}
	return b.Bucket.Get(ctx, key)
}

func TestManagedPreparedCheckpointlessHPrecedesFirstCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, _, fixture := newKVWrapperFixture(t) // Real registered profile-9 quorum; publisher is gated.
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	const policy = sqlpolicy.ManagedRequestMaterializationPolicy
	if _, err := source.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "checkpointless-H", Key: "H", Value: []byte("retained")}); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := source.core.LatestManagedCheckpointSeal(); err != nil || exists {
		t.Fatalf("H must precede the first QCKP: exists=%t err=%v", exists, err)
	}
	prefix := path.Join(source.config.ObjStorePrefix, string(source.config.ClusterID))
	bootstrap := source.core.CurrentCluster()
	view, err := recovery.OpenReadOnlyViewForApplicationProtocol(ctx, source.bucket, prefix, 1, bootstrap, policy, types.DefaultIdempotencyWindowSlots, protocol, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, hasBase := view.RecoveryBase(); hasBase || view.Base() != 0 || view.Tip() != source.core.Tip() {
		t.Fatal("captured H is not an exact checkpointless certified prefix")
	}
	head := view.CapturedHead()

	// Publish the first actual quorum-certified QCKP only after capturing H.
	files, cut, releaseFiles, err := source.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFiles()
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := source.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	cutPrefix, exists := source.core.PrefixHash(quepaxa.Slot(cut))
	if !exists {
		t.Fatal("actual source cut prefix missing")
	}
	next, following, err := source.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: source.core.ConfigIDForSlot(quepaxa.Slot(cut) + 1), Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: cutPrefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.core.Propose(ctx, value); err != nil {
		t.Fatal("certify first QCKP", err)
	}
	sealed, exists, err := source.core.LatestManagedCheckpointSeal()
	if err != nil || !exists || sealed.DecisionSlot <= view.Tip() {
		t.Fatalf("actual J certificate missing: %+v %v", sealed, err)
	}
	if err := source.promoteManagedAppendOnlySeal(ctx, sealed); err != nil {
		t.Fatal("publish first CURRENT after H", err)
	}
	// Promotion first SyncThroughs J, then verifies its archived certificate,
	// and only then publishes CURRENT. H remains the separately captured view.
	archived, archiveTip, err := source.archive.DecisionsFrom(ctx, sealed.DecisionSlot, 1)
	if err != nil || archiveTip < sealed.DecisionSlot || len(archived) != 1 || !bytes.Equal(archived[0].Value, value) || len(archived[0].Certificate) == 0 {
		t.Fatalf("first CURRENT lacks actual durable J: tip=%d err=%v", archiveTip, err)
	}
	locator, hasLocator, err := source.checkpoints.LoadManagedAppendOnlyLocator(ctx, policy)
	if err != nil || !hasLocator || locator.RootHash != root.RootHash || locator.SealDecisionSlot != uint64(sealed.DecisionSlot) {
		t.Fatalf("actual first CURRENT missing: locator=%+v exists=%t err=%v", locator, hasLocator, err)
	}
	backup := func() map[string][]byte {
		t.Helper()
		objects := make(map[string][]byte)
		err := source.bucket.Iter(ctx, prefix+"/", func(key string) error {
			if strings.HasPrefix(key, prefix+"/voters/") {
				return nil
			}
			r, err := source.bucket.Get(ctx, key)
			if err != nil {
				return err
			}
			body, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			objects[key] = body
			return nil
		}, objstore.WithRecursiveIter())
		if err != nil {
			t.Fatal(err)
		}
		return objects
	}
	before := backup()

	t.Run("ordinary-without-H-still-refuses-unproved-CURRENT", func(t *testing.T) {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer wal.Close()
		nonce, registered := source.wal.Identity()
		if !registered {
			t.Fatal("actual source WAL is not registered")
		}
		if err := wal.BindIdentity(nonce); err != nil {
			t.Fatal(err)
		}
		binding, err := source.managedAppendOnlyCoreConfig()
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: source.config.NodeID, Cluster: bootstrap, WAL: wal, Transport: fixture.transport, ValidateValue: types.ValidateManagedKVExecutionPolicy, ManagedAppendOnly: binding})
		if err != nil {
			t.Fatal(err)
		}
		if err := core.AcceptCertifiedValues(view.Decisions()); err != nil {
			t.Fatal(err)
		}
		captured := checkpointlessHHeadBucket{Bucket: source.bucket, key: path.Join(prefix, "archive/head.bin"), head: head}
		archive, err := recovery.NewAppendOnlyManagerWithValueValidator(captured, prefix, 1, types.ValidateManagedKVExecutionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		defer archive.Close()
		if err := archive.Load(ctx); err != nil {
			t.Fatal(err)
		}
		ordinary := New(source.config)
		ordinary.managedStartup = &ManagedStartupConfig{Identity: source.managedStartup.Identity, expectedPolicy: policy}
		ordinary.core, ordinary.archive, ordinary.checkpoints = core, archive, source.checkpoints
		if !core.IsVoter() {
			t.Fatal("ordinary negative is not an actual voter")
		}
		if _, _, err := ordinary.selectManagedAppendOnlyRoot(ctx); err == nil || err.Error() != "managed append-only CURRENT has no certified QCKP" {
			t.Fatalf("ordinary selector accepted CURRENT without its certificate: %v", err)
		}
		if core.Tip() != view.Tip() || !reflect.DeepEqual(before, backup()) {
			t.Fatal("ordinary refusal imported J or mutated the source")
		}
	})

	cfg := CloneManagedRuntimeConfig(*source.config)
	// Public multi-node startup requires an explicit shared provider. Reuse the
	// existing NoPVC S3 fixture and copy actual certified source bytes, not a
	// success-only bucket substitute.
	objectStore := &managedTestObjectStore{objects: make(map[string][]byte)}
	if err := source.bucket.Iter(ctx, prefix+"/", func(key string) error {
		r, err := source.bucket.Get(ctx, key)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		objectStore.put(key, body)
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	objectServer := httptest.NewServer(http.HandlerFunc(objectStore.serve))
	t.Cleanup(objectServer.Close)
	cfg.ObjStoreProvider, cfg.ObjStoreDir = "s3", ""
	cfg.ObjStoreEndpoint, cfg.ObjStoreInsecure = strings.TrimPrefix(objectServer.URL, "http://"), true
	cfg.ObjStoreBucket, cfg.ObjStoreAccessKey, cfg.ObjStoreSecretKey = "prepared-target", "test", "test"
	cfg.EnableReconfiguration = true
	cfg.AdminToken = "cluster-admin"
	if configured, err := validateObjectStoreConfig(&cfg); err != nil || !configured {
		t.Fatalf("shared-store prerequisite: configured=%t err=%v", configured, err)
	}
	cfg.NodeID, cfg.DataDir, cfg.PeerAddr, cfg.PeerToken = "d", filepath.Join(t.TempDir(), "learner-data"), "127.0.0.1:0", "d-token"
	member := peerMember(string(cfg.ClusterID), cfg.NodeID, cfg.PeerToken)
	member.PeerURL = "quic://127.0.0.1:0"
	cfg.Learner = &member
	if err := validateVoterMembership(&cfg); err != nil {
		t.Fatal("learner membership prerequisite", err)
	}
	learner := New(&cfg)
	physical, digest, err := learner.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	identity := source.managedStartup.Identity
	identity.Member.NodeID, identity.Member.PodUID = string(cfg.NodeID), "d-pod"
	identity.Member.SourceBinding, identity.Member.ConfigDigest = physical, digest
	identity.PhysicalBinding = physical
	identity.BootstrapPlanDigest, err = learner.ManagedBootstrapPlanDigestForProtocol(physical, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	remoteBackup := func() map[string][]byte {
		objectStore.mu.RLock()
		defer objectStore.mu.RUnlock()
		objects := make(map[string][]byte)
		for key, body := range objectStore.objects {
			if !strings.HasPrefix(key, prefix+"/voters/") {
				objects[key] = append([]byte(nil), body...)
			}
		}
		return objects
	}
	remoteBefore := remoteBackup()
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoffDir, identity.Member, policy, types.DefaultIdempotencyWindowSlots); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartupForApplicationProtocol(ctx, source.bucket, prefix, 1, bootstrap, handoffDir, cfg.DataDir, identity.Member, policy, types.DefaultIdempotencyWindowSlots, protocol, false)
	if err != nil {
		t.Fatal(err)
	}
	// Match the existing direct-Node fixture's minimal origin contract. The
	// production Host's full repair-plan/owner-chain authority is not simulated.
	origin := []byte(`{"version":1,"repair":{"state":"Closed"}}`)
	checker := &managedTestChecker{}
	if err := checker.ValidateManagedRepairOrigin(ctx, origin); err != nil {
		t.Fatal(err)
	}
	adopted, resume := make(chan struct{}), make(chan struct{})
	var arrived, released sync.Once
	release := func() { released.Do(func() { close(resume) }) }
	restore := setManagedPreparedStartupTestHook(func(event string) {
		if event == "node:managed-prepared:adopted-before-suffix-catchup" {
			arrived.Do(func() { close(adopted) })
			<-resume
		}
	})
	done := make(chan error, 1)
	consumed := false
	t.Cleanup(func() {
		cancel()
		release()
		restore()
		if !consumed {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("prepared opener did not join")
			}
		}
		if err := learner.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	go func() {
		done <- learner.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: "root", Identity: identity, Checker: checker, Prepared: prepared, RepairOrigin: origin})
	}()
	if localtesthooks.Enabled {
		select {
		case <-adopted:
		case err := <-done:
			consumed = true
			t.Fatalf("Open returned before exact H adoption: %v", err)
		case <-ctx.Done():
			t.Fatal("H adoption boundary not reached", ctx.Err())
		}
		adoption, err := loadPreparedAdoption(filepath.Join(cfg.DataDir, "prepared-adoption.json"))
		if err != nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() || adoption.OriginDigest != managedRepairOriginDigest(origin) || learner.core.Tip() != view.Tip() || learner.material.Tip() != uint64(view.Tip()) {
			t.Fatalf("CURRENT displaced H before J: adoption=%+v err=%v", adoption, err)
		}
		if _, hasSeal, err := learner.core.LatestManagedCheckpointSeal(); err != nil || hasSeal {
			t.Fatalf("J was imported before H adoption: hasSeal=%t err=%v", hasSeal, err)
		}
	}
	release()
	select {
	case err := <-done:
		consumed = true
		if err != nil {
			t.Fatal("open exact checkpointless H", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	adoption, err := loadPreparedAdoption(filepath.Join(cfg.DataDir, "prepared-adoption.json"))
	if err != nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() || adoption.OriginDigest != managedRepairOriginDigest(origin) {
		t.Fatalf("persisted H changed after J: %+v %v", adoption, err)
	}
	decision, found := learner.core.CertifiedValue(sealed.DecisionSlot)
	if !found || !bytes.Equal(decision.Value, value) || learner.material.Tip() != uint64(sealed.DecisionSlot) || learner.core.IsVoter() {
		t.Fatal("post-adoption J was not exactly certified/replayed, or learner promoted")
	}
	if !reflect.DeepEqual(before, backup()) {
		t.Fatal("nonvoting adoption changed source backup objects")
	}
	if !reflect.DeepEqual(remoteBefore, remoteBackup()) {
		t.Fatal("nonvoting adoption changed shared backup objects")
	}
	state, err := loadVoterIdentity(&cfg)
	if err != nil || state.identity == nil || !state.identity.Registered {
		t.Fatalf("actual learner registration missing: %+v %v", state.identity, err)
	}
	nonce, registered := learner.wal.Identity()
	if !registered || hex.EncodeToString(nonce) != state.identity.Nonce {
		t.Fatal("learner registration does not bind the actual WAL")
	}
}
