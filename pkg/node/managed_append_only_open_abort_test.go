//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedAppendOnlyOpenFailureDoesNotPublishDuringAbortCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	const clusterID = "managed-open-abort"
	const storageRoot = "open-abort-storage"
	const assignment = "open-abort-assignment"
	namespace, err := ManagedNamespaceFor(storageRoot, "open-abort-cluster-uid", 0, assignment, clusterID)
	if err != nil {
		t.Fatal(err)
	}

	store := &managedTestObjectStore{objects: make(map[string][]byte)}
	server := httptest.NewServer(http.HandlerFunc(store.serve))
	t.Cleanup(server.Close)
	members := make([]quepaxa.Member, 3)
	tokens := map[quepaxa.NodeID]string{}
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		tokens[id] = string(id) + "-token"
		members[i] = peerMember(clusterID, id, tokens[id])
		members[i].PeerURL = fmt.Sprintf("quic://127.0.0.1:%d", 19201+i)
	}
	config := &types.ExecutionConfig{
		NodeID: "n1", ClusterID: clusterID, DataDir: t.TempDir(), Members: members,
		PeerAddr: "127.0.0.1:0", PeerToken: tokens["n1"], AdminToken: "open-abort-admin",
		ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(server.URL, "http://"), ObjStoreInsecure: true,
		ObjStoreBucket: "prepared-target", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test",
		ObjStorePrefix: namespace.ConfigPrefix, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	}
	node := New(config)
	node.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, BootstrapMember: true}
	physical, memberDigest, err := node.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := node.ManagedBootstrapPlanDigestForProtocol(physical, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	node.managedStartup.Identity = ManagedStartupIdentity{
		ApplicationProtocol: protocol, EvidenceFormat: protocol,
		Member: recovery.PreparedStartupIdentity{
			ClusterUID: "open-abort-cluster-uid", Generation: 0, DeploymentUID: "open-abort-deployment",
			PodUID: "open-abort-n1", NodeID: "n1", SourceBinding: physical, ConfigDigest: memberDigest,
		},
		AssignmentID: assignment, IntentHash: "open-abort-intent", StorageID: storageRoot,
		PhysicalBinding: physical, BootstrapPlanDigest: bootstrap,
		Fresh: true,
	}
	node.managedStartup.Checker = &managedTestChecker{}

	// A fresh three-voter registration is an input to the public Open path.
	membership, err := json.Marshal(recovery.NewMembershipRecord(clusterID, members, string(config.ObjStoreDurability)))
	if err != nil {
		t.Fatal(err)
	}
	store.put(path.Join(namespace.FinalPrefix, "voters/membership.json"), membership)

	var sourceCore *quepaxa.Core
	var peers graphReviewTransport
	var remoteWALs []*qlog.WAL
	t.Cleanup(func() {
		for _, wal := range remoteWALs {
			if err := wal.Close(); err != nil {
				t.Errorf("close remote proposal WAL: %v", err)
			}
		}
	})
	var bucket *localobj.MeteredBucket
	var archive *recovery.Manager
	var core *quepaxa.Core
	var beforeStats localobj.Stats
	var beforeObjects map[string][]byte
	var qckpValue []byte
	var slot3Value []byte
	var slot3Decision quepaxa.DecidedValue
	var hookReached bool
	restoreHook := localtesthooks.Set(func(event string) {
		if event != "node:managed-graph-startup:pair_select" {
			return
		}
		hookReached = true
		bucket, archive, core = node.bucket, node.archive, node.core
		if bucket == nil || archive == nil || core == nil || !core.IsVoter() {
			t.Errorf("pair-select hook lacks actual initialized voting runtime: bucket=%t archive=%t core=%t voter=%t", bucket != nil, archive != nil, core != nil, core != nil && core.IsVoter())
			return
		}

		peers = graphReviewTransport{"n1": core}
		cluster := core.CurrentCluster()
		for i, id := range []quepaxa.NodeID{"n2", "n3"} {
			remoteConfig := *config
			remoteConfig.NodeID = id
			remoteConfig.PeerToken = tokens[id]
			remote := New(&remoteConfig)
			remote.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, Identity: node.managedStartup.Identity}
			remote.managedStartup.Identity.Member.NodeID = string(id)
			_, remoteDigest, digestErr := remote.PreparedStartupBindingDigestsForProtocol(protocol)
			if digestErr != nil {
				t.Errorf("derive actual remote voter profile: %v", digestErr)
				return
			}
			identity := bytes.Repeat([]byte{byte(70 + i)}, 32)
			wal, walErr := qlog.Open(filepath.Join(t.TempDir(), "remote-"+string(id)))
			if walErr != nil {
				t.Errorf("open remote voter WAL: %v", walErr)
				return
			}
			remoteWALs = append(remoteWALs, wal)
			if walErr = wal.BindIdentity(identity); walErr != nil {
				t.Errorf("bind remote voter WAL: %v", walErr)
				return
			}
			var walIdentity [32]byte
			copy(walIdentity[:], identity)
			remoteProfile := &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: bootstrap, MemberConfigDigest: remoteDigest,
				Generation: 0, WALIdentity: walIdentity,
			}, Verify: node.verifyManagedAppendOnlyCandidate}
			remoteCore, coreErr := quepaxa.New(quepaxa.Config{
				NodeID: id, Cluster: cluster, WAL: wal, Transport: peers,
				ValidateValue: types.ValidateManagedGraphExecutionPolicy, ManagedAppendOnly: remoteProfile,
			})
			if coreErr != nil {
				t.Errorf("create actual remote source Core: %v", coreErr)
				return
			}
			remoteCore.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
				return node.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
			})
			peers[id] = remoteCore
		}
		sourceCore = peers["n2"]
		app, openErr := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "source.sqlite"), 1, types.DefaultIdempotencyWindowSlots)
		if openErr != nil {
			t.Errorf("open actual paired source materializer: %v", openErr)
			return
		}
		defer func() {
			if closeErr := app.Close(); closeErr != nil {
				t.Errorf("close actual source materializer: %v", closeErr)
			}
		}()
		value, encodeErr := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "open-abort-prefix", Cypher: "CREATE (:OpenAbort {actual:true})"})
		if encodeErr != nil {
			t.Errorf("encode actual source Graph value: %v", encodeErr)
			return
		}
		if slot, _, proposeErr := sourceCore.Propose(ctx, value); proposeErr != nil || slot != 1 {
			t.Errorf("certify actual source Graph slot: slot=%d err=%v", slot, proposeErr)
			return
		}
		if applyErr := app.Apply(ctx, 1, value); applyErr != nil {
			t.Errorf("apply actual source Graph slot: %v", applyErr)
			return
		}
		files, cut, release, filesErr := app.CheckpointFilesAt(ctx)
		if filesErr != nil {
			t.Errorf("capture actual paired checkpoint: %v", filesErr)
			return
		}
		defer release()
		if cut != 1 || len(files) != 2 || files[0].Role != materializer.CheckpointSQLite || files[1].Role != materializer.CheckpointGraphData {
			t.Errorf("source did not produce actual paired cut: cut=%d files=%+v", cut, files)
			return
		}
		sources := make([]checkpoint.Source, len(files))
		for i, file := range files {
			sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
		}
		root, rootErr := node.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
		if rootErr != nil {
			t.Errorf("upload actual paired checkpoint root: %v", rootErr)
			return
		}
		prefix, ok := sourceCore.PrefixHash(quepaxa.Slot(cut))
		if !ok {
			t.Errorf("actual source prefix unavailable")
			return
		}
		next, following, orderErr := sourceCore.CheckpointLeaderOrders(quepaxa.Slot(cut))
		if orderErr != nil {
			t.Errorf("actual source checkpoint orders: %v", orderErr)
			return
		}
		seal := quepaxa.CheckpointSeal{
			ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash,
			PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		}
		if syncErr := archive.SyncThrough(ctx, sourceCore, 1); syncErr != nil {
			t.Errorf("publish only the actual source prefix before QCKP: %v", syncErr)
			return
		}
		for _, peer := range peers {
			if prepareErr := peer.PrepareCheckpoint(ctx, seal); prepareErr != nil {
				t.Errorf("prepare actual quorum checkpoint: %v", prepareErr)
				return
			}
		}
		sealValue, sealErr := quepaxa.EncodeCheckpointSeal(seal)
		if sealErr != nil {
			t.Errorf("encode actual QCKP: %v", sealErr)
			return
		}
		if slot, _, proposeErr := sourceCore.Propose(ctx, sealValue); proposeErr != nil || slot != 2 {
			t.Errorf("certify actual unarchived QCKP: slot=%d err=%v", slot, proposeErr)
			return
		}
		qckpValue = bytes.Clone(sealValue)
		if got, ok := core.CertifiedValue(2); !ok || got.Hash != sha256.Sum256(sealValue) || !bytes.Equal(got.Value, sealValue) {
			t.Errorf("actual target Core did not receive the unarchived QCKP: value=%+v ok=%t", got, ok)
			return
		}
		if archive.Tip() != 1 || core.Tip() != 2 || !core.IsVoter() {
			t.Errorf("setup lost target startup state: Core=%d archive=%d voter=%t", core.Tip(), archive.Tip(), core.IsVoter())
			return
		}
		var nonce [types.ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		slot3Value = types.EncodeReadBarrier(nonce)
		if slot, _, proposeErr := sourceCore.Propose(ctx, slot3Value); proposeErr != nil || slot != 3 {
			t.Errorf("certify actual slot-3 decision while original voter WAL is live: slot=%d err=%v", slot, proposeErr)
			return
		}
		var decisionOK bool
		slot3Decision, decisionOK = sourceCore.CertifiedValue(3)
		if !decisionOK || slot3Decision.Slot != 3 || !bytes.Equal(slot3Decision.Value, slot3Value) {
			t.Errorf("source lacks actual certified slot-3 decision: decision=%+v ok=%t", slot3Decision, decisionOK)
			return
		}
		if got, ok := core.CertifiedValue(3); !ok || got.Hash != slot3Decision.Hash || !bytes.Equal(got.Value, slot3Value) {
			t.Errorf("actual target Core did not receive slot-3 decision: decision=%+v ok=%t", got, ok)
			return
		}
		if archive.Tip() != 1 || core.Tip() != 3 {
			t.Errorf("slot-3 fixture advanced archive or missed target decision: Core=%d archive=%d", core.Tip(), archive.Tip())
			return
		}
		beforeStats = bucket.Stats()
		beforeObjects = snapshotManagedOpenAbortObjects(store)
	})
	hookRestored := false
	t.Cleanup(func() {
		if !hookRestored {
			restoreHook()
		}
	})
	openErr := node.Open(ctx)
	restoreHook()
	hookRestored = true
	if !hookReached || openErr == nil || !strings.Contains(openErr.Error(), "durable archive") {
		t.Fatalf("public Open did not reach actual selector refusal: hook=%t err=%v", hookReached, openErr)
	}
	if sourceCore == nil || core == nil || archive == nil || bucket == nil {
		t.Fatal("actual startup fixture did not capture initialized runtime")
	}
	if got := bucket.Stats(); got.Uploads != beforeStats.Uploads || got.Deletes != beforeStats.Deletes {
		t.Fatalf("failed Open cleanup mutated archive: uploads %d→%d deletes %d→%d", beforeStats.Uploads, got.Uploads, beforeStats.Deletes, got.Deletes)
	}
	if after := snapshotManagedOpenAbortObjects(store); !equalManagedOpenAbortObjects(beforeObjects, after) {
		t.Fatalf("failed Open cleanup changed remote object set/HEAD bytes: before=%v after=%v", sortedManagedOpenAbortKeys(beforeObjects), sortedManagedOpenAbortKeys(after))
	}
	if core.Tip() != 3 || archive.Tip() != 1 {
		t.Fatalf("failed Open changed the actual local/archive tips: Core=%d archive=%d", core.Tip(), archive.Tip())
	}
	if value, ok := core.CertifiedValue(3); !ok || value.Hash != slot3Decision.Hash || !bytes.Equal(value.Value, slot3Value) {
		t.Fatalf("failed Open changed the actual slot-3 certificate: value=%+v ok=%t", value, ok)
	}
	if node.opened.Load() || node.lock != nil || node.wal != nil || node.material != nil || node.archive != nil || node.peer != nil || node.transport != nil || node.cancel != nil {
		t.Fatal("failed Open cleanup retained an owned runtime resource")
	}

	// A restart with the same local cert and stale archive must refuse again
	// without changing HEAD or any object; this is the retained-open neighbor.
	retained := New(config)
	retained.managedStartup = &ManagedStartupConfig{
		expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy,
		Identity: node.managedStartup.Identity, Checker: &managedTestChecker{}, BootstrapMember: true,
	}
	var retainedBucket *localobj.MeteredBucket
	var retainedArchive *recovery.Manager
	var retainedCore *quepaxa.Core
	var retainedStats localobj.Stats
	var retainedObjects map[string][]byte
	retainedRestore := localtesthooks.Set(func(event string) {
		if event == "node:managed-graph-startup:pair_select" {
			retainedBucket, retainedArchive, retainedCore = retained.bucket, retained.archive, retained.core
			if retainedBucket != nil {
				retainedStats = retainedBucket.Stats()
				retainedObjects = snapshotManagedOpenAbortObjects(store)
			}
		}
	})
	retainedHookRestored := false
	t.Cleanup(func() {
		if !retainedHookRestored {
			retainedRestore()
		}
	})
	retainedErr := retained.Open(ctx)
	retainedRestore()
	retainedHookRestored = true
	if retainedErr == nil || !strings.Contains(retainedErr.Error(), "durable archive") || retainedCore == nil || retainedArchive == nil || retainedBucket == nil {
		t.Fatalf("retained Open did not refuse the same unarchived producer: err=%v core=%t archive=%t bucket=%t", retainedErr, retainedCore != nil, retainedArchive != nil, retainedBucket != nil)
	}
	if got := retainedBucket.Stats(); got.Uploads != retainedStats.Uploads || got.Deletes != retainedStats.Deletes {
		t.Fatalf("retained Open cleanup mutated archive: uploads %d→%d deletes %d→%d", retainedStats.Uploads, got.Uploads, retainedStats.Deletes, got.Deletes)
	}
	if after := snapshotManagedOpenAbortObjects(store); !equalManagedOpenAbortObjects(retainedObjects, after) {
		t.Fatalf("retained Open cleanup changed remote objects: before=%v after=%v", sortedManagedOpenAbortKeys(retainedObjects), sortedManagedOpenAbortKeys(after))
	}
	if retainedCore.Tip() != 3 || retainedArchive.Tip() != 1 {
		t.Fatalf("retained Open changed local/archive tips: Core=%d archive=%d", retainedCore.Tip(), retainedArchive.Tip())
	}
	if value, ok := retainedCore.CertifiedValue(2); !ok || value.Hash != sha256.Sum256(qckpValue) || !bytes.Equal(value.Value, qckpValue) {
		t.Fatalf("retained Open changed local certified QCKP: value=%+v ok=%t", value, ok)
	}

	// Explicit archive publication remains the only path that advances the missing QCKP.
	writerBucket, err := localobj.NewBucketForNamespace(ctx, localobj.Config{
		Provider: localobj.ProviderS3, Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "prepared-target",
		Insecure: true, AccessKey: "test", SecretKey: "test",
	}, namespace.FinalPrefix)
	if err != nil {
		t.Fatal("open explicit archive writer", err)
	}
	t.Cleanup(func() {
		if err := writerBucket.Close(); err != nil {
			t.Errorf("close explicit archive bucket: %v", err)
		}
	})
	writer, err := recovery.NewAppendOnlyManagerWithValueValidator(writerBucket, namespace.FinalPrefix, 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal("open explicit archive writer manager", err)
	}
	t.Cleanup(writer.Close)
	writer.SetGroupDelay(0)
	if err := writer.Load(ctx); err != nil {
		t.Fatal("load explicit archive writer", err)
	}
	if err := writer.SyncThrough(ctx, sourceCore, 2); err != nil {
		t.Fatal("explicitly publish actual certified QCKP", err)
	}

	// Once the exact QCKP cert is explicitly published, Open succeeds with the
	// already-retained slot-3 decision. Ordinary Shutdown archives that decision.
	restarted := New(config)
	restarted.managedStartup = &ManagedStartupConfig{
		expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy,
		Identity: node.managedStartup.Identity, Checker: &managedTestChecker{}, BootstrapMember: true,
	}
	if err := restarted.Open(ctx); err != nil {
		t.Fatalf("Open after explicit exact-certificate publication: %v", err)
	}
	t.Cleanup(func() {
		if restarted.opened.Load() {
			if err := restarted.Shutdown(); err != nil {
				t.Errorf("cleanup successful restart: %v", err)
			}
		}
	})
	if restarted.core.Tip() != 3 || restarted.archive.Tip() != 3 {
		t.Fatalf("normal-shutdown fixture lost actual archive gap: source=%d restarted=%d archive=%d", sourceCore.Tip(), restarted.core.Tip(), restarted.archive.Tip())
	}
	if got, ok := restarted.core.CertifiedValue(3); !ok || got.Hash != slot3Decision.Hash || !bytes.Equal(got.Value, slot3Value) {
		t.Fatalf("successful restart lost actual retained slot-3 decision: decision=%+v ok=%t", got, ok)
	}
	shutdownArchive := restarted.archive
	if err := restarted.Shutdown(); err != nil {
		t.Fatal("normal successful shutdown", err)
	}
	if shutdownArchive.Tip() != 3 {
		t.Fatalf("normal shutdown lost already-published archive suffix: archiveTip=%d", shutdownArchive.Tip())
	}
}

func TestManagedAppendOnlyNormalShutdownArchivesExistingCertifiedSuffix(t *testing.T) {
	n, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
	if core.Tip() != 2 || archive.Tip() != 1 {
		t.Fatalf("shutdown fixture lacks actual certified suffix: Core=%d archive=%d", core.Tip(), archive.Tip())
	}
	before := bucket.mutationSnapshot()
	if err := n.Shutdown(); err != nil {
		t.Fatalf("normal shutdown: %v", err)
	}
	after := bucket.mutationSnapshot()
	if archive.Tip() != 2 || len(after) <= len(before) {
		t.Fatalf("normal shutdown did not publish existing actual suffix: archiveTip=%d mutations_before=%+v mutations_after=%+v", archive.Tip(), before, after)
	}
	for _, mutation := range after[len(before):] {
		if mutation.operation != "upload" {
			t.Fatalf("normal shutdown unexpectedly mutated append-only archive with %q: %+v", mutation.operation, after[len(before):])
		}
	}
}

func snapshotManagedOpenAbortObjects(store *managedTestObjectStore) map[string][]byte {
	store.mu.RLock()
	defer store.mu.RUnlock()
	copy := make(map[string][]byte, len(store.objects))
	for key, value := range store.objects {
		copy[key] = bytes.Clone(value)
	}
	return copy
}

func equalManagedOpenAbortObjects(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if !bytes.Equal(value, b[key]) {
			return false
		}
	}
	return true
}

func sortedManagedOpenAbortKeys(objects map[string][]byte) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
