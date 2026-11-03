package node

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestManagedNoPVCPreparedLearnerPromotesSameCutCertifiedRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	protocol := sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	policy := sqlpolicy.ManagedRequestMaterializationPolicy
	storageRoot, assignment, clusterID := "managed-nopvc-fixture", "assignment", "cluster"
	namespace, err := ManagedNamespaceFor(storageRoot, "cluster-uid", 0, assignment, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()
	storeConfig := localobj.Config{Provider: localobj.ProviderFilesystem, FilesystemDir: storeDir}
	bucket, err := localobj.NewBucketForNamespace(ctx, storeConfig, namespace.FinalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	checkpoints, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, namespace.FinalPrefix, t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.Load(ctx); err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, namespace.FinalPrefix, 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	archive.SetGroupDelay(0)
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	ids := []quepaxa.NodeID{"a", "b", "c"}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: make([]quepaxa.Member, len(ids))}
	typesMembers := make([]quepaxa.Member, len(ids))
	transport := &managedAppendOnlyTestTransport{peers: make(map[quepaxa.NodeID]*quepaxa.Core), down: make(map[quepaxa.NodeID]bool)}
	for i, id := range ids {
		member := peerMember(clusterID, id, string(id)+"-peer")
		member.PeerURL = fmt.Sprintf("quic://127.0.0.1:%d", 9100+i)
		member.WALIdentity = hex.EncodeToString(bytes.Repeat([]byte{byte(i + 1)}, 32))
		cluster.Members[i] = member
		typesMembers[i] = member
	}
	source := New(&types.ExecutionConfig{NodeID: "a", ClusterID: types.ClusterID(clusterID), Members: typesMembers, EnableReconfiguration: true})
	source.managedStartup = &ManagedStartupConfig{expectedPolicy: policy, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol}}
	source.checkpoints, source.archive = checkpoints, archive
	var cores []*quepaxa.Core
	var wals []*qlog.WAL
	for i, id := range ids {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		wals = append(wals, wal)
		nonce, _ := hex.DecodeString(cluster.Members[i].WALIdentity)
		if err := wal.BindIdentity(nonce); err != nil {
			t.Fatal(err)
		}
		var walIdentity [32]byte
		copy(walIdentity[:], nonce)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: id, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ValidateValue: types.ValidateManagedKVExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
				Binding: quepaxa.ManagedAppendOnlyBinding{
					ApplicationProtocol: protocol, BootstrapPlanDigest: fmt.Sprintf("%064x", 55),
					MemberConfigDigest: fmt.Sprintf("%064x", i+1), Generation: 0, WALIdentity: walIdentity,
				},
				Verify: source.verifyManagedAppendOnlyCandidate,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		cores = append(cores, core)
		transport.peers[id] = core
		if i == 0 {
			source.core = core
		}
	}
	defer func() {
		for _, wal := range wals {
			_ = wal.Close()
		}
	}()
	mat, err := source.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	source.material = mat
	command, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "nopvc-source", Operation: "put", Key: "evidence", Value: []byte("certified")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores[0].Propose(ctx, command); err != nil {
		t.Fatal("certify source application slot", err)
	}
	if err := mat.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, cores[0], 1); err != nil {
		t.Fatal(err)
	}
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	rootA, err := checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := cores[0].PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("source checkpoint prefix missing")
	}
	next, following, err := cores[0].CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	sealA := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: rootA.RootHash, StateHash: rootA.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	history, err := cores[0].CheckpointMembership(sealA.Index)
	if err != nil {
		t.Fatal("capture exact historical membership for prepared base", err)
	}
	sealA.Membership = &history
	valueA, err := quepaxa.EncodeCheckpointSeal(sealA)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores[0].Propose(ctx, valueA); err != nil {
		t.Fatal("certify physical recovery root A", err)
	}
	if err := archive.SyncThrough(ctx, cores[0], cores[0].Tip()); err != nil {
		t.Fatal(err)
	}
	sealedA, ok, err := cores[0].LatestManagedCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("certified A missing: %v", err)
	}
	if err := source.promoteManagedAppendOnlySeal(ctx, sealedA); err != nil {
		t.Fatal("publish physical A as CURRENT", err)
	}
	decisionA, ok := cores[0].CertifiedValue(sealedA.DecisionSlot)
	if !ok {
		t.Fatal("physical A quorum decision missing")
	}
	if err := archive.TrimThrough(ctx, sealedA, decisionA); err != nil {
		t.Fatalf("establish actual A recovery floor while retaining the certified suffix: %v", err)
	}
	fileDB, err := sql.Open("sqlite3", files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	_, editErr := fileDB.ExecContext(ctx, "PRAGMA user_version=1")
	closeErr := fileDB.Close()
	if editErr != nil || closeErr != nil {
		t.Fatal(editErr, closeErr)
	}
	rootB, err := checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil || rootB.RootHash == rootA.RootHash {
		t.Fatalf("same-cut B root not distinct: %+v err=%v", rootB, err)
	}
	sealB := sealA
	sealB.RootHash, sealB.StateHash = rootB.RootHash, rootB.Hash
	valueB, err := quepaxa.EncodeCheckpointSeal(sealB)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores[0].Propose(ctx, valueB); err != nil {
		t.Fatal("certify same-cut alternate root B", err)
	}
	if err := archive.SyncThrough(ctx, cores[0], cores[0].Tip()); err != nil {
		t.Fatal(err)
	}
	locator, exists, err := checkpoints.LoadManagedAppendOnlyLocator(ctx, policy)
	if err != nil || !exists || locator.RootHash != rootA.RootHash {
		t.Fatalf("fixture CURRENT must remain at physical A before learner startup: %+v exists=%t err=%v", locator, exists, err)
	}
	view, err := recovery.OpenReadOnlyViewForApplicationProtocol(ctx, bucket, namespace.FinalPrefix, 1, cluster, policy, types.DefaultIdempotencyWindowSlots, protocol, true)
	if err != nil {
		t.Fatal(err)
	}
	base, _, hasBase := view.RecoveryBase()
	if !hasBase || view.Base() != quepaxa.Slot(cut) || base.RootHash != sealA.RootHash || view.Tip() != cores[0].Tip() {
		t.Fatalf("prepared fixture is not A-base/B-suffix: base=%+v hasBase=%t cut=%d tip=%d", base, hasBase, view.Base(), view.Tip())
	}
	learner := peerMember(clusterID, "d", "d-peer")
	learner.PeerURL = "quic://127.0.0.1:0"
	dataDir := filepath.Join(t.TempDir(), "prepared")
	objectStore := &managedTestObjectStore{objects: make(map[string][]byte)}
	objectServer := httptest.NewServer(http.HandlerFunc(objectStore.serve))
	t.Cleanup(objectServer.Close)
	config := &types.ExecutionConfig{
		NodeID: learner.ID, ClusterID: types.ClusterID(clusterID), DataDir: dataDir, Members: typesMembers, Learner: &learner,
		PeerAddr: "127.0.0.1:0", PeerToken: "d-peer", AdminToken: "cluster-admin", EnableReconfiguration: true, ObjStoreProvider: "s3",
		ObjStoreEndpoint: strings.TrimPrefix(objectServer.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "prepared-target",
		ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStorePrefix: namespace.ConfigPrefix, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	}
	node := New(config)
	physical, memberDigest, err := node.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := node.ManagedBootstrapPlanDigestForProtocol(physical, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "cluster-uid", Generation: 0, DeploymentUID: "deployment", PodUID: "pod-d", NodeID: "d", SourceBinding: physical, ConfigDigest: memberDigest}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoffDir, identity, policy, types.DefaultIdempotencyWindowSlots); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartupForApplicationProtocol(ctx, bucket, namespace.FinalPrefix, 1, cluster, handoffDir, dataDir, identity, policy, types.DefaultIdempotencyWindowSlots, protocol, true)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := json.Marshal(recovery.NewMembershipRecord(clusterID, cluster.Members, string(config.ObjStoreDurability)))
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, path.Join(namespace.FinalPrefix, "voters/membership.json"), bytes.NewReader(membership)); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Iter(ctx, namespace.FinalPrefix+"/", func(name string) error {
		reader, err := bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		objectStore.put(name, body)
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		t.Fatal("copy exact source objects into isolated S3 SDK fixture", err)
	}
	startupIdentity := ManagedStartupIdentity{
		ApplicationProtocol: protocol, EvidenceFormat: protocol, Member: identity,
		AssignmentID: assignment, IntentHash: "intent", StorageID: storageRoot,
		PhysicalBinding: physical, BootstrapPlanDigest: bootstrap,
	}
	if err := node.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: startupIdentity, Checker: &managedTestChecker{}, Prepared: prepared}); err != nil {
		objectStore.mu.RLock()
		requests := append([]string(nil), objectStore.requests...)
		objectStore.mu.RUnlock()
		t.Fatalf("public prepared learner startup: %v; requests=%v", err, requests)
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if node.core == nil || node.core.IsVoter() || node.Ready() {
		t.Fatalf("prepared learner voted or served before membership promotion: core=%t voter=%t ready=%t", node.core != nil, node.core != nil && node.core.IsVoter(), node.Ready())
	}
	floor, floorRoot, ok := node.core.RecoveryRoot()
	if !ok || floor != quepaxa.Slot(cut) || floorRoot != sealA.RootHash {
		t.Fatalf("startup changed physical A recovery floor: floor=%d root=%x ok=%t", floor, floorRoot, ok)
	}
	selected, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(ctx, policy)
	if err != nil || !exists || selected.RootHash != rootB.RootHash || selected.Index != uint64(cut) {
		latest, latestExists, latestErr := node.core.LatestManagedCheckpointSeal()
		selectedRoot, selectedSeal, selectErr := node.selectManagedAppendOnlyRoot(ctx)
		t.Fatalf("prepared startup did not promote same-cut B representative: current=%+v exists=%t err=%v core_tip=%d latest=%+v latest_exists=%t latest_err=%v selected_root=%+v selected_seal=%+v select_err=%v A=%x B=%x", selected, exists, err, node.core.Tip(), latest, latestExists, latestErr, selectedRoot, selectedSeal, selectErr, sealA.RootHash, rootB.RootHash)
	}
	if node.core.CompactionFloor() != quepaxa.Slot(cut) {
		t.Fatal("representative promotion changed physical recovery floor")
	}
}
