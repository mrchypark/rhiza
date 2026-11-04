//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	"github.com/thanos-io/objstore"
)

type preparedLearnerFixture struct {
	ctx        context.Context
	node       *Node
	sourceCore *quepaxa.Core
	protocol   string
	dataDir    string
	cut        quepaxa.Slot
	sealA      quepaxa.SealedCheckpoint
	sealB      quepaxa.SealedCheckpoint
	rootA      [32]byte
	rootB      [32]byte
	viewTip    quepaxa.Slot
}

func newPreparedLearnerFixture(t *testing.T) *preparedLearnerFixture {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	policy := sqlpolicy.ManagedGraphRequestMaterializationPolicy
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
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, namespace.FinalPrefix, 1, types.ValidateManagedGraphExecutionPolicy)
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
			ValidateValue: types.ValidateManagedGraphExecutionPolicy,
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
	t.Cleanup(func() {
		for _, wal := range wals {
			_ = wal.Close()
		}
	})
	mat, err := source.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	source.material = mat
	command, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "graph-alternate-source", Cypher: "CREATE (:SameFloorAlternate {value:'certified'})"})
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
	bProducer, _, err := cores[0].Propose(ctx, valueB)
	if err != nil {
		t.Fatal("certify same-cut alternate root B", err)
	}
	if err := archive.SyncThrough(ctx, cores[0], cores[0].Tip()); err != nil {
		t.Fatal(err)
	}
	sealedB, ok, err := cores[0].LatestManagedCheckpointSeal()
	if err != nil || !ok || sealedB.DecisionSlot != bProducer || sealedB.RootHash != rootB.RootHash {
		t.Fatalf("certified B producer not available as exact sealed checkpoint: seal=%+v producer=%d err=%v", sealedB, bProducer, err)
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
	if bProducer <= quepaxa.Slot(cut) || bProducer > view.Tip() {
		t.Fatalf("selected B producer is outside the captured K: J=%d B=%d K=%d", cut, bProducer, view.Tip())
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
	return &preparedLearnerFixture{ctx: ctx, node: node, sourceCore: cores[0], protocol: protocol, dataDir: dataDir, cut: quepaxa.Slot(cut), sealA: sealedA, sealB: sealedB, rootA: rootA.RootHash, rootB: rootB.RootHash, viewTip: view.Tip()}
}

func TestManagedGraphStartupInstallPreparedLearnerFreshOpenCancellationClosesHandle(t *testing.T) {
	fixture := newPreparedLearnerFixture(t)
	ctx, node := fixture.ctx, fixture.node
	protocol, dataDir := fixture.protocol, fixture.dataDir
	if node.core == nil || node.core.IsVoter() || node.Ready() {
		t.Fatalf("prepared learner voted or served before membership promotion: core=%t voter=%t ready=%t", node.core != nil, node.core != nil && node.core.IsVoter(), node.Ready())
	}
	floor, floorRoot, ok := node.core.RecoveryRoot()
	if !ok || floor != fixture.cut || floorRoot != fixture.sealA.RootHash {
		t.Fatalf("startup changed physical A recovery floor: floor=%d root=%x ok=%t", floor, floorRoot, ok)
	}
	selected, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || selected.RootHash != fixture.rootA || selected.Index != uint64(fixture.cut) {
		t.Fatalf("startup changed source CURRENT while installing same-floor B: current=%+v exists=%t err=%v A=%x B=%x", selected, exists, err, fixture.rootA, fixture.rootB)
	}
	if node.core.CompactionFloor() != fixture.cut || node.material == nil || node.core.Tip() != fixture.viewTip || quepaxa.Slot(node.material.Tip()) != fixture.viewTip {
		t.Fatalf("learner floor/materialization mismatch: floor=%d core=%d material=%v K=%d", node.core.CompactionFloor(), node.core.Tip(), node.material, fixture.viewTip)
	}

	// Move the already-open target aside so this call exercises a real absent
	// target install, not an overwrite or a synthetic Materializer close.
	stopPreparedLearnerBackgroundWork(t, node)
	if err := node.material.Close(); err != nil {
		t.Fatal("close initial prepared target", err)
	}
	node.material = nil
	target := moveManagedGraphStartupTargetAside(t, dataDir)
	observed, err := materializer.InspectManagedGraphInstallTarget(ctx, target, types.DefaultIdempotencyWindowSlots, protocol)
	if err != nil || !observed.Absent {
		t.Fatalf("fresh cancellation fixture target is not absent: state=%+v err=%v", observed, err)
	}
	installCtx, cancelInstall := context.WithCancel(ctx)
	var afterOpen atomic.Bool
	var completedCloses atomic.Int32
	restoreHook := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-graph-startup:pair_opened-before-final-recheck":
			afterOpen.Store(true)
			cancelInstall()
		case "materializer:close:complete":
			if afterOpen.Load() {
				completedCloses.Add(1)
			}
		}
	})
	opened, _, installErr := node.installManagedGraphStartup(installCtx, node.core, observed, &fixture.sealB)
	restoreHook()
	cancelInstall()
	if opened != nil || !errors.Is(installErr, context.Canceled) {
		t.Fatalf("post-open cancellation result: opened=%t err=%v", opened != nil, installErr)
	}
	if !afterOpen.Load() || completedCloses.Load() != 1 {
		t.Fatalf("acquired materializer cleanup did not complete exactly once: opened_hook=%t completed_closes=%d", afterOpen.Load(), completedCloses.Load())
	}
	reopened, err := node.openMaterializer(target, 4)
	if err != nil {
		t.Fatal("reopen installed target after cancellation cleanup", err)
	}
	if quepaxa.Slot(reopened.Tip()) != fixture.viewTip {
		_ = reopened.Close()
		t.Fatalf("reopened target lost installed K: got=%d want=%d", reopened.Tip(), fixture.viewTip)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal("close verification reopen", err)
	}
}

func TestManagedGraphStartupInstallPreparedLearnerRetainedResumeCancellationClosesHandle(t *testing.T) {
	fixture := newPreparedLearnerFixture(t)
	node := fixture.node
	if node.material == nil {
		t.Fatal("prepared learner did not retain its initial materializer")
	}
	stopPreparedLearnerBackgroundWork(t, node)
	if err := node.material.Close(); err != nil {
		t.Fatal("close initial prepared target", err)
	}
	node.material = nil
	target := moveManagedGraphStartupTargetAside(t, fixture.dataDir)
	projection, err := node.reconstructManagedGraphStartup(fixture.ctx, node.core, &fixture.sealB)
	if err != nil {
		t.Fatal("construct actual retained-resume input from the prepared learner", err)
	}
	defer func() { _ = projection.cleanup() }()
	encoded, err := json.Marshal(materializer.ManagedGraphInstall{ApplicationProtocol: fixture.protocol, Cut: projection.cut, Files: projection.files})
	if err != nil {
		t.Fatal("encode actual prepared install input", err)
	}
	cmd := exec.CommandContext(fixture.ctx, os.Args[0], "-test.run=^TestManagedGraphStartupInstallCrashHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_NODE_INSTALL_INPUT="+string(encoded), "RHIZA_TEST_NODE_INSTALL_TARGET="+target)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("install crash helper unexpectedly returned normally: %s", output)
	} else if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 73 {
		t.Fatalf("install crash helper did not stop at the real after-open boundary: err=%v output=%s", err, output)
	}
	observed, err := materializer.InspectManagedGraphInstallTarget(fixture.ctx, target, types.DefaultIdempotencyWindowSlots, fixture.protocol)
	if err != nil || observed.InstallAttempt == nil || observed.InstallAttempt.Phase != "opened" {
		t.Fatalf("child did not leave the actual inventory-backed opened journal: attempt=%+v err=%v", observed.InstallAttempt, err)
	}
	installCtx, cancelInstall := context.WithCancel(fixture.ctx)
	var afterOpen atomic.Bool
	var completedCloses atomic.Int32
	restoreHook := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-graph-startup:pair_opened-before-final-recheck":
			afterOpen.Store(true)
			cancelInstall()
		case "materializer:close:complete":
			if afterOpen.Load() {
				completedCloses.Add(1)
			}
		}
	})
	opened, _, installErr := node.installManagedGraphStartup(installCtx, node.core, observed, nil)
	restoreHook()
	cancelInstall()
	if opened != nil || !errors.Is(installErr, context.Canceled) {
		t.Fatalf("retained post-open cancellation result: opened=%t err=%v", opened != nil, installErr)
	}
	if !afterOpen.Load() || completedCloses.Load() != 1 {
		t.Fatalf("retained acquired materializer cleanup did not complete exactly once: opened_hook=%t completed_closes=%d", afterOpen.Load(), completedCloses.Load())
	}
	reopened, err := node.openMaterializer(target, 4)
	if err != nil {
		t.Fatal("reopen resumed target after cancellation cleanup", err)
	}
	if quepaxa.Slot(reopened.Tip()) != fixture.viewTip {
		_ = reopened.Close()
		t.Fatalf("reopened retained target lost installed K: got=%d want=%d", reopened.Tip(), fixture.viewTip)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal("close retained-target verification reopen", err)
	}
}

func TestManagedGraphStartupInstallPreparedLearnerRetainedResumeReplaysCertifiedSuffix(t *testing.T) {
	fixture := newPreparedLearnerFixture(t)
	node := fixture.node
	stopPreparedLearnerBackgroundWork(t, node)
	if node.material == nil {
		t.Fatal("prepared learner did not retain its initial materializer")
	}
	if err := node.material.Close(); err != nil {
		t.Fatal("close initial prepared target", err)
	}
	node.material = nil
	target := moveManagedGraphStartupTargetAside(t, fixture.dataDir)
	projection, err := node.reconstructManagedGraphStartup(fixture.ctx, node.core, &fixture.sealB)
	if err != nil {
		t.Fatal("construct actual retained-resume input from the prepared learner", err)
	}
	defer func() { _ = projection.cleanup() }()
	if projection.cut.CertifiedTip != uint64(fixture.viewTip) || fixture.sourceCore.Tip() != fixture.viewTip {
		t.Fatalf("journal/source fixture did not share captured K: journalK=%d sourceTip=%d K=%d", projection.cut.CertifiedTip, fixture.sourceCore.Tip(), fixture.viewTip)
	}
	encoded, err := json.Marshal(materializer.ManagedGraphInstall{ApplicationProtocol: fixture.protocol, Cut: projection.cut, Files: projection.files})
	if err != nil {
		t.Fatal("encode actual prepared install input", err)
	}
	cmd := exec.CommandContext(fixture.ctx, os.Args[0], "-test.run=^TestManagedGraphStartupInstallCrashHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_NODE_INSTALL_INPUT="+string(encoded), "RHIZA_TEST_NODE_INSTALL_TARGET="+target)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("install crash helper unexpectedly returned normally: %s", output)
	} else if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 73 {
		t.Fatalf("install crash helper did not stop at the real after-open boundary: err=%v output=%s", err, output)
	}
	observed, err := materializer.InspectManagedGraphInstallTarget(fixture.ctx, target, types.DefaultIdempotencyWindowSlots, fixture.protocol)
	if err != nil || observed.InstallAttempt == nil || observed.InstallAttempt.Phase != "opened" || observed.InstallAttempt.Cut.CertifiedTip != uint64(fixture.viewTip) {
		t.Fatalf("child did not leave the exact K inventory-backed opened journal: attempt=%+v err=%v", observed.InstallAttempt, err)
	}
	command, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "prepared-learner-post-k", Cypher: "CREATE (:ManagedGraphResume {id:'suffix-j'})"})
	if err != nil {
		t.Fatal("encode actual post-K source decision", err)
	}
	if fixture.sourceCore.Tip() != fixture.viewTip {
		t.Fatalf("source quorum changed before J proposal: tip=%d K=%d", fixture.sourceCore.Tip(), fixture.viewTip)
	}
	j, _, err := fixture.sourceCore.Propose(fixture.ctx, command)
	if err != nil || j <= fixture.viewTip {
		t.Fatalf("actual source quorum did not certify J>K: J=%d K=%d err=%v", j, fixture.viewTip, err)
	}
	decision, certified := fixture.sourceCore.CertifiedValue(j)
	wantHash := quepaxa.ValueHash(sha256.Sum256(command))
	if !certified || decision.Slot != j || decision.Hash != wantHash || !bytes.Equal(decision.Value, command) {
		t.Fatalf("source Core lacks the exact actual J certificate: decision=%+v certified=%t J=%d", decision, certified, j)
	}
	if err := node.core.AcceptCertifiedValues([]quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal("import actual quorum-certified J into the readonly learner", err)
	}
	if node.core.Tip() != j || node.core.IsVoter() {
		t.Fatalf("learner did not retain imported J without voting: tip=%d J=%d voter=%t", node.core.Tip(), j, node.core.IsVoter())
	}
	opened, replayThrough, err := node.installManagedGraphStartup(fixture.ctx, node.core, observed, nil)
	var installedTip uint64
	if opened != nil {
		installedTip = opened.Tip()
	}
	if err != nil || opened == nil || replayThrough != j || quepaxa.Slot(opened.Tip()) != fixture.viewTip {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("retained resume did not install captured K and return fixed J: opened=%t installedTip=%d K=%d replayThrough=%d J=%d err=%v", opened != nil, installedTip, fixture.viewTip, replayThrough, j, err)
	}
	node.material = opened
	if err := node.replayLocalDecisionsThrough(fixture.ctx, replayThrough); err != nil {
		t.Fatal("production fixed-prefix replay through actual certified J", err)
	}
	if node.core.IsVoter() || node.Ready() || quepaxa.Slot(node.material.Tip()) != j {
		t.Fatalf("suffix replay changed learner admission or missed J: voter=%t ready=%t material=%d J=%d", node.core.IsVoter(), node.Ready(), node.material.Tip(), j)
	}
	rows, err := node.material.GraphQuery(fixture.ctx, "MATCH (n:ManagedGraphResume) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "suffix-j" {
		t.Fatalf("Graph readback did not include actual J suffix: rows=%+v err=%v", rows.Rows, err)
	}
	current, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(fixture.ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || current.RootHash != fixture.rootA || current.Index != uint64(fixture.cut) {
		t.Fatalf("retained resume changed physical CURRENT A: current=%+v exists=%t err=%v", current, exists, err)
	}
}

func TestManagedGraphStartupInstallCrashHelper(t *testing.T) {
	encoded := os.Getenv("RHIZA_TEST_NODE_INSTALL_INPUT")
	if encoded == "" {
		return
	}
	var input materializer.ManagedGraphInstall
	if err := json.Unmarshal([]byte(encoded), &input); err != nil {
		t.Fatal("decode actual Node install input", err)
	}
	restore := localtesthooks.Set(func(event string) {
		if event == "materializer:managed-graph-install:after-open-inventory-written" {
			os.Exit(73)
		}
	})
	defer restore()
	opened, err := materializer.OpenManagedGraphFromVerifiedPair(context.Background(), os.Getenv("RHIZA_TEST_NODE_INSTALL_TARGET"), 4, input)
	if err != nil {
		t.Fatal("open actual prepared pair in child", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal("close child materializer", err)
	}
	t.Fatal("crash hook was not reached")
}

func moveManagedGraphStartupTargetAside(t *testing.T, dataDir string) string {
	t.Helper()
	target := filepath.Join(dataDir, "sqlite.db")
	backup := t.TempDir()
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "sqlite.db-journal"} {
		from := filepath.Join(dataDir, name)
		if _, err := os.Lstat(from); err == nil {
			if err := os.Rename(from, filepath.Join(backup, name)); err != nil {
				t.Fatal("preserve prior SQLite target", err)
			}
		} else if !os.IsNotExist(err) {
			t.Fatal("inspect prior SQLite target", err)
		}
	}
	graphDir := filepath.Join(dataDir, "latticedb")
	if _, err := os.Lstat(graphDir); err == nil {
		if err := os.Rename(graphDir, filepath.Join(backup, "latticedb")); err != nil {
			t.Fatal("preserve prior Graph target", err)
		}
	} else if !os.IsNotExist(err) {
		t.Fatal("inspect prior Graph target", err)
	}
	return target
}

func stopPreparedLearnerBackgroundWork(t *testing.T, node *Node) {
	t.Helper()
	if node.cancel == nil {
		t.Fatal("prepared learner has no owned lifecycle cancellation")
	}
	// Stop and join Node.Open's own catch-up/archive workers before the test
	// replaces node.material. Core, WAL, lock, and startup identity stay open
	// for the direct install/recheck path under test.
	node.cancel()
	node.wg.Wait()
	if node.core == nil || node.wal == nil || node.lock == nil || node.managedStartup == nil {
		t.Fatal("stopping owned workers unexpectedly closed reconstruction authority")
	}
}
