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
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestManagedNoPVCPreparedLearnerPreservesCurrentAndCertifiedRoot(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, false, false, false, false, false)
}

func TestManagedNoPVCPreparedLearnerNilPreparedRestartRemainsReadOnly(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, false, false, false, true, false)
}

func TestManagedNoPVCPreparedLearnerAdoptsHBeforeLaterCurrentAndJ(t *testing.T) {
	runManagedNoPVCPreparedLearnerStartup(t, true, false, false, false, true)
}

func runManagedNoPVCPreparedLearnerStartup(t *testing.T, appendSuffix, cancelAfterAcceptedSuffix, partialReplayAfterCancel, nilPreparedRestart, advanceCurrentAfterH bool) {
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
	sealedB, ok, err := cores[0].LatestManagedCheckpointSeal()
	if err != nil || !ok || sealedB.Index != sealB.Index || sealedB.RootHash != sealB.RootHash || sealedB.StateHash != sealB.StateHash {
		t.Fatalf("certified B missing as exact managed checkpoint: seal=%+v exists=%t err=%v expected=%+v", sealedB, ok, err, sealB)
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
	var suffixSlot quepaxa.Slot
	var suffixCommand []byte
	var suffixDecision quepaxa.DecidedValue
	var suffixFirstDecision quepaxa.DecidedValue
	currentRoot := rootA.RootHash
	currentSeal := sealedB
	if appendSuffix {
		if err := source.replayLocalDecisions(ctx); err != nil {
			t.Fatal("apply captured H prefix to source materializer", err)
		}
		if quepaxa.Slot(mat.Tip()) != view.Tip() {
			t.Fatalf("source materializer did not reach prepared H: material=%d H=%d", mat.Tip(), view.Tip())
		}
		firstRequestID, firstKey, firstValue := "nopvc-suffix-j", "suffix-j", "certified at J"
		if partialReplayAfterCancel {
			firstRequestID, firstKey, firstValue = "nopvc-suffix-j1", "suffix-j1", "certified before J"
		}
		suffixCommand, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: firstRequestID, Operation: "put", Key: firstKey, Value: []byte(firstValue)})
		if err != nil {
			t.Fatal("encode certified post-H application", err)
		}
		firstSlot, _, err := cores[0].Propose(ctx, suffixCommand)
		if err != nil || firstSlot <= view.Tip() {
			t.Fatalf("source quorum did not certify post-H slot: J1=%d H=%d err=%v", firstSlot, view.Tip(), err)
		}
		var certified bool
		suffixFirstDecision, certified = cores[0].CertifiedValue(firstSlot)
		if !certified || suffixFirstDecision.Slot != firstSlot || !bytes.Equal(suffixFirstDecision.Value, suffixCommand) || suffixFirstDecision.Hash != quepaxa.ValueHash(sha256.Sum256(suffixCommand)) {
			t.Fatalf("source quorum lacks exact first suffix certificate: decision=%+v certified=%t J1=%d", suffixFirstDecision, certified, firstSlot)
		}
		if err := source.replayLocalDecisions(ctx); err != nil {
			t.Fatal("apply certified first suffix to source materializer", err)
		}
		if partialReplayAfterCancel {
			suffixCommand, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: "nopvc-suffix-j2", Operation: "put", Key: "suffix-j", Value: []byte("certified at J")})
			if err != nil {
				t.Fatal("encode certified second post-H application", err)
			}
			suffixSlot, _, err = cores[0].Propose(ctx, suffixCommand)
			if err != nil || suffixSlot <= firstSlot {
				t.Fatalf("source quorum did not certify J>J1: J=%d J1=%d err=%v", suffixSlot, firstSlot, err)
			}
			suffixDecision, certified = cores[0].CertifiedValue(suffixSlot)
			if !certified || suffixDecision.Slot != suffixSlot || !bytes.Equal(suffixDecision.Value, suffixCommand) || suffixDecision.Hash != quepaxa.ValueHash(sha256.Sum256(suffixCommand)) {
				t.Fatalf("source quorum lacks exact J certificate: decision=%+v certified=%t J=%d", suffixDecision, certified, suffixSlot)
			}
			if err := source.replayLocalDecisions(ctx); err != nil {
				t.Fatal("apply certified J to source materializer", err)
			}
		} else {
			suffixSlot = firstSlot
			suffixDecision = suffixFirstDecision
		}
		if err := archive.SyncThrough(ctx, cores[0], suffixSlot); err != nil {
			t.Fatal("archive certified suffix through J", err)
		}
	}
	if advanceCurrentAfterH {
		// Prepared H was published above. Advance CURRENT only after that frozen
		// source view, using another real quorum-certified same-cut QCKP.
		fileDB, err := sql.Open("sqlite3", files[0].Path)
		if err != nil {
			t.Fatal("open prepared KV source for later CURRENT root", err)
		}
		_, editErr := fileDB.ExecContext(ctx, "PRAGMA user_version=2")
		closeErr := fileDB.Close()
		if editErr != nil || closeErr != nil {
			t.Fatal("make later CURRENT root distinct", editErr, closeErr)
		}
		rootC, err := checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
		if err != nil || rootC.RootHash == rootB.RootHash {
			t.Fatalf("later same-cut CURRENT C root is not distinct from H's B: root=%+v err=%v", rootC, err)
		}
		sealC := sealA
		sealC.RootHash, sealC.StateHash = rootC.RootHash, rootC.Hash
		valueC, err := quepaxa.EncodeCheckpointSeal(sealC)
		if err != nil {
			t.Fatal("encode later certified CURRENT C", err)
		}
		if _, _, err := cores[0].Propose(ctx, valueC); err != nil {
			t.Fatal("certify later CURRENT C with source quorum", err)
		}
		if err := archive.SyncThrough(ctx, cores[0], cores[0].Tip()); err != nil {
			t.Fatal("archive later CURRENT C certificate", err)
		}
		currentSeal, ok, err = cores[0].LatestManagedCheckpointSeal()
		if err != nil || !ok || currentSeal.RootHash != rootC.RootHash {
			t.Fatalf("source quorum did not retain exact later CURRENT C: seal=%+v err=%v", currentSeal, err)
		}
		if err := source.promoteManagedAppendOnlySeal(ctx, currentSeal); err != nil {
			t.Fatal("publish actual later CURRENT C", err)
		}
		currentRoot = rootC.RootHash
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
	backupSnapshot := func() map[string][]byte {
		objectStore.mu.RLock()
		defer objectStore.mu.RUnlock()
		objects := make(map[string][]byte)
		for name, body := range objectStore.objects {
			if strings.HasPrefix(name, namespace.FinalPrefix+"/archive/") || strings.HasPrefix(name, namespace.FinalPrefix+"/checkpoint/") {
				objects[name] = bytes.Clone(body)
			}
		}
		return objects
	}
	backupDiff := func(before, after map[string][]byte) []string {
		keys := make(map[string]struct{}, len(before)+len(after))
		for key := range before {
			keys[key] = struct{}{}
		}
		for key := range after {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			oldBody, oldOK := before[key]
			newBody, newOK := after[key]
			if oldOK != newOK || !bytes.Equal(oldBody, newBody) {
				ordered = append(ordered, key)
			}
		}
		sort.Strings(ordered)
		changes := make([]string, 0, len(ordered))
		for _, key := range ordered {
			oldBody, oldOK := before[key]
			newBody, newOK := after[key]
			oldHash, newHash := "<absent>", "<absent>"
			if oldOK {
				oldHash = fmt.Sprintf("%x", sha256.Sum256(oldBody))
			}
			if newOK {
				newHash = fmt.Sprintf("%x", sha256.Sum256(newBody))
			}
			changes = append(changes, fmt.Sprintf("%s old_sha256=%s new_sha256=%s", key, oldHash, newHash))
		}
		return changes
	}
	writeRequestsSince := func(start int) []string {
		objectStore.mu.RLock()
		defer objectStore.mu.RUnlock()
		if start > len(objectStore.requests) {
			start = len(objectStore.requests)
		}
		var writes []string
		for _, request := range objectStore.requests[start:] {
			if !strings.HasPrefix(request, "GET ") && !strings.HasPrefix(request, "HEAD ") {
				writes = append(writes, request)
			}
		}
		return writes
	}
	backupBeforeOpen := backupSnapshot()
	objectStore.mu.RLock()
	requestsBeforeOpen := len(objectStore.requests)
	objectStore.mu.RUnlock()
	registrationKey := path.Join(namespace.FinalPrefix, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(config.NodeID))))
	objectStore.mu.RLock()
	_, alreadyRegistered := objectStore.objects[registrationKey]
	objectStore.mu.RUnlock()
	if alreadyRegistered {
		t.Fatal("fresh learner registration unexpectedly exists before Open")
	}
	verifyOnlyFreshRegistration := func() {
		t.Helper()
		mutations := writeRequestsSince(requestsBeforeOpen)
		wantPut := "PUT /" + config.ObjStoreBucket + "/" + registrationKey
		if len(mutations) != 1 || mutations[0] != wantPut {
			t.Fatalf("fresh learner may only register its exact voter identity: mutations=%v", mutations)
		}
		objectStore.mu.RLock()
		registration := append([]byte(nil), objectStore.objects[registrationKey]...)
		objectStore.mu.RUnlock()
		var got voterIdentity
		if err := json.Unmarshal(registration, &got); err != nil {
			t.Fatal("decode fresh learner registration", err)
		}
		nonce, err := hex.DecodeString(got.Nonce)
		if err != nil || len(nonce) != 32 || node.core == nil || got.Nonce != node.core.WALIdentity() {
			t.Fatalf("fresh learner registration lacks exact WAL nonce: nonce=%q err=%v", got.Nonce, err)
		}
		profile, err := node.managedVoterProfileFor(config)
		if err != nil || profile == nil {
			t.Fatalf("fresh learner profile unavailable: %v", err)
		}
		want := voterIdentityForProfile(config, profile)
		want.Nonce, want.Registered = got.Nonce, true
		local, err := loadVoterIdentity(config)
		if !sameVoterIdentity(got, want) || err != nil || local.identity == nil || !sameVoterIdentity(got, *local.identity) {
			t.Fatalf("fresh registration differs from exact profile/local identity: got=%+v want=%+v local=%+v err=%v", got, want, local.identity, err)
		}
	}
	startupIdentity := ManagedStartupIdentity{
		ApplicationProtocol: protocol, EvidenceFormat: protocol, Member: identity,
		AssignmentID: assignment, IntentHash: "intent", StorageID: storageRoot,
		PhysicalBinding: physical, BootstrapPlanDigest: bootstrap,
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Error(err)
		}
		mutations := writeRequestsSince(requestsBeforeOpen)
		wantPut := "PUT /" + config.ObjStoreBucket + "/" + registrationKey
		if len(mutations) != 1 || mutations[0] != wantPut {
			t.Errorf("catch-up or shutdown mutated storage beyond exact initial registration: %v", mutations)
		}
		if appendSuffix {
			after := backupSnapshot()
			diff := backupDiff(backupBeforeOpen, after)
			if len(diff) != 0 {
				t.Errorf("unpromoted learner changed archive/checkpoint storage during catch-up or shutdown: objects=%v all_mutations=%v", diff, writeRequestsSince(requestsBeforeOpen))
			}
		}
	})
	if appendSuffix && localtesthooks.Enabled {
		adopted := make(chan struct{})
		resume := make(chan struct{})
		var adoptedOnce, releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(resume) }) }
		suffixAccepted := make(chan struct{})
		resumeSuffix := make(chan struct{})
		var suffixAcceptedOnce, releaseSuffixOnce sync.Once
		releaseSuffix := func() { releaseSuffixOnce.Do(func() { close(resumeSuffix) }) }
		openCtx, cancelOpen := context.WithCancel(ctx)
		defer cancelOpen()
		openDone := make(chan error, 1)
		var openDoneConsumed atomic.Bool
		restore := setManagedPreparedStartupTestHook(func(event string) {
			if event == "node:managed-prepared:adopted-before-suffix-catchup" {
				adoptedOnce.Do(func() { close(adopted) })
				<-resume
			}
			if cancelAfterAcceptedSuffix && event == "node:managed-prepared:suffix-page-accepted" {
				suffixAcceptedOnce.Do(func() {
					close(suffixAccepted)
					<-resumeSuffix
				})
			}
		})
		var restoreOnce sync.Once
		restoreHooks := func() { restoreOnce.Do(restore) }
		t.Cleanup(func() {
			cancelOpen()
			release()
			releaseSuffix()
			restoreHooks()
			if !openDoneConsumed.Load() {
				select {
				case <-openDone:
				case <-time.After(5 * time.Second):
					t.Error("public prepared startup did not finish during cleanup")
				}
			}
		})
		go func() {
			openDone <- node.OpenManaged(openCtx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: startupIdentity, Checker: &managedTestChecker{}, Prepared: prepared})
		}()
		select {
		case <-adopted:
		case err := <-openDone:
			openDoneConsumed.Store(true)
			t.Fatalf("public prepared startup returned before the persisted H adoption hook: %v", err)
		case <-ctx.Done():
			t.Fatalf("fixed H adoption hook was not reached: %v", ctx.Err())
		case <-time.After(5 * time.Second):
			t.Fatal("fixed H adoption hook was not reached")
		}
		adoption, adoptionErr := loadPreparedAdoption(filepath.Join(dataDir, "prepared-adoption.json"))
		var coreTip quepaxa.Slot
		if node.core != nil {
			coreTip = node.core.Tip()
		}
		if adoptionErr != nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() || node.core == nil || coreTip != view.Tip() || node.material == nil || quepaxa.Slot(node.material.Tip()) != view.Tip() {
			t.Fatalf("learner did not persist/install exact H before J catch-up: adoption=%+v err=%v core=%d material=%v H=%d", adoption, adoptionErr, coreTip, node.material, view.Tip())
		}
		diff := backupDiff(backupBeforeOpen, backupSnapshot())
		if len(diff) != 0 {
			t.Fatalf("prepared learner changed archive/checkpoint storage before J catch-up: objects=%v all_mutations=%v", diff, writeRequestsSince(requestsBeforeOpen))
		}
		verifyOnlyFreshRegistration()
		current, exists, currentErr := node.checkpoints.LoadManagedAppendOnlyLocator(ctx, policy)
		if currentErr != nil || !exists || current.RootHash != currentRoot {
			t.Fatalf("prepared learner changed the source CURRENT while adopting H: current=%+v exists=%t err=%v", current, exists, currentErr)
		}
		release()
		if cancelAfterAcceptedSuffix {
			select {
			case <-suffixAccepted:
			case err := <-openDone:
				openDoneConsumed.Store(true)
				t.Fatalf("startup returned before durable J admission: %v", err)
			case <-ctx.Done():
				t.Fatalf("prepared learner did not durably accept J: %v", ctx.Err())
			case <-time.After(5 * time.Second):
				t.Fatal("prepared learner did not reach post-WAL-acceptance cancellation gate")
			}
			if node.core == nil || node.core.Tip() != suffixSlot || node.material == nil || quepaxa.Slot(node.material.Tip()) != view.Tip() {
				t.Fatalf("J must be durable locally while materializer remains at H before cancellation: core=%v material=%v H=%d J=%d", node.core, node.material, view.Tip(), suffixSlot)
			}
			cancelOpen()
			releaseSuffix()
			select {
			case err := <-openDone:
				openDoneConsumed.Store(true)
				if err == nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled H-to-J startup must fail through its context: %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("failed Open did not return before fixture deadline: %v", ctx.Err())
			case <-time.After(5 * time.Second):
				t.Fatal("failed Open did not clean up after context cancellation")
			}
			if err := node.Shutdown(); err != nil {
				t.Fatalf("failed-open Node cleanup before process restart: %v", err)
			}
			retryConfig := *config
			if partialReplayAfterCancel {
				partial, err := node.openMaterializer(filepath.Join(dataDir, "sqlite.db"), 4)
				if err != nil {
					t.Fatalf("open durable local materializer for interrupted-prefix fixture: %v", err)
				}
				if err := partial.ApplyBatch(ctx, []quepaxa.DecidedValue{suffixFirstDecision}); err != nil {
					_ = partial.Close()
					t.Fatalf("apply actual certified J1 to establish H<A<J: %v", err)
				}
				if partial.Tip() != uint64(suffixFirstDecision.Slot) {
					_ = partial.Close()
					t.Fatalf("partial materializer stopped at %d, want J1=%d", partial.Tip(), suffixFirstDecision.Slot)
				}
				if err := partial.Close(); err != nil {
					t.Fatalf("close H<A<J materializer fixture: %v", err)
				}
			}
			node = New(&retryConfig)
			if err := node.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: startupIdentity, Checker: &managedTestChecker{}, Prepared: prepared}); err != nil {
				t.Fatalf("new Node with same Pod, DataDir, registered WAL, and persisted H view must resume verified local J: %v", err)
			}
		} else {
			select {
			case err := <-openDone:
				openDoneConsumed.Store(true)
				if err != nil {
					t.Fatalf("public prepared learner startup with archived J: %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("public prepared learner startup did not finish after J: %v", ctx.Err())
			}
		}
		restoreHooks()
	} else if err := node.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: startupIdentity, Checker: &managedTestChecker{}, Prepared: prepared}); err != nil {
		objectStore.mu.RLock()
		requests := append([]string(nil), objectStore.requests...)
		objectStore.mu.RUnlock()
		t.Fatalf("public prepared learner startup: %v; requests=%v", err, requests)
	}
	adoption, adoptionErr := loadPreparedAdoption(filepath.Join(dataDir, "prepared-adoption.json"))
	if adoptionErr != nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() {
		t.Fatalf("completed startup did not retain exact H adoption: adoption=%+v err=%v", adoption, adoptionErr)
	}
	verifyOnlyFreshRegistration()
	if nilPreparedRestart {
		objectStore.mu.RLock()
		requestsBeforeRestart := len(objectStore.requests)
		objectStore.mu.RUnlock()
		if err := node.Shutdown(); err != nil {
			t.Fatalf("close prepared learner after durable H adoption: %v", err)
		}
		node = New(config)
		if err := node.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: startupIdentity, Checker: &managedTestChecker{}}); err != nil {
			t.Fatalf("same-identity public restart with verified persisted H and Prepared=nil: %v", err)
		}
		if diff := backupDiff(backupBeforeOpen, backupSnapshot()); len(diff) != 0 {
			t.Fatalf("Prepared=nil readonly learner restart changed archive/checkpoint objects: %v", diff)
		}
		if writes := writeRequestsSince(requestsBeforeRestart); len(writes) != 0 {
			t.Fatalf("Prepared=nil readonly learner restart mutated object storage: %v", writes)
		}
	}
	if node.core == nil || node.core.IsVoter() || node.Ready() {
		t.Fatalf("prepared learner voted or served before membership promotion: core=%t voter=%t ready=%t", node.core != nil, node.core != nil && node.core.IsVoter(), node.Ready())
	}
	floor, floorRoot, ok := node.core.RecoveryRoot()
	if !ok || floor != quepaxa.Slot(cut) || floorRoot != sealA.RootHash {
		t.Fatalf("startup changed physical A recovery floor: floor=%d root=%x ok=%t", floor, floorRoot, ok)
	}
	selected, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(ctx, policy)
	if err != nil || !exists || selected.RootHash != currentRoot || selected.Index != uint64(cut) {
		t.Fatalf("unpromoted prepared learner changed physical CURRENT: current=%+v exists=%t err=%v expected=%x A=%x B=%x", selected, exists, err, currentRoot, rootA.RootHash, rootB.RootHash)
	}
	latest, latestExists, latestErr := node.core.LatestManagedCheckpointSeal()
	if latestErr != nil || !latestExists || latest.Index != currentSeal.Index || latest.RootHash != currentSeal.RootHash || latest.StateHash != currentSeal.StateHash {
		t.Fatalf("prepared learner lost certified local selected QCKP representative: latest=%+v exists=%t err=%v expected=%+v", latest, latestExists, latestErr, currentSeal)
	}
	bDecision, bCertified := node.core.CertifiedValue(sealedB.DecisionSlot)
	if !bCertified || !bytes.Equal(bDecision.Value, valueB) || bDecision.Hash != quepaxa.ValueHash(sha256.Sum256(valueB)) {
		t.Fatalf("prepared learner lost exact B certificate: decision=%+v certified=%t", bDecision, bCertified)
	}
	openedB, err := node.checkpoints.OpenRoot(ctx, uint64(rootB.Index), rootB.RootHash)
	if err != nil || openedB.Hash != rootB.Hash {
		t.Fatalf("prepared learner lost B root object: root=%+v err=%v", openedB, err)
	}
	if appendSuffix {
		wantCoreTip := suffixSlot
		if advanceCurrentAfterH {
			wantCoreTip = currentSeal.DecisionSlot
		}
		// The later certified QCKP is also applied, without changing physical floor A.
		if node.core.Tip() != wantCoreTip || node.material == nil || quepaxa.Slot(node.material.Tip()) != wantCoreTip {
			var appliedTip uint64
			if node.material != nil {
				appliedTip = node.material.Tip()
			}
			t.Fatalf("prepared learner did not adopt H, preserve later certified QCKP, and replay exact J suffix: core=%d material_present=%t material_tip=%d J=%d expectedCore=%d current_QCKP_slot=%d", node.core.Tip(), node.material != nil, appliedTip, suffixSlot, wantCoreTip, currentSeal.DecisionSlot)
		}
		appliedDecision, certified := node.core.CertifiedValue(suffixSlot)
		if !certified || appliedDecision.Slot != suffixDecision.Slot || appliedDecision.Hash != suffixDecision.Hash || !bytes.Equal(appliedDecision.Value, suffixDecision.Value) || !bytes.Equal(appliedDecision.Certificate, suffixDecision.Certificate) {
			t.Fatalf("prepared learner did not retain exact source J certificate: decision=%+v certified=%t source=%+v", appliedDecision, certified, suffixDecision)
		}
		value, found, readErr := node.material.KVGet(ctx, "suffix-j", time.Now())
		if readErr != nil || !found || string(value) != "certified at J" {
			t.Fatalf("prepared learner did not apply exact J command: value=%q found=%t err=%v", value, found, readErr)
		}
		if partialReplayAfterCancel {
			firstValue, firstFound, firstErr := node.material.KVGet(ctx, "suffix-j1", time.Now())
			if firstErr != nil || !firstFound || string(firstValue) != "certified before J" {
				t.Fatalf("prepared learner lost actual partially replayed J1: value=%q found=%t err=%v", firstValue, firstFound, firstErr)
			}
		}
		adoption, adoptionErr := loadPreparedAdoption(filepath.Join(dataDir, "prepared-adoption.json"))
		if adoptionErr != nil || adoption.Tip != view.Tip() || adoption.PrefixHash != view.PrefixHash() {
			t.Fatalf("prepared learner adoption boundary moved beyond H: adoption=%+v H=%d err=%v", adoption, view.Tip(), adoptionErr)
		}
	}
	if node.core.CompactionFloor() != quepaxa.Slot(cut) {
		t.Fatal("prepared learner changed physical recovery floor")
	}
}
