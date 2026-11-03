//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestManagedGraphStartupReconstructionAllowsCertifiedSuffixGrowth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const protocol = sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	clusterID := types.ClusterID("managed-graph-reconstruction-growth")
	bucket := objstore.NewInMemBucket()
	cluster := quepaxa.Cluster{ConfigID: 1}
	members := make([]quepaxa.Member, 0, 3)
	for _, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		members = append(members, member)
	}
	cluster.Members = members
	transport := graphReviewTransport{}
	nodes := make(map[quepaxa.NodeID]*Node, len(members))
	wals := make([]*qlog.WAL, 0, len(members))
	archives := make([]*recovery.Manager, 0, len(members))
	for index, member := range members {
		dataDir := t.TempDir()
		config := &types.ExecutionConfig{
			NodeID: member.ID, ClusterID: clusterID, DataDir: dataDir, Members: members,
			PeerToken: string(member.ID) + "-token", AdminToken: "admin-token",
			ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreRegion: "local",
			ObjStorePrefix: string(clusterID), ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
		}
		n := New(config)
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy}
		source, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		generation := int64(0)
		identity := ManagedStartupIdentity{
			ApplicationProtocol: protocol, EvidenceFormat: protocol,
			Member: recovery.PreparedStartupIdentity{
				ClusterUID: string(clusterID), Generation: generation, DeploymentUID: "deployment-growth",
				PodUID: "pod-" + string(member.ID), NodeID: string(member.ID),
				SourceBinding: source, ConfigDigest: memberDigest,
			},
			AssignmentID: "assignment-" + string(member.ID), IntentHash: fmt.Sprintf("%064x", index+1),
			StorageID: "storage-growth", PhysicalBinding: source, BootstrapPlanDigest: planDigest,
		}
		n.managedStartup.Identity = identity
		profile := &managedVoterProfileBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, Generation: generation}
		// Match Node.Open's fresh-voter ordering: inspect registration state
		// before opening the WAL, so an absent local manifest remains a clean
		// fresh enrollment rather than being misclassified as lost existing WAL.
		local, err := loadVoterIdentity(config)
		if err != nil {
			t.Fatal(err)
		}
		wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		wals = append(wals, wal)
		if err := ensureVoterIdentityWithProfile(ctx, config, bucket, wal, local, false, false, profile); err != nil {
			t.Fatalf("register actual voter %s: %v", member.ID, err)
		}
		n.checkpoints, err = checkpoint.NewAppendOnlyManagerForPolicy(bucket, string(clusterID), t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
		if err != nil {
			t.Fatal(err)
		}
		n.archive, err = recovery.NewAppendOnlyManagerWithValueValidator(bucket, string(clusterID), 1, types.ValidateManagedGraphExecutionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.archive.Load(ctx); err != nil {
			t.Fatal(err)
		}
		archives = append(archives, n.archive)
		identityBytes, registered := wal.Identity()
		if !registered || len(identityBytes) != 32 {
			t.Fatalf("voter %s lacks actual registered 32-byte WAL identity", member.ID)
		}
		var walIdentity [32]byte
		copy(walIdentity[:], identityBytes)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport,
			ValidateValue: types.ValidateManagedGraphExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest,
				Generation: generation, WALIdentity: walIdentity,
			}, Verify: n.verifyManagedAppendOnlyCandidate},
		})
		if err != nil {
			t.Fatal(err)
		}
		n.core, n.wal = core, wal
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		transport[member.ID] = core
		nodes[member.ID] = n
	}
	t.Cleanup(func() {
		for _, archive := range archives {
			archive.Close()
		}
		for _, wal := range wals {
			if err := wal.Close(); err != nil {
				t.Errorf("close WAL: %v", err)
			}
		}
	})
	n := nodes["n1"]
	lockClean, lock, err := qlog.Acquire(filepath.Join(n.config.DataDir, "qlog"))
	if err != nil || !lockClean {
		t.Fatalf("acquire actual startup WAL lock: clean=%t err=%v", lockClean, err)
	}
	n.lock = lock
	t.Cleanup(func() {
		if err := lock.Release(); err != nil {
			t.Errorf("release startup WAL lock: %v", err)
		}
	})
	material, err := n.openMaterializer(filepath.Join(t.TempDir(), "source.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := material.Close(); err != nil {
			t.Errorf("close source materializer: %v", err)
		}
	})
	for slot := uint64(1); slot <= 2; slot++ {
		value, err := types.EncodeManagedGraphExecution(types.GraphCommand{
			RequestID: fmt.Sprintf("source-%d", slot), Cypher: fmt.Sprintf("CREATE (:GrowthSource {slot:%d})", slot),
		})
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := n.core.Propose(ctx, value)
		if err != nil || uint64(got) != slot {
			t.Fatalf("certify source Graph slot %d: got=%d err=%v", slot, got, err)
		}
		if err := material.Apply(ctx, slot, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.archive.SyncThrough(ctx, n.core, 2); err != nil {
		t.Fatal(err)
	}
	files, cut, releaseFiles, err := material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cut != 2 || len(files) != 2 {
		releaseFiles()
		t.Fatalf("source is not an actual paired Graph prefix: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for index, file := range files {
		sources[index] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := n.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	releaseFiles()
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := n.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual candidate prefix missing")
	}
	next, following, err := n.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	for _, peer := range transport {
		if err := peer.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("prepare actual paired checkpoint on %v: %v", peer, err)
		}
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := n.core.Propose(ctx, sealValue)
	if err != nil || sealSlot != 3 {
		t.Fatalf("certify actual object-backed QCKP: slot=%d err=%v", sealSlot, err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
		t.Fatal(err)
	}
	selected, ok, err := n.core.LatestManagedCheckpointSeal()
	if err != nil || !ok || selected.Index != 2 || selected.DecisionSlot != 3 {
		t.Fatalf("actual QCKP not selected: seal=%+v found=%t err=%v", selected, ok, err)
	}
	if err := n.promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatalf("promote exact actual locator: %v", err)
	}
	lockIdentity, registered := n.wal.Identity()
	if !registered || hex.EncodeToString(lockIdentity) != n.core.WALIdentity() {
		t.Fatal("startup Node lacks the exact actual registered WAL identity")
	}
	capturedTip := n.core.Tip()
	capturedPrefix, ok := n.core.PrefixHash(capturedTip)
	if !ok || capturedTip != 3 {
		t.Fatalf("initial reconstruction source not fully certified: tip=%d prefix_ok=%t", capturedTip, ok)
	}
	initialCluster := n.core.CurrentCluster()
	initialConfig := n.core.ConfigID()
	initialFloor, initialFloorRoot, _ := n.core.RecoveryRoot()
	initialWAL := n.core.WALIdentity()
	initialIdentity := n.managedStartup.Identity

	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	releaseHook := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseHook)
	restoreHook := localtesthooks.Set(func(event string) {
		if event == "node:managed-graph-startup:source-cut-captured" {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	t.Cleanup(restoreHook)
	type reconstructionResult struct {
		projection *managedGraphStartupProjection
		err        error
	}
	result := make(chan reconstructionResult, 1)
	go func() {
		projection, err := n.reconstructManagedGraphStartup(ctx, n.core, &selected)
		result <- reconstructionResult{projection: projection, err: err}
	}()
	var reconstruction reconstructionResult
	received, projectionCleaned := false, false
	defer func() {
		releaseHook()
		if !received {
			select {
			case reconstruction = <-result:
				received = true
			case <-time.After(5 * time.Second):
				t.Error("reconstruction goroutine did not exit after releasing its gate")
			}
		}
		if received && reconstruction.projection != nil && !projectionCleaned {
			if err := reconstruction.projection.cleanup(); err != nil {
				t.Errorf("cleanup reconstruction projection: %v", err)
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("reconstruction did not reach captured-source boundary: %v", ctx.Err())
	}
	if n.core.Tip() != capturedTip {
		t.Fatalf("fixture grew before the captured-source assertion: captured=%d current=%d", capturedTip, n.core.Tip())
	}
	if got, ok := n.core.PrefixHash(capturedTip); !ok || got != capturedPrefix {
		t.Fatal("captured certified prefix changed before the controlled append")
	}
	if n.core.WALIdentity() != initialWAL || n.core.ConfigID() != initialConfig || !reflect.DeepEqual(n.core.CurrentCluster(), initialCluster) || initialIdentity != n.managedStartup.Identity {
		t.Fatal("startup authority/binding drifted before controlled append")
	}
	if floor, root, _ := n.core.RecoveryRoot(); floor != initialFloor || root != initialFloorRoot {
		t.Fatal("recovery floor/root changed before controlled append")
	}
	growthValue, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "certified-growth", Cypher: "CREATE (:CertifiedGrowth {after_cut:true})"})
	if err != nil {
		t.Fatal(err)
	}
	growthSlot, _, err := nodes["n2"].core.Propose(ctx, growthValue)
	if err != nil || growthSlot != capturedTip+1 {
		t.Fatalf("certify actual monotone Graph suffix: slot=%d err=%v", growthSlot, err)
	}
	for _, peer := range transport {
		decision, found := peer.CertifiedValue(growthSlot)
		if !found || !bytes.Equal(decision.Value, growthValue) || decision.Hash != quepaxa.ValueHash(sha256.Sum256(growthValue)) {
			t.Fatalf("peer %v lacks the exact real growth certificate: found=%t decision=%+v", peer, found, decision)
		}
	}
	if got, ok := n.core.PrefixHash(capturedTip); !ok || got != capturedPrefix {
		t.Fatal("normal certified growth changed the captured prefix")
	}
	if n.core.Tip() != growthSlot || n.core.WALIdentity() != initialWAL || n.core.ConfigID() != initialConfig || !reflect.DeepEqual(n.core.CurrentCluster(), initialCluster) || n.managedStartup.Identity != initialIdentity {
		t.Fatal("controlled growth changed authority or did not advance the certified tip")
	}
	if floor, root, _ := n.core.RecoveryRoot(); floor != initialFloor || root != initialFloorRoot {
		t.Fatal("controlled growth changed recovery floor/root")
	}
	releaseHook()
	select {
	case reconstruction = <-result:
		received = true
	case <-ctx.Done():
		t.Fatalf("reconstruction did not return after controlled growth: %v", ctx.Err())
	}
	if reconstruction.err != nil {
		t.Fatalf("fixed captured-prefix reconstruction rejected legitimate certified suffix growth: %v", reconstruction.err)
	}
	if reconstruction.projection == nil || reconstruction.projection.tip != capturedTip || reconstruction.projection.prefix != capturedPrefix || reconstruction.projection.cut.CertifiedTip != uint64(capturedTip) {
		t.Fatalf("reconstruction did not retain the fixed captured cut: projection=%+v", reconstruction.projection)
	}
	if producer := reconstruction.projection.coreCut.ProducerDecisionSlot(); producer <= quepaxa.Slot(reconstruction.projection.cut.CheckpointSlot) || producer > capturedTip {
		t.Fatalf("selected root producer is outside the captured cut: producer=%d cut=%d", producer, capturedTip)
	}
	replayThrough, err := n.core.RecheckManagedReconstructionCut(ctx, reconstruction.projection.coreCut)
	if err != nil || replayThrough != growthSlot {
		t.Fatalf("atomic recheck did not return the fixed certified replay bound: through=%d want=%d err=%v", replayThrough, growthSlot, err)
	}
	canceled, cancelCheck := context.WithCancel(ctx)
	cancelCheck()
	if _, err := n.core.RecheckManagedReconstructionCut(canceled, reconstruction.projection.coreCut); err != context.Canceled {
		t.Fatalf("canceled reconstruction recheck error=%v, want context canceled", err)
	}
	suffix, err := certifiedSuffixThrough(n.core, capturedTip+1, growthSlot)
	if err != nil || len(suffix) != 1 || suffix[0].Hash != quepaxa.ValueHash(sha256.Sum256(growthValue)) || !bytes.Equal(suffix[0].Value, growthValue) {
		t.Fatalf("real post-cut suffix was lost instead of remaining replayable: suffix=%+v err=%v", suffix, err)
	}
	if err := reconstruction.projection.cleanup(); err != nil {
		t.Fatalf("cleanup successful reconstruction projection: %v", err)
	}
	projectionCleaned = true
}
