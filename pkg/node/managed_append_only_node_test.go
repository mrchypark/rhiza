package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type managedAppendOnlyTestTransport struct {
	peers map[quepaxa.NodeID]*quepaxa.Core
	down  map[quepaxa.NodeID]bool
}

type managedAppendOnlyLostAckBucket struct {
	objstore.Bucket
	loseCurrentAck bool
}

func (b *managedAppendOnlyLostAckBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, options...)
	if err == nil && b.loseCurrentAck && strings.HasSuffix(name, "checkpoint/CURRENT") {
		b.loseCurrentAck = false
		return fmt.Errorf("simulated CURRENT upload acknowledgement loss")
	}
	return err
}

func (p *managedAppendOnlyTestTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if p.down[to] || p.peers[to] == nil {
		return quepaxa.Summary{}, fmt.Errorf("test voter %s unavailable", to)
	}
	return p.peers[to].Record(ctx, r)
}

func (p *managedAppendOnlyTestTransport) SendDecision(_ context.Context, d quepaxa.Decision) error {
	for id, peer := range p.peers {
		if peer != nil && !p.down[id] {
			if err := peer.AcceptDecision(d); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *managedAppendOnlyTestTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	if p.down[to] || p.peers[to] == nil {
		return 0, fmt.Errorf("test voter %s unavailable", to)
	}
	return p.peers[to].Tip(), nil
}

func (p *managedAppendOnlyTestTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	if p.down[to] || p.peers[to] == nil {
		return fmt.Errorf("test voter %s unavailable", to)
	}
	return p.peers[to].StageValue(hash, value)
}

func (p *managedAppendOnlyTestTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	if p.down[from] || p.peers[from] == nil {
		return nil, fmt.Errorf("test voter %s unavailable", from)
	}
	value, ok := p.peers[from].Value(hash)
	if !ok {
		return nil, fmt.Errorf("test staged value unavailable")
	}
	return value, nil
}

func TestManagedAppendOnlyObjectCandidateSurvivesPartialPrepareAndVoterLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	lostAckBucket := &managedAppendOnlyLostAckBucket{Bucket: bucket}
	manager, err := checkpoint.NewAppendOnlyManagerForPolicy(lostAckBucket, "append-only", t.TempDir(), sqlpolicy.ManagedRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "append-only", 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	archive.SetGroupDelay(0)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	transport := &managedAppendOnlyTestTransport{peers: make(map[quepaxa.NodeID]*quepaxa.Core), down: make(map[quepaxa.NodeID]bool)}
	identities := map[quepaxa.NodeID]string{}
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		var nonce [32]byte
		for j := range nonce {
			nonce[j] = byte((i+1)*17 + j + 1)
		}
		identities[id] = hex.EncodeToString(nonce[:])
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1", WALIdentity: identities["n1"]}, {ID: "n2", WALIdentity: identities["n2"]}, {ID: "n3", WALIdentity: identities["n3"]}}}
	nodes := make(map[quepaxa.NodeID]*Node)
	cores := make(map[quepaxa.NodeID]*quepaxa.Core)
	walDirs := make(map[quepaxa.NodeID]string)
	walByID := make(map[quepaxa.NodeID]*qlog.WAL)
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		walDir := t.TempDir()
		walDirs[id] = walDir
		wal, err := qlog.Open(walDir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		nonce, _ := hex.DecodeString(identities[id])
		if err := wal.BindIdentity(nonce); err != nil {
			t.Fatal(err)
		}
		walByID[id] = wal
		n := New(&types.ExecutionConfig{NodeID: id})
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol}}
		n.checkpoints, n.archive = manager, archive
		nodes[id] = n
		var walID [32]byte
		copy(walID[:], nonce)
		memberDigest := fmt.Sprintf("%064x", i+1)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: id, Cluster: cluster, WAL: wal, Transport: transport, ValidateValue: types.ValidateManagedKVExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
				Binding: quepaxa.ManagedAppendOnlyBinding{
					ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol,
					BootstrapPlanDigest: fmt.Sprintf("%064x", 55), MemberConfigDigest: memberDigest, Generation: 0, WALIdentity: walID,
				},
				Verify: n.verifyManagedAppendOnlyCandidate,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		n.core = core
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		cores[id] = core
		transport.peers[id] = core
	}
	mat, err := nodes["n1"].openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	nodes["n1"].material = mat
	command, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "append-only-object-proof", Operation: "put", Key: "proof", Value: []byte("original-QCKP-input")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(ctx, command); err != nil {
		t.Fatal("certify source application decision", err)
	}
	if err := mat.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, cores["n1"], 1); err != nil {
		t.Fatal("archive certified cut before QCKP", err)
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
	root, err := manager.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := cores["n1"].PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual candidate cut prefix missing")
	}
	next, following, err := cores["n1"].CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	if err := cores["n2"].PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal("write the real partial prepare marker", err)
	}
	transport.down["n3"] = true
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(ctx, value); err != nil {
		t.Fatal("direct actual QCKP proposal should pass without a prepare quorum and with one voter unavailable", err)
	}
	selected, exists, err := cores["n1"].LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.Index != seal.Index || selected.RootHash != seal.RootHash {
		t.Fatalf("actual direct QCKP not selected: selected=%+v exists=%t err=%v", selected, exists, err)
	}
	if err := nodes["n1"].promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatal("object-backed archived certificate/CURRENT promotion", err)
	}
	locator, exists, err := manager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || !exists || locator.Index != uint64(seal.Index) || locator.RootHash != seal.RootHash || locator.SealDecisionSlot != uint64(selected.DecisionSlot) {
		t.Fatalf("CURRENT does not point to exact certified QCKP: locator=%+v exists=%t err=%v", locator, exists, err)
	}
	// A second actual physical root at the same cut is independently uploaded
	// and certified; lexicographic (cut, decision slot) selects the later one.
	db, err := sql.Open("sqlite3", files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	_, editErr := db.ExecContext(ctx, "PRAGMA user_version=1")
	closeErr := db.Close()
	if editErr != nil || closeErr != nil {
		t.Fatal(editErr, closeErr)
	}
	root2, err := manager.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil || root2.RootHash == root.RootHash {
		t.Fatalf("actual competing root was not distinct: %+v err=%v", root2, err)
	}
	seal2 := seal
	seal2.RootHash, seal2.StateHash = root2.RootHash, root2.Hash
	value2, err := quepaxa.EncodeCheckpointSeal(seal2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(ctx, value2); err != nil {
		t.Fatal("same-cut alternate root direct QCKP proposal", err)
	}
	selected, exists, err = cores["n1"].LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.Index != seal2.Index || selected.DecisionSlot <= 2 || selected.RootHash != seal2.RootHash {
		t.Fatalf("same-cut later certificate was not selected: %+v exists=%t err=%v", selected, exists, err)
	}
	lostAckBucket.loseCurrentAck = true
	if err := nodes["n1"].promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatal("CURRENT exact readback must resolve lost acknowledgement", err)
	}
	locator, exists, err = manager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || !exists || locator.RootHash != root2.RootHash || locator.SealDecisionSlot != uint64(selected.DecisionSlot) {
		t.Fatalf("lost-ACK CURRENT did not retain exact later root: %+v exists=%t err=%v", locator, exists, err)
	}
	applyThrough := func(target quepaxa.Slot) {
		t.Helper()
		from := quepaxa.Slot(mat.Tip() + 1)
		page, tip, pageErr := cores["n1"].DecisionsFrom(from, int(target-from+1))
		if pageErr != nil || tip < target || len(page) != int(target-from+1) {
			t.Fatalf("actual materializer replay page through %d missing: count=%d tip=%d err=%v", target, len(page), tip, pageErr)
		}
		if applyErr := mat.ApplyBatch(ctx, page); applyErr != nil {
			t.Fatal("apply exact certified suffix to actual materializer", applyErr)
		}
	}
	applyThrough(cores["n1"].Tip())
	// The unavailable voter's cold object/cache-miss validation resolves the
	// later exact QCKP from actual object bytes and archived certified prefix.
	page, _, err := cores["n1"].DecisionsFrom(1, 8)
	if err != nil || len(page) != int(cores["n1"].Tip()) {
		t.Fatalf("certified replay page unavailable: count=%d err=%v", len(page), err)
	}
	if err := cores["n3"].AcceptCertifiedValues(page); err != nil {
		t.Fatal("catch up unavailable voter from real certified values", err)
	}
	decision, found := cores["n3"].CertifiedValue(selected.DecisionSlot)
	if !found || !bytes.Equal(decision.Value, value2) || decision.Hash != quepaxa.ValueHash(sha256.Sum256(value2)) {
		t.Fatal("recovered voter did not retain exact original competing QCKP bytes")
	}
	if err := cores["n3"].ValidateManagedCheckpointCandidate(ctx, quepaxa.ManagedCheckpointCandidate{Value: decision.Value, Hash: decision.Hash, Seal: seal2, DecisionSlot: selected.DecisionSlot, ConfigID: cores["n3"].ConfigIDForSlot(selected.DecisionSlot)}); err != nil {
		t.Fatal("cold cache-miss candidate revalidation from actual objects/archive", err)
	}
	badCandidates := []struct {
		name string
		seal quepaxa.CheckpointSeal
	}{
		{name: "missing immutable root", seal: func() quepaxa.CheckpointSeal { s := seal2; s.RootHash[0] ^= 1; return s }()},
		{name: "wrong cut prefix", seal: func() quepaxa.CheckpointSeal { s := seal2; s.PrefixHash[0] ^= 1; return s }()},
		{name: "wrong historical config", seal: func() quepaxa.CheckpointSeal { s := seal2; s.ConfigID++; return s }()},
		{name: "wrong leader order", seal: func() quepaxa.CheckpointSeal {
			s := seal2
			s.NextLeaderOrder = append([]quepaxa.NodeID(nil), s.NextLeaderOrder...)
			s.NextLeaderOrder[0] = "not-the-certified-leader"
			return s
		}()},
		{name: "unexpected membership proof", seal: func() quepaxa.CheckpointSeal { s := seal2; s.Membership = &quepaxa.MembershipRecord{}; return s }()},
	}
	for _, test := range badCandidates {
		badValue, encodeErr := quepaxa.EncodeCheckpointSeal(test.seal)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		err := cores["n3"].ValidateManagedCheckpointCandidate(ctx, quepaxa.ManagedCheckpointCandidate{
			Value: badValue, Hash: quepaxa.ValueHash(sha256.Sum256(badValue)), Seal: test.seal,
			DecisionSlot: selected.DecisionSlot, ConfigID: cores["n3"].ConfigIDForSlot(selected.DecisionSlot),
		})
		if err == nil {
			t.Errorf("tampered candidate unexpectedly passed: %s", test.name)
		}
	}
	var barrierNonce [types.ReadBarrierNonceSize]byte
	barrier := types.EncodeReadBarrier(barrierNonce)
	if _, _, err := cores["n1"].Propose(ctx, barrier); err != nil {
		t.Fatal("existing control barrier after direct QCKP", err)
	}
	if cores["n1"].Tip() != cores["n2"].Tip() {
		t.Fatalf("available voters did not retain matching suffix: %d != %d", cores["n1"].Tip(), cores["n2"].Tip())
	}
	if err := cores["n3"].AcceptCertifiedValues([]quepaxa.DecidedValue{mustManagedTestDecision(t, cores["n1"], cores["n1"].Tip())}); err != nil {
		t.Fatal("restored voter catch-up through following barrier", err)
	}
	applyThrough(cores["n1"].Tip())
	// A later, higher-cut snapshot becomes CURRENT. A subsequently certified
	// but late lower-cut QCKP remains in the log and cannot move the hint back.
	command2, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "append-only-later-cut", Operation: "put", Key: "proof-2", Value: []byte("later")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(ctx, command2); err != nil {
		t.Fatal("certify later application suffix", err)
	}
	applyThrough(cores["n1"].Tip())
	if err := archive.SyncThrough(ctx, cores["n1"], cores["n1"].Tip()); err != nil {
		t.Fatal("archive later certified cut", err)
	}
	files3, cut3, release3, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release3()
	sources3 := make([]checkpoint.Source, len(files3))
	for i, file := range files3 {
		sources3[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root3, err := manager.CreateManagedAppendOnlyCandidate(ctx, sources3, cut3)
	if err != nil || cut3 <= cut {
		t.Fatalf("higher-cut candidate was not actual: cut=%d root=%+v err=%v", cut3, root3, err)
	}
	prefix3, ok := cores["n1"].PrefixHash(quepaxa.Slot(cut3))
	if !ok {
		t.Fatal("higher-cut prefix unavailable")
	}
	next3, following3, err := cores["n1"].CheckpointLeaderOrders(quepaxa.Slot(cut3))
	if err != nil {
		t.Fatal(err)
	}
	seal3 := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut3), RootHash: root3.RootHash, StateHash: root3.Hash, PrefixHash: prefix3, NextLeaderOrder: next3, FollowingLeaderOrder: following3}
	value3, err := quepaxa.EncodeCheckpointSeal(seal3)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(ctx, value3); err != nil {
		t.Fatal("direct higher-cut QCKP proposal", err)
	}
	selected, exists, err = cores["n1"].LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.Index != seal3.Index || selected.RootHash != seal3.RootHash {
		t.Fatalf("higher-cut QCKP not selected: %+v exists=%t err=%v", selected, exists, err)
	}
	if err := nodes["n1"].promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatal("promote higher-cut certified root", err)
	}
	if _, _, err := cores["n1"].Propose(ctx, value); err != nil {
		t.Fatal("late lower-cut QCKP must remain certifiable without lowering state", err)
	}
	selected, exists, err = cores["n1"].LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.Index != seal3.Index {
		t.Fatalf("late lower-cut value regressed the certified selector: %+v exists=%t err=%v", selected, exists, err)
	}
	if err := nodes["n1"].promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatal("reconcile CURRENT to certified maximum after late lower QCKP", err)
	}
	locator, exists, err = manager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || !exists || locator.Index != uint64(seal3.Index) || locator.RootHash != seal3.RootHash {
		t.Fatalf("late lower-cut QCKP lowered CURRENT: %+v exists=%t err=%v", locator, exists, err)
	}
	// An empty but correctly registered Core has no local cut facts. Its Node
	// verifier must reconstruct and authenticate the real archived certificate
	// suffix instead of treating hash continuity/certificate presence as proof.
	coldWALDir := t.TempDir()
	coldWAL, err := qlog.Open(coldWALDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coldWAL.Close() })
	n1Nonce, _ := hex.DecodeString(identities["n1"])
	if err := coldWAL.BindIdentity(n1Nonce); err != nil {
		t.Fatal(err)
	}
	var coldWALIdentity [32]byte
	copy(coldWALIdentity[:], n1Nonce)
	coldNode := New(&types.ExecutionConfig{NodeID: "n1", Members: cluster.Members})
	coldNode.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol}}
	coldNode.checkpoints, coldNode.archive = manager, archive
	coldCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1", Cluster: cluster, WAL: coldWAL, Transport: transport, ValidateValue: types.ValidateManagedKVExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
			Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: fmt.Sprintf("%064x", 55), MemberConfigDigest: fmt.Sprintf("%064x", 1), Generation: 0, WALIdentity: coldWALIdentity},
			Verify:  coldNode.verifyManagedAppendOnlyCandidate,
		},
	})
	if err != nil {
		t.Fatal("create cold registered Core", err)
	}
	coldNode.core = coldCore
	if err := coldCore.ValidateManagedCheckpointCandidate(ctx, quepaxa.ManagedCheckpointCandidate{
		Value: value3, Hash: quepaxa.ValueHash(sha256.Sum256(value3)), Seal: seal3,
		DecisionSlot: 6, ConfigID: 1,
	}); err != nil {
		t.Fatal("cold cache miss failed actual archived quorum-prefix validation", err)
	}
	for _, test := range badCandidates {
		badValue, encodeErr := quepaxa.EncodeCheckpointSeal(test.seal)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if err := coldCore.ValidateManagedCheckpointCandidate(ctx, quepaxa.ManagedCheckpointCandidate{
			Value: badValue, Hash: quepaxa.ValueHash(sha256.Sum256(badValue)), Seal: test.seal, DecisionSlot: 6, ConfigID: 1,
		}); err == nil {
			t.Errorf("cold archive verifier accepted tampered historical proof: %s", test.name)
		}
	}
	// Reopen the voter that held the real partial marker. Recovery must retain
	// marker integrity but must not re-promote that legacy marker as the V1
	// voting fence; the certified max-rank root and CURRENT remain identical.
	if err := walByID["n2"].Close(); err != nil {
		t.Fatal("close original prepared voter WAL", err)
	}
	reopenedWAL, err := qlog.Open(walDirs["n2"])
	if err != nil {
		t.Fatal("reopen actual prepared voter WAL", err)
	}
	t.Cleanup(func() { _ = reopenedWAL.Close() })
	nonce, _ := hex.DecodeString(identities["n2"])
	var reopenedIdentity [32]byte
	copy(reopenedIdentity[:], nonce)
	reopenedNode := New(&types.ExecutionConfig{NodeID: "n2"})
	reopenedNode.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol}}
	reopenedNode.checkpoints, reopenedNode.archive = manager, archive
	reopenedCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "n2", Cluster: cluster, WAL: reopenedWAL, Transport: transport, ValidateValue: types.ValidateManagedKVExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
			Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: fmt.Sprintf("%064x", 55), MemberConfigDigest: fmt.Sprintf("%064x", 2), Generation: 0, WALIdentity: reopenedIdentity},
			Verify:  reopenedNode.verifyManagedAppendOnlyCandidate,
		},
	})
	if err != nil {
		t.Fatal("recover actual WAL with partial prepare marker", err)
	}
	reopenedNode.core = reopenedCore
	if _, _, prepared := reopenedCore.LatestPreparedCheckpoint(); prepared {
		t.Fatal("managed restart promoted the old prepare marker into V1 authority")
	}
	reopenedCore.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	transport.peers["n2"] = reopenedCore
	reopenedRoot, reopenedSeal, err := reopenedNode.selectManagedAppendOnlyRoot(ctx)
	if err != nil || reopenedSeal == nil || reopenedRoot == nil || reopenedSeal.Index != seal3.Index || reopenedSeal.RootHash != seal3.RootHash {
		t.Fatalf("restart selected a different/lower managed root: root=%+v seal=%+v err=%v", reopenedRoot, reopenedSeal, err)
	}
	reopenedLocator, exists, err := manager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || !exists || reopenedLocator != locator {
		t.Fatalf("restart changed append-only CURRENT: got=%+v want=%+v exists=%t err=%v", reopenedLocator, locator, exists, err)
	}
}

func TestManagedAppendOnlyNodeOpensFinalProfilesAndClassifiesRetiredProfiles(t *testing.T) {
	tests := []struct {
		name     string
		policy   sqlpolicy.MaterializationPolicy
		protocol string
	}{
		{
			name:     "request-append-only",
			policy:   sqlpolicy.ManagedRequestMaterializationPolicy,
			protocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol,
		},
		{
			name:     "graph-append-only",
			policy:   sqlpolicy.ManagedGraphRequestMaterializationPolicy,
			protocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sqlite.db")
			n := New(&types.ExecutionConfig{NodeID: "append-only-profile-test"})
			n.managedStartup = &ManagedStartupConfig{
				expectedPolicy: test.policy,
				Identity:       ManagedStartupIdentity{ApplicationProtocol: test.protocol},
			}
			m, err := n.openMaterializer(path, 1)
			if err != nil {
				t.Fatal("open explicit append-only materializer", err)
			}
			if err := m.Close(); err != nil {
				t.Fatal("close append-only materializer", err)
			}
			if _, _, err := inspectManagedFileForPolicy(context.Background(), path, types.DefaultIdempotencyWindowSlots, test.policy, test.protocol); err != nil {
				t.Fatalf("exact file inspection rejected Node's selected profile: %v", err)
			}
			if test.policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				want := ""
				if test.protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
					want = sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
				} else if test.protocol == sqlpolicy.ManagedGraphRequestApplicationProtocol {
					want = sqlpolicy.ManagedGraphRequestApplicationProtocol
				}
				if got := n.managedGraphInstallProtocol(); got != want {
					t.Fatalf("Graph install protocol=%q, want %q", got, want)
				}
			}
		})
	}
	for _, test := range []struct {
		policy   sqlpolicy.MaterializationPolicy
		protocol string
	}{
		{sqlpolicy.ManagedRequestMaterializationPolicy, sqlpolicy.ManagedRequestApplicationProtocol},
		{sqlpolicy.ManagedGraphRequestMaterializationPolicy, sqlpolicy.ManagedGraphRequestApplicationProtocol},
	} {
		n := New(&types.ExecutionConfig{NodeID: "retired-profile-classification"})
		n.managedStartup = &ManagedStartupConfig{
			expectedPolicy: test.policy,
			Identity:       ManagedStartupIdentity{ApplicationProtocol: test.protocol},
		}
		if got, err := n.managedAppendOnlyProfile(); err != nil || got != nil {
			t.Fatalf("retired prototype unexpectedly enabled append-only peer profile: protocol=%q profile=%+v err=%v", test.protocol, got, err)
		}
		if test.policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
			if got := n.managedGraphInstallProtocol(); got != test.protocol {
				t.Fatalf("retired Graph prototype classification=%q, want exact stored protocol %q", got, test.protocol)
			}
		}
	}
}

func mustManagedTestDecision(t *testing.T, core *quepaxa.Core, slot quepaxa.Slot) quepaxa.DecidedValue {
	t.Helper()
	value, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatalf("certified decision %d unavailable", slot)
	}
	return value
}

func TestManagedGraphAppendOnlyCandidateAuthenticatesActualPair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	manager, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "graph-append-only", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "graph-append-only", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	archive.SetGroupDelay(0)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var nonce [32]byte
	for i := range nonce {
		nonce[i] = byte(i + 71)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.BindIdentity(nonce[:]); err != nil {
		t.Fatal(err)
	}
	id := hex.EncodeToString(nonce[:])
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "graph-voter", WALIdentity: id}}}
	n := New(&types.ExecutionConfig{NodeID: "graph-voter"})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol}}
	n.checkpoints, n.archive = manager, archive
	identityBinding := quepaxa.ManagedAppendOnlyBinding{
		ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol,
		BootstrapPlanDigest: fmt.Sprintf("%064x", 91), MemberConfigDigest: fmt.Sprintf("%064x", 92), Generation: 0,
	}
	copy(identityBinding.WALIdentity[:], nonce[:])
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "graph-voter", Cluster: cluster, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: identityBinding, Verify: n.verifyManagedAppendOnlyCandidate},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.core = core
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	mat, err := n.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	n.material = mat
	command, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "append-only-graph-pair", Cypher: "CREATE (:AppendOnlyPair {ok:true})"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal("certify actual Graph application command", err)
	}
	if err := mat.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, 1); err != nil {
		t.Fatal("archive certified Graph cut", err)
	}
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if cut != 1 || len(files) != 2 || files[0].Role != materializer.CheckpointSQLite || files[1].Role != materializer.CheckpointGraphData {
		t.Fatalf("captured snapshot is not the actual fixed-role pair: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := manager.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Files) != 2 || root.Files[0].Role != checkpoint.RoleSQLite || root.Files[1].Role != checkpoint.RoleGraphData {
		t.Fatalf("immutable manifest lost the fixed pair: %+v", root.Files)
	}
	prefix, ok := core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual Graph candidate prefix missing")
	}
	next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal("direct Graph QCKP proposal with object-backed pair validator", err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal("explicitly archive actual certified Graph QCKP before selection", err)
	}
	selectedRoot, selected, err := n.selectManagedAppendOnlyRoot(ctx)
	if err != nil || selected == nil || selectedRoot == nil || selected.Index != seal.Index || selected.RootHash != seal.RootHash {
		t.Fatalf("actual paired root not selected: root=%+v seal=%+v err=%v", selectedRoot, selected, err)
	}
	if err := n.promoteManagedAppendOnlySeal(ctx, *selected); err != nil {
		t.Fatal("publish actual paired certified root", err)
	}
	locator, exists, err := manager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || locator.Index != uint64(cut) || locator.RootHash != root.RootHash {
		t.Fatalf("Graph CURRENT does not point to exact paired root: %+v exists=%t err=%v", locator, exists, err)
	}
}
