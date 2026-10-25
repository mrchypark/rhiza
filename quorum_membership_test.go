package rhiza

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

// Exercise the public Core and real authenticated QUIC path together. This
// deliberately does not use Node's archive or claim no-PVC automatic recovery.
func TestQuorumMembershipQUIC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	const admin = "membership-test-admin"
	const clusterID = "membership-test"
	members := make([]quepaxa.Member, 4)
	tokens := make([]string, 4)
	reserved := make([]*quic.Transport, len(members))
	t.Cleanup(func() {
		for _, listener := range reserved {
			if listener != nil {
				_ = listener.Close()
				_ = listener.Conn.Close()
			}
		}
	})
	for i := range members {
		listener, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id, token := fmt.Sprintf("v%d", i), fmt.Sprintf("membership-test-voter-%d", i)
		tokens[i] = token
		members[i] = quepaxa.Member{ID: quepaxa.NodeID(id), PeerURL: "quic://" + listener.LocalAddr().String(), PublicKey: quepaxa.PublicKey(network.PeerPublicKey(clusterID, quepaxa.NodeID(id), token))}
		reserved[i] = &quic.Transport{Conn: listener}
	}
	genesis := quepaxa.Cluster{ConfigID: 1, Members: members[:3]}
	type peer struct {
		core      *quepaxa.Core
		transport *network.Transport
		server    *network.Server
		listener  *network.PeerServer
		material  *materializer.Materializer
	}
	peers := make([]*peer, 4)
	var learnerMu sync.Mutex
	learnerTransportFor := func(current quepaxa.Cluster, localID quepaxa.NodeID) (*network.Transport, error) {
		identities := make([]network.PeerIdentity, 0, len(current.Members))
		for _, member := range current.Members {
			identity, err := network.NewPeerIdentity(clusterID, member)
			if err != nil {
				return nil, err
			}
			identities = append(identities, identity)
		}
		return network.NewLearnerTransport(clusterID, localID, current.ConfigID, identities, admin), nil
	}
	syncLearner := func(through quepaxa.Slot) error {
		learner := peers[3]
		current := peers[0].core.CurrentCluster()
		// This direct Core harness supplies the current, authenticated voter
		// endpoints to its read-only learner client. The learner Core still
		// validates every returned certificate from the immutable genesis
		// history; production ReadReplica discovers later configurations only
		// after replaying certified object-store history.
		transport, err := learnerTransportFor(current, learner.core.NodeID())
		if err != nil {
			return err
		}
		defer transport.Close()
		for learner.core.Tip() < through {
			batch, err := transport.FetchDecisions(ctx, members[0].ID, learner.core.Tip()+1, 128)
			if err != nil {
				return err
			}
			if len(batch.Decisions) == 0 {
				return fmt.Errorf("learner prefix has a gap")
			}
			if err := learner.core.AcceptCertifiedValues(batch.Decisions); err != nil {
				return err
			}
			if err := learner.material.ApplyBatch(ctx, batch.Decisions); err != nil {
				return err
			}
		}
		return nil
	}
	start := func(serveCtx context.Context, index int, learner bool) {
		dir := t.TempDir()
		wal, err := qlog.Open(filepath.Join(dir, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		var transport *network.Transport
		if learner {
			// An unpromoted learner holds no voting identity and syncs with the
			// cluster admin token, as pkg/node does.
			transport, err = learnerTransportFor(peers[0].core.CurrentCluster(), members[index].ID)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			transport = network.NewTransport(clusterID, members[index].ID, &genesis, tokens[index])
		}
		t.Cleanup(func() { _ = transport.Close() })
		config := quepaxa.Config{NodeID: members[index].ID, Cluster: genesis, WAL: wal, Transport: transport, EnableReconfiguration: true}
		config.ReconfigurationAdmission = func(callCtx context.Context, admission quepaxa.ReconfigurationAdmission) error {
			learnerMu.Lock()
			defer learnerMu.Unlock()
			if peers[3] == nil {
				return fmt.Errorf("learner is absent")
			}
			if err := syncLearner(admission.Through); err != nil {
				return err
			}
			return transport.VerifyLearner(callCtx, members[3], admission.Through, admission.PrefixHash)
		}
		var core *quepaxa.Core
		if learner {
			core, err = quepaxa.NewLearner(config)
		} else {
			core, err = quepaxa.New(config)
		}
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		if learner {
			members[index].WALIdentity = core.WALIdentity()
		}
		material, err := materializer.Open(filepath.Join(dir, "db.sqlite"), 2)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		server := network.NewServer(core, material, "membership-test", true, transport)
		t.Cleanup(server.Close)
		// Keep the UDP port bound across startup and the fresh-WAL restart.
		// A learner needs its own TLS identity, as in StartLearnerPeerServer.
		identities := genesis.Members
		if learner {
			identities = append(append([]quepaxa.Member(nil), identities...), members[index])
		}
		listener, err := network.StartPeerServerOnTransport(serveCtx, reserved[index], server, identities, tokens[index], admin)
		if err != nil {
			t.Fatal(err)
		}
		p := &peer{core: core, transport: transport, material: material, server: server, listener: listener}
		peers[index] = p
		t.Cleanup(func() {
			if p.listener != nil {
				_ = p.listener.Close()
			}
		})
	}
	for i := 0; i < 3; i++ {
		start(ctx, i, false)
	}
	execute := func(index int, id, sql string) {
		t.Helper()
		if _, err := peers[index].server.Execute(ctx, network.ExecuteRequest{RequestID: id, SQL: sql}); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	execute(0, "create", "CREATE TABLE recovered (id INTEGER PRIMARY KEY)")
	execute(0, "before", "INSERT INTO recovered VALUES (1)")
	checkpointBucketDir := t.TempDir()
	bootstrapMembers := append([]quepaxa.Member(nil), members[:3]...)
	replicaMembers := make([]network.PeerIdentity, len(bootstrapMembers))
	for i, member := range bootstrapMembers {
		identity, err := network.NewPeerIdentity(clusterID, member)
		if err != nil {
			t.Fatal(err)
		}
		replicaMembers[i] = identity
	}
	// Keep the learner lifetime independent from the setup context so the final
	// isolated-current-peer stage can use its own bounded Sync context.
	checkpointLearner, err := OpenLearnerWithHistory(context.Background(), ReplicaConfig{
		ClusterID: clusterID, ReplicaID: "checkpoint-reader", DataDir: t.TempDir(), AdminToken: admin,
		Members: replicaMembers, SyncInterval: time.Hour,
		ObjStoreProvider: string(objstore.ProviderFilesystem), ObjStoreDir: checkpointBucketDir,
	}, ReplicaHistoryVerificationConfig{BootstrapMembers: bootstrapMembers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = checkpointLearner.Close() })
	if status := checkpointLearner.Status(); status.AppliedSlot == 0 {
		t.Fatalf("history-aware learner was not running on the pre-transition source: %+v", status)
	}
	if floor := checkpointLearner.core.CompactionFloor(); floor != 0 {
		t.Fatalf("fresh history-aware learner unexpectedly began with checkpoint floor %d", floor)
	}
	for i := 1; i <= 64; i++ {
		var nonce [quepaxa.ReadBarrierNonceSize]byte
		nonce[0] = byte(i)
		if _, _, err := peers[0].core.ProposeCertified(ctx, quepaxa.EncodeReadBarrier(nonce)); err != nil {
			t.Fatalf("certified pipeline stalled at write %d: %v", i, err)
		}
	}
	if err := peers[2].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[2].listener = nil
	survivors := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	if _, err := peers[0].core.BeginReconfiguration(ctx, survivors); err != nil {
		t.Fatal(err)
	}
	localtesthooks.Hit(fmt.Sprintf("test:membership:finish-start:round=remove-v2:remaining=%s", time.Until(deadline)))
	if err := peers[0].core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	current := peers[0].core.CurrentCluster()
	historical := network.NewTransport(clusterID, members[0].ID, &current, tokens[0])
	t.Cleanup(func() { _ = historical.Close() })
	if page, err := historical.FetchDecisions(ctx, members[0].ID, 1, 1); err != nil || len(page.Decisions) != 1 {
		t.Fatalf("active voter historical sync: %v", err)
	}
	removedVoterTransport := network.NewTransport(clusterID, members[2].ID, &current, tokens[2])
	t.Cleanup(func() { _ = removedVoterTransport.Close() })
	if _, err := removedVoterTransport.FetchDecisions(ctx, members[0].ID, 1, 1); err == nil {
		t.Fatal("removed voter retained historical sync authority")
	}
	_ = removedVoterTransport.Close()
	execute(0, "degraded", "INSERT INTO recovered VALUES (2)")
	start(ctx, 3, true)
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := peers[3].core.Propose(ctx, []byte("INSERT INTO recovered VALUES (99)")); err == nil {
		t.Fatal("unpromoted learner proposed a write")
	}
	if err := peers[0].transport.VerifyLearner(ctx, members[3], peers[0].core.Tip(), quepaxa.ValueHash{1}); err == nil {
		t.Fatal("learner accepted an incorrect prefix")
	}
	prefix, ok := peers[0].core.PrefixHash(peers[0].core.Tip())
	if !ok {
		t.Fatal("current prefix is unavailable")
	}
	wrongIncarnation := members[3]
	first := byte('0')
	if wrongIncarnation.WALIdentity[0] == first {
		first = '1'
	}
	wrongIncarnation.WALIdentity = string(first) + wrongIncarnation.WALIdentity[1:]
	if err := peers[0].transport.VerifyLearner(ctx, wrongIncarnation, peers[0].core.Tip(), prefix); err == nil {
		t.Fatal("learner accepted an incorrect WAL identity")
	}
	replacement := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{members[0], members[1], members[3]}}
	if _, err := peers[0].core.BeginReconfiguration(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	localtesthooks.Hit(fmt.Sprintf("test:membership:finish-start:round=promote-v3:remaining=%s", time.Until(deadline)))
	if err := peers[0].core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := peers[1].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[1].listener = nil
	execute(0, "replacement", "INSERT INTO recovered VALUES (3)")
	storageBucket, err := objstore.NewBucket(objstore.Config{Provider: objstore.ProviderFilesystem, FilesystemDir: checkpointBucketDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageBucket.Close() })
	checkpointManager := checkpoint.NewManager(storageBucket, clusterID, t.TempDir(), 1)
	if err := checkpointManager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	archive := recovery.NewManager(storageBucket, clusterID, 1)
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	// Publish the actual certified membership transitions before the running
	// learner catches up. Its first pass may use object-store replay to learn a
	// configuration the old peer request could not yet name; the next pass
	// must then succeed from the peer using the verifier's advanced config.
	if err := archive.SyncThrough(ctx, peers[0].core, peers[0].core.Tip()); err != nil {
		t.Fatalf("publish certified membership history for learner catch-up: %v", err)
	}
	if err := checkpointLearner.Sync(ctx); err != nil {
		t.Fatalf("running learner replay of certified membership history: %v", err)
	}
	if got, want := checkpointLearner.core.CurrentCluster().ConfigID, peers[0].core.CurrentCluster().ConfigID; got != want {
		t.Fatalf("running learner did not validate the current membership history: config=%d want=%d", got, want)
	}
	if status := checkpointLearner.Status(); status.Source != "object-store" {
		t.Fatalf("first catch-up should record its certified archive source, got %q", status.Source)
	}
	if err := checkpointLearner.Sync(ctx); err != nil {
		t.Fatalf("running learner peer sync after certified history replay: %v", err)
	}
	status := checkpointLearner.Status()
	currentLearnerMembership := checkpointLearner.core.CurrentCluster()
	selectedCurrentPeer := false
	for _, member := range currentLearnerMembership.Members {
		if status.Source == "peer:"+string(member.ID) && status.SourceTip == uint64(peers[0].core.Tip()) {
			selectedCurrentPeer = true
			break
		}
	}
	if !selectedCurrentPeer {
		t.Fatalf("learner did not resume peer sync from the verified current membership at the source tip: status=%+v membership=%+v sourceTip=%d",
			status, currentLearnerMembership, peers[0].core.Tip())
	}
	files, checkpointIndex, cleanupFiles, err := peers[0].material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupFiles()
	if checkpointIndex == 0 || quepaxa.Slot(checkpointIndex) > peers[0].core.Tip() {
		t.Fatalf("invalid checkpoint index %d at source tip %d", checkpointIndex, peers[0].core.Tip())
	}
	claim, err := checkpointManager.AcquirePublisherClaim(ctx, "membership-test", checkpointIndex-1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpointManager.CreateFiles(ctx, claim, []checkpoint.Source{
		{Role: checkpoint.RoleSQLite, Path: files[0].Path},
		{Role: checkpoint.RoleGraphData, Path: files[1].Path},
	}, checkpointIndex)
	if err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	prefix, ok = peers[0].core.PrefixHash(quepaxa.Slot(checkpointIndex))
	if !ok {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal("checkpoint prefix is unavailable")
	}
	next, following, err := peers[0].core.CheckpointLeaderOrders(quepaxa.Slot(checkpointIndex))
	if err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: peers[0].core.ConfigIDForSlot(quepaxa.Slot(checkpointIndex)), Index: quepaxa.Slot(checkpointIndex),
		RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: peers[0].core.GenerationAnchorHash(),
	}
	membership, err := peers[0].core.CheckpointMembership(quepaxa.Slot(checkpointIndex))
	if err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	seal.Membership = &membership
	if seal.ConfigID <= 1 {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("checkpoint did not capture post-reconfiguration consensus ConfigID: %d", seal.ConfigID)
	}
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("catch up current voter before preparing its checkpoint vote: %v", err)
	}
	peers[0].core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return checkpointManager.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	peers[3].core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return checkpointManager.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if err := peers[0].core.PrepareCheckpoint(ctx, seal); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("prepare certified checkpoint root on proposal source: %v", err)
	}
	if err := peers[0].transport.PrepareCheckpoint(ctx, seal); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("prepare exact certified checkpoint on the current voter quorum: %v", err)
	}
	if _, _, err := peers[0].core.Propose(ctx, sealValue); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	sealDecision, ok := peers[0].core.CertifiedValue(peers[0].core.Tip())
	if !ok {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal("certified checkpoint seal decision is missing")
	}
	if err := peers[0].material.ApplyBatch(ctx, []quepaxa.DecidedValue{sealDecision}); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, peers[0].core, peers[0].core.Tip()); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	sealed, ok, err := peers[0].core.LatestCheckpointSeal()
	if err != nil || !ok {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("certified checkpoint seal is missing: %v", err)
	}
	if err := checkpointManager.PromoteCertifiedCurrent(ctx, root); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, sealed, sealDecision); err != nil {
		_ = checkpointManager.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if err := checkpointManager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	storageRoot, err := checkpointManager.OpenRoot(ctx, checkpointIndex, root.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	if storageRoot.ConfigID != 1 || sealed.ConfigID != seal.ConfigID {
		t.Fatalf("storage/consensus config IDs were conflated: storage=%d seal=%d", storageRoot.ConfigID, sealed.ConfigID)
	}
	checkpointLearner.syncMu.Lock()
	checkpointLearner.nextCheckpointProbe = time.Now()
	checkpointLearner.syncMu.Unlock()
	assertCurrentPeerSync := func(stage string) {
		t.Helper()
		status := checkpointLearner.Status()
		current := checkpointLearner.core.CurrentCluster()
		for _, member := range current.Members {
			if status.Source == "peer:"+string(member.ID) && status.SourceTip == uint64(peers[0].core.Tip()) {
				return
			}
		}
		t.Fatalf("%s did not sync from a verified current peer at the exact source tip: status=%+v current=%+v sourceTip=%d",
			stage, status, current, peers[0].core.Tip())
	}
	adoptionDeadline := time.Now().Add(5 * time.Second)
	if deadline.Before(adoptionDeadline) {
		adoptionDeadline = deadline
	}
	adoptionSyncs := 0
	for checkpointLearner.core.CompactionFloor() < quepaxa.Slot(checkpointIndex) && time.Now().Before(adoptionDeadline) {
		if err := checkpointLearner.Sync(ctx); err != nil {
			t.Fatalf("running history-aware learner sync: %v (status=%+v)", err, checkpointLearner.Status())
		}
		adoptionSyncs++
		assertCurrentPeerSync(fmt.Sprintf("checkpoint adoption sync %d", adoptionSyncs))
		time.Sleep(time.Millisecond)
	}
	if adoptionSyncs == 0 {
		t.Fatal("checkpoint adoption completed without observing a peer-sourced sync iteration")
	}
	if floor := checkpointLearner.core.CompactionFloor(); floor != quepaxa.Slot(checkpointIndex) {
		t.Fatalf("running learner did not finish adopting the certified post-reconfiguration checkpoint: floor=%d want=%d status=%+v sealConfigID=%d",
			floor, checkpointIndex, checkpointLearner.Status(), sealed.ConfigID)
	}
	coldLearner, err := OpenLearnerWithHistory(ctx, ReplicaConfig{
		ClusterID: clusterID, ReplicaID: "cold-checkpoint-reader", DataDir: t.TempDir(), AdminToken: admin,
		Members: replicaMembers, SyncInterval: time.Hour,
		ObjStoreProvider: string(objstore.ProviderFilesystem), ObjStoreDir: checkpointBucketDir,
	}, ReplicaHistoryVerificationConfig{BootstrapMembers: bootstrapMembers})
	if err != nil {
		t.Fatalf("open cold learner from certified reconfiguration history: %v", err)
	}
	t.Cleanup(func() { _ = coldLearner.Close() })
	if got, want := coldLearner.core.CurrentCluster().ConfigID, peers[0].core.CurrentCluster().ConfigID; got != want {
		t.Fatalf("cold learner did not replay certified current membership before peer sync: config=%d want=%d", got, want)
	}
	if got := coldLearner.core.ConfigIDForSlot(1); got != 1 {
		t.Fatalf("cold learner changed the certified genesis configuration: slot1 config=%d", got)
	}
	coldRows, err := coldLearner.Query(ctx, QueryRequest{SQL: "SELECT id FROM recovered ORDER BY id"})
	if err != nil || len(coldRows.Rows) != 3 {
		t.Fatalf("cold learner checkpoint rows=%v err=%v", coldRows.Rows, err)
	}
	rows, err := checkpointLearner.Query(ctx, QueryRequest{SQL: "SELECT id FROM recovered ORDER BY id"})
	if err != nil || len(rows.Rows) != 3 {
		t.Fatalf("running learner data after certified checkpoint: rows=%v err=%v", rows.Rows, err)
	}
	for i, row := range rows.Rows {
		if len(row) != 1 || row[0] != int64(i+1) {
			t.Fatalf("running learner checkpoint payload at row %d: got=%v want=%d", i, row, i+1)
		}
	}
	result, err := peers[0].server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM recovered ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows=%v", result.Rows)
	}
	for i, row := range result.Rows {
		if len(row) != 1 || row[0] != int64(i+1) {
			t.Fatalf("source payload at row %d: got=%v want=%d", i, row, i+1)
		}
	}
	if peers[3].core.CurrentCluster().ConfigID != 3 {
		t.Fatal("learner did not learn promotion")
	}
	staleCtx, staleCancel := context.WithTimeout(ctx, time.Second)
	defer staleCancel()
	removedVoterAfterAdd := network.NewTransport(clusterID, members[2].ID, &replacement, tokens[2])
	t.Cleanup(func() { _ = removedVoterAfterAdd.Close() })
	if _, err := removedVoterAfterAdd.ReadTip(staleCtx, members[0].ID); err == nil {
		t.Fatal("removed voter retained quorum read authority")
	}
	if err := peers[3].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[3].listener = nil
	currentPeerCtx, currentPeerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer currentPeerCancel()
	start(currentPeerCtx, 3, true) // Same peer ID/token, but a freshly created WAL incarnation.
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if peers[3].core.IsVoter() {
		t.Fatal("fresh WAL reused the old incarnation's promotion")
	}
	probe := network.NewTransport(clusterID, members[0].ID, &replacement, tokens[0])
	t.Cleanup(func() { _ = probe.Close() })
	probe.BindCore(peers[0].core)
	if batch, err := probe.FetchDecisions(ctx, members[3].ID, 1, 1); err != nil || len(batch.Decisions) != 1 {
		t.Fatalf("fresh peer is not serving its restored history: %v", err)
	}
	value := []byte("stale-incarnation-vote")
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	_, err = probe.SendRecord(ctx, members[3].ID, quepaxa.RecordRequest{
		Slot: peers[0].core.Tip() + 1, Step: 4, ConfigID: 3,
		Proposal: quepaxa.Proposal{Priority: priority, ProposerID: members[0].ID, Hash: sha256.Sum256(value), Value: value},
	})
	if err == nil {
		t.Fatal("fresh WAL voted through QUIC using the old incarnation's promotion")
	}
	// Force the history-aware learner to select the only remaining current
	// voter over real QUIC. The earlier genesis peers are unavailable; this
	// independent bound avoids inheriting the long setup deadline.
	if err := peers[0].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[0].listener = nil
	currentMembers := checkpointLearner.core.CurrentCluster().Members
	reachedCurrentPeer := false
	for attempt := 0; attempt < len(currentMembers); attempt++ {
		cursorBefore := checkpointLearner.peerCursor
		attemptCtx, attemptCancel := context.WithTimeout(currentPeerCtx, time.Second)
		err := checkpointLearner.Sync(attemptCtx)
		attemptErr := attemptCtx.Err()
		attemptCancel()
		if err == nil {
			currentPeerStatus := checkpointLearner.Status()
			if currentPeerStatus.Source != "peer:"+string(members[3].ID) || currentPeerStatus.SourceTip != uint64(peers[3].core.Tip()) {
				t.Fatalf("learner did not use the sole current voter at its exact tip: status=%+v currentTip=%d",
					currentPeerStatus, peers[3].core.Tip())
			}
			reachedCurrentPeer = true
			break
		}
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, attemptErr) || checkpointLearner.peerCursor == cursorBefore {
			t.Fatalf("sync did not advance past an unavailable peer under its bounded caller deadline: err=%v cursor=%d→%d",
				err, cursorBefore, checkpointLearner.peerCursor)
		}
	}
	if !reachedCurrentPeer {
		t.Fatalf("learner did not reach the sole current voter within the bounded sync stage: status=%+v cursor=%d",
			checkpointLearner.Status(), checkpointLearner.peerCursor)
	}
}
