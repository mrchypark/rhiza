package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
	"github.com/thanos-io/objstore"
)

// mixedProviderCheckpoint owns only an already stopped member's files. It uses
// the existing publisher claim, authenticated peer preparation, certified
// proposal, durable archive and CURRENT promotion paths. TrimThrough publishes
// a certified recovery base; it does not delete immutable objects or enable GC.
func mixedProviderCheckpoint(t *testing.T, ctx context.Context, cfg types.ExecutionConfig, cluster quepaxa.Cluster, bucket objstore.Bucket, prefix string) {
	t.Helper()
	_, lock, err := qlog.Acquire(filepath.Join(cfg.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	wal, err := qlog.Open(filepath.Join(cfg.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport, err := network.NewTransportForPolicy(cfg.ClusterID, cfg.NodeID, &cluster, cfg.PeerToken, sqlpolicy.ManagedSQLKVMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: cfg.NodeID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	transport.BindCore(core)
	manager, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, prefix, cfg.DataDir, sqlpolicy.ManagedSQLKVMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	material, err := materializer.OpenManagedSQLKV(filepath.Join(cfg.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	files, index, cleanup, err := material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	claim, err := manager.AcquirePublisherClaim(ctx, string(cfg.NodeID), 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Error(err)
		}
	}()
	if index < claim.ReservedIndex {
		t.Fatal("snapshot is behind publisher reservation")
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := manager.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = manager.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefixHash, ok := core.PrefixHash(quepaxa.Slot(index))
	if !ok {
		t.Fatal("snapshot prefix is not certified")
	}
	order, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(index))
	if err != nil {
		t.Fatal(err)
	}
	history, err := core.CheckpointMembership(quepaxa.Slot(index))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: core.ConfigID(), Index: quepaxa.Slot(index), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefixHash, NextLeaderOrder: order, FollowingLeaderOrder: following, Membership: &history, GenerationAnchorHash: core.GenerationAnchorHash()}
	if err := manager.ValidatePublisherClaim(ctx, string(cfg.NodeID), index, root.RootHash); err != nil {
		t.Fatal(err)
	}
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	if err := transport.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decisionSlot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	for slot := quepaxa.Slot(material.Tip()) + 1; slot <= decisionSlot; slot++ {
		decision, ok := core.CertifiedValue(slot)
		if !ok {
			t.Fatal("checkpoint proposal lacks certified history", slot)
		}
		if err := material.Apply(ctx, uint64(slot), decision.Value); err != nil {
			t.Fatal(err)
		}
	}
	archive, err := recovery.NewAppendOnlyManager(bucket, prefix, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, decisionSlot); err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatal("missing certified checkpoint seal", err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("missing checkpoint certificate")
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
}

func mixedProviderPeer(t *testing.T, ctx context.Context, cluster types.ClusterID, id types.NodeID) (context.Context, string) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	transport := &quic.Transport{Conn: conn}
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	endpoint := conn.LocalAddr().String()
	return peertest.WithTransport(ctx, string(cluster), string(id), endpoint, transport), endpoint
}

type mixedGenesisWriteCounter struct {
	base   http.RoundTripper
	writes atomic.Int64
}

// The checker reads the actual status API on every call. It is an authority
// fixture, not the production Host or Kubernetes owner-chain qualification.
type mixedProviderNodeChecker struct {
	n        *node.Node
	store    *operator.V1Alpha2StatusStore
	ref      operator.V1Alpha2ClusterRef
	plan     operator.V1Alpha2ApplicationInitPlan
	identity node.ManagedStartupIdentity
	active   atomic.Pointer[operator.V1Alpha2WriteRequest]
}

func mixedProviderEvidence(state node.ManagedKVSnapshot) json.RawMessage {
	raw, _ := json.Marshal(struct {
		Format string
		Digest string
	}{types.ManagedApplicationProtocolSQLKV, fmt.Sprintf("%x", state.Application.Digest)})
	return raw
}

func (c *mixedProviderNodeChecker) ValidateManagedStartup(ctx context.Context, identity node.ManagedStartupIdentity) error {
	if identity != c.identity || identity.ApplicationProtocol != types.ManagedApplicationProtocolSQLKV || identity.Member.Generation != 0 {
		return errors.New("provider exact identity changed")
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, c.store, c.ref)
	if err != nil {
		return err
	}
	a := initial.Authority()
	if initial.Assignment().Plan != c.plan || a.Owner.UID != c.ref.UID || a.Generation != 0 || a.Binding.DeploymentUID != c.plan.DeploymentUID || a.Binding.StorageID != c.plan.StorageID {
		return errors.New("provider live assignment or authority changed")
	}
	return nil
}
func (c *mixedProviderNodeChecker) ValidateManagedAdmission(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState) error {
	if err := c.ValidateManagedStartup(ctx, i); err != nil {
		return err
	}
	if runtime.WALIdentity == "" || runtime.AppliedTip != runtime.CertifiedTip || runtime.RecorderTip > runtime.CertifiedTip {
		return errors.New("provider unresolved runtime")
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, c.store, c.ref)
	if err != nil {
		return err
	}
	if pending, exists := a.Pending(); exists {
		active := c.active.Load()
		if active == nil || *active != pending {
			return errors.New("provider another active reservation")
		}
		return nil
	}
	state, err := c.n.InspectManagedKV(ctx, false)
	if err != nil {
		return err
	}
	if !bytes.Equal(a.Evidence(), mixedProviderEvidence(state)) {
		return errors.New("provider canonical evidence mismatch")
	}
	return nil
}
func (c *mixedProviderNodeChecker) ValidateManagedPeerLearn(ctx context.Context, i node.ManagedStartupIdentity) error {
	return c.ValidateManagedStartup(ctx, i)
}
func (c *mixedProviderNodeChecker) ValidateManagedSQLRead(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState) error {
	if err := c.ValidateManagedStartup(ctx, i); err != nil {
		return err
	}
	_, err := c.n.InspectManagedKVRead(ctx)
	return err
}
func (c *mixedProviderNodeChecker) exactKV(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	if err := c.ValidateManagedStartup(ctx, i); err != nil {
		return err
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, c.store, c.ref)
	if err != nil {
		return err
	}
	want, found, err := a.PendingKV()
	if err != nil || !found {
		return errors.Join(errors.New("provider exact pending KV missing"), err)
	}
	got := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS}
	wire, _, err := types.EncodeManagedKVOriginal(got)
	if err != nil {
		return err
	}
	original, _, err := types.EncodeManagedKVOriginal(want)
	if err != nil || !bytes.Equal(wire, original) {
		return errors.New("provider pending original differs")
	}
	return nil
}
func (c *mixedProviderNodeChecker) ValidateManagedKVWrite(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState, operation string, req network.KVMutationRequest) error {
	if c.active.Load() == nil {
		return errors.New("provider no directly applied reservation")
	}
	if err := c.ValidateManagedAdmission(ctx, i, runtime); err != nil {
		return err
	}
	return c.exactKV(ctx, i, operation, req)
}
func (c *mixedProviderNodeChecker) ValidateManagedKVRecovery(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	return c.exactKV(ctx, i, operation, req)
}
func (c *mixedProviderNodeChecker) ValidateManagedSQLWrite(ctx context.Context, i node.ManagedStartupIdentity, runtime node.ManagedRuntimeState, req network.ExecuteRequest) error {
	if err := c.ValidateManagedAdmission(ctx, i, runtime); err != nil {
		return err
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, c.store, c.ref)
	if err != nil {
		return err
	}
	want, found, err := a.PendingSQL()
	if err != nil || !found || c.active.Load() == nil {
		return errors.New("provider exact pending SQL missing")
	}
	wf, we := types.SQLFingerprint(types.SQLCommand{RequestID: want.RequestID, SQL: want.SQL, Args: want.Args})
	gf, ge := types.SQLFingerprint(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args})
	if we != nil || ge != nil || want.RequestID != req.RequestID || wf != gf {
		return errors.New("provider pending SQL differs")
	}
	return nil
}

func TestEmbeddedObjectStorageManagedKVNode(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Skip("requires all-three-provider CI with an explicit isolated Floci endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	base := embeddedStoreConfig(t)
	base.Local, base.EnableReconfiguration = false, true
	base.AdminToken = "mixed-provider-admin"
	base.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	base.CheckpointInterval = 24 * time.Hour
	base.CheckpointTailBytes = 1 << 40
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "managed-provider-fixture"}
	cluster, err := types.ManagedClusterID(ref.UID, 0)
	if err != nil {
		t.Fatal(err)
	}
	base.ClusterID = string(cluster)
	const root = "mixed-node-provider-root"
	profile := operator.V1Alpha2RuntimeProfile{ApplicationProtocol: types.ManagedApplicationProtocolSQLKV, EvidenceFormat: operator.V1Alpha2ManagedSQLKVEvidenceFormat, InitImage: "fixture:v1", PeerPort: 9090, ProfileSecretName: "runtime", Storage: managedProviderRuntime(base)}
	profile.Storage.ObjStorePrefix = root
	entries := make([]operator.V1Alpha2BootstrapMember, 3)
	contexts := make([]context.Context, 3)
	tokens := []string{"mixed-one", "mixed-two", "mixed-three"}
	for i := range entries {
		pod := fmt.Sprintf("mixed-provider-pod-%d", i)
		id, err := types.ManagedNodeID(pod)
		if err != nil {
			t.Fatal(err)
		}
		var endpoint string
		contexts[i], endpoint = mixedProviderPeer(t, ctx, cluster, id)
		entries[i] = operator.V1Alpha2BootstrapMember{PodUID: pod, Member: quepaxa.Member{ID: id, PeerURL: "quic://" + endpoint}}
		copy(entries[i].Member.PublicKey[:], network.PeerPublicKey(cluster, id, tokens[i]))
	}
	plan, bootstrap, err := operator.BuildV1Alpha2RuntimePlan(ref, "managed-provider-logical", "managed-provider-deployment", profile, entries)
	if err != nil {
		t.Fatal(err)
	}
	store, deployment := managedProviderStatusStore(t, ref, types.ManagedApplicationProtocolSQLKV)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, deployment, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	ns, err := node.ManagedNamespaceFor(root, ref.UID, 0, plan.AssignmentID, string(cluster))
	if err != nil {
		t.Fatal(err)
	}
	base.ObjStorePrefix = ns.ConfigPrefix
	base.Members = make([]quepaxa.Member, 3)
	for i := range entries {
		base.Members[i] = entries[i].Member
	}
	runtimes := make([]types.ExecutionConfig, 3)
	nodes := make([]*node.Node, 3)
	checkers := make([]*mixedProviderNodeChecker, 3)
	defer func() {
		for _, n := range nodes {
			if n != nil {
				if err := n.Shutdown(); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	for i := range entries {
		runtimes[i] = managedProviderRuntime(base)
		runtimes[i].NodeID, runtimes[i].PeerToken = entries[i].Member.ID, tokens[i]
		runtimes[i].PeerAddr, runtimes[i].BindAddr = strings.TrimPrefix(entries[i].Member.PeerURL, "quic://"), ""
		runtimes[i].DataDir = filepath.Join(t.TempDir(), "source")
		nodes[i] = node.New(&runtimes[i])
		physical, member, err := nodes[i].PreparedStartupBindingDigestsForProtocol(plan.ApplicationProtocol)
		if err != nil {
			t.Fatal(err)
		}
		identity := node.ManagedStartupIdentity{ApplicationProtocol: plan.ApplicationProtocol, EvidenceFormat: plan.EvidenceFormat, AssignmentID: plan.AssignmentID, IntentHash: plan.IntentHash, StorageID: plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: plan.BootstrapPlanDigest, Fresh: true, Member: recovery.PreparedStartupIdentity{ClusterUID: ref.UID, DeploymentUID: plan.DeploymentUID, PodUID: entries[i].PodUID, NodeID: string(runtimes[i].NodeID), SourceBinding: physical, ConfigDigest: member}}
		checkers[i] = &mixedProviderNodeChecker{n: nodes[i], store: store, ref: ref, plan: plan, identity: identity}
		if _, err := nodes[i].PrepareManagedSQLSource(ctx, identity, true); err != nil {
			t.Fatal(err)
		}
		if err := nodes[i].OpenManaged(contexts[i], node.ManagedStartupConfig{StorageRoot: root, PeerRecovery: true, Identity: identity, Checker: checkers[i], BootstrapMember: true}); err != nil {
			t.Fatalf("public mixed OpenManaged provider=%s: %v", provider, err)
		}
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	state, err := nodes[0].InspectManagedKV(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, observation, err := operator.CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, mixedProviderEvidence(state)); err != nil || observation != operator.V1Alpha2CASApplied {
		t.Fatal("E0 direct CAS", observation, err)
	}
	waitAdmission := func(n *node.Node) {
		t.Helper()
		for {
			if err := n.AdmitManaged(ctx); err == nil {
				return
			}
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal("mixed admission deadline", ctx.Err())
			case <-timer.C:
			}
		}
	}
	for _, n := range nodes {
		waitAdmission(n)
	}
	finalize := func(c *mixedProviderNodeChecker, reserved operator.V1Alpha2AdmissionSnapshot) {
		t.Helper()
		state, err := c.n.InspectManagedKV(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		request, ok := reserved.Pending()
		if !ok {
			t.Fatal("lost actual reservation")
		}
		if _, observation, err := operator.FinalizeV1Alpha2Write(ctx, store, reserved, reserved.Authority(), request, operator.V1Alpha2WriteCommitted, mixedProviderEvidence(state)); err != nil || observation != operator.V1Alpha2CASApplied {
			t.Fatal("finalize direct CAS", observation, err)
		}
		c.active.Store(nil)
	}
	mutate := func(index int, operation string, req network.KVMutationRequest) network.KVMutationResponse {
		t.Helper()
		c := checkers[index]
		waitAdmission(c.n)
		old, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
		if err != nil {
			t.Fatal(err)
		}
		reserved, observation, err := operator.ReserveV1Alpha2KVWrite(ctx, store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS})
		if err != nil || observation != operator.V1Alpha2CASApplied {
			t.Fatal("reserve direct CAS", observation, err)
		}
		pending, ok := reserved.Pending()
		if !ok {
			t.Fatal("missing directly reserved KV")
		}
		c.active.Store(&pending)
		result, err := c.n.ExecuteManagedKV(ctx, operation, req)
		if err != nil {
			t.Fatal("real reserved KV", err)
		}
		resolved, err := c.n.ResolveManagedKV(ctx, operation, req)
		if err != nil || resolved.Receipt == nil || resolved.Durability != network.ResolveKVDurabilityConfirmed || resolved.Receipt.Slot != result.Slot {
			t.Fatal("real original durability", resolved, err)
		}
		finalize(c, reserved)
		return result
	}
	// SQL and KV share one real original/pending lane in the authority fixture.
	sqlRequest := network.ExecuteRequest{RequestID: "mixed-sql", SQL: "CREATE TABLE mixed_node_rows(v INTEGER)"}
	a, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	reserved, observation, err := operator.ReserveV1Alpha2SQLWrite(ctx, store, a, a.Authority(), a.Evidence(), sqlRequest)
	if err != nil || observation != operator.V1Alpha2CASApplied {
		t.Fatal(err, observation)
	}
	pendingSQL, ok := reserved.Pending()
	if !ok {
		t.Fatal("missing directly reserved SQL")
	}
	checkers[0].active.Store(&pendingSQL)
	if _, err := nodes[0].ExecuteManagedSQL(ctx, sqlRequest); err != nil {
		t.Fatal(err)
	}
	finalize(checkers[0], reserved)
	id, key := string([]byte{0xff}), string([]byte{0xfe})
	ack := mutate(0, "put", network.KVMutationRequest{RequestID: id, Key: key, Value: []byte("ack-kept")})
	falseACK := mutate(0, "cas", network.KVMutationRequest{RequestID: "false-cas", Key: "absent", ExpectedExists: true, Expected: []byte{}, Value: nil})
	if falseACK.Applied || falseACK.Status != types.MutationCommitted {
		t.Fatal("CAS false not committed", falseACK)
	}
	for _, n := range nodes {
		waitAdmission(n)
	}
	if _, err := nodes[0].API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw API exposed", err)
	}
	if _, err := nodes[0].Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw Handler exposed", err)
	}
	if err := nodes[0].Shutdown(); err != nil {
		t.Fatal(err)
	}
	// Same-Pod restart, Prepared nil, original registered disk and exact identity.
	nodes[0] = node.New(&runtimes[0])
	checkers[0].n = nodes[0]
	checkers[0].identity.Fresh = false
	if err := nodes[0].OpenManaged(contexts[0], node.ManagedStartupConfig{StorageRoot: root, PeerRecovery: true, Identity: checkers[0].identity, Checker: checkers[0], BootstrapMember: true}); err != nil {
		t.Fatal("same-Pod public reopen", err)
	}
	waitAdmission(nodes[0])
	status, err := nodes[0].RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: id})
	if err != nil || status.Receipt == nil || status.Receipt.Slot != ack.Slot {
		t.Fatal("same-Pod prior ACK", status, err)
	}
	read, err := nodes[0].KVGetManaged(ctx, network.KVGetRequest{Key: key, Consistency: "local"})
	if err != nil || !read.Found || string(read.Value) != "ack-kept" {
		t.Fatal("same-Pod data", read, err)
	}
	bucket := embeddedBucket(t, base)
	readonly := &readonlyProviderBucket{Bucket: bucket}
	trusted := quepaxa.Cluster{ConfigID: 1, Members: base.Members}
	prefix := path.Join(base.ObjStorePrefix, string(cluster))
	if err := nodes[0].Shutdown(); err != nil {
		t.Fatal(err)
	}
	nodes[0] = nil
	// Managed Nodes intentionally do not start automatic checkpoint/GC work.
	// Reuse this stopped member's registered WAL identity for a one-shot,
	// authenticated checkpoint proposal while the other real voters serve it.
	mixedProviderCheckpoint(t, ctx, runtimes[0], trusted, bucket, prefix)
	view, err := recovery.OpenReadOnlyViewForPolicy(ctx, readonly, prefix, 1, trusted, 8, types.DefaultIdempotencyWindowSlots)
	if err != nil || view.Base() == 0 {
		t.Fatal("certified mixed checkpoint base", err)
	}
	baseTip := view.Base()
	suffixACK := mutate(1, "put", network.KVMutationRequest{RequestID: "suffix", Key: "suffix-key", Value: []byte("after-checkpoint")})
	canonical, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	for {
		view, err = recovery.OpenReadOnlyViewForPolicy(ctx, readonly, prefix, 1, trusted, 8, types.DefaultIdempotencyWindowSlots)
		if err == nil && view.Base() == baseTip && uint64(view.Tip()) >= suffixACK.Slot {
			break
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("mixed suffix absent", err)
		case <-timer.C:
		}
	}
	for i, n := range nodes {
		if n != nil {
			if err := n.Shutdown(); err != nil {
				t.Fatal(err)
			}
			nodes[i] = nil
		}
	}
	if readonly.writes != 0 {
		t.Fatal("view mutated source", readonly.writes)
	}
	replacement := runtimes[1]
	pod := "mixed-replacement"
	replacement.NodeID, err = types.ManagedNodeID(pod)
	if err != nil {
		t.Fatal(err)
	}
	replacementContext, endpoint := mixedProviderPeer(t, ctx, cluster, replacement.NodeID)
	replacement.PeerToken, replacement.PeerAddr, replacement.DataDir = "replacement-token", endpoint, filepath.Join(t.TempDir(), "replacement")
	replacement.Learner = &quepaxa.Member{ID: replacement.NodeID}
	copy(replacement.Learner.PublicKey[:], network.PeerPublicKey(cluster, replacement.NodeID, replacement.PeerToken))
	n := node.New(&replacement)
	physical, member, err := n.PreparedStartupBindingDigestsForProtocol(plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	i := node.ManagedStartupIdentity{ApplicationProtocol: plan.ApplicationProtocol, EvidenceFormat: plan.EvidenceFormat, AssignmentID: plan.AssignmentID, IntentHash: plan.IntentHash, StorageID: plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: plan.BootstrapPlanDigest, Member: recovery.PreparedStartupIdentity{ClusterUID: ref.UID, DeploymentUID: plan.DeploymentUID, PodUID: pod, NodeID: string(replacement.NodeID), SourceBinding: physical, ConfigDigest: member}}
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoff, i.Member, 8, types.DefaultIdempotencyWindowSlots); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartupForPolicy(ctx, readonly, prefix, 1, trusted, handoff, replacement.DataDir, i.Member, 8, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	checker := &mixedProviderNodeChecker{n: n, store: store, ref: ref, plan: plan, identity: i}
	if err := n.OpenManaged(replacementContext, node.ManagedStartupConfig{StorageRoot: root, PeerRecovery: true, Identity: i, Checker: checker, Prepared: prepared}); err != nil {
		_ = n.Shutdown()
		t.Fatal("prepared public OpenManaged", err)
	}
	state, err = n.InspectManagedKV(ctx, false)
	if err != nil || state.Runtime.AppliedTip != uint64(view.Tip()) || !bytes.Equal(mixedProviderEvidence(state), canonical.Evidence()) {
		_ = n.Shutdown()
		t.Fatal("prepared mixed evidence", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	// Directly read the data/receipts restored by the public Node path after it
	// has closed. This does not make the unpromoted learner application-ready.
	restored, err := materializer.OpenManagedSQLKV(filepath.Join(replacement.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for _, expected := range []struct {
		id, key, value string
		slot           uint64
	}{{id, key, "ack-kept", ack.Slot}, {"suffix", "suffix-key", "after-checkpoint", suffixACK.Slot}} {
		value, found, err := restored.KVGet(ctx, expected.key, time.Now())
		if err != nil || !found || string(value) != expected.value {
			t.Fatal("restored prior ACK data", err)
		}
		receipt, found, err := restored.MutationReceipt(ctx, types.MutationKV, expected.id)
		if err != nil || !found || receipt.Slot != expected.slot || receipt.Status != types.MutationCommitted {
			t.Fatal("restored original receipt", receipt, err)
		}
	}
	falseReceipt, found, err := restored.MutationReceipt(ctx, types.MutationKV, "false-cas")
	if err != nil || !found || falseReceipt.Applied || falseReceipt.Slot != falseACK.Slot {
		t.Fatal("restored false CAS ACK", falseReceipt, err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	// Same replacement Pod adopts its recorded original, without another restore.
	n = node.New(&replacement)
	checker.n = n
	if err := n.OpenManaged(replacementContext, node.ManagedStartupConfig{StorageRoot: root, PeerRecovery: true, Identity: i, Checker: checker}); err != nil {
		_ = n.Shutdown()
		t.Fatal("prepared same-Pod public reopen", err)
	}
	after, err := n.InspectManagedKV(ctx, false)
	if err != nil || after.Application != state.Application {
		_ = n.Shutdown()
		t.Fatal("prepared restart evidence changed", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if readonly.writes != 0 {
		t.Fatal("two-pass reconstruction mutated source")
	}
	t.Logf("provider=%s actual public mixed Node startup/same-Pod/prepared data adoption; exact live status fixture, not Host/Kubernetes/learner promotion qualification", provider)
}

func (c *mixedGenesisWriteCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		c.writes.Add(1)
	}
	return c.base.RoundTrip(r)
}

// This is the real status initializer with a bounded TLS authority fixture.
// It does not contact a provider or establish Kubernetes qualification.
func TestManagedKVNodeProviderGenesisBinding(t *testing.T) {
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		t.Run(protocol, func(t *testing.T) {
			ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "managed-provider-fixture"}
			format := operator.V1Alpha2ManagedSQLEvidenceFormat
			var options []string
			if protocol != "" {
				format = operator.V1Alpha2ManagedSQLKVEvidenceFormat
				options = []string{protocol}
			}
			profile := operator.V1Alpha2RuntimeProfile{ApplicationProtocol: protocol, EvidenceFormat: format, InitImage: "fixture:v1", PeerPort: 9090, ProfileSecretName: "runtime", Storage: types.ExecutionConfig{AdminToken: "fixture-admin", ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreEndpoint: "127.0.0.1:1", ObjStoreInsecure: true, EnableReconfiguration: true}}
			members := make([]operator.V1Alpha2BootstrapMember, 3)
			cluster, err := types.ManagedClusterID(ref.UID, 0)
			if err != nil {
				t.Fatal(err)
			}
			for i := range members {
				pod := fmt.Sprintf("pod-%d", i)
				id, err := types.ManagedNodeID(pod)
				if err != nil {
					t.Fatal(err)
				}
				member := network.PeerIdentity{ID: id, PeerURL: fmt.Sprintf("quic://127.0.0.1:%d", 19000+i)}
				copy(member.PublicKey[:], network.PeerPublicKey(cluster, id, "fixture-token"))
				members[i] = operator.V1Alpha2BootstrapMember{PodUID: pod}
				members[i].Member.ID, members[i].Member.PeerURL, members[i].Member.PublicKey = id, member.PeerURL, member.PublicKey
			}
			assignment, bootstrap, err := operator.BuildV1Alpha2RuntimePlan(ref, "managed-provider-logical", "managed-provider-deployment", profile, members)
			if err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"", "digest", "protocol"} {
				t.Run(fault, func(t *testing.T) {
					store, deployment := managedProviderStatusStore(t, ref, options...)
					counter := &mixedGenesisWriteCounter{base: store.Kube.Client.Transport}
					client := *store.Kube.Client
					client.Transport = counter
					store.Kube.Client = &client
					request := assignment
					if fault == "digest" {
						request.BootstrapPlanDigest = strings.Repeat("1", 64)
					}
					if fault == "protocol" {
						if protocol == "" {
							request.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
						} else {
							request.ApplicationProtocol = ""
						}
					}
					_, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(context.Background(), store, ref, deployment, request, bootstrap)
					if fault != "" {
						if err == nil || counter.writes.Load() != 0 {
							t.Fatal("invalid plan wrote authority", err, counter.writes.Load())
						}
						return
					}
					if err != nil || counter.writes.Load() != 1 {
						t.Fatalf("actual profile-bound genesis initializer: %v writes=%d", err, counter.writes.Load())
					}
					loaded, err := operator.LoadV1Alpha2ApplicationInit(context.Background(), store, ref)
					if err != nil || loaded.Assignment().Plan != assignment {
						t.Fatal("initialized plan changed", err)
					}
				})
			}
		})
	}
}
