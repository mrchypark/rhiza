package node

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestMembershipOperationJournalAndFence(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")

	remove := network.MembershipChange{
		OperationID: "remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := n.core.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 || got.Members[0].ID != "a" || got.Members[1].ID != "b" {
		t.Fatalf("cluster after removal: %+v", got)
	}
	if got := transport.cores["b"].CurrentCluster(); got.ConfigID != 2 {
		t.Fatalf("surviving peer configuration=%d, want 2", got.ConfigID)
	}
	if got := transport.cores["c"].CurrentCluster(); got.ConfigID != 1 {
		t.Fatalf("unavailable peer configuration=%d, want 1", got.ConfigID)
	}
	tip := n.core.Tip()
	terminal, ok := n.core.CertifiedValue(tip)
	if !ok {
		t.Fatalf("terminal slot %d is not certified", tip)
	}
	if control, err := quepaxa.DecodeReconfiguration(terminal.Value); err != nil || !control {
		t.Fatalf("terminal slot is not reconfiguration control: control=%t err=%v", control, err)
	}
	reloaded := recovery.NewManager(bucket, "cluster", 1)
	defer reloaded.Close()
	if err := reloaded.Load(ctx); err != nil {
		t.Fatalf("reload archive: %v", err)
	}
	if got := reloaded.Tip(); got != tip {
		t.Fatalf("reloaded archive tip=%d, want terminal %d", got, tip)
	}
	values, _, err := reloaded.DecisionsFrom(ctx, tip, 1)
	if err != nil || len(values) != 1 || values[0].Hash != terminal.Hash {
		t.Fatalf("reloaded terminal=%+v err=%v", values, err)
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := n.core.Tip(); got != tip {
		t.Fatalf("retry changed tip from %d to %d", tip, got)
	}

	conflict := remove
	conflict.OperationID = "remove-c-conflict"
	if err := n.changeMembership(ctx, conflict); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("conflicting operation error=%v, want request conflict", err)
	}

	wrongFence := network.MembershipChange{
		OperationID: "remove-b-wrong-fence", ClusterID: "cluster", ExpectedConfigID: 2, Remove: "b",
		Fence: &network.MembershipFence{NodeID: "b", WALIdentity: "wrong", WorkloadUID: "workload-b", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, wrongFence); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("wrong fence error=%v, want invalid request", err)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 2)

	malformed := network.MembershipChange{
		OperationID: "malformed-add", ClusterID: "cluster", ExpectedConfigID: 2,
		Add: &quepaxa.Member{Token: "candidate-token", WALIdentity: identities["c"], PeerURL: "https://candidate"},
	}
	if err := n.changeMembership(ctx, malformed); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("malformed add error=%v, want invalid request", err)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 2)

	retired := network.MembershipChange{
		OperationID: "retired-add", ClusterID: "cluster", ExpectedConfigID: 2,
		Add: &quepaxa.Member{ID: "c", Token: "c-token", WALIdentity: identities["c"], PeerURL: "https://c"},
	}
	if err := n.changeMembership(ctx, retired); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("retired add error=%v, want invalid request", err)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 2)
}

func TestMembershipOperationRejectsDifferentPendingTargetBeforeJournal(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")
	pending := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), n.core.CurrentCluster().Members[:2]...)}
	if _, err := n.core.BeginReconfiguration(ctx, pending); err != nil {
		t.Fatalf("freeze pending removal: %v", err)
	}
	request := network.MembershipChange{
		OperationID: "remove-b-during-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "b",
		Fence: &network.MembershipFence{NodeID: "b", WALIdentity: identities["b"], WorkloadUID: "workload-b", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, request); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("different pending target error=%v, want request conflict", err)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 1)
}

func TestMembershipAdditionAbortAllowsNextRevision(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, _ := newMembershipOperationNode(t)
	learner := registerMembershipLearner(t, bucket, "fresh")
	addition := network.MembershipChange{
		OperationID: "add-fresh", ClusterID: "cluster", ExpectedConfigID: 1, Add: &learner,
	}
	if err := n.changeMembership(ctx, addition); err == nil {
		t.Fatal("addition unexpectedly completed without an admission callback")
	}
	if pending, ok := n.core.PendingReconfiguration(); !ok || pending.ConfigID != 2 {
		t.Fatalf("failed addition did not retain its freeze: pending=%+v ok=%t", pending, ok)
	}
	if err := n.abortMembership(ctx, addition); err != nil {
		t.Fatalf("abort addition: %v", err)
	}
	abortSlot, _, aborted := n.core.LastReconfigurationAbort()
	if !aborted || abortSlot == 0 {
		t.Fatalf("missing abort revision: slot=%d aborted=%t", abortSlot, aborted)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, abortSlot)
	if err := n.abortMembership(ctx, addition); err != nil {
		t.Fatalf("abort retry: %v", err)
	}
	if err := n.changeMembership(ctx, addition); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("stale addition error=%v, want request conflict", err)
	}

	removal := network.MembershipChange{
		OperationID: "remove-c-after-abort", ClusterID: "cluster", ExpectedConfigID: 1, ExpectedAbortSlot: abortSlot, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, removal); err != nil {
		t.Fatalf("removal after abort: %v", err)
	}
	if got := n.core.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 {
		t.Fatalf("next revision cluster=%+v", got)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, n.core.Tip())
}

func TestMembershipAdmissionUsesDrainConfigurationAfterTerminal(t *testing.T) {
	ctx := context.Background()
	initial := []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}}
	target := quepaxa.Cluster{ConfigID: 2, Members: append(append([]quepaxa.Member(nil), initial...), quepaxa.Member{
		ID: "learner", Token: "learner-token", PeerURL: "https://127.0.0.1:1", WALIdentity: strings.Repeat("1", 64),
	})}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core)}
	var admitted atomic.Bool
	for _, member := range initial {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: initial}, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { admitted.Store(true); return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
		transport.ids = append(transport.ids, member.ID)
	}
	core := transport.cores["a"]
	if _, err := core.BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin addition: %v", err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish addition: %v", err)
	}
	if !admitted.Load() || core.CurrentCluster().ConfigID != target.ConfigID {
		t.Fatalf("addition did not activate through admission: admitted=%t cluster=%+v", admitted.Load(), core.CurrentCluster())
	}

	peer := network.NewTransport("cluster", "a", &quepaxa.Cluster{ConfigID: 1, Members: initial}, "a-token")
	defer peer.Close()
	n := &Node{core: core, transport: peer}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	through := core.Tip() - 1
	if err := n.verifyMembershipAdmission(canceled, target, through, quepaxa.ValueHash{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("admission after terminal error=%v, want canceled learner verification", err)
	}
}

func newMembershipOperationNode(t *testing.T) (*Node, map[quepaxa.NodeID]string, *objectstore.MeteredBucket, *membershipOperationTransport) {
	t.Helper()
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(bucket.SupportedObjectUploadOptions(), thanosobjstore.IfNotExists) {
		t.Fatal("filesystem bucket lacks atomic conditional writes")
	}
	members := []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}, {ID: "c", Token: "c-token"}}
	bootstrap := members[:2]
	identities := make(map[quepaxa.NodeID]string, len(members))
	wals := make(map[quepaxa.NodeID]*qlog.WAL, len(members))
	configs := make(map[quepaxa.NodeID]*types.ExecutionConfig, len(members))
	for _, member := range members {
		config := &types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: "cluster", NodeID: types.NodeID(member.ID), ObjStoreProvider: "filesystem", ObjStoreDir: "unused", Members: append([]quepaxa.Member(nil), bootstrap...)}
		if member.ID == "c" {
			// c's immutable registration was created while it was a valid learner
			// against the earlier a,b configuration, before it became a voter.
			config.Learner = &quepaxa.Member{ID: "c", Token: member.Token}
		}
		state, err := loadVoterIdentity(config)
		if err != nil {
			t.Fatal(err)
		}
		wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureVoterIdentity(context.Background(), config, bucket, wal, state, false); err != nil {
			t.Fatalf("register %s: %v", member.ID, err)
		}
		state, err = loadVoterIdentity(config)
		if err != nil || state.identity == nil {
			t.Fatalf("load registration for %s: %v", member.ID, err)
		}
		identities[member.ID], wals[member.ID], configs[member.ID] = state.identity.Nonce, wal, config
		t.Cleanup(func() { _ = wal.Close() })
	}
	for i := range members {
		members[i].WALIdentity = identities[members[i].ID]
	}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	for _, member := range members {
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wals[member.ID], Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
		transport.ids = append(transport.ids, member.ID)
	}
	config := *configs["a"]
	config.Members = append([]quepaxa.Member(nil), members...)
	config.EnableReconfiguration, config.AdminToken = true, "admin-token"
	archive := recovery.NewManager(bucket, "cluster", 1)
	if err := archive.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	return &Node{config: &config, core: transport.cores["a"], bucket: bucket, archive: archive}, identities, bucket, transport
}

func registerMembershipLearner(t *testing.T, bucket *objectstore.MeteredBucket, id quepaxa.NodeID) quepaxa.Member {
	t.Helper()
	config := &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: "cluster", NodeID: types.NodeID(id), ObjStoreProvider: "filesystem", ObjStoreDir: "unused",
		Members: []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}}, Learner: &quepaxa.Member{ID: id, Token: "fresh-token"},
	}
	state, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := ensureVoterIdentity(context.Background(), config, bucket, wal, state, false); err != nil {
		t.Fatalf("register learner: %v", err)
	}
	state, err = loadVoterIdentity(config)
	if err != nil || state.identity == nil {
		t.Fatalf("load learner registration: %v", err)
	}
	return quepaxa.Member{ID: id, Token: "fresh-token", PeerURL: "https://fresh", WALIdentity: state.identity.Nonce}
}

func assertDurableArchivedMembership(t *testing.T, ctx context.Context, n *Node, bucket *objectstore.MeteredBucket, through quepaxa.Slot) {
	t.Helper()
	if _, err := n.core.DurablePrefix(through); err != nil {
		t.Fatalf("durable through %d: %v", through, err)
	}
	reloaded := recovery.NewManager(bucket, "cluster", 1)
	defer reloaded.Close()
	if err := reloaded.Load(ctx); err != nil {
		t.Fatalf("reload archive: %v", err)
	}
	if got := reloaded.Tip(); got != through {
		t.Fatalf("archive tip=%d, want %d", got, through)
	}
}

func assertNoMembershipOperation(t *testing.T, ctx context.Context, n *Node, bucket *objectstore.MeteredBucket, configID uint) {
	t.Helper()
	data, err := readVoterRegistration(ctx, bucket, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "membership", fmt.Sprintf("%d.json", configID)))
	if err != nil || data != nil {
		t.Fatalf("membership/%d journal=%q err=%v", configID, data, err)
	}
}

type membershipOperationTransport struct {
	mu       sync.RWMutex
	cores    map[quepaxa.NodeID]*quepaxa.Core
	ids      []quepaxa.NodeID
	disabled map[quepaxa.NodeID]bool
}

func (t *membershipOperationTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	core, err := t.core(to)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	return core.Record(ctx, request)
}

func (t *membershipOperationTransport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	t.mu.RLock()
	cores := make([]*quepaxa.Core, 0, len(t.ids))
	for _, id := range t.ids {
		if !t.disabled[id] {
			cores = append(cores, t.cores[id])
		}
	}
	t.mu.RUnlock()
	for _, core := range cores {
		if err := core.AcceptDecision(decision); err != nil {
			return err
		}
	}
	return nil
}

func (t *membershipOperationTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	core, err := t.core(to)
	if err != nil {
		return 0, err
	}
	return core.Tip(), nil
}

func (t *membershipOperationTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	core, err := t.core(to)
	if err != nil {
		return err
	}
	return core.StageValue(hash, value)
}

func (t *membershipOperationTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	core, err := t.core(from)
	if err != nil {
		return nil, err
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("value unavailable")
	}
	return value, nil
}

func (t *membershipOperationTransport) core(id quepaxa.NodeID) (*quepaxa.Core, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	core := t.cores[id]
	if core == nil || t.disabled[id] {
		return nil, errors.New("peer unavailable")
	}
	return core, nil
}

func (t *membershipOperationTransport) disable(ids ...quepaxa.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		t.disabled[id] = true
	}
}
