package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type managedTransitionTransport struct {
	mu               sync.RWMutex
	cores            map[quepaxa.NodeID]*quepaxa.Core
	wals             map[quepaxa.NodeID]*qlog.WAL
	dropTerminalFrom quepaxa.Slot
	dropTerminalTo   quepaxa.NodeID
	exhaustFrom      quepaxa.Slot
	exhaustTo        quepaxa.NodeID
	fetches          int
	records          int
}

func (t *managedTransitionTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	t.mu.Lock()
	t.records++
	t.mu.Unlock()
	return t.core(to).Record(ctx, request)
}

func (t *managedTransitionTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	for id, core := range t.cores {
		if id == t.dropTerminalTo && decision.Slot == t.dropTerminalFrom {
			continue
		}
		if id == t.exhaustTo && decision.Slot == t.exhaustFrom {
			wal := t.wals[id]
			if wal == nil {
				return errors.New("terminal capacity fixture has no WAL")
			}
			if err := wal.SetMaxBytes(wal.Capacity().Used); err != nil {
				return err
			}
			if err := core.AcceptDecision(decision); !errors.Is(err, qlog.ErrCapacity) {
				return err
			}
			continue
		}
		if err := core.AcceptDecision(decision); err != nil {
			return err
		}
	}
	return nil
}

func (t *managedTransitionTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	return t.core(to).Tip(), nil
}

func (t *managedTransitionTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	return t.core(to).StageValue(hash, value)
}

func (t *managedTransitionTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	value, ok := t.core(from).Value(hash)
	if !ok {
		return nil, errors.New("value unavailable")
	}
	return value, nil
}

func (t *managedTransitionTransport) FetchCertifiedDecisions(_ context.Context, from quepaxa.NodeID, slot quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	core := t.core(from)
	if core == nil {
		return nil, 0, errors.New("certified recovery source unavailable")
	}
	t.mu.Lock()
	t.fetches++
	t.mu.Unlock()
	return core.DecisionsFrom(slot, limit)
}

func (t *managedTransitionTransport) FetchFrozenTargetDecisions(ctx context.Context, from quepaxa.NodeID, slot quepaxa.Slot, limit int, target quepaxa.Cluster) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	if _, retained := target.MemberSet()[from]; !retained {
		return nil, 0, errors.New("frozen target source not retained")
	}
	return t.FetchCertifiedDecisions(ctx, from, slot, limit)
}

func (t *managedTransitionTransport) core(id quepaxa.NodeID) *quepaxa.Core {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cores[id]
}

func TestManagedTransitionWaitInstallsCertifiedTerminalBeforeAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members := []quepaxa.Member{{ID: "a"}, {ID: "b"}}
	transport := &managedTransitionTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	a, b := transport.cores["a"], transport.cores["b"]
	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{members[0], members[1], quepaxa.Member{ID: "c", WALIdentity: strings.Repeat("1", 64)}}}
	freeze, err := b.BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	transport.dropTerminalTo, transport.dropTerminalFrom = a.NodeID(), freeze+16
	if err := b.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, pending := a.PendingReconfiguration(); !pending {
		t.Fatal("retained voter unexpectedly installed the terminal")
	}

	transport.mu.RLock()
	recordsBefore := transport.records
	transport.mu.RUnlock()
	if err := (&Node{core: a}).waitForManagedVoterAdmission(context.Background()); err != nil {
		pending, _ := a.PendingReconfiguration()
		page, _, fetchErr := transport.FetchFrozenTargetDecisions(ctx, b.NodeID(), a.Tip()+1, 128, pending)
		acceptErr := a.AcceptCertifiedValues(page)
		t.Fatalf("wait=%v a_tip=%d b_tip=%d page=%d fetch=%v accept=%v", err, a.Tip(), b.Tip(), len(page), fetchErr, acceptErr)
	}
	if pending, _ := a.PendingReconfiguration(); pending.ConfigID != 0 || a.CurrentCluster().ConfigID != 2 {
		t.Fatalf("transition remained active or target was not installed: pending=%+v current=%+v", pending, a.CurrentCluster())
	}
	transport.mu.RLock()
	fetches := transport.fetches
	recordsAfter := transport.records
	transport.mu.RUnlock()
	if fetches == 0 {
		t.Fatal("retained voter did not fetch the certified terminal")
	}
	if recordsAfter != recordsBefore {
		t.Fatalf("admission pre-gate submitted %d records before ReadIndex", recordsAfter-recordsBefore)
	}
}

func TestManagedLocalReadVoterWaitsForRetainedTerminalDurability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	protocol := sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	clusterID := types.ClusterID("managed-local-read-transition")
	members := make([]quepaxa.Member, 0, 3)
	for i, id := range []quepaxa.NodeID{"a", "b", "c"} {
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString([]byte(strings.Repeat(fmt.Sprintf("%d", i), 32)))
		members = append(members, member)
	}
	transport := &managedTransitionTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}, wals: map[quepaxa.NodeID]*qlog.WAL{}}
	var local *Node
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		config := &types.ExecutionConfig{
			NodeID: member.ID, ClusterID: clusterID, Members: members, DataDir: t.TempDir(),
			ObjStorePrefix: "managed-local-read-transition", ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local",
			EnableReconfiguration: true,
		}
		n := New(config)
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{
			ApplicationProtocol: protocol, EvidenceFormat: protocol,
			Member: recovery.PreparedStartupIdentity{ClusterUID: string(clusterID), NodeID: string(member.ID)},
		}}
		source, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		n.managedStartup.Identity.PhysicalBinding = source
		n.managedStartup.Identity.BootstrapPlanDigest = planDigest
		n.managedStartup.Identity.Member.SourceBinding = source
		n.managedStartup.Identity.Member.ConfigDigest = memberDigest
		var walIdentity [32]byte
		copy(walIdentity[:], identity)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ValidateValue: types.ValidateManagedKVExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
				Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, WALIdentity: walIdentity},
				Verify:  n.verifyManagedAppendOnlyCandidate,
			},
			ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		n.core, n.wal = core, wal
		transport.cores[member.ID], transport.wals[member.ID] = core, wal
		if member.ID == "a" {
			local = n
		}
	}
	n := local
	if n == nil {
		t.Fatal("local Node fixture missing")
	}
	mat, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	n.material = mat
	n.opened.Store(true)
	n.managedPeerVerified.Store(true)
	if err := n.installInternalApplicationBinding(ctx); err != nil {
		t.Fatal(err)
	}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, network.ManagedPeerProfile{
		ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	n.managedInitiallyAdmitted.Store(true)
	a, b := transport.cores["a"], transport.cores["b"]
	target := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	freeze, err := b.BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	transport.exhaustTo, transport.exhaustFrom = a.NodeID(), freeze+16
	if err := b.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, ok := a.PendingReconfiguration(); ok || a.CurrentCluster().ConfigID != 2 {
		t.Fatalf("terminal was not installed: pending=%+v ok=%t current=%+v", pending, ok, a.CurrentCluster())
	}
	if a.IsVoter() {
		t.Fatal("fixture unexpectedly retained local terminal durability")
	}
	transport.mu.RLock()
	recordsBefore := transport.records
	transport.mu.RUnlock()
	used := transport.wals[a.NodeID()].Capacity().Used
	if err := transport.wals[a.NodeID()].SetMaxBytes(used + 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		t.Fatalf("CheckManagedLocalRead=%v", err)
	}
	if !a.IsVoter() {
		t.Fatal("local-read voter gate returned without establishing voter eligibility")
	}
	response, err := n.QueryManagedSQL(ctx, network.QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"})
	if err != nil {
		t.Fatalf("linearizable QueryManagedSQL=%v", err)
	}
	if len(response.Rows) != 1 || len(response.Rows[0]) != 1 || fmt.Sprint(response.Rows[0][0]) != "1" {
		t.Fatalf("linearizable QueryManagedSQL response=%+v", response)
	}
	transport.mu.RLock()
	recordsAfter := transport.records
	transport.mu.RUnlock()
	if recordsAfter != recordsBefore {
		t.Fatalf("local-read voter gate proposed %d records", recordsAfter-recordsBefore)
	}
}
