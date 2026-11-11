package node

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type managedTransitionTransport struct {
	mu               sync.RWMutex
	cores            map[quepaxa.NodeID]*quepaxa.Core
	dropTerminalFrom quepaxa.Slot
	dropTerminalTo   quepaxa.NodeID
	fetches          int
}

func (t *managedTransitionTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return t.core(to).Record(ctx, request)
}

func (t *managedTransitionTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	for id, core := range t.cores {
		if id == t.dropTerminalTo && decision.Slot == t.dropTerminalFrom {
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

	if err := (&Node{core: a}).WaitForManagedMembershipTransition(ctx); err != nil {
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
	transport.mu.RUnlock()
	if fetches == 0 {
		t.Fatal("retained voter did not fetch the certified terminal")
	}
}
