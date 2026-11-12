package node

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type replicaProbeTransport struct {
	mu    sync.Mutex
	calls []quepaxa.NodeID
	fail  quepaxa.NodeID
}

func (t *replicaProbeTransport) ReadTip(_ context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	t.mu.Lock()
	t.calls = append(t.calls, id)
	t.mu.Unlock()
	if id == t.fail {
		return 0, errors.New("unavailable")
	}
	return 7, nil
}

func (t *replicaProbeTransport) sortedCalls() []quepaxa.NodeID {
	t.mu.Lock()
	defer t.mu.Unlock()
	calls := slices.Clone(t.calls)
	slices.Sort(calls)
	return calls
}

func (*replicaProbeTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, errors.New("unused")
}

func (*replicaProbeTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return errors.New("unused")
}

func (*replicaProbeTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return errors.New("unused")
}

func (*replicaProbeTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("unused")
}

func TestProbeManagedReplicaVotersRequiresEveryOtherVoter(t *testing.T) {
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	transport := &replicaProbeTransport{}
	if err := probeManagedReplicaVoters(context.Background(), transport, cluster, "a", ""); err != nil {
		t.Fatal(err)
	}
	if want := []quepaxa.NodeID{"b", "c"}; !reflect.DeepEqual(transport.sortedCalls(), want) {
		t.Fatalf("probed %v, want %v", transport.sortedCalls(), want)
	}

	transport = &replicaProbeTransport{fail: "b"}
	if err := probeManagedReplicaVoters(context.Background(), transport, cluster, "a", ""); err == nil {
		t.Fatal("one unavailable voter was accepted as a complete-health proof")
	}
	if want := []quepaxa.NodeID{"b", "c"}; !reflect.DeepEqual(transport.sortedCalls(), want) {
		t.Fatalf("failure probed %v, want %v", transport.sortedCalls(), want)
	}
}

func TestProbeManagedReplicaSurvivorsExcludesOnlyTheActualRemovedVoter(t *testing.T) {
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}}
	transport := &replicaProbeTransport{}
	if err := probeManagedReplicaVoters(context.Background(), transport, cluster, "a", "d"); err != nil {
		t.Fatal(err)
	}
	if want := []quepaxa.NodeID{"b", "c"}; !reflect.DeepEqual(transport.sortedCalls(), want) {
		t.Fatalf("probed %v, want %v", transport.sortedCalls(), want)
	}
	if err := probeManagedReplicaVoters(context.Background(), &replicaProbeTransport{}, cluster, "a", "outside"); err == nil {
		t.Fatal("removal outside the certified source roster was accepted")
	}
}

func TestProbeManagedReplicaVotersRejectsAmbiguousRoster(t *testing.T) {
	for _, cluster := range []quepaxa.Cluster{
		{},
		{Members: []quepaxa.Member{{ID: "a"}, {ID: "a"}}},
		{Members: []quepaxa.Member{{ID: "b"}}},
	} {
		if err := probeManagedReplicaVoters(context.Background(), &replicaProbeTransport{}, cluster, "a", ""); err == nil {
			t.Fatalf("ambiguous roster was probed: %+v", cluster)
		}
	}
}
