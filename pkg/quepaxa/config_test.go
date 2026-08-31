package quepaxa_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNewValidatesAndCopiesMembership(t *testing.T) {
	if _, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}}}}); !errors.Is(err, quepaxa.ErrInvalidConfig) {
		t.Fatalf("nil WAL error = %v", err)
	}

	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	for name, config := range map[string]quepaxa.Config{
		"empty membership":  {NodeID: "n1", WAL: wal},
		"duplicate member":  {NodeID: "n1", Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}, {ID: "n1"}}}, WAL: wal, Transport: testTransport{}},
		"local missing":     {NodeID: "n1", Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n2"}}}, WAL: wal},
		"transport missing": {NodeID: "n1", Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}}}, WAL: wal},
	} {
		t.Run(name, func(t *testing.T) {
			core, err := quepaxa.New(config)
			if core != nil || !errors.Is(err, quepaxa.ErrInvalidConfig) {
				t.Fatalf("core=%v error=%v", core, err)
			}
		})
	}
	members := []quepaxa.Member{{ID: "n1"}}
	_ = quepaxa.Config{"n1", quepaxa.Cluster{}, wal, testTransport{}}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{Members: members}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	members[0].ID = "changed"
	if core.NodeID() != "n1" || core.ProposerOrder()[0] != "n1" {
		t.Fatal("Core retained caller-owned membership")
	}
}

func TestObserverIsOutsideMembershipAndCannotParticipate(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: "observer-1", WAL: wal,
		Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, members := range [][]quepaxa.Member{{{ID: "n1"}}, {{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}} {
		if memberCore, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "n1", WAL: wal, Cluster: quepaxa.Cluster{Members: members}}); memberCore != nil || !errors.Is(err, quepaxa.ErrInvalidConfig) {
			t.Fatalf("member observer core=%v error=%v", memberCore, err)
		}
	}
	if _, _, err := core.Propose(context.Background(), []byte("no")); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("observer proposal error=%v", err)
	}
	if _, err := core.Record(context.Background(), quepaxa.RecordRequest{Slot: 1, Step: 4}); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("observer record error=%v", err)
	}
	if _, _, err := core.ReadIndex(context.Background()); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("observer read-index error=%v", err)
	}
	if _, err := core.CompleteDecision(context.Background(), 1); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("observer decision dissemination error=%v", err)
	}
	if err := core.RecoverThrough(context.Background(), 1); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("observer recovery error=%v", err)
	}
}

type testTransport struct{}

func (testTransport) SendRecord(_ context.Context, _ quepaxa.NodeID, _ quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, nil
}

func (testTransport) SendDecision(context.Context, quepaxa.Decision) error          { return nil }
func (testTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) { return 0, nil }
func (testTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return nil
}
func (testTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("missing")
}
