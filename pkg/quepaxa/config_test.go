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
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{Members: members}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	members[0].ID = "changed"
	if core.NodeID() != "n1" || core.ProposerOrder()[0] != "n1" {
		t.Fatal("Core retained caller-owned membership")
	}
}

type testTransport struct{}

func (testTransport) SendRecord(_ context.Context, _ quepaxa.NodeID, _ quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, nil
}

func (testTransport) SendDecision(context.Context, quepaxa.Decision) error { return nil }
