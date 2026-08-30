package network

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// graphRequestIDRaceTransport forces two independently admitted first-round
// proposals to meet before either can form a recorder quorum.
type graphRequestIDRaceTransport struct {
	mu    sync.RWMutex
	cores map[quepaxa.NodeID]*quepaxa.Core

	gateMu sync.Mutex
	hashes map[quepaxa.ValueHash]struct{}
	gate   chan struct{}
}

func (t *graphRequestIDRaceTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if request.Slot == 1 && request.Step == 4 {
		t.gateMu.Lock()
		t.hashes[request.Proposal.Hash] = struct{}{}
		if len(t.hashes) == 2 {
			select {
			case <-t.gate:
			default:
				close(t.gate)
			}
		}
		gate := t.gate
		t.gateMu.Unlock()
		select {
		case <-gate:
		case <-ctx.Done():
			return quepaxa.Summary{}, ctx.Err()
		}
	}
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return quepaxa.Summary{}, errors.New("unknown peer")
	}
	return core.Record(ctx, request)
}

func (t *graphRequestIDRaceTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	t.mu.RLock()
	cores := make([]*quepaxa.Core, 0, len(t.cores))
	for _, core := range t.cores {
		cores = append(cores, core)
	}
	t.mu.RUnlock()
	for _, core := range cores {
		if err := core.AcceptDecision(decision); err != nil {
			return err
		}
	}
	return nil
}

func (t *graphRequestIDRaceTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return 0, errors.New("unknown peer")
	}
	return core.Tip(), nil
}

func (t *graphRequestIDRaceTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return errors.New("unknown peer")
	}
	return core.StageValue(hash, value)
}

func (t *graphRequestIDRaceTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	t.mu.RLock()
	core := t.cores[from]
	t.mu.RUnlock()
	if core == nil {
		return nil, errors.New("unknown peer")
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("missing value")
	}
	return value, nil
}

func TestCrossIngressGraphRequestIDConflictDoesNotBlockApply(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := &graphRequestIDRaceTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), hashes: make(map[quepaxa.ValueHash]struct{}), gate: make(chan struct{})}
	cores := make(map[quepaxa.NodeID]*quepaxa.Core)
	materials := make(map[quepaxa.NodeID]*materializer.Materializer)
	servers := make(map[quepaxa.NodeID]*Server)
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: members}, WAL: wal, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		material, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
		if err != nil {
			t.Fatal(err)
		}
		server := NewServer(core, material, "cluster", true, nil, members, 0)
		cores[member.ID], materials[member.ID], servers[member.ID] = core, material, server
		transport.mu.Lock()
		transport.cores[member.ID] = core
		transport.mu.Unlock()
		t.Cleanup(func() { server.Close(); material.Close(); wal.Close() })
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for id, command := range map[quepaxa.NodeID]types.GraphCommand{
		"n1": {RequestID: "shared", Cypher: `CREATE (:Race {name: 'a'})`},
		"n2": {RequestID: "shared", Cypher: `CREATE (:Race {name: 'b'})`},
	} {
		go func(id quepaxa.NodeID, command types.GraphCommand) {
			<-start
			_, err := servers[id].GraphExecute(context.Background(), command)
			results <- err
		}(id, command)
	}
	close(start)
	successes, conflicts := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			successes++
		case errors.Is(err, ErrRequestConflict):
			conflicts++
		default:
			t.Fatalf("race result=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tip quepaxa.Slot
	for _, core := range cores {
		if core.Tip() > tip {
			tip = core.Tip()
		}
	}
	for id, server := range servers {
		if err := server.applyDecisions(ctx, tip); err != nil {
			t.Fatalf("apply %s through %d: %v", id, tip, err)
		}
		if got := quepaxa.Slot(materials[id].Tip()); got != tip {
			t.Fatalf("materializer %s tip=%d want %d", id, got, tip)
		}
	}
	if _, err := servers["n3"].GraphExecute(context.Background(), types.GraphCommand{RequestID: "after", Cypher: `CREATE (:Race {name: 'after'})`}); err != nil {
		t.Fatalf("follow-up mutation: %v", err)
	}
}
