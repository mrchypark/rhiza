//go:build !graph

package network

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// requestIDRaceTransport is an in-memory three-peer transport. It holds the
// first Record round until two distinct proposals have reached remote ingress,
// making the otherwise rare cross-node admission race deterministic.
type requestIDRaceTransport struct {
	mu    sync.RWMutex
	cores map[quepaxa.NodeID]*quepaxa.Core

	gateMu sync.Mutex
	hashes map[quepaxa.ValueHash]struct{}
	gate   chan struct{}
}

func (t *requestIDRaceTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
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

func (t *requestIDRaceTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
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

func (t *requestIDRaceTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return 0, errors.New("unknown peer")
	}
	return core.Tip(), nil
}

func (t *requestIDRaceTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return errors.New("unknown peer")
	}
	return core.StageValue(hash, value)
}

func (t *requestIDRaceTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
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

type requestIDRaceCluster struct {
	servers  map[quepaxa.NodeID]*Server
	cores    map[quepaxa.NodeID]*quepaxa.Core
	material map[quepaxa.NodeID]*materializer.Materializer
}

func newRequestIDRaceCluster(t *testing.T) requestIDRaceCluster {
	t.Helper()
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := &requestIDRaceTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), hashes: make(map[quepaxa.ValueHash]struct{}), gate: make(chan struct{})}
	cluster := requestIDRaceCluster{servers: make(map[quepaxa.NodeID]*Server), cores: make(map[quepaxa.NodeID]*quepaxa.Core), material: make(map[quepaxa.NodeID]*materializer.Materializer)}
	for _, member := range members {
		core := mustCore(t, member.ID, members, nil, transport)
		material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		server := NewServer(core, material, "cluster", true, nil, members, 0)
		cluster.cores[member.ID] = core
		cluster.material[member.ID] = material
		cluster.servers[member.ID] = server
		transport.mu.Lock()
		transport.cores[member.ID] = core
		transport.mu.Unlock()
		t.Cleanup(func() { server.Close(); material.Close() })
	}
	return cluster
}

func (c requestIDRaceCluster) applyAll(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tip quepaxa.Slot
	for _, core := range c.cores {
		if core.Tip() > tip {
			tip = core.Tip()
		}
	}
	for id, server := range c.servers {
		if err := server.applyDecisions(ctx, tip); err != nil {
			t.Fatalf("apply %s through %d: %v", id, tip, err)
		}
		if got := quepaxa.Slot(c.material[id].Tip()); got != tip {
			t.Fatalf("materializer %s tip=%d want %d", id, got, tip)
		}
	}
}

func assertRaceResults(t *testing.T, results []error) {
	t.Helper()
	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
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
}

func TestCrossIngressRequestIDConflictDoesNotBlockSQLKVOrNotify(t *testing.T) {
	t.Run("sql", func(t *testing.T) {
		cluster := newRequestIDRaceCluster(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := cluster.servers["n1"].Execute(context.Background(), ExecuteRequest{RequestID: "shared", SQL: "CREATE TABLE request_id_race_a (id INTEGER)"})
			results <- err
		}()
		go func() {
			<-start
			_, err := cluster.servers["n2"].Execute(context.Background(), ExecuteRequest{RequestID: "shared", SQL: "CREATE TABLE request_id_race_b (id INTEGER)"})
			results <- err
		}()
		close(start)
		assertRaceResults(t, []error{<-results, <-results})
		cluster.applyAll(t)
		if _, err := cluster.servers["n3"].Execute(context.Background(), ExecuteRequest{RequestID: "after", SQL: "CREATE TABLE request_id_race_after (id INTEGER)"}); err != nil {
			t.Fatalf("follow-up mutation: %v", err)
		}
		cluster.applyAll(t)
	})

	t.Run("kv", func(t *testing.T) {
		cluster := newRequestIDRaceCluster(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := cluster.servers["n1"].KVPut(context.Background(), KVMutationRequest{RequestID: "shared", Key: "race", Value: []byte("a")})
			results <- err
		}()
		go func() {
			<-start
			_, err := cluster.servers["n2"].KVPut(context.Background(), KVMutationRequest{RequestID: "shared", Key: "race", Value: []byte("b")})
			results <- err
		}()
		close(start)
		assertRaceResults(t, []error{<-results, <-results})
		cluster.applyAll(t)
		if _, err := cluster.servers["n3"].KVPut(context.Background(), KVMutationRequest{RequestID: "after", Key: "after", Value: []byte("ok")}); err != nil {
			t.Fatalf("follow-up mutation: %v", err)
		}
		cluster.applyAll(t)
	})

	t.Run("notify", func(t *testing.T) {
		cluster := newRequestIDRaceCluster(t)
		ch, cancel, err := cluster.servers["n3"].NotifySubscribe("race")
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := cluster.servers["n1"].NotifyPublish(context.Background(), types.NotifyCommand{RequestID: "shared", Topic: "race", Payload: []byte("a")})
			results <- err
		}()
		go func() {
			<-start
			_, err := cluster.servers["n2"].NotifyPublish(context.Background(), types.NotifyCommand{RequestID: "shared", Topic: "race", Payload: []byte("b")})
			results <- err
		}()
		close(start)
		assertRaceResults(t, []error{<-results, <-results})
		cluster.applyAll(t)
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("first notification was not published")
		}
		select {
		case duplicate := <-ch:
			t.Fatalf("conflicting notification published %q", duplicate)
		default:
		}
		if _, err := cluster.servers["n3"].NotifyPublish(context.Background(), types.NotifyCommand{RequestID: "after", Topic: "race", Payload: []byte("ok")}); err != nil {
			t.Fatalf("follow-up mutation: %v", err)
		}
		cluster.applyAll(t)
	})
}
