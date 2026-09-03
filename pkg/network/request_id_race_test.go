package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
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
	mu               sync.RWMutex
	cores            map[quepaxa.NodeID]*quepaxa.Core
	disabled         map[quepaxa.NodeID]bool
	failNextDecision bool
	benchmarkQuorum  bool

	gateMu sync.Mutex
	hashes map[quepaxa.ValueHash]struct{}
	gate   chan struct{}
}

func (t *requestIDRaceTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if t.gate != nil && request.Slot == 1 && request.Step == 4 {
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
	core, err := t.peer(to)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	return core.Record(ctx, request)
}

func (t *requestIDRaceTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	t.mu.Lock()
	if t.failNextDecision {
		t.failNextDecision = false
		t.mu.Unlock()
		return errors.New("injected learner failure")
	}
	cores := make([]*quepaxa.Core, 0, len(t.cores))
	proposer := decision.Proposal.ProposerID
	if core := t.cores[proposer]; core != nil && !t.disabled[proposer] {
		cores = append(cores, core)
	}
	for id, core := range t.cores {
		if id != proposer && !t.disabled[id] {
			cores = append(cores, core)
		}
	}
	quorum := len(t.cores)/2 + 1
	t.mu.Unlock()
	if len(cores) < quorum {
		return quepaxa.ErrQuorumUnavailable
	}
	if t.benchmarkQuorum {
		// ponytail: serialize the minimum quorum because these in-memory peers share one benchmark disk.
		successes := 1 // proposer is the local learner, matching Transport.SendDecision
		cores = cores[1:]
		for i, core := range cores {
			if decisionHasRecorder(decision, core.NodeID()) {
				cores[0], cores[i] = cores[i], cores[0]
				break
			}
		}
		for _, core := range cores {
			var err error
			if decisionHasRecorder(decision, core.NodeID()) {
				err = core.AcceptDecisionHint(decision)
			} else {
				err = core.AcceptDecision(decision)
			}
			if err == nil {
				successes++
				if successes >= quorum {
					return nil
				}
			}
		}
		return quepaxa.ErrQuorumUnavailable
	}
	results := make(chan error, len(cores))
	for _, core := range cores {
		go func() { results <- core.AcceptDecision(decision) }()
	}
	successes := 0
	for range cores {
		if err := <-results; err == nil {
			successes++
			if successes >= quorum {
				return nil
			}
		}
	}
	return quepaxa.ErrQuorumUnavailable
}

func TestBenchmarkDecisionQuorumPrefersDurableRecorder(t *testing.T) {
	cluster := newInMemoryThreePeerCluster(t, false)
	cluster.transport.benchmarkQuorum = true
	value := []byte("recorder-first")
	priority := quepaxa.Priority{}
	for i := range priority {
		priority[i] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "n1", Hash: sha256.Sum256(value), Value: value}
	decision := quepaxa.Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []quepaxa.Summary{
		{RecorderID: "n1", Step: 4, FirstCurrent: &proposal},
		{RecorderID: "n2", Step: 4, FirstCurrent: &proposal},
	}}
	if err := cluster.transport.SendDecision(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	if !cluster.cores["n2"].IsDecided(1) || cluster.cores["n3"].IsDecided(1) {
		t.Fatal("benchmark decision quorum did not use the recorder learner")
	}
}

func (t *requestIDRaceTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	core, err := t.peer(to)
	if err != nil {
		return 0, err
	}
	return core.Tip(), nil
}

func (t *requestIDRaceTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	core, err := t.peer(to)
	if err != nil {
		return err
	}
	return core.StageValue(hash, value)
}

func (t *requestIDRaceTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	core, err := t.peer(from)
	if err != nil {
		return nil, err
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("missing value")
	}
	return value, nil
}

func (t *requestIDRaceTransport) peer(id quepaxa.NodeID) (*quepaxa.Core, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.disabled[id] {
		return nil, errors.New("peer unavailable")
	}
	core := t.cores[id]
	if core == nil {
		return nil, errors.New("unknown peer")
	}
	return core, nil
}

func (t *requestIDRaceTransport) disable(ids ...quepaxa.NodeID) {
	t.mu.Lock()
	for _, id := range ids {
		t.disabled[id] = true
	}
	t.mu.Unlock()
}

type requestIDRaceCluster struct {
	servers   map[quepaxa.NodeID]*Server
	cores     map[quepaxa.NodeID]*quepaxa.Core
	material  map[quepaxa.NodeID]*materializer.Materializer
	transport *requestIDRaceTransport
}

func newRequestIDRaceCluster(t *testing.T) requestIDRaceCluster {
	return newInMemoryThreePeerCluster(t, true)
}

func newInMemoryThreePeerCluster(t testing.TB, gated bool) requestIDRaceCluster {
	t.Helper()
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := &requestIDRaceTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool), hashes: make(map[quepaxa.ValueHash]struct{})}
	if gated {
		transport.gate = make(chan struct{})
	}
	cluster := requestIDRaceCluster{servers: make(map[quepaxa.NodeID]*Server), cores: make(map[quepaxa.NodeID]*quepaxa.Core), material: make(map[quepaxa.NodeID]*materializer.Materializer), transport: transport}
	for _, member := range members {
		core := mustCore(t, member.ID, members, nil, transport)
		material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		server := NewServer(core, material, "cluster", true, nil)
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

func BenchmarkThreePeerSQLExecute(b *testing.B) {
	for _, parallelism := range []int{2, 8, 32} {
		b.Run("c"+strconv.Itoa(parallelism*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			cluster := newInMemoryThreePeerCluster(b, false)
			cluster.transport.benchmarkQuorum = true
			server := cluster.servers["n1"]
			if _, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE bench (id INTEGER PRIMARY KEY)"}); err != nil {
				b.Fatal(err)
			}
			before := cluster.cores["n1"].Tip()
			var sequence atomic.Uint64
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := sequence.Add(1)
					if _, err := server.Execute(context.Background(), ExecuteRequest{
						RequestID: strconv.FormatUint(id, 10), SQL: "INSERT INTO bench(id) VALUES (?)", Args: []any{int64(id)},
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			slots := cluster.cores["n1"].Tip() - before
			if slots != 0 {
				b.ReportMetric(float64(b.N)/float64(slots), "commands/slot")
			}
		})
	}
}

func BenchmarkCertifiedThreePeerSQLExecute(b *testing.B) {
	for _, parallelism := range []int{2, 8, 32} {
		b.Run("c"+strconv.Itoa(parallelism*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			cluster := newInMemoryThreePeerCluster(b, false)
			proposer := cluster.cores["n1"]
			ingress := cluster.cores["n2"]
			apply := cluster.servers["n2"].applyDecisions
			propose := func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
				slot, _, err := proposer.ProposeCertified(ctx, value)
				if err != nil {
					return slot, err
				}
				decision, ok := proposer.CertifiedValue(slot)
				if !ok {
					return slot, errors.New("certified decision unavailable")
				}
				if err := ingress.AcceptCertifiedValueForAck(decision); err != nil {
					return slot, err
				}
				if err := proposer.WaitTip(ctx, slot); err != nil {
					return slot, err
				}
				from := ingress.Tip() + 1
				if from <= slot {
					missing := make([]quepaxa.DecidedValue, 0, int(slot-from+1))
					for candidate := from; candidate <= slot; candidate++ {
						value, ok := proposer.CertifiedValue(candidate)
						if !ok {
							return slot, errors.New("catch-up decision unavailable")
						}
						missing = append(missing, value)
					}
					if err := ingress.AcceptCertifiedValues(missing); err != nil {
						return slot, err
					}
				}
				return slot, nil
			}
			batcher := newSQLBatcher(propose, apply)
			defer batcher.Close()
			if _, err := batcher.submit(context.Background(), types.SQLCommand{RequestID: "schema", SQL: "CREATE TABLE bench (id INTEGER PRIMARY KEY)"}); err != nil {
				b.Fatal(err)
			}
			before := ingress.Tip()
			var sequence atomic.Uint64
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := sequence.Add(1)
					if _, err := batcher.submit(context.Background(), types.SQLCommand{
						RequestID: strconv.FormatUint(id, 10), SQL: "INSERT INTO bench(id) VALUES (?)", Args: []any{int64(id)},
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			slots := ingress.Tip() - before
			if slots != 0 {
				b.ReportMetric(float64(b.N)/float64(slots), "commands/slot")
			}
		})
	}
}

func (c requestIDRaceCluster) applyAvailable(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tip := c.cores["n1"].Tip()
	for id, server := range c.servers {
		c.transport.mu.RLock()
		disabled := c.transport.disabled[id]
		c.transport.mu.RUnlock()
		if !disabled {
			if err := server.applyDecisions(ctx, tip); err != nil {
				t.Fatalf("apply %s through %d: %v", id, tip, err)
			}
		}
	}
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

func TestThreePeerSQLHAContract(t *testing.T) {
	t.Run("idempotency and conditional transaction", func(t *testing.T) {
		cluster := newInMemoryThreePeerCluster(t, false)
		ctx := context.Background()
		for _, request := range []ExecuteRequest{
			{RequestID: "schema-claim", SQL: "CREATE TABLE claims (id INTEGER PRIMARY KEY, marker TEXT NOT NULL, generation INTEGER NOT NULL)"},
			{RequestID: "schema-events", SQL: "CREATE TABLE claim_events (marker TEXT NOT NULL, generation INTEGER NOT NULL)"},
			{RequestID: "seed", SQL: "INSERT INTO claims VALUES (1, 'open', 0)"},
		} {
			if _, err := cluster.servers["n1"].Execute(ctx, request); err != nil {
				t.Fatal(err)
			}
		}
		cluster.applyAll(t)
		request := ExecuteRequest{RequestID: "claim", Statements: []types.SQLStatement{
			{SQL: "UPDATE claims SET marker = ?, generation = generation + 1 WHERE id = 1 AND generation = 0", Args: []any{"worker-a"}},
			{SQL: "INSERT INTO claim_events SELECT marker, generation FROM claims WHERE id = 1 AND marker = ?", Args: []any{"worker-a"}},
		}}
		first, err := cluster.servers["n1"].Execute(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := cluster.servers["n2"].Execute(ctx, request)
		if err != nil || !reflect.DeepEqual(first, replay) {
			t.Fatalf("replay=%#v first=%#v err=%v", replay, first, err)
		}
		conflict := request
		conflict.Statements = []types.SQLStatement{{SQL: "UPDATE claims SET marker = 'other' WHERE id = 1"}}
		if _, err := cluster.servers["n3"].Execute(ctx, conflict); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("conflicting request_id error=%v", err)
		}
		cluster.applyAll(t)
		result, err := cluster.servers["n2"].Query(ctx, QueryRequest{SQL: "SELECT c.marker, c.generation, COUNT(e.marker) FROM claims c LEFT JOIN claim_events e ON e.marker = c.marker AND e.generation = c.generation GROUP BY c.id", Consistency: "linearizable"})
		if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "worker-a" || result.Rows[0][1] != int64(1) || result.Rows[0][2] != int64(1) {
			t.Fatalf("claim rows=%#v err=%v", result.Rows, err)
		}
	})

	t.Run("quorum loss", func(t *testing.T) {
		cluster := newInMemoryThreePeerCluster(t, false)
		ctx := context.Background()
		if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE availability (id INTEGER PRIMARY KEY)"}); err != nil {
			t.Fatal(err)
		}
		cluster.applyAll(t)
		cluster.transport.disable("n3")
		if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "one-down", SQL: "INSERT INTO availability VALUES (1)"}); err != nil {
			t.Fatalf("one-peer loss write: %v", err)
		}
		cluster.applyAvailable(t)
		cluster.transport.disable("n2")
		if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "two-down", SQL: "INSERT INTO availability VALUES (2)"}); err == nil {
			t.Fatal("two-peer loss write succeeded")
		}
		if _, err := cluster.servers["n1"].Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM availability", Consistency: "linearizable"}); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
			t.Fatalf("linearizable read error=%v", err)
		}
		result, err := cluster.servers["n1"].Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM availability", Consistency: "local"})
		if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
			t.Fatalf("local rows=%#v err=%v", result.Rows, err)
		}
	})

	t.Run("commit unknown retry converges", func(t *testing.T) {
		cluster := newInMemoryThreePeerCluster(t, false)
		ctx := context.Background()
		if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE uncertain (id INTEGER PRIMARY KEY)"}); err != nil {
			t.Fatal(err)
		}
		cluster.applyAll(t)
		cluster.transport.mu.Lock()
		cluster.transport.failNextDecision = true
		cluster.transport.mu.Unlock()
		request := ExecuteRequest{RequestID: "uncertain", SQL: "INSERT INTO uncertain VALUES (1)"}
		if _, err := cluster.servers["n1"].Execute(ctx, request); !errors.Is(err, ErrCommitUnknown) {
			t.Fatalf("first error=%v, want commit unknown", err)
		}
		if _, err := cluster.servers["n2"].Execute(ctx, request); err != nil {
			t.Fatalf("retry: %v", err)
		}
		cluster.applyAll(t)
		result, err := cluster.servers["n3"].Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM uncertain", Consistency: "linearizable"})
		if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
			t.Fatalf("rows=%#v err=%v", result.Rows, err)
		}
	})
}
