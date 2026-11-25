package node

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSQLRetentionWorkerRecoversActualFullPool(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(fmt.Sprintf("managed=%t", managed), func(t *testing.T) {
			testSQLRetentionWorkerRecoversActualFullPool(t, managed)
		})
	}
}

func testSQLRetentionWorkerRecoversActualFullPool(t *testing.T, managed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.SetMaxBytes(math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "db.sqlite")
	var material *materializer.Materializer
	if managed {
		material, err = materializer.OpenManagedSQLKVWithAppendOnlyOriginals(path, 1, 1024)
	} else {
		material, err = materializer.Open(path, 1, 1024)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	var server *network.Server
	if managed {
		server, err = network.NewServerForPolicy(core, material, "cluster", true, nil, sqlpolicy.ManagedRequestMaterializationPolicy)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		server = network.NewServer(core, material, "cluster", true, nil)
	}
	t.Cleanup(server.Close)
	n := &Node{core: core, material: material, server: server}
	n.ready.Store(true)
	apply := func(commands []types.SQLCommand) {
		t.Helper()
		value, err := types.EncodeSQLBatch(commands)
		if err != nil {
			t.Fatal(err)
		}
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		if err := material.Apply(ctx, uint64(slot), value); err != nil {
			t.Fatal(err)
		}
	}
	const payloadBytes = materializer.MaxMutationResultBytes - 56
	apply([]types.SQLCommand{{RequestID: "setup", Statements: []types.SQLStatement{
		{SQL: "CREATE TABLE retained (count INTEGER NOT NULL, payload BLOB NOT NULL)"},
		{SQL: "INSERT INTO retained VALUES (0, zeroblob(?))", Args: []any{payloadBytes}},
	}}})
	command := func(id string) types.SQLCommand {
		return types.SQLCommand{RequestID: id, Statements: []types.SQLStatement{{SQL: "UPDATE retained SET count=count+1, payload=payload RETURNING payload", WantRows: true}}}
	}
	const totalResults = 256
	for start := 0; start < totalResults; start += types.MaxSQLCommandsPerDecidedValue {
		commands := make([]types.SQLCommand, types.MaxSQLCommandsPerDecidedValue)
		for i := range commands {
			commands[i] = command(fmt.Sprintf("retained-%d", start+i))
		}
		apply(commands)
	}
	rejected := command("rejected-at-capacity")
	apply([]types.SQLCommand{rejected})
	fingerprint, err := types.SQLFingerprint(rejected)
	if err != nil {
		t.Fatal(err)
	}
	checkRejected := func() {
		t.Helper()
		receipt, _, found, matches, err := material.SQLRequestResultFingerprint(ctx, rejected.RequestID, fingerprint, true)
		if err != nil || !found || !matches || receipt.Status != types.MutationRejected || receipt.ErrorCode != types.MutationErrorCodeResultRetentionFull {
			t.Fatalf("rejected receipt=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
		}
	}
	checkRejected()
	target, err := material.SQLRetentionBarrierTarget(ctx)
	if err != nil || target == 0 {
		t.Fatalf("retention pressure target=%d err=%v", target, err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSQLRetentionBarrierLoop(workerCtx, time.Millisecond, n.advanceSQLRetention)
	}()
	defer func() { stop(); <-done }()
	for {
		if n.sqlRetentionBarrierCompleted.Load() > 0 {
			target, err = material.SQLRetentionBarrierTarget(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if target == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("retention worker did not recover capacity: target=%d status=%+v", target, n.SQLRetentionBarriers())
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	<-done
	checkRejected()
	apply([]types.SQLCommand{command("after-capacity-recovery")})
	result, err := material.QueryResult(ctx, "SELECT count FROM retained", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(totalResults+1) {
		t.Fatalf("recovered count=%+v err=%v", result.Rows, err)
	}
}

func TestSQLRetentionBarrierUsesRealLocalConsensus(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "db.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := network.NewServer(core, material, "cluster", true, nil)
	t.Cleanup(server.Close)
	n := &Node{core: core, material: material, server: server}
	n.ready.Store(true)
	n.advanceSQLRetention(context.Background())
	if core.Tip() != 0 || n.sqlRetentionBarrierAttempts.Load() != 0 {
		t.Fatal("barrier proposed without result retention pressure")
	}
	n.proposeSQLRetentionBarrier(context.Background(), context.Background())
	if core.Tip() != 1 || material.Tip() != 1 || n.sqlRetentionBarrierCompleted.Load() != 1 {
		t.Fatalf("barrier did not complete through consensus and apply: tip=%d applied=%d completed=%d", core.Tip(), material.Tip(), n.sqlRetentionBarrierCompleted.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.proposeSQLRetentionBarrier(ctx, ctx)
	if core.Tip() != 1 || n.sqlRetentionBarrierAttempts.Load() != 1 {
		t.Fatal("canceled worker proposed a barrier")
	}
}

func TestSQLRetentionBarrierUsesThreeVoterConsensus(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	fixture.transport.releaseAll()
	fixture.transport.setUnavailable("n3", true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := fixture.node.core.Tip()
	fixture.node.proposeSQLRetentionBarrier(ctx, ctx)
	if fixture.node.sqlRetentionBarrierCompleted.Load() != 1 {
		t.Fatalf("three-voter barrier did not complete: attempts=%d errors=%d", fixture.node.sqlRetentionBarrierAttempts.Load(), fixture.node.sqlRetentionBarrierErrors.Load())
	}
	if fixture.node.core.Tip() <= before || fixture.node.material.Tip() != uint64(fixture.node.core.Tip()) {
		t.Fatalf("barrier did not advance and apply: before=%d tip=%d applied=%d", before, fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	completed := fixture.node.core.Tip()
	fixture.transport.setUnavailable("n2", true)
	minority, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	fixture.node.proposeSQLRetentionBarrier(minority, minority)
	if fixture.node.sqlRetentionBarrierCompleted.Load() != 1 || fixture.node.core.Tip() != completed || fixture.node.material.Tip() != uint64(completed) || !fixture.node.SQLRetentionBarriers().QuorumCheckPending {
		t.Fatalf("minority advanced barrier: completed=%d tip=%d applied=%d", fixture.node.sqlRetentionBarrierCompleted.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

// Uses the production 256 MiB pool with a test-only 1,024-slot window and
// 1 ms worker interval. The fixture has three real consensus cores and a
// filesystem object store, but its peer transport is in-process.
func TestManagedThreeVoterSQLRetentionFullRecovery(t *testing.T) {
	n, _, fixture := newKVWrapperFixtureWindow(t, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	n.ready.Store(true)
	execute := func(req network.ExecuteRequest) network.ExecuteResponse {
		t.Helper()
		result, err := n.ExecuteManagedSQL(ctx, req)
		if err != nil {
			t.Fatalf("request %s: %v", req.RequestID, err)
		}
		return result
	}
	setup := execute(network.ExecuteRequest{RequestID: "setup-retention", SQL: "CREATE TABLE retained (count INTEGER NOT NULL, payload BLOB NOT NULL)"})
	if setup.Status != types.MutationCommitted {
		t.Fatalf("setup: %+v", setup)
	}
	insert := execute(network.ExecuteRequest{RequestID: "insert-retention", SQL: "INSERT INTO retained VALUES (0, zeroblob(?))", Args: []any{materializer.MaxMutationResultBytes - 56}})
	if insert.Status != types.MutationCommitted {
		t.Fatalf("insert: %+v", insert)
	}
	request := func(id string) network.ExecuteRequest {
		return network.ExecuteRequest{RequestID: id, SQL: "UPDATE retained SET count=count+1, payload=payload RETURNING payload", WantRows: true}
	}
	for i := 0; i < 256; i++ {
		result := execute(request(fmt.Sprintf("full-%d", i)))
		if result.Status != types.MutationCommitted {
			t.Fatalf("fill %d: %+v", i, result.MutationReceipt)
		}
	}
	rejected := request("rejected-retention")
	first := execute(rejected)
	if first.Status != types.MutationRejected || first.ErrorCode != types.MutationErrorCodeResultRetentionFull {
		t.Fatalf("expected capacity rejection: %+v", first.MutationReceipt)
	}
	// Withhold one voter's network traffic; this is not a process shutdown.
	fixture.transport.unavailable.Store(quepaxa.NodeID("n3"), true)
	defer fixture.transport.unavailable.Delete(quepaxa.NodeID("n3"))
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSQLRetentionBarrierLoop(workerCtx, time.Millisecond, n.advanceSQLRetention)
	}()
	defer func() { stop(); <-done }()
	for {
		target, err := n.material.SQLRetentionBarrierTarget(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if target == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no capacity recovery: %+v", n.SQLRetentionBarriers())
		case <-time.After(25 * time.Millisecond):
		}
	}
	stop()
	<-done
	retry := execute(rejected)
	if retry.Status != first.Status || retry.Slot != first.Slot || retry.ErrorCode != first.ErrorCode {
		t.Fatalf("old failure changed: first=%+v retry=%+v", first.MutationReceipt, retry.MutationReceipt)
	}
	result := execute(request("after-recovery"))
	if result.Status != types.MutationCommitted {
		t.Fatalf("new request: %+v", result.MutationReceipt)
	}
	rows, err := n.material.QueryResult(ctx, "SELECT count FROM retained", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(257) {
		t.Fatalf("count=%+v err=%v", rows.Rows, err)
	}
}

func TestSQLRetentionWorkerBoundsInflightAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		runSQLRetentionBarrierLoop(ctx, time.Millisecond, func(ctx context.Context) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("overlapping work: calls=%d", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestSQLRetentionPreferredAndFallbackVoters(t *testing.T) {
	order := []quepaxa.NodeID{"n1", "n2", "n3"}
	var current atomic.Uint64
	current.Store(5)
	tip := func() quepaxa.Slot { return quepaxa.Slot(current.Load()) }
	if !waitRetentionProposer(context.Background(), "n1", order, tip) {
		t.Fatal("preferred voter did not proceed")
	}
	if !waitRetentionProposer(context.Background(), "n2", order, tip) {
		t.Fatal("fallback voter did not proceed while preferred made no progress")
	}
	if waitRetentionProposer(context.Background(), "not-member", order, tip) {
		t.Fatal("non-member proceeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan bool, 1)
	started := make(chan struct{}, 1)
	observedTip := func() quepaxa.Slot {
		value := tip()
		select {
		case started <- struct{}{}:
		default:
		}
		return value
	}
	go func() { result <- waitRetentionProposer(ctx, "n3", order, observedTip) }()
	<-started
	current.Add(1)
	if <-result {
		t.Fatal("fallback voter proceeded after progress")
	}
}

func TestSQLRetentionFallbackStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitRetentionProposer(ctx, "n2", []quepaxa.NodeID{"n1", "n2"}, func() quepaxa.Slot { return 1 }) {
		t.Fatal("canceled voter proceeded")
	}
}

func TestSQLRetentionWorkerDoesNotRunBeforeInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSQLRetentionBarrierLoop(ctx, 100*time.Millisecond, func(context.Context) { started <- struct{}{} })
	}()
	select {
	case <-started:
		t.Fatal("worker started before its first interval")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
