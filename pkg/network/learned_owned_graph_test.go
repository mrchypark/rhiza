//go:build rhiza_local_testhooks

package network

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedOwnedGraphNativeCommitRemainsGuardedOnServerClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	dir := t.TempDir()
	wal, err := qlog.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := testMember("graph-owned", "n1", "graph-owned-token")
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	command := types.GraphCommand{RequestID: "graph-owned", Cypher: `CREATE (:OwnedGraph {id: 'one'})`}
	value, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, value); err != nil || slot != 1 {
		t.Fatalf("certify graph: slot=%d err=%v", slot, err)
	}
	if err := core.EnsureDurableThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	values, _, err := core.DecisionsFrom(1, 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("graph decision count=%d err=%v", len(values), err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[0].Value
	path := filepath.Join(dir, "state.sqlite")
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	server, err := NewServerForPolicy(core, mat, "graph-owned", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var gateOnce, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	restore := localtesthooks.Set(func(event string) {
		if event == "materializer:graph:after-native-before-sqlite" {
			gateOnce.Do(func() { close(entered); <-release })
		}
	})
	defer restore()
	stream := context.WithValue(ctx, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: ctx, deadline: time.Now().Add(20 * time.Second)})
	request := &peerfb.RequestT{Operation: peerfb.OperationLearned, ClusterId: "graph-owned", ConfigId: 1, SenderId: "n1", Decision: decisionToWire(decision)}
	peer := &PeerServer{server: server}
	done := make(chan error, 1)
	go func() { _, err := peer.handle(stream, member.PublicKey[:], request); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("graph native gate not reached: %v", err)
	case <-ctx.Done():
		t.Fatal("graph native gate timed out")
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-server.proposalCtx.Done():
	case <-ctx.Done():
		t.Fatal("close did not cancel local owner")
	}
	releaseGate()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled graph handoff acknowledged")
		}
		t.Logf("canceled handler error: %v", err)
	case <-ctx.Done():
		t.Fatal("handler did not return")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("server close did not join")
	}
	if _, err := mat.GraphQuery(ctx, `MATCH (n:OwnedGraph) RETURN n.id`, nil); err == nil || err.Error() != "graph materializer tip 1 does not match SQLite tip 0" {
		t.Fatalf("paired read before reopen: %v", err)
	}
	if err := mat.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := materializer.OpenManagedSQLKVGraphWithOriginals(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.GraphQuery(ctx, `MATCH (n:OwnedGraph) RETURN n.id`, nil); err == nil || err.Error() != "graph materializer tip 1 does not match SQLite tip 0" {
		t.Fatalf("paired read after reopen: %v", err)
	}
	if err := reopened.ApplyBatch(ctx, values); err != nil {
		t.Fatalf("replay same certified Graph decision: %v", err)
	}
	rows, err := reopened.GraphQuery(ctx, `MATCH (n:OwnedGraph) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "one" {
		t.Fatalf("replayed Graph result=%+v err=%v", rows, err)
	}
	if receipt, found, err := reopened.GraphMutationReceipt(ctx, "graph-owned"); err != nil || !found || receipt.Status != types.MutationCommitted || receipt.Slot != 1 {
		t.Fatalf("graph receipt=%+v found=%t err=%v", receipt, found, err)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("test deadline exceeded")
	}
}
