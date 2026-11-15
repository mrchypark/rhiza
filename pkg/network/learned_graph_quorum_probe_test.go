//go:build rhiza_local_testhooks

package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestLearnedGraphSlowQuorumProbe(t *testing.T) {
	t.Run("remote-only", func(t *testing.T) { runLearnedGraphSlowQuorumProbe(t, "") })
	t.Run("server-close", func(t *testing.T) { runLearnedGraphSlowQuorumProbe(t, "server") })
	t.Run("peer-close", func(t *testing.T) { runLearnedGraphSlowQuorumProbe(t, "peer") })
}

func runLearnedGraphSlowQuorumProbe(t *testing.T, shutdown string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := &foregroundAPIPeers{cores: map[quepaxa.NodeID]*quepaxa.Core{}, servers: map[quepaxa.NodeID]*Server{}, materials: map[quepaxa.NodeID]*materializer.Materializer{}}
	t.Cleanup(p.close)
	p.config.ConfigID = 1
	paths := make(map[quepaxa.NodeID]string)
	for _, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p.connections = append(p.connections, conn)
		member := testMember("foreground-api", id, "foreground-voter-"+string(id))
		member.PeerURL = "quic://" + conn.LocalAddr().String()
		p.config.Members = append(p.config.Members, member)
	}
	for _, member := range p.config.Members {
		transport, err := NewTransportForPolicy("foreground-api", member.ID, &p.config, "foreground-voter-"+string(member.ID), sqlpolicy.ManagedGraphRequestMaterializationPolicy)
		if err != nil {
			t.Fatal(err)
		}
		p.transports = append(p.transports, transport)
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		p.wals = append(p.wals, wal)
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: p.config, WAL: wal, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		p.cores[member.ID] = core
		paths[member.ID] = filepath.Join(t.TempDir(), "db.sqlite")
		mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(paths[member.ID], 1)
		if err != nil {
			t.Fatal(err)
		}
		p.materials[member.ID] = mat
		server, err := NewServerForPolicy(core, mat, "foreground-api", true, transport, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
		if err != nil {
			t.Fatal(err)
		}
		p.servers[member.ID] = server
	}
	for i, member := range p.config.Members {
		listener := &quic.Transport{Conn: p.connections[i]}
		p.listeners = append(p.listeners, listener)
		peer, err := StartPeerServerOnTransport(ctx, listener, p.servers[member.ID], p.config.Members, "foreground-voter-"+string(member.ID), "foreground-admin")
		if err != nil {
			t.Fatal(err)
		}
		p.peerServers = append(p.peerServers, peer)
	}
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "graph-probe", Cypher: `CREATE (:Item {id:'first'})`})
	if err != nil {
		t.Fatal(err)
	}
	slot := quepaxa.Slot(1)
	entered, release, fast := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enteredOnce, slowResponse atomic.Bool
	var releaseOnce, fastOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }); fastOnce.Do(func() { close(fast) }) }
	t.Cleanup(unblock)
	observed := make(chan learnedStreamEvent, 8)
	handlerResult := make(chan string, 2)
	testLearnedStreamEvents.Store(&observed)
	t.Cleanup(func() { testLearnedStreamEvents.Store(nil) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=handler-result:node=n3:sender=n1:slot=1") {
			select {
			case handlerResult <- event:
			default:
			}
		}
		if strings.Contains(event, "network:learned:phase=response-written:node=n3:sender=n1:slot=1") {
			slowResponse.Store(true)
		}
		if strings.Contains(event, "network:learned:phase=received:node=n2:sender=n1:slot=1") {
			select {
			case <-fast:
			case <-ctx.Done():
			}
		}
		if event == "materializer:graph:after-native-before-sqlite" {
			if enteredOnce.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}
	})
	t.Cleanup(restore)
	done := make(chan error, 1)
	go func() {
		got, _, err := p.cores["n1"].Propose(ctx, value)
		if err == nil && got != slot {
			err = fmt.Errorf("slot=%d", got)
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("complete before graph gate: %v", err)
	case <-ctx.Done():
		t.Fatal("graph gate not reached")
	}
	var streamCtx context.Context
	for streamCtx == nil {
		select {
		case e := <-observed:
			if e.node == "n3" && e.slot == slot {
				streamCtx = e.ctx
			}
		case <-ctx.Done():
			t.Fatal("slow stream not observed")
		}
	}
	fastOnce.Do(func() { close(fast) })
	select {
	case <-streamCtx.Done():
	case <-ctx.Done():
		t.Fatal("slow stream not canceled")
	}
	if ctx.Err() != nil {
		t.Fatal("parent canceled")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("quorum did not complete")
	}
	if shutdown != "" {
		closed := make(chan error, 1)
		if shutdown == "server" {
			go func() { p.servers["n3"].Close(); closed <- nil }()
			select {
			case <-p.servers["n3"].proposalCtx.Done():
			case <-ctx.Done():
				t.Fatal("server close did not cancel")
			}
		} else {
			// Cancel the peer's own lifetime before invoking its joining Close.
			p.peerServers[2].cancel()
			go func() { closed <- p.peerServers[2].Close() }()
		}
		releaseOnce.Do(func() { close(release) })
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("local close did not drain")
		}
		select {
		case event := <-handlerResult:
			// Compare the complete error, not a substring: any joined UNKNOWN,
			// rollback, or unrelated error must remain a failure of this probe.
			hash := sha256.Sum256(value)
			prefix := fmt.Sprintf("certified replay apply first_slot=1 last_slot=1 last_hash=%x ctx_canceled=true: ", hash)
			wantCanceled := fmt.Sprintf("network:learned:phase=handler-result:node=n3:sender=n1:slot=1:error=%q", prefix+"context canceled")
			wantInterrupted := fmt.Sprintf("network:learned:phase=handler-result:node=n3:sender=n1:slot=1:error=%q", prefix+"sqlite3: interrupted")
			if event != wantCanceled && event != wantInterrupted {
				t.Fatalf("unexpected handler result: %s", event)
			}
			t.Logf("locally canceled handler: %s", event)
		case <-ctx.Done():
			t.Fatal("handler result not observed")
		}
		if _, err := p.materials["n3"].GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil); err == nil || !strings.Contains(err.Error(), "does not match SQLite tip") {
			t.Fatalf("paired read must reject native ahead: %v", err)
		}
		leader, ok := p.cores["n1"].CertifiedValue(slot)
		if !ok {
			t.Fatal("leader certified value missing")
		}
		slow, ok := p.cores["n3"].CertifiedValue(slot)
		if !ok || !bytes.Equal(leader.Value, slow.Value) || !bytes.Equal(leader.Certificate, slow.Certificate) {
			t.Fatal("slow certified decision differs")
		}
		if err := p.materials["n3"].Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := materializer.OpenManagedSQLKVGraphWithOriginals(paths["n3"], 1)
		if err != nil {
			t.Fatal(err)
		}
		p.materials["n3"] = reopened
		if _, err := reopened.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil); err == nil || !strings.Contains(err.Error(), "does not match SQLite tip") {
			t.Fatalf("reopened paired read must reject native ahead: %v", err)
		}
		decisions, _, err := p.cores["n3"].DecisionsFrom(slot, 1)
		if err != nil || len(decisions) != 1 || !bytes.Equal(decisions[0].Certificate, leader.Certificate) || !bytes.Equal(decisions[0].Value, leader.Value) {
			t.Fatalf("certified replay source: %v", err)
		}
		if err := reopened.ApplyBatch(ctx, decisions); err != nil {
			t.Fatalf("replay: %v", err)
		}
		rows, err := reopened.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "first" {
			t.Fatalf("replay rows=%+v err=%v", rows, err)
		}
		if receipt, found, err := reopened.GraphMutationReceipt(ctx, "graph-probe"); err != nil || !found || receipt.Status != types.MutationCommitted || receipt.Slot != 1 {
			t.Fatalf("replay receipt=%+v found=%t err=%v", receipt, found, err)
		}
		if err := p.peerServers[2].Close(); err != nil {
			t.Fatal(err)
		}
		if slowResponse.Load() {
			t.Fatal("canceled slow stream emitted response")
		}
		return
	}
	releaseOnce.Do(func() { close(release) })
	for p.materials["n3"].Tip() < uint64(slot) {
		select {
		case <-ctx.Done():
			t.Fatal("slow apply did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	rows, err := p.materials["n3"].GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "first" {
		t.Fatalf("graph result=%+v err=%v", rows, err)
	}
	leader, ok := p.cores["n1"].CertifiedValue(slot)
	if !ok {
		t.Fatal("leader certified value missing")
	}
	slow, ok := p.cores["n3"].CertifiedValue(slot)
	if !ok || !bytes.Equal(leader.Value, slow.Value) || !bytes.Equal(leader.Certificate, slow.Certificate) {
		t.Fatal("slow certified decision differs")
	}
	if err := p.peerServers[2].Close(); err != nil {
		t.Fatal(err)
	}
	if slowResponse.Load() {
		t.Fatal("canceled slow stream emitted response")
	}
}
