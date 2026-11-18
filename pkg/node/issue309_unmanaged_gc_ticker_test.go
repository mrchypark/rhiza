package node

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type issue309LogCapture struct {
	mu sync.Mutex
	b  strings.Builder
}

func (c *issue309LogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.Write(p)
}

func (c *issue309LogCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

func issue309Wait(ctx context.Context, ready func() bool) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func issue309Deletes(store *managedTestObjectStore) int {
	store.mu.RLock()
	defer store.mu.RUnlock()
	deletes := 0
	for _, request := range store.requests {
		if strings.HasPrefix(request, http.MethodDelete+" ") {
			deletes++
		}
	}
	return deletes
}

func issue309OwnProposer(nodes []*Node) *Node {
	for _, node := range nodes {
		order := node.core.ProposerOrder()
		if len(order) > 0 && order[0] == node.core.NodeID() {
			return node
		}
	}
	return nil
}

func issue309StableProposerBase(ctx context.Context, nodes []*Node, node *Node) bool {
	if _, ok := node.validatedArchiveRecoveryBase(ctx); !ok {
		return false
	}
	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	check := time.NewTicker(5 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-check.C:
			if issue309OwnProposer(nodes) != node {
				return false
			}
		case <-deadline.C:
			if issue309OwnProposer(nodes) != node {
				return false
			}
			_, ok := node.validatedArchiveRecoveryBase(ctx)
			return ok
		}
	}
}

func issue309WaitForStableProposerBase(ctx context.Context, nodes []*Node) (*Node, bool) {
	for {
		if node := issue309OwnProposer(nodes); node != nil && issue309StableProposerBase(ctx, nodes, node) {
			return node, true
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func issue309MatchingLogs(logs, want string) string {
	var matches strings.Builder
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, want) {
			matches.WriteString(line)
			matches.WriteByte('\n')
			if matches.Len() >= 4096 {
				break
			}
		}
	}
	return matches.String()
}

func TestUnmanagedMultiNodeDoesNotStartAppendOnlyGCTicker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const clusterID = "issue309-unmanaged-gc-ticker"
	store := &managedTestObjectStore{objects: make(map[string][]byte)}
	server := httptest.NewServer(http.HandlerFunc(store.serve))
	defer server.Close()

	ids := []quepaxa.NodeID{"n1", "n2", "n3"}
	tokens := make(map[quepaxa.NodeID]string, len(ids))
	addresses := make(map[quepaxa.NodeID]string, len(ids))
	members := make([]quepaxa.Member, 0, len(ids))
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := packet.LocalAddr().String()
		if err := packet.Close(); err != nil {
			t.Fatal(err)
		}
		tokens[id] = string(id) + "-token"
		addresses[id] = address
		member := peerMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + address
		members = append(members, member)
	}

	capture := &issue309LogCapture{}
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(capture)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	nodes := make([]*Node, 0, len(ids))
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			if err := nodes[i].Shutdown(); err != nil {
				t.Errorf("shutdown node %s: %v", nodes[i].config.NodeID, err)
			}
		}
	}()
	for _, id := range ids {
		config := &types.ExecutionConfig{
			NodeID: id, ClusterID: clusterID, DataDir: t.TempDir(), Members: members,
			PeerAddr: addresses[id], PeerToken: tokens[id], AdminToken: "issue309-admin",
			ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(server.URL, "http://"),
			ObjStoreBucket: "prepared-target", ObjStorePrefix: clusterID, ObjStoreRegion: "us-east-1",
			ObjStoreInsecure: true, ObjStoreAccessKey: "test", ObjStoreSecretKey: "test",
			ObjStoreDurability:   types.ObjectStoreDurabilityBeforeAck,
			ObjStoreSyncInterval: time.Hour, CheckpointInterval: 20 * time.Millisecond,
			ObjStoreGCInterval: 5 * time.Millisecond, ObjStoreGCGracePeriod: time.Nanosecond,
		}
		node := New(config)
		if err := node.Open(ctx); err != nil {
			t.Fatalf("open %s: %v\nlogs:\n%s", id, err, capture.String())
		}
		nodes = append(nodes, node)
	}

	if !issue309Wait(ctx, func() bool {
		for _, node := range nodes {
			if !node.Ready() {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("cluster did not become ready: %v\nlogs:\n%s", ctx.Err(), capture.String())
	}
	proposer := issue309OwnProposer(nodes)
	if proposer == nil {
		t.Fatalf("cluster has no selected proposer\nlogs:\n%s", capture.String())
	}
	if _, err := proposer.server.Execute(ctx, network.ExecuteRequest{RequestID: "issue309-row", SQL: "CREATE TABLE issue309 (id INTEGER)"}); err != nil {
		t.Fatalf("create checkpoint data: %v\nlogs:\n%s", err, capture.String())
	}
	if !issue309Wait(ctx, func() bool {
		return proposer.checkpoints.Latest() != nil
	}) {
		t.Fatalf("checkpoint manager produced no root: %v\nlogs:\n%s", ctx.Err(), capture.String())
	}

	// The selected proposer must retain its own validated base for the complete
	// window. Leadership changes restart the wait instead of producing a false pass.
	deletes := issue309Deletes(store)
	proposer, ok := issue309WaitForStableProposerBase(ctx, nodes)
	if !ok {
		t.Fatalf("no proposer retained a validated recovery base: %v\nlogs:\n%s", ctx.Err(), capture.String())
	}

	const tickerError = "checkpoint GC failed: checkpoint garbage collection is disabled for append-only manager"
	logs := capture.String()
	if strings.Contains(logs, tickerError) {
		t.Fatalf("unmanaged HA started the append-only checkpoint GC ticker\n%s", issue309MatchingLogs(logs, tickerError))
	}
	if got := issue309Deletes(store); got != deletes {
		t.Fatalf("unmanaged HA GC changed delete count from %d to %d", deletes, got)
	}
	if !strings.Contains(logs, "checkpoint prepared") {
		t.Fatalf("fixture did not exercise certified checkpoint publication\nlogs:\n%s", logs)
	}
	if issue309OwnProposer(nodes) != proposer {
		t.Fatalf("proposer changed during regression observation\nlogs:\n%s", logs)
	}
}
