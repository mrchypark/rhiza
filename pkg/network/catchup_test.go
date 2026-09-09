package network

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type catchUpWaitingContext struct {
	context.Context
	seen chan struct{}
	once sync.Once
}

func (c *catchUpWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.seen) })
	return c.Context.Done()
}

func controlledCatchUpPeer(t *testing.T, id quepaxa.NodeID, values []quepaxa.DecidedValue) (*catchUpPeer, <-chan chan struct{}) {
	requests := make(chan chan struct{}, 16)
	peer := startCatchUpPeer(t, id, values, func(ctx context.Context, _ *peerfb.RequestT) error {
		release := make(chan struct{})
		select {
		case requests <- release:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return peer, requests
}

func nextCatchUpRequest(t *testing.T, ctx context.Context, requests <-chan chan struct{}) chan struct{} {
	t.Helper()
	select {
	case release := <-requests:
		return release
	case <-ctx.Done():
		t.Fatal("missing QUIC fetch", ctx.Err())
		return nil
	}
}

func catchUpTestServer(t *testing.T, config quepaxa.Cluster, peers ...*catchUpPeer) (*Server, *qlog.WAL, string) {
	t.Helper()
	members := make([]quepaxa.Member, len(peers))
	for i, p := range peers {
		members[i] = p.member
	}
	transport := NewTransport("cluster", "follower", &quepaxa.Cluster{ConfigID: 1, Members: members}, "admin")
	t.Cleanup(func() { _ = transport.Close() })
	dir := t.TempDir()
	server, wal := newCatchUpFollower(t, dir, config, transport, false)
	t.Cleanup(func() { server.Close(); _ = wal.Close() })
	return server, wal, dir
}

func awaitCatchUp(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal("catch-up did not finish", ctx.Err())
		return ctx.Err()
	}
}

func TestCatchUpSameSourceMixedDurability(t *testing.T) {
	config, values := catchUpValues(t, 8)
	peer, requests := controlledCatchUpPeer(t, "a", values)
	server, wal, dir := catchUpTestServer(t, config, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owner, waiter := make(chan error, 1), make(chan error, 1)
	go func() { owner <- server.catchUpFrom(ctx, "a", 8, false) }()
	release := nextCatchUpRequest(t, ctx, requests)
	waitCtx := &catchUpWaitingContext{Context: ctx, seen: make(chan struct{})}
	go func() { waiter <- server.catchUpFrom(waitCtx, "a", 8, true) }()
	select {
	case <-waitCtx.seen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(server.syncLimit) != 1 || peer.calls.Load() != 1 {
		t.Fatal("same-source waiter consumed a fetch slot")
	}
	close(release)
	if err := awaitCatchUp(t, ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err := awaitCatchUp(t, ctx, waiter); err != nil {
		t.Fatal(err)
	}
	if peer.calls.Load() != 1 {
		t.Fatalf("fetches=%d, want 1", peer.calls.Load())
	}
	server.Close()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, reopened := newCatchUpFollower(t, dir, config, nil, false)
	defer restarted.Close()
	defer reopened.Close()
	if restarted.core.Tip() != 8 {
		t.Fatalf("durable waiter lost hinted prefix on restart: %d", restarted.core.Tip())
	}
}

func TestCatchUpDurablePromotesAlreadyHintedPrefix(t *testing.T) {
	config, values := catchUpValues(t, 8)
	server, wal := newCatchUpFollower(t, t.TempDir(), config, nil, false)
	defer server.Close()
	defer wal.Close()
	if err := server.core.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	if err := server.catchUpFrom(context.Background(), "a", 8, true); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	decisions := 0
	for _, entry := range entries {
		if entry.Type == qlog.EntryDecide {
			decisions++
		}
	}
	if decisions != 8 {
		t.Fatalf("Tip-only success persisted %d of 8 decisions", decisions)
	}
}

func TestCatchUpCanceledOwnerDoesNotCancelWaiter(t *testing.T) {
	config, values := catchUpValues(t, 1)
	peer, requests := controlledCatchUpPeer(t, "a", values)
	server, _, _ := catchUpTestServer(t, config, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ownerCtx, stopOwner := context.WithCancel(ctx)
	defer stopOwner()
	owner, waiter := make(chan error, 1), make(chan error, 1)
	go func() { owner <- server.catchUpFrom(ownerCtx, "a", 1, false) }()
	first := nextCatchUpRequest(t, ctx, requests)
	waitCtx := &catchUpWaitingContext{Context: ctx, seen: make(chan struct{})}
	go func() { waiter <- server.catchUpFrom(waitCtx, "a", 1, false) }()
	select {
	case <-waitCtx.seen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopOwner()
	if err := awaitCatchUp(t, ctx, owner); !errors.Is(err, context.Canceled) {
		t.Fatalf("owner error=%v", err)
	}
	second := nextCatchUpRequest(t, ctx, requests)
	close(second)
	if err := awaitCatchUp(t, ctx, waiter); err != nil {
		t.Fatalf("waiter inherited owner cancellation: %v", err)
	}
	close(first)
	if server.core.Tip() != 1 {
		t.Fatal("waiter did not install prefix")
	}
}

func TestCatchUpDifferentSourceProgresses(t *testing.T) {
	config, values := catchUpValues(t, 1)
	a, aRequests := controlledCatchUpPeer(t, "a", values)
	b, bRequests := controlledCatchUpPeer(t, "b", values)
	server, _, _ := catchUpTestServer(t, config, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owner, waiter, other := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { owner <- server.catchUpFrom(ctx, "a", 1, true) }()
	aRelease := nextCatchUpRequest(t, ctx, aRequests)
	waitCtx := &catchUpWaitingContext{Context: ctx, seen: make(chan struct{})}
	go func() { waiter <- server.catchUpFrom(waitCtx, "a", 1, false) }()
	select {
	case <-waitCtx.seen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { other <- server.catchUpFrom(ctx, "b", 1, false) }()
	bRelease := nextCatchUpRequest(t, ctx, bRequests)
	if len(server.syncLimit) != 2 {
		t.Fatal("different sources did not run concurrently")
	}
	close(bRelease)
	if err := awaitCatchUp(t, ctx, other); err != nil {
		t.Fatal(err)
	}
	close(aRelease)
	for _, done := range []<-chan error{owner, waiter} {
		if err := awaitCatchUp(t, ctx, done); err != nil {
			t.Fatal(err)
		}
	}
	if a.calls.Load() != 1 || b.calls.Load() != 1 {
		t.Fatal("unexpected extra page fetch")
	}
}

func TestCatchUpControlPageIsDurableEvenAsHint(t *testing.T) {
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a", Token: "a-token"}}}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source, err := quepaxa.New(quepaxa.Config{NodeID: "a", Cluster: config, WAL: sourceWAL, EnableReconfiguration: true,
		ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := source.Propose(ctx, []byte("ordinary-prefix")); err != nil {
		t.Fatal(err)
	}
	target := quepaxa.Cluster{ConfigID: 2, Members: append(append([]quepaxa.Member(nil), config.Members...), quepaxa.Member{ID: "b", Token: "b-token", WALIdentity: strings.Repeat("b", 64)})}
	if _, err := source.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err := source.DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	peer := startCatchUpPeer(t, "a", values, nil)
	transport := NewTransport("cluster", "follower", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peer.member}}, "admin")
	defer transport.Close()
	dir := t.TempDir()
	server, wal := newCatchUpFollower(t, dir, config, transport, true)
	defer server.Close()
	defer wal.Close()
	if err := server.catchUpFrom(ctx, "a", source.Tip(), false); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, reopened := newCatchUpFollower(t, dir, config, nil, true)
	defer restarted.Close()
	defer reopened.Close()
	if restarted.core.Tip() != source.Tip() || restarted.core.ConfigID() != 2 {
		t.Fatal("hint control page lost terminal prefix or configuration across restart")
	}
}
