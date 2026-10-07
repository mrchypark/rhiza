package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCheckpointPrepareConcurrentHandlerReplacement(t *testing.T) {
	server := &Server{}
	first, second := errors.New("first handler"), errors.New("second handler")
	handler := func(result error) func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error {
		return func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error { return result }
	}
	server.SetCheckpointPrepare(handler(first))
	start := make(chan struct{})
	unexpected := make(chan error, 3)
	var workers sync.WaitGroup
	for range 3 {
		workers.Go(func() {
			<-start
			for range 100 {
				if err := server.prepareCheckpoint(context.Background(), "n2", quepaxa.CheckpointSeal{}); err != first && err != second {
					unexpected <- err
					return
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 100 {
			server.SetCheckpointPrepare(handler(second))
			server.SetCheckpointPrepare(handler(first))
		}
	})
	close(start)
	workers.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Errorf("unexpected handler result: %v", err)
	}
}

func TestCheckpointPrepareReplacementPreservesInflightHandler(t *testing.T) {
	server := &Server{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	first, second := errors.New("first handler"), errors.New("second handler")
	server.SetCheckpointPrepare(func(callCtx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) error {
		if callCtx != ctx || sender != "n2" || seal.Index != 7 {
			return errors.New("checkpoint arguments changed")
		}
		close(entered)
		select {
		case <-release:
		case <-callCtx.Done():
		}
		return first
	})
	done := make(chan error, 1)
	go func() { done <- server.prepareCheckpoint(ctx, "n2", quepaxa.CheckpointSeal{Index: 7}) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first handler did not enter")
	}
	replaced := make(chan struct{})
	go func() {
		server.SetCheckpointPrepare(func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error {
			// Callbacks run outside the publication lock and may clear themselves.
			server.SetCheckpointPrepare(nil)
			return second
		})
		close(replaced)
	}()
	select {
	case <-replaced:
	case <-ctx.Done():
		t.Fatal("replacement blocked behind the in-flight handler")
	}
	next := make(chan error, 1)
	go func() { next <- server.prepareCheckpoint(ctx, "n3", quepaxa.CheckpointSeal{}) }()
	select {
	case err := <-next:
		if err != second {
			t.Fatalf("replacement result=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("replacement could not clear itself")
	}
	// Canceling releases the old call without changing its captured handler.
	cancel()
	select {
	case err := <-done:
		if err != first {
			t.Fatalf("in-flight result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight handler did not return")
	}
}

// A peer can reject an unchanged, locally valid seal while its certified
// prefix is behind. This characterizes preparation; it does not relax the
// identity check or attribute a particular hosted failure to this case.
func TestPrepareCheckpointPeerPrefixLagThenSameSealQuorum(t *testing.T) {
	const genericIdentityError = "checkpoint seal does not match local certified prefix"
	identityFlags := []string{"membership_error", "anchor_mismatch", "config_mismatch", "prefix_absent", "prefix_mismatch", "tip_behind", "order_unavailable", "next_order_mismatch", "following_order_mismatch"}
	checkIdentity := func(t *testing.T, err error, wrapped bool, trueFlags ...string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), genericIdentityError) {
			t.Fatalf("identity rejection=%v", err)
		}
		if !localtesthooks.Enabled {
			if !wrapped && err.Error() != genericIdentityError {
				t.Fatalf("ordinary identity error changed: %q", err.Error())
			}
			return
		}
		wantTrue := make(map[string]bool, len(trueFlags))
		for _, name := range trueFlags {
			wantTrue[name] = true
		}
		for _, name := range identityFlags {
			field := name + "=" + strconv.FormatBool(wantTrue[name])
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("identity rejection %q missing %s", err, field)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Obtain an actual quorum certificate with the same member identities.
	donor := newForegroundAPIPeers(t, t.TempDir())
	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 1
	slot, err := donor.servers["n1"].ProposeControl(ctx, types.EncodeReadBarrier(nonce))
	if err != nil {
		t.Fatalf("certify donor prefix: %v", err)
	}
	certified, ok := donor.cores["n1"].CertifiedValue(slot)
	if !ok {
		t.Fatal("donor quorum did not certify prefix")
	}

	peers := newForegroundAPIPeers(t, t.TempDir())
	for _, core := range peers.cores {
		core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	}
	if err := peers.cores["n1"].AcceptCertifiedValue(certified); err != nil {
		t.Fatalf("install authentic certificate at source: %v", err)
	}
	source := peers.cores["n1"]
	if source.Tip() != slot || peers.cores["n2"].Tip() >= slot || peers.cores["n3"].Tip() >= slot {
		t.Fatalf("unexpected prefix tips: source=%d n2=%d n3=%d", source.Tip(), peers.cores["n2"].Tip(), peers.cores["n3"].Tip())
	}
	prefix, ok := source.PrefixHash(slot)
	if !ok {
		t.Fatal("certified source prefix unavailable")
	}
	next, following, err := source.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatalf("source leader schedule: %v", err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: source.ConfigIDForSlot(slot), Index: slot,
		RootHash:   sha256.Sum256([]byte("unchanged checkpoint root")),
		StateHash:  sha256.Sum256([]byte("unchanged checkpoint state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		GenerationAnchorHash: source.GenerationAnchorHash(),
	}
	if err := source.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("prepare source: %v", err)
	}
	for _, id := range []quepaxa.NodeID{"n2", "n3"} {
		if _, ok := peers.cores[id].PrefixHash(slot); ok {
			t.Fatalf("%s unexpectedly has candidate prefix", id)
		}
		if _, _, err := peers.cores[id].CheckpointLeaderOrders(slot); err != nil {
			t.Fatalf("%s leader schedule unavailable independently of lag: %v", id, err)
		}
	}
	if err := peers.transports[0].PrepareCheckpoint(ctx, seal); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("real peer preparation with both peers behind: %v", err)
	} else {
		checkIdentity(t, err, true, "prefix_absent", "tip_behind")
	}
	if currentPrefix, ok := source.PrefixHash(slot); !ok || currentPrefix != prefix || source.CompactionFloor() != 0 {
		t.Fatalf("failed preparation changed source prefix/floor: present=%t prefix=%x floor=%d", ok, currentPrefix, source.CompactionFloor())
	}
	if _, ok, err := source.LatestCheckpointSeal(); err != nil || ok {
		t.Fatalf("failed preparation certified a seal: present=%t err=%v", ok, err)
	}

	if err := peers.servers["n2"].catchUpFrom(ctx, "n1", slot, true); err != nil {
		t.Fatalf("durable peer catch-up: %v", err)
	}
	if peers.cores["n2"].Tip() != slot {
		t.Fatalf("caught-up peer tip=%d, want %d", peers.cores["n2"].Tip(), slot)
	}
	if caughtUpPrefix, ok := peers.cores["n2"].PrefixHash(slot); !ok || caughtUpPrefix != prefix {
		t.Fatalf("caught-up peer prefix=%x present=%t, want %x", caughtUpPrefix, ok, prefix)
	}
	if err := peers.transports[0].PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("same seal after one peer catch-up: %v", err)
	}
	if err := peers.cores["n2"].RequirePreparedCheckpoint(seal); err != nil {
		t.Fatalf("caught-up peer did not persist preparation: %v", err)
	}
	if peers.cores["n3"].Tip() >= slot {
		t.Fatal("second remote peer caught up without a requested repair")
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(ctx, value); err != nil {
		t.Fatalf("certify unchanged prepared seal: %v", err)
	}
	got, ok, err := source.LatestCheckpointSeal()
	if err != nil || !ok || got.Index != seal.Index || got.RootHash != seal.RootHash || got.PrefixHash != seal.PrefixHash {
		t.Fatalf("certified seal=%+v present=%t err=%v", got, ok, err)
	}
	if source.CompactionFloor() != 0 {
		t.Fatalf("certification unexpectedly advanced compaction floor to %d", source.CompactionFloor())
	}

	for name, test := range map[string]struct {
		mutate func(*quepaxa.CheckpointSeal)
		flags  []string
	}{
		"prefix":            {func(s *quepaxa.CheckpointSeal) { s.PrefixHash[0] ^= 1 }, []string{"prefix_mismatch"}},
		"config":            {func(s *quepaxa.CheckpointSeal) { s.ConfigID++ }, []string{"config_mismatch"}},
		"anchor":            {func(s *quepaxa.CheckpointSeal) { s.GenerationAnchorHash[0] ^= 1 }, []string{"anchor_mismatch"}},
		"next leader order": {func(s *quepaxa.CheckpointSeal) { s.NextLeaderOrder = nil }, []string{"next_order_mismatch"}},
		"following order":   {func(s *quepaxa.CheckpointSeal) { s.FollowingLeaderOrder = []quepaxa.NodeID{"n1"} }, []string{"following_order_mismatch"}},
		"schedule absent":   {func(s *quepaxa.CheckpointSeal) { s.Index = 300 }, []string{"prefix_absent", "tip_behind", "order_unavailable"}},
	} {
		t.Run(name+" remains rejected at caught-up peer", func(t *testing.T) {
			wrong := seal
			test.mutate(&wrong)
			checkIdentity(t, peers.cores["n2"].PrepareCheckpoint(ctx, wrong), false, test.flags...)
		})
	}
}

// This deliberately fails against the one-shot peer preparation path. Both
// remote Core checks finish while behind, but their real QUIC responses are
// held until one peer has durably caught up to the unchanged seal prefix.
func TestPrepareCheckpointSameQUICCallSurvivesPeerPrefixCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	donor := newForegroundAPIPeers(t, t.TempDir())
	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 2
	slot, err := donor.servers["n1"].ProposeControl(ctx, types.EncodeReadBarrier(nonce))
	if err != nil {
		t.Fatal(err)
	}
	certified, ok := donor.cores["n1"].CertifiedValue(slot)
	if !ok {
		t.Fatal("donor prefix has no authentic quorum certificate")
	}
	peers := newForegroundAPIPeers(t, t.TempDir())
	for _, core := range peers.cores {
		core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	}
	if err := peers.cores["n1"].AcceptCertifiedValue(certified); err != nil {
		t.Fatal(err)
	}
	source := peers.cores["n1"]
	prefix, ok := source.PrefixHash(slot)
	if !ok {
		t.Fatal("source prefix unavailable")
	}
	next, following, err := source.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: source.ConfigIDForSlot(slot), Index: slot,
		RootHash: sha256.Sum256([]byte("same-call checkpoint root")), StateHash: sha256.Sum256([]byte("same-call checkpoint state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		GenerationAnchorHash: source.GenerationAnchorHash(),
	}
	if err := source.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	for _, id := range []quepaxa.NodeID{"n2", "n3"} {
		if peers.cores[id].Tip() >= slot {
			t.Fatalf("%s was not behind before QUIC preparation", id)
		}
	}
	for name, mutate := range map[string]func(*quepaxa.CheckpointSeal){
		"wrong config": func(s *quepaxa.CheckpointSeal) { s.ConfigID++ },
		"wrong anchor": func(s *quepaxa.CheckpointSeal) { s.GenerationAnchorHash[0] ^= 1 },
		"wrong order":  func(s *quepaxa.CheckpointSeal) { s.NextLeaderOrder = nil },
	} {
		t.Run(name+" does not wait", func(t *testing.T) {
			wrong := seal
			mutate(&wrong)
			waited, err := peers.cores["n2"].WaitCheckpointPrefixIfLagging(ctx, wrong)
			if waited || err != nil {
				t.Fatalf("unsafe seal waited=%t err=%v", waited, err)
			}
			if err := peers.cores["n2"].PrepareCheckpoint(ctx, wrong); err == nil {
				t.Fatal("unsafe seal prepared")
			}
		})
	}
	peers.cores["n2"].SetCheckpointValidator(nil)
	if waited, err := peers.cores["n2"].WaitCheckpointPrefixIfLagging(ctx, seal); waited || err != nil {
		t.Fatalf("missing validator waited=%t err=%v", waited, err)
	}
	peers.cores["n2"].SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })

	entered := make(chan quepaxa.NodeID, 2)
	checkedN2 := make(chan error, 1)
	canceledN3 := make(chan error, 1)
	releaseN2 := make(chan struct{})
	releasedN2 := false
	defer func() {
		if !releasedN2 {
			close(releaseN2)
		}
	}()
	for _, id := range []quepaxa.NodeID{"n2", "n3"} {
		peerCore := peers.cores[id]
		peers.servers[id].SetCheckpointPrepare(func(callCtx context.Context, _ quepaxa.NodeID, requested quepaxa.CheckpointSeal) error {
			entered <- id
			_, err := peerCore.WaitCheckpointPrefixIfLagging(callCtx, requested)
			if err == nil {
				err = peerCore.PrepareCheckpoint(callCtx, requested)
			}
			if id == "n3" {
				canceledN3 <- err
				return err
			}
			checkedN2 <- err
			select {
			case <-releaseN2:
			case <-callCtx.Done():
			}
			return err
		})
	}
	sameCall := make(chan error, 1)
	go func() { sameCall <- peers.transports[0].PrepareCheckpoint(ctx, seal) }()
	seen := make(map[quepaxa.NodeID]bool, 2)
	for range 2 {
		select {
		case id := <-entered:
			if seen[id] {
				t.Fatalf("duplicate peer handler entry: %s", id)
			}
			seen[id] = true
		case <-ctx.Done():
			t.Fatalf("both peer handlers did not enter: %v", ctx.Err())
		}
	}
	if err := peers.servers["n2"].catchUpFrom(ctx, "n1", slot, true); err != nil {
		t.Fatalf("durable peer catch-up before responses: %v", err)
	}
	if got, present := peers.cores["n2"].PrefixHash(slot); !present || got != prefix {
		t.Fatalf("caught-up prefix=%x present=%t, want %x", got, present, prefix)
	}
	select {
	case err := <-checkedN2:
		if err != nil {
			t.Fatalf("n2 did not prepare after authentic catch-up: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("n2 did not finish preparation: %v", ctx.Err())
	}
	if err := peers.cores["n3"].RequirePreparedCheckpoint(seal); err == nil {
		t.Fatal("uncaught-up n3 prepared the seal before cancellation")
	}
	close(releaseN2)
	releasedN2 = true
	select {
	case err := <-sameCall:
		if err != nil {
			t.Fatalf("same QUIC preparation rejected without n3 response: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("n2 success did not establish quorum before n3 response: %v", ctx.Err())
	}
	select {
	case err := <-canceledN3:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("n3 incoming QUIC context did not cancel: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("n3 incoming QUIC context remained active: %v", ctx.Err())
	}
	if err := peers.cores["n3"].RequirePreparedCheckpoint(seal); err == nil {
		t.Fatal("canceled n3 prepared the seal")
	}
}
