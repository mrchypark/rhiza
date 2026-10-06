package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

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
