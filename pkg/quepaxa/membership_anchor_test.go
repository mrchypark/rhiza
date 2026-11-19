package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestMembershipAnchorCompactsLongLivedRotationsAndRestoresBase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var storeMu sync.Mutex
	proofs := make(map[[32]byte]MembershipRecord)
	storeProof := func(_ context.Context, digest [32]byte, proof MembershipRecord) error {
		storeMu.Lock()
		defer storeMu.Unlock()
		proofs[digest] = cloneMembershipRecord(proof)
		return nil
	}
	loadProof := func(_ context.Context, digest [32]byte) (MembershipRecord, error) {
		storeMu.Lock()
		defer storeMu.Unlock()
		rec, ok := proofs[digest]
		if !ok {
			return MembershipRecord{}, fmt.Errorf("proof %x missing", digest[:8])
		}
		return cloneMembershipRecord(rec), nil
	}
	verifyAnchor := func(ctx context.Context, initial Cluster, anchor MembershipAnchor) error {
		return VerifyMembershipAnchorProof(ctx, initial, anchor, loadProof)
	}

	dir := t.TempDir()
	initial := Cluster{ConfigID: 1, Members: []Member{
		{ID: "n1", URL: "http://10.0.0.1:8080", WALIdentity: strings.Repeat("11", 32)},
		{ID: "n2", URL: "http://10.0.0.2:8080", WALIdentity: strings.Repeat("22", 32)},
		{ID: "n3", URL: "http://10.0.0.3:8080", WALIdentity: strings.Repeat("33", 32)},
	}}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	cores := make(map[NodeID]*Core)
	addNode := func(m Member, isInitial bool) *Core {
		wal, err := qlog.Open(filepath.Join(dir, string(m.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		idBytes, err := hex.DecodeString(m.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(idBytes); err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			NodeID:                   m.ID,
			Cluster:                  initial,
			WAL:                      wal,
			Transport:                transport,
			EnableReconfiguration:    true,
			ReconfigurationAdmission: func(context.Context, Cluster, Slot, [32]byte) error { return nil },
			VerifyMembershipAnchor:   verifyAnchor,
			StoreMembershipAnchor:    storeProof,
		}
		var c *Core
		if isInitial {
			c, err = New(cfg)
		} else {
			c, err = NewLearner(cfg)
		}
		if err != nil {
			t.Fatal(err)
		}
		c.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		cores[m.ID] = c
		transport.cores[m.ID] = c
		return c
	}
	for _, m := range initial.Members {
		addNode(m, true)
	}
	leader := cores["n1"]

	// Perform 24 sequential transitions (12 ADD + REMOVE Pod rotations) until
	// the uncompacted MembershipRecord exceeds membershipInlineLimit (96 KiB).
	active := append([]Member(nil), initial.Members...)
	cfgID := uint(1)
	for round := 1; round <= 12; round++ {
		repl := Member{
			ID:          NodeID(fmt.Sprintf("r-%02d", round)),
			URL:         fmt.Sprintf("http://10.0.1.%d:8080", round),
			WALIdentity: strings.Repeat(fmt.Sprintf("%02x", round+0x40), 32),
		}
		c := addNode(repl, false)
		for nextSlot := Slot(1); nextSlot <= leader.Tip(); {
			decs, tip, err := leader.DecisionsFrom(nextSlot, 64)
			if err != nil {
				t.Fatalf("round %d learner catchup: %v", round, err)
			}
			if len(decs) == 0 {
				break
			}
			if err := c.AcceptCertifiedValues(decs); err != nil {
				t.Fatalf("round %d learner catchup accept: %v", round, err)
			}
			nextSlot = decs[len(decs)-1].Slot + 1
			if nextSlot > tip {
				break
			}
		}
		cfgID++
		addTarget := Cluster{ConfigID: cfgID, Members: append(append([]Member(nil), active...), repl)}
		if _, err := leader.BeginReconfiguration(ctx, addTarget); err != nil {
			t.Fatalf("round %d add begin: %v", round, err)
		}
		if err := leader.FinishReconfiguration(ctx); err != nil {
			t.Fatalf("round %d add finish: %v", round, err)
		}

		// Remove the oldest non-leader voter (index 1).
		removed := active[1]
		var nextMembers []Member
		for _, m := range addTarget.Members {
			if m.ID != removed.ID {
				nextMembers = append(nextMembers, m)
			}
		}
		cfgID++
		remTarget := Cluster{ConfigID: cfgID, Members: nextMembers}
		if _, err := leader.BeginReconfiguration(ctx, remTarget); err != nil {
			t.Fatalf("round %d remove begin: %v", round, err)
		}
		if err := leader.FinishReconfiguration(ctx); err != nil {
			t.Fatalf("round %d remove finish: %v", round, err)
		}
		transport.fail(removed.ID)
		transport.mu.Lock()
		delete(transport.cores, removed.ID)
		transport.mu.Unlock()
		active = nextMembers
	}

	if _, _, err := leader.Propose(ctx, []byte("after-24-transitions")); err != nil {
		t.Fatal(err)
	}
	cut := leader.Tip()
	fullHistory, err := leader.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if len(fullHistory.Transitions) != 24 {
		t.Fatalf("transitions=%d, want 24", len(fullHistory.Transitions))
	}
	fullBytes, err := json.Marshal(fullHistory)
	if err != nil {
		t.Fatal(err)
	}
	if len(fullBytes) <= membershipInlineLimit {
		t.Fatalf("expected 24 transitions (%d B) to exceed inline limit (%d B)", len(fullBytes), membershipInlineLimit)
	}

	anchored, err := leader.CheckpointMembership(cut)
	if err != nil {
		t.Fatalf("CheckpointMembership(%d): %v", cut, err)
	}
	if anchored.Anchor == nil || len(anchored.Transitions) != 0 {
		t.Fatalf("expected anchored compact membership, got anchor=%+v transitions=%d", anchored.Anchor, len(anchored.Transitions))
	}
	prefix, _ := leader.PrefixHash(cut)
	next, following, err := leader.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID:             leader.ConfigID(),
		Index:                cut,
		RootHash:             sha256.Sum256([]byte("anchored-root")),
		StateHash:            sha256.Sum256([]byte("anchored-state")),
		PrefixHash:           prefix,
		NextLeaderOrder:      next,
		FollowingLeaderOrder: following,
		Membership:           &anchored,
	}
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatalf("EncodeCheckpointSeal failed: %v", err)
	}
	if len(encodedSeal) >= 16<<10 {
		t.Fatalf("anchored CheckpointSeal size=%d B, expected < 16 KiB", len(encodedSeal))
	}
	for _, m := range active {
		if err := cores[m.ID].WaitTip(ctx, cut); err != nil {
			t.Fatal(err)
		}
		if err := cores[m.ID].PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("PrepareCheckpoint on %s: %v", m.ID, err)
		}
	}
	sealSlot, _, err := leader.Propose(ctx, encodedSeal)
	if err != nil {
		t.Fatalf("propose anchored seal: %v", err)
	}
	sealDecision, ok := leader.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("missing certified seal decision")
	}

	// Verify fresh NoPVC Pod restores from the anchored seal using VerifyMembershipAnchor.
	freshWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = freshWAL.Close() }()
	fresh, err := NewLearner(Config{
		NodeID:                 "fresh-learner",
		Cluster:                initial,
		WAL:                    freshWAL,
		Transport:              &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}},
		EnableReconfiguration:  true,
		VerifyMembershipAnchor: verifyAnchor,
		StoreMembershipAnchor:  storeProof,
	})
	if err != nil {
		t.Fatal(err)
	}
	fresh.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := fresh.RestoreCheckpointBase(ctx, seal, sealDecision); err != nil {
		t.Fatalf("RestoreCheckpointBase from anchored seal: %v", err)
	}
	if fresh.ConfigID() != cfgID || fresh.CompactionFloor() != cut {
		t.Fatalf("restored config=%d floor=%d, want %d / %d", fresh.ConfigID(), fresh.CompactionFloor(), cfgID, cut)
	}
	// Verify hex digest key helper produces canonical path.
	if key := MembershipAnchorProofKey("ns/c1", anchored.Anchor.Digest); !strings.HasPrefix(key, "ns/c1/membership-anchors/") || !strings.HasSuffix(key, hex.EncodeToString(anchored.Anchor.Digest[:])+".json") {
		t.Fatalf("unexpected anchor key %q", key)
	}
}
