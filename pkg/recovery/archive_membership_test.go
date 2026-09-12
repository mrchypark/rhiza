package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

var errNodeNotFound = errors.New("node not found")

type archiveTestTransport struct {
	mu    sync.RWMutex
	cores map[quepaxa.NodeID]*quepaxa.Core
}

func (t *archiveTestTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, req quepaxa.RecordRequest) (quepaxa.Summary, error) {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return quepaxa.Summary{}, errNodeNotFound
	}
	return core.Record(ctx, req)
}

func (t *archiveTestTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, core := range t.cores {
		_ = core.AcceptDecision(decision)
	}
	return nil
}

func (t *archiveTestTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return 0, errNodeNotFound
	}
	return core.Tip(), nil
}

func (t *archiveTestTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	t.mu.RLock()
	core := t.cores[to]
	t.mu.RUnlock()
	if core == nil {
		return errNodeNotFound
	}
	return core.StageValue(hash, value)
}

func (t *archiveTestTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	t.mu.RLock()
	core := t.cores[from]
	t.mu.RUnlock()
	if core == nil {
		return nil, errNodeNotFound
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("missing value")
	}
	return value, nil
}

func newArchiveTestCluster(t *testing.T, members []quepaxa.Member) *archiveTestTransport {
	t.Helper()
	transport := &archiveTestTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core)}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { wal.Close() })
		cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
		core, err := quepaxa.New(quepaxa.Config{
			NodeID:                member.ID,
			Cluster:               cluster,
			WAL:                   wal,
			Transport:             transport,
			EnableReconfiguration: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	return transport
}

// TestArchivePublishReloadAcrossVoterRemoval proves that an archive can be
// published and reloaded across a voter-removal reconfiguration while its
// configID identity stays at 1. After replaying archive decisions into a
// correctly configured passive observer, the retained prefix and config
// transition are observable and correct.
func TestArchivePublishReloadAcrossVoterRemoval(t *testing.T) {
	ctx := context.Background()

	// --- config1: two-member cluster ---
	transport := newArchiveTestCluster(t, []quepaxa.Member{{ID: "n1"}, {ID: "n2"}})
	n1 := transport.cores["n1"]

	// Propose five values before reconfiguration (slots 1-5).
	for i := 0; i < 5; i++ {
		if _, _, err := n1.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}

	// --- archive publish (configID = 1, before-ack) ---
	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	if err := writer.SyncThrough(ctx, n1, n1.Tip()); err != nil {
		t.Fatal(err)
	}

	// Reload and confirm archive identity is 1.
	loader := NewManager(bucket, "cluster", 1)
	if err := loader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if loader.configID != 1 {
		t.Fatalf("archive configID=%d, want 1", loader.configID)
	}
	if loader.Tip() != n1.Tip() {
		t.Fatalf("archive tip=%d, want %d", loader.Tip(), n1.Tip())
	}
	prefixBefore, _ := n1.PrefixHash(n1.Tip())

	// --- voter-removal reconfiguration config1 -> config2 ---
	cluster2 := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "n1"}}}
	if _, err := n1.BeginReconfiguration(ctx, cluster2); err != nil {
		t.Fatal(err)
	}
	if err := n1.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if got := n1.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 1 {
		t.Fatalf("cluster after reconfig: %+v", got)
	}

	// Propose three values in config2 (slots after the drain).
	for i := 0; i < 3; i++ {
		if _, _, err := n1.Propose(ctx, []byte{byte(i + 10)}); err != nil {
			t.Fatal(err)
		}
	}

	// --- archive sync-through across the reconfiguration boundary ---
	if err := writer.SyncThrough(ctx, n1, n1.Tip()); err != nil {
		t.Fatal(err)
	}

	// Reload again. Archive identity must still be 1.
	reloaded := NewManager(bucket, "cluster", 1)
	if err := reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reloaded.configID != 1 {
		t.Fatalf("reloaded archive configID=%d, want 1", reloaded.configID)
	}
	if reloaded.Tip() != n1.Tip() {
		t.Fatalf("reloaded archive tip=%d, want %d", reloaded.Tip(), n1.Tip())
	}

	// Verify every decision is retrievable from the archive.
	values, tip, err := reloaded.DecisionsFrom(ctx, 1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if tip != n1.Tip() {
		t.Fatalf("decisions tip=%d, want %d", tip, n1.Tip())
	}
	if int64(len(values)) != int64(n1.Tip()) {
		t.Fatalf("decisions count=%d, want %d", len(values), n1.Tip())
	}

	// --- replay into a passive observer ---
	passiveWAL, err := qlog.Open(filepath.Join(t.TempDir(), "passive"))
	if err != nil {
		t.Fatal(err)
	}
	defer passiveWAL.Close()
	passiveCore, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID:                "observer",
		Cluster:               quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}}},
		WAL:                   passiveWAL,
		EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Replay all archived decisions in order.
	for from := quepaxa.Slot(1); from <= tip; {
		batch, _, batchErr := reloaded.DecisionsFrom(ctx, from, 128)
		if batchErr != nil {
			t.Fatal(batchErr)
		}
		if len(batch) == 0 {
			break
		}
		if err := passiveCore.AcceptCertifiedValues(batch); err != nil {
			t.Fatalf("replay slot %d: %v", from, err)
		}
		from += quepaxa.Slot(len(batch))
	}

	// --- verify retained prefix and config after replay ---
	activePrefix, ok := n1.PrefixHash(n1.Tip())
	if !ok {
		t.Fatal("active prefix unavailable")
	}
	passivePrefix, ok := passiveCore.PrefixHash(passiveCore.Tip())
	if !ok {
		t.Fatal("passive prefix unavailable")
	}
	if activePrefix != passivePrefix {
		t.Fatalf("prefix mismatch after replay: active=%x passive=%x", activePrefix, passivePrefix)
	}
	if passiveCore.ConfigID() != 2 {
		t.Fatalf("passive configID=%d, want 2", passiveCore.ConfigID())
	}
	if passiveCore.Tip() != n1.Tip() {
		t.Fatalf("passive tip=%d, want %d", passiveCore.Tip(), n1.Tip())
	}

	// Prefix before reconfiguration must also be retained.
	passivePrefixAt5, ok := passiveCore.PrefixHash(5)
	if !ok {
		t.Fatal("passive prefix at slot 5 unavailable")
	}
	if passivePrefixAt5 != prefixBefore {
		t.Fatalf("prefix at slot 5 mismatch: active=%x passive=%x", prefixBefore, passivePrefixAt5)
	}
}
