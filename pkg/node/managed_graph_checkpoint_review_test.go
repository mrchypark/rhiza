package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// This in-process transport calls actual durable Core recorder APIs. It stores
// no synthetic ledger and does not replace the separate QUIC/Host qualification.
type graphReviewTransport map[quepaxa.NodeID]*quepaxa.Core

func (p graphReviewTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return p[to].Record(ctx, r)
}

func TestManagedGraphPeerCheckpointCleanupPreservesFailure(t *testing.T) {
	prior := errors.New("prior prepare failure")
	dir := t.TempDir() + "/owned-stage"
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshot"), []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := cleanupManagedGraphCheckpointStage(dir, prior); !errors.Is(err, prior) {
		t.Fatal("cleanup lost original failure", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("owned stage remains after successful cleanup", err)
	}
	// An invalid child of the owned directory forces the actual os.RemoveAll
	// error without chmod of shared paths or replacing the cleanup function.
	bad := t.TempDir() + "/invalid\x00owned-stage"
	err := cleanupManagedGraphCheckpointStage(bad, prior)
	var pathError *os.PathError
	if !errors.Is(err, prior) || !errors.As(err, &pathError) {
		t.Fatal("cleanup failure or original cause disappeared", err)
	}
}

func TestManagedGraphCheckpointUsesCutSuccessorMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := graphReviewCores(t, true)
	core := peers["n1"]
	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}}}
	if _, err := core.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	terminal := core.Tip()
	n := New(&types.ExecutionConfig{EnableReconfiguration: true})
	n.core = core
	if core.ClusterForSlot(terminal).ConfigID != 1 || core.ClusterForSlot(terminal+1).ConfigID != 2 {
		t.Fatal("actual terminal boundary absent")
	}
	if got := n.managedGraphCheckpointCluster(terminal); got.ConfigID != 2 {
		t.Fatalf("checkpoint certifying membership came from cut rather than cut+1: got=%d", got.ConfigID)
	}
	// Core intentionally refuses a checkpoint ON the terminal itself. The
	// correct selector must not be confused with weakening that independent rule.
	if _, err := core.CheckpointMembership(terminal); err == nil {
		t.Fatal("terminal boundary unexpectedly became checkpoint-eligible")
	}
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, types.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	cut := core.Tip()
	history, err := core.CheckpointMembership(cut)
	if err != nil {
		t.Fatal("actual stable post-terminal cut rejected", err)
	}
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	decisions, _, err := core.DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	if err := mat.ApplyBatch(ctx, decisions); err != nil {
		t.Fatal(err)
	}
	files, actualCut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if actualCut != uint64(cut) {
		t.Fatal("actual pair cut differs")
	}
	sources := make([]checkpoint.Source, len(files))
	for i, f := range files {
		sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
	}
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(objstore.NewInMemBucket(), "membership", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, actualCut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(cut)
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 2, Index: cut, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &history}
	core.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	n.checkpoints = cp
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy}
	if err := n.prepareManagedGraphPeerCheckpoint(ctx, "n2", seal); err != nil {
		t.Fatal("valid current voter post-terminal prepare failed", err)
	}
	if err := n.prepareManagedGraphPeerCheckpoint(ctx, "n3", seal); err == nil {
		t.Fatal("retired old-config voter accepted")
	}
	wrong := seal
	wrong.ConfigID = 1
	if err := n.prepareManagedGraphPeerCheckpoint(ctx, "n2", wrong); err == nil {
		t.Fatal("wrong certifying config accepted")
	}
}
func (p graphReviewTransport) SendDecision(_ context.Context, d quepaxa.Decision) error {
	for _, c := range p {
		if err := c.AcceptDecision(d); err != nil {
			return err
		}
	}
	return nil
}
func (p graphReviewTransport) ReadTip(_ context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	return p[id].Tip(), nil
}
func (p graphReviewTransport) StageValue(_ context.Context, id quepaxa.NodeID, h quepaxa.ValueHash, v []byte) error {
	return p[id].StageValue(h, v)
}
func (p graphReviewTransport) FetchValue(_ context.Context, id quepaxa.NodeID, h quepaxa.ValueHash) ([]byte, error) {
	v, ok := p[id].Value(h)
	if !ok {
		return nil, fmt.Errorf("actual staged value missing")
	}
	return v, nil
}

type graphReviewCertificateSource struct {
	*quepaxa.Core
	slot        quepaxa.Slot
	certificate []byte
}

func (s graphReviewCertificateSource) DecisionsFromBounded(from quepaxa.Slot, count, budget int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	page, tip, err := s.Core.DecisionsFromBounded(from, count, budget)
	for i := range page {
		if page[i].Slot == s.slot {
			page[i].Certificate = bytes.Clone(s.certificate)
		}
	}
	return page, tip, err
}

func graphReviewCores(t *testing.T, reconfiguration bool) graphReviewTransport {
	t.Helper()
	peers := graphReviewTransport{}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	for _, m := range cluster.Members {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: cluster, WAL: wal, Transport: peers, EnableReconfiguration: reconfiguration, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
		if err != nil {
			t.Fatal(err)
		}
		peers[m.ID] = core
	}
	return peers
}

func TestManagedGraphPromotionAcceptsReorderedValidCertificate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := graphReviewCores(t, false)
	core := peers["n1"]
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "certificate", Cypher: "CREATE (:CertificateCI {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if err := mat.Apply(ctx, uint64(slot), value); err != nil {
		t.Fatal(err)
	}
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	sources := make([]checkpoint.Source, len(files))
	for i, f := range files {
		sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
	}
	bucket := objstore.NewInMemBucket()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "certificate", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(slot)
	next, following, err := core.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: slot, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	for _, c := range peers {
		c.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
			return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
		})
		if err := c.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	d, ok := core.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("actual seal missing")
	}
	var certificate map[string]json.RawMessage
	if err := json.Unmarshal(d.Certificate, &certificate); err != nil {
		t.Fatal(err)
	}
	var summaries []json.RawMessage
	if err := json.Unmarshal(certificate["summaries"], &summaries); err != nil || len(summaries) < 2 {
		t.Fatal("actual quorum summaries absent", err)
	}
	for i, j := 0, len(summaries)-1; i < j; i, j = i+1, j-1 {
		summaries[i], summaries[j] = summaries[j], summaries[i]
	}
	certificate["summaries"], err = json.Marshal(summaries)
	if err != nil {
		t.Fatal(err)
	}
	other := d
	other.Certificate, err = json.Marshal(certificate)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(other.Certificate, d.Certificate) {
		t.Fatal("certificate representation did not change")
	}
	if err := core.ValidateCheckpointBase(ctx, seal, d); err != nil {
		t.Fatal("local certificate invalid", err)
	}
	if err := core.ValidateCheckpointBase(ctx, seal, other); err != nil {
		t.Fatal("reordered certificate invalid", err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "certificate", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.SyncThrough(ctx, graphReviewCertificateSource{Core: core, slot: sealSlot, certificate: other.Certificate}, sealSlot); err != nil {
		t.Fatal(err)
	}
	actual, _, err := archive.ObserveManagedGraphDecision(ctx, sealSlot)
	if err != nil || !bytes.Equal(actual.Certificate, other.Certificate) {
		t.Fatal("archive did not retain alternate valid certificate", err)
	}
	n := New(&types.ExecutionConfig{})
	n.core, n.checkpoints, n.archive = core, cp, archive
	sealed, exists, err := core.LatestCheckpointSeal()
	if err != nil || !exists {
		t.Fatal(err)
	}
	if err := n.promoteManagedGraphSeal(ctx, sealed); err != nil {
		t.Fatal("valid reordered certificate blocked promotion", err)
	}
	after, _, err := archive.ObserveManagedGraphDecision(ctx, sealSlot)
	if err != nil || !bytes.Equal(after.Certificate, other.Certificate) {
		t.Fatal("promotion rewrote archived certificate", err)
	}
	bad := other
	certificate["summaries"], _ = json.Marshal(summaries[:1])
	bad.Certificate, _ = json.Marshal(certificate)
	if err := n.validateManagedGraphArchivedSeal(ctx, sealed, d, bad, sealSlot); err == nil {
		t.Fatal("insufficient archived quorum certificate accepted")
	}
}
