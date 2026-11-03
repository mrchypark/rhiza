package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A private startup result, not an admission permit or a durable manifest.
// Its files are a fresh projection of one registered, certified WAL prefix.
type managedGraphStartupProjection struct {
	files       []materializer.CheckpointFile
	tip         quepaxa.Slot
	liveTip     quepaxa.Slot
	prefix      [32]byte
	walIdentity string
	floor       quepaxa.Slot
	floorRoot   [32]byte
	cut         materializer.ManagedGraphInstallCut
	cleanup     func() error
}

// Called only while Open owns the actual WAL lock and before opening the
// previous application files. An authenticated nonzero compaction floor can
// never fall back to genesis when its exact paired root is unavailable.
func (n *Node) reconstructManagedGraphStartup(ctx context.Context, core *quepaxa.Core) (result *managedGraphStartupProjection, resultErr error) {
	return n.reconstructManagedGraphStartupAt(ctx, core, nil)
}

func (n *Node) reconstructManagedGraphStartupAt(ctx context.Context, core *quepaxa.Core, expected *materializer.ManagedGraphInstallCut) (result *managedGraphStartupProjection, resultErr error) {
	if localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:pair_reconstruct")
	}
	if n.lock == nil || n.wal == nil || core == nil || n.core != core || n.managedStartup == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return nil, fmt.Errorf("managed Graph reconstruction requires locked startup")
	}
	local, err := loadVoterIdentity(n.config)
	if err != nil || local.identity == nil || !local.identity.Registered {
		return nil, errors.Join(fmt.Errorf("managed Graph reconstruction requires registered WAL identity"), err)
	}
	if core.WALIdentity() != local.identity.Nonce || core.GenerationAnchorHash() != ([32]byte{}) {
		return nil, fmt.Errorf("managed Graph reconstruction WAL binding or generation differs")
	}
	liveTip := core.Tip()
	tip := liveTip
	if expected != nil {
		if err := n.authenticateManagedGraphInstallCut(ctx, core, *expected); err != nil {
			return nil, err
		}
		tip = quepaxa.Slot(expected.CertifiedTip)
	}
	prefix, ok := core.PrefixHash(tip)
	if tip != 0 && !ok {
		return nil, fmt.Errorf("managed Graph certified target prefix unavailable")
	}
	seal, hasSeal, err := core.LatestCheckpointSeal()
	if err != nil {
		return nil, err
	}
	if !hasSeal {
		return nil, fmt.Errorf("managed Graph required recovery root is unavailable")
	}
	if expected != nil {
		seal, err = n.managedGraphSealForCut(ctx, core, *expected)
		if err != nil {
			return nil, err
		}
	}
	stage, err := os.MkdirTemp(n.config.DataDir, ".rhiza-managed-graph-reconstruction-*")
	if err != nil {
		return nil, err
	}
	var mat *materializer.Materializer
	var release func()
	cleanup := func() error {
		if release != nil {
			release()
			release = nil
		}
		var closeErr error
		if mat != nil {
			closeErr = mat.Close()
			mat = nil
		}
		return errors.Join(closeErr, os.RemoveAll(stage))
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, cleanup())
		}
	}()
	mat, err = materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(stage, "sqlite.db"), 1, n.receiptWindow())
	if err != nil {
		return nil, err
	}
	base := quepaxa.Slot(0)
	if hasSeal {
		if n.checkpoints == nil || seal.Index > tip || seal.Index < core.CompactionFloor() {
			return nil, fmt.Errorf("managed Graph checkpoint lies outside certified source")
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		decision, found := core.CertifiedValue(seal.DecisionSlot)
		if !found {
			return nil, fmt.Errorf("managed Graph checkpoint certificate is unavailable")
		}
		if err := core.ValidateCheckpointBase(ctx, seal.CheckpointSeal, decision); err != nil {
			return nil, err
		}
		dir := filepath.Join(stage, "checkpoint")
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		files, err := n.checkpoints.DownloadRootFiles(ctx, uint64(seal.Index), seal.RootHash, dir)
		if err != nil {
			return nil, err
		}
		if err := mat.RestoreCheckpoint(ctx, checkpointMaterializerFiles(files)); err != nil {
			return nil, err
		}
		base = seal.Index
	}
	values, err := certifiedSuffixThrough(core, base+1, tip)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value.Hash != quepaxa.ValueHash(sha256.Sum256(value.Value)) {
			return nil, fmt.Errorf("managed Graph certified suffix full hash differs at slot %d", value.Slot)
		}
	}
	if err := mat.ApplyBatch(ctx, values); err != nil {
		return nil, err
	}
	if mat.Tip() != uint64(tip) {
		return nil, fmt.Errorf("managed Graph reconstruction did not reach certified ACK target")
	}
	if err := mat.ProjectionReadable(ctx); err != nil {
		return nil, err
	}
	history, err := mat.ManagedGraphHistory(ctx)
	if err != nil {
		return nil, err
	}
	files, actual, releaseFiles, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		return nil, err
	}
	release = releaseFiles
	if actual != uint64(tip) || core.Tip() != liveTip {
		return nil, fmt.Errorf("managed Graph reconstruction source moved")
	}
	for i := range files {
		info, err := os.Lstat(files[i].Path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			return nil, errors.Join(fmt.Errorf("managed Graph reconstruction snapshot is not a regular nonempty file"), err)
		}
		file, err := os.Open(files[i].Path)
		if err != nil {
			return nil, err
		}
		hash := sha256.New()
		size, readErr := io.Copy(hash, contextReader{ctx: ctx, reader: file})
		if err := errors.Join(readErr, file.Close(), ctx.Err()); err != nil {
			return nil, err
		}
		if size != info.Size() {
			return nil, fmt.Errorf("managed Graph reconstruction snapshot size changed")
		}
		files[i].ExpectedLength = uint64(size)
		files[i].ExpectedSHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	}
	if err := mat.Close(); err != nil {
		mat = nil
		return nil, err
	}
	mat = nil
	binding, walBinding, err := n.managedGraphInstallBinding(core)
	if err != nil {
		return nil, err
	}
	floor, floorRoot, _ := core.RecoveryRoot()
	cut := materializer.ManagedGraphInstallCut{BindingHash: binding, WALBindingHash: walBinding, ConfigID: core.ConfigID(), Generation: uint64(n.managedStartup.Identity.Member.Generation), CheckpointSlot: uint64(seal.Index), CheckpointRoot: seal.RootHash, CheckpointStateHash: seal.StateHash, CheckpointPrefixHash: seal.PrefixHash, CertifiedTip: uint64(tip), CertifiedPrefixHash: prefix, ExpectedGraphHistory: history, IdempotencyWindow: n.receiptWindow()}
	if expected != nil && cut != *expected {
		return nil, fmt.Errorf("retained managed Graph cut differs from actual certified reconstruction")
	}
	return &managedGraphStartupProjection{files: files, tip: tip, liveTip: liveTip, prefix: prefix, walIdentity: core.WALIdentity(), floor: floor, floorRoot: floorRoot, cut: cut, cleanup: cleanup}, nil
}

func (n *Node) managedGraphInstallBinding(core *quepaxa.Core) ([32]byte, [32]byte, error) {
	bound, err := json.Marshal(struct {
		Identity ManagedStartupIdentity
		Policy   sqlpolicy.MaterializationPolicy
		Window   uint64
	}{n.managedStartup.Identity, n.materializationPolicy(), n.receiptWindow()})
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	wal, err := json.Marshal(struct {
		Identity string
		Cluster  quepaxa.Cluster
	}{core.WALIdentity(), core.CurrentCluster()})
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("rhiza/managed-graph-install-binding/v1\x00"), bound...)), sha256.Sum256(append([]byte("rhiza/managed-graph-install-wal/v1\x00"), wal...)), nil
}

func (n *Node) installManagedGraphStartup(ctx context.Context, core *quepaxa.Core, observed materializer.ManagedGraphTargetState) (opened *materializer.Materializer, resultErr error) {
	if observed.InstallAttempt != nil {
		return n.resumeManagedGraphStartupInstall(ctx, core, observed)
	}
	if observed.SQLiteTip > uint64(core.Tip()) {
		return nil, fmt.Errorf("previous managed Graph application tip exceeds authenticated Core")
	}
	projection, err := n.reconstructManagedGraphStartup(ctx, core)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, projection.cleanup())
		if resultErr != nil && opened != nil {
			resultErr = errors.Join(resultErr, opened.Close())
			opened = nil
		}
	}()
	if err := n.managedStartup.Checker.ValidateManagedStartup(ctx, n.managedStartup.Identity); err != nil {
		return nil, err
	}
	if err := n.validateManagedGraphProjectionCut(core, projection); err != nil {
		return nil, err
	}
	if len(observed.Replay) != 0 {
		if err := n.reconcileManagedGraphTarget(ctx, core, projection, observed); err != nil {
			return nil, err
		}
	}
	if err := n.validateManagedGraphProjectionCut(core, projection); err != nil {
		return nil, err
	}
	if localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:pair_install")
	}
	return materializer.OpenManagedGraphFromVerifiedPair(ctx, filepath.Join(n.config.DataDir, "sqlite.db"), 4, materializer.ManagedGraphInstall{Cut: projection.cut, Files: projection.files})
}

func (n *Node) validateManagedGraphProjectionCut(core *quepaxa.Core, projection *managedGraphStartupProjection) error {
	bound, walBound, err := n.managedGraphInstallBinding(core)
	if err != nil {
		return err
	}
	floor, root, _ := core.RecoveryRoot()
	prefix, valid := core.PrefixHash(projection.tip)
	identity, err := loadVoterIdentity(n.config)
	if err != nil || identity.identity == nil || !identity.identity.Registered || identity.identity.Nonce != projection.walIdentity || n.core != core || n.lock == nil || core.WALIdentity() != projection.walIdentity || core.Tip() != projection.liveTip || !valid || prefix != projection.prefix || floor != projection.floor || root != projection.floorRoot || bound != projection.cut.BindingHash || walBound != projection.cut.WALBindingHash || core.ConfigID() != projection.cut.ConfigID {
		return errors.Join(fmt.Errorf("actual managed Graph Core/source cut changed before installation"), err)
	}
	return nil
}

// Journal metadata selects a historical cut; it never authenticates that cut.
func (n *Node) authenticateManagedGraphInstallCut(ctx context.Context, core *quepaxa.Core, cut materializer.ManagedGraphInstallCut) error {
	if n.lock == nil || n.wal == nil || n.core != core || n.managedStartup == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || cut.Generation != uint64(n.managedStartup.Identity.Member.Generation) || cut.Generation != 0 || cut.IdempotencyWindow != n.receiptWindow() || cut.CheckpointSlot == 0 || cut.CheckpointSlot >= cut.CertifiedTip || cut.CertifiedTip > uint64(core.Tip()) || cut.ConfigID != core.ConfigID() {
		return fmt.Errorf("retained managed Graph cut is outside the locked runtime")
	}
	identity, err := loadVoterIdentity(n.config)
	if err != nil || identity.identity == nil || !identity.identity.Registered || identity.identity.Nonce != core.WALIdentity() {
		return errors.Join(fmt.Errorf("retained managed Graph cut lacks registered WAL identity"), err)
	}
	bound, walBound, err := n.managedGraphInstallBinding(core)
	prefix, valid := core.PrefixHash(quepaxa.Slot(cut.CertifiedTip))
	basePrefix, baseValid := core.PrefixHash(quepaxa.Slot(cut.CheckpointSlot))
	if err != nil || !valid || !baseValid || prefix != cut.CertifiedPrefixHash || basePrefix != cut.CheckpointPrefixHash || bound != cut.BindingHash || walBound != cut.WALBindingHash || core.GenerationAnchorHash() != ([32]byte{}) {
		return errors.Join(fmt.Errorf("retained managed Graph cut binding or certified prefix differs"), err)
	}
	_, err = n.managedGraphSealForCut(ctx, core, cut)
	return err
}

func (n *Node) managedGraphSealForCut(ctx context.Context, core *quepaxa.Core, cut materializer.ManagedGraphInstallCut) (quepaxa.SealedCheckpoint, error) {
	values, err := certifiedSuffixThrough(core, quepaxa.Slot(cut.CheckpointSlot)+1, quepaxa.Slot(cut.CertifiedTip))
	if err != nil || n.checkpoints == nil {
		return quepaxa.SealedCheckpoint{}, errors.Join(fmt.Errorf("retained managed Graph seal history unavailable"), err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	for _, value := range values {
		seal, isSeal, err := quepaxa.DecodeCheckpointSeal(value.Value)
		if err != nil || sha256.Sum256(value.Value) != value.Hash {
			return quepaxa.SealedCheckpoint{}, errors.Join(fmt.Errorf("retained managed Graph certified value differs"), err)
		}
		if !isSeal || uint64(seal.Index) != cut.CheckpointSlot || seal.RootHash != cut.CheckpointRoot || seal.StateHash != cut.CheckpointStateHash || seal.PrefixHash != cut.CheckpointPrefixHash {
			continue
		}
		if err := core.ValidateCheckpointBase(ctx, seal, value); err != nil {
			return quepaxa.SealedCheckpoint{}, err
		}
		return quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: value.Slot}, nil
	}
	return quepaxa.SealedCheckpoint{}, fmt.Errorf("retained managed Graph cut has no exact certified producer seal")
}

func (n *Node) resumeManagedGraphStartupInstall(ctx context.Context, core *quepaxa.Core, observed materializer.ManagedGraphTargetState) (opened *materializer.Materializer, resultErr error) {
	attempt := observed.InstallAttempt
	if err := n.managedStartup.Checker.ValidateManagedStartup(ctx, n.managedStartup.Identity); err != nil {
		return nil, err
	}
	projection, err := n.reconstructManagedGraphStartupAt(ctx, core, &attempt.Cut)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, projection.cleanup())
		if resultErr != nil && opened != nil {
			resultErr = errors.Join(resultErr, opened.Close())
			opened = nil
		}
	}()
	if err := n.validateManagedGraphProjectionCut(core, projection); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(n.config.DataDir, "sqlite.db")
	current, err := materializer.InspectManagedGraphInstallTarget(ctx, dbPath, n.receiptWindow())
	if err != nil || current.InstallAttempt == nil || current.InventoryHash != observed.InventoryHash {
		return nil, errors.Join(fmt.Errorf("retained managed Graph install changed before authenticated resume"), err)
	}
	// These are the observed fixed owned input files, not freshly reconstructed
	// physical snapshots. The reconstruction above authenticates original C/T.
	if localtesthooks.Enabled {
		localtesthooks.Hit("node:managed-graph-startup:pair_install")
	}
	opened, err = materializer.OpenManagedGraphFromVerifiedPair(ctx, dbPath, 4, materializer.ManagedGraphInstall{Cut: attempt.Cut, Files: current.InstallAttempt.Files})
	if err != nil {
		return nil, err
	}
	values, err := certifiedSuffixThrough(core, projection.tip+1, projection.liveTip)
	if err == nil {
		err = opened.ApplyBatch(ctx, values)
	}
	if err == nil {
		err = opened.ProjectionReadable(ctx)
	}
	if err != nil || opened.Tip() != uint64(projection.liveTip) {
		return nil, errors.Join(fmt.Errorf("retained managed Graph install did not reach current certified suffix"), err, opened.Close())
	}
	return opened, nil
}

// The old pair is opened only after every observed native decision is tied to
// this locked Core's certified full value. Replaying those same slots finishes
// its SQLite fold; an independently reconstructed temporary pair cannot hide
// an unknown old effect or a commit-unknown failure.
func (n *Node) reconcileManagedGraphTarget(ctx context.Context, core *quepaxa.Core, projection *managedGraphStartupProjection, observed materializer.ManagedGraphTargetState) error {
	if observed.InstallAttempt != nil || observed.Absent {
		return fmt.Errorf("ambiguous managed Graph target cannot be reconciled")
	}
	for _, record := range observed.Replay {
		value, found := core.CertifiedValue(quepaxa.Slot(record.Slot))
		if !found || record.Slot <= observed.SQLiteTip || record.Slot > uint64(projection.tip) || value.Hash != record.DecisionHash || sha256.Sum256(value.Value) != value.Hash {
			return fmt.Errorf("previous native Graph slot %d lacks matching certified full decision", record.Slot)
		}
	}
	dbPath := filepath.Join(n.config.DataDir, "sqlite.db")
	current, err := materializer.InspectManagedGraphInstallTarget(ctx, dbPath, n.receiptWindow())
	if err != nil || current.InventoryHash != observed.InventoryHash {
		return errors.Join(fmt.Errorf("previous managed Graph target changed before certified reconciliation"), err)
	}
	values, err := certifiedSuffixThrough(core, quepaxa.Slot(observed.SQLiteTip)+1, projection.tip)
	if err != nil {
		return err
	}
	old, err := materializer.OpenManagedSQLKVGraphWithOriginals(dbPath, 1, n.receiptWindow())
	if err != nil {
		return err
	}
	applyErr := old.ApplyBatch(ctx, values)
	if applyErr == nil {
		applyErr = old.ProjectionReadable(ctx)
	}
	if applyErr == nil {
		history, err := old.ManagedGraphHistory(ctx)
		if err != nil || old.Tip() != uint64(projection.tip) || history != projection.cut.ExpectedGraphHistory {
			applyErr = errors.Join(fmt.Errorf("certified old Graph reconciliation differs from authenticated projection"), err)
		}
	}
	return errors.Join(applyErr, old.Close())
}
