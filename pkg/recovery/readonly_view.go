package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const maxReadOnlyDecisions = 1 << 20
const maxReadOnlyPayloadBytes = 256 << 20

// ReadOnlyView is verified candidate data, not an activation or retention proof.
// The source must remain append-only for the lifetime of the view.
type ReadOnlyView struct {
	headHash, prefixHash [32]byte
	headBytes            []byte
	baseSeal             []byte
	baseDecision         []byte
	base, tip            quepaxa.Slot
	decisions            []quepaxa.DecidedValue
	checkpointManager    *checkpoint.Manager
	root                 *checkpoint.Checkpoint
	verified             bool
}

// OpenReadOnlyView captures one stable head and verifies its complete suffix.
func OpenReadOnlyView(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster) (result *ReadOnlyView, resultErr error) {
	return openReadOnlyView(ctx, bucket, prefix, configID, bootstrap)
}

// OpenReadOnlyViewAtHead verifies the captured head and its immutable source
// objects, independently of subsequent changes to the live HEAD. The caller
// must establish the captured head's provenance and source binding separately.
func OpenReadOnlyViewAtHead(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, capturedHead []byte) (*ReadOnlyView, error) {
	if bucket == nil || len(capturedHead) == 0 || len(capturedHead) > maxArchiveHeadSize {
		return nil, fmt.Errorf("invalid captured archive head")
	}
	return openReadOnlyView(ctx, &capturedHeadBucket{Bucket: bucket, name: path.Join(prefix, "archive/head.bin"), head: bytes.Clone(capturedHead)}, prefix, configID, bootstrap)
}

// Only the mutable HEAD is substituted. Every immutable object is fetched from
// the actual source and validated by the ordinary archive verifier.
type capturedHeadBucket struct {
	objstore.Bucket
	name string
	head []byte
}

func (b *capturedHeadBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if name != b.name {
		return b.Bucket.Attributes(ctx, name)
	}
	if err := ctx.Err(); err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return objstore.ObjectAttributes{Size: int64(len(b.head)), Version: &objstore.ObjectVersion{Type: objstore.ETag, Value: fmt.Sprintf("%x", sha256.Sum256(b.head))}}, nil
}

func (b *capturedHeadBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name != b.name {
		return b.Bucket.Get(ctx, name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b.head)), nil
}

func openReadOnlyView(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster) (result *ReadOnlyView, resultErr error) {
	if bucket == nil || len(bootstrap.Members) == 0 {
		return nil, fmt.Errorf("read-only view requires a bucket and trusted bootstrap")
	}
	m := NewManager(bucket, prefix, configID)
	defer m.Close()
	if err := m.loadHead(ctx, make(map[extentObject]Extent), true); err != nil {
		return nil, err
	}
	head := m.head
	encoded, err := encodeHead(head)
	if err != nil {
		return nil, err
	}
	v := &ReadOnlyView{headHash: sha256.Sum256(encoded), headBytes: encoded, base: head.Base, tip: head.Tip}
	cp := checkpoint.NewManager(bucket, prefix, "", configID)
	v.checkpointManager = cp
	snapshot := &RecoverySnapshot{manager: m, head: head, refs: m.extents}
	seal, decision, hasCheckpoint := snapshot.RecoveryBase()
	if hasCheckpoint {
		v.baseSeal, err = json.Marshal(seal)
		if err != nil {
			return nil, err
		}
		v.baseDecision, err = json.Marshal(decision)
		if err != nil {
			return nil, err
		}
	}
	anchorHash, anchored := snapshot.GenerationAnchor()
	var anchor GenerationAnchor
	if anchored {
		stored, hash, err := ReadGenerationAnchor(ctx, bucket, prefix)
		if err != nil || hash != anchorHash {
			return nil, fmt.Errorf("source anchored lineage does not match archive")
		}
		membership := NewMembershipRecord(stored.TargetMembership.Cluster, bootstrap.Members, stored.TargetMembership.Durability)
		anchor, err = verifyGenerationAnchor(ctx, bucket, prefix, membership, anchorHash, &head)
		if err != nil {
			return nil, fmt.Errorf("verify source lineage: %w", err)
		}
	}
	if hasCheckpoint {
		v.root, err = cp.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
		if err == nil && v.root.Hash != seal.StateHash {
			err = fmt.Errorf("checkpoint state hash mismatch")
		}
	} else if anchored {
		rootHash, hashErr := parseGenerationHash(anchor.Checkpoint.RootHash)
		if hashErr != nil {
			return nil, hashErr
		}
		v.root, err = cp.OpenRoot(ctx, anchor.Checkpoint.Index, rootHash)
		if err == nil && v.root.Hash != mustGenerationHash(anchor.Checkpoint.StateHash) {
			err = fmt.Errorf("anchor state hash mismatch")
		}
	} else if head.Base != 0 {
		return nil, fmt.Errorf("unsupported archive recovery base")
	}
	if err != nil {
		return nil, err
	}
	if v.root != nil {
		if err := cp.Verify(ctx, v.root.Index, v.root.RootHash, v.root.Hash); err != nil {
			return nil, err
		}
	}
	dir, err := os.MkdirTemp("", "rhiza-readonly-verify-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			result = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("remove verification directory %s: %w", dir, err))
		}
	}()
	wal, err := qlog.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := wal.Close(); err != nil {
			result = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("close verification WAL: %w", err))
		}
	}()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "rhiza-readonly-verifier", Cluster: bootstrap, WAL: wal, EnableReconfiguration: !hasCheckpoint || seal.Membership != nil})
	if err != nil {
		return nil, err
	}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if anchored {
		if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{ConfigID: bootstrap.ConfigID, Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: mustGenerationHash(anchor.SourcePrefixHash), RootHash: mustGenerationHash(anchor.Checkpoint.RootHash), AnchorHash: anchorHash}); err != nil {
			return nil, err
		}
	}
	if hasCheckpoint {
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return nil, err
		}
	}
	bytes := 0
	for core.Tip() < head.Tip {
		values, _, err := snapshot.DecisionsFrom(ctx, core.Tip()+1, 256)
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("archive suffix ends before head")
		}
		for _, value := range values {
			bytes += len(value.Value) + len(value.Certificate)
			if bytes > maxReadOnlyPayloadBytes {
				return nil, fmt.Errorf("read-only archive suffix exceeds byte limit")
			}
		}
		if err := core.AcceptCertifiedValues(values); err != nil {
			return nil, err
		}
		v.decisions = append(v.decisions, values...)
	}
	v.prefixHash, _ = core.PrefixHash(core.Tip())
	expected := head.BasePrefix
	if len(m.extents) != 0 {
		expected = m.extents[len(m.extents)-1].EndPrefix
	}
	if v.prefixHash != expected {
		return nil, fmt.Errorf("verified archive prefix mismatch")
	}
	v.verified = true
	return v, nil
}

func (v *ReadOnlyView) HeadHash() [32]byte   { return v.headHash }
func (v *ReadOnlyView) Base() quepaxa.Slot   { return v.base }
func (v *ReadOnlyView) Tip() quepaxa.Slot    { return v.tip }
func (v *ReadOnlyView) PrefixHash() [32]byte { return v.prefixHash }

// CapturedHead returns the canonical encoded head verified by this view. It is
// evidence to reverify, not authority or a substitute for a live source check.
func (v *ReadOnlyView) CapturedHead() []byte {
	return append([]byte(nil), v.headBytes...)
}

// RecoveryBase returns independent copies of the certified checkpoint base.
func (v *ReadOnlyView) RecoveryBase() (quepaxa.CheckpointSeal, quepaxa.DecidedValue, bool) {
	var seal quepaxa.CheckpointSeal
	var decision quepaxa.DecidedValue
	if len(v.baseSeal) == 0 || json.Unmarshal(v.baseSeal, &seal) != nil || json.Unmarshal(v.baseDecision, &decision) != nil {
		return seal, decision, false
	}
	return seal, decision, true
}

// Decisions returns an independent copy of the verified bounded suffix.
func (v *ReadOnlyView) Decisions() []quepaxa.DecidedValue {
	out := make([]quepaxa.DecidedValue, len(v.decisions))
	for i, d := range v.decisions {
		out[i] = d
		out[i].Value = append([]byte(nil), d.Value...)
		out[i].Certificate = append([]byte(nil), d.Certificate...)
	}
	return out
}

// DownloadCheckpointFiles downloads and verifies the captured root into dir.
// A nil result means that the archive has no checkpoint base.
func (v *ReadOnlyView) DownloadCheckpointFiles(ctx context.Context, dir string) ([]checkpoint.File, error) {
	if v.root == nil {
		return nil, nil
	}
	return v.checkpointManager.DownloadAndVerifyRootFiles(ctx, v.root, dir)
}
