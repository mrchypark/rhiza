package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// ForkOptions identifies an offline, externally fenced source generation and
// an empty target generation. Fork does not establish that fence itself.
type ForkOptions struct {
	SourcePrefix string
	TargetPrefix string
	Members      []quepaxa.Member
	OperationID  string
}

type ForkResult struct {
	Tip          uint64 `json:"tip"`
	PrefixHash   string `json:"prefix_hash"`
	ManifestHash string `json:"manifest_hash"`
}

type forkIntent struct {
	Version      int      `json:"version"`
	OperationID  string   `json:"operation_id"`
	SourcePrefix string   `json:"source_prefix"`
	TargetPrefix string   `json:"target_prefix"`
	MemberIDs    []string `json:"member_ids"`
	ManifestHash string   `json:"manifest_hash"`
}

const forkLease = 10 * time.Minute

// Fork copies a verified, pinned archive generation and its recovery
// checkpoint into an isolated target prefix. The caller must fence every old
// writer before calling it and must not start the target until it returns.
func Fork(ctx context.Context, bucket objstore.Bucket, options ForkOptions) (ForkResult, error) {
	if bucket == nil || !supportsCAS(bucket) {
		return ForkResult{}, fmt.Errorf("generation fork requires conditional object writes")
	}
	sourcePrefix, targetPrefix, err := forkPrefixes(options.SourcePrefix, options.TargetPrefix)
	if err != nil {
		return ForkResult{}, err
	}
	if options.OperationID == "" || !validForkMemberIDs(forkMemberIDs(options.Members)) {
		return ForkResult{}, fmt.Errorf("fork operation ID and members are required")
	}
	existing, hasIntent, err := readForkIntent(ctx, bucket, targetPrefix)
	if err != nil {
		return ForkResult{}, err
	}
	if hasIntent {
		if !sameForkRequest(existing, options, sourcePrefix, targetPrefix) {
			return ForkResult{}, fmt.Errorf("fork target belongs to a different operation")
		}
		if result, ok, err := readForkResult(ctx, bucket, targetPrefix); err != nil || ok {
			if err != nil {
				return ForkResult{}, err
			}
			if result.ManifestHash != existing.ManifestHash {
				return ForkResult{}, fmt.Errorf("fork result does not match intent")
			}
			if err := verifyCachedForkResult(ctx, bucket, targetPrefix, options.OperationID, options.Members, result); err != nil {
				return ForkResult{}, fmt.Errorf("verify cached fork result: %w", err)
			}
			return result, nil
		}
	} else if err := ensureForkTargetEmpty(ctx, bucket, targetPrefix); err != nil {
		return ForkResult{}, err
	}

	source := NewManager(bucket, sourcePrefix, 1)
	defer source.Close()
	if err := source.Load(ctx); err != nil {
		return ForkResult{}, fmt.Errorf("load source archive: %w", err)
	}
	owner := "fork-" + shortHash(options.OperationID)
	snapshot, err := source.BeginRecoverySnapshot(ctx, owner, forkLease)
	if err != nil {
		return ForkResult{}, fmt.Errorf("pin source archive: %w", err)
	}
	defer closeSnapshot(snapshot)
	if !snapshot.head.Sealed {
		return ForkResult{}, fmt.Errorf("source archive must be sealed before generation fork")
	}
	seal, decision, ok := snapshot.RecoveryBase()
	if snapshot.Tip() == 0 {
		return ForkResult{}, fmt.Errorf("source archive has no certified decisions")
	}
	var sourceCheckpoint *checkpoint.Manager
	var root *checkpoint.Checkpoint
	var rootPin *checkpoint.RecoveryPin
	if ok {
		sourceCheckpoint = checkpoint.NewManager(bucket, sourcePrefix, "", 1)
		root, err = sourceCheckpoint.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return ForkResult{}, fmt.Errorf("open source checkpoint: %w", err)
		}
		if err := sourceCheckpoint.Verify(ctx, root.Index, root.RootHash, seal.StateHash); err != nil {
			return ForkResult{}, fmt.Errorf("verify source checkpoint: %w", err)
		}
		rootPin, err = sourceCheckpoint.PinRecoveryRoot(ctx, root, owner, forkLease)
		if err != nil {
			return ForkResult{}, fmt.Errorf("pin source checkpoint: %w", err)
		}
		defer closeRootPin(rootPin)
	}
	guard := newForkEvidenceGuard(ctx, snapshot, rootPin)
	defer guard.Close()
	ctx = guard.Context()

	headData, err := encodeHead(snapshot.head)
	if err != nil {
		return ForkResult{}, err
	}
	manifestHash := sha256.Sum256(headData)
	intent := forkIntent{Version: 1, OperationID: options.OperationID, SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix, MemberIDs: forkMemberIDs(options.Members), ManifestHash: hex.EncodeToString(manifestHash[:])}
	if hasIntent && existing.ManifestHash != intent.ManifestHash {
		return ForkResult{}, fmt.Errorf("source archive changed since fork intent was recorded")
	}
	if err := acquireForkIntent(ctx, bucket, targetPrefix, intent); err != nil {
		return ForkResult{}, err
	}

	prefixHash, err := verifyForkSnapshot(ctx, snapshot, seal, decision, sourceCheckpoint, options.Members)
	if err != nil {
		return ForkResult{}, err
	}
	if root != nil {
		if err := checkpoint.CopyRoot(ctx, bucket, sourcePrefix, targetPrefix, root); err != nil {
			return ForkResult{}, fmt.Errorf("copy checkpoint: %w", err)
		}
	}
	for _, ref := range snapshot.refs {
		if err := copyArchiveObject(ctx, bucket, source.key(extentObjectKey(ref.hash, ref.object)), forkKey(targetPrefix, extentObjectKey(ref.hash, ref.object)), ref.hash, maxExtentSize); err != nil {
			return ForkResult{}, fmt.Errorf("copy archive extent: %w", err)
		}
	}
	targetHead := snapshot.head
	targetHead.Sealed = false
	targetHeadData, err := encodeHead(targetHead)
	if err != nil {
		return ForkResult{}, fmt.Errorf("encode fork archive head: %w", err)
	}
	if err := writeForkArchiveHead(ctx, bucket, forkKey(targetPrefix, "archive/head.bin"), targetHeadData); err != nil {
		return ForkResult{}, fmt.Errorf("copy archive head: %w", err)
	}
	if err := source.Load(ctx); err != nil || !archiveHeadsEqual(source.head, snapshot.head) {
		if err != nil {
			return ForkResult{}, fmt.Errorf("recheck source archive: %w", err)
		}
		return ForkResult{}, fmt.Errorf("source archive changed during fork")
	}
	if root != nil {
		targetCheckpoint := checkpoint.NewManager(bucket, targetPrefix, "", 1)
		copiedRoot, err := targetCheckpoint.OpenRoot(ctx, root.Index, root.RootHash)
		if err != nil {
			return ForkResult{}, fmt.Errorf("verify copied checkpoint: %w", err)
		}
		if err := targetCheckpoint.PromoteCertifiedCurrent(ctx, copiedRoot); err != nil {
			return ForkResult{}, fmt.Errorf("publish fork checkpoint: %w", err)
		}
	}
	result := ForkResult{Tip: uint64(snapshot.Tip()), PrefixHash: hex.EncodeToString(prefixHash[:]), ManifestHash: intent.ManifestHash}
	if err := writeForkResult(ctx, bucket, targetPrefix, result); err != nil {
		return ForkResult{}, err
	}
	if err := guard.Check(); err != nil {
		return ForkResult{}, fmt.Errorf("renew source fork evidence: %w", err)
	}
	return result, nil
}

type forkEvidenceGuard struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	err      error
	snapshot *RecoverySnapshot
	root     *checkpoint.RecoveryPin
}

func newForkEvidenceGuard(ctx context.Context, snapshot *RecoverySnapshot, root *checkpoint.RecoveryPin) *forkEvidenceGuard {
	work, cancel := context.WithCancel(ctx)
	g := &forkEvidenceGuard{ctx: work, cancel: cancel, done: make(chan struct{}), snapshot: snapshot, root: root}
	go func() {
		defer close(g.done)
		ticker := time.NewTicker(forkLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-work.Done():
				return
			case <-ticker.C:
				var err error
				if g.snapshot != nil {
					err = g.snapshot.Renew(work, forkLease)
				}
				if err == nil && g.root != nil {
					err = g.root.Renew(work, forkLease)
				}
				if err != nil {
					g.mu.Lock()
					g.err = err
					g.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return g
}

func (g *forkEvidenceGuard) Context() context.Context { return g.ctx }
func (g *forkEvidenceGuard) Check() error             { g.mu.Lock(); defer g.mu.Unlock(); return g.err }
func (g *forkEvidenceGuard) Close()                   { g.cancel(); <-g.done }

func verifyForkSnapshot(ctx context.Context, snapshot *RecoverySnapshot, seal quepaxa.CheckpointSeal, decision quepaxa.DecidedValue, checkpoints *checkpoint.Manager, members []quepaxa.Member) ([32]byte, error) {
	var zero [32]byte
	dir, err := os.MkdirTemp("", "rhiza-fork-verify-*")
	if err != nil {
		return zero, err
	}
	defer os.RemoveAll(dir)
	wal, err := qlog.Open(dir)
	if err != nil {
		return zero, err
	}
	defer wal.Close()
	observerID := quepaxa.NodeID("rhiza-fork-observer")
	for _, member := range members {
		if member.ID == observerID {
			return zero, fmt.Errorf("fork observer ID collides with member")
		}
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: observerID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal})
	if err != nil {
		return zero, err
	}
	if checkpoints != nil {
		core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
			return checkpoints.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
		})
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return zero, err
		}
	}
	for core.Tip() < snapshot.Tip() {
		values, _, err := snapshot.DecisionsFrom(ctx, core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("source archive gap at %d", core.Tip()+1)
			}
			return zero, err
		}
		if err := core.AcceptCertifiedValues(values); err != nil {
			return zero, err
		}
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		return zero, fmt.Errorf("fork verifier has no tip prefix")
	}
	return prefix, nil
}

func verifyCachedForkResult(ctx context.Context, bucket objstore.Bucket, targetPrefix, operationID string, members []quepaxa.Member, result ForkResult) error {
	target := NewManager(bucket, targetPrefix, 1)
	defer target.Close()
	if err := target.Load(ctx); err != nil {
		return err
	}
	snapshot, err := target.BeginRecoverySnapshot(ctx, "fork-verify-"+shortHash(operationID), forkLease)
	if err != nil {
		return err
	}
	defer closeSnapshot(snapshot)
	if snapshot.head.Sealed || snapshot.Tip() != quepaxa.Slot(result.Tip) {
		return fmt.Errorf("fork target archive head does not match result")
	}
	sealedHead := snapshot.head
	sealedHead.Sealed = true
	data, err := encodeHead(sealedHead)
	hash := sha256.Sum256(data)
	if err != nil || hex.EncodeToString(hash[:]) != result.ManifestHash {
		return fmt.Errorf("fork target archive head does not match source manifest")
	}

	seal, decision, hasBase := snapshot.RecoveryBase()
	var checkpoints *checkpoint.Manager
	if hasBase {
		checkpoints = checkpoint.NewManager(bucket, targetPrefix, "", 1)
		root, err := checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return err
		}
		if err := checkpoints.Verify(ctx, root.Index, root.RootHash, seal.StateHash); err != nil {
			return err
		}
		if err := checkpoints.Load(ctx); err != nil {
			return err
		}
		current := checkpoints.Latest()
		if current == nil || current.Index != root.Index || current.RootHash != root.RootHash {
			return fmt.Errorf("fork target checkpoint CURRENT does not match recovery base")
		}
	}
	prefix, err := verifyForkSnapshot(ctx, snapshot, seal, decision, checkpoints, members)
	if err != nil || hex.EncodeToString(prefix[:]) != result.PrefixHash {
		if err != nil {
			return err
		}
		return fmt.Errorf("fork target prefix does not match result")
	}
	if err := target.Load(ctx); err != nil || !archiveHeadsEqual(target.head, snapshot.head) {
		if err != nil {
			return err
		}
		return fmt.Errorf("fork target archive changed during verification")
	}
	return nil
}

func forkPrefixes(source, target string) (string, string, error) {
	for _, prefix := range []string{source, target} {
		for _, segment := range strings.Split(prefix, "/") {
			if segment == ".." {
				return "", "", fmt.Errorf("fork prefixes must not traverse namespaces")
			}
		}
	}
	source, target = strings.Trim(path.Clean(source), "/"), strings.Trim(path.Clean(target), "/")
	if source == "" || target == "" || source == "." || target == "." || source == target || source == ".." || target == ".." || strings.HasPrefix(source, "../") || strings.HasPrefix(target, "../") || strings.HasPrefix(source, target+"/") || strings.HasPrefix(target, source+"/") {
		return "", "", fmt.Errorf("distinct source and target prefixes are required")
	}
	return source, target, nil
}

func supportsCAS(bucket objstore.Bucket) bool {
	options := bucket.SupportedObjectUploadOptions()
	for _, option := range options {
		if option == objstore.IfNotExists {
			for _, candidate := range options {
				if candidate == objstore.IfMatch {
					return true
				}
			}
		}
	}
	return false
}

func forkKey(prefix, name string) string { return prefix + "/" + name }
func forkIntentKey(prefix string) string { return forkKey(prefix, "fork/intent.json") }
func forkResultKey(prefix string) string { return forkKey(prefix, "fork/result.json") }
func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func copyArchiveObject(ctx context.Context, bucket objstore.Bucket, source, target string, expected [32]byte, limit int64) error {
	r, err := bucket.Get(ctx, source)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, limit+1))
	closeErr := r.Close()
	sourceHash := sha256.Sum256(data)
	if readErr != nil || closeErr != nil || int64(len(data)) > limit || sourceHash != expected {
		return fmt.Errorf("fork source object integrity mismatch")
	}
	err = bucket.Upload(ctx, target, bytes.NewReader(data), objstore.WithIfNotExists())
	if err != nil && !bucket.IsConditionNotMetErr(err) {
		return err
	}
	r, err = bucket.Get(ctx, target)
	if err != nil {
		return err
	}
	hash := sha256.New()
	read, err := io.Copy(hash, io.LimitReader(r, limit+1))
	closeErr = r.Close()
	var got [32]byte
	copy(got[:], hash.Sum(nil))
	if err != nil || closeErr != nil || read > limit || got != expected {
		return fmt.Errorf("fork object integrity mismatch")
	}
	return nil
}

func writeForkArchiveHead(ctx context.Context, bucket objstore.Bucket, target string, data []byte) error {
	if len(data) > maxHeadSize {
		return fmt.Errorf("fork archive head is too large")
	}
	err := bucket.Upload(ctx, target, bytes.NewReader(data), objstore.WithIfNotExists())
	if err != nil && !bucket.IsConditionNotMetErr(err) {
		return err
	}
	r, err := bucket.Get(ctx, target)
	if err != nil {
		return err
	}
	got, readErr := io.ReadAll(io.LimitReader(r, maxHeadSize+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(got) > maxHeadSize || !bytes.Equal(got, data) {
		return fmt.Errorf("fork archive head integrity mismatch")
	}
	return nil
}

func acquireForkIntent(ctx context.Context, bucket objstore.Bucket, prefix string, want forkIntent) error {
	data, _ := json.Marshal(want)
	if err := bucket.Upload(ctx, forkIntentKey(prefix), bytes.NewReader(data), objstore.WithIfNotExists()); err == nil {
		return nil
	} else if !bucket.IsConditionNotMetErr(err) {
		return err
	}
	got, ok, err := readForkIntent(ctx, bucket, prefix)
	if err != nil || !ok {
		return fmt.Errorf("read fork intent")
	}
	canonical, _ := json.Marshal(got)
	if !bytes.Equal(canonical, data) {
		return fmt.Errorf("fork target belongs to a different operation")
	}
	return nil
}

func readForkIntent(ctx context.Context, bucket objstore.Bucket, prefix string) (forkIntent, bool, error) {
	r, err := bucket.Get(ctx, forkIntentKey(prefix))
	if bucket.IsObjNotFoundErr(err) {
		return forkIntent{}, false, nil
	}
	if err != nil {
		return forkIntent{}, false, err
	}
	data, err := io.ReadAll(io.LimitReader(r, maxHeadSize+1))
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(data) > maxHeadSize {
		return forkIntent{}, false, fmt.Errorf("read fork intent")
	}
	var intent forkIntent
	if err := json.Unmarshal(data, &intent); err != nil || intent.Version != 1 || intent.OperationID == "" || !validForkHash(intent.ManifestHash) {
		return forkIntent{}, false, fmt.Errorf("invalid fork intent")
	}
	canonical, _ := json.Marshal(intent)
	if !bytes.Equal(canonical, data) {
		return forkIntent{}, false, fmt.Errorf("invalid fork intent")
	}
	source, target, err := forkPrefixes(intent.SourcePrefix, intent.TargetPrefix)
	if err != nil || source != intent.SourcePrefix || target != intent.TargetPrefix || !validForkMemberIDs(intent.MemberIDs) {
		return forkIntent{}, false, fmt.Errorf("invalid fork intent")
	}
	return intent, true, nil
}

func sameForkRequest(intent forkIntent, options ForkOptions, source, target string) bool {
	if intent.Version != 1 || intent.OperationID != options.OperationID || intent.SourcePrefix != source || intent.TargetPrefix != target {
		return false
	}
	data, _ := json.Marshal(intent.MemberIDs)
	want, _ := json.Marshal(forkMemberIDs(options.Members))
	return bytes.Equal(data, want)
}

func forkMemberIDs(members []quepaxa.Member) []string {
	ids := make([]string, 0, len(members))
	for _, member := range members {
		ids = append(ids, string(member.ID))
	}
	sort.Strings(ids)
	return ids
}

func validForkMemberIDs(ids []string) bool {
	if len(ids) == 0 {
		return false
	}
	for i, id := range ids {
		if id == "" || i > 0 && ids[i-1] >= id {
			return false
		}
	}
	return true
}

func ensureForkTargetEmpty(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	seen := false
	err := bucket.Iter(ctx, prefix+"/", func(string) error { seen = true; return nil })
	if err != nil {
		return err
	}
	if seen {
		return fmt.Errorf("fork target is not empty")
	}
	return nil
}

func readForkResult(ctx context.Context, bucket objstore.Bucket, prefix string) (ForkResult, bool, error) {
	r, err := bucket.Get(ctx, forkResultKey(prefix))
	if bucket.IsObjNotFoundErr(err) {
		return ForkResult{}, false, nil
	}
	if err != nil {
		return ForkResult{}, false, err
	}
	data, err := io.ReadAll(io.LimitReader(r, maxHeadSize+1))
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(data) > maxHeadSize {
		return ForkResult{}, false, fmt.Errorf("read fork result")
	}
	var result ForkResult
	if err := json.Unmarshal(data, &result); err != nil || result.Tip == 0 || !validForkHash(result.PrefixHash) || !validForkHash(result.ManifestHash) {
		return ForkResult{}, false, fmt.Errorf("invalid fork result")
	}
	canonical, _ := json.Marshal(result)
	if !bytes.Equal(canonical, data) {
		return ForkResult{}, false, fmt.Errorf("invalid fork result")
	}
	return result, true, nil
}

func validForkHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func closeSnapshot(snapshot *RecoverySnapshot) {
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = snapshot.Close(closeCtx)
}
func closeRootPin(pin *checkpoint.RecoveryPin) {
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pin.Close(closeCtx)
}

func writeForkResult(ctx context.Context, bucket objstore.Bucket, prefix string, result ForkResult) error {
	data, _ := json.Marshal(result)
	if err := bucket.Upload(ctx, forkResultKey(prefix), bytes.NewReader(data), objstore.WithIfNotExists()); err == nil {
		return nil
	} else if !bucket.IsConditionNotMetErr(err) {
		return err
	}
	got, ok, err := readForkResult(ctx, bucket, prefix)
	if err != nil || !ok || got != result {
		return fmt.Errorf("fork result conflict")
	}
	return nil
}
