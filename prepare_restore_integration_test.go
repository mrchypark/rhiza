package rhiza_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const preparedRestoreBaseValueBytes = 64 << 10

// TestEmbeddedObjectStoragePrepareRestore uses a real multi-voter archive so the public
// API is exercised against its actual object-store and consensus boundaries.
// The archive fixtures are isolated local emulators selected by the existing
// RHIZA_EMBEDDED_* contract; filesystem storage is intentionally skipped here
// because PrepareRestore requires shared storage and bootstrap voters.
func TestEmbeddedObjectStoragePrepareRestore(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider == "" || provider == "filesystem" {
		t.Skip("PrepareRestore integration requires an explicitly configured shared-storage emulator")
	}
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Fatalf("unsupported explicit emulator provider %q", provider)
	}
	upstream := embeddedEndpoint(t)
	observer := newPrepareRestoreMutationObserver(t, upstream)
	t.Setenv("RHIZA_EMBEDDED_ENDPOINT", observer.endpoint())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	generation := fmt.Sprintf("prepare-restore-%d", time.Now().UnixNano())
	cluster := generation + "-source"
	base := embeddedStoreConfig(t)
	base.Local = false
	base.ObjStorePrefix = generation
	base.ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
	base.ObjStoreSyncInterval = 10 * time.Millisecond
	base.CheckpointInterval = 3 * time.Second
	base.CheckpointTailBytes = 1
	base.EnableReconfiguration = true
	base.AdminToken = generation + "-admin"
	observer.setRoot(path.Join(base.ObjStorePrefix, cluster))

	members, tokens := recoveryTestMembers(t, cluster, generation+"-source-token")
	source, sourceConfigs := openRecoveryTestVoters(t, ctx, base, cluster, members, tokens)
	defer closeRecoveryTestVoters(t, source)
	if _, _, err := executeRecoveryTest(ctx, source, rhiza.ExecuteRequest{
		RequestID: "prepare-restore-sql-schema",
		SQL:       "CREATE TABLE prepared_restore_rows (id INTEGER PRIMARY KEY, value TEXT)",
	}); err != nil {
		t.Fatalf("create source SQL schema: %v", err)
	}
	if _, err := executeGraphRecoveryTest(ctx, source, rhiza.GraphCommand{RequestID: "prepare-restore-graph-schema", Cypher: "CREATE NODE TABLE PreparedRestore(id INTEGER, value STRING, PRIMARY KEY(id))"}); err != nil {
		t.Fatalf("create source Graph schema: %v", err)
	}
	largeValue := strings.Repeat("base-", preparedRestoreBaseValueBytes/len("base-"))
	_, baseSQL, err := executeRecoveryTest(ctx, source, rhiza.ExecuteRequest{
		RequestID: "prepare-restore-base-sql",
		SQL:       "INSERT INTO prepared_restore_rows VALUES (1, '" + largeValue + "')",
	})
	if err != nil {
		t.Fatalf("write checkpoint SQL base: %v", err)
	}
	baseGraphSlot, err := executeGraphRecoveryTest(ctx, source, rhiza.GraphCommand{RequestID: "prepare-restore-base-graph", Cypher: "CREATE (:PreparedRestore {id: 1, value: 'graph-base'})"})
	if err != nil {
		t.Fatalf("write checkpoint Graph base: %v", err)
	}

	archiveBucket := embeddedBucket(t, base)
	archive := recovery.NewManager(archiveBucket, path.Join(base.ObjStorePrefix, cluster), 1)
	defer archive.Close()
	var baseIndex uint64
	for {
		if err := archive.Load(ctx); err == nil {
			if seal, _, ok := archive.RecoveryBase(); ok && uint64(seal.Index) >= maxSlot(baseSQL.Slot, baseGraphSlot) {
				baseIndex = uint64(seal.Index)
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("source archive did not publish a certified checkpoint: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if baseIndex == 0 {
		t.Fatal("source archive published an empty recovery base")
	}

	_, suffixSQL, err := executeRecoveryTest(ctx, source, rhiza.ExecuteRequest{
		RequestID: "prepare-restore-suffix-sql",
		SQL:       "INSERT INTO prepared_restore_rows VALUES (2, 'sql-suffix')",
	})
	if err != nil {
		t.Fatalf("write certified SQL suffix: %v", err)
	}
	suffixGraphSlot, err := executeGraphRecoveryTest(ctx, source, rhiza.GraphCommand{RequestID: "prepare-restore-suffix-graph", Cypher: "CREATE (:PreparedRestore {id: 2, value: 'graph-suffix'})"})
	if err != nil {
		t.Fatalf("write certified Graph suffix: %v", err)
	}
	for {
		if err := archive.Load(ctx); err == nil {
			if seal, _, ok := archive.RecoveryBase(); ok {
				selectedBase := uint64(seal.Index)
				certifiedTip := uint64(archive.Tip())
				if selectedBase >= minSlot(suffixSQL.Slot, suffixGraphSlot) {
					t.Fatalf("selected checkpoint %d already includes a certified suffix write (SQL=%d Graph=%d); cannot prove suffix replay", selectedBase, suffixSQL.Slot, suffixGraphSlot)
				}
				if maxSlot(baseSQL.Slot, baseGraphSlot) <= selectedBase && selectedBase < minSlot(suffixSQL.Slot, suffixGraphSlot) && certifiedTip >= maxSlot(suffixSQL.Slot, suffixGraphSlot) {
					break
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("source archive did not durably publish suffix after checkpoint %d: %v", baseIndex, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	// Shutdown joins each checkpointer and archive writer. Capture only after
	// every source is closed so the image and API-decoded anchors are stable.
	if err := closePreparedArchiveSource(source); err != nil {
		t.Fatalf("quiesce source archive writers: %v", err)
	}
	if err := archive.Load(ctx); err != nil {
		t.Fatalf("reload quiesced source archive: %v", err)
	}
	seal, _, hasBase := archive.RecoveryBase()
	if !hasBase {
		t.Fatal("quiesced source archive lost its certified checkpoint base")
	}
	checkpointManager := checkpoint.NewManager(embeddedBucket(t, base), path.Join(base.ObjStorePrefix, cluster), "", 1)
	if err := checkpointManager.Load(ctx); err != nil {
		t.Fatalf("load source checkpoint through checkpoint manager: %v", err)
	}
	currentCheckpoint := checkpointManager.Latest()
	if currentCheckpoint == nil {
		t.Fatal("quiesced source archive has no current checkpoint")
	}

	learnerID, learnerToken := "fresh-learner", generation+"-learner-token"
	baselineOwner := "prepare-observation-" + generation
	baselineSnapshot, err := archive.BeginRecoverySnapshot(ctx, baselineOwner, time.Minute)
	if err != nil {
		t.Fatalf("capture baseline archive head through recovery API: %v", err)
	}
	if err := baselineSnapshot.Close(ctx); err != nil {
		t.Fatalf("release baseline archive snapshot: %v", err)
	}
	archiveBefore := embeddedRemoteImage(t, base)
	expectedArchivePin := requirePreparedArchivePin(t, archiveBefore, base.ObjStorePrefix, cluster, baselineOwner)
	if expectedArchivePin.Base != uint64(seal.Index) || expectedArchivePin.Tip != uint64(archive.Tip()) {
		t.Fatalf("baseline recovery API snapshot differs from loaded source anchors: pin=%+v base=%d tip=%d", expectedArchivePin, seal.Index, archive.Tip())
	}
	learner := rhiza.Member{ID: rhiza.NodeID(learnerID), PeerURL: "quic://" + freeUDPAddr(t), PublicKey: rhiza.PeerPublicKey(cluster, learnerID, learnerToken)}
	prepared := base
	prepared.ClusterID = cluster
	prepared.NodeID = learnerID
	prepared.DataDir = filepath.Join(t.TempDir(), "prepared")
	prepared.PeerAddr = strings.TrimPrefix(learner.PeerURL, "quic://")
	prepared.PeerToken = learnerToken
	prepared.PreparedPodUID = "pod-incarnation-1"
	prepared.Learner = &learner
	prepared.Members = members

	observer.begin()
	prepareErr := prepareRestoreWithoutRuntimeCredentials(ctx, prepared)
	prepareMutations := observer.end()
	if prepareErr != nil {
		t.Fatalf("prepare learner materialization: %v; observed provider mutations: %+v", prepareErr, prepareMutations)
	}
	if err := assertPreparedRestoreMutationAllowlist(prepareMutations, path.Join(base.ObjStorePrefix, cluster)); err != nil {
		t.Fatalf("PrepareRestore provider mutation scope: %v", err)
	}
	markerBase, markerTip := assertPreparedRestoreFiles(t, ctx, prepared.DataDir)
	if maxSlot(baseSQL.Slot, baseGraphSlot) > markerBase || markerBase >= minSlot(suffixSQL.Slot, suffixGraphSlot) || markerTip < maxSlot(suffixSQL.Slot, suffixGraphSlot) {
		t.Fatalf("prepared marker does not prove checkpoint-base plus certified SQL/Graph suffix replay: base_index=%d certified_tip=%d SQLbase=%d Graphbase=%d SQLsuffix=%d Graphsuffix=%d", markerBase, markerTip, baseSQL.Slot, baseGraphSlot, suffixSQL.Slot, suffixGraphSlot)
	}
	assertNoPreparedRuntimeAuthority(t, prepared)
	listener, err := net.ListenPacket("udp", prepared.PeerAddr)
	if err != nil {
		t.Fatalf("PrepareRestore unexpectedly opened learner peer listener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	archiveAfter := embeddedRemoteImage(t, base)
	if err := comparePreparedRestoreArchiveImages(archiveBefore, archiveAfter, prepared.ObjStorePrefix, string(prepared.ClusterID), string(prepared.NodeID), uint64(archive.Tip()), uint64(seal.Index), expectedArchivePin, currentCheckpoint, time.Now()); err != nil {
		t.Fatalf("PrepareRestore changed canonical archive state or left invalid recovery-pin state: %v", err)
	}

	// A multi-voter learner becomes Ready through peer catch-up; offline archive
	// startup alone is not a Ready signal. Reopen the original source DataDirs
	// only after the immutable archive comparison is complete.
	activeSource := reopenPreparedArchiveSource(t, ctx, sourceConfigs)
	defer closeRecoveryTestVoters(t, activeSource)

	beforeMismatch := readPreparedDataDir(t, prepared.DataDir)
	wrongPod := prepared
	wrongPod.PreparedPodUID = "pod-incarnation-2"
	if err := prepareRestoreWithoutRuntimeCredentials(ctx, wrongPod); err == nil {
		t.Fatal("PrepareRestore accepted a completed DataDir for another Pod UID")
	}
	wrongSource := prepared
	wrongSource.ObjStorePrefix += "/different-source"
	if err := prepareRestoreWithoutRuntimeCredentials(ctx, wrongSource); err == nil {
		t.Fatal("PrepareRestore accepted a completed DataDir for a different archive source")
	}
	if after := readPreparedDataDir(t, prepared.DataDir); !reflect.DeepEqual(beforeMismatch, after) {
		t.Fatal("mismatched Pod/source refusal changed prepared DataDir")
	}

	// Opening the prepared learner creates/holds its runtime WAL lock. A
	// concurrent preparation attempt must refuse; after catch-up has written a
	// valid WAL, same-Pod retry must preserve that newer WAL and materialized tip.
	runtimeCtx, runtimeCancel := context.WithTimeout(ctx, 90*time.Second)
	defer runtimeCancel()
	db, err := rhiza.Open(runtimeCtx, prepared)
	if err != nil {
		t.Fatalf("open prepared learner for same-Pod WAL check: %v", err)
	}
	if err := prepareRestoreWithoutRuntimeCredentials(runtimeCtx, prepared); err == nil {
		_ = db.Close()
		t.Fatal("PrepareRestore accepted concurrent use of the same DataDir")
	}
	if err := waitPreparedLearnerReady(runtimeCtx, db); err != nil {
		_ = db.Close()
		t.Fatalf("prepared learner did not become ready from certified archive: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close prepared learner: %v", err)
	}
	validWAL := readPreparedDataDir(t, prepared.DataDir)
	if !hasPreparedRuntimeWAL(validWAL) {
		t.Fatal("opening the learner did not produce a runtime WAL for the no-rewind check")
	}
	if err := prepareRestoreWithoutRuntimeCredentials(runtimeCtx, prepared); err != nil {
		t.Fatalf("same-Pod retry with valid runtime WAL: %v", err)
	}
	if after := readPreparedDataDir(t, prepared.DataDir); !reflect.DeepEqual(validWAL, after) {
		t.Fatal("same-Pod preparation rewrote or removed valid runtime WAL/materialized state")
	}
	if provider == "azure" {
		missing := prepared
		missing.ObjStoreBucket = "rhiza-never-create-" + generation
		missing.DataDir = filepath.Join(t.TempDir(), "missing-container")
		observer.begin()
		missingErr := prepareRestoreWithoutRuntimeCredentials(ctx, missing)
		missingMutations := observer.end()
		if missingErr == nil {
			t.Fatal("PrepareRestore unexpectedly succeeded with an absent Azure container")
		}
		if len(missingMutations) != 0 {
			t.Fatalf("absent Azure container caused provider mutations: %+v", missingMutations)
		}
	}
}

type preparedArchivePin struct {
	OwnerID      string   `json:"owner_id"`
	Token        string   `json:"token"`
	Base         uint64   `json:"base"`
	Tip          uint64   `json:"tip"`
	TailHash     [32]byte `json:"tail_hash"`
	TailObject   uint64   `json:"tail_object"`
	LeaseUntilMS int64    `json:"lease_until_unix_ms"`
}

type preparedArchiveGCLock struct {
	OwnerID      string `json:"owner_id"`
	Generation   uint64 `json:"generation"`
	LeaseUntilMS int64  `json:"lease_until_unix_ms"`
}

type preparedCheckpointPublisher struct {
	ConfigID      uint   `json:"config_id"`
	Generation    uint64 `json:"generation"`
	OwnerID       string `json:"owner_id"`
	Purpose       string `json:"purpose,omitempty"`
	LeaseUntilMS  int64  `json:"lease_until_unix_ms"`
	ReservedIndex uint64 `json:"reserved_index"`
	BoundIndex    uint64 `json:"bound_index,omitempty"`
	RootHash      string `json:"root_hash,omitempty"`
}

type preparedCheckpointPin struct {
	ConfigID     uint            `json:"config_id"`
	OwnerID      string          `json:"owner_id"`
	Token        string          `json:"token"`
	Index        uint64          `json:"index"`
	RootHash     string          `json:"root_hash"`
	Root         json.RawMessage `json:"root"`
	LeaseUntilMS int64           `json:"lease_until_unix_ms"`
}

// comparePreparedRestoreArchiveImages permits only new, correctly scoped,
// expired recovery pins. Every canonical object and every pre-existing pin
// remains byte-for-byte identical.
func comparePreparedRestoreArchiveImages(before, after map[string][]byte, prefix, clusterID, nodeID string, archiveTip, archiveBase uint64, expectedArchivePin preparedArchivePin, currentCheckpoint *checkpoint.Checkpoint, now time.Time) error {
	root := path.Join(prefix, clusterID)
	archivePins := path.Join(root, "archive/recovery-pins") + "/"
	checkpointPins := path.Join(root, "checkpoint/recovery-pins") + "/"
	headKey := path.Join(root, "archive/head.bin")
	currentKey := path.Join(root, "checkpoint/CURRENT")
	gcLockKey := path.Join(root, "archive/GC_LOCK")
	checkpointPublisherKey := path.Join(root, "checkpoint/PUBLISHER")
	if len(before[headKey]) == 0 || len(before[currentKey]) == 0 || archiveTip == 0 || currentCheckpoint == nil || currentCheckpoint.Index == 0 {
		return fmt.Errorf("original archive/checkpoint anchors are incomplete")
	}
	if expectedArchivePin.Tip != archiveTip || expectedArchivePin.Base != archiveBase || expectedArchivePin.Tip < expectedArchivePin.Base || (expectedArchivePin.Tip > expectedArchivePin.Base) != (expectedArchivePin.TailHash != ([32]byte{})) || (expectedArchivePin.TailHash == ([32]byte{})) != (expectedArchivePin.TailObject == 0) {
		return fmt.Errorf("baseline recovery snapshot does not identify the loaded archive head")
	}
	checkpointRootHash := hex.EncodeToString(currentCheckpoint.RootHash[:])
	if len(checkpointRootHash) != 64 || !isLowerHexString(checkpointRootHash) {
		return fmt.Errorf("loaded checkpoint manager returned an invalid root hash")
	}
	ownerPrefix := "startup-prepare-" + nodeID + "-"
	newArchivePins, newCheckpointPins := 0, 0
	newOwner := ""
	for name, oldData := range before {
		if strings.HasPrefix(name, archivePins) || strings.HasPrefix(name, checkpointPins) {
			newData, ok := after[name]
			if !ok || !bytes.Equal(oldData, newData) {
				return fmt.Errorf("pre-existing recovery pin changed or disappeared at %q", name)
			}
			continue
		}
		if name == gcLockKey || name == checkpointPublisherKey {
			continue // These two lease records are explicitly validated below.
		}
		newData, ok := after[name]
		if !ok || !bytes.Equal(oldData, newData) {
			return fmt.Errorf("canonical archive object changed or disappeared at %q", name)
		}
	}
	for name, data := range after {
		if _, existed := before[name]; existed {
			continue
		}
		switch {
		case strings.HasPrefix(name, archivePins):
			var pin preparedArchivePin
			if err := decodePreparedJSON(data, &pin); err != nil {
				return fmt.Errorf("decode new archive recovery pin %q: %w", name, err)
			}
			if err := validatePreparedPinIdentity(name, archivePins, pin.OwnerID, pin.Token, pin.LeaseUntilMS, ownerPrefix, now); err != nil {
				return err
			}
			if pin.Base != expectedArchivePin.Base || pin.Tip != expectedArchivePin.Tip || pin.TailHash != expectedArchivePin.TailHash || pin.TailObject != expectedArchivePin.TailObject {
				return fmt.Errorf("new archive recovery pin %q is not bound to the original head", name)
			}
			if newOwner != "" && newOwner != pin.OwnerID {
				return fmt.Errorf("new recovery pins have different owners")
			}
			newOwner = pin.OwnerID
			newArchivePins++
		case strings.HasPrefix(name, checkpointPins):
			var pin preparedCheckpointPin
			if err := decodePreparedJSON(data, &pin); err != nil {
				return fmt.Errorf("decode new checkpoint recovery pin %q: %w", name, err)
			}
			if err := validatePreparedPinIdentity(name, checkpointPins, pin.OwnerID, pin.Token, pin.LeaseUntilMS, ownerPrefix, now); err != nil {
				return err
			}
			rootName := path.Join(root, "checkpoint/roots", fmt.Sprintf("%020d_%s.json", pin.Index, pin.RootHash))
			canonicalRoot, ok := before[rootName]
			rootHash := sha256.Sum256(canonicalRoot)
			if pin.ConfigID != 1 || pin.Index != currentCheckpoint.Index || pin.RootHash != checkpointRootHash || !ok || !bytes.Equal(pin.Root, canonicalRoot) || hex.EncodeToString(rootHash[:]) != pin.RootHash {
				return fmt.Errorf("new checkpoint recovery pin %q is not bound to the original checkpoint", name)
			}
			if newOwner != "" && newOwner != pin.OwnerID {
				return fmt.Errorf("new recovery pins have different owners")
			}
			newOwner = pin.OwnerID
			newCheckpointPins++
		case name == gcLockKey || name == checkpointPublisherKey:
			// The resulting records are checked against their exact lease contracts
			// after the newly created pin owner has been established.
		default:
			return fmt.Errorf("unexpected object was added at %q", name)
		}
	}
	if newArchivePins != 1 || newCheckpointPins != 1 {
		return fmt.Errorf("new recovery pins: archive=%d checkpoint=%d, want one of each", newArchivePins, newCheckpointPins)
	}
	if err := validatePreparedArchiveGCLock(before[gcLockKey], after[gcLockKey], newOwner, now); err != nil {
		return err
	}
	if err := validatePreparedCheckpointPublisher(before[checkpointPublisherKey], after[checkpointPublisherKey], now); err != nil {
		return err
	}
	return nil
}

func validatePreparedArchiveGCLock(before, after []byte, pinOwner string, now time.Time) error {
	var previous, current preparedArchiveGCLock
	if len(before) != 0 {
		if err := decodePreparedJSON(before, &previous); err != nil {
			return fmt.Errorf("decode original archive GC lock: %w", err)
		}
	}
	if err := decodePreparedJSON(after, &current); err != nil {
		return fmt.Errorf("decode resulting archive GC lock: %w", err)
	}
	if current.OwnerID != "snapshot:"+pinOwner || current.Generation != previous.Generation+1 || current.Generation == 0 || current.LeaseUntilMS > now.UnixMilli() {
		return fmt.Errorf("archive GC lock is not the expired snapshot lock for the new pin")
	}
	return nil
}

func validatePreparedCheckpointPublisher(before, after []byte, now time.Time) error {
	var previous, current preparedCheckpointPublisher
	if len(before) != 0 {
		if err := decodePreparedJSON(before, &previous); err != nil {
			return fmt.Errorf("decode original checkpoint publisher claim: %w", err)
		}
	}
	if err := decodePreparedJSON(after, &current); err != nil {
		return fmt.Errorf("decode resulting checkpoint publisher claim: %w", err)
	}
	if current.ConfigID != 1 || current.Generation != previous.Generation+1 || current.Generation == 0 || current.OwnerID != "checkpoint-gc" || current.Purpose != "maintenance" || current.LeaseUntilMS > now.UnixMilli() || current.ReservedIndex != 0 || current.BoundIndex != 0 || current.RootHash != "" {
		return fmt.Errorf("checkpoint publisher is not the expired maintenance claim")
	}
	return nil
}

func validatePreparedPinIdentity(name, keyPrefix, ownerID, token string, leaseUntilMS int64, ownerPrefix string, now time.Time) error {
	suffix := strings.TrimPrefix(name, keyPrefix)
	if len(suffix) != 64 || !isLowerHexString(suffix) {
		return fmt.Errorf("recovery pin has malformed key %q", name)
	}
	ownerSuffix := strings.TrimPrefix(ownerID, ownerPrefix)
	if !strings.HasPrefix(ownerID, ownerPrefix) || len(ownerSuffix) != 16 || !isLowerHexString(ownerSuffix) {
		return fmt.Errorf("recovery pin %q has unexpected owner", name)
	}
	ownerHash := sha256.Sum256([]byte(ownerID))
	if hex.EncodeToString(ownerHash[:]) != suffix {
		return fmt.Errorf("recovery pin key %q does not match its owner", name)
	}
	if len(token) != 32 || !isLowerHexString(token) {
		return fmt.Errorf("recovery pin %q has malformed token", name)
	}
	if leaseUntilMS > now.UnixMilli() {
		return fmt.Errorf("recovery pin %q remains active", name)
	}
	return nil
}

func decodePreparedJSON(data []byte, target any) error {
	if len(data) == 0 {
		return fmt.Errorf("object is missing")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return fmt.Errorf("trailing JSON value")
}

func isLowerHexString(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func prepareRestoreWithoutRuntimeCredentials(ctx context.Context, config rhiza.Config) error {
	config.Learner = nil
	config.AdminToken = ""
	config.PeerToken = ""
	return rhiza.PrepareRestore(ctx, config)
}

func executeGraphRecoveryTest(ctx context.Context, voters []*rhiza.DB, command rhiza.GraphCommand) (uint64, error) {
	var last error
	for {
		for _, voter := range voters {
			if response, err := voter.GraphExecute(ctx, command); err == nil {
				return response.Slot, nil
			} else {
				last = err
			}
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("execute Graph request %q: %w (last error: %v)", command.RequestID, ctx.Err(), last)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func maxSlot(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minSlot(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func closePreparedArchiveSource(voters []*rhiza.DB) error {
	var closeErr error
	for i, voter := range voters {
		if err := voter.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close source voter %d: %w", i, err))
		}
	}
	return closeErr
}

func reopenPreparedArchiveSource(t testing.TB, ctx context.Context, configs []rhiza.Config) []*rhiza.DB {
	t.Helper()
	voters := make([]*rhiza.DB, 0, len(configs))
	for i, config := range configs {
		voter, err := rhiza.Open(ctx, config)
		if err != nil {
			closeRecoveryTestVoters(t, voters)
			t.Fatalf("reopen source voter %d from its original DataDir: %v", i, err)
		}
		voters = append(voters, voter)
	}
	return voters
}

func assertPreparedRestoreFiles(t *testing.T, ctx context.Context, dataDir string) (uint64, uint64) {
	t.Helper()
	markerBytes, err := os.ReadFile(filepath.Join(dataDir, "prepared-restore.json"))
	if err != nil {
		t.Fatalf("read prepared completion marker: %v", err)
	}
	var marker struct {
		Phase        string `json:"phase"`
		BaseIndex    uint64 `json:"base_index"`
		CertifiedTip uint64 `json:"certified_tip"`
	}
	if err := json.Unmarshal(markerBytes, &marker); err != nil || marker.Phase != "complete" {
		t.Fatalf("invalid prepared completion marker %s: %v", markerBytes, err)
	}
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatalf("open prepared materialized SQL/Graph state before application Open: %v", err)
	}
	rows, err := material.Query(ctx, "SELECT id, value FROM prepared_restore_rows ORDER BY id")
	if err != nil {
		_ = material.Close()
		t.Fatalf("query prepared SQL state: %v", err)
	}
	var sqlRows []string
	for rows.Next() {
		var id int64
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			_ = rows.Close()
			_ = material.Close()
			t.Fatal(err)
		}
		sqlRows = append(sqlRows, fmt.Sprintf("%d:%s", id, value))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		_ = material.Close()
		t.Fatalf("read prepared SQL rows: %v", err)
	}
	wantBaseValue := strings.Repeat("base-", preparedRestoreBaseValueBytes/len("base-"))
	if len(sqlRows) != 2 || sqlRows[0] != "1:"+wantBaseValue || sqlRows[1] != "2:sql-suffix" {
		_ = material.Close()
		t.Fatalf("prepared SQL rows=%v; want checkpoint base and certified suffix", sqlRows)
	}
	for _, want := range []struct {
		id    int
		value string
	}{{1, "graph-base"}, {2, "graph-suffix"}} {
		graph, err := material.GraphQuery(ctx, fmt.Sprintf("MATCH (n:PreparedRestore {id: %d}) RETURN n.value", want.id), nil)
		if err != nil || len(graph.Rows) != 1 || graph.Rows[0][0] != want.value {
			_ = material.Close()
			t.Fatalf("prepared Graph row id=%d rows=%#v err=%v", want.id, graph.Rows, err)
		}
	}
	tip := material.Tip()
	if err := material.Close(); err != nil {
		t.Fatalf("close prepared state reader: %v", err)
	}
	if tip == 0 {
		t.Fatal("prepared materializer has no applied certified tip")
	}
	return marker.BaseIndex, marker.CertifiedTip
}

func assertNoPreparedRuntimeAuthority(t *testing.T, config rhiza.Config) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(config.DataDir, "qlog", "voter-identity.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PrepareRestore created voter identity: %v", err)
	}
	manifests, err := filepath.Glob(filepath.Join(config.DataDir, "qlog", "manifest_*.bin"))
	if err != nil || len(manifests) != 0 {
		t.Fatalf("PrepareRestore created runtime WAL manifests %v, err=%v", manifests, err)
	}
	key := path.Join(config.ObjStorePrefix, config.ClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(config.NodeID))))
	image := embeddedRemoteImage(t, config)
	if _, exists := image[key]; exists {
		t.Fatalf("PrepareRestore registered learner voter at %q", key)
	}
	marker, err := os.ReadFile(filepath.Join(config.DataDir, "prepared-restore.json"))
	if err != nil || !bytes.Contains(marker, []byte(`"phase":"complete"`)) || !bytes.Contains(marker, []byte(`"pod_uid":"pod-incarnation-1"`)) {
		t.Fatalf("prepared completion marker=%s err=%v", marker, err)
	}
}

func waitPreparedLearnerReady(ctx context.Context, db *rhiza.DB) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if db.Ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func readPreparedDataDir(t *testing.T, root string) map[string][]byte {
	t.Helper()
	image := make(map[string][]byte)
	err := filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected nonregular prepared path %q", name)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		image[rel] = data
		return nil
	})
	if err != nil {
		t.Fatalf("capture prepared DataDir: %v", err)
	}
	return image
}

func hasPreparedRuntimeWAL(image map[string][]byte) bool {
	for name := range image {
		if strings.HasPrefix(filepath.ToSlash(name), "qlog/manifest_") && strings.HasSuffix(name, ".bin") {
			return true
		}
	}
	return false
}

type prepareRestoreMutation struct {
	Method string
	Key    string
	Status int
	Denied bool
}

type prepareRestoreMutationObserver struct {
	t        *testing.T
	provider string
	upstream *url.URL
	server   *http.Server
	listener net.Listener
	mu       sync.Mutex
	active   bool
	root     string
	records  []prepareRestoreMutation
	sessions map[string]string
}

func newPrepareRestoreMutationObserver(t *testing.T, upstream string) *prepareRestoreMutationObserver {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil || target.Scheme != "http" || target.Host == "" {
		t.Fatalf("invalid local provider upstream %q", upstream)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for provider observer: %v", err)
	}
	o := &prepareRestoreMutationObserver{
		t: t, provider: os.Getenv("RHIZA_EMBEDDED_PROVIDER"), upstream: target,
		listener: listener, sessions: make(map[string]string),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request == nil {
			return nil
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		if len(o.records) > 0 {
			for i := len(o.records) - 1; i >= 0; i-- {
				if o.records[i].Status == 0 && o.records[i].Method == response.Request.Method {
					o.records[i].Status = response.StatusCode
					break
				}
			}
		}
		if response.Request.Method == http.MethodPost && strings.HasPrefix(response.Request.URL.Path, "/upload/") {
			if location := response.Header.Get("Location"); location != "" {
				u, err := url.Parse(location)
				if err != nil || (u.IsAbs() && !strings.EqualFold(u.Host, o.upstream.Host) && !strings.EqualFold(u.Host, o.listener.Addr().String())) {
					return fmt.Errorf("unrecognized provider resumable upload location")
				}
				if u.IsAbs() {
					u.Scheme = "http"
					u.Host = o.listener.Addr().String()
					response.Header.Set("Location", u.String())
				}
				if key := o.sessions[prepareRestoreRequestFingerprint(response.Request.URL)]; key != "" {
					o.sessions[prepareRestoreURLFingerprint(u)] = key
				}
			}
		}
		return nil
	}
	o.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		active, root := o.active, o.root
		if !active || !isPrepareRestoreMutation(r.Method) || (r.Method == http.MethodPost && r.URL.Path == "/token") {
			o.mu.Unlock()
			proxy.ServeHTTP(w, r)
			return
		}
		body, err := readPrepareRestoreBody(r)
		if err != nil {
			o.records = append(o.records, prepareRestoreMutation{Method: r.Method, Denied: true})
			o.mu.Unlock()
			http.Error(w, "provider mutation body exceeds observer bound", http.StatusRequestEntityTooLarge)
			return
		}
		key, parseErr := prepareRestoreMutationKey(r, body, o.provider, o.sessions)
		allowed := parseErr == nil && isAllowedPrepareRestoreMutationKey(key, root)
		record := prepareRestoreMutation{Method: r.Method, Key: key, Denied: !allowed}
		o.records = append(o.records, record)
		if allowed && r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			o.sessions[prepareRestoreRequestFingerprint(r.URL)] = key
		}
		o.mu.Unlock()
		if !allowed {
			http.Error(w, "provider mutation is outside PrepareRestore allowlist", http.StatusForbidden)
			return
		}
		proxy.ServeHTTP(w, r)
	})}
	go func() { _ = o.server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := o.server.Shutdown(ctx); err != nil {
			t.Errorf("stop provider observer: %v", err)
		}
	})
	return o
}

func (o *prepareRestoreMutationObserver) endpoint() string {
	return "http://" + o.listener.Addr().String()
}

func (o *prepareRestoreMutationObserver) setRoot(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.root = strings.Trim(root, "/")
}

func (o *prepareRestoreMutationObserver) begin() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.active {
		o.t.Fatal("provider mutation observer capture already active")
	}
	o.records = nil
	o.sessions = make(map[string]string)
	o.active = true
}

func (o *prepareRestoreMutationObserver) end() []prepareRestoreMutation {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.active = false
	return append([]prepareRestoreMutation(nil), o.records...)
}

func readPrepareRestoreBody(r *http.Request) ([]byte, error) {
	const maxObservedBody = 1 << 20
	if r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxObservedBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxObservedBody {
		return nil, fmt.Errorf("request body exceeds observation limit")
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return body, nil
}

func isPrepareRestoreMutation(method string) bool {
	switch method {
	case http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func prepareRestoreMutationKey(r *http.Request, body []byte, provider string, sessions map[string]string) (string, error) {
	if key := sessions[prepareRestoreRequestFingerprint(r.URL)]; key != "" {
		return key, nil
	}
	decodedPath, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil || strings.Contains(decodedPath, "%") || path.Clean(decodedPath) != decodedPath {
		return "", fmt.Errorf("noncanonical or multiply escaped mutation path")
	}
	if provider == "gcs" {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return "", fmt.Errorf("parse GCS mutation query: %w", err)
		}
		queryNames := query["name"]
		if len(queryNames) > 1 {
			return "", fmt.Errorf("GCS mutation has duplicate query object names")
		}
		if len(queryNames) == 1 && queryNames[0] == "" {
			return "", fmt.Errorf("GCS mutation has an empty query object name")
		}
		bodyName := ""
		mediaType, _, mediaTypeErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaTypeErr == nil && (strings.HasPrefix(mediaType, "multipart/") || strings.Contains(mediaType, "json")) {
			bodyName, err = gcsObjectNameFromBody(r.Header.Get("Content-Type"), body)
			if err != nil {
				return "", err
			}
		}
		key := ""
		for _, candidate := range []string{firstGCSObjectName(queryNames), bodyName, gcsObjectNameFromPath(decodedPath)} {
			if candidate == "" {
				continue
			}
			if key != "" && key != candidate {
				return "", fmt.Errorf("GCS mutation object names conflict")
			}
			key = candidate
		}
		if key == "" {
			return "", fmt.Errorf("GCS mutation has no exact object name")
		}
		return key, nil
	}
	parts := strings.Split(strings.Trim(decodedPath, "/"), "/")
	strip := 1
	if provider == "azure" {
		strip = 2 // account and container; container creation has no blob key.
	}
	if len(parts) <= strip {
		return "", fmt.Errorf("provider mutation path has no object key")
	}
	key := strings.Join(parts[strip:], "/")
	if key == "" || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("provider mutation has an empty object key")
	}
	return key, nil
}

func firstGCSObjectName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func gcsObjectNameFromPath(p string) string {
	const marker = "/o/"
	index := strings.Index(p, marker)
	if index < 0 || !strings.HasPrefix(p, "/storage/v1/b/") {
		return ""
	}
	return strings.TrimPrefix(p[index+len(marker):], "/")
}

func gcsObjectNameFromBody(contentType string, body []byte) (string, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(mediaType, "multipart/") {
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		metadataParts := 0
		multipleMetadata := false
		partIndex := 0
		var objectName string
		for {
			part, nextErr := reader.NextPart()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return "", nextErr
			}
			partMediaType, _, partMediaTypeErr := mime.ParseMediaType(part.Header.Get("Content-Type"))
			isFormMetadata := part.FormName() == "metadata"
			isRelatedMetadata := mediaType == "multipart/related" && partIndex == 0 && partMediaTypeErr == nil && strings.Contains(partMediaType, "json")
			if isFormMetadata || isRelatedMetadata {
				metadataParts++
				if metadataParts > 1 {
					multipleMetadata = true
					_ = part.Close()
					partIndex++
					continue
				}
				objectName, err = readPrepareRestoreMultipartMetadata(part)
				_ = part.Close()
				if err != nil {
					return "", err
				}
			} else if mediaType == "multipart/related" && partIndex > 0 && partMediaTypeErr == nil && strings.Contains(partMediaType, "json") {
				metadata, parseErr := readPrepareRestoreMultipartMetadata(part)
				_ = part.Close()
				if parseErr != nil {
					return "", parseErr
				}
				if metadata != "" {
					multipleMetadata = true
				}
			} else {
				_ = part.Close()
			}
			partIndex++
		}
		if multipleMetadata || metadataParts > 1 {
			return "", fmt.Errorf("multipart request has multiple metadata parts")
		}
		if metadataParts != 1 {
			return "", fmt.Errorf("multipart metadata part missing")
		}
		return objectName, nil
	}
	if strings.Contains(mediaType, "json") {
		return decodeGCSMetadataName(body)
	}
	return "", fmt.Errorf("unrecognized GCS mutation content type")
}

func readPrepareRestoreMultipartMetadata(part *multipart.Part) (string, error) {
	const maxMetadataBytes = 64 << 10
	data, err := io.ReadAll(io.LimitReader(part, maxMetadataBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxMetadataBytes {
		return "", fmt.Errorf("multipart metadata exceeds observer bound")
	}
	return decodeGCSMetadataName(data)
}

func decodeGCSMetadataName(data []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	opening, err := decoder.Token()
	if err != nil {
		return "", err
	}
	if opening != json.Delim('{') {
		return "", fmt.Errorf("GCS metadata must be a JSON object")
	}
	var name string
	nameSeen := false
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return "", err
		}
		field, ok := fieldToken.(string)
		if !ok {
			return "", fmt.Errorf("GCS metadata field name is not a string")
		}
		if field == "name" {
			if nameSeen {
				return "", fmt.Errorf("GCS metadata has duplicate name fields")
			}
			nameSeen = true
			if err := decoder.Decode(&name); err != nil {
				return "", fmt.Errorf("decode GCS metadata name: %w", err)
			}
			continue
		}
		var ignored json.RawMessage
		if err := decoder.Decode(&ignored); err != nil {
			return "", err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return "", fmt.Errorf("GCS metadata object is incomplete")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", fmt.Errorf("GCS metadata has trailing JSON")
		}
		return "", fmt.Errorf("GCS metadata trailing data: %w", err)
	}
	if nameSeen && name == "" {
		return "", fmt.Errorf("GCS metadata has an empty name")
	}
	return name, nil
}

func prepareRestoreRequestFingerprint(u *url.URL) string {
	return prepareRestoreURLFingerprint(u)
}

func prepareRestoreURLFingerprint(u *url.URL) string {
	if u == nil {
		return ""
	}
	sum := sha256.Sum256([]byte(u.EscapedPath() + "?" + u.RawQuery))
	return hex.EncodeToString(sum[:])
}

func isAllowedPrepareRestoreMutationKey(key, root string) bool {
	if key == "" || strings.Contains(key, "\\") || strings.Contains(key, "%") || path.Clean(key) != key || strings.HasPrefix(key, "../") {
		return false
	}
	if key == path.Join(root, "archive/GC_LOCK") || key == path.Join(root, "checkpoint/PUBLISHER") {
		return true
	}
	for _, prefix := range []string{path.Join(root, "archive/recovery-pins") + "/", path.Join(root, "checkpoint/recovery-pins") + "/"} {
		if strings.HasPrefix(key, prefix) {
			leaf := strings.TrimPrefix(key, prefix)
			return leaf != "" && !strings.Contains(leaf, "/") && leaf != "." && leaf != ".."
		}
	}
	return false
}

func assertPreparedRestoreMutationAllowlist(records []prepareRestoreMutation, root string) error {
	if len(records) == 0 {
		return fmt.Errorf("no provider mutations were observed during PrepareRestore")
	}
	for _, record := range records {
		if record.Denied || record.Status == 0 || !isAllowedPrepareRestoreMutationKey(record.Key, root) {
			return fmt.Errorf("unexpected provider mutation method=%s key=%q status=%d", record.Method, record.Key, record.Status)
		}
	}
	return nil
}

func TestPrepareRestoreProviderMutationParsing(t *testing.T) {
	root := "prefix/cluster"
	allowed := root + "/archive/recovery-pins/pin-1"
	newRequest := func(method, target, contentType string, body []byte) *http.Request {
		r := httptest.NewRequest(method, target, bytes.NewReader(body))
		r.Header.Set("Content-Type", contentType)
		return r
	}
	makeRelated := func(t *testing.T, metadata ...string) (string, []byte) {
		t.Helper()
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		for _, document := range metadata {
			header := make(textproto.MIMEHeader)
			header.Set("Content-Type", "application/json; charset=UTF-8")
			part, err := writer.CreatePart(header)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(part, document); err != nil {
				t.Fatal(err)
			}
		}
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", "application/octet-stream")
		media, err := writer.CreatePart(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(media, "payload"); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return "multipart/related; boundary=" + writer.Boundary(), body.Bytes()
	}
	t.Run("GCS SDK multipart/related metadata header", func(t *testing.T) {
		contentType, body := makeRelated(t, `{"name":"`+allowed+`"}`)
		r := newRequest(http.MethodPost, "http://emulator/upload/storage/v1/b/bucket/o?uploadType=multipart&name="+url.QueryEscape(allowed), contentType, body)
		key, err := prepareRestoreMutationKey(r, body, "gcs", nil)
		if err != nil || key != allowed {
			t.Fatalf("SDK multipart/related name=%q err=%v", key, err)
		}
	})
	t.Run("GCS resumable query exact name", func(t *testing.T) {
		r := newRequest(http.MethodPost, "http://emulator/upload/storage/v1/b/bucket/o?uploadType=resumable&name="+url.QueryEscape(allowed), "application/json", []byte(`{}`))
		key, err := prepareRestoreMutationKey(r, []byte(`{}`), "gcs", nil)
		if err != nil || key != allowed {
			t.Fatalf("query name=%q err=%v", key, err)
		}
	})
	t.Run("unknown GCS name rejected", func(t *testing.T) {
		r := newRequest(http.MethodPost, "http://emulator/upload/storage/v1/b/bucket/o?uploadType=resumable", "application/json", []byte(`{"name":"other/object"}`))
		key, err := prepareRestoreMutationKey(r, []byte(`{"name":"other/object"}`), "gcs", nil)
		if err != nil || isAllowedPrepareRestoreMutationKey(key, root) {
			t.Fatalf("unknown name accepted: key=%q err=%v", key, err)
		}
	})
	t.Run("missing GCS name rejected", func(t *testing.T) {
		r := newRequest(http.MethodPost, "http://emulator/upload/storage/v1/b/bucket/o?uploadType=resumable", "application/json", []byte(`{}`))
		if _, err := prepareRestoreMutationKey(r, []byte(`{}`), "gcs", nil); err == nil {
			t.Fatal("missing object name accepted")
		}
	})
	t.Run("duplicate JSON name rejected", func(t *testing.T) {
		if _, err := decodeGCSMetadataName([]byte(`{"name":"` + allowed + `","name":"` + allowed + `"}`)); err == nil {
			t.Fatal("duplicate JSON name accepted")
		}
	})
	t.Run("trailing JSON object rejected", func(t *testing.T) {
		if _, err := decodeGCSMetadataName([]byte(`{"name":"` + allowed + `"}{"name":"other/object"}`)); err == nil {
			t.Fatal("trailing JSON object accepted")
		}
	})
	t.Run("multiple SDK metadata parts rejected", func(t *testing.T) {
		contentType, body := makeRelated(t, `{"name":"`+allowed+`"}`, `{"name":"`+allowed+`"}`)
		if _, err := gcsObjectNameFromBody(contentType, body); err == nil {
			t.Fatal("multiple metadata parts accepted")
		}
	})
	t.Run("query and body name conflict rejected", func(t *testing.T) {
		body := []byte(`{"name":"other/object"}`)
		target := "http://emulator/upload/storage/v1/b/bucket/o?uploadType=resumable&name=" + url.QueryEscape(allowed)
		r := newRequest(http.MethodPost, target, "application/json", body)
		if _, err := prepareRestoreMutationKey(r, body, "gcs", nil); err == nil {
			t.Fatal("conflicting query and body object names accepted")
		}
	})
	t.Run("HEAD is not mutation", func(t *testing.T) {
		if isPrepareRestoreMutation(http.MethodHead) {
			t.Fatal("HEAD treated as mutation")
		}
	})
	t.Run("DELETE canonical control allowed", func(t *testing.T) {
		if !isPrepareRestoreMutation(http.MethodDelete) || !isAllowedPrepareRestoreMutationKey(root+"/archive/GC_LOCK", root) {
			t.Fatal("canonical control DELETE was not recognized")
		}
	})
	t.Run("escaped traversal rejected", func(t *testing.T) {
		r := newRequest(http.MethodPut, "http://emulator/bucket/prefix%2f..%2fcluster%2farchive%2fGC_LOCK", "application/octet-stream", nil)
		if _, err := prepareRestoreMutationKey(r, nil, "s3", nil); err == nil {
			t.Fatal("escaped traversal accepted")
		}
	})
	t.Run("double escaped path rejected", func(t *testing.T) {
		r := newRequest(http.MethodPut, "http://emulator/bucket/prefix/%252e%252e/cluster/archive/GC_LOCK", "application/octet-stream", nil)
		if _, err := prepareRestoreMutationKey(r, nil, "s3", nil); err == nil {
			t.Fatal("double escaped path accepted")
		}
	})
}

func TestPrepareRestoreProviderObserverForwardsBodyAndTracksResumableUpload(t *testing.T) {
	var gotBody []byte
	var gotHost string
	var gotAuthorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotAuthorization = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "http://"+r.Host+"/upload/session?token=opaque")
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	t.Setenv("RHIZA_EMBEDDED_PROVIDER", "gcs")
	observer := newPrepareRestoreMutationObserver(t, upstream.URL)
	observer.setRoot("prefix/cluster")
	observer.begin()
	body := []byte(`{"name":"prefix/cluster/archive/recovery-pins/pin-a"}`)
	request, err := http.NewRequest(http.MethodPost, observer.endpoint()+"/upload/storage/v1/b/bucket/o?uploadType=resumable", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer signature-preservation-test")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	location := response.Header.Get("Location")
	_ = response.Body.Close()
	parsedLocation, err := url.Parse(location)
	if err != nil || parsedLocation.Host != observer.listener.Addr().String() {
		t.Fatalf("resumable location not kept on observer: %q err=%v", location, err)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("forwarded body changed: got=%q want=%q", gotBody, body)
	}
	if gotHost != observer.listener.Addr().String() {
		t.Fatalf("request Host changed: got=%q want=%q", gotHost, observer.listener.Addr().String())
	}
	if gotAuthorization != "Bearer signature-preservation-test" {
		t.Fatalf("authorization/signature header changed: got=%q", gotAuthorization)
	}
	put, err := http.NewRequest(http.MethodPut, location, bytes.NewReader([]byte("chunk")))
	if err != nil {
		t.Fatal(err)
	}
	putResponse, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	_ = putResponse.Body.Close()
	records := observer.end()
	if len(records) != 2 || records[0].Key != "prefix/cluster/archive/recovery-pins/pin-a" || records[1].Key != records[0].Key {
		t.Fatalf("resumable upload mutations not both observed against exact object: %+v", records)
	}
}

func TestComparePreparedRestoreArchiveImages(t *testing.T) {
	t.Run("only validated expired pins may be added", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err != nil {
			t.Fatalf("valid pin lifecycle refused: %v", err)
		}
	})
	t.Run("canonical mutation refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		fixture.after[path.Join(fixture.prefix, fixture.cluster, "archive/head.bin")] = []byte("mutated raw head")
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("canonical archive mutation accepted")
		}
	})
	t.Run("active archive pin refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		setPreparedPinLease(t, fixture, path.Join(fixture.prefix, fixture.cluster, "archive/recovery-pins")+"/", fixture.now.Add(time.Minute).UnixMilli())
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("active archive pin accepted")
		}
	})
	t.Run("active checkpoint pin refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		setPreparedPinLease(t, fixture, path.Join(fixture.prefix, fixture.cluster, "checkpoint/recovery-pins")+"/", fixture.now.Add(time.Minute).UnixMilli())
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("active checkpoint pin accepted")
		}
	})
	t.Run("changed pre-existing pin refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		key := firstMapKeyWithPrefix(fixture.before, path.Join(fixture.prefix, fixture.cluster, "archive/recovery-pins")+"/")
		fixture.after[key] = append(fixture.after[key], 'x')
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("changed pre-existing pin accepted")
		}
	})
	t.Run("unexpected object refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		fixture.after[path.Join(fixture.prefix, fixture.cluster, "archive/unexpected-object")] = []byte("mutation")
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("unexpected archive object accepted")
		}
	})
	t.Run("same tip with wrong nonzero tail hash refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		mutateNewArchivePin(t, fixture, func(pin *preparedArchivePin) { pin.TailHash[0] ^= 0xff })
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("same-tip recovery pin with another nonzero tail hash accepted")
		}
	})
	t.Run("same tip with wrong nonzero tail object refused", func(t *testing.T) {
		fixture := preparedRestoreImagePair(t)
		mutateNewArchivePin(t, fixture, func(pin *preparedArchivePin) { pin.TailObject++ })
		if err := comparePreparedRestoreArchiveImages(fixture.before, fixture.after, fixture.prefix, fixture.cluster, fixture.node, fixture.archiveTip, fixture.archiveBase, fixture.expectedArchivePin, fixture.currentCheckpoint, fixture.now); err == nil {
			t.Fatal("same-tip recovery pin with another nonzero tail object accepted")
		}
	})
}

type preparedRestoreImageFixture struct {
	before, after      map[string][]byte
	prefix, cluster    string
	node               string
	archiveTip         uint64
	archiveBase        uint64
	expectedArchivePin preparedArchivePin
	currentCheckpoint  *checkpoint.Checkpoint
	now                time.Time
}

func preparedRestoreImagePair(t *testing.T) preparedRestoreImageFixture {
	t.Helper()
	ctx := context.Background()
	prefix, cluster, node := "test-prefix", "test-cluster", "fresh-learner"
	rootPrefix := path.Join(prefix, cluster)
	bucket := objstore.NewInMemBucket()
	archive := recovery.NewManager(bucket, rootPrefix, 1)
	t.Cleanup(archive.Close)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "fixture-voter", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "fixture-voter"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("archive-tail-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoints := checkpoint.NewManager(bucket, rootPrefix, t.TempDir(), 1)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	files, _, closeFiles, err := material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFiles)
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "fixture-publisher", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	rootCheckpoint, err := checkpoints.CreateFiles(ctx, claim, sources, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.PromoteCertifiedCurrent(ctx, rootCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.Load(ctx); err != nil {
		t.Fatal(err)
	}
	current := checkpoints.Latest()
	if current == nil {
		t.Fatal("checkpoint manager did not load the product-published CURRENT")
	}
	priorArchivePin, err := archive.BeginRecoverySnapshot(ctx, "prior-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := priorArchivePin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	priorCheckpointPin, err := checkpoints.PinRecoveryRoot(ctx, current, "prior-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := priorCheckpointPin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := capturePreparedObjectImage(t, ctx, bucket)
	expectedArchivePin := requirePreparedArchivePin(t, before, prefix, cluster, "prior-owner")
	if expectedArchivePin.Tip <= expectedArchivePin.Base || expectedArchivePin.TailHash == ([32]byte{}) || expectedArchivePin.TailObject == 0 {
		t.Fatalf("fixture must contain a real nonempty archive tail: %+v", expectedArchivePin)
	}
	owner := "startup-prepare-" + node + "-0123456789abcdef"
	archivePin, err := archive.BeginRecoverySnapshot(ctx, owner, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	checkpointPin, err := checkpoints.PinRecoveryRoot(ctx, current, owner, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := archivePin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := checkpointPin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	after := capturePreparedObjectImage(t, ctx, bucket)
	now := time.Now()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	return preparedRestoreImageFixture{
		before: before, after: after, prefix: prefix, cluster: cluster, node: node,
		archiveTip: uint64(archive.Tip()), archiveBase: uint64(expectedArchivePin.Base), expectedArchivePin: expectedArchivePin,
		currentCheckpoint: current, now: now,
	}
}

func capturePreparedObjectImage(t *testing.T, ctx context.Context, bucket objstore.Bucket) map[string][]byte {
	t.Helper()
	image := make(map[string][]byte)
	err := bucket.Iter(ctx, "", func(name string) error {
		reader, err := bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		image[name] = data
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		t.Fatalf("capture product-published object image: %v", err)
	}
	return image
}

func requirePreparedArchivePin(t *testing.T, image map[string][]byte, prefix, cluster, owner string) preparedArchivePin {
	t.Helper()
	ownerHash := sha256.Sum256([]byte(owner))
	name := path.Join(prefix, cluster, "archive/recovery-pins", hex.EncodeToString(ownerHash[:]))
	var pin preparedArchivePin
	if err := decodePreparedJSON(image[name], &pin); err != nil {
		t.Fatalf("decode baseline recovery snapshot pin %q: %v", name, err)
	}
	if pin.OwnerID != owner {
		t.Fatalf("baseline recovery pin owner=%q want=%q", pin.OwnerID, owner)
	}
	return pin
}

func setPreparedPinLease(t *testing.T, fixture preparedRestoreImageFixture, prefix string, lease int64) {
	t.Helper()
	for name, data := range fixture.after {
		if !strings.HasPrefix(name, prefix) || fixture.before[name] != nil {
			continue
		}
		if strings.Contains(prefix, "/archive/recovery-pins/") {
			var pin preparedArchivePin
			if err := decodePreparedJSON(data, &pin); err != nil {
				t.Fatal(err)
			}
			pin.LeaseUntilMS = lease
			fixture.after[name], _ = json.Marshal(pin)
			return
		}
		var pin preparedCheckpointPin
		if err := decodePreparedJSON(data, &pin); err != nil {
			t.Fatal(err)
		}
		pin.LeaseUntilMS = lease
		fixture.after[name], _ = json.Marshal(pin)
		return
	}
	t.Fatalf("no newly generated pin under %q", prefix)
}

func mutateNewArchivePin(t *testing.T, fixture preparedRestoreImageFixture, mutate func(*preparedArchivePin)) {
	t.Helper()
	prefix := path.Join(fixture.prefix, fixture.cluster, "archive/recovery-pins") + "/"
	for name, data := range fixture.after {
		if !strings.HasPrefix(name, prefix) || fixture.before[name] != nil {
			continue
		}
		var pin preparedArchivePin
		if err := decodePreparedJSON(data, &pin); err != nil {
			t.Fatal(err)
		}
		mutate(&pin)
		fixture.after[name], _ = json.Marshal(pin)
		return
	}
	t.Fatal("no newly generated archive pin to mutate")
}

func firstMapKeyWithPrefix(image map[string][]byte, prefix string) string {
	for name := range image {
		if strings.HasPrefix(name, prefix) {
			return name
		}
	}
	return ""
}
