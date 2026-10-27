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
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/recovery"
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
	// every source is closed so the full-image comparison cannot race a valid
	// background checkpoint publication.
	if err := closePreparedArchiveSource(source); err != nil {
		t.Fatalf("quiesce source archive writers: %v", err)
	}

	archiveBefore := embeddedRemoteImage(t, base)
	learnerID, learnerToken := "fresh-learner", generation+"-learner-token"
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

	if err := prepareRestoreWithoutRuntimeCredentials(ctx, prepared); err != nil {
		t.Fatalf("prepare learner materialization: %v", err)
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
	if err := comparePreparedRestoreArchiveImages(archiveBefore, archiveAfter, prepared.ObjStorePrefix, string(prepared.ClusterID), string(prepared.NodeID), time.Now()); err != nil {
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
}

type preparedArchiveHead struct {
	ConfigID      uint            `json:"config_id"`
	Generation    uint64          `json:"Generation"`
	Base          uint64          `json:"base"`
	BasePrefix    [32]byte        `json:"base_prefix"`
	BaseSeal      json.RawMessage `json:"base_seal,omitempty"`
	BaseDecision  json.RawMessage `json:"base_decision,omitempty"`
	BaseAnchor    json.RawMessage `json:"base_anchor,omitempty"`
	LineageAnchor json.RawMessage `json:"lineage_anchor,omitempty"`
	Tip           uint64          `json:"tip"`
	TailHash      [32]byte        `json:"tail_hash"`
	TailObject    uint64          `json:"tail_object"`
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

type preparedCheckpointPointer struct {
	Index    uint64 `json:"index"`
	RootHash string `json:"root_hash"`
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
func comparePreparedRestoreArchiveImages(before, after map[string][]byte, prefix, clusterID, nodeID string, now time.Time) error {
	root := path.Join(prefix, clusterID)
	archivePins := path.Join(root, "archive/recovery-pins") + "/"
	checkpointPins := path.Join(root, "checkpoint/recovery-pins") + "/"
	headKey := path.Join(root, "archive/head.bin")
	currentKey := path.Join(root, "checkpoint/CURRENT")
	gcLockKey := path.Join(root, "archive/GC_LOCK")
	checkpointPublisherKey := path.Join(root, "checkpoint/PUBLISHER")
	var head preparedArchiveHead
	if err := decodePreparedJSON(before[headKey], &head); err != nil {
		return fmt.Errorf("decode original archive head: %w", err)
	}
	var pointer preparedCheckpointPointer
	if err := decodePreparedJSON(before[currentKey], &pointer); err != nil {
		return fmt.Errorf("decode original checkpoint pointer: %w", err)
	}
	if head.Tip == 0 || head.TailObject == 0 || pointer.Index == 0 || len(pointer.RootHash) != 64 || !isLowerHexString(pointer.RootHash) {
		return fmt.Errorf("original archive/checkpoint anchors are incomplete")
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
			if pin.Base != head.Base || pin.Tip != head.Tip || pin.TailHash != head.TailHash || pin.TailObject != head.TailObject {
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
			if pin.ConfigID != 1 || pin.Index != pointer.Index || pin.RootHash != pointer.RootHash || !ok || !bytes.Equal(pin.Root, canonicalRoot) || hex.EncodeToString(rootHash[:]) != pin.RootHash {
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

func TestComparePreparedRestoreArchiveImages(t *testing.T) {
	now := time.Now()
	t.Run("only validated expired pins may be added", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err != nil {
			t.Fatalf("valid pin lifecycle refused: %v", err)
		}
	})
	t.Run("canonical mutation refused", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		after[path.Join(prefix, cluster, "archive/head.bin")] = []byte(`{}`)
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err == nil {
			t.Fatal("canonical archive mutation accepted")
		}
	})
	t.Run("active archive pin refused", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		key := path.Join(prefix, cluster, "archive/recovery-pins") + "/"
		for name, data := range after {
			if strings.HasPrefix(name, key) && before[name] == nil {
				var pin preparedArchivePin
				if err := json.Unmarshal(data, &pin); err != nil {
					t.Fatal(err)
				}
				pin.LeaseUntilMS = now.Add(time.Minute).UnixMilli()
				after[name], _ = json.Marshal(pin)
			}
		}
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err == nil {
			t.Fatal("active archive pin accepted")
		}
	})
	t.Run("active checkpoint pin refused", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		key := path.Join(prefix, cluster, "checkpoint/recovery-pins") + "/"
		for name, data := range after {
			if strings.HasPrefix(name, key) && before[name] == nil {
				var pin preparedCheckpointPin
				if err := json.Unmarshal(data, &pin); err != nil {
					t.Fatal(err)
				}
				pin.LeaseUntilMS = now.Add(time.Minute).UnixMilli()
				after[name], _ = json.Marshal(pin)
			}
		}
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err == nil {
			t.Fatal("active checkpoint pin accepted")
		}
	})
	t.Run("changed pre-existing pin refused", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		key := path.Join(prefix, cluster, "archive/recovery-pins") + "/old"
		before[key], after[key] = []byte("old-pin"), []byte("changed-pin")
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err == nil {
			t.Fatal("changed pre-existing pin accepted")
		}
	})
	t.Run("unexpected object refused", func(t *testing.T) {
		before, after, prefix, cluster, node := preparedRestoreImagePair(t, now)
		after[path.Join(prefix, cluster, "archive/unexpected-object")] = []byte("mutation")
		if err := comparePreparedRestoreArchiveImages(before, after, prefix, cluster, node, now); err == nil {
			t.Fatal("unexpected archive object accepted")
		}
	})
}

func preparedRestoreImagePair(t *testing.T, now time.Time) (map[string][]byte, map[string][]byte, string, string, string) {
	t.Helper()
	prefix, cluster, node := "test-prefix", "test-cluster", "fresh-learner"
	root := path.Join(prefix, cluster)
	owner := "startup-prepare-" + node + "-0123456789abcdef"
	ownerHash := sha256.Sum256([]byte(owner))
	var tailHash [32]byte
	tailHash[0] = 9
	head, err := json.Marshal(preparedArchiveHead{ConfigID: 1, Generation: 1, Base: 1, Tip: 2, TailHash: tailHash, TailObject: 2})
	if err != nil {
		t.Fatal(err)
	}
	rootJSON := []byte(`{"config_id":1,"index":1}`)
	rootHash := sha256.Sum256(rootJSON)
	rootHashText := hex.EncodeToString(rootHash[:])
	checkpointPointer, err := json.Marshal(preparedCheckpointPointer{Index: 1, RootHash: rootHashText})
	if err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{
		path.Join(root, "archive/head.bin"):                                            head,
		path.Join(root, "checkpoint/CURRENT"):                                          checkpointPointer,
		path.Join(root, "checkpoint/roots/00000000000000000001_"+rootHashText+".json"): rootJSON,
		path.Join(root, "archive/canonical-object"):                                    []byte("unchanged"),
		path.Join(root, "archive/recovery-pins") + "/previous-owner":                   []byte("old-pin"),
	}
	before[path.Join(root, "archive/GC_LOCK")], _ = json.Marshal(preparedArchiveGCLock{OwnerID: "previous-snapshot", Generation: 3, LeaseUntilMS: now.Add(-time.Second).UnixMilli()})
	before[path.Join(root, "checkpoint/PUBLISHER")], _ = json.Marshal(preparedCheckpointPublisher{ConfigID: 1, Generation: 4, OwnerID: "previous-writer", Purpose: "publisher", LeaseUntilMS: now.Add(-time.Second).UnixMilli(), ReservedIndex: 1})
	after := make(map[string][]byte, len(before)+2)
	for name, data := range before {
		after[name] = append([]byte(nil), data...)
	}
	archivePin, err := json.Marshal(preparedArchivePin{OwnerID: owner, Token: "0123456789abcdef0123456789abcdef", Base: 1, Tip: 2, TailHash: tailHash, TailObject: 2, LeaseUntilMS: now.Add(-time.Second).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	checkpointPin, err := json.Marshal(preparedCheckpointPin{ConfigID: 1, OwnerID: owner, Token: "fedcba9876543210fedcba9876543210", Index: 1, RootHash: rootHashText, Root: rootJSON, LeaseUntilMS: now.Add(-time.Second).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	after[path.Join(root, "archive/recovery-pins", hex.EncodeToString(ownerHash[:]))] = archivePin
	after[path.Join(root, "checkpoint/recovery-pins", hex.EncodeToString(ownerHash[:]))] = checkpointPin
	after[path.Join(root, "archive/GC_LOCK")], _ = json.Marshal(preparedArchiveGCLock{OwnerID: "snapshot:" + owner, Generation: 4, LeaseUntilMS: now.Add(-time.Second).UnixMilli()})
	after[path.Join(root, "checkpoint/PUBLISHER")], _ = json.Marshal(preparedCheckpointPublisher{ConfigID: 1, Generation: 5, OwnerID: "checkpoint-gc", Purpose: "maintenance", LeaseUntilMS: now.Add(-time.Second).UnixMilli()})
	return before, after, prefix, cluster, node
}
