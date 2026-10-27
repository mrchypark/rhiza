package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestPrepareRestoreMaterializesCertifiedCheckpointAndSuffix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	source, _, bucket, transport := newMembershipOperationNode(t)
	// The shared fixture pre-registers a,b and then constructs a three-voter
	// core for membership-operation tests. Recreate the still-empty source cores
	// with the registered two-voter bootstrap so the archive and immutable
	// membership record describe the same real source generation.
	bootstrap := append([]quepaxa.Member(nil), source.config.Members[:2]...)
	source.config.Members = bootstrap
	transport.ids = []quepaxa.NodeID{bootstrap[0].ID, bootstrap[1].ID}
	transport.cores = make(map[quepaxa.NodeID]*quepaxa.Core, len(bootstrap))
	for _, member := range bootstrap {
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: bootstrap},
			WAL: transport.wals[member.ID], Transport: transport, EnableReconfiguration: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	source.core = transport.cores[bootstrap[0].ID]
	sourceState, err := materializer.Open(filepath.Join(t.TempDir(), "source.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceState.Close()

	propose := func(value []byte) {
		t.Helper()
		if _, _, err := source.core.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
		for slot := quepaxa.Slot(sourceState.Tip() + 1); slot <= source.core.Tip(); slot++ {
			decision, ok := source.core.CertifiedValue(slot)
			if !ok {
				t.Fatalf("missing certified decision at slot %d", slot)
			}
			if err := sourceState.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
				t.Fatal(err)
			}
		}
	}
	createTable, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE prepared_rows (id INTEGER PRIMARY KEY, value TEXT)"}})
	if err != nil {
		t.Fatal(err)
	}
	propose(createTable)
	insertFirst, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "first", SQL: "INSERT INTO prepared_rows VALUES (1, 'checkpoint')"}})
	if err != nil {
		t.Fatal(err)
	}
	propose(insertFirst)
	graph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "graph", Cypher: "CREATE (:PreparedRestore {id: 17})"})
	if err != nil {
		t.Fatal(err)
	}
	propose(graph)

	checkpoints := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	if err := checkpoints.Load(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "prepare-test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := sourceState.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	root, err := checkpoints.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = checkpoints.BindPublisherClaim(ctx, claim, root.Index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := source.core.PrefixHash(quepaxa.Slot(index))
	if !ok {
		t.Fatal("missing source checkpoint prefix")
	}
	next, following, err := source.core.CheckpointLeaderOrders(quepaxa.Slot(index))
	if err != nil {
		t.Fatal(err)
	}
	membership, err := source.core.CheckpointMembership(quepaxa.Slot(index))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(index), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	for _, core := range transport.cores {
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.core.Propose(ctx, sealValue); err != nil {
		t.Fatal(err)
	}
	insertSuffix, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "suffix", SQL: "INSERT INTO prepared_rows VALUES (2, 'suffix')"}})
	if err != nil {
		t.Fatal(err)
	}
	propose(insertSuffix)
	if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	archiveTip := source.archive.Tip()
	registrationKey := filepath.ToSlash(filepath.Join(source.config.ObjStorePrefix, string(source.config.ClusterID), "voters", "membership.json"))
	registration, err := readVoterRegistration(ctx, bucket, registrationKey)
	if err != nil || registration == nil {
		t.Fatalf("read product bootstrap membership registration: %v", err)
	}

	config := *source.config
	config.DataDir = t.TempDir()
	config.NodeID = "fresh-learner"
	config.PreparedPodUID = "pod-uid-new"
	config.Members = append([]quepaxa.Member(nil), source.config.Members[:2]...)
	config.ObjStoreProvider = "s3"
	config.ObjStoreBucket = "test-bucket"
	config.ObjStoreDir = ""
	config.Learner = nil
	config.AdminToken = ""
	config.PeerToken = ""
	config.EnableReconfiguration = true
	var registered recovery.MembershipRecord
	if err := json.Unmarshal(registration, &registered); err != nil {
		t.Fatal(err)
	}
	formattedRecord := []byte(fmt.Sprintf("{\n  \"durability\": %q,\n  \"membership\": %q,\n  \"cluster\": %q,\n  \"version\": %d\n}", registered.Durability, registered.Membership, registered.Cluster, registered.Version))
	if err := bucket.Upload(ctx, registrationKey, bytes.NewReader(formattedRecord)); err != nil {
		t.Fatal(err)
	}
	if err := validatePreparedRestoreMembership(ctx, &config, bucket); err != nil {
		t.Fatalf("semantically equal reordered membership record rejected: %v", err)
	}
	if err := bucket.Upload(ctx, registrationKey, bytes.NewReader(registration)); err != nil {
		t.Fatal(err)
	}
	archiveDir := clonePreparedRestoreArchive(t, ctx, bucket)
	openArchive := func() *objectstore.MeteredBucket {
		return openPreparedRestoreTestBucket(t, archiveDir)
	}
	legacy, err := json.Marshal(recovery.MembershipRecord{Version: 1, Cluster: registered.Cluster, Membership: registered.Membership, Durability: registered.Durability})
	if err != nil {
		t.Fatal(err)
	}
	duplicate := append(bytes.Clone(bytes.TrimSuffix(registration, []byte("}"))), []byte(`,"version":2}`)...)
	trailing := append(bytes.Clone(registration), []byte(" trailing")...)
	for _, tc := range []struct {
		name   string
		remove bool
		data   []byte
	}{
		{name: "missing", remove: true},
		{name: "mismatch", data: []byte(`{"version":2,"cluster":"cluster","membership":"wrong","durability":"async"}`)},
		{name: "legacy", data: legacy},
		{name: "malformed", data: []byte(`{"version":`)},
		{name: "trailing", data: trailing},
		{name: "duplicate-field", data: duplicate},
		{name: "missing-required-field", data: []byte(`{"version":2,"cluster":"cluster","durability":"async"}`)},
	} {
		t.Run("bootstrap-registration-"+tc.name, func(t *testing.T) {
			caseBucket := openPreparedRestoreTestBucket(t, archiveDir)
			if tc.remove {
				if err := caseBucket.Delete(ctx, registrationKey); err != nil {
					t.Fatal(err)
				}
			} else if err := caseBucket.Upload(ctx, registrationKey, bytes.NewReader(tc.data)); err != nil {
				t.Fatal(err)
			}
			beforeRegistration, err := readVoterRegistration(ctx, caseBucket, registrationKey)
			if err != nil {
				t.Fatal(err)
			}
			if err := caseBucket.Close(); err != nil {
				t.Fatal(err)
			}
			failed := config
			failed.DataDir = filepath.Join(t.TempDir(), "fresh")
			err = prepareRestore(ctx, &failed, openPreparedRestoreTestBucket(t, archiveDir))
			if err == nil || !errors.Is(err, ErrVoterStateLost) {
				t.Fatalf("prepare error=%v, want immutable-membership refusal", err)
			}
			freshSnapshot := snapshotPreparedRestoreDataDir(t, failed.DataDir)
			if len(freshSnapshot) != 2 || freshSnapshot["qlog"] != "<dir>" || freshSnapshot[filepath.Join("qlog", "lock.qlog")] != "" {
				t.Fatalf("source refusal wrote beyond the temporary ownership lock: %#v", freshSnapshot)
			}

			failed.DataDir = filepath.Join(t.TempDir(), "interrupted")
			writeInterruptedPreparedRestoreFixture(t, &failed)
			beforeDataDir := snapshotPreparedRestoreDataDir(t, failed.DataDir)
			if err := prepareRestore(ctx, &failed, openPreparedRestoreTestBucket(t, archiveDir)); err == nil || !errors.Is(err, ErrVoterStateLost) {
				t.Fatalf("interrupted restore error=%v, want immutable-membership refusal", err)
			}
			if after := snapshotPreparedRestoreDataDir(t, failed.DataDir); !reflect.DeepEqual(beforeDataDir, after) {
				t.Fatalf("source refusal changed populated interrupted DataDir: before=%#v after=%#v", beforeDataDir, after)
			}
			afterBucket := openPreparedRestoreTestBucket(t, archiveDir)
			afterRegistration, err := readVoterRegistration(ctx, afterBucket, registrationKey)
			if err != nil || !bytes.Equal(beforeRegistration, afterRegistration) {
				t.Fatalf("source refusal changed immutable registration: before=%q after=%q err=%v", beforeRegistration, afterRegistration, err)
			}
			if err := afterBucket.Close(); err != nil {
				t.Fatal(err)
			}
			repairBucket := openPreparedRestoreTestBucket(t, archiveDir)
			if err := repairBucket.Upload(ctx, registrationKey, bytes.NewReader(registration)); err != nil {
				t.Fatalf("restore product registration: %v", err)
			}
			if err := repairBucket.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	lockHeldAtCommit := false
	previousBoundary := preparedRestoreCrashBoundary
	preparedRestoreCrashBoundary = func(stage string) {
		if previousBoundary != nil {
			previousBoundary(stage)
		}
		if stage == "complete-marker-commit" {
			_, lock, err := qlog.Acquire(filepath.Join(config.DataDir, "qlog"))
			if err != nil {
				lockHeldAtCommit = true
				return
			}
			_ = lock.Release()
		}
	}
	execution, prepareErr := prepareRestoreWithExecution(ctx, &config, openArchive())
	preparedRestoreCrashBoundary = previousBoundary
	if prepareErr != nil {
		t.Fatalf("prepare restore without runtime credentials: %v", prepareErr)
	}
	if execution.MarkerNoop || !execution.BucketDrainSucceeded || execution.CleanupErr != nil {
		t.Fatalf("completed restore execution report=%+v, want actual clean bucket drain", execution)
	}
	if !lockHeldAtCommit {
		t.Fatal("qlog lock was not held at complete-marker commit")
	}

	marker, exists, err := readPreparedRestoreMarker(config.DataDir)
	if err != nil || !exists || marker.Phase != "complete" || marker.BaseIndex != index || marker.CertifiedTip != uint64(source.core.Tip()) {
		t.Fatalf("prepared marker=%+v exists=%v err=%v", marker, exists, err)
	}
	completeBefore := snapshotPreparedRestoreDataDir(t, config.DataDir)
	// The configured S3 endpoint is intentionally absent. A matching complete
	// marker must be a local no-op and must not open the unavailable remote.
	noopExecution, err := prepareRestoreWithExecution(ctx, &config, nil)
	if err != nil {
		t.Fatalf("matching complete marker required remote access: %v", err)
	}
	if !noopExecution.MarkerNoop || noopExecution.BucketDrainSucceeded || noopExecution.CleanupErr != nil {
		t.Fatalf("complete-marker no-op report=%+v, want no drain claim", noopExecution)
	}
	unlockErr := errors.New("injected prepared restore qlog unlock failure")
	previousRelease := preparedRestoreReleaseLock
	preparedRestoreReleaseLock = func(lock *qlog.LockFile) error {
		return errors.Join(previousRelease(lock), unlockErr)
	}
	unlockExecution, unlockResultErr := prepareRestoreWithExecution(ctx, &config, nil)
	preparedRestoreReleaseLock = previousRelease
	if !errors.Is(unlockResultErr, unlockErr) || !errors.Is(unlockExecution.CleanupErr, unlockErr) || !unlockExecution.MarkerNoop || unlockExecution.BucketDrainSucceeded {
		t.Fatalf("qlog unlock failure report=%+v err=%v", unlockExecution, unlockResultErr)
	}
	if completeAfter := snapshotPreparedRestoreDataDir(t, config.DataDir); !reflect.DeepEqual(completeBefore, completeAfter) {
		t.Fatalf("complete-marker retry changed DataDir: before=%#v after=%#v", completeBefore, completeAfter)
	}
	if source.archive.Tip() != archiveTip {
		t.Fatalf("preparation changed source archive tip from %d to %d", archiveTip, source.archive.Tip())
	}
	runtimeWithoutLearner := config
	runtimeWithoutLearner.AdminToken = "runtime-admin-token"
	runtimeWithoutLearner.PeerToken = "runtime-peer-token"
	if err := New(&runtimeWithoutLearner).Open(ctx); err == nil {
		t.Fatal("runtime open accepted a restore-only config without Learner")
	}
	databaseBeforeOpen, err := os.ReadFile(filepath.Join(config.DataDir, "sqlite.db"))
	if err != nil {
		t.Fatal(err)
	}
	markerBeforeOpen, err := os.ReadFile(filepath.Join(config.DataDir, preparedRestoreMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	withLearner := config
	withLearner.Learner = &quepaxa.Member{ID: config.NodeID, PublicKey: peerMember(string(config.ClusterID), quepaxa.NodeID(config.NodeID), "fresh-token").PublicKey}
	withLearner.PeerToken = "runtime-peer-token"
	if err := New(&withLearner).Open(ctx); err == nil || !strings.Contains(err.Error(), "admin token") {
		t.Fatalf("runtime open without admin/peer credentials error=%v, want admin-token refusal", err)
	}
	assertPreparedRuntimeOpenPreservedFiles(t, config.DataDir, databaseBeforeOpen, markerBeforeOpen)
	withAdmin := withLearner
	withAdmin.AdminToken = "runtime-admin-token"
	withAdmin.PeerToken = ""
	if err := New(&withAdmin).Open(ctx); err == nil || !strings.Contains(err.Error(), "peer token") {
		t.Fatalf("runtime open without peer credential error=%v, want peer-token refusal", err)
	}
	assertPreparedRuntimeOpenPreservedFiles(t, config.DataDir, databaseBeforeOpen, markerBeforeOpen)
	if manifests, err := filepath.Glob(filepath.Join(config.DataDir, "qlog", "manifest_*.bin")); err != nil || len(manifests) != 0 {
		t.Fatalf("prepare created runtime WAL manifests %v, err=%v", manifests, err)
	}
	if _, err := os.Stat(filepath.Join(config.DataDir, "qlog", "voter-identity.json")); !os.IsNotExist(err) {
		t.Fatalf("prepare created voter identity: %v", err)
	}
	blockedConfig := config
	blockedConfig.DataDir = t.TempDir()
	if err := New(&blockedConfig).Open(ctx); err == nil {
		t.Fatal("runtime startup accepted a missing prepared restore marker")
	}
	if manifests, err := filepath.Glob(filepath.Join(blockedConfig.DataDir, "qlog", "manifest_*.bin")); err != nil || len(manifests) != 0 {
		t.Fatalf("marker refusal opened a runtime WAL: manifests=%v err=%v", manifests, err)
	}

	state, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := state.QueryResult(ctx, "SELECT id, value FROM prepared_rows ORDER BY id", nil)
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][1] != "checkpoint" || rows.Rows[1][1] != "suffix" {
		_ = state.Close()
		t.Fatalf("restored SQL rows=%+v err=%v", rows.Rows, err)
	}
	graphResult, err := state.GraphQuery(ctx, "MATCH (n:PreparedRestore) RETURN n.id", nil)
	if err != nil || len(graphResult.Rows) != 1 || graphResult.Rows[0][0] != int64(17) {
		_ = state.Close()
		t.Fatalf("restored Graph rows=%+v err=%v", graphResult.Rows, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	runPreparedRestoreCloseFailureRegression(t, ctx, config, archiveDir)
	runPreparedRestoreCleanupJoinRegression(t, ctx, config, archiveDir)

	markerBytes := markerBeforeOpen
	if err := prepareRestore(ctx, &config, openArchive()); err != nil {
		t.Fatalf("same-Pod prepare retry: %v", err)
	}
	markerRetry, err := os.ReadFile(filepath.Join(config.DataDir, preparedRestoreMarkerName))
	if err != nil || string(markerRetry) != string(markerBytes) {
		t.Fatalf("same-Pod retry changed marker: err=%v", err)
	}
	changed := config
	changed.PreparedPodUID = "different-pod"
	if err := validatePreparedRestoreMarker(&changed); err == nil {
		t.Fatal("different Pod UID accepted completed DataDir")
	}

	for _, stage := range []string{"intent-before-write", "intent-write-start", "intent-durable", "observer-open", "marker-pending-write-start"} {
		t.Run(stage, func(t *testing.T) {
			crashConfig := config
			crashConfig.DataDir = filepath.Join(t.TempDir(), "data")
			crashConfig.PreparedPodUID = "pod-crash"
			tmpDir := filepath.Join(t.TempDir(), "temp")
			if err := os.MkdirAll(tmpDir, 0700); err != nil {
				t.Fatal(err)
			}
			ready := filepath.Join(t.TempDir(), "ready")
			crashBucketDir := archiveDir
			request, err := json.Marshal(preparedRestoreCrashRequest{Config: crashConfig, Stage: stage, ReadyFile: ready, Operation: "prepare", BucketDir: crashBucketDir})
			if err != nil {
				t.Fatal(err)
			}
			if stage == "marker-pending-write-start" {
				request, err = json.Marshal(preparedRestoreCrashRequest{Config: crashConfig, Stage: stage, ReadyFile: ready, Operation: "pending-marker"})
				if err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestPrepareRestoreCrashHelper$")
			cmd.Env = make([]string, 0, len(os.Environ())+2)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "TMPDIR=") && !strings.HasPrefix(value, "RHIZA_PREPARE_RESTORE_CRASH=") {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "TMPDIR="+tmpDir, "RHIZA_PREPARE_RESTORE_CRASH="+string(request))
			startLocalNodeCrashChild(t, cmd, ready)
			waitForLocalNodeCrashBoundary(t, cmd, ready)

			intent, exists, err := readPreparedRestoreMarkerForConfig(crashConfig.DataDir, func() preparedRestoreMarker {
				marker, markerErr := preparedMarkerForConfig(&crashConfig)
				if markerErr != nil {
					t.Fatal(markerErr)
				}
				marker.Phase = "preparing"
				return marker
			}())
			if err != nil {
				t.Fatalf("read interrupted intent: %v", err)
			}
			if stage == "intent-before-write" {
				if exists {
					t.Fatal("pre-marker crash unexpectedly left a marker")
				}
			} else if !exists || intent.Phase != "preparing" || intent.CertifiedTip != 0 {
				t.Fatalf("interrupted marker=%+v exists=%v", intent, exists)
			}
			if stage == "observer-open" {
				contents, err := os.ReadDir(tmpDir)
				if err != nil || len(contents) == 0 {
					t.Fatalf("observer WAL was not created before kill: entries=%v err=%v", contents, err)
				}
			}
			if stage == "marker-pending-write-start" {
				info, err := os.Stat(filepath.Join(crashConfig.DataDir, preparedRestorePendingName))
				if err != nil || info.Size() != 0 {
					t.Fatalf("marker pending write boundary: info=%v err=%v", info, err)
				}
			}
			// The child is reaped before its private temp tree is cleaned or the
			// same Pod retries against the live certified source.
			if err := os.RemoveAll(tmpDir); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(tmpDir, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("TMPDIR", tmpDir)
			if err := prepareRestore(ctx, &crashConfig, openArchive()); err != nil {
				t.Fatalf("same-Pod retry after %s hard kill: %v", stage, err)
			}
			completed, exists, err := readPreparedRestoreMarker(crashConfig.DataDir)
			if err != nil || !exists || completed.Phase != "complete" || completed.CertifiedTip != uint64(source.core.Tip()) {
				t.Fatalf("retry marker=%+v exists=%v err=%v", completed, exists, err)
			}
			if _, err := os.Stat(filepath.Join(crashConfig.DataDir, preparedRestorePendingName)); !os.IsNotExist(err) {
				t.Fatalf("pending marker remained after completion: %v", err)
			}
		})
	}
}

func TestValidatePreparedRestoreConfigAllowsNoLearnerButRejectsBootstrapCollisions(t *testing.T) {
	valid := &types.ExecutionConfig{
		NodeID: "fresh", ClusterID: "cluster", DataDir: filepath.Join(t.TempDir(), "data"), PreparedPodUID: "pod",
		ObjStoreProvider: "s3", ObjStoreBucket: "archive", EnableReconfiguration: true,
		Members: []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token")},
	}
	if err := validatePreparedRestoreConfig(valid); err != nil {
		t.Fatalf("restore-only config without learner identity rejected: %v", err)
	}
	validLearner := *valid
	validLearner.Learner = &quepaxa.Member{ID: valid.NodeID, PublicKey: peerMember("cluster", "fresh", "fresh-token").PublicKey}
	if err := validatePreparedRestoreConfig(&validLearner); err != nil {
		t.Fatalf("optional matching learner identity rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*types.ExecutionConfig)
	}{
		{name: "empty-bootstrap-id", mutate: func(c *types.ExecutionConfig) { c.Members[0].ID = "" }},
		{name: "target-is-bootstrap-voter", mutate: func(c *types.ExecutionConfig) { c.NodeID = c.Members[0].ID; c.Learner = nil }},
		{name: "verifier-is-bootstrap-voter", mutate: func(c *types.ExecutionConfig) { c.Members[0].ID = "prepare-fresh" }},
		{name: "duplicate-bootstrap-id", mutate: func(c *types.ExecutionConfig) { c.Members[1].ID = c.Members[0].ID }},
		{name: "missing-bootstrap-key", mutate: func(c *types.ExecutionConfig) { c.Members[0].PublicKey = quepaxa.PublicKey{} }},
		{name: "learner-id-mismatch", mutate: func(c *types.ExecutionConfig) {
			c.Learner = &quepaxa.Member{ID: "other", PublicKey: peerMember("cluster", "other", "other-token").PublicKey}
		}},
		{name: "learner-key-missing", mutate: func(c *types.ExecutionConfig) { c.Learner = &quepaxa.Member{ID: c.NodeID} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := *valid
			candidate.Members = append([]quepaxa.Member(nil), valid.Members...)
			candidate.Learner = validLearner.Learner
			tc.mutate(&candidate)
			if err := validatePreparedRestoreConfig(&candidate); err == nil {
				t.Fatal("accepted invalid prepared restore membership input")
			}
		})
	}
}

func TestPrepareRestoreCompleteMarkerWithoutExistingLockPreservesDataDir(t *testing.T) {
	config := &types.ExecutionConfig{
		NodeID: "fresh", ClusterID: "cluster", DataDir: filepath.Join(t.TempDir(), "data"), PreparedPodUID: "pod",
		ObjStoreProvider: "s3", ObjStoreBucket: "unavailable", EnableReconfiguration: true,
		Members: []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token")},
	}
	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker, err := preparedMarkerForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	intent := marker
	intent.Phase = "preparing"
	if err := writePreparedRestoreIntent(config.DataDir, intent); err != nil {
		t.Fatal(err)
	}
	marker.Phase = "complete"
	if err := writePreparedRestoreMarker(config.DataDir, marker); err != nil {
		t.Fatal(err)
	}
	before := snapshotPreparedRestoreDataDir(t, config.DataDir)
	if err := prepareRestore(context.Background(), config, nil); err == nil {
		t.Fatal("complete marker without its existing qlog lock was accepted")
	}
	if after := snapshotPreparedRestoreDataDir(t, config.DataDir); !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal created or changed DataDir entries: before=%#v after=%#v", before, after)
	}
}

func writeInterruptedPreparedRestoreFixture(t *testing.T, config *types.ExecutionConfig) {
	t.Helper()
	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker, err := preparedMarkerForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	marker.Phase = "preparing"
	if err := writePreparedRestoreIntent(config.DataDir, marker); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(config.DataDir, "qlog"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.DataDir, "qlog", "lock.qlog"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.DataDir, "sqlite.db"), []byte("preserve-existing-materialized-state"), 0600); err != nil {
		t.Fatal(err)
	}
}

func snapshotPreparedRestoreDataDir(t *testing.T, dataDir string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	err := filepath.WalkDir(dataDir, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == dataDir {
			return nil
		}
		rel, err := filepath.Rel(dataDir, name)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			snapshot[rel] = "<dir>"
			return nil
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		snapshot[rel] = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot prepared DataDir: %v", err)
	}
	return snapshot
}

func assertPreparedRuntimeOpenPreservedFiles(t *testing.T, dataDir string, databaseBefore, markerBefore []byte) {
	t.Helper()
	databaseAfter, err := os.ReadFile(filepath.Join(dataDir, "sqlite.db"))
	if err != nil || !bytes.Equal(databaseAfter, databaseBefore) {
		t.Fatalf("runtime-open refusal changed prepared database: err=%v", err)
	}
	markerAfter, err := os.ReadFile(filepath.Join(dataDir, preparedRestoreMarkerName))
	if err != nil || !bytes.Equal(markerAfter, markerBefore) {
		t.Fatalf("runtime-open refusal changed prepared marker: err=%v", err)
	}
	manifests, err := filepath.Glob(filepath.Join(dataDir, "qlog", "manifest_*.bin"))
	if err != nil || len(manifests) != 0 {
		t.Fatalf("runtime-open refusal created WAL manifests %v, err=%v", manifests, err)
	}
}

type preparedRestoreCrashRequest struct {
	Config    types.ExecutionConfig
	Stage     string
	ReadyFile string
	Operation string
	BucketDir string
}

func TestPrepareRestoreCrashHelper(t *testing.T) {
	encoded := os.Getenv("RHIZA_PREPARE_RESTORE_CRASH")
	if encoded == "" {
		return
	}
	var request preparedRestoreCrashRequest
	if err := json.Unmarshal([]byte(encoded), &request); err != nil {
		t.Fatal(err)
	}
	preparedRestoreCrashBoundary = func(stage string) {
		if stage != request.Stage {
			return
		}
		if err := os.WriteFile(request.ReadyFile, []byte(stage), 0600); err != nil {
			t.Fatalf("write crash boundary: %v", err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	if request.Operation == "pending-marker" {
		if err := os.MkdirAll(request.Config.DataDir, 0700); err != nil {
			t.Fatal(err)
		}
		_, lock, err := qlog.Acquire(filepath.Join(request.Config.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Release()
		intent, err := preparedMarkerForConfig(&request.Config)
		if err != nil {
			t.Fatal(err)
		}
		intent.Phase = "preparing"
		if err := writePreparedRestoreIntent(request.Config.DataDir, intent); err != nil {
			t.Fatal(err)
		}
		pending := intent
		pending.Phase = "complete"
		if err := writePreparedRestoreMarker(request.Config.DataDir, pending); err != nil {
			t.Fatal(err)
		}
		t.Fatal("requested pending marker boundary was not reached")
	}
	var bucket *objectstore.MeteredBucket
	if request.BucketDir != "" {
		var err error
		bucket, err = objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: request.BucketDir})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareRestore(context.Background(), &request.Config, bucket); err != nil {
		t.Fatalf("prepare restore before requested crash boundary: %v", err)
	}
	t.Fatal("requested prepare restore boundary was not reached")
}

func clonePreparedRestoreArchive(t *testing.T, ctx context.Context, source *objectstore.MeteredBucket) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "prepared-restore-archive")
	target, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Iter(ctx, "", func(name string) error {
		reader, err := source.Get(ctx, name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		return target.Upload(ctx, name, bytes.NewReader(data))
	}, thanosobjstore.WithRecursiveIter()); err != nil {
		_ = target.Close()
		t.Fatalf("copy certified restore archive: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func openPreparedRestoreTestBucket(t *testing.T, archiveDir string) *objectstore.MeteredBucket {
	t.Helper()
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: archiveDir})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func TestPrepareRestoreFailsClosedForUnknownOrPartialData(t *testing.T) {
	ctx := context.Background()
	config := &types.ExecutionConfig{NodeID: "fresh", ClusterID: "cluster", DataDir: t.TempDir(), PreparedPodUID: "pod", ObjStoreProvider: "s3", ObjStoreBucket: "test", EnableReconfiguration: true, AdminToken: "admin", Members: []quepaxa.Member{peerMember("cluster", "a", "a"), peerMember("cluster", "b", "b")}, Learner: &quepaxa.Member{ID: "fresh", PublicKey: peerMember("cluster", "fresh", "fresh").PublicKey}}
	if err := os.MkdirAll(filepath.Join(config.DataDir, "qlog"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.DataDir, "qlog", "voter-identity.json"), []byte("unknown"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareRestore(ctx, config, nil); err == nil {
		t.Fatal("existing identity without a completion marker was accepted")
	}
	if _, err := os.Stat(filepath.Join(config.DataDir, "qlog", "voter-identity.json")); err != nil {
		t.Fatalf("failed preparation modified unknown identity: %v", err)
	}
	unknownDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(unknownDir, "operator-data"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	unknownConfig := *config
	unknownConfig.DataDir = unknownDir
	if err := prepareRestore(ctx, &unknownConfig, nil); err == nil {
		t.Fatal("unknown DataDir contents were accepted")
	}
	if data, err := os.ReadFile(filepath.Join(unknownDir, "operator-data")); err != nil || string(data) != "keep" {
		t.Fatalf("unknown DataDir file changed: data=%q err=%v", data, err)
	}
	if _, err := os.Lstat(filepath.Join(unknownDir, "qlog")); !os.IsNotExist(err) {
		t.Fatalf("unknown DataDir refusal created a qlog path: %v", err)
	}

	partialDir := t.TempDir()
	partialConfig := *config
	partialConfig.DataDir = partialDir
	intent, err := preparedMarkerForConfig(&partialConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePreparedRestoreIntent(partialDir, intent); err != nil {
		t.Fatal(err)
	}
	partialConfig.PreparedPodUID = "different-pod"
	if err := prepareRestore(ctx, &partialConfig, nil); err == nil {
		t.Fatal("marker without matching source identity was accepted")
	}
	if _, err := os.Stat(filepath.Join(partialDir, preparedRestoreMarkerName)); err != nil {
		t.Fatalf("partial marker was removed: %v", err)
	}
}

func TestPreparedRestoreUIDIsLocalAndMarkerExcludesCredentials(t *testing.T) {
	config := &types.ExecutionConfig{
		NodeID: "fresh", ClusterID: "cluster", DataDir: t.TempDir(), PreparedPodUID: "pod-one",
		ObjStoreProvider: "s3", ObjStoreBucket: "archive", ObjStoreEndpoint: "https://user:secret@storage.example/bucket?token=private",
		ObjStoreAccessKey: "access-secret", ObjStoreSecretKey: "secret-key", ObjStoreSessionToken: "session-secret",
		ObjStoreServiceAccount: "service-account-secret", ObjStoreAzureClientSecret: "azure-client-secret",
		ObjStoreAzureStorageAccountKey: "azure-account-secret", ObjStoreAzureConnectionString: "azure-connection-secret",
		Members: []quepaxa.Member{peerMember("cluster", "a", "a"), peerMember("cluster", "b", "b")},
	}
	first, err := preparedMarkerForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	secondConfig := *config
	secondConfig.PreparedPodUID = "pod-two"
	second, err := preparedMarkerForConfig(&secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	if first.PodUID == second.PodUID || first.SourceHash != second.SourceHash || first.StoreHash != second.StoreHash || first.BootstrapHash != second.BootstrapHash {
		t.Fatalf("Pod UID affected shared source/store binding: first=%+v second=%+v", first, second)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret", "private", "access-secret", "secret-key", "session-secret", "service-account-secret", "azure-client-secret", "azure-account-secret", "azure-connection-secret"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("prepared marker leaked credential %q", secret)
		}
	}
}
