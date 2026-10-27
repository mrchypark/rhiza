package node

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestPrepareRestoreMaterializesCertifiedCheckpointAndSuffix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	source, _, bucket, transport := newMembershipOperationNode(t)
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

	config := *source.config
	config.DataDir = t.TempDir()
	config.NodeID = "fresh-learner"
	config.PreparedPodUID = "pod-uid-new"
	config.ObjStoreProvider = "s3"
	config.ObjStoreBucket = "test-bucket"
	config.ObjStoreDir = ""
	config.Learner = &quepaxa.Member{ID: config.NodeID, PublicKey: peerMember(string(config.ClusterID), quepaxa.NodeID(config.NodeID), "fresh-token").PublicKey}
	config.AdminToken = "admin-token"
	config.EnableReconfiguration = true
	if err := prepareRestore(ctx, &config, bucket); err != nil {
		t.Fatalf("prepare restore: %v", err)
	}

	marker, exists, err := readPreparedRestoreMarker(config.DataDir)
	if err != nil || !exists || marker.Phase != "complete" || marker.BaseIndex != index || marker.CertifiedTip != uint64(source.core.Tip()) {
		t.Fatalf("prepared marker=%+v exists=%v err=%v", marker, exists, err)
	}
	if source.archive.Tip() != archiveTip {
		t.Fatalf("preparation changed source archive tip from %d to %d", archiveTip, source.archive.Tip())
	}
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

	markerBytes, err := os.ReadFile(filepath.Join(config.DataDir, preparedRestoreMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareRestore(ctx, &config, bucket); err != nil {
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
			request, err := json.Marshal(preparedRestoreCrashRequest{Config: crashConfig, Stage: stage, ReadyFile: ready, Operation: "prepare"})
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
			if err := prepareRestore(ctx, &crashConfig, bucket); err != nil {
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

type preparedRestoreCrashRequest struct {
	Config    types.ExecutionConfig
	Stage     string
	ReadyFile string
	Operation string
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
	if err := prepareRestore(context.Background(), &request.Config, nil); err != nil {
		t.Fatalf("prepare restore before requested crash boundary: %v", err)
	}
	t.Fatal("requested prepare restore boundary was not reached")
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
