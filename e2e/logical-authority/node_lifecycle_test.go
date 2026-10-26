//go:build localhooks

package logicalauthority_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
)

func TestFlociGCSLogicalAuthorityThreeNodeBeforeAckAndSamePodRestart(t *testing.T) {
	requireFlociEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Metadata-Flavor") != "Google" {
			t.Errorf("unexpected Floci metadata request: %s %s flavor=%q", r.Method, r.URL.Path, r.Header.Get("Metadata-Flavor"))
			http.Error(w, "unexpected metadata request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/computeMetadata/v1/project/project-id":
			_, _ = io.WriteString(w, "floci-local")
		case "/computeMetadata/v1/instance/service-accounts/default/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"floci-reader-token","token_type":"Bearer","expires_in":3600}`)
		case "/computeMetadata/v1/universe/universe-domain":
			// The pinned SDK falls back to googleapis.com when this optional
			// metadata value is absent.
			http.NotFound(w, r)
		default:
			t.Errorf("unexpected Floci metadata endpoint: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(metadata.Close)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(metadata.URL, "http://"))

	clusterID := fmt.Sprintf("logical-%d", time.Now().UnixNano())
	var randomSuffix [8]byte
	if _, err := rand.Read(randomSuffix[:]); err != nil {
		t.Fatal("generate task-owned resource suffix")
	}
	suffix := fmt.Sprintf("%x", randomSuffix[:])
	bucketName := "rhiza-la-" + suffix
	credentials := localFlociCredentials(t)
	setupClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("create Floci GCS setup client: %v", err)
	}
	if err := setupClient.Bucket(bucketName).Create(ctx, "floci-local", nil); err != nil {
		_ = setupClient.Close()
		t.Fatalf("create task-owned bucket: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		objects := setupClient.Bucket(bucketName).Objects(cleanupCtx, nil)
		for {
			attrs, err := objects.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Errorf("list task-owned bucket objects during cleanup: %v", err)
				break
			}
			if err := setupClient.Bucket(bucketName).Object(attrs.Name).Generation(attrs.Generation).Delete(cleanupCtx); err != nil {
				t.Errorf("delete task-owned object %q: %v", attrs.Name, err)
			}
		}
		if err := setupClient.Bucket(bucketName).Delete(cleanupCtx); err != nil {
			t.Errorf("delete task-owned bucket: %v", err)
		}
		if err := setupClient.Close(); err != nil {
			t.Errorf("close Floci setup client: %v", err)
		}
	})

	authorityBucket, err := objstore.NewBucket(objstore.Config{Provider: objstore.ProviderGCS, Bucket: bucketName, Prefix: "authority", ServiceAccount: credentials})
	if err != nil {
		t.Fatalf("create canonical GCS authority bucket: %v", err)
	}
	t.Cleanup(func() { _ = authorityBucket.Close() })
	state, err := operator.NewAuthorityStateStore(authorityBucket, "rhiza-logical-authority")
	if err != nil {
		t.Fatalf("create GCS-backed authority state: %v", err)
	}
	sourceClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("create source mutation GCS client: %v", err)
	}
	t.Cleanup(func() { _ = sourceClient.Close() })

	// Three live voters form quorum in this five-member bootstrap. rhiza-3 is
	// intentionally left pristine until it adopts the verified init stage.
	ids := []string{"rhiza-0", "rhiza-1", "rhiza-2", "rhiza-3", "rhiza-4"}
	peerTokens := map[string]string{}
	members := make([]quepaxa.Member, 0, len(ids))
	peerAddrs := make(map[string]string, len(ids))
	for _, id := range ids {
		peerToken := "peer-" + id + "-" + suffix
		peerTokens[id] = peerToken
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve peer address for %s: %v", id, err)
		}
		peerAddrs[id] = packet.LocalAddr().String()
		if err := packet.Close(); err != nil {
			t.Fatalf("release reserved peer address for %s: %v", id, err)
		}
		var publicKey quepaxa.PublicKey
		copy(publicKey[:], network.PeerPublicKey(types.ClusterID(clusterID), quepaxa.NodeID(id), peerToken))
		members = append(members, quepaxa.Member{ID: quepaxa.NodeID(id), URL: "http://127.0.0.1:1", PeerURL: "quic://" + peerAddrs[id], PublicKey: publicKey})
	}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		t.Fatal("encode initial voter membership")
	}
	peerTokensJSON, err := json.Marshal(peerTokens)
	if err != nil {
		t.Fatal("encode initial voter tokens")
	}
	configMap := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "rhiza-config", "namespace": "default"}, "data": map[string]string{
		"RHIZA_CLUSTER_ID": clusterID,
	}}
	objectStoreConfigMap := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "rhiza-object-store", "namespace": "default"}, "data": map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": bucketName, "RHIZA_OBJSTORE_PREFIX": "archive/" + clusterID,
	}}
	secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "rhiza-peer-credentials", "namespace": "default"}, "data": map[string]string{
		"members": base64.StdEncoding.EncodeToString(membersJSON), "RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(peerTokensJSON),
	}}

	const statefulSetUID, statefulSetName = "logical-authority-sts-uid", "rhiza"
	rootUID := "logical-authority-root-" + suffix
	ledgerKey, err := json.Marshal([]string{statefulSetUID, clusterID})
	if err != nil {
		t.Fatal("encode legacy-ledger lookup identity")
	}
	ledgerDigest := sha256.Sum256(ledgerKey)
	legacyLedgerPath := "/api/v1/namespaces/default/configmaps/rhiza-fence-ledger-" + hex.EncodeToString(ledgerDigest[:])[:40]
	statefulSet, template, err := loadLogicalAuthorityStatefulSet(t, rootUID)
	if err != nil {
		t.Fatal(err)
	}
	podUIDs := map[string]string{}
	tokens := map[string]string{}
	pods := map[string]map[string]any{}
	for _, id := range ids {
		podUIDs[id] = "uid-" + id + "-" + suffix
		tokens[id] = "pod-token-" + id + "-" + suffix
		podSpec, err := cloneJSONMap(template["spec"])
		if err != nil {
			t.Fatalf("clone rendered logical-authority Pod spec: %v", err)
		}
		pods[id] = map[string]any{"metadata": map[string]any{"name": id, "uid": podUIDs[id],
			"namespace": "default", "annotations": map[string]string{
				"rhiza.mrchypark.dev/cooperative-fencing": "1", "rhiza.mrchypark.dev/fencing-root-uid": rootUID,
			}, "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": statefulSetName, "uid": statefulSetUID, "controller": true}}}, "spec": podSpec}
	}
	statefulSet["metadata"].(map[string]any)["name"] = statefulSetName
	statefulSet["metadata"].(map[string]any)["namespace"] = "default"
	statefulSet["metadata"].(map[string]any)["uid"] = statefulSetUID
	statefulSet["spec"].(map[string]any)["replicas"] = len(ids)
	templateMetadata := template["metadata"].(map[string]any)
	templateMetadata["annotations"] = map[string]string{
		"rhiza.mrchypark.dev/cooperative-fencing": "1", "rhiza.mrchypark.dev/fencing-root-uid": rootUID,
	}

	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer operator-test-token" {
			t.Errorf("fake Kubernetes API received unauthenticated request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "wrong operator token", http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews":
			var request struct {
				Spec struct {
					Token string `json:"token"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "invalid token review", http.StatusBadRequest)
				return
			}
			podName := ""
			for id, token := range tokens {
				if token == request.Spec.Token {
					podName = id
					break
				}
			}
			if podName == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"authenticated": false}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"authenticated": true, "audiences": []string{"rhiza-fencer"}, "user": map[string]any{
				"username": "system:serviceaccount:default:rhiza-voter-reader", "extra": map[string]any{"authentication.kubernetes.io/pod-uid": []string{podUIDs[podName]}, "authentication.kubernetes.io/pod-name": []string{podName}}}}})
		case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/pods/"):
			name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/default/pods/")
			if pod, ok := pods[name]; ok {
				_ = json.NewEncoder(w).Encode(pod)
				return
			}
			http.NotFound(w, r)
		case r.URL.Path == "/api/v1/namespaces/default/configmaps/rhiza-config":
			_ = json.NewEncoder(w).Encode(configMap)
		case r.URL.Path == "/api/v1/namespaces/default/configmaps/rhiza-object-store":
			_ = json.NewEncoder(w).Encode(objectStoreConfigMap)
		case r.URL.Path == "/api/v1/namespaces/default/secrets/rhiza-peer-credentials":
			_ = json.NewEncoder(w).Encode(secret)
		case r.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(statefulSet)
		case r.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{statefulSet}})
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
				"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaCluster",
				"metadata": map[string]any{"name": statefulSetName, "namespace": "default", "uid": rootUID},
				"spec":     map[string]any{"statefulSet": statefulSetName, "container": "rhiza", "adoptClusterID": clusterID, "automatic": false},
			}}})
		case r.Method == http.MethodGet && r.URL.Path == legacyLedgerPath:
			http.NotFound(w, r)
		default:
			t.Errorf("unexpected fake Kubernetes API request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	kubeTokenPath := filepath.Join(t.TempDir(), "operator-token")
	if err := os.WriteFile(kubeTokenPath, []byte("operator-test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	executor := &operator.FencingExecutor{Kube: &operator.Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: kubeTokenPath, Client: api.Client()},
		AuthorityState: state, ArchiveBucket: authorityBucket, SourceMutationStorage: sourceClient,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	stageID := ids[3]
	droppedRegistrationReply := &atomic.Bool{}
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/supervisor" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read request", http.StatusBadRequest)
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var message fencing.AuthorityMessage
			if json.Unmarshal(body, &message) == nil && message.Action == "register" && message.Identity.PodUID == podUIDs[stageID] && !droppedRegistrationReply.Load() {
				recorded := httptest.NewRecorder()
				executor.Handler().ServeHTTP(recorded, r)
				var grant fencing.StartupGrant
				committedGrant := recorded.Code == http.StatusOK && json.Unmarshal(recorded.Body.Bytes(), &grant) == nil &&
					grant.AuthorityProfile == fencing.LogicalWriterProfileV1 && grant.Role == fencing.StartupRoleVoterActive && grant.Validate(message.Identity) == nil
				if committedGrant && droppedRegistrationReply.CompareAndSwap(false, true) {
					// The handler durably committed an exact active grant; deliberately
					// lose only this successful response to exercise same-Pod retry.
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					return
				}
				for name, values := range recorded.Header() {
					for _, value := range values {
						w.Header().Add(name, value)
					}
				}
				w.WriteHeader(recorded.Code)
				_, _ = w.Write(recorded.Body.Bytes())
				return
			}
		}
		executor.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(authority.Close)
	caPath := filepath.Join(t.TempDir(), "authority-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: authority.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}

	nodes := make(map[string]*node.Node, len(ids))
	configs := make(map[string]*types.ExecutionConfig, len(ids))
	closed := make(map[string]bool, len(ids))
	for _, id := range ids {
		tokenPath := filepath.Join(t.TempDir(), "pod-token")
		if err := os.WriteFile(tokenPath, []byte(tokens[id]), 0600); err != nil {
			t.Fatal(err)
		}
		dataDir := filepath.Join(t.TempDir(), "data")
		cfg := &types.ExecutionConfig{DataDir: dataDir, ClusterID: types.ClusterID(clusterID), NodeID: types.NodeID(id),
			BindAddr: "127.0.0.1:0", PeerAddr: peerAddrs[id], PeerToken: peerTokens[id], AdminToken: "admin-" + suffix,
			Members: members, EnableReconfiguration: true, AuthorityProfile: fencing.LogicalWriterProfileV1,
			AuthorityURL: authority.URL + "/v1/supervisor", AuthorityCAFile: caPath, AuthorityTokenFile: tokenPath,
			ObjStoreProvider: "gcs", ObjStoreBucket: bucketName, ObjStorePrefix: "archive/" + clusterID,
			ObjStoreDurability:   types.ObjectStoreDurabilityBeforeAck,
			ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Second}
		configs[id] = cfg
		if id == ids[3] || id == ids[4] {
			continue
		}
		t.Setenv("POD_UID", podUIDs[id])
		t.Setenv("RHIZA_POD_NAME", id)
		instance := node.New(cfg)
		if err := instance.Open(ctx); err != nil {
			t.Fatalf("Node.Open(%s): %v", id, err)
		}
		nodes[id] = instance
		t.Cleanup(func() {
			if !closed[id] {
				if err := instance.Shutdown(); err != nil {
					t.Errorf("shutdown %s: %v", id, err)
				}
				closed[id] = true
			}
		})
	}
	for _, id := range ids[:3] {
		for !nodes[id].Ready() {
			select {
			case <-ctx.Done():
				t.Fatalf("node %s did not become ready: %v", id, ctx.Err())
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	apiA, err := nodes[ids[0]].API()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apiA.Execute(ctx, network.ExecuteRequest{RequestID: "floci-create-" + suffix, SQL: "CREATE TABLE lifecycle (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("before-ack create table: %v", err)
	}
	if _, err := apiA.Execute(ctx, network.ExecuteRequest{RequestID: "floci-insert-" + suffix, SQL: "INSERT INTO lifecycle(value) VALUES ('durable')"}); err != nil {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("before-ack insert: %v", err)
	}
	if _, err := apiA.Execute(ctx, network.ExecuteRequest{RequestID: "floci-create-compaction-" + suffix, SQL: "CREATE TABLE mutation_history (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("create bounded mutation-history table: %v", err)
	}
	for i := range 70 {
		requestID := fmt.Sprintf("floci-compaction-%s-%02d", suffix, i)
		sql := fmt.Sprintf("INSERT INTO mutation_history(id, value) VALUES (%d, 'entry-%02d')", i+1, i)
		if _, err := apiA.Execute(ctx, network.ExecuteRequest{RequestID: requestID, SQL: sql}); err != nil {
			t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
			t.Fatalf("before-ack mutation %d/70: %v", i+1, err)
		}
	}
	apiB, err := nodes[ids[1]].API()
	if err != nil {
		t.Fatal(err)
	}
	query, err := apiB.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM lifecycle WHERE id = 1", Consistency: "linearizable"})
	if err != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] != "durable" {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("cross-node linearizable read: rows=%v err=%v", query.Rows, err)
	}

	beforeRestartRunID := readLogicalRunID(t, configs[ids[1]].DataDir)
	if err := nodes[ids[1]].Shutdown(); err != nil {
		t.Fatalf("stop same-Pod node: %v", err)
	}
	closed[ids[1]] = true
	t.Setenv("POD_UID", podUIDs[ids[1]])
	t.Setenv("RHIZA_POD_NAME", ids[1])
	restarted := node.New(configs[ids[1]])
	if err := restarted.Open(ctx); err != nil {
		t.Fatalf("same-Pod exact-WAL Node.Open: %v", err)
	}
	if got := readLogicalRunID(t, configs[ids[1]].DataDir); got != beforeRestartRunID {
		t.Fatalf("same-Pod continuation changed canonical RunID: before=%q after=%q", beforeRestartRunID, got)
	}
	nodes[ids[1]] = restarted
	closed[ids[1]] = false
	t.Cleanup(func() {
		if !closed[ids[1]] {
			if err := restarted.Shutdown(); err != nil {
				t.Errorf("shutdown restarted %s: %v", ids[1], err)
			}
			closed[ids[1]] = true
		}
	})
	for !restarted.Ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("same-Pod reopened node did not become ready: %v", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	restartedAPI, err := restarted.API()
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, err := restartedAPI.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM lifecycle WHERE id = 1", Consistency: "linearizable"})
	if err != nil || len(afterRestart.Rows) != 1 || len(afterRestart.Rows[0]) != 1 || afterRestart.Rows[0][0] != "durable" {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("same-Pod continuation lost durable row: rows=%v err=%v", afterRestart.Rows, err)
	}
	history, err := restartedAPI.Query(ctx, network.QueryRequest{SQL: "SELECT COUNT(*) FROM mutation_history", Consistency: "linearizable"})
	if err != nil || len(history.Rows) != 1 || len(history.Rows[0]) != 1 || history.Rows[0][0] != int64(70) && history.Rows[0][0] != float64(70) {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("same-Pod continuation lost mutations across the compaction threshold: rows=%v err=%v", history.Rows, err)
	}
	if _, err := restartedAPI.Execute(ctx, network.ExecuteRequest{RequestID: "floci-restart-insert-" + suffix,
		SQL: "INSERT INTO lifecycle(value) VALUES ('after-restart')"}); err != nil {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("same-Pod continuation could not reuse the canonical source producer: %v", err)
	}
	continued, err := restartedAPI.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM lifecycle ORDER BY id", Consistency: "linearizable"})
	if err != nil || len(continued.Rows) != 2 || continued.Rows[1][0] != "after-restart" {
		t.Logf("logical_failure_snapshot=%s", collectLogicalAuthorityFailureSnapshot(nodes, state, executor, rootUID, clusterID))
		t.Fatalf("same-Pod continuation readback: rows=%v err=%v", continued.Rows, err)
	}

	stageConfig := configs[stageID]
	stageConfig.HAInitRestoreDir = filepath.Join(t.TempDir(), "ha-init-stage")
	archiveBucket, err := objstore.NewBucket(objstore.Config{Provider: objstore.ProviderGCS, Bucket: bucketName})
	if err != nil {
		t.Fatalf("open isolated Floci bucket for init staging: %v", err)
	}
	t.Cleanup(func() { _ = archiveBucket.Close() })
	var stageErr error
	for {
		stageErr = node.PrepareHAInitRestore(ctx, stageConfig, archiveBucket)
		if stageErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("prepare stage from the certified Floci checkpoint: last error: %v; context: %v", stageErr, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	manager := checkpoint.NewManager(archiveBucket, path.Join(stageConfig.ObjStorePrefix, string(stageConfig.ClusterID)), "", recovery.ArchiveConfigID)
	if err := manager.Load(ctx); err != nil {
		t.Fatalf("load Floci checkpoint root after staging: %v", err)
	}
	root := manager.Latest()
	if root == nil {
		t.Fatal("stage preparation succeeded without a certified checkpoint root")
	}
	blocks := captureFlociCheckpointBlocks(t, ctx, setupClient, bucketName,
		path.Join(stageConfig.ObjStorePrefix, string(stageConfig.ClusterID)), root)
	if err := removeFlociCheckpointBlocks(ctx, setupClient, bucketName, blocks); err != nil {
		t.Fatalf("remove remote checkpoint payloads to force staged-file adoption: %v", err)
	}
	t.Setenv("POD_UID", podUIDs[stageID])
	t.Setenv("RHIZA_POD_NAME", stageID)
	failedAttempt := node.New(stageConfig)
	if err := failedAttempt.Open(ctx); err == nil {
		_ = failedAttempt.Shutdown()
		t.Fatal("first registration unexpectedly received the grant response")
	}
	if !droppedRegistrationReply.Load() {
		t.Fatal("registration response-drop fault did not reach the intended staged Pod")
	}
	preWALRunID := readLogicalRunID(t, stageConfig.DataDir)
	if _, err := os.Lstat(filepath.Join(stageConfig.DataDir, "qlog", "voter-identity.json")); !os.IsNotExist(err) {
		t.Fatalf("failed pre-WAL registration unexpectedly created voter identity: %v", err)
	}
	stagedNode := node.New(stageConfig)
	if err := stagedNode.Open(ctx); err != nil {
		_ = restoreFlociCheckpointBlocks(ctx, setupClient, bucketName, blocks)
		t.Fatalf("first-use logical Node.Open from staged checkpoint bytes: %v", err)
	}
	if got := readLogicalRunID(t, stageConfig.DataDir); got != preWALRunID {
		_ = stagedNode.Shutdown()
		t.Fatalf("same-Pod retry changed pre-WAL RunID: before=%q after=%q", preWALRunID, got)
	}
	if err := restoreFlociCheckpointBlocks(ctx, setupClient, bucketName, blocks); err != nil {
		_ = stagedNode.Shutdown()
		t.Fatalf("restore checkpoint payloads after adoption oracle: %v", err)
	}
	authorityRoot, _, err := state.ReadRoot(ctx, statefulSetUID, clusterID)
	if err != nil {
		_ = stagedNode.Shutdown()
		t.Fatalf("read canonical logical authority root after pre-WAL retry: %v", err)
	}
	stageRuns := 0
	for _, run := range authorityRoot.Ledger.Runs {
		if run.Identity.PodUID == podUIDs[stageID] && run.Identity.NodeID == stageID && run.Identity.RunID == preWALRunID {
			stageRuns++
			if run.MutationProducerUID == "" {
				_ = stagedNode.Shutdown()
				t.Fatal("successful retry did not retain its canonical logical producer")
			}
		}
	}
	if stageRuns != 1 {
		_ = stagedNode.Shutdown()
		t.Fatalf("same-Pod retry registered %d matching canonical Runs, want exactly one", stageRuns)
	}
	for !stagedNode.Ready() {
		select {
		case <-ctx.Done():
			_ = stagedNode.Shutdown()
			t.Fatalf("fresh logical startup did not become Ready from staged files: %v", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	stagedAPI, err := stagedNode.API()
	if err != nil {
		_ = stagedNode.Shutdown()
		t.Fatal(err)
	}
	stagedRead, err := stagedAPI.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM lifecycle WHERE id = 1", Consistency: "linearizable"})
	if err != nil || len(stagedRead.Rows) != 1 || stagedRead.Rows[0][0] != "durable" {
		_ = stagedNode.Shutdown()
		t.Fatalf("staged logical startup lost checkpointed data: rows=%v err=%v", stagedRead.Rows, err)
	}
	closed[stageID] = false
	nodes[stageID] = stagedNode
	t.Cleanup(func() {
		if !closed[stageID] {
			if err := stagedNode.Shutdown(); err != nil {
				t.Errorf("shutdown staged startup %s: %v", stageID, err)
			}
			closed[stageID] = true
		}
	})
}

func readLogicalRunID(t *testing.T, dataDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "logical-run.json"))
	if err != nil {
		t.Fatalf("read persisted logical Run state: %v", err)
	}
	var state struct {
		RunID string `json:"runID"`
	}
	if err := json.Unmarshal(data, &state); err != nil || state.RunID == "" {
		t.Fatalf("decode persisted logical Run state: runID=%q err=%v", state.RunID, err)
	}
	return state.RunID
}

func loadLogicalAuthorityStatefulSet(t *testing.T, rootUID string) (map[string]any, map[string]any, error) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "operator", "logical-authority", "statefulset.yaml"))
	if err != nil {
		return nil, nil, fmt.Errorf("read logical-authority StatefulSet manifest: %w", err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	var statefulSet map[string]any
	for {
		var document any
		if err := decoder.Decode(&document); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, fmt.Errorf("decode logical-authority manifest: %w", err)
		}
		resource, ok := document.(map[string]any)
		if !ok || resource["kind"] != "StatefulSet" {
			continue
		}
		statefulSet = resource
	}
	if statefulSet == nil {
		return nil, nil, fmt.Errorf("logical-authority manifest has no StatefulSet")
	}
	template, ok := statefulSet["spec"].(map[string]any)["template"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("logical-authority StatefulSet has no Pod template")
	}
	spec, ok := template["spec"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("logical-authority StatefulSet has no Pod spec")
	}
	for _, group := range []string{"containers", "initContainers"} {
		entries, ok := spec[group].([]any)
		if !ok || len(entries) != 1 {
			return nil, nil, fmt.Errorf("logical-authority manifest %s inventory is not the expected single-container profile", group)
		}
		container, ok := entries[0].(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("logical-authority manifest %s entry is malformed", group)
		}
		container["image"] = "ghcr.io/example/rhiza@sha256:" + strings.Repeat("a", 64)
	}
	templateMetadata, ok := template["metadata"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("logical-authority manifest template metadata is malformed")
	}
	templateMetadata["annotations"] = map[string]any{
		"rhiza.mrchypark.dev/cooperative-fencing": "1",
		"rhiza.mrchypark.dev/fencing-root-uid":    rootUID,
	}
	return statefulSet, template, nil
}

func cloneJSONMap(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var cloned map[string]any
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

type flociCheckpointBlock struct {
	key        string
	generation int64
	data       []byte
}

func captureFlociCheckpointBlocks(t *testing.T, ctx context.Context, client *storage.Client, bucketName, prefix string, root *checkpoint.Checkpoint) []flociCheckpointBlock {
	t.Helper()
	seen := make(map[string]bool)
	var blocks []flociCheckpointBlock
	for _, file := range root.Files {
		for _, block := range file.Blocks {
			name := block.Hash + ".block"
			if block.Generation != 0 {
				name = fmt.Sprintf("%s_%020d.block", block.Hash, block.Generation)
			}
			key := path.Join(prefix, "checkpoint", "blocks", name)
			if seen[key] {
				continue
			}
			seen[key] = true
			object := client.Bucket(bucketName).Object(key)
			attrs, err := object.Attrs(ctx)
			if err != nil {
				t.Fatalf("read checkpoint block attributes %q: %v", key, err)
			}
			reader, err := object.Generation(attrs.Generation).NewReader(ctx)
			if err != nil {
				t.Fatalf("read checkpoint block %q: %v", key, err)
			}
			data, readErr := io.ReadAll(reader)
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read checkpoint block %q: %v", key, errors.Join(readErr, closeErr))
			}
			blocks = append(blocks, flociCheckpointBlock{key: key, generation: attrs.Generation, data: data})
		}
	}
	if len(blocks) == 0 {
		t.Fatal("certified staged checkpoint has no payload blocks")
	}
	return blocks
}

func removeFlociCheckpointBlocks(ctx context.Context, client *storage.Client, bucketName string, blocks []flociCheckpointBlock) error {
	for _, block := range blocks {
		if err := client.Bucket(bucketName).Object(block.key).Generation(block.generation).Delete(ctx); err != nil {
			return fmt.Errorf("delete checkpoint block %q: %w", block.key, err)
		}
	}
	return nil
}

func restoreFlociCheckpointBlocks(ctx context.Context, client *storage.Client, bucketName string, blocks []flociCheckpointBlock) error {
	for _, block := range blocks {
		writer := client.Bucket(bucketName).Object(block.key).NewWriter(ctx)
		if _, err := writer.Write(block.data); err != nil {
			_ = writer.Close()
			return fmt.Errorf("restore checkpoint block %q: %w", block.key, err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("restore checkpoint block %q: %w", block.key, err)
		}
	}
	return nil
}

func requireFlociEnvironment(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "true" || os.Getenv("RHIZA_LOGICAL_AUTHORITY_FLOCI") != "1" ||
		os.Getenv("STORAGE_EMULATOR_HOST") != flociGCSEndpoint || os.Getenv("GOOGLE_CLOUD_PROJECT") != "floci-local" ||
		os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" || os.Getenv("RHIZA_LOCALHOOKS_ALLOW_FLOCI_GCS") != "1" {
		t.Fatal("requires explicit pinned Floci-GCS localhooks CI environment; refusing SDK construction")
	}
}
