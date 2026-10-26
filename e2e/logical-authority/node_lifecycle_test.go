package logicalauthority_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestFlociGCSLogicalAuthorityThreeNodeBeforeAckAndSamePodRestart(t *testing.T) {
	requireFlociEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

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

	ids := []string{"voter-a", "voter-b", "voter-c"}
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
	configMap := map[string]any{"data": map[string]string{
		"RHIZA_CLUSTER_ID": clusterID, "RHIZA_CLUSTER_MEMBERS": string(membersJSON),
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": bucketName, "RHIZA_OBJSTORE_PREFIX": "archive/" + clusterID,
	}}
	secret := map[string]any{"data": map[string]string{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(peerTokensJSON)}}

	const statefulSetUID, statefulSetName = "logical-authority-sts-uid", "rhiza"
	podUIDs := map[string]string{}
	tokens := map[string]string{}
	pods := map[string]map[string]any{}
	for _, id := range ids {
		podUIDs[id] = "uid-" + id + "-" + suffix
		tokens[id] = "pod-token-" + id + "-" + suffix
		container := map[string]any{"name": "rhiza", "image": "test", "securityContext": map[string]any{"allowPrivilegeEscalation": false},
			"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "rhiza-config"}}, map[string]any{"secretRef": map[string]any{"name": "rhiza-peer-tokens"}}}}
		podSpec := map[string]any{"serviceAccountName": "default", "containers": []any{container}}
		pods[id] = map[string]any{"metadata": map[string]any{"name": id, "uid": podUIDs[id],
			"annotations":     map[string]string{"rhiza.mrchypark.dev/fencing-protocol": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": statefulSetName, "uid": statefulSetUID, "controller": true}}}, "spec": podSpec}
	}
	template := map[string]any{"metadata": map[string]any{"annotations": map[string]string{"rhiza.mrchypark.dev/fencing-protocol": "1"}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "rhiza", "image": "test", "securityContext": map[string]any{"allowPrivilegeEscalation": false},
			"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "rhiza-config"}}, map[string]any{"secretRef": map[string]any{"name": "rhiza-peer-tokens"}}}}}}}
	statefulSet := map[string]any{"metadata": map[string]string{"name": statefulSetName, "uid": statefulSetUID}, "spec": map[string]any{"template": template}}

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
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
				"username": "system:serviceaccount:default:default", "extra": map[string]any{"authentication.kubernetes.io/pod-uid": []string{podUIDs[podName]}, "authentication.kubernetes.io/pod-name": []string{podName}}}}})
		case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/pods/"):
			name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/default/pods/")
			if pod, ok := pods[name]; ok {
				_ = json.NewEncoder(w).Encode(pod)
				return
			}
			http.NotFound(w, r)
		case r.URL.Path == "/api/v1/namespaces/default/configmaps/rhiza-config":
			_ = json.NewEncoder(w).Encode(configMap)
		case r.URL.Path == "/api/v1/namespaces/default/secrets/rhiza-peer-tokens":
			_ = json.NewEncoder(w).Encode(secret)
		case r.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(statefulSet)
		default:
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
	authority := httptest.NewTLSServer(executor.Handler())
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
			ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, ObjStoreServiceAccount: credentials,
			ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
		configs[id] = cfg
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
	for _, id := range ids {
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
		t.Fatalf("before-ack create table: %v", err)
	}
	if _, err := apiA.Execute(ctx, network.ExecuteRequest{RequestID: "floci-insert-" + suffix, SQL: "INSERT INTO lifecycle(value) VALUES ('durable')"}); err != nil {
		t.Fatalf("before-ack insert: %v", err)
	}
	apiB, err := nodes[ids[1]].API()
	if err != nil {
		t.Fatal(err)
	}
	query, err := apiB.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM lifecycle WHERE id = 1", Consistency: "linearizable"})
	if err != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] != "durable" {
		t.Fatalf("cross-node linearizable read: rows=%v err=%v", query.Rows, err)
	}

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
		t.Fatalf("same-Pod continuation lost durable row: rows=%v err=%v", afterRestart.Rows, err)
	}
}

func requireFlociEnvironment(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "true" || os.Getenv("RHIZA_LOGICAL_AUTHORITY_FLOCI") != "1" ||
		os.Getenv("STORAGE_EMULATOR_HOST") != flociGCSEndpoint || os.Getenv("GOOGLE_CLOUD_PROJECT") != "floci-local" ||
		os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" || os.Getenv("RHIZA_LOCALHOOKS_ALLOW_FLOCI_GCS") != "1" {
		t.Fatal("requires explicit pinned Floci-GCS localhooks CI environment; refusing SDK construction")
	}
}
