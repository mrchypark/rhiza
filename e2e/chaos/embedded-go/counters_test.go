package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"
)

func TestMain(m *testing.M) {
	for key, value := range map[string]string{
		"RHIZA_GCP_PROJECT_ID":       "example-project",
		"RHIZA_GCP_PROJECT_NUMBER":   "123456789012",
		"RHIZA_GKE_CLUSTER":          "test-cluster",
		"RHIZA_GKE_LOCATION":         "test-region1",
		"RHIZA_KUBE_CONTEXT":         "gke_example-project_test-region1_test-cluster",
		"RHIZA_GCS_BUCKET":           "rhiza-qualification-fixture",
		"RHIZA_GCS_PREFIX":           "qualification/a1b2c3d4/",
		"RHIZA_NAMESPACE":            "rhiza-v0191-20261008-a1b2c3d4",
		"RHIZA_DATA_GSA_ID":          "rhiza-v0191-gcs-a1b2c3d4",
		"RHIZA_NODE_A":               "gke-test-cluster-fixture-a",
		"RHIZA_NODE_B":               "gke-test-cluster-fixture-b",
		"RHIZA_NODE_C":               "gke-test-cluster-fixture-c",
		"RHIZA_METADATA_IMAGE":       "registry.example/cloud-cli@sha256:" + strings.Repeat("b", 64),
		"RHIZA_GITHUB_REPOSITORY":    "example/example",
		"RHIZA_GITHUB_REPOSITORY_ID": "12345",
		"RHIZA_GITHUB_OWNER_ID":      "67890",
		"RHIZA_GITHUB_WORKFLOW_REF":  "example/example/.github/workflows/qualification.yml@refs/heads/main",
		"RHIZA_GITHUB_ENVIRONMENT":   "qualification",
		"RHIZA_CI_GSA_ID":            "rhiza-v0191-ci-a1b2c3d4",
	} {
		if err := os.Setenv(key, value); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

func qualificationAssetEnv(run string) []string {
	return []string{
		"RHIZA_GCP_PROJECT_ID=example-project",
		"RHIZA_GCP_PROJECT_NUMBER=123456789012",
		"RHIZA_GKE_CLUSTER=test-cluster",
		"RHIZA_GKE_LOCATION=test-region1",
		"RHIZA_KUBE_CONTEXT=gke_example-project_test-region1_test-cluster",
		"RHIZA_GCS_BUCKET=rhiza-qualification-fixture",
		"RHIZA_GCS_PREFIX=qualification/" + run + "/",
		"RHIZA_DATA_GSA_ID=rhiza-v0191-gcs-" + run,
	}
}

func TestQualificationCloudInputsFailBeforeMutation(t *testing.T) {
	run := "a1b2c3d4"
	host := "registry.example/rhiza@sha256:" + strings.Repeat("a", 64)
	base := []string{"PATH=" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run,
		"RHIZA_NAMESPACE=rhiza-qualification-" + run,
		"RHIZA_HOST_IMAGE=" + host, "RHIZA_METADATA_IMAGE=registry.example/cloud-cli@sha256:" + strings.Repeat("b", 64),
		"RHIZA_NODE_A=node-a", "RHIZA_NODE_B=node-b", "RHIZA_NODE_C=node-c",
		"RHIZA_AUTH_CREATION_SHA=" + strings.Repeat("c", 40)}

	authState := filepath.Join(t.TempDir(), "auth-state")
	bootstrap := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "apply-local")
	bootstrap.Env = append(base,
		"RHIZA_HARNESS_SHA="+strings.Repeat("c", 40),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
		"RHIZA_AUTH_STATE="+authState,
		"RHIZA_BOOTSTRAP_GO=I_APPROVE_NEW_SCOPE", "RHIZA_COST_CAP_KRW=10000", "RHIZA_EXPECTED_ADMIN_ACCOUNT=admin@example.invalid")
	if output, err := bootstrap.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("RHIZA_GCP_PROJECT_ID")) {
		t.Fatalf("bootstrap did not fail on the first missing cloud input: err=%v output=%s", err, output)
	}
	if _, err := os.Stat(authState); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("bootstrap mutated state before validating cloud inputs")
	}

	outputDir := filepath.Join(t.TempDir(), "runtime-output")
	runtime := exec.Command("/bin/sh", "../run-gcs-postrelease.sh", "render")
	runtime.Env = append(base, "RHIZA_OUTPUT="+outputDir)
	if output, err := runtime.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("RHIZA_GCP_PROJECT_ID")) {
		t.Fatalf("runtime did not fail on the first missing cloud input: err=%v output=%s", err, output)
	}
	if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("runtime created output before validating cloud inputs")
	}
}

func TestQualificationHarnessUsesExplicitAssetContract(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, variable := range []string{
		"RHIZA_GCP_PROJECT_ID", "RHIZA_GCP_PROJECT_NUMBER", "RHIZA_GKE_CLUSTER", "RHIZA_GKE_LOCATION",
		"RHIZA_KUBE_CONTEXT", "RHIZA_GCS_BUCKET", "RHIZA_GCS_PREFIX", "RHIZA_NAMESPACE",
		"RHIZA_DATA_GSA_ID", "RHIZA_NODE_A", "RHIZA_NODE_B", "RHIZA_NODE_C", "RHIZA_METADATA_IMAGE",
	} {
		required := []byte("${" + variable + ":?")
		if !bytes.Contains(bootstrap, required) || !bytes.Contains(runtime, required) {
			t.Fatalf("cloud asset %s is not an explicit required input in both harness stages", variable)
		}
	}
	workloadTemplate, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	for _, placeholder := range []string{"__GCS_BUCKET__", "__GCS_PREFIX__", "__NODE_A__", "__NODE_B__", "__NODE_C__", "__HOST_IMAGE__", "__METADATA_IMAGE__"} {
		if !bytes.Contains(workloadTemplate, []byte(placeholder)) {
			t.Fatalf("workload template lacks explicit placeholder %s", placeholder)
		}
	}
}

func TestQualificationLocalRenderDoesNotRequireFederationInputs(t *testing.T) {
	run := "a1b2c3d4"
	cmd := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "render-local")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "RHIZA_GITHUB_") && !strings.HasPrefix(entry, "RHIZA_CI_GSA_ID=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, qualificationAssetEnv(run)...)
	cmd.Env = append(cmd.Env,
		"RHIZA_RUN_ID="+run,
		"RHIZA_NAMESPACE=rhiza-qualification-"+run,
		"RHIZA_HARNESS_SHA="+strings.Repeat("a", 40),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
		"RHIZA_HOST_IMAGE=registry.example/rhiza@sha256:"+strings.Repeat("a", 64),
		"RHIZA_METADATA_IMAGE=registry.example/cloud-cli@sha256:"+strings.Repeat("b", 64),
		"RHIZA_NODE_A=node-a", "RHIZA_NODE_B=node-b", "RHIZA_NODE_C=node-c",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("local direct render required federation inputs: %v: %s", err, output)
	}
}

func TestQualificationFederationWorkflowMustBelongToRepository(t *testing.T) {
	run := "a1b2c3d4"
	cmd := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "render")
	cmd.Env = append(os.Environ(), qualificationAssetEnv(run)...)
	cmd.Env = append(cmd.Env,
		"RHIZA_RUN_ID="+run,
		"RHIZA_NAMESPACE=rhiza-qualification-"+run,
		"RHIZA_WORKFLOW_SHA="+strings.Repeat("a", 40),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
		"RHIZA_HOST_IMAGE=registry.example/rhiza@sha256:"+strings.Repeat("a", 64),
		"RHIZA_METADATA_IMAGE=registry.example/cloud-cli@sha256:"+strings.Repeat("b", 64),
		"RHIZA_NODE_A=node-a", "RHIZA_NODE_B=node-b", "RHIZA_NODE_C=node-c",
		"RHIZA_GITHUB_REPOSITORY=example/project",
		"RHIZA_GITHUB_REPOSITORY_ID=12345", "RHIZA_GITHUB_OWNER_ID=67890",
		"RHIZA_GITHUB_WORKFLOW_REF=other/project/.github/workflows/qualification.yml@refs/heads/main",
		"RHIZA_GITHUB_ENVIRONMENT=qualification", "RHIZA_CI_GSA_ID=rhiza-ci-"+run,
	)
	output, err := cmd.CombinedOutput()
	if err == nil || !bytes.Contains(output, []byte("workflow ref must belong to the exact repository")) {
		t.Fatalf("cross-repository workflow ref was not rejected: err=%v output=%s", err, output)
	}
}

func TestQualificationIdentityChecksPrecedeMutation(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	applyStart := strings.Index(text, "if [ \"$mode\" = apply ]; then\n    record project-identity")
	if applyStart < 0 {
		t.Fatal("apply identity preflight block missing")
	}
	text = text[applyStart:]
	project := strings.Index(text, `record project-identity g projects describe "$project" --format=json`)
	cluster := strings.Index(text, `capture_cluster_identity || die`)
	kubeContext := strings.Index(text, `capture_kube_context_identity || die`)
	repository := strings.Index(text, `record github-repository gh api "repos/$RHIZA_GITHUB_REPOSITORY"`)
	workflow := strings.Index(text, `record github-workflow gh api`)
	firstMutation := strings.Index(text, `record data-create create_account "$data_id"`)
	firstKubernetesMutation := strings.Index(text, `record kubernetes-create k create -f`)
	if project < 0 || cluster < 0 || kubeContext < 0 || repository < 0 || workflow < 0 || firstMutation < 0 || firstKubernetesMutation < 0 ||
		project >= cluster || cluster >= kubeContext || kubeContext >= repository || repository >= workflow ||
		workflow >= firstMutation || kubeContext >= firstKubernetesMutation {
		t.Fatalf("identity preflight ordering changed: project=%d cluster=%d kube=%d repository=%d workflow=%d cloud-mutation=%d kube-mutation=%d", project, cluster, kubeContext, repository, workflow, firstMutation, firstKubernetesMutation)
	}
	for _, validation := range []string{
		`validate_project_identity "$RHIZA_AUTH_STATE/project-identity.json"`,
		`capture_cluster_identity || die`,
		`capture_kube_context_identity || die`,
		`validate_github_repository_identity "$RHIZA_AUTH_STATE/github-repository.json"`,
		`jq -e '.type=="file"`,
	} {
		position := strings.Index(text, validation)
		if position < 0 || position >= firstMutation {
			t.Fatalf("identity validation does not precede mutation: %s", validation)
		}
	}
	if strings.Contains(text, `record kube-context-identity k config view`) {
		t.Fatal("raw kubeconfig must not be captured through the receipt recorder")
	}
	if !strings.Contains(string(source), `k config view --minify --raw --flatten -o json 2>/dev/null | project_kube_context`) {
		t.Fatal("kubeconfig must be minified and projected before any file")
	}
	if strings.Contains(string(source), `record mint-cluster g container clusters describe`) ||
		strings.Contains(string(source), `record cluster-identity g container clusters describe`) {
		t.Fatal("full cluster metadata must not enter the receipt recorder")
	}
	if !strings.Contains(string(source), `describe_cluster_metadata() { exec gcloud --project="$project" --quiet "$@"; }`) ||
		strings.Count(string(source), `describe_cluster_metadata container clusters describe`) != 2 {
		t.Fatal("bootstrap cluster metadata producer must exec gcloud at both real call sites")
	}
}

func TestQualificationLocalAuthSanitizesKubeEvidence(t *testing.T) {
	run := "a1b2c3d4"
	cmd := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "check-local-auth")
	cmd.Env = append(os.Environ(), qualificationAssetEnv(run)...)
	cmd.Env = append(cmd.Env,
		"RHIZA_RUN_ID="+run,
		"RHIZA_NAMESPACE=rhiza-qualification-"+run,
		"RHIZA_HARNESS_SHA="+strings.Repeat("a", 40),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
		"RHIZA_HOST_IMAGE=registry.example/rhiza@sha256:"+strings.Repeat("a", 64),
		"RHIZA_METADATA_IMAGE=registry.example/cloud-cli@sha256:"+strings.Repeat("b", 64),
		"RHIZA_NODE_A=node-a", "RHIZA_NODE_B=node-b", "RHIZA_NODE_C=node-c",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sanitized local auth fixture failed: %v: %s", err, output)
	}
	if !bytes.Contains(output, []byte("identity mismatch")) || bytes.Contains(output, []byte("DO_NOT_RECORD")) {
		t.Fatalf("sanitized local auth fixture output changed: %s", output)
	}
}

func TestQualificationApplicationProvenance(t *testing.T) {
	workflow, err := os.ReadFile("../../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"APPLICATION_SHA:", "$APPLICATION_SHA", `test "$APPLICATION_SHA" != "$GITHUB_SHA"`} {
		if bytes.Contains(workflow, []byte(forbidden)) {
			t.Fatalf("qualification image provenance must not pin a split application source: %s", forbidden)
		}
	}
	if bytes.Contains(script, []byte("application_sha=")) {
		t.Fatal("qualification runtime must not pin a split application source")
	}
	for _, required := range []string{
		`git fetch --no-tags --depth=1 origin "$GITHUB_REF"`,
		`test "$(git rev-parse HEAD)" = "$GITHUB_SHA"`,
		`test "$(git rev-parse FETCH_HEAD)" = "$GITHUB_SHA"`,
		`test -z "$(git status --porcelain --untracked-files=all)"`,
		`--label "org.opencontainers.image.revision=$GITHUB_SHA"`,
		`--label "io.rhiza.qualification.harness=$GITHUB_SHA"`,
		`--label "io.rhiza.qualification.application=$GITHUB_SHA"`,
		`.Config.Labels["org.opencontainers.image.revision"] == $revision`,
		`.Config.Labels["io.rhiza.qualification.harness"] == $revision`,
		`.Config.Labels["io.rhiza.qualification.application"] == $revision`,
	} {
		if !bytes.Contains(workflow, []byte(required)) {
			t.Fatalf("qualification image must bind checkout and all provenance labels to GITHUB_SHA: %s", required)
		}
	}
	for _, required := range []string{
		`source_sha=$GITHUB_SHA`,
		`source_sha=$RHIZA_HARNESS_SHA`,
		`[ "${RHIZA_APPLICATION_SHA:-}" = "$source_sha" ] || die 'candidate application mismatch'`,
	} {
		if !bytes.Contains(script, []byte(required)) {
			t.Fatalf("qualification runtime must derive application provenance from verified source: %s", required)
		}
	}
}

func TestObjectStoreCountersAllowlist(t *testing.T) {
	handler := objectStoreCounters(func() (rhiza.ObjectStoreStats, bool) {
		return rhiza.ObjectStoreStats{HTTPRequests: 17, BytesUploaded: 29, BytesPublished: 23,
			S3HTTPRequests: 99, ObservedRequestRepeats: 88}, true
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/qualification/object-store", nil))
	var counters map[string]uint64
	if err := json.Unmarshal(response.Body.Bytes(), &counters); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(counters) != 16 || counters["http_requests"] != 17 || counters["bytes_uploaded"] != 29 || counters["bytes_published"] != 23 {
		t.Fatalf("unexpected allowlisted counters: status=%d counters=%v", response.Code, counters)
	}
	for _, excluded := range []string{"s3_http_requests", "observed_request_repeats", "config", "token", "service_account"} {
		if _, ok := counters[excluded]; ok {
			t.Fatalf("unexpected disclosure: %s", excluded)
		}
	}
}

func TestQualificationApplicationRole(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		ok          bool
	}{{"", "voter", true}, {"voter", "voter", true}, {"object-store", "object-store", true}, {"learner", "", false}} {
		got, err := qualificationRole(tc.input)
		if got != tc.want || (err == nil) != tc.ok {
			t.Fatalf("role %q = %q, %v; want %q, ok=%t", tc.input, got, err, tc.want, tc.ok)
		}
	}
}

func TestQualificationObjectStoreReplicaHandler(t *testing.T) {
	ctx := context.Background()
	store := t.TempDir()
	voter, err := rhiza.Open(ctx, rhiza.Config{ClusterID: "qualification-replica", NodeID: "voter",
		DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: store,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck, CheckpointInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE qualification (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "seed", SQL: "INSERT INTO qualification VALUES (1, 'sentinel')"}); err != nil {
		t.Fatal(err)
	}
	checkpointDeadline := time.Now().Add(3 * time.Second)
	for {
		_, currentErr := os.Stat(filepath.Join(store, "qualification-replica", "checkpoint", "CURRENT"))
		if currentErr == nil {
			break
		} else if currentErr != nil && !errors.Is(currentErr, os.ErrNotExist) {
			t.Fatal(currentErr)
		}
		if time.Now().After(checkpointDeadline) {
			t.Fatal("certified checkpoint was not published")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// CURRENT is published before the certified archive base and maintenance
	// claim are finalized. Give that same publication operation time to finish.
	time.Sleep(500 * time.Millisecond)
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
	config := objectStoreReplicaConfig(rhiza.Config{ClusterID: "qualification-replica", NodeID: "cold-reader",
		DataDir: t.TempDir(), Members: []rhiza.Member{{ID: "voter", PeerURL: "quic://forbidden", PublicKey: rhiza.PublicKey{1}}},
		ObjStoreProvider: "filesystem", ObjStoreDir: store, ObjStoreSyncInterval: time.Hour})
	if len(config.Members) != 1 || config.Members[0].ID != "voter" || config.Members[0].PeerURL != "" || config.Members[0].PublicKey != ([32]byte{}) {
		t.Fatalf("object-store replica retained peer credentials: %+v", config.Members)
	}
	replica, err := rhiza.OpenReadReplica(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	handler := objectStoreReplicaHandler(replica)
	request := func(method, target, body string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(response, req)
		return response
	}
	if response := request(http.MethodGet, "/ready", ""); response.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s replica=%+v", response.Code, response.Body.String(), replica.Status())
	}
	status := request(http.MethodGet, "/replica/status", "")
	var rawStatus map[string]any
	if err := json.Unmarshal(status.Body.Bytes(), &rawStatus); err != nil {
		t.Fatal(err)
	}
	if status.Code != http.StatusOK || rawStatus["mode"] != "object-store" || rawStatus["source"] != "object-store" || rawStatus["lag_slots"] != float64(0) || rawStatus["applied_slot"] != rawStatus["source_tip"] ||
		rawStatus["checkpoint_base_slot"] == float64(0) || rawStatus["checkpoint_root_hash"] == "" || rawStatus["archive_head_version"] == "" ||
		rawStatus["recovery_pins_created"] != float64(2) || rawStatus["recovery_pins_released"] != float64(2) {
		t.Fatalf("replica status=%d %s", status.Code, status.Body.String())
	}
	local := request(http.MethodPost, "/sql/query", `{"sql":"SELECT id,value FROM qualification","consistency":"local"}`)
	if local.Code != http.StatusOK || !strings.Contains(local.Body.String(), `"sentinel"`) {
		t.Fatalf("local read status=%d body=%s", local.Code, local.Body.String())
	}
	mutation := request(http.MethodPost, "/sql/execute", `{"request_id":"blocked","sql":"INSERT INTO qualification VALUES (2,'blocked')"}`)
	if mutation.Code != http.StatusServiceUnavailable || !strings.Contains(mutation.Body.String(), `"code":"not_ready"`) {
		t.Fatalf("mutation status=%d body=%s", mutation.Code, mutation.Body.String())
	}
	linear := request(http.MethodPost, "/sql/query", `{"sql":"SELECT id FROM qualification","consistency":"linearizable"}`)
	if linear.Code != http.StatusServiceUnavailable || !strings.Contains(linear.Body.String(), `"code":"quorum_unavailable"`) {
		t.Fatalf("linearizable read status=%d body=%s", linear.Code, linear.Body.String())
	}
	counters := request(http.MethodGet, "/qualification/object-store", "")
	if counters.Code != http.StatusOK || !strings.Contains(counters.Body.String(), `"bytes_downloaded":`) || replica.ObjectStoreStats().BytesDownloaded == 0 {
		t.Fatalf("download counters status=%d body=%s", counters.Code, counters.Body.String())
	}
}

func TestHistoryAwareObjectStoreReplicaConfigKeepsOnlyPublicIdentity(t *testing.T) {
	member := rhiza.Member{ID: "voter", URL: "http://voter", PeerURL: "quic://voter:9090", LogURL: "http://voter/log", PublicKey: rhiza.PublicKey{1}}
	config := objectStoreReplicaConfig(rhiza.Config{ClusterID: "history", NodeID: "cold", EnableReconfiguration: true, Members: []rhiza.Member{member}, AdminToken: "admin-secret", PeerToken: "peer-secret"})
	if len(config.Members) != 1 || config.Members[0].PeerURL != member.PeerURL || config.Members[0].PublicKey != member.PublicKey {
		t.Fatalf("history-aware public identity was not preserved: %+v", config)
	}
	if config.AdminToken != "" {
		t.Fatal("object-store replica retained an admin secret")
	}
}

func TestQualificationColdRestoreContract(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	template, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	for _, forbidden := range []string{"learner-cold", "rhiza-quic-probe", "cold-policy", "probe-connected", "probe-blocked"} {
		if strings.Contains(source, forbidden) || bytes.Contains(template, []byte(forbidden)) {
			t.Fatalf("unsupported cold gate remains: %s", forbidden)
		}
	}
	last := -1
	for _, step := range []string{`execute "$RHIZA_RUN_ID-cold-sentinel"`, "capture_shutdown ||", "run stop-voters k scale statefulset/rhiza-voter --current-replicas=3 --replicas=0", "finish_shutdown_capture ||", "verify_voter_absence ||", "run inspector-create", "run inspector-identity", "run head-metadata-freeze-1", "run head-freeze", "run head-metadata-freeze-2", "run archive-head-decoded", "run archive-root-freeze", "run checkpoint-metadata-freeze-1", "run checkpoint-current-freeze", "run checkpoint-metadata-freeze-2", "run checkpoint-root-before", "delete_inspector_owned ||", "run inventory-before", "run head-metadata-prelearner-1", "run head-prelearner", "run head-metadata-prelearner-2", "run checkpoint-metadata-prelearner-1", "run checkpoint-current-prelearner", "run checkpoint-metadata-prelearner-2", "run cold-create", "run learner-identity", "run learner-ready", `forward rhiza-learner "$port_learner" "$learner_uid"`, "run learner-local", "run learner-status", `validate_cold_restore_status "$learner_status"`, "run learner-mutation-rejected", "run learner-linearizable-rejected", "run learner-topology", "run head-metadata-after", "run checkpoint-metadata-after", "run learner-clock", "run archive-gc-lock-terminal", "run checkpoint-publisher-terminal", "run archive-recovery-pin-terminal", "run checkpoint-recovery-pin-terminal", "run inventory-after", `validate_cold_inventory "$out/object-inventory-before.json"`} {
		at := strings.Index(source, step)
		if at <= last {
			t.Fatalf("cold restore step missing or out of order: %q", step)
		}
		last = at
	}
	for _, required := range []string{
		`.status.readyReplicas // 0)==0 and (.status.currentReplicas // 0)==0`,
		`all($pods[0].items[]; all(.metadata.ownerReferences[]?; .uid!=$sts_uid))`,
		`(.targetRef.uid // "") as $uid|$uids|index($uid)|not`,
		`(.ip as $ip|$ips|index($ip)|not)`,
		`.mode=="object-store" and .source=="object-store" and .lag_slots==0`,
		`.applied_slot==.source_tip and .applied_slot>=$barrier`,
		`.checkpoint_base_slot==$head[0].base`,
		`.checkpoint_root_hash==$head[0].base_root_hash`,
		`.archive_head_version==$archive[0].generation`,
		`.recovery_pins_created==2 and .recovery_pins_released==2`,
		`.rows==[[1,"before"],[2,"network"],[3,"process"],[4,$sentinel]]`,
		`.applied_slot==$status[0].applied_slot and .consensus_tip==$status[0].source_tip`,
		`jq -e '.code=="not_ready"' "$out/learner-mutation-rejected.json"`,
		`jq -e '.code=="quorum_unavailable"' "$out/learner-linearizable-rejected.json"`,
		`same_object_evidence "$out/archive-head-metadata-before.json"`,
		`same_object_evidence "$out/checkpoint-CURRENT-metadata-before.json"`,
		`normalize_object_inventory "$out/$seq-inventory-before.stdout"`,
		`redacted_storage_json`,
		`del(.token)`,
		`preserve_run_output head-freeze "$out/archive-head-before.bin"`,
		`decode_archive_head "$out/archive-head-before.bin"`,
		`rhiza qualification-unix-milli`,
		`"$out/archive-base-root-before.json"`,
		`validate_cold_inventory "$out/object-inventory-before.json"`,
		`if [ "$source_quiesced" = true ]; then ports=$port_learner`,
		`validate_learner_pod "$out/$seq-learner-identity.stdout"`,
		`validate_learner_pod "$out/$seq-learner-topology.stdout"`,
		`jq -e --arg uid "$learner_uid" '.metadata.uid==$uid' "$identity"`,
		`shutdown_capture_state=idle`,
		`shutdown_capture_state=active`,
		`shutdown_capture_state=finishing`,
		`shutdown_capture_state=finished`,
		`if [ "$shutdown_capture_state" = idle ]; then`,
		`if [ "$shutdown_capture_state" = active ]; then`,
		`elif [ "$shutdown_capture_state" = finished ]; then`,
		`"$out/shutdown-before.next.json"`,
		`length>0 and (map({name,uid,namespace,run})`,
		`validate_inspector_pod`,
		`delete_inspector_owned`,
		`validate_current_against_archive`,
		`validate_checkpoint_root_against_seal`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("cold restore contract missing %q", required)
		}
	}
	if strings.Contains(source, `decode_archive_head "$out/$seq-head-before.stdout"`) {
		t.Fatal("archive head consumer still depends on mutable run sequence")
	}
	manifest := string(template)
	for _, required := range []string{"{name: RHIZA_ROLE, value: object-store}", `{name: RHIZA_ENABLE_RECONFIGURATION, value: "true"}`, "readinessProbe: {httpGet: {path: /ready, port: http}", "key: RHIZA_CLUSTER_MEMBERS"} {
		if !strings.Contains(manifest, required) {
			t.Fatalf("cold object-store Pod contract missing %q", required)
		}
	}
	coldStart := strings.Index(manifest, "# BEGIN COLD LEARNER")
	coldEnd := strings.Index(manifest, "# END COLD LEARNER")
	if coldStart < 0 || coldEnd <= coldStart {
		t.Fatal("cold object-store Pod template missing")
	}
	cold := manifest[coldStart:coldEnd]
	for _, forbidden := range []string{"learner_token", "learner_member", "RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKENS", "RHIZA_ADMIN_TOKEN"} {
		if strings.Contains(cold, forbidden) {
			t.Fatalf("cold object-store Pod retained membership credential %q", forbidden)
		}
	}
	cleanupAt, cleanupEnd := strings.Index(source, "\ncleanup() {"), strings.Index(source, "\ntrap cleanup EXIT")
	cleanup := source[cleanupAt:cleanupEnd]
	if learner, capture := strings.Index(cleanup, "k delete pod/rhiza-learner"), strings.Index(cleanup, "capture_shutdown"); learner < 0 || capture <= learner || !strings.Contains(cleanup, `if [ "${source_quiesced:-false}" = false ]; then`) {
		t.Fatal("failure cleanup does not remove learner first and preserve source quiescence")
	}
}

func TestQualificationPreserveRunOutputUsesCurrentProducer(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\npreserve_run_output() {\n")
	end := strings.Index(source[start+1:], "\nvalidate_cold_inventory() {\n")
	if start < 0 || end < 0 {
		t.Fatal("run-output preservation helper missing")
	}
	helper := source[start : start+1+end]
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "138-head-before.stdout"), []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "913-head-before.stdout"), []byte("current"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", "out=$1; seq=913; die() { printf '%s\\n' \"$*\" >&2; exit 1; }; "+helper+"\npreserve_run_output head-before \"$out/archive-head-before.bin\"", "fixture", dir)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("renumbered producer was not preserved: %v output=%s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(dir, "archive-head-before.bin"))
	if err != nil || string(data) != "current" {
		t.Fatalf("preserved output=%q err=%v", data, err)
	}
	command = exec.Command("/bin/sh", "-c", "out=$1; seq=914; die() { printf '%s\\n' \"$*\" >&2; exit 1; }; "+helper+"\npreserve_run_output head-before \"$out/missing.bin\"", "fixture", dir)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "missing run output for head-before") {
		t.Fatalf("missing producer did not fail clearly: %v output=%s", err, output)
	}
}

func TestQualificationLearnerIdentityAndVolumeContract(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nvalidate_learner_pod() {\n")
	end := strings.Index(source[start+1:], "\nprivate_file() {\n")
	if start < 0 || end < 0 {
		t.Fatal("learner validator missing")
	}
	validator := source[start : start+1+end]
	container := `{"name":"rhiza","envFrom":[{"configMapRef":{"name":"rhiza-config"}}],"env":[{"name":"RHIZA_NAMESPACE","valueFrom":{"fieldRef":{"fieldPath":"metadata.namespace"}}},{"name":"RHIZA_QUALIFICATION_RUN_ID","valueFrom":{"fieldRef":{"fieldPath":"metadata.labels['chaos.rhiza.io/run']"}}},{"name":"RHIZA_POD_UID","valueFrom":{"fieldRef":{"fieldPath":"metadata.uid"}}},{"name":"RHIZA_NODE_ID","value":"rhiza-learner"},{"name":"RHIZA_ROLE","value":"object-store"},{"name":"RHIZA_ENABLE_RECONFIGURATION","value":"true"},{"name":"RHIZA_CLUSTER_MEMBERS","valueFrom":{"secretKeyRef":{"name":"rhiza-qualification-auth","key":"RHIZA_CLUSTER_MEMBERS"}}}],"readinessProbe":{"httpGet":{"path":"/ready","port":"http"}}}`
	for _, tc := range []struct {
		name, uid, volumes, container string
		ok                            bool
	}{
		{"valid", "created", `[{"name":"data","emptyDir":{"sizeLimit":"1Gi"}}]`, container, true},
		{"uid-replacement-create", "replacement", `[{"name":"data","emptyDir":{}}]`, container, false},
		{"persistent-volume", "created", `[{"name":"data","persistentVolumeClaim":{"claimName":"data"}}]`, container, false},
		{"additional-volume", "created", `[{"name":"data","emptyDir":{}},{"name":"extra","emptyDir":{}}]`, container, false},
		{"learner-role", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"object-store"`, `"learner"`, 1), false},
		{"missing-namespace", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `{"name":"RHIZA_NAMESPACE","valueFrom":{"fieldRef":{"fieldPath":"metadata.namespace"}}},`, ``, 1), false},
		{"wrong-namespace-source", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"fieldPath":"metadata.namespace"`, `"fieldPath":"metadata.name"`, 1), false},
		{"missing-qualification-run-id", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `{"name":"RHIZA_QUALIFICATION_RUN_ID","valueFrom":{"fieldRef":{"fieldPath":"metadata.labels['chaos.rhiza.io/run']"}}},`, ``, 1), false},
		{"wrong-qualification-run-id-source", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `metadata.labels['chaos.rhiza.io/run']`, `metadata.labels['other']`, 1), false},
		{"missing-pod-uid", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `{"name":"RHIZA_POD_UID","valueFrom":{"fieldRef":{"fieldPath":"metadata.uid"}}},`, ``, 1), false},
		{"wrong-pod-uid-source", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"fieldPath":"metadata.uid"`, `"fieldPath":"metadata.name"`, 1), false},
		{"missing-node-id", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `{"name":"RHIZA_NODE_ID","value":"rhiza-learner"},`, ``, 1), false},
		{"wrong-node-id", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"RHIZA_NODE_ID","value":"rhiza-learner"`, `"RHIZA_NODE_ID","value":"rhiza-voter-0"`, 1), false},
		{"missing-reconfiguration", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `,{"name":"RHIZA_ENABLE_RECONFIGURATION","value":"true"}`, ``, 1), false},
		{"wrong-reconfiguration", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"RHIZA_ENABLE_RECONFIGURATION","value":"true"`, `"RHIZA_ENABLE_RECONFIGURATION","value":"false"`, 1), false},
		{"missing-cluster-members", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `,{"name":"RHIZA_CLUSTER_MEMBERS","valueFrom":{"secretKeyRef":{"name":"rhiza-qualification-auth","key":"RHIZA_CLUSTER_MEMBERS"}}}`, ``, 1), false},
		{"wrong-cluster-members-secret", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"name":"rhiza-qualification-auth"`, `"name":"other-secret"`, 1), false},
		{"wrong-cluster-members-key", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `"key":"RHIZA_CLUSTER_MEMBERS"`, `"key":"OTHER"`, 1), false},
		{"peer-credential", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `{"name":"RHIZA_ROLE"`, `{"name":"RHIZA_PEER_TOKEN"},{"name":"RHIZA_ROLE"`, 1), false},
		{"no-readiness", "created", `[{"name":"data","emptyDir":{}}]`, strings.Replace(container, `,"readinessProbe":{"httpGet":{"path":"/ready","port":"http"}}`, ``, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			created := `{"metadata":{"uid":"created"}}`
			pod := fmt.Sprintf(`{"metadata":{"name":"rhiza-learner","namespace":"synthetic","uid":%q,"labels":{"chaos.rhiza.io/run":"a1b2c3d4","chaos.rhiza.io/role":"object-store"}},"spec":{"volumes":%s,"containers":[%s]}}`, tc.uid, tc.volumes, tc.container)
			if err := os.WriteFile(filepath.Join(dir, "learner-created.json"), []byte(created), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "pod.json"), []byte(pod), 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "-c", "set -eu; out=$1; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; "+validator+"\nvalidate_learner_pod \"$out/pod.json\"", "fixture", dir)
			err := command.Run()
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%t want=%t error=%v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestQualificationInspectorIsolationContract(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"system:serviceaccount:", "system:serviceaccounts:", `system:serviceaccounts"`, `system:authenticated"`} {
		if !bytes.Contains(bootstrap, []byte(alias)) {
			t.Fatalf("bootstrap inspector RBAC alias rejection missing %q", alias)
		}
	}
	template, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	rendered := strings.NewReplacer("__NAMESPACE__", "ns", "__RUN_ID__", "a1b2c3d4", "__CLUSTER_ID__", "cluster",
		"__HOST_IMAGE__", "candidate@sha256:"+strings.Repeat("a", 64), "__METADATA_IMAGE__", "metadata@sha256:"+strings.Repeat("b", 64),
		"__NODE_A__", "node-a", "__NODE_B__", "node-b", "__NODE_C__", "node-c").Replace(string(template))
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	var inspector, policy map[string]any
	for {
		var document map[string]any
		if err := decoder.Decode(&document); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		metadata, _ := document["metadata"].(map[string]any)
		if document["kind"] == "Pod" && metadata["name"] == "rhiza-inspector" {
			inspector = document
		} else if document["kind"] == "NetworkPolicy" && metadata["name"] == "rhiza-inspector-deny-all" {
			policy = document
		}
	}
	if inspector == nil || policy == nil {
		t.Fatal("inspector Pod or default-deny NetworkPolicy missing")
	}
	raw, err := json.Marshal(inspector)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"envFrom", `"env"`, "secretKeyRef", "RHIZA_", "containerPort", "readinessProbe", "livenessProbe", "rhiza-voter", "rhiza-learner", `"volumes"`, `"volumeMounts"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("inspector retained forbidden identity/permission %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{`"serviceAccountName":"rhiza-inspector"`, `"automountServiceAccountToken":false`, `"hostNetwork":false`, `"hostPID":false`, `"hostIPC":false`, `"image":"candidate@sha256:`, `"readOnlyRootFilesystem":true`, `"runAsNonRoot":true`, `"drop":["ALL"]`} {
		if !strings.Contains(text, required) {
			t.Fatalf("inspector isolation contract missing %q: %s", required, text)
		}
	}
	policyRaw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	policyText := string(policyRaw)
	for _, required := range []string{`"policyTypes":["Ingress","Egress"]`, `"ingress":[]`, `"egress":[]`, `"app":"rhiza-inspector"`} {
		if !strings.Contains(policyText, required) {
			t.Fatalf("inspector network isolation missing %q: %s", required, policyText)
		}
	}
	script, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`run inspector-networkpolicy-bootstrap`, `validate_inspector_pod`, `validate_inspector_control_pod`, `validate_inspector_access`, `delete_inspector_owned`, `inspector-terminal-pods.json`, `inspector-terminal-networkpolicy.json`, `inspector-manual-cleanup-required`, `run inspector-control-before-reachable probe_inspector_control_egress`, `run inspector-egress-denied probe_inspector_egress`, `run inspector-control-after-reachable probe_inspector_control_egress`, `paired-network-policy-proof=control-reachable`, `paired-network-policy-proof=inspector-denied`, `busybox nc -w 3`, `reject_runtime_networkpolicy_mutation runtime-networkpolicy-delete-denied`, `reject_runtime_networkpolicy_mutation runtime-networkpolicy-replace-denied`, `reject_runtime_networkpolicy_mutation runtime-networkpolicy-additive-denied`, `"topology.kubernetes.io/region","topology.kubernetes.io/zone"`} {
		if !bytes.Contains(script, []byte(required)) {
			t.Fatalf("inspector lifecycle contract missing %q", required)
		}
	}
	source := string(script)
	policyVerify := strings.Index(source, "run inspector-networkpolicy-bootstrap")
	podCreate := strings.Index(source, "run inspector-create k create")
	controlCreate := strings.Index(source, "run inspector-control-create k create")
	controlProbeBefore := strings.Index(source, "run inspector-control-before-reachable")
	probe := strings.Index(source, "run inspector-egress-denied")
	controlProbeAfter := strings.Index(source, "run inspector-control-after-reachable")
	networkVerify := strings.LastIndex(source, `verify_inspector_network_isolation "$api_server"`)
	decode := strings.Index(source, "run archive-head-decoded")
	if policyVerify < 0 || podCreate <= policyVerify || controlCreate <= podCreate || controlProbeBefore < 0 || probe <= controlProbeBefore || controlProbeAfter <= probe || networkVerify <= controlCreate || decode <= networkVerify {
		t.Fatal("inspector ordering must be bootstrap policy verification, inspector/control creation, paired reachability/denial, then decode")
	}
	start := strings.Index(source, "\nvalidate_probe_pod() {\n")
	end := strings.Index(source[start+1:], "\nvalidate_learner_pod() {\n")
	if start < 0 || end < 0 {
		t.Fatal("inspector validator missing")
	}
	validator := source[start : start+1+end]
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "inspector-created.json"), []byte(`{"metadata":{"uid":"created"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	validPod := fmt.Sprintf(`{"metadata":{"name":"rhiza-inspector","namespace":"ns","uid":"created","labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}},"spec":{"serviceAccountName":"rhiza-inspector","automountServiceAccountToken":false,"hostNetwork":false,"hostPID":false,"hostIPC":false,"nodeName":"node-a","restartPolicy":"Never","securityContext":{"runAsNonRoot":true,"runAsUser":65532,"runAsGroup":65532,"fsGroup":65532,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"inspector","image":"candidate@%s","command":["sh","-c","sleep 1500"],"securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"runAsNonRoot":true,"capabilities":{"drop":["ALL"]}}}]},"status":{"phase":"Running","containerStatuses":[{"name":"inspector","ready":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}},"imageID":"candidate@%s"}]}}`, digest, digest)
	defaultedTopologyPod := strings.Replace(validPod,
		`"labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}`,
		`"labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4","topology.kubernetes.io/region":"region","topology.kubernetes.io/zone":"zone"}`, 1)
	for _, tc := range []struct {
		name, pod string
		valid     bool
	}{
		{"exact", validPod, true},
		{"defaulted-topology-labels", defaultedTopologyPod, true},
		{"wrong-run-label", strings.Replace(validPod, `"chaos.rhiza.io/run":"a1b2c3d4"`, `"chaos.rhiza.io/run":"wrong"`, 1), false},
		{"wrong-app-label", strings.Replace(validPod, `"app":"rhiza-inspector"`, `"app":"other"`, 1), false},
		{"unexpected-label", strings.Replace(validPod, `"labels":{`, `"labels":{"unexpected":"value",`, 1), false},
		{"partial-topology-labels", strings.Replace(validPod, `"labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}`, `"labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4","topology.kubernetes.io/region":"region"}`, 1), false},
		{"credential-env", strings.Replace(validPod, `"command":[`, `"env":[{"name":"GOOGLE_APPLICATION_CREDENTIALS","value":"/secret"}],"command":[`, 1), false},
		{"service-account-token", strings.Replace(validPod, `"automountServiceAccountToken":false`, `"automountServiceAccountToken":true`, 1), false},
		{"wrong-service-account", strings.Replace(validPod, `"serviceAccountName":"rhiza-inspector"`, `"serviceAccountName":"rhiza-gcs"`, 1), false},
		{"wrong-image", strings.Replace(validPod, "candidate@sha256:", "other@sha256:", 1), false},
		{"wrong-image-id", strings.Replace(validPod, `"imageID":"candidate@sha256:`, `"imageID":"candidate@sha256:b`, 1), false},
		{"host-network", strings.Replace(validPod, `"hostNetwork":false`, `"hostNetwork":true`, 1), false},
		{"root-user", strings.Replace(validPod, `"runAsUser":65532`, `"runAsUser":0`, 1), false},
		{"not-ready", strings.Replace(validPod, `"ready":true`, `"ready":false`, 1), false},
		{"credential-volume", strings.Replace(validPod, `"containers":[`, `"volumes":[{"name":"token","secret":{"secretName":"x"}}],"containers":[`, 1), false},
		{"lifecycle", strings.Replace(validPod, `"command":[`, `"lifecycle":{"postStart":{"exec":{"command":["true"]}}},"command":[`, 1), false},
		{"working-dir", strings.Replace(validPod, `"command":[`, `"workingDir":"/tmp","command":[`, 1), false},
		{"stdin", strings.Replace(validPod, `"command":[`, `"stdin":true,"command":[`, 1), false},
		{"tty", strings.Replace(validPod, `"command":[`, `"tty":true,"command":[`, 1), false},
	} {
		podPath := filepath.Join(dir, tc.name+".json")
		if err := os.WriteFile(podPath, []byte(tc.pod), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", "out=$1; ns=ns; RHIZA_RUN_ID=a1b2c3d4; RHIZA_HOST_IMAGE=candidate@sha256:"+strings.Repeat("a", 64)+"; RHIZA_NODE_A=node-a; "+validator+"\nvalidate_inspector_pod \"$2\"", "fixture", dir, podPath)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("inspector validator %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "inspector-control-created.json"), []byte(`{"metadata":{"uid":"control-created"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	validControl := strings.NewReplacer(`"name":"rhiza-inspector"`, `"name":"rhiza-inspector-control"`,
		`"uid":"created"`, `"uid":"control-created"`, `"app":"rhiza-inspector"`, `"app":"rhiza-inspector-control"`,
		`"name":"inspector"`, `"name":"control"`).Replace(validPod)
	controlPath := filepath.Join(dir, "control.json")
	if err := os.WriteFile(controlPath, []byte(validControl), 0600); err != nil {
		t.Fatal(err)
	}
	controlCommand := exec.Command("/bin/sh", "-c", "out=$1; ns=ns; RHIZA_RUN_ID=a1b2c3d4; RHIZA_HOST_IMAGE=candidate@sha256:"+strings.Repeat("a", 64)+"; RHIZA_NODE_A=node-a; "+validator+"\nvalidate_inspector_control_pod \"$2\"", "fixture", dir, controlPath)
	if err := controlCommand.Run(); err != nil {
		t.Fatalf("exact control Pod rejected: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inspector-networkpolicy-bootstrap.json"), []byte(`{"metadata":{"uid":"policy-created"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	ksa := `{"metadata":{"name":"rhiza-inspector","namespace":"ns"},"automountServiceAccountToken":false}`
	bindings := `{"items":[]}`
	policyJSON := `{"metadata":{"name":"rhiza-inspector-deny-all","namespace":"ns","uid":"policy-created","labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"},"annotations":{"rhiza.dev/auth-owner":"rhiza-postrelease-a1b2c3d4-` + strings.Repeat("a", 40) + `"}},"spec":{"podSelector":{"matchLabels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}},"policyTypes":["Ingress","Egress"],"ingress":[],"egress":[]}}`
	for _, tc := range []struct {
		name, ksa, rb, crb, policy string
		valid                      bool
	}{
		{"exact", ksa, bindings, bindings, policyJSON, true},
		{"omitted-rules", ksa, bindings, bindings, strings.Replace(strings.Replace(policyJSON, `,"ingress":[]`, ``, 1), `,"egress":[]`, ``, 1), true},
		{"null-rules", ksa, bindings, bindings, strings.Replace(strings.Replace(policyJSON, `"ingress":[]`, `"ingress":null`, 1), `"egress":[]`, `"egress":null`, 1), true},
		{"ksa-annotation", strings.Replace(ksa, `"namespace":"ns"`, `"namespace":"ns","annotations":{"iam.gke.io/gcp-service-account":"x"}`, 1), bindings, bindings, policyJSON, false},
		{"ksa-secret", strings.Replace(ksa, `,"automount`, `,"secrets":[{"name":"token"}],"automount`, 1), bindings, bindings, policyJSON, false},
		{"rolebinding", ksa, `{"items":[{"subjects":[{"kind":"ServiceAccount","name":"rhiza-inspector","namespace":"ns"}]}]}`, bindings, policyJSON, false},
		{"clusterrolebinding", ksa, bindings, `{"items":[{"subjects":[{"kind":"ServiceAccount","name":"rhiza-inspector","namespace":"ns"}]}]}`, policyJSON, false},
		{"alias-user", ksa, `{"items":[{"subjects":[{"kind":"User","name":"system:serviceaccount:ns:rhiza-inspector"}]}]}`, bindings, policyJSON, false},
		{"namespace-group", ksa, bindings, `{"items":[{"subjects":[{"kind":"Group","name":"system:serviceaccounts:ns"}]}]}`, policyJSON, false},
		{"all-serviceaccounts", ksa, bindings, `{"items":[{"subjects":[{"kind":"Group","name":"system:serviceaccounts"}]}]}`, policyJSON, false},
		{"authenticated", ksa, bindings, `{"items":[{"subjects":[{"kind":"Group","name":"system:authenticated"}]}]}`, policyJSON, false},
		{"egress-open", ksa, bindings, bindings, strings.Replace(policyJSON, `"egress":[]`, `"egress":[{}]`, 1), false},
		{"ingress-false", ksa, bindings, bindings, strings.Replace(policyJSON, `"ingress":[]`, `"ingress":false`, 1), false},
		{"egress-object", ksa, bindings, bindings, strings.Replace(policyJSON, `"egress":[]`, `"egress":{}`, 1), false},
		{"ingress-string", ksa, bindings, bindings, strings.Replace(policyJSON, `"ingress":[]`, `"ingress":"none"`, 1), false},
		{"missing-policy-types", ksa, bindings, bindings, strings.Replace(policyJSON, `,"policyTypes":["Ingress","Egress"]`, ``, 1), false},
		{"selector-expression", ksa, bindings, bindings, strings.Replace(policyJSON, `"matchLabels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}`, `"matchLabels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"},"matchExpressions":[]`, 1), false},
		{"wrong-owner", ksa, bindings, bindings, strings.Replace(policyJSON, `rhiza-postrelease-a1b2c3d4-`, `wrong-a1b2c3d4-`, 1), false},
		{"wrong-uid", ksa, bindings, bindings, strings.Replace(policyJSON, `"uid":"policy-created"`, `"uid":"other"`, 1), false},
		{"selector-spoof", ksa, bindings, bindings, strings.Replace(policyJSON, `"app":"rhiza-inspector"`, `"app":"other"`, 1), false},
	} {
		paths := []string{"ksa", "rb", "crb", "policy"}
		values := []string{tc.ksa, tc.rb, tc.crb, tc.policy}
		for i := range paths {
			if err := os.WriteFile(filepath.Join(dir, tc.name+"-"+paths[i]+".json"), []byte(values[i]), 0600); err != nil {
				t.Fatal(err)
			}
		}
		command := exec.Command("/bin/sh", "-c", "out=$1; ns=ns; RHIZA_RUN_ID=a1b2c3d4; RHIZA_AUTH_CREATION_SHA="+strings.Repeat("a", 40)+"; "+validator+"\nvalidate_inspector_access \"$2\" \"$3\" \"$4\" \"$5\"", "fixture", dir,
			filepath.Join(dir, tc.name+"-ksa.json"), filepath.Join(dir, tc.name+"-rb.json"), filepath.Join(dir, tc.name+"-crb.json"), filepath.Join(dir, tc.name+"-policy.json"))
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("inspector access %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
}

func TestQualificationInspectorBaselineClusterRoleGuard(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(bootstrap)
	start := strings.Index(source, "\nvalidate_inspector_clusterrolebindings() {\n")
	end := strings.Index(source[start+1:], "\nextract_created_identity() {\n")
	if start < 0 || end < 0 {
		t.Fatal("inspector ClusterRoleBinding guard missing")
	}
	guard := source[start : start+1+end]

	group := func(name string) map[string]any {
		return map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": name}
	}
	roleRef := func(name string) map[string]any {
		return map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name}
	}
	binding := func(name string, role string, subjects ...map[string]any) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": name}, "roleRef": roleRef(role), "subjects": subjects}
	}
	labels := map[string]any{"kubernetes.io/bootstrapping": "rbac-defaults"}
	roles := map[string]any{"items": []any{
		map[string]any{"metadata": map[string]any{"name": "system:basic-user", "labels": labels}, "rules": []any{
			map[string]any{"apiGroups": []string{"authorization.k8s.io"}, "resources": []string{"selfsubjectaccessreviews", "selfsubjectrulesreviews"}, "verbs": []string{"create"}},
			map[string]any{"apiGroups": []string{"authentication.k8s.io"}, "resources": []string{"selfsubjectreviews"}, "verbs": []string{"create"}},
		}},
		map[string]any{"metadata": map[string]any{"name": "system:discovery", "labels": labels}, "rules": []any{
			map[string]any{"nonResourceURLs": []string{"/api", "/api/*", "/apis", "/apis/*", "/healthz", "/livez", "/openapi", "/openapi/*", "/readyz", "/version", "/version/"}, "verbs": []string{"get"}},
		}},
		map[string]any{"metadata": map[string]any{"name": "system:public-info-viewer", "labels": labels}, "rules": []any{
			map[string]any{"nonResourceURLs": []string{"/healthz", "/livez", "/readyz", "/version", "/version/"}, "verbs": []string{"get"}},
		}},
		map[string]any{"metadata": map[string]any{"name": "system:service-account-issuer-discovery", "labels": labels}, "rules": []any{
			map[string]any{"nonResourceURLs": []string{"/.well-known/openid-configuration", "/.well-known/openid-configuration/", "/openid/v1/jwks", "/openid/v1/jwks/"}, "verbs": []string{"get"}},
		}},
	}}
	bindings := map[string]any{"items": []any{
		binding("system:basic-user", "system:basic-user", group("system:authenticated")),
		binding("system:discovery", "system:discovery", group("system:authenticated")),
		binding("system:public-info-viewer", "system:public-info-viewer", group("system:authenticated"), group("system:unauthenticated")),
		binding("system:service-account-issuer-discovery", "system:service-account-issuer-discovery", group("system:serviceaccounts")),
	}}
	runGuard := func(t *testing.T, bindings, roles map[string]any) error {
		t.Helper()
		dir := t.TempDir()
		bindingsPath := filepath.Join(dir, "bindings.json")
		rolesPath := filepath.Join(dir, "roles.json")
		for path, value := range map[string]map[string]any{bindingsPath: bindings, rolesPath: roles} {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		command := exec.Command("/bin/sh", "-c", guard+"\nvalidate_inspector_clusterrolebindings \"$1\" \"$2\" ns", "fixture", bindingsPath, rolesPath)
		return command.Run()
	}
	if err := runGuard(t, bindings, roles); err != nil {
		t.Fatalf("reviewed baseline ClusterRoleBindings rejected: %v", err)
	}
	clone := func(value map[string]any) map[string]any {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var copied map[string]any
		if err := json.Unmarshal(raw, &copied); err != nil {
			t.Fatal(err)
		}
		return copied
	}
	unrelated := clone(bindings)
	unrelated["items"] = append(unrelated["items"].([]any), binding("other-inspector", "system:basic-user",
		map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ServiceAccount", "name": "rhiza-inspector", "namespace": "other"}))
	if err := runGuard(t, unrelated, roles); err != nil {
		t.Fatalf("unrelated-namespace inspector subject was rejected: %v", err)
	}
	mutations := map[string]func(map[string]any, map[string]any){
		"extra-subject": func(bindings, roles map[string]any) {
			first := bindings["items"].([]any)[0].(map[string]any)
			first["subjects"] = append(first["subjects"].([]any), group("system:unauthenticated"))
		},
		"extra-role-rule": func(bindings, roles map[string]any) {
			second := roles["items"].([]any)[1].(map[string]any)
			second["rules"] = append(second["rules"].([]any), map[string]any{"apiGroups": []string{""}, "resources": []string{"secrets"}, "verbs": []string{"get"}})
		},
		"name-spoof": func(bindings, roles map[string]any) {
			first := bindings["items"].([]any)[0].(map[string]any)
			first["metadata"].(map[string]any)["name"] = "spoofed-baseline"
		},
		"direct-inspector": func(bindings, roles map[string]any) {
			bindings["items"] = append(bindings["items"].([]any), binding("inspector-alias", "system:basic-user", map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ServiceAccount", "name": "rhiza-inspector", "namespace": "ns"}))
		},
		"target-group": func(bindings, roles map[string]any) {
			bindings["items"] = append(bindings["items"].([]any), binding("inspector-group", "system:basic-user", group("system:serviceaccounts:ns")))
		},
		"target-user": func(bindings, roles map[string]any) {
			bindings["items"] = append(bindings["items"].([]any), binding("inspector-user", "system:basic-user", map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "system:serviceaccount:ns:rhiza-inspector"}))
		},
		"aggregation": func(bindings, roles map[string]any) {
			third := roles["items"].([]any)[2].(map[string]any)
			third["aggregationRule"] = map[string]any{"clusterRoleSelectors": []any{}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			badBindings := clone(bindings)
			badRoles := clone(roles)
			mutate(badBindings, badRoles)
			if err := runGuard(t, badBindings, badRoles); err == nil {
				t.Fatalf("mutated baseline guard %s was accepted", name)
			}
		})
	}
}

func TestQualificationCreateReceiptsPrecedeInspectorGuard(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(bootstrap)
	create := strings.Index(source, "record kubernetes-create k create -f")
	mark := strings.Index(source, "mark kubernetes")
	extractNamespace := strings.Index(source, "extract_created_identity namespace")
	extractInspector := strings.Index(source, "extract_created_identity ServiceAccount rhiza-inspector")
	extractPolicies := strings.Index(source, "extract_created_identity \"$kind\"")
	extractReader := strings.Index(source, "extract_created_identity clusterrole")
	guard := create + strings.Index(source[create:], "validate_inspector_clusterrolebindings")
	if create < 0 || mark < create || extractNamespace <= mark || extractInspector <= extractNamespace ||
		extractPolicies <= extractInspector || extractReader <= extractPolicies || guard <= extractReader {
		t.Fatalf("create receipt extraction ordering changed: create=%d mark=%d namespace=%d inspector=%d policies=%d reader=%d guard=%d",
			create, mark, extractNamespace, extractInspector, extractPolicies, extractReader, guard)
	}
	for _, required := range []string{"validatingadmissionpolicy", "validatingadmissionpolicybinding", "clusterrole-reader-identity.json", "clusterrolebinding-reader-identity.json"} {
		if !strings.Contains(source, required) {
			t.Fatalf("cleanup identity extraction missing %q", required)
		}
	}
	for _, required := range []string{
		"namespace) created_kind=Namespace",
		"validatingadmissionpolicy) created_kind=ValidatingAdmissionPolicy",
		"validatingadmissionpolicybinding) created_kind=ValidatingAdmissionPolicyBinding",
		"clusterrole) created_kind=ClusterRole",
		"clusterrolebinding) created_kind=ClusterRoleBinding",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("native create receipt kind normalization missing %q", required)
		}
	}
}

func TestQualificationExtractCreatedIdentitiesSurviveGuardFailure(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(bootstrap)
	start := strings.Index(source, "\nextract_created_identity() {\n")
	end := strings.Index(source[start+1:], "\nverify_deleted_role() {\n")
	if start < 0 || end < 0 {
		t.Fatal("create receipt extraction helper missing")
	}
	extraction := source[start : start+1+end]
	ns := "rhiza-v0191-fixture"
	owner := "rhiza-postrelease-fixture-owner"
	object := func(kind, name string, owned bool) map[string]any {
		metadata := map[string]any{"name": name, "uid": "uid-" + name, "resourceVersion": "rv-" + name}
		if owned {
			metadata["annotations"] = map[string]any{"rhiza.dev/auth-owner": owner}
		}
		return map[string]any{"kind": kind, "apiVersion": "fixture/v1", "metadata": metadata}
	}
	base := []map[string]any{
		object("Namespace", ns, true),
		object("ServiceAccount", "rhiza-inspector", false),
	}
	for _, suffix := range []string{"faults", "network-faults"} {
		base = append(base,
			object("ValidatingAdmissionPolicy", ns+"-"+suffix, true),
			object("ValidatingAdmissionPolicyBinding", ns+"-"+suffix, true),
		)
	}
	base = append(base, object("ClusterRole", ns+"-reader", true), object("ClusterRoleBinding", ns+"-reader", true))
	expected := []string{
		"namespace-identity.json", "inspector-ksa-identity.json",
		"validatingadmissionpolicy-faults-identity.json", "validatingadmissionpolicybinding-faults-identity.json",
		"validatingadmissionpolicy-network-faults-identity.json", "validatingadmissionpolicybinding-network-faults-identity.json",
		"clusterrole-reader-identity.json", "clusterrolebinding-reader-identity.json",
	}
	runExtraction := func(t *testing.T, docs []map[string]any, forceGuardFailure bool) error {
		t.Helper()
		dir := t.TempDir()
		raw := make([][]byte, len(docs))
		for i, doc := range docs {
			raw[i], err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "kubernetes-create.json"), bytes.Join(raw, []byte("\n")), 0600); err != nil {
			t.Fatal(err)
		}
		script := "set -eu\n" + extraction +
			"\nextract_created_identity namespace \"" + ns + "\" \"$RHIZA_AUTH_STATE/namespace-identity.json\" \"" + owner + "\" || exit $?" +
			"\nextract_created_identity ServiceAccount rhiza-inspector \"$RHIZA_AUTH_STATE/inspector-ksa-identity.json\" \"\" || exit $?"
		for _, kind := range []string{"validatingadmissionpolicy", "validatingadmissionpolicybinding"} {
			for _, suffix := range []string{"faults", "network-faults"} {
				script += "\nextract_created_identity " + kind + " \"" + ns + "-" + suffix + "\" \"$RHIZA_AUTH_STATE/" + kind + "-" + suffix + "-identity.json\" \"" + owner + "\" || exit $?"
			}
		}
		script += "\nextract_created_identity clusterrole \"" + ns + "-reader\" \"$RHIZA_AUTH_STATE/clusterrole-reader-identity.json\" \"" + owner + "\" || exit $?"
		script += "\nextract_created_identity clusterrolebinding \"" + ns + "-reader\" \"$RHIZA_AUTH_STATE/clusterrolebinding-reader-identity.json\" \"" + owner + "\" || exit $?"
		if forceGuardFailure {
			script += "\nfalse"
		}
		command := exec.Command("/bin/sh", "-c", script)
		command.Env = append(os.Environ(), "RHIZA_AUTH_STATE="+dir)
		err = command.Run()
		if forceGuardFailure {
			for _, name := range expected {
				if _, statErr := os.Stat(filepath.Join(dir, name)); statErr != nil {
					t.Fatalf("identity receipt %s missing after forced guard failure: %v", name, statErr)
				}
			}
		}
		return err
	}
	if err := runExtraction(t, base, true); err == nil {
		t.Fatal("forced post-create guard failure unexpectedly succeeded")
	}
	missing := append([]map[string]any(nil), base...)
	missing = append(missing[:4], missing[5:]...)
	if err := runExtraction(t, missing, false); err == nil {
		t.Fatal("missing create identity unexpectedly accepted")
	}
	duplicate := append([]map[string]any(nil), base...)
	duplicate = append(duplicate, object("Namespace", ns, true))
	if err := runExtraction(t, duplicate, false); err == nil {
		t.Fatal("duplicate create identity unexpectedly accepted")
	}
	wrongOwner := append([]map[string]any(nil), base...)
	wrongNamespace := object("Namespace", ns, true)
	wrongNamespace["metadata"].(map[string]any)["annotations"] = map[string]any{"rhiza.dev/auth-owner": "wrong"}
	wrongOwner[0] = wrongNamespace
	if err := runExtraction(t, wrongOwner, false); err == nil {
		t.Fatal("wrong-owner create identity unexpectedly accepted")
	}
}

func TestQualificationInspectorPartialCleanup(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\ndelete_inspector_owned() {\n")
	end := strings.Index(source[start+1:], "\ncleanup_metadata_only() (\n")
	if start < 0 || end < 0 {
		t.Fatal("inspector cleanup function missing")
	}
	cleanup := source[start : start+1+end]
	for _, tc := range []struct {
		name                                                                     string
		pod, podReceipt, control, controlReceipt, ok, podDeleted, controlDeleted bool
	}{
		{"no-pod", false, false, false, false, true, false, false},
		{"both-owned", true, true, true, true, true, true, true},
		{"control-only-owned", false, false, true, true, true, false, true},
		{"pod-unknown", true, false, true, true, false, false, true},
		{"control-unknown", true, true, true, false, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "inspector-manual-cleanup-required"), []byte("required\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.podReceipt {
				if err := os.WriteFile(filepath.Join(dir, "inspector-created.json"), []byte(`{"metadata":{"uid":"pod-uid"}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.controlReceipt {
				if err := os.WriteFile(filepath.Join(dir, "inspector-control-created.json"), []byte(`{"metadata":{"uid":"control-uid"}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "inspector-networkpolicy-bootstrap.json"), []byte(`{"metadata":{"uid":"policy-uid"}}`), 0600); err != nil {
				t.Fatal(err)
			}
			fixture := `set -eu
out=$1; ns=ns; RHIZA_RUN_ID=a1b2c3d4; pod_exists=$2; control_exists=$3
validate_inspector_policy() { return 0; }
k() {
  case "$*" in
    "get pods -o json")
      printf '{"items":['; comma=
      if [ "$pod_exists" = true ] && [ ! -f "$out/pod.deleted" ]; then printf '%s' '{"metadata":{"name":"rhiza-inspector","namespace":"ns","uid":"pod-uid","resourceVersion":"11","labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4","topology.kubernetes.io/region":"region","topology.kubernetes.io/zone":"zone"}}}'; comma=,; fi
      if [ "$control_exists" = true ] && [ ! -f "$out/control.deleted" ]; then printf '%s' "$comma"'{"metadata":{"name":"rhiza-inspector-control","namespace":"ns","uid":"control-uid","resourceVersion":"12","labels":{"app":"rhiza-inspector-control","chaos.rhiza.io/run":"a1b2c3d4","topology.kubernetes.io/region":"region","topology.kubernetes.io/zone":"zone"}}}'; fi
      printf ']}\n' ;;
    "get networkpolicy.networking.k8s.io rhiza-inspector-deny-all -o json") printf '%s\n' '{"metadata":{"name":"rhiza-inspector-deny-all"}}' ;;
    "get networkpolicies.networking.k8s.io -o json") printf '%s\n' '{"items":[{"metadata":{"uid":"policy-uid"}}]}' ;;
    "delete --raw /api/v1/namespaces/ns/pods/rhiza-inspector -f "*) : > "$out/pod.deleted" ;;
    "delete --raw /api/v1/namespaces/ns/pods/rhiza-inspector-control -f "*) : > "$out/control.deleted" ;;
    "wait pod/rhiza-inspector --for=delete --timeout=90s") : ;;
    "wait pod/rhiza-inspector-control --for=delete --timeout=90s") : ;;
    *) return 91 ;;
  esac
}
` + cleanup + `
delete_inspector_owned`
			command := exec.Command("/bin/sh", "-c", fixture, "fixture", dir, strconv.FormatBool(tc.pod), strconv.FormatBool(tc.control))
			err := command.Run()
			if (err == nil) != tc.ok {
				t.Fatalf("cleanup success=%t want=%t error=%v", err == nil, tc.ok, err)
			}
			_, markerErr := os.Stat(filepath.Join(dir, "inspector-manual-cleanup-required"))
			if (markerErr != nil) != tc.ok {
				t.Fatalf("manual marker cleared=%t want=%t", markerErr != nil, tc.ok)
			}
			for path, want := range map[string]bool{"pod.deleted": tc.podDeleted, "control.deleted": tc.controlDeleted, "policy.deleted": false} {
				_, statErr := os.Stat(filepath.Join(dir, path))
				if (statErr == nil) != want {
					t.Fatalf("%s exists=%t want=%t", path, statErr == nil, want)
				}
			}
		})
	}
}

func TestQualificationRuntimeNetworkPolicyMutationDenied(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nreject_runtime_networkpolicy_mutation() {\n")
	end := strings.Index(source[start+1:], "\nstarted=")
	if start < 0 || end < 0 {
		t.Fatal("runtime NetworkPolicy denial helper missing")
	}
	for _, invocation := range []string{
		`reject_runtime_networkpolicy_mutation runtime-networkpolicy-delete-denied delete --dry-run=server -f "$out/inspector-networkpolicy-bootstrap.json" -o name`,
		`reject_runtime_networkpolicy_mutation runtime-networkpolicy-replace-denied replace --dry-run=server -f "$out/inspector-networkpolicy-replacement.json" -o json`,
		`reject_runtime_networkpolicy_mutation runtime-networkpolicy-additive-denied create --dry-run=server -f "$out/inspector-networkpolicy-additive.json" -o json`,
	} {
		if !strings.Contains(source, invocation) {
			t.Fatalf("runtime NetworkPolicy mutation invocation missing %q", invocation)
		}
	}
	helper := source[start : start+1+end]
	for _, tc := range []struct {
		name, verb, output, expectedOutput, outcome string
		ok                                          bool
	}{
		{"delete-forbidden", "delete", "name", "name", "forbidden", true},
		{"replace-forbidden", "replace", "json", "json", "forbidden", true},
		{"create-forbidden", "create", "json", "json", "forbidden", true},
		{"delete-unsupported-output", "delete", "json", "name", "forbidden", false},
		{"delete-admitted", "delete", "name", "name", "admitted", false},
		{"replace-transport-failure", "replace", "json", "json", "transport", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fixture := `set -eu
out=$1; seq=0; outcome=$2; expected_verb=$3; expected_output=$4
die() { exit 97; }
k() {
	  [ "$1" = "$expected_verb" ] && [ "$2" = --dry-run=server ] && [ "$3" = -f ] &&
	    [ "$4" = manifest ] && [ "$5" = -o ] && [ "$6" = "$expected_output" ] || {
	    printf '%s\n' 'unexpected NetworkPolicy mutation argv' >&2; return 96
	  }
  case "$outcome" in
    forbidden) printf 'Error from server (Forbidden): cannot %s networkpolicies\n' "$expected_verb" >&2; return 1 ;;
    admitted) printf '%s\n' '{"kind":"NetworkPolicy"}'; return 0 ;;
    transport) printf '%s\n' 'connection reset by peer' >&2; return 1 ;;
  esac
}
` + helper + `
reject_runtime_networkpolicy_mutation mutation ` + tc.verb + ` --dry-run=server -f manifest -o ` + tc.output
			command := exec.Command("/bin/sh", "-c", fixture, "fixture", dir, tc.outcome, tc.verb, tc.expectedOutput)
			err := command.Run()
			if (err == nil) != tc.ok {
				t.Fatalf("outcome=%s success=%t want=%t error=%v", tc.outcome, err == nil, tc.ok, err)
			}
		})
	}
}

func TestQualificationInspectorEgressProbeFailClosed(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nprobe_inspector_control_egress() {\n")
	end := strings.Index(source[start+1:], "\nstarted=")
	if start < 0 || end < 0 {
		t.Fatal("inspector egress probe helper missing")
	}
	helper := source[start : start+1+end]
	for _, tc := range []struct {
		name, controlBefore, inspectorOutcome, controlAfter string
		ok                                                  bool
	}{
		{"bracketed-proof", "connect", "drop", "connect", true},
		{"control-before-failure", "refuse", "drop", "connect", false},
		{"control-after-failure", "connect", "drop", "refuse", false},
		{"immediate-refusal", "connect", "refuse", "connect", false},
		{"inspector-connected", "connect", "connect", "connect", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			busybox := `#!/bin/sh
[ "$1 $2 $3" = "nc -w 3" ] || exit 91
case "$OUTCOME" in drop) sleep 2; exit 1 ;; refuse) exit 1 ;; connect) exit 0 ;; esac
exit 92
`
			if err := os.WriteFile(filepath.Join(dir, "busybox"), []byte(busybox), 0700); err != nil {
				t.Fatal(err)
			}
			fixture := `set -eu
bin=$1; CONTROL_BEFORE=$2; INSPECTOR_OUTCOME=$3; CONTROL_AFTER=$4
k() {
	pod=$2
	[ "$3 $4 $5" = "-- sh -c" ] || return 90
	shift 5
	script=$1; shift
	case "$pod" in
	  rhiza-inspector-control) if [ "$4" = before ]; then OUTCOME=$CONTROL_BEFORE; else OUTCOME=$CONTROL_AFTER; fi ;;
	  rhiza-inspector) OUTCOME=$INSPECTOR_OUTCOME ;;
	  *) return 90 ;;
	esac
	export OUTCOME
  PATH="$bin:$PATH" sh -c "$script" "$@"
}
` + helper + `
probe_inspector_control_egress 192.0.2.1 443 before && probe_inspector_egress 192.0.2.1 443 && probe_inspector_control_egress 192.0.2.1 443 after`
			command := exec.Command("/bin/sh", "-c", fixture, "fixture", dir, tc.controlBefore, tc.inspectorOutcome, tc.controlAfter)
			err := command.Run()
			if (err == nil) != tc.ok {
				t.Fatalf("before=%s inspector=%s after=%s success=%t want=%t error=%v", tc.controlBefore, tc.inspectorOutcome, tc.controlAfter, err == nil, tc.ok, err)
			}
		})
	}
}

func TestQualificationKubernetesServerURL(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nparse_https_443() {\n")
	end := strings.Index(source[start+1:], "\nprobe_inspector_control_egress() {\n")
	if start < 0 || end < 0 {
		t.Fatal("Kubernetes server URL parser missing")
	}
	parser := source[start : start+1+end]
	for _, tc := range []struct {
		name, server, host string
		ok                 bool
	}{
		{"implicit-443-ip", "https://192.0.2.10", "192.0.2.10", true},
		{"explicit-443-ip", "https://192.0.2.10:443", "192.0.2.10", true},
		{"implicit-443-ipv6", "https://[2001:db8::10]", "2001:db8::10", true},
		{"explicit-443-ipv6", "https://[2001:db8::10]:443", "2001:db8::10", true},
		{"http", "http://192.0.2.10", "", false},
		{"wrong-port", "https://192.0.2.10:6443", "", false},
		{"path", "https://192.0.2.10/api", "", false},
		{"query", "https://192.0.2.10?x=1", "", false},
		{"userinfo", "https://user@192.0.2.10", "", false},
		{"unbracketed-ipv6", "https://2001:db8::10", "", false},
		{"malformed-ipv6", "https://[2001:db8::10", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := "set -eu\n" + parser + `
parse_https_443 "$1"
[ "$api_host" = "$2" ]
[ "$api_port" = 443 ]`
			command := exec.Command("/bin/sh", "-c", fixture, "fixture", tc.server, tc.host)
			err := command.Run()
			if (err == nil) != tc.ok {
				t.Fatalf("server=%q success=%t want=%t error=%v", tc.server, err == nil, tc.ok, err)
			}
		})
	}
	endAll := strings.Index(source[start+1:], "\nstarted=")
	if endAll < 0 {
		t.Fatal("network isolation verifier boundary missing")
	}
	helper := source[start : start+1+endAll]
	dir := t.TempDir()
	busybox := `#!/bin/sh
[ "$1 $2 $3" = "nc -w 3" ] || exit 91
case "$OUTCOME" in control) exit 0 ;; inspector) sleep 2; exit 1 ;; esac
exit 92
`
	if err := os.WriteFile(filepath.Join(dir, "busybox"), []byte(busybox), 0700); err != nil {
		t.Fatal(err)
	}
	fixture := `set -eu
bin=$1; trace=$2
die() { exit 97; }
run() { label=$1; shift; printf '%s\n' "$label" >> "$trace"; "$@"; }
k() {
  case "$1 $2" in
    "exec rhiza-inspector-control") OUTCOME=control ;;
    "exec rhiza-inspector") OUTCOME=inspector ;;
    *) return 90 ;;
  esac
  export OUTCOME
  [ "$3 $4 $5" = "-- sh -c" ] || return 90
  shift 5
  script=$1; shift
  PATH="$bin:$PATH" sh -c "$script" "$@"
}
` + helper + `
verify_inspector_network_isolation https://192.0.2.10
[ "$(cat "$trace")" = "inspector-control-before-reachable
inspector-egress-denied
inspector-control-after-reachable" ]`
	trace := filepath.Join(dir, "trace")
	if output, err := exec.Command("/bin/sh", "-c", fixture, "fixture", dir, trace).CombinedOutput(); err != nil {
		t.Fatalf("implicit-443 end-to-end isolation path failed: %v\n%s", err, output)
	}
}

func TestQualificationLearnerCreateAck(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nnormalize_learner_create_ack() {\n")
	end := strings.Index(source[start+1:], "\nprivate_file() {\n")
	if start < 0 || end < 0 {
		t.Fatal("learner create ACK normalizer missing")
	}
	normalizer := source[start : start+1+end]
	service := `{"apiVersion":"v1","kind":"Service","metadata":{"name":"rhiza-learner","namespace":"synthetic","uid":"service-uid","labels":{"chaos.rhiza.io/run":"a1b2c3d4"}}}`
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"rhiza-learner","namespace":"synthetic","uid":"pod-uid","labels":{"chaos.rhiza.io/run":"a1b2c3d4"}}}`
	if json.Valid([]byte(service + "\n" + pod)) {
		t.Fatal("concatenated kubectl output unexpectedly is one JSON document")
	}
	for _, tc := range []struct {
		name, raw string
		ok        bool
	}{
		{"valid-stream", service + "\n" + pod, true},
		{"missing-service", pod, false},
		{"missing-pod-uid", service + "\n" + strings.Replace(pod, `"uid":"pod-uid",`, "", 1), false},
		{"wrong-service-name", strings.Replace(service, `"name":"rhiza-learner"`, `"name":"other"`, 1) + "\n" + pod, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			raw := filepath.Join(dir, "raw.json")
			if err := os.WriteFile(raw, []byte(tc.raw), 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "-c", "set -eu; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; "+normalizer+"\nnormalize_learner_create_ack \"$1\"", "fixture", raw)
			output, err := command.Output()
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%t want=%t error=%v", err == nil, tc.ok, err)
			}
			if !tc.ok {
				return
			}
			var ack struct {
				Kind  string `json:"kind"`
				Items []struct {
					Kind     string `json:"kind"`
					Metadata struct {
						Name string `json:"name"`
						UID  string `json:"uid"`
					} `json:"metadata"`
				} `json:"items"`
			}
			if err := json.Unmarshal(output, &ack); err != nil {
				t.Fatalf("generated ACK is not valid JSON: %v", err)
			}
			if ack.Kind != "List" || len(ack.Items) != 2 || ack.Items[0].Kind != "Service" || ack.Items[0].Metadata.Name != "rhiza-learner" || ack.Items[0].Metadata.UID != "service-uid" || ack.Items[1].Kind != "Pod" || ack.Items[1].Metadata.Name != "rhiza-learner" || ack.Items[1].Metadata.UID != "pod-uid" {
				t.Fatalf("unexpected structured ACK: %+v", ack)
			}
		})
	}
}

func TestQualificationColdRestoreAbsencePredicate(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "\nverify_voter_absence() {\n")
	end := strings.Index(source[start+1:], "\ncleanup_metadata_only() (\n")
	if start < 0 || end < 0 {
		t.Fatal("voter absence predicate missing")
	}
	function := source[start : start+1+end]
	for _, tc := range []struct {
		name, pods, slices, endpoints string
		ok                            bool
	}{
		{"clean", `{"items":[]}`, `{"items":[]}`, `{"metadata":{"name":"rhiza-peers"},"subsets":[]}`, true},
		{"terminating-owned", `{"items":[{"metadata":{"deletionTimestamp":"now","ownerReferences":[{"uid":"sts"}]}}]}`, `{"items":[]}`, `{"metadata":{"name":"rhiza-peers"},"subsets":[]}`, false},
		{"slice-target", `{"items":[]}`, `{"items":[{"metadata":{"labels":{"kubernetes.io/service-name":"rhiza-peers"}},"endpoints":[{"targetRef":{"uid":"v0"},"addresses":["10.0.0.1"]}]}]}`, `{"metadata":{"name":"rhiza-peers"},"subsets":[]}`, false},
		{"slice-ip", `{"items":[]}`, `{"items":[{"metadata":{"labels":{"kubernetes.io/service-name":"rhiza-peers"}},"endpoints":[{"addresses":["10.0.0.1"]}]}]}`, `{"metadata":{"name":"rhiza-peers"},"subsets":[]}`, false},
		{"wrong-service", `{"items":[]}`, `{"items":[]}`, `{"metadata":{"name":"other"},"subsets":[]}`, false},
		{"legacy", `{"items":[]}`, `{"items":[]}`, `{"metadata":{"name":"rhiza-peers"},"subsets":[{"notReadyAddresses":[{"ip":"10.0.0.1"}]}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, value := range map[string]string{
				"shutdown-controller-before.json": `{"metadata":{"uid":"sts"}}`,
				"shutdown-before.json":            `[{"name":"rhiza-voter-0","uid":"v0"},{"name":"rhiza-voter-1","uid":"v1"},{"name":"rhiza-voter-2","uid":"v2"}]`,
				"shutdown-pods-before.json":       `{"items":[{"metadata":{"uid":"v0"},"status":{"podIP":"10.0.0.1"}}]}`,
				"controller":                      `{"metadata":{"uid":"sts"},"spec":{"replicas":0},"status":{"replicas":0,"readyReplicas":0,"currentReplicas":0}}`,
				"pods":                            tc.pods, "slices": tc.slices, "endpoints": tc.endpoints,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			fixture := `set -eu
out=$1
k() { case "$*" in
 'get statefulset/rhiza-voter -o json') cat "$out/controller";;
 'get pods -o json') cat "$out/pods";;
 'get endpointslices.discovery.k8s.io -l kubernetes.io/service-name=rhiza-peers -o json') cat "$out/slices";;
 'get endpoints rhiza-peers -o json') cat "$out/endpoints";;
 *) return 91;; esac; }
` + function + `
verify_voter_absence`
			err := exec.Command("/bin/sh", "-c", fixture, "fixture", dir).Run()
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%t want=%t error=%v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestQualificationRuntimeEndpointRBAC(t *testing.T) {
	command := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "render-local")
	command.Env = append(os.Environ(),
		"RHIZA_RUN_ID=a1b2c3d4",
		"RHIZA_HARNESS_SHA=9b53158b38352ae78446e70a298616a31dc44a69",
		"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:"+strings.Repeat("a", 64),
		"RHIZA_AUTH_EXPIRES=2026-10-11T00:00:00Z",
	)
	rendered, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, rendered)
	}

	rules := []string{
		"  - apiGroups: ['']\n    resources: [endpoints]\n    verbs: [get, list, watch]\n",
		"  - apiGroups: [discovery.k8s.io]\n    resources: [endpointslices]\n    verbs: [get, list, watch]\n",
	}
	var runtimeRole string
	for _, document := range strings.Split(string(rendered), "\n---\n") {
		if strings.Contains(document, "\nkind: Role\n") && strings.Contains(document, "  name: rhiza-ci-runtime\n") {
			runtimeRole = document
		}
		if strings.Contains(document, "\nkind: ClusterRole\n") && (strings.Contains(document, "resources: [endpoints]") || strings.Contains(document, "discovery.k8s.io")) {
			t.Fatal("endpoint access escaped the namespaced runtime role")
		}
	}
	for _, rule := range rules {
		if strings.Count(runtimeRole, rule) != 1 {
			t.Fatalf("rendered runtime endpoint role is not exact:\n%s", runtimeRole)
		}
	}
	if strings.Count(runtimeRole, "resources: [endpoints]") != 1 || strings.Count(runtimeRole, "discovery.k8s.io") != 1 || !strings.Contains(runtimeRole, "  namespace: rhiza-v0191-20261008-a1b2c3d4\n") {
		t.Fatalf("rendered runtime endpoint role is not exact:\n%s", runtimeRole)
	}
	networkRule := strings.Index(runtimeRole, "resources: [networkpolicies]")
	networkRuleEnd := len(runtimeRole)
	if networkRule >= 0 {
		if next := strings.Index(runtimeRole[networkRule:], "\n  - apiGroups:"); next >= 0 {
			networkRuleEnd = networkRule + next
		}
	}
	if networkRule < 0 || !strings.Contains(runtimeRole[networkRule:networkRuleEnd], "verbs: [get, list, watch]") ||
		strings.Contains(runtimeRole[networkRule:networkRuleEnd], "create") || strings.Contains(runtimeRole[networkRule:networkRuleEnd], "delete") ||
		strings.Contains(runtimeRole[networkRule:networkRuleEnd], "update") || strings.Contains(runtimeRole[networkRule:networkRuleEnd], "patch") {
		t.Fatalf("runtime NetworkPolicy access is not read-only:\n%s", runtimeRole)
	}
	if !strings.Contains(string(rendered), "kind: NetworkPolicy\nmetadata:\n  name: rhiza-inspector-deny-all") ||
		!strings.Contains(string(rendered), "annotations: {rhiza.dev/auth-owner:") {
		t.Fatal("bootstrap-owned inspector deny-all policy missing")
	}
}

func TestQualificationRenderedMetadataValuesAreStrings(t *testing.T) {
	const runID = "63602191"
	auth := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "render-local")
	auth.Env = append(os.Environ(),
		"RHIZA_RUN_ID="+runID,
		"RHIZA_HARNESS_SHA=9b53158b38352ae78446e70a298616a31dc44a69",
		"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:"+strings.Repeat("a", 64),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
	)
	auth.Env = append(auth.Env, qualificationAssetEnv(runID)...)
	auth.Env = append(auth.Env, "RHIZA_NAMESPACE=rhiza-qualification-"+runID)
	authRendered, err := auth.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, authRendered)
	}
	if bytes.Contains(authRendered, []byte("kind: ValidatingAdmissionPolicy\nmetadata:\n  name: rhiza-qualification-"+runID+"-runtime")) {
		t.Fatal("rendered auth contract retained the excluded runtime admission policy")
	}
	for _, policy := range []string{"faults", "network-faults"} {
		if !bytes.Contains(authRendered, []byte("kind: ValidatingAdmissionPolicy\nmetadata:\n  name: rhiza-qualification-"+runID+"-"+policy)) {
			t.Fatalf("rendered auth contract lost %s admission policy", policy)
		}
	}
	template, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	workloadRendered := strings.NewReplacer(
		"__NAMESPACE__", "rhiza-v0191-20261008-"+runID,
		"__RUN_ID__", runID,
		"__CLUSTER_ID__", "cluster-"+runID,
		"__HOST_IMAGE__", "example.invalid/rhiza@sha256:"+strings.Repeat("0", 64),
		"__METADATA_IMAGE__", "example.invalid/gcloud@sha256:"+strings.Repeat("1", 64),
		"__GCS_BUCKET__", "rhiza-qualification-fixture",
		"__GCS_PREFIX__", "qualification/"+runID+"/",
		"__NODE_A__", "node-a", "__NODE_B__", "node-b", "__NODE_C__", "node-c",
	).Replace(string(template))
	for _, placeholder := range []string{"__GCS_BUCKET__", "__GCS_PREFIX__"} {
		if strings.Contains(workloadRendered, placeholder) {
			t.Fatalf("rendered workload retained placeholder %s", placeholder)
		}
	}

	for name, rendered := range map[string][]byte{"auth": authRendered, "workload": []byte(workloadRendered)} {
		t.Run(name, func(t *testing.T) {
			decoder := yaml.NewDecoder(bytes.NewReader(rendered))
			serviceSelectors := 0
			for document := 1; ; document++ {
				var value any
				if err := decoder.Decode(&value); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatalf("document %d YAML: %v", document, err)
				}
				if value == nil {
					continue
				}
				if _, err := json.Marshal(value); err != nil {
					t.Fatalf("document %d is not Kubernetes JSON-compatible: %v", document, err)
				}
				assertStringMaps(t, value, fmt.Sprintf("document %d", document))
				object := value.(map[string]any)
				if object["kind"] == "Service" {
					selector := object["spec"].(map[string]any)["selector"].(map[string]any)
					for key, selectorValue := range selector {
						if _, ok := selectorValue.(string); !ok {
							t.Fatalf("document %d Service spec.selector.%s is %T, want string", document, key, selectorValue)
						}
					}
					serviceSelectors++
				}
			}
			if name == "workload" && serviceSelectors != 2 {
				t.Fatalf("checked %d Service selectors, want 2", serviceSelectors)
			}
		})
	}
}

func assertStringMaps(t *testing.T, value any, path string) {
	t.Helper()
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			childPath := path + "." + key
			switch key {
			case "labels", "annotations", "matchLabels", "labelSelectors":
				entries, ok := child.(map[string]any)
				if !ok {
					t.Fatalf("%s is %T, want string map", childPath, child)
				}
				for label, labelValue := range entries {
					if _, ok := labelValue.(string); !ok {
						t.Fatalf("%s.%s is %T (%v), want string", childPath, label, labelValue, labelValue)
					}
				}
			}
			assertStringMaps(t, child, childPath)
		}
	case []any:
		for index, child := range value {
			assertStringMaps(t, child, fmt.Sprintf("%s[%d]", path, index))
		}
	}
}

func TestBootstrapCustomRoleDeleteReceiptIsStructuredAndValidated(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		`record "$tag-delete" g iam roles delete "$role" --format=json`,
		`.name==$created[0].name and .name==$current[0].name`,
		`.description==$owner and .deleted==true`,
		`Custom role delete receipt rejected`,
		`record data-delete g iam service-accounts delete "$data_uid"`,
		`wait_deleted_sa data`,
		`GSA deletion identity drift`,
		`ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing structured custom-role rollback check %q", required)
		}
	}
}

func TestBootstrapRollbackKeepsAuthorityThroughCloudVerification(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	last := -1
	for _, step := range []string{
		`verify_absent folder g storage managed-folders describe`,
		`verify_absent provider g iam workload-identity-pools providers describe`,
		`verify_absent pool g iam workload-identity-pools describe`,
		`verify_absent ci g iam service-accounts describe`,
		`record data-delete g iam service-accounts delete "$data_uid"`,
		`wait_deleted_sa data`,
		`verify_deleted_role "$tag" "$role"`,
		`record project-rollback g projects get-iam-policy`,
		`record revoke-project g projects remove-iam-policy-binding`,
		`tag='cluster-role'; role=$cluster_role`,
		`Custom role delete receipt rejected`,
		`verify_deleted_role "$tag" "$role"`,
		`> "$RHIZA_AUTH_STATE/rollback-terminal.json"`,
	} {
		at := strings.Index(text[last+1:], step)
		if at < 0 {
			t.Fatalf("rollback step missing or out of order: %q", step)
		}
		last += at + 1
	}
	if strings.Contains(text, `verify_absent data g iam service-accounts describe`) {
		t.Fatal("redundant data GSA absence describe remains after authoritative worker")
	}
}

func TestBootstrapRollbackAbsenceStopsOnFirstUnprovenResult(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nprecise_not_found() {\n")
	end := strings.Index(string(source)[start+1:], "\nmark() {")
	if start < 0 || end < 0 {
		t.Fatal("rollback absence helper missing")
	}
	helper := string(source)[start+1 : start+1+end]
	for _, tc := range []struct {
		name, probe string
		want        int
	}{
		{"exact-not-found", `printf '%s\n' 'ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account.' >&2; return 7`, 0},
		{"not-found-with-auth-note", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent. This command is authenticated as [admin@example.invalid] which is the active account specified by the [core/account] property.' >&2; return 7`, 0},
		{"conflicting-success", `printf '%s\n' '{"name":"still-present"}'; return 0`, 1},
		{"mixed-conflict", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent; HTTP 409 conflict' >&2; return 7`, 1},
		{"mixed-permission", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent.' 'ERROR: PERMISSION_DENIED' >&2; return 7`, 1},
		{"mixed-server", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent; HTTP 503 backend unavailable' >&2; return 7`, 1},
		{"unknown", `printf '%s\n' 'ERROR: unknown transport failure' >&2; return 7`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command("/bin/sh", "-c", `set -eu
die() { printf '%s\n' "$*" >&2; exit 1; }
`+helper+`
probe() { `+tc.probe+`; }
verify_absent data probe
: > "$RHIZA_AUTH_STATE/after-first-failure"
`)
			command.Env = append(os.Environ(), "RHIZA_AUTH_STATE="+dir)
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) {
					t.Fatal(runErr)
				}
				code = exited.ExitCode()
			}
			if code != tc.want {
				t.Fatalf("exit=%d want=%d output=%s", code, tc.want, output)
			}
			_, markerErr := os.Stat(filepath.Join(dir, "after-first-failure"))
			if (markerErr == nil) != (tc.want == 0) {
				t.Fatalf("continued=%t want=%t error=%v", markerErr == nil, tc.want == 0, markerErr)
			}
			if tc.name == "mixed-permission" {
				if exit, err := os.ReadFile(filepath.Join(dir, "rollback-absence-data.exit")); err != nil || string(exit) != "7\n" {
					t.Fatalf("first failure receipt missing: exit=%q error=%v", exit, err)
				}
			}
		})
	}
}

func TestBootstrapClusterRoleTerminalVerification(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nprecise_not_found() {\n")
	end := strings.Index(string(source)[start+1:], "\nmark() {")
	if start < 0 || end < 0 {
		t.Fatal("rollback terminal role helpers missing")
	}
	helper := string(source)[start+1 : start+1+end]
	for _, tc := range []struct {
		name, response string
		want           int
	}{
		{"soft-deleted", `printf '%s\n' '{"name":"projects/p/roles/r","description":"owner","deleted":true,"includedPermissions":["p.one"],"stage":"GA"}'`, 0},
		{"exact-not-found", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent.' >&2; return 7`, 0},
		{"wrong-owner", `printf '%s\n' '{"name":"projects/p/roles/r","description":"other","deleted":true,"includedPermissions":["p.one"],"stage":"GA"}'`, 1},
		{"wrong-name", `printf '%s\n' '{"name":"projects/p/roles/other","description":"owner","deleted":true,"includedPermissions":["p.one"],"stage":"GA"}'`, 1},
		{"wrong-permissions", `printf '%s\n' '{"name":"projects/p/roles/r","description":"owner","deleted":true,"includedPermissions":["p.two"],"stage":"GA"}'`, 1},
		{"not-deleted", `printf '%s\n' '{"name":"projects/p/roles/r","description":"owner","deleted":false,"includedPermissions":["p.one"],"stage":"GA"}'`, 1},
		{"mixed-permission", `printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: Role absent. PERMISSION_DENIED' >&2; return 7`, 1},
		{"malformed", `printf '%s\n' '{'`, 1},
	} {
		for _, tag := range []string{"folder-role", "cluster-role"} {
			t.Run(tag+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, tag+"-create.json"), []byte(`{"name":"projects/p/roles/r","includedPermissions":["p.one"],"stage":"GA"}`), 0600); err != nil {
					t.Fatal(err)
				}
				command := exec.Command("/bin/sh", "-c", `set -eu
die() { printf '%s\n' "$*" >&2; exit 1; }
owner=owner
`+helper+`
g() {
	case "$*" in
		*--show-deleted*)
			printf '%s\n' 'ERROR: (gcloud.iam.roles.describe) unrecognized arguments: --show-deleted' >&2
			return 2
			;;
	esac
	`+tc.response+`
}
verify_deleted_role `+tag+` r
: > "$RHIZA_AUTH_STATE/after-terminal-check"
`)
				command.Env = append(os.Environ(), "RHIZA_AUTH_STATE="+dir)
				output, runErr := command.CombinedOutput()
				code := 0
				if runErr != nil {
					var exited *exec.ExitError
					if !errors.As(runErr, &exited) {
						t.Fatal(runErr)
					}
					code = exited.ExitCode()
				}
				if code != tc.want {
					t.Fatalf("exit=%d want=%d output=%s", code, tc.want, output)
				}
				_, markerErr := os.Stat(filepath.Join(dir, "after-terminal-check"))
				if (markerErr == nil) != (tc.want == 0) {
					t.Fatalf("continued=%t want=%t error=%v", markerErr == nil, tc.want == 0, markerErr)
				}
			})
		}
	}
}

func TestBootstrapGSADeleteWaitIsBoundedAndFailClosed(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(source, []byte(`timeout --kill-after=5s 415s /bin/sh "$script_dir/bootstrap-gcs-postrelease.sh"`)) {
		t.Fatal("GSA deletion wait lacks the 415-second worker plus five-second monotonic kill bound")
	}
	const runID, workflowSHA, expiry, admin = "a1b2c3d4", "0123456789abcdef0123456789abcdef01234567", "2099-01-01T00:00:00Z", "fixture-admin@example.invalid"
	hostImage := "ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64)
	owner := "rhiza-postrelease-" + runID + "-" + workflowSHA
	workerEnv := func(dir, scenario string) []string {
		state, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatal(err)
		}
		return append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario,
			"RHIZA_BOOTSTRAP_GO=I_APPROVE_NEW_SCOPE", "RHIZA_COST_CAP_KRW=10000", "RHIZA_EXPECTED_ADMIN_ACCOUNT="+admin,
			"RHIZA_AUTH_STATE="+state, "RHIZA_RUN_ID="+runID, "RHIZA_WORKFLOW_SHA="+workflowSHA, "RHIZA_AUTH_EXPIRES="+expiry,
			"RHIZA_HOST_IMAGE="+hostImage)
	}
	seedState := func(t *testing.T, dir string) {
		t.Helper()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{
			"attempts":         "0\n",
			"identity.json":    `{"run":"` + runID + `","workflow_sha":"` + workflowSHA + `","expiry":"` + expiry + `","cost_cap_krw":"10000","owner":"` + owner + `","host_image":"` + hostImage + `"}`,
			"data-create.json": `{"email":"rhiza-v0191-gcs-` + runID + `@example-project.iam.gserviceaccount.com","uniqueId":"fixture-uid","description":"` + owner + `"}`,
			"data.created":     owner + "\n",
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range []string{"kubectl", "curl", "timeout", "sha256sum", "gh"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, scenario := range []struct {
		name           string
		want, attempts int
	}{
		{"eventual-not-found", 0, 2},
		{"identity-drift", 1, 1},
		{"permission-denied", 1, 1},
		{"permission-denied-or-missing", 0, 2},
		{"list-present", 1, 2},
		{"list-failure", 1, 2},
		{"rate-limit", 1, 1},
		{"server-error", 1, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			gcloud := `#!/bin/sh
set -eu
case "$*" in
 *'config get-value account') printf '%s\n' 'fixture-admin@example.invalid'; exit 0 ;;
esac
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 printf '%s\n' "$*" >> "$FAKE_DIR/calls"
 case "$FAKE_SCENARIO" in
  eventual-not-found)
   if [ "$attempt" = 1 ]; then printf '%s\n' '{"email":"rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com","uniqueId":"fixture-uid","description":"rhiza-postrelease-a1b2c3d4-0123456789abcdef0123456789abcdef01234567"}'; exit 0; fi
   printf '%s\n' 'ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account. This command is authenticated as [fixture@example.invalid] which is the active account specified by the [core/account] property' >&2; exit 1 ;;
  identity-drift) printf '%s\n' '{"email":"other@example.invalid","uniqueId":"fixture-uid","description":"rhiza-postrelease-a1b2c3d4-0123456789abcdef0123456789abcdef01234567"}'; exit 0 ;;
  permission-denied) printf '%s\n' 'ERROR: PERMISSION_DENIED' >&2; exit 1 ;;
  rate-limit) printf '%s\n' 'ERROR: HTTPError 429' >&2; exit 1 ;;
  server-error) printf '%s\n' 'ERROR: HTTPError 503' >&2; exit 1 ;;
 esac
`
			listFallback := "\n case \"$*\" in\n  *'iam service-accounts list'*)\n   case \"$FAKE_SCENARIO\" in\n    permission-denied-or-missing) printf '[]\\n'; exit 0 ;;\n    list-present) printf '[{\"email\":\"rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\",\"uniqueId\":\"fixture-uid\"}]\\n'; exit 0 ;;\n    list-failure) printf 'ERROR: PERMISSION_DENIED\\n' >&2; exit 1 ;;\n   esac\n  ;;\n esac\n"
			gcloud = strings.Replace(gcloud, "\n case \"$FAKE_SCENARIO\" in", listFallback+"\n case \"$FAKE_SCENARIO\" in", 1)
			gcloud = strings.Replace(gcloud, "  permission-denied) printf '%s\\n' 'ERROR: PERMISSION_DENIED' >&2; exit 1 ;;", "  permission-denied) printf '%s\\n' 'ERROR: PERMISSION_DENIED' >&2; exit 1 ;;\n  permission-denied-or-missing|list-present|list-failure) printf '%s\\n' 'ERROR: (gcloud.iam.service-accounts.describe) PERMISSION_DENIED: Permission denied on resource (or it may not exist).' >&2; exit 1 ;;", 1)
			seedState(t, dir)
			if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "gsa-delete-wait-worker", "data")
			command.Env = workerEnv(dir, scenario.name)
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) {
					t.Fatal(runErr)
				}
				code = exited.ExitCode()
			}
			attempts, err := os.ReadFile(filepath.Join(dir, "attempts"))
			if err != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, err)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil || bytes.Contains(calls, []byte(" delete ")) || !bytes.Contains(calls, []byte("iam service-accounts describe rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com --format=json")) {
				t.Fatalf("worker issued a non-describe operation: %q error=%v", calls, err)
			}
		})
	}
	t.Run("persistent-exists-frozen-backward-clock", func(t *testing.T) {
		dir := t.TempDir()
		gcloud := "#!/bin/sh\ncase \"$*\" in *'config get-value account') printf '%s\\n' 'fixture-admin@example.invalid'; exit 0;; esac\nprintf '%s\\n' \"$*\" >> \"$FAKE_DIR/calls\"\nprintf '%s\\n' '{\"email\":\"rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\",\"uniqueId\":\"fixture-uid\",\"description\":\"" + owner + "\"}'\n"
		date := "#!/bin/sh\nvalue=$(cat \"$FAKE_DIR/clock\")\nprintf '%s\\n' \"$value\"\nprintf '%s\\n' \"$((value - 100))\" > \"$FAKE_DIR/clock\"\n"
		for name, content := range map[string]string{"gcloud": gcloud, "date": date} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
				t.Fatal(err)
			}
		}
		seedState(t, dir)
		if err := os.WriteFile(filepath.Join(dir, "clock"), []byte("1000\n"), 0600); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		command := exec.Command("timeout", "--kill-after=1s", "2s", "/bin/sh", "../bootstrap-gcs-postrelease.sh", "gsa-delete-wait-worker", "data")
		command.Env = workerEnv(dir, "persistent")
		runErr := command.Run()
		var exited *exec.ExitError
		if !errors.As(runErr, &exited) || exited.ExitCode() != 124 || time.Since(started) > 3*time.Second {
			t.Fatalf("outer watchdog failed: error=%v elapsed=%s", runErr, time.Since(started))
		}
		calls, err := os.ReadFile(filepath.Join(dir, "calls"))
		if err != nil || bytes.Contains(calls, []byte(" delete ")) || !bytes.Contains(calls, []byte("iam service-accounts describe")) {
			t.Fatalf("bounded worker issued a non-describe operation: %q error=%v", calls, err)
		}
	})
	for _, unsafe := range []struct {
		name string
		args []string
	}{
		{"path-tag", []string{"gsa-delete-wait-worker", "../outside"}},
		{"arbitrary-account", []string{"gsa-delete-wait-worker", "data", "attacker@example.invalid"}},
	} {
		t.Run("reject-"+unsafe.name, func(t *testing.T) {
			dir := t.TempDir()
			outside := filepath.Join(filepath.Dir(dir), "outside-write")
			gcloud := "#!/bin/sh\nprintf '%s\\n' called >> \"$FAKE_DIR/calls\"\nexit 99\n"
			if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", append([]string{"../bootstrap-gcs-postrelease.sh"}, unsafe.args...)...)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "RHIZA_AUTH_STATE="+outside,
				"RHIZA_GSA_DELETE_ACCOUNT=attacker@example.invalid")
			if err := command.Run(); err == nil {
				t.Fatal("unsafe direct worker invocation passed")
			}
			if _, err := os.Stat(filepath.Join(dir, "calls")); !os.IsNotExist(err) {
				t.Fatalf("unsafe invocation reached cloud command: %v", err)
			}
			if _, err := os.Stat(outside); !os.IsNotExist(err) {
				t.Fatalf("unsafe invocation wrote outside validated state: %v", err)
			}
		})
	}
}

func TestQualificationQUICHandshake(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("test identity unavailable")
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Unix(0, 0), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal("test certificate unavailable")
	}
	// Match PeerServer's early listener: the probe closes immediately after its
	// client handshake, before a normal listener may enqueue the connection.
	listener, err := quic.ListenAddrEarly("127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{sqlpolicy.PeerALPN}, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}, nil)
	if err != nil {
		t.Fatal("test QUIC listener unavailable")
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type observation struct {
		stage      string
		err        error
		contextErr error
	}
	streamResult := make(chan observation, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			streamResult <- observation{"accept", err, ctx.Err()}
			return
		}
		_, err = conn.AcceptStream(ctx)
		streamResult <- observation{"stream", err, ctx.Err()}
	}()
	var pinned rhiza.PublicKey
	copy(pinned[:], public)
	if probeQUICHandshake(ctx, listener.Addr().String(), "rhiza-voter-1", pinned) != nil {
		t.Fatal("pinned handshake failed")
	}
	observed := <-streamResult
	if observed.stage != "stream" || observed.err == nil {
		t.Fatalf("probe observation: stage=%s errType=%T contextErr=%v deadline=%t", observed.stage, observed.err, observed.contextErr, errors.Is(observed.err, context.DeadlineExceeded))
	}
	var closed *quic.ApplicationError
	var transport *quic.TransportError
	applicationClose := errors.As(observed.err, &closed) && closed.Remote && closed.ErrorCode == 0 && closed.ErrorMessage == "qualification handshake only"
	// QUIC maps application close to transport APPLICATION_ERROR in Initial
	// or Handshake packets (quic-go packet_packer.go). Neither permits a stream;
	// arbitrary transport errors and context timeouts must still fail here.
	handshakeClose := errors.As(observed.err, &transport) && transport.Remote && transport.ErrorCode == quic.ApplicationErrorErrorCode && transport.FrameType == 0 && transport.ErrorMessage == ""
	if !applicationClose && !handshakeClose {
		t.Fatalf("probe did not close without a stream: errType=%T contextErr=%v", observed.err, observed.contextErr)
	}
	t.Logf("no-stream close observed: application=%t handshakeTransport=%t contextErr=%v", applicationClose, handshakeClose, observed.contextErr)
	pinned[0] ^= 1
	if err := probeQUICHandshake(ctx, listener.Addr().String(), "rhiza-voter-1", pinned); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("certificate mismatch was not rejected distinctly")
	}
	// An owned black-hole UDP socket proves deadline classification locally;
	// it does NOT constitute Kubernetes NetworkPolicy evidence.
	socket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("test UDP socket unavailable")
	}
	defer socket.Close()
	blocked, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if err := probeQUICHandshake(blocked, socket.LocalAddr().String(), "rhiza-voter-1", pinned); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked probe did not return context deadline")
	}
}

func TestQualificationFixture(t *testing.T) {
	run := "a1b2c3d4"
	namespace := "rhiza-v0191-" + run
	owner := "rhiza-postrelease-" + run + "-" + strings.Repeat("a", 40)
	// Synthetic deterministic entropy only; no generated live credentials/logs.
	entropy := make([]byte, 160)
	for i := range entropy {
		entropy[i] = byte(i)
	}
	fixture, err := qualificationFixture(namespace, run, owner, bytes.NewReader(entropy))
	if err != nil {
		t.Fatal("valid fixture failed")
	}
	data := fixture["stringData"].(map[string]string)
	if len(data) != 5 || fixture["immutable"] != true || fixture["kind"] != "Secret" || fixture["type"] != "Opaque" {
		t.Fatal("fixture schema mismatch")
	}
	var peers map[string]string
	var members []rhiza.Member
	var learner rhiza.Member
	if json.Unmarshal([]byte(data["RHIZA_PEER_TOKENS"]), &peers) != nil || json.Unmarshal([]byte(data["RHIZA_CLUSTER_MEMBERS"]), &members) != nil || json.Unmarshal([]byte(data["learner_member"]), &learner) != nil {
		t.Fatal("fixture configuration invalid")
	}
	seen := map[string]bool{data["RHIZA_ADMIN_TOKEN"]: true, data["learner_token"]: true}
	if len(members) != 3 || len(peers) != 3 || len(seen) != 2 {
		t.Fatal("fixture cardinality mismatch")
	}
	for _, member := range members {
		id := string(member.ID)
		token := peers[id]
		if len(token) != 64 || seen[token] || member.PublicKey != rhiza.PeerPublicKey("gcs-"+run, id, token) || member.URL != "http://"+id+".rhiza-peers:8080" || member.PeerURL != "quic://"+id+".rhiza-peers:9090" {
			t.Fatal("voter fixture/KDF mismatch")
		}
		seen[token] = true
	}
	if learner.ID != "rhiza-learner" || learner.URL != "http://rhiza-learner:8080" || learner.PeerURL != "quic://rhiza-learner:9090" || learner.PublicKey != rhiza.PeerPublicKey("gcs-"+run, string(learner.ID), data["learner_token"]) {
		t.Fatal("learner fixture/KDF mismatch")
	}
	metadata := fixture["metadata"].(map[string]any)
	if metadata["namespace"] != namespace || metadata["annotations"].(map[string]string)["rhiza.dev/auth-owner"] != owner || metadata["labels"].(map[string]string)["chaos.rhiza.io/run"] != run {
		t.Fatal("fixture ownership mismatch")
	}
	t.Setenv("RHIZA_CLUSTER_ID", "gcs-"+run)
	t.Setenv("RHIZA_NODE_ID", "rhiza-voter-0")
	t.Setenv("RHIZA_PEER_TOKEN", "")
	t.Setenv("RHIZA_LEARNER", "")
	t.Setenv("RHIZA_CLUSTER_MEMBERS", data["RHIZA_CLUSTER_MEMBERS"])
	t.Setenv("RHIZA_PEER_TOKENS", data["RHIZA_PEER_TOKENS"])
	config, err := rhiza.ConfigFromEnv()
	if err != nil || config.PeerToken != peers["rhiza-voter-0"] || len(config.Members) != 3 {
		t.Fatal("voter fixture rejected by released config API")
	}
	t.Setenv("RHIZA_NODE_ID", "rhiza-learner")
	t.Setenv("RHIZA_PEER_TOKENS", "")
	t.Setenv("RHIZA_PEER_TOKEN", data["learner_token"])
	t.Setenv("RHIZA_LEARNER", data["learner_member"])
	config, err = rhiza.ConfigFromEnv()
	if err != nil || config.PeerToken != data["learner_token"] || config.Learner == nil || config.Learner.PublicKey != learner.PublicKey {
		t.Fatal("learner fixture rejected by released config API")
	}
	for _, args := range [][3]string{
		{"wrong", run, owner},
		{"rhiza-v0190-" + run, run, owner},
		{"rhiza-v0191-20261008-" + run, run, owner},
		{"rhiza-v0191-b1c2d3e4", run, owner},
		{namespace, "BAD", owner},
		{namespace, run, "wrong"},
	} {
		if _, err := qualificationFixture(args[0], args[1], args[2], bytes.NewReader(entropy)); err == nil {
			t.Fatal("invalid identity accepted")
		}
	}
	if _, err := qualificationFixture(namespace, run, owner, bytes.NewReader(nil)); err == nil {
		t.Fatal("missing entropy accepted")
	}
	if _, err := qualificationFixture(namespace, run, owner, bytes.NewReader(make([]byte, 160))); err == nil {
		t.Fatal("repeated entropy accepted")
	}
}

func TestPostreleaseCustomRoleReadiness(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nnow_epoch() {")
	end := strings.Index(string(source), "\nlocal_token_files() {")
	if start < 0 || end < 0 {
		t.Fatal("custom role readiness helpers missing")
	}
	if strings.Count(string(source), "custom_role_deadline=$(($(now_epoch) + 420))") != 1 {
		t.Fatal("single exact 420-second custom role deadline missing")
	}
	helpers := string(source[start:end])
	for _, scenario := range []struct {
		name           string
		deadline, want int
		attempts       int
	}{
		{"transient", 1420, 0, 2},
		{"deleted-omitted", 1420, 0, 1},
		{"forbidden", 1420, 1, 1},
		{"deleted", 1420, 1, 1},
		{"deleted-null", 1420, 1, 1},
		{"deleted-string", 1420, 1, 1},
		{"wrong-name", 1420, 1, 1},
		{"missing-etag", 1420, 1, 1},
		{"wrong-description", 1420, 1, 1},
		{"wrong-stage", 1420, 1, 1},
		{"wrong-permissions", 1420, 1, 1},
		{"deadline", 1004, 1, 1},
		{"inflight-timeout", 1420, 1, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, value := range map[string]string{"clock": "1000\n", "attempts": "0\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			fixture := `
set -eu
project=fixture-project; owner=fixture-owner; RHIZA_AUTH_STATE=$FAKE_DIR
now_epoch() { cat "$FAKE_DIR/clock"; }
sleep() { now=$(now_epoch); printf '%s\n' "$((now + $1))" > "$FAKE_DIR/clock"; printf '%s\n' "$1" >> "$FAKE_DIR/sleeps"; }
die() { printf '%s\n' "$*" >&2; return 1; }
timeout() {
 [ "$1" = --signal=KILL ] || return 98
 seconds=${2%s}; shift 2; printf '%s\n' "$seconds" >> "$FAKE_DIR/timeouts"
 [ "$FAKE_SCENARIO" != inflight-timeout ] || return 124
 "$@"
}
gcloud() {
 [ "$1" = --project=fixture-project ] && [ "$2" = --quiet ] && [ "$3 $4 $5 $6 $7" = 'iam roles describe fixture-role --format=json' ] || return 97
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 case "$FAKE_SCENARIO:$attempt" in transient:1|deadline:1) printf 'NOT_FOUND\n' >&2; return 1;; forbidden:1) printf 'PERMISSION_DENIED\n' >&2; return 1;; esac
 name=projects/fixture-project/roles/fixture-role; etag=etag; description=fixture-owner; stage=GA; deleted=false; permissions='["p.one","p.two"]'
 case "$FAKE_SCENARIO" in deleted) deleted=true;; wrong-name) name=projects/fixture-project/roles/other;; missing-etag) etag=;; wrong-description) description=other;; wrong-stage) stage=BETA;; wrong-permissions) permissions='["p.one"]';; esac
 jq -n --arg scenario "$FAKE_SCENARIO" --arg name "$name" --arg etag "$etag" --arg description "$description" --arg stage "$stage" --argjson deleted "$deleted" --argjson permissions "$permissions" '
   {name:$name,etag:$etag,description:$description,stage:$stage,deleted:$deleted,includedPermissions:$permissions} |
   if $scenario=="deleted-omitted" then del(.deleted) elif $scenario=="deleted-null" then .deleted=null elif $scenario=="deleted-string" then .deleted="false" else . end'
}
`
			command := exec.Command("/bin/sh", "-c", helpers+fixture+"\nwait_custom_role fixture fixture-role p.two,p.one "+strconv.Itoa(scenario.deadline))
			command.Env = append(os.Environ(), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name)
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) {
					t.Fatal(runErr)
				}
				code = exited.ExitCode()
			}
			attempts, readErr := os.ReadFile(filepath.Join(dir, "attempts"))
			if readErr != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, readErr)
			}
			timeouts, timeoutErr := os.ReadFile(filepath.Join(dir, "timeouts"))
			if timeoutErr != nil {
				t.Fatal(timeoutErr)
			}
			firstTimeout := strings.Split(strings.TrimSpace(string(timeouts)), "\n")[0]
			wantTimeout := "420"
			if scenario.name == "deadline" {
				wantTimeout = "4"
			}
			if firstTimeout != wantTimeout {
				t.Fatalf("bounded timeout=%s want=%s", firstTimeout, wantTimeout)
			}
			for attempt := 1; attempt <= scenario.attempts; attempt++ {
				for _, suffix := range []string{"json", "stderr", "exit"} {
					if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("fixture-readiness-%d.%s", attempt, suffix))); err != nil {
						t.Fatalf("attempt evidence missing: %v", err)
					}
				}
			}
		})
	}
}

func TestPostreleaseSHA256ToolFallback(t *testing.T) {
	for _, script := range []string{"../bootstrap-gcs-postrelease.sh", "../run-gcs-postrelease.sh"} {
		source, err := os.ReadFile(script)
		if err != nil {
			t.Fatal(err)
		}
		start := strings.Index(string(source), "\nsha256_file() {\n")
		end := strings.Index(string(source)[start+1:], "\n}\n")
		if start < 0 || end < 0 {
			t.Fatalf("%s: SHA-256 helper missing", script)
		}
		helper := string(source)[start+1 : start+1+end+3]
		for _, scenario := range []string{"sha256sum-only", "shasum-only", "neither", "malformed"} {
			t.Run(filepath.Base(script)+"/"+scenario, func(t *testing.T) {
				dir := t.TempDir()
				tool := "sha256sum"
				body := "printf '%064d  %s\\n' 0 \"$1\"\n"
				if scenario == "shasum-only" {
					tool = "shasum"
					body = "[ \"$1 $2\" = '-a 256' ] || exit 9\nprintf '%064d  %s\\n' 0 \"$3\"\n"
				} else if scenario == "malformed" {
					body = "printf 'not-a-digest  %s\\n' \"$1\"\n"
				}
				if scenario != "neither" {
					if err := os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"+body), 0700); err != nil {
						t.Fatal(err)
					}
				}
				command := exec.Command("/bin/sh", "-c", helper+"\nsha256_file fixture")
				command.Env = []string{"PATH=" + dir}
				output, err := command.CombinedOutput()
				wantSuccess := scenario == "sha256sum-only" || scenario == "shasum-only"
				if (err == nil) != wantSuccess {
					t.Fatalf("success=%v output=%q", err == nil, output)
				}
				if wantSuccess && string(output) != strings.Repeat("0", 64)+"\n" {
					t.Fatalf("unexpected digest %q", output)
				}
			})
		}
	}
}

func TestQualificationCurlHelper(t *testing.T) {
	if os.Getenv("RHIZA_CURL_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	if os.Getenv("FAKE_SCENARIO") != "foreign-healthy-listener" {
		if _, err := os.Stat(filepath.Join(os.Getenv("FAKE_DIR"), "listening")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, arg := range os.Args {
		if !strings.HasPrefix(arg, "http://") {
			continue
		}
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(arg)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("health status=%d", response.StatusCode)
		}
		if err := os.WriteFile(filepath.Join(os.Getenv("FAKE_DIR"), "health-ok"), []byte("ok\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("health URL missing")
}

func TestQualificationArchiveHeadEvidence(t *testing.T) {
	const headerSize = 128
	rootHash := sha256.Sum256([]byte("root"))
	stateHash := sha256.Sum256([]byte("state"))
	prefixHash := sha256.Sum256([]byte("prefix"))
	value, err := quepaxa.EncodeCheckpointSeal(quepaxa.CheckpointSeal{ConfigID: 1, Index: 7,
		RootHash: rootHash, StateHash: stateHash, PrefixHash: prefixHash, NextLeaderOrder: []quepaxa.NodeID{"n1"}})
	if err != nil {
		t.Fatal(err)
	}
	certificate := []byte(`{"proof":"fixture"}`)
	payload := make([]byte, 16+len(value)+len(certificate))
	binary.BigEndian.PutUint64(payload[:8], 7)
	binary.BigEndian.PutUint32(payload[8:12], uint32(len(value)))
	binary.BigEndian.PutUint32(payload[12:16], uint32(len(certificate)))
	copy(payload[16:], value)
	copy(payload[16+len(value):], certificate)
	data := make([]byte, headerSize+len(payload)+4)
	copy(data[:8], "RHZAHD2!")
	binary.BigEndian.PutUint32(data[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(data[12:16], 1|2) // regular sealed checkpoint head
	binary.BigEndian.PutUint64(data[16:24], 1)
	binary.BigEndian.PutUint64(data[24:32], 2)
	binary.BigEndian.PutUint64(data[32:40], 7)
	copy(data[40:72], prefixHash[:])
	binary.BigEndian.PutUint64(data[72:80], 14)
	copy(data[80:112], bytes.Repeat([]byte{0x22}, sha256.Size))
	binary.BigEndian.PutUint64(data[112:120], 1)
	binary.BigEndian.PutUint32(data[124:128], uint32(len(payload)))
	copy(data[128:], payload)
	binary.BigEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], crc32.MakeTable(crc32.Castagnoli)))

	head, err := qualificationArchiveHead(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfigID != 1 || !head.RegularCheckpoint || head.Base != 7 || head.BaseRootHash != fmt.Sprintf("%x", rootHash) ||
		head.BaseStateHash != fmt.Sprintf("%x", stateHash) || head.BasePrefixHash != fmt.Sprintf("%x", prefixHash) ||
		head.Tip != 14 || head.TailObject != 1 || head.TailHash != [32]byte(bytes.Repeat([]byte{0x22}, sha256.Size)) {
		t.Fatalf("unexpected archive head evidence: %+v", head)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"malformed", []byte("not-a-head")},
		{"truncated", data[:len(data)-1]},
		{"checksum", append(append([]byte(nil), data[:128]...), append([]byte{0x5b}, data[129:]...)...)},
		{"unsealed", func() []byte {
			bad := append([]byte(nil), data...)
			binary.BigEndian.PutUint32(bad[12:16], 4)
			binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crc32.MakeTable(crc32.Castagnoli)))
			return bad
		}()},
		{"anchor-only", func() []byte {
			anchor := bytes.Repeat([]byte{0x5a}, sha256.Size)
			bad := make([]byte, headerSize+len(anchor)+4)
			copy(bad, data[:headerSize])
			binary.BigEndian.PutUint32(bad[8:12], uint32(len(bad)))
			binary.BigEndian.PutUint32(bad[12:16], 2|4)
			binary.BigEndian.PutUint32(bad[124:128], uint32(len(anchor)))
			copy(bad[128:], anchor)
			binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crc32.MakeTable(crc32.Castagnoli)))
			return bad
		}()},
		{"decision-slot-mismatch", func() []byte {
			bad := append([]byte(nil), data...)
			binary.BigEndian.PutUint64(bad[128:136], 8)
			binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crc32.MakeTable(crc32.Castagnoli)))
			return bad
		}()},
		{"config-mismatch", func() []byte {
			bad := append([]byte(nil), data...)
			binary.BigEndian.PutUint64(bad[16:24], 2)
			binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crc32.MakeTable(crc32.Castagnoli)))
			return bad
		}()},
		{"prefix-mismatch", func() []byte {
			bad := append([]byte(nil), data...)
			bad[40] ^= 0xff
			binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crc32.MakeTable(crc32.Castagnoli)))
			return bad
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := qualificationArchiveHead(bytes.NewReader(tc.data)); err == nil {
				t.Fatal("invalid archive head accepted")
			}
		})
	}
}

func TestQualificationArchiveHeadAndPortForwardLifecycle(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	for _, forbidden := range []string{"archive/HEAD", "HEAD.json"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("binary archive head regressed to JSON/uppercase lookup: %s", forbidden)
		}
	}
	metadata := `gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'`
	metadataAt, learnerAt := strings.Index(source, metadata), strings.Index(source, "run cold-create")
	readbackAt, metadataAfterAt := strings.Index(source, "run learner-local"), strings.Index(source, "run head-metadata-after")
	if metadataAt < 0 || learnerAt <= metadataAt || readbackAt <= learnerAt || metadataAfterAt <= readbackAt {
		t.Fatal("head.bin metadata proof or required fresh-learner semantic proof missing")
	}
	validationStart := strings.Index(source, "\nvalidate_archive_head_metadata() {\n")
	validationEnd := strings.Index(source, "\nprivate_file() {\n")
	if validationStart < 0 || validationEnd <= validationStart {
		t.Fatal("archive head metadata validators missing")
	}
	validators := "sha256_file() { if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{print $1}'; else shasum -a 256 \"$1\" | awk '{print $1}'; fi; }\n" + source[validationStart:validationEnd]
	metadataDir := t.TempDir()
	valid := `{"bucket":"rhiza-qualification-fixture","generation":"1","name":"prefix/cluster/archive/head.bin","size":"132"}`
	metadataCases := []struct {
		name, value string
		valid       bool
	}{
		{"valid-string-size", valid, true},
		{"wrong-bucket", strings.Replace(valid, "rhiza-qualification-fixture", "other", 1), false},
		{"wrong-name", strings.Replace(valid, "prefix/cluster/archive/head.bin", "prefix/cluster/archive/HEAD", 1), false},
		{"numeric-generation", strings.Replace(valid, `"generation":"1"`, `"generation":1`, 1), false},
		{"leading-zero-generation", strings.Replace(valid, `"generation":"1"`, `"generation":"01"`, 1), false},
		{"valid-numeric-size", strings.Replace(valid, `"size":"132"`, `"size":132`, 1), true},
		{"valid-decimal-numeric-size", strings.Replace(valid, `"size":"132"`, `"size":132.0`, 1), true},
		{"valid-exponent-numeric-size", strings.Replace(valid, `"size":"132"`, `"size":132e0`, 1), true},
		{"valid-fractional-exponent-numeric-size", strings.Replace(valid, `"size":"132"`, `"size":1.32e2`, 1), true},
		{"leading-zero-size", strings.Replace(valid, `"size":"132"`, `"size":"0132"`, 1), false},
		{"fractional-size", strings.Replace(valid, `"size":"132"`, `"size":132.5`, 1), false},
		{"negative-size", strings.Replace(valid, `"size":"132"`, `"size":-132`, 1), false},
		{"null-size", strings.Replace(valid, `"size":"132"`, `"size":null`, 1), false},
		{"overflow-size", strings.Replace(valid, `"size":"132"`, `"size":1e100`, 1), false},
		{"small", strings.Replace(valid, `"size":"132"`, `"size":"131"`, 1), false},
		{"small-numeric", strings.Replace(valid, `"size":"132"`, `"size":131`, 1), false},
		{"large", strings.Replace(valid, `"size":"132"`, `"size":"8388633"`, 1), false},
		{"large-numeric", strings.Replace(valid, `"size":"132"`, `"size":8388633`, 1), false},
		{"extra-field", strings.TrimSuffix(valid, "}") + `,"etag":"secret"}`, false},
	}
	for _, tc := range metadataCases {
		t.Run("metadata-"+tc.name, func(t *testing.T) {
			path := filepath.Join(metadataDir, tc.name+".json")
			if err := os.WriteFile(path, []byte(tc.value), 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "-c", "prefix=prefix/; cluster=cluster; "+validators+"\nvalidate_archive_head_metadata \"$1\"", "fixture", path)
			err := command.Run()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
		})
	}
	before, after := filepath.Join(metadataDir, "before.json"), filepath.Join(metadataDir, "after.json")
	if err := os.WriteFile(before, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{{"same", valid, true}, {"same-numeric-size", strings.Replace(valid, `"size":"132"`, `"size":1.32e2`, 1), true}, {"generation-change", strings.Replace(valid, `"generation":"1"`, `"generation":"2"`, 1), false}, {"size-change", strings.Replace(valid, `"size":"132"`, `"size":"133"`, 1), false}} {
		if err := os.WriteFile(after, []byte(tc.value), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nsame_archive_head_metadata \"$1\" \"$2\"", "fixture", before, after)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("metadata comparison %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
	checkpoint := strings.Replace(valid, "archive/head.bin", "checkpoint/CURRENT", 1)
	if err := os.WriteFile(after, []byte(checkpoint), 0600); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("/bin/sh", "-c", "prefix=prefix/; cluster=cluster; "+validators+"\nvalidate_checkpoint_current_metadata \"$1\"", "fixture", after).Run(); err != nil {
		t.Fatalf("valid checkpoint metadata rejected: %v", err)
	}
	digestBefore, digestAfter := filepath.Join(metadataDir, "before.sha256"), filepath.Join(metadataDir, "after.sha256")
	if err := os.WriteFile(digestBefore, []byte("same\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, digest string
		valid        bool
	}{{"same", "same\n", true}, {"digest-change", "changed\n", false}} {
		if err := os.WriteFile(digestAfter, []byte(tc.digest), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nsame_object_evidence \"$1\" \"$2\" \"$3\" \"$4\"", "fixture", before, before, digestBefore, digestAfter)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("object evidence %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
	sealPath := filepath.Join(metadataDir, "seal.json")
	currentPath := filepath.Join(metadataDir, "current.json")
	sealRoot := strings.Repeat("a", 64)
	sealState := strings.Repeat("b", 64)
	if err := os.WriteFile(sealPath, []byte(fmt.Sprintf(`{"config_id":1,"base":17,"base_root_hash":%q,"base_state_hash":%q,"base_prefix_hash":%q,"regular_checkpoint":true,"tip":19}`, sealRoot, sealState, strings.Repeat("c", 64))), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, current string
		valid         bool
	}{
		{"older-current", `{"index":15,"root_hash":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}`, true},
		{"same-current", fmt.Sprintf(`{"index":17,"root_hash":%q}`, sealRoot), true},
		{"ahead-current", fmt.Sprintf(`{"index":18,"root_hash":%q}`, sealRoot), false},
		{"same-index-conflict", `{"index":17,"root_hash":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}`, false},
	} {
		if err := os.WriteFile(currentPath, []byte(tc.current), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nvalidate_current_against_archive \"$1\" \"$2\"", "fixture", currentPath, sealPath)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("CURRENT relation %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
	rootPath := filepath.Join(metadataDir, "root.json")
	stateBytes := "[" + strings.TrimSuffix(strings.Repeat("187,", 32), ",") + "]"
	validRoot := fmt.Sprintf(`{"sql_execution_policy":1,"config_id":1,"generation":1,"index":17,"hash":%s,"size":8,"files":[{"role":"sqlite","hash":"%s","size":8,"blocks":[{"hash":"%s","generation":1,"size":8}]}]}`, stateBytes, strings.Repeat("d", 64), strings.Repeat("e", 64))
	// 0xbb repeated is the hex state hash in the frozen seal fixture.
	for _, tc := range []struct {
		name, root string
		valid      bool
	}{
		{"matching-root", validRoot, true},
		{"wrong-config", strings.Replace(validRoot, `"config_id":1`, `"config_id":2`, 1), false},
		{"wrong-index", strings.Replace(validRoot, `"index":17`, `"index":16`, 1), false},
		{"wrong-state", strings.Replace(validRoot, "187,187", "186,187", 1), false},
		{"malformed-state", strings.Replace(validRoot, stateBytes, `[187]`, 1), false},
	} {
		if err := os.WriteFile(rootPath, []byte(tc.root), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nvalidate_checkpoint_root_against_seal \"$1\" \"$2\"", "fixture", rootPath, sealPath)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("archive root %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}

	checkpointCurrent := filepath.Join(metadataDir, "archive-head.json")
	archiveMetadata := filepath.Join(metadataDir, "archive.json")
	countersPath := filepath.Join(metadataDir, "counters.json")
	statusPath := filepath.Join(metadataDir, "status.json")
	rootHash := strings.Repeat("a", 64)
	validStatus := fmt.Sprintf(`{"mode":"object-store","source":"object-store","lag_slots":0,"applied_slot":19,"source_tip":19,"checkpoint_base_slot":17,"checkpoint_root_hash":"%s","checkpoint_state_hash":"%s","checkpoint_prefix_hash":"%s","archive_head_version_type":"generation","archive_head_version":"1","recovery_pin_id":"%s","recovery_pins_created":2,"recovery_pins_released":2}`, rootHash, strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64))
	for path, value := range map[string]string{
		checkpointCurrent: fmt.Sprintf(`{"config_id":1,"base":17,"base_root_hash":"%s","base_state_hash":"%s","base_prefix_hash":"%s","regular_checkpoint":true,"tip":19,"tail_hash":[],"tail_object":1}`, rootHash, strings.Repeat("b", 64), strings.Repeat("c", 64)),
		archiveMetadata:   valid,
		countersPath:      `{"uploads":4,"deletes":0,"failures":0}`,
	} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"certified-base", validStatus, true},
		{"archive-only-missing-base", strings.Replace(validStatus, `,"checkpoint_base_slot":17,"checkpoint_root_hash":"`+rootHash+`"`, "", 1), false},
		{"old-current-tip-regression", strings.Replace(validStatus, `"applied_slot":19,"source_tip":19`, `"applied_slot":15,"source_tip":15`, 1), false},
		{"base-mismatch", strings.Replace(validStatus, `"checkpoint_root_hash":"`+rootHash+`"`, `"checkpoint_root_hash":"`+strings.Repeat("e", 64)+`"`, 1), false},
		{"head-generation-mismatch", strings.Replace(validStatus, `"archive_head_version":"1"`, `"archive_head_version":"2"`, 1), false},
		{"checkpoint-pin-missing", strings.Replace(validStatus, `"recovery_pins_created":2`, `"recovery_pins_created":1`, 1), false},
	} {
		if err := os.WriteFile(statusPath, []byte(tc.value), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nvalidate_cold_restore_status \"$1\" \"$2\" \"$3\" \"$4\"", "fixture", statusPath, checkpointCurrent, archiveMetadata, countersPath)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("cold status %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}
	if err := os.WriteFile(statusPath, []byte(validStatus), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"exact-pin-uploads", `{"uploads":4,"deletes":0}`, true},
		{"retry-upload", `{"uploads":5,"deletes":0}`, true},
		{"insufficient-uploads", `{"uploads":3,"deletes":0}`, false},
		{"unexpected-delete", `{"uploads":4,"deletes":1}`, false},
	} {
		if err := os.WriteFile(countersPath, []byte(tc.value), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nvalidate_cold_restore_status \"$1\" \"$2\" \"$3\" \"$4\"", "fixture", statusPath, checkpointCurrent, archiveMetadata, countersPath)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("cold counters %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}

	inventoryBefore := filepath.Join(metadataDir, "inventory-before.json")
	inventoryAfter := filepath.Join(metadataDir, "inventory-after.json")
	owner := "replica-rhiza-learner-0123456789abcdef"
	pinID := fmt.Sprintf("%x", sha256.Sum256([]byte(owner)))
	checkpointRoot := `{"config_id":1,"index":7,"state_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","files":[]}`
	terminalRootHash := fmt.Sprintf("%x", sha256.Sum256([]byte(checkpointRoot)))
	terminalCurrent := fmt.Sprintf(`{"index":7,"root_hash":%q}`, terminalRootHash)
	archiveHead := fmt.Sprintf(`{"config_id":1,"base":7,"base_root_hash":%q,"base_state_hash":%q,"base_prefix_hash":%q,"regular_checkpoint":true,"tip":14,"tail_hash":%s,"tail_object":1}`, terminalRootHash, strings.Repeat("b", 64), strings.Repeat("c", 64), "["+strings.TrimSuffix(strings.Repeat("1,", 32), ",")+"]")
	baseObjects := fmt.Sprintf(`[{"name":"prefix/cluster/archive/GC_LOCK","generation":"1","size":96},{"name":"prefix/cluster/archive/PUBLISH_LOCK","generation":"2","size":96},{"name":"prefix/cluster/archive/blocks/0001","generation":"3","size":256},{"name":"prefix/cluster/archive/head.bin","generation":"4","size":132},{"name":"prefix/cluster/archive/roots/root","generation":"5","size":256},{"name":"prefix/cluster/checkpoint/CURRENT","generation":"6","size":96},{"name":"prefix/cluster/checkpoint/PUBLISHER","generation":"7","size":128},{"name":"prefix/cluster/checkpoint/blocks/0001","generation":"8","size":256},{"name":"prefix/cluster/checkpoint/roots/00000000000000000007_%s.json","generation":"9","size":256}]`, terminalRootHash)
	terminalObjects := fmt.Sprintf(`[{"name":"prefix/cluster/archive/GC_LOCK","generation":"10","size":120},{"name":"prefix/cluster/archive/PUBLISH_LOCK","generation":"2","size":96},{"name":"prefix/cluster/archive/blocks/0001","generation":"3","size":256},{"name":"prefix/cluster/archive/head.bin","generation":"4","size":132},{"name":"prefix/cluster/archive/recovery-pins/%s","generation":"11","size":512},{"name":"prefix/cluster/archive/roots/root","generation":"5","size":256},{"name":"prefix/cluster/checkpoint/CURRENT","generation":"6","size":96},{"name":"prefix/cluster/checkpoint/PUBLISHER","generation":"12","size":160},{"name":"prefix/cluster/checkpoint/blocks/0001","generation":"8","size":256},{"name":"prefix/cluster/checkpoint/recovery-pins/%s","generation":"13","size":1024},{"name":"prefix/cluster/checkpoint/roots/00000000000000000007_%s.json","generation":"9","size":256}]`, pinID, pinID, terminalRootHash)
	terminalStatus := strings.Replace(strings.Replace(validStatus, strings.Repeat("d", 64), pinID, 1), rootHash, terminalRootHash, 1)
	validGC := fmt.Sprintf(`{"owner_id":"snapshot:%s","generation":3,"lease_until_unix_ms":100000}`, owner)
	validPublisher := `{"config_id":1,"generation":4,"owner_id":"checkpoint-gc","purpose":"maintenance","lease_until_unix_ms":100000,"reserved_index":0}`
	tailHash := "[" + strings.TrimSuffix(strings.Repeat("1,", 32), ",") + "]"
	validArchivePin := fmt.Sprintf(`{"owner_id":%q,"base":7,"tip":14,"tail_hash":%s,"tail_object":1,"lease_until_unix_ms":100000}`, owner, tailHash)
	validCheckpointPin := fmt.Sprintf(`{"config_id":1,"owner_id":%q,"index":7,"root_hash":%q,"root":%s,"lease_until_unix_ms":100000}`, owner, terminalRootHash, checkpointRoot)
	gcPath, publisherPath := filepath.Join(metadataDir, "gc-lock.json"), filepath.Join(metadataDir, "publisher.json")
	archivePinPath, checkpointPinPath := filepath.Join(metadataDir, "archive-pin.json"), filepath.Join(metadataDir, "checkpoint-pin.json")
	checkpointRootPath, archiveHeadPath := filepath.Join(metadataDir, "checkpoint-root.json"), filepath.Join(metadataDir, "archive-head-decoded.json")
	for _, tc := range []struct {
		name, inventory, status, current, root, gc, publisher, archivePin, checkpointPin, head string
		clock                                                                                  int64
		valid                                                                                  bool
	}{
		{name: "exact-terminal", valid: true},
		{name: "publish-lock-change", inventory: strings.Replace(terminalObjects, `archive/PUBLISH_LOCK","generation":"2"`, `archive/PUBLISH_LOCK","generation":"20"`, 1)},
		{name: "head-change", inventory: strings.Replace(terminalObjects, `archive/head.bin","generation":"4"`, `archive/head.bin","generation":"20"`, 1)},
		{name: "current-change", inventory: strings.Replace(terminalObjects, `checkpoint/CURRENT","generation":"6"`, `checkpoint/CURRENT","generation":"20"`, 1)},
		{name: "archive-root-change", inventory: strings.Replace(terminalObjects, `archive/roots/root","generation":"5"`, `archive/roots/root","generation":"20"`, 1)},
		{name: "checkpoint-root-change", inventory: strings.Replace(terminalObjects, `checkpoint/roots/00000000000000000007_`, `checkpoint/roots/changed_`, 1)},
		{name: "block-change", inventory: strings.Replace(terminalObjects, `checkpoint/blocks/0001","generation":"8"`, `checkpoint/blocks/0001","generation":"20"`, 1)},
		{name: "additional-object", inventory: strings.TrimSuffix(terminalObjects, "]") + `,{"name":"prefix/cluster/z-unexpected","generation":"20","size":1}]`},
		{name: "wrong-pin-id", status: strings.Replace(terminalStatus, pinID, strings.Repeat("e", 64), 1)},
		{name: "wrong-pin-owner", archivePin: strings.Replace(validArchivePin, owner, "replica-rhiza-learner-fedcba9876543210", 1)},
		{name: "live-lease", archivePin: strings.Replace(validArchivePin, `"lease_until_unix_ms":100000`, `"lease_until_unix_ms":300000`, 1)},
		{name: "same-millisecond-expiry", archivePin: strings.Replace(validArchivePin, `"lease_until_unix_ms":100000`, `"lease_until_unix_ms":200000`, 1), valid: true},
		{name: "same-second-future-lease", archivePin: strings.Replace(validArchivePin, `"lease_until_unix_ms":100000`, `"lease_until_unix_ms":200500`, 1)},
		{name: "wrong-publisher-owner", publisher: strings.Replace(validPublisher, `"owner_id":"checkpoint-gc"`, `"owner_id":"other"`, 1)},
		{name: "wrong-publisher-purpose", publisher: strings.Replace(validPublisher, `"purpose":"maintenance"`, `"purpose":"publisher"`, 1)},
		{name: "malformed-checkpoint-root", root: `{"config_id":1`},
		{name: "truncated-checkpoint-root", root: `{"config_id":1,"index":7}`},
		{name: "checkpoint-root-content-mismatch", root: strings.Replace(checkpointRoot, `"index":7`, `"index":8`, 1)},
		{name: "checkpoint-root-hash-mismatch", head: strings.Replace(archiveHead, terminalRootHash, strings.Repeat("e", 64), 1)},
		{name: "checkpoint-pin-root-mismatch", checkpointPin: strings.Replace(validCheckpointPin, `"state_hash":"bbbb`, `"state_hash":"cbbb`, 1)},
		{name: "archive-base-mismatch", archivePin: strings.Replace(validArchivePin, `"base":7`, `"base":8`, 1)},
		{name: "archive-tip-mismatch", archivePin: strings.Replace(validArchivePin, `"tip":14`, `"tip":15`, 1)},
		{name: "archive-tail-hash-mismatch", archivePin: strings.Replace(validArchivePin, `"tail_hash":[1,`, `"tail_hash":[2,`, 1)},
		{name: "archive-tail-object-mismatch", archivePin: strings.Replace(validArchivePin, `"tail_object":1`, `"tail_object":2`, 1)},
	} {
		values := map[string]string{inventoryBefore: baseObjects, inventoryAfter: terminalObjects, statusPath: terminalStatus,
			checkpointCurrent: terminalCurrent, checkpointRootPath: checkpointRoot, archiveHeadPath: archiveHead,
			gcPath: validGC, publisherPath: validPublisher, archivePinPath: validArchivePin, checkpointPinPath: validCheckpointPin}
		for path, value := range map[string]string{inventoryAfter: tc.inventory, statusPath: tc.status, checkpointCurrent: tc.current,
			checkpointRootPath: tc.root, gcPath: tc.gc, publisherPath: tc.publisher, archivePinPath: tc.archivePin,
			checkpointPinPath: tc.checkpointPin, archiveHeadPath: tc.head} {
			if value != "" {
				values[path] = value
			}
		}
		for path, value := range values {
			if err := os.WriteFile(path, []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
		}
		clock := tc.clock
		if clock == 0 {
			clock = 200000
		}
		command := exec.Command("/bin/sh", "-c", "prefix=prefix/; cluster=cluster; "+validators+"\nvalidate_cold_inventory \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$6\" \"$7\" \"$8\" \"$9\" \"${10}\" \"${11}\" \"${12}\"", "fixture", inventoryBefore, inventoryAfter, pinID, statusPath, archiveHeadPath, checkpointRootPath, gcPath, publisherPath, archivePinPath, checkpointPinPath, archiveHeadPath, strconv.FormatInt(clock, 10))
		output, err := command.CombinedOutput()
		if (err == nil) != tc.valid {
			t.Fatalf("cold inventory %s valid=%t error=%v output=%s", tc.name, tc.valid, err, output)
		}
	}

	stopStart := strings.Index(source, "\nstop_forward() {\n")
	stopEnd := strings.Index(source, "\npf_pids=''\n")
	start := strings.Index(source, "\nforward() {\n")
	end := strings.Index(source[start+1:], "\nforward rhiza-voter-0")
	if stopStart < 0 || stopEnd <= stopStart || start < 0 || end < 0 {
		t.Fatal("port-forward helper missing")
	}
	functions := source[stopStart:stopEnd] + source[start:start+1+end]
	for _, scenario := range []struct {
		name string
		want int
	}{{"complete", 0}, {"initial-uid-mismatch", 1}, {"uid-change", 1}, {"container-change", 1}, {"image-change", 1}, {"wrong-stable-image", 1}, {"extra-sidecar", 1}, {"forward-death", 1}, {"foreign-healthy-listener", 1}, {"stubborn-child", 0}} {
		t.Run("forward-"+scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			selectedPort := 0
			var occupied net.Listener
			for _, candidate := range []int{28080, 28081, 28082, 28083} {
				listener, listenErr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", candidate))
				if listenErr != nil {
					continue
				}
				selectedPort = candidate
				if scenario.name == "foreign-healthy-listener" {
					occupied = listener
				} else {
					listener.Close()
				}
				break
			}
			if selectedPort == 0 {
				t.Fatal("no supported production port available")
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			write := func(name, content string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			testBinary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			write("curl", "#!/bin/sh\nexec \"$FAKE_TEST_BINARY\" -test.run '^TestQualificationCurlHelper$' -- \"$@\"\n")
			write("kubectl", `#!/bin/sh
case "$*" in *"port-forward --address=127.0.0.1 pod/rhiza-voter-0 $FAKE_PORT:8080 $FAKE_RAFT_PORT:8081"*) ;; *) exit 97 ;; esac
[ "$FAKE_SCENARIO" != forward-death ] || exit 53
if [ -e "$FAKE_DIR/listening" ]; then
  owner=$(cat "$FAKE_DIR/listening")
  if [ "$owner" = forced ] || kill -0 "$owner" 2>/dev/null; then touch "$FAKE_DIR/address-in-use"; touch "$FAKE_DIR/bind-exited"; exit 98; fi
  rm -f "$FAKE_DIR/listening"
fi
printf '%s\n' "$$" > "$FAKE_DIR/listening"; printf 'start\n' >> "$FAKE_DIR/events"
trap '[ "$FAKE_SCENARIO" = stubborn-child ] || { rm -f "$FAKE_DIR/listening"; printf "reaped\n" >> "$FAKE_DIR/events"; exit 0; }' TERM INT
while :; do sleep 1; done
`)
			write("lsof", `#!/bin/sh
case "$*" in
  *"-iTCP@127.0.0.1:$FAKE_PORT"*|*"-iTCP@127.0.0.1:$FAKE_RAFT_PORT"*)
    [ -e "$FAKE_DIR/listening" ] || exit 1
    owner=$(cat "$FAKE_DIR/listening")
    [ "$owner" = forced ] && owner=999
    if [ "$owner" != 999 ] && ! kill -0 "$owner" 2>/dev/null; then
      rm -f "$FAKE_DIR/listening"
      exit 1
    fi
    printf '%s\n' "$owner"
    exit 0
    ;;
esac
exit 1
`)
			write("ps", `#!/bin/sh
for value in "$@"; do pid=$value; done
group=$(cat "$FAKE_DIR/forward-pgid" 2>/dev/null || true)
listener=$(cat "$FAKE_DIR/listening" 2>/dev/null || true)
if [ "$pid" = "$group" ] || { [ "$pid" = "$listener" ] && [ "$listener" != forced ]; }; then
  printf ' %s\n' "$group"
else
  printf ' %s\n' "$pid"
fi
`)
			write("setsid", `#!/bin/sh
printf '%s\n' "$$" > "$FAKE_DIR/forward-pgid"
exec "$@"
`)
			fixture := `
set -eu
mode=run; out=$FAKE_DIR; pf_pids=''; context=synthetic; ns=synthetic; started=$(date +%s); pf_generation=0
RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
local_port_base=$FAKE_PORT
port_voter0=$local_port_base
port_voter1=$((local_port_base + 1))
port_voter2=$((local_port_base + 2))
port_learner=$((local_port_base + 3))
port_raft_voter0=$((port_voter0 + 100))
port_raft_voter1=$((port_voter1 + 100))
port_raft_voter2=$((port_voter2 + 100))
port_raft_learner=$((port_learner + 100))
port=$port_voter0
die() { printf '%s\n' "$*" >&2; exit 1; }
k() {
  [ "$1" = get ] || return 97
  count=0; [ ! -f "$FAKE_DIR/gets" ] || count=$(cat "$FAKE_DIR/gets"); count=$((count + 1)); printf '%s\n' "$count" > "$FAKE_DIR/gets"
  uid=uid-a; container=container-a; image=$RHIZA_HOST_IMAGE; spec_extra=''; status_extra=''
  [ "$FAKE_SCENARIO" != initial-uid-mismatch ] || uid=uid-b
  [ "$FAKE_SCENARIO" != wrong-stable-image ] || image=ghcr.io/mrchypark/rhiza-sql@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  if [ "$FAKE_SCENARIO" = extra-sidecar ]; then spec_extra=',{"name":"sidecar","image":"sidecar"}'; status_extra=',{"name":"sidecar","containerID":"sidecar","imageID":"sidecar"}'; fi
  [ "$count" = 1 ] || case "$FAKE_SCENARIO" in uid-change) uid=uid-b;; container-change) container=container-b;; image-change) image=image-b;; esac
  printf '{"metadata":{"name":"rhiza-voter-0","uid":"%s"},"spec":{"containers":[{"name":"rhiza","image":"%s"}%s]},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"rhiza","containerID":"%s","imageID":"host@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}%s]}}\n' "$uid" "$image" "$spec_extra" "$container" "$status_extra"
}
` + functions + `
trap 'for pid in $pf_pids; do stop_forward "$pid" || true; done' EXIT
[ "$FAKE_SCENARIO" != foreign-healthy-listener ] || printf 'forced\n' > "$FAKE_DIR/listening"
forward rhiza-voter-0 "$port" uid-a
case "$port" in "$port_voter0") first=$pf_voter0;; "$port_voter1") first=$pf_voter1;; "$port_voter2") first=$pf_voter2;; "$port_learner") first=$pf_learner;; esac
[ "$FAKE_SCENARIO" != complete ] && [ "$FAKE_SCENARIO" != stubborn-child ] && exit 0
forward rhiza-voter-0 "$port" uid-a
case "$port" in "$port_voter0") second=$pf_voter0;; "$port_voter1") second=$pf_voter1;; "$port_voter2") second=$pf_voter2;; "$port_learner") second=$pf_learner;; esac
[ "$first" != "$second" ]
[ "$(cat "$FAKE_DIR/forward-pgid")" = "$second" ]
! kill -0 "$first" 2>/dev/null
[ "$(printf '%s\n' $pf_pids | wc -l | tr -d ' ')" = 1 ]
stop_forward "$second"
[ "$(grep -c '^start$' "$FAKE_DIR/events")" = 2 ]
[ ! -e "$FAKE_DIR/address-in-use" ]
`
			var occupiedServer *http.Server
			if scenario.name == "foreign-healthy-listener" {
				occupiedServer = &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
					if request.URL.Path != "/healthz" {
						http.NotFound(response, request)
						return
					}
					response.WriteHeader(http.StatusOK)
				})}
				go func() { _ = occupiedServer.Serve(occupied) }()
				defer occupiedServer.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture)
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name,
				"FAKE_TEST_BINARY="+testBinary, "RHIZA_CURL_HELPER=1", "FAKE_PORT="+strconv.Itoa(selectedPort), "FAKE_RAFT_PORT="+strconv.Itoa(selectedPort+100))
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate: %v output=%s", err, output)
				}
				code = exited.ExitCode()
			}
			if (code == 0) != (scenario.want == 0) {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			if scenario.name == "foreign-healthy-listener" {
				if _, healthErr := os.Stat(filepath.Join(dir, "health-ok")); !errors.Is(healthErr, os.ErrNotExist) {
					t.Fatalf("foreign listener was used as port-forward readiness: %v", healthErr)
				}
			}
		})
	}
}

// Exercise the actual shell entry with synthetic files and a non-networking
// kubectl. No token is minted, no application opens, and no cloud call is made.
func TestQualificationLocalRuntime(t *testing.T) {
	nativeTimeout, err := exec.LookPath("timeout")
	if err != nil {
		t.Fatal("native GNU timeout required for the local runtime tests")
	}
	nativeTimeout, err = filepath.Abs(nativeTimeout)
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := os.Getenv("RHIZA_RUNTIME_TEST_SCRIPT")
	if scriptPath == "" {
		scriptPath = "../run-gcs-postrelease.sh"
	}
	script, err := filepath.Abs(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	const syntheticFixtureRevision = "1111111111111111111111111111111111111111"
	for _, scenario := range []string{"embedded-token", "exec", "alternate-context", "token-permissions", "wrong-identity", "fake-ci", "head-mismatch", "application-mismatch", "expiry", "command-failure", "deadline", "watchdog", "parser-timeout", "parser-argument-error", "runtime-kubeconfig-path", "cluster-receipt-changed", "workload-identity-receipt-changed", "workload-identity-wrong-binding", "workload-identity-extra-member", "workload-identity-extra-binding", "workload-identity-preexisting-binding", "workload-identity-recreated-gsa", "workload-identity-mode-false", "workload-identity-mode-absent", "workload-identity-mode-other", "metadata-worker-entrypoint", "metadata-credential-failure", "metadata-wrong-service-account", "metadata-gce-mode", "outside-prefix-auth-failure", "outside-prefix-accessible", "inside-prefix-auth-failure", "voter-create-failure", "voter-create-unknown"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			write := func(name, content string, mode os.FileMode) {
				t.Helper()
				if err := os.WriteFile(name, []byte(content), mode); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(bin, "setsid"), "#!/bin/sh\nexec \"$@\"\n", 0700)
			write(filepath.Join(bin, "lsof"), "#!/bin/sh\nexit 1\n", 0700)
			const run = "a1b2c3d4"
			const harness = "9b53158b38352ae78446e70a298616a31dc44a69"
			application := syntheticFixtureRevision
			if scenario == "application-mismatch" {
				application = strings.Repeat("3", 40)
			}
			mockFixtureRevision := syntheticFixtureRevision
			if scenario == "head-mismatch" {
				mockFixtureRevision = strings.Repeat("2", 40)
			}
			const contextName = "gke_example-project_test-region1_test-cluster"
			namespace := "rhiza-v0191-20261008-" + run
			metadata, err := json.Marshal(map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "rhiza-metadata", "namespace": namespace,
				"uid": "metadata-uid", "resourceVersion": "7", "labels": map[string]string{"chaos.rhiza.io/run": run}},
				"spec": map[string]any{"serviceAccountName": "rhiza-gcs", "nodeName": "gke-test-cluster-fixture-a",
					"containers": []any{map[string]string{"name": "metadata", "image": "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64)}}}})
			if err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(dir, "metadata.json"), string(metadata), 0600)
			clusterMetadata := `{"name":"test-cluster","location":"test-region1","endpoint":"example.invalid","clusterCaSHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","nodePools":[{"name":"fixture","workloadMetadataMode":"GKE_METADATA"}]}`
			if scenario == "metadata-gce-mode" {
				clusterMetadata = `{"name":"test-cluster","location":"test-region1","endpoint":"example.invalid","clusterCaSHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","nodePools":[{"name":"fixture","workloadMetadataMode":"GCE_METADATA"}]}`
			}
			clusterMetadataPath := filepath.Join(dir, "mint-cluster.json")
			write(clusterMetadataPath, clusterMetadata, 0600)
			clusterMetadataSum := sha256.Sum256([]byte(clusterMetadata))
			write(filepath.Join(dir, "mint-cluster.sha256"), fmt.Sprintf("%x\n", clusterMetadataSum), 0600)
			if scenario == "cluster-receipt-changed" {
				write(clusterMetadataPath, clusterMetadata+"\n", 0600)
			}
			token := filepath.Join(dir, "token")
			write(token, "synthetic-offline-token", 0600)
			user := map[string]any{"tokenFile": token}
			if scenario == "embedded-token" {
				user["token"] = "synthetic-rejected-token"
			}
			if scenario == "exec" {
				user["exec"] = map[string]string{"command": "never-execute"}
			}
			config := map[string]any{"apiVersion": "v1", "kind": "Config", "current-context": contextName,
				"clusters": []any{map[string]any{"name": "fixture-cluster", "cluster": map[string]string{"server": "https://127.0.0.1", "certificate-authority-data": "synthetic-pinned-ca"}}},
				"contexts": []any{map[string]any{"name": contextName, "context": map[string]string{"cluster": "fixture-cluster", "namespace": namespace, "user": "runtime"}}},
				"users":    []any{map[string]any{"name": "runtime", "user": user}}}
			if scenario == "alternate-context" {
				config["contexts"] = append(config["contexts"].([]any), map[string]string{"name": "admin"})
			}
			encoded, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			kubeconfig := filepath.Join(dir, "runtime.kubeconfig")
			write(kubeconfig, string(encoded), 0600)
			runtimeKubeconfig := kubeconfig
			if scenario == "runtime-kubeconfig-path" {
				runtimeKubeconfig = filepath.Join(dir, "other.kubeconfig")
				write(runtimeKubeconfig, string(encoded), 0600)
			}
			physicalDir, err := filepath.EvalSymlinks(dir)
			if err != nil {
				t.Fatal(err)
			}
			physicalKubeconfig := filepath.Join(physicalDir, "runtime.kubeconfig")
			if scenario == "token-permissions" {
				if err := os.Chmod(token, 0644); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().Unix()
			write(filepath.Join(bin, "date"), "#!/bin/sh\nif [ \"$*\" = '+%s' ]; then\n if [ -f \"$FAKE_CLOCK\" ]; then cat \"$FAKE_CLOCK\"; else printf '%s\\n' \"$FAKE_NOW\"; fi\nelse exec /bin/date \"$@\"; fi\n", 0700)
			write(filepath.Join(bin, "git"), `#!/bin/sh
[ "$#" = 4 ] && [ "$1" = -C ] && [ "$2" = "$FAKE_GIT_ROOT" ] &&
 [ "$3" = rev-parse ] && [ "$4" = HEAD ] || exit 91
printf '%s\n' "$FAKE_GIT_REVISION"
`, 0700)
			write(filepath.Join(bin, "kubectl"), `#!/bin/sh
printf 'mock-entry\n' >> "$FAKE_CONFIG_EVENTS"
if [ "$FAKE_SCENARIO" = parser-argument-error ]; then set -- --unexpected "$@"; fi
case "$1" in --kubeconfig=*) config=${1#*=}; shift ;; *) exit 92 ;; esac
[ "$1" = "--context=gke_example-project_test-region1_test-cluster" ] || exit 93
shift
[ "$1" = "--namespace=rhiza-v0191-20261008-a1b2c3d4" ] || exit 94
shift
case "$1" in --request-timeout=*) shift ;; esac
printf 'args-ok\n' >> "$FAKE_CONFIG_EVENTS"
printf '%s\n' "$*" >> "$FAKE_CALLS"
case "$1 $2" in
 "config view")
  printf 'config-view-start\n' >> "$FAKE_CONFIG_EVENTS"
  if [ "$FAKE_SCENARIO" = parser-timeout ]; then
   /bin/sleep 30 & child=$!
   printf '%s\n' "$child" > "$FAKE_CHILD"
   trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 143' TERM
   wait "$child"
  fi
  cat "$config"
  code=$?
  printf 'config-view-end exit=%s\n' "$code" >> "$FAKE_CONFIG_EVENTS"
  exit "$code" ;;
 "auth whoami")
  if [ "$FAKE_SCENARIO" = watchdog ]; then touch "$FAKE_SIGNAL_READY"; /bin/sleep 1; fi
  if [ "$FAKE_SCENARIO" = wrong-identity ]; then printf '%s\n' '{"status":{"userInfo":{"username":"admin","groups":[]}}}'
  else printf '%s\n' '{"status":{"userInfo":{"username":"system:serviceaccount:rhiza-v0191-20261008-a1b2c3d4:rhiza-runtime","groups":["system:authenticated","system:serviceaccounts","system:serviceaccounts:rhiza-v0191-20261008-a1b2c3d4"]}}}'; fi ;;
 "auth can-i")
  case "$*" in
   "auth can-i get secrets"|"auth can-i create serviceaccounts --subresource=token"|"auth can-i create roles"|"auth can-i create validatingadmissionpolicies.admissionregistration.k8s.io --all-namespaces") printf 'no\n'; exit 1 ;;
   "auth can-i create networkpolicies.networking.k8s.io"|"auth can-i update networkpolicies.networking.k8s.io"|"auth can-i patch networkpolicies.networking.k8s.io"|"auth can-i delete networkpolicies.networking.k8s.io") printf 'no\n'; exit 1 ;;
   *) exit 96 ;;
  esac ;;
 "get namespace")
  if [ "$FAKE_SCENARIO" = command-failure ]; then exit 53; fi
  printf '%s\n' '{"metadata":{"uid":"synthetic-uid","labels":{"chaos.rhiza.io/run":"a1b2c3d4"},"annotations":{"rhiza.dev/auth-owner":"rhiza-postrelease-a1b2c3d4-9b53158b38352ae78446e70a298616a31dc44a69","chaos-mesh.org/inject":"enabled"}}}' ;;
 "get pods,statefulsets,services,configmaps,podchaos,networkchaos")
  if [ "$FAKE_SCENARIO" = deadline ]; then printf '%s\n' "$((FAKE_NOW + 1199))" > "$FAKE_CLOCK"; fi
  printf '%s\n' '{"items":[]}' ;;
 "get networkpolicy.networking.k8s.io")
  printf '%s\n' '{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"rhiza-inspector-deny-all","namespace":"rhiza-v0191-20261008-a1b2c3d4","uid":"policy-uid","resourceVersion":"12","labels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"},"annotations":{"rhiza.dev/auth-owner":"rhiza-postrelease-a1b2c3d4-9b53158b38352ae78446e70a298616a31dc44a69"}},"spec":{"podSelector":{"matchLabels":{"app":"rhiza-inspector","chaos.rhiza.io/run":"a1b2c3d4"}},"policyTypes":["Ingress","Egress"],"ingress":[],"egress":[]}}' ;;
 "get networkpolicies.networking.k8s.io") printf '%s\n' '{"items":[{"metadata":{"uid":"policy-uid"}}]}' ;;
 "delete --dry-run=server"|"replace --dry-run=server") printf '%s\n' 'Error from server (Forbidden): networkpolicies is forbidden' >&2; exit 1 ;;
 "create --dry-run=server")
  [ "$3" = -f ] && [ "$5 $6" = '-o json' ] || exit 97
  case "$4" in */initial.yaml|*/learner.yaml|*/inspector-pod.yaml|*/inspector-control-pod.yaml) printf '%s\n' '{"kind":"List","items":[]}' ;; */inspector-networkpolicy-additive.json) printf '%s\n' 'Error from server (Forbidden): networkpolicies is forbidden' >&2; exit 1 ;; *) exit 97 ;; esac ;;
 "create -f")
  case "$FAKE_SCENARIO" in
   metadata-credential-failure|metadata-wrong-service-account|metadata-gce-mode|outside-prefix-auth-failure|outside-prefix-accessible|inside-prefix-auth-failure|voter-create-failure|voter-create-unknown)
    case "$3" in
     */metadata.yaml) [ "$4 $5" = '-o json' ] || exit 97; cat "$FAKE_METADATA"; exit 0 ;;
     */voters.yaml)
      if [ "$FAKE_SCENARIO" = voter-create-unknown ]; then touch "$FAKE_VOTER_ATTEMPT"; printf '{"kind":"StatefulSet"}'; exit 55; fi
      exit 53 ;;
     *) exit 97 ;;
    esac ;;
  esac
  /bin/sleep 30 & child=$!
  printf '%s\n' "$child" > "$FAKE_CHILD"
  trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 143' TERM
  wait "$child" ;;
 "exec rhiza-metadata")
  case "$4" in
   curl)
    case "$*" in
     *'/instance/service-accounts/default/email')
      if [ "$FAKE_SCENARIO" = metadata-wrong-service-account ]; then printf 'node@project.iam.gserviceaccount.com\n200'
      else printf 'rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\n200'; fi ;;
     *'/instance/service-accounts/default/token')
      if [ "$FAKE_SCENARIO" = metadata-credential-failure ]; then printf 'synthetic-metadata-token\n401'
      else printf '{"access_token":"synthetic-metadata-token","token_type":"Bearer","expires_in":100}\n200'; fi ;;
     *) exit 98 ;;
    esac ;;
   gcloud)
    case "$5 $6" in
     'storage cat')
      [ "$FAKE_SCENARIO" != outside-prefix-accessible ] || exit 0
      if [ "$FAKE_SCENARIO" = outside-prefix-auth-failure ]; then printf 'MetadataServerException authentication unavailable\n' >&2
      else printf '403\n' >&2; fi
      exit 1 ;;
     'storage ls')
      if [ "$FAKE_SCENARIO" = inside-prefix-auth-failure ]; then printf '401 UNAUTHENTICATED\n' >&2
      else printf 'matched no objects\n' >&2; fi
      exit 1 ;;
     *) exit 99 ;;
    esac ;;
   *) exit 99 ;;
  esac ;;
 "get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos")
  if [ -f "$FAKE_METADATA_DELETED" ]; then printf '{"items":[]}'
  else jq '{items:[.]}' "$FAKE_METADATA"; fi ;;
 "get pods") printf '%s\n' '{"items":[]}' ;;
 "delete --raw")
  [ "$3" = /api/v1/namespaces/rhiza-v0191-20261008-a1b2c3d4/pods/rhiza-metadata ] && [ "$4" = -f ] || exit 99
  jq -e '.preconditions=={uid:"metadata-uid",resourceVersion:"7"}' "$5" >/dev/null || exit 99
  touch "$FAKE_METADATA_DELETED" ;;
 "logs "*) kill -USR1 "$(cat "$FAKE_PARENT")"; exit 0 ;;
 "scale "*|"wait "*|"delete "*) exit 0 ;;
 *) exit 95 ;;
esac
`, 0700)
			// Functional cases check parser arguments and auth boundaries, not
			// process-launch performance. Timing cases keep the native timer.
			if scenario != "parser-timeout" && scenario != "deadline" && scenario != "watchdog" {
				write(filepath.Join(bin, "timeout"), `#!/bin/sh
previous=
parser=no
for argument do
 [ "$previous $argument" != "config view" ] || parser=yes
 previous=$argument
done
if [ "$parser" = yes ]; then
 [ "$#" = 11 ] && [ "$1" = --kill-after=5s ] && [ "$2" = 5s ] &&
  [ "$3" = kubectl ] && [ "$4" = "--kubeconfig=$FAKE_KUBECONFIG" ] &&
  [ "$5" = --context=gke_example-project_test-region1_test-cluster ] &&
  [ "$6" = --namespace=rhiza-v0191-20261008-a1b2c3d4 ] &&
  [ "$7" = config ] && [ "$8" = view ] && [ "$9" = --raw ] &&
  [ "${10}" = -o ] && [ "${11}" = json ] || exit 98
 shift 3
 exec "$FAKE_KUBECTL" "$@"
fi
exec "$FAKE_NATIVE_TIMEOUT" "$@"
`, 0700)
			}
			expiry := time.Unix(now+3300, 0).UTC().Format(time.RFC3339)
			if scenario == "expiry" {
				expiry = time.Unix(now+14400, 0).UTC().Format(time.RFC3339)
			}
			dataEmail := "rhiza-v0191-gcs-" + run + "@example-project.iam.gserviceaccount.com"
			owner := "rhiza-postrelease-" + run + "-" + harness
			member := "serviceAccount:example-project.svc.id.goog[" + namespace + "/rhiza-gcs]"
			condition := map[string]string{"title": "rhiza-" + run + "-expiry", "expression": "request.time < timestamp('" + expiry + "')"}
			dataCreate, _ := json.Marshal(map[string]string{"email": dataEmail, "name": "projects/example-project/serviceAccounts/" + dataEmail, "description": owner, "uniqueId": "synthetic-gsa-unique-id"})
			dataPolicy, _ := json.Marshal(map[string]any{"bindings": []any{map[string]any{"role": "roles/iam.workloadIdentityUser", "members": []string{member}, "condition": condition}}})
			write(filepath.Join(dir, "data-create.json"), string(dataCreate), 0600)
			write(filepath.Join(dir, "data-wi-before.json"), `{"bindings":[]}`, 0600)
			write(filepath.Join(dir, "data-wi-grant.json"), string(dataPolicy), 0600)
			workloadIdentity := fmt.Sprintf(`{"run":"%s","namespace":"%s","ksa":{"name":"rhiza-gcs","uid":"synthetic-data-ksa-uid","gcp_service_account":"%s","return_principal_id_as_email":true},"gsa":{"email":"%s","unique_id":"synthetic-gsa-unique-id","owner":"%s","member":"%s","role":"roles/iam.workloadIdentityUser","condition":{"title":"rhiza-%s-expiry","expression":"request.time < timestamp('%s')"}}}`,
				run, namespace, dataEmail, dataEmail, owner, member, run, expiry)
			workloadIdentityPath := filepath.Join(dir, "workload-identity.json")
			write(workloadIdentityPath, workloadIdentity, 0600)
			workloadIdentitySum := sha256.Sum256([]byte(workloadIdentity))
			write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			if scenario == "workload-identity-receipt-changed" {
				write(workloadIdentityPath, workloadIdentity+"\n", 0600)
			}
			if scenario == "workload-identity-wrong-binding" {
				workloadIdentity = strings.Replace(workloadIdentity, namespace+"/rhiza-gcs]", namespace+"/other]", 1)
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			if scenario == "workload-identity-extra-member" {
				write(filepath.Join(dir, "data-wi-grant.json"), strings.Replace(string(dataPolicy), `"members":["`+member+`"]`, `"members":["`+member+`","user:extra@example.invalid"]`, 1), 0600)
			}
			if scenario == "workload-identity-extra-binding" {
				write(filepath.Join(dir, "data-wi-grant.json"), strings.Replace(string(dataPolicy), `]}`, `,{"role":"roles/viewer","members":["user:extra@example.invalid"]}]}`, 1), 0600)
			}
			if scenario == "workload-identity-preexisting-binding" {
				write(filepath.Join(dir, "data-wi-before.json"), string(dataPolicy), 0600)
			}
			if scenario == "workload-identity-recreated-gsa" {
				workloadIdentity = strings.Replace(workloadIdentity, "synthetic-gsa-unique-id", "replacement-gsa-unique-id", 1)
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			if strings.HasPrefix(scenario, "workload-identity-mode-") {
				switch scenario {
				case "workload-identity-mode-false":
					workloadIdentity = strings.Replace(workloadIdentity, `"return_principal_id_as_email":true`, `"return_principal_id_as_email":false`, 1)
				case "workload-identity-mode-absent":
					workloadIdentity = strings.Replace(workloadIdentity, `,"return_principal_id_as_email":true`, ``, 1)
				case "workload-identity-mode-other":
					workloadIdentity = strings.Replace(workloadIdentity, `"return_principal_id_as_email":true`, `"return_principal_id_as_email":"true"`, 1)
				}
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			for _, name := range []string{"data-create.json", "data-wi-before.json", "data-wi-grant.json", "workload-identity.json"} {
				content, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || !json.Valid(content) {
					t.Fatalf("invalid synthetic trust receipt %s: %v", name, err)
				}
			}
			out := filepath.Join(dir, "evidence")
			if scenario == "metadata-worker-entrypoint" {
				if err := os.Mkdir(out, 0700); err != nil {
					t.Fatal(err)
				}
			}
			runtimeBudget := 15 * time.Second
			if scenario == "outside-prefix-auth-failure" {
				runtimeBudget = 30 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), runtimeBudget)
			defer cancel()
			entrypoint := "run-local"
			if scenario == "metadata-worker-entrypoint" {
				entrypoint = "metadata-readiness-worker"
			}
			cmd := exec.CommandContext(ctx, "/bin/sh", script, entrypoint)
			// Do not inherit any live credential/CI/approval variables.
			cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run, "RHIZA_NAMESPACE=" + namespace,
				"RHIZA_AUTH_CREATION_SHA=" + harness, "RHIZA_HARNESS_SHA=" + syntheticFixtureRevision, "RHIZA_APPLICATION_SHA=" + application,
				"RHIZA_EXECUTION_GO=" + syntheticFixtureRevision + ":" + run, "RHIZA_APPROVED_CAP_KRW=10000", "RHIZA_BOOTSTRAP_UID=synthetic-uid",
				"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64),
				"RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64),
				"RHIZA_NODE_A=gke-test-cluster-fixture-a", "RHIZA_NODE_B=gke-test-cluster-fixture-b", "RHIZA_NODE_C=gke-test-cluster-fixture-c",
				"RHIZA_OUTPUT=" + out, "RHIZA_AUTH_STATE=" + dir, "RHIZA_LOCAL_RUNTIME_KUBECONFIG=" + runtimeKubeconfig,
				"RHIZA_LOCAL_API_SERVER=https://127.0.0.1", "RHIZA_LOCAL_CA_DATA=synthetic-pinned-ca",
				"RHIZA_AUTH_STARTED_EPOCH=" + strconv.FormatInt(now, 10), "RHIZA_AUTH_EXPIRES=" + expiry, "RHIZA_LOCAL_TOKEN_EXPIRES=" + expiry,
				"FAKE_NOW=" + strconv.FormatInt(now, 10), "FAKE_CLOCK=" + filepath.Join(dir, "clock"),
				"FAKE_SCENARIO=" + scenario, "FAKE_CALLS=" + filepath.Join(dir, "calls"), "FAKE_CHILD=" + filepath.Join(dir, "child"),
				"FAKE_METADATA=" + filepath.Join(dir, "metadata.json"), "FAKE_METADATA_DELETED=" + filepath.Join(dir, "metadata-deleted"),
				"FAKE_VOTER_ATTEMPT=" + filepath.Join(dir, "voter-attempt"),
				"FAKE_CONFIG_EVENTS=" + filepath.Join(dir, "config-events"),
				"FAKE_NATIVE_TIMEOUT=" + nativeTimeout, "FAKE_KUBECTL=" + filepath.Join(bin, "kubectl"), "FAKE_KUBECONFIG=" + physicalKubeconfig,
				"FAKE_GIT_ROOT=" + filepath.Dir(script) + "/../..", "FAKE_GIT_REVISION=" + mockFixtureRevision,
				"FAKE_PARENT=" + filepath.Join(dir, "parent"), "FAKE_SIGNAL_READY=" + filepath.Join(dir, "signal-ready")}
			cmd.Env = append(cmd.Env, qualificationAssetEnv(run)...)
			if scenario == "metadata-worker-entrypoint" {
				cmd.Env = append(cmd.Env, "RHIZA_METADATA_PARENT_MODE=run-local", "RHIZA_CLUSTER_METADATA="+clusterMetadataPath)
			}
			if scenario == "fake-ci" {
				cmd.Env = append(cmd.Env, "GITHUB_ACTIONS=true")
			}
			if scenario == "embedded-token" {
				rejected := exec.Command(filepath.Join(bin, "timeout"), "--kill-after=5s", "6s", "kubectl",
					"--kubeconfig="+kubeconfig, "--context="+contextName, "--namespace="+namespace, "config", "view", "--raw", "-o", "json")
				rejected.Env = cmd.Env
				var exited *exec.ExitError
				if err := rejected.Run(); !errors.As(err, &exited) || exited.ExitCode() != 98 {
					t.Fatalf("malformed parser budget accepted: %v", err)
				}
				if _, err := os.Stat(filepath.Join(dir, "config-events")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("malformed parser tuple reached kubectl")
				}
			}
			var captured bytes.Buffer
			cmd.Stdout, cmd.Stderr = &captured, &captured
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(dir, "parent"), strconv.Itoa(cmd.Process.Pid), 0600)
			if scenario == "watchdog" {
				deadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(filepath.Join(dir, "signal-ready")); err == nil {
						if err := syscall.Kill(cmd.Process.Pid, syscall.SIGUSR1); err != nil {
							t.Fatal("owned runtime signal failed")
						}
						break
					}
					if time.Now().After(deadline) {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
						t.Fatal("runtime did not arm its owned watchdog")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			err = cmd.Wait()
			output := captured.Bytes()
			events, eventErr := os.ReadFile(filepath.Join(dir, "config-events"))
			calls, callsErr := os.ReadFile(filepath.Join(dir, "calls"))
			t.Logf("mock events: %q readError=%v; calls: %q readError=%v", events, eventErr, calls, callsErr)
			if scenario == "metadata-worker-entrypoint" {
				if err != nil || ctx.Err() != nil {
					t.Fatalf("actual metadata worker rejected sealed inputs: err=%v context=%v output=%s", err, ctx.Err(), output)
				}
				if !bytes.Contains(calls, []byte("/instance/service-accounts/default/email")) || !bytes.Contains(calls, []byte("/instance/service-accounts/default/token")) ||
					!bytes.Contains(output, []byte("category=linked-gsa-email")) || !bytes.Contains(output, []byte("category=token-valid")) ||
					bytes.Contains(output, []byte("cluster metadata receipt required")) {
					t.Fatalf("actual worker did not reach sealed token classification: calls=%s output=%s", calls, output)
				}
				return
			}
			var exited *exec.ExitError
			if !errors.As(err, &exited) || ctx.Err() != nil {
				t.Fatalf("runtime did not fail within its bounded test: err=%v context=%v", err, ctx.Err())
			}
			if bytes.Contains(output, []byte("synthetic-offline-token")) || bytes.Contains(output, []byte("synthetic-rejected-token")) || bytes.Contains(output, []byte("synthetic-metadata-token")) {
				t.Fatal("rejected credential content disclosed")
			}
			if bytes.Contains(output, []byte("node@project.iam.gserviceaccount.com")) {
				t.Fatal("rejected metadata identity body disclosed")
			}
			want := 1
			if scenario == "command-failure" || scenario == "voter-create-failure" {
				want = 53
			}
			if scenario == "voter-create-unknown" {
				want = 55
			}
			if scenario == "deadline" || scenario == "watchdog" {
				want = 124
			}
			if exited.ExitCode() != want {
				t.Fatalf("runtime exit=%d want=%d diagnostic=%s", exited.ExitCode(), want, output)
			}
			if scenario == "application-mismatch" && !bytes.Contains(output, []byte("candidate application mismatch")) {
				t.Fatalf("runtime accepted mismatched application provenance: %s", output)
			}
			if scenario == "metadata-credential-failure" || scenario == "metadata-wrong-service-account" || scenario == "outside-prefix-auth-failure" || scenario == "outside-prefix-accessible" || scenario == "inside-prefix-auth-failure" || scenario == "voter-create-failure" || scenario == "voter-create-unknown" {
				ready := bytes.Index(calls, []byte("wait pod/rhiza-metadata --for=condition=Ready"))
				credential := bytes.Index(calls, []byte("exec rhiza-metadata -- curl"))
				voters := bytes.Index(calls, []byte(out+"/voters.yaml"))
				outside := bytes.Index(calls, []byte("exec rhiza-metadata -- gcloud storage cat"))
				inside := bytes.Index(calls, []byte("exec rhiza-metadata -- gcloud storage ls"))
				if ready < 0 || (scenario != "metadata-gce-mode" && credential <= ready) {
					t.Fatalf("actual metadata Ready/readiness order missing: %s", calls)
				}
				status, err := os.ReadFile(filepath.Join(out, "cleanup-status.txt"))
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "metadata-credential-failure" || scenario == "metadata-wrong-service-account" || scenario == "metadata-gce-mode" || scenario == "outside-prefix-auth-failure" || scenario == "outside-prefix-accessible" || scenario == "inside-prefix-auth-failure" {
					if voters >= 0 || !bytes.Contains(status, []byte("Metadata-only Pod cleanup verified")) {
						t.Fatalf("credential failure bypassed EXIT cleanup or created voters: %s %s", calls, status)
					}
					if _, err := os.Stat(filepath.Join(out, "shutdown-cleanup-capture.exit")); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("metadata-only invoked voter Close capture")
					}
					switch scenario {
					case "metadata-credential-failure":
						if bytes.Contains(calls, []byte("gcloud")) || bytes.Contains(calls, []byte("storage")) {
							t.Fatal("failed credential readiness reached a storage probe")
						}
					case "metadata-wrong-service-account", "metadata-gce-mode":
						if bytes.Contains(calls, []byte("/token")) || bytes.Contains(calls, []byte("gcloud")) || bytes.Contains(calls, []byte("storage")) {
							t.Fatal("metadata identity/mode rejection reached token or storage")
						}
					case "outside-prefix-auth-failure", "outside-prefix-accessible":
						if outside <= credential || inside >= 0 || !bytes.Contains(output, []byte("outside-folder access did not fail closed")) {
							t.Fatalf("outside authentication failure was mistaken for expected denial: %s %s", calls, output)
						}
						if scenario == "outside-prefix-accessible" {
							paths, err := filepath.Glob(filepath.Join(out, "*-outside-scope.exit"))
							if err != nil || len(paths) != 1 {
								t.Fatalf("actual outside probe exit missing: %v %v", paths, err)
							}
							code, err := os.ReadFile(paths[0])
							if err != nil || strings.TrimSpace(string(code)) != "0" {
								t.Fatalf("outside probe did not actually succeed: %s %v", code, err)
							}
						}
					case "inside-prefix-auth-failure":
						if outside <= credential || inside <= outside || !bytes.Contains(output, []byte("prefix/list permission gate failed")) {
							t.Fatalf("inside authentication failure bypassed its distinct gate: %s %s", calls, output)
						}
					}
				} else {
					if outside <= credential || inside <= outside || voters <= inside || !bytes.Contains(status, []byte("NOT CLEAN")) {
						t.Fatalf("voter attempt bypassed strict cleanup: %s %s", calls, status)
					}
					capture, err := os.ReadFile(filepath.Join(out, "shutdown-cleanup-capture.exit"))
					if err != nil || strings.TrimSpace(string(capture)) != "1" {
						t.Fatalf("voter attempt did not require actual Close capture: %s %v", capture, err)
					}
					if scenario == "voter-create-unknown" {
						if _, err := os.Stat(filepath.Join(dir, "voter-attempt")); err != nil {
							t.Fatal("ambiguous create did not record its possible side effect")
						}
					}
				}
				if err := filepath.WalkDir(out, func(path string, entry os.DirEntry, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if entry.IsDir() {
						return nil
					}
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if bytes.Contains(data, []byte("synthetic-metadata-token")) || bytes.Contains(data, []byte("node@project.iam.gserviceaccount.com")) {
						return fmt.Errorf("metadata response persisted in %s", entry.Name())
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "deadline" && scenario != "command-failure" && scenario != "watchdog" && scenario != "metadata-credential-failure" && scenario != "metadata-wrong-service-account" && scenario != "outside-prefix-auth-failure" && scenario != "outside-prefix-accessible" && scenario != "inside-prefix-auth-failure" && scenario != "voter-create-failure" && scenario != "voter-create-unknown" {
				reason := "local kubeconfig must be one pinned tokenFile-only identity"
				switch scenario {
				case "head-mismatch":
					reason = "local harness HEAD mismatch"
				case "application-mismatch":
					reason = "candidate application mismatch"
				case "parser-argument-error":
					reason = "local kubeconfig parsing failed (exit 92)"
				case "parser-timeout":
					reason = "local kubeconfig parsing failed (exit 124)"
				case "token-permissions":
					reason = "private credential ownership/permissions required"
				case "wrong-identity":
					reason = "local runtime server identity mismatch"
				case "fake-ci":
					reason = "local entry must not impersonate CI"
				case "expiry":
					reason = "fixed 55-minute scope/preparation/runtime/cleanup reserve invalid"
				case "runtime-kubeconfig-path":
					reason = "runtime kubeconfig must be the mint-local receipt"
				case "cluster-receipt-changed":
					reason = "trusted cluster metadata preparation failed"
				case "workload-identity-receipt-changed", "workload-identity-wrong-binding", "workload-identity-extra-member", "workload-identity-extra-binding", "workload-identity-preexisting-binding", "workload-identity-recreated-gsa", "workload-identity-mode-false", "workload-identity-mode-absent", "workload-identity-mode-other":
					reason = "trusted workload identity binding receipt rejected"
				case "metadata-gce-mode":
					reason = "trusted cluster metadata preparation failed"
				}
				if !bytes.Contains(output, []byte(reason)) {
					t.Fatalf("rejected at the wrong boundary: %s", output)
				}
				if bytes.Contains(calls, []byte("create")) {
					t.Fatal("rejected local entry attempted a resource write")
				}
				if scenario == "fake-ci" || scenario == "head-mismatch" || scenario == "application-mismatch" || scenario == "runtime-kubeconfig-path" || strings.HasPrefix(scenario, "workload-identity-") {
					if !errors.Is(eventErr, os.ErrNotExist) || !errors.Is(callsErr, os.ErrNotExist) {
						t.Fatal("pre-parser rejection unexpectedly invoked the local mock")
					}
				} else if scenario == "parser-argument-error" {
					if eventErr != nil || string(events) != "mock-entry\n" || !errors.Is(callsErr, os.ErrNotExist) {
						t.Fatal("argument rejection did not preserve mock-entry without dispatch")
					}
				} else if eventErr != nil || callsErr != nil || !bytes.HasPrefix(events, []byte("mock-entry\nargs-ok\nconfig-view-start\n")) {
					t.Fatal("local mock entry/dispatch evidence absent or unreadable")
				}
				if scenario == "parser-timeout" {
					if string(events) != "mock-entry\nargs-ok\nconfig-view-start\n" {
						t.Fatal("timed parser did not preserve its start-only events")
					}
					child, err := os.ReadFile(filepath.Join(dir, "child"))
					if err != nil {
						t.Fatal("timed parser's owned child was not recorded")
					}
					pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
					if err != nil || syscall.Kill(pid, 0) == nil {
						t.Fatal("timed parser's owned child was not reaped")
					}
					t.Log("native parser exit124 preserved; owned child reaped")
				}
				return
			}
			for _, name := range []string{"workload.exit", "root.exit"} {
				value, err := os.ReadFile(filepath.Join(out, name))
				if err != nil || strings.TrimSpace(string(value)) != strconv.Itoa(want) {
					t.Fatalf("original exit not preserved in %s", name)
				}
			}
			watchdog, err := os.ReadFile(filepath.Join(out, "watchdog.pid"))
			if err != nil {
				t.Fatal("watchdog ownership evidence absent")
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(watchdog)))
			if err != nil || syscall.Kill(pid, 0) == nil {
				t.Fatal("owned watchdog not reaped before sealing")
			}
			verify := exec.Command("sha256sum", "-c", "SHA256SUMS")
			verify.Dir = out
			if verifyOutput, err := verify.CombinedOutput(); err != nil {
				if len(verifyOutput) > 1024 {
					verifyOutput = verifyOutput[:1024]
				}
				t.Fatalf("runtime seal does not verify: %v; diagnostic=%s", err, verifyOutput)
			}
			if scenario == "deadline" {
				child, err := os.ReadFile(filepath.Join(dir, "child"))
				if err != nil {
					t.Fatal("actual timed child was not started")
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
				if err != nil || syscall.Kill(pid, 0) == nil {
					t.Fatal("timed command's owned child escaped timeout")
				}
			}
		})
	}
}

func TestObjectStoreCountersUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	objectStoreCounters(func() (rhiza.ObjectStoreStats, bool) { return rhiza.ObjectStoreStats{}, false }).ServeHTTP(response,
		httptest.NewRequest(http.MethodGet, "/qualification/object-store", nil))
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "object store unavailable\n" {
		t.Fatalf("unexpected unavailable response: %d %q", response.Code, response.Body.String())
	}
}

func TestQualificationSQLReceipt(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nexecute() {\n")
	end := strings.Index(string(source), "\nreadback() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual SQL execution function missing")
	}
	// Exercise the host's real response encoding, including Applied's omission.
	response := httptest.NewRecorder()
	respond(response, rhiza.ExecuteResponse{MutationReceipt: rhiza.MutationReceipt{Slot: 1, Status: rhiza.MutationCommitted, RetryThroughSlot: 65536}}, nil)
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(`"applied"`)) {
		t.Fatal("SQL receipt response contract changed")
	}
	for _, scenario := range []struct {
		name, receipt string
		want          int
	}{
		{"encoded-sql", response.Body.String(), 0},
		{"observed-sql", `{"slot":1,"status":"committed","retry_through_slot":65536}`, 0},
		{"sql-false-applied", `{"slot":2,"status":"committed","applied":false,"error_code":""}`, 0},
		{"rejected", `{"slot":1,"status":"rejected"}`, 1},
		{"execution-error", `{"slot":1,"status":"committed","error_code":"execution_failed"}`, 1},
		{"unknown", `{"slot":1,"status":"commit_unknown"}`, 1},
		{"zero", `{"slot":0,"status":"committed"}`, 1},
		{"negative", `{"slot":-1,"status":"committed"}`, 1},
		{"fraction", `{"slot":1.5,"status":"committed"}`, 1},
		{"string", `{"slot":"1","status":"committed"}`, 1},
		{"missing-slot", `{"status":"committed"}`, 1},
		{"malformed", `{`, 1},
		{"curl-error", response.Body.String(), 22},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "receipt.json"), []byte(scenario.receipt), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; port_voter0=28080; port_voter1=28081; port_voter2=28082; port_learner=28083
die() { printf '%s\n' "$*" >&2; exit 1; }
run() { [ "$1" = ack ] && [ "$2" = curl ] || exit 91; seq=1; [ "$3" = -fsS ] || exit 92; [ "$FAKE_CURL_EXIT" = 0 ] || exit "$FAKE_CURL_EXIT"; cp "$out/receipt.json" "$out/1-ack.stdout"; }
metrics() { touch "$out/metrics"; }
`+string(source[start:end])+`
execute synthetic-schema 'CREATE TABLE qualification (id INTEGER PRIMARY KEY,value TEXT NOT NULL)'`, "fixture", dir)
			curlExit := "0"
			if scenario.name == "curl-error" {
				curlExit = "22"
			}
			command.Env = append(os.Environ(), "FAKE_CURL_EXIT="+curlExit)
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("receipt fixture did not exit normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			ids, idsErr := os.ReadFile(filepath.Join(dir, "ack-ids.txt"))
			_, metricsErr := os.Stat(filepath.Join(dir, "metrics"))
			if scenario.want == 0 {
				if idsErr != nil || string(ids) != "synthetic-schema\n" || metricsErr != nil {
					t.Fatal("valid SQL ACK was not retained/sampled")
				}
			} else if !errors.Is(idsErr, os.ErrNotExist) || !errors.Is(metricsErr, os.ErrNotExist) {
				t.Fatal("failed SQL receipt was counted as a successful ACK")
			}
		})
	}
}

func TestQualificationNetworkMutationFailureCapturesDiagnosticsWithoutRetry(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nexecute() {\n")
	end := strings.Index(string(source), "\nreadback() {\n")
	if start < 0 || end <= start {
		t.Fatal("network mutation diagnostic path missing")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	curlScript := `#!/bin/sh
output=''; url=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) output=$2; shift 2 ;;
    --write-out|--max-time|-H|-d) shift 2 ;;
    http://*) url=$1; shift ;;
    *) shift ;;
  esac
done
printf '%s\n' "$url" >> "$FAKE_DIR/curl-urls"
case "$url" in
  */sql/execute) printf '%s\n' '{"code":"quorum_unavailable"}' > "$output"; printf '503\n0.125\n' ;;
  *) printf '%s\n' '{}' > "$output"; printf '200\n0.010\n' ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(curlScript), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\nprintf '%s\\n' '{}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; active_fault=NetworkChaos/network-one; port_voter0=28080; port_voter1=28081; port_voter2=28082; port_learner=28083
die() { printf '%s\n' "$*" >&2; exit 1; }
metrics() { exit 97; }
mode=run; context=synthetic; ns=synthetic
`+string(source[start:end])+`
execute a1b2c3d4-network "INSERT INTO qualification VALUES (2,'network')"`, "fixture", dir)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 1 || !bytes.Contains(output, []byte("category=unconfirmed")) {
		t.Fatalf("failure was not preserved: error=%v output=%s", err, output)
	}
	urls, err := os.ReadFile(filepath.Join(dir, "curl-urls"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(urls), "/sql/execute") != 1 {
		t.Fatalf("ambiguous mutation retried: %s", urls)
	}
	for _, name := range []string{
		"1-ack.stdout", "1-ack.http-status", "1-ack.time-total", "1-ack.exit",
		"network-failure-28080-request-status.stdout", "network-failure-28081-recovery-status.stdout",
		"network-failure-28082-linearizable-read.stdout", "network-failure-fault.stdout",
		"network-failure-pods.stdout", "network-failure-endpoints.stdout", "network-failure-endpointslices.stdout",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("diagnostic evidence missing %s: %v", name, err)
		}
	}
	status, _ := os.ReadFile(filepath.Join(dir, "1-ack.http-status"))
	timing, _ := os.ReadFile(filepath.Join(dir, "1-ack.time-total"))
	body, _ := os.ReadFile(filepath.Join(dir, "1-ack.stdout"))
	if string(status) != "503\n" || string(timing) != "0.125\n" || !bytes.Contains(body, []byte("quorum_unavailable")) {
		t.Fatalf("raw failure evidence changed: status=%q timing=%q body=%s", status, timing, body)
	}
}

func TestQualificationNetworkFailureDiagnosticsBoundHungKubectl(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\ndiagnostic_k() {\n")
	end := strings.Index(string(source), "\nreadback() {\n")
	if start < 0 || end <= start {
		t.Fatal("bounded network diagnostics missing")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(`#!/bin/sh
output=''
while [ "$#" -gt 0 ]; do case "$1" in --output) output=$2; shift 2;; --write-out|--max-time|-H|-d) shift 2;; *) shift;; esac; done
: > "$output"
printf '200\n0.001\n'
`), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
	out=$1; mode=run; context=synthetic; ns=synthetic; active_fault=NetworkChaos/network-one
	port_voter0=28080; port_voter1=28081; port_voter2=28082
jq() { command jq "$@"; }
`+string(source[start:end])+`
original=22
capture_network_failure a1b2c3d4-network
[ "$original" = 22 ]
for name in fault pods endpoints endpointslices; do
  case "$(cat "$out/network-failure-$name.exit")" in 124|137) ;; *) exit 91 ;; esac
done`, "fixture", dir)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	started := time.Now()
	output, err := command.CombinedOutput()
	if err != nil || ctx.Err() != nil || time.Since(started) > 6*time.Second {
		t.Fatalf("hung diagnostics were not bounded or changed original state: elapsed=%s error=%v context=%v output=%s", time.Since(started), err, ctx.Err(), output)
	}
}

func TestQualificationCommittedResponseRequiresLiveInjectedFault(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	executeStart := strings.Index(string(source), "\nnetwork_fault_current() (\n")
	executeEnd := strings.Index(string(source), "\nreadback() {\n")
	validatorStart := strings.Index(string(source), "\nvalidate_network_injection() {\n")
	validatorEnd := strings.Index(string(source), "\nfault() {\n")
	if executeStart < 0 || executeEnd <= executeStart || validatorStart < 0 || validatorEnd <= validatorStart {
		t.Fatal("network ACK live-fault gate missing")
	}
	dir := t.TempDir()
	recovered := `{"kind":"NetworkChaos","metadata":{"name":"network-one","namespace":"synthetic","labels":{"chaos.rhiza.io/run":"a1b2c3d4"}},"spec":{"action":"partition","direction":"both","mode":"one","duration":"45s","selector":{"labelSelectors":{"statefulset.kubernetes.io/pod-name":"rhiza-voter-0"}},"target":{"mode":"all","selector":{"expressionSelectors":[{"key":"statefulset.kubernetes.io/pod-name","operator":"In","values":["rhiza-voter-1","rhiza-voter-2"]}]}}},"status":{"conditions":[{"type":"AllInjected","status":"True"},{"type":"AllRecovered","status":"True"}],"experiment":{"containerRecords":[{"selectorKey":".","id":"synthetic/rhiza-voter-0","phase":"Injected","injectedCount":1,"recoveredCount":0,"events":[{"operation":"Apply","type":"Succeeded"}]},{"selectorKey":".Target","id":"synthetic/rhiza-voter-1","phase":"Injected","injectedCount":1,"recoveredCount":0,"events":[{"operation":"Apply","type":"Succeeded"}]},{"selectorKey":".Target","id":"synthetic/rhiza-voter-2","phase":"Injected","injectedCount":1,"recoveredCount":0,"events":[{"operation":"Apply","type":"Succeeded"}]}]}}}`
	if err := os.WriteFile(filepath.Join(dir, "fault.json"), []byte(recovered), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", `set -eu
out=$1; seq=0; active_fault=NetworkChaos/network-one; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; port_voter0=28080; port_voter1=28081; port_voter2=28082; port_learner=28083
die() { printf '%s\n' "$*" >&2; exit 1; }
curl() { output=''; while [ "$#" -gt 0 ]; do case "$1" in --output) output=$2; shift 2;; --write-out|--max-time|-H|-d) shift 2;; *) shift;; esac; done; printf '%s\n' '{"slot":2,"status":"committed"}' > "$output"; printf '200\n0.010\n'; }
k() { cat "$out/fault.json"; }
run() { seq=$((seq+1)); label=$1; shift; "$@" > "$out/$seq-$label.stdout"; printf '0\n' > "$out/$seq-$label.exit"; }
metrics() { touch "$out/metrics"; }
`+string(source[executeStart:executeEnd])+string(source[validatorStart:validatorEnd])+`
execute a1b2c3d4-network "INSERT INTO qualification VALUES (2,'network')"`, "fixture", dir)
	output, err := command.CombinedOutput()
	if err == nil || !bytes.Contains(output, []byte("fault recovered or changed")) {
		t.Fatalf("recovered fault accepted committed response: error=%v output=%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(dir, "ack-ids.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered fault recorded ACK: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "metrics")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered fault reached metrics: %v", err)
	}
}

func TestQualificationContainerKillRecovery(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\ncontainer_kill() {\n")
	end := strings.Index(string(source), "\nfault NetworkChaos partition network-one\n")
	if start < 0 || end <= start {
		t.Fatal("actual container-kill branch missing")
	}
	for _, scenario := range []string{"success", "delayed-ready", "before-replaced", "two-injections", "wrong-target", "empty-records", "apply-failed", "delete-failed", "not-ready", "readiness-timeout", "ready-uid-drift", "pod-replaced", "wal-replaced", "no-restart", "two-restarts", "same-container", "other-restarted", "image-changed", "barrier-failed", "restart-timeout", "hung-get"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			writeJSON := func(name string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			pods := func(after bool) []any {
				var result []any
				for i := range 3 {
					uid, volume, containerID, image, restarts, ready := fmt.Sprintf("uid-%d", i), "data", fmt.Sprintf("containerd://old-%d", i), "sha256:fixed", 0, true
					if after && i == 0 {
						containerID, restarts = "containerd://new-0", 1
						switch scenario {
						case "not-ready":
							ready = false
						case "pod-replaced":
							uid = "replacement"
						case "wal-replaced":
							volume = "new-wal"
						case "no-restart":
							restarts = 0
						case "two-restarts":
							restarts = 2
						case "same-container":
							containerID = "containerd://old-0"
						case "image-changed":
							image = "sha256:other"
						}
					}
					if after && i == 1 && scenario == "other-restarted" {
						restarts = 1
					}
					result = append(result, map[string]any{"name": fmt.Sprintf("rhiza-voter-%d", i), "uid": uid, "node": fmt.Sprintf("node-%d", i), "volumes": []any{map[string]any{"name": volume, "emptyDir": map[string]any{}}}, "ready": ready, "deleting": nil,
						"containers": []any{map[string]any{"name": "rhiza", "containerID": containerID, "imageID": image, "restartCount": restarts}}})
				}
				return result
			}
			writeJSON("voters-before.json", pods(false))
			before := pods(false)
			if scenario == "before-replaced" {
				before[0].(map[string]any)["uid"] = "replacement"
			}
			writeJSON("before.json", before)
			writeJSON("after.json", pods(true))
			restart := pods(true)[0].(map[string]any)
			if scenario == "delayed-ready" {
				restart["ready"] = false
			}
			writeJSON("restart.json", map[string]any{"metadata": map[string]any{"name": restart["name"], "uid": restart["uid"], "deletionTimestamp": restart["deleting"]},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[restart["ready"].(bool)]}}, "containerStatuses": restart["containers"]}})
			ready := pods(true)[0].(map[string]any)
			if scenario == "ready-uid-drift" {
				ready["uid"] = "replacement"
			}
			writeJSON("ready.json", map[string]any{"metadata": map[string]any{"name": ready["name"], "uid": ready["uid"], "deletionTimestamp": ready["deleting"]},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[ready["ready"].(bool)]}}, "containerStatuses": ready["containers"]}})
			count, target, eventType := 1, "synthetic/rhiza-voter-0/rhiza", "Succeeded"
			if scenario == "two-injections" {
				count = 2
			}
			if scenario == "wrong-target" {
				target = "synthetic/rhiza-voter-1/rhiza"
			}
			if scenario == "apply-failed" {
				eventType = "Failed"
			}
			records := []any{map[string]any{"id": target, "phase": "Injected", "injectedCount": count, "recoveredCount": 0, "events": []any{map[string]any{"operation": "Apply", "type": eventType}}}}
			if scenario == "empty-records" {
				records = nil
			}
			// Same shape as the saved 620769db one-shot response, including the
			// deliberately false AllRecovered condition; never claim recovery from it.
			writeJSON("injection.json", map[string]any{"kind": "PodChaos", "metadata": map[string]any{"name": "process-one", "namespace": "synthetic", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}},
				"spec":   map[string]any{"action": "container-kill", "mode": "one", "duration": "10s", "containerNames": []string{"rhiza"}, "selector": map[string]any{"namespaces": []string{"synthetic"}, "labelSelectors": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4", "statefulset.kubernetes.io/pod-name": "rhiza-voter-0"}}},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "AllInjected", "status": "True"}, map[string]any{"type": "AllRecovered", "status": "False"}}, "experiment": map[string]any{"containerRecords": records}}})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; active_fault=''; port_voter0=28080; port_voter1=28081; port_voter2=28082; port_learner=28083
die() { printf '%s\n' "$*" >&2; exit 1; }
run() { name=$1; shift; seq=$((seq+1)); printf '%s\n' "$name" >> "$out/order"; "$@" > "$out/$seq-$name.stdout"; }
date() { fake_time=$(cat "$out/time" 2>/dev/null || printf 0); fake_time=$((fake_time+16)); printf '%s\n' "$fake_time" | tee "$out/time"; }
sleep() { :; }
pod_snapshot() { if [ "$seq" = 1 ]; then cat "$out/before.json"; else [ -f "$out/deleted" ] || exit 91; cat "$out/after.json"; fi; }
fault() { [ "$*" = 'PodChaos container-kill process-one' ] || exit 92; active_fault=PodChaos/process-one; seq=$((seq+1)); cp "$out/injection.json" "$out/$seq-injection.stdout"; printf 'injected\n' >> "$out/order"; }
execute() { printf 'ack\n' >> "$out/order"; }
k() {
  if [ "$*" = 'get pod rhiza-voter-0 -o json' ]; then
    if [ "$FAKE_SCENARIO" = hung-get ]; then
      [ -n "${voter_wait_deadline:-}" ] || exit 97
      printf '%s\n' "$voter_wait_deadline" > "$out/hung-get-deadline"
      return 124
    fi
    if [ -f "$out/deleted" ]; then cat "$out/ready.json"; return; fi
    if [ "$FAKE_SCENARIO" = restart-timeout ]; then
      restart_reads=$(cat "$out/restart-reads" 2>/dev/null || printf 0); restart_reads=$((restart_reads+1)); printf '%s\n' "$restart_reads" > "$out/restart-reads"
      [ "$restart_reads" = 1 ] && cat "$out/before.json" || return 124
    else
      cat "$out/restart.json"
    fi
  else
    [ "$*" = 'delete PodChaos/process-one --wait=true --timeout=60s' ] || exit 93
    [ "$FAKE_SCENARIO" != delete-failed ] || exit 53
    touch "$out/deleted"
  fi
}
wait_voters() {
  [ "$1" -le 180 ] && [ "$1" -gt 0 ] && [ -f "$out/deleted" ] || exit 94
  [ "$FAKE_SCENARIO" != readiness-timeout ] || return 124
}
forward() { [ -f "$out/deleted" ] || exit 95; }
metrics() { printf 'metrics\n' >> "$out/order"; }
readback() { [ "$2" = linearizable ] && [ "$3" = '[[1,"before"],[2,"network"],[3,"process"]]' ] || exit 96; printf 'barrier-%s\n' "$1" >> "$out/order"; [ "$FAKE_SCENARIO" != barrier-failed ] || exit 54; }
`+string(source[start:end])+`
container_kill
[ -z "$active_fault" ] && [ "$voter0_restarts" = 1 ] && [ -z "${voter_wait_deadline:-}" ]`, "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("fixture timeout: %v", ctx.Err())
			}
			if scenario == "success" || scenario == "delayed-ready" {
				if err != nil {
					t.Fatalf("actual branch: %v: %s", err, output)
				}
				order, err := os.ReadFile(filepath.Join(dir, "order"))
				if err != nil || string(order) != "process-before\ninjected\nrestart-evidence\nfault-delete\nvoters-ready\nack\nafter\nmetrics\nbarrier-28080\nbarrier-28081\nbarrier-28082\n" {
					t.Fatalf("wrong one-shot recovery order: %s (%v)", order, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe recovery accepted")
				}
				if scenario == "hung-get" {
					var exited *exec.ExitError
					if !errors.As(err, &exited) || exited.ExitCode() != 124 {
						t.Fatalf("hung get exit=%v want=124 output=%s", err, output)
					}
					if _, statErr := os.Stat(filepath.Join(dir, "hung-get-deadline")); statErr != nil {
						t.Fatalf("hung get lacked scoped deadline: %v", statErr)
					}
				}
				if scenario == "restart-timeout" {
					evidence, evidenceErr := os.ReadFile(filepath.Join(dir, "process-restart-current.json"))
					beforeEvidence, beforeErr := os.ReadFile(filepath.Join(dir, "before.json"))
					if evidenceErr != nil || beforeErr != nil || !bytes.Equal(evidence, beforeEvidence) {
						t.Fatalf("restart timeout did not preserve last successful evidence: %v %v", evidenceErr, beforeErr)
					}
				}
				order, readErr := os.ReadFile(filepath.Join(dir, "order"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				if scenario == "before-replaced" && string(order) != "process-before\n" {
					t.Fatalf("fault injected into a replacement Pod: %s", order)
				}
				switch scenario {
				case "two-injections", "wrong-target", "empty-records", "apply-failed":
					if string(order) != "process-before\ninjected\n" {
						t.Fatalf("unproven injection reached mutation: %s", order)
					}
				case "delete-failed":
					if bytes.Contains(order, []byte("voters-ready")) {
						t.Fatalf("finalizer failure reached recovery: %s", order)
					}
				}
				switch scenario {
				case "not-ready", "readiness-timeout", "ready-uid-drift", "pod-replaced", "no-restart", "two-restarts", "same-container", "restart-timeout", "hung-get":
					if bytes.Contains(order, []byte("ack")) {
						t.Fatalf("unproven restart reached ACK: %s", order)
					}
				}
				if scenario != "barrier-failed" && bytes.Contains(order, []byte("barrier-")) {
					t.Fatalf("unsafe incarnation reached SQL recovery: %s", order)
				}
			}
		})
	}
}

func TestQualificationNetworkRecoveryWaitsForVoters(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nfault NetworkChaos partition network-one\n")
	end := strings.Index(string(source), "\ncontainer_kill\n")
	if start < 0 || end <= start {
		t.Fatal("actual network recovery branch missing")
	}
	for _, scenario := range []string{"success", "readiness-failed", "budget-exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; RHIZA_RUN_ID=a1b2c3d4; active_fault='NetworkChaos/network-one'; started=900; mode=run; port_voter0=28080; port_voter1=28081; port_voter2=28082; port_learner=28083
[ "$FAKE_SCENARIO" != budget-exhausted ] || forward_deadline=999
run() { name=$1; shift; printf '%s\n' "$name" >> "$out/order"; "$@"; }
fault() { [ "$*" = 'NetworkChaos partition network-one' ] || exit 91; printf '%s\n' fault >> "$out/order"; }
execute() { printf '%s\n' execute >> "$out/order"; }
k() { :; }
wait_voters() { [ "$1" = 120 ] || exit 92; [ "$FAKE_SCENARIO" != readiness-failed ] || exit 53; }
date() { printf '1000\n'; }
post_network_readback() { [ "$2" = '[[1,"before"],[2,"network"]]' ] || exit 93; printf 'readback-%s\n' "$1" >> "$out/order"; }
metrics() { printf '%s\n' metrics >> "$out/order"; }
`+string(source[start:end]), "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("fixture timeout: %v", ctx.Err())
			}
			order, readErr := os.ReadFile(filepath.Join(dir, "order"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "success" {
				if err != nil {
					t.Fatalf("actual branch: %v: %s", err, output)
				}
				want := "fault\nexecute\nrecovered\nfault-delete\nvoters-ready\nreadback-28080\nreadback-28081\nreadback-28082\nmetrics\n"
				if string(order) != want {
					t.Fatalf("wrong network recovery order: %s", order)
				}
			} else if err == nil || bytes.Contains(order, []byte("readback-")) {
				t.Fatalf("failed readiness reached readback: %s", order)
			} else if scenario == "budget-exhausted" && !bytes.Contains(output, []byte("category=budget-exhausted")) {
				t.Fatalf("exhausted convergence budget was not classified: %s", output)
			}
		})
	}
}

func TestQualificationNetworkInjectionIsExactAndDisjoint(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nvalidate_network_injection() {\n")
	end := strings.Index(string(source), "\nfault() {\n")
	if start < 0 || end <= start {
		t.Fatal("network injection validator missing")
	}
	validator := string(source[start:end])
	if injection, mutation := strings.Index(string(source), `validate_network_injection "$out/$seq-injection.stdout"`), strings.Index(string(source), `execute "$RHIZA_RUN_ID-network"`); injection < 0 || mutation <= injection {
		t.Fatal("network mutation can start before exact injection validation")
	}
	auth, err := os.ReadFile("../gcs-postrelease-auth.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		`expressionSelectors:[{key:"statefulset.kubernetes.io/pod-name",operator:"In",values:["rhiza-voter-1","rhiza-voter-2"]}]`,
		`size(object.spec.target.selector.expressionSelectors) == 1`,
		`object.spec.target.selector.expressionSelectors[0].key == 'statefulset.kubernetes.io/pod-name'`,
		`object.spec.target.selector.expressionSelectors[0].operator == 'In'`,
		`size(object.spec.target.selector.expressionSelectors[0].values) == 2`,
		`'rhiza-voter-1' in object.spec.target.selector.expressionSelectors[0].values`,
		`'rhiza-voter-2' in object.spec.target.selector.expressionSelectors[0].values`,
	} {
		if !bytes.Contains(source, []byte(required)) && !bytes.Contains(auth, []byte(required)) {
			t.Fatalf("disjoint selector contract missing %q", required)
		}
	}
	if bytes.Contains(auth, []byte(`object.spec.target.selector.expressionSelectors == [`)) {
		t.Fatal("typed expressionSelectors list must not use map/list equality")
	}
	record := func(selector, pod, event string) map[string]any {
		return map[string]any{"selectorKey": selector, "id": "synthetic/" + pod, "phase": "Injected", "injectedCount": 1, "recoveredCount": 0,
			"events": []any{map[string]any{"operation": "Apply", "type": event}}}
	}
	base := func() map[string]any {
		return map[string]any{
			"kind": "NetworkChaos", "metadata": map[string]any{"name": "network-one", "namespace": "synthetic", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}},
			"spec": map[string]any{"action": "partition", "direction": "both", "mode": "one", "duration": "45s",
				"selector": map[string]any{"labelSelectors": map[string]any{"statefulset.kubernetes.io/pod-name": "rhiza-voter-0"}},
				"target":   map[string]any{"mode": "all", "selector": map[string]any{"expressionSelectors": []any{map[string]any{"key": "statefulset.kubernetes.io/pod-name", "operator": "In", "values": []any{"rhiza-voter-1", "rhiza-voter-2"}}}}}},
			"status": map[string]any{"conditions": []any{map[string]any{"type": "AllInjected", "status": "True"}}, "experiment": map[string]any{"containerRecords": []any{
				record(".", "rhiza-voter-0", "Succeeded"), record(".Target", "rhiza-voter-1", "Succeeded"), record(".Target", "rhiza-voter-2", "Succeeded")}}},
		}
	}
	for _, scenario := range []struct {
		name string
		edit func(map[string]any)
		ok   bool
	}{
		{name: "exact", ok: true},
		{name: "overlap", edit: func(v map[string]any) { records(v)[2].(map[string]any)["id"] = "synthetic/rhiza-voter-0" }},
		{name: "missing-target", edit: func(v map[string]any) { setRecords(v, records(v)[:2]) }},
		{name: "duplicate-source", edit: func(v map[string]any) { setRecords(v, append(records(v), record(".", "rhiza-voter-0", "Succeeded"))) }},
		{name: "latest-apply-failed", edit: func(v map[string]any) {
			r := records(v)[1].(map[string]any)
			r["events"] = append(r["events"].([]any), map[string]any{"operation": "Apply", "type": "Failed"})
		}},
		{name: "incomplete", edit: func(v map[string]any) { records(v)[1].(map[string]any)["injectedCount"] = 0 }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			value := base()
			if scenario.edit != nil {
				scenario.edit(value)
			}
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "injection.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "-c", "set -eu; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; "+validator+"\nvalidate_network_injection \"$1\"", "fixture", path)
			output, err := command.CombinedOutput()
			if (err == nil) != scenario.ok {
				t.Fatalf("accepted=%t want=%t output=%s", err == nil, scenario.ok, output)
			}
		})
	}
}

func records(value map[string]any) []any {
	return value["status"].(map[string]any)["experiment"].(map[string]any)["containerRecords"].([]any)
}

func setRecords(value map[string]any, next []any) {
	value["status"].(map[string]any)["experiment"].(map[string]any)["containerRecords"] = next
}

func TestQualificationPostNetworkConvergence(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\npost_network_readback() {\n")
	end := strings.Index(string(source), "\nexecute \"$RHIZA_RUN_ID-schema\"")
	if start < 0 || end <= start {
		t.Fatal("actual post-network convergence function missing")
	}
	for _, scenario := range []struct {
		name string
		want int
	}{
		{"transient-503", 0},
		{"permanent-503", 124},
		{"wrong-data", 1},
		{"auth-401", 1},
		{"transport-failure", 7},
		{"malformed-200", 1},
		{"invalid-tips", 1},
		{"missing-tips", 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; post_network_deadline=1005; printf '1000\n' > "$out/clock"
date() { now=$(cat "$out/clock"); now=$((now + 1)); printf '%s\n' "$now" > "$out/clock"; printf '%s\n' "$now"; }
sleep() { :; }
curl() {
  attempt_file="$out/curl-attempts"; attempt=0; [ ! -f "$attempt_file" ] || attempt=$(cat "$attempt_file"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$attempt_file"
  output=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --output) output=$2; shift 2 ;;
      --max-time|--write-out|-H|-d) shift 2 ;;
      *) shift ;;
    esac
  done
  case "$FAKE_SCENARIO" in
    transient-503) if [ "$attempt" = 1 ]; then printf '{"error":"unavailable"}' > "$output"; printf '503\n'; else printf '{"rows":[[1,"before"],[2,"network"]],"applied_slot":2,"consensus_tip":2}' > "$output"; printf '200\n'; fi ;;
    permanent-503) printf '{"error":"unavailable"}' > "$output"; printf '503\n' ;;
    wrong-data) printf '{"rows":[[1,"before"]],"applied_slot":2,"consensus_tip":2}' > "$output"; printf '200\n' ;;
    auth-401) printf '{"error":"unauthorized"}' > "$output"; printf '401\n' ;;
    transport-failure) printf '{"error":"connection reset"}' > "$output"; printf '000\n'; return 7 ;;
    malformed-200) printf '{' > "$output"; printf '200\n' ;;
    invalid-tips) printf '{"rows":[[1,"before"],[2,"network"]],"applied_slot":2,"consensus_tip":1}' > "$output"; printf '200\n' ;;
    missing-tips) printf '{"rows":[[1,"before"],[2,"network"]]}' > "$output"; printf '200\n' ;;
  esac
}
`+string(source[start:end])+`
post_network_readback 28080 '[[1,"before"],[2,"network"]]'`, "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario.name)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("fixture timeout: %v", ctx.Err())
			}
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) {
					t.Fatalf("fixture did not exit normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			attempts, readErr := os.ReadFile(filepath.Join(dir, "curl-attempts"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario.name == "transient-503" {
				if string(attempts) != "2\n" {
					t.Fatalf("transient 503 attempts=%s", attempts)
				}
				for _, name := range []string{"1-post-network-readback-28080-attempt-1.stdout", "1-post-network-readback-28080-attempt-1.http-status", "1-post-network-readback-28080-attempt-1.exit"} {
					if _, statErr := os.Stat(filepath.Join(dir, name)); statErr != nil {
						t.Fatalf("first 503 evidence missing: %s: %v", name, statErr)
					}
				}
			} else if scenario.name == "permanent-503" {
				count, countErr := strconv.Atoi(strings.TrimSpace(string(attempts)))
				if countErr != nil || count < 2 || !bytes.Contains(output, []byte("category=unavailable-timeout")) {
					t.Fatalf("permanent 503 was not bounded and classified: attempts=%s output=%s", attempts, output)
				}
			} else if string(attempts) != "1\n" {
				t.Fatalf("non-retryable response attempts=%s", attempts)
			}
		})
	}
}

func TestQualificationShutdownSnapshotWatch(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nshutdown_watch() {\n")
	end := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual capture/watch functions missing")
	}
	for _, scenario := range []string{"success", "learner", "generic-list", "empty-rv", "continued", "wrong-uid", "wrong-namespace", "wrong-run", "missing-writer", "duplicate-writer", "missing-learner", "list-error"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			writeJSON := func(name string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			var before, items []any
			for i := range 3 {
				name, uid := fmt.Sprintf("rhiza-voter-%d", i), fmt.Sprintf("uid-%d", i)
				before = append(before, map[string]any{"name": name, "uid": uid})
				if i == 0 && scenario == "wrong-uid" {
					uid = "replacement"
				}
				if i == 0 && scenario == "missing-writer" {
					continue
				}
				ns, run := "synthetic", "a1b2c3d4"
				if scenario == "wrong-namespace" {
					ns = "foreign"
				}
				if scenario == "wrong-run" {
					run = "other"
				}
				items = append(items, map[string]any{"metadata": map[string]any{"name": name, "uid": uid, "namespace": ns, "labels": map[string]any{"app": "rhiza-voter", "chaos.rhiza.io/run": run}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "rhiza", "image": "pinned"}}}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://" + name, "imageID": "pinned", "restartCount": 0}}}})
			}
			learner := map[string]any{"metadata": map[string]any{"name": "rhiza-learner", "uid": "learner-uid", "namespace": "synthetic", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "rhiza", "image": "pinned"}}}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://learner", "imageID": "pinned", "restartCount": 0}}}}
			if scenario == "learner" {
				items = append(items, learner)
			}
			if scenario == "duplicate-writer" {
				items = append(items, items[0])
			}
			kind, rv, continuation := "PodList", "opaque/&?=rv", ""
			if scenario == "generic-list" {
				kind = "List"
			}
			if scenario == "empty-rv" {
				rv = ""
			}
			if scenario == "continued" {
				continuation = "next-page"
			}
			writeJSON("voters-before.json", before)
			writeJSON("learner-created.json", learner)
			writeJSON("list.json", map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"resourceVersion": rv, "continue": continuation}, "items": items})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; mode=run-local; local_deadline=9999999999
learner_attempted=false; case "$FAKE_SCENARIO" in learner|missing-learner) learner_attempted=true;; esac
shutdown_log_pids=''; shutdown_watch_pid=''; shutdown_capture_state=idle
k() {
 [ "$#" = 2 ] && [ "$1" = get ] && [ "$2" = '--raw=/api/v1/namespaces/synthetic/pods?labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4' ] || return 91
 printf 'snapshot\n' > "$out/transport-order"
 [ "$FAKE_SCENARIO" != list-error ] || return 53
 cat "$out/list.json"
}
shutdown_stream() {
 if [ "$1" = get ]; then
  case "$2" in '--raw=/api/v1/namespaces/synthetic/pods?watch=true&resourceVersion=opaque%2F%26%3F%3Drv&labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4&timeoutSeconds='*) ;; *) return 92;; esac
  [ "$(cat "$out/transport-order")" = snapshot ] || return 93
  printf '%s\n' "$2" > "$out/watch-command"
  printf '{"type":"ERROR","object":{"code":410}}\n'
 else
  [ "$1" = logs ] && [ "$3" = --container=rhiza ] && [ "$4" = --follow ] || return 94
  printf 'fixture log\n'
 fi
}
`+string(source[start:end])+`
capture_shutdown
[ "$shutdown_capture_state" = active ]
wait "$shutdown_watch_pid"
for log_owner in $shutdown_log_pids; do wait "${log_owner#*:}"; done`, "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("snapshot fixture timeout: %v", ctx.Err())
			}
			_, watchErr := os.Stat(filepath.Join(dir, "watch-command"))
			if scenario == "success" || scenario == "learner" {
				if err != nil || watchErr != nil {
					t.Fatalf("snapshot/watch: %v (%v): %s", err, watchErr, output)
				}
				stream, err := os.ReadFile(filepath.Join(dir, "shutdown-watch.json"))
				if err != nil || !bytes.Contains(stream, []byte(`"code":410`)) {
					t.Fatal("native watch error stream was not retained")
				}
				// Capture is not a proof of Close: the existing proof parser must
				// still reject this ERROR event; this fixture only checks transport.
			} else if err == nil || !errors.Is(watchErr, os.ErrNotExist) {
				t.Fatalf("invalid snapshot reached watch: %v (%v): %s", err, watchErr, output)
			}
		})
	}
}

func TestQualificationShutdownCaptureLifecycle(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	end := strings.Index(string(source), "\nverify_voter_absence() {\n")
	if start < 0 || end <= start {
		t.Fatal("shutdown finish lifecycle missing")
	}
	finish := string(source[start:end])
	for _, scenario := range []string{"normal", "proof-failure", "pod-exit1", "shutdown-failed", "collector-failure", "partial-child"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fixture := `
set +e
out=$1; SCENARIO=$2; shutdown_capture_state=active; shutdown_watch_deadline=9999999999
(trap 'exit 143' TERM; while :; do sleep 1; done) & shutdown_watch_pid=$!
sleep 0.05
shutdown_log_pids=''
count=2; [ "$SCENARIO" = partial-child ] && count=1
i=0
while [ "$i" -lt "$count" ]; do
  if [ "$SCENARIO" = collector-failure ] && [ "$i" = 0 ]; then (exit 1) & else (exit 0) & fi
  shutdown_log_pids="$shutdown_log_pids pod-$i:$!"
  i=$((i+1))
done
original_watch=$shutdown_watch_pid
original_logs=$shutdown_log_pids
wait_shutdown_watch() { [ "$SCENARIO" != proof-failure ]; }
k() { case "$*" in 'get statefulset/rhiza-voter -o json') printf '{}';; get\ pods\ *) printf '{"items":[]}';; *) return 91;; esac; }
verify_shutdown() { case "$SCENARIO" in pod-exit1|shutdown-failed) return 1;; *) return 0;; esac; }
` + finish + `
finish_shutdown_capture; first=$?
for evidence in shutdown-watch-proof.exit shutdown-watch.exit shutdown-log-collectors.exit shutdown-finish.exit; do cp "$out/$evidence" "$out/$evidence.first" || exit 92; done
for owner in $original_logs; do cp "$out/${owner%:*}-shutdown-log.exit" "$out/${owner%:*}-shutdown-log.exit.first" || exit 93; done
finish_shutdown_capture; second=$?
[ "$first" = "$second" ] || exit 94
for evidence in shutdown-watch-proof.exit shutdown-watch.exit shutdown-log-collectors.exit shutdown-finish.exit; do cmp "$out/$evidence.first" "$out/$evidence" || exit 95; done
for owner in $original_logs; do
  cmp "$out/${owner%:*}-shutdown-log.exit.first" "$out/${owner%:*}-shutdown-log.exit" || exit 96
  [ "$(cat "$out/${owner%:*}-shutdown-log.exit")" != 127 ] || exit 97
  kill -0 "${owner#*:}" 2>/dev/null && exit 98
done
kill -0 "$original_watch" 2>/dev/null && exit 99
printf '%s %s\n' "$first" "$second"
`
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture, "fixture", dir, scenario)
			output, err := command.CombinedOutput()
			if err != nil || ctx.Err() != nil {
				t.Fatalf("lifecycle scenario=%s err=%v context=%v output=%s", scenario, err, ctx.Err(), output)
			}
			want := "0 0\n"
			if scenario != "normal" && scenario != "partial-child" {
				want = "1 1\n"
			}
			if string(output) != want {
				t.Fatalf("lifecycle scenario=%s result=%q want=%q", scenario, output, want)
			}
			collector, err := os.ReadFile(filepath.Join(dir, "shutdown-log-collectors.exit"))
			if err != nil {
				t.Fatal(err)
			}
			if (strings.TrimSpace(string(collector)) == "1") != (scenario == "collector-failure") {
				t.Fatalf("collector result scenario=%s: %s", scenario, collector)
			}
		})
	}
}

func TestQualificationShutdownFinishSignalReapsOutstandingChildren(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	end := strings.Index(string(source), "\nverify_voter_absence() {\n")
	if start < 0 || end <= start {
		t.Fatal("shutdown finish/reaper lifecycle missing")
	}
	functions := string(source[start:end])
	dir := t.TempDir()
	fixture := `
set +e
out=$1; shutdown_capture_state=active; shutdown_watch_deadline=9999999999
(trap 'exit 143' TERM; while :; do sleep 1; done) & shutdown_watch_pid=$!; printf '%s\n' "$shutdown_watch_pid" > "$out/watch.pid"
(exit 0) & first_log_pid=$!; printf '%s\n' "$first_log_pid" > "$out/first.pid"
(trap 'exit 143' TERM; while :; do sleep 1; done) & second_log_pid=$!; printf '%s\n' "$second_log_pid" > "$out/second.pid"
shutdown_log_pids=" pod-0:$first_log_pid pod-1:$second_log_pid"
shutdown_finish_watch_pid=''; shutdown_finish_log_pids=''
wait_shutdown_watch() { return 0; }
k() { return 91; }
verify_shutdown() { return 91; }
` + functions + `
trap 'cleanup_shutdown_capture_children; printf "%s\n" "$?" > "$out/exit-cleanup.exit"; printf "%s\n" "$shutdown_capture_state" > "$out/final.state"' EXIT
trap 'exit 130' TERM INT
(while [ ! -f "$out/pod-0-shutdown-log.exit" ]; do sleep 0.01; done; cp "$out/pod-0-shutdown-log.exit" "$out/pod-0-shutdown-log.exit.first"; kill -TERM $$) &
finish_shutdown_capture
exit 94
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture, "fixture", dir)
	output, err := command.CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 130 || ctx.Err() != nil {
		t.Fatalf("signal finish result=%v context=%v output=%s", err, ctx.Err(), output)
	}
	for _, name := range []string{"watch.pid", "first.pid", "second.pid"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || syscall.Kill(pid, 0) == nil {
			t.Fatalf("child not reaped: %s pid=%d err=%v", name, pid, err)
		}
	}
	first, err := os.ReadFile(filepath.Join(dir, "pod-0-shutdown-log.exit"))
	if err != nil || string(first) != "0\n" {
		t.Fatalf("first collector evidence changed: %q err=%v", first, err)
	}
	frozen, err := os.ReadFile(filepath.Join(dir, "pod-0-shutdown-log.exit.first"))
	if err != nil || !bytes.Equal(first, frozen) {
		t.Fatalf("first collector evidence not preserved: %q err=%v", frozen, err)
	}
	for _, name := range []string{"pod-1-shutdown-log-cleanup.exit", "shutdown-watch-cleanup.exit", "shutdown-finish.exit", "exit-cleanup.exit"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || strings.TrimSpace(string(data)) == "127" {
			t.Fatalf("cleanup evidence %s=%q err=%v", name, data, err)
		}
	}
	state, err := os.ReadFile(filepath.Join(dir, "final.state"))
	if err != nil || string(state) != "finished\n" {
		t.Fatalf("final capture state=%q err=%v", state, err)
	}
}

func TestQualificationShutdownWatchCommand(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nshutdown_stream() {\n")
	end := strings.Index(string(source), "\ncapture_shutdown() {\n")
	if start < 0 || end <= start || !strings.Contains(string(source), `shutdown_watch "$resource_version" "$shutdown_watch_deadline"`) {
		t.Fatal("actual scoped shutdown watch/caller missing")
	}
	for _, scenario := range []struct {
		name, mode, version string
		deadline, want      int
	}{
		{"local", "run-local", "1791476651663055017", 340, 0},
		{"ci", "run", "1791476651663055017", 340, 0},
		{"encoded-version", "run-local", "rv/&?=next", 340, 0},
		{"empty-version", "run-local", "", 340, 1},
		{"deadline", "run-local", "1791476651663055017", 100, 124},
		{"client-error", "run-local", "1791476651663055017", 340, 53},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range map[string]string{
				"date":    "#!/bin/sh\nprintf '100\\n'\n",
				"timeout": "#!/bin/sh\n[ \"$1\" = --kill-after=5s ] && [ \"$2\" = \"$FAKE_LIMIT\" ] || exit 91\nshift 2\nexec \"$@\"\n",
				"kubectl": `#!/bin/sh
if [ "$FAKE_MODE" = run-local ]; then [ "$1" = --kubeconfig=/synthetic/private/config ] || exit 92; shift; fi
[ "$1" = --context=synthetic ] && [ "$2" = --namespace=rhiza-v0191-20261008-a1b2c3d4 ] && [ "$3" = --request-timeout=0 ] || exit 93
shift 3
[ "$#" = 2 ] && [ "$1" = get ] && [ "$2" = "$FAKE_RAW" ] || exit 94
touch "$FAKE_DIR/called"
[ "$FAKE_SCENARIO" != client-error ] || exit 53
printf '{"type":"ERROR","object":{"code":410}}\n'
`,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			version := scenario.version
			if scenario.name == "encoded-version" {
				version = "rv%2F%26%3F%3Dnext"
			}
			limit := "240s"
			if scenario.mode == "run-local" {
				limit = "7s"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
mode=$FAKE_MODE; context=synthetic; ns=rhiza-v0191-20261008-a1b2c3d4
RHIZA_RUN_ID=a1b2c3d4; RHIZA_LOCAL_RUNTIME_KUBECONFIG=/synthetic/private/config
remaining() { printf '7\n'; }
`+string(source[start:end])+`
shutdown_watch "$1" "$2"`, "fixture", scenario.version, strconv.Itoa(scenario.deadline))
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_MODE="+scenario.mode, "FAKE_SCENARIO="+scenario.name, "FAKE_LIMIT="+limit,
				"FAKE_RAW=--raw=/api/v1/namespaces/rhiza-v0191-20261008-a1b2c3d4/pods?watch=true&resourceVersion="+version+"&labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4&timeoutSeconds=240")
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("watch fixture did not exit normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			_, called := os.Stat(filepath.Join(dir, "called"))
			if scenario.name == "empty-version" || scenario.name == "deadline" {
				if !errors.Is(called, os.ErrNotExist) {
					t.Fatal("invalid watch input reached transport")
				}
			} else if called != nil || (scenario.want == 0 && !bytes.Contains(output, []byte(`"code":410`))) {
				t.Fatal("native watch response/error was not preserved")
			}
		})
	}
}

func TestQualificationVoterReadiness(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nk() {\n")
	end := strings.Index(string(source), "\nprivate_file() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual scoped kubectl/readiness functions missing")
	}
	functions := string(source[start:end])
	for _, caller := range []string{"run voters-ready wait_voters 180", "run voters-ready wait_voters 120"} {
		if !strings.Contains(string(source), caller) {
			t.Fatalf("shared readiness caller missing: %s", caller)
		}
	}
	if strings.Contains(string(source), "rollout status") {
		t.Fatal("OnDelete voters must not use RollingUpdate rollout status")
	}
	template, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil || !bytes.Contains(template, []byte("updateStrategy: {type: OnDelete}")) {
		t.Fatal("deliberate OnDelete strategy changed")
	}
	for _, scenario := range []struct {
		name   string
		mode   string
		budget int
		want   int
	}{
		{"initial", "run", 180, 0},
		{"postfault", "run-local", 120, 0},
		{"absent", "run", 180, 1},
		{"create-timeout", "run-local", 120, 124},
		{"ready-timeout", "run", 180, 124},
		{"deadline", "run-local", 120, 124},
		{"missing", "run", 180, 1},
		{"not-ready", "run-local", 120, 1},
		{"terminating", "run", 180, 1},
		{"replacement", "run-local", 120, 1},
		{"get-error", "run", 180, 53},
		{"empty", "run-local", 120, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, content string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write("clock", "100\n")
			write("date", "#!/bin/sh\ncat \"$FAKE_DIR/clock\"\n")
			write("timeout", `#!/bin/sh
[ "$1" = --kill-after=5s ] || exit 91
seconds=${2%s}
[ "$seconds" -gt 0 ] && [ "$seconds" -le "$FAKE_BUDGET" ] || exit 92
printf '%s\n' "$seconds" >> "$FAKE_DIR/timeouts"
shift 2
exec "$@"
`)
			write("kubectl", `#!/bin/sh
case "$1" in --kubeconfig=*) [ "$1" = --kubeconfig=/synthetic/config ] || exit 93; shift ;; esac
[ "$1" = --context=synthetic ] && [ "$2" = --namespace=synthetic ] || exit 94
shift 2
case "$1" in --request-timeout=30s) shift ;; esac
printf '%s\n' "$*" >> "$FAKE_DIR/calls"
now=$(cat "$FAKE_DIR/clock")
step=10; [ "$FAKE_SCENARIO" != deadline ] || step=40
printf '%s\n' "$((now + step))" > "$FAKE_DIR/clock"
case "$1" in
 wait)
  case "$2" in pod/rhiza-voter-[012]) ;; *) exit 95 ;; esac
  case "$4" in --timeout=*) ;; *) exit 96 ;; esac
  case "$3" in
   --for=create)
    [ "$FAKE_SCENARIO" != absent ] || exit 1
    [ "$FAKE_SCENARIO" != create-timeout ] || exit 124
    touch "$FAKE_DIR/${2#pod/}" ;;
   --for=condition=Ready)
    [ -f "$FAKE_DIR/${2#pod/}" ] || exit 97
    [ "$FAKE_SCENARIO" != ready-timeout ] || exit 124 ;;
   *) exit 98 ;;
  esac ;;
 get)
  [ "$*" = 'get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json' ] || exit 99
  if [ -e "$FAKE_DIR/got-before" ]; then
   [ "$FAKE_SCENARIO" != get-error ] || exit 53
   [ "$FAKE_SCENARIO" != empty ] || exit 0
   cat "$FAKE_DIR/after.json"
  else touch "$FAKE_DIR/got-before"; cat "$FAKE_DIR/before.json"; fi ;;
 *) exit 90 ;;
esac
`)
			pods := make([]map[string]any, 3)
			for i := range pods {
				pods[i] = map[string]any{"metadata": map[string]any{"name": fmt.Sprintf("rhiza-voter-%d", i), "uid": fmt.Sprintf("uid-%d", i)},
					"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
			}
			before, err := json.Marshal(map[string]any{"items": pods})
			if err != nil {
				t.Fatal(err)
			}
			write("before.json", string(before))
			switch scenario.name {
			case "missing":
				pods = pods[:2]
			case "not-ready":
				pods[0]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False"}}}
			case "terminating":
				pods[0]["metadata"].(map[string]any)["deletionTimestamp"] = "2026-10-09T00:00:00Z"
			case "replacement":
				pods[0]["metadata"].(map[string]any)["uid"] = "replacement-uid"
			}
			after, err := json.Marshal(map[string]any{"items": pods})
			if err != nil {
				t.Fatal(err)
			}
			write("after.json", string(after))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", "mode=$FAKE_MODE; context=synthetic; ns=synthetic; RHIZA_LOCAL_RUNTIME_KUBECONFIG=/synthetic/config; remaining() { printf '300\\n'; }; "+functions+"\nwait_voters \"$FAKE_BUDGET\"; code=$?; [ -z \"${voter_wait_deadline:-}\" ] || exit 89; exit \"$code\"")
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name,
				"FAKE_MODE="+scenario.mode, "FAKE_BUDGET="+strconv.Itoa(scenario.budget))
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			if scenario.want == 0 {
				calls, err := os.ReadFile(filepath.Join(dir, "calls"))
				if err != nil || strings.Count(string(calls), "--for=create") != 3 || strings.Count(string(calls), "--for=condition=Ready") != 3 {
					t.Fatalf("named creation/readiness calls missing: %s error=%v", calls, err)
				}
				timeouts, err := os.ReadFile(filepath.Join(dir, "timeouts"))
				lastTimeout := scenario.budget - 70
				if scenario.mode == "run-local" && lastTimeout > 30 {
					lastTimeout = 30
				}
				if err != nil || !strings.HasPrefix(string(timeouts), strconv.Itoa(scenario.budget)+"\n") || !strings.HasSuffix(string(timeouts), strconv.Itoa(lastTimeout)+"\n") || !strings.Contains(string(calls), "--timeout="+strconv.Itoa(scenario.budget-60)+"s") {
					t.Fatalf("shared deadline reset: %q error=%v", timeouts, err)
				}
			}
		})
	}
}

func TestQualificationMetadataCredentialReadiness(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nmetadata_credentials_ready_inner() (\n")
	end := strings.Index(string(source), "\nwait_voters() (\n")
	if start < 0 || end <= start {
		t.Fatal("actual metadata readiness function missing")
	}
	invalidResponses := map[string]string{
		"missing-token":   `{"token_type":"Bearer","expires_in":100}`,
		"empty-token":     `{"access_token":"","token_type":"Bearer","expires_in":100}`,
		"token-type":      `{"access_token":123,"token_type":"Bearer","expires_in":100}`,
		"bearer-type":     `{"access_token":"synthetic-token-never-public","token_type":123,"expires_in":100}`,
		"expiry-type":     `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":"100"}`,
		"expiry-zero":     `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":0}`,
		"expiry-negative": `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":-1}`,
	}
	for _, scenario := range []struct {
		name             string
		want, attempts   int
		metadataCategory string
	}{
		{"ready", 0, 1, "token-valid"}, {"post-token-drift", 1, 1, "token-valid"}, {"post-token-native-body", 1, 1, "token-valid"}, {"principal-uri-identity", 1, 0, ""}, {"legacy-alias-identity", 1, 0, ""},
		{"wrong-run-gsa-email", 1, 0, ""}, {"wrong-project-gsa-email", 1, 0, ""}, {"wrong-gsa-email", 1, 0, ""},
		{"node-default-identity", 1, 0, ""}, {"wrong-identity", 1, 0, ""}, {"identity-http", 1, 0, ""}, {"identity-native", 1, 0, ""},
		{"propagation-then-ready", 0, 2, "propagation-pending"}, {"whitespace-then-ready", 0, 2, "propagation-pending"},
		{"arbitrary-403-then-ready", 0, 2, "propagation-pending"},
		{"plain-propagation-then-ready", 0, 2, "propagation-pending"}, {"plain-whitespace-then-ready", 0, 2, "propagation-pending"},
		{"backward-wall-then-ready", 0, 2, "propagation-pending"}, {"propagation-exhausted", 124, 23, "propagation-pending"},
		{"connection", 1, 1, "native-error"}, {"server", 1, 1, "unknown"}, {"not-found", 1, 1, "unknown"}, {"rate-limit", 1, 1, "unknown"},
		{"unknown-native", 1, 1, "native-error"}, {"unknown-http", 1, 1, "unknown"}, {"context-deadline", 124, 1, "deadline"},
		{"invalid", 1, 1, "invalid-response"}, {"multiple", 1, 1, "invalid-response"}, {"deadline", 124, 1, "token-valid"},
		{"budget-reserve", 124, 0, ""}, {"missing-token", 1, 1, "invalid-response"}, {"empty-token", 1, 1, "invalid-response"},
		{"token-type", 1, 1, "invalid-response"}, {"bearer-type", 1, 1, "invalid-response"},
		{"expiry-type", 1, 1, "invalid-response"}, {"expiry-zero", 1, 1, "invalid-response"}, {"expiry-negative", 1, 1, "invalid-response"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range map[string]string{"clock": "1000\n", "attempts": "0\n", "identity-attempts": "0\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "cluster.json"), []byte(`{"nodePools":[{"name":"fixture","workloadMetadataMode":"GKE_METADATA"}]}`), 0600); err != nil {
				t.Fatal(err)
			}
			invalid, malformed := invalidResponses[scenario.name]
			if malformed {
				if err := os.WriteFile(filepath.Join(dir, "response"), []byte(invalid), 0600); err != nil {
					t.Fatal(err)
				}
			}
			localDeadline, authExpiry, expectedDeadline := 1420, 3000, 1420
			if scenario.name == "budget-reserve" {
				authExpiry, expectedDeadline = 2200, 1000
			}
			fixture := `
mode=run-local; local_deadline=$FAKE_LOCAL_DEADLINE; auth_expiry=$FAKE_AUTH_EXPIRY; RHIZA_RUN_ID=a1b2c3d4; ns=rhiza-v0191-20261008-a1b2c3d4; data_gsa_email=rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com; RHIZA_NODE_A=gke-test-cluster-fixture-a; RHIZA_NODE_B=gke-test-cluster-fixture-b; RHIZA_NODE_C=gke-test-cluster-fixture-c; RHIZA_CLUSTER_METADATA="$FAKE_DIR/cluster.json"
date() { cat "$FAKE_DIR/clock"; }
sleep() { now=$(date); case "$FAKE_SCENARIO" in backward-wall-then-ready) now=$((now - 100));; esac; printf '%s\n' "$((now + $1))" > "$FAKE_DIR/clock"; }
k() {
 [ "$metadata_wait_deadline" = "$FAKE_EXPECTED_DEADLINE" ] || return 96
 case "$*" in
  *'/instance/service-accounts/default/email')
   identity_attempt=$(cat "$FAKE_DIR/identity-attempts"); identity_attempt=$((identity_attempt + 1)); printf '%s\n' "$identity_attempt" > "$FAKE_DIR/identity-attempts"
   case "$FAKE_SCENARIO" in
    principal-uri-identity) printf 'principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/example-project.svc.id.goog/subject/ns/rhiza-v0191-20261008-a1b2c3d4/sa/rhiza-gcs\n200' ;;
    legacy-alias-identity) printf 'rhiza-v0191-gcs-a1b2c3d4.svc.id.goog\n200' ;;
    wrong-run-gsa-email) printf 'rhiza-v0191-gcs-deadbeef@example-project.iam.gserviceaccount.com\n200' ;;
    wrong-project-gsa-email) printf 'rhiza-v0191-gcs-a1b2c3d4@other-project.iam.gserviceaccount.com\n200' ;;
    wrong-gsa-email) printf 'other@example-project.iam.gserviceaccount.com\n200' ;;
    node-default-identity) printf '123456789012-compute@developer.gserviceaccount.com\n200' ;;
    wrong-identity) printf 'unrelated.svc.id.goog\n200' ;;
    identity-http) printf 'rhiza-v0191-gcs-a1b2c3d4.svc.id.goog\n404' ;;
    identity-native) return 7 ;;
    post-token-drift) if [ "$identity_attempt" = 1 ]; then printf 'rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\n200'; else printf 'other@example-project.iam.gserviceaccount.com\n200'; fi ;;
    post-token-native-body) if [ "$identity_attempt" = 1 ]; then printf 'rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\n200'; else printf 'synthetic-identity-body-never-public'; return 7; fi ;;
    *) printf 'rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\n200' ;;
   esac
   return 0 ;;
  *'/instance/service-accounts/default/token') ;;
  *) return 95 ;;
 esac
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 printf 'synthetic-token-never-public\n' >&2
 case "$FAKE_SCENARIO" in
  propagation-then-ready|backward-wall-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' '{"error":{"code":403,"message":"loading GenerateAccessToken(\"rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com\"): Permission iam.serviceAccounts.getAccessToken denied on resource","status":"PERMISSION_DENIED"}}'; return 0; } ;;
  arbitrary-403-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' 'synthetic-arbitrary-forbidden-body-never-public'; return 0; } ;;
  whitespace-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403\r' '{ "error" : { "status" : "PERMISSION_DENIED", "message" : "Permission  iam.serviceAccounts.getAccessToken denied.\nGenerateAccessToken ( rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com )", "code" : 403 } }'; return 0; } ;;
  plain-propagation-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).'; return 0; } ;;
  plain-whitespace-then-ready) [ "$attempt" != 1 ] || { printf 'HTTP/403:\tgeneric::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com", ""):\r\ngoogleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).\n403'; return 0; } ;;
  propagation-exhausted) printf '%s\n403' '{"error":{"status":"PERMISSION_DENIED","message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com failed: iam.serviceAccounts.getAccessToken permission denied","code":403}}'; return 0 ;;
  nonretryable-403) printf 'Permission denied for an unrelated reason\n403'; return 0 ;;
  wrong-gsa-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-deadbeef@example-project.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  wrong-permission-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com: iam.serviceAccounts.actAs denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  wrong-operation-403) printf '%s\n403' '{"error":{"code":403,"message":"SignBlob for rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  noncanonical-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"FORBIDDEN"}}'; return 0 ;;
  plain-wrong-gsa-403) printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("wrong@example-project.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).'; return 0 ;;
  plain-wrong-permission-403) printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.actAs'\'' denied on resource (or it may not exist).'; return 0 ;;
  plain-arbitrary-403) printf '%s\n403' 'permission denied while GenerateAccessToken rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com iam.serviceAccounts.getAccessToken'; return 0 ;;
  connection) [ "$attempt" != 1 ] || return 7 ;;
  server) if [ "$attempt" = 1 ]; then printf 'synthetic-token-never-public\n503'; return 0; fi ;;
  not-found) printf 'synthetic-token-never-public\n404'; return 0 ;;
  rate-limit) printf 'synthetic-token-never-public\n429'; return 0 ;;
  unknown-native) return 53 ;;
  unknown-http) printf 'synthetic-token-never-public\n401'; return 0 ;;
  context-deadline) return 124 ;;
  invalid) printf '{"access_token":"synthetic-token-never-public","token_type":"Other","expires_in":100}\n200'; return 0 ;;
  missing-token|empty-token|token-type|bearer-type|expiry-type|expiry-zero|expiry-negative) cat "$FAKE_RESPONSE"; printf '\n200'; return 0 ;;
  multiple) printf '{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":100}\n';;
  deadline) printf '%s\n' "$FAKE_EXPECTED_DEADLINE" > "$FAKE_DIR/clock" ;;
 esac
 printf '{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":100}\n200'
}

`
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[start:end])+`
if [ "$FAKE_SCENARIO" = post-token-drift ]; then /bin/sleep 30 & printf '%s\n' "$!" > "$FAKE_DIR/orphan-pid"; fi
metadata_credentials_ready_inner`)
			command.WaitDelay = time.Second
			command.Env = append(os.Environ(), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name, "FAKE_RESPONSE="+filepath.Join(dir, "response"),
				"FAKE_LOCAL_DEADLINE="+strconv.Itoa(localDeadline), "FAKE_AUTH_EXPIRY="+strconv.Itoa(authExpiry), "FAKE_EXPECTED_DEADLINE="+strconv.Itoa(expectedDeadline))
			started := time.Now()
			output, runErr := command.CombinedOutput()
			elapsed := time.Since(started)
			if scenario.name == "post-token-drift" {
				pidFile := filepath.Join(dir, "orphan-pid")
				data, err := os.ReadFile(pidFile)
				if err != nil {
					t.Fatalf("orphan PID missing: %v", err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
				if err != nil || pid <= 0 {
					t.Fatalf("invalid orphan PID %q: %v", data, err)
				}
				func() {
					defer syscall.Kill(pid, syscall.SIGKILL)
					if err := syscall.Kill(pid, 0); err != nil {
						t.Fatalf("orphan process was not live: %v", err)
					}
				}()
				_ = os.Remove(pidFile)
			}
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate: %v", runErr)
				}
				code = exited.ExitCode()
			}
			attempts, err := os.ReadFile(filepath.Join(dir, "attempts"))
			if err != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, err)
			}
			if bytes.Contains(output, []byte("synthetic-token-never-public")) || bytes.Contains(output, []byte("synthetic-identity-body-never-public")) || bytes.Contains(output, []byte("synthetic-arbitrary-forbidden-body-never-public")) || bytes.Contains(output, []byte("access_token")) {
				t.Fatal("credential response leaked into public output")
			}
			if scenario.name == "post-token-drift" && (elapsed < 750*time.Millisecond || elapsed >= 5*time.Second || !bytes.Contains(output, []byte("category=identity-drift"))) {
				t.Fatalf("post-token drift did not terminate safely: elapsed=%s output=%s", elapsed, output)
			}
			if scenario.metadataCategory != "" && !bytes.Contains(output, []byte("category="+scenario.metadataCategory)) {
				t.Fatalf("metadata category=%s missing from %s", scenario.metadataCategory, output)
			}
			if scenario.metadataCategory == "propagation-pending" && !bytes.Contains(output, []byte("http=403")) {
				t.Fatalf("status-only propagation evidence missing from %s", output)
			}
			identityCategory := "linked-gsa-email"
			switch scenario.name {
			case "principal-uri-identity":
				identityCategory = "principal-uri"
			case "legacy-alias-identity":
				identityCategory = "legacy-ksa-alias"
			case "wrong-run-gsa-email", "wrong-project-gsa-email", "wrong-gsa-email":
				identityCategory = "other-gsa-email"
			case "node-default-identity":
				identityCategory = "node-default"
			case "wrong-identity":
				identityCategory = "unknown"
			case "identity-http":
				identityCategory = "http-error"
			case "identity-native":
				identityCategory = "native-error"
			case "budget-reserve":
				identityCategory = "budget-exhausted"
			}
			if !bytes.Contains(output, []byte("category="+identityCategory)) {
				t.Fatalf("metadata identity category=%s missing from %s", identityCategory, output)
			}
			for _, identity := range []string{
				"principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/example-project.svc.id.goog/subject/ns/rhiza-v0191-20261008-a1b2c3d4/sa/rhiza-gcs",
				"rhiza-v0191-gcs-a1b2c3d4.svc.id.goog",
				"rhiza-v0191-gcs-a1b2c3d4@example-project.iam.gserviceaccount.com",
				"rhiza-v0191-gcs-deadbeef@example-project.iam.gserviceaccount.com",
				"rhiza-v0191-gcs-a1b2c3d4@other-project.iam.gserviceaccount.com",
				"other@example-project.iam.gserviceaccount.com",
				"123456789012-compute@developer.gserviceaccount.com",
				"unrelated.svc.id.goog",
				"iam.serviceAccounts.getAccessToken",
				"iam.serviceAccounts.actAs",
				"Permission denied for an unrelated reason",
				"denied on some resource",
				"GenerateAccessToken",
				"PERMISSION_DENIED",
			} {
				if bytes.Contains(output, []byte(identity)) {
					t.Fatal("raw metadata identity leaked into public output")
				}
			}
			for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
				fields := strings.Fields(line)
				if fields[0] != "stage=metadata-credential-readiness" || !strings.HasPrefix(fields[len(fields)-1], "category=") {
					t.Fatalf("unexpected public evidence: %q", line)
				}
			}
			entries, err := os.ReadDir(dir)
			wantEntries := 4
			if malformed {
				wantEntries++
			}
			if err != nil || len(entries) != wantEntries {
				t.Fatalf("response persisted: entries=%v error=%v", entries, err)
			}
		})
	}
}

func TestQualificationMetadataReadinessOuterWatchdog(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nmetadata_credentials_ready() {\n")
	end := strings.Index(string(source), "\nvalidate_cluster_metadata() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual metadata readiness watchdog missing")
	}
	if !bytes.Contains(source[start:end], []byte("auth_expiry - 1200 - metadata_budget_now - metadata_kill_grace")) {
		t.Fatal("watchdog does not reserve kill grace before the full auth cleanup reserve")
	}
	if output, err := exec.Command("timeout", "--version").CombinedOutput(); err != nil || !bytes.Contains(output, []byte("GNU coreutils")) {
		t.Fatalf("GNU timeout required for monotonic watchdog regression: %v %s", err, output)
	}
	dir := t.TempDir()
	worker := filepath.Join(dir, "run-gcs-postrelease.sh")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fixture := `
mode=run-local
script_dir=$FAKE_SCRIPT_DIR
local_deadline=1006
auth_expiry=2206
# A frozen/backward wall clock cannot extend GNU timeout's monotonic budget.
date() { printf '%s\n' 1000; }
`
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[start:end])+"\nmetadata_credentials_ready")
	command.Env = append(os.Environ(), "FAKE_SCRIPT_DIR="+dir)
	started := time.Now()
	output, runErr := command.CombinedOutput()
	elapsed := time.Since(started)
	var exited *exec.ExitError
	if !errors.As(runErr, &exited) || exited.ExitCode() != 137 || ctx.Err() != nil {
		t.Fatalf("uncooperative worker escaped watchdog: error=%v output=%s", runErr, output)
	}
	if elapsed < 5*time.Second || elapsed > 8*time.Second {
		t.Fatalf("watchdog elapsed outside one-second budget plus five-second kill grace: %s", elapsed)
	}
}

func TestQualificationCIClusterMetadataReceipt(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	projection, err := os.ReadFile("../cluster-metadata-projection.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nvalidate_cluster_metadata() {\n")
	end := strings.Index(string(source), "\nwait_voters() (\n")
	privateStart := strings.Index(string(source), "\nprivate_file() {\n")
	privateEnd := strings.Index(string(source), "\nsed -e \"s|__NAMESPACE__|")
	helperStart := strings.Index(string(source), "\nsha256_file() {\n")
	helperEnd := strings.Index(string(source)[helperStart+1:], "\n}\n") + helperStart + 4
	if start < 0 || end <= start || privateStart < 0 || privateEnd <= privateStart || helperStart < 0 || helperEnd <= helperStart {
		t.Fatal("cluster metadata preparation functions missing")
	}
	for _, scenario := range []string{"full-json-compatible", "credential-fields", "describe-failure", "jq-failure", "jq-large-failure", "sanitizer-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			gcloud := `#!/bin/sh
[ "$*" = '--project=example-project --quiet container clusters describe test-cluster --location=test-region1 --format=json' ] || exit 97
[ "$FAKE_SCENARIO" != describe-failure ] || exit 53
[ "$FAKE_SCENARIO" != jq-failure ] || { printf '%s\n' 'not-json'; exit 0; }
if [ "$FAKE_SCENARIO" = jq-large-failure ]; then
  printf '%s\n' '{bad-json'
  n=0
  while [ "$n" -lt 8192 ]; do printf '%s\n' 'DO_NOT_RECORD_LARGE_RAW_PAYLOAD'; n=$((n + 1)); done
  exit 0
fi
cat "$FAKE_CLUSTER"
`
			if err := os.WriteFile(filepath.Join(bin, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			cluster := `{"name":"test-cluster","location":"test-region1","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"synthetic-ca","clientKey":"","clientCertificate":""},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`
			if scenario == "credential-fields" {
				cluster = `{"name":"test-cluster","location":"test-region1","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"synthetic-ca","clientKey":"DO_NOT_RECORD_GKE_KEY","clientCertificate":"DO_NOT_RECORD_GKE_CERT","token":"DO_NOT_RECORD_GKE_TOKEN","username":"DO_NOT_RECORD_GKE_USERNAME","password":"DO_NOT_RECORD_GKE_PASSWORD","exec":{"command":"DO_NOT_RECORD_GKE_EXEC"},"authProvider":{"name":"DO_NOT_RECORD_GKE_AUTH_PROVIDER"}},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`
			}
			if scenario == "sanitizer-failure" {
				cluster = `{"name":"wrong-cluster","location":"test-region1","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"DO_NOT_RECORD_GKE_CA"},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`
			}
			clusterPath := filepath.Join(dir, "fresh.json")
			if err := os.WriteFile(clusterPath, []byte(cluster), 0600); err != nil {
				t.Fatal(err)
			}
			callerPath := filepath.Join(dir, "caller.json")
			if err := os.WriteFile(callerPath, []byte(`{"nodePools":[]}`), 0600); err != nil {
				t.Fatal(err)
			}
			if scenario == "full-json-compatible" {
				limited := exec.Command(filepath.Join(bin, "gcloud"), "--project=example-project", "--quiet", "container", "clusters", "describe", "test-cluster", "--location=test-region1", "--format=json(name,location,endpoint,masterAuth.clusterCaCertificate,nodePools.name,nodePools.config.workloadMetadataConfig.mode)")
				limited.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario, "FAKE_CLUSTER="+clusterPath)
				var exited *exec.ExitError
				if err := limited.Run(); !errors.As(err, &exited) || exited.ExitCode() != 97 {
					t.Fatalf("limited gcloud projection incompatibility not reproduced: %v", err)
				}
			}
			fixture := `
mode=run; out="$FAKE_DIR"; RHIZA_CLUSTER_METADATA="$FAKE_CALLER"
RHIZA_NODE_A=gke-test-cluster-fixture-a; RHIZA_NODE_B=gke-test-cluster-fixture-b; RHIZA_NODE_C=gke-test-cluster-fixture-c
prepare_cluster_metadata || exit $?
[ "$RHIZA_CLUSTER_METADATA" = "$FAKE_DIR/cluster-metadata-validation.json" ] || exit 96
touch "$FAKE_DIR/metadata-pod-created"
`
			command := exec.Command("/bin/sh", "-c", string(projection)+string(source[helperStart:helperEnd])+string(source[privateStart:privateEnd])+string(source[start:end])+fixture)
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario,
				"FAKE_CLUSTER="+clusterPath, "FAKE_CALLER="+callerPath, "TMPDIR="+dir)
			output, runErr := command.CombinedOutput()
			if bytes.Contains(output, []byte("DO_NOT_RECORD")) || bytes.Contains(output, []byte("synthetic-ca")) || bytes.Contains(output, []byte("bad-json")) {
				t.Fatalf("raw cluster metadata leaked to diagnostics: %q", output)
			}
			if scenario == "full-json-compatible" {
				if runErr != nil {
					t.Fatalf("fresh CI metadata rejected: %v %s", runErr, output)
				}
				data, err := os.ReadFile(filepath.Join(dir, "cluster-metadata-validation.json"))
				if err != nil || bytes.Contains(data, []byte("masterAuth")) || bytes.Contains(data, []byte("synthetic-ca")) || !bytes.Contains(data, []byte("clusterCaSHA256")) || !bytes.Contains(data, []byte("GKE_METADATA")) {
					t.Fatalf("sanitized validation receipt invalid: %v %q", err, data)
				}
				if matches, _ := filepath.Glob(filepath.Join(dir, "rhiza-cluster-metadata.*")); len(matches) != 0 {
					t.Fatalf("raw CI metadata survived success: %v", matches)
				}
				return
			}
			if scenario == "credential-fields" || scenario == "jq-failure" || scenario == "jq-large-failure" || scenario == "sanitizer-failure" {
				if runErr == nil {
					t.Fatalf("cluster projection failure accepted: %s", scenario)
				}
				if _, err := os.Stat(filepath.Join(dir, "metadata-pod-created")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("credential-bearing metadata reached metadata pod creation")
				}
				for _, name := range []string{"cluster-metadata-validation.json", "cluster-metadata-validation.sha256", "cluster-metadata-projection-failure.json"} {
					if data, err := os.ReadFile(filepath.Join(dir, name)); err == nil && bytes.Contains(data, []byte("DO_NOT_RECORD_GKE")) {
						t.Fatalf("credential sentinel persisted in %s", name)
					}
				}
				failure, err := os.ReadFile(filepath.Join(dir, "cluster-metadata-projection-failure.json"))
				if err != nil || bytes.Contains(failure, []byte("synthetic-ca")) || bytes.Contains(failure, []byte("not-json")) {
					t.Fatalf("sanitized failure receipt invalid: %v %q", err, failure)
				}
				var expectedStage string
				switch scenario {
				case "credential-fields", "jq-failure", "jq-large-failure":
					expectedStage = "jq"
				case "sanitizer-failure":
					expectedStage = "sanitizer"
				}
				var receipt struct {
					Stage  string `json:"stage"`
					Exit   int    `json:"exit"`
					Reason string `json:"reason"`
				}
				if err := json.Unmarshal(failure, &receipt); err != nil || receipt.Stage != expectedStage || receipt.Exit == 0 || receipt.Reason == "" {
					t.Fatalf("failure stage receipt mismatch: %v %+v", err, receipt)
				}
				if matches, _ := filepath.Glob(filepath.Join(dir, "rhiza-cluster-metadata.*")); len(matches) != 0 {
					t.Fatalf("projected CI metadata/stderr survived rejection: %v", matches)
				}
				return
			}
			var exited *exec.ExitError
			if !errors.As(runErr, &exited) || exited.ExitCode() != 53 {
				t.Fatalf("describe failure exit=%v output=%s", runErr, output)
			}
			if _, err := os.Stat(filepath.Join(dir, "metadata-pod-created")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("describe failure reached metadata pod creation")
			}
			if matches, _ := filepath.Glob(filepath.Join(dir, "rhiza-cluster-metadata.*")); len(matches) != 0 {
				t.Fatalf("raw CI metadata/stderr survived failure: %v", matches)
			}
			failure, err := os.ReadFile(filepath.Join(dir, "cluster-metadata-projection-failure.json"))
			var receipt struct {
				Stage string `json:"stage"`
				Exit  int    `json:"exit"`
			}
			if err != nil || json.Unmarshal(failure, &receipt) != nil || receipt.Stage != "gcloud" || receipt.Exit != 53 {
				t.Fatalf("gcloud failure stage receipt invalid: %v %q", err, failure)
			}
		})
	}
}

func TestQualificationClusterProjectionSignalCleanupIsBounded(t *testing.T) {
	helper, err := filepath.Abs("../cluster-metadata-projection.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	producer := filepath.Join(dir, "producer")
	producerScript := `#!/bin/sh
trap '' TERM
printf '%s\n' "$PPID" > "$CAPTURE_PID_FILE"
printf '%s\n' "$$" > "$PRODUCER_PID_FILE"
exec sleep 30
`
	if err := os.WriteFile(producer, []byte(producerScript), 0700); err != nil {
		t.Fatal(err)
	}
	capturePIDFile := filepath.Join(dir, "capture.pid")
	producerPIDFile := filepath.Join(dir, "producer.pid")
	script := `
set -eu
. "$HELPER"
producer_wrapper() { exec "$PRODUCER" "$@"; }
capture_cluster_metadata_projection "$OUT" "$FAILURE" producer_wrapper
`
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", script)
	command.Env = append(os.Environ(), "HELPER="+helper, "OUT="+filepath.Join(dir, "projected.json"), "FAILURE="+filepath.Join(dir, "failure.json"), "PRODUCER="+producer, "CAPTURE_PID_FILE="+capturePIDFile, "PRODUCER_PID_FILE="+producerPIDFile)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	started := time.Now()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }()
	var capturePID int
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		output, err := os.ReadFile(capturePIDFile)
		if err != nil {
			continue
		}
		capturePID, err = strconv.Atoi(strings.TrimSpace(string(output)))
		if err == nil {
			break
		}
	}
	if capturePID == 0 {
		_ = command.Process.Kill()
		t.Fatal("capture subshell was not observed")
	}
	if err := syscall.Kill(capturePID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	runErr := command.Wait()
	elapsed := time.Since(started)
	if ctx.Err() != nil || elapsed > 6*time.Second {
		t.Fatalf("TERM-ignoring producer cleanup was not bounded: elapsed=%s err=%v", elapsed, runErr)
	}
	var exited *exec.ExitError
	if !errors.As(runErr, &exited) || exited.ExitCode() != 130 {
		t.Fatalf("signal cleanup exit mismatch: elapsed=%s err=%v", elapsed, runErr)
	}
	assertProcessExited(t, producerPIDFile, "producer")
	for _, name := range []string{"projected.json", "failure.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("signal cleanup retained private projection artifact %s: %v", name, err)
		}
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, ".cluster-projection.*")); len(matches) != 0 {
		entries, _ := os.ReadDir(matches[0])
		t.Fatalf("signal cleanup retained private projection directory: %v entries=%v", matches, entries)
	}
}

func TestQualificationClusterProjectionReapsTermIgnoringProjector(t *testing.T) {
	helper, err := filepath.Abs("../cluster-metadata-projection.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	projector := filepath.Join(dir, "projector")
	projectorScript := `#!/bin/sh
trap '' TERM
printf '%s\n' "$$" > "$PROJECTOR_PID_FILE"
printf '%s\n' "$PPID" > "$CONSUMER_PID_FILE"
exec sleep 30
`
	if err := os.WriteFile(projector, []byte(projectorScript), 0700); err != nil {
		t.Fatal(err)
	}
	producer := filepath.Join(dir, "producer")
	if err := os.WriteFile(producer, []byte("#!/bin/sh\nprintf '%s\\n' '{}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	projectorPIDFile := filepath.Join(dir, "projector.pid")
	consumerPIDFile := filepath.Join(dir, "consumer.pid")
	script := `
set -eu
. "$HELPER"
project_cluster_metadata() { exec "$PROJECTOR"; }
capture_cluster_metadata_projection "$OUT" "$FAILURE" "$PRODUCER"
`
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", script)
	command.Env = append(os.Environ(), "HELPER="+helper, "OUT="+filepath.Join(dir, "projected.json"), "FAILURE="+filepath.Join(dir, "failure.json"), "PROJECTOR="+projector, "PRODUCER="+producer, "PROJECTOR_PID_FILE="+projectorPIDFile, "CONSUMER_PID_FILE="+consumerPIDFile)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	started := time.Now()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }()
	consumerPID := waitForProcessPID(t, consumerPIDFile, "consumer")
	if err := syscall.Kill(consumerPID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	runErr := command.Wait()
	elapsed := time.Since(started)
	if ctx.Err() != nil || elapsed > 5*time.Second {
		t.Fatalf("TERM-ignoring projector cleanup was not bounded: elapsed=%s err=%v", elapsed, runErr)
	}
	var exited *exec.ExitError
	if !errors.As(runErr, &exited) || exited.ExitCode() != 130 {
		t.Fatalf("projector signal cleanup exit mismatch: elapsed=%s err=%v", elapsed, runErr)
	}
	assertProcessExited(t, projectorPIDFile, "projector")
	if matches, _ := filepath.Glob(filepath.Join(dir, ".cluster-projection.*")); len(matches) != 0 {
		t.Fatalf("signal cleanup retained private projection directory: %v", matches)
	}
}

func waitForProcessPID(t *testing.T, path, label string) int {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		output, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(output)))
		if err == nil && pid > 0 {
			return pid
		}
	}
	t.Fatalf("%s PID was not observed", label)
	return 0
}

func assertProcessExited(t *testing.T, path, label string) {
	t.Helper()
	pid := waitForProcessPID(t, path, label)
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("%s process %d remains after cleanup: %v", label, pid, err)
	}
}

func TestQualificationServerDryRunPrecedesEveryPersistentCreate(t *testing.T) {
	bootstrap, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	bootstrapText := string(bootstrap)
	renderCall := strings.Index(bootstrapText, "render > \"$RHIZA_AUTH_STATE/auth.yaml\"")
	policyRender := strings.Index(bootstrapText, `"$RHIZA_AUTH_STATE/auth.yaml" > "$RHIZA_AUTH_STATE/auth-admission-policies.yaml"`)
	policyPreflight := strings.Index(bootstrapText, `record kubernetes-auth-preflight k create --dry-run=server`)
	firstCloudCreate := strings.Index(bootstrapText, `record data-create create_account`)
	kubernetesCreate := strings.Index(bootstrapText, `record kubernetes-create k create -f "$RHIZA_AUTH_STATE/auth.yaml"`)
	if renderCall < 0 || policyRender < renderCall || policyPreflight <= policyRender ||
		firstCloudCreate <= renderCall ||
		firstCloudCreate <= policyPreflight || kubernetesCreate <= policyPreflight {
		t.Fatal("exact admission-policy server dry-run must precede persistent bootstrap creates")
	}
	if strings.Contains(bootstrapText[policyPreflight:kubernetesCreate], `mark kubernetes`) {
		t.Fatal("auth dry-run must not create an ownership marker")
	}
}

func TestQualificationMetadataOnlyCleanup(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\ncleanup_metadata_only() (\n")
	end := strings.Index(string(source), "\ntrap cleanup EXIT\n")
	captureStart := strings.Index(string(source), "\ncapture_shutdown() {\n")
	captureEnd := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	stopStart := strings.Index(string(source), "\nstop_forward() {\n")
	stopEnd := strings.Index(string(source), "\npf_pids=''\n")
	helperStart := strings.Index(string(source), "\nsha256_file() {\n")
	helperEnd := strings.Index(string(source)[helperStart+1:], "\n}\n") + helperStart + 4
	if start < 0 || end <= start || captureStart < 0 || captureEnd <= captureStart || stopStart < 0 || stopEnd <= stopStart || helperStart < 0 || helperEnd <= helperStart {
		t.Fatal("actual cleanup functions missing")
	}
	if !strings.Contains(string(source), "voters_attempted=false\n") || !strings.Contains(string(source), "voters_attempted=true\nrun create-voters") ||
		!strings.Contains(string(source), "run create-metadata k create -f \"$out/metadata.yaml\" -o json\ncp \"$out/$seq-create-metadata.stdout\" \"$out/metadata-created.json\"") {
		t.Fatal("create attempt/ACK ordering changed")
	}
	auth, err := os.ReadFile("../gcs-postrelease-auth.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"resources: [deployments, replicasets, daemonsets]\n    verbs: [list]", "resources: [cronjobs]\n    verbs: [list]"} {
		if !bytes.Contains(auth, []byte(rule)) {
			t.Fatalf("required list-only controller rule missing: %s", rule)
		}
	}
	nativeChecksum, err := exec.LookPath("sha256sum")
	if err != nil {
		t.Fatal(err)
	}
	nativeFind, err := exec.LookPath("find")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"metadata", "no-owned", "unknown-create", "partial-voters", "namespace-owner", "namespace-uid", "pod-uid", "run", "ksa", "image", "node", "rv", "owner-reference", "controller", "replicaset", "daemonset", "statefulset", "cronjob", "writer", "job", "fault", "list-error", "delete-error", "wait-timeout", "replacement", "delayed-child", "manifest-failure", "manifest-failure-existing", "manifest-malformed", "manifest-find-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "out")
			if err := os.Mkdir(out, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "sha256sum"), []byte(`#!/bin/sh
if [ "$FAKE_SCENARIO" = delayed-child ]; then
 child=$(cat "$FAKE_DIR/child") || exit 98
 kill -0 "$child" 2>/dev/null && exit 98
 printf 'begin\ncomplete\n' | cmp - "$FAKE_OUT/child-final.txt" || exit 98
 printf 'reaped-before-checksum\n' > "$FAKE_DIR/reap-at-seal"
fi
case "$FAKE_SCENARIO" in manifest-failure|manifest-failure-existing) exit 86 ;; esac
[ "$FAKE_SCENARIO" != manifest-malformed ] || { printf 'not-a-digest  %s\n' "$1"; exit 0; }
exec "$FAKE_NATIVE_CHECKSUM" "$@"
`), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "find"), []byte(`#!/bin/sh
if [ "$FAKE_SCENARIO" = manifest-find-failure ]; then
 printf './cleanup-owner.txt\n'
 exit 87
fi
exec "$FAKE_NATIVE_FIND" "$@"
`), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "delayed-child"), []byte(`#!/bin/sh
finish() {
 trap - TERM
 kill "$sleep_pid" 2>/dev/null || true
 wait "$sleep_pid" 2>/dev/null || true
 printf 'begin\n' > "$FAKE_OUT/child-final.txt"
 printf 'complete\n' >> "$FAKE_OUT/child-final.txt"
 exit 0
}
trap finish TERM
/bin/sleep 30 & sleep_pid=$!
printf 'armed\n' > "$FAKE_DIR/child-ready"
wait "$sleep_pid"
finish
`), 0700); err != nil {
				t.Fatal(err)
			}
			writeJSON := func(path string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			pod := map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "rhiza-metadata", "namespace": "synthetic", "uid": "metadata-uid", "resourceVersion": "7", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}},
				"spec": map[string]any{"serviceAccountName": "rhiza-gcs", "nodeName": "node-a", "containers": []any{map[string]any{"name": "metadata", "image": "metadata-digest"}}}}
			if scenario != "unknown-create" {
				writeJSON(filepath.Join(out, "metadata-created.json"), pod)
			}
			meta := pod["metadata"].(map[string]any)
			spec := pod["spec"].(map[string]any)
			namespace := map[string]any{"metadata": map[string]any{"uid": "namespace-uid", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}, "annotations": map[string]any{"rhiza.dev/auth-owner": "rhiza-postrelease-a1b2c3d4-owner"}}}
			switch scenario {
			case "namespace-owner":
				namespace["metadata"].(map[string]any)["annotations"] = map[string]any{}
			case "namespace-uid":
				namespace["metadata"].(map[string]any)["uid"] = "other"
			case "pod-uid":
				meta["uid"] = "other"
			case "run":
				meta["labels"] = map[string]any{"chaos.rhiza.io/run": "other"}
			case "ksa":
				spec["serviceAccountName"] = "other"
			case "image":
				spec["containers"].([]any)[0].(map[string]any)["image"] = "other"
			case "node":
				spec["nodeName"] = "other"
			case "rv":
				delete(meta, "resourceVersion")
			case "owner-reference":
				meta["ownerReferences"] = []any{map[string]any{"uid": "unexpected-controller"}}
			}
			items := []any{pod}
			switch scenario {
			case "writer", "job", "fault", "controller", "replicaset", "daemonset", "statefulset", "cronjob":
				kind := map[string]string{"writer": "Pod", "job": "Job", "fault": "NetworkChaos", "controller": "Deployment", "replicaset": "ReplicaSet", "daemonset": "DaemonSet", "statefulset": "StatefulSet", "cronjob": "CronJob"}[scenario]
				items = append(items, map[string]any{"kind": kind, "metadata": map[string]any{"name": "unexpected"}})
			}
			writeJSON(filepath.Join(dir, "namespace.json"), namespace)
			writeJSON(filepath.Join(dir, "actors.json"), map[string]any{"items": items})
			fixture := `
set +e
out=$FAKE_OUT; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; RHIZA_AUTH_CREATION_SHA=owner
RHIZA_BOOTSTRAP_UID=namespace-uid; RHIZA_METADATA_IMAGE=metadata-digest; RHIZA_NODE_A=node-a
mode=run; pf_pids=''; active_fault=''; learner_attempted=false; shutdown_capture_state=idle
shutdown_watch_pid=''; shutdown_log_pids=''; owned_created=true; voters_attempted=false
[ "$FAKE_SCENARIO" != no-owned ] || owned_created=false
[ "$FAKE_SCENARIO" != partial-voters ] || voters_attempted=true
if [ "$FAKE_SCENARIO" = delayed-child ]; then
 voters_attempted=true
 mkfifo "$FAKE_DIR/child-ready" || exit 98
 /bin/sh "$FAKE_DIR/bin/delayed-child" & pf_pids=$!
 printf '%s\n' "$pf_pids" > "$FAKE_DIR/child"
 IFS= read -r child_ready < "$FAKE_DIR/child-ready"
 [ "$child_ready" = armed ] || exit 98
 kill -0 "$pf_pids" || exit 98
fi
stop_watchdog() { :; }
k() {
 printf '%s\n' "$1" >> "$FAKE_DIR/calls"
 case "$*" in
  'get namespace synthetic -o json') cat "$FAKE_DIR/namespace.json" ;;
  'get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json')
   [ "$FAKE_SCENARIO" != list-error ] || return 53
   if [ -f "$FAKE_DIR/deleted" ]; then
    if [ "$FAKE_SCENARIO" = replacement ]; then cat "$FAKE_DIR/actors.json"; else printf '{"items":[]}'; fi
   else cat "$FAKE_DIR/actors.json"; fi ;;
  'delete --raw /api/v1/namespaces/synthetic/pods/rhiza-metadata -f '*)
   [ "$voter_wait_deadline" -gt "$(date +%s)" ] || return 124
   jq -e '.kind=="DeleteOptions" and .preconditions.uid=="metadata-uid" and .preconditions.resourceVersion=="7"' "$5" >/dev/null || return 95
   [ "$FAKE_SCENARIO" != delete-error ] || return 53
   touch "$FAKE_DIR/deleted" ;;
  'wait pod/rhiza-metadata --for=delete --timeout=90s') [ "$FAKE_SCENARIO" != wait-timeout ] || return 124 ;;
  'get pods --selector='*) printf '{"items":[]}' ;;
  logs\ *|scale\ *|wait\ *|delete\ *) [ "$voters_attempted" = true ] || return 96 ;;
  *) return 97 ;;
 esac
}
`
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			initialCode := 53
			if scenario == "manifest-failure" || scenario == "manifest-malformed" || scenario == "manifest-find-failure" {
				initialCode = 0
			}
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[helperStart:helperEnd])+string(source[stopStart:stopEnd])+string(source[captureStart:captureEnd])+string(source[start:end])+fmt.Sprintf("\n(exit %d); cleanup", initialCode))
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_OUT="+out, "FAKE_SCENARIO="+scenario, "FAKE_NATIVE_CHECKSUM="+nativeChecksum, "FAKE_NATIVE_FIND="+nativeFind)
			output, runErr := command.CombinedOutput()
			var exited *exec.ExitError
			wantCode := initialCode
			if scenario == "manifest-failure" {
				wantCode = 86
			} else if scenario == "manifest-malformed" {
				wantCode = 1
			} else if scenario == "manifest-find-failure" {
				wantCode = 87
			}
			if !errors.As(runErr, &exited) || exited.ExitCode() != wantCode || ctx.Err() != nil {
				t.Fatalf("original exit lost: %v output=%s", runErr, output)
			}
			status, err := os.ReadFile(filepath.Join(out, "cleanup-status.txt"))
			if err != nil {
				t.Fatal(err)
			}
			clean := scenario == "metadata" || scenario == "no-owned" || strings.HasPrefix(scenario, "manifest-")
			if bytes.Contains(status, []byte("NOT CLEAN")) == clean || bytes.Contains(status, []byte("HTTP/DB close and pinned")) {
				t.Fatalf("wrong cleanup claim: %s", status)
			}
			if strings.HasPrefix(scenario, "manifest-") {
				rootExit, err := os.ReadFile(filepath.Join(out, "root.exit"))
				if err != nil || strings.TrimSpace(string(rootExit)) != strconv.Itoa(wantCode) {
					t.Fatalf("manifest failure root exit=%q error=%v", rootExit, err)
				}
				if _, err := os.Stat(filepath.Join(out, "SHA256SUMS")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed manifest retained: %v", err)
				}
				return
			}
			_, deleteErr := os.Stat(filepath.Join(out, "metadata-delete-options.json"))
			shouldDelete := scenario == "metadata" || scenario == "delete-error" || scenario == "wait-timeout" || scenario == "replacement"
			if (deleteErr == nil) != shouldDelete {
				t.Fatalf("unsafe/missing delete: %v output=%s", deleteErr, output)
			}
			if scenario == "partial-voters" || scenario == "delayed-child" {
				capture, err := os.ReadFile(filepath.Join(out, "shutdown-cleanup-capture.exit"))
				if err != nil || strings.TrimSpace(string(capture)) != "1" {
					t.Fatalf("partial voter bypassed Close capture: %s %v", capture, err)
				}
			}
			if scenario == "delayed-child" {
				marker, err := os.ReadFile(filepath.Join(dir, "reap-at-seal"))
				if err != nil || string(marker) != "reaped-before-checksum\n" {
					t.Fatalf("child not reaped when checksum ran: %s %v", marker, err)
				}
				child, err := os.ReadFile(filepath.Join(dir, "child"))
				if err != nil {
					t.Fatal(err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
				if err != nil || syscall.Kill(pid, 0) == nil {
					t.Fatal("delayed cleanup child escaped reaping")
				}
				artifact, err := os.ReadFile(filepath.Join(out, "child-final.txt"))
				if err != nil || string(artifact) != "begin\ncomplete\n" {
					t.Fatalf("child final artifact incomplete: %q %v", artifact, err)
				}
				seal, err := os.ReadFile(filepath.Join(out, "SHA256SUMS"))
				if err != nil || !bytes.Contains(seal, []byte("  ./child-final.txt\n")) {
					t.Fatalf("child final artifact omitted from seal: %s %v", seal, err)
				}
			}
			for _, name := range []string{"workload.exit", "root.exit"} {
				code, err := os.ReadFile(filepath.Join(out, name))
				if err != nil || strings.TrimSpace(string(code)) != "53" {
					t.Fatalf("%s changed original exit: %s %v", name, code, err)
				}
			}
			verify := exec.Command("sha256sum", "-c", "SHA256SUMS")
			verify.Dir = out
			if output, err := verify.CombinedOutput(); err != nil {
				t.Fatalf("cleanup seal: %v %s", err, output)
			}
		})
	}
}

func TestQualificationHostShutdown(t *testing.T) {
	identity := shutdownIdentity{"namespace", "a1b2c3d4", "pod-uid", "node", "process"}
	for _, scenario := range []string{"complete", "close-error", "close-canceled", "close-timeout", "serve-error", "drain-timeout"} {
		t.Run(scenario, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				_, _ = w.Write([]byte("done"))
			}))
			defer server.Close()
			requestDone := make(chan struct{})
			go func() {
				defer close(requestDone)
				response, err := http.Get(server.URL)
				if err == nil {
					_ = response.Body.Close()
				}
			}()
			<-entered
			closeStarted, closeReleased := make(chan struct{}), make(chan struct{})
			defer close(closeReleased)
			closeDB := func() error {
				close(closeStarted)
				if scenario == "close-timeout" {
					<-closeReleased
				}
				if scenario == "close-error" {
					return errors.New("synthetic close failure")
				}
				if scenario == "close-canceled" {
					return context.Canceled
				}
				return nil
			}
			var cause error
			if scenario == "serve-error" {
				cause = errors.New("synthetic serve failure")
			}
			var receipt bytes.Buffer
			done := make(chan error, 1)
			drainLimit, closeLimit := 5*time.Second, 5*time.Second
			if scenario == "drain-timeout" {
				drainLimit = 10 * time.Millisecond
			}
			if scenario == "close-timeout" {
				closeLimit = 10 * time.Millisecond
			}
			go func() {
				done <- shutdownHost([]*http.Server{server.Config}, closeDB, identity, &receipt, cause, drainLimit, closeLimit)
			}()
			// An entered request must finish before DB.Close. Release it only
			// after the drain-timeout case has actually returned.
			var err error
			if scenario == "drain-timeout" {
				err = <-done
				select {
				case <-closeStarted:
					t.Fatal("Close raced an undrained HTTP handler")
				default:
				}
				close(release)
			} else {
				select {
				case <-closeStarted:
					t.Fatal("Close preceded HTTP handler completion")
				default:
				}
				close(release)
				err = <-done
			}
			<-requestDone
			var marker map[string]any
			if json.Unmarshal(receipt.Bytes(), &marker) != nil {
				t.Fatal("shutdown receipt missing")
			}
			complete := scenario == "complete"
			if (err == nil) != complete || (marker["event"] == "qualification-host-shutdown-complete") != complete {
				t.Fatalf("incorrect outcome: scenario=%s err=%v marker=%v", scenario, err, marker)
			}
			if marker["pod_uid"] != identity.PodUID || marker["process"] != identity.Process {
				t.Fatal("shutdown identity lost")
			}
		})
	}
}

func TestQualificationHostSIGTERM(t *testing.T) {
	const child = "RHIZA_OFFLINE_SIGTERM_CHILD"
	if os.Getenv(child) == "yes" {
		db, err := rhiza.Open(context.Background(), rhiza.Config{Local: true, ClusterID: "shutdown", NodeID: "child", DataDir: os.Getenv("RHIZA_OFFLINE_DIR")})
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		identity := shutdownIdentity{"namespace", "a1b2c3d4", "pod-uid", "child", "child-process"}
		servers := []*http.Server{{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}, {Addr: "127.0.0.1:0", Handler: http.NewServeMux()}}
		err = serveHost(ctx, servers, identity, os.Stdout)
		if err := shutdownHost(servers, db.Close, identity, os.Stdout, err, time.Second, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestQualificationHostSIGTERM$")
	command.Env = append(os.Environ(), child+"=yes", "RHIZA_OFFLINE_DIR="+t.TempDir())
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	scanner := bufio.NewScanner(stdout)
	signaled := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		captured.Write(line)
		captured.WriteByte('\n')
		var marker map[string]any
		if json.Unmarshal(line, &marker) == nil && marker["event"] == "qualification-host-start" && !signaled {
			signaled = true
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := command.Wait(); err != nil || ctx.Err() != nil || scanner.Err() != nil {
		t.Fatalf("actual SIGTERM child failed: %v %v stderr=%s stdout=%s", err, ctx.Err(), stderr.String(), captured.String())
	}
	if !signaled || !bytes.Contains(captured.Bytes(), []byte(`"event":"qualification-host-shutdown-complete"`)) || !bytes.Contains(captured.Bytes(), []byte(`"db_closed":true`)) {
		t.Fatalf("actual owned DB Close completion absent: %s", captured.String())
	}
}

func TestQualificationShutdownProof(t *testing.T) {
	for _, scenario := range []string{"complete", "missing-completion", "close-failure", "wrong-uid", "wrong-process", "forced-kill", "different-container", "different-image", "restarted", "no-terminal-event", "log-failure", "recreation", "replacement-event", "writers-remain", "watch-timeout", "watch-error", "empty-before", "missing-voter", "replacement-uid", "learner-omitted", "unresolved-learner-uid", "incomplete-startup", "planned-restart-current", "learner-current", "async-terminal", "async-parse-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			const run = "a1b2c3d4"
			ns := "rhiza-v0191-20261008-" + run
			image := "ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64)
			writeJSON := func(name string, value any) {
				t.Helper()
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), append(encoded, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			pod := map[string]any{"name": "rhiza-voter-0", "namespace": ns, "run": run, "uid": "owned-uid", "container_id": "containerd://owned", "image": image, "image_id": image, "restart_count": 0}
			status := map[string]any{"name": "rhiza", "containerID": "containerd://owned", "imageID": image, "restartCount": 0,
				"state": map[string]any{"terminated": map[string]any{"exitCode": 0, "signal": 0}}}
			if scenario == "planned-restart-current" {
				// Original PodUID is unchanged; do not demand graceful Close of
				// the historical killed container. Only this current one closes0.
				pod["container_id"], pod["restart_count"] = "containerd://current", 1
				status["containerID"], status["restartCount"] = "containerd://current", 1
			}
			if scenario == "forced-kill" {
				status["state"] = map[string]any{"terminated": map[string]any{"exitCode": 137, "signal": 9}}
			}
			if scenario == "different-container" {
				status["containerID"] = "containerd://replacement"
			}
			if scenario == "different-image" {
				status["imageID"] = "other-image"
			}
			if scenario == "restarted" {
				status["restartCount"] = 1
			}
			event := map[string]any{"type": "DELETED", "object": map[string]any{"metadata": map[string]any{"uid": "owned-uid", "name": "rhiza-voter-0"}, "status": map[string]any{"containerStatuses": []any{status}}}}
			writeJSON("shutdown-watch.json", event)
			if scenario == "replacement-event" || scenario == "watch-error" {
				second := map[string]any{"type": "ADDED", "object": map[string]any{"metadata": map[string]any{"uid": "replacement", "name": "rhiza-voter-0"}}}
				if scenario == "watch-error" {
					second = map[string]any{"type": "ERROR", "object": map[string]any{"code": 410}}
				}
				encoded, _ := json.Marshal(second)
				file, err := os.OpenFile(filepath.Join(dir, "shutdown-watch.json"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.Write(append(encoded, '\n'))
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			watchExit := "143\n"
			if scenario == "watch-timeout" {
				watchExit = "124\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "shutdown-watch.exit"), []byte(watchExit), 0600); err != nil {
				t.Fatal(err)
			}
			after := []any{}
			if scenario == "writers-remain" {
				after = append(after, pod)
			}
			writeJSON("shutdown-writers-after.json", map[string]any{"items": after})
			if scenario == "no-terminal-event" {
				writeJSON("shutdown-watch.json", map[string]any{"type": "MODIFIED", "object": map[string]any{"metadata": map[string]any{"uid": "owned-uid"}}})
			}
			replicas := 0
			if scenario == "recreation" {
				replicas = 1
			}
			writeJSON("shutdown-controller.json", map[string]any{"spec": map[string]any{"replicas": replicas}, "status": map[string]any{"replicas": replicas}})
			identity := shutdownIdentity{ns, run, "owned-uid", "rhiza-voter-0", strings.Repeat("b", 32)}
			var logs bytes.Buffer
			if err := hostMarker(&logs, "qualification-host-start", identity, false, false); err != nil {
				t.Fatal(err)
			}
			if scenario == "wrong-uid" {
				identity.PodUID = "replacement"
			}
			if scenario == "wrong-process" {
				identity.Process = strings.Repeat("c", 32)
			}
			completion := "qualification-host-shutdown-complete"
			if scenario == "close-failure" {
				completion = "qualification-host-shutdown-failed"
			}
			if scenario != "missing-completion" {
				if err := hostMarker(&logs, completion, identity, true, scenario != "close-failure"); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "rhiza-voter-0-shutdown.log"), logs.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			logExit := "0\n"
			if scenario == "log-failure" {
				logExit = "1\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "rhiza-voter-0-shutdown-log.exit"), []byte(logExit), 0600); err != nil {
				t.Fatal(err)
			}
			before := []any{pod}
			expected := []any{map[string]any{"name": "rhiza-voter-0", "namespace": ns, "run": run, "uid": "owned-uid"}}
			peers := 2
			if scenario == "learner-current" {
				peers = 3
			}
			for i := 1; i <= peers; i++ {
				name, uid := fmt.Sprintf("rhiza-voter-%d", i), fmt.Sprintf("owned-uid-%d", i)
				if i == 3 {
					name, uid = "rhiza-learner", "created-learner-uid"
				}
				before = append(before, map[string]any{"name": name, "namespace": ns, "run": run, "uid": uid, "container_id": "containerd://owned", "image": image, "image_id": image, "restart_count": 0})
				expected = append(expected, map[string]any{"name": name, "namespace": ns, "run": run, "uid": uid})
				encoded, err := json.Marshal(map[string]any{"type": "DELETED", "object": map[string]any{"metadata": map[string]any{"name": name, "uid": uid}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://owned", "imageID": image, "restartCount": 0, "state": map[string]any{"terminated": map[string]int{"exitCode": 0, "signal": 0}}}}}}})
				if err != nil {
					t.Fatal(err)
				}
				file, err := os.OpenFile(filepath.Join(dir, "shutdown-watch.json"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.Write(append(encoded, '\n'))
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
				var peerLogs bytes.Buffer
				peerIdentity := shutdownIdentity{ns, run, uid, name, strings.Repeat("b", 32)}
				if err := hostMarker(&peerLogs, "qualification-host-start", peerIdentity, false, false); err != nil {
					t.Fatal(err)
				}
				if err := hostMarker(&peerLogs, "qualification-host-shutdown-complete", peerIdentity, true, true); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name+"-shutdown.log"), peerLogs.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name+"-shutdown-log.exit"), []byte("0\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "empty-before":
				before = []any{}
			case "missing-voter":
				before = before[:2]
			case "replacement-uid":
				pod["uid"] = "replacement"
			case "learner-omitted", "unresolved-learner-uid":
				learnerUID := "created-learner-uid"
				if scenario == "unresolved-learner-uid" {
					learnerUID = ""
				}
				expected = append(expected, map[string]any{"name": "rhiza-learner", "namespace": ns, "run": run, "uid": learnerUID})
			case "incomplete-startup":
				expected, before = expected[:2], before[:2]
			}
			writeJSON("shutdown-before.json", before)
			writeJSON("shutdown-expected.json", expected)
			script, err := filepath.Abs("../run-gcs-postrelease.sh")
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command("sh", script, "check-shutdown")
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run, "RHIZA_NAMESPACE=" + ns, "RHIZA_AUTH_CREATION_SHA=" + strings.Repeat("d", 40),
				"RHIZA_HOST_IMAGE=" + image, "RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("e", 64),
				"RHIZA_NODE_A=gke-test-cluster-fixture-a", "RHIZA_NODE_B=gke-test-cluster-fixture-b", "RHIZA_NODE_C=gke-test-cluster-fixture-c",
				"RHIZA_OUTPUT=" + filepath.Join(dir, "render"), "RHIZA_SHUTDOWN_FIXTURE=" + dir}
			command.Env = append(command.Env, qualificationAssetEnv(run)...)
			var producer *exec.Cmd
			if scenario == "async-terminal" || scenario == "async-parse-failure" {
				// Logs already have EOF; the independent native stream completes
				// later. Exercise the actual shell waiter, not a completed file.
				ready, err := os.ReadFile(filepath.Join(dir, "shutdown-watch.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "watch-ready.json"), ready, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "shutdown-watch.exit")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "shutdown-watch.json"), []byte("{\"type\":"), 0600); err != nil {
					t.Fatal(err)
				}
				producer = exec.Command("sh", "-c", `sleep 1; if [ "$2" = async-terminal ]; then cp "$1/watch-ready.json" "$1/shutdown-watch.json"; fi; printf '0\n' > "$1/shutdown-watch.exit"`, "fixture", dir, scenario)
				if err := producer.Start(); err != nil {
					t.Fatal(err)
				}
				command.Env = append(command.Env, "RHIZA_SHUTDOWN_WAIT=yes")
			}
			output, err := command.CombinedOutput()
			if producer != nil {
				if err := producer.Wait(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(dir, "shutdown-watch-first-parse.stderr")); err != nil {
					t.Fatal("initial incomplete parser evidence not retained")
				}
			}
			if (err == nil) != (scenario == "complete" || scenario == "planned-restart-current" || scenario == "learner-current" || scenario == "async-terminal") {
				for _, name := range []string{"shutdown-watch.json", "shutdown-watch.exit", "shutdown-watch-proof.json", "shutdown-watch-parse.stderr", "shutdown-watch-first-parse.stderr", "shutdown-watch-first-parse.exit", "shutdown-before.json", "shutdown-expected.json"} {
					data, readErr := os.ReadFile(filepath.Join(dir, name))
					t.Logf("synthetic %s: read=%v bytes=%s", name, readErr, data)
				}
				t.Fatalf("proof scenario=%s result=%v output=%s", scenario, err, output)
			}
		})
	}
}
