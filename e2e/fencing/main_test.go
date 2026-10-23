package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

func TestWorkflowExcludesRawKindArtifacts(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string `yaml:"uses"`
				With struct {
					Path string `yaml:"path"`
				} `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	for job, root := range map[string]string{"operator-chaos": "operator-chaos-results", "automatic-operator-chaos": "automatic-chaos-results", "anchor-operator-chaos": "anchor-chaos-results"} {
		found := false
		for _, step := range workflow.Jobs[job].Steps {
			if strings.HasPrefix(step.Uses, "actions/upload-artifact@") && strings.TrimSpace(step.With.Path) == root+"\n!"+root+"/kind/**" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s upload must exclude the entire raw Kind subtree", job)
		}
	}
}

func TestWorkflowPublishesOnlyPreKindFailureReceiptWhenQualificationIsSkipped(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		ID   string `yaml:"id"`
		Name string `yaml:"name"`
		If   string `yaml:"if"`
		Run  string `yaml:"run"`
		Uses string `yaml:"uses"`
		With struct {
			Path string `yaml:"path"`
		} `yaml:"with"`
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []step `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	steps := workflow.Jobs["cooperative-fencing"].Steps
	var qualify, prepare, upload *step
	for i := range steps {
		switch steps[i].ID {
		case "qualify":
			qualify = &steps[i]
		case "fencing_public":
			prepare = &steps[i]
		}
		if strings.HasPrefix(steps[i].Uses, "actions/upload-artifact@") {
			upload = &steps[i]
		}
	}
	if qualify == nil || qualify.Name != "Qualify shipped runtime agent and cooperative executor in disposable Kubernetes" {
		t.Fatal("qualification step must have a stable id for skip handling")
	}
	if prepare == nil || prepare.If != "always()" {
		t.Fatal("public evidence preparation must still run after a pre-kind failure")
	}
	branch, attempted, ok := strings.Cut(prepare.Run, "else\n")
	if !ok || !strings.Contains(branch, "steps.qualify.outcome") || !strings.Contains(branch, "qualification-not-started.json") ||
		!strings.Contains(branch, `"stage":"pre-kind"`) || !strings.Contains(branch, "priorStepOutcome") ||
		strings.Contains(branch, "actual-exit.txt") || strings.Contains(branch, "safe-artifacts") {
		t.Fatal("skipped qualification must emit only a fixed pre-kind receipt, never a fabricated driver exit or raw fallback")
	}
	if !strings.Contains(attempted, `"$RUNNER_TEMP/fencing-e2e" safe-artifacts`) || strings.Contains(attempted, "qualification-not-started.json") {
		t.Fatal("attempted qualification must continue through the strict allowlist projection")
	}
	if upload == nil || strings.TrimSpace(upload.With.Path) != "cooperative-fencing-results/public" {
		t.Fatal("workflow may upload only the sanitized public evidence directory")
	}
}

func TestGCSAckLossQualificationHasDedicatedCommand(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(data)
	start := strings.Index(mainSource, `if os.Args[1] == "run-gcs-ha-ack-loss" {`)
	if start < 0 {
		t.Fatal("dedicated ACK-loss qualification command is missing")
	}
	end := strings.Index(mainSource[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("dedicated ACK-loss command block is not bounded")
	}
	block := mainSource[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSHAAckLoss(ctx))") {
		t.Fatal("ACK-loss command does not validate the GCS profile before running its driver")
	}
}

func TestGCSDeadPodQualificationHasDedicatedCommand(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(data)
	start := strings.Index(mainSource, `if os.Args[1] == "run-gcs-ha-dead-pod" {`)
	if start < 0 {
		t.Fatal("dedicated dead-Pod qualification command is missing")
	}
	end := strings.Index(mainSource[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("dedicated dead-Pod command block is not bounded")
	}
	block := mainSource[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSDeadPod(ctx))") {
		t.Fatal("dead-Pod command does not validate the GCS profile before running its driver")
	}
}

func TestFixtureOnlyWorkflowIsExclusiveAndRunsFixedGoSelectors(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	type input struct {
		Type    string `yaml:"type"`
		Default bool   `yaml:"default"`
	}
	type step struct {
		Run string `yaml:"run"`
	}
	var workflow struct {
		On struct {
			WorkflowDispatch struct {
				Inputs map[string]input `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
		Jobs map[string]struct {
			If    string `yaml:"if"`
			Steps []step `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	fixtureInput, ok := workflow.On.WorkflowDispatch.Inputs["fixture_only"]
	if !ok || fixtureInput.Type != "boolean" || fixtureInput.Default {
		t.Fatal("fixture_only must be an opt-in boolean that defaults to the normal full workflow")
	}
	wantConditions := map[string]string{
		"fencing-only":             "github.event_name == 'workflow_dispatch' && inputs.fencing_only && !inputs.fixture_only",
		"fixture-only":             "github.event_name == 'workflow_dispatch' && inputs.fixture_only",
		"cooperative-fencing":      "github.event_name != 'workflow_dispatch' || !inputs.fixture_only",
		"operator-chaos":           "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"automatic-operator-chaos": "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"membership-e2e":           "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"rust-sdk":                 "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"go":                       "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"containers":               "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"anchor-operator-chaos":    "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"publish-chaos-image":      "github.repository == 'mrchypark/rhiza' && github.event_name == 'workflow_dispatch' && inputs.publish_chaos_image && !inputs.fencing_only && !inputs.fixture_only",
	}
	if len(workflow.Jobs) != len(wantConditions) {
		t.Fatalf("workflow job set changed: got %d jobs, contract covers %d", len(workflow.Jobs), len(wantConditions))
	}
	for name, want := range wantConditions {
		job, ok := workflow.Jobs[name]
		if !ok || job.If != want {
			t.Errorf("job %s condition=%q, want %q", name, job.If, want)
		}
	}
	var run string
	for _, step := range workflow.Jobs["fixture-only"].Steps {
		run += step.Run
	}
	hasLine := func(want string) bool {
		for _, line := range strings.Split(run, "\n") {
			if line == want {
				return true
			}
		}
		return false
	}
	wantEmbeddedSelector := "embedded_tests='^(TestIndependentRollbackReconcilesInterruptedCreateMarkers|TestRollbackMarkerGateCoversEveryIndependentAndCIWrite|TestIndependentGCSChaosManifestContract|TestNamespaceExpiryRendersUIDResourceVersionDelete|TestQualificationHostContainsPinnedKubectlTools|TestRuntimeAgentRenderPinsNodesAndNamespaceOwnerReferences|TestVoterCountLauncherRunRequiresPrivilegedInstallCredential|TestGCSIAM.*)$'"
	if !hasLine(wantEmbeddedSelector) || strings.Contains(run, "operator_tests=") {
		t.Fatal("fixture-only job must retain only the fixed embedded rollback/IAM/manifest/launcher selector")
	}
	wantCommands := []string{
		`go test ./pkg/node -run '^(TestMembershipRetryDurablyPublishesCommittedConfiguration|TestMembershipAdditionAbortAllowsNextRevision)$' -count=1 -timeout=5m`,
		`go test ./e2e/chaos/embedded-go -run "$embedded_tests" -count=1 -timeout=5m`,
		`go test ./pkg/fencing ./pkg/fencing/cri ./pkg/fencing/runtimeagent ./pkg/operator ./pkg/recovery ./internal/objstore ./e2e/fencing -count=1 -timeout=5m`,
		`go test -race ./pkg/node -run '^(TestMembershipRetryDurablyPublishesCommittedConfiguration|TestMembershipAdditionAbortAllowsNextRevision)$' -count=1 -timeout=5m`,
		`go test -race ./e2e/chaos/embedded-go -run "$embedded_tests" -count=1 -timeout=5m`,
		`go test -race ./pkg/fencing ./pkg/fencing/cri ./pkg/fencing/runtimeagent ./pkg/operator ./pkg/recovery ./internal/objstore ./e2e/fencing -count=1 -timeout=5m`,
	}
	for _, command := range wantCommands {
		if !hasLine(command) {
			t.Errorf("fixture-only job omits exact package/selector command %q", command)
		}
	}
	if strings.Count(run, "go test ") != len(wantCommands) {
		t.Fatal("fixture-only job must contain only the six asserted focused test commands")
	}
	if strings.Contains(run, "go test ./...") || strings.Contains(run, "kind ") || strings.Contains(run, "docker ") {
		t.Fatal("fixture-only job must remain a focused Go feedback path without cluster/image work")
	}
}

func TestFreshNamespaceRendererRewritesPinnedNamespaceLookup(t *testing.T) {
	const namespace = "rhiza-fencing-isolated"
	runBytes, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	const renderExpression = `-e "s/resourceNames: \[rhiza\]$/resourceNames: [$fencing_namespace]/"`
	if !strings.Contains(string(runBytes), renderExpression) {
		t.Fatalf("run.sh must rewrite the pinned namespace resourceNames when rendering the fresh qualification namespace")
	}
	manifest, err := os.ReadFile("../../deploy/operator/fencing/authority.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rendered := strings.ReplaceAll(string(manifest), "namespace: rhiza\n", "namespace: "+namespace+"\n")
	rendered = strings.ReplaceAll(rendered, "resourceNames: [rhiza]\n", "resourceNames: ["+namespace+"]\n")
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	found := false
	for {
		var doc obj
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] != "ClusterRole" || nested(doc, "metadata", "name") != "rhiza-fencer-tokenreview" {
			continue
		}
		for _, raw := range doc["rules"].([]any) {
			rule := raw.(map[string]any)
			if len(rule["resources"].([]any)) != 1 || rule["resources"].([]any)[0] != "namespaces" {
				continue
			}
			found = true
			if len(rule["resourceNames"].([]any)) != 1 || rule["resourceNames"].([]any)[0] != namespace {
				t.Fatalf("rendered namespace resourceNames=%v, want only %s", rule["resourceNames"], namespace)
			}
		}
	}
	if !found {
		t.Fatal("rendered authority manifest omitted the namespace lookup rule")
	}
}

func TestImmutableImageResolutionDescriptors(t *testing.T) {
	config := "sha256:" + strings.Repeat("a", 64)
	for _, reference := range []string{config, "containerd://" + config, "docker.io/library/import-date@" + config} {
		if got, err := immutableDigest(reference); err != nil || got != config {
			t.Fatalf("immutable reference %q: %q, %v", reference, got, err)
		}
	}
	for _, reference := range []string{"image:local", "sha256:abc", "sha256:" + strings.Repeat("z", 64)} {
		if _, err := immutableDigest(reference); err == nil {
			t.Fatalf("accepted mutable or invalid reference %q", reference)
		}
	}
	manifest := `{"digest":"` + config + `","size":708,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"tag":"host"}}`
	alias := strings.Replace(manifest, `"host"`, `"operator"`, 1)
	index := func(second string) string { return `{"manifests":[` + manifest + `,` + second + `]}` }
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"config":{"digest":"` + config + `"}}`, true},
		{`{"manifests":[{"digest":"` + config + `"}]}`, true},
		{`{"manifests":[{"digest":"` + config + `"},{"digest":"` + config + `"}]}`, false},
		{index(alias), true},
		{index(strings.Replace(alias, config, "sha256:"+strings.Repeat("b", 64), 1)), false},
		{index(strings.Replace(alias, `"size":708`, `"size":709`, 1)), false},
		{index(strings.Replace(alias, "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json", 1)), false},
		{index(strings.Replace(alias, `"size":708`, `"size":708,"platform":{"architecture":"arm64","os":"linux"}`, 1)), false},
		{`{"config":{"digest":"image:local"}}`, false},
		{`{}`, false},
	} {
		body := []byte(tc.body)
		d := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
		got, err := imageDescriptor(d, body)
		if tc.valid && (err != nil || got != config) || !tc.valid && err == nil {
			t.Fatalf("descriptor %s: %q, %v", tc.body, got, err)
		}
		if _, err := imageDescriptor(config, body); err == nil {
			t.Fatal("accepted content that did not hash to the requested digest")
		}
	}
}

func TestFixtureHoldsWritersUntilSupervisorAndStoreReady(t *testing.T) {
	items := manifests()["items"].([]obj)
	var sts, job, operator bool
	for _, item := range items {
		if nested(item, "metadata", "namespace") != namespace {
			t.Fatal("fixture escaped owned namespace")
		}
		switch item["kind"] {
		case "StatefulSet":
			sts = true
			if nested(item, "spec", "updateStrategy", "type") != "OnDelete" {
				t.Fatal("qualification changed the recovery OnDelete strategy")
			}
			if nested(item, "spec", "replicas") != 0 {
				t.Fatal("writer launched before canonical supervised patch")
			}
		case "Job":
			job = true
			if nested(item, "spec", "suspend") != true {
				t.Fatal("bucket setup races store readiness")
			}
		case "Deployment":
			if nested(item, "metadata", "name") == "rhiza-operator" {
				operator = true
				if nested(item, "spec", "replicas") != 0 {
					t.Fatal("operator launched before voters")
				}
			}
		}
	}
	if !sts || !job || !operator {
		t.Fatal("fixture omitted lifecycle resource")
	}
}

func TestFixtureKeepsStoreCoordinatesPublicAndWriterKeysLocalOnly(t *testing.T) {
	items := manifests()["items"].([]obj)
	var configMap, statefulSet, operator bool
	for _, item := range items {
		name := nested(item, "metadata", "name")
		if item["kind"] == "ConfigMap" && name == "rhiza-config" {
			configMap = true
			data := item["data"].(obj)
			for key, want := range map[string]string{
				"RHIZA_OBJSTORE_ENDPOINT": "rhiza-minio:9000",
				"RHIZA_OBJSTORE_BUCKET":   "rhiza",
				"RHIZA_OBJSTORE_REGION":   "us-east-1",
				"RHIZA_OBJSTORE_INSECURE": "true",
			} {
				if data[key] != want {
					t.Errorf("public store coordinate %s=%v, want %q", key, data[key], want)
				}
			}
			for _, key := range []string{"RHIZA_OBJSTORE_ACCESS_KEY", "RHIZA_OBJSTORE_SECRET_KEY"} {
				if _, ok := data[key]; ok {
					t.Errorf("writer credential %s leaked into public ConfigMap", key)
				}
			}
		}
		if item["kind"] == "Secret" && name == "rhiza-object-store" {
			t.Fatal("base fixture manifests include the local writer Secret")
		}
		if item["kind"] == "StatefulSet" && name == "rhiza" {
			statefulSet = true
			if referencesStoreWriterSecret(nested(item, "spec", "template", "spec", "containers").([]obj)[0]) {
				t.Fatal("base StatefulSet references the local writer Secret")
			}
		}
		if item["kind"] == "Deployment" && name == "rhiza-operator" {
			operator = true
			if referencesStoreWriterSecret(nested(item, "spec", "template", "spec", "containers").([]obj)[0]) {
				t.Fatal("base operator references the local writer Secret")
			}
		}
	}
	if !configMap || !statefulSet || !operator {
		t.Fatal("base fixture omitted ConfigMap, source StatefulSet, or Operator")
	}

	secret := localCooperativeStoreSecret()
	if secret["kind"] != "Secret" || nested(secret, "metadata", "name") != "rhiza-object-store" {
		t.Fatalf("local overlay Secret identity=%v/%v", secret["kind"], nested(secret, "metadata", "name"))
	}
	credentials := secret["stringData"].(obj)
	if len(credentials) != 2 || credentials["RHIZA_OBJSTORE_ACCESS_KEY"] != "fencing-local" || credentials["RHIZA_OBJSTORE_SECRET_KEY"] != "fencing-local-secret" {
		t.Fatalf("local overlay must contain only fake writer keys: %#v", credentials)
	}

	patchBytes, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(patchBytes), "rhiza-object-store") || strings.Contains(string(patchBytes), "RHIZA_OBJSTORE_ACCESS_KEY") || strings.Contains(string(patchBytes), "RHIZA_OBJSTORE_SECRET_KEY") {
		t.Fatal("shipped supervised patch references or exposes the local writer Secret")
	}
	runScript, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	run := string(runScript)
	productionPatch := strings.Index(run, `k patch statefulset rhiza --type=strategic --patch-file "$fencing_tmp/supervised.patch.yaml"`)
	localSecret := strings.Index(run, `"$FENCING_E2E_BIN" local-cooperative-store-secret`)
	statefulOverlay := strings.Index(run, `k patch statefulset rhiza --type=json -p "$local_store_ref"`)
	operatorOverlay := strings.Index(run, `k patch deployment rhiza-operator --type=json -p "$local_store_ref"`)
	if productionPatch < 0 || localSecret <= productionPatch || statefulOverlay <= localSecret || operatorOverlay <= statefulOverlay {
		t.Fatal("local fake writer credential overlay must be applied after the shipped production patch")
	}
}

func referencesStoreWriterSecret(container obj) bool {
	for _, source := range container["envFrom"].([]obj) {
		if nested(source, "secretRef", "name") == "rhiza-object-store" {
			return true
		}
	}
	return false
}

func TestFixtureRestoreHasWritableTmpWithReadOnlyRoot(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var patch obj
	if err := yaml.NewDecoder(strings.NewReader(string(data))).Decode(&patch); err != nil {
		t.Fatal(err)
	}
	if err := verifyScratch(patch); err != nil {
		t.Fatal(err)
	}
	nested(patch, "spec", "template", "spec").(obj)["volumes"] = []any{}
	if verifyScratch(patch) == nil {
		t.Fatal("accepted read-only root without writable scratch storage")
	}
}

func TestInitialHeadLauncherExitCleanupPrecedesTokenRemovalAndPreservesStatus(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	const startMarker = "cleanup() {"
	const endMarker = "\n}\ntrap cleanup EXIT"
	start := strings.Index(string(launcher), startMarker)
	if start < 0 {
		t.Fatal("launcher has no cleanup function")
	}
	endOffset := strings.Index(string(launcher[start:]), endMarker)
	if endOffset < 0 {
		t.Fatal("launcher cleanup function is not installed as the EXIT trap")
	}
	cleanupFunction := string(launcher[start : start+endOffset+2])

	tmp := t.TempDir()
	out := filepath.Join(tmp, "private")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(out, "admin-token")
	if err := os.WriteFile(tokenPath, []byte("test-only-token"), 0600); err != nil {
		t.Fatal(err)
	}
	// A restore record marks that the fencer fault was armed. The fake driver
	// fails deliberately after recording whether the token still exists.
	if err := os.WriteFile(filepath.Join(out, gcsInitialHeadInstallStateName), []byte("test-only-state"), 0600); err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(tmp, "events")
	driverPath := filepath.Join(tmp, "fencing-e2e")
	driver := "#!/bin/sh\n" +
		"printf '%s token=%s\\n' \"$1\" \"$(test -f \"$TEST_TOKEN\" && echo present || echo absent)\" >> \"$TEST_EVENTS\"\n" +
		"exit 29\n"
	if err := os.WriteFile(driverPath, []byte(driver), 0700); err != nil {
		t.Fatal(err)
	}
	script := cleanupFunction + `
set -eu
out=$TEST_OUT
admin_token_file=$TEST_TOKEN
tls_tmp=
remove_admin_token=true
mode=install
initial_head_ack_loss=yes
cleanup_namespace=false
FENCING_E2E_BIN=$TEST_DRIVER
export FENCING_E2E_OUT=$out
export RHIZA_KUBE_CONTEXT=test-context RHIZA_NAMESPACE=test-namespace RHIZA_RUN_ID=a1b2c3d4
trap cleanup EXIT
exit 23
`
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"TEST_OUT="+out,
		"TEST_TOKEN="+tokenPath,
		"TEST_DRIVER="+driverPath,
		"TEST_EVENTS="+eventsPath,
	)
	err = cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("cleanup changed the original launcher exit status: err=%v", err)
	}
	events, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("armed fencer cleanup was not attempted: %v", err)
	}
	if got := string(events); got != "cleanup-gcs-ha-initial-head-ack-loss token=present\n" {
		t.Fatalf("fencer cleanup did not precede local token removal: %q", got)
	}
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("launcher did not remove its local token after fencer cleanup: %v", err)
	}
}

func TestScratchAcceptsAPIOmittedFalsePIDOptionsOnly(t *testing.T) {
	// Matches API serialization: default-false hostPID is omitted, not emitted
	// as false; all nondefault security and scratch-volume fields are retained.
	apiJSON := `{"spec":{"template":{"spec":{"containers":[{"securityContext":{"readOnlyRootFilesystem":true,"allowPrivilegeEscalation":false},"volumeMounts":[{"name":"temporary","mountPath":"/tmp"}]}],"volumes":[{"name":"temporary","emptyDir":{}}]}}}}`
	var sts obj
	if err := json.Unmarshal([]byte(apiJSON), &sts); err != nil {
		t.Fatal(err)
	}
	if err := verifyScratch(sts); err != nil {
		t.Fatalf("API-omitted false PID options rejected: %v", err)
	}
	spec := nested(sts, "spec", "template", "spec").(obj)
	for _, key := range []string{"hostPID", "shareProcessNamespace"} {
		spec[key] = false
		if err := verifyScratch(sts); err != nil {
			t.Fatalf("explicit false %s rejected: %v", key, err)
		}
		for _, malformed := range []any{true, "false", float64(0), nil, []any{}} {
			spec[key] = malformed
			if verifyScratch(sts) == nil {
				t.Fatalf("accepted enabled/malformed %s=%#v", key, malformed)
			}
		}
		delete(spec, key)
	}
	containers := spec["containers"].([]any)
	security := nested(containers[0], "securityContext").(obj)
	security["readOnlyRootFilesystem"] = false
	if verifyScratch(sts) == nil {
		t.Fatal("accepted writable root while tolerating omitted PID defaults")
	}
	security["readOnlyRootFilesystem"] = true
	delete(security, "allowPrivilegeEscalation")
	if verifyScratch(sts) == nil {
		t.Fatal("accepted missing explicit privilege-escalation guard")
	}
}

func TestObservationFaultMatchesOnlyOperatorToSelectedVoterHTTP(t *testing.T) {
	rule, err := observationRule("10.244.0.8", "10.244.0.9", "target-uid")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"OUTPUT", "-s", "10.244.0.8", "-d", "10.244.0.9", "-p", "tcp", "--dport", "8080", "-m", "comment", "--comment", "rhiza-fencing-e2e-target-uid", "-j", "DROP"}
	if string(encoded(rule)) != string(encoded(want)) {
		t.Fatalf("observation fault escaped exact operator/target HTTP tuple: %v", rule)
	}
	for _, pair := range [][2]string{{"", "10.244.0.9"}, {"10.244.0.8", "0.0.0.0/0"}, {"10.244.0.8", "10.244.0.8"}, {"127.0.0.1", "10.244.0.9"}, {"::1", "10.244.0.9"}} {
		if _, err := observationRule(pair[0], pair[1], "target-uid"); err == nil {
			t.Fatalf("accepted unsafe observation tuple %v", pair)
		}
	}
	if _, err := observationRule("10.244.0.8", "10.244.0.9", ""); err == nil {
		t.Fatal("accepted rule without owned target UID marker")
	}
	fault := observationFault{PID: "42", Rule: rule}
	insert, remove, check := fault.args("-I"), fault.args("-D"), fault.args("-C")
	if string(encoded(insert[8:])) != string(encoded(remove[8:])) || string(encoded(remove[8:])) != string(encoded(check[8:])) {
		t.Fatal("cleanup/check did not preserve the exact owned rule")
	}
}

func TestGenerationProofRejectsUnrelatedAuthorityBeforeReadingLedger(t *testing.T) {
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "chaos-source", OperationID: "operation", Scope: operator.ScopeGeneration}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), ProofID: "cooperative-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	for _, change := range []func(*operator.FenceRequest, *operator.FenceProof){
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.SourceClusterID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.StatefulSetUID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.BindingUID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.OperationID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.RequestHash = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.StorageQuiesced = false },
	} {
		r, p := request, proof
		change(&r, &p)
		if err := verifyGenerationProof(context.Background(), obj{"spec": r, "status": obj{"proof": p}}, "sts", "binding", []string{"old"}, t.TempDir()); err == nil {
			t.Fatal("unrelated/incomplete proof accepted")
		}
	}
}

func TestOnlineMembershipMappingAndExactIncarnations(t *testing.T) {
	refs := []operator.VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "learner", Pod: "learner"}}
	if err := validateReplacementRefs(refs, "rhiza-2", "learner"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]operator.VoterPod{refs[:2], {refs[0], refs[1], {NodeID: "rhiza-2", Pod: "rhiza-2"}}, {refs[0], refs[0], refs[2]}, {refs[0], refs[1], {NodeID: "learner", Pod: "rhiza-1"}}} {
		if validateReplacementRefs(bad, "rhiza-2", "learner") == nil {
			t.Fatal("accepted missing, retired, duplicated or incorrectly mapped learner")
		}
	}
	status := network.MembershipStatus{NodeID: "learner", ClusterID: "chaos-source", ConfigID: 3, WALIdentity: "wal", Voting: true, Voters: []quepaxa.NodeID{"rhiza-1", "learner", "rhiza-0"}}
	if !membershipMatches(status, refs, "learner") {
		t.Fatal("reordered exact current membership rejected")
	}
	for _, change := range []func(*network.MembershipStatus){
		func(s *network.MembershipStatus) { s.Pending = true },
		func(s *network.MembershipStatus) { s.Voting = false },
		func(s *network.MembershipStatus) { s.WALIdentity = "" },
		func(s *network.MembershipStatus) { s.Voters = []quepaxa.NodeID{"rhiza-0", "rhiza-1", "rhiza-2"} },
		func(s *network.MembershipStatus) { s.Voters = []quepaxa.NodeID{"rhiza-0", "rhiza-0", "learner"} },
	} {
		bad := status
		change(&bad)
		if membershipMatches(bad, refs, "learner") {
			t.Fatal("accepted uncertified or wrong current membership")
		}
	}
	targets := []operator.FenceTarget{{NodeID: "n", Pod: "p", PodUID: "uid", WALIdentity: "wal"}, {NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"}}
	if !sameTargets([]operator.FenceTarget{targets[1], targets[0]}, targets) {
		t.Fatal("exact reordered targets rejected")
	}
	stale := append([]operator.FenceTarget(nil), targets...)
	stale[1].PodUID = "stale"
	if sameTargets(stale, targets) || sameTargets(targets[:1], targets) {
		t.Fatal("accepted missing learner or stale Pod UID")
	}
}

func TestGenerationRetainsImmutableEarlierVoterStop(t *testing.T) {
	identity := fencing.Identity{SourceClusterID: "chaos-source", NodeID: "rhiza-2", PodUID: "old", RunID: "run", WALIdentity: "wal"}
	stop := fencing.StopRequest{OperationID: "online", RequestHash: "online-hash", Identity: identity}
	run := fencing.Run{Identity: identity, Stop: &stop, Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}, Exited: true}
	prior := fenceLedgerState{StatefulSetUID: "sts", Ledger: fencing.Ledger{SourceClusterID: "chaos-source", Runs: map[string]fencing.Run{"run": run}}}
	out := t.TempDir()
	if err := os.WriteFile(out+"/validated-voter-proof.json", encoded(obj{"request": operator.FenceRequest{OperationID: "online"}, "ledger": prior}), 0600); err != nil {
		t.Fatal(err)
	}
	current := prior.Ledger
	current.ClosedBy, current.ClosedHash = "generation", "generation-hash"
	if err := verifyPriorVoterStops(out, current); err != nil {
		t.Fatal(err)
	}
	changed := run
	changed.Exited = false
	current.Runs["run"] = changed
	if verifyPriorVoterStops(out, current) == nil {
		t.Fatal("generation accepted altered earlier voter-stop evidence")
	}
}

func TestVoterProofRejectsWrongIncarnationBeforeLedgerRead(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "live-uid", WALIdentity: "live-wal"}
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "chaos-source", OperationID: "online", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target}}
	request.Targets[0].PodUID = "stale-uid"
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), ProofID: "cooperative-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	if verifyScopedProof(context.Background(), obj{"spec": request, "status": obj{"proof": proof}}, "sts", "binding", []string{target.PodUID}, operator.ScopeVoter, []operator.FenceTarget{target}, t.TempDir()) == nil {
		t.Fatal("voter proof accepted stale target UID")
	}
}

func TestHistoricalVoterProofAllowsLaterLearnerButRejectsTargetTampering(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "old-uid", WALIdentity: "old-wal"}
	request := operator.FenceRequest{StatefulSetUID: "sts", SourceClusterID: "chaos-source", BindingUID: "binding", OperationID: "online", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target}}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	identity := fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "old-run", WALIdentity: target.WALIdentity}
	stop := fencing.StopRequest{OperationID: request.OperationID, RequestHash: hash, Identity: identity}
	run := fencing.Run{Identity: identity, Stop: &stop, Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}, Exited: true}
	state := fenceLedgerState{StatefulSetUID: "sts", Ledger: fencing.Ledger{SourceClusterID: request.SourceClusterID, ClosedNodes: map[string]fencing.StopRequest{target.NodeID: {OperationID: request.OperationID, RequestHash: hash}}, Runs: map[string]fencing.Run{"old-run": run}}}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "cooperative-" + fmt.Sprintf("%x", sha256.Sum256(encoded(state))), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	state.Ledger.Runs["learner-run"] = fencing.Run{Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: "learner", PodUID: "learner-uid", RunID: "learner-run", WALIdentity: "learner-wal"}}
	if proof.ProofID == "cooperative-"+fmt.Sprintf("%x", sha256.Sum256(encoded(state))) {
		t.Fatal("test did not advance the current ledger past proof issuance")
	}
	if err := verifyLedgerProof(state, request, proof); err != nil {
		t.Fatalf("unrelated later learner invalidated historical voter proof: %v", err)
	}
	for _, change := range []func(*fencing.Run){
		func(r *fencing.Run) { r.Identity.PodUID = "different-uid" },
		func(r *fencing.Run) { r.Identity.WALIdentity = "different-wal" },
		func(r *fencing.Run) { r.Stop.OperationID = "different-operation" },
		func(r *fencing.Run) { r.Stop.RequestHash = "different-hash" },
		func(r *fencing.Run) { r.Drain.UnknownMutations = 1 },
		func(r *fencing.Run) { r.Drain.PendingMutations = 1 },
		func(r *fencing.Run) { r.Drain.WritersJoined = false },
		func(r *fencing.Run) { r.Exited = false },
	} {
		var bad fencing.Run
		if err := json.Unmarshal(encoded(run), &bad); err != nil {
			t.Fatal(err)
		}
		change(&bad)
		state.Ledger.Runs["old-run"] = bad
		if verifyLedgerProof(state, request, proof) == nil {
			t.Fatal("historical proof accepted tampered terminal target evidence")
		}
	}
}
