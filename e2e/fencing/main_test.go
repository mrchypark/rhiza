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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"gopkg.in/yaml.v3"
)

func TestRunCapturesDiagnosticsBeforeDeleteAndPreservesPrimaryFailure(t *testing.T) {
	result := runDiagnosticTrapFixture(t, 1, true, false, false)
	if result.exit != 1 {
		t.Fatalf("run.sh exit=%d, want original cluster-create exit 1 despite diagnostic exit 23", result.exit)
	}
	if !reflect.DeepEqual(result.events, []string{"create", "export", "diagnostics", "delete"}) {
		t.Fatalf("cleanup event order=%v, want capture before owned delete", result.events)
	}
	if string(result.preExit) != "1\n" || string(result.actualExit) != "1\n" {
		t.Fatalf("pre-cleanup/actual exit=%q/%q, want original failure", result.preExit, result.actualExit)
	}
}

func TestRunFinalExitReceiptFailurePreservesPrimaryStatusAndCleanup(t *testing.T) {
	t.Run("primary 37 is not masked", func(t *testing.T) {
		result := runDiagnosticTrapFixture(t, 37, false, false, true)
		if result.exit != 37 {
			t.Fatalf("run.sh exit=%d, want original primary exit 37 despite final receipt failure", result.exit)
		}
		if string(result.preExit) != "37\n" {
			t.Fatalf("pre-cleanup-exit=%q, want original primary status", result.preExit)
		}
		if !reflect.DeepEqual(result.events, []string{"create", "export", "diagnostics", "delete"}) {
			t.Fatalf("cleanup event order=%v, want final receipt failure not to skip teardown", result.events)
		}
	})
	t.Run("success becomes failure when final receipt cannot be written", func(t *testing.T) {
		result := runDiagnosticTrapFixture(t, 0, false, false, true)
		if result.exit == 0 {
			t.Fatal("run.sh returned success despite failed final exit receipt")
		}
		if string(result.preExit) != "0\n" {
			t.Fatalf("pre-cleanup-exit=%q, want original success status", result.preExit)
		}
		if !reflect.DeepEqual(result.events, []string{"create", "export", "diagnostics", "delete"}) {
			t.Fatalf("cleanup event order=%v, want all diagnostics and owned deletion after receipt failure", result.events)
		}
	})
	t.Run("valid neighboring receipt", func(t *testing.T) {
		result := runDiagnosticTrapFixture(t, 0, false, false, false)
		if result.exit != 0 || string(result.preExit) != "0\n" || string(result.actualExit) != "0\n" {
			t.Fatalf("valid run exit/receipts=%d/%q/%q, want zero", result.exit, result.preExit, result.actualExit)
		}
		if !reflect.DeepEqual(result.events, []string{"create", "export", "diagnostics", "delete"}) {
			t.Fatalf("valid cleanup event order=%v", result.events)
		}
	})
}

func TestRunPreCleanupReceiptFailureStillCleansUp(t *testing.T) {
	result := runDiagnosticTrapFixture(t, 1, false, true, false)
	if result.exit != 1 {
		t.Fatalf("run.sh exit=%d, want original primary exit 1", result.exit)
	}
	if !reflect.DeepEqual(result.events, []string{"create", "export", "diagnostics", "delete"}) {
		t.Fatalf("cleanup event order=%v, want pre-cleanup receipt failure not to skip teardown", result.events)
	}
}

type diagnosticTrapResult struct {
	exit       int
	events     []string
	preExit    []byte
	actualExit []byte
}

func runDiagnosticTrapFixture(t *testing.T, createExit int, diagnosticsFail, blockPreReceipt, blockActualReceipt bool) diagnosticTrapResult {
	t.Helper()
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0700); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(root, "events")
	writeExecutable := func(name, body string) string {
		t.Helper()
		path := filepath.Join(binDir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	writeExecutable("docker", "printf '%s\\n' sha256:"+strings.Repeat("a", 64)+" sha256:"+strings.Repeat("b", 64))
	diagnosticsExit := "0"
	if diagnosticsFail {
		diagnosticsExit = "23"
	}
	writeExecutable("kind", `case "$*" in
  'get clusters') exit 0 ;;
  'create cluster'*) echo create >>"$FENCING_TEST_EVENTS"; exit "$FENCING_TEST_CREATE_EXIT" ;;
  'export logs'*) echo export >>"$FENCING_TEST_EVENTS"; exit 0 ;;
  'delete cluster'*) echo delete >>"$FENCING_TEST_EVENTS"; exit 0 ;;
  *) exit 0 ;;
esac`)
	writeExecutable("kubectl", "exit 0")
	writeExecutable("helm", "exit 0")
	driver := filepath.Join(root, "driver")
	if err := os.WriteFile(driver, []byte("#!/bin/sh\ncase \"$1\" in\n  manifests) printf '{\"items\":[]}\\n' ;;\n  capture-runtime-diagnostics) echo diagnostics >>\"$FENCING_TEST_EVENTS\"; exit \"$FENCING_TEST_DIAGNOSTICS_EXIT\" ;;\n  wait-source|run) exit 0 ;;\n  *) exit 1 ;;\nesac\n"), 0700); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "out")
	if err := os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	if blockPreReceipt {
		if err := os.MkdirAll(filepath.Join(out, "pre-cleanup-exit.txt"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if blockActualReceipt {
		if err := os.MkdirAll(filepath.Join(out, "actual-exit.txt"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("sh", "run.sh")
	cmd.Dir = "."
	env := map[string]string{}
	for _, entry := range os.Environ() {
		parts := strings.SplitN(entry, "=", 2)
		env[parts[0]] = parts[1]
	}
	env["PATH"] = binDir + string(os.PathListSeparator) + env["PATH"]
	env["FENCING_E2E_BIN"] = driver
	env["FENCING_E2E_OUT"] = out
	env["FENCING_TEST_EVENTS"] = events
	env["FENCING_TEST_CREATE_EXIT"] = fmt.Sprint(createExit)
	env["FENCING_TEST_DIAGNOSTICS_EXIT"] = diagnosticsExit
	cmd.Env = make([]string, 0, len(env))
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	err := cmd.Run()
	var exitErr *exec.ExitError
	exit := 0
	if err != nil {
		if !errors.As(err, &exitErr) {
			t.Fatalf("run.sh failed without an exit status: %v", err)
		}
		exit = exitErr.ExitCode()
	}
	sequence, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	ordered := strings.Split(strings.TrimSpace(string(sequence)), "\n")
	if !reflect.DeepEqual(ordered, []string{"create", "export", "diagnostics", "delete"}) {
		t.Fatalf("cleanup event order=%v, want capture before owned delete", ordered)
	}
	var actualExit []byte
	if !blockActualReceipt {
		actualExit, err = os.ReadFile(filepath.Join(out, "actual-exit.txt"))
		if err != nil {
			t.Fatalf("actual-exit read: %v", err)
		}
	}
	preCleanupExit, err := os.ReadFile(filepath.Join(out, "pre-cleanup-exit.txt"))
	if err != nil && !blockPreReceipt {
		t.Fatalf("pre-cleanup-exit read: %v", err)
	}
	if err != nil {
		preCleanupExit = nil
	}
	return diagnosticTrapResult{exit: exit, events: ordered, preExit: preCleanupExit, actualExit: actualExit}
}

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
	attestation := `{"digest":"sha256:` + strings.Repeat("c", 64) + `","size":600,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"` + config + `"}}`
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"config":{"digest":"` + config + `"}}`, true},
		{`{"manifests":[{"digest":"` + config + `"}]}`, true},
		{`{"manifests":[{"digest":"` + config + `"},{"digest":"` + config + `"}]}`, false},
		{index(alias), true},
		{index(attestation), true},
		{`{"manifests":[` + attestation + `,` + manifest + `]}`, true},
		{`{"manifests":[` + attestation + `]}`, false},
		{index(strings.Replace(attestation, `"attestation-manifest"`, `"other"`, 1)), false},
		{index(strings.Replace(attestation, `"os":"unknown"`, `"os":"linux"`, 1)), false},
		{index(strings.Replace(attestation, "application/vnd.oci.image.manifest.v1+json", "application/vnd.oci.image.index.v1+json", 1)), false},
		{index(strings.Replace(attestation, config, "sha256:"+strings.Repeat("b", 64), 1)), false},
		{index(strings.Replace(attestation, `"size":600`, `"size":0`, 1)), false},
		{index(strings.Replace(attestation, `"vnd.docker.reference.type":"attestation-manifest",`, ``, 1)), false},
		{index(strings.Replace(attestation, "sha256:"+strings.Repeat("c", 64), "sha256:invalid", 1)), false},
		{`{"manifests":[` + manifest + `,` + attestation + `,` + strings.Replace(alias, config, "sha256:"+strings.Repeat("b", 64), 1) + `]}`, false},
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

func TestFixtureUsesRealRecoveryAnchorServiceAndApprovedDRGate(t *testing.T) {
	items := manifests()["items"].([]obj)
	var anchorConfig, anchorDeployment, anchorService, operator, binding obj
	for _, item := range items {
		switch nested(item, "kind") {
		case "ConfigMap":
			if nested(item, "metadata", "name") == recoveryAnchorConfigMap {
				anchorConfig = item
			}
		case "Deployment":
			switch nested(item, "metadata", "name") {
			case "anchor-service":
				anchorDeployment = item
			case "rhiza-operator":
				operator = item
			}
		case "Service":
			if nested(item, "metadata", "name") == "anchor-service" {
				anchorService = item
			}
		case "RhizaCluster":
			binding = item
		}
	}
	if anchorConfig == nil || anchorDeployment == nil || anchorService == nil || operator == nil || binding == nil {
		t.Fatal("fixture omitted a real anchor resource or the anchored workload binding")
	}
	var initial recoveryanchor.Record
	if err := json.Unmarshal([]byte(text(nested(anchorConfig, "data", "anchor.json"))), &initial); err != nil || initial.Binding.ClusterID != "chaos-source" || initial.EvidenceFormat != "sql-epoch-token/v1" || initial.PendingWrite || initial.Generation != 0 {
		t.Fatalf("initial anchor record is not the source-bound empty generation: %+v, %v", initial, err)
	}
	container := nested(anchorDeployment, "spec", "template", "spec", "containers").([]obj)[0]
	ports, _ := nested(anchorService, "spec", "ports").([]obj)
	if nested(container, "image") != "rhiza-anchor-service:local" || nested(container, "command") == nil || len(ports) != 1 || fmt.Sprint(nested(ports[0], "port")) != "9191" {
		t.Fatal("fixture does not run the actual anchor-service image behind the TLS service")
	}
	var anchorURL, tokenPath, caPath string
	for _, env := range nested(operator, "spec", "template", "spec", "containers").([]obj)[0]["env"].([]obj) {
		switch nested(env, "name") {
		case "RHIZA_RECOVERY_ANCHOR_URL":
			anchorURL = text(nested(env, "value"))
		case "RHIZA_RECOVERY_ANCHOR_TOKEN_FILE":
			tokenPath = text(nested(env, "value"))
		case "RHIZA_RECOVERY_ANCHOR_CA_FILE":
			caPath = text(nested(env, "value"))
		}
	}
	if anchorURL != "https://anchor-service."+namespace+".svc:9191" || tokenPath == "" || caPath == "" || nested(binding, "spec", "anchorID") != recoveryAnchorID {
		t.Fatal("operator/workload are not configured to use the real anchor authority")
	}
}

func TestDRPodControllerBindingRequiresExactStatefulSetUID(t *testing.T) {
	pod := obj{"metadata": obj{"ownerReferences": []any{obj{"apiVersion": "apps/v1", "kind": "StatefulSet", "uid": "sts-uid", "controller": true}}}}
	if !podControlledByStatefulSet(pod, "sts-uid") {
		t.Fatal("exact StatefulSet controller binding was rejected")
	}
	if podControlledByStatefulSet(pod, "recreated-sts-uid") {
		t.Fatal("recreated StatefulSet UID was accepted")
	}
	pod["metadata"].(obj)["ownerReferences"] = []any{obj{"apiVersion": "apps/v1", "kind": "StatefulSet", "uid": "sts-uid", "controller": false}}
	if podControlledByStatefulSet(pod, "sts-uid") {
		t.Fatal("non-controller owner reference was accepted")
	}
}

func TestDRPreRuntimeAccumulatesStaggeredPendingPods(t *testing.T) {
	base := drPreRuntimeObservation{Version: 1, TargetClusterID: "target", OperationID: "op", RequestHash: "request", Receipt: recoveryanchor.Receipt{AnchorID: recoveryAnchorID, OperationID: "op", RequestHash: "request", Generation: 1}}
	a := drPreRuntimeAssignment{PodName: "rhiza-0", PodUID: "pod-0", NodeID: "rhiza-0", CredentialUID: "credential", LedgerUID: "ledger", MembershipHash: "membership"}
	b := drPreRuntimeAssignment{PodName: "rhiza-1", PodUID: "pod-1", NodeID: "rhiza-1", CredentialUID: "credential", LedgerUID: "ledger", MembershipHash: "membership"}
	c := drPreRuntimeAssignment{PodName: "rhiza-2", PodUID: "pod-2", NodeID: "rhiza-2", CredentialUID: "credential", LedgerUID: "ledger", MembershipHash: "membership"}
	first := base
	first.ExpectedAssignments = []drPreRuntimeAssignment{a}
	first.Assignments = []drPreRuntimeAssignment{a}
	second := base // A has completed; B has not appeared yet.
	second.ExpectedAssignments = []drPreRuntimeAssignment{a, b}
	third := base
	third.ExpectedAssignments = []drPreRuntimeAssignment{a, b, c}
	third.Assignments = []drPreRuntimeAssignment{b, c}

	legacySawAll := len(first.Assignments) == 3 || len(second.Assignments) == 3 || len(third.Assignments) == 3
	if legacySawAll {
		t.Fatal("staggered fixture unexpectedly satisfied the old same-poll requirement")
	}
	merged, err := mergeDRPreRuntimeObservation(drPreRuntimeObservation{}, first)
	if err != nil {
		t.Fatal(err)
	}
	merged, err = mergeDRPreRuntimeObservation(merged, second)
	if err != nil {
		t.Fatal(err)
	}
	merged, err = mergeDRPreRuntimeObservation(merged, third)
	if err != nil {
		t.Fatal(err)
	}
	if !sameDRPreRuntimeAssignments(merged.ExpectedAssignments, merged.Assignments) {
		t.Fatalf("accumulated assignments = %+v, expected %+v", merged.Assignments, merged.ExpectedAssignments)
	}

	t.Run("replacement UID rejected", func(t *testing.T) {
		changed := base
		changedA := a
		changedA.PodUID = "replacement-pod-0"
		changed.ExpectedAssignments = []drPreRuntimeAssignment{changedA, b, c}
		changed.Assignments = []drPreRuntimeAssignment{changedA}
		if _, err := mergeDRPreRuntimeObservation(merged, changed); err == nil {
			t.Fatal("replacement UID overwrote the captured Pod identity")
		}
	})
	t.Run("approval change rejected", func(t *testing.T) {
		changed := base
		changed.RequestHash = "different-request"
		if _, err := mergeDRPreRuntimeObservation(merged, changed); err == nil {
			t.Fatal("changed approval overwrote the captured request")
		}
	})
}

func TestFixtureUsesGeneratedBootstrapCredentialsAndMandatoryRestoreInit(t *testing.T) {
	items := manifests()["items"].([]obj)
	var config, sts, binding obj
	for _, item := range items {
		switch item["kind"] {
		case "ConfigMap":
			if nested(item, "metadata", "name") == "rhiza-config" {
				config = item
			}
		case "StatefulSet":
			sts = item
		case "RhizaCluster":
			binding = item
		case "Secret":
			if nested(item, "metadata", "name") == "rhiza-peer-credentials" {
				t.Fatal("fixture must not seed static admin or peer credentials")
			}
		}
	}
	if config == nil || nested(config, "data", "RHIZA_CLUSTER_MEMBERS") != "" || nested(binding, "spec", "bootstrap") != true {
		t.Fatal("fixture must opt into fresh bootstrap with empty initial membership")
	}
	if nested(sts, "spec", "replicas") != 0 {
		t.Fatal("fresh bootstrap workload must remain writer-free until operator setup")
	}
	podSpec := nested(sts, "spec", "template", "spec")
	initContainers, _ := nested(podSpec, "initContainers").([]obj)
	containers, _ := nested(podSpec, "containers").([]obj)
	if len(initContainers) != 1 || len(containers) != 1 {
		t.Fatalf("init/runtime containers = %d/%d, want one each", len(initContainers), len(containers))
	}
	init := initContainers[0]
	if nested(init, "name") != "rhiza-prepare" || nested(init, "image") != nested(containers[0], "image") || nested(init, "command") == nil || nested(init, "args") == nil {
		t.Fatal("restore init must use the application image and fixed direct command")
	}
	command, _ := nested(init, "command").([]string)
	args, _ := nested(init, "args").([]string)
	if fmt.Sprint(command) != "[/usr/local/bin/rhiza]" || fmt.Sprint(args) != "[--prepare-restore]" {
		t.Fatalf("restore init command=%v args=%v", command, args)
	}
	initEnv, _ := nested(init, "env").([]obj)
	seen := map[string]obj{}
	for _, entry := range initEnv {
		name := text(entry["name"])
		seen[name] = entry
		if name == "RHIZA_ADMIN_TOKEN" || name == "RHIZA_PEER_TOKEN" || name == "RHIZA_PEER_TOKENS" {
			t.Fatalf("runtime credential %s leaked into restore init", name)
		}
	}
	memberRef := nested(seen["RHIZA_CLUSTER_MEMBERS"], "valueFrom", "secretKeyRef")
	if nested(memberRef, "name") != "rhiza-bootstrap-pending" || nested(memberRef, "key") != "members" || nested(seen["POD_NAME"], "valueFrom", "fieldRef", "fieldPath") != "metadata.name" || nested(seen["POD_UID"], "valueFrom", "fieldRef", "fieldPath") != "metadata.uid" || nested(seen["RHIZA_PREPARED_POD_UID"], "valueFrom", "fieldRef", "fieldPath") != "metadata.uid" {
		t.Fatal("restore init lacks generated public members or Pod identity bindings")
	}
	if nested(seen["RHIZA_PREPARE_OPERATION_ID"], "valueFrom", "fieldRef", "fieldPath") != "metadata.annotations['rhiza.mrchypark.dev/prepare-operation-id']" || nested(seen["FENCER_AUTHORITY_URL"], "value") != "https://rhiza-fencer:9444/v1/prepare-restore" || nested(seen["FENCER_CA_FILE"], "value") != "/fencer-ca/ca.crt" {
		t.Fatal("restore init lacks the trusted operation and fencing authority profile")
	}
	for _, source := range listAny(nested(init, "envFrom")) {
		if nested(source, "secretRef") != nil && nested(source, "secretRef", "name") != "rhiza-object-store" {
			t.Fatal("restore init imports a non-storage Secret")
		}
	}
	initMounts, _ := nested(init, "volumeMounts").([]obj)
	appMounts, _ := nested(containers[0], "volumeMounts").([]obj)
	if !hasNamedMount(initMounts, "data", "/data") || !hasNamedMount(appMounts, "data", "/data") {
		t.Fatal("restore init and application must share /data")
	}
	volumes, _ := nested(podSpec, "volumes").([]obj)
	dataEmptyDir, tokenProjection := false, false
	for _, volume := range volumes {
		if nested(volume, "name") == "data" && nested(volume, "emptyDir") != nil {
			dataEmptyDir = true
		}
		if nested(volume, "persistentVolumeClaim") != nil {
			t.Fatal("NoPVC fixture contains a PVC volume")
		}
	}
	patchData, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var patch obj
	if err := yaml.NewDecoder(strings.NewReader(string(patchData))).Decode(&patch); err != nil {
		t.Fatal(err)
	}
	patchVolumes, _ := nested(patch, "spec", "template", "spec", "volumes").([]any)
	for _, volume := range patchVolumes {
		if nested(volume, "name") == "fence-token" && nested(volume, "projected", "sources") != nil {
			sources, _ := nested(volume, "projected", "sources").([]any)
			for _, source := range sources {
				if nested(source, "serviceAccountToken", "path") == "token" && nested(source, "serviceAccountToken", "audience") == "rhiza-fencer" && nested(source, "serviceAccountToken", "expirationSeconds") == 600 {
					tokenProjection = true
				}
			}
		}
	}
	if !dataEmptyDir || !tokenProjection {
		t.Fatal("fixture must share emptyDir and projected authority token")
	}
}

func TestPodChaosIsRunScopedToExactOldPodIdentity(t *testing.T) {
	first, err := podChaosForTarget("pod-uid-one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := podChaosForTarget("pod-uid-two")
	if err != nil {
		t.Fatal(err)
	}
	if nested(first, "apiVersion") != "chaos-mesh.org/v1alpha1" || nested(first, "kind") != "PodChaos" || nested(first, "metadata", "namespace") != namespace || nested(first, "spec", "action") != "pod-kill" || nested(first, "spec", "mode") != "one" || nested(first, "spec", "duration") != "45s" {
		t.Fatalf("PodChaos scope/action differs: %+v", first)
	}
	if nested(first, "spec", "selector", "namespaces") == nil || nested(first, "spec", "selector", "namespaces").([]string)[0] != namespace || nested(first, "spec", "selector", "labelSelectors", chaosTargetLabel) == nested(second, "spec", "selector", "labelSelectors", chaosTargetLabel) {
		t.Fatalf("PodChaos does not isolate one labelled Pod UID: %+v", first)
	}
}

func TestPreparePodReadyRequiresSuccessfulInitBeforeRunningApp(t *testing.T) {
	valid := preparedPodFixture()
	ready, err := preparePodReady(valid)
	if err != nil || !ready {
		t.Fatalf("valid prepared replacement Pod: ready=%v err=%v", ready, err)
	}
	t.Run("init still running", func(t *testing.T) {
		pod := preparedPodFixture()
		initStatuses := listAny(nested(pod, "status", "initContainerStatuses"))
		initStatuses[0].(obj)["state"] = obj{"running": obj{"startedAt": "2026-10-28T00:00:00Z"}}
		ready, err := preparePodReady(pod)
		if err != nil || ready {
			t.Fatalf("incomplete init accepted: ready=%v err=%v", ready, err)
		}
	})
	t.Run("init failure", func(t *testing.T) {
		pod := preparedPodFixture()
		initStatuses := listAny(nested(pod, "status", "initContainerStatuses"))
		initStatuses[0].(obj)["state"] = obj{"terminated": obj{"exitCode": float64(1), "finishedAt": "2026-10-28T00:00:01Z"}}
		if _, err := preparePodReady(pod); err == nil {
			t.Fatal("nonzero init exit accepted")
		}
	})
	t.Run("app started before init", func(t *testing.T) {
		pod := preparedPodFixture()
		appStatuses := listAny(nested(pod, "status", "containerStatuses"))
		appStatuses[0].(obj)["state"] = obj{"running": obj{"startedAt": "2026-10-28T00:00:00Z"}}
		if _, err := preparePodReady(pod); err == nil {
			t.Fatal("app start before init completion accepted")
		}
	})
	t.Run("runtime peer secret", func(t *testing.T) {
		pod := preparedPodFixture()
		initContainers := listAny(nested(pod, "spec", "initContainers"))
		initEnv := listAny(nested(initContainers[0], "env"))
		initEnv = append(initEnv, value("RHIZA_PEER_TOKENS", "secret"))
		initContainers[0].(obj)["env"] = initEnv
		if _, err := preparePodReady(pod); err == nil {
			t.Fatal("runtime peer secret in init accepted")
		}
	})
	t.Run("bootstrap placeholder", func(t *testing.T) {
		pod := preparedPodFixture()
		initContainers := listAny(nested(pod, "spec", "initContainers"))
		initEnv := listAny(nested(initContainers[0], "env"))
		for _, env := range initEnv {
			entry := env.(obj)
			if entry["name"] == "RHIZA_CLUSTER_MEMBERS" {
				entry["valueFrom"] = obj{"secretKeyRef": obj{"name": "rhiza-bootstrap-pending", "key": "members"}}
			}
		}
		if _, err := preparePodReady(pod); err == nil {
			t.Fatal("unresolved bootstrap Secret placeholder accepted")
		}
	})
	t.Run("persistent volume", func(t *testing.T) {
		pod := preparedPodFixture()
		volumes := listAny(nested(pod, "spec", "volumes"))
		podSpec := nested(pod, "spec").(obj)
		podSpec["volumes"] = append(volumes, obj{"name": "persistent", "persistentVolumeClaim": obj{"claimName": "unexpected"}})
		if _, err := preparePodReady(pod); err == nil {
			t.Fatal("PVC in NoPVC fixture accepted")
		}
	})
}

func TestPeerAuthenticationRejectionRequiresExactRemoteResponse(t *testing.T) {
	valid := errors.New("peer rejected request: peer RPC phase=response-decode elapsed=1.25ms cause_type=*errors.errorString: peer rejected request: peer authentication failed")
	phase, elapsed, cause, ok := peerAuthenticationRejection(valid)
	if !ok || phase != "response-decode" || elapsed != 1250*time.Microsecond || cause != "*errors.errorString" {
		t.Fatalf("valid structured remote rejection was not recognized: %q %s %q %v", phase, elapsed, cause, ok)
	}
	for _, message := range []string{
		"peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=dial elapsed=1ms cause_type=*errors.errorString: peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=response-decode elapsed=not-a-duration cause_type=*errors.errorString: peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=response-decode elapsed=-0s cause_type=*errors.errorString: peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=response-decode elapsed=11s cause_type=*errors.errorString: peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=response-decode elapsed=1000us cause_type=*errors.errorString: peer rejected request: peer authentication failed",
		"peer rejected request: peer RPC phase=response-decode elapsed=1ms cause_type=*context.deadlineExceededError: context deadline exceeded",
		"peer rejected request: peer RPC phase=response-decode elapsed=1ms cause_type=*errors.errorString: peer rejected request: peer authentication failed: extra",
		"peer RPC phase=response-decode elapsed=1ms cause_type=*errors.errorString: peer rejected request: peer authentication failed",
	} {
		if _, _, _, ok := peerAuthenticationRejection(errors.New(message)); ok {
			t.Fatalf("accepted ambiguous peer error %q", message)
		}
	}
	if _, _, _, ok := peerAuthenticationRejection(nil); ok {
		t.Fatal("accepted nil error as an authentication rejection")
	}
}

func TestPeerProbeJobUsesOnlyScalarPeerCredentialAndPublicMembers(t *testing.T) {
	var job obj
	if err := json.Unmarshal(encoded(peerProbeJobResource("peer-probe-test", "old-peer-scalar", "generated-bootstrap", "stale", "run-id", "old-node", "survivor", 9, "request-id", "marker-value")), &job); err != nil {
		t.Fatal(err)
	}
	jobSpec := nested(job, "spec", "template", "spec")
	if nested(jobSpec, "automountServiceAccountToken") != false || nested(jobSpec, "volumes") != nil || nested(jobSpec, "serviceAccountName") != nil {
		t.Fatal("peer probe Job has a service-account token, mounted credential volume, or privileged identity")
	}
	containers := listAny(nested(jobSpec, "containers"))
	if len(containers) != 1 || nested(containers[0], "image") != "rhiza-fencing-host:local" || nested(containers[0], "command") == nil || nested(containers[0], "envFrom") != nil {
		t.Fatal("peer probe Job is not an isolated one-container driver invocation")
	}
	command := stringsFromAny(nested(containers[0], "command"))
	args := stringsFromAny(nested(containers[0], "args"))
	if strings.Join(command, " ") != "/usr/local/bin/fencing-e2e" || strings.Join(args, " ") != "peer-probe stale" {
		t.Fatalf("peer probe Job did not override the operator image entrypoint: command=%v args=%v", command, args)
	}
	env := listAny(nested(containers[0], "env"))
	values := make(map[string]any, len(env))
	for _, item := range env {
		name := text(nested(item, "name"))
		values[name] = item
	}
	if values["RHIZA_ADMIN_TOKEN"] != nil || values["RHIZA_PEER_TOKENS"] != nil || values["RHIZA_DATA_DIR"] != nil || values["FENCING_PEER_TOKEN"] == nil || values["FENCING_PUBLIC_MEMBERS"] == nil {
		t.Fatalf("peer probe Job exposes an admin, whole peer map, or node data path: %v", values)
	}
	if nested(values["FENCING_PEER_TOKEN"], "valueFrom", "secretKeyRef", "name") != "old-peer-scalar" || nested(values["FENCING_PEER_TOKEN"], "valueFrom", "secretKeyRef", "key") != "peer-token" {
		t.Fatal("peer probe token does not use its dedicated scalar Secret key")
	}
	if nested(values["FENCING_PUBLIC_MEMBERS"], "valueFrom", "secretKeyRef", "name") != "generated-bootstrap" || nested(values["FENCING_PUBLIC_MEMBERS"], "valueFrom", "secretKeyRef", "key") != "members" {
		t.Fatal("peer probe public membership is not read from the generated bootstrap Secret")
	}
}

func TestPeerProbeJobFailureDoesNotExposeRawLogsInErrorOrPublicProjection(t *testing.T) {
	const marker = "PRIVATE_PROBE_LOG_SECRET_MARKER_42"
	job := obj{"status": obj{"conditions": []any{obj{
		"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "message": marker,
	}}}}
	pods := obj{"items": []any{obj{
		"metadata": obj{"labels": obj{"job-name": "probe-job"}},
		"status": obj{"containerStatuses": []any{obj{
			"name": "peer-probe", "state": obj{"terminated": obj{"exitCode": float64(17), "message": marker}},
		}}},
	}}}
	probeErr := peerProbeJobFailure("stale", "probe-job", job, pods)
	if probeErr == nil || !strings.Contains(probeErr.Error(), "classification=backoff_limit_exceeded") || !strings.Contains(probeErr.Error(), "exitCode=17") {
		t.Fatalf("probe failure=%v, want bounded failure class and exit code", probeErr)
	}
	if strings.Contains(probeErr.Error(), marker) {
		t.Fatalf("probe error exposed raw Pod output: %v", probeErr)
	}

	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"source-head.txt":      []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":        []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"actual-exit.txt":      []byte("1\n"),
		"peer-probe-stale.log": []byte("probe output: " + marker + "\n"),
		"events.jsonl":         append(encoded(obj{"event": "peer_probe_failed", "error": probeErr.Error(), "rawLogs": marker}), '\n'),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("project safe artifacts: %v", err)
	}
	publicEvents, err := os.ReadFile(filepath.Join(raw, "public", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(publicEvents), marker) || strings.Contains(string(publicEvents), probeErr.Error()) {
		t.Fatalf("public projection exposed probe diagnostics: %s", publicEvents)
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "peer-probe-stale.log")); !os.IsNotExist(err) {
		t.Fatalf("private probe log entered public projection: err=%v", err)
	}
}

func TestPeerStageUsesActualTransportForControlAndRetiredIdentity(t *testing.T) {
	if err := actualPeerStage(t, true); err != nil {
		t.Fatalf("valid pre-fault peer identity was not accepted over QUIC: %v", err)
	}
	err := actualPeerStage(t, false)
	if phase, _, cause, ok := peerAuthenticationRejection(err); !ok || phase != "response-decode" || cause != "*errors.errorString" {
		t.Fatalf("retired sender did not receive exact server-side auth rejection: %v", err)
	}
}

func actualPeerStage(t *testing.T, senderIsVoter bool) error {
	t.Helper()
	const clusterID types.ClusterID = "peer-probe-test"
	const senderToken, targetToken = "old-peer-token-for-test", "target-peer-token-for-test"
	sender := quepaxa.Member{ID: "old-node", PublicKey: quepaxa.PublicKey(network.PeerPublicKey(clusterID, "old-node", senderToken))}
	target := quepaxa.Member{ID: "survivor", PublicKey: quepaxa.PublicKey(network.PeerPublicKey(clusterID, "survivor", targetToken))}
	serverMembers := []quepaxa.Member{target}
	if senderIsVoter {
		serverMembers = append(serverMembers, sender)
	}
	cluster := quepaxa.Cluster{ConfigID: 7, Members: serverMembers}
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	var coreTransport *network.Transport
	if len(serverMembers) > 1 {
		coreTransport = network.NewTransport(clusterID, target.ID, &cluster, targetToken)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: target.ID, Cluster: cluster, WAL: wal, Transport: coreTransport})
	if err != nil {
		if coreTransport != nil {
			_ = coreTransport.Close()
		}
		_ = wal.Close()
		t.Fatal(err)
	}
	server := network.NewServer(core, nil, clusterID, true, coreTransport)
	serverCtx, cancel := context.WithCancel(context.Background())
	peer, err := network.StartPeerServer(serverCtx, "127.0.0.1:0", server, serverMembers, targetToken, "distinct-admin-token")
	if err != nil {
		cancel()
		server.Close()
		if coreTransport != nil {
			_ = coreTransport.Close()
		}
		_ = wal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = peer.Close()
		server.Close()
		if coreTransport != nil {
			_ = coreTransport.Close()
		}
		_ = wal.Close()
	})
	target.PeerURL = "quic://" + peer.Addr()
	client := network.NewTransport(clusterID, sender.ID, &quepaxa.Cluster{ConfigID: cluster.ConfigID, Members: []quepaxa.Member{target}}, senderToken)
	t.Cleanup(func() { _ = client.Close() })
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if senderIsVoter {
		if _, err := client.ReadTip(ctx, target.ID); err != nil {
			return err
		}
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "transport-probe", SQL: "SELECT 'transport-marker'"}})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(value)
	return client.StageValue(ctx, target.ID, quepaxa.ValueHash(hash), value)
}

func TestOperatorIsolationWaitsForProcessExit(t *testing.T) {
	deployment := obj{"spec": obj{"replicas": float64(0)}, "status": obj{"replicas": float64(0)}}
	if !operatorStoppedSnapshot(deployment, obj{"items": []any{}}) {
		t.Fatal("scaled-to-zero operator with no Pods was not considered stopped")
	}
	terminating := obj{"metadata": obj{"labels": obj{"app.kubernetes.io/name": "rhiza-operator"}, "deletionTimestamp": "2026-10-28T00:00:00Z"}, "status": obj{"phase": "Running"}}
	if operatorStoppedSnapshot(deployment, obj{"items": []any{terminating}}) {
		t.Fatal("terminating but still-running operator Pod was considered stopped")
	}
	exited := obj{"metadata": obj{"labels": obj{"app.kubernetes.io/name": "rhiza-operator"}, "deletionTimestamp": "2026-10-28T00:00:00Z"}, "status": obj{"phase": "Failed"}}
	if !operatorStoppedSnapshot(deployment, obj{"items": []any{exited}}) {
		t.Fatal("terminated operator Pod prevented isolation")
	}
	deployment["spec"].(obj)["replicas"] = float64(1)
	if operatorStoppedSnapshot(deployment, obj{"items": []any{}}) {
		t.Fatal("nonzero desired operator was considered stopped")
	}
}

func TestReplacementVoterSetRequiresExactNodeRemovalAndAddition(t *testing.T) {
	old := []quepaxa.NodeID{"pod-old", "pod-1", "pod-2"}
	want, err := replacementVoterSet(old, "pod-old", "pod-new")
	if err != nil || !sameNodeSet([]quepaxa.NodeID{"pod-new", "pod-1", "pod-2"}, want) {
		t.Fatalf("one-Pod voter replacement = %v, %v", want, err)
	}
	for _, tc := range []struct {
		name    string
		old     []quepaxa.NodeID
		removed quepaxa.NodeID
		added   quepaxa.NodeID
	}{
		{"removed identity absent", []quepaxa.NodeID{"pod-1", "pod-2"}, "pod-old", "pod-new"},
		{"new identity already present", old, "pod-old", "pod-1"},
		{"duplicate old identity", []quepaxa.NodeID{"pod-old", "pod-old"}, "pod-old", "pod-new"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := replacementVoterSet(tc.old, tc.removed, tc.added); err == nil {
				t.Fatal("ambiguous replacement membership accepted")
			}
		})
	}
}

func preparedPodFixture() obj {
	fixture := obj{
		"metadata": obj{"name": "rhiza-0", "uid": "new-pod-uid", "annotations": obj{"rhiza.mrchypark.dev/prepare-operation-id": "binding-uid:bootstrap"}},
		"spec": obj{
			"initContainers": []obj{{
				"name": "rhiza-prepare", "image": "rhiza-fencing-host:local",
				"command": []string{"/usr/local/bin/rhiza"}, "args": []string{"--prepare-restore"},
				"env": []obj{
					{"name": "RHIZA_CLUSTER_MEMBERS", "valueFrom": obj{"secretKeyRef": obj{"name": "rhiza-cluster-generated", "key": "members"}}},
					field("POD_NAME", "metadata.name"), field("POD_UID", "metadata.uid"),
					field("RHIZA_PREPARED_POD_UID", "metadata.uid"),
					field("RHIZA_PREPARE_OPERATION_ID", "metadata.annotations['rhiza.mrchypark.dev/prepare-operation-id']"),
					value("FENCER_AUTHORITY_URL", "https://rhiza-fencer:9444/v1/prepare-restore"),
					value("FENCER_CA_FILE", "/fencer-ca/ca.crt"),
				},
				"envFrom":      []obj{{"configMapRef": obj{"name": "rhiza-config"}}, {"secretRef": obj{"name": "rhiza-object-store"}}},
				"volumeMounts": []obj{{"name": "data", "mountPath": "/data"}, {"name": "fence-token", "mountPath": "/var/run/secrets/rhiza-fencer", "readOnly": true}, {"name": "fence-ca", "mountPath": "/fencer-ca", "readOnly": true}},
			}},
			"containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}}},
			"volumes": []obj{
				{"name": "data", "emptyDir": obj{}},
				{"name": "fence-token", "projected": obj{"sources": []obj{{"serviceAccountToken": obj{"path": "token", "audience": "rhiza-fencer", "expirationSeconds": 600}}}}},
				{"name": "fence-ca", "configMap": obj{"name": "rhiza-fencer-ca"}},
			},
		},
		"status": obj{
			"initContainerStatuses": []obj{{"name": "rhiza-prepare", "state": obj{"terminated": obj{"exitCode": float64(0), "finishedAt": "2026-10-28T00:00:01Z"}}}},
			"containerStatuses":     []obj{{"name": "rhiza", "state": obj{"running": obj{"startedAt": "2026-10-28T00:00:02Z"}}}},
			"conditions":            []obj{{"type": "Ready", "status": "True"}},
		},
	}
	var normalized obj
	if err := json.Unmarshal(encoded(fixture), &normalized); err != nil {
		panic(err)
	}
	return normalized
}

func hasNamedMount(mounts []obj, name, path string) bool {
	for _, mount := range mounts {
		if nested(mount, "name") == name && nested(mount, "mountPath") == path {
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

// The captured CI summaries omit raw bytes and attestation annotations. Rebuild
// the same graph shape so every traversed descriptor still needs its real hash.
func TestDocker29ImageConfigGraph(t *testing.T) {
	config := "sha256:78ec9d4f75a30d82997fb638f276b6b1b8ee311b05fd637784ea94f4788000a5"
	foreign := "sha256:" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name                                 string
		foreign, ambiguous, corrupt, missing bool
	}{
		{name: "index ID and running import resolve to config"},
		{name: "foreign config", foreign: true},
		{name: "ambiguous platform", ambiguous: true},
		{name: "digest mismatch", corrupt: true},
		{name: "missing immutable content", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := map[string][]byte{}
			store := func(body string) string {
				d := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(body)))
				content[d] = []byte(body)
				return d
			}
			manifest := store(`{"config":{"digest":"` + config + `"}}`)
			runnable := `{"digest":"` + manifest + `","size":866,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"linux","architecture":"amd64"}}`
			attestation := `{"digest":"sha256:531ca7d0ffd5ffce13835fcef138be1e7794b9a9931aef3bb234336a56dad845","size":564,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"` + manifest + `"}}`
			if tc.ambiguous {
				attestation = strings.Replace(runnable, "amd64", "arm64", 1)
			}
			builtID := store(`{"manifests":[` + runnable + `,` + attestation + `]}`)
			alias := `{"digest":"` + builtID + `","size":855,"mediaType":"application/vnd.oci.image.index.v1+json"}`
			importID := store(`{"manifests":[` + alias + `,` + alias + `]}`)
			if tc.corrupt {
				content[builtID] = []byte(`{}`)
			}
			if tc.missing {
				delete(content, builtID)
			}
			run := func(_ context.Context, name string, args ...string) ([]byte, error) {
				d := args[len(args)-1]
				if name != "docker" {
					t.Fatalf("unexpected command %s", name)
				}
				if args[2] == "crictl" {
					if d == config || d == foreign {
						return encoded(obj{"status": obj{"id": d}}), nil
					}
					return nil, fmt.Errorf("captured CRI lookup failed")
				}
				if data, ok := content[d]; ok {
					return data, nil
				}
				return nil, fmt.Errorf("content unavailable")
			}
			running, _, runningErr := resolveImageConfig(context.Background(), "docker.io/library/import-2026-10-26@"+importID, run)
			if tc.foreign {
				builtID = store(`{"config":{"digest":"` + foreign + `"}}`)
			}
			built, _, builtErr := resolveImageConfig(context.Background(), builtID, run)
			matches := runningErr == nil && builtErr == nil && nested(running, "status", "id") == nested(built, "status", "id")
			if matches != (!tc.foreign && !tc.ambiguous && !tc.corrupt && !tc.missing) {
				t.Fatalf("matches=%v; running=%v (%v), built=%v (%v)", matches, running, runningErr, built, builtErr)
			}
			if matches {
				for _, reference := range []string{manifest, config} {
					cri, _, err := resolveImageConfig(context.Background(), reference, run)
					if err != nil || nested(cri, "status", "id") != config {
						t.Fatalf("manifest/config ID %s: %v, %v", reference, cri, err)
					}
				}
				for _, reference := range []string{"rhiza-fencing-host:local", "sha256:" + strings.Repeat("d", 64)} {
					if _, _, err := resolveImageConfig(context.Background(), reference, run); err == nil {
						t.Fatalf("accepted reference without authoritative immutable content: %s", reference)
					}
				}
			}
		})
	}
}

func TestArchiveIdentityDoesNotFollowAdvancedMembershipAndRetiredUIDsIncludeInitialPod(t *testing.T) {
	if got := sourceArchiveIdentity(9); got != 1 {
		t.Fatalf("archive identity followed advanced membership ConfigID: got %d, want fixed identity 1", got)
	}

	got := uniqueUIDs("chaos-old", []string{"replacement", "survivor"}, []string{"chaos-old", "replacement", "initial-other"})
	want := []string{"chaos-old", "replacement", "survivor", "initial-other"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime termination inventory = %v, want captured old UID first and deduplicated %v", got, want)
	}
}
