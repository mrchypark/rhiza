package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
	"gopkg.in/yaml.v3"
)

type anchorRestoreFaultBucket struct {
	objstore.Bucket
	uploadErr   error
	getErrAt    int
	getErr      error
	closeErrAt  int
	closeErr    error
	uploadCalls int
	getCalls    int
}

type anchorRestoreCloseErrorReader struct {
	io.ReadCloser
	err error
}

func (r anchorRestoreCloseErrorReader) Close() error {
	return errors.Join(r.ReadCloser.Close(), r.err)
}

func (b *anchorRestoreFaultBucket) Upload(ctx context.Context, key string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploadCalls++
	if b.uploadErr != nil {
		return b.uploadErr
	}
	return b.Bucket.Upload(ctx, key, reader, options...)
}

func (b *anchorRestoreFaultBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.getCalls++
	if b.getErr != nil && b.getCalls == b.getErrAt {
		return nil, b.getErr
	}
	reader, err := b.Bucket.Get(ctx, key)
	if err == nil && b.closeErr != nil && b.getCalls == b.closeErrAt {
		reader = anchorRestoreCloseErrorReader{ReadCloser: reader, err: b.closeErr}
	}
	return reader, err
}

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

func TestSafeArtifactsPreservesSecondSameOrdinalReplacementAcceptance(t *testing.T) {
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":   []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"actual-exit.txt": []byte("0\n"),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := operator.FenceTarget{NodeID: "pod-old", Pod: "rhiza-0", PodUID: "old-pod-uid", WALIdentity: "old-wal"}
	next := operator.FenceTarget{NodeID: "pod-new", Pod: "rhiza-0", PodUID: "new-pod-uid", WALIdentity: "new-wal"}
	events := []obj{
		{"event": "podchaos_replacement_recovered", "oldUIDs": []string{old.PodUID}, "target": old, "rows": 1, "voters": 3, "configID": 2},
		{"event": "podchaos_second_same_ordinal_cycle_accepted", "oldUIDs": []string{old.PodUID}, "target": next, "rows": 1, "voters": 3, "configID": 3},
	}
	var data []byte
	for _, item := range events {
		data = append(data, encoded(item)...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(filepath.Join(raw, "events.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("project replacement evidence: %v", err)
	}
	projected, err := os.ReadFile(filepath.Join(raw, "public", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var got []eventView
	for _, line := range strings.Split(strings.TrimSpace(string(projected)), "\n") {
		var event eventView
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		got = append(got, event)
	}
	if len(got) != 2 || got[1].Event != "podchaos_second_same_ordinal_cycle_accepted" || string(got[1].Rows) != "1" ||
		got[1].Voters != 3 || got[1].ConfigID != 3 || got[1].Target == nil || *got[1].Target != next || !reflect.DeepEqual(got[1].OldUIDs, []string{old.PodUID}) {
		t.Fatalf("second replacement acceptance was not preserved in the public projection: %+v", got)
	}
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
	writeExecutable("git", `test "$*" = 'rev-parse HEAD' || exit 2
printf '%s\\n' `+strings.Repeat("a", 40))
	writeExecutable("docker", "printf '%s\\n' sha256:"+strings.Repeat("a", 64)+" sha256:"+strings.Repeat("b", 64)+" sha256:"+strings.Repeat("c", 64))
	// The production script uses openssl to create throwaway TLS key/cert
	// files. The fixture only needs those paths to exist for fake kubectl;
	// don't depend on openssl being installed in the minimal Alpine test image.
	writeExecutable("openssl", `while [ "$#" -gt 0 ]; do
  case "$1" in
    -keyout|-out) shift; : >"$1" ;;
  esac
  shift
done`)
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
	for job, root := range map[string]string{"operator-chaos": "operator-chaos-results", "automatic-operator-chaos": "automatic-chaos-results"} {
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
	anchorFound := false
	for _, step := range workflow.Jobs["anchor-operator-chaos"].Steps {
		if strings.HasPrefix(step.Uses, "actions/upload-artifact@") && strings.TrimSpace(step.With.Path) == "anchor-chaos-results" {
			anchorFound = true
		}
	}
	if !anchorFound {
		t.Fatal("anchor job must upload only its bounded evidence summary")
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

func TestPreRuntimeApplicationStatusesAcceptOnlyPodInitializing(t *testing.T) {
	valid := obj{"name": "rhiza", "ready": false, "restartCount": float64(0), "state": obj{"waiting": obj{"reason": "PodInitializing"}}}
	if !preRuntimeApplicationStatusesSafe([]any{valid}) {
		t.Fatal("normal kubelet PodInitializing application status was rejected while init runs")
	}
	if !preRuntimeApplicationStatusesSafe(nil) {
		t.Fatal("absent application status should remain admissible before kubelet publishes it")
	}
	for name, status := range map[string]obj{
		"running":              {"name": "rhiza", "ready": false, "restartCount": float64(0), "state": obj{"running": obj{}}},
		"terminated":           {"name": "rhiza", "ready": false, "restartCount": float64(0), "state": obj{"terminated": obj{}}},
		"restarted":            {"name": "rhiza", "ready": false, "restartCount": float64(1), "state": obj{"waiting": obj{"reason": "PodInitializing"}}},
		"unexpected-container": {"name": "unexpected", "ready": false, "restartCount": float64(0), "state": obj{"waiting": obj{"reason": "PodInitializing"}}},
		"other-wait":           {"name": "rhiza", "ready": false, "restartCount": float64(0), "state": obj{"waiting": obj{"reason": "ImagePullBackOff"}}},
		"prior-state":          {"name": "rhiza", "ready": false, "restartCount": float64(0), "lastState": obj{"terminated": obj{}}, "state": obj{"waiting": obj{"reason": "PodInitializing"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if preRuntimeApplicationStatusesSafe([]any{status}) {
				t.Fatal("unsafe application status accepted")
			}
		})
	}
	if preRuntimeApplicationStatusesSafe([]any{valid, valid}) {
		t.Fatal("unexpected additional application container accepted")
	}
}

func TestOnlineFenceSelectionIgnoresUnrelatedHistory(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "pod-old", WALIdentity: "wal-old"}
	makeFence := func(operation string, targets []operator.FenceTarget) obj {
		request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "root-uid", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "chaos-source", OperationID: operation, Scope: operator.ScopeVoter, Targets: targets}
		return obj{"spec": request, "status": obj{"proof": obj{}}}
	}
	unrelated := makeFence("earlier", []operator.FenceTarget{{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "older-uid", WALIdentity: "older-wal"}})
	intended := makeFence("current", []operator.FenceTarget{target})
	for _, items := range [][]obj{{unrelated, intended}, {intended, unrelated}} {
		selected := 0
		for _, item := range items {
			if isIntendedOnlineVoterFence(item, "sts-uid", "root-uid", target) {
				selected++
			}
		}
		if selected != 1 {
			t.Fatalf("selected %d exact target proofs from mixed history, want 1", selected)
		}
	}
	if isIntendedOnlineVoterFence(unrelated, "sts-uid", "root-uid", target) {
		t.Fatal("unrelated historical exact-scope proof selected for current target")
	}
}

func TestOnlineRepairRequiresRetainedAcceptedCompletion(t *testing.T) {
	record, old := completedOnlineRepairFixture(t)
	if !membershipRepairCompletionAccepted(record, "online-op", "sts-uid", "root-uid", old) {
		t.Fatal("exact retained ADD completion was rejected")
	}
	mutations := map[string]func(*fencing.MembershipRepairRecord){
		"not complete":              func(r *fencing.MembershipRepairRecord) { r.Progress.Stage = fencing.MembershipRepairAdded },
		"missing accepted response": func(r *fencing.MembershipRepairRecord) { r.Progress.Completion.Accepted = nil },
		"wrong accepted request digest": func(r *fencing.MembershipRepairRecord) {
			r.Progress.Completion.Accepted.RequestDigest = strings.Repeat("0", 64)
		},
		"wrong completion phase": func(r *fencing.MembershipRepairRecord) {
			r.Progress.Completion.Request.PhaseHash = strings.Repeat("0", 64)
		},
		"wrong ADD result": func(r *fencing.MembershipRepairRecord) { r.Progress.Results[0].Outcome = "pending" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := record
			bad.Progress = record.Progress
			bad.Progress.Phases = append([]fencing.MembershipRepairPhase(nil), record.Progress.Phases...)
			bad.Progress.Candidates = append([]fencing.MembershipRepairCandidate(nil), record.Progress.Candidates...)
			bad.Progress.Results = append([]fencing.MembershipRepairPhaseResult(nil), record.Progress.Results...)
			completion := *record.Progress.Completion
			request := completion.Request
			response := *completion.Accepted
			completion.Request, completion.Accepted = request, &response
			bad.Progress.Completion = &completion
			mutate(&bad)
			if membershipRepairCompletionAccepted(bad, "online-op", "sts-uid", "root-uid", old) {
				t.Fatal("incomplete or mismatched authority completion accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*fencing.MembershipRepairRecord){
		"wrong ADD source config": func(r *fencing.MembershipRepairRecord) {
			r.Progress.Completion.Request.SourceConfigID++
			r.Progress.Completion.Request.Target.ConfigID++
			r.Progress.Results[0].Target.ConfigID++
		},
		"wrong ADD abort slot": func(r *fencing.MembershipRepairRecord) {
			wrong := *r.Progress.Completion.Request.AbortSlot + 1
			r.Progress.Completion.Request.AbortSlot = &wrong
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := record
			bad.Progress = record.Progress
			bad.Progress.Phases = append([]fencing.MembershipRepairPhase(nil), record.Progress.Phases...)
			bad.Progress.Candidates = append([]fencing.MembershipRepairCandidate(nil), record.Progress.Candidates...)
			bad.Progress.Results = append([]fencing.MembershipRepairPhaseResult(nil), record.Progress.Results...)
			completion := *record.Progress.Completion
			request := completion.Request
			response := *completion.Accepted
			completion.Request, completion.Accepted = request, &response
			bad.Progress.Completion = &completion
			mutate(&bad)
			completion.Request.ScopeDigest = fencing.MembershipRepairCompletionScopeDigest(completion.Request)
			digest, err := network.MembershipRepairCompletionRequestDigest(completion.Request)
			if err != nil {
				t.Fatal(err)
			}
			completion.Accepted.ScopeDigest = completion.Request.ScopeDigest
			completion.Accepted.RequestDigest = digest
			if membershipRepairCompletionAccepted(bad, "online-op", "sts-uid", "root-uid", old) {
				t.Fatal("self-consistent completion for another ADD config/abort point accepted")
			}
		})
	}
}

func TestReplacementCompletionIsScopedToExactUIDLedgerAndLiveQuorum(t *testing.T) {
	record, old := completedOnlineRepairFixture(t)
	newMember := *record.Progress.Candidates[0].Member
	candidate := operator.FenceTarget{NodeID: string(newMember.ID), Pod: old.Pod, PodUID: "new-pod", WALIdentity: newMember.WALIdentity}
	otherMember := record.Authorization.Membership.Members[1]
	current := network.MembershipStatus{NodeID: newMember.ID, ClusterID: oldSourceClusterID, ConfigID: 3, Voters: []quepaxa.NodeID{otherMember.ID, newMember.ID}, WALIdentity: newMember.WALIdentity, Voting: true}
	statuses := []network.MembershipStatus{
		{NodeID: otherMember.ID, ClusterID: oldSourceClusterID, ConfigID: 3, Voters: append([]quepaxa.NodeID(nil), current.Voters...), WALIdentity: otherMember.WALIdentity, Voting: true},
		current,
	}
	encode := func(t *testing.T, record fencing.MembershipRepairRecord) string {
		t.Helper()
		ledger := fencing.Ledger{SourceClusterID: oldSourceClusterID, MembershipRepairHistory: map[string]fencing.MembershipRepairRecord{record.Authorization.OperationID: record}}
		data, err := json.Marshal(fenceLedgerState{StatefulSetUID: "sts-uid", Ledger: ledger})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	item := func(uid string, record fencing.MembershipRepairRecord) any {
		return obj{"metadata": obj{"uid": uid}, "data": obj{"ledger.json": encode(t, record)}}
	}
	good := obj{"items": []any{item(record.Authorization.LedgerUID, record)}}
	if !retainedReplacementRepairCompletionFromMaps(good, "sts-uid", "root-uid", old, candidate, 1, 8, current, statuses) {
		t.Fatal("exact pinned accepted ADD completion and current quorum were rejected")
	}
	cases := map[string]func(operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus){
		"wrong candidate UID": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			target.PodUID = "different-pod"
			return target, cfg, abort, live, all
		},
		"wrong source config": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			return target, cfg + 1, abort, live, all
		},
		"wrong old abort slot": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			return target, cfg, abort + 1, live, all
		},
		"wrong current candidate WAL": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			live.WALIdentity = "different-wal"
			return target, cfg, abort, live, all
		},
		"voter target config differs": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			all[0].ConfigID++
			return target, cfg, abort, live, all
		},
		"missing quorum voter": func(target operator.FenceTarget, cfg uint, abort quepaxa.Slot, live network.MembershipStatus, all []network.MembershipStatus) (operator.FenceTarget, uint, quepaxa.Slot, network.MembershipStatus, []network.MembershipStatus) {
			return target, cfg, abort, live, all[:1]
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			all := append([]network.MembershipStatus(nil), statuses...)
			target, cfg, abort, live, all := mutate(candidate, 1, 8, current, all)
			if retainedReplacementRepairCompletionFromMaps(good, "sts-uid", "root-uid", old, target, cfg, abort, live, all) {
				t.Fatal("mismatched per-cycle evidence accepted")
			}
		})
	}
	if retainedReplacementRepairCompletionFromMaps(obj{"items": []any{item("copied-ledger-uid", record)}}, "sts-uid", "root-uid", old, candidate, 1, 8, current, statuses) {
		t.Fatal("copied completion under a different ConfigMap UID authorized recovery")
	}
	if retainedReplacementRepairCompletionFromMaps(obj{"items": []any{item(record.Authorization.LedgerUID, record), item(record.Authorization.LedgerUID, record)}}, "sts-uid", "root-uid", old, candidate, 1, 8, current, statuses) {
		t.Fatal("ambiguous duplicate retained completions authorized recovery")
	}
}

func TestRetainedOnlineRepairCompletionRequiresPinnedLedgerUID(t *testing.T) {
	record, old := completedOnlineRepairFixture(t)
	encodeLedger := func(t *testing.T, record fencing.MembershipRepairRecord) string {
		t.Helper()
		ledger := fencing.Ledger{SourceClusterID: oldSourceClusterID, MembershipRepairHistory: map[string]fencing.MembershipRepairRecord{record.Authorization.OperationID: record}}
		data, err := json.Marshal(fenceLedgerState{StatefulSetUID: "sts-uid", Ledger: ledger})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	item := func(uid string, record fencing.MembershipRepairRecord) any {
		return obj{"metadata": obj{"uid": uid}, "data": obj{"ledger.json": encodeLedger(t, record)}}
	}

	staleCopy := item("replacement-ledger-uid", record)
	pending := record
	pending.Progress.Stage = fencing.MembershipRepairAdded
	realPending := item(record.Authorization.LedgerUID, pending)
	if retainedOnlineRepairCompletionFromMaps(obj{"items": []any{staleCopy, realPending}}, "online-op", "sts-uid", "root-uid", old) {
		t.Fatal("stale copied completion or correct-UID pending record authorized completion")
	}
	if !retainedOnlineRepairCompletionFromMaps(obj{"items": []any{staleCopy, item(record.Authorization.LedgerUID, record)}}, "online-op", "sts-uid", "root-uid", old) {
		t.Fatal("correctly UID-bound retained completion was rejected beside a stale copy")
	}
}

func TestOnlineRepairQualificationWaitsForAuthorityCompletion(t *testing.T) {
	child := operator.Resource{Status: operator.Status{Phase: "Complete", Membership: &operator.MembershipStatus{Phase: "Complete"}}}
	if onlineRepairQualificationReady(child, false) {
		t.Fatal("matching voter/child completion authorized qualification without retained authority acceptance")
	}
	if !onlineRepairQualificationReady(child, true) {
		t.Fatal("qualification did not proceed after exact retained authority completion")
	}
	child.Status.Membership.Phase = "Added"
	if onlineRepairQualificationReady(child, true) {
		t.Fatal("authority acceptance bypassed an incomplete operator membership status")
	}
}

func completedOnlineRepairFixture(t *testing.T) (fencing.MembershipRepairRecord, operator.FenceTarget) {
	t.Helper()
	old := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "old-pod", WALIdentity: "old-wal"}
	oldIdentity := fencing.Identity{SourceClusterID: "chaos-source", NodeID: old.NodeID, PodUID: old.PodUID, RunID: "old-run", WALIdentity: old.WALIdentity}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(old.NodeID), URL: "http://rhiza-2", PeerURL: "http://rhiza-2:9090", LogURL: "http://rhiza-2/log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old-key"))), WALIdentity: old.WALIdentity}
	otherMember := quepaxa.Member{ID: "rhiza-0", URL: "http://rhiza-0", PeerURL: "http://rhiza-0:9090", LogURL: "http://rhiza-0/log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other-key"))), WALIdentity: "other-wal"}
	baseMembers := []quepaxa.Member{oldMember, otherMember}
	baseHash := fmt.Sprintf("%x", sha256.Sum256(encoded(baseMembers)))
	abort := quepaxa.Slot(8)
	auth := fencing.MembershipRepairAuthorization{OperationID: "online-op", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64), RootUID: "root-uid", StatefulSetUID: "sts-uid", LedgerUID: "ledger-uid", SourceClusterID: "chaos-source", MembershipHash: baseHash, ExpectedAbortSlot: &abort, OldIdentity: oldIdentity, Membership: quepaxa.Cluster{ConfigID: 1, Members: baseMembers}}
	if err := auth.Validate(); err != nil {
		t.Fatalf("test authorization invalid: %v", err)
	}
	newID, err := fencing.ReplacementNodeID("new-pod")
	if err != nil {
		t.Fatal(err)
	}
	newMember := quepaxa.Member{ID: quepaxa.NodeID(newID), URL: "http://rhiza-2", PeerURL: "http://rhiza-2:9090", LogURL: "http://rhiza-2/log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("new-key"))), WALIdentity: strings.Repeat("4", 64)}
	assignment := fencing.BootstrapAssignment{OperationID: "init-op", RequestHash: strings.Repeat("c", 64), AuthorizationHash: strings.Repeat("d", 64), Purpose: fencing.BootstrapLearner, RootUID: "root-uid", StatefulSetUID: "sts-uid", SourceClusterID: "chaos-source", PodName: old.Pod, PodUID: "new-pod", NodeID: newID, CredentialName: "candidate-credentials", CredentialUID: "candidate-secret-uid", MembershipHash: strings.Repeat("e", 64), ProfileHash: strings.Repeat("f", 64), LedgerUID: "ledger-uid"}
	phaseChange := network.MembershipChange{OperationID: "online-op:add", ClusterID: "chaos-source", ExpectedConfigID: 2, ExpectedAbortSlot: abort, Add: &newMember}
	changeHash := fmt.Sprintf("%x", sha256.Sum256(encoded(phaseChange)))
	phase := fencing.MembershipRepairPhase{Kind: fencing.MembershipRepairAdd, CandidatePodUID: assignment.PodUID, Change: phaseChange, AbortSlot: &abort}
	phase.Hash, err = fencing.MembershipRepairPhaseHash(phase)
	if err != nil {
		t.Fatal(err)
	}
	target := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{otherMember, newMember}}
	result := fencing.MembershipRepairPhaseResult{PhaseHash: phase.Hash, JournalOperationID: phaseChange.OperationID, JournalRequestHash: changeHash, Target: target, FreezeSlot: 10, FreezeHash: strings.Repeat("1", 64), TerminalSlot: 12, TerminalHash: strings.Repeat("2", 64), Outcome: "completed"}
	request := network.MembershipRepairCompletionRequest{Version: 1, Action: network.MembershipRepairCompletionAction, Challenge: strings.Repeat("3", 64), AttemptRevision: 9, RepairOperationID: auth.OperationID, RepairBindingDigest: fencing.MembershipRepairAuthorizationHash(auth), PhaseHash: phase.Hash, JournalOperationID: phaseChange.OperationID, JournalRequestHash: changeHash, SourceClusterID: "chaos-source", SourceConfigID: 2, AbortSlot: &abort, FreezeSlot: result.FreezeSlot, FreezeHash: result.FreezeHash, TerminalSlot: result.TerminalSlot, TerminalHash: result.TerminalHash, Target: target, CandidatePodUID: assignment.PodUID, CandidateMember: newMember, CandidateBindRef: fencing.BootstrapAssignmentDigest(assignment), ExecutorNodeID: "rhiza-0", ExecutorPodUID: "other-pod", ExecutorRunID: "executor-run", ExecutorWALIdentity: strings.Repeat("5", 64)}
	request.ScopeDigest = fencing.MembershipRepairCompletionScopeDigest(request)
	digest, err := network.MembershipRepairCompletionRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	accepted := &network.MembershipRepairCompletionResponse{Version: request.Version, Action: request.Action, Challenge: request.Challenge, AttemptRevision: request.AttemptRevision, ScopeDigest: request.ScopeDigest, RequestDigest: digest, ExecutorNodeID: request.ExecutorNodeID, ExecutorPodUID: request.ExecutorPodUID, ExecutorRunID: request.ExecutorRunID, ExecutorWALIdentity: request.ExecutorWALIdentity, TerminalSlot: request.TerminalSlot, TerminalHash: request.TerminalHash, Outcome: "completed", MAC: "authority-signed"}
	return fencing.MembershipRepairRecord{Authorization: auth, Progress: fencing.MembershipRepairProgress{Stage: fencing.MembershipRepairComplete, Phases: []fencing.MembershipRepairPhase{phase}, Candidates: []fencing.MembershipRepairCandidate{{Assignment: assignment, Member: &newMember}}, Results: []fencing.MembershipRepairPhaseResult{result}, Completion: &fencing.MembershipRepairCompletion{Request: request, Accepted: accepted}}}, old
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
	var anchorConfig, anchorClientConfig, anchorDeployment, anchorService, operator, binding obj
	for _, item := range items {
		switch nested(item, "kind") {
		case "ConfigMap":
			switch nested(item, "metadata", "name") {
			case recoveryAnchorConfigMap:
				anchorConfig = item
			case "rhiza-recovery-anchor":
				anchorClientConfig = item
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
	if anchorConfig == nil || anchorClientConfig == nil || anchorDeployment == nil || anchorService == nil || operator == nil || binding == nil {
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
	clientData := nested(anchorClientConfig, "data")
	if nested(clientData, "RHIZA_RECOVERY_ANCHOR_URL") != "https://anchor-service."+namespace+".svc:9191" ||
		nested(clientData, "RHIZA_RECOVERY_ANCHOR_TOKEN_FILE") != "/var/run/secrets/rhiza-recovery-anchor-token/token" ||
		nested(clientData, "RHIZA_RECOVERY_ANCHOR_CA_FILE") != "/var/run/secrets/rhiza-recovery-anchor-ca/ca.crt" ||
		nested(clientData, "RHIZA_RECOVERY_ANCHOR_BINDING_POLICY") != "derived" ||
		nested(clientData, "RHIZA_RECOVERY_ANCHOR_STORAGE_ID") != nil || nested(binding, "spec", "anchorID") != recoveryAnchorID {
		t.Fatal("shared client config does not select derived binding with trusted URL and read-only credential paths")
	}
	for _, workload := range []struct {
		name       string
		deployment obj
	}{{name: "operator", deployment: operator}, {name: "anchor service", deployment: anchorDeployment}} {
		container := nested(workload.deployment, "spec", "template", "spec", "containers").([]obj)[0]
		sources := map[string]bool{}
		for _, source := range nested(container, "envFrom").([]obj) {
			for _, ref := range []string{"configMapRef", "secretRef"} {
				if name := text(nested(source, ref, "name")); name != "" {
					sources[name] = true
				}
			}
		}
		for _, name := range []string{"rhiza-config", "rhiza-object-store", "rhiza-recovery-anchor"} {
			if !sources[name] {
				t.Fatalf("%s does not consume effective store and shared anchor config %s", workload.name, name)
			}
		}
	}
}

func TestFixtureAnchorBindingMatchesEffectiveStoreAndRejectsReceiptGuess(t *testing.T) {
	items := manifests()["items"].([]obj)
	resources := map[string]obj{}
	for _, item := range items {
		resources[text(nested(item, "metadata", "name"))] = item
	}
	bindingConfig := e2eRecoveryAnchorStorageBindingConfig()
	rootStoreConfig := nested(resources["rhiza-config"], "data")
	storeCredentials := nested(resources["rhiza-object-store"], "stringData")
	for key, want := range map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": bindingConfig.Provider,
		"RHIZA_OBJSTORE_PREFIX":   bindingConfig.Prefix,
		"RHIZA_OBJSTORE_ENDPOINT": bindingConfig.Endpoint,
		"RHIZA_OBJSTORE_BUCKET":   bindingConfig.Bucket,
	} {
		actual := nested(rootStoreConfig, key)
		if key == "RHIZA_OBJSTORE_ENDPOINT" || key == "RHIZA_OBJSTORE_BUCKET" {
			actual = nested(storeCredentials, key)
		}
		if actual != want {
			t.Fatalf("effective store setting %s=%v, want binding input %q", key, actual, want)
		}
	}
	if nested(rootStoreConfig, "RHIZA_RECOVERY_ANCHOR_BINDING_POLICY") != nil {
		t.Fatal("anchor-only policy leaked into the application-wide object-store ConfigMap")
	}
	var anchorClientConfig obj
	for _, item := range items {
		if nested(item, "kind") == "ConfigMap" && nested(item, "metadata", "name") == "rhiza-recovery-anchor" {
			anchorClientConfig = item
		}
	}
	if anchorClientConfig == nil || nested(anchorClientConfig, "data", "RHIZA_RECOVERY_ANCHOR_BINDING_POLICY") != string(bindingConfig.Policy) {
		t.Fatal("shared anchor ConfigMap lacks the explicit derived policy")
	}
	source, err := expectedRecoveryAnchorBinding("chaos-source")
	if err != nil || initialRecoveryAnchorRecord().Binding != source {
		t.Fatalf("initial source record was not derived from effective store config: got=%+v want=%+v err=%v", initialRecoveryAnchorRecord().Binding, source, err)
	}
	target, err := expectedRecoveryAnchorBinding("target-cluster")
	if err != nil {
		t.Fatal(err)
	}
	request := recoveryanchor.Request{TargetClusterID: "target-cluster"}
	receipt := recoveryanchor.Receipt{Target: target}
	if err := validateRecoveryAnchorTargetBinding(request, receipt); err != nil {
		t.Fatalf("independently computed exact target binding rejected: %v", err)
	}
	receipt.Target.StorageID = strings.Repeat("0", 64)
	if err := validateRecoveryAnchorTargetBinding(request, receipt); err == nil {
		t.Fatal("receipt-provided storage binding was accepted instead of independently computed expectation")
	}
}

func TestFencingRunnerPreparesSharedAnchorInputsBeforeFencer(t *testing.T) {
	script, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	manifests := strings.Index(source, `"$FENCING_E2E_BIN" manifests >"$fencing_tmp/manifests.json"`)
	fencer := strings.Index(source, `k apply -f "$fencing_tmp/authority.yaml"`)
	if manifests < 0 || fencer < 0 || manifests >= fencer {
		t.Fatal("shared anchor and effective-store ConfigMaps must be created before the fencer Deployment")
	}
	for _, required := range []string{
		`k create secret generic rhiza-recovery-anchor-token --from-literal=token=anchor-test-token`,
		`k create configmap rhiza-recovery-anchor-ca --from-file="ca.crt=$fencing_tmp/anchor-tls.crt"`,
	} {
		index := strings.Index(source, required)
		if index < 0 || index >= manifests {
			t.Fatalf("runner does not provision shared anchor input before applying manifests: %s", required)
		}
	}
}

func TestStorageBindingConfigRequiresExplicitDerivedOrOpaquePolicy(t *testing.T) {
	tests := []struct {
		name    string
		config  recoveryanchor.StorageBindingConfig
		cluster string
		wantID  string
		wantErr bool
	}{
		{name: "derived from effective location", config: e2eRecoveryAnchorStorageBindingConfig(), cluster: "target-a"},
		{name: "explicit opaque mapping", config: recoveryanchor.StorageBindingConfig{Policy: recoveryanchor.StorageBindingOpaque, OpaqueStorageID: "independent-target-store"}, cluster: "target-a", wantID: "independent-target-store"},
		{name: "missing policy", config: recoveryanchor.StorageBindingConfig{Provider: "s3", Endpoint: "rhiza-minio:9000", Bucket: "rhiza", Prefix: "rhiza"}, cluster: "target-a", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding, err := test.config.ExpectedBinding(test.cluster)
			if test.wantErr {
				if err == nil {
					t.Fatal("binding config without an explicit supported policy was accepted")
				}
				return
			}
			if err != nil || binding.ClusterID != test.cluster || test.wantID != "" && binding.StorageID != test.wantID || binding.StorageID == "" {
				t.Fatalf("expected binding=%+v err=%v", binding, err)
			}
		})
	}
}

func TestAnchorRecoveryGateBindsExactRetainedIdentity(t *testing.T) {
	child := obj{
		"metadata": obj{"name": "recovery-name", "uid": "recovery-uid"},
		"spec":     obj{"recoveryID": "recovery-name", "sourceClusterID": "chaos-source", "anchorID": recoveryAnchorID},
		"status":   obj{"recoveryID": "recovery-name", "target": "target-cluster"},
	}
	operationID, target, err := exactAnchorRecoveryIdentity(child)
	if err != nil || operationID != "recovery-uid:recovery-name" || target != "target-cluster" {
		t.Fatalf("exact retained recovery identity=%q/%q err=%v", operationID, target, err)
	}
	for name, mutate := range map[string]func(obj){
		"wrong target path":          func(v obj) { v["status"].(obj)["target"] = "../other" },
		"wrong source":               func(v obj) { v["spec"].(obj)["sourceClusterID"] = "other" },
		"wrong anchor":               func(v obj) { v["spec"].(obj)["anchorID"] = "other" },
		"wrong retained recovery ID": func(v obj) { v["status"].(obj)["recoveryID"] = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := jsonCloneObject(t, child)
			mutate(bad)
			if _, _, err := exactAnchorRecoveryIdentity(bad); err == nil {
				t.Fatal("mismatched recovery identity accepted")
			}
		})
	}
	otherUID := jsonCloneObject(t, child)
	otherUID["metadata"].(obj)["uid"] = "other-uid"
	otherOperation, otherTarget, err := exactAnchorRecoveryIdentity(otherUID)
	if err != nil || otherOperation == operationID || otherTarget != target {
		t.Fatalf("new child incarnation was not distinguished in retained identity: %q/%q err=%v", otherOperation, otherTarget, err)
	}
}

func TestRestoreCorruptedAnchorPreservesCleanupErrorsAndDoesNotOverwriteNewState(t *testing.T) {
	const key = "rhiza/target/generation-anchor"
	ctx := context.Background()
	saved := []byte("exact saved generation anchor")
	corrupt := []byte("CORRUPTED")
	newer := []byte("a newer generation's artifact")
	newBucket := func() *objmetrics.MeteredBucket {
		t.Helper()
		bucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bucket.Close() })
		return bucket
	}
	seed := func(t *testing.T, bucket *objmetrics.MeteredBucket, value []byte) {
		t.Helper()
		if err := bucket.Upload(ctx, key, bytes.NewReader(value)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(t *testing.T, bucket *objmetrics.MeteredBucket) []byte {
		t.Helper()
		data, err := readAnchorObject(ctx, bucket, key, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	t.Run("successful conditional restore verifies exact bytes", func(t *testing.T) {
		bucket := newBucket()
		seed(t, bucket, corrupt)
		if err := restoreExactCorruptedAnchor(ctx, bucket, key, corrupt, saved); err != nil {
			t.Fatal(err)
		}
		if got := read(t, bucket); !bytes.Equal(got, saved) {
			t.Fatalf("restored bytes=%q, want exact saved bytes %q", got, saved)
		}
	})

	t.Run("upload failure is joined with primary failure and restoration was attempted", func(t *testing.T) {
		bucket := newBucket()
		seed(t, bucket, corrupt)
		uploadErr := errors.New("injected conditional restore upload failure")
		faulty := &anchorRestoreFaultBucket{Bucket: bucket, uploadErr: uploadErr}
		primaryErr := errors.New("injected primary gate failure")
		restoreErr := restoreExactCorruptedAnchor(ctx, faulty, key, corrupt, saved)
		joined := joinAnchorRestoreError(primaryErr, restoreErr)
		if !errors.Is(joined, primaryErr) || !errors.Is(joined, uploadErr) || faulty.uploadCalls != 1 {
			t.Fatalf("primary/restore errors or attempt lost: joined=%v uploadCalls=%d", joined, faulty.uploadCalls)
		}
		if got := read(t, bucket); !bytes.Equal(got, corrupt) {
			t.Fatalf("failed restore changed target unexpectedly: %q", got)
		}
	})

	t.Run("readback close failure is joined while restore remains verified by bytes", func(t *testing.T) {
		bucket := newBucket()
		seed(t, bucket, corrupt)
		closeErr := errors.New("injected restored-anchor reader close failure")
		faulty := &anchorRestoreFaultBucket{Bucket: bucket, closeErrAt: 2, closeErr: closeErr}
		primaryErr := errors.New("injected primary gate failure")
		restoreErr := restoreExactCorruptedAnchor(ctx, faulty, key, corrupt, saved)
		joined := joinAnchorRestoreError(primaryErr, restoreErr)
		if !errors.Is(joined, primaryErr) || !errors.Is(joined, closeErr) || faulty.uploadCalls != 1 {
			t.Fatalf("primary/close errors or restoration attempt lost: joined=%v uploadCalls=%d", joined, faulty.uploadCalls)
		}
		if got := read(t, bucket); !bytes.Equal(got, saved) {
			t.Fatalf("restore bytes changed despite close reporting: %q", got)
		}
	})

	t.Run("newer artifact is never overwritten", func(t *testing.T) {
		bucket := newBucket()
		seed(t, bucket, newer)
		faulty := &anchorRestoreFaultBucket{Bucket: bucket}
		if err := restoreExactCorruptedAnchor(ctx, faulty, key, corrupt, saved); err == nil {
			t.Fatal("restore accepted an artifact that no longer matches the injected corruption")
		}
		if faulty.uploadCalls != 0 {
			t.Fatalf("restore wrote over newer target state: uploads=%d", faulty.uploadCalls)
		}
		if got := read(t, bucket); !bytes.Equal(got, newer) {
			t.Fatalf("newer target artifact changed: %q", got)
		}
	})
}

func TestAnchorChaosJobConsumesCooperativeRuntimeEvidence(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	start := strings.Index(text, "  anchor-operator-chaos:")
	if start < 0 {
		t.Fatal("anchor job block not found")
	}
	end := strings.Index(text[start+1:], "\n  publish-chaos-image:")
	if end < 0 {
		t.Fatal("anchor job block terminator not found")
	}
	job := text[start : start+1+end]
	for _, required := range []string{
		"needs: cooperative-fencing",
		"actions/download-artifact@v4",
		"cooperative-fencing-${{ github.run_id }}",
		"sha256sum --check cooperative-fencing-results/public/SHA256SUMS",
		"test \"$(cat cooperative-fencing-results/public/source-head.txt)\" = \"$GITHUB_SHA\"",
		"verify-anchor-gate-evidence cooperative-fencing-results/public/events.jsonl cooperative-fencing-results/public/dr-qualification.json cooperative-fencing-results/public/source-head.txt \"$GITHUB_SHA\"",
		"Reject optional publication until the published host has exact-runtime qualification",
		"name: Build the qualification host for the optional publication flow\n        if: env.PUBLISH_CHAOS_IMAGE == 'true'",
		"exit 1",
	} {
		if !strings.Contains(job, required) {
			t.Fatalf("anchor job does not consume cooperative runtime evidence: missing %q", required)
		}
	}
	if strings.Contains(job, "anchor_recovery.py") || strings.Contains(job, "kind create cluster") {
		t.Fatal("anchor job still duplicates the runtime scenario or uses the parked Python gate")
	}
}

func TestReadQualifiedGenerationAnchorRequiresExactTargetAndOperation(t *testing.T) {
	bucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	anchor := recovery.GenerationAnchor{
		Version: 1, OperationID: "recovery-uid:recovery-name", SourcePrefix: "rhiza/chaos-source",
		TargetPrefix: "rhiza/target-cluster", SourceTip: 1,
		SourceManifest: strings.Repeat("a", 64), SourcePrefixHash: strings.Repeat("b", 64), SourceBootstrap: strings.Repeat("c", 64),
		TargetMembership: recovery.NewMembershipRecord("target-cluster", []quepaxa.Member{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}}, "before-ack"),
		Checkpoint:       recovery.GenerationCheckpoint{Index: 1, RootHash: strings.Repeat("d", 64), StateHash: strings.Repeat("e", 64)},
	}
	data, err := json.Marshal(anchor)
	if err != nil {
		t.Fatal(err)
	}
	key := recovery.GenerationAnchorKey("rhiza/target-cluster")
	if err := bucket.Upload(context.Background(), key, strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}
	got, err := readQualifiedGenerationAnchor(context.Background(), bucket, "target-cluster", anchor.OperationID)
	if err != nil || !reflect.DeepEqual(got, data) {
		t.Fatalf("exact target anchor bytes differ: %v", err)
	}
	if _, err := readQualifiedGenerationAnchor(context.Background(), bucket, "other-target", anchor.OperationID); err == nil {
		t.Fatal("anchor at another target path accepted")
	}
	if _, err := readQualifiedGenerationAnchor(context.Background(), bucket, "target-cluster", "other-operation"); err == nil {
		t.Fatal("anchor for another operation accepted")
	}
}

func jsonCloneObject(t *testing.T, value obj) obj {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var clone obj
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
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
