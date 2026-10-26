package main

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestCaptureGCSChaosPodsCleansAndReportsAmbiguousCurrentPod(t *testing.T) {
	first := gcsControlPodIdentity{Name: "rhiza-runtime-agent-a", UID: "uid-a"}
	second := gcsControlPodIdentity{Name: "rhiza-runtime-agent-b", UID: "uid-b"}
	patchErr := errors.New("patch response lost")
	cleanupErr := errors.New("label cleanup failed")
	marked := map[string]gcsControlPodIdentity{}
	cleaned := false
	err := captureGCSChaosPods([]gcsControlPodIdentity{first, second}, "capture-id",
		func(pod gcsControlPodIdentity) (bool, error) {
			if pod.Name == first.Name {
				return true, nil
			}
			return true, patchErr
		},
		func(pods map[string]gcsControlPodIdentity) error {
			cleaned = true
			if len(pods) != 2 || pods[first.Name] != first || pods[second.Name] != second {
				t.Fatalf("cleanup did not include both the successful and ambiguous exact Pod identities: %#v", pods)
			}
			return cleanupErr
		}, marked)
	if !cleaned {
		t.Fatal("cleanup was not attempted after the ambiguous label patch")
	}
	if err == nil || !strings.Contains(err.Error(), patchErr.Error()) || !strings.Contains(err.Error(), cleanupErr.Error()) {
		t.Fatalf("capture error did not preserve both patch and cleanup failures: %v", err)
	}
}

func TestGCSChaosPodNamesAreExplicitUniqueAndBounded(t *testing.T) {
	got, err := sortedGCSChaosPodNames([]string{"rhiza-voter-2", "rhiza-voter-0", "rhiza-voter-1"}, 2)
	if err != nil || strings.Join(got, ",") != "rhiza-voter-0,rhiza-voter-1,rhiza-voter-2" {
		t.Fatalf("sorted explicit cohort = %v, %v", got, err)
	}
	for _, input := range [][]string{
		{},
		{"rhiza-voter-0", "rhiza-voter-0"},
		{"*"},
		{"rhiza-voter-0", strings.Repeat("x", 64)},
	} {
		if _, err := sortedGCSChaosPodNames(input, 1); err == nil {
			t.Fatalf("accepted unsafe Pod set %q", input)
		}
	}
	tooMany := make([]string, gcsChaosMaxPods+1)
	for i := range tooMany {
		tooMany[i] = "rhiza-voter-" + strings.Repeat("a", i+1)
	}
	if _, err := sortedGCSChaosPodNames(tooMany, 1); err == nil {
		t.Fatal("accepted more than the bounded Pod capacity")
	}
}

func TestGCSVoterCohortSelectorsAreSameExplicitFullMesh(t *testing.T) {
	names := []string{"rhiza-voter-0", "rhiza-voter-1", "rhiza-voter-2"}
	labels := map[string]string{"app": "rhiza-voter", "app.kubernetes.io/part-of": "rhiza-chaos", gcsChaosRunLabel: "a1b2c3d4"}
	source := gcsChaosPodSelector("run-a1b2c3d4", labels, names)
	target := gcsChaosPodSelector("run-a1b2c3d4", labels, names)
	if !jsonSemanticEqual(source, target) {
		t.Fatal("full-mesh source and target selectors differ")
	}
	pods, ok := source["pods"].(obj)
	if !ok || !jsonSemanticEqual(pods["run-a1b2c3d4"], names) {
		t.Fatalf("cohort selector is not an exact namespace-scoped Pod set: %#v", source)
	}
}

func TestGCSNetworkChaosAdmissionRemainsNarrowAndAddsOnlyBoundedShapes(t *testing.T) {
	data, err := os.ReadFile("../chaos/gcs-postrelease-auth.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	policy := string(data)
	for _, required := range []string{
		"selectorRuntimeAgent",
		"selectorOperatorPod",
		"selectorVoterCohort",
		"rhiza.dev/gcs-capture",
		"variables.selector.labelSelectors['rhiza.dev/gcs-capture'].matches('^[a-f0-9]{32}$')",
		"size(variables.selector.labelSelectors) == 4",
		"size(variables.selector.labelSelectors) == 3",
		"size(variables.selector.pods['@NAMESPACE@']) <= 64",
		"object.spec.target.selector.pods == variables.selector.pods",
		"object.spec.target.selector.labelSelectors == variables.selector.labelSelectors",
		"object.spec.target.selector.labelSelectors['rhiza.dev/gcs-capture'] == variables.selector.labelSelectors['rhiza.dev/gcs-capture']",
		"object.spec.duration == '180s'",
		"object.spec.mode == 'all' && variables.selectorRuntimeAgent",
		"object.spec.mode == 'one' && variables.selectorOperatorPod",
		"variables.selector.pods['@NAMESPACE@'].all(p, p.matches('^rhiza-voter-[0-9]+$'))",
		"!has(variables.selector.nodes) && !has(variables.selector.nodeSelectors)",
		"!has(object.spec.target.selector.nodes)",
		"resources: [pods]\n    verbs: [create, get, list, watch, delete, patch]",
		"resources: [configmaps, services]\n    verbs: [create, get, list, watch, delete]",
	} {
		if !strings.Contains(policy, required) {
			t.Errorf("admission policy is missing bounded selector requirement %q", required)
		}
	}
	if got := strings.Count(policy, ".matches('^[a-f0-9]{32}$')"); got != 3 {
		t.Fatalf("capture selectors must enforce exactly 32 lowercase hex characters in all three selector shapes, got %d anchored checks", got)
	}
	if strings.Contains(policy, ".matches('[a-f0-9]{32}')") || strings.Contains(policy, ".matches('rhiza-voter-[0-9]+')") {
		t.Fatal("admission policy retains a substring-only capture or voter-name pattern")
	}
	if strings.Contains(policy, "resources: [pods, configmaps, services]\n    verbs: [create, get, list, watch, delete, patch]") {
		t.Fatal("patch permission is broader than the exact Pod label-capture operation")
	}
	if got := strings.Count(policy, "verbs: [create, get, list, watch, delete, patch]"); got != 1 {
		t.Fatalf("expected Pod patch to be the only create/get/list/watch/delete/patch grant, got %d", got)
	}
}

func TestGCSChaosCreateReconciliationRequiresExactIntentAndSelector(t *testing.T) {
	body := obj{
		"metadata": obj{
			"name": "gcs-agent-loss-runtime-agent", "namespace": "run-a1b2c3d4",
			"labels":      obj{gcsChaosRunLabel: "a1b2c3d4", "rhiza.dev/fault-phase": "agent-loss", "rhiza.dev/fault-role": "runtime-agent"},
			"annotations": obj{"rhiza.dev/intent-sha256": strings.Repeat("a", 64)},
		},
		"spec": obj{"action": "partition", "direction": "both", "mode": "all", "duration": gcsChaosDuration,
			"selector": obj{"namespaces": []string{"run-a1b2c3d4"}, "labelSelectors": obj{"rhiza.dev/gcs-capture": strings.Repeat("b", 32)}, "pods": obj{"run-a1b2c3d4": []string{"rhiza-runtime-agent-node-a"}}},
			"target":   obj{"mode": "all", "selector": obj{"namespaces": []string{"run-a1b2c3d4"}, "labelSelectors": obj{"rhiza.dev/gcs-capture": strings.Repeat("b", 32)}, "pods": obj{"run-a1b2c3d4": []string{"rhiza-fencer-a"}}}}},
	}
	specBytes, _ := json.Marshal(body["spec"])
	var currentSpec obj
	_ = json.Unmarshal(specBytes, &currentSpec)
	current := obj{
		"metadata": obj{"uid": "networkchaos-uid", "name": "gcs-agent-loss-runtime-agent", "namespace": "run-a1b2c3d4",
			"labels":      obj{gcsChaosRunLabel: "a1b2c3d4", "rhiza.dev/fault-phase": "agent-loss", "rhiza.dev/fault-role": "runtime-agent"},
			"annotations": obj{"rhiza.dev/intent-sha256": strings.Repeat("a", 64)}},
		"spec": currentSpec,
	}
	if !chaosObjectMatchesExpected(current, body, strings.Repeat("a", 64), "run-a1b2c3d4", "gcs-agent-loss-runtime-agent", "a1b2c3d4") {
		t.Fatal("did not accept exact deterministic NetworkChaos intent")
	}
	mutated := current["spec"].(obj)["selector"].(obj)
	mutated["pods"] = obj{"run-a1b2c3d4": []string{"rhiza-runtime-agent-replacement"}}
	if chaosObjectMatchesExpected(current, body, strings.Repeat("a", 64), "run-a1b2c3d4", "gcs-agent-loss-runtime-agent", "a1b2c3d4") {
		t.Fatal("accepted a same-name NetworkChaos with a different captured selector")
	}
}

func TestGCSChaosScopeAndSourceIdentityValidation(t *testing.T) {
	if err := validateGCSChaosScope("run-a1b2c3d4", "a1b2c3d4", "source-agent-loss"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ namespace, runID, phase string }{
		{"", "a1b2c3d4", "phase"},
		{"run-a1b2c3d4", "not-a-run", "phase"},
		{"run-a1b2c3d4", "a1b2c3d4", "BadPhase"},
	} {
		if err := validateGCSChaosScope(tc.namespace, tc.runID, tc.phase); err == nil {
			t.Errorf("accepted invalid scope %#v", tc)
		}
	}
	validAgent := obj{"metadata": obj{"name": "rhiza-runtime-agent-node-a", "uid": "agent-uid", "labels": obj{"app.kubernetes.io/name": "rhiza-runtime-agent"},
		"ownerReferences": []any{obj{"controller": true, "kind": "DaemonSet", "name": "rhiza-runtime-agent", "uid": "daemonset-uid"}}},
		"status": obj{"conditions": []any{obj{"type": "Ready", "status": "True"}}}}
	identity := gcsControlPodIdentity{Name: "rhiza-runtime-agent-node-a", UID: "agent-uid", OwnerUID: "daemonset-uid"}
	if !gcsChaosPodBelongs(validAgent, "a1b2c3d4", "runtime-agent", identity) {
		t.Fatal("did not recognize exact runtime-agent DaemonSet Pod in the isolated namespace")
	}
	validAgent["metadata"].(obj)["labels"].(obj)["app.kubernetes.io/name"] = "other"
	if gcsChaosPodBelongs(validAgent, "a1b2c3d4", "runtime-agent", identity) {
		t.Fatal("accepted a same-name Pod without the runtime-agent identity label")
	}
	validAgent["metadata"].(obj)["labels"].(obj)["app.kubernetes.io/name"] = "rhiza-runtime-agent"
	validAgent["metadata"].(obj)["uid"] = "replacement-uid"
	if gcsChaosPodBelongs(validAgent, "a1b2c3d4", "runtime-agent", identity) {
		t.Fatal("accepted a replacement Pod with the same name but a different UID")
	}
	validAgent["metadata"].(obj)["uid"] = "agent-uid"
	validAgent["metadata"].(obj)["ownerReferences"].([]any)[0].(obj)["uid"] = "replacement-daemonset"
	if gcsChaosPodBelongs(validAgent, "a1b2c3d4", "runtime-agent", identity) {
		t.Fatal("accepted a Pod with the right name and UID but a different controller UID")
	}
}
