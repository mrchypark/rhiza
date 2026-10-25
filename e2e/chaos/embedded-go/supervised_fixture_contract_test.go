package main

import (
	"strings"
	"testing"
)

func TestChaosFixtureUsesSupervisedBootstrapBeforeVoters(t *testing.T) {
	fixture := readContractFile(t, "../operator_fixture.py")
	setup := readContractFile(t, "../operator_recovery.py")
	automatic := readContractFile(t, "../automatic_recovery.py")
	anchor := readContractFile(t, "../anchor_recovery.py")

	for _, want := range []string{
		`"replicas": 0 if supervised else 3`,
		`"replicas": 0 if supervised else 1`,
	} {
		if !strings.Contains(fixture, want) {
			t.Errorf("fixture does not hold supervised source/operator at zero replicas: %q", want)
		}
	}
	ordered := []string{
		"self.install_supervised_authority()",
		"self.apply(workloads)",
		"self.apply(self.supervised_root_resource())",
		"self.k(\"scale\", \"deployment/rhiza-operator\", \"--replicas=1\")",
		"self.wait(\"supervised root UID pinned before first Pod\"",
		"self.k(\"scale\", \"statefulset/rhiza\", \"--replicas=3\")",
		"self.wait(\"initial voter \"",
	}
	position := -1
	for _, want := range ordered {
		next := strings.Index(setup, want)
		if next <= position {
			t.Fatalf("supervised bootstrap order is missing or inverted at %q", want)
		}
		position = next
	}
	for _, want := range []string{
		`ctr["command"] = ["/usr/local/bin/rhiza-fencer"]`,
		`"supervise", "--", "/bin/sh", "-ec"`,
		`exec /usr/local/bin/rhiza-entrypoint`,
		`"audience": "rhiza-fencer"`,
	} {
		if !strings.Contains(automatic, want) {
			t.Errorf("automatic fixture is missing supervised child launch contract %q", want)
		}
	}
	if strings.Contains(automatic, "KindFencer") || strings.Contains(anchor, "KindFencer") {
		t.Fatal("supervised automatic and anchor scenarios must not run a competing proof writer")
	}
}

func TestSupervisedRuntimeProfileRestartsAgentAndHidesCaptureDetails(t *testing.T) {
	setup := readContractFile(t, "../operator_recovery.py")
	authority := readContractFile(t, "../../../deploy/operator/fencing/authority.yaml")
	for _, want := range []string{
		`self.k("patch", "deployment", "rhiza-fencer", "--type=strategic", "-p", json.dumps(`,
		`self.k("rollout", "restart", "daemonset/rhiza-runtime-agent")`,
		`self.k("rollout", "status", "daemonset/rhiza-runtime-agent", "--timeout=180s")`,
	} {
		if !strings.Contains(setup, want) {
			t.Errorf("supervised authority setup is missing %q", want)
		}
	}
	if strings.Contains(setup, `self.patch("deployment", "rhiza-fencer"`) ||
		!strings.Contains(setup, `"name": "authority", "envFrom": [`) {
		t.Fatal("authority envFrom update must use a name-keyed Deployment patch, not replace the containers array")
	}
	profilePatch := strings.Index(setup, `self.patch("configmap", "rhiza-runtime-agent-profile"`)
	restart := strings.Index(setup, `self.k("rollout", "restart", "daemonset/rhiza-runtime-agent")`)
	ready := strings.Index(setup, `self.k("rollout", "status", "daemonset/rhiza-runtime-agent"`)
	if profilePatch < 0 || restart <= profilePatch || ready <= restart {
		t.Fatal("runtime-agent restart/readiness must follow the actual authority UID profile patch")
	}
	for _, want := range []string{
		"image: rhiza-operator:latest",
		"command: [/usr/local/bin/rhiza-fencer]",
		"args: [serve, --tls-cert=/tls/tls.crt",
		"volumeMounts:",
		"securityContext:",
	} {
		if !strings.Contains(authority, want) {
			t.Errorf("authority manifest is missing required container configuration %q", want)
		}
	}

	for _, file := range []string{"../operator_recovery.py", "../automatic_recovery.py", "../anchor_recovery.py"} {
		source := readContractFile(t, file)
		if strings.Contains(source, `errors = [f"diagnostic capture failed:`) ||
			strings.Contains(source, `errors.append(f`) {
			t.Errorf("%s exposes raw diagnostic failure details publicly", file)
		}
		if !strings.Contains(source, `errors = ["diagnostic_capture_failed"]`) {
			t.Errorf("%s lacks a fixed public diagnostic failure code", file)
		}
	}
	if !strings.Contains(setup, `error_details.append({"code": code, "detail": detail})`) ||
		!strings.Contains(setup, `return sorted(set(errors))`) ||
		!strings.Contains(setup, `errors = ["diagnostic_capture_failed"]`) {
		t.Fatal("diagnostic details must remain private while public errors use fixed codes")
	}
}

func TestAutomaticExecutorOutageWaitsForCapturedAuthorityIdentity(t *testing.T) {
	automatic := readContractFile(t, "../automatic_recovery.py")
	for _, want := range []string{
		`authority_uid = authority.get("metadata", {}).get("uid", "")`,
		`initial_generation = authority.get("metadata", {}).get("generation")`,
		`selector_labels = authority.get("spec", {}).get("selector", {}).get("matchLabels", {})`,
		`identities.append((name, uid))`,
		`captured_pods = authority_pods()`,
		`captured_pod_uids = [uid for _, uid in captured_pods]`,
		`self.k("get", "pods", "-l", selector, "-o", "json")`,
		`if metadata.get("uid") != authority_uid:`,
		`if spec.get("replicas") != 0:`,
		`if generation <= initial_generation or observed_generation < generation:`,
		`if authority_pods():`,
		`for name, uid in captured_pods:`,
		`self.k("get", "pod", name, "--ignore-not-found", "-o", "json")`,
		`if pod_metadata["uid"] != uid:`,
		`self.wait("captured authority Deployment and Pods stopped", authority_absent, 180)`,
		`self.event("automatic_executor_outage_observed", deployment_uid=authority_uid,`,
	} {
		if !strings.Contains(automatic, want) {
			t.Errorf("automatic executor outage does not verify %q", want)
		}
	}

	outage := strings.Index(automatic, "def executor_outage(self, stopped):")
	partition := strings.Index(automatic, "self.executor_outage(True)")
	if outage < 0 || partition < outage {
		t.Fatal("automatic scenario no longer uses the verified executor outage")
	}
	deleteLostPod := strings.Index(automatic[partition:], `self.k("delete","pod","rhiza-2"`)
	if deleteLostPod < 0 {
		t.Fatal("lost-voter deletion is missing after executor outage observation")
	}
	observed := strings.Index(automatic[outage:], `self.wait("captured authority Deployment and Pods stopped"`)
	if observed < 0 || observed > partition-outage {
		t.Fatal("lost-voter fault can begin before the captured authority outage is observed")
	}
}
