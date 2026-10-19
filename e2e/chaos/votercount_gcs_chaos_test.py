"""Cluster-free checks for the voterCount GCS/Chaos harness.

Renders the manifests, checks the admission policies are correlated (not a
cross-product of source, window and target), and exercises the fail-safe,
binding, readback and cleanup helpers so none of them can pass vacuously. The
real admission decision is proved on the cluster by
VoterCountChaos.prove_admission(): a token search over policy text cannot
evaluate CEL.
"""
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import yaml

import votercount_gcs_chaos as harness

HERE = Path(__file__).resolve().parent
NS = "rhiza-chaos-abc12345"


def render(path, **values):
    text = (HERE / path).read_text()
    for key, value in values.items():
        text = text.replace("__" + key + "__", value)
    return text


RUN = dict(NAMESPACE=NS, RUN_ID="abc12345", CLUSTER_ID="abc12345-cluster",
           GCS_BUCKET="bucket", GCS_PREFIX="rhiza/abc12345/",
           OPERATOR_IMAGE="ghcr.io/mrchypark/rhiza-operator@sha256:aa",
           HOST_IMAGE="ghcr.io/mrchypark/rhiza-application@sha256:bb")


class RunManifestTest(unittest.TestCase):
    def docs(self):
        return [d for d in yaml.safe_load_all(render("votercount_gcs_chaos.yaml.in", **RUN)) if d]

    def test_bootstrap_statefulset_starts_at_zero_without_members(self):
        sts = next(d for d in self.docs() if d["kind"] == "StatefulSet")
        self.assertEqual(sts["spec"]["replicas"], 0)
        self.assertNotIn("RHIZA_CLUSTER_MEMBERS", json.dumps(sts))
        labels = sts["spec"]["template"]["metadata"]["labels"]
        self.assertEqual((labels["app.kubernetes.io/part-of"], labels["app"]),
                         ("rhiza-chaos", "rhiza-voter"))

    def test_cluster_and_operator_contract(self):
        cluster = next(d for d in self.docs() if d["kind"] == "RhizaCluster")
        self.assertEqual((cluster["spec"]["voterCount"], cluster["spec"]["bootstrap"]), (3, True))
        dep = next(d for d in self.docs()
                   if d["kind"] == "Deployment" and d["metadata"]["name"] == "rhiza-operator")
        env = {e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual((env["RHIZA_AUTOMATIC_RECOVERY"], env["RHIZA_FENCER_BACKEND"]),
                         ("true", "kubernetes"))

    def test_tokenreview_binding_is_run_named(self):
        crb = next(d for d in self.docs() if d["kind"] == "ClusterRoleBinding"
                   and "tokenreview" in d["metadata"]["name"])
        self.assertEqual(crb["metadata"]["name"], NS + "-fencer-tokenreview")

    def test_probe_pair_is_rendered_with_only_the_derivation_deltas(self):
        pods = {d["metadata"]["name"]: d for d in self.docs() if d["kind"] == "Pod"}
        self.assertEqual(set(pods), {"rhiza-inspector", "rhiza-inspector-control"})
        for pod, app, container in ((pods["rhiza-inspector"], "rhiza-inspector", "inspector"),
                                    (pods["rhiza-inspector-control"], "rhiza-inspector-control", "control")):
            self.assertEqual(pod["metadata"]["labels"],
                             {"chaos.rhiza.io/run": RUN["RUN_ID"], "app": app})
            self.assertEqual(pod["spec"]["serviceAccountName"], "rhiza-inspector")
            self.assertIs(pod["spec"]["automountServiceAccountToken"], False)
            self.assertNotIn("volumes", pod["spec"])
            self.assertEqual(pod["spec"]["restartPolicy"], "Never")
            self.assertEqual(pod["spec"]["containers"][0]["name"], container)
            self.assertEqual(pod["spec"]["containers"][0]["image"], RUN["HOST_IMAGE"])
            self.assertEqual(pod["spec"]["containers"][0]["command"], ["sh", "-c", "sleep 1500"])

        def derived(pod, container):
            shape = json.loads(json.dumps(pod))
            shape["metadata"]["name"] = "<name>"
            shape["metadata"]["labels"]["app"] = "<app>"
            shape["spec"]["containers"][0]["name"] = container
            shape["spec"].pop("affinity", None)
            return shape

        self.assertEqual(derived(pods["rhiza-inspector"], "<container>"),
                         derived(pods["rhiza-inspector-control"], "<container>"))

    def test_probe_pair_is_pinned_to_one_node(self):
        pods = {d["metadata"]["name"]: d for d in self.docs() if d["kind"] == "Pod"}
        self.assertNotIn("affinity", pods["rhiza-inspector"]["spec"])
        self.assertEqual(pods["rhiza-inspector-control"]["spec"]["affinity"], {
            "podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
                {"labelSelector": {"matchLabels": {"chaos.rhiza.io/run": RUN["RUN_ID"],
                                                     "app": "rhiza-inspector"}},
                 "topologyKey": "kubernetes.io/hostname"}]}})

    def test_the_deny_all_policy_stays_bootstrap_owned(self):
        # Re-applying the bootstrap policy from this manifest would strip its
        # rhiza.dev/auth-owner annotation, so the run must only observe it.
        self.assertNotIn("rhiza-inspector-deny-all", json.dumps(self.docs()))

    def test_probe_block_is_marked_for_the_launcher_split(self):
        text = render("votercount_gcs_chaos.yaml.in", **RUN)
        start = text.index("# BEGIN INSPECTOR PROBE")
        block = text[start:text.index("# END INSPECTOR PROBE")]
        self.assertIn("name: rhiza-inspector\n", block)
        self.assertIn("name: rhiza-inspector-control\n", block)
        self.assertNotIn("name: rhiza-inspector", text[:start])


class AdmissionPolicyTest(unittest.TestCase):
    def policies(self):
        text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", NS), ("@OWNER@", "owner"), ("@RUN_ID@", "abc12345"),
                             ("@RUNTIME_KIND@", "ServiceAccount"), ("@RUNTIME_API_GROUP@", ""),
                             ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_NAMESPACE@", "    namespace: " + NS)):
            text = text.replace(token, value)
        return [d for d in yaml.safe_load_all(text)
                if d and d.get("kind") == "ValidatingAdmissionPolicy"]

    def expressions(self, suffix):
        policy = next(p for p in self.policies() if p["metadata"]["name"] == NS + suffix)
        return [v["expression"] for v in policy["spec"]["validations"]]

    def test_exactly_the_reviewed_pair_is_extended(self):
        self.assertEqual({p["metadata"]["name"] for p in self.policies()},
                         {NS + "-faults", NS + "-network-faults"})

    def test_network_windows_are_correlated_in_one_disjunction(self):
        exprs = self.expressions("-network-faults")
        both = [e for e in exprs if "'45s'" in e and "'60s'" in e]
        self.assertEqual(len(both), 1, "windows must be correlated, not a cross-product")
        for expr in exprs:
            if "'45s'" in expr:
                self.assertIn("selectorIsolatedVoter", expr)
            if "'60s'" in expr:
                self.assertIn("selectorOperator", expr)

    def test_podchaos_shapes_are_correlated(self):
        exprs = self.expressions("-faults")
        correlated = [e for e in exprs if "container-kill" in e and "pod-kill" in e]
        self.assertEqual(len(correlated), 1)
        self.assertIn("rhiza-voter-0", correlated[0])


def stub():
    chaos = object.__new__(harness.VoterCountChaos)
    chaos.ns, chaos.run_id = "ns", "run"
    chaos.out = Path("/tmp")
    chaos.row_seq = 0
    chaos.acks = set()
    chaos.cleanup_errors = []
    chaos.cluster_scoped = [("ns-fencer-tokenreview", "clusterrolebinding")]
    chaos.events = []
    chaos.event = lambda name, **f: chaos.events.append({"event": name, **f})
    chaos.fences = lambda: getattr(chaos, "_fences", [])
    chaos.recoveries = lambda: getattr(chaos, "_recs", [])
    return chaos


IDENT = {"node_id": "rhiza-voter-2", "pod_uid": "uid-2", "wal_identity": "wal-2", "config_id": "c"}
TARGET = {"nodeID": "rhiza-voter-2", "pod": "rhiza-voter-2", "podUID": "uid-2", "walIdentity": "wal-2"}


class FailSafeTest(unittest.TestCase):
    def test_fence_for_requires_all_four_identity_fields(self):
        chaos = stub()
        chaos._fences = [
            {"metadata": {"name": "full", "uid": "f1"}, "spec": {"operationID": "op-1", "targets": [dict(TARGET)]}},
            {"metadata": {"name": "no-node", "uid": "f2"}, "spec": {"targets": [{k: v for k, v in TARGET.items() if k != "nodeID"}]}},
            {"metadata": {"name": "wrong-node", "uid": "f3"}, "spec": {"targets": [dict(TARGET, nodeID="rhiza-voter-9")]}}]
        self.assertEqual([f["metadata"]["name"] for f in chaos.fence_for(IDENT)], ["full"])
        self.assertEqual([f["metadata"]["name"] for f in chaos.fence_for(IDENT, operation="op-1")], ["full"])
        self.assertEqual(chaos.fence_for(IDENT, new_since={"f1"}), [])

    def test_assert_no_proof_is_not_vacuous(self):
        chaos = stub()
        chaos._fences = []
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof_structural(IDENT)
        chaos._fences = [{"metadata": {"name": "a", "uid": "f1"}, "spec": {"targets": [dict(TARGET)]},
                          "status": {"proof": {"recreationBlocked": True}}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof_structural(IDENT)
        chaos._fences = [{"metadata": {"name": "a", "uid": "f1"}, "spec": {"targets": [dict(TARGET)]}, "status": {}}]
        self.assertEqual(len(chaos.assert_no_proof_structural(IDENT)), 1)

    def test_wait_completion_binds_to_the_exact_operation_field(self):
        chaos = stub()
        chaos._recs = [
            {"metadata": {"uid": "c1"}, "spec": {"note": "op-1"}, "status": {"phase": "Complete"}},
            {"metadata": {"uid": "c2"}, "spec": {"membership": {"operationID": "op-1"}},
             "status": {"membership": {"phase": "Complete"}}}]
        chaos.wait = lambda label, pred, timeout=0: pred()
        child = chaos.wait_completion(set(), {"spec": {"operationID": "op-1"}})
        self.assertEqual(child["metadata"]["uid"], "c2")

    def test_readback_writes_a_unique_row_and_verifies_afterwards(self):
        chaos = stub()
        counts = iter([3, 4, 5, 6])
        chaos.verify_data = lambda pod: next(counts)
        writes = []
        chaos.execute = lambda pod, rid, sql: writes.append((rid, sql))
        chaos.readback("rhiza-voter-1", "tag-a")
        chaos.readback("rhiza-voter-1", "tag-b")
        self.assertEqual([w[0] for w in writes], ["vc-post-tag-a", "vc-post-tag-b"])
        rows = [w[1] for w in writes]
        self.assertNotEqual(rows[0], rows[1], "rows must not collide")
        self.assertEqual([e["event"] for e in chaos.events], ["readback_pass", "readback_pass"])

    def test_cleanup_failure_is_hard(self):
        chaos = stub()
        chaos.declared = {"chaos_created": []}
        chaos.stop = mock.MagicMock()
        chaos.writer = None
        chaos.delete_chaos = lambda *a, **k: None
        chaos.delete_cluster_scoped = lambda: (_ for _ in ()).throw(AssertionError("leftover"))
        chaos.command = lambda *a, **k: ""
        with mock.patch.object(harness.Chaos, "cleanup", lambda self: None):
            with self.assertRaises(AssertionError):
                chaos.cleanup()


def dead_pod_stub(same_uid=False):
    chaos = stub()
    chaos._fences = [{"metadata": {"name": "a", "uid": "f1"}, "spec": {"targets": [dict(TARGET)]}, "status": {}}]
    chaos._recs = []
    chaos.pod_identity = lambda name: dict(IDENT, node_id=name)
    chaos.cluster_phase = lambda: "Healthy"
    chaos.cluster_revision = lambda: "1"
    chaos.recovery_uids = lambda: set()
    chaos.fence_uids = lambda: {"f0"}
    chaos.status = lambda name: {"wal_identity": "wal-2"}
    chaos.pod = lambda name: {"metadata": {"name": name, "uid": "uid-2" if same_uid else "uid-new"},
                              "status": {"phase": "Failed"}}
    chaos.pods_with = lambda sel: []
    chaos.create_chaos = lambda *a, **k: None
    chaos.chaos_injected = lambda *a, **k: True
    chaos.experiment = lambda kind, name: {"podRecords": [{"name": "rhiza-voter-2"}]}
    chaos.wait = lambda label, pred, timeout=0: pred()
    chaos.assert_controllers_running = lambda: True
    chaos.assert_cluster_observed = lambda *a, **k: True
    chaos.delete_chaos = lambda *a, **k: None
    return chaos


class ScenarioSplitTest(unittest.TestCase):
    ARGV = ["--context", "c", "--namespace", "n", "--run-id", "r",
            "--app-image", "a@sha256:" + "a" * 64, "--operator-image", "o@sha256:" + "b" * 64,
            "--inspector-probe", "inspector-probe.yaml"]

    def test_default_arguments_are_positive(self):
        self.assertEqual(harness.build_parser().parse_args(self.ARGV).scenarios, "positive")

    def test_default_run_neither_validates_nor_clears_part2_state(self):
        chaos = object.__new__(harness.VoterCountChaos)
        chaos.args = harness.build_parser().parse_args(self.ARGV)
        chaos.run_id = "r"
        chaos.events = []
        chaos.event = lambda name, **f: chaos.events.append({"event": name, **f})
        chaos.prove_admission = lambda: chaos.events.append({"event": "admission"}) or True
        chaos.prepare = lambda: True
        chaos.run_positive = lambda: chaos.events.append({"event": "positive"})
        chaos.run_dead_pod = lambda: chaos.events.append({"event": "dead-pod"})
        chaos.reset_fences = lambda: chaos.events.append({"event": "reset"})
        chaos.run()
        names = [e["event"] for e in chaos.events]
        self.assertIn("positive", names)
        self.assertNotIn("dead-pod", names)
        self.assertNotIn("reset", names)
        start = next(e for e in chaos.events if e["event"] == "start")
        self.assertEqual(start["claim_failsafe"], "not_run")
        complete = next(e for e in chaos.events if e["event"] == "complete")
        self.assertEqual(complete["fail_safe"], "not_run")

    def test_positive_admission_proof_omits_the_part2_shape(self):
        chaos = object.__new__(harness.VoterCountChaos)
        chaos.args = harness.build_parser().parse_args(self.ARGV)
        chaos.ns, chaos.run_id = "n", "r"
        actions = []

        def apply_json(body, extra=None):
            actions.append(body["spec"]["action"])
            denied = "forbidden" in body["metadata"]["name"]
            return type("R", (), {"returncode": 1 if denied else 0, "stderr": ""})()

        chaos.apply_json = apply_json
        chaos.voter_selector = lambda pod: {"namespaces": ["n"]}
        chaos.isolate_spec = lambda: {"action": "partition", "target": {"selector": {}}}
        chaos.generation_spec = lambda: {"action": "partition", "target": {"selector": {}}}
        chaos.event = lambda *a, **k: None
        chaos.prove_admission()
        self.assertIn("container-kill", actions)
        self.assertNotIn("pod-kill", actions)


class DeadPodTest(unittest.TestCase):
    def test_dead_pod_case_never_reports_pass(self):
        chaos = dead_pod_stub()
        with mock.patch("time.sleep"):
            chaos.dead_pod_case("D2", "rhiza-voter-2")
        names = [e["event"] for e in chaos.events]
        self.assertIn("dead_pod_failsafe_observed", names)
        self.assertNotIn("PASS", names)

    def test_dead_pod_requires_a_new_fence(self):
        chaos = dead_pod_stub()
        chaos._fences = [{"metadata": {"name": "old", "uid": "f0"}, "spec": {"targets": [dict(TARGET)]}, "status": {}}]
        chaos.fence_uids = lambda: {"f0"}
        with mock.patch("time.sleep"), self.assertRaises(AssertionError):
            chaos.dead_pod_case("D2", "rhiza-voter-2")

    def test_dead_pod_rejects_a_surviving_incarnation(self):
        chaos = dead_pod_stub(same_uid=True)
        with mock.patch("time.sleep"), self.assertRaises(AssertionError):
            chaos.dead_pod_case("D2", "rhiza-voter-2")


class InspectorIsolationTest(unittest.TestCase):
    """The paired NetworkPolicy isolation gate must not pass vacuously."""

    def stub(self):
        chaos = stub()
        chaos.args = harness.build_parser().parse_args(ScenarioSplitTest.ARGV)
        chaos.out = Path(tempfile.mkdtemp())
        return chaos

    def probing(self):
        chaos = self.stub()
        chaos.api_endpoint = lambda: ("10.0.0.1", "443")
        chaos.validate_deny_all_policy = lambda canonical=None, receipt="": {"metadata": {"uid": "p1"}}
        chaos.validate_policy_inventory = lambda canonical, receipt: None
        chaos.capture_probe_pods = lambda: {harness.INSPECTOR: {"metadata": {"uid": "u1"}},
                                            harness.INSPECTOR_CONTROL: {"metadata": {"uid": "u2"}}}
        chaos.validate_probe_pair = lambda receipts: ("u1", "u2", "n1")
        chaos.probe_egress = lambda pod, script, *args: "paired-network-policy-proof=%s\n" % pod
        chaos.get = lambda kind, name: {"metadata": {
            "uid": "u1" if name == harness.INSPECTOR else "u2"}}
        chaos.release_probe_pods = lambda: None
        return chaos

    def test_default_mode_is_required_and_bad_modes_are_rejected(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("RHIZA_INSPECTOR_NETWORK_ISOLATION", None)
            chaos = self.stub()
            self.assertEqual(chaos.isolation_mode(), "required")
        chaos.args.inspector_network_isolation = "skipped-because-i-say-so"
        with self.assertRaises(AssertionError):
            chaos.isolation_mode()

    def test_exclusion_records_a_scoped_receipt_without_probing(self):
        chaos = self.stub()
        chaos.args.inspector_network_isolation = "exclude-user-approved"
        chaos.capture_probe_pods = lambda: (_ for _ in ()).throw(AssertionError("must not probe"))
        self.assertEqual(chaos.verify_inspector_network_isolation(),
                         "EXCLUDED inspector-network-isolation: user-approved")
        self.assertEqual((chaos.out / "inspector-network-isolation.txt").read_text().strip(),
                         "EXCLUDED inspector-network-isolation: user-approved")

    def test_pass_writes_the_receipt_and_the_three_proofs(self):
        chaos = self.probing()
        self.assertEqual(chaos.verify_inspector_network_isolation(), "PASS inspector-network-isolation")
        self.assertEqual((chaos.out / "inspector-network-isolation.txt").read_text().strip(),
                         "PASS inspector-network-isolation")
        names = [e["event"] for e in chaos.events]
        self.assertIn("inspector_network_isolation_pass", names)
        self.assertNotIn("inspector_network_isolation_fail", names)
        passed = next(e for e in chaos.events if e["event"] == "inspector_network_isolation_pass")
        self.assertEqual(passed["node"], "n1")

    def test_a_reachable_inspector_fails_the_run_and_is_evidence(self):
        chaos = self.probing()
        chaos.probe_egress = lambda pod, script, *args: (_ for _ in ()).throw(
            AssertionError("first failure: inspector-egress-denied exit 41"))
        with self.assertRaises(AssertionError):
            chaos.verify_inspector_network_isolation()
        failure = next(e for e in chaos.events if e["event"] == "inspector_network_isolation_fail")
        self.assertIn("exit 41", failure["error"])
        self.assertFalse((chaos.out / "inspector-network-isolation.txt").exists())

    def test_release_failure_is_a_cleanup_error(self):
        chaos = self.stub()
        chaos.k = lambda *argv: json.dumps({"items": [{"metadata": {"name": harness.INSPECTOR}}]})
        chaos.release_probe_pods()
        self.assertEqual(len(chaos.cleanup_errors), 1)
        self.assertIn(harness.INSPECTOR, chaos.cleanup_errors[0])

    def test_the_positive_pass_requires_the_isolation_receipt(self):
        chaos = self.stub()
        chaos.assert_bootstrap = lambda: None
        chaos.verify_inspector_network_isolation = lambda: "FAIL"
        for scenario in ("scenario_intact_wal_restart", "scenario_online_replacement",
                         "scenario_generation_recovery"):
            setattr(chaos, scenario, lambda: None)
        (chaos.out / "inspector-network-isolation.txt").write_text("FAIL\n")
        with self.assertRaises(AssertionError):
            chaos.run_positive()
        (chaos.out / "inspector-network-isolation.txt").write_text("PASS inspector-network-isolation\n")
        chaos.run_positive()
        passed = next(e for e in chaos.events if e["event"] == "positive_set_pass")
        self.assertEqual(passed["inspector_network_isolation"], "PASS inspector-network-isolation")

    def test_control_derivation_rejects_drift(self):
        chaos = self.stub()
        chaos.validate_probe_pod = lambda pod, receipt, name, app, container: pod["metadata"]["name"]
        inspector = {"metadata": {"name": harness.INSPECTOR, "labels": {"app": harness.INSPECTOR}},
                     "spec": {"nodeName": "n1", "containers": [{"name": "inspector", "image": "a"}]}}
        control = json.loads(json.dumps(inspector))
        control["metadata"]["name"] = harness.INSPECTOR_CONTROL
        control["metadata"]["labels"]["app"] = harness.INSPECTOR_CONTROL
        control["spec"]["containers"][0]["name"] = "control"
        chaos.get = lambda kind, name: inspector if name == harness.INSPECTOR else control
        receipts = {harness.INSPECTOR: {}, harness.INSPECTOR_CONTROL: {}}
        self.assertEqual(chaos.validate_probe_pair(receipts)[2], "n1")
        control["spec"]["containers"][0]["image"] = "drifted"
        with self.assertRaises(AssertionError):
            chaos.validate_probe_pair(receipts)

    def test_a_pair_split_across_nodes_fails_the_gate(self):
        chaos = self.stub()
        chaos.validate_probe_pod = lambda pod, receipt, name, app, container: pod["metadata"]["name"]
        inspector = {"metadata": {"name": harness.INSPECTOR, "labels": {"app": harness.INSPECTOR}},
                     "spec": {"nodeName": "n1", "containers": [{"name": "inspector", "image": "a"}]}}
        control = json.loads(json.dumps(inspector))
        control["metadata"]["name"] = harness.INSPECTOR_CONTROL
        control["metadata"]["labels"]["app"] = harness.INSPECTOR_CONTROL
        control["spec"]["containers"][0]["name"] = "control"
        control["spec"]["nodeName"] = "n2"
        chaos.get = lambda kind, name: inspector if name == harness.INSPECTOR else control
        with self.assertRaises(AssertionError):
            chaos.validate_probe_pair({harness.INSPECTOR: {}, harness.INSPECTOR_CONTROL: {}})

    def test_the_deny_all_must_be_the_only_networkpolicy(self):
        chaos = self.stub()
        canonical = {"metadata": {"name": harness.INSPECTOR_POLICY, "uid": "p1"}}
        chaos.k = lambda *argv: json.dumps({"items": [{"metadata": {"name": harness.INSPECTOR_POLICY,
                                                                      "uid": "p1"}}]})
        chaos.validate_policy_inventory(canonical, "inventory.json")
        receipt = json.loads((chaos.out / "inventory.json").read_text())
        self.assertEqual(receipt["items"][0]["metadata"]["uid"], "p1")
        for items in ([],
                      [{"metadata": {"name": harness.INSPECTOR_POLICY, "uid": "p9"}}],
                      [{"metadata": {"name": harness.INSPECTOR_POLICY, "uid": "p1"}},
                       {"metadata": {"name": "rhiza-inspector-additive-allow", "uid": "p2"}}]):
            with self.subTest(items=items):
                chaos.k = lambda *argv, items=items: json.dumps({"items": items})
                with self.assertRaises(AssertionError):
                    chaos.validate_policy_inventory(canonical, "inventory.json")

    def test_an_additive_policy_after_probing_fails_the_gate(self):
        chaos = self.probing()
        inventories = iter([
            {"items": [{"metadata": {"name": harness.INSPECTOR_POLICY, "uid": "p1"}}]},
            {"items": [{"metadata": {"name": harness.INSPECTOR_POLICY, "uid": "p1"}},
                        {"metadata": {"name": "rhiza-inspector-additive-allow", "uid": "p2"}}]}])
        chaos.validate_policy_inventory = harness.VoterCountChaos.validate_policy_inventory.__get__(chaos)
        chaos.k = lambda *argv: json.dumps(next(inventories))
        with self.assertRaises(AssertionError):
            chaos.verify_inspector_network_isolation()
        self.assertIn("inspector_network_isolation_fail", [e["event"] for e in chaos.events])

    def test_api_endpoint_must_be_exact_https_443(self):
        self.assertEqual(harness.parse_https_443("https://34.64.0.1"), ("34.64.0.1", "443"))
        self.assertEqual(harness.parse_https_443("https://34.64.0.1:443/"), ("34.64.0.1", "443"))
        self.assertEqual(harness.parse_https_443("https://[fe80::1]:443"), ("fe80::1", "443"))
        for bad in ("http://34.64.0.1", "https://34.64.0.1:8443", "34.64.0.1", "https:///x"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    harness.parse_https_443(bad)


class LauncherDigestTest(unittest.TestCase):
    """The launcher must reject anything that is not name@sha256:<64 hex>."""

    BASE = dict(RHIZA_RUN_ID="abc12345", RHIZA_NAMESPACE="ns", RHIZA_KUBE_CONTEXT="ctx",
                RHIZA_GCS_BUCKET="b", RHIZA_GCS_PREFIX="p/", RHIZA_CLUSTER_ID="cid",
                RHIZA_OPERATOR_IMAGE="ghcr.io/mrchypark/rhiza-operator@sha256:" + "f" * 64)

    def run_launcher(self, app_image):
        env = dict(os.environ, **self.BASE)
        env["RHIZA_APP_IMAGE"] = app_image
        return subprocess.run(["sh", str(HERE / "run-votercount-gcs-chaos.sh")],
                              env=env, capture_output=True, text=True, timeout=60)

    def test_malformed_digests_are_rejected(self):
        for bad in ("repo@sha256:", "repo@sha256:not-a-digest", "repo@sha256:zz",
                    "repo:v0.19.5", "repo@sha256:" + "a" * 63):
            with self.subTest(bad=bad):
                result = self.run_launcher(bad)
                self.assertEqual(result.returncode, 2, result.stderr[-200:])

    def test_non_hex_digest_is_rejected(self):
        result = self.run_launcher("repo@sha256:" + "g" * 64)
        self.assertEqual(result.returncode, 2, result.stderr[-200:])


if __name__ == "__main__":
    unittest.main()
