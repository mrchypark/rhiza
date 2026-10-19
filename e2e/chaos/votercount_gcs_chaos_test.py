"""Cluster-free checks for the voterCount GCS/Chaos harness.

Renders the manifests, checks the admission policies admit both the original
and the new fault shapes, and exercises the fail-safe helpers so an assertion
cannot pass vacuously.
"""
import json
import unittest
from unittest import mock
from pathlib import Path

import yaml

import votercount_gcs_chaos as harness

HERE = Path(__file__).resolve().parent


def render(path, **values):
    text = (HERE / path).read_text()
    for key, value in values.items():
        text = text.replace("__" + key + "__", value)
    return text


RUN = dict(NAMESPACE="rhiza-chaos-abc12345", RUN_ID="abc12345", CLUSTER_ID="abc12345-cluster",
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
        self.assertEqual(labels["app.kubernetes.io/part-of"], "rhiza-chaos")
        self.assertEqual(labels["app"], "rhiza-voter")

    def test_cluster_and_operator_contract(self):
        cluster = next(d for d in self.docs() if d["kind"] == "RhizaCluster")
        self.assertEqual((cluster["spec"]["voterCount"], cluster["spec"]["bootstrap"]), (3, True))
        dep = next(d for d in self.docs()
                   if d["kind"] == "Deployment" and d["metadata"]["name"] == "rhiza-operator")
        env = {e["name"]: e.get("value") for e in
               dep["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual((env["RHIZA_AUTOMATIC_RECOVERY"], env["RHIZA_FENCER_BACKEND"]),
                         ("true", "kubernetes"))

    def test_tokenreview_binding_is_run_named(self):
        crb = next(d for d in self.docs() if d["kind"] == "ClusterRoleBinding"
                   and "tokenreview" in d["metadata"]["name"])
        self.assertEqual(crb["metadata"]["name"], "rhiza-chaos-abc12345-fencer-tokenreview")


class AdmissionPolicyTest(unittest.TestCase):
    def policies(self):
        text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", "rhiza-chaos-abc12345"), ("@OWNER@", "owner"),
                             ("@RUN_ID@", "abc12345"), ("@RUNTIME_KIND@", "ServiceAccount"),
                             ("@RUNTIME_API_GROUP@", ""), ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_NAMESPACE@", "    namespace: rhiza-chaos-abc12345")):
            text = text.replace(token, value)
        return [d for d in yaml.safe_load_all(text)
                if d and d.get("kind") == "ValidatingAdmissionPolicy"]

    def test_reviewed_pair_is_extended_to_admit_both_shapes(self):
        names = {p["metadata"]["name"] for p in self.policies()}
        self.assertEqual(names, {"rhiza-chaos-abc12345-faults", "rhiza-chaos-abc12345-network-faults"})
        faults = json.dumps(next(p for p in self.policies()
                                 if p["metadata"]["name"].endswith("-faults")))
        for shape in ("container-kill", "pod-kill", "rhiza-voter-0", "rhiza-voter-2"):
            self.assertIn(shape, faults)
        net = json.dumps(next(p for p in self.policies()
                             if p["metadata"]["name"].endswith("-network-faults")))
        for shape in ("45s", "60s", "rhiza-operator", "rhiza-voter"):
            self.assertIn(shape, net)


def stub():
    chaos = object.__new__(harness.VoterCountChaos)
    chaos.ns, chaos.run_id = "ns", "run"
    chaos.events = []
    chaos.event = lambda name, **f: chaos.events.append({"event": name, **f})
    chaos.fences = lambda: getattr(chaos, "_fences", [])
    chaos.recoveries = lambda: getattr(chaos, "_recs", [])
    return chaos


IDENT = {"node_id": "rhiza-voter-2", "pod_uid": "uid-2", "wal_identity": "wal-2", "config_id": "c"}


class FailSafeTest(unittest.TestCase):
    def test_fence_for_matches_target_fields_structurally(self):
        chaos = stub()
        chaos._fences = [
            {"metadata": {"name": "a"}, "spec": {"targets": [
                {"pod": "rhiza-voter-2", "podUID": "uid-2", "walIdentity": "wal-2"}]}},
            {"metadata": {"name": "b"}, "spec": {"targets": [
                {"pod": "rhiza-voter-2", "podUID": "uid-9", "walIdentity": "wal-2"}]}}]
        self.assertEqual([f["metadata"]["name"] for f in chaos.fence_for(IDENT)], ["a"])

    def test_assert_no_proof_is_not_vacuous(self):
        chaos = stub()
        target = {"pod": "rhiza-voter-2", "podUID": "uid-2", "walIdentity": "wal-2"}
        chaos._fences = []
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof_structural(IDENT)
        chaos._fences = [{"metadata": {"name": "a"}, "spec": {"targets": [target]},
                          "status": {"proof": {"recreationBlocked": True}}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof_structural(IDENT)
        chaos._fences = [{"metadata": {"name": "a"}, "spec": {"targets": [target]}, "status": {}}]
        self.assertEqual(len(chaos.assert_no_proof_structural(IDENT)), 1)

    def test_assert_no_new_child_detects_a_child(self):
        chaos = stub()
        chaos._recs = [{"metadata": {"uid": "new"}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_new_child(set())

    def test_dead_pod_case_never_reports_pass(self):
        chaos = stub()
        target = {"pod": "rhiza-voter-2", "podUID": "uid-2", "walIdentity": "wal-2"}
        chaos._fences = [{"metadata": {"name": "a", "uid": "f1"}, "spec": {"targets": [target]}, "status": {}}]
        chaos._recs = []
        chaos.pod_identity = lambda name: dict(IDENT, node_id=name)
        chaos.cluster_phase = lambda: "Degraded"
        chaos.recovery_uids = lambda: set()
        chaos.pod = lambda name: {"status": {"phase": "Failed"}}
        chaos.create_chaos = lambda *a, **k: None
        chaos.chaos_injected = lambda *a, **k: True
        chaos.wait = lambda label, pred, timeout=0: pred()
        chaos.assert_controllers_running = lambda: True
        chaos.assert_cluster_observed = lambda: (1, 1)
        chaos.delete_chaos = lambda *a, **k: None
        with mock.patch("time.sleep"):
            chaos.dead_pod_case("D2", "rhiza-voter-2", "rhiza-voter-2")
        names = [e["event"] for e in chaos.events]
        self.assertIn("dead_pod_failsafe_observed", names)
        self.assertNotIn("PASS", names)


if __name__ == "__main__":
    unittest.main()
