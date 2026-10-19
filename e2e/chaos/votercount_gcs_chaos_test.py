"""Cluster-free checks for the voterCount GCS/Chaos harness.

Renders the manifests, checks the admission policies are correlated (not a
cross-product of source, window and target), and exercises the fail-safe
helpers so an assertion cannot pass vacuously. The real admission decision is
proved on the cluster by VoterCountChaos.prove_admission(), because a token
search over policy text cannot evaluate CEL.
"""
import json
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

    def policy(self, suffix):
        return next(p for p in self.policies() if p["metadata"]["name"] == NS + suffix)

    def expressions(self, suffix):
        return [v["expression"] for v in self.policy(suffix)["spec"]["validations"]]

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
        body = correlated[0]
        self.assertIn("rhiza-voter-0", body)
        self.assertIn("has(object.spec.containerNames)", body)


def stub():
    chaos = object.__new__(harness.VoterCountChaos)
    chaos.ns, chaos.run_id = "ns", "run"
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

    def test_assert_no_new_child_detects_a_child(self):
        chaos = stub()
        chaos._recs = [{"metadata": {"uid": "new"}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_new_child(set())

    def test_wait_completion_binds_to_the_fence_operation(self):
        chaos = stub()
        chaos._recs = [
            {"metadata": {"uid": "c1"}, "spec": {"operationID": "other"}, "status": {"phase": "Complete"}},
            {"metadata": {"uid": "c2"}, "spec": {"operationID": "op-1"},
             "status": {"membership": {"phase": "Complete"}}}]
        chaos.wait = lambda label, pred, timeout=0: pred()
        child = chaos.wait_completion(set(), {"spec": {"operationID": "op-1"}})
        self.assertEqual(child["metadata"]["uid"], "c2")

    def test_dead_pod_case_never_reports_pass(self):
        chaos = stub()
        chaos._fences = [{"metadata": {"name": "a", "uid": "f1"}, "spec": {"targets": [dict(TARGET)]}, "status": {}}]
        chaos._recs = []
        chaos.pod_identity = lambda name: dict(IDENT, node_id=name)
        chaos.cluster_phase = lambda: "Healthy"
        chaos.recovery_uids = lambda: set()
        chaos.fence_uids = lambda: {"f0"}
        chaos.pod = lambda name: {"metadata": {"name": name}, "status": {"phase": "Failed"}}
        chaos.pods_with = lambda sel: []
        chaos.create_chaos = lambda *a, **k: None
        chaos.chaos_injected = lambda *a, **k: True
        chaos.experiment = lambda kind, name: {"podRecords": [{"name": "rhiza-voter-2"}]}
        chaos.wait = lambda label, pred, timeout=0: pred()
        chaos.assert_controllers_running = lambda: True
        chaos.assert_cluster_observed = lambda *a, **k: True
        chaos.delete_chaos = lambda *a, **k: None
        with mock.patch("time.sleep"):
            chaos.dead_pod_case("D2", "rhiza-voter-2")
        names = [e["event"] for e in chaos.events]
        self.assertIn("dead_pod_failsafe_observed", names)
        self.assertNotIn("PASS", names)

    def test_dead_pod_requires_a_new_fence(self):
        chaos = stub()
        chaos._fences = [{"metadata": {"name": "old", "uid": "f0"}, "spec": {"targets": [dict(TARGET)]}, "status": {}}]
        chaos._recs = []
        chaos.pod_identity = lambda name: dict(IDENT, node_id=name)
        chaos.cluster_phase = lambda: "Healthy"
        chaos.recovery_uids = lambda: set()
        chaos.fence_uids = lambda: {"f0"}
        chaos.pod = lambda name: {"metadata": {"name": name}, "status": {"phase": "Failed"}}
        chaos.pods_with = lambda sel: []
        chaos.create_chaos = lambda *a, **k: None
        chaos.chaos_injected = lambda *a, **k: True
        chaos.experiment = lambda kind, name: {"podRecords": [{"name": "rhiza-voter-1"}]}
        chaos.wait = lambda label, pred, timeout=0: pred()
        chaos.assert_controllers_running = lambda: True
        chaos.assert_cluster_observed = lambda *a, **k: True
        chaos.delete_chaos = lambda *a, **k: None
        with mock.patch("time.sleep"), self.assertRaises(AssertionError):
            chaos.dead_pod_case("D2", "rhiza-voter-1")


if __name__ == "__main__":
    unittest.main()
