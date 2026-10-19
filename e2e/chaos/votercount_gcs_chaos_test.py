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
    def test_default_is_positive_and_dead_pod_is_deferred(self):
        parser = harness.main.__globals__["argparse"]
        import sys as _sys
        old = _sys.argv
        _sys.argv = ["votercount_gcs_chaos.py", "--context", "c", "--namespace", "n", "--run-id", "r",
                     "--app-image", "a@sha256:x", "--operator-image", "o@sha256:y"]
        try:
            args = None
            for action in []:
                pass
            chaos = object.__new__(harness.VoterCountChaos)
            chaos.args = type("A", (), {"scenarios": "positive"})()
            chaos.run_id = "r"
            chaos.events = []
            chaos.event = lambda name, **f: chaos.events.append({"event": name, **f})
            chaos.prove_admission = lambda: True
            chaos.prepare = lambda: True
            chaos.run_positive = lambda: chaos.events.append({"event": "positive"})
            chaos.run_dead_pod = lambda: chaos.events.append({"event": "dead-pod"})
            chaos.run()
            names = [e["event"] for e in chaos.events]
            self.assertIn("positive", names)
            self.assertNotIn("dead-pod", names)
            complete = next(e for e in chaos.events if e["event"] == "complete")
            self.assertEqual(complete["fail_safe"], "not_run")
        finally:
            _sys.argv = old


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
