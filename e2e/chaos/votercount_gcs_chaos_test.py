"""Local checks for the voterCount GCS/Chaos harness.

These run without a cluster: they render the manifests and exercise the
fail-safe assertions so an assertion cannot pass vacuously.
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


RUN_VALUES = dict(NAMESPACE="rhiza-chaos-abc12345", RUN_ID="abc12345", CLUSTER_ID="abc12345-cluster",
                  GCS_BUCKET="bucket", GCS_PREFIX="rhiza/abc12345/",
                  OPERATOR_IMAGE="ghcr.io/mrchypark/rhiza-operator@sha256:aa",
                  HOST_IMAGE="ghcr.io/mrchypark/rhiza-application@sha256:bb")


class RunManifestTest(unittest.TestCase):
    def docs(self):
        return list(yaml.safe_load_all(render("votercount_gcs_chaos.yaml.in", **RUN_VALUES)))

    def test_bootstrap_statefulset_starts_at_zero_and_exports_no_members(self):
        sts = next(d for d in self.docs() if d["kind"] == "StatefulSet")
        self.assertEqual(sts["spec"]["replicas"], 0)
        self.assertEqual(sts["spec"]["ordinals"], {"start": 0})
        body = json.dumps(sts)
        self.assertNotIn("RHIZA_CLUSTER_MEMBERS", body)
        self.assertEqual(sts["spec"]["template"]["spec"]["containers"][0]["name"], "rhiza")

    def test_cluster_uses_voter_count_bootstrap_opt_in(self):
        cluster = next(d for d in self.docs() if d["kind"] == "RhizaCluster")
        self.assertEqual(cluster["spec"]["voterCount"], 3)
        self.assertIs(cluster["spec"]["bootstrap"], True)
        self.assertEqual(cluster["spec"]["statefulSet"], "rhiza-voter")
        self.assertNotIn("voterPods", cluster["spec"])

    def test_operator_enables_automatic_kubernetes_fencing(self):
        deployment = next(d for d in self.docs() if d["kind"] == "Deployment"
                          and d["metadata"]["name"] == "rhiza-operator")
        env = {e["name"]: e.get("value") for e in
               deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["RHIZA_AUTOMATIC_RECOVERY"], "true")
        self.assertEqual(env["RHIZA_FENCER_BACKEND"], "kubernetes")

    def test_tokenreview_binding_is_run_named(self):
        crb = next(d for d in self.docs() if d["kind"] == "ClusterRoleBinding"
                   and "tokenreview" in d["metadata"]["name"])
        self.assertEqual(crb["metadata"]["name"], "rhiza-chaos-abc12345-fencer-tokenreview")


class AuthManifestTest(unittest.TestCase):
    def policies(self):
        text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", "rhiza-chaos-abc12345"), ("@OWNER@", "owner"),
                             ("@RUN_ID@", "abc12345"), ("@RUNTIME_KIND@", "ServiceAccount"),
                             ("@RUNTIME_API_GROUP@", ""), ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_NAMESPACE@", "    namespace: rhiza-chaos-abc12345")):
            text = text.replace(token, value)
        return [d for d in yaml.safe_load_all(text)
                if d and d.get("kind") == "ValidatingAdmissionPolicy"]

    def test_two_new_policies_are_added_alongside_the_reviewed_pair(self):
        names = {p["metadata"]["name"] for p in self.policies()}
        self.assertEqual(len(names), 4, names)
        for expected in ("rhiza-chaos-abc12345-faults", "rhiza-chaos-abc12345-network-faults",
                         "rhiza-chaos-abc12345-generation-faults",
                         "rhiza-chaos-abc12345-podkill-faults"):
            self.assertIn(expected, names)

    def test_new_policies_confine_selectors_to_this_run(self):
        for policy in self.policies():
            if not policy["metadata"]["name"].endswith(("-generation-faults", "-podkill-faults")):
                continue
            self.assertEqual(policy["spec"]["failurePolicy"], "Fail")
            body = json.dumps(policy)
            self.assertIn("chaos.rhiza.io/run", body)
            self.assertIn("abc12345", body)


def harness_stub():
    chaos = object.__new__(harness.VoterCountChaos)
    chaos.ns = "ns"
    chaos.run_id = "run"
    chaos.events = []
    chaos.event = lambda name, **fields: chaos.events.append({"event": name, **fields})
    chaos.recoveries = lambda: chaos._recs
    chaos._recs = []
    chaos.running = lambda label: [{"metadata": {"name": "up"}}]
    chaos.status = lambda pod: {"wal_identity": "wal-1"}
    chaos.get = lambda kind, name: {"metadata": {"uid": "uid-1"}}
    chaos.chaos = lambda *a, **k: None
    chaos.wait_chaos = lambda *a, **k: None
    chaos.delete_chaos = lambda *a, **k: None
    return chaos


class FailSafeTest(unittest.TestCase):
    def test_assert_no_proof_requires_a_real_unresolved_request(self):
        chaos = harness_stub()
        chaos.fences = lambda: []
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof("rhiza-voter-2")
        chaos.fences = lambda: [{"spec": {"targets": [{"pod": "rhiza-voter-2"}]},
                                 "metadata": {"name": "fence-x"},
                                 "status": {"proof": {"processesTerminated": True}}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_proof("rhiza-voter-2")
        chaos.fences = lambda: [{"spec": {"targets": [{"pod": "rhiza-voter-2"}]},
                                 "metadata": {"name": "fence-x"}, "status": {}}]
        self.assertEqual(len(chaos.assert_no_proof("rhiza-voter-2")), 1)

    def test_assert_no_new_child_detects_a_child(self):
        chaos = harness_stub()
        chaos._recs = [{"metadata": {"uid": "new"}}]
        with self.assertRaises(AssertionError):
            chaos.assert_no_new_child(set())

    def test_dead_pod_scenario_never_reports_pass(self):
        chaos = harness_stub()
        chaos.fences = lambda: [{"spec": {"targets": [{"pod": "rhiza-voter-2"}]},
                                 "metadata": {"name": "fence-x"}, "status": {}}]
        chaos.recoveries = lambda: []
        with mock.patch("time.sleep"):
            chaos.scenario_dead_pod_failsafe()
        names = [e["event"] for e in chaos.events]
        self.assertIn("dead_pod_failsafe_observed", names)
        self.assertNotIn("PASS", names)


if __name__ == "__main__":
    unittest.main()
