"""Cluster-free checks for the voterCount GCS/Chaos harness.

Renders the manifests, checks the admission policies are correlated (not a
cross-product of source, window and target), and exercises the fail-safe,
binding, readback and cleanup helpers so none of them can pass vacuously. The
real admission decision is proved on the cluster by
VoterCountChaos.prove_admission(): a token search over policy text cannot
evaluate CEL.
"""
import json
import io
import os
import hashlib
import re
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock
from pathlib import Path

import yaml

import votercount_gcs_chaos as harness
from operator_recovery import parse_json_documents

HERE = Path(__file__).resolve().parent
NS = "rhiza-chaos-abc12345"


def load_release_version():
    """Derive the current release identity forms from the SDK manifest.

    sdk/rust/Cargo.toml is the single source of truth for the release version
    family. The path is resolved from this test file's own directory so the
    derived values never depend on the caller's working directory. A missing
    or malformed version raises instead of silently defaulting.
    """
    manifest = HERE / "../../sdk/rust/Cargo.toml"
    try:
        source = manifest.read_text()
    except OSError as error:
        raise RuntimeError("cannot read the release manifest %s: %s" % (manifest, error)) from error
    match = re.search(r'^version = "([^"]+)"', source, re.M)
    if match is None:
        raise RuntimeError("%s declares no package version" % manifest)
    version = match.group(1)
    if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+$", version):
        raise RuntimeError("%s declares a malformed package version %r" % (manifest, version))
    return {
        "version": version,
        "compact": version.replace(".", ""),
        "prefix": "v" + version.replace(".", ""),
        "dotted": "v" + version,
    }


RELEASE = load_release_version()
RELEASE_VERSION = RELEASE["version"]
RELEASE_COMPACT = RELEASE["compact"]
RELEASE_PREFIX = RELEASE["prefix"]
RELEASE_DOTTED = RELEASE["dotted"]


def stale_release_prefix():
    """Return a release-family prefix that is guaranteed not to be the current one.

    The negative fixtures feed the harness a family that must be rejected, so
    the stale value may never equal the current family. Deriving it keeps the
    fixture a rejection case even when the SDK version moves onto the family
    that used to be hardcoded here.
    """
    for candidate in ("v0191", "v0190", "v0180"):
        if candidate != RELEASE_PREFIX:
            return candidate
    digits = RELEASE_COMPACT
    return "v" + digits[:-1] + ("0" if digits[-1] != "0" else "1")


STALE_RELEASE_PREFIX = stale_release_prefix()


def render(path, **values):
    text = (HERE / path).read_text()
    for key, value in values.items():
        text = text.replace("__" + key + "__", value)
    return text


RUN = dict(NAMESPACE=NS, RUN_ID="abc12345", CLUSTER_ID="abc12345-cluster",
           GCS_BUCKET="bucket", GCS_PREFIX="postrelease/v%s/abc12345/" % RELEASE_VERSION,
           POD_GSA="data@example.iam.gserviceaccount.com",
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

    def test_operator_uses_the_bootstrap_workload_identity(self):
        docs = self.docs()
        dep = next(d for d in docs if d["kind"] == "Deployment"
                   and d["metadata"]["name"] == "rhiza-operator")
        pod = dep["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], "rhiza-operator")
        binding = next(d for d in docs if d["kind"] == "RoleBinding"
                       and d["metadata"]["name"] == "rhiza-operator")
        self.assertEqual(binding["subjects"], [{"kind": "ServiceAccount", "name": "rhiza-operator",
                                                "namespace": NS}])
        ksa = next(d for d in docs if d["kind"] == "ServiceAccount"
                   and d["metadata"]["name"] == "rhiza-operator")
        self.assertEqual(ksa["metadata"]["annotations"]["iam.gke.io/gcp-service-account"], RUN["POD_GSA"])

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

    def test_workload_cpu_stays_inside_the_namespace_quota(self):
        # The namespace quota tracks limits.cpu, so every container must declare
        # it: a missing limit is rejected by the API server at admission.
        quota_text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", NS), ("@OWNER@", "owner"), ("@RUN_ID@", "abc12345"),
                             ("@POD_GSA@", "data@example.iam.gserviceaccount.com"),
                             ("@RUNTIME_KIND@", "ServiceAccount"), ("@RUNTIME_API_GROUP@", ""),
                             ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_NAMESPACE@", "    namespace: " + NS)):
            quota_text = quota_text.replace(token, value)
        quota = next(d for d in yaml.safe_load_all(quota_text)
                     if d and d.get("kind") == "ResourceQuota")
        hard = quota["spec"]["hard"]

        def millis(value):
            value = str(value)
            return int(value[:-1]) if value.endswith("m") else int(float(value) * 1000)

        docs = self.docs()
        voter_count = next(d for d in docs if d["kind"] == "RhizaCluster")["spec"]["voterCount"]
        limit_sum = request_sum = 0
        workload_kinds = {"Pod", "StatefulSet", "Deployment", "DaemonSet", "Job", "CronJob"}
        for doc in docs:
            if doc["kind"] not in workload_kinds:
                continue
            spec = doc.get("spec", {})
            if doc["kind"] == "Pod":
                pod_spec, copies = spec, 1
            elif doc["kind"] == "CronJob":
                pod_spec, copies = doc["jobTemplate"]["spec"]["template"]["spec"], 1
            else:
                pod_spec = spec["template"]["spec"]
                copies = (voter_count if doc["kind"] == "StatefulSet"
                          else spec.get("replicas", 1))
            for container in pod_spec.get("containers", []) + pod_spec.get("initContainers", []):
                resources = container.get("resources", {})
                where = "%s/%s container %s" % (doc["kind"], doc["metadata"]["name"], container["name"])
                for key in ("limits", "requests"):
                    self.assertIn("cpu", resources.get(key, {}),
                                  "%s declares no %s.cpu" % (where, key))
                limit_sum += millis(resources["limits"]["cpu"]) * copies
                request_sum += millis(resources["requests"]["cpu"]) * copies
        self.assertLessEqual(limit_sum, millis(hard["limits.cpu"]),
                             "workload limits.cpu exceeds the namespace quota")
        self.assertLessEqual(request_sum, millis(hard["requests.cpu"]),
                             "workload requests.cpu exceeds the namespace quota")

    def test_service_quota_covers_static_services_and_learner_retries(self):
        quota_text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", NS), ("@OWNER@", "owner"), ("@RUN_ID@", "abc12345"),
                             ("@POD_GSA@", "data@example.iam.gserviceaccount.com"),
                             ("@RUNTIME_KIND@", "ServiceAccount"), ("@RUNTIME_API_GROUP@", ""),
                             ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_NAMESPACE@", "    namespace: " + NS)):
            quota_text = quota_text.replace(token, value)
        quota = next(d for d in yaml.safe_load_all(quota_text)
                     if d and d.get("kind") == "ResourceQuota")
        hard = quota["spec"]["hard"]
        static_services = [d for d in self.docs() if d["kind"] == "Service"]
        learner_service_incarnations = 3
        declared_services = int(hard["services"])
        self.assertEqual({service["metadata"]["name"] for service in static_services},
                         {"rhiza-headless", "rhiza-fencer"})
        self.assertGreaterEqual(declared_services,
                                len(static_services) + learner_service_incarnations,
                                "service quota must leave room for three retained learner retry Services")


class BootstrapIdentityPreflightTest(unittest.TestCase):
    def preflight(self, mode, **overrides):
        env = dict(os.environ,
                   RHIZA_RUN_ID="abc12345",
                   RHIZA_HARNESS_SHA="a" * 40,
                   RHIZA_AUTH_EXPIRES="2099-01-01T00:00:00Z",
                   RHIZA_HOST_IMAGE="registry.example/rhiza@sha256:" + "a" * 64,
                   RHIZA_METADATA_IMAGE="registry.example/cloud-cli@sha256:" + "b" * 64,
                   RHIZA_GCP_PROJECT_ID="project-12345",
                   RHIZA_GCP_PROJECT_NUMBER="123456",
                   RHIZA_GKE_CLUSTER="cluster-1",
                   RHIZA_GKE_LOCATION="asia-northeast3",
                   RHIZA_KUBE_CONTEXT="ctx",
                   RHIZA_GCS_BUCKET="bucket-1",
                   RHIZA_GCS_PREFIX="postrelease/v%s/abc12345/" % RELEASE_VERSION,
                   RHIZA_NAMESPACE="rhiza-%s-abc12345" % RELEASE_PREFIX,
                   RHIZA_DATA_GSA_ID="rhiza-%s-gcs-abc12345" % RELEASE_PREFIX,
                   RHIZA_NODE_A="node-a", RHIZA_NODE_B="node-b", RHIZA_NODE_C="node-c")
        env.update(overrides)
        return subprocess.run(["sh", str(HERE / "bootstrap-gcs-postrelease.sh"), mode],
                              env=env, capture_output=True, text=True, timeout=30)

    def test_preflight_local_rejects_stale_namespace_before_cloud_access(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "gcloud-called"
            fake_gcloud = Path(directory) / "gcloud"
            fake_gcloud.write_text("#!/bin/sh\ntouch \"$RHIZA_TEST_GCLOUD_MARKER\"\nexit 99\n")
            fake_gcloud.chmod(0o755)
            result = self.preflight("preflight-local",
                                    RHIZA_NAMESPACE="rhiza-%s-abc12345" % STALE_RELEASE_PREFIX,
                                    RHIZA_DATA_GSA_ID="rhiza-%s-gcs-abc12345" % STALE_RELEASE_PREFIX,
                                    RHIZA_TEST_GCLOUD_MARKER=str(marker),
                                    PATH=directory + os.pathsep + os.environ["PATH"])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Namespace identity mismatch: expected rhiza-%s-abc12345, got rhiza-%s-abc12345"
                          % (RELEASE_PREFIX, STALE_RELEASE_PREFIX), result.stderr)
            self.assertFalse(marker.exists(), "stale identity reached gcloud before preflight rejection")

    def test_preflight_rejects_stale_ci_gsa_before_cloud_access(self):
        result = self.preflight("preflight",
                                RHIZA_WORKFLOW_SHA="a" * 40,
                                RHIZA_GITHUB_REPOSITORY="example/project",
                                RHIZA_GITHUB_REPOSITORY_ID="12345",
                                RHIZA_GITHUB_OWNER_ID="67890",
                                RHIZA_GITHUB_WORKFLOW_REF="example/project/.github/workflows/qualification.yml@refs/heads/main",
                                RHIZA_GITHUB_ENVIRONMENT="qualification",
                                RHIZA_CI_GSA_ID="rhiza-%s-ci-abc12345" % STALE_RELEASE_PREFIX)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CI GSA identity mismatch: expected rhiza-%s-ci-abc12345, got rhiza-%s-ci-abc12345"
                      % (RELEASE_PREFIX, STALE_RELEASE_PREFIX), result.stderr)


class BootstrapIdentityApplyTest(unittest.TestCase):
    def apply_local(self, **overrides):
        env = dict(os.environ,
                   RHIZA_RUN_ID="abc12345",
                   RHIZA_HARNESS_SHA="a" * 40,
                   RHIZA_AUTH_EXPIRES=(datetime.now(timezone.utc) + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   RHIZA_HOST_IMAGE="registry.example/rhiza@sha256:" + "a" * 64,
                   RHIZA_METADATA_IMAGE="registry.example/cloud-cli@sha256:" + "b" * 64,
                   RHIZA_GCP_PROJECT_ID="project-12345",
                   RHIZA_GCP_PROJECT_NUMBER="123456",
                   RHIZA_GKE_CLUSTER="cluster-1",
                   RHIZA_GKE_LOCATION="asia-northeast3",
                   RHIZA_KUBE_CONTEXT="ctx",
                   RHIZA_GCS_BUCKET="bucket-1",
                   RHIZA_GCS_PREFIX="postrelease/v%s/abc12345/" % RELEASE_VERSION,
                   RHIZA_NAMESPACE="rhiza-%s-abc12345" % RELEASE_PREFIX,
                   RHIZA_DATA_GSA_ID="rhiza-%s-gcs-abc12345" % RELEASE_PREFIX,
                   RHIZA_NODE_A="node-a", RHIZA_NODE_B="node-b", RHIZA_NODE_C="node-c")
        env.update(overrides)
        return subprocess.run(["sh", str(HERE / "bootstrap-gcs-postrelease.sh"), "apply-local"],
                              env=env, capture_output=True, text=True, timeout=30)

    def remote_tools(self, directory, marker):
        script = """#!/bin/sh
tool=${0##*/}
printf '%s %s\\n' "$tool" "$*" >> "$RHIZA_TEST_REMOTE_SENTINEL"
if [ "$tool" = gcloud ] && [ "$*" = "--project=project-12345 --quiet config get-value account" ]; then
    printf '%s\\n' "$RHIZA_EXPECTED_ADMIN_ACCOUNT"
    exit 0
fi
exit 97
"""
        for name in ("gcloud", "gh", "kubectl"):
            command = Path(directory) / name
            command.write_text(script)
            command.chmod(0o755)
        marker.write_text("")

    def test_apply_local_rejects_stale_namespace_before_any_remote_call(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "remote-calls"
            generator = Path(directory) / "fixture-generator"
            generator.write_text("#!/bin/sh\nexit 0\n")
            generator.chmod(0o755)
            self.remote_tools(directory, marker)
            result = self.apply_local(
                RHIZA_NAMESPACE="rhiza-%s-abc12345" % STALE_RELEASE_PREFIX,
                RHIZA_DATA_GSA_ID="rhiza-%s-gcs-abc12345" % STALE_RELEASE_PREFIX,
                RHIZA_BOOTSTRAP_GO="I_APPROVE_NEW_SCOPE",
                RHIZA_COST_CAP_KRW="10000",
                RHIZA_EXPECTED_ADMIN_ACCOUNT="admin@example.com",
                RHIZA_AUTH_STATE=str((Path(directory) / "auth-state").resolve()),
                RHIZA_FIXTURE_GENERATOR=str(generator),
                RHIZA_FIXTURE_GENERATOR_SHA256=hashlib.sha256(generator.read_bytes()).hexdigest(),
                RHIZA_TEST_REMOTE_SENTINEL=str(marker),
                PATH=directory + os.pathsep + os.environ["PATH"])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Namespace identity mismatch: expected rhiza-%s-abc12345, got rhiza-%s-abc12345"
                          % (RELEASE_PREFIX, STALE_RELEASE_PREFIX), result.stderr)
            self.assertEqual(marker.read_text(), "",
                             "stale identity reached a remote client before apply rejection")

    def test_apply_local_valid_identity_reaches_a_remote_call(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "remote-calls"
            generator = Path(directory) / "fixture-generator"
            generator.write_text("#!/bin/sh\nexit 0\n")
            generator.chmod(0o755)
            self.remote_tools(directory, marker)
            result = self.apply_local(
                RHIZA_BOOTSTRAP_GO="I_APPROVE_NEW_SCOPE",
                RHIZA_COST_CAP_KRW="10000",
                RHIZA_EXPECTED_ADMIN_ACCOUNT="admin@example.com",
                RHIZA_AUTH_STATE=str((Path(directory) / "auth-state").resolve()),
                RHIZA_FIXTURE_GENERATOR=str(generator),
                RHIZA_FIXTURE_GENERATOR_SHA256=hashlib.sha256(generator.read_bytes()).hexdigest(),
                RHIZA_TEST_REMOTE_SENTINEL=str(marker),
                PATH=directory + os.pathsep + os.environ["PATH"])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Stopped at project-identity (exit 97)", result.stderr)
            calls = marker.read_text()
            self.assertNotEqual(calls, "", "valid identity control did not reach any remote client")
            self.assertIn("gcloud --project=project-12345 --quiet config get-value account", calls)
            self.assertIn("gcloud --project=project-12345 --quiet projects describe project-12345 --format=json", calls)


class BootstrapReceiptRollbackTest(unittest.TestCase):
    def rollback_local(self, receipt_mutation=None, pool_receipt=None):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            state = directory / "auth-state"
            state.mkdir(mode=0o700)
            marker = directory / "remote-calls"
            marker.write_text("")
            for name in ("gcloud", "kubectl"):
                tool = directory / name
                tool.write_text("#!/bin/sh\nprintf '%s %s\\n' \"${0##*/}\" \"$*\" >> \"$RHIZA_TEST_REMOTE_SENTINEL\"\nexit 97\n")
                tool.chmod(0o755)
            expiry = (datetime.now(timezone.utc) + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            owner = "rhiza-postrelease-abc12345-" + "a" * 40
            identity = {
                "run": "abc12345", "workflow_sha": "a" * 40, "expiry": expiry,
                "cost_cap_krw": "10000", "owner": owner,
                "host_image": "registry.example/rhiza@sha256:" + "a" * 64,
                "namespace": "rhiza-%s-abc12345" % RELEASE_PREFIX,
                "data_gsa_id": "rhiza-%s-gcs-abc12345" % RELEASE_PREFIX,
                "ci_gsa_id": "unused-local-ci-account",
                "gcs_prefix": "postrelease/v%s/abc12345/" % RELEASE_VERSION,
                "pool": "rhiza-%s-gcs-abc12345" % RELEASE_PREFIX, "provider": "github",
                "auth_mode": "local",
            }
            if receipt_mutation:
                receipt_mutation(identity)
            identity_path = state / "identity.json"
            identity_path.write_text(json.dumps(identity))
            identity_path.chmod(0o600)
            if pool_receipt is not None:
                (state / "pool.created").write_text(owner + "\n")
                (state / "pool.created").chmod(0o600)
                pool_path = state / "pool-create.json"
                pool_path.write_text(json.dumps(pool_receipt))
                pool_path.chmod(0o600)
            env = dict(os.environ,
                       RHIZA_RUN_ID="abc12345", RHIZA_HARNESS_SHA="a" * 40,
                       RHIZA_AUTH_EXPIRES=expiry,
                       RHIZA_HOST_IMAGE="registry.example/rhiza@sha256:" + "a" * 64,
                       RHIZA_METADATA_IMAGE="registry.example/cloud-cli@sha256:" + "b" * 64,
                       RHIZA_GCP_PROJECT_ID="project-12345", RHIZA_GCP_PROJECT_NUMBER="123456",
                       RHIZA_GKE_CLUSTER="cluster-1", RHIZA_GKE_LOCATION="asia-northeast3",
                       RHIZA_KUBE_CONTEXT="ctx", RHIZA_GCS_BUCKET="bucket-1",
                       RHIZA_GCS_PREFIX="postrelease/v%s/abc12345/" % RELEASE_VERSION,
                       RHIZA_NAMESPACE="rhiza-%s-abc12345" % RELEASE_PREFIX,
                       RHIZA_DATA_GSA_ID="rhiza-%s-gcs-abc12345" % RELEASE_PREFIX,
                       RHIZA_NODE_A="node-a", RHIZA_NODE_B="node-b", RHIZA_NODE_C="node-c",
                       RHIZA_BOOTSTRAP_GO="I_APPROVE_NEW_SCOPE", RHIZA_COST_CAP_KRW="10000",
                       RHIZA_EXPECTED_ADMIN_ACCOUNT="admin@example.com", RHIZA_AUTH_STATE=str(state),
                       RHIZA_ROLLBACK_QUIESCENT="YES_VERIFIED", RHIZA_TEST_REMOTE_SENTINEL=str(marker),
                       PATH=str(directory) + os.pathsep + os.environ["PATH"])
            result = subprocess.run(["sh", str(HERE / "bootstrap-gcs-postrelease.sh"), "rollback-local"],
                                    env=env, capture_output=True, text=True, timeout=30)
            return result, marker.read_text()

    def test_rollback_rejects_stale_receipt_before_remote_call(self):
        result, calls = self.rollback_local(
            lambda identity: identity.update(namespace="rhiza-%s-abc12345" % STALE_RELEASE_PREFIX))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Receipt identity mismatch", result.stderr)
        self.assertEqual(calls, "")

    def test_rollback_rejects_release_family_change_before_remote_call(self):
        result, calls = self.rollback_local(
            lambda identity: identity.update(gcs_prefix="postrelease/v0.0.0/abc12345/"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Receipt identity mismatch", result.stderr)
        self.assertEqual(calls, "")

    def test_rollback_rejects_tampered_pool_receipt_before_remote_call(self):
        result, calls = self.rollback_local(pool_receipt={
            "name": "projects/123456/locations/global/workloadIdentityPools/tampered-pool"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Pool create receipt identity rejected", result.stderr)
        self.assertEqual(calls, "")


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

    def test_runtime_role_can_observe_the_qualification_resources(self):
        text = (HERE / "gcs-postrelease-auth.yaml.in").read_text()
        for token, value in (("@NAMESPACE@", NS), ("@RUN_ID@", "abc12345"),
                             ("@RUNTIME_KIND@", "ServiceAccount"), ("@RUNTIME_NAME@", "runtime"),
                             ("@RUNTIME_API_GROUP@", ""), ("@RUNTIME_NAMESPACE@", "    namespace: " + NS)):
            text = text.replace(token, value)
        role = next(d for d in yaml.safe_load_all(text)
                    if d and d.get("kind") == "Role" and d["metadata"]["name"] == "rhiza-ci-runtime")
        self.assertIn({"apiGroups": ["rhiza.mrchypark.dev"],
                       "resources": ["rhizaclusters", "rhizafences", "rhizarecoveries"],
                       "verbs": ["get", "list", "watch"]}, role["rules"])


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
    def test_intact_wal_waits_ready_and_preserves_config(self):
        chaos = stub()
        ident = {"node_id": "rhiza-voter-0", "pod_uid": "u0", "wal_identity": "w0", "config_id": "c0"}
        identities = iter([ident, dict(ident, config_id="c1")])
        chaos.recovery_uids = lambda: set()
        chaos.pod_identity = lambda name: next(identities)
        chaos.ready = lambda name: True
        chaos.create_chaos = lambda *a, **k: None
        chaos.chaos_injected = lambda *a, **k: True
        chaos.wait_chaos = lambda *a, **k: True
        chaos.wait = lambda label, pred, timeout=0: pred()
        chaos.assert_no_new_child = lambda before: True
        chaos.fence_for = lambda ident: []
        chaos.delete_chaos = lambda *a, **k: None
        chaos.readback = lambda *a, **k: None
        with self.assertRaisesRegex(AssertionError, "configuration identity drifted"):
            chaos.scenario_intact_wal_restart()

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
            "--admin-token-file", "admin-token", "--inspector-probe", "inspector-probe.yaml"]

    def test_default_arguments_are_positive(self):
        self.assertEqual(harness.build_parser().parse_args(self.ARGV).scenarios, "positive")

    def test_admin_credential_file_is_mandatory(self):
        args = self.ARGV.copy()
        index = args.index("--admin-token-file")
        del args[index:index + 2]
        with mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit):
                harness.build_parser().parse_args(args)

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

    def test_capture_probe_pods_keeps_each_multi_document_receipt(self):
        chaos = stub()
        chaos.args = harness.build_parser().parse_args(ScenarioSplitTest.ARGV)
        chaos.context = "kind-test"
        probe = """\
apiVersion: v1
kind: Pod
metadata:
  name: rhiza-inspector
---
apiVersion: v1
kind: Pod
metadata:
  name: rhiza-inspector-control
"""
        documents = list(yaml.safe_load_all(probe))
        for document, uid in zip(documents, ("inspector-uid", "control-uid")):
            document["metadata"]["uid"] = uid
        # kubectl concatenates its documents with no separator, so the zero
        # separator shape is the one that actually failed.
        for separator in ("\n", ""):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as output:
                chaos.out = Path(output)
                chaos.command = mock.Mock(return_value=separator.join(json.dumps(document) for document in documents))
                chaos.wait = mock.Mock(return_value=True)

                receipts = chaos.capture_probe_pods()

                self.assertEqual(receipts, {document["metadata"]["name"]: document for document in documents})
                self.assertEqual(chaos.command.call_args.args[0][-5:],
                                 ["create", "-f", chaos.args.inspector_probe, "-o", "json"])
                self.assertEqual(chaos.wait.call_count, 2)
                for document in documents:
                    name = document["metadata"]["name"]
                    self.assertEqual(json.loads((chaos.out / (name + "-created.json")).read_text()), document)

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


class JsonDocumentParsingTest(unittest.TestCase):
    def test_parse_json_documents_splits_kubectl_multi_document_output(self):
        documents = [{"metadata": {"name": "first"}}, {"metadata": {"name": "second"}}]
        for separator in ("\n", ""):
            with self.subTest(separator=separator):
                self.assertEqual(parse_json_documents(separator.join(json.dumps(document) for document in documents)),
                                 documents)


class LauncherDigestTest(unittest.TestCase):
    """The launcher must reject anything that is not name@sha256:<64 hex>."""

    SDK_VERSION = RELEASE_VERSION
    BASE = dict(RHIZA_RUN_ID="abc12345",
                RHIZA_NAMESPACE="rhiza-v%s-abc12345" % RELEASE_COMPACT,
                RHIZA_KUBE_CONTEXT="ctx",
                RHIZA_GCS_BUCKET="b",
                RHIZA_GCS_PREFIX="postrelease/v%s/abc12345/" % SDK_VERSION,
                RHIZA_CLUSTER_ID="cid",
                RHIZA_GCP_PROJECT_ID="project",
                RHIZA_DATA_GSA_ID="rhiza-v%s-gcs-abc12345" % RELEASE_COMPACT,
                RHIZA_OPERATOR_IMAGE="ghcr.io/mrchypark/rhiza-operator@sha256:" + "f" * 64)

    def run_launcher(self, app_image):
        env = dict(os.environ, **self.BASE)
        env["RHIZA_APP_IMAGE"] = app_image
        return subprocess.run(["sh", str(HERE / "run-votercount-gcs-chaos.sh")],
                              env=env, capture_output=True, text=True, timeout=60)

    def test_malformed_digests_are_rejected(self):
        for bad in ("repo@sha256:", "repo@sha256:not-a-digest", "repo@sha256:zz",
                    "repo:" + RELEASE_DOTTED, "repo@sha256:" + "a" * 63):
            with self.subTest(bad=bad):
                result = self.run_launcher(bad)
                self.assertEqual(result.returncode, 2, result.stderr[-200:])

    def test_non_hex_digest_is_rejected(self):
        result = self.run_launcher("repo@sha256:" + "g" * 64)
        self.assertEqual(result.returncode, 2, result.stderr[-200:])

    def test_launcher_has_strict_shell_syntax(self):
        result = subprocess.run(["sh", "-n", str(HERE / "run-votercount-gcs-chaos.sh")],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_runtime_phase_requires_the_privileged_install_credential(self):
        output = tempfile.mkdtemp()
        env = dict(os.environ, **self.BASE, RHIZA_OUT=output,
                   RHIZA_APP_IMAGE="repo@sha256:" + "a" * 64)
        result = subprocess.run(["sh", str(HERE / "run-votercount-gcs-chaos.sh"), "run"],
                                env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 2)
        self.assertIn("missing privileged-install credential", result.stderr)


if __name__ == "__main__":
    unittest.main()
