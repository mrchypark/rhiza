"""Check that the S3 fixture can retry bucket creation safely."""

import os
import argparse
import json
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from automatic_recovery import AutomaticChaos, proof_authorized_generation_child, require_pending_generation_fence
from anchor_recovery import AnchorChaos, ANCHOR_CM
from operator_recovery import Chaos, safe_startup_findings
from operator_fixture import resources


class BucketJobTest(unittest.TestCase):
    @staticmethod
    def _diagnostic_fixture():
        learner_name = "rhiza-learner-2"
        return {
            "root": {
                "apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaCluster",
                "metadata": {"name": "rhiza", "uid": "root-uid-test", "generation": 7},
                "spec": {"automatic": True, "statefulSet": "rhiza", "logicalID": "logical-test"},
                "status": {"activeRecovery": "recovery-test", "activeClusterID": "source-test",
                           "observedGeneration": 7, "phase": "Recovering",
                           "message": "PRIVATE_ROOT_CANARY"},
            },
            "statefulset": {
                "apiVersion": "apps/v1", "kind": "StatefulSet",
                "metadata": {"name": "rhiza", "uid": "sts-uid-test", "generation": 3},
                "spec": {"replicas": 2, "template": {"metadata": {"annotations": {
                    "rhiza.mrchypark.dev/cooperative-fencing": "1",
                    "rhiza.mrchypark.dev/fencing-root-uid": "root-uid-test",
                }}, "spec": {"containers": [{"name": "rhiza", "image": "rhiza:test"}]}}},
                "status": {"observedGeneration": 3},
            },
            "recovery": {
                "apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
                "metadata": {"name": "recovery-test", "uid": "recovery-uid-test",
                    "annotations": {"rhiza.mrchypark.dev/automatic-owner": "root-uid-test",
                                    "rhiza.mrchypark.dev/automatic-logical": "logical-test"}},
                "spec": {"statefulSet": "rhiza", "sourceClusterID": "source-test",
                         "membership": {"operationID": "recovery-test", "replacementPod": learner_name,
                                        "replacementSecret": learner_name + "-credentials"}},
            },
            "secret": {
                "metadata": {"name": learner_name + "-credentials", "uid": "credential-uid-private",
                    "annotations": {"rhiza.mrchypark.dev/recovery-owner": "recovery-test"}},
                "immutable": True,
            },
            "pod": {
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": learner_name, "uid": "learner-pod-uid-test",
                    "labels": {"rhiza.mrchypark.dev/automatic-learner": learner_name},
                    "ownerReferences": [{"apiVersion": "apps/v1", "kind": "StatefulSet",
                                         "name": "rhiza", "uid": "sts-uid-test", "controller": False}],
                    "annotations": {"rhiza.mrchypark.dev/fencing-root-uid": "root-uid-test",
                                    "rhiza.mrchypark.dev/recovery-owner": "recovery-test",
                                    "rhiza.mrchypark.dev/cooperative-fencing": "1",
                                    "rhiza.mrchypark.dev/fencing-credential-uid": "credential-uid-private"}},
                "spec": {"containers": [{"name": "rhiza", "image": "rhiza:test"}]},
                "status": {"phase": "Running", "containerStatuses": [{"name": "rhiza",
                    "containerID": "containerd://current-private", "restartCount": 4,
                    "lastState": {"terminated": {"containerID": "containerd://previous-private"}}}]},
            },
        }

    @staticmethod
    def _capture_diagnostics(chaos, fixture, *, supervised, after_pod=None, log_error=False, log_payloads=None):
        chaos.supervised_bootstrap = supervised

        def fake_run(argv, **kwargs):
            if "logs" in argv:
                if log_error and "--previous" not in argv:
                    return subprocess.CompletedProcess(argv, 1, b"", b"PRIVATE_ERROR_CANARY")
                key = "previous" if "--previous" in argv else "current"
                payload = (log_payloads or {}).get(key)
                if payload is None:
                    payload = b"PRIVATE_PREVIOUS_CANARY" if key == "previous" else b"PRIVATE_CURRENT_CANARY"
                return subprocess.CompletedProcess(argv, 0, payload, b"")
            if argv[argv.index("get") + 1:argv.index("get") + 2] == ["secret"]:
                projection = argv[argv.index("-o") + 1]
                if not projection.startswith("jsonpath=") or ".data" in projection:
                    raise AssertionError("credential diagnostics must request metadata fields only")
                secret = fixture["secret"]
                metadata = secret["metadata"]
                return subprocess.CompletedProcess(argv, 0, "\n".join((
                    metadata["name"], metadata["uid"],
                    metadata["annotations"]["rhiza.mrchypark.dev/recovery-owner"],
                    str(secret["immutable"]).lower())), "")
            if argv[argv.index("get") + 1:argv.index("get") + 3] == ["rhizacluster", "rhiza"]:
                value = fixture["root"]
            elif argv[argv.index("get") + 1:argv.index("get") + 3] == ["statefulset", "rhiza"]:
                value = fixture["statefulset"]
            elif argv[argv.index("get") + 1:argv.index("get") + 2] == ["rhizafences"]:
                value = fixture.get("fences", {"items": []})
            elif argv[argv.index("get") + 1:argv.index("get") + 2] == ["rhizarecoveries"]:
                value = fixture.get("recoveries", {"items": [fixture["recovery"]]})
            elif argv[argv.index("get") + 1:argv.index("get") + 2] == ["pods"]:
                value = {"items": [fixture["pod"]]}
            elif argv[argv.index("get") + 1:argv.index("get") + 2] == ["rhizarecovery"]:
                if fixture.get("recovery") is None:
                    return subprocess.CompletedProcess(argv, 0, "", "")
                value = fixture["recovery"]
            elif argv[argv.index("get") + 1:argv.index("get") + 2] == ["pod"]:
                value = after_pod if after_pod is not None and fake_run.pod_reads == 1 else fixture["pod"]
                fake_run.pod_reads += 1
            else:
                raise AssertionError(f"unexpected diagnostic command: {argv}")
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")

        fake_run.pod_reads = 0
        with mock.patch("operator_recovery.subprocess.run", side_effect=fake_run):
            return chaos.capture_failure_diagnostics()

    def test_voter_statefulset_template_opts_into_cooperative_fencing(self):
        stateful_set = next(
            item for item in resources("test", "db", "operator")
            if item["kind"] == "StatefulSet" and item["metadata"]["name"] == "rhiza"
        )
        annotations = stateful_set["spec"]["template"]["metadata"].get("annotations", {})
        self.assertEqual("1", annotations.get("rhiza.mrchypark.dev/cooperative-fencing"))

    def test_bucket_creation_is_idempotent_and_rejects_bad_credentials(self):
        job = next(
            item for item in resources("test", "db", "operator")
            if item["kind"] == "Job" and item["metadata"]["name"] == "rhiza-create-bucket"
        )
        command = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
        with tempfile.TemporaryDirectory() as directory:
            fake_aws = Path(directory, "aws")
            fake_aws.write_text(
                '#!/bin/sh\n'
                'test "$AWS_SECRET_ACCESS_KEY" = valid || exit 1\n'
                'case "$*" in\n'
                '  *head-bucket*) test -f "$BUCKET_MARKER" ;;\n'
                '  *"s3 mb"*) touch "$BUCKET_MARKER" ;;\n'
                'esac\n'
            )
            fake_aws.chmod(0o755)
            environment = dict(os.environ, PATH=f"{directory}:{os.environ['PATH']}",
                               BUCKET_MARKER=str(Path(directory, "created")),
                               AWS_SECRET_ACCESS_KEY="valid")
            for _ in range(2):
                self.assertEqual(subprocess.run(["/bin/sh", "-c", command], env=environment,
                                                capture_output=True, timeout=5).returncode, 0)
            environment["AWS_SECRET_ACCESS_KEY"] = "invalid"
            self.assertNotEqual(subprocess.run(["/bin/sh", "-c", command.replace("-lt 120", "-lt 1")],
                                              env=environment, capture_output=True, timeout=5).returncode, 0)

    def test_supervised_automatic_template_is_zero_replica_and_keeps_gate_under_pid1(self):
        fixture = resources("test", "db", "operator", supervised=True)
        stateful_set = next(o for o in fixture if o["kind"] == "StatefulSet")
        deployment = next(o for o in fixture if o.get("metadata", {}).get("name") == "rhiza-operator")
        self.assertEqual(0, stateful_set["spec"]["replicas"])
        self.assertEqual(0, deployment["spec"]["replicas"])

        with tempfile.TemporaryDirectory() as directory:
            chaos = AutomaticChaos(argparse.Namespace(
                output=directory, cluster="fixture", embedded_host="go"))
            applied = {}
            chaos.k = lambda *args, **kwargs: applied.update(kwargs)
            chaos.apply([stateful_set])
        template = stateful_set["spec"]["template"]
        container = template["spec"]["containers"][0]
        self.assertEqual(["/usr/local/bin/rhiza-fencer"], container["command"])
        self.assertEqual("supervise", container["args"][0])
        self.assertEqual("--", container["args"][1])
        self.assertEqual("/bin/sh", container["args"][2])
        self.assertIn("/data/.chaos-start", container["args"][4])
        self.assertIn("exec /usr/local/bin/rhiza-entrypoint", container["args"][4])
        self.assertEqual("rhiza-supervised", template["spec"]["serviceAccountName"])
        self.assertFalse(template["spec"]["automountServiceAccountToken"])
        self.assertEqual("0.0.0.0:8080", next(
            o["data"]["RHIZA_BIND_ADDR"] for o in fixture if o["kind"] == "ConfigMap"))
        self.assertIn({"name": "recovery", "containerPort": 9091}, container["ports"])
        self.assertTrue(applied)

    def test_generation_recovery_child_waits_for_exact_fence_proof(self):
        operation = "auto-pending"
        stateful_set = {"metadata": {"name": "rhiza", "uid": "sts-uid"}}
        root = {"metadata": {"generation": 4, "uid": "root-uid"},
                "spec": {"logicalID": "logical-test", "statefulSet": "rhiza"}, "status": {
            "phase": "Fencing", "observedGeneration": 4, "activeClusterID": "source",
            "activeRecovery": operation}}
        fence = {"metadata": {"name": "fence-pending"}, "spec": {
            "operationID": operation, "scope": "Generation", "sourceClusterID": "source",
            "bindingUID": "root-uid", "logicalID": "logical-test",
            "statefulSet": "rhiza", "statefulSetUID": "sts-uid"}, "status": {}}

        # With the fencer unavailable the durable intent/fence is visible, but
        # no child may be dispatched before a verified completion proof exists.
        require_pending_generation_fence(root, fence, {"items": []}, stateful_set)
        with self.assertRaisesRegex(AssertionError, "child exists before"):
            require_pending_generation_fence(
                root, fence, {"items": [{"metadata": {"name": operation}}]}, stateful_set)
        with self.assertRaisesRegex(AssertionError, "active recovery intent"):
            require_pending_generation_fence(
                {**root, "status": {**root["status"], "activeRecovery": "other"}},
                fence, {"items": []}, stateful_set)
        for bad_fence, bad_sts in (
                ({**fence, "spec": {**fence["spec"], "bindingUID": "other-root"}}, stateful_set),
                ({**fence, "spec": {**fence["spec"], "bindingUID": ""}}, stateful_set),
                ({**fence, "spec": {**fence["spec"], "logicalID": "other-logical"}}, stateful_set),
                ({**fence, "spec": {**fence["spec"], "logicalID": ""}}, stateful_set),
                ({**fence, "spec": {**fence["spec"], "statefulSet": "other"}}, stateful_set),
                ({**fence, "spec": {**fence["spec"], "statefulSet": ""}}, stateful_set),
                (fence, {"metadata": {"name": "rhiza", "uid": "other-sts"}}),
                ({**fence, "spec": {**fence["spec"], "statefulSetUID": ""}}, stateful_set)):
            with self.assertRaisesRegex(AssertionError, "not bound"):
                require_pending_generation_fence(root, bad_fence, {"items": []}, bad_sts)

        proof = {"operationID": operation, "sourceClusterID": "source", "bindingUID": "root-uid",
                 "requestHash": "request-digest", "proofID": "proof-digest",
                 "processesTerminated": True, "recreationBlocked": True, "storageQuiesced": True}
        fence["status"]["proof"] = proof
        child = {"metadata": {"name": operation, "annotations": {
            "rhiza.mrchypark.dev/automatic-owner": "root-uid",
            "rhiza.mrchypark.dev/automatic-logical": "logical-test"}, "uid": "child-uid"}, "spec": {
            "statefulSet": "rhiza", "recoveryID": operation, "sourceClusterID": "source", "fence": {
                "recoveryID": operation, "clusterID": "source", "statefulSetUID": "sts-uid",
                "confirmed": True, "evidence": "proof-digest"}},
            "status": {"phase": "Recovering"}}
        self.assertIs(child, proof_authorized_generation_child(root, fence, child, stateful_set))
        mismatched = {**child, "spec": {**child["spec"], "fence": {
            **child["spec"]["fence"], "evidence": "other-proof"}}}
        with self.assertRaisesRegex(AssertionError, "not bound to the verified"):
            proof_authorized_generation_child(root, fence, mismatched, stateful_set)
        for bad_root, bad_fence in (
                ({**root, "metadata": {**root["metadata"], "uid": "wrong-root"}}, fence),
                ({**root, "metadata": {**root["metadata"], "uid": ""}}, fence),
                (root, {**fence, "spec": {**fence["spec"], "logicalID": "wrong-logical"}}),
                (root, {**fence, "spec": {**fence["spec"], "statefulSet": "other"}}),
                (root, {**fence, "spec": {**fence["spec"], "statefulSet": ""}}),
                (root, {**fence, "spec": {**fence["spec"], "statefulSetUID": "wrong-sts"}})):
            with self.assertRaisesRegex(AssertionError, "not bound"):
                proof_authorized_generation_child(bad_root, bad_fence, child, stateful_set)
        wrong_owner = {**child, "metadata": {**child["metadata"], "annotations": {
            **child["metadata"]["annotations"], "rhiza.mrchypark.dev/automatic-owner": "other-root"}}}
        with self.assertRaisesRegex(AssertionError, "not bound to the verified"):
            proof_authorized_generation_child(root, fence, wrong_owner, stateful_set)

        completed_root = {**root, "status": {
            **root["status"], "phase": "Stabilizing", "activeRecovery": "",
            "activeClusterID": "target"}}
        completed_child = {**child, "status": {"phase": "Complete", "target": "target"}}
        self.assertIs(completed_child, proof_authorized_generation_child(
            completed_root, fence, completed_child, stateful_set))
        with self.assertRaisesRegex(AssertionError, "not bound to the verified"):
            proof_authorized_generation_child(completed_root, fence, child, stateful_set)
        with self.assertRaisesRegex(AssertionError, "not bound to the verified"):
            proof_authorized_generation_child(
                {**completed_root, "status": {**completed_root["status"], "activeClusterID": "source"}},
                fence, completed_child, stateful_set)

    def test_authority_env_patch_preserves_rendered_container_with_kubectl_local(self):
        kubectl = shutil.which("kubectl")
        self.assertTrue(kubectl, "CI must install the repository-pinned kubectl client")

        with tempfile.TemporaryDirectory() as directory:
            calls = []
            chaos = AutomaticChaos(argparse.Namespace(
                output=directory, cluster="fixture", embedded_host="go",
                operator_image="operator:test", db_image="db:test"))
            chaos._runtime_profile = lambda: {
                "containerd-version": "containerd-test",
                "containerd-runtime-type": "io.containerd.runc.v2",
                "kernel-release": "kernel-test",
                "cgroup-profile-id": "kind-ci-cgroup-v2-component-v1",
            }

            def fake_k(*args, data=None):
                calls.append((args, data))
                if args[:3] == ("get", "deployment", "rhiza-fencer"):
                    return json.dumps({"metadata": {"uid": "authority-uid-test"}})
                return "{}"

            chaos.k = fake_k
            with mock.patch("operator_recovery.subprocess.run",
                            return_value=subprocess.CompletedProcess([], 0, "", "")):
                chaos.install_supervised_authority()

            applied = next(data for args, data in calls if args[:2] == ("apply", "-f"))
            patch_args = next(args for args, _ in calls
                              if args[:3] == ("patch", "deployment", "rhiza-fencer"))
            patch_json = json.loads(patch_args[patch_args.index("-p") + 1])
            self.assertEqual([{"name": "authority", "envFrom": [
                {"configMapRef": {"name": "rhiza-config"}},
                {"secretRef": {"name": "rhiza-object-store"}},
            ]}], patch_json["spec"]["template"]["spec"]["containers"])

            deployment_yaml = next(
                document for document in applied.split("\n---\n")
                if "\nkind: Deployment\n" in f"\n{document}\n"
                and "\n  name: rhiza-fencer\n" in f"\n{document}\n"
            )

            def kubectl_json(argv, data):
                empty_kubeconfig = Path(directory, "empty-kubeconfig")
                empty_kubeconfig.write_text("")
                environment = dict(os.environ, HOME=directory,
                                   KUBECONFIG=str(empty_kubeconfig), KUBERNETES_MASTER="")
                result = subprocess.run(
                    [kubectl, "--cache-dir", str(Path(directory, "empty-cache")), *argv],
                    input=data, text=True, capture_output=True, timeout=15, env=environment)
                self.assertEqual(0, result.returncode, result.stderr)
                return json.loads(result.stdout)

            original = kubectl_json(
                ["patch", "--local", "-f", "-", "--type=strategic",
                 "-p", "{}", "-o", "json"],
                deployment_yaml)
            patched = kubectl_json(
                ["patch", "--local", "-f", "-", "--type=strategic", "-p",
                 json.dumps(patch_json), "-o", "json"], json.dumps(original))
            before = original["spec"]["template"]["spec"]["containers"][0]
            after = patched["spec"]["template"]["spec"]["containers"][0]
            for field in ("name", "image", "command", "args", "ports", "volumeMounts",
                          "securityContext", "resources"):
                self.assertEqual(before.get(field), after.get(field), field)
            self.assertEqual(patch_json["spec"]["template"]["spec"]["containers"][0]["envFrom"],
                             after["envFrom"])

            commands = [args for args, _ in calls]
            profile = next(i for i, args in enumerate(commands)
                           if args[:2] == ("patch", "configmap"))
            deployment = commands.index(patch_args)
            authority_ready = next(i for i, args in enumerate(commands)
                                   if args[:3] == ("rollout", "status", "deployment/rhiza-fencer"))
            agent_restart = next(i for i, args in enumerate(commands)
                                 if args[:3] == ("rollout", "restart", "daemonset/rhiza-runtime-agent"))
            agent_ready = next(i for i, args in enumerate(commands)
                               if args[:3] == ("rollout", "status", "daemonset/rhiza-runtime-agent"))
            self.assertLess(profile, deployment)
            self.assertLess(deployment, authority_ready)
            self.assertLess(authority_ready, agent_restart)
            self.assertLess(agent_restart, agent_ready)

    def test_private_learner_diagnostics_are_bound_and_public_summary_is_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            errors = self._capture_diagnostics(chaos, fixture, supervised=True)

            private = Path(directory, "private-startup-diagnostics")
            record = json.loads(Path(private, "record.json").read_text())
            summary_path = Path(directory, "startup-diagnostics-summary.json")
            summary = summary_path.read_text()
            summary_json = json.loads(summary)
            self.assertEqual([], errors)
            self.assertEqual("PRIVATE_ROOT_CANARY", record["root"]["message"])
            learner = next(item for item in record["captures"] if item["component"] == "learner")
            self.assertEqual("learner-pod-uid-test", learner["identity"]["uid"])
            self.assertEqual("sts-uid-test", learner["identity"]["ownerUID"])
            self.assertTrue(learner["logs"]["current"]["captured"])
            self.assertTrue(learner["logs"]["previous"]["captured"])
            self.assertIn("PRIVATE_CURRENT_CANARY", Path(private, "rhiza-learner-2-rhiza-current.log").read_text())
            self.assertIn("PRIVATE_PREVIOUS_CANARY", Path(private, "rhiza-learner-2-rhiza-previous.log").read_text())
            for canary in ("PRIVATE_ROOT_CANARY", "PRIVATE_CURRENT_CANARY", "PRIVATE_PREVIOUS_CANARY",
                           "credential-uid-private",
                           "learner-pod-uid-test", "containerd://current-private"):
                self.assertNotIn(canary, summary)
            self.assertEqual(0o700, private.stat().st_mode & 0o777)
            for path in private.iterdir():
                self.assertEqual(0o600, path.stat().st_mode & 0o777, path.name)
            self.assertEqual(0o600, summary_path.stat().st_mode & 0o777)
            self.assertEqual("complete", summary_json["status"])
            self.assertEqual(["learner"], [item["component"] for item in summary_json["captures"]])

    def test_unmanaged_root_and_missing_previous_incarnation_are_not_applicable(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            fixture["pod"]["metadata"]["name"] = "rhiza-2"
            fixture["pod"]["metadata"]["labels"] = {"app.kubernetes.io/name": "rhiza"}
            fixture["pod"]["metadata"]["ownerReferences"][0]["controller"] = True
            fixture["pod"]["metadata"].pop("annotations")
            fixture["pod"]["status"]["containerStatuses"][0]["restartCount"] = 0
            fixture["pod"]["status"]["containerStatuses"][0].pop("lastState")

            errors = self._capture_diagnostics(chaos, fixture, supervised=False)

            private = Path(directory, "private-startup-diagnostics")
            record = json.loads(Path(private, "record.json").read_text())
            summary = json.loads(Path(directory, "startup-diagnostics-summary.json").read_text())
            voter = next(item for item in record["captures"] if item["component"] == "rhiza")
            self.assertEqual([], errors)
            self.assertEqual({"status": "not_applicable"}, record["root"])
            self.assertEqual("not_applicable", summary["rootStatus"])
            self.assertEqual({"captured": False, "status": "not_applicable"},
                             voter["logs"]["previous"])
            self.assertEqual("complete", summary["status"])
            self.assertNotIn("root_status_read_failed", summary["captureErrors"])
            self.assertNotIn("container_incarnation_unavailable", summary["captureErrors"])

    def test_missing_incarnation_with_restart_history_is_a_failure(self):
        for missing in ("previous", "current"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
                fixture = self._diagnostic_fixture()
                status = fixture["pod"]["status"]["containerStatuses"][0]
                if missing == "previous":
                    status.pop("lastState")
                else:
                    status.pop("containerID")

                errors = self._capture_diagnostics(chaos, fixture, supervised=True)

                private = Path(directory, "private-startup-diagnostics")
                record = json.loads(Path(private, "record.json").read_text())
                summary = json.loads(Path(directory, "startup-diagnostics-summary.json").read_text())
                learner = next(item for item in record["captures"] if item["component"] == "learner")
                self.assertIn("container_incarnation_unavailable", errors)
                self.assertIn("container_incarnation_unavailable", summary["captureErrors"])
                self.assertEqual("failed", learner["logs"][missing]["status"])
                self.assertEqual("container_incarnation_unavailable",
                                 learner["logs"][missing]["errorCode"])

    def test_startup_findings_are_fixed_enums_only(self):
        self.assertEqual([], safe_startup_findings(
            "node open phase consensus: nested error says open WAL: PRIVATE_CANARY"))
        self.assertEqual([], safe_startup_findings(
            "2026/10/26 12:34:56 unrelated prefix: fencing: engine repair preflight: PRIVATE_CANARY"))
        self.assertEqual(
            ["engine_repair_preflight", "engine_repair_state_continuity_lost"],
            safe_startup_findings(
                "2026/10/26 12:34:56 fencing: engine repair preflight: "
                "voter state continuity cannot be established: PRIVATE_STATE_CANARY"))
        self.assertEqual(
            ["engine_repair_preflight"],
            safe_startup_findings(
                "2026/10/26 12:34:56 fencing: engine repair preflight: "
                "unrecognized PRIVATE_UNKNOWN_CANARY"))
        self.assertEqual([], safe_startup_findings(
            "2026/10/26 12:34:56 unrelated: registered voter WAL is absent PRIVATE_UNMARKED_CANARY"))
        self.assertEqual(
            ["engine_repair_preflight", "engine_repair_state_continuity_lost", "engine_repair_wal_absent"],
            safe_startup_findings(
                "2026/10/26 12:34:56 fencing: engine repair preflight: "
                "voter state continuity cannot be established\nregistered voter WAL is absent"))
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            payload = b"\n".join((
                b"2026/10/26 12:34:56 open rhiza: node open phase identity_storage: ensure voter identity: PRIVATE_CANARY_A",
                b"2026/10/26 12:34:56 open rhiza: node open phase identity_storage: open object store: PRIVATE_CANARY_B",
                b"2026/10/26 12:34:56 open rhiza: node open phase wal: open WAL: PRIVATE_CANARY_C",
                b"2026/10/26 12:34:56 fencing: engine repair preflight: PRIVATE_CANARY_D",
                b"2026/10/26 12:34:56 fencing: engine repair preflight: voter state continuity cannot be established: PRIVATE_PREFLIGHT_STATE_CANARY",
                b"registered voter WAL is absent",
                b"2026/10/26 12:34:56 fencing: engine repair preflight: registered voter WAL is absent",
                b"2026/10/26 12:34:56 fencing: engine repair preflight: unknown PRIVATE_PREFLIGHT_UNKNOWN_CANARY; voter state continuity cannot be established",
                b"2026/10/26 12:34:56 unrelated PRIVATE_UNMARKED_CANARY: registered voter WAL is absent",
                b"2026/10/26 12:34:56 fencing: child exited without a terminal receipt: PRIVATE_CANARY_E",
                b"2026/10/26 12:34:56 open rhiza: node open phase unknown_phase: PRIVATE_CANARY_F",
                b"2026/10/26 12:34:56 unrecognized private failure PRIVATE_CANARY_G",
                b"2026/10/26 12:34:56 open rhiza: node open phase consensus: nested open WAL: PRIVATE_CANARY_H",
                b"2026/10/26 12:34:56 open rhiza: node open phase wal: nested ensure voter identity: PRIVATE_CANARY_I",
                b"2026/10/26 12:34:56 node open phase network: peer transport setup failed: PRIVATE_CANARY_J",
                b"2026/10/26 12:34:56 read failed: phase=read-index-apply node=rhiza-0 read_index=7 "
                b"source= error_class=quorum-unavailable private=PRIVATE_READ_CANARY",
            ))
            errors = self._capture_diagnostics(
                chaos, self._diagnostic_fixture(), supervised=True, log_payloads={"current": payload})
            self.assertEqual([], errors)
            summary = Path(directory, "startup-diagnostics-summary.json").read_text()
            capture = json.loads(summary)["captures"][0]
            self.assertEqual([
                "consensus", "engine_repair_preflight", "engine_repair_state_continuity_lost",
                "engine_repair_wal_absent", "identity_enrollment", "identity_storage", "network",
                "storage_initialization", "supervisor_child_exit", "wal", "wal_open",
            ], capture["startupFindings"])
            self.assertEqual([{"phase": "read-index-apply", "errorClass": "quorum-unavailable",
                               "attribution": "unattributed"}], capture["readFailureFindings"])
            for canary in ("PRIVATE_CANARY_A", "PRIVATE_CANARY_B", "PRIVATE_CANARY_C",
                           "PRIVATE_CANARY_D", "PRIVATE_CANARY_E", "PRIVATE_CANARY_F", "PRIVATE_CANARY_J",
                           "PRIVATE_CANARY_G", "PRIVATE_CANARY_H", "PRIVATE_CANARY_I"):
                self.assertNotIn(canary, summary)
            self.assertNotIn("PRIVATE_READ_CANARY", summary)
            for canary in ("PRIVATE_PREFLIGHT_STATE_CANARY", "PRIVATE_PREFLIGHT_UNKNOWN_CANARY",
                           "PRIVATE_UNMARKED_CANARY"):
                self.assertNotIn(canary, summary)

        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            errors = self._capture_diagnostics(
                chaos, self._diagnostic_fixture(), supervised=True, log_error=True)
            private = Path(directory, "private-startup-diagnostics")
            record = json.loads(Path(private, "record.json").read_text())
            summary = Path(directory, "startup-diagnostics-summary.json").read_text()
            self.assertIn("pod_log_capture_failed", errors)
            self.assertIn("PRIVATE_ERROR_CANARY", json.dumps(record))
            self.assertIn("pod_log_capture_failed", summary)
            self.assertNotIn("PRIVATE_ERROR_CANARY", summary)

    def test_embedded_go_startup_findings_use_inherited_automatic_and_anchor_collectors(self):
        payload = b"2026/10/26 12:34:56 node open phase network: peer transport setup failed: PRIVATE_EMBEDDED_CANARY"
        for harness in (AutomaticChaos, AnchorChaos):
            with self.subTest(harness=harness.__name__), tempfile.TemporaryDirectory() as directory:
                chaos = harness(argparse.Namespace(output=directory, cluster="fixture"))
                errors = self._capture_diagnostics(
                    chaos, self._diagnostic_fixture(), supervised=True, log_payloads={"current": payload})
                self.assertEqual([], errors)
                summary = Path(directory, "startup-diagnostics-summary.json").read_text()
                capture = json.loads(summary)["captures"][0]
                self.assertEqual(["network"], capture["startupFindings"])
                self.assertNotIn("PRIVATE_EMBEDDED_CANARY", summary)

    def test_absent_recovery_child_is_pending_only_for_exact_unproved_generation_fence(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            operation = "recovery-pending"
            fixture["recovery"] = None
            fixture["root"]["status"].update({
                "phase": "Fencing", "activeRecovery": operation, "observedGeneration": 7})
            fixture["fences"] = {"items": [{"spec": {
                "operationID": operation, "scope": "Generation", "sourceClusterID": "source-test",
                "bindingUID": "root-uid-test", "logicalID": "logical-test",
                "statefulSet": "rhiza", "statefulSetUID": "sts-uid-test"},
                "status": {}}]}

            errors = self._capture_diagnostics(chaos, fixture, supervised=True)
            record = json.loads(Path(directory, "private-startup-diagnostics", "record.json").read_text())
            summary = json.loads(Path(directory, "startup-diagnostics-summary.json").read_text())
            self.assertNotIn("active_recovery_binding_unavailable", errors)
            self.assertEqual({"status": "awaiting_fence_proof"}, record["activeRecoveryChild"])
            self.assertEqual("awaiting_fence_proof", summary["activeRecoveryChildStatus"])

            for field, value in (("bindingUID", "other-root"), ("bindingUID", ""),
                                 ("logicalID", "other-logical"), ("logicalID", ""),
                                 ("statefulSet", "other-sts"), ("statefulSetUID", "")):
                bad_fixture = json.loads(json.dumps(fixture))
                bad_fixture["fences"]["items"][0]["spec"][field] = value
                bad_dir = tempfile.TemporaryDirectory()
                try:
                    bad_chaos = Chaos(argparse.Namespace(output=bad_dir.name, cluster="fixture"))
                    bad_errors = self._capture_diagnostics(bad_chaos, bad_fixture, supervised=True)
                    self.assertIn("active_recovery_binding_unavailable", bad_errors)
                finally:
                    bad_dir.cleanup()

    def test_diagnostic_binding_and_incarnation_drift_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            fixture["statefulset"]["spec"]["template"]["metadata"]["annotations"][
                "rhiza.mrchypark.dev/fencing-root-uid"] = "wrong-root"
            errors = self._capture_diagnostics(chaos, fixture, supervised=True)
            self.assertIn("active_recovery_binding_mismatch", errors)
            record = json.loads(Path(directory, "private-startup-diagnostics", "record.json").read_text())
            self.assertFalse(any(item.get("component") == "learner" for item in record["captures"]))

        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            fixture["pod"]["metadata"]["annotations"][
                "rhiza.mrchypark.dev/fencing-credential-uid"] = "wrong-credential"
            errors = self._capture_diagnostics(chaos, fixture, supervised=True)
            self.assertIn("learner_pod_binding_mismatch", errors)
            record = json.loads(Path(directory, "private-startup-diagnostics", "record.json").read_text())
            self.assertFalse(any(item.get("component") == "learner" for item in record["captures"]))

        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            fixture = self._diagnostic_fixture()
            fixture["secret"]["metadata"]["uid"] = "different-nonempty-credential-uid"
            errors = self._capture_diagnostics(chaos, fixture, supervised=True)
            self.assertIn("learner_pod_binding_mismatch", errors)
            record = json.loads(Path(directory, "private-startup-diagnostics", "record.json").read_text())
            self.assertFalse(any(item.get("component") == "learner" for item in record["captures"]))

        for drift in ("pod_uid", "container_id", "restart_count", "previous_container_id"):
            with self.subTest(drift=drift), tempfile.TemporaryDirectory() as directory:
                chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
                fixture = self._diagnostic_fixture()
                changed = json.loads(json.dumps(fixture["pod"]))
                if drift == "pod_uid":
                    changed["metadata"]["uid"] = "replacement-pod-uid"
                elif drift == "container_id":
                    changed["status"]["containerStatuses"][0]["containerID"] = "containerd://replacement"
                elif drift == "restart_count":
                    changed["status"]["containerStatuses"][0]["restartCount"] += 1
                else:
                    changed["status"]["containerStatuses"][0]["lastState"]["terminated"][
                        "containerID"] = "containerd://replacement-previous"
                errors = self._capture_diagnostics(
                    chaos, fixture, supervised=True, after_pod=changed)
                self.assertIn("pod_or_container_identity_changed", errors)
                private = Path(directory, "private-startup-diagnostics")
                record = json.loads(Path(private, "record.json").read_text())
                learner = next(item for item in record["captures"] if item["component"] == "learner")
                self.assertFalse(learner["logs"]["current"]["captured"])
                self.assertFalse(learner["logs"]["previous"]["captured"])
                self.assertFalse(Path(private, "rhiza-learner-2-rhiza-current.log").exists())

    def test_all_chaos_artifacts_exclude_private_diagnostics(self):
        workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
        source = workflow.read_text()
        for artifact in ("operator-chaos-results", "automatic-chaos-results", "anchor-chaos-results"):
            with self.subTest(artifact=artifact):
                self.assertIn(f"{artifact}\n            !{artifact}/kind/**\n            !{artifact}/private-startup-diagnostics/**", source)

    def test_cleanup_does_not_write_raw_pod_logs_to_public_artifact_root(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = Chaos(argparse.Namespace(output=directory, cluster="fixture"))
            pod = {"metadata": {"name": "rhiza-learner-test"}}
            pod_list = json.dumps({"items": [pod]})

            def fake_k(*args, **kwargs):
                if args[:2] == ("get", "pods"):
                    return pod_list
                if args[:1] == ("logs",):
                    return "PRIVATE_CLEANUP_LOG_CANARY"
                return "{}"

            chaos.k = fake_k
            chaos.cleanup()
            self.assertFalse(list(Path(directory).glob("*.log")),
                             "raw cleanup logs would be included by the root artifact path")
            private_log = Path(directory, "private-startup-diagnostics", "rhiza-learner-test.log")
            self.assertEqual("PRIVATE_CLEANUP_LOG_CANARY", private_log.read_text())
            self.assertEqual(0o600, private_log.stat().st_mode & 0o777)

    def test_automatic_learner_startup_log_is_private(self):
        with tempfile.TemporaryDirectory() as directory:
            chaos = AutomaticChaos(argparse.Namespace(output=directory, cluster="fixture"))
            process = object()
            chaos.k = lambda *args, **kwargs: ""
            with mock.patch("automatic_recovery.subprocess.Popen", return_value=process):
                chaos.start_learner("rhiza-learner-test")
            private_log = Path(directory, "private-startup-diagnostics", "rhiza-learner-test-startup.log")
            self.assertEqual(0o600, private_log.stat().st_mode & 0o777)
            self.assertFalse(list(Path(directory).glob("*.log")))
            self.assertEqual([process], chaos.learner_logs)

    @staticmethod
    def _outage_deployment(generation, observed, replicas):
        return {"metadata": {"uid": "authority-deployment", "generation": generation},
                "spec": {"replicas": replicas, "selector": {
                    "matchLabels": {"app": "rhiza-fencer"}}},
                "status": {"observedGeneration": observed}}

    def _run_executor_outage(self, observations, pod_lists, named_pods, expected_error=None):
        with tempfile.TemporaryDirectory() as directory:
            chaos = AutomaticChaos(argparse.Namespace(output=directory, cluster="fixture"))
            calls = []
            deployments = [self._outage_deployment(5, 5, 1), *observations]

            def fake_k(*args, **kwargs):
                calls.append(args)
                if args[:3] == ("get", "pods", "-l"):
                    result = pod_lists.pop(0)
                    if isinstance(result, Exception):
                        raise result
                    return json.dumps(result)
                if args[:2] == ("get", "pod"):
                    self.assertEqual(("get", "pod", "rhiza-fencer-old",
                                      "--ignore-not-found", "-o", "json"), args)
                    result = named_pods.pop(0)
                    return json.dumps(result) if result else ""
                if args[:2] == ("scale", "deployment/rhiza-fencer"):
                    return "scaled"
                raise AssertionError(f"unexpected authority command: {args}")

            chaos.k = fake_k
            chaos.get = lambda kind, name: deployments.pop(0)
            tick = [0]

            def monotonic():
                tick[0] += 1
                return float(tick[0])

            with mock.patch("operator_recovery.time.monotonic", side_effect=monotonic), \
                    mock.patch("operator_recovery.time.sleep"):
                if expected_error:
                    with self.assertRaisesRegex(AssertionError, expected_error):
                        chaos.executor_outage(True)
                    self.assertEqual([], chaos.events)
                else:
                    chaos.executor_outage(True)
                    self.assertEqual([], deployments, "outage returned before expected observations")
                    self.assertEqual([], pod_lists, "outage skipped an expected Pod-list observation")
                    self.assertEqual([], named_pods, "outage skipped a captured-name UID check")
                    self.assertIn(("scale", "deployment/rhiza-fencer", "--replicas=0"), calls)
                    self.assertEqual("automatic_executor_outage_observed", chaos.events[-1]["event"])
                    self.assertEqual(["old-authority-pod"], chaos.events[-1]["pod_uids"])

    def test_executor_outage_waits_for_captured_pod_after_selector_drift(self):
        ready = self._outage_deployment(6, 6, 0)
        self._run_executor_outage(
            [ready, ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                {"items": []},
                {"items": []},
            ],
            [
                {"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod",
                              "labels": {"app": "changed"}}},
                None,
            ])

    def test_executor_outage_waits_for_delayed_scale_down(self):
        delayed = self._outage_deployment(6, 6, 1)
        ready = self._outage_deployment(6, 6, 0)
        self._run_executor_outage(
            [delayed, ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                {"items": []},
            ], [None])

    def test_executor_outage_waits_for_controller_generation(self):
        pending = self._outage_deployment(7, 6, 0)
        ready = self._outage_deployment(7, 7, 0)
        self._run_executor_outage(
            [pending, ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                {"items": []},
            ], [None])

    def test_executor_outage_waits_for_replacement_authority_pod(self):
        ready = self._outage_deployment(6, 6, 0)
        self._run_executor_outage(
            [ready, ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                {"items": [{"metadata": {"name": "rhiza-fencer-new", "uid": "new-authority-pod"}}]},
                {"items": []},
            ], [None])

    def test_executor_outage_retries_pod_list_api_error(self):
        ready = self._outage_deployment(6, 6, 0)
        self._run_executor_outage(
            [ready, ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                RuntimeError("temporary Pod-list API failure"),
                {"items": []},
            ], [None])

    def test_executor_outage_rejects_captured_pod_uid_replacement(self):
        ready = self._outage_deployment(6, 6, 0)
        self._run_executor_outage(
            [ready],
            [
                {"items": [{"metadata": {"name": "rhiza-fencer-old", "uid": "old-authority-pod"}}]},
                {"items": []},
            ],
            [{"metadata": {"name": "rhiza-fencer-old", "uid": "replacement-authority-pod"}}],
            expected_error="captured authority Pod identity changed")

    def test_anchor_and_non_anchor_profiles_keep_separate_narrow_service_accounts(self):
        args = argparse.Namespace(output=tempfile.gettempdir(), cluster="fixture", embedded_host="go",
                                  operator_image="operator:test", db_image="db:test",
                                  anchor_image="anchor:test")
        anchor = AnchorChaos(args)
        anchored = resources(anchor.ns, "db:test", "operator:test", supervised=True)
        applied = []
        anchor.k = lambda *argv, data=None: applied.append((argv, data))
        anchor.apply(anchored)
        self.assertEqual(1, len(applied))
        config = next(item for item in anchored
                      if item["kind"] == "ConfigMap" and item["metadata"]["name"] == "rhiza-config")
        self.assertEqual(f"https://anchor-service.{anchor.ns}.svc:9191",
                         config["data"]["RHIZA_RECOVERY_ANCHOR_URL"])
        self.assertEqual("/anchor/token", config["data"]["RHIZA_RECOVERY_ANCHOR_TOKEN_FILE"])
        self.assertEqual("/anchor/ca.crt", config["data"]["RHIZA_RECOVERY_ANCHOR_CA_FILE"])

        roles = AnchorChaos(args)._anchor_rbac()
        app_role = next(item for item in roles
                        if item["kind"] == "Role" and item["metadata"]["name"] == "anchor-app-guard")
        app_binding = next(item for item in roles
                           if item["kind"] == "RoleBinding"
                           and item["metadata"]["name"] == "anchor-app-guard")
        self.assertEqual(anchor.ns, app_role["metadata"]["namespace"])
        self.assertEqual([{"apiGroups": [""], "resources": ["configmaps"],
                           "resourceNames": [ANCHOR_CM], "verbs": ["get", "update"]}],
                         app_role["rules"])
        self.assertEqual([{"kind": "ServiceAccount", "name": "rhiza-supervised",
                           "namespace": anchor.ns}], app_binding["subjects"])
        self.assertEqual({"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                          "name": "anchor-app-guard"}, app_binding["roleRef"])

        stateful_set = next(item for item in anchored if item["kind"] == "StatefulSet")
        podspec = stateful_set["spec"]["template"]["spec"]
        container = podspec["containers"][0]
        self.assertEqual("rhiza-supervised", podspec["serviceAccountName"])
        self.assertFalse(podspec["automountServiceAccountToken"])
        self.assertEqual(65532, podspec["securityContext"]["fsGroup"])
        workload_env = {entry["name"]: entry.get("value")
                        for entry in container["env"]}
        self.assertEqual("chaos-anchor", workload_env["RHIZA_RECOVERY_ANCHOR_ID"])
        self.assertEqual(anchor.ns, workload_env["RHIZA_NAMESPACE"])
        volumes = {volume["name"]: volume for volume in podspec["volumes"]}
        anchor_projection = volumes["anchor-api"]["projected"]
        token_source, ca_source = anchor_projection["sources"]
        self.assertEqual({"path": "token"}, token_source["serviceAccountToken"])
        self.assertNotIn("audience", token_source["serviceAccountToken"])
        self.assertEqual({"name": "kube-root-ca.crt", "optional": False,
                          "items": [{"key": "ca.crt", "path": "ca.crt"}]},
                         ca_source["configMap"])
        self.assertEqual(0o440, anchor_projection["defaultMode"])
        anchor_mount = next(mount for mount in container["volumeMounts"]
                            if mount["name"] == "anchor-api")
        self.assertEqual("/var/run/secrets/kubernetes.io/serviceaccount", anchor_mount["mountPath"])
        self.assertTrue(anchor_mount["readOnly"])
        self.assertNotIn("subPath", anchor_mount)
        fencer_token = volumes["fence-token"]["projected"]["sources"][0]["serviceAccountToken"]
        self.assertEqual({"path": "token", "audience": "rhiza-fencer",
                          "expirationSeconds": 600}, fencer_token)
        fencer_mount = next(mount for mount in container["volumeMounts"]
                            if mount["name"] == "fence-token")
        self.assertEqual("/var/run/secrets/rhiza-fencer", fencer_mount["mountPath"])
        self.assertTrue(fencer_mount["readOnly"])

        plain = resources(anchor.ns, "db:test", "operator:test", supervised=True)
        automatic = AutomaticChaos(args)
        automatic.k = lambda *argv, data=None: None
        automatic.apply(plain)
        plain_config = next(item for item in plain
                            if item["kind"] == "ConfigMap"
                            and item["metadata"]["name"] == "rhiza-config")
        self.assertFalse(any(key.startswith("RHIZA_RECOVERY_ANCHOR_")
                             for key in plain_config["data"]))
        plain_podspec = next(item for item in plain if item["kind"] == "StatefulSet")[
            "spec"]["template"]["spec"]
        self.assertNotIn("anchor-api", {volume["name"] for volume in plain_podspec["volumes"]})
        self.assertNotIn("anchor-app-guard", {item.get("metadata", {}).get("name")
                                               for item in plain})
        plain_container = plain_podspec["containers"][0]
        self.assertFalse(any(env["name"].startswith("RHIZA_RECOVERY_ANCHOR_")
                             for env in plain_container.get("env", [])))


if __name__ == "__main__":
    unittest.main()
