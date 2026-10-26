#!/usr/bin/env python3
"""Unattended recovery: faults only, no manual recovery CR or fence approval."""
import argparse
import base64
import json
import subprocess
from pathlib import Path
import time
import urllib.error

from operator_recovery import Chaos


def require_pending_generation_fence(root, fence, recoveries, stateful_set):
    root_meta = root.get("metadata", {})
    root_spec = root.get("spec", {})
    root_status = root.get("status", {})
    fence_spec = fence.get("spec", {})
    sts_meta = stateful_set.get("metadata", {})
    if (root_status.get("phase") != "Fencing" or
            root_status.get("observedGeneration") != root_meta.get("generation") or
            not root_status.get("activeClusterID") or
            root_status.get("activeRecovery") != fence_spec.get("operationID") or
            fence_spec.get("scope") != "Generation" or
            fence_spec.get("sourceClusterID") != root_status.get("activeClusterID") or
            not root_meta.get("uid") or fence_spec.get("bindingUID") != root_meta.get("uid") or
            not root_spec.get("logicalID") or fence_spec.get("logicalID") != root_spec.get("logicalID") or
            not root_spec.get("statefulSet") or sts_meta.get("name") != root_spec.get("statefulSet") or
            fence_spec.get("statefulSet") != root_spec.get("statefulSet") or
            not sts_meta.get("uid") or fence_spec.get("statefulSetUID") != sts_meta.get("uid")):
        raise AssertionError("pending generation fence is not bound to the active recovery intent")
    if fence.get("status", {}).get("proof"):
        raise AssertionError("generation fence already has a completion proof")
    items = recoveries.get("items")
    if not isinstance(items, list) or items:
        raise AssertionError("recovery child exists before the generation fence proof")


def proof_authorized_generation_child(root, fence, child, stateful_set):
    root_meta = root.get("metadata", {})
    root_spec = root.get("spec", {})
    root_status = root.get("status", {})
    fence_spec = fence.get("spec", {})
    proof = fence.get("status", {}).get("proof")
    operation = fence_spec.get("operationID")
    source = fence_spec.get("sourceClusterID")
    binding_uid = fence_spec.get("bindingUID")
    sts_meta = stateful_set.get("metadata", {})
    if not isinstance(proof, dict):
        return False
    if (not operation or not source or not binding_uid or not root_meta.get("uid") or
            binding_uid != root_meta.get("uid") or not root_spec.get("logicalID") or
            fence_spec.get("logicalID") != root_spec.get("logicalID") or not root_spec.get("statefulSet") or
            fence_spec.get("statefulSet") != root_spec.get("statefulSet") or
            sts_meta.get("name") != root_spec.get("statefulSet") or not sts_meta.get("uid") or
            fence_spec.get("statefulSetUID") != sts_meta.get("uid")):
        raise AssertionError("generation fence is not bound to the current root and StatefulSet")
    if (proof.get("operationID") != operation or proof.get("sourceClusterID") != source or
            proof.get("bindingUID") != binding_uid or not proof.get("requestHash") or
            not proof.get("proofID") or not all(proof.get(field) is True for field in
                ("processesTerminated", "recreationBlocked", "storageQuiesced"))):
        raise AssertionError("generation fence proof is not complete or exact")
    if not isinstance(child, dict):
        return False
    child_spec = child.get("spec", {})
    child_meta = child.get("metadata", {})
    child_annotations = child_meta.get("annotations", {})
    child_fence = child_spec.get("fence", {})
    active = root_status.get("activeRecovery") == operation
    completed_before_poll = (
        not root_status.get("activeRecovery") and child.get("status", {}).get("phase") == "Complete" and
        child.get("status", {}).get("target") == root_status.get("activeClusterID") and
        root_status.get("activeClusterID") != source)
    if (not (active or completed_before_poll) or
            child.get("metadata", {}).get("name") != operation or
            child_annotations.get("rhiza.mrchypark.dev/automatic-owner") != root_meta.get("uid") or
            child_annotations.get("rhiza.mrchypark.dev/automatic-logical") != root_spec.get("logicalID") or
            child_spec.get("statefulSet") != root_spec.get("statefulSet") or
            child_spec.get("recoveryID") != operation or
            child_spec.get("sourceClusterID") != source or
            child_fence.get("recoveryID") != operation or
            child_fence.get("clusterID") != source or
            child_fence.get("statefulSetUID") != fence_spec.get("statefulSetUID") or
            child_fence.get("confirmed") is not True or
            child_fence.get("evidence") != proof.get("proofID")):
        raise AssertionError("recovery child is not bound to the verified generation fence proof")
    return child


class AutomaticChaos(Chaos):
    def __init__(self, args):
        super().__init__(args)
        self.supervised_bootstrap = True

    def apply(self, objs):
        # Hold only newly provisioned learners before the database starts. This
        # gives the fault injector a deterministic window to install UDP loss.
        for obj in objs if isinstance(objs, list) else [objs]:
            if obj.get("kind") == "StatefulSet" and obj["metadata"]["name"] == "rhiza":
                ctr = obj["spec"]["template"]["spec"]["containers"][0]
                if self.args.embedded_host != "server":
                    ctr["ports"].append({"name": "recovery", "containerPort": 9091})
                ctr["command"] = ["/usr/local/bin/rhiza-fencer"]
                ctr["args"] = ["supervise", "--", "/bin/sh", "-ec",
                    'if [ -n "${RHIZA_LEARNER:-}" ]; then while [ ! -f /data/.chaos-start ]; do sleep 0.2; done; fi; exec /usr/local/bin/rhiza-entrypoint']
                ctr["env"].extend([
                    {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
                    {"name": "RHIZA_POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                    {"name": "FENCER_AUTHORITY_URL", "value": "https://rhiza-fencer:9444/v1/supervisor"},
                    {"name": "FENCER_CA_FILE", "value": "/fencer-ca/ca.crt"},
                ])
                ctr["securityContext"] = {"allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
                ctr["volumeMounts"].extend([
                    {"name": "temporary", "mountPath": "/tmp"},
                    {"name": "fence-socket", "mountPath": "/run/rhiza-fencer"},
                    {"name": "fence-token", "mountPath": "/var/run/secrets/rhiza-fencer", "readOnly": True},
                    {"name": "fence-ca", "mountPath": "/fencer-ca", "readOnly": True},
                ])
                podspec = obj["spec"]["template"]["spec"]
                podspec.update({"serviceAccountName": "rhiza-supervised",
                                "automountServiceAccountToken": False,
                                "hostPID": False, "shareProcessNamespace": False})
                podspec["volumes"].extend([
                    {"name": "temporary", "emptyDir": {}},
                    {"name": "fence-socket", "emptyDir": {}},
                    {"name": "fence-token", "projected": {"sources": [{"serviceAccountToken": {
                        "path": "token", "audience": "rhiza-fencer", "expirationSeconds": 600}}]}},
                    {"name": "fence-ca", "configMap": {"name": "rhiza-fencer-ca"}},
                ])
        super().apply(objs)

    def supervised_root_resource(self):
        return {"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaCluster",
                "metadata": {"name": "rhiza", "namespace": self.ns}, "spec": {
                    "logicalID": "chaos", "statefulSet": "rhiza", "container": "rhiza",
                    "adoptClusterID": "chaos-source", "automatic": True, "voterCount": 3,
                    "policy": {"failureGraceSeconds": 15, "cooldownSeconds": 3,
                                "maxRecoveriesPerDay": 10, "allowDataLoss": False}}}

    def executor_outage(self, stopped):
        if not stopped:
            self.k("scale", "deployment/rhiza-fencer", "--replicas=1")
            self.k("rollout", "status", "deployment/rhiza-fencer", "--timeout=120s")
            return

        authority = self.get("deployment", "rhiza-fencer")
        authority_uid = authority.get("metadata", {}).get("uid", "")
        initial_generation = authority.get("metadata", {}).get("generation")
        selector_labels = authority.get("spec", {}).get("selector", {}).get("matchLabels", {})
        initial_replicas = authority.get("spec", {}).get("replicas")
        if not authority_uid or not selector_labels:
            raise AssertionError("authority Deployment identity or Pod selector is unavailable")
        if not isinstance(initial_generation, int) or initial_generation < 1:
            raise AssertionError("authority Deployment generation is unavailable")
        if not isinstance(initial_replicas, int) or initial_replicas < 1:
            raise AssertionError("authority Deployment is not running before outage")
        selector = ",".join(f"{key}={value}" for key, value in sorted(selector_labels.items()))

        def authority_pods():
            result = json.loads(self.k("get", "pods", "-l", selector, "-o", "json"))
            pods = result.get("items")
            if not isinstance(pods, list):
                raise RuntimeError("authority Pod inventory is invalid")
            identities = []
            for pod in pods:
                metadata = pod.get("metadata", {})
                name, uid = metadata.get("name", ""), metadata.get("uid", "")
                if not name or not uid:
                    raise RuntimeError("authority Pod name or UID is unavailable")
                identities.append((name, uid))
            return identities

        captured_pods = authority_pods()
        if not captured_pods:
            raise AssertionError("no authority Pod identity was present before outage")
        captured_pod_uids = [uid for _, uid in captured_pods]
        self.k("scale", "deployment/rhiza-fencer", "--replicas=0")

        def authority_absent():
            current = self.get("deployment", "rhiza-fencer")
            metadata = current.get("metadata", {})
            spec = current.get("spec", {})
            if metadata.get("uid") != authority_uid:
                raise AssertionError("authority Deployment identity changed during outage")
            if spec.get("selector", {}).get("matchLabels", {}) != selector_labels:
                raise AssertionError("authority Pod selector changed during outage")
            if spec.get("replicas") != 0:
                return False
            generation = metadata.get("generation")
            observed_generation = current.get("status", {}).get("observedGeneration")
            if not isinstance(generation, int) or not isinstance(observed_generation, int):
                raise RuntimeError("authority Deployment generation status is unavailable")
            if generation <= initial_generation or observed_generation < generation:
                return False
            if authority_pods():
                return False
            for name, uid in captured_pods:
                raw = self.k("get", "pod", name, "--ignore-not-found", "-o", "json").strip()
                if not raw or raw == "null":
                    continue
                pod_metadata = json.loads(raw).get("metadata", {})
                if pod_metadata.get("name") != name or not pod_metadata.get("uid"):
                    raise RuntimeError("captured authority Pod identity is unavailable")
                if pod_metadata["uid"] != uid:
                    raise AssertionError(f"captured authority Pod identity changed: {name}")
                return False
            return True

        self.wait("captured authority Deployment and Pods stopped", authority_absent, 180)
        self.event("automatic_executor_outage_observed", deployment_uid=authority_uid,
                   pod_uids=captured_pod_uids)

    def verify_embedded(self, pod):
        if self.args.embedded_host == "server":
            return
        host = self.http(pod, "/host")
        assert host["host"] == self.args.embedded_host + "-embedded", host
        for route, port in (("/membership/status", 8080), ("/sql/query", 9091)):
            try:
                self.http(pod, route, remote_port=port)
            except urllib.error.HTTPError as exc:
                assert exc.code == 404, (route, port, exc.code)
            else:
                raise AssertionError(f"unexpected API exposure: {route} on {port}")
        self.event("embedded_host_verified", pod=pod, host=host["host"], management_port=9091)

    def verify_data(self, pod):
        result = super().verify_data(pod)
        self.verify_embedded(pod)
        return result

    def start_learner(self, name):
        if not hasattr(self, "learner_logs"):
            self.learner_logs = []
        log_path = self.private_diagnostics_dir() / f"{name}-startup.log"
        with log_path.open("a") as log:
            log_path.chmod(0o600)
            self.learner_logs.append(subprocess.Popen(
                ["kubectl", "--context", self.context, "-n", self.ns, "logs", "-f", name, "-c", "rhiza"],
                stdout=log, stderr=subprocess.STDOUT))
        self.k("exec", name, "-c", "rhiza", "--", "touch", "/data/.chaos-start")

    def cluster(self):
        return self.get("rhizacluster", "rhiza")

    def healthy(self):
        return self.cluster().get("status", {}).get("phase") == "Healthy"

    def fences(self):
        return json.loads(self.k("get", "rhizafences", "-o", "json"))["items"]

    def run(self):
        root = Path(__file__).resolve().parents[2]
        for name, plural in (("cluster", "rhizaclusters"), ("fence", "rhizafences")):
            self.k("apply", "-f", str(root / f"deploy/operator/{name}-crd.yaml"))
            self.k("wait", "--for=condition=Established", f"crd/{plural}.rhiza.mrchypark.dev", "--timeout=60s")
        self.setup()
        for pod in ("rhiza-0", "rhiza-1", "rhiza-2"):
            self.verify_embedded(pod)
        permission = subprocess.run(["kubectl", "--context", self.context, "-n", self.ns,
            "auth", "can-i", "update", "rhizafences.rhiza.mrchypark.dev", "--subresource=status",
            "--as=system:serviceaccount:" + self.ns + ":rhiza-operator"], capture_output=True, text=True, timeout=30)
        assert permission.returncode == 1 and permission.stdout.strip() == "no", "Operator can self-approve fence or RBAC check failed"
        self.event("fence_proof_rbac_pass")
        self.wait("automatic cluster healthy", self.healthy, 180)
        before = self.status("rhiza-1")
        uid, cid, pid = self.runtime("rhiza-1")
        self.command(["docker", "exec", self.node, "kill", "-9", str(pid)])
        self.wait("container restart", lambda: self.runtime("rhiza-1")[1] != cid)
        self.wait("intact WAL healthy", self.healthy)
        after = self.wait("voter ready", lambda: self.status("rhiza-1") if self.ready("rhiza-1") else None)
        assert after["wal_identity"] == before["wal_identity"]
        assert self.get("pod","rhiza-1")["metadata"]["uid"] == uid
        assert not self.fences(), "intact restart unexpectedly requested fencing"
        self.event("automatic_intact_wal_pass", pod_uid=uid)

        # Live old processes lose QUIC connectivity. The Operator must not start
        # DR while its executor is unavailable, even with all processes visible.
        self.executor_outage(True)
        current = ["rhiza-0", "rhiza-1", "rhiza-2"]
        old_uids = [self.runtime(p)[0] for p in current]
        for pod in current:
            self.udp(pod, True)
        dr = self.wait("automatic generation fence", lambda: next((f for f in self.fences() if f["spec"]["scope"] == "Generation"), None), 180)
        time.sleep(5)
        assert all(not self.runtime_gone(uid) for uid in old_uids)
        require_pending_generation_fence(
            self.cluster(), dr, json.loads(self.k("get", "rhizarecoveries", "-o", "json")),
            self.get("statefulset", "rhiza"))
        self.event("automatic_partition_barrier_pass", operation=dr["spec"]["operationID"], old_uids=old_uids,
                   recovery_children=0, recovery_dispatch="waiting_for_fence_proof")
        self.stop.set()
        self.writer.join(timeout=20)
        assert not self.writer.is_alive()
        self.executor_outage(False)
        operation = dr["spec"]["operationID"]
        def proof_child_ready():
            root = self.cluster()
            fence = self.get("rhizafence", dr["metadata"]["name"])
            proof = fence.get("status", {}).get("proof")
            if not proof:
                return None
            child = self.cr(operation)
            return proof_authorized_generation_child(
                root, fence, child, self.get("statefulset", "rhiza"))

        self.wait("proof-authorized generation recovery child", proof_child_ready, 240)
        self.wait("unattended generation recovery", lambda: self.healthy() and self.cluster()["status"]["activeClusterID"] != "chaos-source", 480)
        child = self.cr(operation)
        self.admin = base64.b64decode(self.get("secret",child["status"]["secretName"])["data"]["admin"]).decode()
        count = self.verify_data("rhiza-0")
        new_write_rows = self.verify_idempotent_write(
            "rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'recovered')",
            (10002, "recovered"))
        assert all(self.runtime_gone(uid) for uid in old_uids)
        self.event("automatic_generation_recovery_pass", acknowledged_rows=len(self.acks), recovered_rows=count,
                   target=child["status"]["target"], request_replay_scope="one final new-generation write",
                   request_replay="same-id-same-sql", new_write_readback=new_write_rows)

        # An ordinary Pod loss with a surviving quorum is a controller-routing
        # check only. It must not create another ScopeVoter operation or start
        # Generation DR. The independent Generation DR positive above remains
        # a separate scenario and result.
        config = self.status("rhiza-0")
        voters = sorted(config.get("voters", []))
        assert len(voters) == 3 and "rhiza-0" in voters, config
        survivors = tuple(voters[:2])
        lost_pod = next(pod for pod in voters if pod not in survivors)
        before = {pod: self.status(pod) for pod in survivors}
        assert all(state["voting"] and not state["pending"] for state in before.values()), before
        assert before[survivors[0]]["config_id"] == before[survivors[1]]["config_id"]
        fences_before = {item["metadata"]["name"] for item in self.fences()}
        recoveries_before = {item["metadata"]["name"] for item in
                             json.loads(self.k("get", "rhizarecoveries", "-o", "json"))["items"]}
        lost_uid = self.runtime(lost_pod)[0]
        self.k("delete", "pod", lost_pod, "--wait=true")
        time.sleep(10)
        after = {pod: self.status(pod) for pod in survivors}
        assert all(state["voting"] and not state["pending"] for state in after.values()), after
        assert all(after[pod]["config_id"] == before[pod]["config_id"] for pod in survivors), after
        fences_after = {item["metadata"]["name"] for item in self.fences()}
        recoveries_after = {item["metadata"]["name"] for item in
                            json.loads(self.k("get", "rhizarecoveries", "-o", "json"))["items"]}
        assert fences_after == fences_before, "quorum-alive Pod loss created a fencing scope"
        assert recoveries_after == recoveries_before, "quorum-alive Pod loss dispatched controller recovery"
        self.event("automatic_quorum_alive_dr_only_routing_pass", lost_pod=lost_pod, lost_uid=lost_uid,
                   survivors=list(survivors), config_id=after[survivors[0]]["config_id"],
                   controller_fences=len(fences_after), controller_recoveries=len(recoveries_after),
                   ordinary_recovery_claimed=False)
        self.event("PASS", scenarios=4, unattended=True)

    def cleanup(self):
        # Observe management state before closing port forwards; never replay a
        # mutation from the fault injector to diagnose an unattended failure.
        try:
            pods = json.loads(self.k("get", "pods", "-o", "json"))["items"]
            statuses = {}
            for pod in pods:
                if not any(c["name"] == "rhiza" for c in pod["spec"]["containers"]):
                    continue
                name = pod["metadata"]["name"]
                try:
                    statuses[name] = self.status(name)
                except (RuntimeError, OSError, ValueError):
                    statuses[name] = {"unavailable": True}
            (self.out / "membership-status.json").write_text(json.dumps(statuses, indent=2))
        except (RuntimeError, OSError, ValueError):
            pass
        for kind in ("rhizaclusters","rhizafences"):
            try:
                (self.out / f"{kind}.json").write_text(self.k("get",kind,"-o","json"))
            except RuntimeError:
                pass
        super().cleanup()
        for proc in getattr(self, "learner_logs", []):
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--embedded-host", choices=("server", "go", "rust"), default="server")
    parser.add_argument("--cluster",default="rhiza-operator-chaos")
    parser.add_argument("--db-image",default="rhiza-chaos:local")
    parser.add_argument("--operator-image",default="rhiza-operator-chaos:local")
    parser.add_argument("--output",default="automatic-chaos-results")
    chaos=AutomaticChaos(parser.parse_args())
    try:
        chaos.run()
    except Exception as original:
        try:
            errors = chaos.capture_failure_diagnostics()
        except Exception:
            errors = ["diagnostic_capture_failed"]
        if errors:
            raise RuntimeError(f"{original}; bounded diagnostic capture errors: {errors}") from original
        raise
    finally:
        chaos.cleanup()


if __name__ == "__main__":
    main()
