#!/usr/bin/env python3
"""Verify the unsupported S3/Kubernetes anchored-recovery profile blocks safely.

The Kind fixture supplies a valid external anchor, but its S3 archive and
Kubernetes cooperative fencer cannot perform canonical archive-fork
registration. This is negative contract evidence, not recovery qualification.
"""
import argparse
import base64
import hashlib
import json
import subprocess
import time

from automatic_recovery import AutomaticChaos
from kind_fencer import KindFencer
from operator_fixture import AWS_CLI_IMAGE

ANCHOR_ID = "chaos-anchor"
ANCHOR_CM = "rhiza-anchor-chaos-anchor"
SOURCE_CLUSTER = "chaos-source"
ANCHOR_SA = "anchor-service"
STORE_CFG = ["s3", "rhiza-minio:9000", "rhiza", "rhiza", SOURCE_CLUSTER]
ANCHOR_ENV = [
    {"name": "RHIZA_RECOVERY_ANCHOR_ID", "value": ANCHOR_ID},
    {"name": "RHIZA_NAMESPACE", "value": "__NS__"},
]
OP_ANCHOR_ENV = [
    {"name": "RHIZA_RECOVERY_ANCHOR_URL",
     "value": "https://anchor-service.__NS__.svc:9191"},
    {"name": "RHIZA_RECOVERY_ANCHOR_TOKEN_FILE", "value": "/anchor/token"},
    {"name": "RHIZA_RECOVERY_ANCHOR_CA_FILE", "value": "/anchor/ca.crt"},
]


def storage_id(cluster):
    return hashlib.sha256(
        json.dumps(STORE_CFG[:-1] + [cluster], separators=(",", ":")).encode()
    ).hexdigest()


def canonical_evidence(epoch=1, token="initial"):
    return base64.b64encode(
        json.dumps({"epoch": epoch, "token": token},
                    separators=(",", ":")).encode()
    ).decode()


def make_anchor_record(src_cluster):
    return {
        "version": 1, "evidence_format": "sql-epoch-token/v1",
        "binding": {"cluster_id": src_cluster,
                    "storage_id": storage_id(src_cluster)},
        "evidence": canonical_evidence(),
        "pending_write": False, "generation": 0,
        "transition": None, "last_transition": None}


class AnchorChaos(AutomaticChaos):

    def apply(self, objs):
        ns = self.ns
        items = objs if isinstance(objs, list) else [objs]
        for obj in items:
            kind = obj.get("kind", "")
            name = obj.get("metadata", {}).get("name", "")
            if kind == "ConfigMap" and name == "rhiza-config":
                obj["data"]["RHIZA_RECOVERY_ANCHOR_URL"] = \
                    f"https://anchor-service.{ns}.svc:9191"
                obj["data"]["RHIZA_RECOVERY_ANCHOR_TOKEN_FILE"] = "/anchor/token"
                obj["data"]["RHIZA_RECOVERY_ANCHOR_CA_FILE"] = "/anchor/ca.crt"
            if kind == "StatefulSet" and name == "rhiza":
                ctr = obj["spec"]["template"]["spec"]["containers"][0]
                env = ctr.get("env", [])
                for e in ANCHOR_ENV:
                    env.append({"name": e["name"],
                                "value": e["value"].replace("__NS__", ns)})
                ctr["env"] = env
            if kind == "Deployment" and name == "rhiza-operator":
                ctr = obj["spec"]["template"]["spec"]["containers"][0]
                env = ctr.get("env", [])
                for e in OP_ANCHOR_ENV:
                    env.append({"name": e["name"],
                                "value": e["value"].replace("__NS__", ns)})
                ctr["env"] = env
                vols = obj["spec"]["template"]["spec"].get("volumes", [])
                vols.append({"name": "anchor",
                    "secret": {"secretName": "anchor-token"}})
                obj["spec"]["template"]["spec"]["volumes"] = vols
                mounts = ctr.get("volumeMounts", [])
                mounts.append({"name": "anchor", "mountPath": "/anchor",
                               "readOnly": True})
                ctr["volumeMounts"] = mounts
        super().apply(objs)

    def cluster(self):
        return self.get("rhizacluster", "rhiza")

    def healthy(self):
        return self.cluster().get("status", {}).get("phase") == "Healthy"

    def fences(self):
        return json.loads(self.k("get", "rhizafences", "-o", "json"))["items"]

    def _anchor_running(self):
        try:
            pods = json.loads(self.k("get", "pods",
                "-l", "app=anchor-service", "-o", "json"))["items"]
            return any(
                p["status"].get("phase") == "Running"
                and all(c.get("ready")
                    for c in p["status"].get("containerStatuses", []))
                for p in pods
            )
        except Exception:
            return False

    def _anchor_rbac(self):
        ns = self.ns
        return [
            {"apiVersion": "v1", "kind": "ServiceAccount",
             "metadata": {"name": ANCHOR_SA, "namespace": ns}},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
             "metadata": {"name": "anchor-service", "namespace": ns},
             "rules": [{"apiGroups": [""], "resources": ["configmaps"],
                        "resourceNames": [ANCHOR_CM],
                        "verbs": ["get", "update"]}]},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
             "metadata": {"name": "anchor-service", "namespace": ns},
             "subjects": [{"kind": "ServiceAccount", "name": ANCHOR_SA,
                           "namespace": ns}],
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                         "kind": "Role", "name": "anchor-service"}},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
             "metadata": {"name": "anchor-app-guard", "namespace": ns},
             "rules": [{"apiGroups": [""], "resources": ["configmaps"],
                        "resourceNames": [ANCHOR_CM],
                        "verbs": ["get", "update"]}]},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
             "metadata": {"name": "anchor-app-guard", "namespace": ns},
             "subjects": [{"kind": "ServiceAccount", "name": "default",
                           "namespace": ns}],
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                         "kind": "Role", "name": "anchor-app-guard"}},
        ]

    def _gen_tls(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            cnf = f"{d}/san.cnf"
            with open(cnf, "w") as f:
                f.write(
                    f"[req]\nreq_extensions = v3_req\n"
                    f"distinguished_name = req_dn\n[req_dn]\n"
                    f"[v3_req]\nbasicConstraints = CA:FALSE\n"
                    f"keyUsage = digitalSignature, keyEncipherment\n"
                    f"extendedKeyUsage = serverAuth\n"
                    f"subjectAltName = @alt_names\n"
                    f"[alt_names]\n"
                    f"DNS.1 = anchor-service.{self.ns}.svc\n")
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", f"{d}/key", "-out", f"{d}/cert",
                "-days", "1", "-nodes",
                "-subj", f"/CN=anchor-service.{self.ns}.svc",
                "-config", cnf, "-extensions", "v3_req",
            ], capture_output=True, check=True)
            with open(f"{d}/cert") as fc:
                cert = fc.read()
            with open(f"{d}/key") as fk:
                key = fk.read()
        return cert, key

    def _read_anchor(self):
        cm = self.get("configmap", ANCHOR_CM)
        return json.loads(cm["data"]["anchor.json"])

    def _mc_run(self, mc_args, expect_phase="Succeeded", *, service="s3",
                log_tail=50):
        ns = self.ns
        args = mc_args[1:]
        if service == "s3":
            args = ["s3://" + arg.removeprefix("local/") for arg in args]
        full = ("aws --endpoint-url http://rhiza-minio:9000 " + service + " "
                + mc_args[0] + " " + " ".join(args))
        pod = f"mc-{time.time_ns()}"
        self.apply({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": pod, "namespace": ns},
            "spec": {"restartPolicy": "Never",
                     "containers": [{"name": "aws-cli", "image": AWS_CLI_IMAGE,
                         "command": ["/bin/sh", "-c"],
                         "args": [full],
                         "env": [
                             {"name": "AWS_ACCESS_KEY_ID", "value": "chaos-minio"},
                             {"name": "AWS_SECRET_ACCESS_KEY", "value": "chaos-minio-secret"},
                             {"name": "AWS_DEFAULT_REGION", "value": "us-east-1"},
                         ]}]}})
        self.wait(f"mc {mc_args[0]}", lambda: self._mc_done(pod), 120)
        p = self.get("pod", pod)
        phase = p["status"].get("phase", "")
        out = self.k("logs", pod, f"--tail={log_tail}")
        self.k("delete", "pod", pod, "--ignore-not-found", "--wait=false")
        if phase != expect_phase:
            raise RuntimeError(f"mc {mc_args[0]} phase={phase}: {out[:200]}")
        return out

    def _source_archive_snapshot(self):
        raw = self._mc_run(
            ["list-objects-v2", "--bucket", "rhiza",
             "--prefix", f"rhiza/{SOURCE_CLUSTER}/"],
            service="s3api", log_tail=10000)
        response = json.loads(raw)
        if response.get("IsTruncated"):
            raise RuntimeError("source archive listing was truncated")
        return sorted((item["Key"], item["ETag"], item["Size"])
                      for item in response.get("Contents", []))

    def _stable_source_archive_snapshot(self):
        previous = self._source_archive_snapshot()

        def stable():
            nonlocal previous
            current = self._source_archive_snapshot()
            if current == previous:
                return current
            previous = current
            return None

        return self.wait("source archive quiescence", stable, 30)

    def _mc_done(self, name):
        try:
            p = self.get("pod", name)
            return p["status"].get("phase", "") in ("Succeeded", "Failed")
        except Exception:
            return False

    def _child_for_op(self, op):
        items = json.loads(
            self.k("get", "rhizarecoveries", "-o", "json"))["items"]
        for item in items:
            s = item.get("status", {})
            if s.get("recoveryID") == op or item["metadata"]["name"] == op:
                return item
        return None

    def run(self):
        root = self.repo_root()
        for name, plural in (("cluster", "rhizaclusters"),
                             ("fence", "rhizafences")):
            self.k("apply", "-f",
                    str(root / f"deploy/operator/{name}-crd.yaml"))
            self.k("wait", "--for=condition=Established",
                   f"crd/{plural}.rhiza.mrchypark.dev", "--timeout=60s")

        # Provision namespace + RBAC + anchor manifests BEFORE setup.
        self.apply({"apiVersion": "v1", "kind": "Namespace",
                    "metadata": {"name": self.ns}})
        self.apply(self._anchor_rbac())
        self.apply(self._anchor_cm_only())
        self.apply(self._anchor_deployment_and_service())

        # Now setup (reapplies namespace idempotently, creates fixture).
        self.setup()

        # Enable automatic recovery.
        self.k("set", "env", "deployment/rhiza-operator",
                "RHIZA_AUTOMATIC_RECOVERY=true",
                "RHIZA_FENCER_BACKEND=kubernetes")
        self.k("rollout", "status", "deployment/rhiza-operator",
                "--timeout=120s")

        self.wait("anchor pod running", self._anchor_running, 120)

        # Create RhizaCluster WITH anchorID.
        self.apply({"apiVersion": "rhiza.mrchypark.dev/v1alpha1",
                    "kind": "RhizaCluster",
                    "metadata": {"name": "rhiza", "namespace": self.ns},
                    "spec": {"anchorID": ANCHOR_ID,
                             "logicalID": "chaos",
                             "statefulSet": "rhiza", "container": "rhiza",
                             "adoptClusterID": SOURCE_CLUSTER,
                             "automatic": True,
                             "voterCount": 3,
                             "policy": {"failureGraceSeconds": 15,
                                        "cooldownSeconds": 3,
                                        "maxRecoveriesPerDay": 10,
                                        "allowDataLoss": False}}})
        self.wait("automatic cluster healthy", self.healthy, 180)

        # Stop writer before snapshotting the source archive.
        self.stop.set()
        self.writer.join(timeout=15)
        assert not self.writer.is_alive()

        anchor_before = self.get("configmap", ANCHOR_CM)
        assert json.loads(anchor_before["data"]["anchor.json"]) == \
            make_anchor_record(SOURCE_CLUSTER), "fixture anchor is not valid"
        source_cluster_before = self.cluster()["status"].get("activeClusterID")
        assert source_cluster_before == SOURCE_CLUSTER, \
            f"unexpected active source before fenced recovery: {source_cluster_before!r}"
        sts_before = self.get("statefulset", "rhiza")
        workload_env = {entry["name"]: entry.get("value", "")
                        for entry in sts_before["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        assert workload_env.get("RHIZA_RECOVERY_ANCHOR_ID") == ANCHOR_ID, \
            "source workload is not configured for the valid anchor"
        operator = self.get("deployment", "rhiza-operator")
        operator_env = {entry["name"]: entry.get("value", "")
                        for entry in operator["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        assert operator_env.get("RHIZA_RECOVERY_ANCHOR_URL") == \
            f"https://anchor-service.{self.ns}.svc:9191", \
            "Operator is not configured for the valid anchor service"
        assert operator_env.get("RHIZA_RECOVERY_ANCHOR_TOKEN_FILE") == "/anchor/token"
        assert operator_env.get("RHIZA_RECOVERY_ANCHOR_CA_FILE") == "/anchor/ca.crt"
        archive_before = self._stable_source_archive_snapshot()

        self.executor = KindFencer(self)
        self.executor.start()

        current = ["rhiza-0", "rhiza-1", "rhiza-2"]
        for pod in current:
            self.udp(pod, True)
        self.executor.enabled = True
        dr = self.wait("confirmed generation fence",
            lambda: next((f for f in self.fences()
                if f["spec"]["scope"] == "Generation" and
                f.get("status", {}).get("proof", {}).get("processesTerminated") and
                f.get("status", {}).get("proof", {}).get("recreationBlocked") and
                f.get("status", {}).get("proof", {}).get("storageQuiesced")), None), 180)
        operation = dr["spec"]["operationID"]
        self.wait("recovery child",
            lambda: self._child_for_op(operation) is not None, 120)
        def unsupported_profile_blocked():
            child = self._child_for_op(operation)
            if child is None:
                return None
            status = child.get("status", {})
            if (status.get("phase") == "Blocked" and
                    "canonical archive fork registration" in
                    status.get("message", "")):
                return child
            return None

        child = self.wait("unsupported S3/Kubernetes anchor profile blocked",
                          unsupported_profile_blocked, 60)
        child_status = child.get("status", {})
        assert not child_status.get("target"), \
            f"unsupported profile reserved a recovery target: {child_status}"
        assert not child_status.get("stage"), \
            f"unsupported profile advanced recovery stages: {child_status}"

        anchor_after = self.get("configmap", ANCHOR_CM)
        assert anchor_after["metadata"].get("uid") == anchor_before["metadata"].get("uid")
        assert anchor_after["metadata"].get("resourceVersion") == \
            anchor_before["metadata"].get("resourceVersion")
        assert anchor_after["data"] == anchor_before["data"], \
            "blocked unsupported profile changed the valid anchor"
        sts = self.get("statefulset", "rhiza")
        assert sts["metadata"].get("uid") == sts_before["metadata"].get("uid")
        assert sts["metadata"].get("generation") == sts_before["metadata"].get("generation")
        assert sts["spec"] == sts_before["spec"], \
            "blocked unsupported profile mutated the source StatefulSet"
        source_cluster_after = self.cluster()["status"].get("activeClusterID")
        assert source_cluster_after == SOURCE_CLUSTER, \
            f"unsupported profile promoted a target: {source_cluster_after!r}"
        archive_after = self._source_archive_snapshot()
        assert archive_after == archive_before, \
            "blocked unsupported profile changed source archive objects"
        fence_after = next((f for f in self.fences()
                            if f.get("spec", {}).get("operationID") == operation), None)
        assert fence_after is not None and \
            fence_after.get("metadata", {}).get("uid") == dr["metadata"]["uid"] and \
            fence_after.get("spec") == dr.get("spec") and \
            fence_after.get("status") == dr.get("status"), \
            "blocked unsupported profile changed the confirmed source fence"
        self.event("SUPPORTED-NEGATIVE", op=operation,
                   profile="S3/KubernetesFencer",
                   message=child_status.get("message", ""),
                   source_archive_objects=len(archive_after),
                   target_reserved=False, anchor_unchanged=True,
                   source_statefulset_unchanged=True,
                   source_fence_unchanged=True)

    def _anchor_cm_only(self):
        ns = self.ns
        return {"apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": ANCHOR_CM, "namespace": ns,
                             "labels": {"rhiza.mrchypark.dev/anchor": "true"}},
                "data": {"anchor.json": json.dumps(
                    make_anchor_record(SOURCE_CLUSTER))}}

    def _anchor_deployment_and_service(self):
        ns = self.ns
        cert, key = self._gen_tls()
        return [
            {"apiVersion": "v1", "kind": "Secret",
             "metadata": {"name": "anchor-tls", "namespace": ns},
             "type": "kubernetes.io/tls",
             "stringData": {"tls.crt": cert, "tls.key": key}},
            {"apiVersion": "v1", "kind": "Secret",
             "metadata": {"name": "anchor-token", "namespace": ns},
             "type": "Opaque",
             "stringData": {"token": "anchor-test-token",
                            "ca.crt": cert}},
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "anchor-service", "namespace": ns,
                          "labels": {"app": "anchor-service"}},
             "spec": {
                 "replicas": 1,
                 "selector": {"matchLabels": {"app": "anchor-service"}},
                 "template": {
                     "metadata": {"labels": {"app": "anchor-service"}},
                     "spec": {
                         "serviceAccountName": ANCHOR_SA,
                         "containers": [{
                             "name": "anchor",
                             "image": self.args.anchor_image,
                             "ports": [{"name": "https",
                                        "containerPort": 9191}],
                             "env": [
                                 {"name": "RHIZA_NAMESPACE", "value": ns},
                                 {"name": "ANCHOR_TOKEN_FILE",
                                  "value": "/token/token"},
                                 {"name": "ANCHOR_TLS_CERT",
                                  "value": "/certs/tls.crt"},
                                 {"name": "ANCHOR_TLS_KEY",
                                  "value": "/certs/tls.key"},
                                 {"name": "RHIZA_OBJSTORE_PROVIDER",
                                  "value": "s3"},
                                 {"name": "RHIZA_OBJSTORE_ENDPOINT",
                                  "value": "rhiza-minio:9000"},
                                 {"name": "RHIZA_OBJSTORE_BUCKET",
                                  "value": "rhiza"},
                                 {"name": "RHIZA_OBJSTORE_PREFIX",
                                  "value": "rhiza"},
                                 {"name": "RHIZA_OBJSTORE_INSECURE",
                                  "value": "true"},
                                 {"name": "AWS_ACCESS_KEY_ID",
                                  "value": "chaos-minio"},
                                 {"name": "AWS_SECRET_ACCESS_KEY",
                                  "value": "chaos-minio-secret"},
                                 {"name": "AWS_REGION",
                                  "value": "us-east-1"},
                             ],
                             "volumeMounts": [
                                 {"name": "certs",
                                  "mountPath": "/certs",
                                  "readOnly": True},
                                 {"name": "token",
                                  "mountPath": "/token",
                                  "readOnly": True},
                             ],
                         }],
                         "volumes": [
                             {"name": "certs",
                              "secret": {"secretName": "anchor-tls"}},
                             {"name": "token",
                              "secret": {"secretName": "anchor-token"}},
                         ],
                     },
                 },
             }},
            {"apiVersion": "v1", "kind": "Service",
             "metadata": {"name": "anchor-service", "namespace": ns},
             "spec": {
                 "selector": {"app": "anchor-service"},
                 "ports": [{"name": "https", "port": 9191,
                            "targetPort": 9191}],
             }},
        ]

    def repo_root(self):
        from pathlib import Path
        return Path(__file__).resolve().parents[2]

    def cleanup(self):
        try:
            self.k("delete", "deployment", "anchor-service",
                    "--ignore-not-found")
            self.k("delete", "service", "anchor-service",
                    "--ignore-not-found")
            self.k("delete", "serviceaccount", ANCHOR_SA,
                    "--ignore-not-found")
            self.k("delete", "role", "anchor-service",
                    "anchor-app-guard", "--ignore-not-found")
            self.k("delete", "rolebinding", "anchor-service",
                    "anchor-app-guard", "--ignore-not-found")
            self.k("delete", "secret", "anchor-tls", "anchor-token",
                    "--ignore-not-found")
            self.k("delete", "configmap", ANCHOR_CM, "--ignore-not-found")
            self.k("delete", "rhizacluster", "rhiza", "--ignore-not-found")
        except RuntimeError:
            pass
        super().cleanup()
        if hasattr(self, "executor"):
            self.executor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--embedded-host",
                        choices=("go",), default="go")
    parser.add_argument("--cluster", default="rhiza-anchor-chaos")
    parser.add_argument("--db-image", default="rhiza-chaos:local")
    parser.add_argument("--operator-image",
                        default="rhiza-operator-chaos:local")
    parser.add_argument("--anchor-image",
                        default="rhiza-anchor-service:local")
    parser.add_argument("--output", default="anchor-chaos-results")
    chaos = AnchorChaos(parser.parse_args())
    try:
        chaos.run()
    finally:
        chaos.cleanup()


if __name__ == "__main__":
    main()
