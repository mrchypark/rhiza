#!/usr/bin/env python3
"""Anchor recovery: external CAS anchor gates generation transition.

Fixtures: namespace+RBAC+anchor CM BEFORE self.setup(). Seed trust_state
AFTER stop writer. Evidence correct from initial seed (no later preseed).
Three negative gates, then resume and verify.
"""
import argparse
import base64
import hashlib
import json
import subprocess
import time

from automatic_recovery import AutomaticChaos
from kind_fencer import KindFencer
from operator_fixture import MC_IMAGE

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
        "pending_write": False, "generation": 0}


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

    def seed_trust_state(self, pod):
        self.execute(pod, "seed-schema",
            "CREATE TABLE IF NOT EXISTS trust_state "
            "(id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, "
            "token TEXT NOT NULL)")
        self.execute(pod, "seed-evidence",
            "INSERT OR REPLACE INTO trust_state (id, epoch, token) "
            "VALUES (1, 1, 'initial')")
        rows = self.http(pod, "/sql/query",
            {"sql": "SELECT epoch, token FROM trust_state WHERE id=1"})
        assert rows["rows"] == [["1", "initial"]]
        self.event("trust_state_seeded")

    def verify_trust_state(self, pod, epoch, token):
        rows = self.http(pod, "/sql/query",
            {"sql": "SELECT epoch, token FROM trust_state WHERE id=1"})
        assert rows["rows"] == [[str(epoch), token]]

    def _read_anchor(self):
        cm = self.get("configmap", ANCHOR_CM)
        return json.loads(cm["data"]["anchor.json"])

    def _write_anchor(self, record):
        self.k("patch", "configmap", ANCHOR_CM, "--type=merge",
               "-p", json.dumps({"data": {
                   "anchor.json": json.dumps(record)}}))

    def _set_pending(self, pending):
        rec = self._read_anchor()
        rec["pending_write"] = pending
        self._write_anchor(rec)

    def _mc_run(self, mc_args, expect_phase="Succeeded"):
        ns = self.ns
        alias = ("mc alias set local http://rhiza-minio:9000 "
                 "chaos-minio chaos-minio-secret")
        full = alias + " && mc " + " ".join(mc_args)
        pod = f"mc-{int(time.time())}"
        self.apply({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": pod, "namespace": ns},
            "spec": {"restartPolicy": "Never",
                     "containers": [{"name": "mc", "image": MC_IMAGE,
                         "command": ["/bin/sh", "-c"],
                         "args": [full]}]}})
        self.wait(f"mc {mc_args[0]}", lambda: self._mc_done(pod), 120)
        p = self.get("pod", pod)
        phase = p["status"].get("phase", "")
        out = self.k("logs", pod, "--tail=50")
        self.k("delete", "pod", pod, "--ignore-not-found", "--wait=false")
        if phase != expect_phase:
            raise RuntimeError(f"mc {mc_args[0]} phase={phase}: {out[:200]}")
        return out

    def _mc_done(self, name):
        try:
            p = self.get("pod", name)
            return p["status"].get("phase", "") in ("Succeeded", "Failed")
        except Exception:
            return False

    def mc_backup_anchor(self, tp):
        self._mc_run(["cp",
            f"local/rhiza/rhiza/{tp}/recovery/anchor.json",
            f"local/rhiza/rhiza/{tp}/recovery/anchor.json.bak"])

    def mc_restore_anchor(self, tp):
        self._mc_run(["cp",
            f"local/rhiza/rhiza/{tp}/recovery/anchor.json.bak",
            f"local/rhiza/rhiza/{tp}/recovery/anchor.json"])

    def mc_corrupt_anchor(self, tp):
        ns = self.ns
        alias = ("mc alias set local http://rhiza-minio:9000 "
                 "chaos-minio chaos-minio-secret")
        dst = f"local/rhiza/rhiza/{tp}/recovery/anchor.json"
        full = f'{alias} && printf CORRUPTED | mc pipe {dst}'
        pod = f"mc-corrupt-{int(time.time())}"
        self.apply({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": pod, "namespace": ns},
            "spec": {"restartPolicy": "Never",
                     "containers": [{"name": "mc", "image": MC_IMAGE,
                         "command": ["/bin/sh", "-c"],
                         "args": [full]}]}})
        self.wait(f"mc corrupt {pod}",
            lambda: self._mc_done(pod), 60)
        p = self.get("pod", pod)
        self.k("delete", "pod", pod, "--ignore-not-found", "--wait=false")
        if p["status"].get("phase") != "Succeeded":
            raise RuntimeError("mc corrupt failed")
        self.event("anchor_json_corrupted", target=tp)

    def _child_for_op(self, op):
        items = json.loads(
            self.k("get", "rhizarecoveries", "-o", "json"))["items"]
        for item in items:
            s = item.get("status", {})
            if s.get("recoveryID") == op or item["metadata"]["name"] == op:
                return item
        return None

    def _child_sealed_blocked(self, op):
        item = self._child_for_op(op)
        if item is None:
            return False
        s = item.get("status", {})
        return (s.get("stage") == "Sealed" and
                s.get("phase") == "Blocked")

    def _child_target(self, op):
        item = self._child_for_op(op)
        return item.get("status", {}).get("target") if item else None

    def _child_message(self, op):
        item = self._child_for_op(op)
        return item.get("status", {}).get("message", "") if item else ""

    def _source_cluster_id(self):
        """Resolve source cluster ID from rhiza-config envFrom."""
        cfg = self.get("configmap", "rhiza-config")
        return cfg["data"].get("RHIZA_CLUSTER_ID", SOURCE_CLUSTER)

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
                             "voterPods": [{"nodeID": f"rhiza-{i}",
                                "pod": f"rhiza-{i}"} for i in range(3)],
                             "policy": {"failureGraceSeconds": 15,
                                        "cooldownSeconds": 3,
                                        "maxRecoveriesPerDay": 10,
                                        "allowDataLoss": False}}})
        self.wait("automatic cluster healthy", self.healthy, 180)

        # Stop writer BEFORE seed — no pending races.
        self.stop.set()
        self.writer.join(timeout=15)
        assert not self.writer.is_alive()

        self.seed_trust_state("rhiza-0")

        self.executor = KindFencer(self)
        self.executor.start()

        # Resolve source cluster ID from ConfigMap (envFrom, not inline env).

        current = ["rhiza-0", "rhiza-1", "rhiza-2"]
        old_uids = [self.runtime(p)[0] for p in current]

        # ── Gate 1: Missing anchor ConfigMap ──
        saved_cm = self.k("get", "configmap", ANCHOR_CM, "-o", "json")
        self.k("delete", "configmap", ANCHOR_CM)
        for pod in current:
            self.udp(pod, True)
        self.executor.enabled = True
        dr = self.wait("generation fence",
            lambda: next((f for f in self.fences()
                if f["spec"]["scope"] == "Generation"), None), 180)
        operation = dr["spec"]["operationID"]
        self.wait("recovery child",
            lambda: self._child_for_op(operation) is not None, 120)
        self.wait("blocked without anchor",
            lambda: self._child_sealed_blocked(operation), 60)
        sts = self.get("statefulset", "rhiza")
        env = {e["name"]: e.get("value", "")
               for e in sts["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        assert env.get("RHIZA_CLUSTER_ID", self._source_cluster_id()) == SOURCE_CLUSTER, \
            "target generation started while anchor blocked"
        assert sts["spec"]["replicas"] == 3, "STS replicas changed"
        msg1 = self._child_message(operation)
        assert "anchor activation" in msg1.lower(), \
            f"gate1 message missing anchor activation: {msg1}"
        self.event("missing_anchor_gate", op=operation, msg=msg1)

        # Restore CM ATOMICALLY with Pending=true.
        saved = json.loads(saved_cm)
        del saved.get("metadata", {})["resourceVersion"]
        saved["metadata"].pop("uid", None)
        anchor_data = json.loads(saved["data"]["anchor.json"])
        anchor_data["pending_write"] = True
        saved["data"]["anchor.json"] = json.dumps(anchor_data)
        self.apply(saved)
        self.event("anchor_cm_restored_pending", op=operation)

        # ── Gate 2: PendingWrite flag ──
        prev_rv_gate2 = self._child_for_op(operation)["metadata"]["resourceVersion"]
        self.wait("pending flag blocked", lambda: (
            self._child_sealed_blocked(operation)
            and self._child_for_op(operation)["metadata"]["resourceVersion"] != prev_rv_gate2
            and "anchor activation" in self._child_message(operation).lower()
        ), 60)
        msg2 = self._child_message(operation)
        self.event("pending_flag_gate", op=operation, msg=msg2,
                   prev_rv=prev_rv_gate2,
                   new_rv=self._child_for_op(operation)["metadata"]["resourceVersion"])

        # ── Gate 3: Target checkpoint corruption ──
        target = self._child_target(operation)
        assert target, "target must be allocated"
        self.mc_backup_anchor(target)
        prev_child_rv = self._child_for_op(operation)["metadata"]["resourceVersion"]
        prev_msg3 = self._child_message(operation)
        self.mc_corrupt_anchor(target)
        self._set_pending(False)
        self.wait("blocked after corruption", lambda: (
            self._child_sealed_blocked(operation)
            and self._child_for_op(operation)["metadata"]["resourceVersion"] != prev_child_rv
            and self._child_message(operation) != prev_msg3
        ), 60)
        new_child_rv = self._child_for_op(operation)["metadata"]["resourceVersion"]
        msg3 = self._child_message(operation)
        self.event("corrupt_anchor_gate", op=operation, msg=msg3,
                   prev_rv=prev_child_rv, new_rv=new_child_rv)

        self.mc_restore_anchor(target)
        self.event("anchor_json_restored", target=target)

        # ── Recovery resumes (evidence correct from initial seed) ──
        self.wait("generation recovery complete",
            lambda: self.healthy() and
                self.cluster()["status"].get("activeClusterID") != self._source_cluster_id(),
            480)
        target = self._child_target(operation)
        assert target and target != self._source_cluster_id()
        self.event("recovery_pass", op=operation, target=target)

        count = self.verify_data("rhiza-0")
        self.verify_trust_state("rhiza-0", 1, "initial")
        self.execute("rhiza-0", "target-write",
            "INSERT INTO chaos_items VALUES (10002, 'anchor-recovered')")
        self.event("data_pass", rows=count)

        assert all(self.runtime_gone(uid) for uid in old_uids)
        record = self._read_anchor()
        assert record["binding"]["cluster_id"] == target
        assert record["binding"]["cluster_id"] != self._source_cluster_id()
        self.event("oldgen_pass", old_uids=old_uids)

        self.event("PASS", scenarios=1, anchor=True, negative_gates=3)

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
