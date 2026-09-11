#!/usr/bin/env python3
"""Unattended recovery: faults only, no manual recovery CR or fence approval."""
import argparse
import base64
import json
import subprocess
from pathlib import Path
import time

from operator_recovery import Chaos
from kind_fencer import KindFencer


class AutomaticChaos(Chaos):
    def apply(self, objs):
        # Hold only newly provisioned learners before the database starts. This
        # gives the fault injector a deterministic window to install UDP loss.
        for obj in objs if isinstance(objs, list) else [objs]:
            if obj.get("kind") == "StatefulSet" and obj["metadata"]["name"] == "rhiza":
                ctr = obj["spec"]["template"]["spec"]["containers"][0]
                ctr["command"] = ["/bin/sh", "-ec"]
                ctr["args"] = ['if [ -n "${RHIZA_LEARNER:-}" ]; then while [ ! -f /data/.chaos-start ]; do sleep 0.2; done; fi; exec /usr/local/bin/rhiza-entrypoint']
        super().apply(objs)

    def pending_learner(self, excluded=""):
        pods = json.loads(self.k("get", "pods", "-l", "rhiza.mrchypark.dev/automatic-learner", "-o", "json"))["items"]
        for pod in pods:
            name = pod["metadata"]["name"]
            if name != excluded:
                self.runtime(name)
                return name
        return None

    def start_learner(self, name):
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
        permission = subprocess.run(["kubectl", "--context", self.context, "-n", self.ns,
            "auth", "can-i", "update", "rhizafences.rhiza.mrchypark.dev", "--subresource=status",
            "--as=system:serviceaccount:" + self.ns + ":rhiza-operator"], capture_output=True, text=True, timeout=30)
        assert permission.returncode == 1 and permission.stdout.strip() == "no", "Operator can self-approve fence or RBAC check failed"
        self.event("fence_proof_rbac_pass")
        self.executor = KindFencer(self)
        self.executor.start()
        self.k("set", "env", "deployment/rhiza-operator", "RHIZA_AUTOMATIC_RECOVERY=true", "RHIZA_FENCER_BACKEND=kubernetes")
        self.k("rollout", "status", "deployment/rhiza-operator", "--timeout=120s")
        self.apply({"apiVersion":"rhiza.mrchypark.dev/v1alpha1", "kind":"RhizaCluster",
                    "metadata":{"name":"rhiza","namespace":self.ns}, "spec":{
                    "logicalID":"chaos", "statefulSet":"rhiza", "container":"rhiza",
                    "adoptClusterID":"chaos-source", "automatic":True,
                    "voterPods":[{"nodeID":f"rhiza-{i}","pod":f"rhiza-{i}"} for i in range(3)],
                    "policy":{"failureGraceSeconds":15,"cooldownSeconds":3,"maxRecoveriesPerDay":10,"allowDataLoss":False}}})
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

        lost_uid = self.runtime("rhiza-2")[0]
        self.k("delete","pod","rhiza-2","--wait=true")
        fence = self.wait("automatic voter fence request", lambda: next(iter(self.fences()), None), 180)
        assert fence["spec"]["scope"] == "Voter"
        operation = fence["spec"]["operationID"]
        mutation = subprocess.run(["kubectl","--context",self.context,"-n",self.ns,"patch","rhizafence",fence["metadata"]["name"],
                                   "--type=merge","-p",json.dumps({"spec":{"scope":"Generation"}})],
                                  capture_output=True,text=True,timeout=30)
        assert mutation.returncode != 0 and "immutable" in mutation.stderr, "fencing operation spec was mutable"
        self.event("fence_request_immutable_pass", operation=operation)
        time.sleep(5)
        children = json.loads(self.k("get","rhizarecoveries","-o","json"))["items"]
        assert not children, "recovery dispatched without executor proof"
        self.operator_restart()
        assert self.fences()[0]["spec"]["operationID"] == operation
        self.event("automatic_executor_outage_restart_pass", operation=operation)
        self.executor.enabled = True
        first_learner = self.wait("automatic learner provisioned", self.pending_learner, 240)
        self.udp(first_learner, True)
        self.start_learner(first_learner)
        self.wait("automatic addition frozen", lambda: self.cr(operation).get("status", {}).get("membership", {}).get("add") and self.status("rhiza-0").get("pending"), 180)
        first_uid = self.runtime(first_learner)[0]
        self.k("delete", "pod", first_learner, "--wait=true")
        fresh = self.wait("automatic failed learner abort and retry", lambda: self.pending_learner(first_learner), 300)
        self.start_learner(fresh)
        self.wait("unattended online replacement", self.healthy, 360)
        assert self.runtime_gone(first_uid)
        child = self.cr(operation)
        # Promotion clears the live abort marker; the new addition journal
        # retains the abort revision that authorized this fresh learner.
        abort_slot = child["status"]["membership"]["add"]["expectedAbortSlot"]
        assert abort_slot > 0
        self.event("automatic_lost_learner_abort_retry_pass", old_learner=first_learner, new_learner=fresh, old_uid=first_uid, abort_slot=abort_slot)
        assert child["status"]["phase"] == "Complete"
        learner = child["spec"]["membership"]["replacementPod"]
        assert self.status(learner)["voting"]
        self.verify_data(learner)
        self.event("automatic_online_replacement_pass", learner=learner, old_uid=lost_uid)

        self.executor.close()
        self.executor = KindFencer(self)
        self.executor.start()
        old_identity = {"apiVersion":"v1","kind":"Pod", "metadata":{"name":"fence-recreation-probe","namespace":self.ns},
                        "spec":{"containers":[{"name":"rhiza","image":self.args.db_image,"env":[
                            {"name":"RHIZA_CLUSTER_ID","value":"chaos-source"},
                            {"name":"RHIZA_NODE_ID","value":"rhiza-2"}]}]}}
        denial = subprocess.run(["kubectl","--context",self.context,"-n",self.ns,"create","--dry-run=server","-f","-"],
                                input=json.dumps(old_identity),capture_output=True,text=True,timeout=30)
        assert denial.returncode != 0 and "fenced" in denial.stderr and operation in denial.stderr, "completed fence lost after executor restart"
        self.event("automatic_executor_restart_barrier_pass", operation=operation)

        # Live old processes lose QUIC connectivity. The Operator must not start
        # DR while its executor is unavailable, even with all processes visible.
        self.executor.enabled = False
        current = ["rhiza-0", "rhiza-1", learner]
        old_uids = [self.runtime(p)[0] for p in current]
        for pod in current:
            self.udp(pod, True)
        dr = self.wait("automatic generation fence", lambda: next((f for f in self.fences() if f["spec"]["scope"] == "Generation"), None), 180)
        time.sleep(5)
        assert all(not self.runtime_gone(uid) for uid in old_uids)
        assert len(json.loads(self.k("get","rhizarecoveries","-o","json"))["items"]) == 1
        self.event("automatic_partition_barrier_pass", operation=dr["spec"]["operationID"], old_uids=old_uids)
        self.stop.set()
        self.writer.join(timeout=20)
        assert not self.writer.is_alive()
        self.executor.enabled = True
        operation = dr["spec"]["operationID"]
        self.wait("unattended generation recovery", lambda: self.healthy() and self.cluster()["status"]["activeClusterID"] != "chaos-source", 480)
        child = self.cr(operation)
        self.admin = base64.b64decode(self.get("secret",child["status"]["secretName"])["data"]["admin"]).decode()
        count = self.verify_data("rhiza-0")
        self.execute("rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'recovered')")
        assert all(self.runtime_gone(uid) for uid in old_uids)
        self.event("automatic_generation_recovery_pass", acknowledged_rows=len(self.acks), recovered_rows=count,
                   target=child["status"]["target"], new_write=True, dedup_preserved=True)
        self.event("PASS", scenarios=7, unattended=True)

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
        if hasattr(self,"executor"):
            self.executor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster",default="rhiza-operator-chaos")
    parser.add_argument("--db-image",default="rhiza-chaos:local")
    parser.add_argument("--operator-image",default="rhiza-operator-chaos:local")
    parser.add_argument("--output",default="automatic-chaos-results")
    chaos=AutomaticChaos(parser.parse_args())
    try:
        chaos.run()
    finally:
        chaos.cleanup()


if __name__ == "__main__":
    main()
