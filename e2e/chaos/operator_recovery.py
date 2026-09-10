#!/usr/bin/env python3
"""Real Kubernetes/CRI faults against the running Operator; no mocked APIs."""
import argparse
import base64
import json
import http.client
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

from operator_fixture import resources, learner_resources


class Chaos:
    def __init__(self, args):
        self.args = args
        self.ns = "rhiza-chaos"
        self.context = "kind-" + args.cluster
        self.node = args.cluster + "-control-plane"
        self.out = Path(args.output)
        self.out.mkdir(parents=True, exist_ok=True)
        self.forwards = {}
        self.events = []
        self.acks = {0}
        self.stop = threading.Event()
        self.writer = None
        self.admin = "chaos-admin"

    def command(self, argv, data=None):
        result = subprocess.run(argv, input=data, text=True, capture_output=True, timeout=240)
        if result.returncode:
            raise RuntimeError(f"{argv[:7]}: {result.stderr[-1500:]}")
        return result.stdout

    def k(self, *argv, data=None):
        return self.command(["kubectl", "--context", self.context, "-n", self.ns, *argv], data)

    def get(self, kind, name):
        return json.loads(self.k("get", kind, name, "-o", "json"))

    def apply(self, objs):
        if isinstance(objs, list):
            objs = {"apiVersion": "v1", "kind": "List", "items": objs}
        self.k("apply", "-f", "-", data=json.dumps(objs))

    def patch(self, kind, name, value):
        self.k("patch", kind, name, "--type=merge", "-p", json.dumps(value))

    def event(self, name, **evidence):
        entry = {"event": name, "time": time.time(), **evidence}
        self.events.append(entry)
        (self.out / "evidence.json").write_text(json.dumps(self.events, indent=2))
        print(json.dumps(entry), flush=True)

    def wait(self, label, predicate, timeout=180):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except (RuntimeError, OSError, http.client.HTTPException, ValueError, KeyError) as exc:
                last = str(exc)
            time.sleep(0.5)
        raise AssertionError(f"timeout: {label}; last={last}")

    def forward(self, pod):
        # Each writer has its own forward; replacing a Pod invalidates its UID.
        key = (threading.get_ident(), pod)
        uid = self.get("pod", pod)["metadata"]["uid"]
        old = self.forwards.get(key)
        if old and old[0] == uid and old[1].poll() is None:
            return old[2]
        if old:
            old[1].terminate()
            old[1].wait(timeout=10)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        log = open(self.out / f"forward-{pod}-{key[0]}.log", "a")
        proc = subprocess.Popen(["kubectl", "--context", self.context, "-n", self.ns,
                                 "port-forward", "pod/" + pod, f"{port}:8080"], stdout=log, stderr=log)
        log.close()
        self.forwards[key] = (uid, proc, port)
        try:
            self.wait("port forward " + pod, lambda: self.port_open(port), 15)
        except AssertionError as exc:
            proc.terminate()
            raise RuntimeError(str(exc)) from exc
        return port

    @staticmethod
    def port_open(port):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            return False

    def http(self, pod, route, body=None, admin=None):
        port = self.forward(pod)
        request = urllib.request.Request(f"http://127.0.0.1:{port}{route}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + (admin or self.admin)})
        with urllib.request.urlopen(request, timeout=6) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}

    def status(self, pod):
        return self.http(pod, "/membership/status")

    def execute(self, pod, rid, sql):
        result = self.http(pod, "/sql/execute", {"request_id": rid, "sql": sql})
        assert not result.get("error_code"), result
        return result

    def ready(self, pod):
        return self.status(pod).get("voting") and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in self.get("pod", pod)["status"].get("conditions", []))

    def runtime(self, pod):
        obj = self.get("pod", pod)
        statuses = obj["status"].get("containerStatuses", [])
        item = next((s for s in statuses if s["name"] == "rhiza" and s.get("state", {}).get("running")), None)
        if item is None:
            raise RuntimeError("rhiza container is not running")
        cid = item["containerID"].split("://", 1)[1]
        info = json.loads(self.command(["docker", "exec", self.node, "crictl", "inspect", cid]))
        return obj["metadata"]["uid"], cid, int(info["info"]["pid"])

    def runtime_gone(self, uid):
        data = json.loads(self.command(["docker", "exec", self.node, "crictl", "ps", "-a", "-o", "json"]))
        return not any(c.get("labels", {}).get("io.kubernetes.pod.uid") == uid and
                       c["state"] == "CONTAINER_RUNNING" for c in data.get("containers", []))

    def udp(self, pod, blocked):
        uid, cid, pid = self.runtime(pod)
        self.command(["docker", "exec", self.node, "nsenter", "-t", str(pid), "-n", "iptables",
                      "-I" if blocked else "-D", "INPUT", "-p", "udp", "--dport", "9090", "-j", "DROP"])
        self.event("learner_udp_drop" if blocked else "learner_udp_restore", pod=pod, uid=uid, container=cid)

    def cr(self, name):
        return self.get("rhizarecovery", name)

    def phase(self, name, phase):
        return self.cr(name).get("status", {}).get("membership", {}).get("phase") == phase

    def operator_restart(self):
        pod = json.loads(self.k("get", "pods", "-l", "app.kubernetes.io/name=rhiza-operator", "-o", "json"))["items"][0]
        uid = pod["metadata"]["uid"]
        self.k("delete", "pod", pod["metadata"]["name"], "--wait=true")
        self.k("rollout", "status", "deployment/rhiza-operator", "--timeout=120s")
        self.event("operator_pod_restarted", old_uid=uid)

    def create_learner(self, name):
        self.apply(learner_resources(self.ns, name, self.args.db_image, self.members))
        state = self.wait("learner HTTP " + name, lambda: self.status(name))
        assert not state["voting"] and state["wal_identity"], state
        return state

    def replacement(self, name, removed, removed_uid, nonce, voters, learner):
        spec = {"statefulSet": "rhiza", "container": "rhiza", "sourceClusterID": "chaos-source",
                "durability": "before-ack", "membership": {"operationID": name, "remove": removed,
                "removedPod": removed, "voterPods": [{"nodeID": p, "pod": p} for p in voters],
                "replacementPod": learner, "replacementSecret": learner + "-credentials",
                "fence": {"nodeID": removed, "walIdentity": nonce, "workloadUID": removed_uid,
                          "confirmed": False, "evidence": ""}}}
        self.apply({"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
                    "metadata": {"name": name, "namespace": self.ns}, "spec": spec})
        self.wait("unfenced request blocked", lambda: self.phase(name, "Blocked"))
        before = self.status(voters[0])["config_id"]
        time.sleep(3)
        assert self.status(voters[0])["config_id"] == before
        assert self.runtime_gone(removed_uid), "old voter still running"
        self.patch("rhizarecovery", name, {"spec": {"membership": {"fence": {
            "confirmed": True, "evidence": f"CRI confirms Pod UID {removed_uid} has no running container; StatefulSet scaled below its ordinal"}}}})
        self.wait("addition journal", lambda: self.cr(name).get("status", {}).get("membership", {}).get("add"))
        self.wait("certified addition freeze", lambda: self.status(voters[0])["pending"])
        self.event("replacement_frozen", operation=name, config=self.status(voters[0])["config_id"])

    def start_writer(self):
        def write():
            i = 1
            while not self.stop.is_set() and i <= 1000:
                try:
                    self.execute("rhiza-0", f"chaos-write-{i}", f"INSERT INTO chaos_items VALUES ({i}, 'acknowledged')")
                    self.acks.add(i)
                    i += 1
                except (RuntimeError, OSError, http.client.HTTPException, ValueError):
                    pass  # Retry the same request ID after the injected fault.
                self.stop.wait(0.15)
        self.writer = threading.Thread(target=write, daemon=True)
        self.writer.start()

    def verify_data(self, pod):
        data = self.http(pod, "/sql/query", {"sql": "SELECT id FROM chaos_items ORDER BY id"})
        found = {int(row[0]) for row in data["rows"]}
        assert self.acks <= found, f"lost acknowledged rows: {self.acks - found}"
        self.execute(pod, "chaos-seed", "INSERT INTO chaos_items VALUES (0, 'seed')")
        return len(found)

    def run(self):
        root = Path(__file__).resolve().parents[2]
        self.k("apply", "-f", str(root / "deploy/operator/crd.yaml"))
        self.k("wait", "--for=condition=Established", "crd/rhizarecoveries.rhiza.mrchypark.dev", "--timeout=60s")
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.ns}})
        raw = self.command(["kubectl", "--context", self.context, "create", "--dry-run=client", "-f", str(root / "deploy/operator/rbac.yaml"), "-o", "json"])
        rbac = []
        while raw.strip():
            obj, end = json.JSONDecoder().raw_decode(raw.lstrip())
            rbac.append(obj)
            raw = raw.lstrip()[end:]
        for obj in rbac:
            obj["metadata"]["namespace"] = self.ns
            for subject in obj.get("subjects", []):
                subject["namespace"] = self.ns
        self.apply(rbac)
        fixture = resources(self.ns, self.args.db_image, self.args.operator_image)
        workloads = [o for o in fixture if o["kind"] == "StatefulSet" or o["metadata"]["name"] == "rhiza-operator"]
        self.apply([o for o in fixture if o not in workloads])
        self.k("wait", "--for=condition=Complete", "job/rhiza-create-bucket", "--timeout=180s")
        self.apply(workloads)
        config = self.get("configmap", "rhiza-config")["data"]
        self.members = json.loads(config["RHIZA_CLUSTER_MEMBERS"])
        self.k("rollout", "status", "deployment/rhiza-operator", "--timeout=180s")
        for pod in ("rhiza-0", "rhiza-1", "rhiza-2"):
            self.wait("initial voter " + pod, lambda p=pod: self.ready(p), 240)
        self.execute("rhiza-0", "chaos-schema", "CREATE TABLE chaos_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.execute("rhiza-0", "chaos-seed", "INSERT INTO chaos_items VALUES (0, 'seed')")
        self.start_writer()
        self.wait("workload began", lambda: len(self.acks) >= 5)

        # SIGKILL the real process, preserving the Pod's emptyDir/WAL.
        before = self.status("rhiza-1")
        uid, cid, pid = self.runtime("rhiza-1")
        self.command(["docker", "exec", self.node, "kill", "-9", str(pid)])
        self.wait("container restarted", lambda: self.runtime("rhiza-1")[1] != cid)
        after = self.wait("intact WAL restart", lambda: self.status("rhiza-1") if self.ready("rhiza-1") else None)
        assert self.get("pod", "rhiza-1")["metadata"]["uid"] == uid
        assert after["wal_identity"] == before["wal_identity"] and after["config_id"] == before["config_id"]
        self.event("sigkill_intact_wal_pass", pod_uid=uid, wal_identity=after["wal_identity"])

        # Pod deletion really destroys emptyDir; replacement must fail closed.
        lost = self.status("rhiza-2")
        old_uid, _, _ = self.runtime("rhiza-2")
        self.k("delete", "pod", "rhiza-2", "--wait=true")
        self.wait("new Pod UID", lambda: self.get("pod", "rhiza-2")["metadata"]["uid"] != old_uid)
        self.wait("lost WAL refusal", lambda: "voter state continuity" in self.k("logs", "rhiza-2", "--tail=30"))
        self.patch("statefulset", "rhiza", {"spec": {"replicas": 2}})
        self.wait("removed runtime fenced", lambda: self.runtime_gone(old_uid))
        self.k("wait", "--for=delete", "pod/rhiza-2", "--timeout=90s")
        self.event("lost_wal_refused", old_uid=old_uid)
        self.create_learner("learner-a")
        self.udp("learner-a", True)
        self.replacement("replace-2", "rhiza-2", old_uid, lost["wal_identity"], ["rhiza-0", "rhiza-1"], "learner-a")
        journal = self.cr("replace-2")["status"]["membership"]
        self.operator_restart()
        assert self.cr("replace-2")["status"]["membership"]["add"] == journal["add"]
        self.udp("learner-a", False)
        self.wait("first replacement complete", lambda: self.phase("replace-2", "Complete"), 240)
        assert self.status("learner-a")["voting"]
        assert self.get("statefulset", "rhiza")["spec"]["replicas"] == 2
        self.execute("learner-a", "promoted-write", "INSERT INTO chaos_items VALUES (10001, 'promoted')")
        self.acks.add(10001)
        self.event("partition_and_operator_restart_pass", config=self.status("learner-a")["config_id"])

        # A second replacement uses an already promoted standalone voter.
        lost = self.status("rhiza-1")
        old_uid, _, _ = self.runtime("rhiza-1")
        self.patch("statefulset", "rhiza", {"spec": {"replicas": 1}})
        self.k("wait", "--for=delete", "pod/rhiza-1", "--timeout=90s")
        self.create_learner("learner-b")
        self.udp("learner-b", True)
        self.replacement("replace-1", "rhiza-1", old_uid, lost["wal_identity"], ["rhiza-0", "learner-a"], "learner-b")
        self.k("delete", "pod", "learner-b", "--wait=true")
        self.patch("rhizarecovery", "replace-1", {"spec": {"membership": {"abortAddition": True}}})
        self.wait("failed learner addition aborted", lambda: self.phase("replace-1", "Aborted"), 240)
        abort_slot = self.status("rhiza-0")["abort_slot"]
        assert abort_slot > 0
        self.create_learner("learner-c")
        self.patch("rhizarecovery", "replace-1", {"spec": {"membership": {"abortAddition": False,
            "replacementPod": "learner-c", "replacementSecret": "learner-c-credentials"}}})
        self.wait("fresh incarnation promoted", lambda: self.phase("replace-1", "Complete"), 240)
        assert self.status("learner-c")["voting"]
        self.event("lost_learner_abort_resume_pass", abort_slot=abort_slot, config=self.status("learner-c")["config_id"])
        self.stop.set()
        self.writer.join(timeout=15)
        assert not self.writer.is_alive()
        self.verify_data("rhiza-0")

        # External scheduling fence prevents old voters from being recreated.
        old_uids = [self.runtime(p)[0] for p in ("rhiza-0", "learner-a", "learner-c")]
        self.patch("statefulset", "rhiza", {"spec": {"replicas": 3, "template": {"spec": {
            "nodeSelector": {"rhiza-chaos/fenced": "true"}}}}})
        self.k("delete", "pod", "rhiza-0", "rhiza-1", "rhiza-2", "learner-a", "learner-c", "--ignore-not-found", "--wait=true")
        self.wait("all old runtimes fenced", lambda: all(self.runtime_gone(uid) for uid in old_uids))
        sts_uid = self.get("statefulset", "rhiza")["metadata"]["uid"]
        fence = {"recoveryID": "whole-generation", "clusterID": "chaos-source", "statefulSetUID": sts_uid,
                 "confirmed": False, "evidence": ""}
        self.apply({"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
            "metadata": {"name": "whole-generation", "namespace": self.ns},
            "spec": {"statefulSet": "rhiza", "sourceClusterID": "chaos-source", "durability": "before-ack",
                     "recoveryID": "whole-generation", "fence": fence}})
        self.wait("whole generation requires fence", lambda: self.cr("whole-generation").get("status", {}).get("phase") == "AwaitingFence")
        assert not self.cr("whole-generation").get("status", {}).get("target")
        self.patch("rhizarecovery", "whole-generation", {"spec": {"fence": {"confirmed": True,
            "evidence": "All old Pod UIDs have no running CRI containers; unmatched nodeSelector prevents source StatefulSet rescheduling"}}})
        def target_started():
            cr = self.cr("whole-generation")
            target = cr.get("status", {}).get("target")
            env = self.get("statefulset", "rhiza")["spec"]["template"]["spec"]["containers"][0]["env"]
            return target if target and any(e.get("name") == "RHIZA_CLUSTER_ID" and e.get("value") == target for e in env) else None
        target = self.wait("target credentials activated", target_started, 300)
        self.patch("statefulset", "rhiza", {"spec": {"template": {"spec": {"nodeSelector": None}}}})
        self.k("delete", "pod", "rhiza-0", "rhiza-1", "rhiza-2", "--ignore-not-found", "--wait=true")
        self.wait("whole generation complete", lambda: self.cr("whole-generation").get("status", {}).get("phase") == "Complete", 300)
        secret_name = self.cr("whole-generation")["status"]["secretName"]
        self.admin = base64.b64decode(self.get("secret", secret_name)["data"]["admin"]).decode()
        count = self.verify_data("rhiza-0")
        self.execute("rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'new-generation')")
        assert self.status("rhiza-0")["cluster_id"] == target
        self.event("whole_generation_recovery_pass", target=target, acknowledged_rows=len(self.acks), recovered_rows=count,
                   old_pod_uids=old_uids, new_write=True, dedup_preserved=True)
        self.event("PASS", scenarios=5)

    def cleanup(self):
        self.stop.set()
        if self.writer:
            self.writer.join(timeout=15)
        for _, proc, _ in self.forwards.values():
            proc.terminate()
        for kind in ("pods", "statefulsets", "deployments", "rhizarecoveries", "events"):
            try:
                (self.out / f"{kind}.json").write_text(self.k("get", kind, "-o", "json"))
            except RuntimeError:
                pass
        try:
            pods = json.loads(self.k("get", "pods", "-o", "json"))["items"]
            for pod in pods:
                name = pod["metadata"]["name"]
                for previous in (False, True):
                    try:
                        args = ["logs", name, "--all-containers", "--tail=500"] + (["--previous"] if previous else [])
                        (self.out / f"{name}{'-previous' if previous else ''}.log").write_text(self.k(*args))
                    except RuntimeError:
                        pass
        except RuntimeError:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", default="rhiza-operator-chaos")
    parser.add_argument("--db-image", default="rhiza-chaos:local")
    parser.add_argument("--operator-image", default="rhiza-operator-chaos:local")
    parser.add_argument("--output", default="operator-chaos-results")
    chaos = Chaos(parser.parse_args())
    try:
        chaos.run()
    finally:
        chaos.cleanup()


if __name__ == "__main__":
    main()
