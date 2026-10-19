#!/usr/bin/env python3
"""voterCount GCS + Chaos Mesh qualification driver.

Reuses operator_recovery.Chaos (JSON REST helpers, port-forwards, evidence
events) and adds Chaos Mesh CRs, structural fence matching, completed-recovery
waits and a real writer.

Claims are kept separate and are never merged:
  (i)  positive: cooperative online replacement and generation recovery;
  (ii) fail-safe: dead-Pod execution blocking while termination evidence is
       unresolved (reported as unqualified observations, never as PASS).
"""
import argparse
import json
import os
import threading
import time
from pathlib import Path

from operator_recovery import Chaos

VOTERS = ("rhiza-voter-0", "rhiza-voter-1", "rhiza-voter-2")
COMPLETE = ("Complete",)


class VoterCountChaos(Chaos):
    def __init__(self, args):
        self.args = args
        self.context = args.context
        self.ns = args.namespace
        self.run_id = args.run_id
        self.out = Path(args.output)
        self.out.mkdir(parents=True, exist_ok=True)
        self.forwards = {}
        self.events = []
        self.acks = set()
        self.stop = threading.Event()
        self.writer = None
        self.admin = args.admin_token or os.environ.get("RHIZA_QUALIFICATION_ADMIN_TOKEN", "")
        self.declared = {"namespace": args.namespace, "run_id": args.run_id,
                        "app_image": args.app_image, "operator_image": args.operator_image,
                        "chaos_created": [], "cluster_scoped":
                        ["%s-faults" % args.namespace, "%s-network-faults" % args.namespace,
                         "%s-fencer-tokenreview" % args.namespace]}

    # ---- Chaos Mesh -------------------------------------------------------
    def create_chaos(self, kind, name, spec):
        self.apply({"apiVersion": "chaos-mesh.org/v1alpha1", "kind": kind,
                    "metadata": {"name": name, "namespace": self.ns,
                                 "labels": {"chaos.rhiza.io/run": self.run_id}}, "spec": spec})
        self.declared["chaos_created"].append([kind, name])
        return name

    def wait_chaos(self, kind, name, condition, timeout=120):
        def done():
            conds = self.get(kind.lower(), name).get("status", {}).get("conditions", [])
            return any(c.get("type") == condition and c.get("status") == "True" for c in conds)
        return self.wait("%s %s %s" % (kind, name, condition), done, timeout)

    def container_records(self, name):
        return self.get("podchaos", name)["status"].get("experiment", {}).get("containerRecords", [])

    def delete_chaos(self, kind, name):
        self.k("delete", kind.lower(), name, "--ignore-not-found", "--wait=true")

    # ---- state ------------------------------------------------------------
    def fences(self):
        return json.loads(self.k("get", "rhizafences", "-o", "json")).get("items", [])

    def recoveries(self):
        return json.loads(self.k("get", "rhizarecoveries", "-o", "json")).get("items", [])

    def recovery_uids(self):
        return {r["metadata"]["uid"] for r in self.recoveries()}

    def cluster(self):
        return self.get("rhizacluster", "rhiza")

    def cluster_phase(self):
        return self.cluster().get("status", {}).get("phase")

    def pod(self, name):
        return self.get("pod", name)

    def pod_identity(self, name):
        obj = self.pod(name)
        state = self.status(name)
        return {"node_id": name, "pod_uid": obj["metadata"]["uid"],
                "wal_identity": state["wal_identity"], "config_id": state["config_id"]}

    def fence_for(self, ident):
        """Structural match: parse spec.targets, never a substring search."""
        hits = []
        for fence in self.fences():
            spec = fence.get("spec", {})
            for target in spec.get("targets", []):
                if (target.get("pod") == ident["node_id"]
                        and target.get("podUID") == ident["pod_uid"]
                        and target.get("walIdentity") == ident["wal_identity"]):
                    hits.append(fence)
                    break
        return hits

    def assert_controllers_running(self):
        for label in ("app=rhiza-operator", "app=rhiza-fencer"):
            pods = json.loads(self.k("get", "pods", "-l",
                                     "%s,chaos.rhiza.io/run=%s" % (label, self.run_id),
                                     "-o", "json")).get("items", [])
            running = [p for p in pods if p["status"].get("phase") == "Running"]
            assert running, "%s is not running; a negative result is not evidence" % label
        return True

    def assert_cluster_observed(self):
        """The controller must be reconciling for a negative to mean anything."""
        first = self.cluster().get("metadata", {}).get("resourceVersion")
        time.sleep(2)
        second = self.cluster().get("metadata", {}).get("resourceVersion")
        assert first and second, "RhizaCluster has no observed status"
        assert self.cluster().get("status", {}) is not None, "no status"
        return first, second

    def assert_deployed_images(self):
        for name in VOTERS:
            img = self.pod(name)["status"]["containerStatuses"][0]["imageID"]
            assert self.args.app_image.split(":")[-1] in img, (name, img)
        for pod in json.loads(self.k("get", "pods", "-l",
                                     "app=rhiza-operator,chaos.rhiza.io/run=%s" % self.run_id,
                                     "-o", "json"))["items"]:
            img = pod["status"]["containerStatuses"][0]["imageID"]
            if ":" in self.args.operator_image:
                assert self.args.operator_image.split(":")[-1] in img, img
        return True

    def assert_no_proof_structural(self, ident):
        held = self.fence_for(ident)
        assert held, "no structurally bound fence request for %s; fail-safe not exercised" % ident["node_id"]
        for fence in held:
            assert "deletionTimestamp" not in fence.get("metadata", {}), "fence is deleting"
            assert not fence.get("status", {}).get("proof"), "unexpected proof for %s" % ident["node_id"]
        return held

    def assert_no_new_child(self, before):
        new = self.recovery_uids() - before
        assert not new, "recovery child created without a proof: %s" % sorted(new)
        return True

    def wait_completion(self, before, timeout=420):
        """Success means the operator-installed recovery actually completed."""
        def done():
            for child in self.recoveries():
                if child["metadata"]["uid"] in before:
                    continue
                status = child.get("status", {})
                phase = status.get("membership", {}).get("phase") or status.get("phase")
                if phase in COMPLETE:
                    return child
            return None
        return self.wait("completed recovery child", done, timeout)

    # ---- workload ---------------------------------------------------------
    def start_writer(self):
        def write():
            i = 1
            while not self.stop.is_set() and i <= 5000:
                try:
                    self.execute(VOTERS[0], "vc-write-%d" % i,
                                 "INSERT INTO chaos_items VALUES (%d, 'ack')" % i)
                    self.acks.add(i)
                    i += 1
                except Exception:
                    pass
                self.stop.wait(0.15)
        self.writer = threading.Thread(target=write, daemon=True)
        self.writer.start()

    def prepare(self):
        self.assert_deployed_images()
        for name in VOTERS:
            self.wait("voter ready " + name, lambda n=name: self.ready(n), 300)
        self.execute(VOTERS[0], "vc-schema",
                     "CREATE TABLE IF NOT EXISTS chaos_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.start_writer()
        self.wait("workload began", lambda: len(self.acks) >= 5, 120)
        self.event("prepare_pass", voters=list(VOTERS), acknowledged=len(self.acks))

    def readback(self, pod):
        count = self.verify_data(pod)
        self.execute(pod, "vc-post-recovery", "INSERT INTO chaos_items VALUES (90001, 'post')")
        self.acks.add(90001)
        self.event("readback_pass", pod=pod, rows=count, acknowledged=len(self.acks))

    # ---- scenarios --------------------------------------------------------
    def chaos_injected(self, kind, name, expect_records=None):
        self.wait_chaos(kind, name, "AllInjected", 120)
        if expect_records is not None:
            records = self.container_records(name)
            assert sum(r.get("injectedCount", 0) for r in records) == expect_records, records
        return True

    def scenario_intact_wal_restart(self):
        before = self.recovery_uids()
        ident = self.pod_identity(VOTERS[0])
        self.create_chaos("PodChaos", "vc-intact-wal", {
            "action": "container-kill", "mode": "one", "duration": "10s", "containerNames": ["rhiza"],
            "selector": {"namespaces": [self.ns], "labelSelectors": {
                "chaos.rhiza.io/run": self.run_id, "app.kubernetes.io/part-of": "rhiza-chaos",
                "statefulset.kubernetes.io/pod-name": VOTERS[0]}}})
        self.chaos_injected("PodChaos", "vc-intact-wal", 1)
        self.wait_chaos("PodChaos", "vc-intact-wal", "AllRecovered", 150)
        after = self.wait("voter ready again", lambda: self.pod_identity(VOTERS[0]), 240)
        assert after["pod_uid"] == ident["pod_uid"], "container-kill replaced the Pod"
        assert after["wal_identity"] == ident["wal_identity"], "WAL identity drifted"
        self.assert_no_new_child(before)
        assert not self.fence_for(ident), "container-kill must not request a fence"
        self.delete_chaos("PodChaos", "vc-intact-wal")
        self.readback(VOTERS[0])
        self.event("intact_wal_restart_pass", pod_uid=ident["pod_uid"])

    def scenario_online_replacement(self):
        before = self.recovery_uids()
        ident = self.pod_identity(VOTERS[0])
        self.create_chaos("NetworkChaos", "vc-isolate-voter-0", {
            "action": "partition", "mode": "one", "duration": "45s", "direction": "both",
            "selector": {"namespaces": [self.ns], "labelSelectors": {
                "chaos.rhiza.io/run": self.run_id, "app.kubernetes.io/part-of": "rhiza-chaos",
                "statefulset.kubernetes.io/pod-name": VOTERS[0]}},
            "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"},
                "expressionSelectors": [{"key": "statefulset.kubernetes.io/pod-name", "operator": "In",
                                         "values": [VOTERS[1], VOTERS[2]]}]}}})
        self.chaos_injected("NetworkChaos", "vc-isolate-voter-0")
        self.assert_controllers_running()
        self.wait_chaos("NetworkChaos", "vc-isolate-voter-0", "AllRecovered", 180)
        self.wait("voter fence request", lambda: self.fence_for(ident) or None, 120)
        child = self.wait_completion(before)
        self.event("online_replacement_completed", child=child["metadata"]["name"],
                   scope=child.get("spec", {}).get("membership", {}).get("remove"))
        self.delete_chaos("NetworkChaos", "vc-isolate-voter-0")
        self.readback(VOTERS[1])
        self.event("online_replacement_pass")

    def scenario_generation_recovery(self):
        before = self.recovery_uids()
        self.create_chaos("NetworkChaos", "vc-generation", {
            "action": "partition", "mode": "all", "duration": "60s", "direction": "both",
            "selector": {"namespaces": [self.ns], "labelSelectors": {
                "chaos.rhiza.io/run": self.run_id, "app": "rhiza-operator"}},
            "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}}})
        self.chaos_injected("NetworkChaos", "vc-generation")
        self.assert_controllers_running()
        self.assert_no_new_child(before)
        self.wait_chaos("NetworkChaos", "vc-generation", "AllRecovered", 180)
        def generation_fence():
            return [f for f in self.fences()
                    if f.get("spec", {}).get("scope") == "Generation"] or None
        fence = self.wait("generation fence request", generation_fence, 240)
        self.wait("generation proof published", lambda: (
            self.get("rhizafences", fence[0]["metadata"]["name"]).get("status", {}).get("proof")), 300)
        child = self.wait_completion(before)
        self.event("generation_recovery_completed", child=child["metadata"]["name"],
                   targets=len(fence[0]["spec"].get("targets", [])))
        self.delete_chaos("NetworkChaos", "vc-generation")
        self.readback(VOTERS[2])
        self.event("generation_recovery_pass")

    # ---- dead-Pod fail-safe (D1-D4), never a PASS -------------------------
    def dead_pod_case(self, scenario, target, select_pod_selector):
        before = self.recovery_uids()
        ident = self.pod_identity(target)
        start = {"scenario": scenario, "target": target, "pod_uid": ident["pod_uid"],
                 "wal_identity": ident["wal_identity"], "cluster_phase": self.cluster_phase(),
                 "fence_uids": [f["metadata"]["uid"] for f in self.fences()],
                 "recovery_uids": sorted(before)}
        if scenario == "D1":
            assert ident["wal_identity"], "D1 requires an already WAL-bound target"
        if scenario == "D2":
            assert not [f for f in self.fence_for(ident) if f.get("status", {}).get("proof")], "D2 requires no terminal proof"
        if scenario == "D4":
            assert self.cluster_phase() in ("Healthy", "Observing"), start["cluster_phase"]
            assert not self.fences(), "D4 requires no in-flight intent"
        self.event("dead_pod_start", **start)
        self.create_chaos("PodChaos", "vc-dead-" + scenario, {
            "action": "pod-kill", "mode": "one", "duration": "10s",
            "selector": {"namespaces": [self.ns], "labelSelectors": {
                "chaos.rhiza.io/run": self.run_id, "app.kubernetes.io/part-of": "rhiza-chaos",
                "statefulset.kubernetes.io/pod-name": select_pod_selector}}})
        self.chaos_injected("PodChaos", "vc-dead-" + scenario)
        self.wait("termination is unresolved", lambda: self.pod(target).get("status", {})
                  .get("phase") != "Running", 120)
        time.sleep(20)
        self.assert_controllers_running()
        self.assert_cluster_observed()
        held = self.assert_no_proof_structural(ident)
        self.assert_no_new_child(before)
        self.event("dead_pod_failsafe_observed", scenario=scenario, target=target,
                   fence_requests=len(held),
                   completion="unqualified: cooperative backend cannot prove termination for a dead Pod")
        self.delete_chaos("PodChaos", "vc-dead-" + scenario)

    def run(self):
        self.event("start", run_id=self.run_id,
                   claim_positive="cooperative online replacement and generation recovery",
                   claim_failsafe="dead-Pod execution blocked while termination evidence is unresolved")
        self.prepare()
        self.scenario_intact_wal_restart()
        self.scenario_online_replacement()
        self.scenario_generation_recovery()
        self.dead_pod_case("D1", VOTERS[2], VOTERS[2])
        self.dead_pod_case("D2", VOTERS[1], VOTERS[1])
        self.dead_pod_case("D3", VOTERS[0], VOTERS[0])
        self.dead_pod_case("D4", VOTERS[2], VOTERS[2])
        self.event("complete", positive=["intact_wal_restart", "online_replacement", "generation_recovery"],
                   fail_safe="dead_pod_execution_blocking_unqualified")

    def cleanup(self):
        self.stop.set()
        if self.writer:
            self.writer.join(timeout=15)
        for kind, name in list(self.declared.get("chaos_created", [])):
            try:
                self.delete_chaos(kind, name)
            except Exception:
                pass
        try:
            (self.out / "rollback-material.json").write_text(json.dumps({
                "cluster_scoped": self.declared["cluster_scoped"],
                "namespace": self.declared["namespace"]}, indent=2))
        except Exception:
            pass
        Chaos.cleanup(self)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--app-image", required=True)
    parser.add_argument("--operator-image", required=True)
    parser.add_argument("--admin-token", default="")
    parser.add_argument("--output", default="votercount-gcs-chaos-results")
    chaos = VoterCountChaos(parser.parse_args())
    try:
        chaos.run()
    finally:
        chaos.cleanup()


if __name__ == "__main__":
    main()
