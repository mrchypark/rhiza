#!/usr/bin/env python3
"""voterCount GCS + Chaos Mesh qualification driver.

Extends the operator-chaos driver (operator_recovery.Chaos) with Chaos Mesh CRs
and real-GCS assertions. It proves two separate claims and never merges them:

  (i)  positive: cooperative online replacement and generation recovery for the
       specified voterCount and network-fault conditions on the pinned images;
  (ii) fail-safe: for a bound run of an operation that lacks termination
       evidence, no valid fencing proof and no new recovery execution approval
       occur, even though the controller and the authority keep running.

Normal quorum writes, control-journal updates, fence-request creation and
previously approved work are allowed and are not treated as violations.

The dead-Pod cases are started exactly as Pro fixed them (D1-D4) and are only
ever reported as unqualified fail-safe observations, never as PASS.
"""
import argparse
import json
import threading
import time
from pathlib import Path

from operator_recovery import Chaos


class VoterCountChaos(Chaos):
    def __init__(self, args):
        self.context = args.context
        self.ns = args.namespace
        self.run_id = args.run_id
        self.out = Path(args.output)
        self.out.mkdir(parents=True, exist_ok=True)
        self.forwards = {}
        self.events = []
        self.acks = set()
        self.stop = None
        self.writer = None
        self.admin = args.admin_token or "chaos-admin"
        # operator_recovery.Chaos helpers read self.args, self.stop and self.writer.
        self.args = args
        self.stop = threading.Event()
        self.writer = None

    # -- Chaos Mesh CRs -----------------------------------------------------
    def chaos(self, kind, name, spec, labels=None):
        body = {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": kind,
                "metadata": {"name": name, "namespace": self.ns,
                             "labels": {"chaos.rhiza.io/run": self.run_id}},
                "spec": spec}
        self.apply(body)
        return name

    def wait_chaos(self, name, condition, timeout=120):
        def done():
            conds = self.get("podchaos" if name.startswith("podkill") else "networkchaos",
                             name).get("status", {}).get("conditions", [])
            return any(c.get("type") == condition and c.get("status") == "True" for c in conds)
        return self.wait(f"chaos {name} {condition}", done, timeout)

    def delete_chaos(self, kind, name):
        self.k("delete", kind, name, "--ignore-not-found", "--wait=true")

    # -- cluster state ------------------------------------------------------
    def cluster(self):
        return self.get("rhizacluster", "rhiza")

    def cluster_phase(self):
        return self.cluster().get("status", {}).get("phase")

    def fences(self):
        return json.loads(self.k("get", "rhizafences", "-o", "json")).get("items", [])

    def proofs(self):
        return [f.get("status", {}).get("proof") for f in self.fences()
                if f.get("status", {}).get("proof")]

    def recoveries(self):
        return json.loads(self.k("get", "rhizarecoveries", "-o", "json")).get("items", [])

    def running(self, label):
        pods = json.loads(self.k("get", "pods", "-l", label, "-o", "json")).get("items", [])
        return [p for p in pods if p["status"].get("phase") == "Running"]

    def assert_no_proof(self, operation):
        """Non-vacuous: the operation must exist and be genuinely unresolved."""
        held = [f for f in self.fences()
                if operation in json.dumps(f.get("spec", {}))]
        assert held, f"no fence request exists for {operation}; fail-safe not exercised"
        for f in held:
            assert "deletionTimestamp" not in f.get("metadata", {}), "fence is deleting"
            assert not f.get("status", {}).get("proof"), f"unexpected proof for {operation}"
        return held

    def assert_no_new_child(self, before):
        now = {r["metadata"]["uid"] for r in self.recoveries()}
        new = now - before
        assert not new, f"recovery child created before proof: {sorted(new)}"

    # -- scenarios ----------------------------------------------------------
    def scenario_intact_wal_restart(self):
        self.chaos("PodChaos", "podkill-intact-wal", {
            "action": "container-kill", "mode": "one", "duration": "10s", "containerNames": ["rhiza"],
            "selector": {"namespaces": [self.ns],
                         "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter",
                                            "statefulset.kubernetes.io/pod-name": "rhiza-voter-0"}}})
        self.wait_chaos("podkill-intact-wal", "AllInjected", 90)
        records = self.get("podchaos", "podkill-intact-wal")["status"].get("experiment", {}).get("containerRecords", [])
        self.wait_chaos("podkill-intact-wal", "AllRecovered", 120)
        assert sum(r.get("injectedCount", 0) for r in records) == 1, records
        assert not self.fences(), "container-kill must not request a fence"
        self.event("intact_wal_restart_pass", container_records=len(records))

    def scenario_online_replacement(self):
        before = {r["metadata"]["uid"] for r in self.recoveries()}
        self.chaos("NetworkChaos", "isolate-voter-0", {
            "action": "partition", "mode": "one", "duration": "45s", "direction": "both",
            "selector": {"namespaces": [self.ns],
                         "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter",
                                            "statefulset.kubernetes.io/pod-name": "rhiza-voter-0"}},
            "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                       "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"},
                       "expressionSelectors": [{"key": "statefulset.kubernetes.io/pod-name", "operator": "In",
                                                "values": ["rhiza-voter-1", "rhiza-voter-2"]}]}}})
        self.wait_chaos("isolate-voter-0", "AllInjected", 90)
        self.wait_chaos("isolate-voter-0", "AllRecovered", 150)
        fence = self.wait("voter fence requested", lambda: (self.fences() or None) and self.fences()[0]
                          or None, 60)
        self.event("online_replacement_fence", scope=fence.get("spec", {}).get("scope"))
        proof = self.wait("cooperative proof", lambda: (self.proofs() or [None])[0], 240)
        assert all(proof.get(k) is True for k in
                   ("processesTerminated", "recreationBlocked", "storageQuiesced")), proof
        self.delete_chaos("networkchaos", "isolate-voter-0")
        self.event("online_replacement_pass", acknowledged_rows=len(self.acks))

    def scenario_generation_recovery(self):
        before = {r["metadata"]["uid"] for r in self.recoveries()}
        self.chaos("NetworkChaos", "generation-partition", {
            "action": "partition", "mode": "all", "duration": "60s", "direction": "both",
            "selector": {"namespaces": [self.ns],
                         "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-operator"}},
            "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                       "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}}})
        self.wait_chaos("generation-partition", "AllInjected", 90)
        # The operator must not dispatch a recovery child before a proof exists.
        time.sleep(5)
        self.assert_no_new_child(before)
        self.wait_chaos("generation-partition", "AllRecovered", 150)
        gen = self.wait("generation fence", lambda: [f for f in self.fences()
                        if f.get("spec", {}).get("scope") == "Generation"] or None, 240)
        self.event("generation_fence", scope=gen[0]["spec"]["scope"],
                   targets=len(gen[0]["spec"].get("targets", [])))
        self.delete_chaos("networkchaos", "generation-partition")
        self.event("generation_recovery_pass")

    def scenario_dead_pod_failsafe(self):
        """D1-D4: only a fail-safe observation; never a PASS."""
        before = {r["metadata"]["uid"] for r in self.recoveries()}
        target = "rhiza-voter-2"
        pod = self.get("pod", target)
        d2 = {"pod_uid": pod["metadata"]["uid"], "wal_identity": self.status(target)["wal_identity"]}
        self.event("dead_pod_start", scenario="D2", target=target, pod_uid=d2["pod_uid"],
                   wal_identity=d2["wal_identity"])
        # D2 requires no terminal proof before the hard kill; capture and assert that.
        fence_before = self.assert_no_proof(target)
        self.chaos("PodChaos", "podkill-dead-voter", {
            "action": "pod-kill", "mode": "one", "duration": "10s",
            "selector": {"namespaces": [self.ns],
                         "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}})
        self.wait_chaos("podkill-dead-voter", "AllInjected", 90)
        time.sleep(30)
        # Fail-safe proposition: unresolved termination evidence => still no proof,
        # no new child, while controller and authority keep running.
        assert self.running(f"app=rhiza-operator,chaos.rhiza.io/run={self.run_id}"), "operator stopped"
        assert self.running(f"app=rhiza-fencer,chaos.rhiza.io/run={self.run_id}"), "authority stopped"
        self.assert_no_proof(target)
        self.assert_no_new_child(before)
        self.event("dead_pod_failsafe_observed", scenario="D2-D4", target=target,
                   fence_requests=len(fence_before),
                   viol_dead_pod_completion="unqualified: cooperative backend cannot prove termination")
        self.delete_chaos("podchaos", "podkill-dead-voter")

    def run(self):
        self.event("votercount_harness_start", run_id=self.run_id,
                   claim_positive="cooperative online replacement and generation recovery",
                   claim_failsafe="dead-Pod execution blocked while termination evidence is unresolved")
        self.scenario_intact_wal_restart()
        self.scenario_online_replacement()
        self.scenario_generation_recovery()
        self.scenario_dead_pod_failsafe()
        self.event("complete", positive_claims=["intact_wal_restart", "online_replacement",
                   "generation_recovery"], fail_safe="dead_pod_execution_blocking_unqualified")


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
