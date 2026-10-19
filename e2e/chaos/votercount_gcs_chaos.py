#!/usr/bin/env python3
"""voterCount GCS + Chaos Mesh qualification driver.

Reuses operator_recovery.Chaos (JSON REST helpers, port-forwards, evidence
events) and adds Chaos Mesh CRs, fence matching bound to the fault operation,
completed-recovery waits, a real writer and admission proof.

Claims stay separate and are never merged:
  (i)  positive: cooperative online replacement and generation recovery;
  (ii) fail-safe: dead-Pod execution blocking while termination evidence is
       unresolved (unqualified observations, never PASS).

The positive set and the dead-Pod set run as separate passes: the fences the
positive set proves are removed (run-owned namespace) so D1-D4 each start from
their own precondition.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from operator_recovery import Chaos

VOTERS = ("rhiza-voter-0", "rhiza-voter-1", "rhiza-voter-2")
COMPLETE = ("Complete",)
HEALTHY = ("Healthy", "Observing")


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
        self.row_seq = 0
        self.cleanup_errors = []
        self.admin = args.admin_token or os.environ.get("RHIZA_QUALIFICATION_ADMIN_TOKEN", "")
        self.cluster_scoped = [("%s-fencer-tokenreview" % args.namespace, "clusterrolebinding"),
                               ("%s-fencer-tokenreview" % args.namespace, "clusterrole")]
        self.declared = {"namespace": args.namespace, "run_id": args.run_id,
                         "app_image": args.app_image, "operator_image": args.operator_image,
                         "chaos_created": []}

    # ---- kubectl ----------------------------------------------------------
    def apply_json(self, body, extra=None):
        argv = ["kubectl", "--context", self.context, "-n", self.ns, "apply"] + (extra or []) + ["-f", "-"]
        return subprocess.run(argv, input=json.dumps(body), text=True, capture_output=True, timeout=240)

    # ---- Chaos Mesh -------------------------------------------------------
    def chaos_body(self, kind, name, spec):
        return {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": kind,
                "metadata": {"name": name, "namespace": self.ns,
                             "labels": {"chaos.rhiza.io/run": self.run_id}}, "spec": spec}

    def create_chaos(self, kind, name, spec):
        result = self.apply_json(self.chaos_body(kind, name, spec))
        if result.returncode:
            raise RuntimeError("chaos apply failed: %s" % result.stderr[-400:])
        self.declared["chaos_created"].append([kind, name])
        return name

    def wait_chaos(self, kind, name, condition, timeout=120):
        def done():
            conds = self.get(kind.lower(), name).get("status", {}).get("conditions", [])
            return any(c.get("type") == condition and c.get("status") == "True" for c in conds)
        return self.wait("%s %s %s" % (kind, name, condition), done, timeout)

    def experiment(self, kind, name):
        return self.get(kind.lower(), name).get("status", {}).get("experiment", {})

    def delete_chaos(self, kind, name):
        self.k("delete", kind.lower(), name, "--ignore-not-found", "--wait=true")

    def prove_admission(self):
        """Real admission evaluation: server-side dry-run accept and reject."""
        allowed = [
            ("PodChaos", "vc-prove-container-kill", {"action": "container-kill", "mode": "one",
             "duration": "10s", "containerNames": ["rhiza"], "selector": self.voter_selector(VOTERS[0])}),
            ("PodChaos", "vc-prove-pod-kill", {"action": "pod-kill", "mode": "one", "duration": "10s",
             "selector": self.voter_selector(VOTERS[2])}),
            ("NetworkChaos", "vc-prove-isolate", self.isolate_spec()),
            ("NetworkChaos", "vc-prove-generation", self.generation_spec()),
        ]
        rejected = [
            ("NetworkChaos", "vc-prove-forbidden", {"action": "partition", "mode": "all", "duration": "60s",
             "direction": "both", "selector": self.voter_selector(VOTERS[0]),
             "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                        "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}}}),
            ("PodChaos", "vc-prove-forbidden", {"action": "pod-kill", "mode": "all", "duration": "10s",
             "selector": {"namespaces": [self.ns],
                          "labelSelectors": {"chaos.rhiza.io/run": "other-run", "app": "rhiza-voter"}}}),
        ]
        evidence = {"accepted": [], "rejected": []}
        for kind, name, spec in allowed:
            result = self.apply_json(self.chaos_body(kind, name, spec), ["--dry-run=server"])
            evidence["accepted"].append([kind, name, result.returncode])
            assert result.returncode == 0, "intended shape rejected: %s %s %s" % (name, kind, result.stderr[-300:])
        for kind, name, spec in rejected:
            result = self.apply_json(self.chaos_body(kind, name, spec), ["--dry-run=server"])
            evidence["rejected"].append([kind, name, result.returncode])
            assert result.returncode != 0, "forbidden shape was admitted: %s %s" % (name, kind)
        self.event("admission_proof_pass", **evidence)
        return True

    # ---- selectors --------------------------------------------------------
    def voter_selector(self, pod):
        return {"namespaces": [self.ns], "labelSelectors": {
            "chaos.rhiza.io/run": self.run_id, "app.kubernetes.io/part-of": "rhiza-chaos",
            "statefulset.kubernetes.io/pod-name": pod}}

    def isolate_spec(self):
        return {"action": "partition", "mode": "one", "duration": "45s", "direction": "both",
                "selector": self.voter_selector(VOTERS[0]),
                "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                    "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"},
                    "expressionSelectors": [{"key": "statefulset.kubernetes.io/pod-name", "operator": "In",
                                             "values": [VOTERS[1], VOTERS[2]]}]}}}

    def generation_spec(self):
        return {"action": "partition", "mode": "all", "duration": "60s", "direction": "both",
                "selector": {"namespaces": [self.ns], "labelSelectors": {
                    "chaos.rhiza.io/run": self.run_id, "app": "rhiza-operator"}},
                "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                    "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}}}

    # ---- state ------------------------------------------------------------
    def fences(self):
        return json.loads(self.k("get", "rhizafences", "-o", "json")).get("items", [])

    def recoveries(self):
        return json.loads(self.k("get", "rhizarecoveries", "-o", "json")).get("items", [])

    def recovery_uids(self):
        return {r["metadata"]["uid"] for r in self.recoveries()}

    def fence_uids(self):
        return {f["metadata"]["uid"] for f in self.fences()}

    def cluster(self):
        return self.get("rhizacluster", "rhiza")

    def cluster_phase(self):
        return self.cluster().get("status", {}).get("phase")

    def cluster_revision(self):
        return self.cluster().get("metadata", {}).get("resourceVersion")

    def pod(self, name):
        return self.get("pod", name)

    def pod_identity(self, name):
        state = self.status(name)
        return {"node_id": name, "pod_uid": self.pod(name)["metadata"]["uid"],
                "wal_identity": state["wal_identity"], "config_id": state["config_id"]}

    def reset_fences(self):
        """Run-owned namespace: clear the positive pass so D1-D4 start clean."""
        self.k("delete", "rhizafences", "--all", "--ignore-not-found", "--wait=true")
        self.wait("fences cleared", lambda: not self.fences(), 120)
        return True

    def fence_for(self, ident, operation=None, new_since=None):
        """Structural match on all four identity fields, never a substring scan."""
        hits = []
        for fence in self.fences():
            if new_since is not None and fence["metadata"]["uid"] in new_since:
                continue
            spec = fence.get("spec", {})
            if operation and spec.get("operationID") != operation:
                continue
            for target in spec.get("targets", []):
                if (target.get("nodeID") == ident["node_id"] and target.get("pod") == ident["node_id"]
                        and target.get("podUID") == ident["pod_uid"]
                        and target.get("walIdentity") == ident["wal_identity"]):
                    hits.append(fence)
                    break
        return hits

    # ---- assertions -------------------------------------------------------
    def assert_controllers_running(self):
        for label in ("app=rhiza-operator", "app=rhiza-fencer"):
            assert self.pods_with(label), "%s is not running; a negative result is not evidence" % label
        return True

    def pods_with(self, selector):
        pods = json.loads(self.k("get", "pods", "-l",
                                 "%s,chaos.rhiza.io/run=%s" % (selector, self.run_id),
                                 "-o", "json")).get("items", [])
        return [p for p in pods if p["status"].get("phase") == "Running"]

    def assert_cluster_observed(self, baseline=None, window=45):
        """Require observation of the CURRENT generation, or a post-baseline change."""
        current = self.cluster()
        meta, status = current.get("metadata", {}), current.get("status", {})
        generation, observed = meta.get("generation"), status.get("observedGeneration")
        if generation is not None and observed is not None and baseline is None:
            assert observed == generation, "controller has not observed generation %s" % generation
            return observed
        deadline = time.time() + window
        while time.time() < deadline:
            if self.cluster_revision() != baseline:
                return self.cluster_revision()
            time.sleep(2)
        raise AssertionError("no post-baseline observation change; a negative result is not evidence")

    def assert_deployed_images(self):
        pinned = {"voter": self.args.app_image, "operator": self.args.operator_image,
                  "fencer": self.args.operator_image}
        pods = {"voter": [self.pod(name) for name in VOTERS],
                "operator": self.pods_with("app=rhiza-operator"),
                "fencer": self.pods_with("app=rhiza-fencer")}
        for role, items in pods.items():
            assert items, "no %s Pods to verify" % role
            digest = pinned[role].split("@")[-1] if "@" in pinned[role] else ""
            assert digest, "%s image must be digest-pinned" % role
            for item in items:
                image_id = item["status"]["containerStatuses"][0]["imageID"]
                assert digest in image_id, "%s image differs: %s !~ %s" % (role, image_id, digest)
        return True

    def assert_no_proof_structural(self, ident, new_since=None):
        held = self.fence_for(ident, new_since=new_since)
        assert held, "no structurally bound fence request for %s" % ident["node_id"]
        for fence in held:
            assert "deletionTimestamp" not in fence.get("metadata", {}), "fence is deleting"
            assert not fence.get("status", {}).get("proof"), "unexpected proof for %s" % ident["node_id"]
        return held

    def assert_no_new_child(self, before):
        new = self.recovery_uids() - before
        assert not new, "recovery child created without a proof: %s" % sorted(new)
        return True

    @staticmethod
    def child_operation(child):
        """The child binds to the fence operation in an exact spec field."""
        spec = child.get("spec", {})
        membership = spec.get("membership") or {}
        return [v for v in (spec.get("operationID"), spec.get("recoveryID"),
                            membership.get("operationID")) if v]

    def wait_completion(self, before, fence=None, timeout=420):
        operation = (fence or {}).get("spec", {}).get("operationID")
        def done():
            for child in self.recoveries():
                if child["metadata"]["uid"] in before:
                    continue
                status = child.get("status", {})
                phase = status.get("membership", {}).get("phase") or status.get("phase")
                if phase not in COMPLETE:
                    continue
                if operation and operation not in self.child_operation(child):
                    continue
                if fence and not operation:
                    continue
                return child
            return None
        return self.wait("completed recovery child bound to the fence operation", done, timeout)

    # ---- workload ---------------------------------------------------------
    def next_row(self):
        self.row_seq += 1
        return 100000 + self.row_seq

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

    def readback(self, pod, tag):
        before = self.verify_data(pod)
        row = self.next_row()
        self.acks.add(row)
        self.execute(pod, "vc-post-%s" % tag, "INSERT INTO chaos_items VALUES (%d, 'post')" % row)
        after = self.verify_data(pod)
        assert after >= before + 1, "scenario write was not readable afterwards: %s" % tag
        self.event("readback_pass", pod=pod, tag=tag, row=row, before=before, after=after)

    # ---- scenarios --------------------------------------------------------
    def chaos_injected(self, kind, name, expect_records=None):
        self.wait_chaos(kind, name, "AllInjected", 120)
        if expect_records is not None:
            records = self.experiment(kind, name).get("containerRecords", [])
            assert sum(r.get("injectedCount", 0) for r in records) == expect_records, records
        return True

    def scenario_intact_wal_restart(self):
        before = self.recovery_uids()
        ident = self.pod_identity(VOTERS[0])
        self.create_chaos("PodChaos", "vc-intact-wal", {
            "action": "container-kill", "mode": "one", "duration": "10s", "containerNames": ["rhiza"],
            "selector": self.voter_selector(VOTERS[0])})
        self.chaos_injected("PodChaos", "vc-intact-wal", 1)
        self.wait_chaos("PodChaos", "vc-intact-wal", "AllRecovered", 150)
        after = self.wait("voter ready again", lambda: self.pod_identity(VOTERS[0]), 240)
        assert after["pod_uid"] == ident["pod_uid"], "container-kill replaced the Pod"
        assert after["wal_identity"] == ident["wal_identity"], "WAL identity drifted"
        self.assert_no_new_child(before)
        assert not self.fence_for(ident), "container-kill must not request a fence"
        self.delete_chaos("PodChaos", "vc-intact-wal")
        self.readback(VOTERS[0], "intact-wal")
        self.event("intact_wal_restart_pass", pod_uid=ident["pod_uid"])

    def scenario_online_replacement(self):
        before = self.recovery_uids()
        pre = self.fence_uids()
        ident = self.pod_identity(VOTERS[0])
        self.create_chaos("NetworkChaos", "vc-isolate-voter-0", self.isolate_spec())
        self.chaos_injected("NetworkChaos", "vc-isolate-voter-0")
        self.assert_controllers_running()
        self.wait_chaos("NetworkChaos", "vc-isolate-voter-0", "AllRecovered", 180)
        fence = self.wait("new voter fence", lambda: (self.fence_for(ident, new_since=pre) or None), 180)[0]
        self.wait("voter fence proof", lambda: self.get("rhizafences", fence["metadata"]["name"])
                  .get("status", {}).get("proof"), 300)
        child = self.wait_completion(before, fence)
        self.event("online_replacement_completed", child=child["metadata"]["name"],
                   operation=fence["spec"].get("operationID"))
        self.delete_chaos("NetworkChaos", "vc-isolate-voter-0")
        self.readback(VOTERS[1], "online-replacement")
        self.event("online_replacement_pass")

    def scenario_generation_recovery(self):
        before = self.recovery_uids()
        pre = self.fence_uids()
        self.create_chaos("NetworkChaos", "vc-generation", self.generation_spec())
        self.chaos_injected("NetworkChaos", "vc-generation")
        self.assert_controllers_running()
        self.assert_no_new_child(before)
        self.wait_chaos("NetworkChaos", "vc-generation", "AllRecovered", 180)
        def new_generation_fence():
            return [f for f in self.fences() if f["metadata"]["uid"] not in pre
                    and f.get("spec", {}).get("scope") == "Generation"] or None
        fence = self.wait("new generation fence", new_generation_fence, 240)[0]
        self.wait("generation proof", lambda: self.get("rhizafences", fence["metadata"]["name"])
                  .get("status", {}).get("proof"), 300)
        child = self.wait_completion(before, fence)
        self.event("generation_recovery_completed", child=child["metadata"]["name"],
                   operation=fence["spec"].get("operationID"),
                   targets=len(fence["spec"].get("targets", [])))
        self.delete_chaos("NetworkChaos", "vc-generation")
        self.readback(VOTERS[2], "generation")
        self.event("generation_recovery_pass")

    # ---- Part 2 (DEFERRED): dead-Pod fail-safe D1-D4, never a PASS --------
    # Deferred until the production fencing executor project lands: the
    # cooperative backend cannot prove termination for a dead Pod, so these are
    # unqualified observations and are not part of the default positive run.
    def dead_pod_case(self, scenario, target):
        before = self.recovery_uids()
        pre = self.fence_uids()
        ident = self.pod_identity(target)
        baseline = self.cluster_revision()
        start = {"scenario": scenario, "target": target, "pod_uid": ident["pod_uid"],
                 "wal_identity": ident["wal_identity"], "cluster_phase": self.cluster_phase(),
                 "fence_uids": sorted(pre), "recovery_uids": sorted(before)}
        if scenario == "D1":
            assert ident["wal_identity"], "D1 requires an already WAL-bound target"
            first = self.status(target)
            time.sleep(5)
            second = self.status(target)
            assert first["wal_identity"] == second["wal_identity"] == ident["wal_identity"], \
                "D1 requires no clean drain/exit before the kill"
            start["drain_samples"] = [first["wal_identity"], second["wal_identity"]]
        if scenario == "D2":
            assert not [f for f in self.fences() if f.get("status", {}).get("proof")], \
                "D2 requires no terminal proof"
            assert ident["pod_uid"], "D2 requires a captured Pod UID"
        if scenario == "D3":
            assert ident["pod_uid"] and ident["wal_identity"], "D3 requires the full identity"
            start["child_authorization"] = sorted(before)
            start["learner_binding"] = [p["metadata"]["name"] for p in self.pods_with("app=rhiza-voter")
                                        if "learner" in p["metadata"]["name"]]
        if scenario == "D4":
            assert self.cluster_phase() in HEALTHY, start["cluster_phase"]
            assert not self.fences(), "D4 requires no in-flight intent"
            assert not self.recoveries(), "D4 requires no in-flight recovery"
        self.event("dead_pod_start", **start)
        self.create_chaos("PodChaos", "vc-dead-" + scenario, {
            "action": "pod-kill", "mode": "one", "duration": "10s",
            "selector": self.voter_selector(target)})
        self.chaos_injected("PodChaos", "vc-dead-" + scenario)
        self.wait("incarnation is not running", lambda: self.pod(target).get("status", {})
                  .get("phase") != "Running", 120)
        time.sleep(20)
        self.assert_controllers_running()
        self.assert_cluster_observed(baseline=baseline)
        records = self.experiment("PodChaos", "vc-dead-" + scenario).get("podRecords", [])
        assert records and target in json.dumps(records), \
            "Chaos Mesh killed a different Pod: %s" % records
        current_uid = self.pod(target).get("metadata", {}).get("uid")
        assert current_uid != ident["pod_uid"], \
            "the captured incarnation %s survived: a replacement was killed instead" % ident["pod_uid"]
        if scenario == "D3":
            assert sorted(self.recovery_uids()) == start["recovery_uids"], \
                "D3: child authorization changed during the fault"
        held = self.fence_for(ident, new_since=pre)
        assert held, "no new fence tied to the killed incarnation %s" % target
        for fence in held:
            assert not fence.get("status", {}).get("proof"), "unexpected proof for %s" % target
        for fence in self.fences():
            if fence["metadata"]["uid"] in pre:
                assert not fence.get("status", {}).get("proof"), "a pre-existing fence gained a proof"
        self.assert_no_new_child(before)
        self.event("dead_pod_failsafe_observed", scenario=scenario, target=target,
                   fence_requests=len(held), pod_records=len(records),
                   completion="unqualified: cooperative backend cannot prove termination for a dead Pod")
        self.delete_chaos("PodChaos", "vc-dead-" + scenario)

    def assert_bootstrap(self):
        """Part 1: the bootstrap opt-in must have derived the voterCount voters."""
        cluster = self.cluster()["spec"]
        sts = self.get("statefulset", "rhiza-voter")["spec"]
        assert sts["replicas"] == cluster["voterCount"] == 3, (sts["replicas"], cluster["voterCount"])
        env = sts["template"]["spec"]["containers"][0].get("env", [])
        members = [e for e in env if e.get("name") == "RHIZA_CLUSTER_MEMBERS"]
        assert members and "secretKeyRef" in members[0].get("valueFrom", {}), \
            "operator did not install the derived membership on the bootstrap path"
        self.event("bootstrap_opt_in_pass", replicas=sts["replicas"], voter_count=cluster["voterCount"])

    def run_positive(self):
        self.assert_bootstrap()
        self.scenario_intact_wal_restart()
        self.scenario_online_replacement()
        self.scenario_generation_recovery()
        self.reset_fences()
        self.event("positive_set_pass", acknowledged=len(self.acks))

    def run_dead_pod(self):
        for scenario, target in (("D1", VOTERS[2]), ("D2", VOTERS[1]),
                                 ("D3", VOTERS[0]), ("D4", VOTERS[2])):
            self.dead_pod_case(scenario, target)

    def run(self):
        scenarios = self.args.scenarios
        self.event("start", run_id=self.run_id, scenarios=scenarios,
                   claim_positive="cooperative online replacement and generation recovery",
                   claim_failsafe="dead-Pod execution blocked while termination evidence is unresolved")
        self.prove_admission()
        self.prepare()
        if scenarios in ("positive", "all"):
            self.run_positive()
        if scenarios in ("dead-pod", "all"):
            # DEFERRED: Part 2 runs only after the production fencing executor
            # lands (separate implementation project). Until then the dead-Pod
            # claim is unqualified and this set is never executed by default.
            self.run_dead_pod()
        self.event("complete", scenarios=scenarios,
                   positive=["intact_wal_restart", "online_replacement", "generation_recovery"]
                   if scenarios in ("positive", "all") else [],
                   fail_safe=("dead_pod_execution_blocking_unqualified"
                              if scenarios in ("dead-pod", "all") else "not_run"))

    # ---- cleanup ----------------------------------------------------------
    def delete_cluster_scoped(self):
        for name, kind in self.cluster_scoped:
            self.command(["kubectl", "--context", self.context, "delete", kind, name,
                          "--ignore-not-found", "--wait=true"])

    def verify_absence(self):
        remaining = []
        for name, kind in self.cluster_scoped:
            out = self.command(["kubectl", "--context", self.context, "get", kind, name,
                                "--ignore-not-found", "-o", "name"])
            if out.strip():
                remaining.append("%s/%s" % (kind, name))
        if remaining:
            raise AssertionError("run-owned cluster-scoped objects remain: %s" % remaining)
        return True

    def cleanup(self):
        self.stop.set()
        if self.writer:
            self.writer.join(timeout=15)
        for kind, name in list(self.declared.get("chaos_created", [])):
            try:
                self.delete_chaos(kind, name)
            except Exception as exc:
                self.cleanup_errors.append("chaos %s/%s: %s" % (kind, name, exc))
        try:
            self.delete_cluster_scoped()
            self.verify_absence()
        except Exception as exc:
            self.cleanup_errors.append(str(exc))
        try:
            (self.out / "rollback-material.json").write_text(json.dumps({
                "namespace": self.declared["namespace"], "run_id": self.declared["run_id"],
                "cluster_scoped": [list(x) for x in self.cluster_scoped],
                "cleanup_errors": self.cleanup_errors}, indent=2))
        except Exception:
            pass
        Chaos.cleanup(self)
        if self.cleanup_errors:
            raise AssertionError("cleanup incomplete: %s" % self.cleanup_errors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--app-image", required=True)
    parser.add_argument("--operator-image", required=True)
    parser.add_argument("--admin-token", default="")
    parser.add_argument("--output", default="votercount-gcs-chaos-results")
    parser.add_argument("--scenarios", default="positive", choices=("positive", "dead-pod", "all"))
    chaos = VoterCountChaos(parser.parse_args())
    status = 0
    try:
        chaos.run()
    except Exception as exc:
        print("RUN FAILED: %s" % exc, file=sys.stderr)
        status = 1
    finally:
        try:
            chaos.cleanup()
        except Exception as exc:
            print("CLEANUP FAILED: %s" % exc, file=sys.stderr)
            status = 1
    sys.exit(status)


if __name__ == "__main__":
    main()
