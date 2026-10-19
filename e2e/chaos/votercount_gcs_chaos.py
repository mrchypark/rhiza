#!/usr/bin/env python3
"""voterCount GCS + Chaos Mesh qualification driver.

Reuses operator_recovery.Chaos (JSON REST helpers, port-forwards, evidence
events) and adds Chaos Mesh CRs, fence matching bound to the fault operation,
completed-recovery waits, a real writer and admission proof.

Claims stay separate and are never merged:
  (i)  positive: cooperative online replacement, generation recovery and the
       approved paired NetworkPolicy isolation (the deny-all inspector is
       denied, its derived control Pod is reachable);
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
INSPECTOR = "rhiza-inspector"
INSPECTOR_CONTROL = "rhiza-inspector-control"
INSPECTOR_POLICY = "rhiza-inspector-deny-all"
# Receipts kept identical to run-gcs-postrelease.sh: an unselected Pod reaches
# the API, the selected inspector is denied (never a fast refusal), and the
# control still reaches it afterwards.
CONTROL_EGRESS_PROBE = """\
busybox nc -w 3 "$1" "$2" </dev/null >/dev/null 2>&1 || exit 43
printf "paired-network-policy-proof=control-reachable phase=%s target=kubernetes-api host=%s port=%s\n" "$3" "$1" "$2"
"""
INSPECTOR_EGRESS_PROBE = """\
started=$(date +%s)
if busybox nc -w 3 "$1" "$2" </dev/null >/dev/null 2>&1; then exit 41; fi
elapsed=$(($(date +%s)-started))
[ "$elapsed" -ge 2 ] || exit 42
printf "paired-network-policy-proof=inspector-denied elapsed_seconds=%s target=kubernetes-api host=%s port=%s\n" "$elapsed" "$1" "$2"
"""


def parse_https_443(server_url):
    """Exact HTTPS Kubernetes API endpoint on port 443, or reject."""
    error = "exact HTTPS Kubernetes API endpoint on port 443 required for isolation probe"
    if not server_url.startswith("https://"):
        raise ValueError(error)
    authority = server_url[len("https://"):].rstrip("/")
    if authority.startswith("["):
        host, _, tail = authority[1:].partition("]")
        if not host or tail not in ("", ":443"):
            raise ValueError(error)
        return host, "443"
    host, _, port = authority.rpartition(":")
    if not host:  # no port at all: the whole authority is the host
        host, port = authority, "443"
    if not host or port != "443" or any(char in host for char in "[]:/"):
        raise ValueError(error)
    return host, "443"


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
            ("NetworkChaos", "vc-prove-isolate", self.isolate_spec()),
            ("NetworkChaos", "vc-prove-generation", self.generation_spec()),
        ]
        rejected = [
            ("NetworkChaos", "vc-prove-forbidden", {"action": "partition", "mode": "all", "duration": "60s",
             "direction": "both", "selector": self.voter_selector(VOTERS[0]),
             "target": {"mode": "all", "selector": {"namespaces": [self.ns],
                        "labelSelectors": {"chaos.rhiza.io/run": self.run_id, "app": "rhiza-voter"}}}}),
        ]
        if self.args.scenarios in ("dead-pod", "all"):
            rejected.append(("PodChaos", "vc-prove-forbidden", {"action": "pod-kill", "mode": "all", "duration": "10s",
                             "selector": {"namespaces": [self.ns],
                                          "labelSelectors": {"chaos.rhiza.io/run": "other-run", "app": "rhiza-voter"}}}))
        if self.args.scenarios in ("dead-pod", "all"):
            allowed.append(("PodChaos", "vc-prove-pod-kill", {"action": "pod-kill", "mode": "one",
                           "duration": "10s", "selector": self.voter_selector(VOTERS[2])}))
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

    # ---- paired network isolation ----------------------------------------
    def isolation_mode(self):
        """Visibility contract: isolation is required unless the user approved an exclusion."""
        mode = getattr(self.args, "inspector_network_isolation", "required")
        assert mode in ("required", "exclude-user-approved"), \
            "RHIZA_INSPECTOR_NETWORK_ISOLATION must be required or exclude-user-approved"
        return mode

    def api_endpoint(self):
        server = os.environ.get("RHIZA_LOCAL_API_SERVER") or self.command(
            ["kubectl", "--context", self.context, "config", "view", "--minify",
             "-o", "jsonpath={.clusters[0].cluster.server}"])
        return parse_https_443(server.strip())

    def running_probe_pod(self, name):
        try:
            pod = self.get("pod", name)
        except RuntimeError:
            return None
        if pod.get("status", {}).get("phase") != "Running":
            return None
        statuses = pod["status"].get("containerStatuses") or []
        return pod if statuses and statuses[0].get("ready") else None

    def capture_probe_pods(self):
        """Create the run-owned probe pair just-in-time and keep its receipts."""
        created = json.loads(self.command(["kubectl", "--context", self.context, "-n", self.ns,
                                           "create", "-f", self.args.inspector_probe, "-o", "json"]))
        items = created["items"] if created.get("kind") == "List" else [created]
        receipts = {item["metadata"]["name"]: item for item in items}
        assert sorted(receipts) == sorted((INSPECTOR, INSPECTOR_CONTROL)), sorted(receipts)
        for name, pod in receipts.items():
            (self.out / ("%s-created.json" % name)).write_text(json.dumps(pod, indent=2))
        for name in (INSPECTOR, INSPECTOR_CONTROL):
            self.wait("probe Pod %s running" % name, lambda n=name: self.running_probe_pod(n), 240)
        return receipts

    def validate_probe_pod(self, probe, receipt, name, app, container):
        """Port of run-gcs-postrelease.sh validate_probe_pod: the denied inspector and
        the reachable control must keep the one prescribed shape."""
        metadata, spec, status = probe["metadata"], probe["spec"], probe["status"]
        labels = metadata["labels"]
        assert metadata["name"] == name and metadata["namespace"] == self.ns, metadata["name"]
        assert metadata["uid"] == receipt["metadata"]["uid"], "probe Pod identity changed"
        assert (metadata.get("ownerReferences") or []) == [], metadata.get("ownerReferences")
        assert labels["app"] == app and labels["chaos.rhiza.io/run"] == self.run_id, labels
        # A bound Pod may carry the node's topology labels on top of its own.
        assert sorted(labels) in (["app", "chaos.rhiza.io/run"],
                                  ["app", "chaos.rhiza.io/run",
                                   "topology.kubernetes.io/region", "topology.kubernetes.io/zone"]), labels
        assert spec["serviceAccountName"] == INSPECTOR, spec["serviceAccountName"]
        assert spec.get("automountServiceAccountToken") is False, "probe token automount"
        assert not any(spec.get(key) for key in ("hostNetwork", "hostPID", "hostIPC")), "host namespaces"
        assert spec.get("nodeName"), "probe Pod must be bound to a node"
        assert spec["restartPolicy"] == "Never" and (spec.get("volumes") or []) == [], "probe Pod spec"
        sandbox = spec["securityContext"]
        assert (sandbox["runAsNonRoot"], sandbox["runAsUser"], sandbox["runAsGroup"], sandbox["fsGroup"]) \
            == (True, 65532, 65532, 65532), sandbox
        assert sandbox["seccompProfile"] == {"type": "RuntimeDefault"}, sandbox
        assert (spec.get("initContainers") or []) == [] and (spec.get("ephemeralContainers") or []) == []
        containers = spec["containers"]
        assert len(containers) == 1, [c["name"] for c in containers]
        live = containers[0]
        assert live["name"] == container and live["image"] == self.args.app_image, live["image"]
        assert live["command"] == ["sh", "-c", "sleep 1500"], live["command"]
        for empty in ("args", "env", "envFrom", "ports", "volumeMounts"):
            assert (live.get(empty) or []) == [], (empty, live.get(empty))
        for absent in ("readinessProbe", "livenessProbe", "startupProbe", "lifecycle"):
            assert live.get(absent) is None, absent
        assert not live.get("workingDir") and not any(live.get(k) for k in ("stdin", "stdinOnce", "tty"))
        runtime = live["securityContext"]
        assert runtime["allowPrivilegeEscalation"] is False and runtime["readOnlyRootFilesystem"] is True
        assert runtime["runAsNonRoot"] is True and runtime["capabilities"]["drop"] == ["ALL"], runtime
        assert status["phase"] == "Running", status["phase"]
        states = status["containerStatuses"]
        assert len(states) == 1, [s["name"] for s in states]
        state = states[0]
        assert (state["name"], state["ready"], state["restartCount"]) == (container, True, 0), state
        assert isinstance(state["state"].get("running"), dict), state["state"]
        assert state["imageID"].endswith(self.args.app_image.split("@")[-1]), state["imageID"]
        return metadata["uid"]

    def validate_probe_pair(self, receipts):
        """Control-pod derivation contract (~lines 737-741): the control is the
        inspector with the name, app label and container name renamed, plus the
        placement constraint that pins both to one scheduler-selected node so no
        node-specific route or CNI fault can explain the denied probe."""
        inspector = self.get("pod", INSPECTOR)
        control = self.get("pod", INSPECTOR_CONTROL)
        inspector_uid = self.validate_probe_pod(inspector, receipts[INSPECTOR], INSPECTOR, INSPECTOR,
                                                "inspector")
        control_uid = self.validate_probe_pod(control, receipts[INSPECTOR_CONTROL], INSPECTOR_CONTROL,
                                              INSPECTOR_CONTROL, "control")
        node = inspector["spec"]["nodeName"]
        assert node == control["spec"]["nodeName"], \
            "inspector and control must share one node: %s != %s" % (node, control["spec"]["nodeName"])

        def shape(pod, container):
            spec = json.loads(json.dumps(pod["spec"]))
            # Placement is asserted equal above, so it is not a shape difference:
            # the control's co-location constraint is what makes it equal.
            for placement in ("nodeName", "affinity"):
                spec.pop(placement, None)
            spec["containers"][0]["name"] = container
            return spec

        assert shape(inspector, "<container>") == shape(control, "<container>"), \
            "control Pod is not the inspector Pod with only the derivation's deltas"
        inspector_labels = dict(inspector["metadata"]["labels"])
        control_labels = dict(control["metadata"]["labels"])
        assert inspector_labels.pop("app") == INSPECTOR, inspector_labels
        assert control_labels.pop("app") == INSPECTOR_CONTROL, control_labels
        assert inspector_labels == control_labels, control_labels
        return inspector_uid, control_uid, node

    def validate_deny_all_policy(self, canonical=None, receipt="inspector-networkpolicy-bootstrap.json"):
        """The bootstrap owns rhiza-inspector-deny-all; this run only observes it,
        because re-applying it would strip the bootstrap's auth-owner annotation."""
        policy = self.get("networkpolicy.networking.k8s.io", INSPECTOR_POLICY)
        metadata, spec = policy["metadata"], policy["spec"]
        selector = {"chaos.rhiza.io/run": self.run_id, "app": INSPECTOR}
        assert metadata["namespace"] == self.ns and (metadata.get("ownerReferences") or []) == []
        if canonical is not None:
            assert metadata["uid"] == canonical["metadata"]["uid"], "deny-all policy was replaced"
        assert metadata["labels"] == {"chaos.rhiza.io/run": self.run_id, "app": INSPECTOR}, metadata["labels"]
        assert spec["podSelector"] == {"matchLabels": selector}, spec["podSelector"]
        assert sorted(spec.get("policyTypes") or []) == ["Egress", "Ingress"], spec.get("policyTypes")
        assert not spec.get("ingress") and not spec.get("egress"), "deny-all must carry no rules"
        (self.out / receipt).write_text(json.dumps(policy, indent=2))
        return policy

    def validate_policy_inventory(self, canonical, receipt):
        """Source singleton receipt (~lines 1518-1524): the canonical deny-all must be
        the only NetworkPolicy, so no additive policy can explain the denial."""
        inventory = json.loads(self.k("get", "networkpolicies.networking.k8s.io", "-o", "json"))
        items = inventory.get("items") or []
        (self.out / receipt).write_text(json.dumps(inventory, indent=2))
        assert len(items) == 1 and items[0]["metadata"]["uid"] == canonical["metadata"]["uid"], \
            "unexpected additive NetworkPolicy present: %s" % sorted(i["metadata"]["name"] for i in items)
        return inventory

    def probe_egress(self, pod, script, *args):
        return self.command(["kubectl", "--context", self.context, "-n", self.ns, "exec", pod,
                             "--", "sh", "-c", script, "sh", *args])

    def release_probe_pods(self):
        """Terminal absence of the run-owned probe Pods; lingering Pods fail the run."""
        try:
            self.k("delete", "pod", INSPECTOR, INSPECTOR_CONTROL, "--ignore-not-found", "--wait=true")
            remaining = [item["metadata"]["name"] for item in
                         json.loads(self.k("get", "pods", "-o", "json"))["items"]
                         if item["metadata"]["name"] in (INSPECTOR, INSPECTOR_CONTROL)]
            assert not remaining, "probe Pods remain: %s" % remaining
        except Exception as exc:
            self.cleanup_errors.append("inspector probe release: %s" % exc)

    def verify_inspector_network_isolation(self):
        """Positive-claim gate: the deny-all policy, not the Pod shape, denies the
        inspector. Reachable control, denied inspector, reachable control again."""
        if self.isolation_mode() == "exclude-user-approved":
            receipt = "EXCLUDED inspector-network-isolation: user-approved"
            (self.out / "inspector-network-isolation.txt").write_text(receipt + "\n")
            self.event("inspector_network_isolation_excluded", receipt=receipt)
            return receipt
        try:
            host, port = self.api_endpoint()
            policy = self.validate_deny_all_policy()
            self.validate_policy_inventory(policy, "inspector-networkpolicies-bootstrap.json")
            receipts = self.capture_probe_pods()
            inspector_uid, control_uid, node = self.validate_probe_pair(receipts)
            control_before = self.probe_egress(INSPECTOR_CONTROL, CONTROL_EGRESS_PROBE, host, port, "before")
            denied = self.probe_egress(INSPECTOR, INSPECTOR_EGRESS_PROBE, host, port)
            control_after = self.probe_egress(INSPECTOR_CONTROL, CONTROL_EGRESS_PROBE, host, port, "after")
            for name, receipt in receipts.items():
                assert self.get("pod", name)["metadata"]["uid"] == receipt["metadata"]["uid"], \
                    "probe Pod %s was replaced during the isolation check" % name
            self.validate_deny_all_policy(policy, "inspector-terminal-networkpolicy.json")
            self.validate_policy_inventory(policy, "inspector-networkpolicies-terminal.json")
            receipt = "PASS inspector-network-isolation"
            (self.out / "inspector-network-isolation.txt").write_text(receipt + "\n")
            for label, output in (("control-before-reachable", control_before),
                                  ("egress-denied", denied),
                                  ("control-after-reachable", control_after)):
                (self.out / ("inspector-%s.txt" % label)).write_text(output)
            self.event("inspector_network_isolation_pass", target_host=host, target_port=port,
                       node=node, inspector_uid=inspector_uid, control_uid=control_uid,
                       control_before=control_before.strip(), inspector_denied=denied.strip(),
                       control_after=control_after.strip())
        except Exception as exc:
            self.event("inspector_network_isolation_fail", error=str(exc)[-300:])
            raise
        finally:
            self.release_probe_pods()
        return receipt

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
        self.verify_inspector_network_isolation()
        self.scenario_intact_wal_restart()
        self.scenario_online_replacement()
        self.scenario_generation_recovery()
        receipt = (self.out / "inspector-network-isolation.txt").read_text().strip()
        assert receipt in ("PASS inspector-network-isolation",
                           "EXCLUDED inspector-network-isolation: user-approved"), receipt
        self.event("positive_set_pass", acknowledged=len(self.acks), inspector_network_isolation=receipt)

    def run_dead_pod(self):
        for scenario, target in (("D1", VOTERS[2]), ("D2", VOTERS[1]),
                                 ("D3", VOTERS[0]), ("D4", VOTERS[2])):
            self.dead_pod_case(scenario, target)

    def run(self):
        scenarios = self.args.scenarios
        self.event("start", run_id=self.run_id, scenarios=scenarios,
                   claim_positive="cooperative online replacement, generation recovery "
                                  "and approved NetworkPolicy isolation",
                   claim_failsafe=("dead-Pod execution blocked while termination evidence is unresolved"
                                   if scenarios in ("dead-pod", "all") else "not_run"))
        self.prove_admission()
        self.prepare()
        if scenarios in ("positive", "all"):
            self.run_positive()
        if scenarios in ("dead-pod", "all"):
            self.reset_fences()
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


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--app-image", required=True)
    parser.add_argument("--operator-image", required=True)
    parser.add_argument("--admin-token", default="")
    parser.add_argument("--output", default="votercount-gcs-chaos-results")
    parser.add_argument("--scenarios", default="positive", choices=("positive", "dead-pod", "all"))
    parser.add_argument("--inspector-probe", required=True,
                        help="rendered paired isolation probe Pods from the launcher")
    parser.add_argument("--inspector-network-isolation",
                        default=os.environ.get("RHIZA_INSPECTOR_NETWORK_ISOLATION", "required"))
    return parser


def main():
    chaos = VoterCountChaos(build_parser().parse_args())
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
