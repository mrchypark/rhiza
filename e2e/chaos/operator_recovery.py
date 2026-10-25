#!/usr/bin/env python3
"""Real Kubernetes/CRI faults against the running Operator; no mocked APIs."""
import argparse
import base64
import hashlib
import json
import http.client
import re
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

from operator_fixture import resources, learner_resources


STARTUP_PHASE_CODES = frozenset({
    "configuration", "fencing", "local_state", "wal", "identity_storage",
    "archive", "materializer", "consensus", "recovery", "network", "services",
})
STARTUP_MARKER_CODES = (
    ("fencing: engine repair preflight:", "engine_repair_preflight"),
    ("fencing: child exited without a terminal receipt:", "supervisor_child_exit"),
)
ENGINE_REPAIR_PREFLIGHT_PREFIX = "fencing: engine repair preflight:"
ENGINE_REPAIR_PREFLIGHT_CAUSES = (
    ("voter state continuity cannot be established", "engine_repair_state_continuity_lost"),
    ("registered voter WAL is absent", "engine_repair_wal_absent"),
)
GO_LOG_PREFIX = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")
READ_FAILURE_PHASES = frozenset({"read-index", "read-index-catch-up", "read-index-apply", "read-durability"})
READ_FAILURE_CLASSES = frozenset({"backend", "canceled", "deadline", "quorum-unavailable",
                                  "not-ready", "durability-unavailable"})
SAFE_DIAGNOSTIC_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
POD_ORDINAL = re.compile(r"^rhiza-(\d+)$")


def diagnostic_id_hash(value):
    """Hash bounded identifier-shaped values; never copy arbitrary API text."""
    if not isinstance(value, str) or not SAFE_DIAGNOSTIC_ID.fullmatch(value):
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def diagnostic_ordinal(value):
    match = POD_ORDINAL.fullmatch(value) if isinstance(value, str) else None
    return int(match.group(1)) if match else None


def diagnostic_uid_relation(cached, current):
    if not isinstance(cached, str) or not SAFE_DIAGNOSTIC_ID.fullmatch(cached) or \
            not isinstance(current, str) or not SAFE_DIAGNOSTIC_ID.fullmatch(current):
        return "unknown"
    return "same" if cached == current else "changed"


def diagnostic_error_category(exc):
    """Return a fixed category without persisting exception text or URLs."""
    if isinstance(exc, urllib.error.HTTPError):
        return "http_error"
    if isinstance(exc, urllib.error.URLError):
        return "timeout" if isinstance(exc.reason, (TimeoutError, socket.timeout)) else "transport_error"
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "timeout"
    if isinstance(exc, (OSError, http.client.HTTPException)):
        return "transport_error"
    return "other_error"


def diagnostic_membership(status):
    """Project membership status to bounded, non-secret comparison fields."""
    if not isinstance(status, dict):
        return {"errorCategory": "malformed_status"}
    voters = status.get("voters")
    voter_count = None
    unique_voter_count = None
    duplicate_voter_count = None
    self_included = None
    voter_ordinals = None
    if isinstance(voters, list) and len(voters) <= 16 and all(
            isinstance(voter, str) and SAFE_DIAGNOSTIC_ID.fullmatch(voter) for voter in voters):
        voter_set = "\n".join(sorted(voters)).encode()
        voter_count = len(voters)
        unique_voter_count = len(set(voters))
        duplicate_voter_count = voter_count - unique_voter_count
        node_id = status.get("node_id")
        self_included = node_id in voters if isinstance(node_id, str) and SAFE_DIAGNOSTIC_ID.fullmatch(node_id) else None
        ordinals = [diagnostic_ordinal(voter) for voter in voters]
        voter_ordinals = sorted(ordinals) if all(value is not None for value in ordinals) else None
        voter_set_hash = hashlib.sha256(voter_set).hexdigest()[:16]
    else:
        voter_set_hash = None
    config_id = status.get("config_id")
    return {
        "nodeOrdinal": diagnostic_ordinal(status.get("node_id")),
        "nodeIDHash": diagnostic_id_hash(status.get("node_id")),
        "clusterIDHash": diagnostic_id_hash(status.get("cluster_id")),
        "configID": config_id if type(config_id) is int and 0 <= config_id <= 2**64 - 1 else None,
        "voterCount": voter_count,
        "uniqueVoterCount": unique_voter_count,
        "duplicateVoterCount": duplicate_voter_count,
        "selfIncluded": self_included,
        "voterOrdinals": voter_ordinals,
        "voterSetHash": voter_set_hash,
        "walIdentityHash": diagnostic_id_hash(status.get("wal_identity")),
        "voting": status.get("voting") if type(status.get("voting")) is bool else None,
        "pending": status.get("pending") if type(status.get("pending")) is bool else None,
    }


def safe_read_failure_findings(payload):
    """Extract only the server's fixed read-failure fields; never infer causation."""
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)
    findings = set()
    for line in text.splitlines():
        if not GO_LOG_PREFIX.match(line):
            continue
        line = GO_LOG_PREFIX.sub("", line, count=1)
        marker = "read failed: "
        if not line.startswith(marker):
            continue
        fields = {}
        for token in line[len(marker):].split():
            key, separator, value = token.partition("=")
            if separator and key in ("phase", "error_class"):
                fields[key] = value if key not in fields else None
        phase, error_class = fields.get("phase"), fields.get("error_class")
        if phase in READ_FAILURE_PHASES and error_class in READ_FAILURE_CLASSES:
            findings.add((phase, error_class))
    return [{"phase": phase, "errorClass": error_class, "attribution": "unattributed"}
            for phase, error_class in sorted(findings)]


def safe_startup_findings(payload):
    """Project private startup logs to fixed diagnostic enums only."""
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)
    lines = text.splitlines()
    findings = set()
    for index, line in enumerate(lines):
        if not GO_LOG_PREFIX.match(line):
            continue
        line = GO_LOG_PREFIX.sub("", line, count=1)
        if line.startswith("open rhiza: "):
            line = line[len("open rhiza: "):]
        marker = "node open phase "
        if line.startswith(marker):
            phase, separator, detail = line[len(marker):].partition(":")
            if separator and phase.strip() in STARTUP_PHASE_CODES:
                phase = phase.strip()
                findings.add(phase)
                detail = detail.lstrip()
                expected = {
                    ("identity_storage", "ensure voter identity:"): "identity_enrollment",
                    ("identity_storage", "open object store:"): "storage_initialization",
                    ("wal", "open WAL:"): "wal_open",
                }
                for (expected_phase, prefix), code in expected.items():
                    if phase == expected_phase and detail.startswith(prefix):
                        findings.add(code)
        for prefix, code in STARTUP_MARKER_CODES:
            if line.startswith(prefix):
                findings.add(code)
                if prefix == ENGINE_REPAIR_PREFLIGHT_PREFIX:
                    detail = line[len(prefix):].lstrip()
                    for cause, cause_code in ENGINE_REPAIR_PREFLIGHT_CAUSES:
                        if detail.startswith(cause):
                            findings.add(cause_code)
                    # errors.Join renders each fixed sentinel on its own line.
                    # Only recognize the exact WAL sentinel immediately after
                    # the exact state-loss sentinel; never copy log text out.
                    if (detail.startswith(ENGINE_REPAIR_PREFLIGHT_CAUSES[0][0]) and
                            index + 1 < len(lines) and
                            lines[index + 1] == ENGINE_REPAIR_PREFLIGHT_CAUSES[1][0]):
                        findings.add(ENGINE_REPAIR_PREFLIGHT_CAUSES[1][1])
    return sorted(findings)


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
        self.supervised_bootstrap = False
        self.diagnostic_deadline = None

    def _diagnostic_remaining(self):
        if getattr(self, "diagnostic_deadline", None) is None:
            self.diagnostic_deadline = time.monotonic() + 10
        return self.diagnostic_deadline - time.monotonic()

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

    def private_diagnostics_dir(self):
        private = self.out / "private-startup-diagnostics"
        private.mkdir(mode=0o700, exist_ok=True)
        private.chmod(0o700)
        return private

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

    def forward(self, pod, remote_port=8080):
        # Each writer has its own forward; replacing a Pod invalidates its UID.
        key = (threading.get_ident(), pod, remote_port)
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
        log_path = self.private_diagnostics_dir() / f"forward-{pod}-{key[0]}-{remote_port}.log"
        log = log_path.open("a")
        log_path.chmod(0o600)
        proc = subprocess.Popen(["kubectl", "--context", self.context, "-n", self.ns,
                                 "port-forward", "pod/" + pod, f"{port}:{remote_port}"], stdout=log, stderr=log)
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

    def http(self, pod, route, body=None, admin=None, remote_port=None):
        if remote_port is None:
            embedded = getattr(self.args, "embedded_host", "server") != "server"
            remote_port = 9091 if embedded and route.startswith(("/membership/", "/recovery/")) else 8080
        port = self.forward(pod, remote_port)
        request = urllib.request.Request(f"http://127.0.0.1:{port}{route}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + (admin or self.admin)})
        with urllib.request.urlopen(request, timeout=6) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}

    def status(self, pod):
        return self.http(pod, "/membership/status")

    def capture_linearizable_read_failure(self, voters, before_membership, error, sql_started=None):
        """Best-effort status snapshot; never adds retries or masks the read error."""
        failed_at = time.monotonic()
        self._diagnostic_remaining()
        captures = []
        for pod in voters[:3]:
            ordinal = diagnostic_ordinal(pod)
            entry = {"ordinal": ordinal, "membership": diagnostic_membership(before_membership.get(pod))}
            entry["cachedForwardUID"] = "unknown"
            captures.append(entry)

        pod_states = []
        snapshot_started = time.monotonic()
        try:
            remaining = self._diagnostic_remaining()
            if remaining <= 0:
                raise TimeoutError("diagnostic_deadline_exhausted")
            result = subprocess.run(
                ["kubectl", "--context", self.context, "-n", self.ns,
                 "--request-timeout=1s", "get", "pods", *voters[:3], "--ignore-not-found", "-o",
                 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,'
                 'READY:.status.conditions[?(@.type=="Ready")].status,'
                 'RESTARTS:.status.containerStatuses[?(@.name=="rhiza")].restartCount',
                 "--no-headers"],
                capture_output=True, text=True, timeout=min(1.5, remaining))
            if result.returncode:
                raise RuntimeError("pod_snapshot_failed")
            requested = {pod: {"ordinal": diagnostic_ordinal(pod), "podPresent": False,
                               "uidHash": None, "phase": None, "ready": None,
                               "restartCount": None} for pod in voters[:3]}
            seen = set()
            for line in result.stdout.splitlines():
                fields = line.split()
                if len(fields) != 5:
                    raise ValueError("pod_snapshot_invalid")
                name, uid, phase, ready, restarts = fields
                if name not in requested:
                    continue
                if name in seen:
                    raise ValueError("pod_snapshot_duplicate")
                seen.add(name)
                requested[name].update({
                    "podPresent": True,
                    "uidHash": diagnostic_id_hash(uid),
                    "phase": phase if phase in ("Pending", "Running", "Succeeded", "Failed", "Unknown") else None,
                    "ready": True if ready == "True" else False if ready == "False" else None,
                    "restartCount": int(restarts) if re.fullmatch(r"\d+", restarts) else None,
                })
            pod_states = list(requested.values())
            self._diagnostic_pod_uids = {name: next((line.split()[1] for line in result.stdout.splitlines()
                if len(line.split()) == 5 and line.split()[0] == name), None) for name in requested}
            for entry, pod in zip(captures, voters[:3]):
                key = (threading.get_ident(), pod,
                       9091 if getattr(self.args, "embedded_host", "server") != "server" else 8080)
                cached = self.forwards.get(key) if pod else None
                if cached and cached[1].poll() is None:
                    current_uid = self._diagnostic_pod_uids.get(pod)
                    entry["cachedForwardUID"] = diagnostic_uid_relation(cached[0], current_uid)
        except Exception as exc:
            pod_states = [{"errorCategory": diagnostic_error_category(exc)}]
        snapshot_elapsed_ms = max(0, int((time.monotonic() - snapshot_started) * 1000))

        # event() writes evidence.json and stdout; every field here is projected.
        try:
            self.event("linearizable_read_failure_diagnostics",
                       errorCategory=diagnostic_error_category(error), survivors=captures,
                       pods=pod_states,
                       postFailureProbeRequested=False,
                       sqlElapsedMs=(max(0, int((failed_at - sql_started) * 1000)) if sql_started is not None else None),
                       podSnapshotElapsedMs=snapshot_elapsed_ms,
                       podSnapshotOffsetMs=max(0, int((snapshot_started - failed_at) * 1000)))
        except Exception:
            pass

    def execute(self, pod, rid, sql):
        result = self.http(pod, "/sql/execute", {"request_id": rid, "sql": sql})
        assert not result.get("error_code"), result
        return result

    def verify_idempotent_write(self, pod, rid, sql, expected_row):
        self.execute(pod, rid, sql)
        self.execute(pod, rid, sql)
        data = self.http(pod, "/sql/query", {
            "sql": "SELECT id, value FROM chaos_items WHERE id = ?",
            "args": [expected_row[0]],
            "consistency": "linearizable",
        })
        rows = data["rows"]
        expected = [list(expected_row)]
        assert rows == expected and all(
            type(actual) is type(want) for actual, want in zip(rows[0], expected[0])
        ), f"idempotent write readback differs: got {rows!r}, want {expected!r}"
        return rows

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
        status = self.cr(name).get("status", {})
        return status.get("membership", {}).get("phase", status.get("phase")) == phase

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

    def reject_legacy_replacement(self, name, removed, removed_uid, nonce, voters, learner):
        """Prove the quorum-alive legacy operator membership route is rejected."""
        reason = "new operator-managed membership changes are disabled; ordinary Pod recovery is engine-owned"
        before_membership = {pod: self.status(pod) for pod in voters}
        before_rows = self.http(voters[0], "/sql/query", {
            "sql": "SELECT id, value FROM chaos_items ORDER BY id",
            "consistency": "linearizable",
        }).get("rows")
        spec = {"statefulSet": "rhiza", "container": "rhiza", "sourceClusterID": "chaos-source",
                "durability": "before-ack", "membership": {"operationID": name, "remove": removed,
                "removedPod": removed, "voterPods": [{"nodeID": p, "pod": p} for p in voters],
                "replacementPod": learner, "replacementSecret": learner + "-credentials",
                "fence": {"nodeID": removed, "walIdentity": nonce, "workloadUID": removed_uid,
                          "confirmed": True,
                          "evidence": f"CRI confirms Pod UID {removed_uid} has no running container; StatefulSet is scaled below its ordinal"}}}
        self.apply({"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
                    "metadata": {"name": name, "namespace": self.ns}, "spec": spec})
        self.wait("legacy operator membership request rejected", lambda: self.phase(name, "Blocked"))
        blocked = self.cr(name).get("status", {})
        assert blocked.get("phase") == "Blocked" and blocked.get("message") == reason, blocked
        journal = blocked.get("membership", {})
        assert not journal.get("removeRequest") and not journal.get("add"), journal
        assert self.runtime_gone(removed_uid), "old voter still running"
        time.sleep(3)
        final_status = self.cr(name).get("status", {})
        assert final_status.get("phase") == "Blocked" and final_status.get("message") == reason, final_status
        final_journal = final_status.get("membership", {})
        assert not final_journal.get("removeRequest") and not final_journal.get("add"), final_journal
        after_membership = {pod: self.status(pod) for pod in voters}
        assert after_membership == before_membership, (
            f"membership changed: before={before_membership!r} after={after_membership!r}")
        sql_started = time.monotonic()
        try:
            after_rows = self.http(voters[0], "/sql/query", {
                "sql": "SELECT id, value FROM chaos_items ORDER BY id",
                "consistency": "linearizable",
            }).get("rows")
        except Exception as original:
            try:
                self.diagnostic_deadline = time.monotonic() + 10
                self.capture_linearizable_read_failure(voters, before_membership, original, sql_started)
            except Exception:
                pass
            raise
        assert after_rows == before_rows, f"data changed: before={before_rows!r} after={after_rows!r}"
        self.event("legacy_membership_request_rejected_only", operation=name,
                   reason="ordinary_recovery_is_engine_owned", voters=voters,
                   config_id=before_membership[voters[0]]["config_id"],
                   unchanged_rows=len(after_rows or []))

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
        expected = self.acks.copy()
        data = self.http(pod, "/sql/query", {
            "sql": "SELECT id FROM chaos_items ORDER BY id",
            "consistency": "linearizable",
        })
        found = {int(row[0]) for row in data["rows"]}
        missing = sorted(expected - found)
        self.event("data_verification", pod=pod, consistency="linearizable",
                   expected_count=len(expected), found_count=len(found), missing=missing,
                   applied_slot=data.get("applied_slot"), consensus_tip=data.get("consensus_tip"))
        assert not missing, (
            f"missing acknowledged rows: {missing}; "
            f"applied_slot={data.get('applied_slot')} consensus_tip={data.get('consensus_tip')}"
        )
        self.execute(pod, "chaos-seed", "INSERT INTO chaos_items VALUES (0, 'seed')")
        return len(found)

    def setup(self):
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
        fixture = resources(self.ns, self.args.db_image, self.args.operator_image,
                            supervised=self.supervised_bootstrap)
        workloads = [o for o in fixture if o["kind"] == "StatefulSet" or o["metadata"]["name"] == "rhiza-operator"]
        self.apply([o for o in fixture if o not in workloads])
        self.k("wait", "--for=condition=Complete", "job/rhiza-create-bucket", "--timeout=180s")
        if self.supervised_bootstrap:
            self.install_supervised_authority()
            self.apply({"apiVersion": "v1", "kind": "ServiceAccount",
                        "metadata": {"name": "rhiza-supervised", "namespace": self.ns},
                        "automountServiceAccountToken": False})
            operator = next(o for o in workloads if o["metadata"]["name"] == "rhiza-operator")
            env = operator["spec"]["template"]["spec"]["containers"][0].setdefault("env", [])
            env.extend([
                {"name": "RHIZA_AUTOMATIC_RECOVERY", "value": "true"},
                {"name": "RHIZA_FENCER_BACKEND", "value": "kubernetes"},
            ])
        self.apply(workloads)
        config = self.get("configmap", "rhiza-config")["data"]
        self.members = json.loads(config["RHIZA_CLUSTER_MEMBERS"])
        if self.supervised_bootstrap:
            self.before_supervised_root()
            if json.loads(self.k("get", "pods", "-l", "app.kubernetes.io/name=rhiza", "-o", "json"))["items"]:
                raise RuntimeError("supervised root binding requires zero source Pods")
            self.apply(self.supervised_root_resource())
            self.k("scale", "deployment/rhiza-operator", "--replicas=1")
            self.k("rollout", "status", "deployment/rhiza-operator", "--timeout=180s")
            root_uid = self.get("rhizacluster", "rhiza")["metadata"]["uid"]
            sts_uid = self.get("statefulset", "rhiza")["metadata"]["uid"]
            self.wait("supervised root UID pinned before first Pod", lambda: self._root_pinned(root_uid, sts_uid), 120)
            if self.get("rhizacluster", "rhiza")["metadata"].get("uid") != root_uid:
                raise RuntimeError("supervised source root identity changed before initial scale")
            if json.loads(self.k("get", "pods", "-l", "app.kubernetes.io/name=rhiza", "-o", "json"))["items"]:
                raise RuntimeError("source Pod appeared before the root UID pin was verified")
            self.event("supervised_root_pinned_before_scale", root_uid=root_uid, stateful_set_uid=sts_uid)
            self.k("scale", "statefulset/rhiza", "--replicas=3")
        else:
            self.k("rollout", "status", "deployment/rhiza-operator", "--timeout=180s")
        for pod in ("rhiza-0", "rhiza-1", "rhiza-2"):
            self.wait("initial voter " + pod, lambda p=pod: self.ready(p), 240)
        self.execute("rhiza-0", "chaos-schema", "CREATE TABLE chaos_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.execute("rhiza-0", "chaos-seed", "INSERT INTO chaos_items VALUES (0, 'seed')")
        self.start_writer()
        self.wait("workload began", lambda: len(self.acks) >= 5)

    def before_supervised_root(self):
        """Hook for scenario-specific dependencies that must precede the root."""

    def supervised_root_resource(self):
        raise RuntimeError("supervised bootstrap requires a root resource")

    def _root_pinned(self, root_uid, sts_uid):
        sts = self.get("statefulset", "rhiza")
        return (sts["metadata"].get("uid") == sts_uid and
                sts["spec"].get("replicas") == 0 and
                sts["spec"]["template"].get("metadata", {}).get("annotations", {}).get(
                    "rhiza.mrchypark.dev/fencing-root-uid") == root_uid)

    def _runtime_profile(self):
        version = self.command(["docker", "exec", self.node, "crictl", "version"])

        def field(name):
            values = [line.split(":", 1)[1].strip() for line in version.splitlines()
                      if line.split(":", 1)[0].strip() == name]
            if len(values) != 1 or not values[0]:
                raise RuntimeError(f"runtime profile field {name} is unavailable or ambiguous")
            return values[0]

        info = json.loads(self.command(["docker", "exec", self.node, "crictl", "info", "-o", "json"]))
        runtime = info.get("config", {}).get("containerd", {})
        runtime_type = runtime.get("runtimes", {}).get(runtime.get("defaultRuntimeName"), {}).get("runtimeType", "")
        kernel = self.command(["docker", "exec", self.node, "uname", "-r"]).strip()
        cgroup = self.command(["docker", "exec", self.node, "stat", "-fc", "%T", "/sys/fs/cgroup"]).strip()
        values = {"containerd-version": field("RuntimeVersion"),
                  "containerd-runtime-type": runtime_type,
                  "kernel-release": kernel,
                  "cgroup-profile-id": "kind-ci-cgroup-v2-component-v1"}
        if field("RuntimeName") != "containerd" or field("RuntimeApiVersion") != "v1" or cgroup != "cgroup2fs":
            raise RuntimeError("supervised fixture requires the existing containerd CRI v1/cgroup-v2 profile")
        if any(not re.fullmatch(r"[A-Za-z0-9._+-]+", value) for value in values.values()):
            raise RuntimeError("runtime profile contains an unsafe or missing field")
        return values

    def install_supervised_authority(self):
        root = Path(__file__).resolve().parents[2]
        profile = self._runtime_profile()
        with tempfile.TemporaryDirectory(prefix="rhiza-supervised-") as directory:
            tls_config = Path(directory, "openssl.cnf")
            tls_config.write_text(
                "[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=rhiza-fencer\n"
                "[ext]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,digitalSignature,keyEncipherment,keyCertSign\n"
                f"extendedKeyUsage=serverAuth\nsubjectAltName=DNS:rhiza-fencer,DNS:rhiza-fencer.{self.ns}.svc,DNS:runtime-agent\n")
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                            "-config", str(tls_config), "-keyout", f"{directory}/tls.key",
                            "-out", f"{directory}/tls.crt"], check=True, capture_output=True, timeout=30)
            for secret_name in ("rhiza-fencer-tls", "rhiza-runtime-agent-tls"):
                self.k("create", "secret", "tls", secret_name, f"--cert={directory}/tls.crt",
                       f"--key={directory}/tls.key")
            self.k("create", "configmap", "rhiza-fencer-ca",
                   f"--from-file=ca.crt={directory}/tls.crt")
            self.k("create", "secret", "generic", "rhiza-runtime-agent-ca",
                   f"--from-file=ca.crt={directory}/tls.crt")

        authority = (root / "deploy/operator/fencing/authority.yaml").read_text()
        replacements = {
            "namespace: rhiza": f"namespace: {self.ns}",
            "resourceNames: [rhiza]": f"resourceNames: [{self.ns}]",
            "image: rhiza-operator:latest": f"image: {self.args.operator_image}",
            "QUALIFIED_CONTAINERD_VERSION": profile["containerd-version"],
            "QUALIFIED_CONTAINERD_RUNTIME_TYPE": profile["containerd-runtime-type"],
            "QUALIFIED_PRIVATE_CGROUP_V2_PROFILE_ID": profile["cgroup-profile-id"],
            "QUALIFIED_LINUX_KERNEL_RELEASE": profile["kernel-release"],
            "QUALIFIED_SANDBOX_EXECUTABLE_SHA256": "",
            "PINNED_RHIZA_FENCER_DEPLOYMENT_UID": "pending-authority-uid",
        }
        for old, new in replacements.items():
            authority = authority.replace(old, new)
        self.k("apply", "-f", "-", data=authority)
        authority_uid = self.get("deployment", "rhiza-fencer")["metadata"]["uid"]
        self.patch("configmap", "rhiza-runtime-agent-profile", {"data": {
            **profile, "authority-deployment-uid": authority_uid,
            "sandbox-executable-sha256": "", "sandbox-args-json": '["/pause"]'}})
        self.k("patch", "deployment", "rhiza-fencer", "--type=strategic", "-p", json.dumps({"spec": {"template": {"spec": {"containers": [{
            "name": "authority", "envFrom": [
                {"configMapRef": {"name": "rhiza-config"}},
                {"secretRef": {"name": "rhiza-object-store"}},
            ]}]}}}}))
        self.k("rollout", "status", "deployment/rhiza-fencer", "--timeout=180s")
        self.k("rollout", "restart", "daemonset/rhiza-runtime-agent")
        self.k("rollout", "status", "daemonset/rhiza-runtime-agent", "--timeout=180s")

    def capture_failure_diagnostics(self):
        """Capture identity-bound, bounded startup evidence before cleanup."""
        private = self.private_diagnostics_dir()
        self._diagnostic_remaining()
        deadline = self.diagnostic_deadline
        errors = []
        error_details = []
        record = {"namespace": self.ns, "context": self.context, "captures": []}

        def capture_error(code, detail):
            errors.append(code)
            error_details.append({"code": code, "detail": detail})

        def read_json(args):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("diagnostic deadline exhausted")
            result = subprocess.run(
                ["kubectl", "--context", self.context, "-n", self.ns,
                 "--request-timeout=2s", *args], capture_output=True,
                text=True, timeout=min(3, remaining))
            if result.returncode:
                raise RuntimeError(result.stderr[-500:])
            return json.loads(result.stdout)

        def read_secret_identity(name):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("diagnostic deadline exhausted")
            fields = r'jsonpath={.metadata.name}{"\n"}{.metadata.uid}{"\n"}{.metadata.annotations.rhiza\.mrchypark\.dev/recovery-owner}{"\n"}{.immutable}'
            result = subprocess.run(
                ["kubectl", "--context", self.context, "-n", self.ns,
                 "--request-timeout=2s", "get", "secret", name, "-o", fields],
                capture_output=True, text=True, timeout=min(3, remaining))
            if result.returncode:
                raise RuntimeError(result.stderr[-500:])
            values = result.stdout.splitlines()
            if len(values) != 4:
                raise ValueError("credential metadata is incomplete")
            return {"name": values[0], "uid": values[1],
                    "owner": values[2], "immutable": values[3] == "true"}

        root = None
        try:
            root = read_json(["get", "rhizacluster", "rhiza", "-o", "json"])
            status = root.get("status", {})
            record["root"] = {"uid": root.get("metadata", {}).get("uid"),
                "generation": root.get("metadata", {}).get("generation"),
                "observedGeneration": status.get("observedGeneration"),
                "phase": status.get("phase"), "message": status.get("message")}
        except Exception as exc:
            record["root"] = {"readError": f"{type(exc).__name__}: {exc}"}
            capture_error("root_status_read_failed", f"{type(exc).__name__}: {exc}")

        sts = {}
        try:
            sts = read_json(["get", "statefulset", "rhiza", "-o", "json"])
            sts_uid = sts.get("metadata", {}).get("uid", "")
            record["statefulSet"] = {"uid": sts_uid,
                "generation": sts.get("metadata", {}).get("generation"),
                "replicas": sts.get("spec", {}).get("replicas"),
                "observedGeneration": sts.get("status", {}).get("observedGeneration"),
                "template": {"annotations": sts.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {}),
                    "containers": [{k: c.get(k) for k in ("name", "image", "command", "args", "ports")}
                                   for c in sts.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])]}}
            pods = read_json(["get", "pods", "-o", "json"]).get("items", [])
        except Exception as exc:
            capture_error("source_identity_list_failed", f"{type(exc).__name__}: {exc}")
            pods, sts_uid = [], ""

        learner_binding = None
        root_meta = (root or {}).get("metadata", {})
        root_spec = (root or {}).get("spec", {})
        root_status = (root or {}).get("status", {})
        root_uid = root_meta.get("uid", "")
        active_recovery = root_status.get("activeRecovery", "")
        if active_recovery:
            try:
                child = read_json(["get", "rhizarecovery", active_recovery, "-o", "json"])
                child_meta = child.get("metadata", {})
                child_spec = child.get("spec", {})
                membership = child_spec.get("membership", {})
                annotations = child_meta.get("annotations", {})
                sts_annotations = sts.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {})
                operation = membership.get("operationID", "")
                learner_name = membership.get("replacementPod", "")
                credential_name = membership.get("replacementSecret", "")
                child_owned = (root_uid and root_spec.get("automatic") is True and root_spec.get("statefulSet") == "rhiza" and
                        root_spec.get("logicalID") and root_status.get("activeClusterID") and
                        root_status.get("observedGeneration") == root_meta.get("generation") and
                        sts_uid and sts.get("metadata", {}).get("name") == root_spec.get("statefulSet") and
                        sts_annotations.get("rhiza.mrchypark.dev/cooperative-fencing") == "1" and
                        sts_annotations.get("rhiza.mrchypark.dev/fencing-root-uid") == root_uid and
                        child_meta.get("name") == active_recovery and child_meta.get("uid") and
                        annotations.get("rhiza.mrchypark.dev/automatic-owner") == root_uid and
                        annotations.get("rhiza.mrchypark.dev/automatic-logical") == root_spec.get("logicalID") and
                        child_spec.get("statefulSet") == root_spec.get("statefulSet") and
                        child_spec.get("sourceClusterID") == root_status.get("activeClusterID"))
                if child_owned and membership:
                    if operation != active_recovery or not learner_name or not credential_name:
                        capture_error("active_recovery_binding_mismatch", active_recovery)
                    else:
                        credential = read_secret_identity(credential_name)
                        if (credential["name"] != credential_name or not credential["uid"] or
                                not credential["immutable"] or credential["owner"] != operation):
                            capture_error("active_recovery_binding_mismatch", active_recovery)
                        else:
                            learner_binding = {"name": learner_name, "rootUID": root_uid,
                                               "statefulSetUID": sts_uid, "operationID": operation,
                                               "credentialUID": credential["uid"]}
                elif not child_owned:
                    capture_error("active_recovery_binding_mismatch", active_recovery)
            except Exception as exc:
                capture_error("active_recovery_binding_unavailable", f"{type(exc).__name__}: {exc}")

        candidates = []
        learner_pod_seen = False
        for pod in pods:
            metadata = pod.get("metadata", {})
            labels = metadata.get("labels", {})
            name = metadata.get("name", "")
            component = labels.get("app.kubernetes.io/name", "")
            is_bound_learner = False
            if learner_binding and name == learner_binding["name"]:
                learner_pod_seen = True
                refs = metadata.get("ownerReferences", [])
                annotations = metadata.get("annotations", {})
                is_bound_learner = (
                    metadata.get("uid") and labels.get("rhiza.mrchypark.dev/automatic-learner") == name and
                    len(refs) == 1 and refs[0].get("apiVersion") == "apps/v1" and
                    refs[0].get("kind") == "StatefulSet" and refs[0].get("name") == "rhiza" and
                    refs[0].get("uid") == sts_uid and refs[0].get("controller") is False and
                    annotations.get("rhiza.mrchypark.dev/fencing-root-uid") == learner_binding["rootUID"] and
                    annotations.get("rhiza.mrchypark.dev/recovery-owner") == learner_binding["operationID"] and
                    annotations.get("rhiza.mrchypark.dev/cooperative-fencing") == "1" and
                    annotations.get("rhiza.mrchypark.dev/fencing-credential-uid") == learner_binding["credentialUID"])
                if not is_bound_learner:
                    capture_error("learner_pod_binding_mismatch", name)
            if component in ("rhiza", "rhiza-fencer", "rhiza-runtime-agent", "rhiza-operator") or is_bound_learner:
                owner_ok = any(ref.get("controller") and ref.get("kind") == "StatefulSet" and
                               ref.get("name") == "rhiza" and ref.get("uid") == sts_uid
                               for ref in metadata.get("ownerReferences", []))
                if component == "rhiza" and not owner_ok:
                    record["captures"].append({"pod": name, "identity": "owner-mismatch"})
                    continue
                if is_bound_learner:
                    component = "learner"
                pod["_diagnostic_component"] = component
                candidates.append(pod)
        if learner_binding and not learner_pod_seen:
            capture_error("bound_learner_pod_not_listed", learner_binding["name"])
        candidates.sort(key=lambda pod: (pod.get("_diagnostic_component") != "learner",
                                         pod.get("status", {}).get("phase") == "Running",
                                         pod.get("metadata", {}).get("name", "")))
        for pod in candidates[:5]:
            metadata = pod.get("metadata", {})
            name = metadata.get("name", "")
            identity = {"name": name, "uid": metadata.get("uid"),
                        "phase": pod.get("status", {}).get("phase"),
                        "ownerUID": next((ref.get("uid") for ref in metadata.get("ownerReferences", [])
                                          if ref.get("kind") == "StatefulSet"), None)}
            for container in pod.get("spec", {}).get("containers", []):
                cname = container.get("name", "")
                if cname not in ("rhiza", "authority", "runtime-agent", "operator"):
                    continue
                state = next((status for status in pod.get("status", {}).get("containerStatuses", [])
                              if status.get("name") == cname), {})
                item = {"identity": identity, "component": pod.get("_diagnostic_component"), "container": cname,
                        "containerStatus": {"containerID": state.get("containerID"),
                            "restartCount": state.get("restartCount"),
                            "state": state.get("state"), "lastState": state.get("lastState")},
                        "logs": {}, "startupFindings": [], "readFailureFindings": []}

                def incarnation(pod_status, previous):
                    container_status = next((status for status in pod_status.get("status", {}).get("containerStatuses", [])
                                             if status.get("name") == cname), {})
                    restart_count = container_status.get("restartCount")
                    if previous:
                        terminated = container_status.get("lastState", {}).get("terminated", {})
                        container_id = terminated.get("containerID")
                    else:
                        container_id = container_status.get("containerID")
                    return (container_id, restart_count) if container_id else None

                try:
                    before_pod = read_json(["get", "pod", name, "-o", "json"])
                    before_ok = (before_pod.get("metadata", {}).get("uid") == identity["uid"] and
                                 all(incarnation(pod, previous) == incarnation(before_pod, previous)
                                     for previous in (False, True)))
                except Exception as exc:
                    before_pod, before_ok = {}, False
                    capture_error("pod_identity_precheck_failed", f"{name}/{cname}: {type(exc).__name__}: {exc}")
                pending_logs = {}
                for previous in (False, True):
                    remaining = deadline - time.monotonic()
                    key = "previous" if previous else "current"
                    expected_incarnation = incarnation(before_pod, previous) if before_ok else None
                    if not before_ok or expected_incarnation is None:
                        code = "pod_identity_changed" if not before_ok else "container_incarnation_unavailable"
                        item["logs"][key] = {"captured": False, "errorCode": code}
                        capture_error(code, f"{name}/{cname}/{key}")
                        continue
                    if remaining <= 0:
                        item["logs"][key] = {"captured": False, "errorCode": "diagnostic_deadline_exhausted"}
                        capture_error("diagnostic_deadline_exhausted", f"{name}/{cname}/{key}")
                        continue
                    args = ["logs", name, "-c", cname, "--tail=100", "--limit-bytes=32768"]
                    if previous:
                        args.append("--previous")
                    try:
                        result = subprocess.run(
                            ["kubectl", "--context", self.context, "-n", self.ns,
                             "--request-timeout=2s", *args], capture_output=True,
                            timeout=min(3, remaining))
                        payload = result.stdout[:32768]
                        captured = result.returncode == 0
                        detail = result.stderr.decode(errors="replace")[-500:] if result.returncode else ""
                    except Exception as exc:
                        payload = b""
                        captured = False
                        detail = f"{type(exc).__name__}: {exc}"
                    pending_logs[key] = (payload, captured, detail)

                after_ok = False
                if pending_logs:
                    try:
                        after_pod = read_json(["get", "pod", name, "-o", "json"])
                        after_ok = (before_ok and after_pod.get("metadata", {}).get("uid") == identity["uid"] and
                                    all(incarnation(before_pod, previous) == incarnation(after_pod, previous)
                                        for previous in (False, True)))
                    except Exception as exc:
                        capture_error("pod_identity_postcheck_failed", f"{name}/{cname}: {type(exc).__name__}: {exc}")
                if not after_ok:
                    for key in pending_logs:
                        item["logs"][key] = {"captured": False, "errorCode": "pod_or_container_identity_changed"}
                    capture_error("pod_or_container_identity_changed", f"{name}/{cname}")
                for key, (payload, captured, detail) in pending_logs.items():
                    if after_ok:
                        if captured:
                            item["startupFindings"] = sorted(set(item["startupFindings"]) |
                                                               set(safe_startup_findings(payload)))
                            known = {(finding["phase"], finding["errorClass"])
                                     for finding in item["readFailureFindings"]}
                            for finding in safe_read_failure_findings(payload):
                                key = (finding["phase"], finding["errorClass"])
                                if key not in known:
                                    item["readFailureFindings"].append(finding)
                                    known.add(key)
                        path = private / f"{name}-{cname}-{key}.log"
                        path.write_bytes(payload)
                        path.chmod(0o600)
                        item["logs"][key] = {"captured": captured,
                            "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
                            "truncated": len(payload) == 32768}
                    if not captured and after_ok:
                        item["logs"].setdefault(key, {"captured": False})
                        item["logs"][key]["errorCode"] = "pod_log_capture_failed"
                        item["logs"][key]["error"] = detail
                        capture_error("pod_log_capture_failed", f"{name}/{cname}/{key}: {detail}")
                record["captures"].append(item)
        record["captureErrors"] = error_details
        path = private / "record.json"
        path.write_text(json.dumps(record, indent=2))
        path.chmod(0o600)

        summary = {"version": 1, "status": "partial" if errors else "complete",
            "captureErrors": sorted(set(errors)), "captures": []}
        for item in record["captures"]:
            logs = {}
            for key, result in item.get("logs", {}).items():
                logs[key] = {field: result[field] for field in ("captured", "bytes", "sha256", "truncated", "errorCode")
                             if field in result}
            summary["captures"].append({"component": item.get("component"),
                                        "container": item.get("container"), "logs": logs,
                                        "startupFindings": item.get("startupFindings", []),
                                        "readFailureFindings": item.get("readFailureFindings", [])})
        public_path = self.out / "startup-diagnostics-summary.json"
        public_path.write_text(json.dumps(summary, indent=2))
        public_path.chmod(0o600)
        return sorted(set(errors))

    def run(self):
        self.setup()


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
        # Freeze concurrent writes so the negative legacy-request assertion can
        # compare exact data snapshots without racing the workload writer.
        self.stop.set()
        if self.writer:
            self.writer.join(timeout=15)
            assert not self.writer.is_alive(), "workload writer did not stop before the legacy-route assertion"
        self.reject_legacy_replacement(
            "replace-2", "rhiza-2", old_uid, lost["wal_identity"], ["rhiza-0", "rhiza-1"], "learner-a")
        self.stop.set()
        self.verify_data("rhiza-0")

        # External scheduling fence prevents old voters from being recreated.
        old_uids = [self.runtime(p)[0] for p in ("rhiza-0", "rhiza-1")] + [old_uid]
        self.patch("statefulset", "rhiza", {"spec": {"replicas": 3, "template": {"spec": {
            "nodeSelector": {"rhiza-chaos/fenced": "true"}}}}})
        self.k("delete", "pod", "rhiza-0", "rhiza-1", "rhiza-2", "learner-a", "learner-c", "--ignore-not-found", "--wait=true")
        self.wait("all old runtimes fenced", lambda: all(self.runtime_gone(uid) for uid in old_uids))
        sts_uid = self.get("statefulset", "rhiza")["metadata"]["uid"]
        fence = {"recoveryID": "whole-generation", "clusterID": "chaos-source", "statefulSetUID": sts_uid,
                 "confirmed": False, "evidence": "Awaiting explicit runtime fence verification"}
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
        new_write_rows = self.verify_idempotent_write(
            "rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'new-generation')",
            (10002, "new-generation"))
        assert self.status("rhiza-0")["cluster_id"] == target
        self.event("whole_generation_recovery_pass", target=target, acknowledged_rows=len(self.acks), recovered_rows=count,
                   old_pod_uids=old_uids, request_replay_scope="one final new-generation write",
                   request_replay="same-id-same-sql", new_write_readback=new_write_rows)
        self.event("PASS", scenarios=["intact_wal_restart", "lost_wal_refused",
                                       "legacy_membership_request_rejected_only", "whole_generation_recovery"])

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
                        path = self.private_diagnostics_dir() / f"{name}{'-previous' if previous else ''}.log"
                        path.write_text(self.k(*args))
                        path.chmod(0o600)
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
