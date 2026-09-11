#!/usr/bin/env python3
"""Disposable Kind-only runtime executor for CR RhizaFence v1alpha1.

Interface: KindFencer(chaos).start() / .close() / .enabled
Paths: <chaos.out>/barriers.json (atomic), <chaos.out>/executor-events.jsonl
CRD: rhiza.mrchypark.dev/v1alpha1/rhizafences

Requires: chaos object with .k(), .get(), .command(), .runtime_gone(),
          .context, .ns, .node, .out (Path), .stop (Event)
"""
import base64, hashlib, json, secrets, shutil, ssl, subprocess, sys
import tempfile, threading, time, urllib.error, urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CR_GROUP, CR_VER, CR_PLURAL = "rhiza.mrchypark.dev", "v1alpha1", "rhizafences"
WEBHOOK_NAME = "rhiza-chaos-fencing-webhook"
WEBHOOK_PATH = "/fence"
WEBHOOK_LABEL = "rhiza-chaos-fencing"
MINIO_DEPLOY = "rhiza-minio"
POLL_SEC = 2.0
KILL_TIMEOUT = 30.0
MINIO_TIMEOUT = 60.0
MAX_BODY = 1 << 20
PROXY_PORT = 18001
WEBHOOK_PORT = 18443


def _log(msg, level="INFO"):
    print(f"[{level}] {msg}", file=sys.stderr, flush=True)


# ── BarrierStore ─────────────────────────────────────────────────────────────


class BarrierStore:
    def __init__(self, path):
        self._path = Path(path)
        self._lock = threading.Lock()
        self._data = {}

    def load(self):
        with self._lock:
            if self._path.exists():
                self._data = json.loads(self._path.read_text())
            else:
                self._data = {}

    def save(self):
        with self._lock:
            data = json.dumps(self._data, separators=(",", ":"))
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(data)
            tmp.replace(self._path)

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def items_snapshot(self):
        with self._lock:
            return list(self._data.items())

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def update(self, key, **kw):
        with self._lock:
            if key in self._data:
                self._data[key].update(kw)

    def get_uid_set(self, key):
        with self._lock:
            b = self._data.get(key)
            return set(b["uid_set"]) if b else set()


# ── Request hash (Go json.Marshal field order, compact) ─────────────────────


def _compute_request_hash(spec):
    ordered = {
        "logicalID": spec["logicalID"],
        "bindingUID": spec["bindingUID"],
        "namespace": spec["namespace"],
        "statefulSet": spec["statefulSet"],
        "statefulSetUID": spec["statefulSetUID"],
        "sourceClusterID": spec["sourceClusterID"],
        "operationID": spec["operationID"],
        "scope": spec["scope"],
        "targets": [
            {k: t[k] for k in ("nodeID", "pod", "podUID", "walIdentity")}
            for t in spec["targets"]
        ],
    }
    return hashlib.sha256(
        json.dumps(ordered, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


# ── Pod identity resolution (container name 'rhiza' only) ───────────────────


def _resolve_rhiza_identity(chaos, namespace, pod_spec, pod_name):
    """Returns (cluster_id, node_id) or None for infra pods.

    Only examines containers named 'rhiza'. Raises on unresolvable Rhiza env.
    """
    rhiza_ctr = next(
        (c for c in pod_spec.get("containers", []) if c.get("name") == "rhiza"),
        None,
    )
    if rhiza_ctr is None:
        return None

    has_rhiza = False
    for entry in rhiza_ctr.get("env", []):
        if entry.get("name", "").startswith("RHIZA_"):
            has_rhiza = True
            break
    if not has_rhiza:
        for entry in rhiza_ctr.get("envFrom", []):
            for kind in ("configMapRef", "secretRef"):
                ref = entry.get(kind)
                if not ref:
                    continue
                resource = "configmaps" if kind == "configMapRef" else "secrets"
                try:
                    data = json.loads(
                        chaos.k("get", resource, ref["name"], "-o", "json"))
                    if any(k.startswith("RHIZA_") for k in data.get("data", {})):
                        has_rhiza = True
                        break
                except Exception:
                    has_rhiza = True
                    break
            if has_rhiza:
                break
    if not has_rhiza:
        return None

    cluster_id = None
    node_id = pod_name
    seen = set()

    for entry in rhiza_ctr.get("envFrom", []):
        prefix = entry.get("prefix", "")
        for kind in ("configMapRef", "secretRef"):
            ref = entry.get(kind)
            if not ref:
                continue
            resource = "configmaps" if kind == "configMapRef" else "secrets"
            ck = (resource, ref["name"])
            if ck in seen:
                continue
            seen.add(ck)
            data = json.loads(chaos.k("get", resource, ref["name"], "-o", "json"))
            for k, v in (data.get("data") or {}).items():
                full = prefix + k
                val = base64.b64decode(v).decode() if kind == "secretRef" else v
                if full == "RHIZA_CLUSTER_ID":
                    cluster_id = val
                elif full == "RHIZA_NODE_ID":
                    node_id = val

    for entry in rhiza_ctr.get("env", []):
        name = entry.get("name", "")
        if "value" in entry:
            if name == "RHIZA_CLUSTER_ID":
                cluster_id = entry["value"]
            elif name == "RHIZA_NODE_ID":
                node_id = entry["value"]
        elif "valueFrom" in entry:
            vf = entry["valueFrom"]
            if "fieldRef" in vf:
                if (vf["fieldRef"].get("fieldPath") == "metadata.name"
                        and name == "RHIZA_NODE_ID"):
                    node_id = pod_name
            elif "secretKeyRef" in vf:
                ref = vf["secretKeyRef"]
                d = json.loads(chaos.k("get", "secrets", ref["name"], "-o", "json"))
                val = base64.b64decode(d["data"][ref["key"]]).decode()
                if name == "RHIZA_CLUSTER_ID":
                    cluster_id = val
                elif name == "RHIZA_NODE_ID":
                    node_id = val
            elif "configMapKeyRef" in vf:
                ref = vf["configMapKeyRef"]
                d = json.loads(chaos.k("get", "configmaps", ref["name"], "-o", "json"))
                val = d["data"][ref["key"]]
                if name == "RHIZA_CLUSTER_ID":
                    cluster_id = val
                elif name == "RHIZA_NODE_ID":
                    node_id = val
            else:
                raise ValueError(f"unsupported valueFrom for {name}")

    if cluster_id is None:
        raise ValueError("RHIZA_CLUSTER_ID unresolvable")
    return (cluster_id, node_id)


# ── Barrier match helper (shared by webhook, capture, recreation) ────────────


def _barrier_matches_pod(spec, cluster_id, node_id):
    """Check if resolved identity matches this barrier's scope.

    Voter: requires BOTH sourceClusterID AND nodeID match.
    Generation: requires sourceClusterID match.
    """
    if spec["scope"] == "Voter":
        if cluster_id != spec["sourceClusterID"]:
            return False
        target_node_ids = {t["nodeID"] for t in spec["targets"]}
        return node_id in target_node_ids
    elif spec["scope"] == "Generation":
        return cluster_id == spec["sourceClusterID"]
    return False


# ── Admission webhook ───────────────────────────────────────────────────────


class _FenceHandler(BaseHTTPRequestHandler):
    barriers = None
    chaos = None

    def log_message(self, *a):
        pass

    def do_POST(self):
        if self.path != WEBHOOK_PATH:
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0 or length > MAX_BODY:
            self._respond({"uid": "", "allowed": False,
                           "status": {"message": "invalid content length"}})
            return
        raw = self.rfile.read(length)
        try:
            review = json.loads(raw)
        except Exception:
            self._respond({"uid": "", "allowed": False,
                           "status": {"message": "invalid JSON"}})
            return
        try:
            resp = self._handle(review)
        except Exception as exc:
            _log(f"webhook error: {exc}", "ERROR")
            uid = review.get("request", {}).get("uid", "")
            resp = {"uid": uid, "allowed": False,
                    "status": {"message": f"webhook internal: {exc}"}}
        self._respond(resp)

    def _respond(self, resp):
        body = json.dumps({"apiVersion": "admission.k8s.io/v1",
                           "kind": "AdmissionReview",
                           "response": resp}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, review):
        req = review.get("request", {})
        uid = req.get("uid", "")
        if req.get("operation") != "CREATE":
            return {"uid": uid, "allowed": True}
        pod = req.get("object", {})
        pod_spec = pod.get("spec", {})
        pod_name = pod.get("metadata", {}).get("name", "")
        ns = pod.get("metadata", {}).get("namespace", self.chaos.ns)
        try:
            identity = _resolve_rhiza_identity(
                self.chaos, ns, pod_spec, pod_name)
        except Exception as exc:
            _log(f"webhook deny (resolve): {exc}", "WARN")
            return {"uid": uid, "allowed": False,
                    "status": {"code": 403,
                               "message": f"rhiza identity resolution failed: {exc}"}}
        if identity is None:
            return {"uid": uid, "allowed": True}
        cluster_id, node_id = identity
        for op_id, barrier in self.barriers.items_snapshot():
            if _barrier_matches_pod(barrier["spec"], cluster_id, node_id):
                scope = barrier["spec"]["scope"]
                return {"uid": uid, "allowed": False,
                        "status": {"code": 403,
                                   "message": f"fenced {scope}: cluster={cluster_id} node={node_id} op={op_id}"}}
        return {"uid": uid, "allowed": True}


# ── TLS cert generation ─────────────────────────────────────────────────────


def _gen_certs(cert_dir, gateway_ip):
    d = Path(cert_dir)
    ca_key, ca_crt = d / "ca.key", d / "ca.crt"
    srv_key, srv_crt, srv_csr = d / "server.key", d / "server.crt", d / "server.csr"
    cnf = d / "san.cnf"
    cnf.write_text(
        f"[req]\nreq_extensions = v3_req\ndistinguished_name = req_dn\n"
        f"[req_dn]\n[v3_req]\nbasicConstraints = CA:FALSE\n"
        f"keyUsage = digitalSignature, keyEncipherment\n"
        f"extendedKeyUsage = serverAuth\nsubjectAltName = @alt_names\n"
        f"[alt_names]\nIP.1 = {gateway_ip}\n")
    for args in [
        ["openssl", "genrsa", "-out", str(ca_key), "2048"],
        ["openssl", "req", "-new", "-x509", "-key", str(ca_key),
         "-out", str(ca_crt), "-days", "1", "-subj", "/CN=chaos-fence-ca"],
        ["openssl", "genrsa", "-out", str(srv_key), "2048"],
        ["openssl", "req", "-new", "-key", str(srv_key),
         "-out", str(srv_csr), "-subj", f"/CN={gateway_ip}",
         "-config", str(cnf)],
        ["openssl", "x509", "-req", "-in", str(srv_csr),
         "-CA", str(ca_crt), "-CAkey", str(ca_key), "-CAcreateserial",
         "-out", str(srv_crt), "-days", "1",
         "-extensions", "v3_req", "-extfile", str(cnf)],
    ]:
        subprocess.run(args, check=True, capture_output=True)
    return ca_crt, srv_crt, srv_key


def _kind_gateway(node):
    r = subprocess.run(
        ["docker", "inspect", node,
         "--format", "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}"],
        capture_output=True, text=True, timeout=10)
    gw = r.stdout.strip()
    if not gw:
        raise RuntimeError(f"no gateway for {node}")
    return gw


def _install_vwc(gateway_ip, port, ca_cert_path, context):
    ca_b64 = base64.b64encode(ca_cert_path.read_bytes()).decode()
    vwc = {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": WEBHOOK_NAME},
        "webhooks": [{
            "name": f"fence.{CR_GROUP}",
            "admissionReviewVersions": ["v1"],
            "failurePolicy": "Fail",
            "sideEffects": "None",
            "namespaceSelector": {"matchLabels": {WEBHOOK_LABEL: "true"}},
            "clientConfig": {
                "caBundle": ca_b64,
                "url": f"https://{gateway_ip}:{port}{WEBHOOK_PATH}",
            },
            "rules": [{
                "apiGroups": [""], "apiVersions": ["v1"],
                "resources": ["pods"], "operations": ["CREATE"],
            }],
            "timeoutSeconds": 10,
        }],
    }
    r = subprocess.run(
        ["kubectl", "--context", context, "apply", "-f", "-"],
        input=json.dumps(vwc).encode(),
        capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(f"VWC install failed: {r.stderr}")
    _log(f"VWC installed: {WEBHOOK_NAME}")


# ── KindFencer ───────────────────────────────────────────────────────────────


class KindFencer:
    def __init__(self, chaos, *, webhook_port=WEBHOOK_PORT):
        self.chaos = chaos
        self.enabled = False
        self._barriers = BarrierStore(str(chaos.out / "barriers.json"))
        self._stop = threading.Event()
        self._proxy_proc = None
        self._webhook_server = None
        self._webhook_thread = None
        self._poll_thread = None
        self._cert_dir = None
        self._webhook_port = webhook_port

    def start(self):
        self._barriers.load()
        self._start_proxy()
        self._setup_webhook()
        self._label_namespace()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        _log("KindFencer started")

    def close(self):
        self._stop.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=5)
            if self._poll_thread.is_alive():
                raise RuntimeError("poll thread failed to stop")
        self._stop_webhook()
        self._stop_proxy()
        self._barriers.save()
        _log("KindFencer stopped")

    # ── proxy ────────────────────────────────────────────────────────────

    def _start_proxy(self):
        self._proxy_proc = subprocess.Popen(
            ["kubectl", "--context", self.chaos.context,
             "proxy", f"--port={PROXY_PORT}", "--disable-filter"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(40):
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{PROXY_PORT}/version", timeout=2)
                _log(f"kubectl proxy on {PROXY_PORT}")
                return
            except Exception:
                time.sleep(0.25)
        raise RuntimeError("kubectl proxy did not start")

    def _stop_proxy(self):
        if self._proxy_proc:
            self._proxy_proc.terminate()
            self._proxy_proc.wait(timeout=5)
            self._proxy_proc = None

    def _proxy_request(self, method, path, body=None):
        url = f"http://127.0.0.1:{PROXY_PORT}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode(errors="replace")
            if e.code == 404:
                return None
            raise RuntimeError(
                f"proxy {method} {path}: {e.code} {err_body[:300]}")

    def _proxy_request_raw(self, method, path, body=None):
        """Returns (status_code, body_bytes) without raising on non-2xx."""
        url = f"http://127.0.0.1:{PROXY_PORT}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    # ── webhook ──────────────────────────────────────────────────────────

    def _setup_webhook(self):
        gw = _kind_gateway(self.chaos.node)
        self._cert_dir = tempfile.mkdtemp(prefix="rhiza-fence-")
        ca, cert, key = _gen_certs(self._cert_dir, gw)
        handler = type("H", (_FenceHandler,),
                       {"chaos": self.chaos, "barriers": self._barriers})
        srv = ThreadingHTTPServer(("0.0.0.0", self._webhook_port), handler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(cert), str(key))
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        self._webhook_server = srv
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        self._webhook_thread = t
        _install_vwc(gw, self._webhook_port, ca, self.chaos.context)

    def _verify_webhook_dryrun(self, barrier_spec):
        """API dryRun Pod CREATE; must return 403 with our fenced message."""
        ns = barrier_spec["namespace"]
        source_cid = barrier_spec["sourceClusterID"]
        scope = barrier_spec["scope"]
        targets = barrier_spec.get("targets", [])
        node_id = targets[0]["nodeID"] if targets else "fence-verify"
        env = [{"name": "RHIZA_CLUSTER_ID", "value": source_cid},
               {"name": "RHIZA_NODE_ID", "value": node_id}]
        pod_body = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "fence-dryrun", "namespace": ns},
            "spec": {"restartPolicy": "Never",
                     "containers": [{"name": "rhiza", "image": "busybox",
                                     "env": env}]},
        }
        create_body = {"apiVersion": "v1", "kind": "Pod", **pod_body}
        path = f"/api/v1/namespaces/{ns}/pods?dryRun=All"
        status, raw = self._proxy_request_raw("POST", path, create_body)
        if status != 403:
            raise AssertionError(
                f"webhook dryRun expected 403, got {status}: {raw[:200]}")
        msg = raw.decode(errors="replace")
        op_id = barrier_spec["operationID"]
        if "fenced" not in msg or op_id not in msg:
            raise AssertionError(
                f"webhook dryRun 403 missing fenced+opID message: {msg[:200]}")
        _log(f"webhook dryRun verified: 403 with fenced message")

    def _stop_webhook(self):
        if self._webhook_server:
            self._webhook_server.shutdown()
            self._webhook_server.server_close()
            self._webhook_server = None
        if self._cert_dir:
            shutil.rmtree(self._cert_dir, ignore_errors=True)
            self._cert_dir = None
        _log("webhook server stopped (VWC preserved)")

    def _label_namespace(self):
        r = subprocess.run(
            ["kubectl", "--context", self.chaos.context,
             "label", "namespace", self.chaos.ns,
             f"{WEBHOOK_LABEL}=true", "--overwrite"],
            capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            raise RuntimeError(f"namespace label failed: {r.stderr}")
        _log(f"namespace {self.chaos.ns} labeled {WEBHOOK_LABEL}=true")

    # ── events ───────────────────────────────────────────────────────────

    def _event(self, name, **kw):
        entry = {"event": name, "time": time.time(), **kw}
        path = self.chaos.out / "executor-events.jsonl"
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        _log(json.dumps(entry))

    # ── polling ──────────────────────────────────────────────────────────

    def _poll_loop(self):
        while not self._stop.is_set():
            if self.enabled:
                try:
                    self._reconcile_all()
                except Exception as exc:
                    _log(f"reconcile cycle: {exc}", "ERROR")
            self._stop.wait(POLL_SEC)

    def _reconcile_all(self):
        raw = self.chaos.k("get", CR_PLURAL, "-A", "-o", "json")
        items = json.loads(raw).get("items", [])
        for cr in items:
            if self._stop.is_set():
                break
            try:
                self._reconcile_cr(cr)
            except Exception as exc:
                name = cr.get("metadata", {}).get("name", "?")
                _log(f"reconcile {name}: {exc}", "ERROR")

    def _reconcile_cr(self, cr):
        if not self.enabled:
            return
        spec = cr["spec"]
        ns = spec["namespace"]
        if ns != self.chaos.ns:
            raise ValueError(
                f"namespace mismatch: {ns} != {self.chaos.ns}")
        op_id = spec["operationID"]
        request_hash = _compute_request_hash(spec)

        barrier = self._barriers.get(op_id)
        if barrier is None:
            barrier = {"operationID": op_id, "spec": spec,
                       "requestHash": request_hash,
                       "uid_set": [], "proof": None}
            self._barriers.set(op_id, barrier)
            self._barriers.save()
            self._event("barrier_active", operationID=op_id)
        elif barrier["requestHash"] != request_hash:
            raise ValueError(
                f"spec changed for same binding/op: {op_id}")

        if not barrier.get("proof"):
            sts_obj = self.chaos.get("statefulset", spec["statefulSet"])
            actual_uid = sts_obj["metadata"]["uid"]
            if actual_uid != spec["statefulSetUID"]:
                raise ValueError(
                    f"STS UID mismatch: {actual_uid} != {spec['statefulSetUID']}")

            uid_set = self._capture_pods(spec)
            existing_uids = self._barriers.get_uid_set(op_id)
            merged_uids = existing_uids | uid_set
            self._barriers.update(op_id, uid_set=sorted(merged_uids))
            self._barriers.save()
            self._event("uids_captured", operationID=op_id,
                        count=len(merged_uids))

            self._verify_webhook_dryrun(spec)

            self._delete_pods(spec, merged_uids)

            self._wait_pods_gone(merged_uids)
            self._wait_no_recreation(spec, merged_uids)


            self._check_no_orphaned_containers(spec, merged_uids)
            self._quiesce_storage()

            proof = {
                "operationID": op_id,
                "sourceClusterID": spec["sourceClusterID"],
                "bindingUID": spec["bindingUID"],
                "requestHash": request_hash,
                "proofID": secrets.token_hex(16),
                "processesTerminated": bool(merged_uids),
                "recreationBlocked": True,
                "storageQuiesced": True,
            }
            self._barriers.update(op_id, proof=proof)
            self._barriers.save()
            self._event("proof_generated", proofID=proof["proofID"])

        self._submit_proof(cr, barrier)

    def _submit_proof(self, cr, barrier):
        ns = barrier["spec"]["namespace"]
        name = cr["metadata"]["name"]
        fresh = json.loads(self.chaos.k(
            "get", CR_PLURAL, name, "-n", ns, "-o", "json"))
        if _compute_request_hash(fresh["spec"]) != barrier["requestHash"]:
            raise ValueError(f"spec changed before status: {name}")
        existing = (fresh.get("status") or {}).get("proof", {})
        if existing.get("proofID") == barrier["proof"]["proofID"]:
            return
        fresh["status"] = {"proof": barrier["proof"]}
        path = (f"/apis/{CR_GROUP}/{CR_VER}/namespaces/{ns}/{CR_PLURAL}"
                f"/{name}/status")
        self._proxy_request("PUT", path, fresh)
        _log(f"proof submitted: {barrier['proof']['proofID']} for {name}")

    # ── pod capture / delete ─────────────────────────────────────────────

    def _capture_pods(self, spec):
        ns = spec["namespace"]
        raw = self.chaos.k("get", "pods", "-n", ns, "-o", "json")
        all_pods = json.loads(raw).get("items", [])
        discovered_uids = set()
        for pod in all_pods:
            uid = pod["metadata"]["uid"]
            ctrs = pod["spec"].get("containers", [])
            rhiza_ctr = next((c for c in ctrs if c["name"] == "rhiza"), None)
            if not rhiza_ctr:
                continue
            try:
                ident = _resolve_rhiza_identity(
                    self.chaos, ns,
                    {"containers": [rhiza_ctr]},
                    pod["metadata"]["name"])
            except Exception:
                raise
            if ident is None:
                continue
            if _barrier_matches_pod(spec, ident[0], ident[1]):
                discovered_uids.add(uid)
        advisory_uids = {t["podUID"] for t in spec.get("targets", [])
                         if t.get("podUID")}
        return advisory_uids | discovered_uids

    def _delete_pods(self, spec, uid_set):
        ns = spec["namespace"]
        raw = self.chaos.k("get", "pods", "-n", ns, "-o", "json")
        pods = json.loads(raw).get("items", [])
        for pod in pods:
            uid = pod["metadata"]["uid"]
            if uid not in uid_set:
                continue
            name = pod["metadata"]["name"]
            body = {"apiVersion": "v1", "kind": "DeleteOptions",
                    "preconditions": {"uid": uid}}
            path = f"/api/v1/namespaces/{ns}/pods/{name}"
            try:
                self._proxy_request("DELETE", path, body)
            except Exception as exc:
                _log(f"delete {name}: {exc}", "WARN")

    def _wait_pods_gone(self, uid_set):
        deadline = time.monotonic() + KILL_TIMEOUT
        while time.monotonic() < deadline:
            if self._stop.is_set():
                raise RuntimeError("stopped during pod termination wait")
            if all(self.chaos.runtime_gone(uid) for uid in uid_set):
                _log(f"all {len(uid_set)} pods CRI-verified gone")
                return
            time.sleep(0.5)
        raise AssertionError(f"pods not gone within {KILL_TIMEOUT}s")

    def _wait_no_recreation(self, spec, uid_set):
        ns = spec["namespace"]
        deadline = time.monotonic() + KILL_TIMEOUT
        while time.monotonic() < deadline:
            if self._stop.is_set():
                raise RuntimeError("stopped during recreation denial wait")
            time.sleep(1.0)
            raw = self.chaos.k("get", "pods", "-n", ns, "-o", "json")
            pods = json.loads(raw).get("items", [])
            for pod in pods:
                if pod["metadata"]["uid"] in uid_set:
                    continue
                ctrs = pod["spec"].get("containers", [])
                rhiza_ctr = next((c for c in ctrs if c["name"] == "rhiza"), None)
                if not rhiza_ctr:
                    continue
                try:
                    ident = _resolve_rhiza_identity(
                        self.chaos, ns,
                        {"containers": [rhiza_ctr]},
                        pod["metadata"]["name"])
                except Exception:
                    continue
                if ident and _barrier_matches_pod(spec, ident[0], ident[1]):
                    raise AssertionError(
                        f"recreation detected: {pod['metadata']['name']}")
        _log("recreation denial confirmed")


    def _check_no_orphaned_containers(self, spec, verified_uids):
        """CRI-vs-API check: no running container whose Pod is gone from API and not in verified set."""
        ns = spec["namespace"]
        data = json.loads(self.chaos.command(
            ["docker", "exec", self.chaos.node, "crictl", "ps", "-a", "-o", "json"]))
        running = [c for c in data.get("containers", [])
                   if c.get("state") == "CONTAINER_RUNNING"
                   and c.get("labels", {}).get("io.kubernetes.pod.namespace") == ns]
        if not running:
            return
        raw = self.chaos.k("get", "pods", "-n", ns, "-o", "json")
        api_uids = {p["metadata"]["uid"] for p in json.loads(raw).get("items", [])}
        for c in running:
            pod_uid = c.get("labels", {}).get("io.kubernetes.pod.uid", "")
            if not pod_uid:
                continue
            if pod_uid in api_uids or pod_uid in verified_uids:
                continue
            raise AssertionError(
                f"orphaned CRI container: Pod UID {pod_uid} running but absent from API and verified set")
    # ── storage quiesce ──────────────────────────────────────────────────

    def _quiesce_storage(self):
        ns = self.chaos.ns
        dep = self.chaos.get("deployment", MINIO_DEPLOY)
        selector = dep["spec"]["selector"]["matchLabels"]
        sel = ",".join(f"{k}={v}" for k, v in selector.items())
        raw = self.chaos.k("get", "pods", "-n", ns, "-l", sel, "-o", "json")
        pods = json.loads(raw).get("items", [])
        if not pods:
            raise AssertionError("no MinIO pod found")
        pod = pods[0]
        pod_name = pod["metadata"]["name"]
        pod_uid = pod["metadata"]["uid"]
        statuses = pod["status"].get("containerStatuses", [])
        minio_st = next(
            (s for s in statuses
             if s["name"] == "minio"
             and s.get("state", {}).get("running")),
            None)
        if not minio_st:
            raise AssertionError("MinIO container not running")
        old_cid = minio_st["containerID"].split("://", 1)[1]
        self.chaos.command(
            ["docker", "exec", self.chaos.node,
             "crictl", "stop", old_cid])
        _log(f"MinIO stopped: {old_cid}")

        deadline = time.monotonic() + MINIO_TIMEOUT
        new_cid = None
        while time.monotonic() < deadline:
            if self._stop.is_set():
                raise RuntimeError("stopped during MinIO restart wait")
            try:
                p = self.chaos.get("pod", pod_name)
                if p["metadata"]["uid"] != pod_uid:
                    raise AssertionError("MinIO Pod UID changed")
                for s in p["status"].get("containerStatuses", []):
                    if (s["name"] == "minio"
                            and s.get("state", {}).get("running")):
                        cid = s["containerID"].split("://", 1)[1]
                        if cid != old_cid:
                            new_cid = cid
                            break
            except AssertionError:
                raise
            except Exception:
                pass
            if new_cid:
                break
            time.sleep(1.0)
        if not new_cid:
            raise AssertionError("MinIO did not restart")
        _log(f"MinIO restarted: {new_cid}")

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self._stop.is_set():
                raise RuntimeError("stopped during MinIO ready wait")
            p = self.chaos.get("pod", pod_name)
            conds = p["status"].get("conditions", [])
            if any(c["type"] == "Ready" and c["status"] == "True"
                   for c in conds):
                _log("MinIO Pod Ready")
                return
            time.sleep(1.0)
        raise AssertionError("MinIO Pod not Ready after restart")
