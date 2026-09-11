#!/usr/bin/env python3
"""Unittest tests for kind_fencer.py."""
import json, os, sys, tempfile, threading, unittest
import urllib.error, urllib.parse, urllib.request
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(__file__))
from kind_fencer import (
    BarrierStore, _barrier_matches_pod, _FenceHandler, KindFencer,
    _resolve_rhiza_identity, CR_PLURAL, WEBHOOK_PATH, WEBHOOK_PORT,
)


def _make_barrier(scope, source="cluster-a", targets=None, proof=None):
    if targets is None:
        targets = [{"nodeID": "rhiza-0", "pod": "rhiza-0",
                     "podUID": "u0", "walIdentity": "w0"}]
    return {"spec": {"scope": scope, "sourceClusterID": source,
                     "operationID": "op-test", "namespace": "ns",
                     "statefulSet": "sts", "statefulSetUID": "su",
                     "logicalID": "L", "bindingUID": "B",
                     "targets": targets},
            "requestHash": "h", "uid_set": [], "proof": proof}


class TestScopeMatching(unittest.TestCase):
    def test_voter_both_source_and_nodeid(self):
        b = _make_barrier("Voter")
        self.assertTrue(_barrier_matches_pod(b["spec"], "cluster-a", "rhiza-0"))
        self.assertFalse(_barrier_matches_pod(b["spec"], "cluster-a", "rhiza-2"))
        self.assertFalse(_barrier_matches_pod(b["spec"], "cluster-b", "rhiza-0"))

    def test_generation_source_cluster(self):
        b = _make_barrier("Generation")
        self.assertTrue(_barrier_matches_pod(b["spec"], "cluster-a", "any"))
        self.assertFalse(_barrier_matches_pod(b["spec"], "cluster-b", "any"))

    def test_future_generation_passes_voter(self):
        b = _make_barrier("Voter", source="old")
        self.assertFalse(_barrier_matches_pod(b["spec"], "new", "rhiza-0"))

    def test_future_generation_passes_generation(self):
        b = _make_barrier("Generation", source="old")
        self.assertFalse(_barrier_matches_pod(b["spec"], "new", "any"))


class TestCompletedBarrierDenies(unittest.TestCase):
    def test_handler_denies_after_proof_reload(self):
        """Barrier saved to disk, loaded fresh, completed proof still denies."""
        with tempfile.TemporaryDirectory() as td:
            bs = BarrierStore(str(Path(td) / "barriers.json"))
            bs.set("op-test", _make_barrier("Voter", proof={"proofID": "done"}))
            bs.save()

            fresh = BarrierStore(str(Path(td) / "barriers.json"))
            fresh.load()
            self.assertEqual(fresh.get("op-test")["proof"]["proofID"], "done")

            chaos = MagicMock(ns="ns")
            handler = _FenceHandler.__new__(_FenceHandler)
            handler.chaos, handler.barriers = chaos, fresh
            handler.path = "/fence"
            handler.requestline = "POST /fence HTTP/1.1"
            handler.headers, handler.wfile = {}, MagicMock()
            handler.request_version = "HTTP/1.1"
            handler.client_address, handler.server = ("", 0), MagicMock()

            review = {"request": {"uid": "u", "operation": "CREATE",
                                  "object": {"metadata": {"name": "rhiza-0",
                                                         "namespace": "ns"},
                                             "spec": {"containers": [
                                                 {"name": "rhiza", "env": [
                                                     {"name": "RHIZA_CLUSTER_ID",
                                                      "value": "cluster-a"},
                                                     {"name": "RHIZA_NODE_ID",
                                                      "value": "rhiza-0"}]}]}}}}
            resp = handler._handle(review)
            self.assertFalse(resp["allowed"])
            self.assertIn("fenced", resp["status"]["message"])
            self.assertIn("op-test", resp["status"]["message"])


class TestDisabledReconcile(unittest.TestCase):
    def test_disabled_skips_cr(self):
        chaos = MagicMock(ns="ns", context="kind-t", node="t-node",
                          out=Path(tempfile.mkdtemp()), stop=threading.Event())
        fencer = KindFencer(chaos)
        fencer._reconcile_cr({"metadata": {"name": "x"}, "spec": {
            "namespace": "ns", "operationID": "op", "logicalID": "L",
            "bindingUID": "B", "statefulSet": "s", "statefulSetUID": "u",
            "sourceClusterID": "src", "scope": "Voter", "targets": []}})
        chaos.k.assert_not_called()
        self.assertIsNone(fencer._barriers.get("op"))


class TestStorageFailureInReconcile(unittest.TestCase):
    def test_reconcile_cr_storage_fails_no_proof(self):
        """_reconcile_cr through _quiesce_storage failure: no proof, no status."""
        CR = {"metadata": {"name": "f1", "uid": "cr-uid", "resourceVersion": "1"},
              "spec": {"logicalID": "L", "bindingUID": "B", "namespace": "ns",
                       "statefulSet": "sts", "statefulSetUID": "sts-uid",
                       "sourceClusterID": "cluster-a", "operationID": "op1",
                       "scope": "Voter",
                       "targets": [{"nodeID": "rhiza-0", "pod": "rhiza-0",
                                     "podUID": "u0", "walIdentity": "w0"}]}}
        tmp = Path(tempfile.mkdtemp())
        chaos = MagicMock(ns="ns", context="kind-t", node="t-node",
                          out=tmp, stop=threading.Event())

        def fake_k(*a, data=None):
            cmd = " ".join(a)
            if "statefulset" in cmd:
                return json.dumps({"metadata": {"uid": "sts-uid"}})
            return json.dumps({"items": []})

        chaos.k = fake_k
        chaos.get = lambda k, n: (
            {"metadata": {"uid": "sts-uid"}} if k == "statefulset"
            else {"spec": {"selector": {"matchLabels": {"app": "rhiza-minio"}}},
                  "metadata": {"name": "rhiza-minio", "uid": "mu"},
                  "status": {"containerStatuses": []}})
        chaos.command = lambda a, d=None: json.dumps({"containers": []})
        chaos.runtime_gone = lambda uid: True

        fencer = KindFencer(chaos)
        fencer.enabled = True
        with patch.object(fencer, "_verify_webhook_dryrun"),              patch("kind_fencer.KILL_TIMEOUT", 0.01),              patch("kind_fencer.MINIO_TIMEOUT", 0.01):
            with self.assertRaises(AssertionError) as cm:
                fencer._reconcile_cr(CR)
        self.assertIn("no MinIO", str(cm.exception))
        barrier = fencer._barriers.get("op1")
        self.assertIsNotNone(barrier)
        self.assertIsNone(barrier.get("proof"))
        events = [json.loads(l) for l in
                  (tmp / "executor-events.jsonl").read_text().splitlines() if l]
        self.assertTrue(any(e["event"] == "barrier_active" for e in events))
        self.assertFalse(any(e["event"] == "proof_generated" for e in events))


class TestOrphanedContainerCheck(unittest.TestCase):
    def _chaos(self, containers, pods=None):
        c = MagicMock(ns="ns")
        c.command.return_value = json.dumps({"containers": containers})
        c.k.return_value = json.dumps(
            {"items": [{"metadata": {"uid": u}} for u in (pods or [])]})
        return c

    def test_verified_uid_still_running_raises(self):
        uid = "v-uid-1"
        fencer = KindFencer(self._chaos([
            {"id": "c1", "state": "CONTAINER_RUNNING",
             "labels": {"io.kubernetes.pod.namespace": "ns",
                        "io.kubernetes.pod.uid": uid}}]))
        with self.assertRaises(AssertionError) as cm:
            fencer._check_no_orphaned_containers({"namespace": "ns"}, {uid})
        self.assertIn("still running", str(cm.exception))

    def test_orphan_absent_from_api_raises(self):
        uid = "o-uid-2"
        fencer = KindFencer(self._chaos([
            {"id": "c2", "state": "CONTAINER_RUNNING",
             "labels": {"io.kubernetes.pod.namespace": "ns",
                        "io.kubernetes.pod.uid": uid}}]))
        with self.assertRaises(AssertionError) as cm:
            fencer._check_no_orphaned_containers({"namespace": "ns"}, set())
        self.assertIn("orphaned", str(cm.exception))

    def test_running_uid_in_api_passes(self):
        uid = "l-uid-3"
        fencer = KindFencer(self._chaos(
            [{"id": "c3", "state": "CONTAINER_RUNNING",
              "labels": {"io.kubernetes.pod.namespace": "ns",
                         "io.kubernetes.pod.uid": uid}}], [uid]))
        fencer._check_no_orphaned_containers({"namespace": "ns"}, set())


class TestWebhookQueryParams(unittest.TestCase):
    def test_post_with_query_string_resolves(self):
        """Real HTTPServer: POST /fence?timeout=10s returns 200 with same UID."""
        barrier = _make_barrier("Voter")
        store = BarrierStore("/dev/null")
        store.set("op-test", barrier)
        chaos = MagicMock(ns="ns")
        handler_type = type("H", (_FenceHandler,),
                            {"chaos": chaos, "barriers": store})
        srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_type)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            review = {"apiVersion": "admission.k8s.io/v1",
                      "kind": "AdmissionReview",
                      "request": {"uid": "test-uid-qs",
                                  "kind": {"group": "", "version": "v1",
                                           "kind": "Pod"},
                                  "operation": "CREATE",
                                  "object": {
                                      "metadata": {"name": "infra-pod",
                                                   "namespace": "other"},
                                      "spec": {"containers": [
                                          {"name": "web", "image": "nginx"}]}}}}
            url = f"http://127.0.0.1:{port}{WEBHOOK_PATH}?timeout=10s"
            data = json.dumps(review).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = json.loads(resp.read())
            self.assertEqual(resp.status, 200)
            self.assertEqual(body["response"]["uid"], "test-uid-qs")
            self.assertTrue(body["response"]["allowed"])
        finally:
            srv.shutdown()
            srv.server_close()


class TestInstallVwcInputEncoding(unittest.TestCase):
    def test_install_vwc_passes_string_input(self):
        """Regression: input must be str when text=True, not bytes."""
        tmp = Path(tempfile.mkdtemp())
        ca = tmp / "ca.crt"
        ca.write_text("fake-ca")
        captured = {}

        def fake_run(argv, **kw):
            captured["input"] = kw.get("input")
            captured["text"] = kw.get("text")
            m = MagicMock()
            m.returncode = 0
            return m

        with patch("kind_fencer.subprocess.run", fake_run):
            from kind_fencer import _install_vwc
            _install_vwc("127.0.0.1", 9443, ca, "kind-test")
        self.assertIs(captured["text"], True)
        self.assertIsInstance(captured["input"], str)


if __name__ == "__main__":
    unittest.main(verbosity=2)
