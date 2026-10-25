import unittest
from copy import deepcopy
from unittest import mock

from operator_recovery import Chaos


class VerifyDataTest(unittest.TestCase):
    def make_chaos(self, response):
        chaos = object.__new__(Chaos)
        chaos.acks = {0, 337}
        events = []
        chaos.event = lambda name, **fields: events.append((name, fields))
        chaos.execute = lambda *args: None
        chaos.http = lambda pod, route, body: response(pod, route, body)
        return chaos, events

    def test_ack_added_during_query_is_outside_expected_snapshot(self):
        query = {}
        chaos, events = self.make_chaos(lambda pod, route, body: {})

        def http(pod, route, body):
            query.update(pod=pod, route=route, body=body)
            chaos.acks.add(338)
            return {"rows": [[0], [337]], "applied_slot": 900, "consensus_tip": 900}

        chaos.http = http

        self.assertEqual(2, Chaos.verify_data(chaos, "learner"))
        self.assertEqual("/sql/query", query["route"])
        self.assertEqual("linearizable", query["body"]["consistency"])
        self.assertEqual([338], sorted(chaos.acks - {0, 337}))
        self.assertEqual({"expected_count": 2, "found_count": 2, "missing": [],
                          "consistency": "linearizable", "applied_slot": 900,
                          "consensus_tip": 900, "pod": "learner"}, events[0][1])

    def test_lagging_query_fails_with_read_position_evidence(self):
        chaos, events = self.make_chaos(lambda pod, route, body: {
            "rows": [[0]], "applied_slot": 899, "consensus_tip": 900,
        })

        with self.assertRaisesRegex(AssertionError, r"missing acknowledged rows: \[337\].*applied_slot=899"):
            Chaos.verify_data(chaos, "learner")
        self.assertEqual([337], events[0][1]["missing"])
        self.assertEqual(899, events[0][1]["applied_slot"])
        self.assertEqual(900, events[0][1]["consensus_tip"])


class VerifyIdempotentWriteTest(unittest.TestCase):
    def test_replays_same_request_and_requires_exact_linearizable_row(self):
        chaos = object.__new__(Chaos)
        calls = []
        queries = []
        chaos.execute = lambda *args: calls.append(args)
        chaos.http = lambda pod, route, body: queries.append((pod, route, body)) or {
            "rows": [[10002, "recovered"]],
        }

        rows = Chaos.verify_idempotent_write(
            chaos, "rhiza-0", "new-generation-write",
            "INSERT INTO chaos_items VALUES (10002, 'recovered')", (10002, "recovered"))

        self.assertEqual([
            ("rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'recovered')"),
            ("rhiza-0", "new-generation-write", "INSERT INTO chaos_items VALUES (10002, 'recovered')"),
        ], calls)
        self.assertEqual([("rhiza-0", "/sql/query", {
            "sql": "SELECT id, value FROM chaos_items WHERE id = ?",
            "args": [10002], "consistency": "linearizable",
        })], queries)
        self.assertEqual([[10002, "recovered"]], rows)

    def test_rejects_missing_duplicate_or_wrong_payload_readback(self):
        for rows in (
            [],
            [[10002, "recovered"], [10002, "recovered"]],
            [[10002, "wrong"]],
            [[10002.0, "recovered"]],
        ):
            with self.subTest(rows=rows):
                chaos = object.__new__(Chaos)
                chaos.execute = lambda *args: None
                chaos.http = lambda pod, route, body: {"rows": rows}
                with self.assertRaisesRegex(AssertionError, "idempotent write readback differs"):
                    Chaos.verify_idempotent_write(
                        chaos, "rhiza-0", "new-generation-write",
                        "INSERT INTO chaos_items VALUES (10002, 'recovered')", (10002, "recovered"))


class LegacyMembershipRejectionTest(unittest.TestCase):
    blocked_reason = "new operator-managed membership changes are disabled; ordinary Pod recovery is engine-owned"

    def make_chaos(self, *, reason=None, after_status=None, after_rows=None, after_cr_status=None):
        chaos = object.__new__(Chaos)
        chaos.ns = "fixture"
        membership = {
            "rhiza-0": {"node_id": "rhiza-0", "cluster_id": "source", "config_id": 3,
                        "voters": ["rhiza-0", "rhiza-1", "rhiza-2"], "wal_identity": "wal-0",
                        "voting": True, "pending": False},
            "rhiza-1": {"node_id": "rhiza-1", "cluster_id": "source", "config_id": 3,
                        "voters": ["rhiza-0", "rhiza-1", "rhiza-2"], "wal_identity": "wal-1",
                        "voting": True, "pending": False},
        }
        status_reads = {pod: 0 for pod in membership}
        def status(pod):
            status_reads[pod] += 1
            source = after_status if after_status is not None and status_reads[pod] > 1 else membership
            return deepcopy(source[pod])
        chaos.status = status
        rows = [[0, "seed"], [4, "acknowledged"]]
        row_reads = 0
        def http(*args):
            nonlocal row_reads
            row_reads += 1
            result = after_rows if after_rows is not None and row_reads > 1 else rows
            return {"rows": deepcopy(result)}
        chaos.http = http
        chaos.apply = lambda resource: setattr(chaos, "request", resource)
        blocked = {"phase": "Blocked", "message": reason or self.blocked_reason, "membership": {}}
        cr_reads = 0
        def cr(name):
            nonlocal cr_reads
            cr_reads += 1
            status = after_cr_status if after_cr_status is not None and cr_reads >= 3 else blocked
            return {"status": deepcopy(status)}
        chaos.cr = cr
        chaos.wait = lambda label, predicate, *args, **kwargs: self.assertTrue(predicate(), label)
        chaos.runtime_gone = lambda uid: True
        chaos.event = lambda name, **fields: setattr(chaos, "last_event", (name, fields))
        return chaos

    def reject(self, chaos):
        with mock.patch("operator_recovery.time.sleep"):
            chaos.reject_legacy_replacement(
                "replace-2", "rhiza-2", "old-pod-uid", "old-wal",
                ["rhiza-0", "rhiza-1"], "learner-a")

    def test_requires_exact_block_reason_and_preserves_membership_and_rows(self):
        chaos = self.make_chaos()
        self.reject(chaos)

        membership = chaos.request["spec"]["membership"]
        self.assertTrue(membership["fence"]["confirmed"])
        self.assertEqual("legacy_membership_request_rejected_only", chaos.last_event[0])
        self.assertEqual("ordinary_recovery_is_engine_owned", chaos.last_event[1]["reason"])

    def test_rejects_other_block_reasons(self):
        chaos = self.make_chaos(reason="confirmed membership fence must bind the removed voter")
        with self.assertRaisesRegex(AssertionError, "confirmed membership fence"):
            self.reject(chaos)

    def test_rejects_membership_or_data_changes(self):
        baseline = {
            "rhiza-0": {"node_id": "rhiza-0", "cluster_id": "source", "config_id": 3,
                        "voters": ["rhiza-0", "rhiza-1", "rhiza-2"], "wal_identity": "wal-0",
                        "voting": True, "pending": False},
            "rhiza-1": {"node_id": "rhiza-1", "cluster_id": "source", "config_id": 3,
                        "voters": ["rhiza-0", "rhiza-1", "rhiza-2"], "wal_identity": "wal-1",
                        "voting": True, "pending": False},
        }
        changed = deepcopy(baseline)
        changed["rhiza-0"]["config_id"] = 4
        with self.assertRaisesRegex(AssertionError, "membership changed"):
            self.reject(self.make_chaos(after_status=changed))
        with self.assertRaisesRegex(AssertionError, "data changed"):
            self.reject(self.make_chaos(after_rows=[[0, "seed"]]))

    def test_rejects_late_membership_journal_mutation(self):
        changed = {"phase": "Blocked", "message": self.blocked_reason,
                   "membership": {"removeRequest": {"operationID": "unexpected"}}}
        with self.assertRaisesRegex(AssertionError, "removeRequest"):
            self.reject(self.make_chaos(after_cr_status=changed))


if __name__ == "__main__":
    unittest.main(verbosity=2)
