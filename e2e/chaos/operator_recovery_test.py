import unittest

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
