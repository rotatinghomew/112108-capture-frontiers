import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from imaging_lake import LakeEngine, LakeStore, replay

DATA = ROOT / "data" / "exchanges.jsonl"


def rows():
    return [json.loads(line) for line in DATA.read_text(encoding="utf-8").splitlines() if line.strip()]


class SampleExchangeTests(unittest.TestCase):
    def setUp(self):
        self.engine = replay(rows(), shift="sample")

    def test_sample_final_facts(self):
        facts = self.engine.facts
        self.assertEqual(facts["g9/1/cam1"]["state"], "paired")
        self.assertEqual(facts["g9/1/cam1"]["paired_frame"]["id"], "f2")
        self.assertEqual(facts["g9/1/cam1"]["paired_frame"]["unified_time"], 103)

        for target in ["g9/1/cam2", "g9/2/cam2", "g9/3/cam1", "g9/3/cam2"]:
            self.assertEqual(facts[target]["state"], "missing_confirmed", target)

    def test_upload_numbers_are_aliases_not_frame_identities(self):
        frames = [f for f in self.engine.frame_views() if f["id"] == "f2"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["rev"], 1)
        self.assertEqual(frames[0]["upload_numbers"], ["h1", "h2"])

    def test_retake_frame_is_not_original(self):
        view = self.engine.operator_view()
        self.assertEqual([f["id"] for f in view["retake_frames"]], ["rep4"])
        self.assertTrue(all(f["id"] != "rep4" for f in view["original_frames"]))
        self.assertIn("rep4", self.engine.intentions["g9/1/cam2"]["retake_frame_ids"])

    def test_intention_lifecycle_and_receipts(self):
        intentions = self.engine.intentions
        self.assertEqual(len(intentions), 5)
        self.assertEqual(
            {t for t, i in intentions.items() if i["claimable"]},
            {"g9/2/cam2", "g9/3/cam1", "g9/3/cam2"},
        )

        cam1 = intentions["g9/1/cam1"]
        self.assertEqual(cam1["status"], "accepted")
        self.assertTrue(cam1["original_recovered_after_acceptance"])
        self.assertEqual(cam1["receipt_ids"], ["o8"])

        cam2 = intentions["g9/1/cam2"]
        self.assertEqual(cam2["status"], "completed")
        self.assertEqual(cam2["receipt_ids"], ["o9", "o10"])

    def test_every_decision_exposes_both_coverages(self):
        for fact in self.engine.facts.values():
            self.assertIn("close_credential", fact["control_coverage"])
            self.assertIn("required_unified_interval", fact["camera_coverage"])
            self.assertEqual(len(fact["candidate_boundary"]), 2)


class SemanticEdgeCaseTests(unittest.TestCase):
    def test_shared_frame_is_conflict_not_first_come_assignment(self):
        engine = replay(
            [
                {"step": 1, "kind": "trigger", "body": {"boot": "b", "seq": 1, "pulse": "p", "time": 100, "channels": ["c"], "tol": 2}},
                {"step": 1, "kind": "trigger", "body": {"boot": "b", "seq": 2, "pulse": "p", "time": 101, "channels": ["c"], "tol": 2}},
                {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 20, "rev": 1}},
                {"step": 1, "kind": "frame", "body": {"camera": "c", "boot": "cb", "id": "f", "tick": 1, "pulse": "p", "rev": 1}},
            ]
        )
        self.assertEqual(engine.facts["b/1/c"]["state"], "ambiguous")
        self.assertEqual(engine.facts["b/2/c"]["state"], "ambiguous")
        self.assertEqual(len(engine.conflicts), 1)
        self.assertIn("frame_matches_multiple_triggers", engine.conflicts[0]["reasons"])
        self.assertEqual(engine.intentions, {})

    def test_global_candidate_set_forces_deterministic_pair(self):
        engine = replay(
            [
                {"step": 1, "kind": "trigger", "body": {"boot": "b", "seq": 1, "pulse": "p", "time": 101, "channels": ["c"], "tol": 0}},
                {"step": 1, "kind": "trigger", "body": {"boot": "b", "seq": 2, "pulse": "p", "time": 102, "channels": ["c"], "tol": 1}},
                {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 20, "rev": 1}},
                {"step": 1, "kind": "frame", "body": {"camera": "c", "boot": "cb", "id": "f1", "tick": 1, "pulse": "p", "rev": 1}},
                {"step": 1, "kind": "frame", "body": {"camera": "c", "boot": "cb", "id": "f2", "tick": 3, "pulse": "p", "rev": 1}},
            ]
        )
        self.assertEqual(engine.facts["b/1/c"]["paired_frame"]["id"], "f1")
        self.assertEqual(engine.facts["b/2/c"]["paired_frame"]["id"], "f2")
        self.assertEqual(engine.conflicts, [])

    def test_missing_confirmation_needs_close_sequence_and_coverage(self):
        base = [
            {"step": 1, "kind": "trigger", "body": {"boot": "b", "seq": 1, "pulse": "p", "time": 100, "channels": ["c"], "tol": 0}},
            {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 10, "rev": 1}},
        ]
        no_close = replay(base + [
            {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 100, "until": 100}},
        ])
        self.assertEqual(no_close.facts["b/1/c"]["state"], "pending")
        self.assertEqual(no_close.intentions, {})

        incomplete = replay(base + [
            {"step": 1, "kind": "close", "body": {"boot": "b", "last": 2}},
            {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 100, "until": 100}},
        ])
        self.assertEqual(incomplete.facts["b/1/c"]["state"], "pending")
        self.assertFalse(incomplete.facts["b/1/c"]["control_coverage"]["continuous"])

        confirmed = replay(base + [
            {"step": 1, "kind": "close", "body": {"boot": "b", "last": 1}},
            {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 100, "until": 100}},
        ])
        self.assertEqual(confirmed.facts["b/1/c"]["state"], "missing_confirmed")
        self.assertTrue(confirmed.intentions["b/1/c"]["claimable"])

    def test_clock_move_withdraws_unaccepted_and_issues_other_end_together(self):
        step1 = [
            {"step": 1, "kind": "trigger", "body": {"boot": "g", "seq": 1, "pulse": "p", "time": 100, "channels": ["c"], "tol": 4}},
            {"step": 1, "kind": "trigger", "body": {"boot": "g", "seq": 2, "pulse": "q", "time": 104, "channels": ["c"], "tol": 0}},
            {"step": 1, "kind": "trigger", "body": {"boot": "g", "seq": 3, "pulse": "p", "time": 109, "channels": ["c"], "tol": 4}},
            {"step": 1, "kind": "close", "body": {"boot": "g", "last": 3}},
            {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 50, "rev": 1}},
            {"step": 1, "kind": "frame", "body": {"camera": "c", "boot": "cb", "id": "f2", "tick": 7, "pulse": "p", "rev": 1}},
            {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 95, "until": 120}},
        ]
        engine = replay(step1)
        self.assertEqual(engine.facts["g/3/c"]["paired_frame"]["id"], "f2")
        self.assertTrue(engine.intentions["g/1/c"]["claimable"])

        engine.ingest_rows([
            {"step": 2, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 96, "first_tick": 0, "last_tick": 50, "rev": 2}},
            {"step": 2, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 2, "from": 95, "until": 120}},
        ])
        self.assertEqual(engine.facts["g/1/c"]["paired_frame"]["id"], "f2")
        self.assertEqual(engine.facts["g/3/c"]["state"], "missing_confirmed")
        self.assertEqual(engine.intentions["g/1/c"]["status"], "withdrawn")
        self.assertEqual(engine.intentions["g/1/c"]["withdrawal_reason"], "original_frame_paired")
        self.assertTrue(engine.intentions["g/3/c"]["claimable"])
        step2_kinds = [(e["kind"], e["target"]) for e in engine.events if e["step"] == 2]
        self.assertIn(("intention.withdrawn", "g/1/c"), step2_kinds)
        self.assertIn(("intention.issued", "g/3/c"), step2_kinds)

    def test_accepted_intention_is_preserved_when_original_returns(self):
        engine = LakeEngine()
        engine.ingest_rows([
            {"step": 1, "kind": "trigger", "body": {"boot": "g", "seq": 1, "pulse": "p", "time": 100, "channels": ["c"], "tol": 0}},
            {"step": 1, "kind": "close", "body": {"boot": "g", "last": 1}},
            {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 100, "until": 100}},
            {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 10, "rev": 1}},
            {"step": 2, "kind": "field", "body": {"target": "g/1/c", "receipt": "r1", "state": "accepted"}},
            {"step": 3, "kind": "frame", "body": {"camera": "c", "boot": "cb", "id": "f1", "tick": 0, "pulse": "p", "rev": 1}},
        ])
        intention = engine.intentions["g/1/c"]
        self.assertEqual(intention["status"], "accepted")
        self.assertTrue(intention["original_recovered_after_acceptance"])
        self.assertFalse(intention["claimable"])
        self.assertTrue(any(e["kind"] == "intention.superseded_after_acceptance" for e in engine.events))


class StoreAndConsumerTests(unittest.TestCase):
    def test_persistence_replay_and_resumable_consumer(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "lake.sqlite")
            store = LakeStore(db, shift="a")
            store.ingest_rows(rows())
            event_count = len(store.engine.events)
            intention_count = len(store.engine.intentions)
            first = store.poll_changes("reader")[0]
            store.ack("reader", first["seq"])
            store.close()

            store = LakeStore(db, shift="a")
            self.assertEqual(len(store.poll_changes("reader")), event_count - 1)
            # Re-ingesting the same exchange must not create duplicate intentions/events.
            store.ingest_rows(rows())
            self.assertEqual(len(store.engine.events), event_count)
            self.assertEqual(len(store.engine.intentions), intention_count)
            self.assertEqual(store.current_offset("reader"), first["seq"])
            store.close()

    def test_receipt_ids_are_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            store = LakeStore(str(Path(td) / "lake.sqlite"))
            store.ingest_rows([
                {"step": 1, "kind": "trigger", "body": {"boot": "g", "seq": 1, "pulse": "p", "time": 100, "channels": ["c"], "tol": 0}},
                {"step": 1, "kind": "close", "body": {"boot": "g", "last": 1}},
                {"step": 1, "kind": "coverage", "body": {"camera": "c", "boot": "cb", "clock_rev": 1, "from": 100, "until": 100}},
                {"step": 1, "kind": "clock", "body": {"camera": "c", "boot": "cb", "offset": 100, "first_tick": 0, "last_tick": 10, "rev": 1}},
            ])
            receipt = {"target": "g/1/c", "receipt": "r1", "state": "accepted"}
            store.register_receipt(receipt, step=2)
            store.register_receipt(receipt, step=2)
            self.assertEqual(store.intention("g/1/c")["receipt_ids"], ["r1"])
            store.close()


if __name__ == "__main__":
    unittest.main()
