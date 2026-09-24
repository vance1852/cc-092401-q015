from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Forbidden, InvalidState, NotFound, ValidationFailed
from robot_trials.export import ExportService
from robot_trials.jsonio import canonical_json, load_json
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        self.exports = ExportService(self.connection, self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", self.protocol)
        for batch_id in ("batch-a", "batch-b", "batch-c"):
            self.service.create_batch("operator", batch_id, "demo-delivery-v1", 1, "build-a")
            self.service.start_batch("operator", batch_id, 1)
            self.service.import_observations("operator", batch_id, f"key-{batch_id}", self.rows)
            self.service.seal_batch("stat", batch_id, 2)
            job = self.service.claim_job("worker", 60)
            analysis = self.service.complete_job("worker", job["job_id"], "stat")
            self.service.decide("approver", batch_id, analysis["analysis_id"], "approved", "满足规则")

    def tearDown(self) -> None:
        self.connection.close()

    def _drive(self, worker: str, export_id: int, root: Path) -> list[dict]:
        outcomes = []
        while True:
            outcome = self.exports.advance_export(worker, export_id, root)
            outcomes.append(outcome)
            if outcome["state"] == "succeeded":
                return outcomes

    def _completed(self, root: Path, batches=("batch-a", "batch-b", "batch-c"), shard_size: int = 2):
        job = self.exports.submit_export("auditor", batches, shard_size=shard_size)
        claimed = self.exports.claim_export("worker-1", 60)
        self.assertEqual(claimed["export_id"], job["export_id"])
        outcomes = self._drive("worker-1", job["export_id"], root)
        return job, outcomes

    def test_full_export_writes_shards_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job, outcomes = self._completed(root, shard_size=2)
            self.assertEqual(job["shard_count"], 2)
            self.assertEqual(
                [item["confirmed_shard"] for item in outcomes if "confirmed_shard" in item], [0, 1]
            )
            final = outcomes[-1]
            self.assertEqual(final["state"], "succeeded")
            shard0 = root / "export-1" / "shard-000000.jsonl"
            shard1 = root / "export-1" / "shard-000001.jsonl"
            manifest_path = root / "export-1" / "manifest.json"
            self.assertTrue(shard0.exists() and shard1.exists() and manifest_path.exists())
            records0 = [json.loads(line) for line in shard0.read_text(encoding="utf-8").splitlines()]
            records1 = [json.loads(line) for line in shard1.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([item["batch_id"] for item in records0], ["batch-a", "batch-b"])
            self.assertEqual([item["batch_id"] for item in records1], ["batch-c"])
            record = records0[0]
            self.assertEqual(record["batch"]["state"], "decided")
            self.assertEqual(len(record["protocol"]["sha256"]), 64)
            self.assertEqual(record["decision"]["decision"], "approved")
            self.assertGreaterEqual(len(record["events"]), 5)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["format"], "robot-trials-export/1")
            self.assertEqual(manifest["shard_count"], 2)
            self.assertEqual(manifest["record_count"], 3)
            self.assertEqual(manifest["shards"][0]["record_from"], 0)
            self.assertEqual(manifest["shards"][0]["record_to"], 2)
            self.assertEqual(manifest["shards"][1]["record_from"], 2)
            self.assertEqual(manifest["shards"][1]["record_to"], 3)
            self.assertEqual(len(manifest["overall_sha256"]), 64)
            view = self.exports.get_export("auditor", job["export_id"])
            self.assertEqual(view["state"], "succeeded")
            self.assertEqual(view["manifest_sha256"], manifest["overall_sha256"])
            verification = self.exports.verify_export(job["export_id"], root)
            self.assertTrue(verification["ok"] and verification["complete"])
            self.assertEqual(verification["verified_shards"], 2)

    def test_replay_same_request_returns_same_task(self) -> None:
        first = self.exports.submit_export("auditor", ["batch-b", "batch-a", "batch-a"], shard_size=2)
        second = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=2)
        self.assertEqual(first["export_id"], second["export_id"])
        self.assertEqual(second["record_count"], 2)
        self.assertEqual(
            [item["batch_id"] for item in second["batches"]], ["batch-a", "batch-b"]
        )
        count = self.connection.execute("SELECT COUNT(*) FROM export_jobs").fetchone()[0]
        self.assertEqual(count, 1)

    def test_replay_with_different_shard_size_is_new_task(self) -> None:
        first = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=2)
        second = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=3)
        self.assertNotEqual(first["export_id"], second["export_id"])

    def test_only_auditor_can_submit(self) -> None:
        with self.assertRaises(Forbidden):
            self.exports.submit_export("operator", ["batch-a"])
        with self.assertRaises(Forbidden):
            self.exports.submit_export("stat", ["batch-a"])

    def test_crash_midway_resumes_from_last_confirmed_shard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a", "batch-b", "batch-c"], shard_size=1)
            claimed = self.exports.claim_export("worker-1", 60)
            self.exports.advance_export("worker-1", claimed["export_id"], root)
            self.exports.advance_export("worker-1", claimed["export_id"], root)
            confirmed = self.connection.execute(
                "SELECT shard_index FROM export_shards ORDER BY shard_index"
            ).fetchall()
            self.assertEqual([row[0] for row in confirmed], [0, 1])
            self.clock.advance(seconds=61)
            taken_over = self.exports.claim_export("worker-2", 60)
            self.assertEqual(taken_over["export_id"], job["export_id"])
            self.assertEqual(taken_over["attempts"], 2)
            with self.assertRaises(InvalidState):
                self.exports.advance_export("worker-1", job["export_id"], root)
            outcome = self.exports.advance_export("worker-2", job["export_id"], root)
            self.assertEqual(outcome["confirmed_shard"], 2)
            final = self.exports.advance_export("worker-2", job["export_id"], root)
            self.assertEqual(final["state"], "succeeded")
            self.assertEqual(len(final["manifest_sha256"]), 64)
            verification = self.exports.verify_export(job["export_id"], root)
            self.assertTrue(verification["ok"] and verification["complete"])
            audit = [
                row[0]
                for row in self.connection.execute(
                    "SELECT event_type FROM audit_events WHERE entity_type='export' ORDER BY event_id"
                ).fetchall()
            ]
            self.assertEqual(
                audit,
                ["export.submitted", "export.shard_confirmed", "export.shard_confirmed",
                 "export.shard_confirmed", "export.completed"],
            )

    def test_failed_task_requeues_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=1)
            claimed = self.exports.claim_export("worker-1", 10)
            self.exports.advance_export("worker-1", job["export_id"], root)
            failed = self.exports.fail_export("worker-1", job["export_id"], "进程异常", retry_seconds=5)
            self.assertEqual(failed["state"], "queued")
            self.assertIsNone(self.exports.claim_export("worker-2", 10))
            self.clock.advance(seconds=5)
            retried = self.exports.claim_export("worker-2", 10)
            self.assertEqual(retried["export_id"], job["export_id"])
            outcome = self.exports.advance_export("worker-2", job["export_id"], root)
            self.assertEqual(outcome["confirmed_shard"], 1)
            final = self.exports.advance_export("worker-2", job["export_id"], root)
            self.assertEqual(final["state"], "succeeded")

    def test_tampered_shard_blocks_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=1)
            claimed = self.exports.claim_export("worker-1", 60)
            self.exports.advance_export("worker-1", job["export_id"], root)
            self.exports.advance_export("worker-1", job["export_id"], root)
            shard_path = root / "export-1" / "shard-000000.jsonl"
            shard_path.write_text(shard_path.read_text(encoding="utf-8") + canonical_json({"forged": True}) + "\n",
                                  encoding="utf-8")
            with self.assertRaises(InvalidState):
                self.exports.advance_export("worker-1", job["export_id"], root)
            row = self.connection.execute(
                "SELECT state,last_error FROM export_jobs WHERE export_id=?", (job["export_id"],)
            ).fetchone()
            self.assertEqual(row["state"], "failed")
            self.assertIn("摘要复核失败", row["last_error"])
            verification = self.exports.verify_export(job["export_id"], root)
            self.assertFalse(verification["ok"])
            self.assertTrue(any("摘要" in item for item in verification["problems"]))

    def test_missing_shard_file_blocks_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a", "batch-b"], shard_size=1)
            self.exports.claim_export("worker-1", 60)
            self.exports.advance_export("worker-1", job["export_id"], root)
            self.exports.advance_export("worker-1", job["export_id"], root)
            (root / "export-1" / "shard-000000.jsonl").unlink()
            with self.assertRaises(InvalidState):
                self.exports.advance_export("worker-1", job["export_id"], root)
            verification = self.exports.verify_export(job["export_id"], root)
            self.assertFalse(verification["ok"])
            self.assertTrue(any("缺失" in item for item in verification["problems"]))

    def test_manifest_tampering_is_detected_by_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job, _ = self._completed(root, shard_size=1)
            manifest_path = root / "export-1" / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["shards"][0]["content_sha256"] = "f" * 64
            manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
            verification = self.exports.verify_export(job["export_id"], root)
            self.assertFalse(verification["ok"])
            self.assertTrue(any("清单" in item for item in verification["problems"]))

    def test_freeze_cutoff_holds_against_later_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a"], shard_size=1)
            view = self.exports.get_export("auditor", job["export_id"])
            frozen_upper = view["batches"][0]["event_upper_bound"]
            self.clock.advance(seconds=30)
            self.service._audit("batch", "batch-a", "batch.note_added", "operator", {"note": "冻结后的新事件"})
            self.exports.claim_export("worker-1", 60)
            self.exports.advance_export("worker-1", job["export_id"], root)
            final = self.exports.advance_export("worker-1", job["export_id"], root)
            self.assertEqual(final["state"], "succeeded")
            record = json.loads(
                (root / "export-1" / "shard-000000.jsonl").read_text(encoding="utf-8").strip()
            )
            event_ids = [event["event_id"] for event in record["events"]]
            self.assertEqual(max(event_ids), frozen_upper)
            self.assertNotIn("batch.note_added", [event["event_type"] for event in record["events"]])
            view = self.exports.get_export("auditor", job["export_id"])
            self.assertEqual(view["batches"][0]["event_upper_bound"], frozen_upper)
            self.assertEqual(view["batches"][0]["events_after_freeze"], 1)
            self.assertEqual(view["batches"][0]["frozen_at"], "2026-09-24T08:00:00Z")
            manifest = json.loads((root / "export-1" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["batches"][0]["event_upper_bound"], frozen_upper)

    def test_takeover_after_expiry_uses_same_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job = self.exports.submit_export("auditor", ["batch-a"], shard_size=1)
            first = self.exports.claim_export("worker-a", 10)
            self.clock.advance(seconds=11)
            second = self.exports.claim_export("worker-b", 10)
            self.assertEqual(second["export_id"], first["export_id"])
            self.assertEqual(second["lease_owner"], "worker-b")
            with self.assertRaises(InvalidState):
                self.exports.advance_export("worker-a", job["export_id"], root)
            outcome = self.exports.advance_export("worker-b", job["export_id"], root)
            self.assertEqual(outcome["confirmed_shard"], 0)
            final = self.exports.advance_export("worker-b", job["export_id"], root)
            self.assertEqual(final["state"], "succeeded")

    def test_unknown_batch_rejected(self) -> None:
        with self.assertRaises(NotFound):
            self.exports.submit_export("auditor", ["batch-a", "missing-batch"])

    def test_empty_request_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.exports.submit_export("auditor", [])


if __name__ == "__main__":
    unittest.main()
