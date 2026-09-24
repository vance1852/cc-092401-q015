from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from robot_trials import exportctl
from robot_trials.clock import FrozenClock
from robot_trials.errors import (
    ExportIntegrityError,
    Forbidden,
    InvalidState,
)
from robot_trials.jsonio import canonical_json, load_json
from robot_trials.service import TrialService
from robot_trials.storage import connect

ROOT = Path(__file__).resolve().parents[1]


class ExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.database = self.base / "foundation.sqlite3"
        self.output_dir = self.base / "exports"
        self.connection = connect(self.database)
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
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
        self.batch_ids = [self._prepare_decided_batch("batch-a"), self._prepare_decided_batch("batch-b")]

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _prepare_decided_batch(self, batch_id: str) -> str:
        self.service.create_batch("operator", batch_id, "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", batch_id, 1)
        self.service.import_observations("operator", batch_id, f"key-{batch_id}", self.rows)
        self.service.seal_batch("stat", batch_id, 2)
        job = self.service.claim_job(f"worker-{batch_id}", 60)
        analysis = self.service.complete_job(f"worker-{batch_id}", job["job_id"], "stat")
        decision = "approved" if analysis["result"]["conclusion"] == "pass" else "rejected"
        self.service.decide("approver", batch_id, analysis["analysis_id"], decision, "测试决定")
        return batch_id

    def _run_to_completion(self, task_id: str, worker: str = "worker-1") -> None:
        while True:
            result = self.service.advance_export(worker, task_id, max_shards=1)
            if result["state"] in {"succeeded", "failed"}:
                self.assertEqual(result["state"], "succeeded", result)
                return

    def test_full_export_is_verifiable_and_deterministic(self) -> None:
        created = self.service.create_export(
            "auditor", self.batch_ids, self.output_dir, records_per_shard=7
        )
        self.assertFalse(created["replayed"])
        self.assertEqual(created["shard_count"], 5)  # 两个批次各 15 条，共 30 条
        claimed = self.service.claim_export("worker-1", 60)
        self.assertEqual(claimed["task_id"], created["task_id"])
        self._run_to_completion(created["task_id"])

        verification = self.service.verify_export(created["task_id"])
        self.assertTrue(verification["ok"])
        self.assertEqual(verification["record_count"], 30)
        self.assertEqual(verification["shard_count"], 5)

        task_dir = self.output_dir / created["task_id"]
        manifest = json.loads((task_dir / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["record_count_total"], 30)
        self.assertEqual(len(manifest["shards"]), 5)
        self.assertEqual([entry["batch_id"] for entry in manifest["batches"]], self.batch_ids)
        # 分片范围必须连续覆盖 [0,30)。
        bounds = [(entry["ordinal_start"], entry["ordinal_end"]) for entry in manifest["shards"]]
        self.assertEqual(bounds, [(0, 7), (7, 14), (14, 21), (21, 28), (28, 30)])
        self.assertTrue(manifest["cutoff"])
        # 整体摘要覆盖全部分片摘要。
        self.assertEqual(len(manifest["overall_sha256"]), 64)
        total_by_types = {"protocol": 0, "analysis": 0, "decision": 0, "observation": 0, "event": 0}
        for entry in manifest["shards"]:
            for key, value in entry["record_types"].items():
                total_by_types[key] += value
        self.assertEqual(total_by_types, {
            "protocol": 2, "analysis": 2, "decision": 2, "observation": 12, "event": 12,
        })

        # 同样的请求换一个输出目录会得到独立任务，但分片字节必须逐字节一致。
        other_dir = self.base / "exports-2"
        second = self.service.create_export("auditor", self.batch_ids, other_dir, records_per_shard=7)
        self.assertNotEqual(second["task_id"], created["task_id"])
        self.service.claim_export("worker-2", 60)
        self._run_to_completion(second["task_id"], "worker-2")
        for index in range(5):
            name = f"data-{index:05d}.jsonl"
            self.assertEqual(
                (task_dir / name).read_bytes(),
                (other_dir / second["task_id"] / name).read_bytes(),
            )

    def test_replay_same_request_returns_same_task(self) -> None:
        first = self.service.create_export("auditor", ["batch-a"], self.output_dir)
        second = self.service.create_export("auditor", ["batch-a"], self.output_dir)
        self.assertEqual(first["task_id"], second["task_id"])
        self.assertTrue(second["replayed"])
        # 批次顺序不同是不同的导出请求。
        reordered = self.service.create_export(
            "auditor", ["batch-b", "batch-a"], self.output_dir
        )
        self.assertNotEqual(reordered["task_id"], first["task_id"])

    def test_resumes_from_last_confirmed_shard_after_crash(self) -> None:
        created = self.service.create_export(
            "auditor", self.batch_ids, self.output_dir, records_per_shard=4
        )
        task_id = created["task_id"]
        claimed = self.service.claim_export("worker-1", 600)
        # 先确认第一个分片。
        first = self.service.advance_export("worker-1", task_id, max_shards=1)
        self.assertEqual(first["processed"], 1)
        self.assertEqual(first["shards_pending"], created["shard_count"] - 1)
        first_shard = self.output_dir / task_id / "data-00000.jsonl"
        first_bytes = first_shard.read_bytes()
        # 模拟崩溃：未来得及确认的分片留下了半截文件和临时文件。
        stale = self.output_dir / task_id / "data-00001.jsonl"
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_bytes(b'{"ordinal": 7}\n')
        (self.output_dir / task_id / "data-00001.jsonl.tmp").write_bytes(b"partial")
        # “重启”：新的服务实例打开同一个数据库文件继续。
        self.connection.close()
        self.connection = connect(self.database)
        restarted = TrialService(self.connection, self.clock)
        resumed = restarted.advance_export("worker-1", task_id, max_shards=1)
        self.assertEqual(resumed["processed"], 1)
        self.assertEqual(restarted.get_export("auditor", task_id)["shards_confirmed"], 2)
        # 已确认分片字节不变，崩溃残片被覆盖重写。
        self.assertEqual(first_shard.read_bytes(), first_bytes)
        self.assertNotEqual(stale.read_bytes(), b'{"ordinal": 7}\n')
        while restarted.advance_export("worker-1", task_id, max_shards=1)["state"] != "succeeded":
            pass
        self.assertTrue(restarted.verify_export(task_id)["ok"])

    def test_tampering_confirmed_shard_fails_completion_and_blocks_verify(self) -> None:
        created = self.service.create_export(
            "auditor", ["batch-a"], self.output_dir, records_per_shard=4
        )
        task_id = created["task_id"]
        self.service.claim_export("worker-1", 600)
        self.service.advance_export("worker-1", task_id, max_shards=1)
        target = self.output_dir / task_id / "data-00000.jsonl"
        target.write_bytes(target.read_bytes() + b'{"ordinal":0}\n')
        # 其余分片照常推进，最终复核必须失败。
        with self.assertRaises(ExportIntegrityError):
            while True:
                result = self.service.advance_export("worker-1", task_id, max_shards=1)
                if result["state"] in {"succeeded", "failed"}:
                    break
        status = self.service.get_export("auditor", task_id)
        self.assertEqual(status["state"], "failed")
        self.assertIn("0", status["last_error"])
        self.assertFalse((self.output_dir / task_id / "manifest.json").exists())
        with self.assertRaises(InvalidState):
            self.service.verify_export(task_id)
        # 修复分片内容后，重放仍返回同一任务（不会静默生成第二套产物）。
        replay = self.service.create_export(
            "auditor", ["batch-a"], self.output_dir, records_per_shard=4
        )
        self.assertEqual(replay["task_id"], task_id)
        self.assertTrue(replay["replayed"])

    def test_tampering_after_completion_is_detected(self) -> None:
        created = self.service.create_export(
            "auditor", ["batch-a"], self.output_dir, records_per_shard=4
        )
        task_id = created["task_id"]
        self.service.claim_export("worker-1", 600)
        self._run_to_completion(task_id)
        target = self.output_dir / task_id / "data-00000.jsonl"
        target.write_bytes(target.read_bytes().replace(b" ", b"", 0) + b"\n")
        with self.assertRaises(ExportIntegrityError):
            self.service.verify_export(task_id)

    def test_expired_lease_is_taken_over_and_old_worker_rejected(self) -> None:
        created = self.service.create_export(
            "auditor", self.batch_ids, self.output_dir, records_per_shard=5
        )
        task_id = created["task_id"]
        first = self.service.claim_export("worker-a", 10)
        self.assertEqual(first["attempts"], 1)
        self.service.advance_export("worker-a", task_id, max_shards=1)
        # 租约未到期时不能被别人领取。
        self.assertIsNone(self.service.claim_export("worker-b", 10))
        self.clock.advance(seconds=11)
        second = self.service.claim_export("worker-b", 10)
        self.assertEqual(second["task_id"], task_id)
        self.assertEqual(second["attempts"], 2)
        self.assertEqual(second["lease_owner"], "worker-b")
        # 旧持有者继续推进会被拒绝，新持有者从已确认分片之后接着跑。
        with self.assertRaises(InvalidState):
            self.service.advance_export("worker-a", task_id, max_shards=1)
        self._run_to_completion(task_id, "worker-b")
        self.assertTrue(self.service.verify_export(task_id)["ok"])

    def test_fail_export_requeues_with_delay(self) -> None:
        created = self.service.create_export("auditor", ["batch-a"], self.output_dir)
        task_id = created["task_id"]
        self.service.claim_export("worker-a", 10)
        failed = self.service.fail_export("worker-a", task_id, "磁盘暂时不可用", retry_seconds=30)
        self.assertEqual(failed["state"], "queued")
        self.assertIsNone(self.service.claim_export("worker-b", 10))
        self.clock.advance(seconds=30)
        retaken = self.service.claim_export("worker-b", 10)
        self.assertEqual(retaken["task_id"], task_id)
        self.assertEqual(retaken["attempts"], 2)

    def test_freeze_cutoff_excludes_later_events(self) -> None:
        created = self.service.create_export(
            "auditor", ["batch-a"], self.output_dir, records_per_shard=100
        )
        task_id = created["task_id"]
        frozen_high = self.connection.execute(
            "SELECT event_high_id,event_count,frozen_at FROM export_batches WHERE task_id=?",
            (task_id,),
        ).fetchone()
        # 冻结后批次产生新的审计事件：直接写一条 batch 事件模拟。
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('batch','batch-a','batch.note_added','auditor',?,?)",
            (canonical_json({"note": "冻结之后的事件"}), self.service._now()),
        )
        self.service.claim_export("worker-1", 600)
        self._run_to_completion(task_id)
        manifest = json.loads(
            (self.output_dir / task_id / "manifest.json").read_text(encoding="utf-8")
        )
        entry = manifest["batches"][0]
        self.assertEqual(entry["event_high_id"], frozen_high["event_high_id"])
        self.assertEqual(entry["event_count"], frozen_high["event_count"])
        self.assertEqual(entry["frozen_at"], frozen_high["frozen_at"])
        shard_text = (self.output_dir / task_id / "data-00000.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("batch.note_added", shard_text)

        # 重新提交（不同输出目录）形成新任务时，新事件被纳入，冻结上界前移。
        other_dir = self.base / "exports-later"
        later = self.service.create_export("auditor", ["batch-a"], other_dir, records_per_shard=100)
        self.service.claim_export("worker-2", 600)
        self._run_to_completion(later["task_id"], "worker-2")
        later_text = (other_dir / later["task_id"] / "data-00000.jsonl").read_text(encoding="utf-8")
        self.assertIn("batch.note_added", later_text)

    def test_only_auditor_may_create_or_read_export(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_export("operator", ["batch-a"], self.output_dir)
        created = self.service.create_export("auditor", ["batch-a"], self.output_dir)
        with self.assertRaises(Forbidden):
            self.service.get_export("operator", created["task_id"])


class ExportCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.database = self.base / "foundation.sqlite3"
        self.output_dir = self.base / "exports"
        connection = connect(self.database)
        service = TrialService(connection)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            service.create_user(user_id, user_id, role)
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        service.register_robot("operator", "robot-a", "A 型", "厂商")
        service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        service.publish_protocol("stat", protocol)
        service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        service.start_batch("operator", "batch-a", 1)
        service.import_observations("operator", "batch-a", "key-1", rows)
        service.seal_batch("stat", "batch-a", 2)
        job = service.claim_job("worker-1", 60)
        analysis = service.complete_job("worker-1", job["job_id"], "stat")
        service.decide(
            "approver", "batch-a", analysis["analysis_id"],
            "approved" if analysis["result"]["conclusion"] == "pass" else "rejected",
            "cli",
        )
        connection.close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run_cli(self, *arguments: str) -> tuple[int, str]:
        import contextlib

        stdout = StringIO()
        with contextlib.redirect_stdout(stdout):
            code = exportctl.main(list(arguments))
        return code, stdout.getvalue().strip()

    def test_create_process_verify_without_http(self) -> None:
        code, line = self._run_cli(
            "create", "--database", str(self.database), "--actor", "auditor",
            "--batches", "batch-a", "--output-dir", str(self.output_dir),
            "--records-per-shard", "5",
        )
        self.assertEqual(code, 0)
        task_id = json.loads(line)["task_id"]
        code, line = self._run_cli(
            "process", "--database", str(self.database), "--worker", "worker-cli",
            "--lease-seconds", "60", "--max-shards-per-turn", "1",
        )
        self.assertEqual(code, 0, line)
        self.assertEqual(json.loads(line)["state"], "succeeded")
        code, line = self._run_cli(
            "verify", "--database", str(self.database), "--task", task_id
        )
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(line)["ok"])
        code, line = self._run_cli(
            "status", "--database", str(self.database), "--actor", "auditor", "--task", task_id
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(line)["state"], "succeeded")


if __name__ == "__main__":
    unittest.main()
