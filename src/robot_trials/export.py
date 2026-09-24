"""监管证据包的持久化导出任务、分片清单与独立命令入口。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

from .clock import SystemClock, isoformat
from .errors import Conflict, Forbidden, InvalidState, NotFound, ServiceError, ValidationFailed
from .jsonio import canonical_json, content_digest
from .service import ROLE_PERMISSIONS
from .storage import connect, initialize, transaction


EXPORT_FORMAT_VERSION = "robot-trials-export/1"
DEFAULT_SHARD_SIZE = 100
MAX_SHARD_SIZE = 100000


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_atomic(path: Path, content: str) -> None:
    """先写临时文件再原子替换，避免崩溃留下半个分片。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


class ExportService:
    """把提交时冻结的批次证据按确定顺序导出为可校验的分片与清单。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return isoformat(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"用户不存在: {user_id}")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(self, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('export',?,?,?,?,?)",
            (entity_id, event_type, actor_id, canonical_json(payload), self._now()),
        )

    def _job(self, export_id: int) -> sqlite3.Row:
        job = self.connection.execute(
            "SELECT * FROM export_jobs WHERE export_id=?", (export_id,)
        ).fetchone()
        if job is None:
            raise NotFound("导出任务不存在")
        return job

    def _held_job(self, worker_id: str, export_id: int) -> sqlite3.Row:
        job = self._job(export_id)
        if job["state"] != "leased" or job["lease_owner"] != worker_id:
            raise InvalidState("任务未由当前工作进程持有")
        if job["lease_expires_at"] <= self._now():
            raise InvalidState("任务租约已经过期")
        return job

    def _freeze_batch(self, export_id: int, ordinal: int, batch_id: str, frozen_at: str) -> None:
        """在提交事务内冻结一个批次引用的协议、分析、决定和事件上界。"""

        batch = self.connection.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise NotFound(f"批次不存在: {batch_id}")
        protocol = self.connection.execute(
            "SELECT content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
            (batch["protocol_id"], batch["protocol_version"]),
        ).fetchone()
        if protocol is None:
            raise InvalidState("批次引用的协议版本缺失")
        analysis = self.connection.execute(
            "SELECT analysis_id FROM analyses WHERE batch_id=? ORDER BY analysis_id DESC LIMIT 1",
            (batch_id,),
        ).fetchone()
        decision = None
        if analysis is not None:
            decision = self.connection.execute(
                "SELECT decision_id FROM decisions WHERE analysis_id=?", (analysis["analysis_id"],)
            ).fetchone()
        event_upper_bound = self.connection.execute(
            "SELECT COALESCE(MAX(event_id),0) FROM audit_events WHERE entity_type='batch' AND entity_id=?",
            (batch_id,),
        ).fetchone()[0]
        exclusions = self.connection.execute(
            "SELECT e.exclusion_id,e.observation_id,e.status,e.reason,e.requested_by,e.reviewed_by "
            "FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
            "WHERE o.batch_id=? ORDER BY e.exclusion_id",
            (batch_id,),
        ).fetchall()
        self.connection.execute(
            "INSERT INTO export_job_batches(export_id,ordinal,batch_id,batch_revision,protocol_sha256,"
            "analysis_id,decision_id,event_upper_bound,batch_snapshot_json,exclusions_json,frozen_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                export_id,
                ordinal,
                batch_id,
                batch["revision"],
                protocol["content_sha256"],
                None if analysis is None else analysis["analysis_id"],
                None if decision is None else decision["decision_id"],
                event_upper_bound,
                canonical_json(dict(batch)),
                canonical_json([dict(row) for row in exclusions]),
                frozen_at,
            ),
        )

    def submit_export(
        self,
        actor_id: str,
        batch_ids: Iterable[str],
        shard_size: int = DEFAULT_SHARD_SIZE,
    ) -> dict[str, Any]:
        """创建导出任务；同一请求的重复提交返回同一任务。"""

        self._require(actor_id, "export.create")
        if isinstance(shard_size, bool) or not isinstance(shard_size, int) or not 1 <= shard_size <= MAX_SHARD_SIZE:
            raise ValidationFailed(f"分片大小必须是 1 到 {MAX_SHARD_SIZE} 的整数")
        ids = list(batch_ids)
        if not ids:
            raise ValidationFailed("批次集合不能为空")
        for item in ids:
            if not isinstance(item, str) or not item.strip():
                raise ValidationFailed("批次编号必须是非空字符串")
        ordered = sorted({item.strip() for item in ids})
        request_digest = content_digest([{"batch_ids": ordered, "shard_size": shard_size}])
        record_count = len(ordered)
        shard_count = (record_count + shard_size - 1) // shard_size
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                existing = self.connection.execute(
                    "SELECT export_id FROM export_jobs WHERE request_sha256=?", (request_digest,)
                ).fetchone()
                if existing is not None:
                    export_id = existing["export_id"]
                else:
                    cursor = self.connection.execute(
                        "INSERT INTO export_jobs(request_sha256,requested_by,state,shard_size,shard_count,"
                        "record_count,available_at,created_at,updated_at) VALUES(?,?, 'queued', ?,?,?,?,?,?)",
                        (request_digest, actor_id, shard_size, shard_count, record_count, now, now, now),
                    )
                    export_id = cursor.lastrowid
                    for ordinal, batch_id in enumerate(ordered):
                        self._freeze_batch(export_id, ordinal, batch_id, now)
                    self._audit(
                        str(export_id),
                        "export.submitted",
                        actor_id,
                        {
                            "batch_count": record_count,
                            "shard_size": shard_size,
                            "shard_count": shard_count,
                            "request_sha256": request_digest,
                        },
                    )
        except sqlite3.IntegrityError as exc:
            row = self.connection.execute(
                "SELECT export_id FROM export_jobs WHERE request_sha256=?", (request_digest,)
            ).fetchone()
            if row is None:
                raise Conflict("导出任务创建冲突") from exc
            export_id = row["export_id"]
        return self._view(export_id)

    def claim_export(self, worker_id: str, lease_seconds: int = 60) -> dict[str, Any] | None:
        """领取一个待处理或租约已到期的导出任务。"""

        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT export_id FROM export_jobs WHERE "
                "(state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?) "
                "ORDER BY available_at,export_id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE export_jobs SET state='leased',attempts=attempts+1,lease_owner=?,lease_expires_at=?,"
                "updated_at=? WHERE export_id=?",
                (worker_id, expires, now, row["export_id"]),
            )
            claimed = self.connection.execute(
                "SELECT * FROM export_jobs WHERE export_id=?", (row["export_id"],)
            ).fetchone()
        return dict(claimed)

    def advance_export(
        self,
        worker_id: str,
        export_id: int,
        export_root: str | Path,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        """推进一个工作单元：确认下一个分片，或在全部分片确认后复核并完成。"""

        if lease_seconds is not None and lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        root = Path(export_root)
        job = self._held_job(worker_id, export_id)
        confirmed = {
            row["shard_index"]
            for row in self.connection.execute(
                "SELECT shard_index FROM export_shards WHERE export_id=?", (export_id,)
            ).fetchall()
        }
        next_index = 0
        while next_index in confirmed:
            next_index += 1
        if next_index < job["shard_count"]:
            return self._generate_shard(job, next_index, root, worker_id, lease_seconds)
        return self._finalize(job, root, worker_id)

    def fail_export(
        self, worker_id: str, export_id: int, error: str, retry_seconds: int = 0
    ) -> dict[str, Any]:
        """把持有中的任务退回队列，等待重试。"""

        available = isoformat(self.clock.now() + timedelta(seconds=retry_seconds))
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE export_jobs SET state='queued',available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=?,updated_at=? WHERE export_id=? AND state='leased' AND lease_owner=?",
                (available, error[:1000], self._now(), export_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("任务未由当前工作进程持有")
        return {"export_id": export_id, "state": "queued", "available_at": available}

    def get_export(self, actor_id: str, export_id: int) -> dict[str, Any]:
        self._require(actor_id, "export.read")
        return self._view(export_id)

    def _view(self, export_id: int) -> dict[str, Any]:
        job = self._job(export_id)
        batches = self.connection.execute(
            "SELECT * FROM export_job_batches WHERE export_id=? ORDER BY ordinal", (export_id,)
        ).fetchall()
        shards = self.connection.execute(
            "SELECT * FROM export_shards WHERE export_id=? ORDER BY shard_index", (export_id,)
        ).fetchall()
        return {
            "export_id": job["export_id"],
            "state": job["state"],
            "request_sha256": job["request_sha256"],
            "requested_by": job["requested_by"],
            "attempts": job["attempts"],
            "shard_size": job["shard_size"],
            "shard_count": job["shard_count"],
            "record_count": job["record_count"],
            "confirmed_shards": len(shards),
            "manifest_sha256": job["manifest_sha256"],
            "last_error": job["last_error"],
            "lease_owner": job["lease_owner"],
            "lease_expires_at": job["lease_expires_at"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "batches": [
                {
                    "ordinal": row["ordinal"],
                    "batch_id": row["batch_id"],
                    "batch_revision": row["batch_revision"],
                    "protocol_sha256": row["protocol_sha256"],
                    "analysis_id": row["analysis_id"],
                    "decision_id": row["decision_id"],
                    "event_upper_bound": row["event_upper_bound"],
                    "frozen_at": row["frozen_at"],
                    "events_after_freeze": self.connection.execute(
                        "SELECT COUNT(*) FROM audit_events "
                        "WHERE entity_type='batch' AND entity_id=? AND event_id>?",
                        (row["batch_id"], row["event_upper_bound"]),
                    ).fetchone()[0],
                }
                for row in batches
            ],
            "shards": [
                {
                    "shard_index": row["shard_index"],
                    "record_from": row["record_from"],
                    "record_to": row["record_to"],
                    "record_count": row["record_count"],
                    "content_sha256": row["content_sha256"],
                    "relative_path": row["relative_path"],
                    "confirmed_at": row["confirmed_at"],
                }
                for row in shards
            ],
        }

    def _batch_record(self, freeze: sqlite3.Row) -> dict[str, Any]:
        """由冻结记录与不可变表重建一个批次的确定性证据行。"""

        batch_id = freeze["batch_id"]
        snapshot = json.loads(freeze["batch_snapshot_json"])
        protocol_row = self.connection.execute(
            "SELECT canonical_json,content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
            (snapshot["protocol_id"], snapshot["protocol_version"]),
        ).fetchone()
        if protocol_row is None or protocol_row["content_sha256"] != freeze["protocol_sha256"]:
            raise InvalidState("冻结协议摘要与协议目录不一致")
        protocol = json.loads(protocol_row["canonical_json"])
        analysis = None
        if freeze["analysis_id"] is not None:
            analysis_row = self.connection.execute(
                "SELECT * FROM analyses WHERE analysis_id=?", (freeze["analysis_id"],)
            ).fetchone()
            if analysis_row is None:
                raise InvalidState("冻结分析记录缺失")
            analysis = {
                "analysis_id": analysis_row["analysis_id"],
                "input_sha256": analysis_row["input_sha256"],
                "algorithm_version": analysis_row["algorithm_version"],
                "created_by": analysis_row["created_by"],
                "result": json.loads(analysis_row["result_json"]),
            }
        decision = None
        if freeze["decision_id"] is not None:
            decision_row = self.connection.execute(
                "SELECT * FROM decisions WHERE decision_id=?", (freeze["decision_id"],)
            ).fetchone()
            if decision_row is None:
                raise InvalidState("冻结决定记录缺失")
            decision = dict(decision_row)
        events = self.connection.execute(
            "SELECT event_id,event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='batch' AND entity_id=? AND event_id<=? ORDER BY event_id",
            (batch_id, freeze["event_upper_bound"]),
        ).fetchall()
        return {
            "ordinal": freeze["ordinal"],
            "batch_id": batch_id,
            "frozen_at": freeze["frozen_at"],
            "batch_revision": freeze["batch_revision"],
            "event_upper_bound": freeze["event_upper_bound"],
            "batch": snapshot,
            "protocol": {
                "protocol_id": protocol["protocol_id"],
                "version": protocol["version"],
                "title": protocol["title"],
                "sha256": protocol_row["content_sha256"],
                "seed": protocol["seed"],
                "bootstrap_samples": protocol["bootstrap_samples"],
            },
            "analysis": analysis,
            "decision": decision,
            "exclusions": json.loads(freeze["exclusions_json"]),
            "events": [
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "actor_id": event["actor_id"],
                    "created_at": event["created_at"],
                    "payload": json.loads(event["payload_json"]),
                }
                for event in events
            ],
        }

    def _generate_shard(
        self,
        job: sqlite3.Row,
        shard_index: int,
        root: Path,
        worker_id: str,
        lease_seconds: int | None,
    ) -> dict[str, Any]:
        export_id = job["export_id"]
        record_from = shard_index * job["shard_size"]
        record_to = min(record_from + job["shard_size"], job["record_count"])
        freeze_rows = self.connection.execute(
            "SELECT * FROM export_job_batches WHERE export_id=? AND ordinal>=? AND ordinal<? ORDER BY ordinal",
            (export_id, record_from, record_to),
        ).fetchall()
        if len(freeze_rows) != record_to - record_from:
            raise InvalidState("任务冻结记录不完整")
        records = [self._batch_record(row) for row in freeze_rows]
        content = "".join(canonical_json(record) + "\n" for record in records)
        digest = _sha256_bytes(content.encode("utf-8"))
        relative_path = f"export-{export_id}/shard-{shard_index:06d}.jsonl"
        _write_atomic(root / relative_path, content)
        now = self._now()
        with transaction(self.connection, immediate=True):
            self._held_job(worker_id, export_id)
            if lease_seconds is not None:
                expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
                self.connection.execute(
                    "UPDATE export_jobs SET lease_expires_at=?,updated_at=? WHERE export_id=?",
                    (expires, now, export_id),
                )
            else:
                self.connection.execute(
                    "UPDATE export_jobs SET updated_at=? WHERE export_id=?", (now, export_id)
                )
            self.connection.execute(
                "INSERT INTO export_shards(export_id,shard_index,record_from,record_to,record_count,"
                "content_sha256,relative_path,confirmed_at) VALUES(?,?,?,?,?,?,?,?)",
                (export_id, shard_index, record_from, record_to, record_to - record_from, digest,
                 relative_path, now),
            )
            self._audit(
                str(export_id),
                "export.shard_confirmed",
                worker_id,
                {
                    "shard_index": shard_index,
                    "record_from": record_from,
                    "record_to": record_to,
                    "content_sha256": digest,
                },
            )
        return {
            "export_id": export_id,
            "state": "leased",
            "confirmed_shard": shard_index,
            "confirmed_count": shard_index + 1,
            "shard_count": job["shard_count"],
            "content_sha256": digest,
        }

    def _check_shards(
        self, job: sqlite3.Row, shard_rows: list[sqlite3.Row], root: Path
    ) -> tuple[list[str], int]:
        """逐分片复核记录范围、文件摘要与记录数，返回问题清单和通过数量。"""

        problems: list[str] = []
        verified = 0
        for row in shard_rows:
            expected_from = row["shard_index"] * job["shard_size"]
            expected_to = min(expected_from + job["shard_size"], job["record_count"])
            if row["record_from"] != expected_from or row["record_to"] != expected_to:
                problems.append(f"分片 {row['shard_index']} 的记录范围与任务计划不一致")
                continue
            try:
                data = (root / row["relative_path"]).read_bytes()
            except OSError:
                problems.append(f"分片 {row['shard_index']} 的文件缺失: {row['relative_path']}")
                continue
            if _sha256_bytes(data) != row["content_sha256"]:
                problems.append(f"分片 {row['shard_index']} 的内容摘要与确认摘要不一致")
                continue
            line_count = sum(1 for line in data.decode("utf-8").splitlines() if line.strip())
            if line_count != row["record_count"]:
                problems.append(f"分片 {row['shard_index']} 的记录数与确认值不一致")
                continue
            verified += 1
        return problems, verified

    def _manifest(
        self, job: sqlite3.Row, freeze_rows: list[sqlite3.Row], shard_rows: list[sqlite3.Row]
    ) -> dict[str, Any]:
        entries = [
            {
                "shard_index": row["shard_index"],
                "relative_path": row["relative_path"],
                "record_from": row["record_from"],
                "record_to": row["record_to"],
                "record_count": row["record_count"],
                "content_sha256": row["content_sha256"],
            }
            for row in shard_rows
        ]
        return {
            "format": EXPORT_FORMAT_VERSION,
            "export_id": job["export_id"],
            "request_sha256": job["request_sha256"],
            "requested_by": job["requested_by"],
            "created_at": job["created_at"],
            "shard_size": job["shard_size"],
            "shard_count": job["shard_count"],
            "record_count": job["record_count"],
            "batches": [
                {
                    "ordinal": row["ordinal"],
                    "batch_id": row["batch_id"],
                    "batch_revision": row["batch_revision"],
                    "protocol_sha256": row["protocol_sha256"],
                    "analysis_id": row["analysis_id"],
                    "decision_id": row["decision_id"],
                    "event_upper_bound": row["event_upper_bound"],
                    "frozen_at": row["frozen_at"],
                }
                for row in freeze_rows
            ],
            "shards": entries,
            "overall_sha256": content_digest(entries),
        }

    def _finalize(self, job: sqlite3.Row, root: Path, worker_id: str) -> dict[str, Any]:
        """全部分片确认后复核摘要，只有复核成功才写出清单并完成任务。"""

        export_id = job["export_id"]
        shard_rows = self.connection.execute(
            "SELECT * FROM export_shards WHERE export_id=? ORDER BY shard_index", (export_id,)
        ).fetchall()
        problems, _ = self._check_shards(job, shard_rows, root)
        now = self._now()
        if problems:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "UPDATE export_jobs SET state='failed',last_error=?,lease_owner=NULL,"
                    "lease_expires_at=NULL,updated_at=? WHERE export_id=? AND state='leased' AND lease_owner=?",
                    (f"分片摘要复核失败: {problems[0]}"[:1000], now, export_id, worker_id),
                )
                if cursor.rowcount == 1:
                    self._audit(str(export_id), "export.failed", worker_id, {"problems": problems})
            raise InvalidState(f"分片摘要复核失败: {problems[0]}")
        freeze_rows = self.connection.execute(
            "SELECT * FROM export_job_batches WHERE export_id=? ORDER BY ordinal", (export_id,)
        ).fetchall()
        manifest = self._manifest(job, freeze_rows, shard_rows)
        _write_atomic(root / f"export-{export_id}" / "manifest.json", canonical_json(manifest) + "\n")
        with transaction(self.connection, immediate=True):
            self._held_job(worker_id, export_id)
            self.connection.execute(
                "UPDATE export_jobs SET state='succeeded',manifest_sha256=?,lease_owner=NULL,"
                "lease_expires_at=NULL,updated_at=? WHERE export_id=? AND state='leased' AND lease_owner=?",
                (manifest["overall_sha256"], now, export_id, worker_id),
            )
            self._audit(
                str(export_id),
                "export.completed",
                worker_id,
                {
                    "manifest_sha256": manifest["overall_sha256"],
                    "record_count": job["record_count"],
                    "shard_count": job["shard_count"],
                },
            )
        return {
            "export_id": export_id,
            "state": "succeeded",
            "confirmed_count": job["shard_count"],
            "shard_count": job["shard_count"],
            "manifest_sha256": manifest["overall_sha256"],
        }

    def verify_export(self, export_id: int, export_root: str | Path) -> dict[str, Any]:
        """独立复核分片文件与清单内容，不修改任务状态。"""

        root = Path(export_root)
        job = self._job(export_id)
        shard_rows = self.connection.execute(
            "SELECT * FROM export_shards WHERE export_id=? ORDER BY shard_index", (export_id,)
        ).fetchall()
        problems, verified = self._check_shards(job, shard_rows, root)
        complete = job["state"] == "succeeded"
        if complete:
            if len(shard_rows) != job["shard_count"]:
                problems.append("已确认分片数量与任务分片数不一致")
            manifest_path = root / f"export-{export_id}" / "manifest.json"
            try:
                manifest_text = manifest_path.read_text(encoding="utf-8")
            except OSError:
                problems.append("清单文件缺失")
            else:
                freeze_rows = self.connection.execute(
                    "SELECT * FROM export_job_batches WHERE export_id=? ORDER BY ordinal", (export_id,)
                ).fetchall()
                expected = canonical_json(self._manifest(job, freeze_rows, shard_rows)) + "\n"
                if manifest_text != expected:
                    problems.append("清单内容与任务冻结记录不一致")
                elif json.loads(manifest_text)["overall_sha256"] != job["manifest_sha256"]:
                    problems.append("清单整体摘要与任务记录不一致")
        return {
            "export_id": export_id,
            "state": job["state"],
            "complete": complete,
            "ok": not problems,
            "problems": problems,
            "verified_shards": verified,
            "shard_count": job["shard_count"],
            "record_count": job["record_count"],
            "manifest_sha256": job["manifest_sha256"],
        }


def main(argv: list[str] | None = None) -> int:
    """无需启动 HTTP 服务的导出命令入口：创建、推进、查看并校验证据包。"""

    parser = argparse.ArgumentParser(description="监管证据包导出：创建、推进并校验持久化导出任务")
    parser.add_argument("--database", type=Path, default=Path("robot_trials.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="创建导出任务（同一请求重复提交返回同一任务）")
    submit.add_argument("--actor", required=True, help="提交人（审计人员）")
    submit.add_argument("--batch", action="append", dest="batches", required=True, help="批次编号，可重复")
    submit.add_argument("--shard-size", type=int, default=DEFAULT_SHARD_SIZE)
    work = commands.add_parser("work", help="领取并推进导出任务直到没有可处理任务")
    work.add_argument("--worker", required=True, help="工作进程标识")
    work.add_argument("--export-root", type=Path, default=Path("exports"))
    work.add_argument("--lease-seconds", type=int, default=60)
    work.add_argument("--max-units", type=int, default=None, help="最多推进的工作单元数")
    status = commands.add_parser("status", help="查看导出任务状态与冻结截点")
    status.add_argument("--actor", required=True, help="查询人（审计人员）")
    status.add_argument("--export-id", type=int, required=True)
    verify = commands.add_parser("verify", help="独立复核分片文件与清单摘要")
    verify.add_argument("--export-id", type=int, required=True)
    verify.add_argument("--export-root", type=Path, default=Path("exports"))
    args = parser.parse_args(argv)

    connection = connect(args.database)
    try:
        service = ExportService(connection)
        if args.command == "submit":
            result: Any = service.submit_export(args.actor, args.batches, args.shard_size)
            exit_code = 0
        elif args.command == "work":
            actions: list[dict[str, Any]] = []
            while args.max_units is None or len(actions) < args.max_units:
                job = service.claim_export(args.worker, args.lease_seconds)
                if job is None:
                    break
                while True:
                    outcome = service.advance_export(
                        args.worker, job["export_id"], args.export_root, lease_seconds=args.lease_seconds
                    )
                    actions.append(outcome)
                    if outcome["state"] == "succeeded" or (
                        args.max_units is not None and len(actions) >= args.max_units
                    ):
                        break
            result = {"status": "ok", "worker": args.worker, "actions": actions}
            exit_code = 0
        elif args.command == "status":
            result = service.get_export(args.actor, args.export_id)
            exit_code = 0
        else:
            result = service.verify_export(args.export_id, args.export_root)
            exit_code = 0 if result["ok"] and result["complete"] else 1
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return exit_code
    except ServiceError as exc:
        print(json.dumps(
            {"status": "error", "code": exc.code, "message": str(exc)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ))
        return 1
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
