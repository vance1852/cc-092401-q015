"""统计准入服务的领域用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import exports
from .analysis import ALGORITHM_VERSION, analyze
from .clock import SystemClock, isoformat
from .contracts import Observation, Protocol, ValidationError
from .errors import Conflict, ExportIntegrityError, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest
from .storage import initialize, transaction


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


ROLE_PERMISSIONS = {
    "operator": {
        "catalog.write", "batch.create", "batch.start", "observation.import",
        "exclusion.request", "exclusion.revoke",
    },
    "statistician": {"protocol.publish", "batch.seal", "exclusion.review", "analysis.run"},
    "approver": {"decision.write"},
    "auditor": {"report.read", "audit.read", "export.create", "export.read"},
}


class TrialService:
    """在单个 SQLite 连接上提供全部业务操作。"""

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

    def _audit(
        self, entity_type: str, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload), self._now()),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO users(user_id,display_name,role) VALUES(?,?,?)",
                    (user_id.strip(), display_name.strip(), role),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    def register_robot(
        self, actor_id: str, robot_id: str, model_name: str, vendor: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO robots(robot_id,model_name,vendor,created_at) VALUES(?,?,?,?)",
                    (robot_id, model_name, vendor, self._now()),
                )
                self._audit("robot", robot_id, "robot.registered", actor_id, {"model_name": model_name})
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"机器人已存在: {robot_id}") from exc
        return {"robot_id": robot_id, "model_name": model_name, "vendor": vendor}

    def register_build(
        self, actor_id: str, build_id: str, robot_id: str, version: str, content_sha256: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        if len(content_sha256) != 64:
            raise ValidationFailed("构建摘要必须是 64 位 SHA-256")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO builds(build_id,robot_id,version,content_sha256,created_at) VALUES(?,?,?,?,?)",
                    (build_id, robot_id, version, content_sha256.lower(), self._now()),
                )
                self._audit("build", build_id, "build.registered", actor_id, {"robot_id": robot_id, "version": version})
        except sqlite3.IntegrityError as exc:
            raise Conflict("构建编号、版本或摘要冲突") from exc
        return {"build_id": build_id, "robot_id": robot_id, "version": version}

    def publish_protocol(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "protocol.publish")
        try:
            protocol = Protocol.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        text = canonical_json(raw)
        digest = content_digest([raw])
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO protocol_catalog(protocol_id,version,title,task_family,canonical_json,content_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        protocol.protocol_id,
                        protocol.version,
                        protocol.title,
                        protocol.task_family,
                        text,
                        digest,
                        self._now(),
                    ),
                )
                identity = f"{protocol.protocol_id}@{protocol.version}"
                self._audit("protocol", identity, "protocol.published", actor_id, {"sha256": digest})
        except sqlite3.IntegrityError as exc:
            raise Conflict("协议版本或内容摘要已经存在") from exc
        return {"protocol_id": protocol.protocol_id, "version": protocol.version, "sha256": digest}

    def _protocol(self, protocol_id: str, version: int) -> tuple[Protocol, str]:
        row = self.connection.execute(
            "SELECT canonical_json,content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
            (protocol_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("协议版本不存在")
        return Protocol.from_dict(json.loads(row["canonical_json"])), row["content_sha256"]

    def create_batch(
        self,
        actor_id: str,
        batch_id: str,
        protocol_id: str,
        protocol_version: int,
        build_id: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "batch.create")
        self._protocol(protocol_id, protocol_version)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO batches(batch_id,protocol_id,protocol_version,build_id,state,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (batch_id, protocol_id, protocol_version, build_id, "draft", actor_id, self._now()),
                )
                self._audit("batch", batch_id, "batch.created", actor_id, {"build_id": build_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("批次编号冲突或构建不存在") from exc
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFound("批次不存在")
        return dict(row)

    def start_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.start")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE batches SET state='running',revision=revision+1,started_at=? "
                "WHERE batch_id=? AND state='draft' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次不是当前草稿版本")
            self._audit("batch", batch_id, "batch.started", actor_id, {"from_revision": expected_revision})
        return self.get_batch(batch_id)

    def _idempotent_response(self, scope: str, key: str, request_digest: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT request_sha256,response_json FROM idempotency_keys WHERE scope=? AND key=?",
            (scope, key),
        ).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_digest:
            raise Conflict("同一幂等键对应了不同请求内容")
        return json.loads(row["response_json"])

    def import_observations(
        self,
        actor_id: str,
        batch_id: str,
        idempotency_key: str,
        raw_rows: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        self._require(actor_id, "observation.import")
        rows = tuple(raw_rows)
        if not rows:
            raise ValidationFailed("观测数组不能为空")
        request_digest = content_digest(rows)
        scope = f"observations:{batch_id}"
        existing = self._idempotent_response(scope, idempotency_key, request_digest)
        if existing is not None:
            return existing
        batch = self.get_batch(batch_id)
        if batch["state"] != "running":
            raise InvalidState("只有运行中的批次可以导入观测")
        protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
        parsed: list[Observation] = []
        for raw in rows:
            try:
                item = Observation.from_dict(raw, protocol)
            except ValidationError as exc:
                raise ValidationFailed(str(exc)) from exc
            if item.robot_id != self.connection.execute(
                "SELECT robot_id FROM builds WHERE build_id=?", (batch["build_id"],)
            ).fetchone()["robot_id"]:
                raise ValidationFailed("观测机器人与批次构建不一致")
            parsed.append(item)
        response = {"batch_id": batch_id, "inserted": len(parsed), "request_sha256": request_digest}
        try:
            with transaction(self.connection, immediate=True):
                for item, raw in zip(parsed, rows):
                    self.connection.execute(
                        "INSERT INTO observations(batch_id,source_batch,source_row,robot_id,stratum_key,observed_at," 
                        "metrics_json,content_sha256,imported_by,imported_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            batch_id,
                            item.source_batch,
                            item.source_row,
                            item.robot_id,
                            item.stratum_key,
                            item.observed_at,
                            canonical_json({key: format(value, "f") for key, value in item.metrics.items()}),
                            content_digest([raw]),
                            actor_id,
                            self._now(),
                        ),
                    )
                self.connection.execute(
                    "INSERT INTO idempotency_keys(scope,key,request_sha256,response_json,created_at) VALUES(?,?,?,?,?)",
                    (scope, idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("batch", batch_id, "observations.imported", actor_id, response)
        except sqlite3.IntegrityError as exc:
            raise Conflict("来源行重复或幂等键并发冲突") from exc
        return response

    def request_exclusion(self, actor_id: str, observation_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.request")
        observation = self.connection.execute(
            "SELECT observation_id,batch_id FROM observations WHERE observation_id=?", (observation_id,)
        ).fetchone()
        if observation is None:
            raise NotFound("观测不存在")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
                    "VALUES(?,?,?,?,?)",
                    (observation_id, "pending", reason, actor_id, self._now()),
                )
                exclusion_id = cursor.lastrowid
                self._audit("observation", str(observation_id), "exclusion.requested", actor_id, {"reason": reason})
        except sqlite3.IntegrityError as exc:
            raise Conflict("该观测已有待处理或生效排除") from exc
        return {"exclusion_id": exclusion_id, "status": "pending"}

    def review_exclusion(
        self, actor_id: str, exclusion_id: int, approve: bool, note: str
    ) -> dict[str, Any]:
        self._require(actor_id, "exclusion.review")
        row = self.connection.execute(
            "SELECT * FROM exclusion_requests WHERE exclusion_id=?", (exclusion_id,)
        ).fetchone()
        if row is None:
            raise NotFound("排除申请不存在")
        if row["status"] != "pending":
            raise InvalidState("排除申请已经处理")
        if row["requested_by"] == actor_id:
            raise Forbidden("申请人不能复核自己的排除申请")
        status = "approved" if approve else "rejected"
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE exclusion_requests SET status=?,reviewed_by=?,reviewed_at=?,review_note=? "
                "WHERE exclusion_id=? AND status='pending'",
                (status, actor_id, self._now(), note, exclusion_id),
            )
            self._audit("exclusion", str(exclusion_id), f"exclusion.{status}", actor_id, {"note": note})
        return {"exclusion_id": exclusion_id, "status": status}

    def revoke_exclusion(self, actor_id: str, exclusion_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.revoke")
        row = self.connection.execute(
            "SELECT e.*,o.batch_id FROM exclusion_requests e "
            "JOIN observations o ON o.observation_id=e.observation_id WHERE e.exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        if row is None:
            raise NotFound("排除记录不存在")
        if row["status"] != "approved":
            raise InvalidState("只有已批准的排除可以撤销")
        if row["requested_by"] != actor_id:
            raise Forbidden("只有原申请人可以撤销排除")
        batch = self.get_batch(row["batch_id"])
        if batch["state"] != "running":
            raise InvalidState("批次封存后不能改变排除状态")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE exclusion_requests SET status='revoked',review_note=?,reviewed_at=? "
                "WHERE exclusion_id=? AND status='approved'",
                (reason, self._now(), exclusion_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("排除状态已变化")
            self._audit(
                "observation",
                str(row["observation_id"]),
                "exclusion.revoked",
                actor_id,
                {"exclusion_id": exclusion_id, "reason": reason},
            )
        return {"exclusion_id": exclusion_id, "status": "revoked"}

    def seal_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.seal")
        with transaction(self.connection, immediate=True):
            pending = self.connection.execute(
                "SELECT count(*) FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
                "WHERE o.batch_id=? AND e.status='pending'", (batch_id,)
            ).fetchone()[0]
            if pending:
                raise InvalidState("仍有待复核的排除申请")
            cursor = self.connection.execute(
                "UPDATE batches SET state='sealed',revision=revision+1,sealed_at=? "
                "WHERE batch_id=? AND state='running' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次状态或版本已变化")
            new_revision = expected_revision + 1
            now = self._now()
            self.connection.execute(
                "INSERT INTO analysis_jobs(batch_id,batch_revision,state,available_at,created_at,updated_at) "
                "VALUES(?,?, 'queued', ?,?,?)",
                (batch_id, new_revision, now, now, now),
            )
            self._audit("batch", batch_id, "batch.sealed", actor_id, {"revision": new_revision})
        return self.get_batch(batch_id)

    def claim_job(self, worker_id: str, lease_seconds: int = 60) -> dict[str, Any] | None:
        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT job_id FROM analysis_jobs WHERE "
                "(state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?) "
                "ORDER BY available_at,job_id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE analysis_jobs SET state='leased',attempts=attempts+1,lease_owner=?,lease_expires_at=?,updated_at=? "
                "WHERE job_id=?",
                (worker_id, expires, now, row["job_id"]),
            )
            claimed = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (row["job_id"],)).fetchone()
        return dict(claimed)

    def _analysis_observations(self, batch_id: str, protocol: Protocol) -> tuple[Observation, ...]:
        rows = self.connection.execute(
            "SELECT o.*,e.reason AS excluded_reason FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? ORDER BY o.observation_id",
            (batch_id,),
        ).fetchall()
        items: list[Observation] = []
        for row in rows:
            metrics = json.loads(row["metrics_json"])
            items.append(Observation(
                source_batch=row["source_batch"],
                source_row=row["source_row"],
                robot_id=row["robot_id"],
                protocol_id=protocol.protocol_id,
                protocol_version=protocol.version,
                stratum_key=row["stratum_key"],
                observed_at=row["observed_at"],
                metrics={key: Decimal(str(value)) for key, value in metrics.items()},
                excluded_reason=row["excluded_reason"],
            ))
        return tuple(items)

    def complete_job(self, worker_id: str, job_id: int, statistician_id: str) -> dict[str, Any]:
        self._require(statistician_id, "analysis.run")
        job = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)).fetchone()
        if job is None:
            raise NotFound("分析任务不存在")
        if job["state"] != "leased" or job["lease_owner"] != worker_id:
            raise InvalidState("任务未由当前工作进程持有")
        if job["lease_expires_at"] <= self._now():
            raise InvalidState("任务租约已经过期")
        batch = self.get_batch(job["batch_id"])
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        observations = self._analysis_observations(batch["batch_id"], protocol)
        snapshot_rows = [
            {
                "source_batch": item.source_batch,
                "source_row": item.source_row,
                "stratum": item.stratum_key,
                "metrics": {key: format(value, "f") for key, value in item.metrics.items()},
                "excluded_reason": item.excluded_reason,
            }
            for item in observations
        ]
        input_digest = content_digest(snapshot_rows)
        result = analyze(protocol, observations)
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT analysis_id,result_json FROM analyses WHERE batch_id=? AND batch_revision=? AND input_sha256=?",
                (batch["batch_id"], job["batch_revision"], input_digest),
            ).fetchone()
            if existing is None:
                cursor = self.connection.execute(
                    "INSERT INTO analyses(batch_id,batch_revision,protocol_sha256,input_sha256,algorithm_version,seed," 
                    "result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        batch["batch_id"], job["batch_revision"], protocol_digest, input_digest,
                        ALGORITHM_VERSION, protocol.seed, canonical_json(result), statistician_id, self._now(),
                    ),
                )
                analysis_id = cursor.lastrowid
            else:
                analysis_id = existing["analysis_id"]
                result = json.loads(existing["result_json"])
            self.connection.execute(
                "UPDATE analysis_jobs SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,updated_at=? "
                "WHERE job_id=? AND state='leased' AND lease_owner=?",
                (self._now(), job_id, worker_id),
            )
            self.connection.execute(
                "UPDATE batches SET state='analyzed' WHERE batch_id=? AND state IN ('sealed','analyzing')",
                (batch["batch_id"],),
            )
            self._audit(
                "batch",
                batch["batch_id"],
                "analysis.completed",
                statistician_id,
                {"analysis_id": analysis_id, "input_sha256": input_digest},
            )
        return {"analysis_id": analysis_id, "input_sha256": input_digest, "result": result}

    def fail_job(self, worker_id: str, job_id: int, error: str, retry_seconds: int = 0) -> dict[str, Any]:
        available = isoformat(self.clock.now() + timedelta(seconds=retry_seconds))
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE analysis_jobs SET state='queued',available_at=?,lease_owner=NULL,lease_expires_at=NULL," 
                "last_error=?,updated_at=? WHERE job_id=? AND state='leased' AND lease_owner=?",
                (available, error[:1000], self._now(), job_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("任务未由当前工作进程持有")
        return {"job_id": job_id, "state": "queued", "available_at": available}

    def decide(
        self, actor_id: str, batch_id: str, analysis_id: int, decision: str, reason: str
    ) -> dict[str, Any]:
        self._require(actor_id, "decision.write")
        if decision not in {"needs_more_data", "approved", "rejected"}:
            raise ValidationFailed("未知准入决定")
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE analysis_id=? AND batch_id=?", (analysis_id, batch_id)
        ).fetchone()
        if analysis_row is None:
            raise NotFound("分析版本不存在")
        if analysis_row["created_by"] == actor_id:
            raise Forbidden("统计负责人不能批准自己的分析")
        batch = self.get_batch(batch_id)
        if batch["state"] != "analyzed" or batch["revision"] != analysis_row["batch_revision"]:
            raise InvalidState("分析不是批次当前可审批版本")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO decisions(batch_id,analysis_id,decision,reason,decided_by,decided_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (batch_id, analysis_id, decision, reason, actor_id, self._now()),
                )
                self.connection.execute("UPDATE batches SET state='decided' WHERE batch_id=?", (batch_id,))
                self._audit(
                    "batch",
                    batch_id,
                    "decision.recorded",
                    actor_id,
                    {"decision_id": cursor.lastrowid, "analysis_id": analysis_id, "decision": decision},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该分析版本已经形成决定") from exc
        return {"batch_id": batch_id, "analysis_id": analysis_id, "decision": decision}

    def report(self, actor_id: str, batch_id: str) -> dict[str, Any]:
        user = self._user(actor_id)
        if user["role"] not in {"statistician", "approver", "auditor"}:
            raise Forbidden("当前角色不能读取完整报告")
        batch = self.get_batch(batch_id)
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE batch_id=? ORDER BY analysis_id DESC LIMIT 1", (batch_id,)
        ).fetchone()
        decision_row = None
        if analysis_row is not None:
            decision_row = self.connection.execute(
                "SELECT * FROM decisions WHERE analysis_id=?", (analysis_row["analysis_id"],)
            ).fetchone()
        exclusions = self.connection.execute(
            "SELECT e.exclusion_id,e.observation_id,e.status,e.reason,e.requested_by,e.reviewed_by "
            "FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
            "WHERE o.batch_id=? ORDER BY e.exclusion_id", (batch_id,)
        ).fetchall()
        events = self.connection.execute(
            "SELECT event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='batch' AND entity_id=? "
            "ORDER BY event_id", (batch_id,)
        ).fetchall()
        return {
            "batch": batch,
            "protocol": {
                "protocol_id": protocol.protocol_id,
                "version": protocol.version,
                "sha256": protocol_digest,
                "seed": protocol.seed,
                "bootstrap_samples": protocol.bootstrap_samples,
            },
            "analysis": None if analysis_row is None else {
                "analysis_id": analysis_row["analysis_id"],
                "input_sha256": analysis_row["input_sha256"],
                "algorithm_version": analysis_row["algorithm_version"],
                "created_by": analysis_row["created_by"],
                "result": json.loads(analysis_row["result_json"]),
            },
            "decision": None if decision_row is None else dict(decision_row),
            "exclusions": [dict(row) for row in exclusions],
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in events],
        }

    # ------------------------------------------------------------------
    # 持久化证据包导出
    # ------------------------------------------------------------------

    @staticmethod
    def _export_request(batch_ids: Sequence[str], records_per_shard: int) -> dict[str, Any]:
        return {"batch_ids": list(batch_ids), "records_per_shard": records_per_shard}

    def create_export(
        self,
        actor_id: str,
        batch_ids: Sequence[str],
        output_dir: str | Path,
        *,
        records_per_shard: int = 1000,
    ) -> dict[str, Any]:
        """提交批次集合：立即冻结各批次引用上界并返回（可能已有的）导出任务。"""

        self._require(actor_id, "export.create")
        if not batch_ids:
            raise ValidationFailed("导出批次集合不能为空")
        ordered = [str(item).strip() for item in batch_ids]
        if any(not item for item in ordered):
            raise ValidationFailed("批次编号不能为空")
        if len(set(ordered)) != len(ordered):
            raise ValidationFailed("导出批次集合不能重复")
        if isinstance(records_per_shard, bool) or not isinstance(records_per_shard, int):
            raise ValidationFailed("records_per_shard 必须是整数")
        if not 1 <= records_per_shard <= 100_000:
            raise ValidationFailed("records_per_shard 必须在 1 到 100000 之间")
        target_dir = str(Path(output_dir))
        request = self._export_request(ordered, records_per_shard) | {"output_dir": target_dir}
        request_digest = content_digest([request])
        task_id = f"exp-{request_digest}"

        # 查重、冻结上界读取与计划写入放在同一个立即事务中，保证冻结快照一致；
        # 并发提交同一请求时由 request_sha256 唯一约束兜底。
        try:
            with transaction(self.connection, immediate=True):
                existing = self.connection.execute(
                    "SELECT task_id FROM export_tasks WHERE request_sha256=?", (request_digest,)
                ).fetchone()
                if existing is not None:
                    replayed = existing["task_id"]
                else:
                    replayed = None
                if replayed is None:
                    now = self._now()
                    frozen: list[dict[str, Any]] = []
                    for ordinal, batch_id in enumerate(ordered):
                        batch = self.get_batch(batch_id)
                        protocol_row = self.connection.execute(
                            "SELECT content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
                            (batch["protocol_id"], batch["protocol_version"]),
                        ).fetchone()
                        if protocol_row is None:
                            raise NotFound("批次引用的协议版本不存在")
                        analysis_row = self.connection.execute(
                            "SELECT * FROM analyses WHERE batch_id=? ORDER BY analysis_id DESC LIMIT 1",
                            (batch_id,),
                        ).fetchone()
                        decision_row = None
                        if analysis_row is not None:
                            decision_row = self.connection.execute(
                                "SELECT * FROM decisions WHERE analysis_id=?", (analysis_row["analysis_id"],)
                            ).fetchone()
                        observation_high, observation_count = self.connection.execute(
                            "SELECT COALESCE(MAX(observation_id),0),COUNT(*) FROM observations WHERE batch_id=?",
                            (batch_id,),
                        ).fetchone()
                        event_high, event_count = self.connection.execute(
                            "SELECT COALESCE(MAX(event_id),0),COUNT(*) FROM audit_events "
                            "WHERE entity_type='batch' AND entity_id=?",
                            (batch_id,),
                        ).fetchone()
                        frozen.append({
                            "ordinal": ordinal,
                            "batch_id": batch_id,
                            "batch_revision": batch["revision"],
                            "protocol_id": batch["protocol_id"],
                            "protocol_version": batch["protocol_version"],
                            "protocol_sha256": protocol_row["content_sha256"],
                            "analysis_id": None if analysis_row is None else analysis_row["analysis_id"],
                            "analysis_sha256": None if analysis_row is None
                            else content_digest([json.loads(analysis_row["result_json"])]),
                            "decision_id": None if decision_row is None else decision_row["decision_id"],
                            "decision_sha256": None if decision_row is None
                            else content_digest([dict(decision_row)]),
                            "observation_high_id": int(observation_high),
                            "observation_count": int(observation_count),
                            "event_high_id": int(event_high),
                            "event_count": int(event_count),
                            "frozen_at": now,
                        })

                    def type_count(item: dict[str, Any]) -> dict[str, int]:
                        return {
                            "protocol": 1,
                            "analysis": 0 if item["analysis_id"] is None else 1,
                            "decision": 0 if item["decision_id"] is None else 1,
                            "observation": item["observation_count"],
                            "event": item["event_count"],
                        }

                    batch_type_counts = [type_count(item) for item in frozen]
                    total_records = sum(sum(counts.values()) for counts in batch_type_counts)
                    boundaries = exports.plan_shards(total_records, records_per_shard)
                    request_json = canonical_json(request)
                    self.connection.execute(
                        "INSERT INTO export_tasks(task_id,state,request_sha256,request_json,output_dir,shard_count,"
                        "records_per_shard,available_at,created_by,created_at,updated_at) "
                        "VALUES(?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (task_id, request_digest, request_json, target_dir, len(boundaries),
                         records_per_shard, now, actor_id, now, now),
                    )
                    for item in frozen:
                        self.connection.execute(
                            "INSERT INTO export_batches(task_id,ordinal,batch_id,batch_revision,protocol_id,"
                            "protocol_version,protocol_sha256,analysis_id,analysis_sha256,decision_id,"
                            "decision_sha256,observation_high_id,event_high_id,observation_count,event_count,frozen_at) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                task_id, item["ordinal"], item["batch_id"], item["batch_revision"], item["protocol_id"],
                                item["protocol_version"], item["protocol_sha256"], item["analysis_id"],
                                item["analysis_sha256"], item["decision_id"], item["decision_sha256"],
                                item["observation_high_id"], item["event_high_id"], item["observation_count"],
                                item["event_count"], item["frozen_at"],
                            ),
                        )
                    for shard_index, (start, end) in enumerate(boundaries):
                        expected = {name: 0 for name in exports.RECORD_TYPES}
                        cursor = 0
                        for counts in batch_type_counts:
                            batch_total = sum(counts.values())
                            overlap_start = max(0, start - cursor)
                            overlap_end = min(batch_total, end - cursor)
                            if overlap_end > overlap_start:
                                # 批次内记录按固定类型顺序排列，定位重叠区间覆盖的类型。
                                within = 0
                                for name in exports.RECORD_TYPES:
                                    type_start = within
                                    type_end = within + counts[name]
                                    covered = max(0, min(type_end, overlap_end) - max(type_start, overlap_start))
                                    expected[name] += covered
                                    within = type_end
                            cursor += batch_total
                        self.connection.execute(
                            "INSERT INTO export_shards(task_id,shard_index,ordinal_start,ordinal_end,state,"
                            "record_types_json,updated_at) VALUES(?,?,?,?, 'planned', ?,?)",
                            (task_id, shard_index, start, end, canonical_json(expected), now),
                        )
                    self._audit("export_task", task_id, "export.created", actor_id, {
                        "batch_count": len(ordered),
                        "record_count": total_records,
                        "shard_count": len(boundaries),
                    })
        except sqlite3.IntegrityError:
            # 并发提交了同一请求：重放必须返回同一任务而不是重复产物。
            row = self.connection.execute(
                "SELECT task_id FROM export_tasks WHERE request_sha256=?", (request_digest,)
            ).fetchone()
            replayed = row["task_id"]
        if replayed is not None:
            return self.get_export(actor_id, replayed) | {"replayed": True}
        return self.get_export(actor_id, task_id) | {"replayed": False}

    def claim_export(self, worker_id: str, lease_seconds: int = 60) -> dict[str, Any] | None:
        """领取排队中的导出任务，或接管租约到期的任务。"""

        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT task_id FROM export_tasks WHERE "
                "(state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?) "
                "ORDER BY available_at,created_at LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE export_tasks SET state='leased',attempts=attempts+1,lease_owner=?,lease_expires_at=?,"
                "updated_at=? WHERE task_id=?",
                (worker_id, expires, now, row["task_id"]),
            )
            claimed = self.connection.execute(
                "SELECT * FROM export_tasks WHERE task_id=?", (row["task_id"],)
            ).fetchone()
        return dict(claimed)

    def _lease_export(self, task_id: str, worker_id: str) -> sqlite3.Row:
        task = self.connection.execute(
            "SELECT * FROM export_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise NotFound("导出任务不存在")
        if task["state"] != "leased" or task["lease_owner"] != worker_id:
            raise InvalidState("导出任务未由当前工作进程持有")
        if task["lease_expires_at"] <= self._now():
            raise InvalidState("导出任务租约已经过期")
        return task

    def _frozen_batches(self, task_id: str) -> list[sqlite3.Row]:
        return list(self.connection.execute(
            "SELECT * FROM export_batches WHERE task_id=? ORDER BY ordinal", (task_id,)
        ).fetchall())

    def _batch_records(self, item: sqlite3.Row) -> list[dict[str, Any]]:
        """按固定类型顺序构造单个批次冻结范围内的全部记录。"""

        records: list[dict[str, Any]] = []
        protocol_row = self.connection.execute(
            "SELECT canonical_json,content_sha256 FROM protocol_catalog "
            "WHERE protocol_id=? AND version=?",
            (item["protocol_id"], item["protocol_version"]),
        ).fetchone()
        if protocol_row is None or protocol_row["content_sha256"] != item["protocol_sha256"]:
            raise ExportIntegrityError(
                f"批次 {item['batch_id']} 冻结的协议版本缺失或被改写"
            )
        records.append({
            "batch_id": item["batch_id"],
            "record_type": "protocol",
            "record": {
                "batch_id": item["batch_id"],
                "protocol_id": item["protocol_id"],
                "version": item["protocol_version"],
                "content_sha256": item["protocol_sha256"],
                "document": json.loads(protocol_row["canonical_json"]),
            },
        })
        if item["analysis_id"] is not None:
            analysis_row = self.connection.execute(
                "SELECT * FROM analyses WHERE analysis_id=?", (item["analysis_id"],)
            ).fetchone()
            if analysis_row is None:
                raise ExportIntegrityError(f"冻结的分析 {item['analysis_id']} 已不存在")
            records.append({
                "batch_id": item["batch_id"],
                "record_type": "analysis",
                "record": {
                    "batch_id": item["batch_id"],
                    "analysis_id": analysis_row["analysis_id"],
                    "batch_revision": analysis_row["batch_revision"],
                    "protocol_sha256": analysis_row["protocol_sha256"],
                    "input_sha256": analysis_row["input_sha256"],
                    "algorithm_version": analysis_row["algorithm_version"],
                    "seed": analysis_row["seed"],
                    "result": json.loads(analysis_row["result_json"]),
                },
            })
        if item["decision_id"] is not None:
            decision_row = self.connection.execute(
                "SELECT * FROM decisions WHERE decision_id=?", (item["decision_id"],)
            ).fetchone()
            if decision_row is None:
                raise ExportIntegrityError(f"冻结的决定 {item['decision_id']} 已不存在")
            records.append({
                "batch_id": item["batch_id"],
                "record_type": "decision",
                "record": {
                    "batch_id": item["batch_id"],
                    "decision_id": decision_row["decision_id"],
                    "analysis_id": decision_row["analysis_id"],
                    "decision": decision_row["decision"],
                    "reason": decision_row["reason"],
                    "decided_by": decision_row["decided_by"],
                    "decided_at": decision_row["decided_at"],
                },
            })
        observation_rows = self.connection.execute(
            "SELECT o.*,e.reason AS excluded_reason FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? AND o.observation_id<=? ORDER BY o.observation_id",
            (item["batch_id"], item["observation_high_id"]),
        ).fetchall()
        for row in observation_rows:
            records.append({
                "batch_id": item["batch_id"],
                "record_type": "observation",
                "record": {
                    "batch_id": item["batch_id"],
                    "observation_id": row["observation_id"],
                    "source_batch": row["source_batch"],
                    "source_row": row["source_row"],
                    "robot_id": row["robot_id"],
                    "stratum_key": row["stratum_key"],
                    "observed_at": row["observed_at"],
                    "metrics": json.loads(row["metrics_json"]),
                    "content_sha256": row["content_sha256"],
                    "excluded_reason": row["excluded_reason"],
                },
            })
        event_rows = self.connection.execute(
            "SELECT event_id,event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='batch' AND entity_id=? AND event_id<=? ORDER BY event_id",
            (item["batch_id"], item["event_high_id"]),
        ).fetchall()
        for row in event_rows:
            records.append({
                "batch_id": item["batch_id"],
                "record_type": "event",
                "record": {
                    "batch_id": item["batch_id"],
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "actor_id": row["actor_id"],
                    "payload": json.loads(row["payload_json"]),
                    "created_at": row["created_at"],
                },
            })
        return records

    def _records_for_range(
        self, frozen_rows: Sequence[sqlite3.Row], ordinal_start: int, ordinal_end: int
    ) -> list[dict[str, Any]]:
        """取出全局序号区间 [start,end) 内的记录并补编全局序号。"""

        envelopes: list[dict[str, Any]] = []
        cursor = 0
        for item in frozen_rows:
            batch_total = (
                1
                + (0 if item["analysis_id"] is None else 1)
                + (0 if item["decision_id"] is None else 1)
                + item["observation_count"]
                + item["event_count"]
            )
            if cursor + batch_total > ordinal_start and cursor < ordinal_end:
                batch_records = self._batch_records(item)
                if len(batch_records) != batch_total:
                    raise ExportIntegrityError(
                        f"批次 {item['batch_id']} 冻结记录数与实际不符"
                    )
                low = max(0, ordinal_start - cursor)
                high = min(batch_total, ordinal_end - cursor)
                for record in batch_records[low:high]:
                    envelopes.append(record)
            cursor += batch_total
        if len(envelopes) != ordinal_end - ordinal_start:
            raise ExportIntegrityError("导出记录区间与分片计划不一致")
        for offset, record in enumerate(envelopes):
            record["ordinal"] = ordinal_start + offset
        return envelopes

    def _fail_export_permanently(self, task_id: str, error: str) -> None:
        self.connection.execute(
            "UPDATE export_tasks SET state='failed',lease_owner=NULL,lease_expires_at=NULL,"
            "last_error=?,updated_at=?,finished_at=? WHERE task_id=?",
            (error[:1000], self._now(), self._now(), task_id),
        )

    def advance_export(
        self, worker_id: str, task_id: str, *, max_shards: int | None = None
    ) -> dict[str, Any]:
        """从最后已确认分片继续生成；全部确认并复核通过后才完成任务。"""

        task = self._lease_export(task_id, worker_id)
        shard_rows = self.connection.execute(
            "SELECT * FROM export_shards WHERE task_id=? ORDER BY shard_index", (task_id,)
        ).fetchall()
        frozen_rows = self._frozen_batches(task_id)
        output_dir = task["output_dir"]
        processed = 0
        for shard in shard_rows:
            if shard["state"] == "confirmed":
                continue
            if self._now() >= task["lease_expires_at"]:
                break
            if max_shards is not None and processed >= max_shards:
                break
            start, end = shard["ordinal_start"], shard["ordinal_end"]
            try:
                envelopes = self._records_for_range(frozen_rows, start, end)
                payload = exports.encode_records(envelopes)
                target = exports.shard_path(output_dir, task_id, shard["shard_index"])
                # 未确认分片可能来自崩溃的前次尝试，直接覆盖重写。
                exports.atomic_write(target, payload)
                summary = exports.inspect_shard_file(target, start, end)
                expected_types = json.loads(shard["record_types_json"])
                if summary["record_types"] != expected_types:
                    raise ExportIntegrityError(
                        f"分片 {shard['shard_index']} 记录类型分布与冻结计划不一致"
                    )
            except ExportIntegrityError:
                with transaction(self.connection, immediate=True):
                    self._fail_export_permanently(task_id, f"分片 {shard['shard_index']} 复核失败")
                    self._audit("export_task", task_id, "export.failed", worker_id, {
                        "shard_index": shard["shard_index"],
                    })
                raise
            now = self._now()
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "UPDATE export_shards SET state='confirmed',record_count=?,record_types_json=?,"
                    "first_record_key=?,last_record_key=?,content_sha256=?,updated_at=? "
                    "WHERE task_id=? AND shard_index=? AND state='planned'",
                    (
                        summary["record_count"], canonical_json(summary["record_types"]),
                        summary["first_record_key"], summary["last_record_key"],
                        summary["content_sha256"], now, task_id, shard["shard_index"],
                    ),
                )
                self.connection.execute(
                    "UPDATE export_tasks SET updated_at=? WHERE task_id=?", (now, task_id)
                )
            processed += 1

        refreshed = self.connection.execute(
            "SELECT * FROM export_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        pending = self.connection.execute(
            "SELECT COUNT(*) FROM export_shards WHERE task_id=? AND state='planned'", (task_id,)
        ).fetchone()[0]
        if pending:
            return {
                "task_id": task_id,
                "state": refreshed["state"],
                "processed": processed,
                "shards_pending": pending,
                "lease_active": self._now() < refreshed["lease_expires_at"],
            }
        try:
            return self._finalize_export(task_id, worker_id)
        except ExportIntegrityError as exc:
            # 全量复核（含已确认分片）发现篡改或缺损：任务失败，不会产出清单。
            with transaction(self.connection, immediate=True):
                self._fail_export_permanently(task_id, str(exc))
                self._audit("export_task", task_id, "export.failed", worker_id, {"stage": "finalize"})
            raise

    def _finalize_export(self, task_id: str, worker_id: str) -> dict[str, Any]:
        """重新复核全部分片，构建清单并完成任务。"""

        task = self.connection.execute("SELECT * FROM export_tasks WHERE task_id=?", (task_id,)).fetchone()
        frozen_rows = self._frozen_batches(task_id)
        shard_rows = self.connection.execute(
            "SELECT * FROM export_shards WHERE task_id=? ORDER BY shard_index", (task_id,)
        ).fetchall()
        if len(shard_rows) != task["shard_count"]:
            raise ExportIntegrityError("分片数量与任务计划不一致")
        shard_summaries: list[dict[str, Any]] = []
        for shard in shard_rows:
            target = exports.shard_path(task["output_dir"], task_id, shard["shard_index"])
            try:
                summary = exports.inspect_shard_file(target, shard["ordinal_start"], shard["ordinal_end"])
            except ExportIntegrityError as exc:
                raise ExportIntegrityError(f"分片 {shard['shard_index']} 复核失败: {exc}") from exc
            if shard["state"] != "confirmed" or summary["content_sha256"] != shard["content_sha256"]:
                raise ExportIntegrityError(
                    f"分片 {shard['shard_index']} 已确认内容被改动，拒绝完成导出"
                )
            if summary["record_count"] != shard["record_count"]:
                raise ExportIntegrityError(f"分片 {shard['shard_index']} 记录数与确认摘要不一致")
            shard_summaries.append({
                "index": shard["shard_index"],
                "file": target.name,
                "ordinal_start": shard["ordinal_start"],
                "ordinal_end": shard["ordinal_end"],
                "record_count": summary["record_count"],
                "record_types": summary["record_types"],
                "first_record_key": summary["first_record_key"],
                "last_record_key": summary["last_record_key"],
                "content_sha256": summary["content_sha256"],
            })
        total_records = sum(item["record_count"] for item in shard_summaries)
        manifest_entries = [dict(row) for row in frozen_rows]
        for entry in manifest_entries:
            entry.pop("task_id", None)
        manifest = exports.build_manifest(
            task_id=task_id,
            request_sha256=task["request_sha256"],
            records_per_shard=task["records_per_shard"],
            total_records=total_records,
            batches=manifest_entries,
            shard_summaries=shard_summaries,
            created_at=self._now(),
        )
        manifest_text = canonical_json(manifest)
        manifest_digest = _sha256_text(manifest_text)
        target = exports.manifest_path(task["output_dir"], task_id)
        exports.atomic_write(target, manifest_text.encode("utf-8"))
        now = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "INSERT INTO export_manifests(task_id,manifest_sha256,body_json,created_at) "
                "VALUES(?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET "
                "manifest_sha256=excluded.manifest_sha256,body_json=excluded.body_json,created_at=excluded.created_at",
                (task_id, manifest_digest, manifest_text, now),
            )
            self.connection.execute(
                "UPDATE export_tasks SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=NULL,updated_at=?,finished_at=? WHERE task_id=?",
                (now, now, task_id),
            )
            self._audit("export_task", task_id, "export.completed", worker_id, {
                "shard_count": len(shard_summaries),
                "record_count": total_records,
                "manifest_sha256": manifest_digest,
            })
        return {
            "task_id": task_id,
            "state": "succeeded",
            "processed": 0,
            "shards_pending": 0,
            "manifest_sha256": manifest_digest,
            "record_count": total_records,
        }

    def fail_export(self, worker_id: str, task_id: str, error: str, retry_seconds: int = 0) -> dict[str, Any]:
        self._lease_export(task_id, worker_id)
        available = isoformat(self.clock.now() + timedelta(seconds=retry_seconds))
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE export_tasks SET state='queued',available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=?,updated_at=? WHERE task_id=? AND state='leased' AND lease_owner=?",
                (available, error[:1000], self._now(), task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("导出任务未由当前工作进程持有")
        return {"task_id": task_id, "state": "queued", "available_at": available}

    def get_export(self, actor_id: str, task_id: str) -> dict[str, Any]:
        self._require(actor_id, "export.read")
        return self._export_status(task_id)

    def _export_status(self, task_id: str) -> dict[str, Any]:
        task = self.connection.execute(
            "SELECT * FROM export_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise NotFound("导出任务不存在")
        shards = self.connection.execute(
            "SELECT shard_index,ordinal_start,ordinal_end,state,record_count,record_types_json,"
            "first_record_key,last_record_key,content_sha256 FROM export_shards "
            "WHERE task_id=? ORDER BY shard_index",
            (task_id,),
        ).fetchall()
        manifest = self.connection.execute(
            "SELECT manifest_sha256,created_at FROM export_manifests WHERE task_id=?", (task_id,)
        ).fetchone()
        return {
            "task_id": task["task_id"],
            "state": task["state"],
            "attempts": task["attempts"],
            "output_dir": task["output_dir"],
            "shard_count": task["shard_count"],
            "records_per_shard": task["records_per_shard"],
            "shards_confirmed": sum(1 for row in shards if row["state"] == "confirmed"),
            "shards_pending": sum(1 for row in shards if row["state"] == "planned"),
            "lease_owner": task["lease_owner"],
            "lease_expires_at": task["lease_expires_at"],
            "last_error": task["last_error"],
            "created_at": task["created_at"],
            "finished_at": task["finished_at"],
            "manifest": None if manifest is None else {
                "manifest_sha256": manifest["manifest_sha256"],
                "created_at": manifest["created_at"],
            },
            "shards": [
                {
                    "index": row["shard_index"],
                    "ordinal_start": row["ordinal_start"],
                    "ordinal_end": row["ordinal_end"],
                    "state": row["state"],
                    "record_count": row["record_count"],
                    "record_types": json.loads(row["record_types_json"]),
                    "first_record_key": row["first_record_key"],
                    "last_record_key": row["last_record_key"],
                    "content_sha256": row["content_sha256"],
                }
                for row in shards
            ],
        }

    def verify_export(self, task_id: str) -> dict[str, Any]:
        """独立复核已完成导出：重读磁盘清单与全部分片并逐项比对。"""

        task = self.connection.execute(
            "SELECT * FROM export_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise NotFound("导出任务不存在")
        if task["state"] != "succeeded":
            raise InvalidState("只有已完成的导出任务可以复核")
        manifest_row = self.connection.execute(
            "SELECT * FROM export_manifests WHERE task_id=?", (task_id,)
        ).fetchone()
        if manifest_row is None:
            raise ExportIntegrityError("数据库中缺少导出清单")
        manifest_file = exports.manifest_path(task["output_dir"], task_id)
        try:
            manifest_bytes = manifest_file.read_bytes()
        except OSError as exc:
            raise ExportIntegrityError(f"无法读取清单 {manifest_file.name}: {exc}") from exc
        if _sha256_text(manifest_bytes.decode("utf-8")) != manifest_row["manifest_sha256"]:
            raise ExportIntegrityError("清单文件摘要与数据库记录不一致")
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ExportIntegrityError("清单不是有效 JSON") from exc
        if manifest_bytes.decode("utf-8") != canonical_json(manifest):
            raise ExportIntegrityError("清单序列化形式不是规范化文本")
        shard_rows = self.connection.execute(
            "SELECT * FROM export_shards WHERE task_id=? ORDER BY shard_index", (task_id,)
        ).fetchall()
        if len(manifest["shards"]) != len(shard_rows):
            raise ExportIntegrityError("清单分片数量与数据库不一致")
        for entry, shard in zip(manifest["shards"], shard_rows):
            path = Path(task["output_dir"]) / task_id / entry["file"]
            summary = exports.inspect_shard_file(path, shard["ordinal_start"], shard["ordinal_end"])
            if summary["content_sha256"] != entry["content_sha256"]:
                raise ExportIntegrityError(f"分片 {entry['file']} 内容摘要与清单不一致")
            if summary["content_sha256"] != shard["content_sha256"]:
                raise ExportIntegrityError(f"分片 {entry['file']} 内容摘要与确认记录不一致")
            if summary["record_count"] != entry["record_count"]:
                raise ExportIntegrityError(f"分片 {entry['file']} 记录数与清单不一致")
            if summary["record_types"] != entry["record_types"]:
                raise ExportIntegrityError(f"分片 {entry['file']} 记录类型分布与清单不一致")
        recomputed = _sha256_text(canonical_json([
            {
                "index": item["index"],
                "file": item["file"],
                "ordinal_start": item["ordinal_start"],
                "ordinal_end": item["ordinal_end"],
                "content_sha256": item["content_sha256"],
            }
            for item in manifest["shards"]
        ]))
        if recomputed != manifest["overall_sha256"]:
            raise ExportIntegrityError("整体摘要复核失败")
        frozen = self._frozen_batches(task_id)
        if len(manifest["batches"]) != len(frozen):
            raise ExportIntegrityError("清单冻结批次数量与数据库不一致")
        return {
            "ok": True,
            "task_id": task_id,
            "shard_count": len(shard_rows),
            "record_count": manifest["record_count_total"],
            "manifest_sha256": manifest_row["manifest_sha256"],
            "overall_sha256": manifest["overall_sha256"],
        }
