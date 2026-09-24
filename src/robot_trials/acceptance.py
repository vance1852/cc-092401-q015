"""完整产品流程的离线验收入口。"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from .export import ExportService
from .jsonio import load_json
from .service import TrialService
from .storage import connect, inspect_schema


def _decide_batch(service: TrialService, worker_id: str, stat_id: str, approver_id: str, batch_id: str) -> dict:
    job = service.claim_job(worker_id, lease_seconds=60)
    if job is None or job["batch_id"] != batch_id:
        raise RuntimeError(f"未能领取批次 {batch_id} 的分析任务")
    analysis = service.complete_job(worker_id, job["job_id"], stat_id)
    decision_value = "approved" if analysis["result"]["conclusion"] == "pass" else "rejected"
    service.decide(approver_id, batch_id, analysis["analysis_id"], decision_value, "离线验收决定")
    return analysis


def run(workspace: Path) -> dict[str, object]:
    fixtures = workspace / "fixtures"
    protocol = load_json(fixtures / "demo_protocol.json")
    observation_rows = [
        json.loads(line)
        for line in (fixtures / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    with tempfile.TemporaryDirectory(prefix="robot-trials-") as temporary:
        root = Path(temporary)
        database = root / "foundation.sqlite3"
        export_root = root / "exports"
        connection = connect(database)
        try:
            service = TrialService(connection)
            service.create_user("operator-1", "测试操作员", "operator")
            service.create_user("stat-1", "统计负责人", "statistician")
            service.create_user("approver-1", "准入审批人", "approver")
            service.create_user("auditor-1", "审计人员", "auditor")
            service.register_robot("operator-1", "robot-a", "A 型人形机器人", "示例厂商")
            service.register_build("operator-1", "build-a1", "robot-a", "1.0.0", "a" * 64)
            service.publish_protocol("stat-1", protocol)
            analyses: dict[str, dict] = {}
            for batch_id, import_key in (
                ("batch-demo", "demo-import-1"),
                ("batch-demo-b", "demo-import-2"),
            ):
                service.create_batch("operator-1", batch_id, protocol["protocol_id"], protocol["version"], "build-a1")
                service.start_batch("operator-1", batch_id, 1)
                imported = service.import_observations(
                    "operator-1", batch_id, import_key, observation_rows
                )
                service.seal_batch("stat-1", batch_id, 2)
                analyses[batch_id] = _decide_batch(service, "worker-1", "stat-1", "approver-1", batch_id)
            report = service.report("auditor-1", "batch-demo")
            exports = ExportService(connection)
            export_job = exports.submit_export(
                "auditor-1", ["batch-demo-b", "batch-demo"], shard_size=1
            )
            claimed = exports.claim_export("worker-1", lease_seconds=60)
            if claimed is None or claimed["export_id"] != export_job["export_id"]:
                raise RuntimeError("未能领取导出任务")
            outcomes = []
            while True:
                outcome = exports.advance_export("worker-1", export_job["export_id"], export_root)
                outcomes.append(outcome)
                if outcome["state"] == "succeeded":
                    break
            verification = exports.verify_export(export_job["export_id"], export_root)
            replay = exports.submit_export("auditor-1", ["batch-demo", "batch-demo-b"], shard_size=1)
            export_view = exports.get_export("auditor-1", export_job["export_id"])
            schema = inspect_schema(connection)
        finally:
            connection.close()
    first_analysis = analyses["batch-demo"]
    if schema["missing_tables"] or schema["schema_version"] != "3":
        raise RuntimeError("SQLite 基础结构检查失败")
    if not verification["ok"] or not verification["complete"]:
        raise RuntimeError("导出证据包复核失败")
    if replay["export_id"] != export_job["export_id"]:
        raise RuntimeError("重复导出请求没有返回同一任务")
    return {
        "status": "ok",
        "protocol": f"{protocol['protocol_id']}@{protocol['version']}",
        "observation_count": imported["inserted"],
        "analysis_id": first_analysis["analysis_id"],
        "input_sha256": first_analysis["input_sha256"],
        "conclusion": first_analysis["result"]["conclusion"],
        "decision": report["decision"]["decision"],
        "event_count": len(report["events"]),
        "export_id": export_job["export_id"],
        "export_state": outcomes[-1]["state"],
        "export_shards": verification["verified_shards"],
        "export_records": export_view["record_count"],
        "export_manifest_sha256": outcomes[-1]["manifest_sha256"],
        "export_replay_same_task": replay["export_id"] == export_job["export_id"],
        "schema": schema,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行试验数据基础工具的离线自检")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    result = run(args.workspace.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
