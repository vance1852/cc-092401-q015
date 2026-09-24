"""不依赖 HTTP 服务的证据包导出命令行入口。

子命令：
  create   审计人员提交批次集合并冻结上界
  claim    工作进程领取（或到期接管）一个导出任务
  advance  从最后已确认分片继续推进指定任务
  process  领取并循环推进，直到任务完成、失败或暂时无任务
  status   查看任务与各分片状态
  verify   独立复核已完成导出的清单与全部分片

所有状态都保存在 SQLite 与输出目录中，命令可以反复执行、跨进程接管。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import ServiceError
from .jsonio import canonical_json
from .service import TrialService
from .storage import connect


def _print(value: object) -> None:
    print(canonical_json(value))


def _add_database(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", type=Path, required=True)


def _build_service(args: argparse.Namespace) -> tuple[TrialService, object]:
    connection = connect(args.database)
    return TrialService(connection), connection


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="证据包持久化导出命令行")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="提交批次集合并冻结上界")
    _add_database(create)
    create.add_argument("--actor", required=True, help="审计人员用户编号")
    create.add_argument("--batches", required=True, help="逗号分隔的批次编号（顺序即导出顺序）")
    create.add_argument("--output-dir", type=Path, required=True)
    create.add_argument("--records-per-shard", type=int, default=1000)

    claim = sub.add_parser("claim", help="领取或到期接管一个导出任务")
    _add_database(claim)
    claim.add_argument("--worker", required=True)
    claim.add_argument("--lease-seconds", type=int, default=60)

    advance = sub.add_parser("advance", help="推进指定导出任务的下若干个分片")
    _add_database(advance)
    advance.add_argument("--worker", required=True)
    advance.add_argument("--task", required=True)
    advance.add_argument("--max-shards", type=int, default=None)

    process = sub.add_parser("process", help="领取并循环推进直到完成/失败/无任务")
    _add_database(process)
    process.add_argument("--worker", required=True)
    process.add_argument("--lease-seconds", type=int, default=60)
    process.add_argument("--max-shards-per-turn", type=int, default=1)

    status = sub.add_parser("status", help="查看任务与分片状态")
    _add_database(status)
    status.add_argument("--actor", required=True)
    status.add_argument("--task", required=True)

    verify = sub.add_parser("verify", help="复核已完成导出")
    _add_database(verify)
    verify.add_argument("--task", required=True)

    args = parser.parse_args(argv)
    service, connection = _build_service(args)
    try:
        if args.command == "create":
            batch_ids = [item.strip() for item in args.batches.split(",") if item.strip()]
            result = service.create_export(
                args.actor, batch_ids, args.output_dir,
                records_per_shard=args.records_per_shard,
            )
            _print(result)
            return 0
        if args.command == "claim":
            claimed = service.claim_export(args.worker, args.lease_seconds)
            _print({"job": claimed})
            return 0 if claimed is not None else 3
        if args.command == "advance":
            result = service.advance_export(args.worker, args.task, max_shards=args.max_shards)
            _print(result)
            return 0
        if args.command == "process":
            claimed = service.claim_export(args.worker, args.lease_seconds)
            if claimed is None:
                _print({"state": "idle"})
                return 3
            task_id = claimed["task_id"]
            while True:
                result = service.advance_export(
                    args.worker, task_id, max_shards=args.max_shards_per_turn
                )
                if result["state"] in {"succeeded", "failed"}:
                    _print(result)
                    return 0 if result["state"] == "succeeded" else 4
                if result["shards_pending"] <= 0:
                    _print(result)
                    return 0
                if not result["lease_active"]:
                    _print(result | {"hint": "租约到期，需要重新 claim/接管"})
                    return 5
        if args.command == "status":
            _print(service.get_export(args.actor, args.task))
            return 0
        if args.command == "verify":
            _print(service.verify_export(args.task))
            return 0
    except ServiceError as exc:
        _print({"error": {"code": exc.code, "message": str(exc)}})
        return 2
    finally:
        connection.close()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
