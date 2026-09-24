"""持久化证据包导出：确定性分片 JSONL、原子写入与清单复核。

所有产物只依赖数据库中冻结的上界和不可变行，因此同一导出任务的任意重跑
都会得到字节一致的分片与清单。分片先写临时文件再原子改名，只有复核通过
后才会在数据库中确认；崩溃后未确认的分片可以被安全覆盖重写。
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from .errors import ExportIntegrityError
from .jsonio import canonical_json

MANIFEST_VERSION = "robot-trials-export/1"
RECORD_TYPES = ("protocol", "analysis", "decision", "observation", "event")


def shard_path(output_dir: str | Path, task_id: str, shard_index: int) -> Path:
    return Path(output_dir) / task_id / f"data-{shard_index:05d}.jsonl"


def manifest_path(output_dir: str | Path, task_id: str) -> Path:
    return Path(output_dir) / task_id / "manifest.json"


def plan_shards(total_records: int, records_per_shard: int) -> list[tuple[int, int]]:
    """按全局记录序号 [start, end) 切出确定的分片边界。"""

    if total_records <= 0:
        raise ValueError("导出至少需要一条记录")
    boundaries: list[tuple[int, int]] = []
    start = 0
    while start < total_records:
        end = min(start + records_per_shard, total_records)
        boundaries.append((start, end))
        start = end
    return boundaries


def encode_records(records: Iterable[dict[str, Any]]) -> bytes:
    """把信封记录编码为确定的 JSONL 字节。"""

    return b"".join(
        canonical_json(record).encode("utf-8") + b"\n" for record in records
    )


def _fsync_directory(directory: Path) -> None:
    handle = None
    try:
        handle = os.open(str(directory), os.O_RDONLY)
        os.fsync(handle)
    except OSError:
        # 某些文件系统不支持目录 fsync；原子改名已经足够保证一致性。
        pass
    finally:
        if handle is not None:
            os.close(handle)


def atomic_write(path: Path, payload: bytes) -> None:
    """临时文件 + fsync + 原子改名，保证分片要么完整要么不存在。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _record_key(record: Mapping[str, Any]) -> str:
    record_type = record["record_type"]
    body = record["record"]
    if record_type == "protocol":
        source = f"v{body['version']}"
    elif record_type == "analysis":
        source = str(body["analysis_id"])
    elif record_type == "decision":
        source = str(body["decision_id"])
    elif record_type == "observation":
        source = str(body["observation_id"])
    else:
        source = str(body["event_id"])
    return f"{record['batch_id']}/{record_type}/{source}"


def _order_token(envelope: Mapping[str, Any]) -> tuple[int, int]:
    """批次内确定性排序令牌：记录类型固定顺序 + 数值主键。"""

    record_type = envelope["record_type"]
    rank = RECORD_TYPES.index(record_type)
    body = envelope["record"]
    if record_type == "protocol":
        identity = int(body["version"])
    elif record_type == "analysis":
        identity = int(body["analysis_id"])
    elif record_type == "decision":
        identity = int(body["decision_id"])
    elif record_type == "observation":
        identity = int(body["observation_id"])
    else:
        identity = int(body["event_id"])
    return rank, identity


def inspect_shard_bytes(
    payload: bytes, ordinal_start: int, ordinal_end: int
) -> dict[str, Any]:
    """解析并复核单个分片的字节内容，返回摘要信息。"""

    envelopes: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(payload.splitlines(), start=1):
        if not raw_line.strip():
            raise ExportIntegrityError(f"分片第 {line_number} 行为空")
        try:
            envelope = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExportIntegrityError(f"分片第 {line_number} 行不是有效 JSON") from exc
        envelopes.append(envelope)
    expected = ordinal_end - ordinal_start
    if len(envelopes) != expected:
        raise ExportIntegrityError(
            f"分片记录数应为 {expected}，实际为 {len(envelopes)}"
        )
    type_counts = {name: 0 for name in RECORD_TYPES}
    # 记录按批次的请求顺序、批次内按 RECORD_TYPES 的固定顺序排列；
    # 同类型观测/事件按其不可变主键升序。
    previous_batch: str | None = None
    previous_rank = -1
    previous_identity = -1
    for offset, envelope in enumerate(envelopes):
        ordinal = ordinal_start + offset
        if envelope.get("ordinal") != ordinal:
            raise ExportIntegrityError(
                f"分片第 {offset + 1} 条记录序号应为 {ordinal}，实际为 {envelope.get('ordinal')}"
            )
        record_type = envelope.get("record_type")
        if record_type not in type_counts:
            raise ExportIntegrityError(f"未知记录类型: {record_type}")
        record = envelope.get("record")
        if not isinstance(record, dict) or record.get("batch_id") != envelope.get("batch_id"):
            raise ExportIntegrityError(
                f"分片第 {offset + 1} 条记录缺少一致的 batch_id"
            )
        batch_id = envelope["batch_id"]
        rank, identity = _order_token(envelope)
        if batch_id == previous_batch:
            if rank < previous_rank or (rank == previous_rank and identity <= previous_identity):
                raise ExportIntegrityError("分片内批次记录顺序不确定或存在重复")
        previous_batch = batch_id
        previous_rank = rank
        previous_identity = identity
        type_counts[record_type] += 1
    first = None if not envelopes else _record_key(envelopes[0])
    last = None if not envelopes else _record_key(envelopes[-1])
    return {
        "record_count": len(envelopes),
        "record_types": type_counts,
        "first_record_key": first,
        "last_record_key": last,
        "content_sha256": hashlib.sha256(payload).hexdigest(),
    }


def inspect_shard_file(path: Path, ordinal_start: int, ordinal_end: int) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ExportIntegrityError(f"无法读取分片 {path.name}: {exc}") from exc
    summary = inspect_shard_bytes(payload, ordinal_start, ordinal_end)
    summary["file"] = path.name
    return summary


def build_manifest(
    *,
    task_id: str,
    request_sha256: str,
    records_per_shard: int,
    total_records: int,
    batches: Sequence[Mapping[str, Any]],
    shard_summaries: Sequence[Mapping[str, Any]],
    created_at: str,
) -> dict[str, Any]:
    """聚合各分片摘要构造清单；整体摘要覆盖全部分片摘要。"""

    overall_input = [
        {
            "index": item["index"],
            "file": item["file"],
            "ordinal_start": item["ordinal_start"],
            "ordinal_end": item["ordinal_end"],
            "content_sha256": item["content_sha256"],
        }
        for item in shard_summaries
    ]
    overall_sha256 = hashlib.sha256(
        canonical_json(overall_input).encode("utf-8")
    ).hexdigest()
    return {
        "manifest_version": MANIFEST_VERSION,
        "task_id": task_id,
        "request_sha256": request_sha256,
        "records_per_shard": records_per_shard,
        "record_count_total": total_records,
        "shard_count": len(shard_summaries),
        "overall_sha256": overall_sha256,
        "cutoff": (
            "每个批次按 frozen_at 及其 observation_high_id/event_high_id 冻结；"
            "冻结之后产生的观测与事件不属于本次导出。"
        ),
        "created_at": created_at,
        "batches": [dict(item) for item in batches],
        "shards": [dict(item) for item in shard_summaries],
    }
