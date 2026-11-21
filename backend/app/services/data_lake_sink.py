"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。
跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、event_archive（归档接口）。
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
import zlib
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def __init__(self, lake_base_dir: str | None = None) -> None:
        super().__init__()
        if lake_base_dir is None:
            # 环境变量优先，便于测试与多实例部署做进程级目录隔离。
            lake_base_dir = os.environ.get("DATA_LAKE_BASE_DIR") or os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data_store", "lake",
            )
        self._lake_base = lake_base_dir
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0,
                    record_hash: str | None = None) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 确定分区路径（按日期/事件类型）
        2. 序列化数据（默认JSON格式）
        3. 写入分区文件
        4. 更新写入统计

        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件数据
            source_channel_id: 来源通道ID
            record_hash: 可选的外部内容指纹；不传则按载荷计算。
                断点续做时同一来源行应传入相同指纹，保证回执可对账。

        Returns:
            { ok: bool, message: str, event_id, partition: str,
              file_path: str, record_hash: str }
        """
        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        # 构建数据记录
        if record_hash is None:
            record_hash = hashlib.sha256(
                json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()[:16]
        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "record_hash": record_hash,
        }

        # 写入分区文件：同一秒内可能写入多条事件，旧命名只取 event_id
        # 前8字符会在同批写入时发生文件名碰撞、互相覆盖，因此使用
        # 完整 event_id（清洗路径非法字符）加短随机后缀保证唯一。
        safe_event_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in event_id)
        file_name = f"{now.strftime('%H%M%S')}_{safe_event_id}_{uuid.uuid4().hex[:6]}.json"
        file_path = os.path.join(partition_path, file_name)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1

            return {
                "ok": True,
                "message": f"数据已写入数据湖分区 {date_partition}",
                "event_id": event_id,
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
                "record_hash": record["record_hash"],
            }
        except OSError as e:
            return {
                "ok": False, "message": f"写入失败: {e}",
                "event_id": event_id, "partition": "", "file_path": "",
                "record_hash": record["record_hash"],
            }

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0,
                    batch_file_id: str | None = None) -> dict[str, Any]:
        """批量写入多条记录到数据湖。

        按每小时一个文件聚合写入。``batch_file_id`` 用于稳定文件名，
        断点续做重提同一批数据时传入相同标识，便于与回执对账。
        返回体在原有字段基础上追加 ``items``，逐条给出 event_id、
        record_hash、partition 与 file_path，作为落湖回执的登记依据。
        """
        if not records:
            return {"ok": True, "message": "无数据需要写入", "written": 0, "items": []}

        # 故障注入钩子（仅测试用）：DATA_LAKE_FORCE_FAIL_TYPES 中列出的
        # 事件类型批量写入直接失败，用来模拟磁盘抖动等中断场景。
        forced = os.environ.get("DATA_LAKE_FORCE_FAIL_TYPES", "")
        if event_type in {t.strip() for t in forced.split(",") if t.strip()}:
            return {
                "ok": False, "message": "模拟落湖写入失败（故障注入）",
                "written": 0, "items": [],
            }

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        file_stem = batch_file_id or now.strftime('%H%M%S')
        batch_file = f"batch_{file_stem}.json"
        file_path = os.path.join(partition_path, batch_file)
        partition = f"events/{event_type}/{date_partition}"

        batch_records = []
        items = []
        batch_prefix = batch_file_id or now.strftime('%Y%m%d%H%M%S')
        for i, record in enumerate(records):
            event_id = f"batch_{batch_prefix}_{i:04d}"
            record_hash = hashlib.sha256(
                json.dumps(record, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()[:16]
            batch_records.append({
                "event_id": event_id,
                "event_type": event_type,
                "source_channel_id": source_channel_id,
                "payload": record,
                "ingested_at": now.isoformat(),
                "record_hash": record_hash,
            })
            items.append({
                "ok": True,
                "event_id": event_id,
                "event_type": event_type,
                "record_hash": record_hash,
                "partition": partition,
                "file_path": file_path,
            })

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(batch_records, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)

            return {
                "ok": True,
                "message": f"批量 {len(batch_records)} 条写入完成",
                "written": len(batch_records),
                "partition": partition,
                "file_path": file_path,
                "items": items,
            }
        except OSError as e:
            return {
                "ok": False, "message": f"批量写入失败: {e}",
                "written": 0, "items": [],
            }

    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_str)

        if not os.path.exists(partition_path):
            return {"ok": True, "records": [], "total": 0, "partition": f"events/{event_type}/{date_str}"}

        records = []
        for fname in sorted(os.listdir(partition_path))[:limit]:
            if fname.endswith(".json"):
                source_file = os.path.join(partition_path, fname)
                try:
                    with open(source_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            for item in data:
                                item.setdefault("_source_file", source_file)
                            records.extend(data)
                        else:
                            data.setdefault("_source_file", source_file)
                            records.append(data)
                except (json.JSONDecodeError, OSError):
                    continue

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": f"events/{event_type}/{date_str}",
        }

    def query_all_lake_records(self, event_type: str) -> list[dict[str, Any]]:
        """读取某事件类型全部分区的落湖记录（含来源文件路径）。

        供恢复流程读取重构前的原始落湖数据、补登历史回执使用。
        """
        type_root = os.path.join(self._lake_base, "events", event_type)
        if not os.path.exists(type_root):
            return []
        records: list[dict[str, Any]] = []
        for root, _dirs, files in os.walk(type_root):
            for fname in sorted(files):
                if not fname.endswith(".json"):
                    continue
                source_file = os.path.join(root, fname)
                try:
                    with open(source_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                if isinstance(data, list):
                    for item in data:
                        item.setdefault("_source_file", source_file)
                    records.extend(data)
                else:
                    data.setdefault("_source_file", source_file)
                    records.append(data)
        return records

    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                # 按事件类型统计
                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "operation": self.OPERATION_TERM,
        }