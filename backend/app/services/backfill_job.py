"""历史回补任务领域服务：历史区间回补、覆盖策略与失败区间重采。

管理历史时间序列数据的批量回补任务，支持日期区间分割、
并行采集、覆盖策略（追加/覆写）和失败区间追溯。
跨模块关联：incre_cursor（起始水位）、batch_ingest（批量拉取）、data_lake_sink（回补落湖）。

写入确认（与批量接入共用落湖回执台账）
--------------------------------------
- 回补按「日」分割，每天实际落湖的记录登记共用落湖回执明细；
- 每日末把本任务该日作用域内的回执归并为日级入湖凭证，凭证按
  (backfill_job, 任务ID, 日期) 幂等签发；
- 已确认的日期重提时只取回原凭证、沿用原写入证据，不会重复记账；
- 日进度（已确认日期、失败区间、下一待处理日期）持久化，服务在
  「刚写入记录但尚未登记日进度」时重启，恢复后凭实际落湖回执补结
  当日覆盖范围；批量接入任务的落湖成功不能顶替本回补任务的凭证。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app import persistence
from app.services.base import BaseModuleService
from app.services.lake_receipt import (
    JOB_KIND_BACKFILL,
    lake_receipt_ledger,
    source_key_for,
)

PROGRESS_STORE_KEY = "backfill_job_progress"
EVENT_TYPE_BACKFILL = "backfill"
EVENT_TYPE_BACKFILL_RETRY = "backfill_retry"


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回补任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "关联端点", "起始日期", "截止日期", "并行度", "覆盖策略", "完成水位", "失败区间"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "起始日期"]
    STATUS_ORDER = ["草稿", "运行中", "已完成", "已取消"]
    ACTION_RULES = {"启动回补": "运行中", "确认完成": "已完成", "取消回补": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        # 内存态保留给旧调用方，权威进度以持久化存储为准。
        self._failed_intervals: dict[int, list[dict[str, Any]]] = {}
        self._progress: dict[int, float] = {}

    # ------------------------------------------------------------------
    # 持久化日进度
    # ------------------------------------------------------------------
    def _load_progress(self, job_id: int) -> dict[str, Any]:
        for row in persistence.json_store.get_all(PROGRESS_STORE_KEY):
            if int(row.get("job_id", -1)) == job_id:
                return row
        return {
            "job_id": job_id,
            "confirmed_days": [],
            "failed_intervals": [],
            "certificates": [],
            "updated_at": None,
        }

    def _save_progress(self, progress: dict[str, Any]) -> None:
        progress["updated_at"] = datetime.now(timezone.utc).isoformat()
        existing = [
            r for r in persistence.json_store.get_all(PROGRESS_STORE_KEY)
            if int(r.get("job_id", -1)) == int(progress["job_id"])
        ]
        if existing:
            persistence.json_store.update(PROGRESS_STORE_KEY, int(existing[0]["id"]), progress)
        else:
            persistence.json_store.insert(PROGRESS_STORE_KEY, dict(progress))
        self._failed_intervals[int(progress["job_id"])] = list(progress.get("failed_intervals", []))
        self._progress[int(progress["job_id"])] = progress.get("progress_pct", 0.0)

    def execute_backfill(self, job_id: int) -> dict[str, Any]:
        """执行历史数据回补任务（按日核算，日确认以落湖回执为准）。

        完整流程：
        1. 解析起始/截止日期区间，按日分割
        2. 跳过已持有日级入湖凭证的日期（幂等续做）
        3. 逐日模拟拉取并写入数据湖，逐条登记落湖回执
        4. 日末归并日级入湖凭证并推进日进度
        5. 记录失败区间，更新任务状态

        Returns:
            { ok, days_processed, total_days, records_backfilled,
              records_reused, failed_intervals, progress_pct, ... }
        """
        job = persistence.json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "days_processed": 0}

        # 已完成任务允许幂等重提：所有日凭证都已存在，重提只会沿用原
        # 写入证据、不会重复记账；草稿/已取消仍拒绝执行。
        if job.get("status") not in ("运行中", "已完成"):
            return {"ok": False, "message": f"任务状态{job['status']}，无法执行", "days_processed": 0}

        start_date = self._parse_date(str(job.get("起始日期", "")))
        end_date = self._parse_date(str(job.get("截止日期", "")))
        endpoint_id = int(job.get("关联端点", "0") or "0")
        job_key = str(job_id)

        if start_date is None or end_date is None:
            return {"ok": False, "message": "日期格式无效", "days_processed": 0}

        # 获取增量水位作为起始参考（保持既有行为）
        try:
            from app.services.incre_cursor import IncreCursorService
            IncreCursorService().get_position(endpoint_id).get("position", 0)
        except Exception:
            pass

        days_list: list[str] = []
        current = start_date
        while current <= end_date:
            days_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        progress = self._load_progress(job_id)
        confirmed_days = {str(d) for d in progress.get("confirmed_days", [])}
        failed_map = {
            str(item.get("date")): item
            for item in progress.get("failed_intervals", [])
        }

        days_processed = 0
        records_backfilled = 0     # 本次实际新写入的条数
        records_reused = 0         # 重提时沿用既有凭证覆盖的条数
        certificate_nos: list[str] = []

        import random
        for day_str in days_list:
            scope = day_str

            # 幂等：该日已确认过，重提只沿用原写入证据，不再记账。
            existing_cert = lake_receipt_ledger.get_certificate(JOB_KIND_BACKFILL, job_key, scope)
            if existing_cert is not None:
                records_reused += int(existing_cert.get("confirmed_writes", 0))
                certificate_nos.append(existing_cert["certificate_no"])
                days_processed += 1
                confirmed_days.add(day_str)
                continue

            try:
                daily_data = self._mock_fetch_day(endpoint_id, day_str, random.randint(10, 50))
                records_backfilled += len(daily_data)

                from app.services.data_lake_sink import DataLakeSinkService
                sink_svc = DataLakeSinkService()
                batch_file_id = f"backfill-{job_id}-{day_str}"
                sink_result = sink_svc.write_batch(
                    daily_data,
                    event_type=EVENT_TYPE_BACKFILL,
                    source_channel_id=endpoint_id,
                    batch_file_id=batch_file_id,
                )
                if not sink_result.get("ok"):
                    failed_map[day_str] = {"date": day_str, "reason": sink_result.get("message", "落湖失败")}
                    continue

                # 逐条登记共用落湖回执（与批量接入同一份台账）。
                for index, item in enumerate(sink_result.get("items", [])):
                    source_key = source_key_for(JOB_KIND_BACKFILL, job_key, scope, index)
                    lake_receipt_ledger.record_write(
                        job_kind=JOB_KIND_BACKFILL,
                        job_key=job_key,
                        scope=scope,
                        source_key=source_key,
                        event_id=item["event_id"],
                        event_type=EVENT_TYPE_BACKFILL,
                        record_hash=item.get("record_hash", ""),
                        partition=item.get("partition", ""),
                        file_path=item.get("file_path", ""),
                        source_channel_id=endpoint_id,
                        payload=daily_data[index] if index < len(daily_data) else None,
                    )

                # 日级确认：只凭本任务当日的实际落湖回执归并凭证。
                certificate = self._settle_day(progress, confirmed_days, day_str, len(daily_data))
                certificate_nos.append(certificate["certificate_no"])
                failed_map.pop(day_str, None)
                days_processed += 1

            except Exception as e:
                failed_map[day_str] = {"date": day_str, "reason": str(e)}

        failed_intervals = [failed_map[d] for d in days_list if d in failed_map]
        progress["failed_intervals"] = failed_intervals
        progress["progress_pct"] = round(len(confirmed_days) / max(len(days_list), 1) * 100, 2)
        self._save_progress(progress)

        is_complete = len(confirmed_days) == len(days_list) and not failed_intervals
        persistence.json_store.update(self.MODULE, job_id, {
            "完成水位": f"{len(confirmed_days)}/{len(days_list)}",
            "失败区间": str(failed_intervals) if failed_intervals else "无",
            "status": "已完成" if is_complete else "运行中",
            "pending": not is_complete,
            "abnormal": len(failed_intervals) > 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "message": (
                f"回补完成: {len(confirmed_days)}/{len(days_list)} 天，"
                f"新写入 {records_backfilled} 条，沿用既有写入 {records_reused} 条"
            ),
            "days_processed": days_processed,
            "total_days": len(days_list),
            "confirmed_days": sorted(confirmed_days),
            "records_backfilled": records_backfilled,
            "records_reused": records_reused,
            "failed_intervals": failed_intervals,
            "certificates": certificate_nos,
            "progress_pct": progress["progress_pct"],
        }

    def _settle_day(
        self,
        progress: dict[str, Any],
        confirmed_days: set[str],
        day_str: str,
        expected_count: int,
    ) -> dict[str, Any]:
        """归并当日回执为日级入湖凭证，并把凭证同步到任务日进度。"""
        job_id = int(progress["job_id"])
        certificate = lake_receipt_ledger.confirm_scope(
            job_kind=JOB_KIND_BACKFILL,
            job_key=str(job_id),
            scope=day_str,
            scope_level="day",
            expected_count=expected_count,
        )
        confirmed_days.add(day_str)
        progress["confirmed_days"] = sorted(confirmed_days)
        cert_refs = list(progress.get("certificates", []))
        if certificate["certificate_no"] not in cert_refs:
            cert_refs.append(certificate["certificate_no"])
        progress["certificates"] = cert_refs
        total_days = len(confirmed_days) + len(progress.get("failed_intervals", []))
        progress["progress_pct"] = round(
            len(confirmed_days) / max(total_days, 1) * 100, 2
        )
        self._save_progress(progress)
        return certificate

    def recover_pending_writes(self, job_id: int, day_str: str, expected_count: int) -> dict[str, Any]:
        """恢复入口：回补已写入记录但尚未登记日进度时重启后的补结。

        有台账回执则直接归并日凭证；无台账记录时从数据湖原有落湖
        文件补登历史回执再确认。只核对本回补任务自己的写入，
        批量接入任务的落湖证据不会被并入。
        """
        job = persistence.json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        endpoint_id = int(job.get("关联端点", "0") or "0")
        job_key = str(job_id)
        progress = self._load_progress(job_id)
        confirmed_days = {str(d) for d in progress.get("confirmed_days", [])}

        if not lake_receipt_ledger.list_receipts(
            job_kind=JOB_KIND_BACKFILL, job_key=job_key, scope=day_str
        ):
            from app.services.data_lake_sink import DataLakeSinkService
            # 历史落湖文件按写入时间分区、不含业务日；用载荷里的
            # date 字段甄别属于目标日的记录，其他日期/作业的写入
            # 不会被补登进本日凭证。
            lake_records = [
                r for r in DataLakeSinkService().query_all_lake_records(EVENT_TYPE_BACKFILL)
                if int(r.get("source_channel_id", 0) or 0) == endpoint_id
                and str((r.get("payload") or {}).get("date", "")) == day_str
            ]
            lake_receipt_ledger.reconcile_legacy_lake(
                job_kind=JOB_KIND_BACKFILL,
                job_key=job_key,
                scope=day_str,
                scope_level="day",
                event_type=EVENT_TYPE_BACKFILL,
                source_channel_id=endpoint_id,
                lake_records=lake_records,
                expected_count=expected_count,
            )

        certificate = self._settle_day(progress, confirmed_days, day_str, expected_count)
        return {"ok": True, "day": day_str, "certificate": certificate}

    def _parse_date(self, date_str: str) -> datetime | None:
        """解析日期字符串（支持多种格式）。"""
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _mock_fetch_day(self, endpoint_id: int, day: str, count: int) -> list[dict[str, Any]]:
        """模拟拉取一天的数据。"""
        import random
        return [
            {
                "id": i + 1,
                "date": day,
                "endpoint_id": endpoint_id,
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "timestamp": f"{day}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            }
            for i in range(count)
        ]

    def retry_failed_intervals(self, job_id: int) -> dict[str, Any]:
        """重试失败区间：重写落湖、登记回执并按日补开凭证。"""
        progress = self._load_progress(job_id)
        failed = list(progress.get("failed_intervals", []))
        if not failed:
            return {"ok": True, "message": "无失败区间需要重试", "retried": 0}

        job = persistence.json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        endpoint_id = int(job.get("关联端点", "0") or "0")
        job_key = str(job_id)
        confirmed_days = {str(d) for d in progress.get("confirmed_days", [])}
        retried = 0
        still_failed: list[dict[str, Any]] = []

        for interval in failed:
            day_str = str(interval["date"])
            scope = day_str
            try:
                daily_data = self._mock_fetch_day(endpoint_id, day_str, 20)
                from app.services.data_lake_sink import DataLakeSinkService
                sink_svc = DataLakeSinkService()
                sink_result = sink_svc.write_batch(
                    daily_data,
                    event_type=EVENT_TYPE_BACKFILL_RETRY,
                    source_channel_id=endpoint_id,
                    batch_file_id=f"backfill-retry-{job_id}-{day_str}",
                )
                if not sink_result.get("ok"):
                    still_failed.append(interval)
                    continue

                for index, item in enumerate(sink_result.get("items", [])):
                    source_key = source_key_for(JOB_KIND_BACKFILL, job_key, scope, index)
                    lake_receipt_ledger.record_write(
                        job_kind=JOB_KIND_BACKFILL,
                        job_key=job_key,
                        scope=scope,
                        source_key=source_key,
                        event_id=item["event_id"],
                        event_type=EVENT_TYPE_BACKFILL_RETRY,
                        record_hash=item.get("record_hash", ""),
                        partition=item.get("partition", ""),
                        file_path=item.get("file_path", ""),
                        source_channel_id=endpoint_id,
                        payload=daily_data[index] if index < len(daily_data) else None,
                    )
                # 同一日期重试用同一个日作用域：已确认则复用，未确认则新签。
                self._settle_day(progress, confirmed_days, day_str, len(daily_data))
                retried += 1
            except Exception:
                still_failed.append(interval)

        progress["failed_intervals"] = still_failed
        self._save_progress(progress)

        return {
            "ok": True,
            "message": f"重试完成: 成功 {retried}/{len(failed)}",
            "retried": retried,
            "still_failed": len(still_failed),
        }

    def get_backfill_progress(self, job_id: int) -> dict[str, Any]:
        """获取回补任务进度（含日凭证及其引用的落湖批次证据）。"""
        job = persistence.json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        progress = self._load_progress(job_id)
        certificates = lake_receipt_ledger.list_certificates(
            job_kind=JOB_KIND_BACKFILL, job_key=str(job_id)
        )

        return {
            "ok": True,
            "job_id": job_id,
            "job_no": job.get("任务编号"),
            "status": job.get("status"),
            "progress_pct": progress.get("progress_pct", 0.0),
            "confirmed_days": [str(d) for d in progress.get("confirmed_days", [])],
            "failed_intervals": len(progress.get("failed_intervals", [])),
            "endpoint": job.get("关联端点"),
            "date_range": f"{job.get('起始日期')} ~ {job.get('截止日期')}",
            # 每张凭证标注该日确认用了哪批落湖数据（分区文件 + 回执编号）。
            "certificates": [
                {
                    "certificate_no": c["certificate_no"],
                    "scope": c["scope"],
                    "scope_level": c["scope_level"],
                    "confirmed_writes": c["confirmed_writes"],
                    "expected_writes": c["expected_writes"],
                    "missing_writes": c["missing_writes"],
                    "receipt_nos": c.get("receipt_nos", []),
                    "lake_files": c.get("lake_files", []),
                    "partitions": c.get("partitions", []),
                    "contains_legacy": c.get("contains_legacy", False),
                    "confirmed_at": c.get("confirmed_at"),
                }
                for c in certificates
            ],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出历史回补任务的数据质量快照。"""
        rows = persistence.json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "历史回补任务数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_jobs": sum(1 for r in rows if r.get("status") == "运行中"),
            "completed_jobs": sum(1 for r in rows if r.get("status") == "已完成"),
            "operation": self.OPERATION_TERM,
        }
