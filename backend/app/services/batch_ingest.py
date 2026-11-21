"""批量接入调度领域服务：定时批量拉取、分页控制、断点续传与任务编排。

支持基于Cron表达式的定时拉取、分页遍历、并行批次调度和
断点续传能力。拉取的数据经过转换后写入数据湖。
跨模块关联：api_endpoint（拉取源端点）、data_transform（数据转换）、data_lake_sink（落湖）、incre_cursor（增量标记）。

写入确认（重构后统一收口到共用落湖回执台账）
--------------------------------------------
- 每条实际落湖成功的记录登记一条落湖回执明细（lake_receipt）；
- 每页处理完，把本任务本页作用域内的回执归并为一张页级入湖凭证，
  凭证按 (batch_ingest, 任务ID, 页号) 幂等签发；
- 重复提交同一任务同一页，只取回原凭证、沿用原写入证据，不会把
  已确认的写入再次记账，也不会借用其他作业的落湖成功；
- 断点（下一页页码）与页凭证一起持久化，服务重启后从原断点续做，
  「已写入但未登记页进度」的窗口在恢复时凭实际落湖回执补结。
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any

from app import persistence
from app.services.base import BaseModuleService
from app.services.lake_receipt import (
    JOB_KIND_BATCH,
    lake_receipt_ledger,
    source_key_for,
)

PROGRESS_STORE_KEY = "batch_ingest_progress"


class BatchIngestService(BaseModuleService):
    MODULE = "batch_ingest"
    ENTITY = "接入任务"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["任务编号", "关联端点", "Cron表达式", "分页策略", "并行批次", "断点续传", "失败动作", "任务版本"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "Cron表达式"]
    STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "启动任务": "运行中", "暂停任务": "已暂停"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        # 内存态仅作快速缓存与执行历史，权威断点以持久化进度为准。
        self._checkpoints: dict[int, int] = {}
        self._execution_history: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 持久化页进度（断点 + 已确认页）
    # ------------------------------------------------------------------
    def _load_progress(self, task_id: int) -> dict[str, Any]:
        for row in persistence.json_store.get_all(PROGRESS_STORE_KEY):
            if int(row.get("task_id", -1)) == task_id:
                return row
        return {
            "task_id": task_id,
            "next_page": 1,
            "confirmed_pages": [],
            "certificates": [],
            "updated_at": None,
        }

    def _save_progress(self, progress: dict[str, Any]) -> None:
        progress["updated_at"] = datetime.now(timezone.utc).isoformat()
        existing = [
            r for r in persistence.json_store.get_all(PROGRESS_STORE_KEY)
            if int(r.get("task_id", -1)) == int(progress["task_id"])
        ]
        if existing:
            persistence.json_store.update(PROGRESS_STORE_KEY, int(existing[0]["id"]), progress)
        else:
            persistence.json_store.insert(PROGRESS_STORE_KEY, dict(progress))
        # 同步内存缓存，保持旧调用方读到的 checkpoint 一致。
        self._checkpoints[int(progress["task_id"])] = int(progress["next_page"])

    def execute_batch(self, task_id: int, max_pages: int = 5, page_size: int = 20) -> dict[str, Any]:
        """执行批量拉取任务（按页核算，页确认以落湖回执为准）。

        完整流程：
        1. 验证任务状态，读取持久化断点
        2. 逐页拉取模拟数据并转换
        3. 每条转换记录写入数据湖并登记落湖回执
        4. 页末把本页回执归并为页级入湖凭证（幂等），推进断点
        5. 更新增量水位与任务统计

        Returns:
            { ok, pages_processed, records_fetched, records_sunk,
              records_reused, confirmed_pages, batch_id, ... }
        """
        task = persistence.json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"任务 {task_id} 不存在", "pages_processed": 0}

        if task.get("status") != "运行中":
            return {"ok": False, "message": f"任务状态{task['status']}，无法执行", "pages_processed": 0}

        endpoint_id = int(task.get("关联端点", "0") or "0")
        enable_checkpoint = str(task.get("断点续传", "否")) in ("是", "true", "True", "Y")
        on_failure = str(task.get("失败动作", "记录并继续"))
        job_key = str(task_id)

        progress = self._load_progress(task_id)
        # 开启断点续传时从持久化断点页开始；关闭时每次从第1页遍历
        # （保持既有行为），已确认页由下面的凭证幂等检查自动跳过。
        start_page = int(progress["next_page"]) if enable_checkpoint else 1
        confirmed_pages = {int(p) for p in progress.get("confirmed_pages", [])}

        total_fetched = 0
        total_sunk = 0          # 本次实际新写入数据湖的条数
        total_reused = 0        # 本次沿用既有写入证据、未重复写入的条数
        pages_processed = 0
        page_certificates: list[dict[str, Any]] = []
        errors: list[str] = []
        batch_id = f"BATCH-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

        # 断点续做：断点之前已确认的页本次不再遍历，但其页凭证仍然
        # 有效，先把这些原写入证据计入 reused，并把凭证编号带回结果，
        # 让进度输出能指出本次结算沿用了哪批落湖数据。
        if enable_checkpoint:
            for done_page in sorted(p for p in confirmed_pages if p < start_page):
                prior_cert = lake_receipt_ledger.get_certificate(
                    JOB_KIND_BATCH, job_key, str(done_page)
                )
                if prior_cert is not None:
                    total_reused += int(prior_cert.get("confirmed_writes", 0))
                    page_certificates.append(prior_cert["certificate_no"])

        import random
        # 页级凭证幂等检查统一兜底两类场景：
        # - 崩溃窗口：断点页其实已落湖并持有凭证，重提时沿用原证据；
        # - 关闭断点/重复提交：已确认页不再重复落湖、不重复记账。
        for page in range(start_page, start_page + max_pages):
            pages_processed += 1
            scope = str(page)

            # 幂等：该页已有入湖凭证，重提只沿用原写入证据。
            existing_cert = lake_receipt_ledger.get_certificate(JOB_KIND_BATCH, job_key, scope)
            if existing_cert is not None:
                total_reused += int(existing_cert.get("confirmed_writes", 0))
                page_certificates.append(existing_cert["certificate_no"])
                continue

            mock_data = self._mock_fetch_page(endpoint_id, page, page_size)
            if not mock_data:
                break

            total_fetched += len(mock_data)
            transformed = self._transform_batch(endpoint_id, mock_data)

            page_written = 0
            page_failed = 0
            for index, record in enumerate(transformed):
                source_key = source_key_for(JOB_KIND_BATCH, job_key, scope, index)
                event_id = f"{batch_id}:p{page}:{index:04d}:{uuid.uuid4().hex[:8]}"
                try:
                    from app.services.data_lake_sink import DataLakeSinkService
                    sink_svc = DataLakeSinkService()
                    result = sink_svc.write_event(
                        event_id,
                        JOB_KIND_BATCH,
                        record,
                        source_channel_id=endpoint_id,
                    )
                    if result.get("ok"):
                        lake_receipt_ledger.record_write(
                            job_kind=JOB_KIND_BATCH,
                            job_key=job_key,
                            scope=scope,
                            source_key=source_key,
                            event_id=result["event_id"],
                            event_type=JOB_KIND_BATCH,
                            record_hash=result.get("record_hash", ""),
                            partition=result.get("partition", ""),
                            file_path=result.get("file_path", ""),
                            source_channel_id=endpoint_id,
                            payload=record,
                        )
                        page_written += 1
                        total_sunk += 1
                    else:
                        page_failed += 1
                        errors.append(f"第{page}页落湖失败: {result.get('message')}")
                        if on_failure == "中断任务":
                            # 已写入的记录已有回执；先把本页实际写入结算成凭证，
                            # 再中断，恢复时从本页续做而不重复记账。
                            cert = self._settle_page(progress, confirmed_pages, page, len(transformed))
                            page_certificates.append(cert["certificate_no"])
                            return {
                                "ok": False,
                                "message": f"第{page}页落湖失败，任务中断",
                                "batch_id": batch_id,
                                "pages_processed": pages_processed,
                                "records_fetched": total_fetched,
                                "records_sunk": total_sunk,
                                "records_reused": total_reused,
                                "confirmed_pages": sorted(confirmed_pages),
                                "certificates": page_certificates,
                                "errors": errors,
                            }
                except Exception as e:  # pragma: no cover - 防御性分支
                    page_failed += 1
                    errors.append(str(e))

            # 页级确认：只凭本任务本页的实际落湖回执归并凭证。
            cert = self._settle_page(progress, confirmed_pages, page, len(transformed))
            page_certificates.append(cert["certificate_no"])

            # 模拟页面间延迟
            time.sleep(0.01)

        # 更新增量水位（跨模块：incre_cursor）
        self._update_incre_cursor(task_id, endpoint_id, total_fetched)

        # 更新任务统计（total_processed 保持既有口径：按本次抓取量累计）
        persistence.json_store.update(self.MODULE, task_id, {
            "last_batch_id": batch_id,
            "last_executed_at": datetime.now(timezone.utc).isoformat(),
            "total_processed": int(task.get("total_processed", 0)) + total_fetched,
        })

        self._execution_history.append({
            "task_id": task_id,
            "batch_id": batch_id,
            "pages": pages_processed,
            "fetched": total_fetched,
            "sunk": total_sunk,
            "reused": total_reused,
            "errors": len(errors),
            "certificates": page_certificates,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": len(errors) == 0 or total_sunk > 0 or total_reused > 0,
            "message": (
                f"批量接入完成: {pages_processed} 页, 抓取 {total_fetched} 条, "
                f"落湖 {total_sunk} 条, 沿用既有写入 {total_reused} 条"
            ),
            "batch_id": batch_id,
            "pages_processed": pages_processed,
            "records_fetched": total_fetched,
            "records_sunk": total_sunk,
            "records_reused": total_reused,
            "confirmed_pages": sorted(confirmed_pages),
            "certificates": page_certificates,
            "errors": errors,
        }

    def _settle_page(
        self,
        progress: dict[str, Any],
        confirmed_pages: set[int],
        page: int,
        expected_count: int,
    ) -> dict[str, Any]:
        """归并本页回执为入湖凭证，并把凭证同步到任务页进度。"""
        task_id = int(progress["task_id"])
        certificate = lake_receipt_ledger.confirm_scope(
            job_kind=JOB_KIND_BATCH,
            job_key=str(task_id),
            scope=str(page),
            scope_level="page",
            expected_count=expected_count,
        )
        if page not in confirmed_pages:
            confirmed_pages.add(page)
        progress["confirmed_pages"] = sorted(confirmed_pages)
        progress["next_page"] = max(int(progress.get("next_page", 1)), page + 1)
        cert_refs = list(progress.get("certificates", []))
        if certificate["certificate_no"] not in cert_refs:
            cert_refs.append(certificate["certificate_no"])
        progress["certificates"] = cert_refs
        self._save_progress(progress)
        return certificate

    def recover_pending_writes(self, task_id: int, page: int, expected_count: int) -> dict[str, Any]:
        """恢复入口：服务在「已落湖、未登记页进度」窗口重启后调用。

        先检查该页是否已有回执：有则直接归并凭证；完全没有台账记录
        时，从数据湖原有落湖文件补登历史回执，再签发凭证。
        别的作业（如历史回补）写入的证据不会被并入本任务凭证。
        """
        task = persistence.json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"任务 {task_id} 不存在"}

        endpoint_id = int(task.get("关联端点", "0") or "0")
        job_key = str(task_id)
        progress = self._load_progress(task_id)
        confirmed_pages = {int(p) for p in progress.get("confirmed_pages", [])}

        if not lake_receipt_ledger.list_receipts(
            job_kind=JOB_KIND_BATCH, job_key=job_key, scope=str(page)
        ):
            from app.services.data_lake_sink import DataLakeSinkService
            # 历史落湖文件只按端点/事件类型分区，不含页号；这里用页内
            # 模拟数据的稳定 ID 窗口 [(page-1)*size+1, page*size] 甄别
            # 属于目标页的记录，避免把其他页或其他作业的写入补登进来。
            page_size = max(int(expected_count), 1)
            id_lower = (page - 1) * page_size + 1
            id_upper = page * page_size
            lake_records = [
                r for r in DataLakeSinkService().query_all_lake_records(JOB_KIND_BATCH)
                if int(r.get("source_channel_id", 0) or 0) == endpoint_id
                and id_lower <= int((r.get("payload") or {}).get("id", 0) or 0) <= id_upper
            ]
            lake_receipt_ledger.reconcile_legacy_lake(
                job_kind=JOB_KIND_BATCH,
                job_key=job_key,
                scope=str(page),
                scope_level="page",
                event_type=JOB_KIND_BATCH,
                source_channel_id=endpoint_id,
                lake_records=lake_records,
                expected_count=expected_count,
            )

        certificate = self._settle_page(progress, confirmed_pages, page, expected_count)
        return {"ok": True, "page": page, "certificate": certificate}

    def _mock_fetch_page(self, endpoint_id: int, page: int, size: int) -> list[dict[str, Any]]:
        """模拟从API拉取一页数据。"""
        if page > 10:
            return []
        import random
        return [
            {
                "id": (page - 1) * size + i + 1,
                "name": f"record_{endpoint_id}_{page}_{i}",
                "type": "api_data",
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "status": random.choice(["active", "pending", "done"]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            for i in range(min(size, 20))
        ]

    def _transform_batch(self, endpoint_id: int, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量转换数据。"""
        try:
            from app.services.data_transform import DataTransformService
            DataTransformService()
            return [
                {"source_endpoint": endpoint_id, "transformed": True, **r}
                for r in records
            ]
        except Exception:
            return [{"source_endpoint": endpoint_id, **r} for r in records]

    def _update_incre_cursor(self, task_id: int, endpoint_id: int, fetched_count: int) -> None:
        """更新增量水位。"""
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            cursor_svc.update_position(endpoint_id, fetched_count)
        except Exception:
            pass

    def get_task_progress(self, task_id: int) -> dict[str, Any]:
        """获取任务的执行进度（含页凭证及其引用的落湖批次证据）。"""
        task = persistence.json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": "任务不存在"}

        progress = self._load_progress(task_id)
        history = [h for h in self._execution_history if h["task_id"] == task_id]
        certificates = lake_receipt_ledger.list_certificates(
            job_kind=JOB_KIND_BATCH, job_key=str(task_id)
        )

        return {
            "ok": True,
            "task_id": task_id,
            "task_no": task.get("任务编号"),
            "status": task.get("status"),
            "checkpoint_page": int(progress.get("next_page", 1)),
            "next_page": int(progress.get("next_page", 1)),
            "confirmed_pages": [int(p) for p in progress.get("confirmed_pages", [])],
            "total_processed": task.get("total_processed", 0),
            "execution_count": len(history),
            "last_execution": history[-1] if history else None,
            # 每张凭证标注本页确认用了哪批落湖数据（分区文件 + 回执编号）。
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
        """输出批量接入调度的数据质量快照。"""
        rows = persistence.json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "批量接入调度数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_executions": len(self._execution_history),
            "active_tasks": sum(1 for r in rows if r.get("status") == "运行中"),
            "operation": self.OPERATION_TERM,
        }
