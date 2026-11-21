"""共用落湖回执台账：批量接入（按页）与历史回补（按日）共享的写入证据层。

设计目标
--------
1. 每条实际写入数据湖的记录都登记一条「落湖回执明细」，记录写在哪个
   分区文件、内容指纹、来源行标识（source_key）以及归属作业；
2. 作业在页级（batch_ingest）或日级（backfill_job）确认覆盖范围时，
   把本作业作用域下的回执归并为一张「入湖凭证」，凭证是幂等的：
   重复提交同一作业同一作用域只会取回原凭证，不会重复记账；
3. 两种作业共用同一份回执明细，但凭证严格按作业种类 + 作业实例 +
   页/日作用域隔离，A 作业写入成功不能作为 B 作业覆盖完成的依据；
4. 同一来源行（source_key 相同）允许同时出现在两种作业中，各自持有
   各自的回执与凭证，互不顶替。

所有数据通过 :mod:`app.persistence` 的 json_store 持久化，服务重启后
仍可读取原有回执并重建凭证，支撑「写入中断后从原断点续做」。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app import persistence

# 回执明细与入湖凭证各自独立分区，互不覆盖。
RECEIPT_KEY = "lake_receipt_item"
CERTIFICATE_KEY = "lake_receipt_certificate"

JOB_KIND_BATCH = "batch_ingest"
JOB_KIND_BACKFILL = "backfill_job"


def source_key_for(job_kind: str, job_key: str, scope: str, row_index: int) -> str:
    """生成来源行稳定标识。

    同一作业同一作用域内同一行序号重提时保持不变；批量与回补即便处理
    同一业务行，也因 job_kind/job_key 不同而各自独立。
    """
    raw = f"{job_kind}|{job_key}|{scope}|{row_index}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"src:{digest}"


class LakeReceiptLedger:
    """落湖回执明细与入湖凭证的统一台账（线程安全由 json_store 保证）。"""

    # ------------------------------------------------------------------
    # 回执明细
    # ------------------------------------------------------------------
    def record_write(
        self,
        *,
        job_kind: str,
        job_key: str,
        scope: str,
        source_key: str,
        event_id: str,
        event_type: str,
        record_hash: str,
        partition: str,
        file_path: str,
        source_channel_id: int = 0,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记一条落湖回执明细。

        每次实际写入都产生独立回执；同一 source_key 的重复写入（例如
        不同作业处理同一来源行）保留全部回执，由凭证作用域自行甄别。
        """
        now = datetime.now(timezone.utc).isoformat()
        entry = {
            "receipt_no": self._next_no(RECEIPT_KEY, "receipt_no"),
            "job_kind": job_kind,
            "job_key": str(job_key),
            "scope": str(scope),
            "source_key": source_key,
            "event_id": event_id,
            "event_type": event_type,
            "record_hash": record_hash,
            "partition": partition,
            "file_path": file_path,
            "source_channel_id": source_channel_id,
            "payload_excerpt": self._excerpt(payload),
            "recorded_at": now,
            "legacy": False,
        }
        return persistence.json_store.insert(RECEIPT_KEY, entry)

    def import_legacy_receipt(
        self,
        *,
        job_kind: str,
        job_key: str,
        scope: str,
        source_key: str,
        event_id: str,
        event_type: str,
        record_hash: str,
        partition: str,
        file_path: str,
        source_channel_id: int = 0,
    ) -> dict[str, Any]:
        """把重构前已存在、但没有回执的历史落湖文件补登为回执。

        补登回执带 ``legacy=True`` 标记，保证「能读取原有回执」的同时
        与新流程登记的回执可区分。
        """
        entry = self.record_write(
            job_kind=job_kind,
            job_key=job_key,
            scope=scope,
            source_key=source_key,
            event_id=event_id,
            event_type=event_type,
            record_hash=record_hash,
            partition=partition,
            file_path=file_path,
            source_channel_id=source_channel_id,
        )
        persistence.json_store.update(RECEIPT_KEY, entry["id"], {"legacy": True})
        return persistence.json_store.get_by_id(RECEIPT_KEY, entry["id"])  # type: ignore[return-value]

    def list_receipts(
        self,
        *,
        job_kind: str | None = None,
        job_key: str | None = None,
        scope: str | None = None,
        source_key: str | None = None,
        legacy: bool | None = None,
    ) -> list[dict[str, Any]]:
        """按作业/作用域/来源行过滤回执，全部条件取交集。"""
        rows = persistence.json_store.get_all(RECEIPT_KEY)
        result = []
        for row in rows:
            if job_kind is not None and row.get("job_kind") != job_kind:
                continue
            if job_key is not None and str(row.get("job_key")) != str(job_key):
                continue
            if scope is not None and str(row.get("scope")) != str(scope):
                continue
            if source_key is not None and row.get("source_key") != source_key:
                continue
            if legacy is not None and bool(row.get("legacy")) != legacy:
                continue
            result.append(row)
        return result

    def count_receipts(self, *, job_kind: str, job_key: str, scope: str) -> int:
        """统计某作业某作用域（页或日）内已登记的回执数。"""
        return len(self.list_receipts(job_kind=job_kind, job_key=job_key, scope=scope))

    # ------------------------------------------------------------------
    # 入湖凭证
    # ------------------------------------------------------------------
    def confirm_scope(
        self,
        *,
        job_kind: str,
        job_key: str,
        scope: str,
        scope_level: str,
        expected_count: int,
    ) -> dict[str, Any]:
        """把指定作用域内本作业的落湖回执归并为入湖凭证（幂等）。

        幂等规则：同一 (job_kind, job_key, scope) 已存在凭证时直接返回
        原凭证，不再次归并、不重复记账。凭证只收录本作业作用域内的
        回执，别的作业的写入证据不会被并入。

        Args:
            scope_level: 作用域口径，批量为 ``page``，回补为 ``day``。
            expected_count: 该作用域预期确认的回执条数，用于输出
                缺口信息（不阻断凭证签发）。
        """
        existing = self._find_certificate(job_kind, job_key, scope)
        if existing is not None:
            existing["reused"] = True
            return existing

        receipts = self.list_receipts(job_kind=job_kind, job_key=job_key, scope=scope)
        receipt_ids = sorted(int(r["id"]) for r in receipts)
        receipt_nos = [r["receipt_no"] for r in receipts if r["id"] in set(receipt_ids)]
        files = sorted({r["file_path"] for r in receipts if r.get("file_path")})
        partitions = sorted({r["partition"] for r in receipts if r.get("partition")})
        now = datetime.now(timezone.utc).isoformat()

        certificate = {
            "certificate_no": self._next_no(CERTIFICATE_KEY, "certificate_no"),
            "job_kind": job_kind,
            "job_key": str(job_key),
            "scope": str(scope),
            "scope_level": scope_level,
            "receipt_ids": receipt_ids,
            "receipt_nos": receipt_nos,
            "confirmed_writes": len(receipt_ids),
            "expected_writes": int(expected_count),
            "missing_writes": max(int(expected_count) - len(receipt_ids), 0),
            "lake_files": files,
            "partitions": partitions,
            "contains_legacy": any(bool(r.get("legacy")) for r in receipts),
            "confirmed_at": now,
            "reused": False,
        }
        return persistence.json_store.insert(CERTIFICATE_KEY, certificate)

    def get_certificate(self, job_kind: str, job_key: str, scope: str) -> dict[str, Any] | None:
        """读取某作用域已签发的入湖凭证；未确认过返回 None。"""
        return self._find_certificate(job_kind, job_key, scope)

    def list_certificates(
        self,
        *,
        job_kind: str | None = None,
        job_key: str | None = None,
    ) -> list[dict[str, Any]]:
        """列出凭证，供作业进度输出标注「用了哪批落湖数据」。"""
        rows = persistence.json_store.get_all(CERTIFICATE_KEY)
        result = []
        for row in rows:
            if job_kind is not None and row.get("job_kind") != job_kind:
                continue
            if job_key is not None and str(row.get("job_key")) != str(job_key):
                continue
            result.append(row)
        return sorted(result, key=lambda r: int(r.get("id", 0)))

    def confirmed_scopes(self, *, job_kind: str, job_key: str) -> set[str]:
        """已签发凭证的作用域集合（页号或日期字符串）。"""
        return {
            str(r["scope"])
            for r in self.list_certificates(job_kind=job_kind, job_key=job_key)
        }

    # ------------------------------------------------------------------
    # 原有回执读取（重构前历史落湖文件）
    # ------------------------------------------------------------------
    def reconcile_legacy_lake(
        self,
        *,
        job_kind: str,
        job_key: str,
        scope: str,
        scope_level: str,
        event_type: str,
        source_channel_id: int,
        lake_records: list[dict[str, Any]],
        expected_count: int,
    ) -> dict[str, Any]:
        """恢复入口：把历史落湖文件中的记录补登回执并签发凭证。

        仅当该作业作用域从未登记过任何回执时才补登，避免把新流程的
        回执重复计入；随后走统一的幂等凭证确认。这样重构前已经写入
        数据湖、但没有台账记录的作业，重启后仍能凭实际写入结算。
        """
        already = self.list_receipts(job_kind=job_kind, job_key=job_key, scope=scope)
        imported = 0
        if not already:
            for index, record in enumerate(lake_records):
                payload = record.get("payload") or {}
                self.import_legacy_receipt(
                    job_kind=job_kind,
                    job_key=job_key,
                    scope=scope,
                    source_key=source_key_for(job_kind, job_key, scope, index),
                    event_id=str(record.get("event_id") or f"legacy:{index}"),
                    event_type=str(record.get("event_type") or event_type),
                    record_hash=str(record.get("record_hash") or ""),
                    partition=str(record.get("partition") or ""),
                    file_path=str(record.get("_source_file") or record.get("file_path") or ""),
                    source_channel_id=source_channel_id,
                )
                imported += 1
        certificate = self.confirm_scope(
            job_kind=job_kind,
            job_key=job_key,
            scope=scope,
            scope_level=scope_level,
            expected_count=expected_count,
        )
        certificate["legacy_imported"] = imported
        return certificate

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _find_certificate(self, job_kind: str, job_key: str, scope: str) -> dict[str, Any] | None:
        for row in persistence.json_store.get_all(CERTIFICATE_KEY):
            if (
                row.get("job_kind") == job_kind
                and str(row.get("job_key")) == str(job_key)
                and str(row.get("scope")) == str(scope)
            ):
                return dict(row)
        return None

    def _next_no(self, store_key: str, field: str) -> str:
        rows = persistence.json_store.get_all(store_key)
        prefix = "RCP" if store_key == RECEIPT_KEY else "CRT"
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        return f"{prefix}-{next_id:08d}"

    @staticmethod
    def _excerpt(payload: dict[str, Any] | None) -> dict[str, Any]:
        """回执只保留载荷摘要，避免台账膨胀；完整数据在数据湖文件里。"""
        if not payload:
            return {}
        keys = ("id", "date", "name", "type", "endpoint_id")
        return {k: payload[k] for k in keys if k in payload}


# 模块级单例：批量接入与历史回补共用同一份台账。
lake_receipt_ledger = LakeReceiptLedger()
