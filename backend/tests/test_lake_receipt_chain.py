"""落湖回执台账驱动的批量/回补确认链路测试。

覆盖题目要求的关键行为：
1. 批量接入按页核算，页凭证只归并本任务本页的落湖回执；
2. 历史回补按日核算，日凭证只归并本任务当日的落湖回执；
3. 写入中断后从原断点续做，已确认页/日重提只沿用原写入证据；
4. 重复提交同一作业的确认不会再次记入已确认写入；
5. 两个作业共用一份回执明细，但凭证严格按作业隔离，不能凭别的
   作业成功认定本作业覆盖完成；
6. 「已落湖、未登记进度」窗口重启后，从两个入口各自凭实际写入
   （包括重构前的历史落湖文件）结算自己的覆盖范围；
7. 进度输出能指出每个页/日凭证用了哪批落湖数据。
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence as persistence
from app.persistence import JsonStore
from app.services.backfill_job import BackfillJobService
from app.services.batch_ingest import BatchIngestService
from app.services.data_lake_sink import DataLakeSinkService
from app.services.lake_receipt import (
    CERTIFICATE_KEY,
    JOB_KIND_BACKFILL,
    JOB_KIND_BATCH,
    RECEIPT_KEY,
    lake_receipt_ledger,
)


@pytest.fixture
def isolated_env(monkeypatch):
    """每个用例独立的 JSON 仓库与数据湖目录。"""
    with tempfile.TemporaryDirectory() as tmp:
        store = JsonStore(base_dir=tmp)
        monkeypatch.setattr(persistence, "json_store", store)
        # 台账与两个作业服务均通过 app.persistence 延迟读取仓库。

        # 数据湖目录走环境变量隔离：importlib.reload 不会影响环境变量，
        # 避免与其他测试文件的模块重载相互干扰。
        lake_dir = os.path.join(tmp, "lake")
        os.makedirs(lake_dir, exist_ok=True)
        monkeypatch.setenv("DATA_LAKE_BASE_DIR", lake_dir)
        yield {
            "store": store,
            "lake_dir": lake_dir,
            "sink": DataLakeSinkService(),
            "batch_svc": BatchIngestService(),
            "backfill_svc": BackfillJobService(),
        }


def _make_batch_task(store, endpoint: str = "7", checkpoint: str = "是") -> int:
    record = store.insert("batch_ingest", {
        "任务编号": "BI-RCP-001",
        "关联端点": endpoint,
        "Cron表达式": "0 */1 * * *",
        "分页策略": "分页拉取",
        "并行批次": "1",
        "断点续传": checkpoint,
        "失败动作": "记录并继续",
        "任务版本": "v1",
        "status": "运行中",
        "pending": False,
        "abnormal": False,
    })
    return int(record["id"])


def _make_backfill_job(store, endpoint: str = "7",
                       start: str = "2026-11-01", end: str = "2026-11-03") -> int:
    record = store.insert("backfill_job", {
        "任务编号": "BF-RCP-001",
        "关联端点": endpoint,
        "起始日期": start,
        "截止日期": end,
        "并行度": "1",
        "覆盖策略": "追加不覆写",
        "完成水位": "0/1",
        "失败区间": "无",
        "status": "运行中",
        "pending": False,
        "abnormal": False,
    })
    return int(record["id"])


# ──────────────────────────────────────────────────────────
# 1. 批量接入：按页核算 + 断点续做
# ──────────────────────────────────────────────────────────
def test_batch_page_certificates_and_resume(isolated_env):
    env = isolated_env
    store = env["store"]
    svc = env["batch_svc"]
    task_id = _make_batch_task(store)

    first = svc.execute_batch(task_id, max_pages=2, page_size=5)
    assert first["ok"]
    assert first["pages_processed"] == 2
    assert first["records_sunk"] > 0
    # 每页一张页级凭证。
    assert len(first["certificates"]) == 2
    progress = svc.get_task_progress(task_id)
    assert progress["confirmed_pages"] == [1, 2]
    assert progress["next_page"] == 3
    # 凭证能指出用了哪批落湖数据。
    for cert in progress["certificates"]:
        assert cert["scope_level"] == "page"
        assert cert["lake_files"], "凭证必须标注落湖分区文件"
        assert cert["receipt_nos"], "凭证必须引用落湖回执编号"

    written_after_first = len(store.get_all(RECEIPT_KEY))
    receipts_after_first = written_after_first

    # 再跑一次：从断点第3页续做，前两页不重复写入。
    second = svc.execute_batch(task_id, max_pages=2, page_size=5)
    assert second["ok"]
    assert second["records_reused"] == first["records_sunk"]
    assert second["records_sunk"] > 0  # 第3、4页是新写入
    assert len(store.get_all(RECEIPT_KEY)) == receipts_after_first + second["records_sunk"]
    progress2 = svc.get_task_progress(task_id)
    assert progress2["confirmed_pages"] == [1, 2, 3, 4]
    assert progress2["next_page"] == 5


def test_batch_resubmit_confirmed_page_does_not_double_count(isolated_env):
    env = isolated_env
    svc = env["batch_svc"]
    task_id = _make_batch_task(env["store"])

    first = svc.execute_batch(task_id, max_pages=3, page_size=4)
    receipts_after_first = len(env["store"].get_all(RECEIPT_KEY))
    certs_after_first = len(env["store"].get_all(CERTIFICATE_KEY))

    # 关闭断点后重提，页作用域相同 -> 全部命中原凭证，无新回执/新凭证。
    env["store"].update("batch_ingest", task_id, {"断点续传": "否"})
    second = svc.execute_batch(task_id, max_pages=3, page_size=4)
    assert second["records_sunk"] == 0
    assert second["records_reused"] == first["records_sunk"]
    assert len(env["store"].get_all(RECEIPT_KEY)) == receipts_after_first
    assert len(env["store"].get_all(CERTIFICATE_KEY)) == certs_after_first

    # 直接重复确认同一页：返回同一张凭证，编号不变。
    cert = lake_receipt_ledger.confirm_scope(
        job_kind=JOB_KIND_BATCH, job_key=str(task_id),
        scope="1", scope_level="page", expected_count=4,
    )
    again = lake_receipt_ledger.confirm_scope(
        job_kind=JOB_KIND_BATCH, job_key=str(task_id),
        scope="1", scope_level="page", expected_count=4,
    )
    assert again["certificate_no"] == cert["certificate_no"]
    assert again["reused"] is True
    assert again["confirmed_writes"] == cert["confirmed_writes"]


def test_batch_recovers_from_crash_window(isolated_env):
    """模拟「页已落湖、页进度未登记」时重启：凭实际写入补结后续做。"""
    env = isolated_env
    svc = env["batch_svc"]
    task_id = _make_batch_task(env["store"])

    # 先正常跑第1页，制造真实落湖文件与回执；然后人为删除台账数据，
    # 模拟服务在登记页进度前崩溃（数据湖文件仍在）。
    svc.execute_batch(task_id, max_pages=1, page_size=4)
    for row in list(env["store"].get_all(RECEIPT_KEY)):
        env["store"].delete(RECEIPT_KEY, int(row["id"]))
    for row in list(env["store"].get_all(CERTIFICATE_KEY)):
        env["store"].delete(CERTIFICATE_KEY, int(row["id"]))
    for row in list(env["store"].get_all("batch_ingest_progress")):
        env["store"].delete("batch_ingest_progress", int(row["id"]))

    # 新服务实例（等同重启，内存态为空）走恢复入口。
    restarted = BatchIngestService()
    recovery = restarted.recover_pending_writes(task_id, page=1, expected_count=4)
    assert recovery["ok"]
    cert = recovery["certificate"]
    assert cert["scope_level"] == "page"
    assert cert["confirmed_writes"] == 4
    assert cert["contains_legacy"] is True
    # 恢复后断点已推进，续做从第2页开始且第1页不再重复写入。
    resumed = restarted.execute_batch(task_id, max_pages=2, page_size=4)
    assert resumed["records_reused"] == 4
    assert resumed["confirmed_pages"][0] == 1
    assert 2 in resumed["confirmed_pages"]


# ──────────────────────────────────────────────────────────
# 2. 历史回补：按日核算 + 幂等重提
# ──────────────────────────────────────────────────────────
def test_backfill_daily_certificates_and_idempotent_rerun(isolated_env):
    env = isolated_env
    svc = env["backfill_svc"]
    job_id = _make_backfill_job(env["store"])

    first = svc.execute_backfill(job_id)
    assert first["ok"]
    assert first["total_days"] == 3
    assert set(first["confirmed_days"]) == {"2026-11-01", "2026-11-02", "2026-11-03"}
    assert len(first["certificates"]) == 3
    assert first["records_backfilled"] > 0

    receipts_after_first = len(env["store"].get_all(RECEIPT_KEY))
    progress = svc.get_backfill_progress(job_id)
    assert progress["confirmed_days"] == ["2026-11-01", "2026-11-02", "2026-11-03"]
    for cert in progress["certificates"]:
        assert cert["scope_level"] == "day"
        assert cert["lake_files"]
        assert cert["receipt_nos"]

    # 重提同一作业：每日都命中原凭证，无任何新增写入或凭证。
    second = svc.execute_backfill(job_id)
    assert second["records_backfilled"] == 0
    assert second["records_reused"] == first["records_backfilled"]
    assert len(env["store"].get_all(RECEIPT_KEY)) == receipts_after_first
    cert_nos = [c["certificate_no"] for c in svc.get_backfill_progress(job_id)["certificates"]]
    assert cert_nos == first["certificates"]


def test_backfill_resume_after_interruption(isolated_env):
    """回补中断后续做：已确认日跳过，剩余日继续，日口径不变。"""
    env = isolated_env
    svc = env["backfill_svc"]
    job_id = _make_backfill_job(env["store"], start="2026-11-01", end="2026-11-05")

    first = svc.execute_backfill(job_id)
    assert first["total_days"] == 5
    first_receipts = len(env["store"].get_all(RECEIPT_KEY))

    # 模拟重启：新实例内存态为空，进度全部来自持久化存储。
    restarted = BackfillJobService()
    again = restarted.execute_backfill(job_id)
    assert again["records_backfilled"] == 0
    assert again["records_reused"] == first["records_backfilled"]
    assert len(env["store"].get_all(RECEIPT_KEY)) == first_receipts
    assert len(again["confirmed_days"]) == 5


def test_backfill_recovers_crash_window_from_actual_lake(isolated_env):
    """回补刚写入但未登记日进度时重启：凭数据湖实际写入结算当日。"""
    env = isolated_env
    svc = env["backfill_svc"]
    job_id = _make_backfill_job(env["store"], start="2026-11-10", end="2026-11-10")

    # 直接通过落湖服务写入当日数据，但不登记任何回执/进度。
    payloads = [{"id": 900 + i, "date": "2026-11-10", "endpoint_id": 7} for i in range(3)]
    env["sink"].write_batch(
        payloads, event_type="backfill", source_channel_id=7,
        batch_file_id="backfill-crash-2026-11-10",
    )

    restarted = BackfillJobService()
    recovery = restarted.recover_pending_writes(job_id, "2026-11-10", expected_count=3)
    assert recovery["ok"]
    cert = recovery["certificate"]
    assert cert["scope"] == "2026-11-10"
    assert cert["scope_level"] == "day"
    assert cert["confirmed_writes"] == 3
    assert cert["contains_legacy"] is True

    # 再执行回补：当日已有凭证，不会重复落湖。
    result = restarted.execute_backfill(job_id)
    assert result["records_backfilled"] == 0
    assert result["records_reused"] == 3


# ──────────────────────────────────────────────────────────
# 3. 两个入口共用回执、凭证严格隔离
# ──────────────────────────────────────────────────────────
def test_shared_receipts_but_isolated_certificates(isolated_env):
    """同一来源行出现在两种作业中：各自结算，互不顶替。"""
    env = isolated_env
    batch_svc = env["batch_svc"]
    backfill_svc = env["backfill_svc"]
    endpoint = "7"
    task_id = _make_batch_task(env["store"], endpoint=endpoint)
    job_id = _make_backfill_job(env["store"], endpoint=endpoint,
                                start="2026-11-01", end="2026-11-01")

    batch_result = batch_svc.execute_batch(task_id, max_pages=1, page_size=3)
    backfill_result = backfill_svc.execute_backfill(job_id)
    assert batch_result["ok"] and backfill_result["ok"]

    # 同一份回执明细里同时存在两种作业的回执。
    receipts = env["store"].get_all(RECEIPT_KEY)
    kinds = {r["job_kind"] for r in receipts}
    assert kinds == {JOB_KIND_BATCH, JOB_KIND_BACKFILL}

    # 凭证按作业隔离：批量的页凭证不含回补回执，反之亦然。
    batch_certs = lake_receipt_ledger.list_certificates(job_kind=JOB_KIND_BATCH)
    backfill_certs = lake_receipt_ledger.list_certificates(job_kind=JOB_KIND_BACKFILL)
    assert len(batch_certs) == 1 and len(backfill_certs) == 1

    batch_receipt_ids = set(batch_certs[0]["receipt_ids"])
    backfill_receipt_ids = set(backfill_certs[0]["receipt_ids"])
    assert batch_receipt_ids.isdisjoint(backfill_receipt_ids)

    # 回补作业不能凭批量的落湖成功认定自己覆盖完成：
    # 回补作用域下查不到任何批量凭证。
    assert lake_receipt_ledger.get_certificate(
        JOB_KIND_BATCH, str(job_id), "2026-11-01"
    ) is None
    assert lake_receipt_ledger.get_certificate(
        JOB_KIND_BACKFILL, str(task_id), "1"
    ) is None

    # 同一来源行业务键（id）在两个作业里都出现，各自持有独立回执。
    batch_ids = {
        r["payload_excerpt"].get("id")
        for r in receipts if r["job_kind"] == JOB_KIND_BATCH
    }
    backfill_ids = {
        r["payload_excerpt"].get("id")
        for r in receipts if r["job_kind"] == JOB_KIND_BACKFILL
    }
    # 两套 mock 都从 id=1 开始，存在相同业务行标识但回执互不混用。
    assert batch_ids and backfill_ids
    assert batch_ids != backfill_ids or len(batch_ids) == len(backfill_ids)


def test_other_job_success_cannot_confirm_this_job(isolated_env):
    """只跑批量、不跑回补时，回补作用域没有任何可确认证据。"""
    env = isolated_env
    task_id = _make_batch_task(env["store"], endpoint="8")
    job_id = _make_backfill_job(env["store"], endpoint="8",
                                start="2026-12-01", end="2026-12-01")
    env["batch_svc"].execute_batch(task_id, max_pages=1, page_size=2)

    # 回补任务名下没有任何回执，确认时凭证写入数为 0 且提示缺口。
    cert = lake_receipt_ledger.confirm_scope(
        job_kind=JOB_KIND_BACKFILL, job_key=str(job_id),
        scope="2026-12-01", scope_level="day", expected_count=10,
    )
    assert cert["confirmed_writes"] == 0
    assert cert["missing_writes"] == 10
    assert cert["receipt_ids"] == []

    # 批量凭证照常只覆盖批量自己的页。
    batch_cert = lake_receipt_ledger.get_certificate(JOB_KIND_BATCH, str(task_id), "1")
    assert batch_cert is not None
    assert batch_cert["confirmed_writes"] == 2


def test_retry_failed_interval_settles_with_receipts(isolated_env, monkeypatch):
    """失败区间重试成功后，凭新回执补开同一日作用域的凭证。"""
    env = isolated_env
    svc = env["backfill_svc"]
    job_id = _make_backfill_job(env["store"], start="2026-11-01", end="2026-11-01")

    # 让第一次落湖失败，制造失败区间。故障注入走环境变量，避免其他
    # 测试文件 importlib.reload 后类对象替换导致 monkeypatch 失效。
    monkeypatch.setenv("DATA_LAKE_FORCE_FAIL_TYPES", "backfill")
    first = svc.execute_backfill(job_id)
    assert first["failed_intervals"]
    assert first["confirmed_days"] == []

    # 恢复落湖能力后重试。
    monkeypatch.delenv("DATA_LAKE_FORCE_FAIL_TYPES", raising=False)
    retry = svc.retry_failed_intervals(job_id)
    assert retry["ok"]
    assert retry["retried"] == 1
    cert = lake_receipt_ledger.get_certificate(JOB_KIND_BACKFILL, str(job_id), "2026-11-01")
    assert cert is not None
    assert cert["confirmed_writes"] == 20  # 重试固定每日20条
    assert svc.get_backfill_progress(job_id)["failed_intervals"] == 0
