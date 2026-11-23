"""回溯完成明细：收拢窗口关闭与接入调度两条链路共用的回溯完成写入。

同一个回溯任务的完成结果有两类来源：

- ``window_close``：已关闭时间窗口链路触发的回溯，完成后需要把结果交给
  特征存储做容量核算；
- ``schedule_load``：调度编排串联批量接入后触发的回溯，只同步回溯清单，
  不向特征存储交付。

历史上两类来源各自在链路里手写一遍「状态/当前进度/数据行数」，且窗口链路把
回溯完成与存储写入耦合在同一步，行数写完到容量接收之间一旦部分失败，重试时
无法区分「回溯已经完成」与「存储还在等待」。本模块把回溯完成明细收拢为一处，
并把「完成确认」与「存储交付」拆成两个独立阶段：

    已确认(待交付) → 已交付
          ↘ 交付失败保留在 待交付/交付失败，重试不重算行数

行数与进度的业务口径仍由各来源在调用时传入，本模块只负责幂等落账，兼容两种
历史产出行为。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.store import store

# 回溯结果来源
SOURCE_WINDOW_CLOSE = "window_close"
SOURCE_SCHEDULE_LOAD = "schedule_load"

# 存储交付状态
DELIVERY_PENDING = "待交付"
DELIVERY_DELIVERED = "已交付"
DELIVERY_FAILED = "交付失败"
DELIVERY_PAUSED = "接收暂停"

VALID_SOURCES = (SOURCE_WINDOW_CLOSE, SOURCE_SCHEDULE_LOAD)


def _now() -> str:
    return datetime.now().isoformat()


def parse_row_count(value: Any) -> int:
    """把「890000行」「1250000」等历史口径的行数字段解析为整数。"""
    text = str(value or "").strip().replace(",", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 0


@dataclass
class BackfillCompletion:
    """一笔回溯任务的完成明细，是回溯结果的唯一权威记录。"""

    entry_id: int
    job_no: str
    source: str
    progress: str
    row_count: int
    row_count_display: str
    source_ref: str
    source_receipt: str
    completed_at: str
    delivery_state: str = DELIVERY_PENDING
    store_entry_id: int | None = None
    delivered_at: str | None = None
    last_error: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "job_no": self.job_no,
            "source": self.source,
            "progress": self.progress,
            "row_count": self.row_count,
            "row_count_display": self.row_count_display,
            "source_ref": self.source_ref,
            "source_receipt": self.source_receipt,
            "completed_at": self.completed_at,
            "delivery_state": self.delivery_state,
            "store_entry_id": self.store_entry_id,
            "delivered_at": self.delivered_at,
            "last_error": self.last_error,
            "attempts": self.attempts,
        }


class BackfillCompletionRegistry:
    """回溯完成明细注册表：完成确认与存储交付的统一入口。"""

    def __init__(self) -> None:
        self._completions: dict[int, BackfillCompletion] = {}
        self._locks: dict[int, threading.Lock] = {}
        self._registry_lock = threading.Lock()
        # 测试/演练用：让下一次容量接收抛出，模拟行数写完到容量接收之间的部分失败
        self.fail_next_delivery = False

    def reset(self) -> None:
        with self._registry_lock:
            self._completions.clear()
            self._locks.clear()
            self.fail_next_delivery = False

    def _lock_for(self, entry_id: int) -> threading.Lock:
        with self._registry_lock:
            lock = self._locks.get(entry_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[entry_id] = lock
            return lock

    # ------------------------------------------------------------------
    # 阶段一：回溯完成确认（两类来源共用，幂等，只确认一次）
    # ------------------------------------------------------------------

    def record_completion(
        self,
        backfill_row: dict[str, Any],
        *,
        source: str,
        progress: str,
        row_count_display: str,
        source_ref: str,
        source_receipt: str = "",
    ) -> tuple[BackfillCompletion, bool]:
        """确认一笔回溯完成，原子写入回溯清单与完成明细。

        并发处理同一任务时只有第一个调用真正写入，后续调用直接返回首笔明细，
        行数与进度不会被第二次改写（半份结果不会覆盖已确认结果）。

        Returns:
            (完成明细, 是否本次新建)
        """
        if source not in VALID_SOURCES:
            raise ValueError(f"未知回溯结果来源：{source}")
        entry_id = int(backfill_row.get("id", 0))
        lock = self._lock_for(entry_id)
        with lock:
            existing = self._completions.get(entry_id)
            if existing is not None:
                return existing, False

            row_count = parse_row_count(row_count_display)
            completed_at = _now()

            # 回溯清单的完成字段一次性原子写入，失败则整笔不落账
            backfill_row["status"] = "已完成"
            backfill_row["pending"] = False
            backfill_row["当前进度"] = progress
            backfill_row["数据行数"] = row_count_display
            backfill_row["回溯来源"] = source
            backfill_row["完成确认时间"] = completed_at
            backfill_row["回溯完成明细"] = {
                "source": source,
                "source_ref": source_ref,
                "source_receipt": source_receipt,
                "row_count": row_count,
                "progress": progress,
                "completed_at": completed_at,
            }

            completion = BackfillCompletion(
                entry_id=entry_id,
                job_no=str(backfill_row.get("任务编号", "")),
                source=source,
                progress=progress,
                row_count=row_count,
                row_count_display=row_count_display,
                source_ref=source_ref,
                source_receipt=source_receipt,
                completed_at=completed_at,
            )
            self._completions[entry_id] = completion
            return completion, True

    # ------------------------------------------------------------------
    # 阶段二：特征存储容量交付（仅窗口来源需要，可暂停/重试/不重算）
    # ------------------------------------------------------------------

    def deliver_to_store(
        self, entry_id: int, *, effects: list[Any] | None = None
    ) -> BackfillCompletion:
        """把已确认的回溯结果交付特征存储核算容量。

        只有 ``已确认`` 且未交付的明细会进入容量接收；已交付的重放直接返回，
        行数沿用首笔明细，不重新计算。
        """
        lock = self._lock_for(entry_id)
        with lock:
            completion = self._completions.get(entry_id)
            if completion is None:
                raise LookupError(f"回溯任务 {entry_id} 尚未确认完成，无完成明细可交付")

            if completion.delivery_state == DELIVERY_PAUSED:
                return completion
            if completion.delivery_state == DELIVERY_DELIVERED:
                return completion
            # 只有窗口关闭来源的回溯结果需要交给特征存储；
            # 接入调度来源只同步回溯清单，任何入口都不应触发容量接收。
            if completion.source != SOURCE_WINDOW_CLOSE:
                completion.last_error = (
                    f"来源 {completion.source} 不向特征存储交付，仅窗口关闭来源可交付"
                )
                raise RuntimeError(completion.last_error)

            completion.attempts += 1
            if self.fail_next_delivery:
                self.fail_next_delivery = False
                completion.delivery_state = DELIVERY_FAILED
                completion.last_error = "容量接收部分失败，存储仍在等待交付"
                raise RuntimeError(completion.last_error)

            backfill_row = store.find("backfill_job", entry_id)
            store_row = self._pick_store_row(completion)
            if store_row is None:
                completion.delivery_state = DELIVERY_FAILED
                completion.last_error = "没有可接收的草稿存储配置"
                raise RuntimeError(completion.last_error)

            # 容量账按首笔明细的行数核算，重试不会重复累加
            store_row["status"] = "已启用"
            store_row["pending"] = True
            store_row["storage_size_gb"] = round(completion.row_count * 0.0001, 2)
            store_row["triggered_by"] = f"backfill_job/{completion.job_no}"
            store_row["接收回溯"] = completion.job_no
            store_row["接收入行数"] = completion.row_count

            completion.delivery_state = DELIVERY_DELIVERED
            completion.store_entry_id = int(store_row.get("id", 0))
            completion.delivered_at = _now()
            completion.last_error = None

            if backfill_row is not None:
                backfill_row["存储交付状态"] = DELIVERY_DELIVERED
                backfill_row["接收存储"] = str(store_row.get("配置编号", ""))

            if effects is not None:
                self._append_monitor_effect(completion, backfill_row, effects)
            return completion

    def _pick_store_row(self, completion: BackfillCompletion) -> dict[str, Any] | None:
        for srow in store.rows("feature_store"):
            if srow.get("status") == "草稿":
                return srow
        return None

    def _append_monitor_effect(
        self,
        completion: BackfillCompletion,
        backfill_row: dict[str, Any] | None,
        effects: list[Any],
    ) -> None:
        """交付后沿用原窗口链路的监控检测副作用。"""
        import random

        from app.services.data_flow import FlowEffect

        target = str(backfill_row.get("目标特征", "")) if backfill_row else ""
        for mrow in store.rows("feature_mon"):
            if mrow.get("status") == "草稿" and target in str(mrow.get("目标特征", "")):
                mrow["status"] = "监控中"
                mrow["pending"] = True
                mrow["最近检测"] = datetime.now().date().isoformat()
                mrow["触发源"] = f"backfill_job/{completion.job_no}"
                drift = random.uniform(0, 0.15)
                if drift > 0.1:
                    mrow["status"] = "分布漂移"
                    mrow["漂移方向"] = "正向偏移"
                effects.append(FlowEffect(
                    "启用监控/检测漂移", "feature_mon", int(mrow.get("id", 0)),
                    f"回溯 {completion.job_no} 完成触发监控 {mrow.get('规则编号','')} "
                    f"检测，漂移={drift:.3f}",
                ))
                break

    # ------------------------------------------------------------------
    # 暂停/恢复接收：暂停发生在完成确认之后、容量接收之前
    # ------------------------------------------------------------------

    def pause_delivery(self, entry_id: int) -> BackfillCompletion:
        lock = self._lock_for(entry_id)
        with lock:
            completion = self._completions.get(entry_id)
            if completion is None:
                raise LookupError(f"回溯任务 {entry_id} 尚未确认完成，无可暂停的接收")
            if completion.delivery_state != DELIVERY_DELIVERED:
                completion.delivery_state = DELIVERY_PAUSED
                backfill_row = store.find("backfill_job", entry_id)
                if backfill_row is not None:
                    backfill_row["存储交付状态"] = DELIVERY_PAUSED
            return completion

    def resume_delivery(self, entry_id: int) -> BackfillCompletion:
        lock = self._lock_for(entry_id)
        with lock:
            completion = self._completions.get(entry_id)
            if completion is None:
                raise LookupError(f"回溯任务 {entry_id} 尚未确认完成，无可恢复的接收")
            if completion.delivery_state == DELIVERY_PAUSED:
                completion.delivery_state = DELIVERY_PENDING
                backfill_row = store.find("backfill_job", entry_id)
                if backfill_row is not None:
                    backfill_row["存储交付状态"] = DELIVERY_PENDING
            return completion

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get(self, entry_id: int) -> BackfillCompletion | None:
        return self._completions.get(entry_id)

    def list_completions(self) -> list[dict[str, Any]]:
        return [c.to_dict() for c in sorted(self._completions.values(), key=lambda c: c.entry_id)]


backfill_completion_registry = BackfillCompletionRegistry()
