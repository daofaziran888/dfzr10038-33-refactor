"""回溯完成明细路由：查询完成明细，并把存储交付从链路触发中独立出来。

提供三类入口：

- GET  回溯完成明细列表 / 单笔详情；
- POST 重试交付：行数写完到容量接收之间部分失败后，凭已确认明细继续交付，
  重试不重算行数；
- POST 暂停/恢复接收：在回溯记录完成后暂停特征存储接收，刷新核对完成详情与
  容量账后再恢复交付。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import ActionResult
from app.services.data_flow import FlowEffect
from app.services.backfill_completion import (
    DELIVERY_PAUSED,
    backfill_completion_registry,
)

router = APIRouter(prefix="/api/backfill-completions", tags=["回溯完成明细"])


@router.get("")
def list_completions() -> dict[str, Any]:
    return {"items": backfill_completion_registry.list_completions(),
            "total": len(backfill_completion_registry.list_completions())}


@router.get("/{entry_id}")
def get_completion(entry_id: int) -> dict[str, Any]:
    completion = backfill_completion_registry.get(entry_id)
    if completion is None:
        raise HTTPException(status_code=404,
                            detail=f"回溯任务 {entry_id} 尚无回溯完成明细")
    return completion.to_dict()


@router.post("/{entry_id}/retry-delivery", response_model=ActionResult)
def retry_delivery(entry_id: int) -> ActionResult:
    """容量接收部分失败后的重试入口。

    回溯完成明细已确认（回溯已完成）而存储仍在等待时，凭明细重新交付；
    已交付的重放直接返回首笔结果，行数不重算、容量账不重复累加。
    """
    completion = backfill_completion_registry.get(entry_id)
    if completion is None:
        raise HTTPException(status_code=404,
                            detail=f"回溯任务 {entry_id} 尚无回溯完成明细，无法重试交付")

    if completion.source != "window_close":
        return ActionResult(
            ok=True,
            message=f"回溯任务 {completion.job_no} 来源为 {completion.source}，不需要交付特征存储",
            entry=completion.to_dict(),
        )

    effects: list[FlowEffect] = []
    try:
        backfill_completion_registry.deliver_to_store(entry_id, effects=effects)
    except (RuntimeError, LookupError) as exc:
        return ActionResult(
            ok=False,
            message=f"重试交付仍失败：{exc}；回溯完成明细保持已确认，行数不重算",
            entry=backfill_completion_registry.get(entry_id).to_dict(),
        )

    refreshed = backfill_completion_registry.get(entry_id)
    if refreshed.delivery_state == DELIVERY_PAUSED:
        message = f"回溯任务 {refreshed.job_no} 接收处于暂停状态，未执行容量接收"
    elif refreshed.delivery_state == "已交付":
        message = (f"回溯任务 {refreshed.job_no} 已交付存储，"
                   f"沿用首笔行数 {refreshed.row_count}，未重算")
    else:
        message = f"回溯任务 {refreshed.job_no} 当前交付状态：{refreshed.delivery_state}"
    return ActionResult(ok=True, message=message, entry=refreshed.to_dict())


@router.post("/{entry_id}/pause-delivery", response_model=ActionResult)
def pause_delivery(entry_id: int) -> ActionResult:
    """完成确认后暂停特征存储接收，已交付的任务不受影响。"""
    try:
        completion = backfill_completion_registry.pause_delivery(entry_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return ActionResult(
        ok=True,
        message=f"回溯任务 {completion.job_no} 已暂停存储接收，回溯完成明细保持已确认",
        entry=completion.to_dict(),
    )


@router.post("/{entry_id}/resume-delivery", response_model=ActionResult)
def resume_delivery(entry_id: int) -> ActionResult:
    """恢复接收并立即继续容量交付，沿用首笔明细行数。"""
    try:
        completion = backfill_completion_registry.resume_delivery(entry_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    if completion.source != "window_close":
        # 调度接入来源只同步回溯清单，恢复接收也不触发容量交付
        return ActionResult(
            ok=True,
            message=(f"回溯任务 {completion.job_no} 来源为 {completion.source}，"
                     "不向特征存储交付，已恢复为待交付状态"),
            entry=completion.to_dict(),
        )

    effects: list[FlowEffect] = []
    try:
        backfill_completion_registry.deliver_to_store(entry_id, effects=effects)
    except (RuntimeError, LookupError) as exc:
        return ActionResult(
            ok=False,
            message=f"恢复后交付失败：{exc}；回溯完成明细保持已确认，行数不重算",
            entry=backfill_completion_registry.get(entry_id).to_dict(),
        )

    refreshed = backfill_completion_registry.get(entry_id)
    return ActionResult(
        ok=True,
        message=f"回溯任务 {refreshed.job_no} 已恢复接收并交付存储，行数 {refreshed.row_count} 未重算",
        entry=refreshed.to_dict(),
    )
