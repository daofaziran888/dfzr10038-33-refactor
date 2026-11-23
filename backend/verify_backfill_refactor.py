"""回溯完成明细重构的进程内验证脚本（不依赖 pytest/httpx）。

覆盖：
1. 接入调度链路：接入回执 + 回溯清单同步 + 固定 89 万行口径，不交付存储；
2. 已关闭窗口链路：回溯完成明细 + 随机 5万~50万行口径 + 容量核算 + 交付存储；
3. 行数写完到容量接收之间部分失败：重试入口区分「回溯已完成/存储待交付」，
   重试不重算、容量不重复累加；
4. 并发同一任务只确认一次，无半份结果；
5. 暂停接收/恢复交付：暂停期间完成详情与容量账不变，恢复后首笔行数不重算；
6. 兼容历史两种产出与来源，行数与回执沿用各自业务口径。
"""
from __future__ import annotations

import concurrent.futures

from app.seed import SEED_ROWS
from app.services.backfill_completion import (
    DELIVERY_DELIVERED,
    DELIVERY_FAILED,
    DELIVERY_PAUSED,
    DELIVERY_PENDING,
    SOURCE_SCHEDULE_LOAD,
    SOURCE_WINDOW_CLOSE,
    backfill_completion_registry,
)
from app.services.data_flow import flow_engine, flow_schedule_to_batch_to_backfill
from app.services.data_flow import flow_window_to_backfill_to_store_mon
from app.store import Store, store

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    print(f"[{PASS if condition else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def fresh_store() -> None:
    """用种子数据重建内存仓库与完成明细注册表。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    backfill_completion_registry.reset()


def find(module: str, entry_id: int):
    return store.find(module, entry_id)


def effect_names(effects) -> set[str]:
    """兼容 FlowEffect 对象与 flow_engine 序列化后的 dict 两种形态。"""
    return {e.name if hasattr(e, "name") else e["name"] for e in effects}


# ---------------------------------------------------------------- 场景1：调度接入链路
fresh_store()
find("schedule_job", 1)["status"] = "执行中"  # 模拟动作路由先完成状态流转
effects = flow_engine.on_action("schedule_job", "激活计划", 1)["effects"]
bf = find("backfill_job", 1)
comp = backfill_completion_registry.get(1)
check("调度链路-接入任务沿用原回执口径(1250000/350)",
      find("batch_load", 1)["接入行数"] == "1250000"
      and find("batch_load", 1)["拒绝行数"] == "350",
      str(find('batch_load', 1)["接入行数"]))
check("调度链路-回溯清单同步为已完成/100%/890000行",
      bf["status"] == "已完成" and bf["当前进度"] == "100%" and bf["数据行数"] == "890000行",
      f"{bf['status']},{bf['当前进度']},{bf['数据行数']}")
check("调度链路-完成明细来源=schedule_load",
      comp is not None and comp.source == SOURCE_SCHEDULE_LOAD,
      comp.source if comp else "missing")
check("调度链路-编排读取原接入回执并挂接完成字段",
      comp.source_receipt.startswith("接入回执:BATC-0001") and "接入行数=1250000" in comp.source_receipt,
      comp.source_receipt)
check("调度链路-不交付特征存储(存储仍是草稿)",
      find("feature_store", 1)["status"] == "草稿"
      and comp.delivery_state == DELIVERY_PENDING
      and comp.store_entry_id is None,
      comp.delivery_state)
# 即使直接调用交付入口，调度来源也必须被拒绝，状态保持待交付
try:
    backfill_completion_registry.deliver_to_store(1)
    check("调度链路-交付入口拒绝非窗口来源", False)
except RuntimeError:
    check("调度链路-交付入口拒绝非窗口来源(保持待交付)",
          backfill_completion_registry.get(1).delivery_state == DELIVERY_PENDING)
check("调度链路-副作用包含启动接入与启动回溯",
      effect_names(effects) >= {"启动接入", "启动回溯"}, str(sorted(effect_names(effects))))

# 重放调度链路（同一任务再次触发）：只确认一次，不重算
flow_schedule_to_batch_to_backfill(1)
comp2 = backfill_completion_registry.get(1)
check("调度链路-重放不重复确认、行数仍为890000",
      comp2.row_count == 890000 and bf["数据行数"] == "890000行"
      and backfill_completion_registry.list_completions().__len__() == 1,
      f"{comp2.row_count}, attempts={comp2.attempts}")

# ---------------------------------------------------------------- 场景2：已关闭窗口链路
fresh_store()
effects = flow_engine.on_action("time_window", "关闭窗口", 3)["effects"]
bf = find("backfill_job", 1)
comp = backfill_completion_registry.get(1)
check("窗口链路-回溯清单已完成/100%",
      bf["status"] == "已完成" and bf["当前进度"] == "100%", f"{bf['status']},{bf['当前进度']}")
check("窗口链路-行数沿用窗口口径 5万~50万 且带「行」后缀",
      50000 <= comp.row_count <= 500000 and bf["数据行数"] == f"{comp.row_count}行",
      bf["数据行数"])
check("窗口链路-完成明细来源=window_close",
      comp.source == SOURCE_WINDOW_CLOSE and "窗口关闭回执" in comp.source_receipt,
      comp.source)
check("窗口链路-结果交付特征存储并核算容量",
      comp.delivery_state == DELIVERY_DELIVERED and comp.store_entry_id == 1,
      comp.delivery_state)
fs = find("feature_store", 1)
check("窗口链路-容量账=行数*0.0001 且仅核算一次",
      fs["status"] == "已启用"
      and abs(fs["storage_size_gb"] - round(comp.row_count * 0.0001, 2)) < 1e-9
      and fs["接收入行数"] == comp.row_count,
      f"{fs['storage_size_gb']}, {fs['接收入行数']}")
check("窗口链路-副作用含回溯完成与启用存储",
      effect_names(effects) >= {"启动回溯", "回溯完成", "启用存储"}, str(sorted(effect_names(effects))))

# 已交付后再次走链路/重试：行数不重算、容量不重复累加
first_rows = comp.row_count
first_size = fs["storage_size_gb"]
backfill_completion_registry.deliver_to_store(1)
comp = backfill_completion_registry.get(1)
fs = find("feature_store", 1)
check("窗口链路-交付后重放行数不重算、容量账不变",
      comp.row_count == first_rows and fs["storage_size_gb"] == first_size,
      f"{comp.row_count}/{first_rows}, {fs['storage_size_gb']}/{first_size}")

# ---------------------------------------------------------------- 场景3：部分失败 + 重试入口
fresh_store()
backfill_completion_registry.fail_next_delivery = True
effects = flow_window_to_backfill_to_store_mon(3)
comp = backfill_completion_registry.get(1)
bf = find("backfill_job", 1)
fs = find("feature_store", 1)
check("部分失败-回溯已完成且明细已确认",
      bf["status"] == "已完成" and bf["当前进度"] == "100%" and bf["数据行数"].endswith("行"),
      f"{bf['status']},{bf['数据行数']}")
check("部分失败-存储仍在等待(草稿)且交付状态=交付失败",
      fs["status"] == "草稿" and comp.delivery_state == DELIVERY_FAILED
      and comp.last_error is not None,
      f"{fs['status']},{comp.delivery_state}")
check("部分失败-副作用含存储交付失败提示",
      any((e.name if hasattr(e, "name") else e["name"]) == "存储交付失败" for e in effects), str(sorted(effect_names(effects))))

rows_at_failure = comp.row_count
# 重试入口：凭已确认明细继续交付
backfill_completion_registry.deliver_to_store(1)
comp = backfill_completion_registry.get(1)
fs = find("feature_store", 1)
check("重试-交付成功且状态=已交付",
      comp.delivery_state == DELIVERY_DELIVERED and fs["status"] == "已启用",
      comp.delivery_state)
check("重试-首次行数不被重算",
      comp.row_count == rows_at_failure and fs["接收入行数"] == rows_at_failure,
      f"{comp.row_count}/{rows_at_failure}")
check("重试-容量账按同一行数核算一次",
      abs(fs["storage_size_gb"] - round(rows_at_failure * 0.0001, 2)) < 1e-9,
      str(fs["storage_size_gb"]))

# 连续再次重试：幂等，无重复累加
backfill_completion_registry.deliver_to_store(1)
fs = find("feature_store", 1)
comp = backfill_completion_registry.get(1)
check("重试-重复重试不重复累加容量",
      fs["storage_size_gb"] == round(rows_at_failure * 0.0001, 2)
      and fs["接收入行数"] == rows_at_failure and comp.attempts == 2,
      f"attempts={comp.attempts}, size={fs['storage_size_gb']}")

# 无完成明细直接交付应报错，区别于「已完成待交付」
fresh_store()
try:
    backfill_completion_registry.deliver_to_store(999)
    check("无明细-交付被拒绝", False)
except LookupError:
    check("无明细-交付被拒绝(区别于已完成待交付)", True)

# ---------------------------------------------------------------- 场景4：并发只确认一次
fresh_store()
bf = find("backfill_job", 1)
bf["status"] = "回溯中"

def confirm_once(_):
    return backfill_completion_registry.record_completion(
        bf, source=SOURCE_WINDOW_CLOSE, progress="100%",
        row_count_display="123456行", source_ref="time_window/T",
        source_receipt="r")

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    outs = list(pool.map(confirm_once, range(8)))
created_flags = [created for _, created in outs]
check("并发-8个线程只有1次真正确认", created_flags.count(True) == 1 and created_flags.count(False) == 7,
      str(created_flags))
comp = backfill_completion_registry.get(1)
check("并发-明细唯一且无半份结果(行数/进度完整)",
      len(backfill_completion_registry.list_completions()) == 1
      and comp.row_count == 123456 and comp.progress == "100%"
      and bf["数据行数"] == "123456行" and bf["status"] == "已完成",
      f"{comp.row_count},{bf['数据行数']},{bf['status']}")

# 并发确认与进度改写冲突：后到的不同口径不能覆盖首笔
backfill_completion_registry.record_completion(
    bf, source=SOURCE_SCHEDULE_LOAD, progress="100%",
    row_count_display="890000行", source_ref="schedule_job/S")
check("并发-改写进度/行数不能覆盖已确认的首笔结果",
      comp.row_count == 123456 and comp.source == SOURCE_WINDOW_CLOSE
      and bf["数据行数"] == "123456行", bf["数据行数"])

# ---------------------------------------------------------------- 场景5：暂停接收/恢复交付
fresh_store()
# 先只确认完成、不交付，模拟「记录完成后暂停接收」
bf = find("backfill_job", 1)
bf["status"] = "回溯中"
comp, _ = backfill_completion_registry.record_completion(
    bf, source=SOURCE_WINDOW_CLOSE, progress="100%",
    row_count_display="222222行", source_ref="time_window/TIME-0003",
    source_receipt="窗口关闭回执:TIME-0003")
backfill_completion_registry.pause_delivery(1)
comp = backfill_completion_registry.get(1)
fs = find("feature_store", 1)
check("暂停-完成明细保持已确认、交付状态=接收暂停",
      comp.delivery_state == DELIVERY_PAUSED and comp.row_count == 222222
      and bf["存储交付状态"] == DELIVERY_PAUSED, comp.delivery_state)
check("暂停-刷新后回溯详情行数不变(222222)",
      backfill_completion_registry.get(1).row_count == 222222
      and bf["数据行数"] == "222222行", bf["数据行数"])
check("暂停-容量账未发生(存储仍草稿、无接收入行数)",
      fs["status"] == "草稿" and "storage_size_gb" not in fs and "接收入行数" not in fs,
      fs["status"])
# 暂停期间触发交付：直接返回，不接收
backfill_completion_registry.deliver_to_store(1)
fs = find("feature_store", 1)
check("暂停-暂停期间交付被跳过、容量账仍为空",
      fs["status"] == "草稿" and "storage_size_gb" not in fs, fs["status"])
# 恢复并继续交付
backfill_completion_registry.resume_delivery(1)
backfill_completion_registry.deliver_to_store(1)
comp = backfill_completion_registry.get(1)
fs = find("feature_store", 1)
check("恢复-交付成功且首次行数不重算",
      comp.delivery_state == DELIVERY_DELIVERED and comp.row_count == 222222
      and fs["接收入行数"] == 222222
      and abs(fs["storage_size_gb"] - round(222222 * 0.0001, 2)) < 1e-9,
      f"{comp.delivery_state},{fs['storage_size_gb']}")

# ---------------------------------------------------------------- 场景6：两种来源口径并存
fresh_store()
# 窗口来源任务(id=1)
flow_window_to_backfill_to_store_mon(3)
c1 = backfill_completion_registry.get(1)
# 调度来源任务(id=2)：把 id=2 置为待执行，跑调度链路（接入取 id=1）
find("backfill_job", 2)["status"] = "待执行"
find("schedule_job", 1)["status"] = "执行中"
flow_schedule_to_batch_to_backfill(1)
c2 = backfill_completion_registry.get(2)
check("并存-窗口来源行数在5万~50万且交付存储",
      c1.source == SOURCE_WINDOW_CLOSE and 50000 <= c1.row_count <= 500000
      and c1.delivery_state == DELIVERY_DELIVERED, f"{c1.source},{c1.row_count}")
check("并存-调度来源固定89万且不交付存储",
      c2.source == SOURCE_SCHEDULE_LOAD and c2.row_count == 890000
      and c2.delivery_state == DELIVERY_PENDING, f"{c2.source},{c2.row_count}")
check("并存-两条明细各自携带来源回执",
      "窗口关闭回执" in c1.source_receipt and "接入回执" in c2.source_receipt,
      f"{c1.source_receipt} | {c2.source_receipt}")

# ---------------------------------------------------------------- 汇总
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"\n==== {passed}/{total} 项通过 ====")
raise SystemExit(0 if passed == total else 1)
