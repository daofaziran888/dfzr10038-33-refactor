"""HTTP 端到端验证：通过真实 FastAPI 路由验证重构后的两条回溯链路。

用标准库 urllib，不引入新依赖。服务需已启动在 127.0.0.1:8011。
脚本会通过重启 uvicorn 进程在两条链路之间恢复种子数据（内存仓库）。
"""
from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:8011/api"
ROOT = Path(__file__).resolve().parent
results: list[tuple[str, bool, str]] = []


def call(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read().decode())


def check(name, condition, detail=""):
    results.append((name, bool(condition), detail))
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def wait_healthy(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(BASE.replace("/api", "/api/health"), timeout=2)
            return
        except Exception:
            time.sleep(0.4)
    raise RuntimeError("server did not become healthy")


_SERVER_PROC = None


def restart_server():
    global _SERVER_PROC
    if _SERVER_PROC is not None:
        _SERVER_PROC.send_signal(signal.SIGTERM)
        try:
            _SERVER_PROC.wait(timeout=8)
        except subprocess.TimeoutExpired:
            _SERVER_PROC.kill()
    log = open("/tmp/uvicorn_e2e.log", "ab")
    _SERVER_PROC = subprocess.Popen(
        [str(ROOT / ".venv" / "bin" / "uvicorn"), "app.main:app",
         "--host", "127.0.0.1", "--port", "8011"],
        cwd=ROOT, stdout=log, stderr=log,
        start_new_session=True)
    wait_healthy()
    if _SERVER_PROC.poll() is not None:
        raise RuntimeError("uvicorn 启动失败（端口可能被占用），见 /tmp/uvicorn_e2e.log")


def _cleanup():
    if _SERVER_PROC is not None and _SERVER_PROC.poll() is None:
        _SERVER_PROC.send_signal(signal.SIGTERM)
        try:
            _SERVER_PROC.wait(timeout=8)
        except subprocess.TimeoutExpired:
            _SERVER_PROC.kill()


atexit.register(_cleanup)

restart_server()

# ========== 链路A：已关闭窗口（time_window id=3，待执行回溯 id=1）==========
st, r = call("POST", "/time_window/3/actions", {"action": "关闭窗口"})
check("窗口-动作返回ok", r["ok"] and r["flow"]["hits"] >= 2, str(r.get("flow", {}).get("hits")))
st, bf = call("GET", "/backfill_job/1")
check("窗口-回溯清单 已完成/100%",
      bf["status"] == "已完成" and bf["当前进度"] == "100%", f"{bf['status']},{bf['当前进度']}")
window_rows_text = bf["数据行数"]
window_rows = int(window_rows_text.rstrip("行"))
check("窗口-行数带「行」后缀且在5万~50万口径",
      window_rows_text.endswith("行") and 50000 <= window_rows <= 500000, window_rows_text)
st, comp = call("GET", "/backfill-completions/1")
check("窗口-完成明细来源window_close且已交付",
      comp["source"] == "window_close" and comp["delivery_state"] == "已交付"
      and comp["row_count"] == window_rows, f"{comp['source']},{comp['delivery_state']}")
st, fs = call("GET", "/feature_store/1")
check("窗口-仅窗口交付到存储并核算容量",
      fs["status"] == "已启用" and fs["接收入行数"] == window_rows
      and abs(fs["storage_size_gb"] - round(window_rows * 0.0001, 2)) < 1e-9,
      f"{fs['status']},{fs['storage_size_gb']}")
st, r = call("POST", "/backfill-completions/1/retry-delivery")
check("窗口-已交付后重试行数不重算",
      r["ok"] and r["entry"]["row_count"] == window_rows, str(r["entry"]["row_count"]))

# 重启恢复种子态，再跑调度链路
restart_server()

# ========== 链路B：接入调度（schedule_job id=1，接入 id=1，回溯 id=1 待执行）==========
st, r = call("POST", "/schedule_job/1/actions", {"action": "激活计划"})
check("调度-动作返回ok且触发接入+回溯", r["ok"] and r["flow"]["hits"] >= 2,
      str(r.get("flow", {}).get("hits")))
st, bl = call("GET", "/batch_load/1")
check("调度-原接入回执口径不变(1250000/350)",
      bl["接入行数"] == "1250000" and bl["拒绝行数"] == "350",
      f"{bl['接入行数']}/{bl['拒绝行数']}")
st, bf2 = call("GET", "/backfill_job/1")
check("调度-回溯清单同步 已完成/100%/890000行",
      bf2["status"] == "已完成" and bf2["当前进度"] == "100%" and bf2["数据行数"] == "890000行",
      f"{bf2['status']},{bf2['数据行数']}")
st, comp2 = call("GET", "/backfill-completions/1")
check("调度-明细来源schedule_load并挂接接入回执",
      comp2["source"] == "schedule_load" and "接入回执:BATC-0001" in comp2["source_receipt"],
      comp2["source_receipt"])
check("调度-不交付存储(待交付、无store_entry_id)",
      comp2["delivery_state"] == "待交付" and comp2["store_entry_id"] is None,
      comp2["delivery_state"])
st, fs1 = call("GET", "/feature_store/1")
check("调度-存储仍是草稿、容量账未被调度链路改动",
      fs1["status"] == "草稿" and "接收入行数" not in fs1, fs1["status"])
st, r = call("POST", "/backfill-completions/1/retry-delivery")
check("调度-非窗口来源重试不触发存储交付",
      r["ok"] and r["entry"]["delivery_state"] == "待交付", r["message"])

# ========== 暂停接收/刷新/恢复：调度来源完成后处于「待交付」 ==========
st, r = call("POST", "/backfill-completions/1/pause-delivery")
check("暂停-接口返回ok且状态=接收暂停",
      r["ok"] and r["entry"]["delivery_state"] == "接收暂停", r["message"])
st, again = call("GET", "/backfill-completions/1")
check("暂停-刷新后回溯完成详情行数不变(890000)",
      again["row_count"] == 890000 and again["progress"] == "100%",
      f"{again['row_count']},{again['progress']}")
st, bf2b = call("GET", "/backfill_job/1")
check("暂停-回溯清单仍为已完成、行数未重算",
      bf2b["status"] == "已完成" and bf2b["数据行数"] == "890000行",
      f"{bf2b['status']},{bf2b['数据行数']}")
st, fs1b = call("GET", "/feature_store/1")
check("暂停-容量账仍为空(存储草稿)", fs1b["status"] == "草稿", fs1b["status"])
# 恢复接收：调度来源不交付存储，恢复后回到「待交付」，行数仍为首笔
st, r = call("POST", "/backfill-completions/1/resume-delivery")
check("恢复-接口返回ok且回到待交付、行数不重算",
      r["ok"] and r["entry"]["delivery_state"] == "待交付"
      and r["entry"]["row_count"] == 890000,
      f"{r['entry']['delivery_state']},{r['entry']['row_count']}")

# 无明细的 entry 访问返回 404
try:
    call("GET", "/backfill-completions/999")
    check("无明细-返回404", False)
except urllib.error.HTTPError as e:
    check("无明细-返回404", e.code == 404, str(e.code))

# ========== 明细清单总览：当前服务只有调度来源，校验单源口径 ==========
st, allc = call("GET", "/backfill-completions")
sources = {c["source"] for c in allc["items"]}
check("总览-调度来源明细在列且未交付",
      sources == {"schedule_load"}
      and all(c["delivery_state"] == "待交付" for c in allc["items"]),
      str([(c["source"], c["delivery_state"]) for c in allc["items"]]))

total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"\n==== HTTP E2E {passed}/{total} 项通过 ====")
if _SERVER_PROC is not None:
    _SERVER_PROC.send_signal(signal.SIGTERM)
raise SystemExit(0 if passed == total else 1)
