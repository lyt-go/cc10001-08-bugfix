"""限速策略业务规则：状态流转、字段校验与筛选口径都收在这里。

状态机约定：
- 草稿 →（提交策略评估）→ 策略评估中：评估结论随批次落库；
- 策略评估中 →（启用策略，且最近一次评估结论为通过）→ 已启用；
- 评估失败不能跨级变为启用，未评估的策略不进限速桶；
- 已启用 ⇄ 已暂停：暂停期间不得被新任务选中，恢复启用走「启用策略」。

只有「已启用」的策略才在限速桶里，才会被采集任务选中。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "ratelimiter"
REQUIRED_FIELDS = ["策略编号", "目标域名", "每秒请求数"]
STATUS_ORDER = ["草稿", "策略评估中", "已启用", "已暂停"]
ACTION_RULES = {"提交策略评估": "策略评估中", "启用策略": "已启用", "暂停策略": "已暂停"}
NEGATIVE_ACTIONS = []

_EVAL_PASS = "通过"
_EVAL_FAIL = "不通过"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _eval_records(entry: dict[str, Any]) -> list[dict[str, Any]]:
    return entry.setdefault("评估记录", [])


def latest_evaluation(entry: dict[str, Any]) -> dict[str, Any] | None:
    """最近一次已落库的策略评估结论。"""
    records = _eval_records(entry)
    return records[-1] if records else None


def in_bucket(entry: dict[str, Any]) -> bool:
    """是否在限速桶中：只有已启用的策略才会被新任务选中。"""
    return entry.get("status") == "已启用"


def find_by_code(code: str) -> dict[str, Any] | None:
    """按策略编号找限速策略，供采集任务启动时校验。"""
    for row in store.rows(MODULE):
        if str(row.get("策略编号", "")) == code:
            return row
    return None


class RatelimiterService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with store.lock:
            rows = store.rows(MODULE)
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            entry["评估批次"] = 0
            entry["评估记录"] = []
            rows.append(entry)
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with store.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"限速策略 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于限速策略可执行范围"
            if action == "提交策略评估":
                return self._submit_evaluation(entry, values)
            if action == "启用策略":
                return self._enable(entry)
            return self._pause(entry)

    def _submit_evaluation(
        self, entry: dict[str, Any], values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        status = entry.get("status")
        if status not in ("草稿", "策略评估中"):
            return None, f"当前状态「{status}」不需要重新评估"
        result = _EVAL_FAIL if str(values.get("评估结果") or "").strip() in (_EVAL_FAIL, "失败") else _EVAL_PASS
        batch_no = int(entry.get("评估批次") or 0) + 1
        entry["评估批次"] = batch_no
        _eval_records(entry).append({"批次": batch_no, "结果": result, "落库时间": _now()})
        entry["status"] = "策略评估中"
        entry["pending"] = True
        entry["abnormal"] = result == _EVAL_FAIL
        return entry, f"策略评估已提交（第 {batch_no} 批次），评估结论：{result}"

    def _enable(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        status = entry.get("status")
        if status == "草稿":
            return None, "策略尚未评估，不能启用，也不会进入限速桶"
        if status == "已启用":
            return None, "策略已启用，无需重复操作"
        if status == "已暂停":
            entry["status"] = "已启用"
            entry["pending"] = False
            entry["abnormal"] = False
            return entry, "限速策略已恢复启用，重新进入限速桶"
        latest = latest_evaluation(entry)
        if latest is None or latest.get("结果") != _EVAL_PASS:
            return None, "评估未通过，不能跨级启用，策略保持评估中"
        entry["status"] = "已启用"
        entry["pending"] = False
        entry["abnormal"] = False
        return entry, "评估通过，限速策略已启用并进入限速桶"

    def _pause(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") != "已启用":
            return None, f"当前状态「{entry.get('status')}」不能执行暂停"
        entry["status"] = "已暂停"
        entry["pending"] = False
        entry["abnormal"] = False
        return entry, "限速策略已暂停，暂停期间不会被新任务选中"
