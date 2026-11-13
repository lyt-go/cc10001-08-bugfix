"""代理调度业务规则：状态流转、字段校验与筛选口径都收在这里。

状态机约定：
- 草稿/已下线/可用 →（发起探测）→ 探测中：下线后再次投用必须开新的探测批次；
- 探测中 →（标记可用，且最近一次探测结论为成功）→ 可用；
- 探测失败仍停在探测中，健康结论记为不可用，绝不把旧状态重新写成可用；
- 可用/探测中/草稿 →（下线节点）→ 已下线，立即移出路由池。

同一节点同一时刻只允许一个进行中的探测批次：连续点击或并发提交只认一次有效结果。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "proxyconfig"
REQUIRED_FIELDS = ["节点编号", "代理地址", "代理端口"]
STATUS_ORDER = ["草稿", "探测中", "可用", "已下线"]
ACTION_RULES = {"发起探测": "探测中", "标记可用": "可用", "下线节点": "已下线"}
NEGATIVE_ACTIONS = ["下线节点"]

HEALTH_OK = "可用"
HEALTH_BAD = "不可用"
HEALTH_UNKNOWN = "未探测"

_PROBE_SUCCESS = "成功"
_PROBE_FAILURE = "失败"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _probe_records(entry: dict[str, Any]) -> list[dict[str, Any]]:
    return entry.setdefault("探测记录", [])


def _current_batch(entry: dict[str, Any]) -> dict[str, Any] | None:
    records = _probe_records(entry)
    return records[-1] if records else None


def latest_persisted_probe(entry: dict[str, Any]) -> dict[str, Any] | None:
    """最近一次已落库（已有结论）的探测批次；进行中的批次不算。"""
    for record in reversed(_probe_records(entry)):
        if record.get("结果") in (_PROBE_SUCCESS, _PROBE_FAILURE):
            return record
    return None


def health_conclusion(entry: dict[str, Any]) -> dict[str, Any]:
    """节点健康结论：任务调度与代理详情读取的都是这一份判定。

    结论只认最近一次已落库的探测结果；没有任何落库结论时退回节点登记的健康状态。
    与旧任务出口信息冲突时，同样以最近一次已落库探测结果为准。
    在路由池 = 节点状态为「可用」且最近一次落库结论为「可用」。
    """
    latest = latest_persisted_probe(entry)
    if latest is not None:
        health = HEALTH_OK if latest.get("结果") == _PROBE_SUCCESS else HEALTH_BAD
        basis = f"探测批次{latest.get('批次')}"
    else:
        health = str(entry.get("健康状态") or HEALTH_UNKNOWN)
        basis = "登记信息"
    in_pool = entry.get("status") == "可用" and health == HEALTH_OK
    return {"健康结论": health, "在路由池": in_pool, "结论依据": basis}


def find_by_code(code: str) -> dict[str, Any] | None:
    """按节点编号找代理节点，供采集任务启动时解析出口。"""
    for row in store.rows(MODULE):
        if str(row.get("节点编号", "")) == code:
            return row
    return None


def with_conclusion(entry: dict[str, Any]) -> dict[str, Any]:
    """列表/详情共用的出口：在记录上附带实时健康结论，不写回仓库。"""
    data = dict(entry)
    data.update(health_conclusion(entry))
    return data


class ProxyconfigService:
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
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [with_conclusion(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        return with_conclusion(entry) if entry is not None else None

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
            entry["健康状态"] = HEALTH_UNKNOWN
            entry["探测批次"] = 0
            entry["探测记录"] = []
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
                return None, f"代理节点 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于代理调度可执行范围"
            if action == "发起探测":
                return self._start_probe(entry)
            if action == "标记可用":
                return self._mark_available(entry, values)
            return self._take_offline(entry)

    def _start_probe(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        current = _current_batch(entry)
        if entry.get("status") == "探测中" and current is not None and current.get("结果") is None:
            return None, "已有探测批次进行中，重复发起只保留一次有效结果"
        batch_no = int(entry.get("探测批次") or 0) + 1
        entry["探测批次"] = batch_no
        _probe_records(entry).append(
            {"批次": batch_no, "结果": None, "发起时间": _now(), "落库时间": None}
        )
        entry["status"] = "探测中"
        entry["pending"] = True
        entry["abnormal"] = False
        return entry, f"代理节点已发起探测（第 {batch_no} 批次），探测期间不进入路由池"

    def _mark_available(
        self, entry: dict[str, Any], values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        status = entry.get("status")
        if status != "探测中":
            return None, f"当前状态「{status}」不能直接标记可用，必须先发起探测并取得有效结论"
        current = _current_batch(entry)
        if current is None or current.get("结果") is not None:
            return None, "当前探测批次已有落库结论，如需再次探测请重新发起探测"
        result = _PROBE_FAILURE if str(values.get("探测结果") or "").strip() == _PROBE_FAILURE else _PROBE_SUCCESS
        current["结果"] = result
        current["落库时间"] = _now()
        if result == _PROBE_FAILURE:
            # 探测失败：停在探测中，健康结论只能记为不可用，绝不能把旧状态写回可用
            entry["健康状态"] = HEALTH_BAD
            entry["pending"] = True
            entry["abnormal"] = True
            return None, "探测失败，节点保持探测中，不进入路由池"
        entry["健康状态"] = HEALTH_OK
        entry["status"] = "可用"
        entry["pending"] = False
        entry["abnormal"] = False
        return entry, "探测成功，代理节点已标记可用并进入路由池"

    def _take_offline(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") == "已下线":
            return None, "节点已下线，如需再次投用必须重新发起探测"
        entry["status"] = "已下线"
        entry["pending"] = False
        entry["abnormal"] = True
        return entry, "代理节点已下线，已移出路由池"
