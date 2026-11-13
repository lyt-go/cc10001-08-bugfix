"""采集任务业务规则：状态流转、字段校验与筛选口径都收在这里。

跨模块联动约定：
- 启动任务时，出口代理节点与限速策略必须同时有效，任一失效任务都停在待调度；
- 出口选择读取的健康结论与代理详情是同一份判定（最近一次已落库探测结果）；
- 任务启动时把出口信息快照在任务上，任务完成时随采集日志落库；
  已完成的日志保留当时的出口信息，节点后续下线不得覆盖历史日志。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import proxyconfig as proxy_service
from app.services import ratelimiter as rate_service
from app.store import store

MODULE = "taskcrawl"
REQUIRED_FIELDS = ["任务编号", "采集源名称", "规则编号"]
STATUS_ORDER = ["草稿", "待调度", "执行中", "已完成", "已终止"]
ACTION_RULES = {"提交调度": "待调度", "启动任务": "执行中", "完成任务": "已完成", "终止任务": "已终止"}
NEGATIVE_ACTIONS = ["终止任务"]

LOG_MODULE = "storagelog"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class TaskcrawlService:
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
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
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
            for optional in ("代理节点编号", "限速策略编号"):
                if str(values.get(optional) or "").strip():
                    entry[optional] = values.get(optional)
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            rows.append(entry)
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        with store.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"采集任务 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于采集任务可执行范围"
            if action == "提交调度":
                return self._submit(entry)
            if action == "启动任务":
                return self._start(entry)
            if action == "完成任务":
                return self._complete(entry)
            return self._terminate(entry)

    def _submit(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") != "草稿":
            return None, f"当前状态「{entry.get('status')}」不能重复提交调度"
        entry["status"] = "待调度"
        entry["pending"] = True
        entry["abnormal"] = False
        return entry, "采集任务已提交调度"

    def _start(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") != "待调度":
            return None, f"当前状态「{entry.get('status')}」不能启动，任务需先提交调度"
        ok, message, exit_info = self._select_exit(entry)
        if not ok:
            return None, message
        ok, message = self._check_policy(entry)
        if not ok:
            return None, message
        if exit_info is not None:
            entry["出口信息"] = exit_info
        entry["启动时间"] = _now()
        entry["status"] = "执行中"
        entry["pending"] = False
        entry["abnormal"] = False
        return entry, "采集任务已启动，出口信息已快照"

    def _select_exit(self, entry: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        """选择出口：与代理详情读取同一份健康结论，节点失效则任务停在待调度。"""
        code = str(entry.get("代理节点编号") or "").strip()
        if not code:
            return True, "", None
        node = proxy_service.find_by_code(code)
        if node is None:
            return False, f"出口代理节点 {code} 不存在，任务保持待调度", None
        conclusion = proxy_service.health_conclusion(node)
        if not conclusion["在路由池"]:
            return False, (
                f"出口代理节点 {code} 不可调度（节点状态：{node.get('status')}，"
                f"健康结论：{conclusion['健康结论']}），任务保持待调度"
            ), None
        return True, "", {
            "节点编号": code,
            "代理地址": f"{node.get('代理地址')}:{node.get('代理端口')}",
            "健康结论": conclusion["健康结论"],
            "结论依据": conclusion["结论依据"],
            "选定时间": _now(),
        }

    def _check_policy(self, entry: dict[str, Any]) -> tuple[bool, str]:
        """校验限速策略：未启用（草稿/评估中/已暂停）的策略不进限速桶，任务停在待调度。"""
        code = str(entry.get("限速策略编号") or "").strip()
        if not code:
            return True, ""
        policy = rate_service.find_by_code(code)
        if policy is None:
            return False, f"限速策略 {code} 不存在，任务保持待调度"
        if not rate_service.in_bucket(policy):
            return False, (
                f"限速策略 {code} 当前状态为「{policy.get('status')}」，"
                "未在限速桶中，任务保持待调度"
            )
        return True, ""

    def _complete(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") != "执行中":
            return None, f"当前状态「{entry.get('status')}」不能标记完成"
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        self._write_log(entry)
        return entry, "采集任务已完成，出口信息已随日志落库"

    def _write_log(self, entry: dict[str, Any]) -> None:
        """把当时的出口信息以标量快照写进采集日志，后续节点下线不会覆盖它。"""
        rows = store.rows(LOG_MODULE)
        log_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        exit_info = entry.get("出口信息") or {}
        now = _now()
        rows.append({
            "id": log_id,
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "日志编号": f"STOR-{log_id:04d}",
            "关联任务": entry.get("任务编号"),
            "开始时间": entry.get("启动时间") or now,
            "结束时间": now,
            "请求总数": 0,
            "成功条数": 0,
            "失败条数": 0,
            "平均响应毫秒": 0,
            "出口节点编号": exit_info.get("节点编号"),
            "出口代理地址": exit_info.get("代理地址"),
            "出口健康结论": exit_info.get("健康结论"),
        })

    def _terminate(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") in ("已完成", "已终止"):
            return None, f"任务已{entry.get('status')}，不能重复终止"
        entry["status"] = "已终止"
        entry["pending"] = False
        entry["abnormal"] = True
        return entry, "采集任务已终止"
