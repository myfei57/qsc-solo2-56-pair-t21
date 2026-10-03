"""门控执行。

门控把所有门动作串成一条链：开门前先核对对位是否落盘，关门后写落盘记录，发车
时清空指令队列。批量开门按门区里程排序并受批量上限约束，门区联动按站台门区映射
解析出实际门区。落盘事实只有一份：开门、关门、发车各一个文档。

指令队列支持优先级与时效：紧急指令插到待执行区队首（不打断执行中的指令），
超时未执行的指令两阶段作废并把处置结果写进审计流水；每次变化后门控指令状态
都收成明确的 idle / armed / executing 落盘，崩溃后由恢复流程对照审计补齐。
"""

from __future__ import annotations

from .batch import batch_open
from .close import CloseRecord
from .close import close_and_audit, close_doors, close_durable
from .door import DoorController
from .group import active_group, open_group
from .limit import open_pressure
from .open import open_doors, opened
from .policy import depart_policy
from .queue import (
    command_state,
    depart,
    departed,
    dispositions,
    enqueue,
    execute_next,
    process_queue,
    queued_total,
    recover_commands,
    settle_state,
    void_expired,
    void_expired_all,
)
from .report import door_report
from .state import door_state

__all__ = [
    "CloseRecord",
    "DoorController",
    "active_group",
    "batch_open",
    "close_and_audit",
    "close_doors",
    "close_durable",
    "command_state",
    "depart",
    "depart_policy",
    "departed",
    "dispositions",
    "door_report",
    "door_state",
    "enqueue",
    "execute_next",
    "open_doors",
    "open_group",
    "open_pressure",
    "opened",
    "process_queue",
    "queued_total",
    "recover_commands",
    "settle_state",
    "void_expired",
    "void_expired_all",
]
