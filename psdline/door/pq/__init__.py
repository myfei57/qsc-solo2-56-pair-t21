"""优先级门控队列。

在旧的"先来先服务"待办列表（:mod:`psdline.door.queue`，保留给既有到发链路使用）
之外，这个子包实现一条带优先级与时效的门控执行线：

* 紧急关门插队首，并在可中断边界抢占正在执行的普通指令；紧急指令之间 FIFO；
* 普通指令按提交顺序执行；超过截止时间还没开始的指令自动作废并记录原因；
* 作废/失败/崩溃后自动补系统级故障安全关门，把门关回明确的 CLOSED；
* 队列状态由只追加的 SHA-256 哈希链流水重建（历史不可变、可校验），快照仅是
  可删除重建的缓存；每列车进程内 RLock + 跨进程 flock 串行化，入队幂等，
  状态迁移先落流水再更新快照，崩溃可恢复。

取舍记录见仓库根目录 ``PRIORITY_QUEUE.md``。
"""

from __future__ import annotations

from .actuator import ActuatorError, DoorActuator, ExecutionPlan, GateActuator, SimulatedCrash
from .ledger import GENESIS, HashLedger, known_trains, verify_all
from .locks import LockRegistry, TrainLock
from .model import (
    CommandKind,
    Disposition,
    DispositionRecord,
    DoorSafeState,
    LedgerEvent,
    PreemptPolicy,
    Priority,
    QueuedCommand,
    Status,
)
from .runner import PriorityQueueRunner, USE_DEFAULT_TTL
from .store import PriorityCommandStore, QueueState
from .worker import PQWorker

__all__ = [
    "GENESIS",
    "ActuatorError",
    "CommandKind",
    "Disposition",
    "DispositionRecord",
    "DoorActuator",
    "DoorSafeState",
    "ExecutionPlan",
    "GateActuator",
    "HashLedger",
    "LedgerEvent",
    "LockRegistry",
    "PQWorker",
    "PreemptPolicy",
    "Priority",
    "PriorityCommandStore",
    "PriorityQueueRunner",
    "QueuedCommand",
    "QueueState",
    "SimulatedCrash",
    "Status",
    "TrainLock",
    "USE_DEFAULT_TTL",
    "known_trains",
    "verify_all",
]
