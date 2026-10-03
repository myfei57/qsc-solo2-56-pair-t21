"""优先级门控队列的领域模型。

两类取舍在这里固化成枚举与数据类：

* 插队策略默认 :data:`PreemptPolicy.PREEMPT_NORMAL`——紧急指令可以在最小可中断
  单元边界抢占正在执行的普通指令，但绝不抢占另一条紧急指令（关门动作不可被打断）；
  需要"先到先做完"的可预期现场可切换为 :data:`PreemptPolicy.WAIT_RUNNING`。
* 作废不是删除：每条指令终态都带一个 :class:`Disposition` 原因码，永久保留在
  哈希链流水里供巡检核对。

门控的"明确状态"见 :class:`DoorSafeState`：异常终结后门必须经过 FAILSAFE_CLOSE
回到 CLOSED，而不是停在 UNKNOWN。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Priority(Enum):
    """指令优先级。紧急关门永远先于普通指令被选取。"""

    NORMAL = 0
    EMERGENCY = 100

    @property
    def value_int(self) -> int:
        return int(self.value)


class CommandKind(Enum):
    """门控指令类别。

    OPEN 与 BATCH_OPEN 属于"会把门打开"的指令，紧急关门到达时会顶替这类待执行
    指令；CLOSE 与 EMERGENCY_CLOSE 本身就是关门方向，不会被紧急关门顶替。
    """

    OPEN = "open"
    BATCH_OPEN = "batch_open"
    CLOSE = "close"
    EMERGENCY_CLOSE = "emergency_close"
    FAILSAFE_CLOSE = "failsafe_close"

    @property
    def opens_doors(self) -> bool:
        """该指令执行完成后门是否处于开门方向。"""

        return self in (CommandKind.OPEN, CommandKind.BATCH_OPEN)

    @property
    def is_emergency(self) -> bool:
        """该指令是否按紧急优先级处理。"""

        return self is CommandKind.EMERGENCY_CLOSE or self is CommandKind.FAILSAFE_CLOSE

    @classmethod
    def parse(cls, raw: str) -> "CommandKind":
        try:
            return cls(str(raw))
        except ValueError as error:
            raise ValueError(f"未知门控指令类别: {raw!r}") from error


class Status(Enum):
    """指令生命周期状态。"""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    EXPIRED = "expired"
    CANCELED = "canceled"
    ABORTED = "aborted"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self not in (Status.PENDING, Status.RUNNING)


class Disposition(Enum):
    """作废与失败原因码（写进处置结果，历史不可变）。"""

    EXPIRED = "expired"
    """超过截止时间仍未开始执行。"""

    SUPERSEDED_BY_EMERGENCY = "superseded_by_emergency"
    """待执行的开门类指令被后来的紧急关门顶替。"""

    PREEMPTED = "preempted"
    """正在执行的普通指令被紧急关门在可中断边界中止。"""

    CRASHED = "crashed"
    """上一属主崩溃，恢复时判定指令未能可靠完成。"""

    FAILED = "failed"
    """执行器重试耗尽，指令无法完成。"""

    @classmethod
    def parse(cls, raw: str) -> "Disposition":
        return cls(str(raw))


class PreemptPolicy(Enum):
    """紧急指令相对"正在执行的指令"的插队策略。"""

    PREEMPT_NORMAL = "preempt_normal"
    """可抢占普通指令；紧急指令之间严格 FIFO（默认，实时性优先）。"""

    WAIT_RUNNING = "wait_running"
    """任何指令一旦开始执行就不打断，紧急指令只插到待执行队首（可预期性优先）。"""


class DoorSafeState(Enum):
    """门控在异常处置后必须落回的明确状态。"""

    UNKNOWN = "unknown"
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True)
class QueuedCommand:
    """一条门控指令的完整视图。"""

    id: str
    train_id: str
    kind: CommandKind
    station_id: str
    priority: Priority
    seq: int
    submitted_at: float
    deadline: float | None
    ttl_s: float | None
    origin: str = "client"
    status: Status = Status.PENDING
    started_at: float | None = None
    finished_at: float | None = None
    disposition: str = ""
    attempts: int = 0
    units_total: int = 0
    units_done: int = 0
    stations: tuple[str, ...] = ()

    @property
    def is_terminal(self) -> bool:
        return self.status.terminal

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "train_id": self.train_id,
            "kind": self.kind.value,
            "station_id": self.station_id,
            "priority": self.priority.value_int,
            "seq": self.seq,
            "submitted_at": self.submitted_at,
            "deadline": self.deadline,
            "ttl_s": self.ttl_s,
            "origin": self.origin,
            "status": self.status.value,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "disposition": self.disposition,
            "attempts": self.attempts,
            "units_total": self.units_total,
            "units_done": self.units_done,
            "stations": list(self.stations),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "QueuedCommand":
        stations = payload.get("stations", ())
        return cls(
            id=str(payload["id"]),
            train_id=str(payload["train_id"]),
            kind=CommandKind.parse(str(payload["kind"])),
            station_id=str(payload.get("station_id", "")),
            priority=Priority(int(payload.get("priority", 0))),
            seq=int(payload.get("seq", 0)),
            submitted_at=float(payload.get("submitted_at", 0.0)),
            deadline=_opt_float(payload.get("deadline")),
            ttl_s=_opt_float(payload.get("ttl_s")),
            origin=str(payload.get("origin", "client")),
            status=Status(str(payload.get("status", "pending"))),
            started_at=_opt_float(payload.get("started_at")),
            finished_at=_opt_float(payload.get("finished_at")),
            disposition=str(payload.get("disposition", "")),
            attempts=int(payload.get("attempts", 0)),
            units_total=int(payload.get("units_total", 0)),
            units_done=int(payload.get("units_done", 0)),
            stations=tuple(str(item) for item in stations) if isinstance(stations, list) else (),
        )


def _opt_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)  # type: ignore[arg-type]


# ---- 流水事件 -------------------------------------------------------------

# 事件类型：流水是唯一事实源，只允许追加。
EV_SUBMITTED = "submitted"
EV_DUPLICATE = "submit_duplicate"
EV_CLAIMED = "claimed"
EV_PROGRESS = "progress"
EV_DONE = "done"
EV_EXPIRED = "expired"
EV_CANCELED = "canceled"
EV_PREEMPTED = "preempted"
EV_REQUEUED = "requeued"
EV_FAILED = "failed"
EV_RECOVERED = "recovered"


@dataclass(frozen=True)
class LedgerEvent:
    """哈希链上的一条不可变事件。

    ``prev_hash`` 指向上一条事件的 ``hash``，形成只追加链；任何对历史事件的增删改
    都会让 :func:`psdline.door.pq.ledger.verify_chain` 在断链处报错。
    """

    seq: int
    at: float
    type: str
    train_id: str
    command_id: str
    payload: dict = field(default_factory=dict)
    prev_hash: str = ""
    hash: str = ""

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "at": self.at,
            "type": self.type,
            "train_id": self.train_id,
            "command_id": self.command_id,
            "payload": dict(self.payload),
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "LedgerEvent":
        body = payload.get("payload", {})
        return cls(
            seq=int(payload["seq"]),
            at=float(payload["at"]),
            type=str(payload["type"]),
            train_id=str(payload["train_id"]),
            command_id=str(payload["command_id"]),
            payload=dict(body) if isinstance(body, dict) else {},
            prev_hash=str(payload.get("prev_hash", "")),
            hash=str(payload.get("hash", "")),
        )


@dataclass(frozen=True)
class DispositionRecord:
    """一条可查的指令处置结果（作废/失败/中止终态）。"""

    command: QueuedCommand
    reason: str
    at: float
    detail: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "command_id": self.command.id,
            "train_id": self.command.train_id,
            "kind": self.command.kind.value,
            "status": self.command.status.value,
            "reason": self.reason,
            "at": self.at,
            "station_id": self.command.station_id,
            "detail": dict(self.detail),
        }
