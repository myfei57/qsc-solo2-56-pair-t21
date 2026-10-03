"""门控指令队列。

同一列车的门控指令按优先级排队：紧急指令插到待执行区队首（不打断正在执行的
指令），普通指令保持提交顺序；每条指令带 ``epoch``（提交时的代际）与时效
（``ttl_s``），读取时原样返回，供上位机核对有没有被重放、有没有过期。

每条指令在队列文档里只会处于三个区之一：

``pending``   待执行（紧急在前、普通在后，各自保持提交顺序）；
``executing`` 执行中（每列车同时最多一条，开始执行即承诺执行完）；
``voiding``   已认领作废、等待审计落盘后确认（两阶段作废的第一阶段）。

三区都在同一份文档里原子改写：作废先把指令认领进 ``voiding`` 区，审计落盘后
再确认删除。这样崩溃恢复时只需对照审计流水，就能判断一条指令该补写处置记录
还是直接清理，保证指令不丢、不重、处置可查。代际 ``epoch`` 落盘保存，重启后
继续递增，不会回退。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .jsonfile import JsonStore

PRIORITY_NORMAL = "normal"
PRIORITY_URGENT = "urgent"
PRIORITIES = (PRIORITY_NORMAL, PRIORITY_URGENT)


@dataclass(frozen=True)
class DoorCommand:
    """一条待执行的门控指令。"""

    id: str
    train_id: str
    kind: str
    epoch: int
    station_id: str = ""
    priority: str = PRIORITY_NORMAL
    enqueued_at: float = 0.0
    ttl_s: float = 0.0

    def __post_init__(self) -> None:
        if self.priority not in PRIORITIES:
            raise ValueError(f"未知指令优先级: {self.priority}")
        if self.ttl_s < 0:
            raise ValueError("指令时效不能为负")

    @property
    def expires_at(self) -> float | None:
        """返回作废截止时间；``ttl_s`` 为 0 表示不过期。"""

        if self.ttl_s <= 0:
            return None
        return self.enqueued_at + self.ttl_s

    def is_expired(self, now: float) -> bool:
        """判断指令在 ``now`` 时刻是否已超过时效。"""

        deadline = self.expires_at
        return deadline is not None and now >= deadline

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "train_id": self.train_id,
            "kind": self.kind,
            "epoch": self.epoch,
            "station_id": self.station_id,
            "priority": self.priority,
            "enqueued_at": self.enqueued_at,
            "ttl_s": self.ttl_s,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "DoorCommand":
        return cls(
            id=str(payload.get("id", "")),
            train_id=str(payload.get("train_id", "")),
            kind=str(payload.get("kind", "")),
            epoch=int(payload.get("epoch", 0)),
            station_id=str(payload.get("station_id", "")),
            priority=str(payload.get("priority", PRIORITY_NORMAL)),
            enqueued_at=float(payload.get("enqueued_at", 0.0)),
            ttl_s=float(payload.get("ttl_s", 0.0)),
        )


class CommandLog:
    """按列车保存的门控指令队列（pending / executing / voiding 三区）。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store
        self._lock = threading.RLock()
        self._epoch = self._load_epoch()

    @property
    def epoch(self) -> int:
        """返回当前代际，每次清空队列后递增。"""

        with self._lock:
            return self._epoch

    @staticmethod
    def _epoch_doc_id() -> str:
        return "commands/epoch"

    def _load_epoch(self) -> int:
        raw = self._store.read(self._epoch_doc_id())
        if isinstance(raw, dict):
            return int(raw.get("epoch", 0))
        return 0

    def _doc_id(self, train_id: str) -> str:
        return f"commands/{train_id}"

    def _read_queue(self, train_id: str) -> dict:
        raw = self._store.read(self._doc_id(train_id))
        if isinstance(raw, list):
            # 旧格式：纯待执行列表，没有执行中与作废区。
            return {
                "pending": [DoorCommand.from_dict(item) for item in raw if isinstance(item, dict)],
                "executing": None,
                "voiding": [],
            }
        if isinstance(raw, dict):
            executing = raw.get("executing")
            return {
                "pending": [
                    DoorCommand.from_dict(item)
                    for item in raw.get("pending", [])
                    if isinstance(item, dict)
                ],
                "executing": DoorCommand.from_dict(executing) if isinstance(executing, dict) else None,
                "voiding": [
                    DoorCommand.from_dict(item)
                    for item in raw.get("voiding", [])
                    if isinstance(item, dict)
                ],
            }
        return {"pending": [], "executing": None, "voiding": []}

    def _write_queue(self, train_id: str, queue: dict) -> None:
        if not queue["pending"] and queue["executing"] is None and not queue["voiding"]:
            self._store.remove(self._doc_id(train_id))
            return
        self._store.write(
            self._doc_id(train_id),
            {
                "pending": [command.as_dict() for command in queue["pending"]],
                "executing": queue["executing"].as_dict() if queue["executing"] else None,
                "voiding": [command.as_dict() for command in queue["voiding"]],
            },
        )

    def append(self, command: DoorCommand) -> tuple[DoorCommand, bool]:
        """把一条指令排进队列，返回 ``(指令, 是否新入队)``。

        紧急指令插到待执行区队首（已有紧急指令之后、普通指令之前），普通指令
        追加到队尾。同一标识的指令已在队列（含执行中、作废中）时直接返回原指令，
        不重复入队——并发重试与重复提交因此是幂等的。
        """

        with self._lock:
            queue = self._read_queue(command.train_id)
            for existing in (*queue["pending"], *queue["voiding"]):
                if existing.id == command.id:
                    return existing, False
            executing = queue["executing"]
            if executing is not None and executing.id == command.id:
                return executing, False
            if command.priority == PRIORITY_URGENT:
                index = next(
                    (
                        position
                        for position, queued in enumerate(queue["pending"])
                        if queued.priority != PRIORITY_URGENT
                    ),
                    len(queue["pending"]),
                )
                queue["pending"].insert(index, command)
            else:
                queue["pending"].append(command)
            self._write_queue(command.train_id, queue)
            return command, True

    def pending(self, train_id: str) -> list[DoorCommand]:
        """返回列车当前排队中的指令（执行顺序：紧急在前，各自按提交顺序）。"""

        with self._lock:
            return list(self._read_queue(train_id)["pending"])

    def ids(self, train_id: str) -> list[str]:
        """返回排队指令的标识列表。"""

        return [command.id for command in self.pending(train_id)]

    def executing(self, train_id: str) -> DoorCommand | None:
        """返回列车正在执行的指令；没有时返回 ``None``。"""

        with self._lock:
            return self._read_queue(train_id)["executing"]

    def voiding(self, train_id: str) -> list[DoorCommand]:
        """返回已认领作废、等待确认的指令。"""

        with self._lock:
            return list(self._read_queue(train_id)["voiding"])

    def start_next(self, train_id: str) -> DoorCommand | None:
        """把队首指令移入执行中区并返回；已有指令在执行或队列为空时返回 ``None``。"""

        with self._lock:
            queue = self._read_queue(train_id)
            if queue["executing"] is not None or not queue["pending"]:
                return None
            command = queue["pending"].pop(0)
            queue["executing"] = command
            self._write_queue(train_id, queue)
            return command

    def complete(self, train_id: str, command_id: str) -> DoorCommand | None:
        """结束执行中的指令；标识不匹配或没有执行中指令时返回 ``None``。"""

        with self._lock:
            queue = self._read_queue(train_id)
            executing = queue["executing"]
            if executing is None or executing.id != command_id:
                return None
            queue["executing"] = None
            self._write_queue(train_id, queue)
            return executing

    def claim_expired(self, train_id: str, now: float) -> list[DoorCommand]:
        """把待执行区里已过期的指令原子认领进作废区，返回认领到的指令。"""

        with self._lock:
            queue = self._read_queue(train_id)
            expired = [command for command in queue["pending"] if command.is_expired(now)]
            if not expired:
                return []
            queue["pending"] = [
                command for command in queue["pending"] if not command.is_expired(now)
            ]
            queue["voiding"].extend(expired)
            self._write_queue(train_id, queue)
            return expired

    def confirm_voided(self, train_id: str, command_ids: list[str]) -> int:
        """确认作废：把指令从作废区移除，返回实际移除的条数。"""

        with self._lock:
            queue = self._read_queue(train_id)
            before = len(queue["voiding"])
            if not before:
                return 0
            ids = set(command_ids)
            queue["voiding"] = [command for command in queue["voiding"] if command.id not in ids]
            removed = before - len(queue["voiding"])
            if removed:
                self._write_queue(train_id, queue)
            return removed

    def clear(self, train_id: str) -> int:
        """清空列车队列并推进代际（落盘），返回被清掉的指令条数。"""

        with self._lock:
            queue = self._read_queue(train_id)
            removed = (
                len(queue["pending"])
                + (1 if queue["executing"] is not None else 0)
                + len(queue["voiding"])
            )
            self._store.remove(self._doc_id(train_id))
            self._epoch += 1
            self._store.write(self._epoch_doc_id(), {"epoch": self._epoch})
            return removed

    def queued_train_ids(self) -> list[str]:
        """返回当前有指令队列（任一区非空）的列车标识。"""

        with self._lock:
            epoch_doc = self._epoch_doc_id()
            return [
                doc_id[len("commands/") :]
                for doc_id in self._store.list_ids()
                if doc_id.startswith("commands/") and doc_id != epoch_doc
            ]

    def total_queued(self) -> int:
        """统计全部列车待执行与执行中的指令条数（作废中的不计）。"""

        with self._lock:
            total = 0
            for train_id in self.queued_train_ids():
                queue = self._read_queue(train_id)
                total += len(queue["pending"]) + (1 if queue["executing"] is not None else 0)
            return total
