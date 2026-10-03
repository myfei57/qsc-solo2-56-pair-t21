"""事件溯源的优先级门控队列存储。

事实源只有 :class:`~psdline.door.pq.ledger.HashLedger`（追加式哈希链流水）；
``pq/state/<train>.json`` 快照只是派生缓存，丢了或旧了都能从流水完整重建。每次
状态迁移都先在流水里追加事件（``fsync`` 落盘），再原子替换快照，保证崩溃安全。

调度语义：

* 待执行指令按「优先级降序、同级按提交序号升序」选取，紧急关门天然排到队首；
* 截止时间已过的待执行指令在选取前统一作废（原因 ``expired``）；
* 紧急关门入队时，待执行的开门类指令直接顶替（``superseded_by_emergency``）；
* 任何异常终结都会登记"不安全站台"，队列清空后自动追加系统级故障安全关门指令，
  把门关回明确的 CLOSED。
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field

from ...errors import RailError
from ...store.jsonfile import JsonStore
from .ledger import GENESIS, HashLedger
from .model import (
    DispositionRecord,
    EV_CANCELED,
    EV_CLAIMED,
    EV_DONE,
    EV_DUPLICATE,
    EV_EXPIRED,
    EV_FAILED,
    EV_PREEMPTED,
    EV_PROGRESS,
    EV_RECOVERED,
    EV_REQUEUED,
    EV_SUBMITTED,
    CommandKind,
    Disposition,
    DoorSafeState,
    LedgerEvent,
    Priority,
    QueuedCommand,
    Status,
)

SNAPSHOT_VERSION = 1


def _mutate(command: QueuedCommand, **changes: object) -> QueuedCommand:
    """在流水重放/迁移中替换 frozen 指令的字段，返回同一逻辑指令的新值。"""

    from dataclasses import replace

    return replace(command, **changes)


@dataclass
class QueueState:
    """一列车队列的内存状态（由流水重建）。"""

    train_id: str
    seq: int = 0
    owner_epoch: int = 0
    door_state: DoorSafeState = DoorSafeState.UNKNOWN
    unsafe_stations: set[str] = field(default_factory=set)
    commands: dict[str, QueuedCommand] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    last_event_hash: str = GENESIS
    events_seen: int = 0
    # 故障安全关门持续失败时置位：队列停在显式的"阻塞待修复"现场，不自动无限重试。
    failsafe_blocked: bool = False

    def pending(self) -> list[QueuedCommand]:
        return [self.commands[cid] for cid in self.order if cid in self.commands]

    def running(self) -> QueuedCommand | None:
        for command in self.commands.values():
            if command.status is Status.RUNNING:
                return command
        return None

    def dirty(self) -> bool:
        """是否存在需要故障安全关门收敛、且尚未阻塞待修复的异常现场。"""

        return bool(self.unsafe_stations) and not self.failsafe_blocked


def _snapshot_doc(train_id: str) -> str:
    quoted = urllib.parse.quote(train_id, safe="")
    return f"pq/state/{quoted}"


class PriorityCommandStore:
    """单列车的优先级队列仓储；所有方法假定调用方已持有该列车的锁。"""

    def __init__(self, store: JsonStore, ledger: HashLedger, train_id: str) -> None:
        self._store = store
        self._ledger = ledger
        self._train_id = train_id
        self._dirty = True
        self.state = self._load()
        self._dirty = False

    # ---- 装载与重建 ---------------------------------------------------

    def _load(self) -> QueueState:
        state = self._load_snapshot()
        last_hash = self._ledger.events()[-1].hash if self._ledger.events() else GENESIS
        head_seq = self._ledger.head_seq()
        snapshot_ok = (
            state is not None
            and state.events_seen <= head_seq
            and state.last_event_hash
            == (
                self._ledger.events()[state.events_seen - 1].hash
                if state.events_seen > 0
                else GENESIS
            )
        )
        if not snapshot_ok:
            return self._rebuild()
        if state.events_seen < head_seq:
            # 快照是上一泵留下的检查点：从游标增量回放到链尾（通常 0~少数几条）。
            for event in self._ledger.events()[state.events_seen:]:
                self._apply(state, event)
        return state

    def _load_snapshot(self) -> QueueState | None:
        raw = self._store.read(_snapshot_doc(self._train_id))
        if not isinstance(raw, dict) or int(raw.get("version", 0)) != SNAPSHOT_VERSION:
            return None
        try:
            state = QueueState(train_id=self._train_id)
            state.seq = int(raw["seq"])
            state.owner_epoch = int(raw.get("owner_epoch", 0))
            state.door_state = DoorSafeState(str(raw.get("door_state", "unknown")))
            state.unsafe_stations = {str(s) for s in raw.get("unsafe_stations", [])}
            state.failsafe_blocked = bool(raw.get("failsafe_blocked", False))
            state.last_event_hash = str(raw.get("last_event_hash", GENESIS))
            state.events_seen = int(raw.get("events_seen", 0))
            state.order = [str(cid) for cid in raw.get("order", [])]
            for item in raw.get("commands", []):
                command = QueuedCommand.from_dict(item)
                state.commands[command.id] = command
        except (KeyError, TypeError, ValueError):
            return None
        return state

    def _rebuild(self) -> QueueState:
        state = QueueState(train_id=self._train_id)
        for event in self._ledger.events():
            self._apply(state, event)
        # 首次装载时直接落一份检查点，之后交给 flush 节流。
        self._write_snapshot(state)
        return state

    def catch_up(self) -> list[LedgerEvent]:
        """把本进程内存状态追赶到磁盘链尾，返回本次新回放的事件。

        多进程共用同一流水时，另一进程可能已在 ``flock`` 保护下追加了事件。打开
        流程先对 :class:`HashLedger` 重同步，再调用本方法把新事件回放到内存状态，
        保证本进程不会拿落后的链尾/序号去追加。
        """

        head = self._ledger.head_seq()
        if head == self.state.events_seen:
            return []
        if head < self.state.events_seen:  # pragma: no cover - 链只会单调增长
            self._rebuild()
            return []
        missing = self._ledger.events()[self.state.events_seen:]
        for event in missing:
            self._apply(self.state, event)
        if missing:
            self._mark_dirty()
        return missing

    def flush(self) -> bool:
        """把积累的状态变化落盘成检查点，返回本次是否真的写了。

        一次泵（pump）内可能连续发生多条语义迁移，但快照是可从哈希链增量重建的
        缓存，因此每泵末尾统一刷一次即可，把"每事件一次快照写"降为"每泵一次"。
        """

        if not self._dirty:
            return False
        self._write_snapshot(self.state)
        self._dirty = False
        return True

    def _mark_dirty(self) -> None:
        """登记一次语义迁移，快照推迟到本泵末尾统一刷。"""

        self._dirty = True

    def _write_snapshot(self, state: QueueState) -> None:
        # 快照只保存活的（pending + running）指令：终态指令由哈希链永久保留并在
        # 重建时回放。这样快照大小与"队列当前长度"而不是"历史总条数"成正比，长
        # 队列高频迁移时不会反复重写整段历史。
        pending = state.pending()
        live_ids = {command.id for command in pending}
        running = state.running()
        if running is not None:
            live_ids.add(running.id)
        commands = [state.commands[cid].as_dict() for cid in state.order if cid in live_ids]
        if running is not None and running.id not in {c["id"] for c in commands}:
            commands.append(running.as_dict())
        payload = {
            "version": SNAPSHOT_VERSION,
            "train_id": state.train_id,
            "seq": state.seq,
            "owner_epoch": state.owner_epoch,
            "door_state": state.door_state.value,
            "unsafe_stations": sorted(state.unsafe_stations),
            "failsafe_blocked": state.failsafe_blocked,
            "order": list(state.order),
            "commands": commands,
            "last_event_hash": state.last_event_hash,
            "events_seen": state.events_seen,
        }
        self._store.write(_snapshot_doc(state.train_id), payload)

    # ---- 流水应用 -----------------------------------------------------

    def _append(self, event_type: str, command_id: str, at: float,
                payload: dict | None = None) -> LedgerEvent:
        event = self._ledger.append(event_type, command_id, at, payload)
        self._apply(self.state, event)
        return event

    def _apply(self, state: QueueState, event: LedgerEvent) -> None:
        state.events_seen += 1
        state.last_event_hash = event.hash
        handler = self._handlers.get(event.type)
        if handler is not None:
            handler(self, state, event)

    def _set(self, state: QueueState, command_id: str, **changes: object) -> QueuedCommand:
        updated = _mutate(state.commands[command_id], **changes)
        state.commands[command_id] = updated
        return updated

    def _apply_submitted(self, state: QueueState, event: LedgerEvent) -> None:
        p = event.payload
        command = QueuedCommand(
            id=event.command_id,
            train_id=event.train_id,
            kind=CommandKind.parse(str(p["kind"])),
            station_id=str(p.get("station_id", "")),
            priority=Priority(int(p.get("priority", 0))),
            seq=int(p["seq"]),
            submitted_at=float(p.get("submitted_at", event.at)),
            deadline=float(p["deadline"]) if p.get("deadline") is not None else None,
            ttl_s=float(p["ttl_s"]) if p.get("ttl_s") is not None else None,
            origin=str(p.get("origin", "client")),
            stations=tuple(str(s) for s in p.get("stations", [])),
        )
        state.seq = max(state.seq, command.seq + 1)
        state.commands[command.id] = command
        state.order.append(command.id)

    def _apply_duplicate(self, state: QueueState, event: LedgerEvent) -> None:
        return None

    def _apply_claimed(self, state: QueueState, event: LedgerEvent) -> None:
        command = state.commands[event.command_id]
        total = int(event.payload.get("units_total", command.units_total))
        owner_epoch = int(event.payload.get("owner_epoch", state.owner_epoch))
        self._set(
            state, command.id,
            status=Status.RUNNING, started_at=event.at,
            attempts=command.attempts + 1, units_total=total,
        )
        state.owner_epoch = owner_epoch
        if command.id in state.order:
            state.order.remove(command.id)

    def _apply_progress(self, state: QueueState, event: LedgerEvent) -> None:
        command = state.commands[event.command_id]
        self._set(state, command.id, units_done=int(event.payload.get("units_done", command.units_done)))

    def _apply_done(self, state: QueueState, event: LedgerEvent) -> None:
        command = state.commands[event.command_id]
        self._set(
            state, command.id,
            status=Status.DONE, finished_at=event.at, units_done=command.units_total,
        )
        if command.id in state.order:
            state.order.remove(command.id)
        stations = set(command.stations) or {command.station_id}
        if command.kind.opens_doors:
            state.door_state = DoorSafeState.OPEN
        else:
            state.unsafe_stations.difference_update(stations)
            # 一次成功的关门解除阻塞现场；全部不安全站台收敛后回到 CLOSED。
            state.failsafe_blocked = False
            state.door_state = DoorSafeState.CLOSED if not state.unsafe_stations else DoorSafeState.UNKNOWN

    def _abnormal_finish(self, state: QueueState, event: LedgerEvent, status: Status) -> None:
        command = state.commands[event.command_id]
        reason = str(event.payload.get("reason", ""))
        self._set(state, command.id, status=status, finished_at=event.at, disposition=reason)
        if command.id in state.order:
            state.order.remove(command.id)
        stations = [s for s in event.payload.get("stations", []) if s]
        if not stations:
            stations = list(command.stations) or ([command.station_id] if command.station_id else [])
        state.unsafe_stations.update(str(s) for s in stations)
        state.door_state = DoorSafeState.UNKNOWN
        if event.type == EV_FAILED and bool(event.payload.get("blocks_failsafe", False)):
            state.failsafe_blocked = True

    def _apply_expired(self, state: QueueState, event: LedgerEvent) -> None:
        self._abnormal_finish(state, event, Status.EXPIRED)

    def _apply_canceled(self, state: QueueState, event: LedgerEvent) -> None:
        self._abnormal_finish(state, event, Status.CANCELED)

    def _apply_preempted(self, state: QueueState, event: LedgerEvent) -> None:
        self._abnormal_finish(state, event, Status.ABORTED)

    def _apply_failed(self, state: QueueState, event: LedgerEvent) -> None:
        self._abnormal_finish(state, event, Status.FAILED)

    def _apply_requeued(self, state: QueueState, event: LedgerEvent) -> None:
        command = state.commands[event.command_id]
        self._set(state, command.id, status=Status.PENDING, started_at=None)
        if command.id not in state.order:
            state.order.insert(0, command.id)

    def _apply_recovered(self, state: QueueState, event: LedgerEvent) -> None:
        command_id = event.command_id
        action = str(event.payload.get("action", ""))
        state.owner_epoch = int(event.payload.get("owner_epoch", state.owner_epoch))
        unsafe = [str(s) for s in event.payload.get("unsafe", [])]
        if unsafe:
            state.unsafe_stations.update(unsafe)
            state.door_state = DoorSafeState.UNKNOWN
        if action == "resumed" or command_id not in state.commands:
            return
        command = state.commands[command_id]
        stations = list(command.stations) or ([command.station_id] if command.station_id else [])
        self._set(state, command_id, status=Status.ABORTED, finished_at=event.at,
                  disposition=Disposition.CRASHED.value)
        state.unsafe_stations.update(stations)
        state.door_state = DoorSafeState.UNKNOWN

    _handlers = {
        EV_SUBMITTED: _apply_submitted,
        EV_DUPLICATE: _apply_duplicate,
        EV_CLAIMED: _apply_claimed,
        EV_PROGRESS: _apply_progress,
        EV_DONE: _apply_done,
        EV_EXPIRED: _apply_expired,
        EV_CANCELED: _apply_canceled,
        EV_PREEMPTED: _apply_preempted,
        EV_FAILED: _apply_failed,
        EV_REQUEUED: _apply_requeued,
        EV_RECOVERED: _apply_recovered,
    }

    # ---- 入队 ---------------------------------------------------------

    def submit(
        self,
        command_id: str,
        kind: CommandKind,
        station_id: str,
        submitted_at: float,
        ttl_s: float | None,
        origin: str,
        stations: tuple[str, ...] = (),
    ) -> tuple[QueuedCommand, bool]:
        """幂等入队，返回 ``(指令, 是否新提交)``。

        同一 ``(列车, 指令ID)`` 重复提交时原样返回既有指令，并追加一条
        ``submit_duplicate`` 留痕；若载荷与首次提交不一致则拒绝（防止用同一幂等键
        掩盖语义不同的指令）。
        """

        if not command_id:
            raise RailError("门控指令标识为空", component="pq")
        existing = self.state.commands.get(command_id)
        if existing is not None:
            if existing.kind is not kind or existing.station_id != station_id:
                raise RailError(
                    f"指令 {existing.id} 已存在但载荷与本次提交不一致", component="pq"
                )
            self._append(
                EV_DUPLICATE, command_id, submitted_at,
                {"status": existing.status.value},
            )
            self._mark_dirty()
            return existing, False

        priority = Priority.EMERGENCY if kind.is_emergency else Priority.NORMAL
        deadline = None if ttl_s is None else submitted_at + ttl_s
        seq = self.state.seq
        self._append(
            EV_SUBMITTED, command_id, submitted_at,
            {
                "kind": kind.value,
                "station_id": station_id,
                "priority": priority.value_int,
                "seq": seq,
                "deadline": deadline,
                "ttl_s": ttl_s,
                "origin": origin,
                "stations": list(stations),
            },
        )
        self._mark_dirty()
        return self.state.commands[command_id], True

    # ---- 时效与顶替 ---------------------------------------------------

    def sweep_expired(self, now: float) -> list[QueuedCommand]:
        """把已过截止时间的待执行指令作废，返回被作废的指令。

        时效只约束"等待"：已开始执行（RUNNING）的指令不受截止时间影响，只受紧急
        抢占影响。紧急/故障安全关门没有截止时间，永远不会被时效作废。
        """

        expired = [
            command
            for command in self.state.pending()
            if command.deadline is not None and now > command.deadline
        ]
        for command in expired:
            stations = list(command.stations) or [command.station_id]
            self._append(
                EV_EXPIRED, command.id, now,
                {"reason": Disposition.EXPIRED.value, "deadline": command.deadline,
                 "stations": stations},
            )
        if expired:
            self._mark_dirty()
        return expired

    def apply_emergency(self, emergency: QueuedCommand, now: float) -> list[QueuedCommand]:
        """紧急关门入队后顶替待执行的开门类指令（关门类指令保留）。"""

        superseded = [
            command
            for command in self.state.pending()
            if command.kind.opens_doors and command.id != emergency.id
        ]
        for command in superseded:
            stations = list(command.stations) or [command.station_id]
            self._append(
                EV_CANCELED, command.id, now,
                {"reason": Disposition.SUPERSEDED_BY_EMERGENCY.value,
                 "by": emergency.id, "stations": stations},
            )
        if superseded:
            self._mark_dirty()
        return superseded
    # ---- 选取与执行迁移 -----------------------------------------------

    def next_pending(self, now: float) -> QueuedCommand | None:
        """在作废完过期指令后，返回优先级最高的待执行指令。"""

        self.sweep_expired(now)
        pending = self.state.pending()
        if not pending:
            return None
        return sorted(pending, key=lambda c: (-c.priority.value_int, c.seq))[0]

    def claim(self, command_id: str, now: float, units_total: int,
              owner_epoch: int) -> QueuedCommand:
        """把一条待执行指令置为 RUNNING 并落盘属主代际（抢占前的崩溃屏障）。"""

        self._append(
            EV_CLAIMED, command_id, now,
            {"units_total": units_total, "owner_epoch": owner_epoch},
        )
        # 领取不刷快照：RUNNING 事实已在哈希链 claimed 事件里（唯一事实源）。崩溃
        # 恢复从链重放即可得知有未完成指令；快照仅缓存待执行集合，下一次语义迁移
        # 再落，减少批量执行时的快照写次数。
        return self.state.commands[command_id]

    def progress(self, command_id: str, units_done: int) -> None:
        """推进执行进度。

        进度只在内存里更新，不重写快照：它是可由执行器幂等重做的派生量（关门幂等、
        开门按门区重放）。状态快照在下一次"语义迁移"（完成/作废/抢占等哈希链事件）
        时落盘，避免批量执行时每个单元都全量重写快照带来的 fsync 风暴；真在单元间
        崩溃，恢复裁决本来就会把开门类指令判为 CRASHED 并走故障安全关门。
        """

        self._set(self.state, command_id, units_done=units_done)

    def complete(self, command_id: str, now: float) -> QueuedCommand:
        self._append(EV_DONE, command_id, now)
        self._mark_dirty()
        return self.state.commands[command_id]

    def preempt(self, command_id: str, now: float, by: str,
                stations: list[str]) -> QueuedCommand:
        self._append(
            EV_PREEMPTED, command_id, now,
            {"reason": Disposition.PREEMPTED.value, "by": by, "stations": stations},
        )
        self._mark_dirty()
        return self.state.commands[command_id]

    def fail(self, command_id: str, now: float, reason: str,
             stations: list[str], *, blocks_failsafe: bool = False) -> QueuedCommand:
        self._append(
            EV_FAILED, command_id, now,
            {"reason": Disposition.FAILED.value, "detail": reason,
             "stations": stations, "blocks_failsafe": blocks_failsafe},
        )
        if blocks_failsafe:
            # 故障安全关门持续失败：停在显式阻塞现场，等现场修复后显式重试，
            # 不自动无限新建 failsafe。
            self.state.failsafe_blocked = True
        self._mark_dirty()
        return self.state.commands[command_id]

    def requeue(self, command_id: str, now: float, reason: str) -> QueuedCommand:
        """瞬时失败后把 RUNNING 指令放回队首等待重试。"""

        self._append(EV_REQUEUED, command_id, now, {"reason": reason})
        self._mark_dirty()
        return self.state.commands[command_id]

    def mark_recovered(self, command_id: str | None, now: float, owner_epoch: int,
                       action: str, unsafe: list[str] | None = None) -> None:
        """记录一次崩溃恢复裁决（resumed/aborted/orphan_unsafe）。"""

        self._append(
            EV_RECOVERED, command_id or "-", now,
            {"action": action, "owner_epoch": owner_epoch, "unsafe": list(unsafe or [])},
        )
        if unsafe:
            self.state.unsafe_stations.update(str(s) for s in unsafe)
            self.state.door_state = DoorSafeState.UNKNOWN
        self.state.owner_epoch = owner_epoch
        self._mark_dirty()

    # ---- 故障安全关门 -------------------------------------------------

    def ensure_failsafe(self, now: float) -> QueuedCommand | None:
        """存在不安全站台且没有在途关门指令时，补一条系统级故障安全关门。

        阻塞待修复现场（``failsafe_blocked``）下不再自动新建，避免持续故障时无限
        重试；由 :meth:`retry_failsafe` 在现场修复后显式解除并补一条新的。
        """

        if not self.state.unsafe_stations or self.state.failsafe_blocked:
            return None
        in_flight = [
            c for c in self.state.commands.values()
            if not c.is_terminal and not c.kind.opens_doors
        ]
        if in_flight:
            return None
        fid = f"failsafe-{self.state.train_id}-{self.state.seq}"
        stations = tuple(sorted(self.state.unsafe_stations))
        command, _ = self.submit(
            fid, CommandKind.FAILSAFE_CLOSE, stations[0], now,
            ttl_s=None, origin="system", stations=stations,
        )
        return command

    def retry_failsafe(self, now: float) -> QueuedCommand | None:
        """现场修复后显式解除阻塞，并补一条新的故障安全关门（旧失败记录保留）。"""

        if not self.state.unsafe_stations:
            return None
        self.state.failsafe_blocked = False
        return self.ensure_failsafe(now)

    # ---- 查询 ---------------------------------------------------------

    def get(self, command_id: str) -> QueuedCommand | None:
        return self.state.commands.get(command_id)

    def view(self) -> dict:
        """返回该列车队列的一致性视图。"""

        pending = self.state.pending()
        running = self.state.running()
        return {
            "train_id": self.state.train_id,
            "running": running.as_dict() if running else None,
            "pending": [command.as_dict() for command in pending],
            "pending_total": len(pending),
            "door_state": self.state.door_state.value,
            "unsafe_stations": sorted(self.state.unsafe_stations),
            "failsafe_pending": self.state.dirty(),
            "failsafe_blocked": self.state.failsafe_blocked,
            "owner_epoch": self.state.owner_epoch,
        }

    def dispositions(self) -> list[dict]:
        """返回全部作废/失败/中止指令的处置结果（按终态时间升序）。"""

        records = []
        for command in self.state.commands.values():
            if command.status in (Status.DONE, Status.PENDING, Status.RUNNING):
                continue
            records.append(
                DispositionRecord(
                    command=command,
                    reason=command.disposition,
                    at=command.finished_at or 0.0,
                    detail={"seq": command.seq, "origin": command.origin},
                ).as_dict()
            )
        return sorted(records, key=lambda item: item["at"])

    def history(self) -> list[dict]:
        """返回该列车的不可变流水（哈希链）。"""

        return [event.as_dict() for event in self._ledger.events()]
