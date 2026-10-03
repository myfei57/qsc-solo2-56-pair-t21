"""优先级门控队列调度器。

调度器把 :mod:`psdline.door.pq.store`（队列状态）、
:mod:`psdline.door.pq.actuator`（动门）和哈希链流水装成一条安全执行线，对外
提供四件事：

* :meth:`submit`——幂等入队。紧急关门立即顶替待执行的开门类指令，并唤醒泵送；
* :meth:`pump`——推进一个可中断单元（确定性仿真友好，测试用可推进时钟驱动）；
* :meth:`recover`——新属主首次接管列车时做崩溃恢复裁决；
* :meth:`drain`——把队列一路泵送回明确状态，便于验证与批处理。

属主代际（``owner_epoch``）是崩溃安全的关键：CLAIMED 事件把执行者代际落盘，
新进程代际不同，恢复时把上一属主遗留的 RUNNING 开门指令判为崩溃中止（门可能停
在半开位置），关门指令则因关门幂等而安全续做；随后由系统级故障安全关门把全部
受影响站台关回 CLOSED。

审计一致性：哈希链流水是唯一事实源，平台审计是它的可重建镜像。每条状态迁移先
落流水+快照，再镜像到 :class:`~psdline.audit.AuditLog`（按事件哈希去重）；镜像
失败不会回滚队列，而是登记待补镜像，下一轮用 :meth:`resync_audit` 补齐——因此
任何崩溃窗口下队列与审计最终都一致，且历史永远以流水为准、不可变。
"""

from __future__ import annotations

import threading
import uuid

from ...audit import AuditLog
from ...clock import Clock
from ...errors import StorageError
from ...eventbus import Event, EventBus
from ...store.jsonfile import JsonStore
from .actuator import ActuatorError, DoorActuator, ExecutionPlan
from .ledger import HashLedger, known_trains
from .locks import LockRegistry
from .model import (
    CommandKind,
    DoorSafeState,
    LedgerEvent,
    PreemptPolicy,
    Priority,
    Status,
)
from .store import PriorityCommandStore

# 开门类指令在崩溃恢复时一律中止：无法确认门停在什么位置，必须走故障安全关门。
# 关门类指令幂等，恢复后直接重放即可。

USE_DEFAULT_TTL = -1.0


class PriorityQueueRunner:
    """每列车一条串行执行线的优先级门控调度器。"""

    def __init__(
        self,
        store: JsonStore,
        clock: Clock,
        actuator: DoorActuator,
        *,
        bus: EventBus | None = None,
        audit: AuditLog | None = None,
        policy: PreemptPolicy = PreemptPolicy.PREEMPT_NORMAL,
        default_ttl_s: float | None = 30.0,
        emergency_ttl_s: float | None = None,
        max_attempts: int = 3,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("最大重试次数必须为正")
        self._store_root = store
        self._clock = clock
        self._actuator = actuator
        self._bus = bus or EventBus()
        self._audit = audit
        self._policy = policy
        self._default_ttl = default_ttl_s
        self._emergency_ttl = emergency_ttl_s
        self._max_attempts = max_attempts
        self._locks = LockRegistry(store)
        self._epoch = uuid.uuid4().int & 0x7FFFFFFF
        self._ledgers: dict[str, HashLedger] = {}
        self._queues: dict[str, PriorityCommandStore] = {}
        self._plans: dict[str, ExecutionPlan] = {}
        self._recovered: set[str] = set()
        self._touched: set[str] = set()
        self._audit_pending: set[str] = set()
        self._wake = threading.Condition()

    @property
    def policy(self) -> PreemptPolicy:
        return self._policy

    @property
    def owner_epoch(self) -> int:
        return self._epoch

    def touched_trains(self) -> list[str]:
        """返回本进程接触过的列车（供后台 worker 轮询）。"""

        return sorted(self._touched)

    def known_trains(self) -> list[str]:
        """返回落盘与内存中已知的全部列车。"""

        return sorted(set(known_trains(self._store_root)) | self._touched)

    # ---- 装配辅助 -----------------------------------------------------

    def _ledger_for(self, train_id: str) -> HashLedger:
        ledger = self._ledgers.get(train_id)
        if ledger is None:
            ledger = HashLedger(self._store_root, train_id)
            self._ledgers[train_id] = ledger
        return ledger

    def _queue_for(self, train_id: str, ledger: HashLedger) -> PriorityCommandStore:
        queue = self._queues.get(train_id)
        if queue is None:
            queue = PriorityCommandStore(self._store_root, ledger, train_id)
            self._queues[train_id] = queue
        return queue

    def _open(self, train_id: str) -> tuple[HashLedger, PriorityCommandStore]:
        """打开一列车执行线：重同步链尾 → 建仓储/增量追上 →（首次）崩溃裁决。

        所有入口（入队、泵送、查询）都先走这里，并且调用方已持有该列车的
        ``flock``：先从磁盘重同步哈希链，保证跨进程追加后本进程拿到的链尾/序号是
        最新的，不会各自从旧链尾起算把链写断；同时截掉上一进程写一半的尾行。
        """

        ledger = self._ledger_for(train_id)
        first_time = train_id not in self._recovered
        # 每次在 flock 内进入都重同步链尾并截断半行（别的进程也可能在持锁写后崩溃）。
        ledger.resync(truncate_torn=True)
        existed = train_id in self._queues
        queue = self._queue_for(train_id, ledger)
        if existed:
            # 仓储是本进程早先建的：把别的进程在链上新增的事件回放进内存状态。
            self._replay_caught_up(train_id, queue.catch_up())
        if first_time:
            before = ledger.head_seq()
            self._recover(train_id, queue)
            self._recovered.add(train_id)
            new_events = ledger.events()[before:]
            if new_events:
                queue.flush()
                self._emit(train_id, new_events)
        elif train_id in self._audit_pending:
            self.resync_audit(train_id)
        return ledger, queue

    def _replay_caught_up(self, train_id: str, events: list[LedgerEvent]) -> None:
        """把从别的进程追上的事件镜像到本进程事件总线/审计。"""

        if events:
            self._emit(train_id, events)

    def _notify(self) -> None:
        with self._wake:
            self._wake.notify_all()

    def idle_wait(self, timeout: float | None) -> bool:
        """供后台 worker 在轮询间隙等待：紧急入队会立即唤醒。返回是否被唤醒。"""

        with self._wake:
            return self._wake.wait(timeout)

    # ---- 入队 ---------------------------------------------------------

    def submit(
        self,
        train_id: str,
        command_id: str,
        kind: str | CommandKind,
        station_id: str = "",
        *,
        ttl_s: float | None = USE_DEFAULT_TTL,
        stations: tuple[str, ...] = (),
        origin: str = "client",
    ) -> dict:
        """幂等提交一条门控指令。

        ``ttl_s`` 默认取策略默认值（普通指令默认 TTL、紧急关门不过期）；显式传
        ``None`` 表示该指令永不过期。
        """

        kind = CommandKind.parse(kind) if isinstance(kind, str) else kind
        ttl = self._resolve_ttl(kind, ttl_s)
        if ttl is not None and ttl <= 0:
            raise ValueError("时效必须为正（不需要时效请显式传 None）")

        with self._locks.hold(train_id):
            self._touched.add(train_id)
            ledger, queue = self._open(train_id)
            before = ledger.head_seq()
            command, created = queue.submit(
                command_id, kind, station_id, self._clock.now(), ttl, origin, stations
            )
            superseded: list[str] = []
            # 只有客户端紧急关门顶替待执行的开门类指令；系统故障安全关门（FAILSAFE_CLOSE）
            # 是恢复手段，绝不顶替仍有效的普通指令。
            if (
                created
                and command.priority is Priority.EMERGENCY
                and command.kind is CommandKind.EMERGENCY_CLOSE
            ):
                superseded = [item.id for item in queue.apply_emergency(command, self._clock.now())]
            events = ledger.events()[before:]
            queue.flush()
            self._emit(train_id, events)
            self._notify()
            view = queue.view()
            return {
                "ok": True,
                "train_id": train_id,
                "duplicate": not created,
                "command": command.as_dict(),
                "superseded": superseded,
                "running": view["running"],
                "pending": view["pending"],
            }

    def _resolve_ttl(self, kind: CommandKind, explicit: float | None) -> float | None:
        if explicit != USE_DEFAULT_TTL:
            return explicit
        return self._emergency_ttl if kind.is_emergency else self._default_ttl

    # ---- 泵送 ---------------------------------------------------------

    def pump(self, train_id: str) -> dict:
        """推进一个调度步：恢复裁决 → 抢占判定 → 执行一个可中断单元。"""

        with self._locks.hold(train_id):
            self._touched.add(train_id)
            ledger, queue = self._open(train_id)
            # 恢复裁决可能先追加 recovered 事件，纳入本次镜像窗口。
            before = ledger.head_seq()

            now = self._clock.now()
            summary: dict = {
                "ok": True,
                "train_id": train_id,
                "claimed": None,
                "ran_unit": None,
                "completed": None,
                "preempted": None,
                "requeued": None,
                "failed": None,
                "failsafe": None,
            }

            queue.sweep_expired(now)
            running = queue.state.running()

            if running is not None and self._should_preempt(queue, running):
                emergency = next(
                    c for c in queue.state.pending() if c.priority is Priority.EMERGENCY
                )
                stations = self._stations_of(running)
                queue.preempt(running.id, now, by=emergency.id, stations=stations)
                self._plans.pop(train_id, None)
                summary["preempted"] = running.id
                running = None

            if running is not None:
                self._advance(train_id, queue, running, summary)
            else:
                failsafe = queue.ensure_failsafe(now)
                if failsafe is not None:
                    summary["failsafe"] = failsafe.id
                picked = queue.next_pending(now)
                if picked is not None:
                    plan = self._actuator.plan(picked)
                    queue.claim(picked.id, now, plan.units_total, self._epoch)
                    self._plans[train_id] = plan
                    summary["claimed"] = picked.id
                    self._advance(train_id, queue, queue.state.running(), summary)

            events = ledger.events()[before:]
            queue.flush()  # 本泵语义迁移统一刷一次检查点（每泵至多一次快照写）。
            self._emit(train_id, events)
            view = queue.view()
            summary.update(
                running=view["running"],
                pending_total=view["pending_total"],
                door_state=view["door_state"],
                unsafe_stations=view["unsafe_stations"],
                events=[event.as_dict() for event in events],
            )
            return summary

    def _should_preempt(self, queue: PriorityCommandStore, running) -> bool:
        if self._policy is not PreemptPolicy.PREEMPT_NORMAL:
            return False
        if running.priority is Priority.EMERGENCY:
            # 关门动作绝不被打断：紧急指令之间严格先来先做。
            return False
        return any(c.priority is Priority.EMERGENCY for c in queue.state.pending())

    def _advance(self, train_id: str, queue: PriorityCommandStore,
                 running, summary: dict) -> None:
        plan = self._plans.get(train_id)
        if plan is None:
            plan = self._actuator.plan(running)
            self._plans[train_id] = plan
        done = running.units_done
        unit = plan.units[done]
        try:
            self._actuator.run_unit(running, unit)
        except ActuatorError as error:
            now = self._clock.now()
            self._plans.pop(train_id, None)
            if error.transient and running.attempts < self._max_attempts:
                queue.requeue(running.id, now, reason=error.message)
                summary["requeued"] = running.id
            else:
                # 故障安全关门持续失败：置阻塞现场，停止自动重试，等显式修复。
                blocks = running.kind is CommandKind.FAILSAFE_CLOSE
                queue.fail(
                    running.id, now, reason=error.message,
                    stations=self._stations_of(running), blocks_failsafe=blocks,
                )
                summary["failed"] = running.id
                summary["failsafe_blocked"] = blocks
            return
        done += 1
        queue.progress(running.id, done)
        summary["ran_unit"] = unit
        if done >= plan.units_total:
            queue.complete(running.id, self._clock.now())
            self._plans.pop(train_id, None)
            summary["completed"] = running.id

    def drain(self, train_id: str, *, max_steps: int = 10_000) -> dict:
        """连续泵送直到队列收敛，返回累计摘要。

        收敛的含义是：没有在执行/待执行的指令，并且现场要么安全（无不安全站台），
        要么显式停在"故障安全关门阻塞待修复"。持续故障下不会无限重试。
        """

        steps = 0
        ran = 0
        completed: list[str] = []
        for _ in range(max_steps):
            view = self.view(train_id)
            if (
                view["running"] is None
                and view["pending_total"] == 0
                and (not view["failsafe_pending"])
            ):
                break
            result = self.pump(train_id)
            steps += 1
            if result["ran_unit"]:
                ran += 1
            if result["completed"]:
                completed.append(result["completed"])
        return {
            "ok": True,
            "train_id": train_id,
            "steps": steps,
            "units_run": ran,
            "completed": completed,
            "view": self.view(train_id),
        }

    def retry_failsafe(self, train_id: str) -> dict:
        """现场修复后显式解除故障安全阻塞并立即泵送一次。"""

        with self._locks.hold(train_id):
            ledger, queue = self._open(train_id)
            before = ledger.head_seq()
            command = queue.retry_failsafe(self._clock.now())
            if command is not None:
                queue.flush()
                self._emit(train_id, ledger.events()[before:])
            result = self.pump(train_id)
            result["ok"] = True
            result["retried"] = command.id if command is not None else None
            return result

    # ---- 崩溃恢复 -----------------------------------------------------

    def _recover(self, train_id: str, queue: PriorityCommandStore) -> None:
        """新属主首次接管列车时的恢复裁决。"""

        now = self._clock.now()
        state = queue.state
        running = state.running()
        if running is None:
            if state.unsafe_stations:
                queue.mark_recovered(
                    None, now, self._epoch, "orphan_unsafe",
                    sorted(state.unsafe_stations),
                )
            return
        if state.owner_epoch == self._epoch:
            return
        plan = self._actuator.plan(running)
        if running.kind.opens_doors:
            # 开门可能停在半开位置：无法确认物理态，中止并转故障安全关门。
            queue.mark_recovered(running.id, now, self._epoch, "aborted")
            self._plans.pop(train_id, None)
        else:
            # 关门幂等，续做即可（已关闭的门重复关门保持成功）。
            queue.mark_recovered(running.id, now, self._epoch, "resumed")
            self._plans[train_id] = plan

    def is_recovered(self, train_id: str) -> bool:
        return train_id in self._recovered

    # ---- 审计镜像 -----------------------------------------------------

    def _emit(self, train_id: str, events: list[LedgerEvent]) -> None:
        for event in events:
            if self._bus is not None:
                self._bus.publish(
                    Event.of(
                        "pq", "pq", event.type,
                        train_id=train_id,
                        command_id=event.command_id,
                        seq=event.seq,
                    )
                )
            if self._audit is not None:
                self._mirror_one(train_id, event)

    @staticmethod
    def _audit_detail(event: LedgerEvent) -> dict:
        """把事件载荷转成审计 detail，剔除与 AuditEntry 具名字段冲突的键。"""

        reserved = {"station_id", "kind", "at"}
        return {
            f"event_{key}" if key in reserved else key: value
            for key, value in event.payload.items()
            if key not in ("station_id",)
        }

    def _mirror_one(self, train_id: str, event: LedgerEvent) -> None:
        assert self._audit is not None
        command = event.command_id if event.command_id != "-" else ""
        station_id = str(event.payload.get("station_id", ""))
        try:
            self._audit.record_once(
                event.hash,
                f"pq_{event.type}",
                train_id,
                station_id,
                at=event.at,
                command_id=command,
                **self._audit_detail(event),
            )
        except StorageError:
            # 流水是事实源，镜像可稍后补齐；不阻塞队列执行。
            self._audit_pending.add(train_id)

    def resync_audit(self, train_id: str) -> int:
        """按哈希链重放，把缺失的事件补镜像到平台审计，返回新增条数。"""

        if self._audit is None:
            return 0
        ledger = self._ledger_for(train_id)
        added = 0
        for event in ledger.events():
            station_id = str(event.payload.get("station_id", ""))
            entry = self._audit.record_once(
                event.hash,
                f"pq_{event.type}",
                train_id,
                station_id,
                at=event.at,
                command_id=event.command_id if event.command_id != "-" else "",
                **self._audit_detail(event),
            )
            if entry is None:
                added += 1
        self._audit_pending.discard(train_id)
        return added

    def verify_chain(self, train_id: str | None = None) -> dict[str, int]:
        """校验一列车（或全部已知列车）的哈希链完整性。"""

        from .ledger import verify_all

        return verify_all(self._store_root, None if train_id is None else [train_id])

    # ---- 查询 ---------------------------------------------------------

    def view(self, train_id: str) -> dict:
        """列车队列一致性视图（队列态 + 执行器物理门态 + 恢复标记）。"""

        with self._locks.hold(train_id):
            ledger, queue = self._open(train_id)
            view = queue.view()
            view["recovered"] = train_id in self._recovered
            # 队列认定的门态要与执行器物理门态对照，两者不符即暴露为 mixed/unknown。
            stations = sorted(
                {
                    command.station_id
                    for command in queue.state.commands.values()
                    if command.station_id
                }
            )
            view["physical_states"] = {
                station: self._actuator.physical_state(station) for station in stations
            }
            return view

    def get(self, train_id: str, command_id: str) -> dict | None:
        with self._locks.hold(train_id):
            ledger, queue = self._open(train_id)
            command = queue.get(command_id)
            return None if command is None else command.as_dict()

    def dispositions(self, train_id: str) -> list[dict]:
        """返回该列车全部作废/失败/中止指令的可查处置结果。"""

        with self._locks.hold(train_id):
            ledger, queue = self._open(train_id)
            return queue.dispositions()

    def history(self, train_id: str) -> list[dict]:
        """返回该列车的不可变哈希链流水。"""

        with self._locks.hold(train_id):
            ledger, _queue = self._open(train_id)
            return [event.as_dict() for event in ledger.events()]

    def safe_state(self, train_id: str) -> str:
        return self.view(train_id)["door_state"]

    def is_safe(self, train_id: str) -> bool:
        """门控是否已回到明确的安全状态（CLOSED 且无待收敛站台、队列空闲）。"""

        view = self.view(train_id)
        return (
            self.safe_state(train_id) == DoorSafeState.CLOSED.value
            and not view["unsafe_stations"]
            and view["running"] is None
            and view["pending_total"] == 0
        )

    @staticmethod
    def _stations_of(command) -> list[str]:
        stations = list(command.stations)
        if not stations and command.station_id:
            stations = [command.station_id]
        return stations
