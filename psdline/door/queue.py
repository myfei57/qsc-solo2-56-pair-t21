"""门控指令队列与发车。

同一列车的门控指令按优先级排队：紧急指令插到待执行区队首，普通指令保持提交
顺序。紧急指令不打断正在执行的那条——开关门是物理动作，执行中被打断会留下
半开半关的不可解释现场；不抢占换来任何时刻门控状态机都闭合、可预期，代价是
紧急指令最多等一条在执行指令走完。

每条指令带时效：超时仍未开始执行的自动作废，作废原因（``expired`` /
``execution_failed`` / ``execution_interrupted``）写进审计流水。作废只动队列
不动门——门控状态由 ``settle_state`` 收成明确的三态（``idle`` / ``armed`` /
``executing``）落盘；门开着而队列里已没有任何关门意图时标记 ``hazard``，交给
上位机告警，而不是自动关门（自动关门可能夹人，"明确"不等于"自动复位"）。

作废是两阶段的：先把指令认领进作废区，审计落盘后再确认删除。崩溃恢复时对照
审计流水判断每条指令该补写处置记录还是直接清理，保证处置结果不丢、不重。
指令的终态（完成 / 作废）只写进只追加的审计流水，历史不可变、可回放查询。

发车时先把"已发车"落盘，再清空队列。先落盘再清空保证：即使清空过程中掉电，
重启后也能看到该车已经发过车，不会重放指令。
"""

from __future__ import annotations

from ..audit import AuditLog
from ..errors import InterlockError
from ..store import DoorCommand
from .close import close_and_audit, close_doc_id, close_durable
from .door import DoorController
from .open import open_doc_id, open_doors, opened

DISPOSITION_KINDS = ("door_command_completed", "door_command_voided")


def departed_doc_id(train_id: str) -> str:
    return f"door/{train_id}/departed"


def command_state_doc_id(train_id: str) -> str:
    return f"door/{train_id}/command-state"


def enqueue(
    controller: DoorController,
    train_id: str,
    command_id: str,
    kind: str,
    station_id: str,
    priority: str = "normal",
    ttl_s: float = 0.0,
    at: float = 0.0,
) -> tuple[DoorCommand, bool]:
    """把一条门控指令排进列车队列，返回 ``(指令, 是否新入队)``。

    同一标识的指令仍在队列（含执行中、作废中）时返回原指令且 ``是否新入队``
    为 ``False``，重复提交不会产生第二条。
    """

    command = DoorCommand(
        id=command_id,
        train_id=train_id,
        kind=kind,
        epoch=controller.commands.epoch,
        station_id=station_id,
        priority=priority,
        enqueued_at=at,
        ttl_s=ttl_s,
    )
    return controller.commands.append(command)


def process_queue(controller: DoorController, train_id: str) -> list[str]:
    """返回列车当前排队中的指令标识；已发车时返回空队列。"""

    if departed(controller, train_id):
        return []
    return controller.commands.ids(train_id)


def depart(controller: DoorController, train_id: str, at: float) -> dict:
    """把发车事实落盘并清空指令队列。"""

    controller.store.write(departed_doc_id(train_id), {"train_id": train_id, "at": at})
    cleared = controller.commands.clear(train_id)
    return {"train_id": train_id, "departed": True, "cleared_commands": cleared}


def departed(controller: DoorController, train_id: str) -> bool:
    """判断列车是否已发车。"""

    return controller.store.exists(departed_doc_id(train_id))


def queued_total(controller: DoorController) -> int:
    """返回全部门控队列里的待执行指令条数。"""

    return controller.commands.total_queued()


def _doors_physically_open(controller: DoorController, train_id: str) -> bool:
    """判断门是否处于物理打开：有开门记录且没有被更新的关门落盘覆盖。"""

    open_doc = controller.store.read(open_doc_id(train_id))
    if not isinstance(open_doc, dict):
        return False
    close_doc = controller.store.read(close_doc_id(train_id))
    close_at = -1.0
    if isinstance(close_doc, dict) and close_doc.get("durable"):
        close_at = float(close_doc.get("at", 0.0))
    return float(open_doc.get("at", 0.0)) > close_at


def settle_state(controller: DoorController, train_id: str, now: float, reason: str) -> dict:
    """把列车门控指令状态收成明确的三态并落盘，返回状态文档。

    门开着、未发车、且队列里已没有任何关门意图时标记
    ``hazard=open_without_close_intent``，由上位机告警跟进。
    """

    pending = controller.commands.pending(train_id)
    executing = controller.commands.executing(train_id)
    if executing is not None:
        state = "executing"
    elif pending:
        state = "armed"
    else:
        state = "idle"
    close_intent = any(command.kind == "close" for command in pending) or (
        executing is not None and executing.kind == "close"
    )
    hazard = ""
    if (
        _doors_physically_open(controller, train_id)
        and not departed(controller, train_id)
        and not close_intent
    ):
        hazard = "open_without_close_intent"
    doc = {
        "train_id": train_id,
        "state": state,
        "reason": reason,
        "at": now,
        "head": pending[0].id if pending else "",
        "executing": executing.id if executing else "",
        "hazard": hazard,
    }
    controller.store.write(command_state_doc_id(train_id), doc)
    return doc


def command_state(controller: DoorController, train_id: str) -> dict:
    """返回列车门控指令状态；没有落盘记录时按队列实况推导（纯查询）。"""

    raw = controller.store.read(command_state_doc_id(train_id))
    if isinstance(raw, dict):
        return raw
    pending = controller.commands.pending(train_id)
    executing = controller.commands.executing(train_id)
    if executing is not None:
        state = "executing"
    elif pending:
        state = "armed"
    else:
        state = "idle"
    return {
        "train_id": train_id,
        "state": state,
        "reason": "",
        "at": 0.0,
        "head": pending[0].id if pending else "",
        "executing": executing.id if executing else "",
        "hazard": "",
    }


def _record_void(
    controller: DoorController,
    audit: AuditLog,
    command: DoorCommand,
    now: float,
    reason: str,
    message: str,
) -> dict:
    """写一条作废处置审计，再确认删除；审计失败时指令留在作废区等恢复。"""

    entry = audit.record(
        "door_command_voided",
        command.train_id,
        command.station_id,
        at=now,
        command_id=command.id,
        command_kind=command.kind,
        priority=command.priority,
        reason=reason,
        message=message,
        enqueued_at=command.enqueued_at,
        expires_at=command.expires_at,
        epoch=command.epoch,
    )
    controller.commands.confirm_voided(command.train_id, [command.id])
    return {
        "command_id": command.id,
        "outcome": "voided",
        "reason": reason,
        "audit_id": entry.id,
        "at": now,
    }


def void_expired(
    controller: DoorController,
    audit: AuditLog,
    train_id: str,
    now: float,
    known_voided: frozenset = frozenset(),
) -> list[dict]:
    """作废列车队列里超时未执行的指令，返回作废处置清单。

    ``known_voided`` 是恢复流程从审计流水回放出的已作废标识：已在其中的指令
    直接确认清理，不重复写审计，保证崩溃重放后作废是幂等的。
    """

    claimed = controller.commands.claim_expired(train_id, now)
    dispositions = []
    for command in claimed:
        if command.id in known_voided:
            controller.commands.confirm_voided(train_id, [command.id])
            continue
        dispositions.append(
            _record_void(controller, audit, command, now, "expired", "超过时效未执行")
        )
    if claimed:
        settle_state(controller, train_id, now, "command_voided")
    return dispositions


def void_expired_all(controller: DoorController, audit: AuditLog, now: float) -> dict:
    """扫描全部有队列的列车，作废超时指令，返回 ``{列车: 处置清单}``。"""

    found: dict[str, list[dict]] = {}
    for train_id in controller.commands.queued_train_ids():
        dispositions = void_expired(controller, audit, train_id, now)
        if dispositions:
            found[train_id] = dispositions
    return found


def _apply(controller: DoorController, audit: AuditLog, command: DoorCommand, now: float) -> None:
    """把一条指令落到门控事实上。"""

    if command.kind == "open":
        open_doors(controller, command.train_id, now)
        return
    if command.kind == "close":
        close_and_audit(
            controller, audit, command.train_id, command.station_id, "door_command", now
        )
        return
    raise InterlockError(f"未知门控指令类型: {command.kind}", component="door")


def execute_next(controller: DoorController, audit: AuditLog, train_id: str, now: float) -> dict:
    """执行列车队首指令：先作废超时指令，再取队首执行并落处置结果。

    执行失败（联锁拒绝、对位缺失、审计落盘失败等）时指令按
    ``execution_failed`` 作废并落盘处置结果，门控状态随即归位——失败不会把
    指令卡在队首挡住后面的指令。
    """

    if departed(controller, train_id):
        return {
            "ok": False,
            "train_id": train_id,
            "executed": "",
            "voided": [],
            "violations": ["列车已发车"],
            "state": command_state(controller, train_id),
        }
    voided = void_expired(controller, audit, train_id, now)
    if controller.commands.executing(train_id) is not None:
        return {
            "ok": False,
            "train_id": train_id,
            "executed": "",
            "voided": voided,
            "violations": ["有指令正在执行，等它走完再执行下一条"],
            "state": command_state(controller, train_id),
        }
    command = controller.commands.start_next(train_id)
    if command is None:
        return {
            "ok": True,
            "train_id": train_id,
            "executed": "",
            "voided": voided,
            "violations": [],
            "state": command_state(controller, train_id),
        }
    audit.record(
        "door_command_started",
        train_id,
        command.station_id,
        at=now,
        command_id=command.id,
        command_kind=command.kind,
        priority=command.priority,
    )
    settle_state(controller, train_id, now, "command_started")
    try:
        _apply(controller, audit, command, now)
    except Exception as error:
        message = getattr(error, "message", None) or str(error)
        # 先落处置审计再清执行槽：崩溃窗口由恢复流程按审计判重补齐。
        audit.record(
            "door_command_voided",
            train_id,
            command.station_id,
            at=now,
            command_id=command.id,
            command_kind=command.kind,
            priority=command.priority,
            reason="execution_failed",
            message=message,
            enqueued_at=command.enqueued_at,
            expires_at=command.expires_at,
            epoch=command.epoch,
        )
        controller.commands.complete(train_id, command.id)
        state = settle_state(controller, train_id, now, "execution_failed")
        return {
            "ok": False,
            "train_id": train_id,
            "executed": command.id,
            "voided": voided,
            "violations": [message],
            "state": state,
        }
    audit.record(
        "door_command_completed",
        train_id,
        command.station_id,
        at=now,
        command_id=command.id,
        command_kind=command.kind,
        priority=command.priority,
        epoch=command.epoch,
    )
    controller.commands.complete(train_id, command.id)
    state = settle_state(controller, train_id, now, "command_completed")
    return {
        "ok": True,
        "train_id": train_id,
        "executed": command.id,
        "voided": voided,
        "violations": [],
        "state": state,
    }


def dispositions(audit: AuditLog, train_id: str = "") -> list[dict]:
    """从只追加的审计流水回放指令处置结果（完成 / 作废），可按列车过滤。"""

    found = []
    for entry in audit.replay():
        if entry.kind not in DISPOSITION_KINDS:
            continue
        if train_id and entry.train_id != train_id:
            continue
        found.append(
            {
                "command_id": entry.detail.get("command_id", ""),
                "outcome": "completed" if entry.kind == "door_command_completed" else "voided",
                "reason": entry.detail.get("reason", ""),
                "train_id": entry.train_id,
                "at": entry.at,
                "audit_id": entry.id,
                "detail": dict(entry.detail),
            }
        )
    return found


def _fact_done(controller: DoorController, command: DoorCommand) -> bool:
    """判断指令对应的门控事实是否已落盘。"""

    if command.kind == "open":
        return opened(controller, command.train_id)
    if command.kind == "close":
        return close_durable(controller, command.train_id)
    return False


def recover_commands(controller: DoorController, audit: AuditLog, now: float) -> dict:
    """启动恢复：对照审计流水清理崩溃现场，让每列车门控回到明确状态。

    - 执行中的指令：门事实已落盘的补记完成，未落盘的按
      ``execution_interrupted`` 作废——不自动重放物理动作；
    - 作废区里滞留的指令：审计缺处置记录的补写，然后确认清理；
    - 待执行区里已过期的指令：按正常作废流程处理。

    所有判定都先查审计流水里已落盘的处置结果，因此重复恢复不会重复记录。
    """

    replayed = audit.replay()
    voided_ids = {
        entry.detail.get("command_id") for entry in replayed if entry.kind == "door_command_voided"
    }
    completed_ids = {
        entry.detail.get("command_id")
        for entry in replayed
        if entry.kind == "door_command_completed"
    }
    report: dict = {"completed": [], "voided": [], "trains": []}
    for train_id in controller.commands.queued_train_ids():
        touched = False
        command = controller.commands.executing(train_id)
        if command is not None:
            if _fact_done(controller, command):
                if command.id not in completed_ids:
                    audit.record(
                        "door_command_completed",
                        train_id,
                        command.station_id,
                        at=now,
                        command_id=command.id,
                        command_kind=command.kind,
                        priority=command.priority,
                        recovered=True,
                        epoch=command.epoch,
                    )
                controller.commands.complete(train_id, command.id)
                report["completed"].append(command.id)
            else:
                if command.id not in voided_ids:
                    audit.record(
                        "door_command_voided",
                        train_id,
                        command.station_id,
                        at=now,
                        command_id=command.id,
                        command_kind=command.kind,
                        priority=command.priority,
                        reason="execution_interrupted",
                        message="执行中被重启打断",
                        enqueued_at=command.enqueued_at,
                        expires_at=command.expires_at,
                        epoch=command.epoch,
                    )
                controller.commands.complete(train_id, command.id)
                report["voided"].append(command.id)
            touched = True
        for dangling in controller.commands.voiding(train_id):
            if dangling.id not in voided_ids:
                _record_void(controller, audit, dangling, now, "expired", "超过时效未执行")
                report["voided"].append(dangling.id)
            else:
                controller.commands.confirm_voided(train_id, [dangling.id])
            touched = True
        expired = void_expired(controller, audit, train_id, now, known_voided=voided_ids)
        if expired:
            report["voided"].extend(item["command_id"] for item in expired)
            touched = True
        if touched:
            settle_state(controller, train_id, now, "startup_recover")
            report["trains"].append(train_id)
    if report["trains"]:
        audit.record(
            "door_command_recovered",
            "",
            "",
            at=now,
            completed=len(report["completed"]),
            voided=len(report["voided"]),
            trains=list(report["trains"]),
        )
    return report
