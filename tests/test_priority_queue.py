"""优先级门控队列测试。

覆盖：优先级插队、紧急抢占 vs 等执行完、时效作废、故障安全关门回态、幂等、
并发入队、瞬时失败重试、持续失败转故障安全、崩溃恢复（开门中止/关门续做）、
快照删除重建、哈希链篡改检测、以及队列-门态-审计三者一致。
"""

from __future__ import annotations

import threading

import pytest

from psdline.app import build_platform
from psdline.config import default_config
from psdline.clock import ManualClock
from psdline.door.pq import (
    ActuatorError,
    CommandKind,
    Disposition,
    GateActuator,
    PreemptPolicy,
    PriorityQueueRunner,
    SimulatedCrash,
    Status,
)
from psdline.door.pq.ledger import HashLedger
from psdline.errors import RailError, StorageError


class FaultInjectingActuator(GateActuator):
    """仅用于验证：在生产执行器上叠加故障/掉电注入。"""

    def __init__(self, gates, lockouts=None) -> None:
        super().__init__(gates, lockouts)
        self._fail_once: dict[str, str] = {}
        self._fail_station: set[str] = set()
        self._crash_after: dict[str, int] = {}

    def fail_once(self, station_id: str, message: str = "执行器瞬时故障") -> None:
        self._fail_once[station_id] = message

    def fail_station(self, station_id: str) -> None:
        self._fail_station.add(station_id)

    def repair(self, station_id: str) -> None:
        self._fail_station.discard(station_id)
        self._fail_once.pop(station_id, None)

    def crash_after_units(self, command_id: str, units: int) -> None:
        self._crash_after[command_id] = units

    def run_unit(self, command, unit: str) -> None:
        station_id = unit.split(":", 1)[0]
        if station_id in self._fail_station:
            raise ActuatorError(f"站台 {station_id} 门控持续故障", transient=False)
        once = self._fail_once.pop(station_id, None)
        if once is not None:
            raise ActuatorError(f"{once} @ {station_id}", transient=True)
        super().run_unit(command, unit)
        threshold = self._crash_after.get(command.id)
        if threshold is not None and command.units_done + 1 >= threshold:
            self._crash_after.pop(command.id, None)
            # 物理动作（上一行）已发生，但进度/完成事件尚未落盘即掉电。
            raise SimulatedCrash(
                f"{command.id} 在第 {command.units_done + 1} 单元后掉电"
            )


# ---- 夹具 -----------------------------------------------------------------


@pytest.fixture()
def rig(tmp_path):
    clock = ManualClock()
    platform = build_platform(
        default_config(),
        clock=clock,
        storage_root=str(tmp_path / "state"),
        pq_actuator=None,
    )
    platform.start()
    # runner 惰性装配，先把注入执行器塞进去，首次访问 platform.pq 时生效。
    fault_actuator = FaultInjectingActuator(platform.gates, platform.lockouts)
    platform._pq_actuator_override = fault_actuator
    return platform, clock, platform.pq, fault_actuator


def _open_gates(platform, station_id="ST1"):
    return [g for g in platform.gates.by_station(station_id) if g.open]


# ---- 基础调度 -------------------------------------------------------------


def test_normal_commands_fifo_and_drain_opens_all(rig):
    platform, clock, pq, actuator = rig
    # 不使用时效，避免手动泵送推进时误作废。
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.submit("T01", "n2", CommandKind.CLOSE, "ST1", ttl_s=None)
    result = pq.drain("T01")
    assert result["completed"] == ["n1", "n2"]
    assert result["view"]["door_state"] == "closed"
    assert _open_gates(platform) == []


def test_emergency_inserts_at_head_before_normal(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.submit("T01", "n2", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    view = pq.view("T01")
    pending_ids = [c["id"] for c in view["pending"]]
    # 待执行的开门指令被顶替，紧急关门独占队首。
    assert pending_ids == ["e1"]
    dispositions = {d["command_id"]: d["reason"] for d in pq.dispositions("T01")}
    assert dispositions == {
        "n1": Disposition.SUPERSEDED_BY_EMERGENCY.value,
        "n2": Disposition.SUPERSEDED_BY_EMERGENCY.value,
    }


def test_emergency_preempts_running_batch_open_at_group_boundary(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    # 执行第一个门区单元（ST1 有 4 个门区 G01..G04）。
    pq.pump("T01")
    assert pq.view("T01")["running"]["id"] == "n1"
    open_after_first = len(_open_gates(platform))
    assert open_after_first > 0

    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    # 下一次泵送：n1 在门区边界被抢占；同一步内紧急关门 e1（单站台单单元）被领取并完成。
    step = pq.pump("T01")
    assert step["preempted"] == "n1"
    assert step["completed"] == "e1"
    disposition = next(d for d in pq.dispositions("T01") if d["command_id"] == "n1")
    assert disposition["reason"] == Disposition.PREEMPTED.value
    view = pq.view("T01")
    assert view["door_state"] == "closed"
    assert view["running"] is None and view["pending_total"] == 0
    assert _open_gates(platform) == []


def test_emergency_does_not_preempt_running_emergency(rig):
    platform, clock, pq, actuator = rig
    # e0 关 ST1/ST2/ST3 三个站台，三单元；跑过一个单元后 e1 到达，
    # 此时 e0 仍在执行（第二个单元还没跑）。
    pq.submit("T01", "e0", CommandKind.EMERGENCY_CLOSE, "ST1",
              ttl_s=None, stations=("ST1", "ST2", "ST3"))
    pq.pump("T01")
    assert pq.view("T01")["running"]["id"] == "e0"
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    step = pq.pump("T01")
    # 关门动作不可打断：继续执行 e0 的第二个单元，而不是抢占。
    assert step["preempted"] is None
    assert step["ran_unit"] == "ST2"
    assert pq.view("T01")["running"]["id"] == "e0"
    result = pq.drain("T01")
    assert result["completed"][0] == "e0"
    assert result["completed"][1] == "e1"


def test_wait_running_policy_defers_preemption(rig, tmp_path):
    clock = ManualClock()
    platform = build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "p2"))
    platform.start()
    pq = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts),
        policy=PreemptPolicy.WAIT_RUNNING,
    )
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.pump("T01")  # n1 运行中
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    step = pq.pump("T01")
    assert step["preempted"] is None
    assert pq.view("T01")["running"]["id"] == "n1"
    result = pq.drain("T01")
    # n1 先做完，e1 随后关门。
    assert result["completed"] == ["n1", "e1"]
    assert result["view"]["door_state"] == "closed"


# ---- 时效 -----------------------------------------------------------------


def test_expired_pending_command_is_voided_with_reason(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=10.0)
    clock.advance(11)
    pq.submit("T01", "n2", CommandKind.OPEN, "ST1", ttl_s=None)
    result = pq.drain("T01")
    reasons = {d["command_id"]: d["reason"] for d in pq.dispositions("T01")}
    assert reasons["n1"] == Disposition.EXPIRED.value
    # n1 过期先触发故障安全关门把门关回 CLOSED（恢复到明确状态），
    # 随后仍有效的 n2 正常开门执行。
    completed = result["completed"]
    assert any(c.startswith("failsafe-") for c in completed)
    assert completed[-1] == "n2"
    assert result["view"]["door_state"] == "open"

    # 再发一条关门，最终门明确关回 CLOSED。
    pq.submit("T01", "c1", CommandKind.CLOSE, "ST1", ttl_s=None)
    final = pq.drain("T01")
    assert final["view"]["door_state"] == "closed"
    assert _open_gates(platform) == []


def test_expired_close_command_triggers_failsafe_not_open_door(rig):
    platform, clock, pq, actuator = rig
    # 门先打开。
    pq.submit("T01", "open1", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.drain("T01")
    assert pq.is_safe("T01") is False  # 门开着，不是安全态（属正常停站）
    # 一条过期的关门指令作废后，必须有故障安全关门兜底。
    pq.submit("T01", "lateclose", CommandKind.CLOSE, "ST1", ttl_s=5.0)
    clock.advance(6)
    result = pq.drain("T01")
    assert result["view"]["door_state"] == "closed"
    completed = result["completed"]
    assert any(c.startswith("failsafe-") for c in completed)
    assert _open_gates(platform) == []


def test_ttl_does_not_expire_running_command(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=1.0)
    pq.pump("T01")  # 开始执行
    clock.advance(50)
    result = pq.drain("T01")
    # 已开始的指令不因超时作废；开门后状态为 OPEN。
    assert "n1" in result["completed"]
    assert result["view"]["door_state"] == "open"


def test_emergency_close_never_expires(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    clock.advance(10_000)
    result = pq.drain("T01")
    assert result["completed"] == ["e1"]


# ---- 幂等 -----------------------------------------------------------------


def test_submit_is_idempotent_on_command_id(rig):
    platform, clock, pq, actuator = rig
    first = pq.submit("T01", "dup-1", CommandKind.OPEN, "ST1", ttl_s=None)
    second = pq.submit("T01", "dup-1", CommandKind.OPEN, "ST1", ttl_s=None)
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert second["command"]["seq"] == first["command"]["seq"]
    pq.drain("T01")
    # 同 ID 再来一次：返回已完成的原指令，不重复执行。
    third = pq.submit("T01", "dup-1", CommandKind.OPEN, "ST1", ttl_s=None)
    assert third["duplicate"] is True
    assert third["command"]["status"] == Status.DONE.value


def test_submit_conflicting_payload_rejected(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "x1", CommandKind.OPEN, "ST1", ttl_s=None)
    with pytest.raises(RailError):
        pq.submit("T01", "x1", CommandKind.CLOSE, "ST1", ttl_s=None)


def test_concurrent_enqueue_serialized_and_all_present(rig):
    platform, clock, pq, actuator = rig
    per_thread = 10
    nthreads = 8
    total = per_thread * nthreads

    def worker(prefix):
        for i in range(per_thread):
            pq.submit("T01", f"{prefix}-{i}", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)

    threads = [threading.Thread(target=worker, args=(f"t{n}",)) for n in range(nthreads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    view = pq.view("T01")
    assert view["pending_total"] == total
    # 序号无重复，覆盖 0..total-1。
    seqs = sorted(c["seq"] for c in view["pending"])
    assert seqs == list(range(total))
    # 全部最终可执行完（failsafe 不应出现，因为是正常 drain）。
    result = pq.drain("T01", max_steps=100000)
    assert result["view"]["door_state"] == "open"
    assert len(result["completed"]) == total
    assert not pq.dispositions("T01")


# ---- 失败恢复 -------------------------------------------------------------


def test_transient_failure_is_retried_then_succeeds(rig):
    platform, clock, pq, actuator = rig
    actuator.fail_once("ST1")
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    first = pq.pump("T01")
    assert first["requeued"] == "n1"
    assert pq.view("T01")["pending"][0]["id"] == "n1"
    result = pq.drain("T01")
    assert "n1" in result["completed"]
    assert not pq.dispositions("T01")


def test_persistent_failure_blocks_and_explicit_retry_failsafe(rig):
    platform, clock, pq, actuator = rig
    actuator.fail_station("ST1")
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    result = pq.drain("T01")
    disposition = next(d for d in pq.dispositions("T01") if d["command_id"] == "n1")
    assert disposition["reason"] == Disposition.FAILED.value
    # 故障安全关门也持续失败 → 队列停在显式"阻塞待修复"现场，不无限重试。
    assert result["view"]["door_state"] == "unknown"
    assert result["view"]["failsafe_blocked"] is True
    # 阻塞期间再泵送不会产生新指令（幂等稳定现场）。
    again = pq.drain("T01")
    assert again["steps"] == 0
    # 现场修复后显式重试，补一条新的 failsafe 并关门；旧失败记录不可变保留。
    actuator.repair("ST1")
    healed = pq.retry_failsafe("T01")
    healed_drain = pq.drain("T01")
    assert healed_drain["view"]["door_state"] == "closed"
    assert healed_drain["view"]["failsafe_blocked"] is False
    assert _open_gates(platform) == []
    # 重试当步领取并执行新的 failsafe（单站台单单元）。
    assert healed["retried"] and healed["retried"].startswith("failsafe-")
    assert healed["completed"] == healed["retried"]
    # 历史里保留 n1 与首个 failsafe 的失败记录，以及重试后成功的 failsafe。
    history_types = [e["type"] for e in pq.history("T01")]
    assert history_types.count("failed") >= 2


def test_crash_mid_open_aborts_and_failsafe_closes(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.pump("T01")  # 第一个门区已开
    actuator.crash_after_units("n1", 1)  # 再执行一单元即"掉电"
    with pytest.raises(SimulatedCrash):
        pq.pump("T01")
    partially_open = len(_open_gates(platform))
    assert partially_open > 0

    # 新属主（新进程）接管：新的 runner，同一存储目录。
    runner2 = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts)
    )
    result = runner2.drain("T01")
    disposition = next(d for d in runner2.dispositions("T01") if d["command_id"] == "n1")
    assert disposition["reason"] == Disposition.CRASHED.value
    assert any(c.startswith("failsafe-") for c in result["completed"])
    assert result["view"]["door_state"] == "closed"
    assert _open_gates(platform) == []


def test_crash_mid_close_is_resumed_idempotently(rig):
    platform, clock, pq, actuator = rig
    # 先开门。
    pq.submit("T01", "open1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.drain("T01")
    # 关门指令执行中掉电。
    pq.submit("T01", "c1", CommandKind.EMERGENCY_CLOSE, "ST1", ttl_s=None)
    actuator.crash_after_units("c1", 1)
    with pytest.raises(SimulatedCrash):
        pq.pump("T01")
    runner2 = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts)
    )
    result = runner2.drain("T01")
    # c1 被恢复续做（幂等关门），没有作废记录。
    assert "c1" in result["completed"]
    assert all(d["command_id"] != "c1" for d in runner2.dispositions("T01"))
    assert result["view"]["door_state"] == "closed"


def test_snapshot_deleted_is_rebuilt_from_ledger(rig, tmp_path):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.drain("T01")
    # 删掉快照与内存里的 runner，强制从哈希链重建。
    store_root = platform.store.root
    import os
    for name in os.listdir(os.path.join(store_root, "pq", "state")):
        os.remove(os.path.join(store_root, "pq", "state", name))
    runner2 = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts)
    )
    view = runner2.view("T01")
    assert view["door_state"] == "open"
    cmd = runner2.get("T01", "n1")
    assert cmd["status"] == Status.DONE.value


def test_hash_chain_detects_tampering(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.drain("T01")
    pq.verify_chain("T01")  # 不抛异常
    # 直接篡改流水文件中的一条记录。
    path = HashLedger(platform.store, "T01").path
    import json
    with open(path, "r", encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    event = json.loads(lines[1])
    event["payload"]["kind"] = "emergency_close"
    lines[1] = json.dumps(event, ensure_ascii=False)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    with pytest.raises(StorageError):
        HashLedger(platform.store, "T01").verify()


def test_torn_tail_event_is_truncated_before_recovery(rig):
    platform, clock, pq, actuator = rig
    # batch_open 有 4 个门区单元，pump 一次后停在 RUNNING（claimed 已落链、未完成）。
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.pump("T01")
    assert pq.view("T01")["running"]["id"] == "n1"
    # 在流水末尾追加一条写了一半的 JSON 事件（模拟掉电）。
    path = HashLedger(platform.store, "T01").path
    with open(path, "a", encoding="utf-8") as handle:
        handle.write('{"seq": 99, "type": "done", "train_id": "T01", "co')
    runner2 = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts)
    )
    # 首次泵送即截断半行并做崩溃裁决：开门 RUNNING 判为 CRASHED，再故障安全关门。
    result = runner2.drain("T01")
    assert result["view"]["door_state"] == "closed"
    reasons = {d["command_id"]: d["reason"] for d in runner2.dispositions("T01")}
    assert reasons.get("n1") == Disposition.CRASHED.value
    # 截断后哈希链仍完整可校验，且尾部不再有半行。
    assert runner2.verify_chain("T01")["T01"] >= 1
    assert _open_gates(platform) == []


def test_history_is_append_only_and_complete(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.OPEN, "ST1", ttl_s=None)
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    pq.drain("T01")
    history = pq.history("T01")
    types = [e["type"] for e in history]
    assert types[0] == "submitted"
    assert "canceled" in types
    assert "submitted" in types and "claimed" in types and "done" in types
    # seq 连续、prev_hash 首尾相接。
    for prev, curr in zip(history, history[1:]):
        assert curr["seq"] == prev["seq"] + 1
        assert curr["prev_hash"] == prev["hash"]


# ---- 队列/门态/审计一致 ---------------------------------------------------


def test_queue_gate_audit_consistency_and_mirror_resync(rig):
    platform, clock, pq, actuator = rig
    pq.submit("T01", "n1", CommandKind.BATCH_OPEN, "ST1", ttl_s=None)
    pq.submit("T01", "e1", CommandKind.EMERGENCY_CLOSE, "ST1")
    pq.drain("T01")
    # 队列与物理门态一致。
    assert pq.is_safe("T01")
    assert _open_gates(platform) == []
    # 平台审计含哈希链镜像，且每条都带幂等键。
    pq_entries = [e for e in platform.audit.list() if e.kind.startswith("pq_")]
    assert pq_entries
    history = pq.history("T01")
    assert len(pq_entries) == len(history)
    # 重新再 resync 不会产生重复审计。
    before = len(platform.audit.list())
    added = pq.resync_audit("T01")
    assert added == 0
    assert len(platform.audit.list()) == before
    # 新进程恢复幂等索引后重放同样不重复。
    platform.audit.seed_idempotency()
    runner2 = PriorityQueueRunner(
        platform.store, clock, GateActuator(platform.gates, platform.lockouts),
        audit=platform.audit,
    )
    runner2.resync_audit("T01")
    assert len(platform.audit.list()) == before


def test_platform_endpoints_and_status_summary(rig):
    platform, clock, pq, actuator = rig
    result = platform.pq_submit("T01", "via-platform", "open", "ST1",
                                use_default_ttl=False)
    assert result["ok"] and result["duplicate"] is False
    platform.pq_drain("T01")
    dispositions = platform.pq_dispositions("T01")["dispositions"]
    assert dispositions == []
    status = platform.status()
    assert status["priority_queue"]["active"] is True
    assert any(t["train_id"] == "T01" for t in status["priority_queue"]["trains"])
    # CLI 风格的策略默认值。
    assert status["priority_queue"]["policy"] == "preempt_normal"
