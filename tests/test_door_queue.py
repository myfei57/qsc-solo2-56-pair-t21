"""门控指令队列：优先级、时效、作废处置、崩溃恢复与并发。"""

from __future__ import annotations

import threading

from psdline.app import build_platform
from psdline.config import default_config
from psdline.door import (
    close_and_audit,
    close_durable,
    command_state,
    depart,
    dispositions,
    door_state,
    enqueue,
    execute_next,
    process_queue,
)
from psdline.door.queue import command_state_doc_id


def _rebuild(platform, clock, tmp_path):
    """用同一存储目录重建平台，模拟崩溃后重启。"""

    return build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "state"))


def test_urgent_jumps_ahead_of_normal(platform):
    enqueue(platform.doors, "T01", "cmd-open", "open", "ST1")
    enqueue(platform.doors, "T01", "cmd-close", "close", "ST1")
    enqueue(platform.doors, "T01", "cmd-urgent", "close", "ST1", priority="urgent")
    assert process_queue(platform.doors, "T01") == ["cmd-urgent", "cmd-open", "cmd-close"]


def test_urgents_keep_submission_order(platform):
    enqueue(platform.doors, "T01", "cmd-normal", "open", "ST1")
    enqueue(platform.doors, "T01", "cmd-u1", "close", "ST1", priority="urgent")
    enqueue(platform.doors, "T01", "cmd-u2", "close", "ST1", priority="urgent")
    assert process_queue(platform.doors, "T01") == ["cmd-u1", "cmd-u2", "cmd-normal"]


def test_enqueue_is_idempotent(platform):
    first = platform.enqueue("T01", "cmd-dup", "open", "ST1")
    second = platform.enqueue("T01", "cmd-dup", "open", "ST1")
    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    assert platform.commands.ids("T01") == ["cmd-dup"]
    enqueues = [entry for entry in platform.audit.list() if entry.kind == "door_enqueue"]
    assert len(enqueues) == 1


def test_expired_command_is_voided_with_reason(platform, clock):
    platform.enqueue("T01", "cmd-slow", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    outcome = platform.void_expired("T01")
    assert outcome["voided"]["T01"][0]["command_id"] == "cmd-slow"
    assert platform.commands.ids("T01") == []
    voided = [entry for entry in platform.audit.list() if entry.kind == "door_command_voided"]
    assert len(voided) == 1
    assert voided[0].detail["reason"] == "expired"
    assert voided[0].detail["command_id"] == "cmd-slow"
    found = platform.command_dispositions("T01")["dispositions"]
    assert [(item["command_id"], item["outcome"], item["reason"]) for item in found] == [
        ("cmd-slow", "voided", "expired")
    ]


def test_void_settles_state_and_flags_open_door(platform, clock):
    events = []
    platform.bus.subscribe("*", events.append)
    platform.arrive("T01", "ST1", 0)
    platform.enqueue("T01", "cmd-close", "close", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    platform.void_expired("T01")
    state = command_state(platform.doors, "T01")
    assert state["state"] == "idle"
    assert state["hazard"] == "open_without_close_intent"
    assert platform.store.read(command_state_doc_id("T01"))["state"] == "idle"
    assert any(event.action == "open_without_close_intent" for event in events)


def test_execute_next_runs_urgent_first(platform):
    platform.arrive("T01", "ST1", 0)
    platform.enqueue("T01", "cmd-open", "open", "ST1")
    platform.enqueue("T01", "cmd-close", "close", "ST1", priority="urgent")
    assert platform.process_queue("T01")["ids"] == ["cmd-close", "cmd-open"]
    first = platform.execute_next("T01")
    assert first["ok"] is True and first["executed"] == "cmd-close"
    assert close_durable(platform.doors, "T01") is True
    second = platform.execute_next("T01")
    assert second["ok"] is True and second["executed"] == "cmd-open"
    outcomes = {
        item["command_id"]: item["outcome"]
        for item in platform.command_dispositions("T01")["dispositions"]
    }
    assert outcomes == {"cmd-close": "completed", "cmd-open": "completed"}
    assert door_state(platform.doors, "T01")["command_state"] == "idle"


def test_executing_command_is_not_preempted(platform, clock):
    platform.arrive("T01", "ST1", 0)
    platform.enqueue("T01", "cmd-open", "open", "ST1", ttl_s=5.0)
    running = platform.commands.start_next("T01")
    assert running.id == "cmd-open"
    platform.enqueue("T01", "cmd-urgent", "close", "ST1", priority="urgent")
    clock.advance(10.0)  # 超过时效也只约束排队，执行中的指令承诺执行完
    attempt = platform.execute_next("T01")
    assert attempt["ok"] is False
    assert platform.commands.executing("T01").id == "cmd-open"
    assert platform.commands.ids("T01") == ["cmd-urgent"]


def test_execution_failure_voids_command(platform):
    platform.enqueue("T01", "cmd-open", "open", "ST1")
    outcome = platform.execute_next("T01")
    assert outcome["ok"] is False
    assert outcome["violations"]
    assert platform.commands.executing("T01") is None
    assert command_state(platform.doors, "T01")["state"] == "idle"
    found = platform.command_dispositions("T01")["dispositions"]
    assert [(item["command_id"], item["reason"]) for item in found] == [
        ("cmd-open", "execution_failed")
    ]


def test_recovery_completes_executing_when_fact_durable(platform, clock, tmp_path):
    platform.arrive("T01", "ST1", 0)
    platform.enqueue("T01", "cmd-close", "close", "ST1")
    assert platform.commands.start_next("T01").id == "cmd-close"
    close_and_audit(platform.doors, platform.audit, "T01", "ST1", "door_command", clock.now())
    rebuilt = _rebuild(platform, clock, tmp_path)
    recovery = rebuilt.status()["command_recovery"]
    assert recovery["completed"] == ["cmd-close"]
    assert rebuilt.commands.executing("T01") is None
    assert command_state(rebuilt.doors, "T01")["state"] == "idle"
    found = rebuilt.command_dispositions("T01")["dispositions"]
    assert any(
        item["command_id"] == "cmd-close" and item["outcome"] == "completed" for item in found
    )


def test_recovery_voids_interrupted_execution(platform, clock, tmp_path):
    platform.enqueue("T01", "cmd-open", "open", "ST1")
    assert platform.commands.start_next("T01").id == "cmd-open"
    rebuilt = _rebuild(platform, clock, tmp_path)
    recovery = rebuilt.status()["command_recovery"]
    assert recovery["voided"] == ["cmd-open"]
    assert rebuilt.commands.executing("T01") is None
    found = rebuilt.command_dispositions("T01")["dispositions"]
    assert [(item["command_id"], item["reason"]) for item in found] == [
        ("cmd-open", "execution_interrupted")
    ]


def test_recovery_is_idempotent(platform, clock, tmp_path):
    platform.enqueue("T01", "cmd-open", "open", "ST1")
    platform.commands.start_next("T01")
    rebuilt = _rebuild(platform, clock, tmp_path)
    assert rebuilt.status()["command_recovery"]["voided"] == ["cmd-open"]
    again = build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "state"))
    assert again.status()["command_recovery"]["trains"] == []
    voided = [
        entry
        for entry in again.audit.replay()
        if entry.kind == "door_command_voided"
    ]
    assert len(voided) == 1


def test_recovery_completes_dangling_voiding(platform, clock, tmp_path):
    platform.enqueue("T01", "cmd-x", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    claimed = platform.commands.claim_expired("T01", clock.now())
    assert [command.id for command in claimed] == ["cmd-x"]
    # 模拟崩溃：作废区已认领但审计没落盘
    rebuilt = _rebuild(platform, clock, tmp_path)
    assert rebuilt.commands.voiding("T01") == []
    found = rebuilt.command_dispositions("T01")["dispositions"]
    assert [(item["command_id"], item["reason"]) for item in found] == [("cmd-x", "expired")]


def test_recovery_confirms_voided_without_duplicate_audit(platform, clock, tmp_path):
    platform.enqueue("T01", "cmd-y", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    platform.commands.claim_expired("T01", clock.now())
    platform.audit.record(
        "door_command_voided", "T01", "ST1", command_id="cmd-y", reason="expired"
    )
    # 模拟崩溃：审计已落盘但作废区还没确认
    rebuilt = _rebuild(platform, clock, tmp_path)
    assert rebuilt.commands.voiding("T01") == []
    voided = [entry for entry in rebuilt.audit.replay() if entry.kind == "door_command_voided"]
    assert len(voided) == 1


def test_concurrent_enqueue(platform):
    platform.arrive("T01", "ST1", 0)
    errors = []

    def worker(prefix: str) -> None:
        try:
            for index in range(10):
                platform.enqueue("T01", f"{prefix}-{index}", "open", "ST1")
                platform.enqueue("T01", "shared-id", "open", "ST1")
        except Exception as error:  # pragma: no cover - 失败时断言会展开
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(f"w{index}",)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    ids = platform.commands.ids("T01")
    assert len(ids) == 8 * 10 + 1
    assert ids.count("shared-id") == 1
    assert len(set(ids)) == len(ids)


def test_epoch_persists_across_restart(platform, clock, tmp_path):
    enqueue(platform.doors, "T01", "cmd-1", "open", "ST1")
    depart(platform.doors, "T01", at=clock.now())
    assert platform.commands.epoch == 1
    rebuilt = _rebuild(platform, clock, tmp_path)
    assert rebuilt.commands.epoch == 1


def test_depart_policy_blocked_while_executing(platform):
    platform.arrive("T01", "ST1", 0)
    platform.enqueue("T01", "cmd-close", "close", "ST1")
    assert platform.execute_next("T01")["ok"] is True
    policy = platform.process_queue("T01")
    assert policy["ready"] is True
    platform.enqueue("T01", "cmd-open", "open", "ST1")
    platform.commands.start_next("T01")
    assert platform.process_queue("T01")["ready"] is False


def test_process_queue_and_cycle_void_lazily(platform, clock):
    platform.enqueue("T01", "cmd-lazy", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    assert platform.process_queue("T01")["ids"] == []
    platform.enqueue("T01", "cmd-cycle", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    summary = platform.cycle(dt=1.0)
    assert summary["voided_commands"] == {"T01": 1}
    reasons = [
        item["reason"] for item in platform.command_dispositions("T01")["dispositions"]
    ]
    assert reasons == ["expired", "expired"]


def test_dispositions_survive_restart(platform, clock, tmp_path):
    platform.enqueue("T01", "cmd-slow", "open", "ST1", ttl_s=5.0)
    clock.advance(10.0)
    platform.void_expired("T01")
    rebuilt = _rebuild(platform, clock, tmp_path)
    found = rebuilt.command_dispositions("T01")["dispositions"]
    assert [(item["command_id"], item["outcome"]) for item in found] == [
        ("cmd-slow", "voided")
    ]
    assert dispositions(rebuilt.audit, "T01")[0]["reason"] == "expired"
