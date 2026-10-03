"""门控执行器抽象与站台门模拟执行器。

队列只负责调度，真正动门的是 :class:`DoorActuator`。接口以"可中断工作单元"为
粒度：一次批量开门被拆成若干个门区单元，队列每泵送一个单元就检查一次紧急抢占，
因此紧急关门最迟在一个门区边界处接管，而不是等整批开门跑完。

:class:`GateActuator` 直接驱动现有的 :class:`~psdline.psd.PSDRegistry`：

* 开门逐门区进行，被联锁锁定的门区记为跳过（沿用批量开门语义，不静默漏开）；
* 关门是幂等的——对已经关闭的门重复关门仍是成功，崩溃恢复时可以安全重放。

真实硬件故障由执行器在 :meth:`run_unit` 里抛 :class:`ActuatorError`；故障/掉电
注入属于验证手段，放在测试里的执行器子类（``FaultInjectingActuator``）中，不进
生产类。
"""

from __future__ import annotations

from typing import Protocol

from ...errors import RailError
from ...psd.lockout import LockoutRegistry
from ...psd.psd import PSDRegistry
from .model import CommandKind, QueuedCommand


class ActuatorError(RailError):
    """执行门控指令时的失败。

    ``transient=True`` 表示下一轮重试可能成功（队列会重新入队）；否则视为持续
    故障，重试耗尽后指令作废并转入故障安全关门。
    """

    def __init__(self, message: str, *, transient: bool = True, component: str = "pq") -> None:
        super().__init__(message, component=component)
        self.transient = transient


class SimulatedCrash(RailError):
    """执行器掉电：物理动作已发生，但进度/完成事件不落盘（恢复测试用）。"""

    def __init__(self, message: str, *, train: str = "") -> None:
        super().__init__(message, component="pq")
        self.train = train


class ExecutionPlan:
    """一条指令拆出的可执行计划。"""

    def __init__(self, command: QueuedCommand, stations: list[str], units: list[str]) -> None:
        self.command = command
        self.stations = stations
        self.units = units

    @property
    def units_total(self) -> int:
        return len(self.units)


class DoorActuator(Protocol):
    """门控执行器协议。"""

    def plan(self, command: QueuedCommand) -> ExecutionPlan:
        """把指令拆成可中断单元。"""

    def run_unit(self, command: QueuedCommand, unit: str) -> None:
        """执行一个最小可中断单元；失败抛 :class:`ActuatorError`。"""

    def physical_state(self, station_id: str) -> str:
        """返回站台物理门态：``open``/``closed``/``mixed``/``unknown``。"""


class GateActuator:
    """基于 :class:`PSDRegistry` 的生产模拟执行器。"""

    def __init__(self, gates: PSDRegistry, lockouts: LockoutRegistry | None = None) -> None:
        self._gates = gates
        self._lockouts = lockouts
        # 门区结构（站台→门区→门标识）在运行期不变，只扫描一次缓存 id 分组；门的
        # 开闭/锁定状态每次按 id 读最新对象（set_open 落盘后会以新对象替换）。
        self._unit_ids: dict[str, list[str]] = {}
        self._group_ids: dict[str, dict[str, list[str]]] = {}

    def _unit_gate_ids(self, station_id: str) -> list[str]:
        ids = self._unit_ids.get(station_id)
        if ids is None:
            ids = [gate.id for gate in self._gates.by_station(station_id)]
            self._unit_ids[station_id] = ids
        return ids

    def _group_gate_ids(self, station_id: str) -> dict[str, list[str]]:
        grouped = self._group_ids.get(station_id)
        if grouped is None:
            grouped = {}
            for gate in self._gates.by_station(station_id):
                if gate.group:
                    grouped.setdefault(gate.group, []).append(gate.id)
            self._group_ids[station_id] = grouped
        return grouped

    def plan(self, command: QueuedCommand) -> ExecutionPlan:
        if command.stations:
            stations = list(command.stations)
        elif command.station_id:
            stations = [command.station_id]
        else:
            stations = sorted({gate.station for gate in self._gates.list() if gate.station})
        units: list[str] = []
        for station_id in stations:
            groups = sorted(self._group_gate_ids(station_id))
            if not groups:
                # 没有门区登记时，至少保留一个以站台为粒度的单元，失败仍可被观察。
                units.append(station_id)
                continue
            if command.kind is CommandKind.BATCH_OPEN:
                units.extend(f"{station_id}:{group}" for group in groups)
            else:
                units.append(station_id)
        return ExecutionPlan(command, stations, units)

    def run_unit(self, command: QueuedCommand, unit: str) -> None:
        station_id = unit.split(":", 1)[0]
        group = unit.split(":", 1)[1] if ":" in unit else ""
        if group:
            ids = self._group_gate_ids(station_id).get(group, [])
        else:
            ids = self._unit_gate_ids(station_id)
        for gate_id in ids:
            gate = self._gates.require(gate_id)
            if command.kind.opens_doors:
                if self._lockouts is not None and self._lockouts.is_locked(gate_id):
                    continue
                self._gates.set_open(gate_id, True)
            elif gate.open:
                # 关门幂等：已关闭的门重复关门保持成功，崩溃后可安全重放整指令。
                self._gates.set_open(gate_id, False)

    def physical_state(self, station_id: str) -> str:
        gates = self._gates.by_station(station_id)
        if not gates:
            return "unknown"
        opened = [gate.open for gate in gates]
        if all(opened):
            return "open"
        if not any(opened):
            return "closed"
        return "mixed"
